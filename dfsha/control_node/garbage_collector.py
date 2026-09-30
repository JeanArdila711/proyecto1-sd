from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable

import grpc

from dfsha.control_node.datanode_monitor import DataNodeMonitor
from dfsha.generated import data_node_pb2, data_node_pb2_grpc

logger = logging.getLogger(__name__)

DEFAULT_GC_INTERVAL_S = 60.0
# El doble del lease de subida por defecto (600 s). La metadata ya protege subidas,
# reservas COW y copias del re-replicador de este proceso; la gracia cubre lo que el
# líder actual no ve: copias en vuelo de un líder anterior y carreras entre nodos.
DEFAULT_GC_GRACE_S = 1200.0
DEFAULT_GC_RPC_TIMEOUT_S = 30.0


class GarbageCollector:
    """Borra de los DataNodes los bloques que la metadata ya no usa.

    Dos casos: huérfanos (ningún archivo, subida ni reserva los nombra) y réplicas
    sobrantes (el bloque sigue en uso, pero la metadata no se lo asigna a ese nodo,
    por ejemplo un nodo que volvió después de que re-replicaron su contenido). No
    guarda estado entre ciclos: tras un failover, el líder nuevo parte de cero.
    """

    def __init__(
        self,
        servicer,
        monitor: DataNodeMonitor,
        *,
        interval_s: float = DEFAULT_GC_INTERVAL_S,
        grace_s: float = DEFAULT_GC_GRACE_S,
        rpc_timeout_s: float = DEFAULT_GC_RPC_TIMEOUT_S,
        rereplicator=None,
        clock: Callable[[], float] = time.time,
        channel_factory: Callable[[str], grpc.Channel] = grpc.insecure_channel,
        stub_factory: Callable[[grpc.Channel], object] = data_node_pb2_grpc.DataNodeServiceStub,
    ) -> None:
        if interval_s <= 0 or grace_s < 0 or rpc_timeout_s <= 0:
            raise ValueError("la configuración del recolector no es válida")
        self._servicer = servicer
        self._monitor = monitor
        self._interval_s = interval_s
        self._grace_s = grace_s
        self._rpc_timeout_s = rpc_timeout_s
        self._rereplicator = rereplicator
        self._clock = clock
        self._channel_factory = channel_factory
        self._stub_factory = stub_factory
        self._channels: dict[str, grpc.Channel] = {}
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="dfsha-garbage-collector", daemon=False)
        self._started = False
        self.deleted: list[tuple[str, str]] = []

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self._started:
            self.thread.join()
        for channel in self._channels.values():
            channel.close()

    def _datanode_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = self._channel_factory(address)
        return self._stub_factory(self._channels[address])

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.run_cycle()
            except Exception:  # un bug no puede matar el hilo: el próximo ciclo reintenta
                logger.exception("falló un ciclo del recolector")
            # jitter: los ciclos de líderes sucesivos no quedan sincronizados
            self.stop_event.wait(self._interval_s * random.uniform(0.9, 1.1))

    def run_cycle(self) -> None:
        self.deleted = []
        if not self._servicer._is_leader_raw():
            return
        # 1) Primero el inventario de cada DataNode vivo...
        stored: dict[str, list[tuple[str, float]]] = {}
        for address in self._monitor.alive_addresses():
            try:
                stored[address] = [
                    (block.block_id, block.age_s)
                    for block in self._datanode_stub(address).ListStoredBlocks(
                        data_node_pb2.ListStoredBlocksRequest(), timeout=self._rpc_timeout_s
                    )
                ]
            except grpc.RpcError as exc:
                logger.warning("no se pudo listar %s: %s", address, exc.details())  # se salta
        # 2) ...y DESPUÉS la metadata, al día por la barrera. Con este orden, un bloque
        # reservado entre las dos lecturas ya figura en la foto y no se borra.
        if not self._servicer._read_barrier_raw():
            return
        # Las copias en vuelo se leen ANTES que la metadata: una copia que se confirma
        # entre las dos lecturas sale de _in_flight pero ya figura en `referenced`.
        in_flight = self._rereplicator.in_flight_copies() if self._rereplicator else set()
        referenced = self._servicer._replicated.tree.referenced_blocks(self._clock())
        for address, blocks in stored.items():
            for block_id, age_s in blocks:
                if address in referenced.get(block_id, ()) or (block_id, address) in in_flight:
                    continue
                if age_s < self._grace_s:
                    continue
                if not self._servicer._is_leader_raw():
                    return  # perdió el liderazgo a mitad de ciclo: el nuevo líder decide
                try:
                    self._datanode_stub(address).DeleteBlock(
                        data_node_pb2.DeleteBlockRequest(block_id=block_id), timeout=self._rpc_timeout_s
                    )
                except grpc.RpcError as exc:
                    if exc.code() != grpc.StatusCode.NOT_FOUND:
                        logger.warning("no se pudo borrar %s en %s: %s", block_id, address, exc.details())
                    continue  # NOT_FOUND: ya no estaba, borrar es idempotente
                self.deleted.append((address, block_id))
