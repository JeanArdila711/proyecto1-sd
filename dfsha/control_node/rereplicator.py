from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from collections.abc import Callable

import grpc

from dfsha.client.distributed_client import (
    DEFAULT_BLOCK_TRANSFER_BASE_TIMEOUT_S,
    DEFAULT_MIN_TRANSFER_THROUGHPUT_BYTES_PER_S,
)
from dfsha.control_node.datanode_monitor import DataNodeMonitor
from dfsha.generated import data_node_pb2, data_node_pb2_grpc

logger = logging.getLogger(__name__)

DEFAULT_REREPLICATION_INTERVAL_S = 10.0
DEFAULT_REREPLICATION_DELAY_S = 30.0
DEFAULT_REREPLICATION_MAX_PER_CYCLE = 4
_RETRY_BACKOFF_S = 0.2
_MAX_RETRY_BACKOFF_S = 2.0


class ReReplicator:
    """Repara copias faltantes desde la metadata confirmada por Raft.

    El trabajo no tiene cola propia: tras un failover, un líder nuevo toma otra
    fotografía y deduce de nuevo las reparaciones pendientes del árbol replicado.
    """

    def __init__(
        self,
        servicer,
        monitor: DataNodeMonitor,
        *,
        replication_factor: int,
        interval_s: float = DEFAULT_REREPLICATION_INTERVAL_S,
        delay_s: float = DEFAULT_REREPLICATION_DELAY_S,
        max_per_cycle: int = DEFAULT_REREPLICATION_MAX_PER_CYCLE,
        transfer_base_timeout_s: float = DEFAULT_BLOCK_TRANSFER_BASE_TIMEOUT_S,
        minimum_transfer_throughput_bytes_per_s: float = DEFAULT_MIN_TRANSFER_THROUGHPUT_BYTES_PER_S,
        max_attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = lambda: random.uniform(0.5, 1.5),
        channel_factory: Callable[[str], grpc.Channel] = grpc.insecure_channel,
        stub_factory: Callable[[grpc.Channel], object] = data_node_pb2_grpc.DataNodeServiceStub,
    ) -> None:
        if (
            replication_factor < 1
            or interval_s <= 0
            or delay_s < 0
            or max_per_cycle < 1
            or transfer_base_timeout_s <= 0
            or minimum_transfer_throughput_bytes_per_s <= 0
            or max_attempts < 1
        ):
            raise ValueError("la configuración del re-replicador no es válida")
        self._servicer = servicer
        self._monitor = monitor
        self._replication_factor = replication_factor
        self._interval_s = interval_s
        self._delay_s = delay_s
        self._max_per_cycle = max_per_cycle
        self._transfer_base_timeout_s = transfer_base_timeout_s
        self._minimum_transfer_throughput_bytes_per_s = minimum_transfer_throughput_bytes_per_s
        self._max_attempts = max_attempts
        self._sleep = sleep
        self._jitter = jitter
        self._channel_factory = channel_factory
        self._stub_factory = stub_factory
        self._channels: dict[str, grpc.Channel] = {}
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="dfsha-rereplicator", daemon=False)
        self._started = False
        self.lost_blocks: list[tuple[str, str]] = []
        # (block_id, destino) de copias hechas cuyo commit todavía no entró: el recolector
        # (A3) no las toca aunque la metadata aún no las nombre.
        self._in_flight: set[tuple[str, str]] = set()
        self._in_flight_lock = threading.Lock()
        self.ignored_deleted_paths: list[str] = []

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

    def in_flight_copies(self) -> set[tuple[str, str]]:
        with self._in_flight_lock:
            return set(self._in_flight)

    def _datanode_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = self._channel_factory(address)
        return self._stub_factory(self._channels[address])

    def _block_timeout(self, size_bytes: int) -> float:
        return self._transfer_base_timeout_s + size_bytes / self._minimum_transfer_throughput_bytes_per_s

    def _retry_delay(self, attempt: int) -> float:
        return min(_RETRY_BACKOFF_S * (2**attempt), _MAX_RETRY_BACKOFF_S) * self._jitter()

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.run_cycle()
            self.stop_event.wait(self._interval_s)

    def run_cycle(self) -> None:
        """Ejecuta a lo sumo ``max_per_cycle`` reparaciones como líder actual."""
        if not self._servicer._is_leader_raw() or not self._servicer._read_barrier_raw():
            return

        # iter_blocks produce nuevos BlockRecord/listas: no guardar referencias al árbol.
        snapshot = self._servicer._replicated.tree.iter_blocks()
        alive = self._monitor.alive_addresses()
        completed = 0
        self.lost_blocks = []
        self.ignored_deleted_paths = []
        for path, block in snapshot:
            if completed >= self._max_per_cycle or self.stop_event.is_set():
                break
            expected = list(block.datanode_addresses)
            live = [address for address in expected if address in alive]
            if len(live) >= self._replication_factor:
                continue
            if not live:
                self.lost_blocks.append((path, block.block_id))
                logger.warning("bloque perdido sin réplicas vivas: %s en %s", block.block_id, path)
                continue
            # Una réplica recién caída puede ser un reinicio: se repara solo cuando lleva
            # más de delay_s muerta, sin dormir dentro del ciclo (el próximo ciclo vuelve a
            # mirar). Un bloque escrito con menos copias (D-P2), sin réplicas muertas, no
            # espera nada.
            if any(self._dead_for(address) < self._delay_s for address in expected if address not in alive):
                continue
            target = next((address for address in alive if address not in expected), None)
            if target is None:
                continue
            if not self._servicer._is_leader_raw():
                return
            new = live + [target]
            # Si un origen tiene la copia corrupta o no la tiene, se prueba el siguiente
            # vivo en vez de insistir con el primero.
            with self._in_flight_lock:
                self._in_flight.add((block.block_id, target))
            try:
                for source in live:
                    if self._replicate(block.block_id, source, target, block.size_bytes, block.checksum):
                        self._commit_replicas(path, block.block_id, expected, new)
                        break
            finally:
                with self._in_flight_lock:
                    self._in_flight.discard((block.block_id, target))
            completed += 1

    def _dead_for(self, address: str) -> float:
        """Segundos que lleva muerta; una dirección que el monitor no conoce (se quitó de
        la configuración) cuenta como muerta hace mucho: sus copias hay que reponerlas."""
        dead_for = self._monitor.dead_for(address)
        return float("inf") if dead_for is None else dead_for

    def _replicate(
        self, block_id: str, source: str, target: str, size_bytes: int, expected_checksum: str
    ) -> bool:
        request = data_node_pb2.ReplicateBlockRequest(block_id=block_id, target=target)
        for attempt in range(self._max_attempts):
            try:
                response = self._datanode_stub(source).ReplicateBlock(
                    request, timeout=self._block_timeout(size_bytes)
                )
                if response.checksum == expected_checksum:
                    return True
                logger.warning("checksum inesperado al replicar bloque %s", block_id)
                return False
            except grpc.RpcError as exc:
                if exc.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                    logger.warning("falló la réplica de %s: %s", block_id, exc.details())
                    return False
                if attempt + 1 == self._max_attempts:
                    logger.warning("no se pudo copiar %s desde %s", block_id, source)
                    return False
                self._sleep(self._retry_delay(attempt))
        return False

    def _commit_replicas(self, path: str, block_id: str, expected: list[str], new: list[str]) -> None:
        op_id = uuid.uuid4().hex
        for attempt in range(self._max_attempts):
            outcome = self._servicer._commit_raw(
                op_id, "update_block_replicas", path, block_id, expected, new
            )
            if outcome[0] == "ok":
                return
            if outcome[0] == "unknown":
                if attempt + 1 < self._max_attempts:
                    self._sleep(self._retry_delay(attempt))
                    continue
                return
            if outcome[0] == "error" and outcome[1] == "PathNotFoundError":
                # La copia de destino queda huérfana; A3 la recolecta después.
                self.ignored_deleted_paths.append(path)
                return
            if outcome[0] == "error" and outcome[1] == "ConflictError":
                return
            return
