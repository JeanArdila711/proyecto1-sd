from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable

import grpc

from dfsha.generated import data_node_pb2, data_node_pb2_grpc

DEFAULT_HEARTBEAT_INTERVAL_S = 2.0
DEFAULT_DATANODE_DEAD_AFTER_S = 6.0
DEFAULT_PING_TIMEOUT_S = 1.0


class DataNodeMonitor:
    """Estado blando local de los DataNodes observado mediante Ping pull.

    Cada ControlNode mantiene su propia instancia; nada de este estado pasa por
    Raft. Un nodo permanece elegible durante el período de sospecha para no
    convertir una pérdida aislada en una falsa caída.
    """

    def __init__(
        self,
        addresses: list[str],
        *,
        heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
        dead_after_s: float = DEFAULT_DATANODE_DEAD_AFTER_S,
        rpc_timeout_s: float = DEFAULT_PING_TIMEOUT_S,
        clock: Callable[[], float] = time.monotonic,
        channel_factory: Callable[[str], grpc.Channel] = grpc.insecure_channel,
        stub_factory: Callable[[grpc.Channel], object] = data_node_pb2_grpc.DataNodeServiceStub,
    ) -> None:
        if heartbeat_interval_s <= 0 or dead_after_s <= 0 or rpc_timeout_s <= 0:
            raise ValueError("los tiempos del monitor deben ser mayores que cero")
        self._addresses = list(addresses)
        self._heartbeat_interval_s = heartbeat_interval_s
        self._dead_after_s = dead_after_s
        self._rpc_timeout_s = rpc_timeout_s
        self._clock = clock
        self._channels = {address: channel_factory(address) for address in self._addresses}
        self._stubs = {address: stub_factory(channel) for address, channel in self._channels.items()}
        self._alive = {address: True for address in self._addresses}
        self._dead_since = {address: None for address in self._addresses}
        self._lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="dfsha-datanode-monitor", daemon=False)
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self._started:
            # Ping tiene deadline explícito; por tanto join no puede esperar una
            # llamada de red indefinida ni dejar el hilo vivo al apagar.
            self.thread.join()
        for channel in self._channels.values():
            channel.close()

    def alive_addresses(self) -> list[str]:
        with self._lock:
            return [address for address in self._addresses if self._alive[address]]

    def is_alive(self, address: str) -> bool:
        with self._lock:
            return bool(self._alive.get(address, False))

    def dead_for(self, address: str) -> float | None:
        with self._lock:
            dead_since = self._dead_since.get(address)
        return None if dead_since is None else max(0.0, self._clock() - dead_since)

    def _probe_once(self) -> None:
        for address in self._addresses:
            try:
                self._stubs[address].Ping(data_node_pb2.PingRequest(), timeout=self._rpc_timeout_s)
            except grpc.RpcError:
                now = self._clock()
                with self._lock:
                    if self._dead_since[address] is None:
                        self._dead_since[address] = now
                    if now - self._dead_since[address] >= self._dead_after_s:
                        self._alive[address] = False
            else:
                with self._lock:
                    self._alive[address] = True
                    self._dead_since[address] = None

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self._probe_once()
            # Jitter pequeño evita que todos los ControlNodes reintenten Ping a la
            # vez tras una caída, sin cambiar el límite superior de detección.
            delay = self._heartbeat_interval_s * random.uniform(0.9, 1.1)
            self.stop_event.wait(delay)
