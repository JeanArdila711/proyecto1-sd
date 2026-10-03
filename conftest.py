from __future__ import annotations

import socket
import time

import pytest

from dfsha.control_node.main import serve as serve_control_node

# Llave cruda fija exclusivamente para pruebas de contenedor cifrado C4.
TEST_ENCRYPTION_KEY = bytes.fromhex("00" * 32)

# Raft rápido para tests: un clúster de 1 nodo se elige líder en ~0.1 s en vez de
# ~1 s. pysyncobj exige raftMinTimeout > 3 * appendEntriesPeriod.
FAST_RAFT_CONF = {
    "raftMinTimeout": 0.05,
    "raftMaxTimeout": 0.1,
    "appendEntriesPeriod": 0.01,
    "leaderFallbackTimeout": 0.5,
    "autoTickPeriod": 0.005,
}


def free_port() -> int:
    # ponytail: el puerto se libera antes de que Raft lo tome; otro proceso podría
    # ganarlo en el medio. Improbable en una máquina de desarrollo.
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def wait_for(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def wait_until_datanode_excluded(channel, address: str, timeout: float = 15.0) -> bool:
    """Espera a que el monitor del ControlNode deje de ofrecer `address` en los pipelines.

    No basta con dormir un tiempo fijo: en Windows un connect a un puerto cerrado
    tarda ~2 s en fallar (en Linux es inmediato), así que el Ping que declara muerto
    al nodo llega mucho después que en Linux. Se sondea con BeginUpload y se aborta
    la sonda; UNAVAILABLE (menos réplicas vivas que el mínimo) también cuenta.
    """
    import uuid

    import grpc

    from dfsha.generated import control_node_pb2, control_node_pb2_grpc

    stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)

    def excluded() -> bool:
        op_id = uuid.uuid4().hex
        try:
            response = stub.BeginUpload(
                control_node_pb2.BeginUploadRequest(path="/__sonda__", size_bytes=1, op_id=op_id)
            )
        except grpc.RpcError as exc:
            return exc.code() == grpc.StatusCode.UNAVAILABLE
        stub.AbortUpload(control_node_pb2.AbortUploadRequest(path="/__sonda__", op_id=f"{op_id}-abort"))
        return address not in response.blocks[0].datanode_addresses

    return wait_for(excluded, timeout)


@pytest.fixture
def start_control_node():
    """Levanta un ControlNode con un clúster Raft de 1 nodo (log en memoria) y
    devuelve su dirección gRPC. Se destruye solo al terminar el test."""
    started = []

    def _start(datanode_addresses: list[str], **kwargs) -> str:
        kwargs.setdefault("raft_conf_overrides", FAST_RAFT_CONF)
        server, port, raft = serve_control_node(
            datanode_addresses,
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            **kwargs,
        )
        started.append((server, raft))
        assert wait_for(raft._isLeader), "el ControlNode de un nodo no se eligió líder"
        return f"localhost:{port}"

    yield _start

    for server, raft in started:
        server.stop(grace=None)
        raft.destroy()
