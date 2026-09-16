from __future__ import annotations

import socket
import time

import pytest

from dfsha.control_node.main import serve as serve_control_node

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
