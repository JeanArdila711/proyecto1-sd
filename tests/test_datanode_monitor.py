from __future__ import annotations

import threading

import grpc

from dfsha.control_node.datanode_monitor import DataNodeMonitor
from dfsha.generated import data_node_pb2


class _Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class _Stub:
    def __init__(self, responds: list[bool]) -> None:
        self._responds = responds
        self.timeouts: list[float] = []

    def Ping(self, request, timeout: float):
        self.timeouts.append(timeout)
        if not self._responds.pop(0):
            raise _Unavailable()
        return object()


class _Channel:
    def __init__(self, stub: _Stub) -> None:
        self.stub = stub
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_monitor_marks_dead_only_after_threshold_and_recovers():
    """D-P1: un fallo aislado no saca al nodo del pipeline; el umbral sí."""
    now = [10.0]
    stub = _Stub([False, False, True])
    channel = _Channel(stub)
    monitor = DataNodeMonitor(
        ["dn1:50061"],
        heartbeat_interval_s=1,
        dead_after_s=6,
        rpc_timeout_s=0.25,
        clock=lambda: now[0],
        channel_factory=lambda _: channel,
        stub_factory=lambda _: stub,
    )

    monitor._probe_once()
    assert monitor.is_alive("dn1:50061")
    assert monitor.dead_for("dn1:50061") == 0

    now[0] = 16.0
    monitor._probe_once()
    assert not monitor.is_alive("dn1:50061")
    assert monitor.dead_for("dn1:50061") == 6

    now[0] = 17.0
    monitor._probe_once()
    assert monitor.is_alive("dn1:50061")
    assert monitor.dead_for("dn1:50061") is None
    assert stub.timeouts == [0.25, 0.25, 0.25]
    monitor.stop()
    assert channel.closed


def test_monitor_stop_joins_thread_and_closes_channels():
    stub = _Stub([True] * 20)
    channel = _Channel(stub)
    monitor = DataNodeMonitor(
        ["dn1:50061"],
        heartbeat_interval_s=0.01,
        dead_after_s=0.05,
        rpc_timeout_s=0.01,
        channel_factory=lambda _: channel,
        stub_factory=lambda _: stub,
    )

    monitor.start()
    monitor.stop()

    assert monitor.stop_event.is_set()
    assert not monitor.thread.is_alive()
    assert channel.closed
    assert not [t for t in threading.enumerate() if t.name == monitor.thread.name]


def test_ping_request_is_empty_contract():
    assert data_node_pb2.PingRequest().SerializeToString() == b""


def test_control_server_stop_stops_its_monitor(tmp_path):
    from conftest import FAST_RAFT_CONF, free_port
    from dfsha.control_node.main import serve

    server, _, raft = serve(
        ["localhost:1", "localhost:2"],
        "localhost",
        0,
        raft_self=f"localhost:{free_port()}",
        raft_peers=[],
        data_dir=tmp_path / "raft",
        raft_conf_overrides=FAST_RAFT_CONF,
        heartbeat_interval_s=0.01,
        datanode_dead_after_s=0.04,
    )
    monitor = server._dfsha_datanode_monitor
    try:
        server.stop(grace=None)
        assert monitor.stop_event.is_set()
        assert not monitor.thread.is_alive()
    finally:
        raft.destroy()
