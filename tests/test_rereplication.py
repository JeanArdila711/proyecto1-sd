from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.common.exceptions import ConflictError, PathNotFoundError
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.tree import ControlTree
from dfsha.data_node import block_store
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, data_node_pb2, data_node_pb2_grpc

BLOCK_ID = "1234567890abcdef1234567890abcdef"


def _committed_tree(addresses: list[str]) -> ControlTree:
    tree = ControlTree()
    tree.begin_upload("/archivo.bin", [(BLOCK_ID, addresses)])
    tree.confirm_block("/archivo.bin", BLOCK_ID, "checksum", 2)
    tree.complete_upload("/archivo.bin")
    return tree


def test_update_block_replicas_requires_exact_expected_value_and_copies_inputs():
    tree = _committed_tree(["dn1", "dn2"])
    expected = ["dn1", "dn2"]
    replacement = ["dn1", "dn3"]

    tree.update_block_replicas("/archivo.bin", BLOCK_ID, expected, replacement)
    expected.append("mutated")
    replacement.append("mutated")

    assert tree.list_blocks("/archivo.bin")[0].datanode_addresses == ["dn1", "dn3"]
    with pytest.raises(ConflictError):
        tree.update_block_replicas("/archivo.bin", BLOCK_ID, ["dn1", "dn2"], ["dn1", "dn4"])
    assert tree.list_blocks("/archivo.bin")[0].datanode_addresses == ["dn1", "dn3"]


def test_iter_blocks_is_a_copied_snapshot():
    tree = _committed_tree(["dn1", "dn2"])

    snapshot = tree.iter_blocks()
    path, block = snapshot[0]
    block.datanode_addresses.append("dn3")
    block.checksum = "other"

    assert path == "/archivo.bin"
    stored = tree.list_blocks("/archivo.bin")[0]
    assert stored.datanode_addresses == ["dn1", "dn2"]
    assert stored.checksum == "checksum"


def test_update_block_replicas_is_a_replicated_mutation():
    replicated = ReplicatedTree()

    def apply(op_id, method, *args):
        return replicated.apply(op_id, method, args, _doApply=True)

    apply("upload", "begin_upload", "/archivo.bin", [(BLOCK_ID, ["dn1", "dn2"])])
    apply("confirm", "confirm_block", "/archivo.bin", BLOCK_ID, "checksum", 2)
    apply("complete", "complete_upload", "/archivo.bin")

    assert apply(
        "repair", "update_block_replicas", "/archivo.bin", BLOCK_ID, ["dn1", "dn2"], ["dn1", "dn3"]
    ) == ("ok", None)
    conflict = apply(
        "stale", "update_block_replicas", "/archivo.bin", BLOCK_ID, ["dn1", "dn2"], ["dn1", "dn4"]
    )
    assert conflict[0:2] == ("error", "ConflictError")


@pytest.fixture
def replicated_datanodes(tmp_path):
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_server, source_port = serve_data_node(source_root, "localhost", 0, TEST_ENCRYPTION_KEY)
    target_server, target_port = serve_data_node(target_root, "localhost", 0, TEST_ENCRYPTION_KEY)
    source_channel = grpc.insecure_channel(f"localhost:{source_port}")
    source_stub = data_node_pb2_grpc.DataNodeServiceStub(source_channel)
    try:
        def chunks():
            yield data_node_pb2.WriteBlockChunk(header=data_node_pb2.WriteBlockHeader(block_id=BLOCK_ID))
            yield data_node_pb2.WriteBlockChunk(data=b"replicar bloque completo")

        source_stub.WriteBlock(chunks(), timeout=2)
        yield source_stub, target_root, f"localhost:{target_port}"
    finally:
        source_channel.close()
        source_server.stop(grace=None)
        target_server.stop(grace=None)


def test_replicate_block_copies_verified_source_to_target(replicated_datanodes):
    source_stub, target_root, target = replicated_datanodes

    response = source_stub.ReplicateBlock(
        data_node_pb2.ReplicateBlockRequest(block_id=BLOCK_ID, target=target), timeout=2
    )

    assert response.bytes_written == len(b"replicar bloque completo")
    assert b"".join(
        block_store.read_block(target_root, TEST_ENCRYPTION_KEY, BLOCK_ID, 1024)
    ) == b"replicar bloque completo"
    assert not (target_root / f"{BLOCK_ID}.sha256").exists()


class _Unavailable(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "origen caído"


class _Monitor:
    def __init__(self, alive: list[str], dead: set[str] = frozenset()):
        self.alive = alive
        self.dead = dead

    def alive_addresses(self):
        return list(self.alive)

    def dead_for(self, address):
        return 1.0 if address in self.dead else None


@dataclass
class _Block:
    block_id: str = BLOCK_ID
    datanode_addresses: list[str] | None = None
    checksum: str = "checksum"
    size_bytes: int = 2 * 1024 * 1024

    def __post_init__(self):
        if self.datanode_addresses is None:
            self.datanode_addresses = ["dn1", "dn2"]


class _Replicated:
    def __init__(self, blocks):
        self.tree = type("Tree", (), {"iter_blocks": lambda _: [("/archivo.bin", block) for block in blocks]})()


class _Servicer:
    def __init__(self, blocks, *, leader=True, commit_outcomes=None):
        self._replicated = _Replicated(blocks)
        self.leader = leader
        self.commits = []
        self.commit_outcomes = list(commit_outcomes or [("ok", None)])

    def _is_leader_raw(self):
        return self.leader

    def _read_barrier_raw(self):
        return self.leader

    def _commit_raw(self, op_id, method, *args):
        self.commits.append((op_id, method, args))
        return self.commit_outcomes.pop(0) if self.commit_outcomes else ("ok", None)


class _ReplicateStub:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def ReplicateBlock(self, request, timeout):
        self.calls.append((request, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def test_rereplicator_ignores_source_failure_and_retries_next_cycle(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    block = _Block()
    servicer = _Servicer([block])
    replica = _ReplicateStub([_Unavailable()])
    worker = ReReplicator(
        servicer,
        _Monitor(["dn1", "dn3"], {"dn2"}),
        replication_factor=2,
        delay_s=0,
        max_per_cycle=1,
        max_attempts=1,
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    worker.run_cycle()

    assert not servicer.commits
    assert block.datanode_addresses == ["dn1", "dn2"]
    assert len(replica.calls) == 1


def test_rereplicator_ignores_deleted_file_and_leaves_orphan_for_a3(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    block = _Block()
    servicer = _Servicer([block], commit_outcomes=[("error", "PathNotFoundError", "borrado")])
    replica = _ReplicateStub([data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2)])
    worker = ReReplicator(servicer, _Monitor(["dn1", "dn3"], {"dn2"}), replication_factor=2, delay_s=0)
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    worker.run_cycle()

    assert len(replica.calls) == 1
    assert block.datanode_addresses == ["dn1", "dn2"]
    assert worker.ignored_deleted_paths == ["/archivo.bin"]


def test_rereplicator_rebuilds_snapshot_after_leadership_change(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    block = _Block()
    old_leader = _Servicer([block], leader=False)
    new_leader = _Servicer([block])
    replica = _ReplicateStub([data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2)])
    monitor = _Monitor(["dn1", "dn3"], {"dn2"})
    old_worker = ReReplicator(old_leader, monitor, replication_factor=2, delay_s=0)
    new_worker = ReReplicator(new_leader, monitor, replication_factor=2, delay_s=0)
    monkeypatch.setattr(old_worker, "_datanode_stub", lambda _: replica)
    monkeypatch.setattr(new_worker, "_datanode_stub", lambda _: replica)

    old_worker.run_cycle()
    new_worker.run_cycle()

    assert not old_leader.commits
    assert len(new_leader.commits) == 1


def test_rereplicator_reports_lost_blocks_without_stopping():
    from dfsha.control_node.rereplicator import ReReplicator

    block = _Block(datanode_addresses=["dn1", "dn2"])
    worker = ReReplicator(_Servicer([block]), _Monitor(["dn3"], {"dn1", "dn2"}), replication_factor=3, delay_s=0)

    worker.run_cycle()

    assert worker.lost_blocks == [("/archivo.bin", BLOCK_ID)]


def test_rereplicator_uses_size_deadline_retries_idempotent_copy_and_stops(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    block = _Block()
    servicer = _Servicer([block], commit_outcomes=[("unknown", "timeout"), ("ok", None)])
    replica = _ReplicateStub(
        [_Unavailable(), data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2)]
    )
    sleeps = []
    worker = ReReplicator(
        servicer,
        _Monitor(["dn1", "dn3"], {"dn2"}),
        replication_factor=2,
        interval_s=10,
        delay_s=0,
        transfer_base_timeout_s=1,
        minimum_transfer_throughput_bytes_per_s=1024 * 1024,
        max_attempts=2,
        sleep=lambda value: sleeps.append(value),
        jitter=lambda: 1.0,
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    worker.run_cycle()
    idle_worker = ReReplicator(
        _Servicer([]),
        _Monitor(["dn1"]),
        replication_factor=1,
        interval_s=10,
        delay_s=0,
    )
    idle_worker.start()
    idle_worker.stop()

    assert [timeout for _, timeout in replica.calls] == [3.0, 3.0]
    assert len(servicer.commits) == 2
    assert sleeps == [0.2, 0.2]
    assert idle_worker.stop_event.is_set()
    assert not idle_worker.thread.is_alive()
    assert block.datanode_addresses == ["dn1", "dn2"]


def test_commit_raw_has_no_servicer_context_and_preserves_structured_outcomes():
    import inspect

    from dfsha.control_node.servicer import ControlNodeServicer
    from pysyncobj import SyncObjException

    class Raft:
        def _isLeader(self):
            return True

    class Replicated:
        def __init__(self):
            self.outcomes = [("ok", "done"), ("error", "ConflictError", "stale")]

        def apply(self, *args, **kwargs):
            outcome = self.outcomes.pop(0)
            if outcome == "timeout":
                raise SyncObjException("timeout")
            return outcome

    replicated = Replicated()
    servicer = ControlNodeServicer(Raft(), replicated, ["dn1"], 1, min_write_replicas=1)

    assert "context" not in inspect.signature(servicer._commit_raw).parameters
    assert servicer._commit_raw("op-ok", "update_block_replicas") == ("ok", "done")
    assert servicer._commit_raw("op-conflict", "update_block_replicas") == (
        "error", "ConflictError", "stale"
    )
    replicated.outcomes = ["timeout"]
    assert servicer._commit_raw("op-timeout", "update_block_replicas") [0] == "unknown"


def test_three_node_degraded_upload_is_rereplicated_when_node_returns(tmp_path, start_control_node):
    from conftest import wait_for
    from dfsha.client.distributed_client import DistributedDFShaClient

    roots = [tmp_path / f"dn{i}" for i in range(1, 4)]
    data_servers = []
    addresses = []
    for root in roots:
        server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY)
        data_servers.append(server)
        addresses.append(f"localhost:{port}")
    control_address = start_control_node(
        addresses,
        block_size_bytes=8,
        replication_factor=3,
        min_write_replicas=2,
        heartbeat_interval_s=0.01,
        datanode_dead_after_s=0.03,
        rereplication_interval_s=0.01,
        rereplication_delay_s=0,
        rereplication_max_per_cycle=1,
    )
    client = DistributedDFShaClient([control_address], rpc_timeout_s=1, failover_budget_s=3)
    try:
        data_servers[2].stop(grace=None)
        from conftest import wait_until_datanode_excluded

        assert wait_until_datanode_excluded(client._control_channels[control_address], addresses[2])
        assert len(
            client._call(
                "BeginUpload", control_node_pb2.BeginUploadRequest(path="/probe", size_bytes=1, op_id="probe")
            ).blocks[0].datanode_addresses
        ) == 2
        client._call(
            "AbortUpload",
            control_node_pb2.AbortUploadRequest(path="/probe", op_id="abort-probe"),
        )
        source = tmp_path / "origen.bin"
        source.write_bytes(b"bloque de prueba")
        client.upload(source, "/archivo.bin")
        before = client._call(
            "ListBlocks", control_node_pb2.ListBlocksRequest(path="/archivo.bin")
        ).blocks[0]
        assert len(before.datanode_addresses) == 2
        client.download("/archivo.bin", tmp_path / "con-dn3-caido.bin")
        assert (tmp_path / "con-dn3-caido.bin").read_bytes() == source.read_bytes()

        data_servers[2], port = serve_data_node(
            roots[2], "localhost", int(addresses[2].rsplit(":", 1)[1]), TEST_ENCRYPTION_KEY
        )
        assert wait_for(
            lambda: len(
                client._call(
                    "ListBlocks",
                    control_node_pb2.ListBlocksRequest(path="/archivo.bin"),
                ).blocks[0].datanode_addresses
            ) == 3,
            timeout=5,
        )
        after = client._call(
            "ListBlocks", control_node_pb2.ListBlocksRequest(path="/archivo.bin")
        ).blocks[0]
        assert addresses[2] in after.datanode_addresses
    finally:
        client.close()
        for server in data_servers:
            server.stop(grace=None)


def test_rereplicator_limits_copies_per_cycle(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    blocks = [_Block(block_id="a" * 32), _Block(block_id="b" * 32)]
    servicer = _Servicer(blocks)
    replica = _ReplicateStub(
        [
            data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2),
            data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2),
        ]
    )
    worker = ReReplicator(
        servicer, _Monitor(["dn1", "dn3"], {"dn2"}), replication_factor=2, delay_s=0, max_per_cycle=1
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    worker.run_cycle()

    assert len(replica.calls) == 1
    assert len(servicer.commits) == 1


def test_four_nodes_repair_after_one_registered_replica_dies(tmp_path, start_control_node):
    from conftest import wait_for
    from dfsha.client.distributed_client import DistributedDFShaClient

    roots = [tmp_path / f"dn{i}" for i in range(1, 5)]
    data_servers = []
    addresses = []
    for root in roots:
        server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY)
        data_servers.append(server)
        addresses.append(f"localhost:{port}")
    control_address = start_control_node(
        addresses,
        block_size_bytes=8,
        replication_factor=3,
        min_write_replicas=2,
        heartbeat_interval_s=0.01,
        datanode_dead_after_s=0.03,
        rereplication_interval_s=0.01,
        rereplication_delay_s=0,
        rereplication_max_per_cycle=1,
    )
    client = DistributedDFShaClient([control_address], rpc_timeout_s=1, failover_budget_s=3)
    try:
        source = tmp_path / "origen.bin"
        source.write_bytes(b"cuatro nodos")
        client.upload(source, "/archivo.bin")
        before = client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path="/archivo.bin")).blocks[0]
        assert before.datanode_addresses == addresses[:3]

        data_servers[2].stop(grace=None)
        time.sleep(1.2)
        assert wait_for(
            lambda: client._call(
                "ListBlocks", control_node_pb2.ListBlocksRequest(path="/archivo.bin")
            ).blocks[0].datanode_addresses == [addresses[0], addresses[1], addresses[3]],
            timeout=5,
        )
        client.download("/archivo.bin", tmp_path / "recuperado.bin")
        assert (tmp_path / "recuperado.bin").read_bytes() == source.read_bytes()
    finally:
        client.close()
        for server in data_servers:
            server.stop(grace=None)


# --- Correcciones de revisión: delay por tiempo de muerte y origen corrupto ---------


class _TimedMonitor:
    """Monitor con el tiempo exacto que lleva muerta cada dirección caída."""

    def __init__(self, alive: list[str], dead_for: dict[str, float]):
        self.alive = alive
        self._dead_for = dead_for

    def alive_addresses(self):
        return list(self.alive)

    def dead_for(self, address):
        return self._dead_for.get(address)


class _RpcFailure(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return self._code.name


def _copied():
    return data_node_pb2.ReplicateBlockResponse(checksum="checksum", bytes_written=2)


def _timed_cycle(worker):
    start = time.monotonic()
    worker.run_cycle()
    return time.monotonic() - start


def test_rereplicator_waits_while_the_dead_replica_is_younger_than_the_delay(monkeypatch):
    """Una réplica caída hace 5 s puede ser un reinicio: con delay de 30 s no se toca,
    y el ciclo NO se queda dormido esperando (antes dormía el delay por bloque)."""
    from dfsha.control_node.rereplicator import ReReplicator

    servicer = _Servicer([_Block(datanode_addresses=["dn1", "dn2"])])
    replica = _ReplicateStub([_copied()])
    worker = ReReplicator(
        servicer, _TimedMonitor(["dn1", "dn3"], {"dn2": 5.0}), replication_factor=2, delay_s=30
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    assert _timed_cycle(worker) < 1
    assert replica.calls == []
    assert servicer.commits == []


def test_rereplicator_repairs_once_the_dead_replica_is_older_than_the_delay(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    servicer = _Servicer([_Block(datanode_addresses=["dn1", "dn2"])])
    replica = _ReplicateStub([_copied()])
    worker = ReReplicator(
        servicer, _TimedMonitor(["dn1", "dn3"], {"dn2": 31.0}), replication_factor=2, delay_s=30
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    assert _timed_cycle(worker) < 1
    assert len(replica.calls) == 1
    [(_, method, args)] = servicer.commits
    assert method == "update_block_replicas"
    assert args[2:] == (["dn1", "dn2"], ["dn1", "dn3"])


def test_rereplicator_completes_an_under_replicated_block_without_waiting(monkeypatch):
    """D-P2: un bloque escrito con 2 copias, sin ninguna réplica muerta, se completa
    enseguida: el delay solo protege de reinicios, acá no hubo ninguno."""
    from dfsha.control_node.rereplicator import ReReplicator

    servicer = _Servicer([_Block(datanode_addresses=["dn1", "dn2"])])
    replica = _ReplicateStub([_copied()])
    worker = ReReplicator(
        servicer, _TimedMonitor(["dn1", "dn2", "dn3"], {}), replication_factor=3, delay_s=30
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda _: replica)

    assert _timed_cycle(worker) < 1
    [(_, _, args)] = servicer.commits
    assert args[2:] == (["dn1", "dn2"], ["dn1", "dn2", "dn3"])


@pytest.mark.parametrize("failure", [grpc.StatusCode.DATA_LOSS, grpc.StatusCode.NOT_FOUND])
def test_rereplicator_falls_back_to_the_next_source_when_one_is_corrupted_or_missing(
    monkeypatch, failure
):
    """Antes se insistía siempre con el primer origen: si su copia estaba podrida, el
    bloque no se reparaba nunca aunque hubiera otra réplica sana."""
    from dfsha.control_node.rereplicator import ReReplicator

    servicer = _Servicer([_Block(datanode_addresses=["dn1", "dn2", "dn4"])])
    stubs = {
        "dn1": _ReplicateStub([_RpcFailure(failure)]),
        "dn2": _ReplicateStub([_copied()]),
    }
    worker = ReReplicator(
        servicer,
        _TimedMonitor(["dn1", "dn2", "dn3"], {"dn4": 60.0}),
        replication_factor=3,
        delay_s=30,
        max_attempts=3,
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda address: stubs[address])

    worker.run_cycle()

    assert len(stubs["dn1"].calls) == 1  # un error no transitorio no se reintenta
    assert len(stubs["dn2"].calls) == 1
    [(_, _, args)] = servicer.commits
    assert args[2:] == (["dn1", "dn2", "dn4"], ["dn1", "dn2", "dn3"])


def test_rereplicator_leaves_the_block_for_the_next_cycle_when_every_source_fails(monkeypatch):
    from dfsha.control_node.rereplicator import ReReplicator

    servicer = _Servicer([_Block(datanode_addresses=["dn1", "dn2", "dn4"])])
    stubs = {
        "dn1": _ReplicateStub([_RpcFailure(grpc.StatusCode.DATA_LOSS)]),
        "dn2": _ReplicateStub([_RpcFailure(grpc.StatusCode.NOT_FOUND)]),
    }
    worker = ReReplicator(
        servicer,
        _TimedMonitor(["dn1", "dn2", "dn3"], {"dn4": 60.0}),
        replication_factor=3,
        delay_s=30,
    )
    monkeypatch.setattr(worker, "_datanode_stub", lambda address: stubs[address])

    worker.run_cycle()

    assert servicer.commits == []


def test_replicate_block_reports_a_corrupted_source_as_data_loss(replicated_datanodes, tmp_path):
    """Verificado con un bloque corrupto real: antes salía UNAVAILABLE ("Exception
    iterating requests!"), como si el nodo estuviera caído."""
    source_stub, target_root, target = replicated_datanodes
    stored = tmp_path / "source" / BLOCK_ID
    raw = bytearray(stored.read_bytes())
    raw[0] ^= 0xFF
    stored.write_bytes(bytes(raw))

    with pytest.raises(grpc.RpcError) as exc_info:
        source_stub.ReplicateBlock(
            data_node_pb2.ReplicateBlockRequest(block_id=BLOCK_ID, target=target), timeout=2
        )
    assert exc_info.value.code() == grpc.StatusCode.DATA_LOSS
    assert not (target_root / BLOCK_ID).exists()


def test_replicate_block_reports_a_missing_source_block_as_not_found(replicated_datanodes):
    source_stub, target_root, target = replicated_datanodes
    missing = "fedcba9876543210fedcba9876543210"

    with pytest.raises(grpc.RpcError) as exc_info:
        source_stub.ReplicateBlock(
            data_node_pb2.ReplicateBlockRequest(block_id=missing, target=target), timeout=2
        )
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
