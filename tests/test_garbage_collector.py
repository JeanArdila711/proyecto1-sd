"""A3: recolector de bloques huérfanos y de réplicas sobrantes."""

from __future__ import annotations

import os
import time
import uuid
from types import SimpleNamespace

import grpc
import pytest

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.control_node.main import serve as serve_control_node
from dfsha.control_node.tree import ControlTree
from dfsha.data_node import block_store
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, data_node_pb2, data_node_pb2_grpc

NOW = 1000.0


def _hex(n: int) -> str:
    return f"{n:032x}"


# --- ControlTree: qué bloques están en uso -------------------------------------------------


def _committed(tree: ControlTree, path: str, block_id: str, addresses: list[str]) -> None:
    tree.begin_upload(path, [(block_id, addresses)], NOW, 60.0, 5)
    tree.confirm_block(path, block_id, "sum", 5)
    tree.complete_upload(path)


def test_referenced_blocks_covers_committed_pending_and_live_reservations():
    tree = ControlTree()
    _committed(tree, "/a.bin", _hex(1), ["dn1", "dn2"])
    tree.begin_upload("/pendiente.bin", [(_hex(2), ["dn2"])], NOW, 60.0, 5)  # lease vence en NOW+60
    tree.acquire_lock("/a.bin", "L1", "cliente", "w", NOW, 600.0)
    tree.begin_write("/a.bin", "W1", "L1", 0, 0, 1, [(0, _hex(3), ["dn3"])], NOW, 30.0, 5)

    referenced = tree.referenced_blocks(NOW + 10)
    assert referenced == {_hex(1): {"dn1", "dn2"}, _hex(2): {"dn2"}, _hex(3): {"dn3"}}

    later = tree.referenced_blocks(NOW + 100)
    # la reserva venció: sus bloques quedan libres para el recolector
    assert _hex(3) not in later
    # una subida pendiente con el lease vencido SIGUE en uso: complete_upload la acepta
    # igual si nadie reemplazó el nombre, y borrarle los bloques perdería datos
    assert later[_hex(2)] == {"dn2"}


def test_referenced_blocks_returns_copies():
    tree = ControlTree()
    _committed(tree, "/a.bin", _hex(1), ["dn1"])
    tree.referenced_blocks(NOW)[_hex(1)].add("dn9")
    assert tree.referenced_blocks(NOW)[_hex(1)] == {"dn1"}


def test_pending_blocks_lists_the_upload_without_changing_what_abort_upload_returns():
    """abort_upload sigue devolviendo None: su resultado queda en applied_ops, y un
    retorno distinto haría que el replay de un journal viejo diera otro estado."""
    tree = ControlTree()
    tree.begin_upload("/a.bin", [(_hex(1), ["dn1", "dn2"]), (_hex(2), ["dn2"])], NOW, 60.0, 5)
    blocks = tree.pending_blocks("/a.bin")
    assert [(b.block_id, b.datanode_addresses) for b in blocks] == [(_hex(1), ["dn1", "dn2"]), (_hex(2), ["dn2"])]
    blocks[0].datanode_addresses.append("dn9")  # es una copia
    assert tree.abort_upload("/a.bin") is None
    assert tree.pending_blocks("/a.bin") == []
    assert tree.pending_blocks("/no-existe") == []


# --- DataNode: listar lo que tiene en disco ---------------------------------------------------


def test_block_store_lists_only_blocks_with_their_size_and_age(tmp_path):
    old, young = _hex(1), _hex(2)
    block_store.write_block(tmp_path, old, [b"viejo"])
    block_store.write_block(tmp_path, young, [b"nuevo!!"])
    (tmp_path / f"{_hex(3)}.part-{uuid.uuid4().hex}").write_bytes(b"temporal")  # subida a medias
    past = time.time() - 500
    os.utime(tmp_path / old, (past, past))

    listed = {block_id: (size, age) for block_id, size, age in block_store.list_blocks(tmp_path)}

    assert set(listed) == {old, young}
    assert listed[old][0] == 5 and listed[young][0] == 7
    assert 499 <= listed[old][1] <= 510
    assert listed[young][1] < 5


def test_block_store_lists_nothing_for_a_missing_root(tmp_path):
    assert list(block_store.list_blocks(tmp_path / "no-existe")) == []


def test_list_stored_blocks_rpc_streams_every_block(tmp_path):
    server, port = serve_data_node(tmp_path / "dn", "localhost", 0)
    channel = grpc.insecure_channel(f"localhost:{port}")
    try:
        for n in range(3):
            block_store.write_block(tmp_path / "dn", _hex(n), [bytes(n + 1)])
        stub = data_node_pb2_grpc.DataNodeServiceStub(channel)
        stored = list(stub.ListStoredBlocks(data_node_pb2.ListStoredBlocksRequest(), timeout=5))
        assert sorted((b.block_id, b.size_bytes) for b in stored) == [(_hex(0), 1), (_hex(1), 2), (_hex(2), 3)]
        assert all(b.age_s >= 0 for b in stored)
    finally:
        channel.close()
        server.stop(grace=None)


# --- GarbageCollector con dobles -------------------------------------------------------------


class _Failure(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def details(self):
        return self._code.name


class _FakeDataNode:
    def __init__(self, blocks: dict[str, float], events: list, name: str, list_error=None):
        self.blocks = dict(blocks)  # block_id -> edad
        self.events = events
        self.name = name
        self.list_error = list_error
        self.deleted = []

    def ListStoredBlocks(self, request, timeout=None):
        self.events.append(("list", self.name))
        if self.list_error:
            raise self.list_error
        return [SimpleNamespace(block_id=b, size_bytes=1, age_s=age) for b, age in self.blocks.items()]

    def DeleteBlock(self, request, timeout=None):
        self.events.append(("delete", self.name, request.block_id))
        if request.block_id not in self.blocks:
            raise _Failure(grpc.StatusCode.NOT_FOUND)
        del self.blocks[request.block_id]
        self.deleted.append(request.block_id)


class _FakeServicer:
    def __init__(self, referenced: dict[str, set[str]], events: list, leader=True):
        self.referenced = referenced
        self.events = events
        self.leader = leader
        self._replicated = SimpleNamespace(tree=SimpleNamespace(referenced_blocks=self._snapshot))

    def _snapshot(self, now):
        self.events.append(("snapshot",))
        return {block_id: set(addresses) for block_id, addresses in self.referenced.items()}

    def _is_leader_raw(self):
        return self.leader

    def _read_barrier_raw(self):
        self.events.append(("barrier",))
        return self.leader


def _collector(servicer, datanodes: dict[str, _FakeDataNode], grace_s=100.0, in_flight=frozenset()):
    from dfsha.control_node.garbage_collector import GarbageCollector

    monitor = SimpleNamespace(alive_addresses=lambda: list(datanodes))
    rereplicator = SimpleNamespace(in_flight_copies=lambda: set(in_flight))
    collector = GarbageCollector(servicer, monitor, grace_s=grace_s, rereplicator=rereplicator)
    collector._datanode_stub = lambda address: datanodes[address]
    return collector


def test_collector_deletes_old_orphans_and_keeps_young_ones():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0, _hex(2): 5.0}, events, "dn1")
    _collector(_FakeServicer({}, events), {"dn1": dn}).run_cycle()
    assert dn.deleted == [_hex(1)]
    assert set(dn.blocks) == {_hex(2)}


def test_collector_never_deletes_blocks_in_use_or_being_repaired():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0, _hex(2): 500.0}, events, "dn1")
    servicer = _FakeServicer({_hex(1): {"dn1", "dn2"}}, events)
    _collector(servicer, {"dn1": dn}, in_flight={(_hex(2), "dn1")}).run_cycle()
    assert dn.deleted == []


def test_collector_deletes_a_replica_the_metadata_no_longer_assigns_to_that_node():
    """Un nodo que vuelve después de que re-replicaron su contenido: la copia vieja
    sobra aunque el bloque siga en uso en otros nodos."""
    events = []
    dn3 = _FakeDataNode({_hex(1): 500.0}, events, "dn3")
    _collector(_FakeServicer({_hex(1): {"dn1", "dn2"}}, events), {"dn3": dn3}).run_cycle()
    assert dn3.deleted == [_hex(1)]


def test_collector_lists_every_datanode_before_reading_the_metadata():
    """El orden importa: un bloque reservado entre las dos lecturas ya figura en la foto."""
    events = []
    datanodes = {name: _FakeDataNode({}, events, name) for name in ("dn1", "dn2")}
    _collector(_FakeServicer({}, events), datanodes).run_cycle()
    assert [e[0] for e in events] == ["list", "list", "barrier", "snapshot"]


def test_a_block_reserved_between_listing_and_metadata_is_not_deleted():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0}, events, "dn1")
    servicer = _FakeServicer({}, events)
    real_list = dn.ListStoredBlocks

    def list_then_reserve(request, timeout=None):
        listed = real_list(request, timeout)
        servicer.referenced[_hex(1)] = {"dn1"}  # alguien lo reservó justo después de listar
        return listed

    dn.ListStoredBlocks = list_then_reserve
    _collector(servicer, {"dn1": dn}).run_cycle()
    assert dn.deleted == []


def test_a_datanode_that_fails_to_list_is_skipped_without_stopping_the_cycle():
    events = []
    down = _FakeDataNode({_hex(1): 500.0}, events, "dn1", list_error=_Failure(grpc.StatusCode.UNAVAILABLE))
    up = _FakeDataNode({_hex(2): 500.0}, events, "dn2")
    _collector(_FakeServicer({}, events), {"dn1": down, "dn2": up}).run_cycle()
    assert up.deleted == [_hex(2)] and down.deleted == []


def test_a_second_sweep_is_idempotent_and_not_found_is_tolerated():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0}, events, "dn1")
    collector = _collector(_FakeServicer({}, events), {"dn1": dn})
    collector.run_cycle()
    dn.blocks[_hex(1)] = 500.0  # el listado viejo todavía lo muestra, pero ya no existe
    real_delete = dn.DeleteBlock

    def already_gone(request, timeout=None):
        dn.blocks.pop(request.block_id, None)
        return real_delete(request, timeout)

    dn.DeleteBlock = already_gone
    collector.run_cycle()
    assert collector.deleted == []
    collector.run_cycle()
    assert collector.deleted == []


def test_a_follower_or_a_deposed_leader_does_nothing():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0}, events, "dn1")
    servicer = _FakeServicer({}, events, leader=False)
    _collector(servicer, {"dn1": dn}).run_cycle()
    assert dn.deleted == [] and events == []


def test_leadership_lost_mid_cycle_stops_deleting_and_the_new_leader_starts_fresh():
    events = []
    dn = _FakeDataNode({_hex(1): 500.0, _hex(2): 500.0}, events, "dn1")
    old_leader = _FakeServicer({}, events)
    real_delete = dn.DeleteBlock

    def delete_then_lose_leadership(request, timeout=None):
        real_delete(request, timeout)
        old_leader.leader = False

    dn.DeleteBlock = delete_then_lose_leadership
    _collector(old_leader, {"dn1": dn}).run_cycle()
    assert len(dn.deleted) == 1

    dn.DeleteBlock = real_delete
    _collector(_FakeServicer({}, events), {"dn1": dn}).run_cycle()  # otro proceso, sin estado heredado
    assert not dn.blocks


def test_collector_stops_its_thread_and_closes_its_channels():
    from dfsha.control_node.garbage_collector import GarbageCollector

    closed = []
    channel = SimpleNamespace(close=lambda: closed.append(True))
    collector = GarbageCollector(
        _FakeServicer({}, [], leader=False),
        SimpleNamespace(alive_addresses=lambda: ["dn1"]),
        interval_s=3600,
        channel_factory=lambda address: channel,
        stub_factory=lambda ch: _FakeDataNode({}, [], "dn1"),
    )
    collector.start()
    collector._datanode_stub("dn1")
    collector.stop()
    assert not collector.thread.is_alive()
    assert closed == [True]


# --- Integración: ControlNode real + DataNodes reales ------------------------------------------


@pytest.fixture
def system(tmp_path):
    datanodes = []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0)
        datanodes.append({"server": server, "address": f"localhost:{port}", "root": tmp_path / f"dn{i}"})
    started = []

    def start(**kwargs):
        kwargs.setdefault("gc_interval_s", 3600)  # el hilo no molesta: los ciclos se disparan a mano
        kwargs.setdefault("block_size_bytes", 5)
        server, port, raft = serve_control_node(
            [dn["address"] for dn in datanodes],
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            raft_conf_overrides=FAST_RAFT_CONF,
            **kwargs,
        )
        assert wait_for(raft._isLeader)
        client = DistributedDFShaClient([f"localhost:{port}"])
        started.append((server, raft, client))
        return server, client

    yield datanodes, start
    for server, raft, client in started:
        client.close()
        server.stop(grace=None)
        raft.destroy()
    for dn in datanodes:
        dn["server"].stop(grace=None)


def _stored(datanodes) -> set[str]:
    return {p.name for dn in datanodes if dn["root"].exists() for p in dn["root"].iterdir() if len(p.name) == 32}


def _age(root, block_id, seconds):
    past = time.time() - seconds
    os.utime(root / block_id, (past, past))


def _upload(client, tmp_path, path, data):
    source = tmp_path / f"f-{uuid.uuid4().hex}"
    source.write_bytes(data)
    client.upload(source, path)


def test_old_orphans_are_collected_and_files_stay_readable(system, tmp_path):
    datanodes, start = system
    server, client = start(gc_grace_s=100)
    _upload(client, tmp_path, "/a.bin", bytes(range(12)))
    in_use = _stored(datanodes)
    old_orphan, young_orphan = _hex(7001), _hex(7002)
    block_store.write_block(datanodes[0]["root"], old_orphan, [b"huerfano"])
    block_store.write_block(datanodes[1]["root"], young_orphan, [b"reciente"])
    _age(datanodes[0]["root"], old_orphan, 500)

    server._dfsha_garbage_collector.run_cycle()

    assert _stored(datanodes) == in_use | {young_orphan}
    assert client.read("/a.bin") == bytes(range(12))


def test_abort_upload_deletes_the_written_blocks_immediately(system, tmp_path):
    datanodes, start = system
    _, client = start()
    begun = client._call(
        "BeginUpload", control_node_pb2.BeginUploadRequest(path="/x.bin", size_bytes=5, op_id=uuid.uuid4().hex)
    )
    [block] = begun.blocks
    client._write_block(block, __import__("io").BytesIO(b"12345"))
    assert block.block_id in _stored(datanodes)

    client._call("AbortUpload", control_node_pb2.AbortUploadRequest(path="/x.bin", op_id=uuid.uuid4().hex))

    assert block.block_id not in _stored(datanodes)


def test_abort_upload_of_a_path_without_pending_upload_deletes_nothing(system):
    datanodes, start = system
    _, client = start()
    before = _stored(datanodes)
    with pytest.raises(Exception):
        client._call("AbortUpload", control_node_pb2.AbortUploadRequest(path="/nada.bin", op_id=uuid.uuid4().hex))
    assert _stored(datanodes) == before


def test_a_long_reader_keeps_its_blocks_even_with_zero_grace(system, tmp_path):
    datanodes, start = system
    server, client = start(gc_grace_s=0)
    _upload(client, tmp_path, "/a.bin", bytes(range(12)))
    before = _stored(datanodes)
    handle = client.open("/a.bin", "r")
    try:
        server._dfsha_garbage_collector.run_cycle()
        assert _stored(datanodes) == before
        assert handle.read() == bytes(range(12))
    finally:
        handle.close()


def test_blocks_left_by_a_crash_after_a_cow_commit_are_collected(system, tmp_path, monkeypatch):
    from dfsha.control_node.servicer import ControlNodeServicer

    datanodes, start = system
    server, client = start(gc_grace_s=0)
    _upload(client, tmp_path, "/a.bin", bytes(range(12)))
    old = {b.block_id for b in client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path="/a.bin")).blocks}
    with monkeypatch.context() as patch:
        patch.setattr(ControlNodeServicer, "_delete_blocks", lambda self, blocks: None)  # el líder cae antes de borrar
        client.write("/a.bin", 0, b"reescrito!!!")
    assert old <= _stored(datanodes)

    server._dfsha_garbage_collector.run_cycle()

    assert not (old & _stored(datanodes))
    assert client.read("/a.bin") == b"reescrito!!!"


def test_an_abandoned_cow_reservation_is_kept_while_live_and_collected_after_it_expires(system, tmp_path):
    datanodes, start = system
    server, client = start(gc_grace_s=0, upload_lease_s=1.0)
    _upload(client, tmp_path, "/a.bin", bytes(range(12)))
    held = client.lock("/a.bin", "w")
    begun = client._call(
        "BeginWrite",
        control_node_pb2.BeginWriteRequest(path="/a.bin", offset=0, length=3, lock_id=held.lock_id, op_id=uuid.uuid4().hex),
    )
    [slot] = begun.slots
    new_block = SimpleNamespace(block_id=slot.new_block_id, datanode_addresses=list(slot.new_addresses), size_bytes=5)
    client._write_block(new_block, __import__("io").BytesIO(b"abcde"))  # el cliente muere antes del commit

    server._dfsha_garbage_collector.run_cycle()
    assert slot.new_block_id in _stored(datanodes)  # reserva vigente: en uso

    time.sleep(1.2)
    server._dfsha_garbage_collector.run_cycle()
    assert slot.new_block_id not in _stored(datanodes)
    held.release()
    assert client.read("/a.bin") == bytes(range(12))


def test_rereplicator_reports_its_copy_as_in_flight_until_the_commit():
    """El recolector no puede borrar la copia recién hecha mientras su commit no entró."""
    from dfsha.control_node.rereplicator import ReReplicator

    block_id = _hex(1)
    seen = []
    block = SimpleNamespace(block_id=block_id, datanode_addresses=["dn1", "dn2"], checksum="c", size_bytes=5)
    servicer = SimpleNamespace(
        _is_leader_raw=lambda: True,
        _read_barrier_raw=lambda: True,
        _replicated=SimpleNamespace(tree=SimpleNamespace(iter_blocks=lambda: [("/a.bin", block)])),
        _commit_raw=lambda *args: seen.append(worker.in_flight_copies()) or ("ok", None),
    )
    monitor = SimpleNamespace(alive_addresses=lambda: ["dn1", "dn2", "dn3"], dead_for=lambda a: None)
    worker = ReReplicator(servicer, monitor, replication_factor=3, delay_s=0)
    stub = SimpleNamespace(ReplicateBlock=lambda request, timeout: SimpleNamespace(checksum="c", bytes_written=5))
    worker._datanode_stub = lambda address: stub

    worker.run_cycle()

    assert seen == [{(block_id, "dn3")}]
    assert worker.in_flight_copies() == set()
