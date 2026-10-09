"""Re-replicador y recolector con capabilities (Hito 3, C3, T9). ControlNode y tres
DataNodes reales, todos con la clave."""

import os
import time
import uuid

from conftest import TEST_ENCRYPTION_KEY, wait_for
from dfsha.common.block_token import capability_kwargs
from dfsha.control_node.garbage_collector import GarbageCollector
from dfsha.control_node.rereplicator import ReReplicator
from dfsha.data_node import block_store
from dfsha.generated import control_node_pb2 as cn
from dfsha.generated import data_node_pb2_grpc
from test_capability_control import KEY, _list_blocks, _read, _upload, _write, start  # noqa: F401 (fixture)

ORPHAN = "0f" * 16
ONE_BLOCK = b"cinco"


def _age(root, block_id, seconds):
    past = time.time() - seconds
    os.utime(root / block_id, (past, past))


def _dead_longer_than(monitor, address, seconds) -> bool:
    # dead_for cuenta desde el primer Ping fallido; el nodo deja de figurar vivo recién
    # al pasar dead_after_s. El re-replicador necesita las dos cosas.
    dead_for = monitor.dead_for(address)
    return not monitor.is_alive(address) and dead_for is not None and dead_for > seconds


class _CountingServicer:
    """El servicer real, contando los commits del re-replicador."""

    def __init__(self, real):
        self._real = real
        self.commits = []

    def __getattr__(self, name):
        return getattr(self._real, name)

    def _commit_raw(self, *args):
        self.commits.append(args)
        return self._real._commit_raw(*args)


def _counting_stub_factory(calls):
    def factory(channel):
        real = data_node_pb2_grpc.DataNodeServiceStub(channel)

        class _Stub:
            def ReplicateBlock(self, request, **kwargs):
                calls.append(("ReplicateBlock", request.block_id))
                return real.ReplicateBlock(request, **kwargs)

            def ListStoredBlocks(self, request, **kwargs):
                calls.append(("ListStoredBlocks", None))
                return real.ListStoredBlocks(request, **kwargs)

            def DeleteBlock(self, request, **kwargs):
                calls.append(("DeleteBlock", request.block_id))
                return real.DeleteBlock(request, **kwargs)

        return _Stub()

    return factory


def _start_two_replicas(start, **kwargs):
    kwargs.setdefault("heartbeat_interval_s", 0.05)
    kwargs.setdefault("datanode_dead_after_s", 0.2)
    return start(replication_factor=2, min_write_replicas=2, **kwargs)


# --- con la clave, las tareas internas funcionan ------------------------------------------------


def test_the_rereplicator_repairs_a_replica_on_datanodes_with_key(start):
    system = _start_two_replicas(start, rereplication_interval_s=0.05, rereplication_delay_s=0.3)
    (block,) = _upload(system, "/a.bin", ONE_BLOCK)
    holders = list(block.datanode_addresses)
    (free,) = [address for address in system.datanodes if address not in holders]

    system.stop_datanode(holders[1])

    assert wait_for(lambda: _list_blocks(system, "/a.bin")[0].datanode_addresses == [holders[0], free], timeout=10)
    assert (system.root(free) / block.block_id).exists()
    repaired = _list_blocks(system, "/a.bin")[0]
    assert _read(system.dn(free), repaired.block_id, **capability_kwargs(repaired.capability)) == ONE_BLOCK


def test_the_collector_lists_the_three_datanodes_and_deletes_an_old_orphan(start):
    system = start(gc_grace_s=100)
    _upload(system, "/a.bin")
    target = next(iter(system.datanodes))
    block_store.write_block(system.root(target), TEST_ENCRYPTION_KEY, ORPHAN, [b"huerfano"])
    _age(system.root(target), ORPHAN, 500)
    calls = []
    collector = GarbageCollector(
        system.server._dfsha_control_servicer,
        system.server._dfsha_datanode_monitor,
        interval_s=3600,
        grace_s=100,
        stub_factory=_counting_stub_factory(calls),
        capability_key=KEY,
    )

    collector.run_cycle()

    assert [name for name, _ in calls].count("ListStoredBlocks") == 3
    assert collector.deleted == [(target, ORPHAN)]
    assert not (system.root(target) / ORPHAN).exists()
    collector.stop()


def test_a_write_capability_outside_the_pipeline_leaves_a_copy_that_only_the_collector_removes(start):
    # D2: quien tiene la capability de escritura puede escribir ese bloque en un DataNode
    # fuera del pipeline. La copia no entra a la metadata y la borra el recolector.
    system = _start_two_replicas(start, gc_grace_s=100)
    begun = system.stub.BeginUpload(cn.BeginUploadRequest(path="/a.bin", size_bytes=len(ONE_BLOCK), op_id=uuid.uuid4().hex))
    (block,) = begun.blocks
    (outsider,) = [address for address in system.datanodes if address not in block.datanode_addresses]

    stray = _write(system.dn(outsider), block.block_id, ONE_BLOCK, **capability_kwargs(block.capability))
    assert stray.bytes_written == len(ONE_BLOCK)
    response = _write(
        system.dn(block.datanode_addresses[0]),
        block.block_id,
        ONE_BLOCK,
        downstream=block.datanode_addresses[1:],
        **capability_kwargs(block.capability),
    )
    system.stub.ConfirmBlock(
        cn.ConfirmBlockRequest(
            path="/a.bin", block_id=block.block_id, checksum=response.checksum,
            size_bytes=response.bytes_written, op_id=uuid.uuid4().hex,
        )
    )
    system.stub.CompleteUpload(cn.CompleteUploadRequest(path="/a.bin", op_id=uuid.uuid4().hex))

    (listed,) = _list_blocks(system, "/a.bin")
    assert outsider not in listed.datanode_addresses
    collector = system.server._dfsha_garbage_collector
    collector.run_cycle()
    assert collector.deleted == []
    assert (system.root(outsider) / block.block_id).exists()
    _age(system.root(outsider), block.block_id, 500)
    collector.run_cycle()
    assert collector.deleted == [(outsider, block.block_id)]
    assert not (system.root(outsider) / block.block_id).exists()
    for address in listed.datanode_addresses:
        assert _read(system.dn(address), listed.block_id, **capability_kwargs(listed.capability)) == ONE_BLOCK


# --- sin la clave, contra DataNodes que la exigen ------------------------------------------------


def test_a_rereplicator_without_key_repairs_nothing_and_does_not_retry_the_denial(start):
    # El hilo del ControlNode corre un ciclo al arrancar y después espera una hora.
    system = _start_two_replicas(start, rereplication_interval_s=3600, rereplication_delay_s=0.2)
    (block,) = _upload(system, "/a.bin", ONE_BLOCK)
    holders = list(block.datanode_addresses)
    (free,) = [address for address in system.datanodes if address not in holders]
    monitor = system.server._dfsha_datanode_monitor
    servicer = _CountingServicer(system.server._dfsha_control_servicer)
    calls = []
    without_key = ReReplicator(
        servicer, monitor, replication_factor=2, interval_s=3600, delay_s=0.2,
        stub_factory=_counting_stub_factory(calls), sleep=lambda _: None,
    )
    system.stop_datanode(holders[1])
    assert wait_for(lambda: _dead_longer_than(monitor, holders[1], 0.2), timeout=10)

    without_key.run_cycle()

    assert calls == [("ReplicateBlock", block.block_id)]  # un solo intento: PERMISSION_DENIED no se reintenta
    assert servicer.commits == []
    assert _list_blocks(system, "/a.bin")[0].datanode_addresses == holders
    assert not (system.root(free) / block.block_id).exists()
    without_key.stop()
    # todo lo demás era reparable: el re-replicador del ControlNode, con la clave, sí repara
    system.server._dfsha_rereplicator.run_cycle()
    assert _list_blocks(system, "/a.bin")[0].datanode_addresses == [holders[0], free]


def test_a_collector_without_key_neither_lists_nor_deletes_and_its_cycle_ends(start):
    system = start(gc_grace_s=100)
    target = next(iter(system.datanodes))
    block_store.write_block(system.root(target), TEST_ENCRYPTION_KEY, ORPHAN, [b"huerfano"])
    _age(system.root(target), ORPHAN, 500)
    calls = []
    without_key = GarbageCollector(
        system.server._dfsha_control_servicer,
        system.server._dfsha_datanode_monitor,
        interval_s=3600,
        grace_s=100,
        stub_factory=_counting_stub_factory(calls),
    )

    without_key.run_cycle()  # no lanza

    assert [name for name, _ in calls] == ["ListStoredBlocks"] * 3
    assert without_key.deleted == []
    assert (system.root(target) / ORPHAN).exists()
    without_key.stop()
    system.server._dfsha_garbage_collector.run_cycle()
    assert not (system.root(target) / ORPHAN).exists()


def test_the_collector_signs_with_the_real_time_and_not_with_its_clock(start):
    # Un reloj muy atrasado: si las capabilities se firmaran con él, el DataNode las vería
    # vencidas y el ciclo no listaría ni borraría nada.
    system = start(gc_grace_s=100)
    target = next(iter(system.datanodes))
    block_store.write_block(system.root(target), TEST_ENCRYPTION_KEY, ORPHAN, [b"huerfano"])
    _age(system.root(target), ORPHAN, 500)
    collector = GarbageCollector(
        system.server._dfsha_control_servicer,
        system.server._dfsha_datanode_monitor,
        interval_s=3600,
        grace_s=100,
        clock=lambda: time.time() - 10_000_000,
        capability_key=KEY,
    )

    collector.run_cycle()

    assert collector.deleted == [(target, ORPHAN)]
    collector.stop()


def test_the_internal_calls_carry_no_metadata_without_key():
    # Con capability_key=None, el re-replicador y el recolector llaman a los stubs como
    # antes de C3: los stubs falsos de los tests de A2 y A3 no aceptan `metadata`.
    collector = GarbageCollector(object(), object(), interval_s=3600)
    rereplicator = ReReplicator(object(), object(), replication_factor=2, interval_s=3600)

    assert collector._capability_kwargs(lambda key, now: "no se llama") == {}
    assert rereplicator._replicate_capabilities(ORPHAN) == {}
