"""B3: `write` de RF3 de punta a punta: cliente, ControlNode, pipeline y DataNodes."""

from __future__ import annotations

import time
import uuid

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY, wait_for
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.client.shell import handle_command
from dfsha.common.exceptions import ConflictError, DFShaError, InvalidPathError
from dfsha.control_node.servicer import ControlNodeServicer
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, control_node_pb2_grpc
from test_raft_cluster import CLUSTER_RAFT_CONF, RaftCluster

CONTENT = bytes(range(23))  # con bloques de 5: 5 + 5 + 5 + 5 + 3


def _expected(offset: int, data: bytes, base: bytes = CONTENT) -> bytes:
    return base[:offset] + data + base[offset + len(data) :]


@pytest.fixture
def cluster(tmp_path, start_control_node):
    """3 DataNodes, factor 3, bloques de 5 bytes y /a.bin de 23 bytes."""
    servers, roots, addresses = [], [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        servers.append(server)
        roots.append(tmp_path / f"dn{i}")
        addresses.append(f"localhost:{port}")
    control = start_control_node(addresses, block_size_bytes=5, replication_factor=3, lock_lease_s=5)
    clients = []

    def new_client():
        client = DistributedDFShaClient([control])
        clients.append(client)
        return client

    client = new_client()
    source = tmp_path / "fuente.bin"
    source.write_bytes(CONTENT)
    client.upload(source, "/a.bin")
    yield {"client": client, "new_client": new_client, "roots": roots, "servers": servers, "control": control}
    for c in clients:
        c.close()
    for server in servers:
        server.stop(grace=None)


def _blocks(client, path="/a.bin"):
    return list(client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path=path)).blocks)


def _stored(roots) -> set[str]:
    return {p.name for root in roots if root.exists() for p in root.iterdir() if len(p.name) == 32}


def _stub(address):
    channel = grpc.insecure_channel(address)
    return channel, control_node_pb2_grpc.ControlNodeServiceStub(channel)


# --- casos de escritura ------------------------------------------------------------------


@pytest.mark.parametrize(
    "offset,data",
    [
        (6, b"XY"),  # dentro de un bloque
        (8, b"ABCD"),  # cruza dos bloques
        (0, bytes(23)),  # reescribe todo con el mismo tamaño
    ],
)
def test_write_overwrites_in_place_only_the_touched_blocks(cluster, offset, data):
    client = cluster["client"]
    before = [b.block_id for b in _blocks(client)]

    assert client.write("/a.bin", offset, data) == len(data)

    assert client.read("/a.bin") == _expected(offset, data)
    after = _blocks(client)
    touched = {i for i in range(len(before)) if offset <= i * 5 + 4 and i * 5 < offset + len(data)}
    assert {i for i in range(len(before)) if after[i].block_id != before[i]} == touched
    assert [b.size_bytes for b in after] == [5, 5, 5, 5, 3]
    assert client.locks() == []


def test_write_extends_the_last_block_and_adds_new_ones(cluster):
    client = cluster["client"]
    client.write("/a.bin", 21, b"0123456789")

    assert client.read("/a.bin") == CONTENT[:21] + b"0123456789"
    # D-P4: todos los bloques menos el último quedan llenos
    assert [b.size_bytes for b in _blocks(client)] == [5, 5, 5, 5, 5, 5, 1]
    assert client.list_dir("/")[0].size_bytes == 31


def test_write_at_the_end_of_a_file_whose_size_is_a_multiple_of_the_block(cluster, tmp_path):
    client = cluster["client"]
    source = tmp_path / "veinte.bin"
    source.write_bytes(bytes(20))
    client.upload(source, "/veinte.bin")

    client.write("/veinte.bin", 20, b"fin")

    assert client.read("/veinte.bin") == bytes(20) + b"fin"
    assert [b.size_bytes for b in _blocks(client, "/veinte.bin")] == [5, 5, 5, 5, 3]


def test_write_with_an_offset_beyond_the_end_is_rejected_and_the_file_is_untouched(cluster):
    client = cluster["client"]
    before = [b.block_id for b in _blocks(client)]
    with pytest.raises(InvalidPathError):
        client.write("/a.bin", 24, b"x")
    assert [b.block_id for b in _blocks(client)] == before
    assert client.locks() == []


def test_the_replaced_blocks_are_deleted_from_every_datanode_after_the_commit(cluster):
    client, roots = cluster["client"], cluster["roots"]
    old = _blocks(client)[1]
    assert old.block_id in _stored(roots)

    client.write("/a.bin", 5, b"nuevo")

    assert old.block_id not in _stored(roots)
    assert _blocks(client)[1].block_id in _stored(roots)


def test_open_returns_a_handle_that_reads_and_writes_under_its_own_lock(cluster):
    client = cluster["client"]
    handle = client.open("/a.bin", "w")
    try:
        handle.write(2, b"hola")
        assert handle.read(0, 8) == _expected(2, b"hola")[:8]
    finally:
        handle.close()
    assert client.locks() == []
    assert client.read("/a.bin") == _expected(2, b"hola")


def test_a_handle_opened_for_reading_cannot_write(cluster):
    client = cluster["client"]
    handle = client.open("/a.bin", "r")
    try:
        with pytest.raises(ConflictError):
            handle.write(0, b"x")
    finally:
        handle.close()


# --- locks, versiones y reservas ----------------------------------------------------------


def test_begin_write_without_the_exclusive_lock_is_rejected(cluster):
    client = cluster["client"]
    channel, stub = _stub(cluster["control"])
    shared = client.lock("/a.bin", "r")
    try:
        for lock_id in ("no-existe", shared.lock_id):
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.BeginWrite(
                    control_node_pb2.BeginWriteRequest(
                        path="/a.bin", offset=0, length=1, lock_id=lock_id, op_id=uuid.uuid4().hex
                    ),
                    timeout=5,
                )
            assert exc_info.value.code() == grpc.StatusCode.ABORTED
    finally:
        shared.release()
        channel.close()


def test_a_reader_blocks_the_writer_until_it_releases_its_lock(cluster):
    client, reader = cluster["client"], cluster["new_client"]()
    held = reader.open("/a.bin", "r")
    old = _blocks(client)[0].block_id
    with pytest.raises(ConflictError):
        client.write("/a.bin", 0, b"Z")
    assert client.locks() == []  # el writer no quedó con un lock tomado

    held.close()
    client.write("/a.bin", 0, b"Z")

    assert client.read("/a.bin")[:1] == b"Z"
    assert old not in _stored(cluster["roots"])


def test_begin_write_retried_with_the_same_op_id_returns_the_same_reservation(cluster):
    client = cluster["client"]
    held = client.lock("/a.bin", "w")
    channel, stub = _stub(cluster["control"])
    try:
        request = control_node_pb2.BeginWriteRequest(
            path="/a.bin", offset=4, length=3, lock_id=held.lock_id, op_id=uuid.uuid4().hex
        )
        first = stub.BeginWrite(request, timeout=5)
        retry = stub.BeginWrite(request, timeout=5)
        assert retry == first
        assert [s.index for s in first.slots] == [0, 1]
    finally:
        channel.close()
        held.release()


def test_commit_write_retried_with_the_same_op_id_commits_once(cluster, monkeypatch):
    client = cluster["client"]
    real_call = client._call
    versions = []

    def call_twice(rpc_name, request):
        response = real_call(rpc_name, request)
        if rpc_name == "CommitWrite":
            versions.append(response.version)
            versions.append(real_call(rpc_name, request).version)  # respuesta perdida, reintento
        return response

    monkeypatch.setattr(client, "_call", call_twice)
    client.write("/a.bin", 1, b"a")
    client.write("/a.bin", 2, b"b")

    assert versions == [1, 1, 2, 2]
    assert client.read("/a.bin") == _expected(1, b"ab")


def test_a_stale_reservation_cannot_be_committed_after_another_write(cluster):
    client = cluster["client"]
    held = client.lock("/a.bin", "w")
    channel, stub = _stub(cluster["control"])
    try:
        begin = lambda offset: stub.BeginWrite(  # noqa: E731
            control_node_pb2.BeginWriteRequest(
                path="/a.bin", offset=offset, length=1, lock_id=held.lock_id, op_id=uuid.uuid4().hex
            ),
            timeout=5,
        )
        stale = begin(20)
        held.write(0, b"!")  # otra escritura publica la versión 1
        confirmed = [
            control_node_pb2.ConfirmedSlot(index=s.index, block_id=s.new_block_id, checksum="x", size_bytes=s.new_size)
            for s in stale.slots
        ]
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.CommitWrite(
                control_node_pb2.CommitWriteRequest(
                    path="/a.bin",
                    write_id=stale.write_id,
                    base_version=stale.base_version,
                    lock_id=held.lock_id,
                    slots=confirmed,
                    op_id=uuid.uuid4().hex,
                ),
                timeout=5,
            )
        assert exc_info.value.code() == grpc.StatusCode.ABORTED
        assert client.read("/a.bin") == _expected(0, b"!")
    finally:
        channel.close()
        held.release()


def test_a_lock_that_expires_before_the_commit_rejects_the_write(tmp_path, start_control_node):
    servers, addresses = [], []
    for i in range(2):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    # 1 s: el lock tiene que seguir vigente en BeginWrite aunque la CPU esté cargada
    control = start_control_node(addresses, block_size_bytes=5, replication_factor=2, lock_lease_s=1.0)
    client = DistributedDFShaClient([control])
    channel, stub = _stub(control)
    try:
        source = tmp_path / "f.bin"
        source.write_bytes(CONTENT)
        client.upload(source, "/a.bin")
        lock_id = stub.Lock(control_node_pb2.LockRequest(path="/a.bin", mode="w", op_id=uuid.uuid4().hex)).lock_id
        begun = stub.BeginWrite(
            control_node_pb2.BeginWriteRequest(path="/a.bin", offset=0, length=1, lock_id=lock_id, op_id=uuid.uuid4().hex)
        )
        time.sleep(1.3)  # sin renovador: el lease de 1 s vence antes del commit
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.CommitWrite(
                control_node_pb2.CommitWriteRequest(
                    path="/a.bin",
                    write_id=begun.write_id,
                    base_version=begun.base_version,
                    lock_id=lock_id,
                    slots=[
                        control_node_pb2.ConfirmedSlot(
                            index=s.index, block_id=s.new_block_id, checksum="x", size_bytes=s.new_size
                        )
                        for s in begun.slots
                    ],
                    op_id=uuid.uuid4().hex,
                )
            )
        assert exc_info.value.code() == grpc.StatusCode.ABORTED
        assert client.read("/a.bin") == CONTENT
    finally:
        channel.close()
        client.close()
        for server in servers:
            server.stop(grace=None)


# --- fallas a mitad de camino -------------------------------------------------------------


def test_a_failure_before_the_commit_aborts_and_deletes_the_new_blocks(cluster, monkeypatch):
    client, roots = cluster["client"], cluster["roots"]
    real_write_block = client._write_block
    written = []

    def fail_on_second_block(block, fh):
        if written:
            raise DFShaError("se cayó el cliente entre dos bloques")
        written.append(block.block_id)
        return real_write_block(block, fh)

    monkeypatch.setattr(client, "_write_block", fail_on_second_block)
    with pytest.raises(DFShaError):
        client.write("/a.bin", 3, b"dos bloques")

    assert client.read("/a.bin") == CONTENT
    assert written and written[0] not in _stored(roots)  # AbortWrite borró el bloque ya escrito
    assert client.locks() == []
    monkeypatch.undo()
    client.write("/a.bin", 3, b"dos bloques")  # la reserva se liberó: se puede reintentar
    assert client.read("/a.bin") == _expected(3, b"dos bloques")


def test_a_replica_dying_during_the_write_leaves_the_file_unchanged(cluster):
    """El DataNode muere antes de que el monitor lo note: el pipeline falla, la
    escritura se aborta y el archivo sigue siendo el de antes."""
    client = cluster["client"]
    cluster["servers"][1].stop(grace=None)
    with pytest.raises(DFShaError):
        client.write("/a.bin", 0, b"nunca")
    assert client.read("/a.bin") == CONTENT


def test_a_crash_after_the_commit_leaves_the_new_version_visible(cluster, monkeypatch):
    """Si el líder cae entre el commit y el borrado, la versión nueva ya es la visible
    y los bloques viejos quedan en disco como huérfanos para el recolector (A3)."""
    client, roots = cluster["client"], cluster["roots"]
    old = _blocks(client)[0].block_id
    monkeypatch.setattr(ControlNodeServicer, "_delete_blocks", lambda self, blocks: None)

    client.write("/a.bin", 0, b"Q")

    assert client.read("/a.bin") == _expected(0, b"Q")
    assert old in _stored(roots)


# --- clúster Raft ---------------------------------------------------------------------------


@pytest.fixture
def raft_cluster(tmp_path):
    servers, addresses = [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    cluster = RaftCluster(tmp_path, addresses, persistent=False)
    for i in range(3):
        cluster.start(i)
    cluster.wait_leader()
    client = cluster.client()
    source = tmp_path / "f.bin"
    source.write_bytes(CONTENT)
    client.upload(source, "/a.bin")
    yield cluster, client
    client.close()
    cluster.close()
    for server in servers:
        server.stop(grace=None)


def test_leader_failover_between_begin_and_commit_publishes_the_write_once(raft_cluster):
    cluster, client = raft_cluster
    real_call = client._call
    killed = []

    def kill_leader_before_commit(rpc_name, request):
        if rpc_name == "CommitWrite" and not killed:
            leader = cluster.wait_leader()
            cluster.kill(leader)
            killed.append(leader)
        return real_call(rpc_name, request)

    client._call = kill_leader_before_commit
    handle = client.open("/a.bin", "w")
    try:
        handle.write(7, b"FAILOVER")
    finally:
        handle.close()

    assert killed
    assert client.read("/a.bin") == _expected(7, b"FAILOVER")


def test_begin_write_on_a_follower_is_unavailable(raft_cluster):
    cluster, client = raft_cluster
    held = client.lock("/a.bin", "w")
    follower = next(i for i in range(3) if i != cluster.wait_leader())
    channel, stub = cluster.stub(follower)
    try:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.BeginWrite(
                control_node_pb2.BeginWriteRequest(
                    path="/a.bin", offset=0, length=1, lock_id=held.lock_id, op_id=uuid.uuid4().hex
                ),
                timeout=5,
            )
        assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        channel.close()
        held.release()


def test_begin_write_on_a_leader_without_majority_is_unavailable(tmp_path):
    """BeginWrite lee el árbol para armar la propuesta: un líder aislado no puede
    confirmar la barrera y no debe reservar sobre datos viejos."""
    servers, addresses = [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    cluster = RaftCluster(
        tmp_path, addresses, persistent=False, raft_conf={**CLUSTER_RAFT_CONF, "leaderFallbackTimeout": 30.0}
    )
    for i in range(3):
        cluster.start(i)
    client = None
    try:
        leader = cluster.wait_leader()
        client = cluster.client()
        source = tmp_path / "f.bin"
        source.write_bytes(CONTENT)
        client.upload(source, "/a.bin")
        lock_id = client.lock("/a.bin", "w").lock_id
        client._stop_lock_renewer()
        for i in range(3):
            if i != leader:
                cluster.kill(i)
        assert wait_for(lambda: cluster.nodes[leader][1]._isLeader())
        channel, stub = cluster.stub(leader)
        try:
            with pytest.raises(grpc.RpcError) as exc_info:
                stub.BeginWrite(
                    control_node_pb2.BeginWriteRequest(
                        path="/a.bin", offset=0, length=1, lock_id=lock_id, op_id=uuid.uuid4().hex
                    ),
                    timeout=5,
                )
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
        finally:
            channel.close()
    finally:
        if client is not None:
            client._held_locks.clear()
            client.close()
        cluster.close()
        for server in servers:
            server.stop(grace=None)


# --- shell ---------------------------------------------------------------------------------


class _WriteClient:
    def __init__(self):
        self.writes = []
        self.opened = []
        self.closed = []

    def write(self, path, offset, data):
        self.writes.append((path, offset, data))
        return len(data)

    def open(self, path, mode):
        self.opened.append((path, mode))
        return type("Handle", (), {"lock_id": "L1", "path": path, "mode": mode})()

    def unlock(self, path):
        self.closed.append(path)


def test_shell_write_sends_a_local_file_at_an_offset(tmp_path):
    client = _WriteClient()
    local = tmp_path / "parche.bin"
    local.write_bytes(b"parche")
    _, output = handle_command(client, "/docs", f'write a.bin 10 "{local}"')
    assert client.writes == [("/docs/a.bin", 10, b"parche")]
    assert "6 bytes" in output


def test_shell_open_and_close_map_to_the_handle_lock():
    client = _WriteClient()
    handle_command(client, "/docs", "open a.bin w")
    handle_command(client, "/docs", "close a.bin")
    assert client.opened == [("/docs/a.bin", "w")]
    assert client.closed == ["/docs/a.bin"]


@pytest.mark.parametrize("line", ["write", "write a.bin x f", "write a.bin 3", "open a.bin", "open a.bin x", "close"])
def test_shell_write_open_close_report_usage_errors(line):
    _, output = handle_command(_WriteClient(), "/", line)
    assert "uso" in output


@pytest.mark.parametrize("line", ["write a 0 f", "open a w", "close a"])
def test_shell_write_open_close_are_rejected_on_a_client_without_rf3(line):
    _, output = handle_command(object(), "/", line)
    assert "no disponible" in output
