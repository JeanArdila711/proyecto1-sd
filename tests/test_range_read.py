"""B2: lectura por rangos (`read` de RF3), del disco del DataNode hasta la shell."""

from __future__ import annotations

import threading
import uuid

import grpc
import pytest

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.client.shell import handle_command
from dfsha.common.exceptions import BlockCorruptedError, ConflictError, DFShaError, InvalidPathError
from dfsha.data_node import block_store
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, data_node_pb2, data_node_pb2_grpc

CONTENT = bytes(range(23))  # con bloques de 5: 5 + 5 + 5 + 5 + 3


# --- block_store ---------------------------------------------------------------


def _stored_block(tmp_path, data=b"0123456789"):
    block_id = uuid.uuid4().hex
    block_store.write_block(tmp_path, block_id, [data])
    return block_id


def test_block_store_reads_a_range_inside_the_block(tmp_path):
    block_id = _stored_block(tmp_path)
    assert b"".join(block_store.read_block(tmp_path, block_id, 2, offset=2, length=3)) == b"234"


def test_block_store_range_without_length_reads_until_the_end(tmp_path):
    block_id = _stored_block(tmp_path)
    assert b"".join(block_store.read_block(tmp_path, block_id, 4, offset=7)) == b"789"


def test_block_store_range_at_the_end_is_empty(tmp_path):
    block_id = _stored_block(tmp_path)
    assert b"".join(block_store.read_block(tmp_path, block_id, 4, offset=10, length=5)) == b""


def test_block_store_default_signature_still_reads_the_whole_block(tmp_path):
    block_id = _stored_block(tmp_path)
    assert b"".join(block_store.read_block(tmp_path, block_id, 3)) == b"0123456789"


def test_block_store_range_still_verifies_the_whole_block(tmp_path):
    """Un byte corrupto FUERA del rango pedido igual se detecta: la verificación es
    por bloque completo, así nunca se sirve un rango de un bloque podrido."""
    block_id = _stored_block(tmp_path)
    path = tmp_path / block_id
    raw = bytearray(path.read_bytes())
    raw[9] ^= 0xFF
    path.write_bytes(bytes(raw))
    with pytest.raises(BlockCorruptedError):
        b"".join(block_store.read_block(tmp_path, block_id, 4, offset=0, length=2))


@pytest.mark.parametrize("offset,length", [(-1, None), (0, -1)])
def test_block_store_rejects_negative_ranges(tmp_path, offset, length):
    block_id = _stored_block(tmp_path)
    with pytest.raises(ValueError):
        b"".join(block_store.read_block(tmp_path, block_id, 4, offset=offset, length=length))


# --- RPC del DataNode ------------------------------------------------------------


@pytest.fixture
def datanode(tmp_path):
    server, port = serve_data_node(tmp_path / "dn", "localhost", 0)
    channel = grpc.insecure_channel(f"localhost:{port}")
    yield data_node_pb2_grpc.DataNodeServiceStub(channel), tmp_path / "dn"
    channel.close()
    server.stop(grace=None)


def test_readblock_rpc_serves_only_the_requested_range(datanode):
    stub, root = datanode
    block_id = _stored_block(root)
    request = data_node_pb2.ReadBlockRequest(block_id=block_id, offset=3, length=4)
    assert b"".join(c.data for c in stub.ReadBlock(request, timeout=5)) == b"3456"


def test_readblock_rpc_length_zero_means_until_the_end(datanode):
    stub, root = datanode
    block_id = _stored_block(root)
    request = data_node_pb2.ReadBlockRequest(block_id=block_id, offset=8)
    assert b"".join(c.data for c in stub.ReadBlock(request, timeout=5)) == b"89"


@pytest.mark.parametrize("offset,length", [(-1, 0), (0, -2)])
def test_readblock_rpc_rejects_negative_ranges_with_invalid_argument(datanode, offset, length):
    stub, root = datanode
    block_id = _stored_block(root)
    request = data_node_pb2.ReadBlockRequest(block_id=block_id, offset=offset, length=length)
    with pytest.raises(grpc.RpcError) as exc_info:
        b"".join(c.data for c in stub.ReadBlock(request, timeout=5))
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


# --- Cliente ----------------------------------------------------------------------


@pytest.fixture
def cluster(tmp_path, start_control_node):
    """3 DataNodes, factor 3, bloques de 5 bytes, y un archivo de 23 bytes subido."""
    servers, roots, addresses = [], [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0)
        servers.append(server)
        roots.append(tmp_path / f"dn{i}")
        addresses.append(f"localhost:{port}")
    control = start_control_node(addresses, block_size_bytes=5, replication_factor=3)
    clients = []

    def new_client():
        client = DistributedDFShaClient([control])
        clients.append(client)
        return client

    client = new_client()
    source = tmp_path / "fuente.bin"
    source.write_bytes(CONTENT)
    client.upload(source, "/docs/a.bin")
    yield client, new_client, roots
    for c in clients:
        c.close()
    for server in servers:
        server.stop(grace=None)


def _blocks(client):
    return list(client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path="/docs/a.bin")).blocks)


@pytest.mark.parametrize(
    "offset,length",
    [
        (1, 3),  # dentro de un bloque
        (3, 9),  # cruza tres bloques
        (5, 5),  # empieza y termina justo en bordes de bloque
        (18, 5),  # el último bloque, que es más corto
        (0, 23),  # el archivo completo
        (20, 100),  # pide más de lo que hay: se corta en el final
    ],
)
def test_read_returns_exactly_the_requested_range(cluster, offset, length):
    client, _, _ = cluster
    assert client.read("/docs/a.bin", offset, length) == CONTENT[offset : offset + length]


def test_read_without_length_reads_until_the_end(cluster):
    client, _, _ = cluster
    assert client.read("/docs/a.bin", 12) == CONTENT[12:]


def test_read_with_offset_at_the_end_is_empty(cluster):
    client, _, _ = cluster
    assert client.read("/docs/a.bin", len(CONTENT), 4) == b""


@pytest.mark.parametrize("offset,length", [(len(CONTENT) + 1, 1), (-1, 3), (0, -1)])
def test_read_rejects_invalid_ranges_explicitly(cluster, offset, length):
    client, _, _ = cluster
    with pytest.raises(InvalidPathError):
        client.read("/docs/a.bin", offset, length)
    assert client.locks() == []


def test_read_only_requests_the_needed_bytes_from_each_block(cluster, monkeypatch):
    """Cada ReadBlock pide su parte del rango, con un deadline calculado por esos bytes."""
    client, _, _ = cluster
    requests = []
    real_stub = client._datanode_stub

    class _Recorder:
        def __init__(self, address):
            self._stub = real_stub(address)

        def ReadBlock(self, request, timeout=None):
            requests.append((request.offset, request.length, timeout))
            return self._stub.ReadBlock(request, timeout=timeout)

    monkeypatch.setattr(client, "_datanode_stub", _Recorder)
    assert client.read("/docs/a.bin", 3, 9) == CONTENT[3:12]
    assert [(o, l) for o, l, _ in requests] == [(3, 2), (0, 5), (0, 2)]
    assert [t for _, _, t in requests] == [client._block_transfer_timeout(n) for n in (2, 5, 2)]


def test_read_to_file_writes_the_range_atomically(cluster, tmp_path):
    client, _, _ = cluster
    destination = tmp_path / "rango.bin"
    assert client.read_to_file("/docs/a.bin", 4, 10, destination) == 10
    assert destination.read_bytes() == CONTENT[4:14]
    assert not list(tmp_path.glob("rango.bin.part-*"))


def test_read_to_file_failure_leaves_no_file(cluster, tmp_path):
    client, _, _ = cluster
    destination = tmp_path / "rango.bin"
    with pytest.raises(InvalidPathError):
        client.read_to_file("/docs/a.bin", 99, 1, destination)
    assert not destination.exists()
    assert not list(tmp_path.glob("rango.bin.part-*"))


class _MidStreamError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def test_read_fails_over_when_a_replica_dies_mid_range(cluster, monkeypatch):
    """La réplica manda bytes basura y se cae: el failover los descarta y el rango
    sale exacto desde la siguiente réplica."""
    client, _, _ = cluster
    middle = _blocks(client)[1]
    dying = middle.datanode_addresses[0]
    real_stub = client._datanode_stub

    class _Dying:
        def ReadBlock(self, request, timeout=None):
            if request.block_id != middle.block_id:
                yield from real_stub(dying).ReadBlock(request, timeout=timeout)
                return
            yield data_node_pb2.ReadBlockChunk(data=b"XY")
            raise _MidStreamError()

    monkeypatch.setattr(client, "_datanode_stub", lambda a: _Dying() if a == dying else real_stub(a))
    assert client.read("/docs/a.bin", 2, 12) == CONTENT[2:14]


def test_read_of_a_block_corrupted_in_every_replica_is_data_loss(cluster):
    client, _, roots = cluster
    first = _blocks(client)[0]
    for root in roots:
        path = root / first.block_id
        raw = bytearray(path.read_bytes())
        raw[0] ^= 0xFF
        path.write_bytes(bytes(raw))
    with pytest.raises(BlockCorruptedError):
        client.read("/docs/a.bin", 0, 3)
    assert client.locks() == []


def test_read_holds_the_shared_lock_while_reading(cluster, monkeypatch):
    """D-P3: mientras dura la lectura, otro cliente no puede tomar el lock de
    escritura. Al terminar, sí."""
    client, new_client, _ = cluster
    writer = new_client()
    entered, finish = threading.Event(), threading.Event()
    real_read = client._read_block_with_failover

    def slow_read(block, fh, offset=0, length=0):
        entered.set()
        assert finish.wait(5)
        return real_read(block, fh, offset, length)

    monkeypatch.setattr(client, "_read_block_with_failover", slow_read)
    result = []
    thread = threading.Thread(target=lambda: result.append(client.read("/docs/a.bin", 0, 4)))
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(ConflictError):
            writer.lock("/docs/a.bin", "w")
    finally:
        finish.set()
        thread.join(5)
    assert result == [CONTENT[:4]]
    assert client.locks() == []
    writer.lock("/docs/a.bin", "w").release()


def test_failed_release_does_not_turn_a_successful_read_into_an_error(cluster, monkeypatch):
    client, _, _ = cluster

    def failing_release(held):
        raise DFShaError("el ControlNode no respondió al Unlock")

    monkeypatch.setattr(client, "_release_held_lock", failing_release)
    assert client.read("/docs/a.bin", 0, 5) == CONTENT[:5]


# --- Shell ------------------------------------------------------------------------


class _RangeClient:
    def __init__(self, data: bytes):
        self.data = data
        self.calls = []

    def read(self, path, offset=0, length=None):
        self.calls.append((path, offset, length))
        end = len(self.data) if length is None else offset + length
        return self.data[offset:end]

    def read_to_file(self, path, offset, length, local_path):
        chunk = self.read(path, offset, length)
        local_path.write_bytes(chunk)
        return len(chunk)


def test_shell_cat_prints_the_file_or_a_range():
    client = _RangeClient(b"hola mundo")
    assert handle_command(client, "/docs", "cat a.txt") == ("/docs", "hola mundo")
    assert handle_command(client, "/docs", "cat a.txt 5 5") == ("/docs", "mundo")
    assert client.calls == [("/docs/a.txt", 0, None), ("/docs/a.txt", 5, 5)]


def test_shell_read_saves_a_range_to_a_local_file(tmp_path):
    client = _RangeClient(b"0123456789")
    destination = tmp_path / "salida.bin"
    _, output = handle_command(client, "/", f'read /a.bin 2 4 "{destination}"')
    assert destination.read_bytes() == b"2345"
    assert "4 bytes" in output


@pytest.mark.parametrize(
    "line", ["cat", "cat a.txt uno", "cat a.txt 1 dos", "read /a.bin 1 2", "read /a.bin x 2 out"]
)
def test_shell_range_commands_report_usage_errors(line):
    _, output = handle_command(_RangeClient(b""), "/", line)
    assert "uso" in output


@pytest.mark.parametrize("line", ["cat a.txt", "read /a 0 1 out", "lock a r", "unlock a", "locks"])
def test_shell_rf3_commands_are_rejected_cleanly_on_a_client_without_rf3(line):
    """La shell es compartida con el cliente monolítico de Hito 1, que no tiene
    RF3: el comando tiene que avisar, no tumbar la shell con AttributeError."""

    class _Hito1Client:
        pass

    _, output = handle_command(_Hito1Client(), "/", line)
    assert "no disponible" in output
