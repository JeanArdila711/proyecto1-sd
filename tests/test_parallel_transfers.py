"""send/receive transfieren varios bloques a la vez (requisito de rendimiento)."""

import io
import os
import threading
import time

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.client.distributed_client import DistributedDFShaClient, _RegionWriter
from dfsha.common.exceptions import BlockCorruptedError
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, data_node_pb2

BLOCK = 5
CONTENT = os.urandom(BLOCK * 40 + 3)  # 41 bloques, el último de 3 bytes


@pytest.fixture
def make_client(tmp_path, start_control_node):
    servers, clients = [], []
    addresses = []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    cn_address = start_control_node(addresses, block_size_bytes=BLOCK)

    def _make(parallel_transfers=4):
        client = DistributedDFShaClient([cn_address], parallel_transfers=parallel_transfers)
        clients.append(client)
        return client

    yield _make

    for client in clients:
        client.close()
    for server in servers:
        server.stop(grace=None)


class _Concurrency:
    """Cuenta cuántas llamadas a un método del cliente corren a la vez."""

    def __init__(self, client, method):
        self._real = getattr(client, method)
        self._guard = threading.Lock()
        self._running = 0
        self.max = 0
        self.calls = 0
        setattr(client, method, self)

    def __call__(self, *args, **kwargs):
        with self._guard:
            self._running += 1
            self.calls += 1
            self.max = max(self.max, self._running)
        try:
            time.sleep(0.02)  # que se note el solapamiento
            return self._real(*args, **kwargs)
        finally:
            with self._guard:
                self._running -= 1


def _local(tmp_path, content=CONTENT):
    path = tmp_path / "local.bin"
    path.write_bytes(content)
    return path


def test_upload_and_download_move_several_blocks_at_once(make_client, tmp_path):
    client = make_client(parallel_transfers=4)
    writes = _Concurrency(client, "_write_block")
    reads = _Concurrency(client, "_read_block_with_failover")

    assert client.upload(_local(tmp_path), "/a.bin") == len(CONTENT)
    destination = tmp_path / "copia.bin"
    assert client.download("/a.bin", destination) == len(CONTENT)

    assert destination.read_bytes() == CONTENT
    assert 1 < writes.max <= 4
    assert 1 < reads.max <= 4
    assert writes.calls == reads.calls == 41


def test_one_parallel_transfer_is_sequential(make_client, tmp_path):
    client = make_client(parallel_transfers=1)
    writes = _Concurrency(client, "_write_block")
    reads = _Concurrency(client, "_read_block_with_failover")

    client.upload(_local(tmp_path), "/a.bin")
    destination = tmp_path / "copia.bin"
    client.download("/a.bin", destination)

    assert destination.read_bytes() == CONTENT
    assert writes.max == reads.max == 1


def test_failed_block_stops_the_rest_and_aborts_the_upload(make_client, tmp_path):
    client = make_client(parallel_transfers=4)
    real_write = client._write_block
    calls = []
    guard = threading.Lock()

    def fail_third(block, fh, stop=None):
        with guard:
            calls.append(block.block_id)
            number = len(calls)
        if number == 3:
            raise OSError("disco lleno simulado")
        time.sleep(0.02)
        return real_write(block, fh, stop)

    client._write_block = fail_third
    with pytest.raises(OSError, match="disco lleno simulado"):
        client.upload(_local(tmp_path), "/a.bin")

    assert client.list_dir("/") == []
    # los bloques que no habían empezado no se transfieren
    assert len(calls) < 41
    # y el nombre queda libre: AbortUpload se hizo con todos los hilos quietos
    client._write_block = real_write
    client.upload(_local(tmp_path), "/a.bin")
    assert client.read("/a.bin") == CONTENT


class _MidStreamError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


def test_parallel_download_rewrites_blocks_whose_replica_died_mid_stream(make_client, tmp_path, monkeypatch):
    """Varias réplicas se caen a mitad de bloque al mismo tiempo. Cada hilo descarta
    solo su región y la reescribe desde otra réplica, sin tocar las de los demás."""
    client = make_client(parallel_transfers=4)
    client.upload(_local(tmp_path), "/a.bin")
    blocks = list(client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path="/a.bin")).blocks)
    dying = {block.block_id: block.datanode_addresses[0] for block in blocks[::2]}
    real_stub = client._datanode_stub

    class _DyingStub:
        def __init__(self, address):
            self._address = address

        def ReadBlock(self, request, timeout=None):
            if dying.get(request.block_id) != self._address:
                return real_stub(self._address).ReadBlock(request, timeout=timeout)
            return self._die()

        @staticmethod
        def _die():
            yield data_node_pb2.ReadBlockChunk(data=b"XYZ")
            raise _MidStreamError()

    monkeypatch.setattr(client, "_datanode_stub", _DyingStub)
    destination = tmp_path / "copia.bin"
    client.download("/a.bin", destination)
    assert destination.read_bytes() == CONTENT


def test_region_writer_keeps_each_block_inside_its_region():
    buffer = io.BytesIO(b"." * 12)
    buffer.seek(4)
    region = _RegionWriter(buffer, 4)
    region.write(b"ab")
    region.seek(0)
    region.truncate()  # no recorta el archivo: los otros bloques siguen ahí
    region.write(b"WXYZ")
    with pytest.raises(BlockCorruptedError):
        region.write(b"!")
    assert buffer.getvalue() == b"....WXYZ...."


def test_parallel_transfers_must_be_positive():
    with pytest.raises(ValueError):
        DistributedDFShaClient(["localhost:1"], parallel_transfers=0)
