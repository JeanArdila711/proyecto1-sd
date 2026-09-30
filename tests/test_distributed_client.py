from pathlib import Path

import grpc
import pytest

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import BlockCorruptedError, NotAFileError, PathExistsError, PathNotFoundError
from dfsha.data_node.main import serve as serve_data_node


@pytest.fixture
def client(tmp_path, start_control_node):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0)
    datanode_address = f"localhost:{dn_port}"

    # D-P2: este fixture prueba el modo deliberado de una sola réplica.
    c = DistributedDFShaClient([start_control_node([datanode_address], block_size_bytes=5, min_write_replicas=1)])

    yield c

    c.close()
    dn_server.stop(grace=None)


def test_make_dir_then_list(client):
    client.make_dir("/documentos")

    entries = client.list_dir("/")

    assert [e.name for e in entries] == ["documentos"]


def test_make_dir_existing_raises(client):
    client.make_dir("/documentos")

    with pytest.raises(PathExistsError):
        client.make_dir("/documentos")


def test_upload_nonexistent_local_file_raises(client, tmp_path):
    with pytest.raises(NotAFileError):
        client.upload(tmp_path / "no-existe.txt", "/archivo.txt")


def test_upload_single_block_file(client, tmp_path):
    local = tmp_path / "chico.txt"
    local.write_bytes(b"abc")  # menor al block_size_bytes=5 del fixture

    bytes_written = client.upload(local, "/chico.txt")

    assert bytes_written == 3
    entries = client.list_dir("/")
    assert [e.name for e in entries] == ["chico.txt"]
    assert entries[0].size_bytes == 3


def test_upload_multi_block_file(client, tmp_path):
    local = tmp_path / "grande.txt"
    local.write_bytes(b"0123456789ab")  # 12 bytes, block_size_bytes=5 -> 3 bloques

    bytes_written = client.upload(local, "/grande.txt")

    assert bytes_written == 12
    entries = client.list_dir("/")
    assert entries[0].size_bytes == 12


def test_remove_after_upload(client, tmp_path):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"data")
    client.upload(local, "/archivo.txt")

    client.remove("/archivo.txt")

    assert client.list_dir("/") == []
    with pytest.raises(PathNotFoundError):
        client.remove("/archivo.txt")


def test_download_roundtrip_single_block(client, tmp_path):
    local = tmp_path / "chico.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/chico.txt")

    destino = tmp_path / "descargado.txt"
    bytes_written = client.download("/chico.txt", destino)

    assert bytes_written == 3
    assert destino.read_bytes() == b"abc"


def test_download_roundtrip_multi_block(client, tmp_path):
    local = tmp_path / "grande.txt"
    contenido = b"0123456789abcdef"  # 16 bytes, block_size_bytes=5 -> 4 bloques
    local.write_bytes(contenido)
    client.upload(local, "/grande.txt")

    destino = tmp_path / "descargado.txt"
    client.download("/grande.txt", destino)

    assert destino.read_bytes() == contenido


def test_download_missing_file_raises(client, tmp_path):
    with pytest.raises(PathNotFoundError):
        client.download("/no-existe.txt", tmp_path / "x.txt")


def test_download_corrupted_block_does_not_touch_existing_local_file(client, tmp_path, monkeypatch):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/archivo.txt")

    # corromper el bloque directo en el DataNode
    dn_root = tmp_path / "datanode"
    block_files = list(dn_root.glob("*"))
    block_file = next(f for f in block_files if not f.name.endswith(".sha256"))
    block_file.write_bytes(b"XXX-corrupto")

    destino = tmp_path / "ya_existia.txt"
    destino.write_bytes(b"contenido local previo")

    with pytest.raises(BlockCorruptedError):
        client.download("/archivo.txt", destino)

    assert destino.read_bytes() == b"contenido local previo"  # intacto
    assert list(destino.parent.glob("*.part-*")) == []  # sin temporales huérfanos


def test_data_plane_operations_receive_explicit_deadline(tmp_path, monkeypatch):
    """Un DataNode colgado no puede dejar ReadBlock ni WriteBlock sin límite."""
    from io import BytesIO
    from types import SimpleNamespace

    from dfsha.generated import data_node_pb2

    client = DistributedDFShaClient(["localhost:1"], rpc_timeout_s=0.123)
    timeouts = []

    class Stub:
        def ReadBlock(self, request, timeout):
            timeouts.append(timeout)
            return iter([data_node_pb2.ReadBlockChunk(data=b"abc")])

        def WriteBlock(self, chunks, timeout):
            timeouts.append(timeout)
            list(chunks)
            return data_node_pb2.WriteBlockResponse(checksum="checksum", bytes_written=3)

    monkeypatch.setattr(client, "_datanode_stub", lambda _: Stub())
    block = SimpleNamespace(block_id="1234567890abcdef1234567890abcdef", datanode_addresses=["dn:1"], size_bytes=3)
    out = BytesIO()
    assert client._read_block_with_failover(block, out) == 3
    assert client._write_block(block, BytesIO(b"abc")) == ("checksum", 3)
    assert timeouts == [client._block_transfer_timeout(3)] * 2
    client.close()


def test_data_plane_write_is_not_retried_after_failure(tmp_path, monkeypatch):
    import grpc
    from types import SimpleNamespace

    class Unavailable(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

        def details(self):
            return "caído"

    calls = []

    class Stub:
        def WriteBlock(self, chunks, timeout):
            calls.append(timeout)
            raise Unavailable()

    client = DistributedDFShaClient(["localhost:1"], rpc_timeout_s=0.1)
    monkeypatch.setattr(client, "_datanode_stub", lambda _: Stub())
    block = SimpleNamespace(block_id="1234567890abcdef1234567890abcdef", datanode_addresses=["dn:1"], size_bytes=1)
    with pytest.raises(grpc.RpcError):
        client._write_block(block, __import__("io").BytesIO(b"x"))
    assert calls == [client._block_transfer_timeout(1)]
    client.close()


def test_control_retry_backoff_is_exponential_with_jitter(monkeypatch):
    from dfsha.client.distributed_client import _retry_delay

    monkeypatch.setattr("dfsha.client.distributed_client.random.uniform", lambda low, high: 1.25)
    assert _retry_delay(0) == pytest.approx(0.25)
    assert _retry_delay(1) == pytest.approx(0.5)
    assert _retry_delay(99) == pytest.approx(2.5)  # tope exponencial, jitter conservado


def test_slow_active_write_exceeding_rpc_timeout_completes_before_block_deadline(
    tmp_path, start_control_node, monkeypatch
):
    """Un stream activo puede durar más que un RPC de control corto."""
    import time

    from dfsha.data_node import block_store

    root = tmp_path / "slow-datanode"
    server, port = serve_data_node(root, "localhost", 0)
    client = DistributedDFShaClient(
        [start_control_node([f"localhost:{port}"], block_size_bytes=4, min_write_replicas=1)],
        rpc_timeout_s=0.03,
        transfer_base_timeout_s=0.5,
        minimum_transfer_throughput_bytes_per_s=100,
    )
    original_write_block = block_store.write_block

    def slow_write_block(root, block_id, chunks):
        def delayed_chunks():
            for chunk in chunks:
                time.sleep(0.02)
                yield chunk

        return original_write_block(root, block_id, delayed_chunks())

    monkeypatch.setattr("dfsha.client.distributed_client.CHUNK_SIZE_BYTES", 1)
    monkeypatch.setattr(block_store, "write_block", slow_write_block)
    source = tmp_path / "source.bin"
    source.write_bytes(b"abcd")
    try:
        assert client.upload(source, "/slow.bin") == 4
    finally:
        client.close()
        server.stop(grace=None)


def test_write_exceeding_block_deadline_aborts_upload_without_partial_block(
    tmp_path, start_control_node, monkeypatch
):
    """Un deadline de bloque vencido aborta la subida y descarta el temporal."""
    import time

    from dfsha.data_node import block_store

    root = tmp_path / "slow-datanode"
    server, port = serve_data_node(root, "localhost", 0)
    client = DistributedDFShaClient(
        [start_control_node([f"localhost:{port}"], block_size_bytes=4, min_write_replicas=1)],
        rpc_timeout_s=0.03,
        transfer_base_timeout_s=0.01,
        minimum_transfer_throughput_bytes_per_s=100,
    )
    original_write_block = block_store.write_block

    def slow_write_block(root, block_id, chunks):
        def delayed_chunks():
            for chunk in chunks:
                time.sleep(0.03)
                yield chunk

        return original_write_block(root, block_id, delayed_chunks())

    monkeypatch.setattr("dfsha.client.distributed_client.CHUNK_SIZE_BYTES", 1)
    monkeypatch.setattr(block_store, "write_block", slow_write_block)
    source = tmp_path / "source.bin"
    source.write_bytes(b"abcd")
    try:
        with pytest.raises(Exception) as exc_info:
            client.upload(source, "/timed-out.bin")
        assert getattr(exc_info.value.__cause__, "code")() == grpc.StatusCode.DEADLINE_EXCEEDED
        assert client.list_dir("/") == []
        assert not list(root.iterdir())
    finally:
        client.close()
        server.stop(grace=None)
