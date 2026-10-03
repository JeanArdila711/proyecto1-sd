from pathlib import Path

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import BlockCorruptedError, NotAFileError, PathExistsError, PathNotFoundError
from dfsha.data_node.main import serve as serve_data_node


@pytest.fixture
def client(tmp_path, start_control_node):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0, TEST_ENCRYPTION_KEY)
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
    server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY)
    client = DistributedDFShaClient(
        [start_control_node([f"localhost:{port}"], block_size_bytes=4, min_write_replicas=1)],
        rpc_timeout_s=0.03,
        transfer_base_timeout_s=0.5,
        minimum_transfer_throughput_bytes_per_s=100,
    )
    original_write_block = block_store.write_block

    def slow_write_block(root, key, block_id, chunks):
        def delayed_chunks():
            for chunk in chunks:
                time.sleep(0.02)
                yield chunk

        return original_write_block(root, key, block_id, delayed_chunks())

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
    server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY)
    client = DistributedDFShaClient(
        [start_control_node([f"localhost:{port}"], block_size_bytes=4, min_write_replicas=1)],
        rpc_timeout_s=0.03,
        transfer_base_timeout_s=0.01,
        minimum_transfer_throughput_bytes_per_s=100,
    )
    original_write_block = block_store.write_block

    def slow_write_block(root, key, block_id, chunks):
        def delayed_chunks():
            for chunk in chunks:
                time.sleep(0.03)
                yield chunk

        return original_write_block(root, key, block_id, delayed_chunks())

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


def test_download_holds_shared_lock_until_slow_read_finishes(client, tmp_path, monkeypatch):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/archivo.txt")
    real_read = client._read_block_with_failover
    entered = __import__("threading").Event()
    allow_finish = __import__("threading").Event()

    def slow_read(block, fh, *args, **kwargs):
        entered.set()
        assert allow_finish.wait(3)
        return real_read(block, fh, *args, **kwargs)

    monkeypatch.setattr(client, "_read_block_with_failover", slow_read)
    import threading
    error = []
    thread = threading.Thread(target=lambda: _download_error(client, tmp_path / "out", error))
    thread.start()
    assert entered.wait(2)
    with pytest.raises(Exception):
        client.lock("/archivo.txt", "w")
    allow_finish.set()
    thread.join(timeout=3)
    assert not thread.is_alive() and not error
    writer = client.lock("/archivo.txt", "w")
    writer.release()


def _download_error(client, path, errors):
    try:
        client.download("/archivo.txt", path)
    except BaseException as exc:
        errors.append(exc)


def test_lock_renewer_and_close_release_resources(client, tmp_path):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/archivo.txt")
    held = client.lock("/archivo.txt", "r")
    assert client._lock_renewal_thread is not None and client._lock_renewal_thread.is_alive()
    held.release()
    assert client._lock_renewal_thread is None
    writer = client.lock("/archivo.txt", "w")
    client.close()
    assert client._lock_renewal_thread is None
    assert writer.released


def test_lock_renewer_calls_renew_before_the_lease_expires(client, tmp_path, monkeypatch):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/archivo.txt")
    renewed = __import__("threading").Event()
    original = client._renew_held_lock

    def renew_and_signal(held):
        original(held)
        renewed.set()

    monkeypatch.setattr(client, "_renew_held_lock", renew_and_signal)
    held = client.lock("/archivo.txt", "r")
    held.lease_s = 0.06
    client._stop_lock_renewer()
    client._start_lock_renewer()
    assert renewed.wait(1), "el renovador no llamó RenewLock cada lease/3"
    held.close()



def test_unlock_normalizes_path_and_keeps_canonical_local_lock(client, tmp_path):
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"abc")
    client.upload(local, "/docs//a.txt")

    held = client.lock("docs/a.txt", "r")

    assert held.path == "/docs/a.txt"
    client.unlock("/docs//a.txt")
    assert client.locks() == []


def test_expired_lock_during_slow_download_does_not_mask_success_or_leak_resources(
    tmp_path, start_control_node, monkeypatch
):
    import threading
    import time

    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0, TEST_ENCRYPTION_KEY)
    client = DistributedDFShaClient(
        [
            start_control_node(
                [f"localhost:{dn_port}"], block_size_bytes=5, lock_lease_s=0.05, min_write_replicas=1
            )
        ]
    )
    source = tmp_path / "archivo.txt"
    destination = tmp_path / "descargado.txt"
    source.write_bytes(b"abc")
    client.upload(source, "/archivo.txt")

    renewal_started = threading.Event()
    renewal_finished = threading.Event()
    original_renew = client._renew_held_lock
    original_read = client._read_block_with_failover

    def renew_after_lease_expires(held):
        renewal_started.set()
        time.sleep(0.08)
        try:
            original_renew(held)
        finally:
            renewal_finished.set()

    def slow_read(block, fh, *args, **kwargs):
        assert renewal_started.wait(1)
        assert renewal_finished.wait(1)
        return original_read(block, fh, *args, **kwargs)

    monkeypatch.setattr(client, "_renew_held_lock", renew_after_lease_expires)
    monkeypatch.setattr(client, "_read_block_with_failover", slow_read)
    try:
        assert client.download("/archivo.txt", destination) == 3
        assert destination.read_bytes() == b"abc"
        assert client.locks() == []
        assert client._lock_renewal_thread is None
    finally:
        client.close()
        dn_server.stop(grace=None)
