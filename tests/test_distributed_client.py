from pathlib import Path

import pytest

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import BlockCorruptedError, NotAFileError, PathExistsError, PathNotFoundError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node


@pytest.fixture
def client(tmp_path):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0)
    datanode_address = f"localhost:{dn_port}"

    cn_server, cn_port = serve_control_node(datanode_address, "localhost", 0, block_size_bytes=5)
    c = DistributedDFShaClient(f"localhost:{cn_port}")

    yield c

    c.close()
    cn_server.stop(grace=None)
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
