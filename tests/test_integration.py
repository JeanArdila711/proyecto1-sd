from __future__ import annotations

from pathlib import Path

import grpc
import pytest

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.server.main import serve


@pytest.fixture
def running_server(tmp_path):
    server, port = serve(root=tmp_path / "data", host="localhost", port=0)
    yield port, tmp_path / "data"
    server.stop(grace=None)


@pytest.fixture
def stub(running_server):
    port, _ = running_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    yield dfsha_pb2_grpc.DFShaServiceStub(channel)
    channel.close()


def test_list_dir_vacio(stub):
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    assert list(response.entries) == []


def test_make_dir_y_list_dir(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    names = [e.name for e in response.entries]
    assert names == ["documentos"]
    assert response.entries[0].is_dir is True


def test_make_dir_duplicado_da_already_exists(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    assert exc_info.value.code() == grpc.StatusCode.ALREADY_EXISTS


def test_list_dir_inexistente_da_not_found(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ListDir(dfsha_pb2.ListDirRequest(path="/no-existe"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_remove_dir_no_vacio_da_failed_precondition(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/con-cosas"))
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/con-cosas/subcarpeta"))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/con-cosas"))
    assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_remove_dir_y_luego_no_aparece(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/temporal"))
    stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/temporal"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    assert list(response.entries) == []


def test_path_traversal_da_permission_denied(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ListDir(dfsha_pb2.ListDirRequest(path="/../../etc"))
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED


import hashlib


def _upload_chunks(path: str, data: bytes, chunk_size: int = 4096):
    yield dfsha_pb2.UploadChunk(path=path)
    for i in range(0, len(data), chunk_size):
        yield dfsha_pb2.UploadChunk(data=data[i : i + chunk_size])


def test_upload_y_download_roundtrip(stub):
    contenido = b"x" * (5 * 1024 * 1024 + 123)  # fuerza varios chunks de 1 MiB
    response = stub.Upload(_upload_chunks("/grande.bin", contenido))
    assert response.bytes_written == len(contenido)

    recibido = b"".join(
        chunk.data for chunk in stub.Download(dfsha_pb2.DownloadRequest(path="/grande.bin"))
    )
    assert hashlib.sha256(recibido).hexdigest() == hashlib.sha256(contenido).hexdigest()


def test_upload_luego_aparece_en_list_dir(stub):
    stub.Upload(_upload_chunks("/nota.txt", b"hola mundo"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    entry = next(e for e in response.entries if e.name == "nota.txt")
    assert entry.is_dir is False
    assert entry.size_bytes == len(b"hola mundo")


def test_download_inexistente_da_not_found(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        list(stub.Download(dfsha_pb2.DownloadRequest(path="/no-existe")))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


from dfsha.client.dfsha_client import DFShaClient
from dfsha.common.exceptions import NotAFileError, PathExistsError, PathNotFoundError


@pytest.fixture
def client(running_server):
    port, _ = running_server
    c = DFShaClient(host="localhost", port=port)
    yield c
    c.close()


def test_client_make_dir_y_list_dir(client):
    client.make_dir("/documentos")
    entries = client.list_dir("/")
    assert [e.name for e in entries] == ["documentos"]


def test_client_make_dir_duplicado_traduce_a_domain_error(client):
    client.make_dir("/documentos")
    with pytest.raises(PathExistsError):
        client.make_dir("/documentos")


def test_client_list_dir_inexistente_traduce_a_domain_error(client):
    with pytest.raises(PathNotFoundError):
        client.list_dir("/no-existe")


def test_client_upload_download_roundtrip(client, tmp_path):
    origen = tmp_path / "origen.bin"
    origen.write_bytes(b"contenido de prueba" * 10000)

    bytes_subidos = client.upload(origen, "/subido.bin")
    assert bytes_subidos == origen.stat().st_size

    destino = tmp_path / "destino.bin"
    bytes_bajados = client.download("/subido.bin", destino)
    assert bytes_bajados == bytes_subidos
    assert destino.read_bytes() == origen.read_bytes()


def test_client_download_fallido_no_destruye_archivo_local(client, tmp_path):
    local_path = tmp_path / "no_me_borres.txt"
    local_path.write_text("contenido original")

    with pytest.raises(PathNotFoundError):
        client.download("/no-existe", local_path)

    assert local_path.read_text() == "contenido original"
    assert list(tmp_path.glob("no_me_borres.txt.part-*")) == []


def test_client_upload_archivo_local_inexistente(client, tmp_path):
    with pytest.raises(NotAFileError):
        client.upload(tmp_path / "no-existe.txt", "/destino.txt")


def test_upload_sobre_directorio_da_invalid_argument(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/carpeta"))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.Upload(_upload_chunks("/carpeta", b"datos"))
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_shell_ls_formatea_entradas_reales(client):
    from dfsha.client.shell import handle_command

    handle_command(client, "/", "mkdir docs")
    _, output = handle_command(client, "/", "ls")
    assert output == f"d {0:>10}  docs"
