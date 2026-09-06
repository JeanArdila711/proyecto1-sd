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
