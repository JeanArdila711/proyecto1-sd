from __future__ import annotations

import io
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest

from dfsha.client.distributed_client import (
    ALLOWED_DOMAIN_ERROR_TYPES,
    DistributedDFShaClient,
    _translate,
)
from dfsha.common.exceptions import (
    AccessDeniedError,
    AuthError,
    BlockCorruptedError,
    BlockNotFoundError,
    ConflictError,
    DFShaError,
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.control_node.servicer import _abort_on_domain_error as control_abort
from dfsha.data_node.main import serve as serve_data_node
from dfsha.data_node.servicer import _abort_on_domain_error as data_abort
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc
from dfsha.server.main import serve as serve_server
from dfsha.server.servicer import _abort_on_domain_error as server_abort

VALID_BLOCK_ID = "1234567890abcdef1234567890abcdef"


class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details="error simulado", metadata=None):
        self._code = code
        self._details = details
        self._metadata = metadata

    def code(self):
        return self._code

    def details(self):
        return self._details

    def trailing_metadata(self):
        return self._metadata


class AbortCalled(Exception):
    pass


class RecordingContext:
    def __init__(self):
        self.metadata = None
        self.status = None
        self.details = None

    def set_trailing_metadata(self, metadata):
        self.metadata = metadata

    def abort(self, status, details):
        self.status = status
        self.details = details
        raise AbortCalled


class ReadStub:
    def __init__(self, error=None, chunks=()):
        self.error = error
        self.chunks = chunks
        self.calls = 0

    def ReadBlock(self, request):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return iter(data_node_pb2.ReadBlockChunk(data=data) for data in self.chunks)


def _op() -> str:
    return uuid.uuid4().hex


def _metadata_value(error: grpc.RpcError, key: str) -> str | None:
    return dict(error.trailing_metadata() or ()).get(key)


def _assert_domain_error(call, status: grpc.StatusCode, error_type: type[DFShaError]) -> None:
    with pytest.raises(grpc.RpcError) as exc_info:
        call()
    assert exc_info.value.code() == status
    assert _metadata_value(exc_info.value, "dfsha-error") == error_type.__name__


def _upload_chunks(path: str, data: bytes = b"x"):
    yield data_node_pb2.WriteBlockChunk()  # type marker only; unused by Hito 1 helpers
    del path, data


def _hito1_upload_chunks(path: str, data: bytes = b"x"):
    from dfsha.generated import dfsha_pb2

    yield dfsha_pb2.UploadChunk(path=path)
    yield dfsha_pb2.UploadChunk(data=data)


@pytest.fixture
def hito1_stub(tmp_path):
    server, port = serve_server(root=tmp_path / "server", host="localhost", port=0)
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = __import__("dfsha.generated.dfsha_pb2_grpc", fromlist=["DFShaServiceStub"]).DFShaServiceStub(channel)
    try:
        yield stub
    finally:
        channel.close()
        server.stop(grace=None)


@pytest.fixture
def control_stub(tmp_path, start_control_node):
    address = start_control_node(["localhost:1"], block_size_bytes=5)
    channel = grpc.insecure_channel(address)
    stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)
    try:
        yield stub
    finally:
        channel.close()


@pytest.fixture
def data_stub(tmp_path):
    server, port = serve_data_node(tmp_path / "data", "localhost", 0)
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = data_node_pb2_grpc.DataNodeServiceStub(channel)
    try:
        yield stub, tmp_path / "data"
    finally:
        channel.close()
        server.stop(grace=None)


def test_hito1_error_matrix_sets_status_and_domain_metadata(hito1_stub):
    """Cada fila de Hito 1 en la matriz tiene status y tipo de dominio exactos."""
    from dfsha.generated import dfsha_pb2

    stub = hito1_stub
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/dir"))
    stub.Upload(_hito1_upload_chunks("/file"))
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/nonempty"))
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/nonempty/child"))

    _assert_domain_error(
        lambda: stub.ListDir(dfsha_pb2.ListDirRequest(path="/missing")),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.ListDir(dfsha_pb2.ListDirRequest(path="/file")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.ListDir(dfsha_pb2.ListDirRequest(path="/../../etc")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/dir")),
        grpc.StatusCode.ALREADY_EXISTS,
        PathExistsError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/file/child")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/../../etc")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/missing")),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/file")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/nonempty")),
        grpc.StatusCode.FAILED_PRECONDITION,
        NotEmptyError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/../../etc")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.Remove(dfsha_pb2.RemoveRequest(path="/missing")),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.Remove(dfsha_pb2.RemoveRequest(path="/dir")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotAFileError,
    )
    _assert_domain_error(
        lambda: stub.Remove(dfsha_pb2.RemoveRequest(path="/../../etc")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.Upload(_hito1_upload_chunks("/")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.Upload(_hito1_upload_chunks("/dir")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotAFileError,
    )
    _assert_domain_error(
        lambda: stub.Upload(_hito1_upload_chunks("/file/child")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: list(stub.Download(dfsha_pb2.DownloadRequest(path="/missing"))),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: list(stub.Download(dfsha_pb2.DownloadRequest(path="/dir"))),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotAFileError,
    )
    _assert_domain_error(
        lambda: list(stub.Download(dfsha_pb2.DownloadRequest(path="/../../etc"))),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )


def test_hito1_make_dir_under_file_ancestor_sets_domain_metadata(hito1_stub):
    """MakeDir no deja escapar FileExistsError de un ancestro que es archivo."""
    from dfsha.generated import dfsha_pb2

    hito1_stub.Upload(_hito1_upload_chunks("/file"))

    _assert_domain_error(
        lambda: hito1_stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/file/child/grand")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )


def test_hito1_upload_under_file_ancestor_sets_domain_metadata(hito1_stub):
    """Upload no deja escapar FileExistsError de un ancestro que es archivo."""
    hito1_stub.Upload(_hito1_upload_chunks("/file"))

    _assert_domain_error(
        lambda: hito1_stub.Upload(_hito1_upload_chunks("/file/child/grand")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )


def test_control_node_error_matrix_sets_status_and_domain_metadata(control_stub):
    """Cada fila de ControlNode cubre todas sus excepciones de dominio esperadas."""
    stub = control_stub
    stub.MakeDir(control_node_pb2.MakeDirRequest(path="/dir", op_id=_op()))
    file_upload = stub.BeginUpload(
        control_node_pb2.BeginUploadRequest(path="/file", size_bytes=1, op_id=_op())
    )
    file_block = file_upload.blocks[0]
    stub.ConfirmBlock(
        control_node_pb2.ConfirmBlockRequest(
            path="/file",
            block_id=file_block.block_id,
            checksum="checksum",
            size_bytes=1,
            op_id=_op(),
        )
    )
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/file", op_id=_op()))
    stub.MakeDir(control_node_pb2.MakeDirRequest(path="/nonempty", op_id=_op()))
    stub.MakeDir(control_node_pb2.MakeDirRequest(path="/nonempty/child", op_id=_op()))

    _assert_domain_error(
        lambda: stub.ListDir(control_node_pb2.ListDirRequest(path="/missing")),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.ListDir(control_node_pb2.ListDirRequest(path="/file")),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.ListDir(control_node_pb2.ListDirRequest(path="/../../etc")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(control_node_pb2.MakeDirRequest(path="/dir", op_id=_op())),
        grpc.StatusCode.ALREADY_EXISTS,
        PathExistsError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(control_node_pb2.MakeDirRequest(path="/missing/child", op_id=_op())),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(control_node_pb2.MakeDirRequest(path="/file/child", op_id=_op())),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.MakeDir(control_node_pb2.MakeDirRequest(path="/../../etc", op_id=_op())),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(control_node_pb2.RemoveDirRequest(path="/missing", op_id=_op())),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(control_node_pb2.RemoveDirRequest(path="/file", op_id=_op())),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(control_node_pb2.RemoveDirRequest(path="/nonempty", op_id=_op())),
        grpc.StatusCode.FAILED_PRECONDITION,
        NotEmptyError,
    )
    _assert_domain_error(
        lambda: stub.RemoveDir(control_node_pb2.RemoveDirRequest(path="/", op_id=_op())),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.Remove(control_node_pb2.RemoveRequest(path="/missing", op_id=_op())),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.Remove(control_node_pb2.RemoveRequest(path="/dir", op_id=_op())),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotAFileError,
    )
    _assert_domain_error(
        lambda: stub.Remove(control_node_pb2.RemoveRequest(path="/file/child", op_id=_op())),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.Remove(control_node_pb2.RemoveRequest(path="/", op_id=_op())),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.BeginUpload(
            control_node_pb2.BeginUploadRequest(path="/file", size_bytes=1, op_id=_op())
        ),
        grpc.StatusCode.ALREADY_EXISTS,
        PathExistsError,
    )
    _assert_domain_error(
        lambda: stub.BeginUpload(
            control_node_pb2.BeginUploadRequest(path="/file/child", size_bytes=1, op_id=_op())
        ),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.BeginUpload(
            control_node_pb2.BeginUploadRequest(path="/", size_bytes=1, op_id=_op())
        ),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.BeginUpload(
            control_node_pb2.BeginUploadRequest(path="/zero", size_bytes=0, op_id=_op())
        ),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.ConfirmBlock(
            control_node_pb2.ConfirmBlockRequest(
                path="/missing", block_id=VALID_BLOCK_ID, checksum="x", size_bytes=1, op_id=_op()
            )
        ),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.ConfirmBlock(
            control_node_pb2.ConfirmBlockRequest(
                path="/../../bad", block_id=VALID_BLOCK_ID, checksum="x", size_bytes=1, op_id=_op()
            )
        ),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.CompleteUpload(
            control_node_pb2.CompleteUploadRequest(path="/missing", op_id=_op())
        ),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.CompleteUpload(
            control_node_pb2.CompleteUploadRequest(path="/../../bad", op_id=_op())
        ),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.AbortUpload(control_node_pb2.AbortUploadRequest(path="/missing", op_id=_op())),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.AbortUpload(control_node_pb2.AbortUploadRequest(path="/file/child", op_id=_op())),
        grpc.StatusCode.INVALID_ARGUMENT,
        NotADirectoryError,
    )
    _assert_domain_error(
        lambda: stub.AbortUpload(control_node_pb2.AbortUploadRequest(path="/../../bad", op_id=_op())),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )
    _assert_domain_error(
        lambda: stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/missing")),
        grpc.StatusCode.NOT_FOUND,
        PathNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/../../bad")),
        grpc.StatusCode.PERMISSION_DENIED,
        InvalidPathError,
    )


def test_data_node_error_matrix_sets_status_and_domain_metadata(data_stub):
    """Cada fila DataNode conserva status y clase, incluido WriteBlock inválido."""
    stub, root = data_stub

    def invalid_write():
        return stub.WriteBlock(
            iter(
                [
                    data_node_pb2.WriteBlockChunk(
                        header=data_node_pb2.WriteBlockHeader(block_id="block-invalido")
                    )
                ]
            )
        )

    _assert_domain_error(invalid_write, grpc.StatusCode.NOT_FOUND, BlockNotFoundError)
    _assert_domain_error(
        lambda: list(stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id="block-invalido"))),
        grpc.StatusCode.NOT_FOUND,
        BlockNotFoundError,
    )
    _assert_domain_error(
        lambda: stub.DeleteBlock(data_node_pb2.DeleteBlockRequest(block_id="block-invalido")),
        grpc.StatusCode.NOT_FOUND,
        BlockNotFoundError,
    )

    def valid_write():
        return stub.WriteBlock(
            iter(
                [
                    data_node_pb2.WriteBlockChunk(
                        header=data_node_pb2.WriteBlockHeader(block_id=VALID_BLOCK_ID)
                    ),
                    data_node_pb2.WriteBlockChunk(data=b"contenido original"),
                ]
            )
        )

    valid_write()
    root.joinpath(VALID_BLOCK_ID).write_bytes(b"corrupto")
    _assert_domain_error(
        lambda: list(stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=VALID_BLOCK_ID))),
        grpc.StatusCode.DATA_LOSS,
        BlockCorruptedError,
    )


@pytest.mark.parametrize(
    "error_type",
    [
        InvalidPathError,
        PathNotFoundError,
        PathExistsError,
        NotEmptyError,
        NotAFileError,
        NotADirectoryError,
        BlockNotFoundError,
        BlockCorruptedError,
        ConflictError,
        AccessDeniedError,
        AuthError,
    ],
)
def test_translate_instantiates_only_each_explicitly_allowed_domain_type(error_type):
    error = FakeRpcError(
        grpc.StatusCode.UNKNOWN,
        metadata=(("dfsha-error", error_type.__name__),),
    )

    translated = _translate(error)

    assert type(translated) is error_type
    assert str(translated) == "error simulado"


def test_allowed_domain_error_list_is_exact_and_explicit():
    assert set(ALLOWED_DOMAIN_ERROR_TYPES) == {
        InvalidPathError,
        PathNotFoundError,
        PathExistsError,
        NotEmptyError,
        NotAFileError,
        NotADirectoryError,
        BlockNotFoundError,
        BlockCorruptedError,
        ConflictError,
        AccessDeniedError,
        AuthError,
    }


@pytest.mark.parametrize(
    ("code", "metadata", "expected"),
    [
        (grpc.StatusCode.NOT_FOUND, None, PathNotFoundError),
        (grpc.StatusCode.INVALID_ARGUMENT, (("dfsha-error", "NoExiste"),), NotAFileError),
        (grpc.StatusCode.PERMISSION_DENIED, (("dfsha-error", "DFShaError"),), InvalidPathError),
        (grpc.StatusCode.UNAUTHENTICATED, (("dfsha-error", "builtins.RuntimeError"),), AuthError),
    ],
)
def test_translate_uses_safe_fallback_for_absent_unknown_or_manipulated_metadata(code, metadata, expected):
    translated = _translate(FakeRpcError(code, metadata=metadata))

    assert type(translated) is expected


@pytest.mark.parametrize(
    "metadata",
    [
        (("dfsha-error", "AuthError"), ("dfsha-error", "AuthError")),
        (("dfsha-error", "AuthError"), ("dfsha-error", "AccessDeniedError")),
    ],
    ids=["duplicada-igual", "duplicada-conflictiva"],
)
def test_translate_uses_safe_fallback_for_duplicated_domain_metadata(metadata):
    translated = _translate(FakeRpcError(grpc.StatusCode.PERMISSION_DENIED, metadata=metadata))

    assert type(translated) is InvalidPathError


@pytest.mark.parametrize("aborter", [server_abort, control_abort, data_abort])
@pytest.mark.parametrize(
    ("error", "status"),
    [
        (ConflictError("conflicto"), grpc.StatusCode.ABORTED),
        (AccessDeniedError("denegado"), grpc.StatusCode.PERMISSION_DENIED),
        (AuthError("sin autenticar"), grpc.StatusCode.UNAUTHENTICATED),
    ],
)
def test_all_domain_aborters_map_new_errors_and_attach_metadata(aborter, error, status):
    context = RecordingContext()

    with pytest.raises(AbortCalled):
        aborter(context, error)

    assert context.status == status
    assert context.metadata == (("dfsha-error", type(error).__name__),)


@pytest.mark.parametrize(
    "code,metadata",
    [
        (grpc.StatusCode.UNAVAILABLE, None),
        (grpc.StatusCode.DEADLINE_EXCEEDED, None),
        (grpc.StatusCode.DATA_LOSS, None),
        (grpc.StatusCode.NOT_FOUND, (("dfsha-error", "BlockNotFoundError"),)),
    ],
)
def test_read_failover_retries_only_recoverable_block_errors(code, metadata):
    first = ReadStub(error=FakeRpcError(code, metadata=metadata))
    second = ReadStub(chunks=(b"recuperado",))
    client = DistributedDFShaClient(["localhost:1"])
    client._datanode_stub = lambda address: {"dn1": first, "dn2": second}[address]
    block = SimpleNamespace(block_id=VALID_BLOCK_ID, datanode_addresses=["dn1", "dn2"])
    output = io.BytesIO()
    try:
        written = client._read_block_with_failover(block, output)
    finally:
        client.close()

    assert written == len(b"recuperado")
    assert output.getvalue() == b"recuperado"
    assert first.calls == 1
    assert second.calls == 1


@pytest.mark.parametrize(
    "code,metadata",
    [
        (grpc.StatusCode.CANCELLED, None),
        (grpc.StatusCode.UNKNOWN, None),
        (grpc.StatusCode.INVALID_ARGUMENT, None),
        (grpc.StatusCode.ALREADY_EXISTS, None),
        (grpc.StatusCode.PERMISSION_DENIED, (("dfsha-error", "AccessDeniedError"),)),
        (grpc.StatusCode.RESOURCE_EXHAUSTED, None),
        (grpc.StatusCode.FAILED_PRECONDITION, None),
        (grpc.StatusCode.ABORTED, (("dfsha-error", "ConflictError"),)),
        (grpc.StatusCode.OUT_OF_RANGE, None),
        (grpc.StatusCode.UNIMPLEMENTED, None),
        (grpc.StatusCode.INTERNAL, None),
        (grpc.StatusCode.UNAUTHENTICATED, (("dfsha-error", "AuthError"),)),
        (grpc.StatusCode.NOT_FOUND, (("dfsha-error", "PathNotFoundError"),)),
        (grpc.StatusCode.NOT_FOUND, None),
        (grpc.StatusCode.NOT_FOUND, (("dfsha-error", "UnknownDomainError"),)),
    ],
)
def test_read_failover_does_not_retry_permanent_errors(code, metadata):
    error = FakeRpcError(code, metadata=metadata)
    first = ReadStub(error=error)
    second = ReadStub(chunks=(b"no debe leerse",))
    client = DistributedDFShaClient(["localhost:1"])
    client._datanode_stub = lambda address: {"dn1": first, "dn2": second}[address]
    block = SimpleNamespace(block_id=VALID_BLOCK_ID, datanode_addresses=["dn1", "dn2"])
    try:
        with pytest.raises(grpc.RpcError) as exc_info:
            client._read_block_with_failover(block, io.BytesIO())
    finally:
        client.close()

    assert exc_info.value is error
    assert first.calls == 1
    assert second.calls == 0


def test_datanode_help_describes_pipeline_replication():
    result = subprocess.run(
        [sys.executable, "-m", "dfsha.data_node.main", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "replicación por pipeline" in result.stdout
    assert "sin replicación" not in result.stdout
