from __future__ import annotations

import os
import uuid
from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.common.exceptions import (
    DFShaError,
    InvalidPathError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

_STATUS_ERROR_MAP = {
    grpc.StatusCode.NOT_FOUND: PathNotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: PathExistsError,
    grpc.StatusCode.FAILED_PRECONDITION: NotEmptyError,
    grpc.StatusCode.PERMISSION_DENIED: InvalidPathError,
    # INVALID_ARGUMENT cubre tanto NotAFileError como NotADirectoryError del
    # lado servidor: el cliente solo necesita mostrar el mensaje, no
    # distinguir el subtipo exacto.
    grpc.StatusCode.INVALID_ARGUMENT: NotAFileError,
}


def _translate(rpc_error: grpc.RpcError) -> DFShaError:
    exc_cls = _STATUS_ERROR_MAP.get(rpc_error.code(), DFShaError)
    return exc_cls(rpc_error.details())


class DFShaClient:
    def __init__(self, host: str, port: int) -> None:
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = dfsha_pb2_grpc.DFShaServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def list_dir(self, path: str) -> list[dfsha_pb2.DirEntry]:
        try:
            response = self._stub.ListDir(dfsha_pb2.ListDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return list(response.entries)

    def make_dir(self, path: str) -> None:
        try:
            self._stub.MakeDir(dfsha_pb2.MakeDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove_dir(self, path: str) -> None:
        try:
            self._stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove(self, path: str) -> None:
        try:
            self._stub.Remove(dfsha_pb2.RemoveRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def upload(self, local_path: Path, remote_path: str) -> int:
        if not local_path.is_file():
            raise NotAFileError(f"no existe o no es un archivo: {local_path}")

        def request_iterator():
            yield dfsha_pb2.UploadChunk(path=remote_path)
            with local_path.open("rb") as fh:
                while True:
                    data = fh.read(CHUNK_SIZE_BYTES)
                    if not data:
                        break
                    yield dfsha_pb2.UploadChunk(data=data)

        try:
            response = self._stub.Upload(request_iterator())
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return response.bytes_written

    def download(self, remote_path: str, local_path: Path) -> int:
        tmp_path = local_path.parent / f"{local_path.name}.part-{uuid.uuid4().hex}"
        bytes_written = 0
        try:
            with tmp_path.open("wb") as fh:
                for chunk in self._stub.Download(dfsha_pb2.DownloadRequest(path=remote_path)):
                    fh.write(chunk.data)
                    bytes_written += len(chunk.data)
            os.replace(tmp_path, local_path)
        except grpc.RpcError as exc:
            tmp_path.unlink(missing_ok=True)
            raise _translate(exc) from exc
        return bytes_written
