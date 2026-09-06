from __future__ import annotations

from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.server import filesystem
from dfsha.server.exceptions import (
    InvalidPathError,
    NotAFileError,
    NotADirectoryError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

_ERROR_STATUS_MAP = {
    PathNotFoundError: grpc.StatusCode.NOT_FOUND,
    PathExistsError: grpc.StatusCode.ALREADY_EXISTS,
    NotEmptyError: grpc.StatusCode.FAILED_PRECONDITION,
    InvalidPathError: grpc.StatusCode.PERMISSION_DENIED,
    NotAFileError: grpc.StatusCode.INVALID_ARGUMENT,
    NotADirectoryError: grpc.StatusCode.INVALID_ARGUMENT,
}


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.abort(status_code, str(exc))


class DFShaServicer(dfsha_pb2_grpc.DFShaServiceServicer):
    def __init__(self, root: Path) -> None:
        self._root = root

    def ListDir(self, request, context):
        try:
            entries = filesystem.list_dir(self._root, request.path)
        except (PathNotFoundError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.ListDirResponse(
            entries=[
                dfsha_pb2.DirEntry(name=e.name, is_dir=e.is_dir, size_bytes=e.size_bytes)
                for e in entries
            ]
        )

    def MakeDir(self, request, context):
        try:
            filesystem.make_dir(self._root, request.path)
        except (PathExistsError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.MakeDirResponse()

    def RemoveDir(self, request, context):
        try:
            filesystem.remove_dir(self._root, request.path)
        except (PathNotFoundError, NotADirectoryError, NotEmptyError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.RemoveDirResponse()

    def Remove(self, request, context):
        try:
            filesystem.remove_file(self._root, request.path)
        except (PathNotFoundError, NotAFileError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.RemoveResponse()

    def Upload(self, request_iterator, context):
        first = next(request_iterator)
        path = first.path

        def chunks():
            for msg in request_iterator:
                yield msg.data

        try:
            bytes_written = filesystem.write_file_chunks(self._root, path, chunks())
        except (InvalidPathError, NotAFileError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.UploadResponse(bytes_written=bytes_written)

    def Download(self, request, context):
        try:
            for chunk in filesystem.read_file_chunks(self._root, request.path, CHUNK_SIZE_BYTES):
                yield dfsha_pb2.DownloadChunk(data=chunk)
        except (PathNotFoundError, NotAFileError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
