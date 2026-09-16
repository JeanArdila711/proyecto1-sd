from __future__ import annotations

import uuid

import grpc

from dfsha.common.exceptions import (
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.control_node.tree import ControlTree
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc

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


class ControlNodeServicer(control_node_pb2_grpc.ControlNodeServiceServicer):
    def __init__(self, datanode_address: str, block_size_bytes: int) -> None:
        self._tree = ControlTree()
        self._datanode_address = datanode_address
        self._block_size_bytes = block_size_bytes
        self._datanode_channel = grpc.insecure_channel(datanode_address)
        self._datanode_stub = data_node_pb2_grpc.DataNodeServiceStub(self._datanode_channel)

    def ListDir(self, request, context):
        try:
            entries = self._tree.list_dir(request.path)
        except (PathNotFoundError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.ListDirResponse()
        return control_node_pb2.ListDirResponse(
            entries=[
                control_node_pb2.DirEntry(name=e.name, is_dir=e.is_dir, size_bytes=e.size_bytes)
                for e in entries
            ]
        )

    def MakeDir(self, request, context):
        try:
            self._tree.make_dir(request.path)
        except (PathExistsError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.MakeDirResponse()

    def RemoveDir(self, request, context):
        try:
            self._tree.remove_dir(request.path)
        except (PathNotFoundError, NotADirectoryError, NotEmptyError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.RemoveDirResponse()

    def Remove(self, request, context):
        try:
            blocks = self._tree.remove_file(request.path)
        except (PathNotFoundError, NotAFileError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.RemoveResponse()
        for block in blocks:
            try:
                self._datanode_stub.DeleteBlock(data_node_pb2.DeleteBlockRequest(block_id=block.block_id))
            except grpc.RpcError:
                # best-effort: la metadata ya se borró, un bloque físico que falle
                # en limpiarse no debe tapar ni romper el Remove (mismo criterio
                # que AbortUpload)
                pass
        return control_node_pb2.RemoveResponse()

    def BeginUpload(self, request, context):
        if request.size_bytes <= 0:
            _abort_on_domain_error(context, InvalidPathError("size_bytes debe ser mayor a 0"))
            return control_node_pb2.BeginUploadResponse()

        num_blocks = -(-request.size_bytes // self._block_size_bytes)  # división hacia arriba
        block_ids = [uuid.uuid4().hex for _ in range(num_blocks)]
        try:
            self._tree.begin_upload(request.path, block_ids, self._datanode_address)
        except (PathExistsError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.BeginUploadResponse()

        remaining = request.size_bytes
        locations = []
        for block_id in block_ids:
            this_block_size = min(self._block_size_bytes, remaining)
            locations.append(
                control_node_pb2.BlockLocation(
                    block_id=block_id,
                    datanode_address=self._datanode_address,
                    size_bytes=this_block_size,
                )
            )
            remaining -= this_block_size
        return control_node_pb2.BeginUploadResponse(blocks=locations)

    def ConfirmBlock(self, request, context):
        try:
            self._tree.confirm_block(request.path, request.block_id, request.checksum, request.size_bytes)
        except PathNotFoundError as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ConfirmBlockResponse()

    def CompleteUpload(self, request, context):
        try:
            self._tree.complete_upload(request.path)
        except (PathNotFoundError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.CompleteUploadResponse()

    def AbortUpload(self, request, context):
        try:
            self._tree.abort_upload(request.path)
        except PathNotFoundError as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.AbortUploadResponse()

    def ListBlocks(self, request, context):
        try:
            blocks = self._tree.list_blocks(request.path)
        except PathNotFoundError as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.ListBlocksResponse()
        return control_node_pb2.ListBlocksResponse(
            blocks=[
                control_node_pb2.BlockInfo(
                    block_id=b.block_id,
                    datanode_address=b.datanode_address,
                    checksum=b.checksum,
                    size_bytes=b.size_bytes,
                )
                for b in blocks
            ]
        )
