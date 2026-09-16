from __future__ import annotations

import threading
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

DEFAULT_REPLICATION_FACTOR = 3

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
    def __init__(
        self,
        datanode_addresses: list[str],
        block_size_bytes: int,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    ) -> None:
        if not datanode_addresses:
            raise ValueError("hace falta al menos un DataNode")
        if replication_factor < 1:
            raise ValueError(f"el factor de replicación debe ser >= 1, no {replication_factor}")
        self._tree = ControlTree()
        self._datanode_addresses = list(datanode_addresses)
        self._block_size_bytes = block_size_bytes
        # con menos DataNodes que el factor pedido, se replica en todos los que haya
        self._replication_factor = min(replication_factor, len(self._datanode_addresses))
        self._next_offset = 0
        self._offset_lock = threading.Lock()
        self._channels: dict[str, grpc.Channel] = {}

    def _datanode_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._channels[address])

    def _pick_replicas(self) -> list[str]:
        """Round-robin: cada bloque arranca un nodo más adelante que el anterior, así
        los bloques de un archivo se reparten en vez de apilarse en los mismos 3."""
        total = len(self._datanode_addresses)
        with self._offset_lock:
            start = self._next_offset
            self._next_offset = (start + 1) % total
        return [self._datanode_addresses[(start + i) % total] for i in range(self._replication_factor)]

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
            for address in block.datanode_addresses:
                try:
                    self._datanode_stub(address).DeleteBlock(
                        data_node_pb2.DeleteBlockRequest(block_id=block.block_id)
                    )
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
        placements = [(uuid.uuid4().hex, self._pick_replicas()) for _ in range(num_blocks)]
        try:
            self._tree.begin_upload(request.path, placements)
        except (PathExistsError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.BeginUploadResponse()

        remaining = request.size_bytes
        locations = []
        for block_id, addresses in placements:
            this_block_size = min(self._block_size_bytes, remaining)
            locations.append(
                control_node_pb2.BlockLocation(
                    block_id=block_id,
                    datanode_addresses=addresses,
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
                    datanode_addresses=b.datanode_addresses,
                    checksum=b.checksum,
                    size_bytes=b.size_bytes,
                )
                for b in blocks
            ]
        )
