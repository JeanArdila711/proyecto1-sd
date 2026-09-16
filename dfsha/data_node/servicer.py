from __future__ import annotations

from pathlib import Path

import grpc

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError
from dfsha.data_node import block_store
from dfsha.generated import data_node_pb2, data_node_pb2_grpc

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

_ERROR_STATUS_MAP = {
    BlockNotFoundError: grpc.StatusCode.NOT_FOUND,
    BlockCorruptedError: grpc.StatusCode.DATA_LOSS,
}


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.abort(status_code, str(exc))


class DataNodeServicer(data_node_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, root: Path) -> None:
        self._root = root

    def WriteBlock(self, request_iterator, context):
        try:
            first = next(request_iterator)
        except StopIteration:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "stream vacío")
            return
        block_id = first.block_id

        def chunks():
            for msg in request_iterator:
                yield msg.data

        checksum, bytes_written = block_store.write_block(self._root, block_id, chunks())
        return data_node_pb2.WriteBlockResponse(checksum=checksum, bytes_written=bytes_written)

    def ReadBlock(self, request, context):
        try:
            for chunk in block_store.read_block(self._root, request.block_id, CHUNK_SIZE_BYTES):
                yield data_node_pb2.ReadBlockChunk(data=chunk)
        except (BlockNotFoundError, BlockCorruptedError) as exc:
            _abort_on_domain_error(context, exc)

    def DeleteBlock(self, request, context):
        try:
            block_store.delete_block(self._root, request.block_id)
        except BlockNotFoundError as exc:
            _abort_on_domain_error(context, exc)
        return data_node_pb2.DeleteBlockResponse()
