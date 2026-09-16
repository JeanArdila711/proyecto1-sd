from __future__ import annotations

import contextlib
import queue
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError
from dfsha.data_node import block_store
from dfsha.generated import data_node_pb2, data_node_pb2_grpc

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

# ponytail: la cola acotada es lo único que impide que un bloque entero (128 MB
# por defecto) se acumule en memoria mientras el pipeline aguas abajo va lento —
# con maxsize=4 el tope es 4 MiB y el productor se bloquea. Si algún día hace
# falta más throughput, subir este número, no sacar el límite.
QUEUE_DEPTH_CHUNKS = 4

_ERROR_STATUS_MAP = {
    BlockNotFoundError: grpc.StatusCode.NOT_FOUND,
    BlockCorruptedError: grpc.StatusCode.DATA_LOSS,
}


def _discard_local(root: Path, block_id: str) -> None:
    """La escritura fracasó aguas abajo: la copia local no la conoce nadie, se tira."""
    with contextlib.suppress(BlockNotFoundError):
        block_store.delete_block(root, block_id)


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.abort(status_code, str(exc))


class DataNodeServicer(data_node_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, root: Path) -> None:
        self._root = root
        # Executor propio para el forwarding del pipeline: si compartiera el del
        # servidor gRPC, N escrituras concurrentes podrían quedarse sin worker
        # para reenviar y el pipeline se auto-bloquearía.
        self._forward_pool = futures.ThreadPoolExecutor(max_workers=10)
        self._channels: dict[str, grpc.Channel] = {}

    def _peer_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._channels[address])

    def WriteBlock(self, request_iterator, context):
        try:
            first = next(request_iterator)
        except StopIteration:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "stream vacío")
            return
        if not first.HasField("header"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "el primer mensaje debe ser un header")
            return

        block_id = first.header.block_id
        downstream = list(first.header.downstream)

        if not downstream:
            checksum, bytes_written = block_store.write_block(
                self._root, block_id, (msg.data for msg in request_iterator)
            )
            return data_node_pb2.WriteBlockResponse(checksum=checksum, bytes_written=bytes_written)

        return self._write_and_forward(request_iterator, context, block_id, downstream)

    def _write_and_forward(self, request_iterator, context, block_id: str, downstream: list[str]):
        """Escribe el bloque local y lo reenvía al siguiente del pipeline al mismo
        tiempo, haciendo tee de cada chunk a medida que llega."""
        pending: queue.Queue = queue.Queue(maxsize=QUEUE_DEPTH_CHUNKS)

        def forwarded_chunks():
            yield data_node_pb2.WriteBlockChunk(
                header=data_node_pb2.WriteBlockHeader(block_id=block_id, downstream=downstream[1:])
            )
            while (item := pending.get()) is not None:
                yield data_node_pb2.WriteBlockChunk(data=item)

        stub = self._peer_stub(downstream[0])
        forwarding = self._forward_pool.submit(stub.WriteBlock, forwarded_chunks())

        def tee():
            try:
                for msg in request_iterator:
                    pending.put(msg.data)
                    yield msg.data
            finally:
                # el centinela va sí o sí: si la escritura local revienta, sin esto
                # el hilo de forwarding se queda colgado en pending.get() para siempre
                pending.put(None)

        try:
            checksum, bytes_written = block_store.write_block(self._root, block_id, tee())
        except BaseException:
            forwarding.cancel()
            raise

        try:
            downstream_response = forwarding.result()
        except grpc.RpcError as exc:
            # quórum 3 de 3: si una réplica del pipeline falla, falla la escritura entera
            _discard_local(self._root, block_id)
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"falló la réplica en {downstream[0]}: {exc.details()}",
            )
            return

        if downstream_response.checksum != checksum:
            _discard_local(self._root, block_id)
            context.abort(
                grpc.StatusCode.DATA_LOSS,
                f"checksum distinto entre réplicas del bloque {block_id}: "
                f"local {checksum}, {downstream[0]} {downstream_response.checksum}",
            )
            return

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
