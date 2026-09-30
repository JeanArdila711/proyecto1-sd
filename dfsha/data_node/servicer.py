from __future__ import annotations

import contextlib
import queue
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.common.exceptions import (
    AccessDeniedError,
    AuthError,
    BlockCorruptedError,
    BlockNotFoundError,
    ConflictError,
)
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
    ConflictError: grpc.StatusCode.ABORTED,
    AccessDeniedError: grpc.StatusCode.PERMISSION_DENIED,
    AuthError: grpc.StatusCode.UNAUTHENTICATED,
}


def _discard_local(root: Path, block_id: str) -> None:
    """La escritura fracasó aguas abajo: la copia local no la conoce nadie, se tira."""
    with contextlib.suppress(BlockNotFoundError):
        block_store.delete_block(root, block_id)


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.set_trailing_metadata((("dfsha-error", type(exc).__name__),))
    context.abort(status_code, str(exc))


class DataNodeServicer(data_node_pb2_grpc.DataNodeServiceServicer):
    def __init__(self, root: Path, encryption_key: bytes) -> None:
        self._root = root
        self._encryption_key = encryption_key
        # Executor propio para el forwarding del pipeline: si compartiera el del
        # servidor gRPC, N escrituras concurrentes podrían quedarse sin worker
        # para reenviar y el pipeline se auto-bloquearía.
        self._forward_pool = futures.ThreadPoolExecutor(max_workers=10)
        self._channels: dict[str, grpc.Channel] = {}

    def _peer_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._channels[address])

    def close(self) -> None:
        for channel in self._channels.values():
            channel.close()
        self._forward_pool.shutdown(wait=True, cancel_futures=True)

    def Ping(self, request, context):
        return data_node_pb2.PingResponse()

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
        try:
            # Validar antes de crear el forwarding evita dejar un worker esperando
            # si el identificador que terminaría en disco no es aceptable.
            block_store.validate_block_id(block_id)
        except BlockNotFoundError as exc:
            _abort_on_domain_error(context, exc)
            return data_node_pb2.WriteBlockResponse()

        if not downstream:
            checksum, bytes_written = block_store.write_block(
                self._root, self._encryption_key, block_id, (msg.data for msg in request_iterator)
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
        # El deadline entrante cubre la transferencia completa; al siguiente salto
        # solo le queda ese presupuesto, nunca el timeout corto de un RPC de control.
        forwarding = self._forward_pool.submit(
            stub.WriteBlock, forwarded_chunks(), timeout=context.time_remaining()
        )

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
            checksum, bytes_written = block_store.write_block(
                self._root, self._encryption_key, block_id, tee()
            )
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
        # offset y length llegan de la red: se validan acá, antes de tocar el disco.
        if request.offset < 0 or request.length < 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"rango inválido: offset={request.offset}, length={request.length}",
            )
        length = request.length or None  # 0 = hasta el final del bloque
        try:
            for chunk in block_store.read_block(
                self._root,
                self._encryption_key,
                request.block_id,
                CHUNK_SIZE_BYTES,
                request.offset,
                length,
            ):
                yield data_node_pb2.ReadBlockChunk(data=chunk)
        except (BlockNotFoundError, BlockCorruptedError) as exc:
            _abort_on_domain_error(context, exc)

    def DeleteBlock(self, request, context):
        try:
            block_store.delete_block(self._root, request.block_id)
        except BlockNotFoundError as exc:
            _abort_on_domain_error(context, exc)
        return data_node_pb2.DeleteBlockResponse()

    def ReplicateBlock(self, request, context):
        """Copia un bloque verificado hacia un único destino interno."""
        try:
            block_store.validate_block_id(request.block_id)
            local_chunks = block_store.read_block(
                self._root, self._encryption_key, request.block_id, CHUNK_SIZE_BYTES
            )
            # read_block es un generador: verifica existencia y metadata autenticada en
            # el primer next(). Hay que forzarlo ACÁ, antes de abrir el stream al destino.
            # Si no, un origen corrupto o sin el bloque responde UNAVAILABLE
            # ("Exception iterating requests!"), como si el nodo estuviera caído, y el
            # re-replicador insiste con el mismo origen.
            first_chunk = next(local_chunks, None)

            def chunks():
                yield data_node_pb2.WriteBlockChunk(
                    header=data_node_pb2.WriteBlockHeader(block_id=request.block_id)
                )
                if first_chunk is not None:
                    yield data_node_pb2.WriteBlockChunk(data=first_chunk)
                for chunk in local_chunks:
                    yield data_node_pb2.WriteBlockChunk(data=chunk)

            # C3 agregará aquí la capability administrativa y la capability del
            # bloque destinada a ``request.target``.
            response = self._peer_stub(request.target).WriteBlock(
                chunks(), timeout=context.time_remaining()
            )
        except (BlockNotFoundError, BlockCorruptedError) as exc:
            _abort_on_domain_error(context, exc)
            return data_node_pb2.ReplicateBlockResponse()
        except grpc.RpcError as exc:
            context.abort(
                exc.code()
                if exc.code() in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
                else grpc.StatusCode.UNAVAILABLE,
                f"no se pudo replicar hacia {request.target}: {exc.details()}",
            )
            return data_node_pb2.ReplicateBlockResponse()
        return data_node_pb2.ReplicateBlockResponse(
            checksum=response.checksum, bytes_written=response.bytes_written
        )

    def ListStoredBlocks(self, request, context):
        # C3 agregará acá la capability interna: solo el ControlNode puede pedir el inventario.
        for block_id, size_bytes, age_s in block_store.list_blocks(self._root, self._encryption_key):
            yield data_node_pb2.StoredBlock(block_id=block_id, size_bytes=size_bytes, age_s=age_s)
