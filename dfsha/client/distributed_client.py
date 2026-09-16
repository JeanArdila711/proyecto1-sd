from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

import grpc

from dfsha.common.exceptions import (
    BlockCorruptedError,
    DFShaError,
    InvalidPathError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

# Timeout por intento contra un ControlNode. Sin él, un nodo aislado de la red
# cuelga la llamada para siempre. Tiene que ser mayor que el timeout de commit del
# servidor (DEFAULT_COMMIT_TIMEOUT_S), para que el servidor alcance a responder.
DEFAULT_RPC_TIMEOUT_S = 5.0
# Cuánto tiempo total se sigue reintentando mientras el clúster elige un líder nuevo.
# Una elección tarda entre 0.4 y 1.4 s con los defaults de Raft; esto deja margen.
DEFAULT_FAILOVER_BUDGET_S = 15.0
_RETRY_BACKOFF_S = 0.2

# "Este nodo no puede atender ahora, probá otro": un follower (UNAVAILABLE), un nodo
# caído (UNAVAILABLE) o uno que no respondió a tiempo (DEADLINE_EXCEEDED). Reintentar
# una mutación es seguro solo porque la request lleva op_id.
_RETRYABLE_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})

# El código NOT_FOUND lo puede mandar tanto el ControlNode (ruta que no
# existe) como el DataNode (bloque que no existe) — con un solo mapa no se
# distinguen los dos casos, se pierde el subtipo exacto pero el mensaje de
# texto de la excepción sigue siendo el correcto. Mismo trade-off aceptado
# que INVALID_ARGUMENT en el cliente de Hito 1.
_STATUS_ERROR_MAP = {
    grpc.StatusCode.NOT_FOUND: PathNotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: PathExistsError,
    grpc.StatusCode.FAILED_PRECONDITION: NotEmptyError,
    grpc.StatusCode.PERMISSION_DENIED: InvalidPathError,
    grpc.StatusCode.INVALID_ARGUMENT: NotAFileError,
    grpc.StatusCode.DATA_LOSS: BlockCorruptedError,
}


def _translate(rpc_error: grpc.RpcError) -> DFShaError:
    exc_cls = _STATUS_ERROR_MAP.get(rpc_error.code(), DFShaError)
    return exc_cls(rpc_error.details())


def _new_op_id() -> str:
    return uuid.uuid4().hex


class DistributedDFShaClient:
    def __init__(
        self,
        control_node_addresses: list[str],
        rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
        failover_budget_s: float = DEFAULT_FAILOVER_BUDGET_S,
    ) -> None:
        if isinstance(control_node_addresses, str):
            # un str se iteraría letra por letra como si fueran direcciones
            raise TypeError("control_node_addresses debe ser una lista de host:port")
        if not control_node_addresses:
            raise ValueError("hace falta al menos un ControlNode")
        self._control_addresses = list(control_node_addresses)
        self._control_channels = {a: grpc.insecure_channel(a) for a in self._control_addresses}
        self._leader_index = 0  # último nodo que respondió: el líder más probable
        self._rpc_timeout_s = rpc_timeout_s
        self._failover_budget_s = failover_budget_s
        self._datanode_channels: dict[str, grpc.Channel] = {}

    def close(self) -> None:
        for channel in self._control_channels.values():
            channel.close()
        for channel in self._datanode_channels.values():
            channel.close()

    def _call(self, rpc_name: str, request):
        """Llama un RPC del ControlNode siguiendo al líder.

        Reintenta con el MISMO objeto request, así que el op_id de una mutación es
        idéntico en todos los intentos: no hay forma de regenerarlo por error."""
        deadline = time.monotonic() + self._failover_budget_s
        total = len(self._control_addresses)
        last_error: grpc.RpcError | None = None
        while True:
            for offset in range(total):
                index = (self._leader_index + offset) % total
                channel = self._control_channels[self._control_addresses[index]]
                stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)
                try:
                    response = getattr(stub, rpc_name)(request, timeout=self._rpc_timeout_s)
                except grpc.RpcError as exc:
                    if exc.code() not in _RETRYABLE_CODES:
                        raise _translate(exc) from exc
                    last_error = exc
                    continue
                self._leader_index = index
                return response
            # ponytail: el presupuesto se revisa por vuelta completa; con nodos que
            # cuelgan hasta el timeout, una vuelta puede pasarse por hasta
            # total * rpc_timeout_s. Suficiente para un clúster de 3.
            if time.monotonic() >= deadline:
                raise _translate(last_error) from last_error
            time.sleep(_RETRY_BACKOFF_S)

    def _datanode_stub(self, address: str):
        if address not in self._datanode_channels:
            self._datanode_channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._datanode_channels[address])

    def list_dir(self, path: str):
        response = self._call("ListDir", control_node_pb2.ListDirRequest(path=path))
        return list(response.entries)

    def make_dir(self, path: str) -> None:
        self._call("MakeDir", control_node_pb2.MakeDirRequest(path=path, op_id=_new_op_id()))

    def remove_dir(self, path: str) -> None:
        self._call("RemoveDir", control_node_pb2.RemoveDirRequest(path=path, op_id=_new_op_id()))

    def remove(self, path: str) -> None:
        self._call("Remove", control_node_pb2.RemoveRequest(path=path, op_id=_new_op_id()))

    def upload(self, local_path: Path, remote_path: str) -> int:
        if not local_path.is_file():
            raise NotAFileError(f"no existe o no es un archivo: {local_path}")

        size_bytes = local_path.stat().st_size
        begin_response = self._call(
            "BeginUpload",
            control_node_pb2.BeginUploadRequest(
                path=remote_path, size_bytes=size_bytes, op_id=_new_op_id()
            ),
        )

        total_written = 0
        try:
            with local_path.open("rb") as fh:
                for block in begin_response.blocks:
                    checksum, bytes_written = self._write_block(block, fh)
                    total_written += bytes_written
                    self._call(
                        "ConfirmBlock",
                        control_node_pb2.ConfirmBlockRequest(
                            path=remote_path,
                            block_id=block.block_id,
                            checksum=checksum,
                            size_bytes=bytes_written,
                            op_id=_new_op_id(),
                        ),
                    )
        except (grpc.RpcError, OSError, DFShaError) as exc:
            try:
                self._call(
                    "AbortUpload",
                    control_node_pb2.AbortUploadRequest(path=remote_path, op_id=_new_op_id()),
                )
            except DFShaError:
                pass  # best-effort: no tapar la excepción original con la del abort
            if isinstance(exc, grpc.RpcError):
                raise _translate(exc) from exc
            raise

        self._call(
            "CompleteUpload",
            control_node_pb2.CompleteUploadRequest(path=remote_path, op_id=_new_op_id()),
        )
        return total_written

    def download(self, remote_path: str, local_path: Path) -> int:
        list_response = self._call("ListBlocks", control_node_pb2.ListBlocksRequest(path=remote_path))

        tmp_path = local_path.parent / f"{local_path.name}.part-{uuid.uuid4().hex}"
        bytes_written = 0
        try:
            with tmp_path.open("wb") as fh:
                for block in list_response.blocks:
                    bytes_written += self._read_block_with_failover(block, fh)
            os.replace(tmp_path, local_path)
        except grpc.RpcError as exc:
            tmp_path.unlink(missing_ok=True)
            raise _translate(exc) from exc
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        return bytes_written

    def _read_block_with_failover(self, block, fh) -> int:
        """Prueba cada réplica en orden. Una réplica caída (UNAVAILABLE) o podrida
        (DATA_LOSS) hace caer a la siguiente; solo falla si fallan todas."""
        start = fh.tell()
        last_error: grpc.RpcError | None = None
        for address in block.datanode_addresses:
            try:
                request = data_node_pb2.ReadBlockRequest(block_id=block.block_id)
                for chunk in self._datanode_stub(address).ReadBlock(request):
                    fh.write(chunk.data)
                return fh.tell() - start
            except grpc.RpcError as exc:
                # la réplica pudo haber fallado a mitad de bloque, con bytes ya
                # escritos: sin descartarlos el archivo final queda corrupto en silencio
                fh.seek(start)
                fh.truncate()
                last_error = exc
        if last_error is None:
            raise PathNotFoundError(f"el bloque {block.block_id} no tiene réplicas registradas")
        raise last_error

    def _write_block(self, block, fh) -> tuple[str, int]:
        remaining = block.size_bytes

        def chunks():
            nonlocal remaining
            yield data_node_pb2.WriteBlockChunk(
                header=data_node_pb2.WriteBlockHeader(
                    block_id=block.block_id,
                    downstream=block.datanode_addresses[1:],
                )
            )
            while remaining > 0:
                data = fh.read(min(CHUNK_SIZE_BYTES, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data_node_pb2.WriteBlockChunk(data=data)

        # el cliente sube una sola copia, al primero del pipeline; los DataNodes
        # se encargan de encadenar las réplicas restantes
        stub = self._datanode_stub(block.datanode_addresses[0])
        response = stub.WriteBlock(chunks())
        return response.checksum, response.bytes_written
