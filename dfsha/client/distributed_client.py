from __future__ import annotations

import os
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


class DistributedDFShaClient:
    def __init__(self, control_node_address: str) -> None:
        self._control_channel = grpc.insecure_channel(control_node_address)
        self._control_stub = control_node_pb2_grpc.ControlNodeServiceStub(self._control_channel)
        self._datanode_channels: dict[str, grpc.Channel] = {}

    def close(self) -> None:
        self._control_channel.close()
        for channel in self._datanode_channels.values():
            channel.close()

    def _datanode_stub(self, address: str):
        if address not in self._datanode_channels:
            self._datanode_channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._datanode_channels[address])

    def list_dir(self, path: str):
        try:
            response = self._control_stub.ListDir(control_node_pb2.ListDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return list(response.entries)

    def make_dir(self, path: str) -> None:
        try:
            self._control_stub.MakeDir(control_node_pb2.MakeDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove_dir(self, path: str) -> None:
        try:
            self._control_stub.RemoveDir(control_node_pb2.RemoveDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove(self, path: str) -> None:
        try:
            self._control_stub.Remove(control_node_pb2.RemoveRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def upload(self, local_path: Path, remote_path: str) -> int:
        if not local_path.is_file():
            raise NotAFileError(f"no existe o no es un archivo: {local_path}")

        size_bytes = local_path.stat().st_size
        try:
            begin_response = self._control_stub.BeginUpload(
                control_node_pb2.BeginUploadRequest(path=remote_path, size_bytes=size_bytes)
            )
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

        total_written = 0
        try:
            with local_path.open("rb") as fh:
                for block in begin_response.blocks:
                    checksum, bytes_written = self._write_block(block, fh)
                    total_written += bytes_written
                    self._control_stub.ConfirmBlock(
                        control_node_pb2.ConfirmBlockRequest(
                            path=remote_path,
                            block_id=block.block_id,
                            checksum=checksum,
                            size_bytes=bytes_written,
                        )
                    )
        except (grpc.RpcError, OSError) as exc:
            try:
                self._control_stub.AbortUpload(control_node_pb2.AbortUploadRequest(path=remote_path))
            except grpc.RpcError:
                pass  # best-effort: no tapar la excepción original con la del abort
            if isinstance(exc, grpc.RpcError):
                raise _translate(exc) from exc
            raise

        try:
            self._control_stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path=remote_path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return total_written

    def download(self, remote_path: str, local_path: Path) -> int:
        try:
            list_response = self._control_stub.ListBlocks(
                control_node_pb2.ListBlocksRequest(path=remote_path)
            )
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

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
