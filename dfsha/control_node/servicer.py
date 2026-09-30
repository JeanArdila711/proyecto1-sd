from __future__ import annotations

import threading
import time
import uuid

import grpc
from pysyncobj import SyncObj, SyncObjException

from dfsha.common import exceptions
from dfsha.common.exceptions import (
    AccessDeniedError,
    AuthError,
    ConflictError,
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.control_node.datanode_monitor import DataNodeMonitor
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.tree import plan_write_slots
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc

DEFAULT_REPLICATION_FACTOR = 3
DEFAULT_MIN_WRITE_REPLICAS = 2

# Cuánto espera el líder a que una mutación quede confirmada por la mayoría. Tiene
# que ser menor que el timeout por intento del cliente, para que el servidor alcance
# a responder UNAVAILABLE antes de que el cliente corte por deadline.
DEFAULT_COMMIT_TIMEOUT_S = 3.0

# Cuánto puede estar una subida sin confirmar un bloque antes de que su nombre quede
# libre para otro BeginUpload. Tiene que cubrir la escritura de UN bloque completo
# (128 MB por un enlace lento) más un failover de líder; se renueva con cada ConfirmBlock.
DEFAULT_UPLOAD_LEASE_S = 600.0
DEFAULT_DATA_PLANE_TIMEOUT_S = 5.0

# Un lock se renueva en lease/3; se inyecta en tests cortos y no se calcula dentro
# de apply(), donde cada réplica podría observar una hora distinta.
DEFAULT_LOCK_LEASE_S = 30.0

_ERROR_STATUS_MAP = {
    PathNotFoundError: grpc.StatusCode.NOT_FOUND,
    PathExistsError: grpc.StatusCode.ALREADY_EXISTS,
    NotEmptyError: grpc.StatusCode.FAILED_PRECONDITION,
    InvalidPathError: grpc.StatusCode.PERMISSION_DENIED,
    NotAFileError: grpc.StatusCode.INVALID_ARGUMENT,
    NotADirectoryError: grpc.StatusCode.INVALID_ARGUMENT,
    ConflictError: grpc.StatusCode.ABORTED,
    AccessDeniedError: grpc.StatusCode.PERMISSION_DENIED,
    AuthError: grpc.StatusCode.UNAUTHENTICATED,
}


class InsufficientLiveDataNodes(Exception):
    def __init__(self, available: int, minimum: int) -> None:
        super().__init__(f"solo hay {available} DataNodes vivos; se requieren al menos {minimum}")


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.set_trailing_metadata((("dfsha-error", type(exc).__name__),))
    context.abort(status_code, str(exc))


class ControlNodeServicer(control_node_pb2_grpc.ControlNodeServiceServicer):
    def __init__(
        self,
        raft: SyncObj,
        replicated: ReplicatedTree,
        datanode_addresses: list[str],
        block_size_bytes: int,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
        commit_timeout_s: float = DEFAULT_COMMIT_TIMEOUT_S,
        upload_lease_s: float = DEFAULT_UPLOAD_LEASE_S,
        min_write_replicas: int = DEFAULT_MIN_WRITE_REPLICAS,
        datanode_monitor: DataNodeMonitor | None = None,
        lock_lease_s: float = DEFAULT_LOCK_LEASE_S,
    ) -> None:
        if not datanode_addresses:
            raise ValueError("hace falta al menos un DataNode")
        if replication_factor < 1:
            raise ValueError(f"el factor de replicación debe ser >= 1, no {replication_factor}")
        if not 1 <= min_write_replicas <= replication_factor:
            raise ValueError(
                "min_write_replicas debe estar entre 1 y replication_factor "
                f"({replication_factor}), no {min_write_replicas}"
            )
        if len(datanode_addresses) < min_write_replicas:
            raise ValueError(
                f"se configuraron {len(datanode_addresses)} DataNodes, menos que min_write_replicas={min_write_replicas}"
            )
        self._raft = raft
        # nunca guardar replicated.tree: un snapshot restaurado lo reemplaza entero
        self._replicated = replicated
        self._commit_timeout_s = commit_timeout_s
        self._upload_lease_s = upload_lease_s
        self._lock_lease_s = lock_lease_s
        self._datanode_addresses = list(datanode_addresses)
        self._block_size_bytes = block_size_bytes
        self._replication_factor = replication_factor
        self._min_write_replicas = min_write_replicas
        self._monitor = datanode_monitor
        self._next_offset = 0
        self._offset_lock = threading.Lock()
        self._channels: dict[str, grpc.Channel] = {}

    def close(self) -> None:
        for channel in self._channels.values():
            channel.close()

    def _datanode_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = grpc.insecure_channel(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._channels[address])

    def _require_leader(self, context: grpc.ServicerContext) -> None:
        # Lecturas y escrituras solo en el líder: un follower puede tener el log
        # atrasado. UNAVAILABLE es la señal para que el cliente pruebe otro nodo.
        if not self._raft._isLeader():
            context.abort(grpc.StatusCode.UNAVAILABLE, "este ControlNode no es el líder")

    def _is_leader_raw(self) -> bool:
        """Consulta de liderazgo utilizable por hilos internos sin contexto gRPC."""
        return self._raft._isLeader()

    def _read_barrier_raw(self) -> bool:
        """Barrera linealizable para trabajo interno del líder, sin ServicerContext."""
        if not self._is_leader_raw():
            return False
        try:
            self._replicated.read_barrier(sync=True, timeout=self._commit_timeout_s)
        except SyncObjException:
            return False
        return True

    def _read_barrier(self, context: grpc.ServicerContext) -> None:
        """Antes de leer el árbol local: confirmar por Raft que este nodo sigue
        siendo líder y que ya aplicó todo lo confirmado (ver ReplicatedTree.read_barrier)."""
        self._require_leader(context)
        if not self._read_barrier_raw():
            context.abort(grpc.StatusCode.UNAVAILABLE, "no se pudo confirmar el liderazgo para leer")

    def _commit_raw(self, op_id: str, method: str, *args) -> tuple:
        """Commit interno estructurado, sin ``grpc.ServicerContext``.

        ``unknown`` conserva el op_id para que el re-replicador pueda reintentar un
        resultado que pudo haberse confirmado después del timeout.
        """
        if not self._is_leader_raw():
            return ("unavailable", "este ControlNode no es el líder")
        try:
            return self._replicated.apply(
                op_id, method, args, sync=True, timeout=self._commit_timeout_s
            )
        except SyncObjException as exc:
            return ("unknown", str(exc))

    def _commit(self, context: grpc.ServicerContext, op_id: str, method: str, *args):
        """Replica una mutación por Raft y devuelve su resultado, o lanza la misma
        excepción de dominio que lanzaría ControlTree."""
        if not op_id:
            # un op_id vacío deduplicaría TODAS las requests entre sí: el segundo
            # mkdir recibiría la respuesta del primero. Se rechaza, nunca se inventa.
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "falta op_id")
        self._require_leader(context)
        outcome = self._commit_raw(op_id, method, *args)
        if outcome[0] in {"unknown", "unavailable"}:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"no se pudo confirmar en el clúster ({outcome[1]}); reintentar con el mismo op_id",
            )
        if outcome[0] == "error":
            _, class_name, message = outcome
            raise getattr(exceptions, class_name, exceptions.DFShaError)(message)
        return outcome[1]

    def _pick_replicas(self) -> list[str]:
        """Round-robin entre los DataNodes vivos: cada bloque arranca un nodo más
        adelante que el anterior, así los bloques de un archivo se reparten en vez
        de apilarse en los mismos nodos.

        Es estado local del líder, no replicado: tras un failover el offset vuelve a
        0, lo que solo cambia el reparto, no la corrección."""
        alive = self._monitor.alive_addresses() if self._monitor is not None else self._datanode_addresses
        if len(alive) < self._min_write_replicas:
            raise InsufficientLiveDataNodes(len(alive), self._min_write_replicas)
        count = min(self._replication_factor, len(alive))
        with self._offset_lock:
            start = self._next_offset
            self._next_offset = (start + 1) % len(alive)
        return [alive[(start + i) % len(alive)] for i in range(count)]

    def ListDir(self, request, context):
        self._read_barrier(context)
        try:
            entries = self._replicated.tree.list_dir(request.path)
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
            self._commit(context, request.op_id, "make_dir", request.path)
        except (PathExistsError, PathNotFoundError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.MakeDirResponse()

    def RemoveDir(self, request, context):
        try:
            self._commit(context, request.op_id, "remove_dir", request.path)
        except (PathNotFoundError, NotADirectoryError, NotEmptyError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.RemoveDirResponse()

    def Remove(self, request, context):
        try:
            blocks = self._commit(context, request.op_id, "remove_file", request.path, time.time())
        except (PathNotFoundError, NotAFileError, NotADirectoryError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.RemoveResponse()
        # Efecto secundario fuera de la máquina de estados, solo en el líder y
        # después del commit. Dentro de apply() correría en los 3 nodos y de nuevo
        # en cada replay del journal.
        self._delete_blocks(blocks)
        return control_node_pb2.RemoveResponse()

    def _delete_blocks(self, blocks) -> None:
        for block in blocks:
            for address in block.datanode_addresses:
                try:
                    self._datanode_stub(address).DeleteBlock(
                        data_node_pb2.DeleteBlockRequest(block_id=block.block_id),
                        timeout=DEFAULT_DATA_PLANE_TIMEOUT_S,
                    )
                except grpc.RpcError:
                    # best-effort: la metadata ya se borró, un bloque físico que falle
                    # en limpiarse no debe tapar ni romper la operación
                    pass

    def BeginUpload(self, request, context):
        if request.size_bytes <= 0:
            _abort_on_domain_error(context, InvalidPathError("size_bytes debe ser mayor a 0"))
            return control_node_pb2.BeginUploadResponse()

        num_blocks = -(-request.size_bytes // self._block_size_bytes)  # división hacia arriba
        try:
            proposed = [(uuid.uuid4().hex, self._pick_replicas()) for _ in range(num_blocks)]
        except InsufficientLiveDataNodes as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
            return control_node_pb2.BeginUploadResponse()
        try:
            # La respuesta se arma con lo que DEVUELVE el commit, no con `proposed`: si
            # este op_id ya se había confirmado (reintento tras un failover), el
            # resultado guardado trae los block_id originales.
            placements, stale_blocks = self._commit(
                context,
                request.op_id,
                "begin_upload",
                request.path,
                proposed,
                time.time(),  # lo decide el líder y viaja en el log: apply() no lee el reloj
                self._upload_lease_s,
                self._block_size_bytes,  # B3: el archivo recuerda con qué tamaño se partió
            )
        except (PathExistsError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.BeginUploadResponse()
        # bloques de una subida abandonada cuyo nombre se acaba de reutilizar
        self._delete_blocks(stale_blocks)

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
            self._commit(
                context,
                request.op_id,
                "confirm_block",
                request.path,
                request.block_id,
                request.checksum,
                request.size_bytes,
                time.time(),
                self._upload_lease_s,
            )
        except (PathNotFoundError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ConfirmBlockResponse()

    def CompleteUpload(self, request, context):
        try:
            self._commit(context, request.op_id, "complete_upload", request.path)
        except (PathNotFoundError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.CompleteUploadResponse()

    def AbortUpload(self, request, context):
        try:
            self._commit(context, request.op_id, "abort_upload", request.path)
        except (PathNotFoundError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.AbortUploadResponse()

    def ListBlocks(self, request, context):
        self._read_barrier(context)
        try:
            blocks = self._replicated.tree.list_blocks(request.path)
        except (PathNotFoundError, InvalidPathError) as exc:
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

    def Lock(self, request, context):
        lock_id = uuid.uuid4().hex
        try:
            committed_lock_id = self._commit(
                context,
                request.op_id,
                "acquire_lock",
                request.path,
                lock_id,
                context.peer(),
                request.mode,
                time.time(),
                self._lock_lease_s,
            )
        except (PathNotFoundError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.LockResponse()
        return control_node_pb2.LockResponse(lock_id=committed_lock_id, lease_s=self._lock_lease_s)

    def RenewLock(self, request, context):
        try:
            self._commit(
                context,
                request.op_id,
                "renew_lock",
                request.path,
                request.lock_id,
                time.time(),
                self._lock_lease_s,
            )
        except (PathNotFoundError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.RenewLockResponse()

    def Unlock(self, request, context):
        try:
            self._commit(context, request.op_id, "release_lock", request.path, request.lock_id, time.time())
        except (PathNotFoundError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.UnlockResponse()

    # --- RF3 write: copy-on-write (B3) ---------------------------------------------------

    def BeginWrite(self, request, context):
        # La propuesta se arma leyendo el árbol: tiene que estar al día (misma regla
        # que toda lectura). begin_write la vuelve a validar dentro de apply().
        self._read_barrier(context)
        try:
            version, block_size, sizes = self._replicated.tree.write_layout(request.path, self._block_size_bytes)
            plan = plan_write_slots(sizes, block_size, request.offset, request.length)
        except (PathNotFoundError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.BeginWriteResponse()
        try:
            proposals = [(index, uuid.uuid4().hex, self._pick_replicas()) for index, _ in plan]
        except InsufficientLiveDataNodes as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
            return control_node_pb2.BeginWriteResponse()
        try:
            # Igual que BeginUpload: la respuesta sale de lo que DEVUELVE el commit. Un
            # reintento con el mismo op_id recibe la reserva original, no la propuesta nueva.
            write_id, base_version, block_size, slots = self._commit(
                context,
                request.op_id,
                "begin_write",
                request.path,
                uuid.uuid4().hex,
                request.lock_id,
                version,
                request.offset,
                request.length,
                proposals,
                time.time(),
                # ponytail: la reserva vive un lease de subida y no se renueva; una
                # escritura que tarde más que --upload-lease-s se rechaza en el commit.
                # Renovar por bloque si llega a hacer falta.
                self._upload_lease_s,
                self._block_size_bytes,
            )
        except (PathNotFoundError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.BeginWriteResponse()
        return control_node_pb2.BeginWriteResponse(
            write_id=write_id,
            base_version=base_version,
            block_size=block_size,
            lease_s=self._upload_lease_s,
            slots=[
                control_node_pb2.WriteSlot(
                    index=index,
                    old_block_id=old_block_id,
                    old_size=old_size,
                    old_addresses=old_addresses,
                    new_block_id=new_block_id,
                    new_addresses=new_addresses,
                    new_size=new_size,
                )
                for index, old_block_id, old_size, old_addresses, new_block_id, new_addresses, new_size in slots
            ],
        )

    def CommitWrite(self, request, context):
        try:
            version, replaced = self._commit(
                context,
                request.op_id,
                "commit_write",
                request.path,
                request.write_id,
                request.lock_id,
                request.base_version,
                [(s.index, s.block_id, s.checksum, s.size_bytes) for s in request.slots],
                time.time(),
            )
        except (PathNotFoundError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.CommitWriteResponse()
        # Borrar la versión anterior es seguro recién ahora: el writer tiene el lock
        # exclusivo, así que no hay lectores usándola. Si falla o el líder cae antes,
        # quedan huérfanos para el recolector (A3).
        self._delete_blocks(replaced)
        return control_node_pb2.CommitWriteResponse(version=version)

    def AbortWrite(self, request, context):
        try:
            reserved = self._commit(
                context,
                request.op_id,
                "abort_write",
                request.path,
                request.write_id,
                request.lock_id,
                time.time(),
            )
        except (PathNotFoundError, InvalidPathError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.AbortWriteResponse()
        self._delete_blocks(reserved)
        return control_node_pb2.AbortWriteResponse()
