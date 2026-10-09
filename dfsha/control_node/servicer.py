from __future__ import annotations

import math
import threading
import time
import uuid
from typing import Callable

import grpc
from pysyncobj import SyncObj, SyncObjException

from dfsha.common import exceptions
from dfsha.common.auth import (
    DEFAULT_TOKEN_TTL_S,
    Principal,
    hash_password,
    issue_token,
    verify_password,
    verify_token,
)
from dfsha.common.block_token import (
    DEFAULT_CAPABILITY_TTL_S,
    INTERNAL_TTL_S,
    capability_kwargs,
    issue_block,
    issue_internal,
)
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
from dfsha.control_node.garbage_collector import DEFAULT_GC_RPC_TIMEOUT_S
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.tree import Caller, plan_write_slots
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

# op_id fijo del alta del admin: aunque varios líderes sucesivos lo intenten, la
# deduplicación del log lo aplica una sola vez (D-P9).
BOOTSTRAP_ADMIN_OP_ID = "bootstrap-admin"
ADMIN_USERNAME = "admin"
MAX_PASSWORD_CHARS = 1024
# Los grupos viajan en el token, y el token en la metadata de cada llamada, que gRPC
# limita en tamaño. Con miles de grupos la cuenta quedaría inutilizable para siempre:
# no hay comando para borrar un usuario ni para cambiarle los grupos.
MAX_USER_GROUPS = 16

# Login de un usuario que no existe verifica igual contra esto: mismo scrypt, mismo
# tiempo de respuesta. Sin eso, una respuesta instantánea delata qué usuarios existen.
_DECOY_HASH, _DECOY_SALT = bytes(32), bytes(16)

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
        channel_factory: Callable[[str], grpc.Channel] = grpc.insecure_channel,
        jwt_secret: bytes | None = None,
        token_ttl_s: float = DEFAULT_TOKEN_TTL_S,
        capability_key: bytes | None = None,
        capability_ttl_s: float = DEFAULT_CAPABILITY_TTL_S,
    ) -> None:
        if not math.isfinite(token_ttl_s) or token_ttl_s <= 0:
            # nan e inf no son "<= 0": sin isfinite pasarían, y cada Login reventaría
            raise ValueError(f"token_ttl_s debe ser un número finito mayor que cero, no {token_ttl_s}")
        if not math.isfinite(capability_ttl_s) or capability_ttl_s <= 0:
            raise ValueError(f"capability_ttl_s debe ser un número finito mayor que cero, no {capability_ttl_s}")
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
        self._channel_factory = channel_factory
        # None = sin autenticación (desarrollo y tests): ningún RPC exige token.
        self._jwt_secret = jwt_secret
        self._token_ttl_s = token_ttl_s
        # C3. None = no emite capabilities: los campos `capability` de las respuestas van
        # vacíos y las llamadas a los DataNodes salen sin metadata.
        self._capability_key = capability_key
        self._capability_ttl_s = capability_ttl_s

    def close(self) -> None:
        for channel in self._channels.values():
            channel.close()

    def _datanode_stub(self, address: str):
        if address not in self._channels:
            self._channels[address] = self._channel_factory(address)
        return data_node_pb2_grpc.DataNodeServiceStub(self._channels[address])

    def _authenticate(self, context: grpc.ServicerContext) -> Principal | None:
        """Primera línea de TODO RPC salvo Login. Devuelve a quien llama, o None solo si
        este nodo arrancó sin secreto de JWT (desarrollo y tests).

        Va antes del chequeo de liderazgo y de la barrera: validar el token no toca el
        árbol y da lo mismo en los tres nodos, que comparten el secreto. Así una llamada
        sin token se rechaza en cualquier nodo, sin failover y sin escribir en Raft.
        tests/test_auth_servicer.py recorre el servicio y falla si un RPC nuevo se la saltea."""
        if self._jwt_secret is None:
            return None
        header = dict(context.invocation_metadata()).get("authorization", "")
        scheme, _, token = header.partition(" ")
        try:
            if scheme.lower() != "bearer" or not token.strip():
                raise AuthError("falta el token de sesión")
            return verify_token(self._jwt_secret, token.strip())
        except AuthError as exc:
            _abort_on_domain_error(context, exc)
            raise  # abort() ya lanzó; esto cubre un contexto de prueba que no lance

    @staticmethod
    def _caller(principal: Principal | None) -> Caller | None:
        """Quien llama, como viaja en el comando replicado (C3): una tupla plana, no el
        Principal, para que el journal no dependa de ninguna clase. None sin autenticación:
        el árbol no chequea nada. Toda mutación que llegue a un RPC lo recibe al final
        (tests/test_permissions_tree.py vigila las firmas)."""
        return None if principal is None else (principal.username, tuple(principal.groups), principal.is_admin)

    def _block_capability(self, block_id: str, op: str, ttl_s: float | None = None) -> str:
        """"" si este nodo no tiene clave de capabilities.

        Se firma al armar la respuesta, fuera de apply() y con la hora real: el DataNode
        compara el vencimiento contra su propio reloj. Sin ttl_s, la duración de las que
        recibe el cliente (--capability-ttl-s)."""
        if self._capability_key is None:
            return ""
        return issue_block(
            self._capability_key, block_id, op, self._capability_ttl_s if ttl_s is None else ttl_s, time.time()
        )

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
        # C3: el op_id se ata a quien llama. El log deduplica devolviendo el resultado
        # guardado SIN volver a ejecutar, y por lo tanto sin volver a mirar permisos: con la
        # clave cruda, repetir el op_id de otro usuario devolvía su resultado (los bloques de
        # su subida, con una capability de escritura recién firmada). Con el prefijo, el
        # op_id de otro usuario es otra operación y se ejecuta con sus propios permisos.
        # Un nombre de usuario no lleva ':' y las claves internas (bootstrap-admin, las del
        # re-replicador) no llevan prefijo, así que un cliente no puede fabricar una.
        # ponytail: verifica el token por segunda vez (microsegundos) en vez de pasar el
        # principal por los 19 llamadores; cambiar si alguna vez pesa.
        principal = self._authenticate(context)
        key = op_id if principal is None else f"{principal.username}:{op_id}"
        outcome = self._commit_raw(key, method, *args)
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
        principal = self._authenticate(context)
        self._read_barrier(context)
        try:
            # C3: el permiso se mira después de la barrera, bajo el lock del árbol
            entries = self._replicated.tree.list_dir(request.path, self._caller(principal))
        except (PathNotFoundError, NotADirectoryError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.ListDirResponse()
        return control_node_pb2.ListDirResponse(
            entries=[
                control_node_pb2.DirEntry(
                    name=e.name, is_dir=e.is_dir, size_bytes=e.size_bytes, owner=e.owner, group=e.group, mode=e.mode
                )
                for e in entries
            ]
        )

    def MakeDir(self, request, context):
        principal = self._authenticate(context)
        try:
            self._commit(context, request.op_id, "make_dir", request.path, self._caller(principal))
        except (PathExistsError, PathNotFoundError, NotADirectoryError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.MakeDirResponse()

    def RemoveDir(self, request, context):
        principal = self._authenticate(context)
        try:
            self._commit(context, request.op_id, "remove_dir", request.path, self._caller(principal))
        except (PathNotFoundError, NotADirectoryError, NotEmptyError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.RemoveDirResponse()

    def Remove(self, request, context):
        principal = self._authenticate(context)
        try:
            blocks = self._commit(
                context, request.op_id, "remove_file", request.path, time.time(), self._caller(principal)
            )
        except (
            PathNotFoundError,
            NotAFileError,
            NotADirectoryError,
            InvalidPathError,
            ConflictError,
            AccessDeniedError,
        ) as exc:
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
                        **capability_kwargs(self._block_capability(block.block_id, "delete", INTERNAL_TTL_S)),
                    )
                except grpc.RpcError:
                    # best-effort: la metadata ya se borró, un bloque físico que falle
                    # en limpiarse no debe tapar ni romper la operación
                    pass

    def BeginUpload(self, request, context):
        principal = self._authenticate(context)
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
                self._caller(principal),  # C3: va último, después de block_size
            )
        except (PathExistsError, NotADirectoryError, InvalidPathError, AccessDeniedError) as exc:
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
                    # C3: recién firmada también en un reintento del mismo op_id
                    capability=self._block_capability(block_id, "write"),
                )
            )
            remaining -= this_block_size
        return control_node_pb2.BeginUploadResponse(blocks=locations)

    def ConfirmBlock(self, request, context):
        principal = self._authenticate(context)
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
                self._caller(principal),
            )
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ConfirmBlockResponse()

    def CompleteUpload(self, request, context):
        principal = self._authenticate(context)
        try:
            self._commit(context, request.op_id, "complete_upload", request.path, self._caller(principal))
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.CompleteUploadResponse()

    def AbortUpload(self, request, context):
        principal = self._authenticate(context)
        # Camino rápido: se leen los bloques de la subida ANTES del commit (con la barrera,
        # sobre el árbol al día) y se borran si el abort sale bien. abort_upload no los
        # devuelve para no cambiar el resultado que guarda el log (ver tree.abort_upload).
        # Si la subida cambió entre la lectura y el commit, lo que quede lo recoge A3.
        self._read_barrier(context)
        blocks = self._replicated.tree.pending_blocks(request.path)
        try:
            # C3: si la subida es de otro usuario, el commit se rechaza y no se borra nada
            self._commit(context, request.op_id, "abort_upload", request.path, self._caller(principal))
        except (PathNotFoundError, NotADirectoryError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.AbortUploadResponse()
        self._delete_blocks(blocks)
        return control_node_pb2.AbortUploadResponse()

    def ListBlocks(self, request, context):
        principal = self._authenticate(context)
        self._read_barrier(context)
        try:
            # C3: sin `r` no hay ubicaciones; el permiso se mira después de la barrera
            blocks = self._replicated.tree.list_blocks(request.path, self._caller(principal))
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.ListBlocksResponse()
        return control_node_pb2.ListBlocksResponse(
            blocks=[
                control_node_pb2.BlockInfo(
                    block_id=b.block_id,
                    datanode_addresses=b.datanode_addresses,
                    checksum=b.checksum,
                    size_bytes=b.size_bytes,
                    capability=self._block_capability(b.block_id, "read"),
                )
                for b in blocks
            ]
        )

    def Lock(self, request, context):
        principal = self._authenticate(context)
        lock_id = uuid.uuid4().hex
        try:
            committed_lock_id = self._commit(
                context,
                request.op_id,
                "acquire_lock",
                request.path,
                lock_id,
                # con autenticación, el dueño es el usuario; sin ella, la conexión
                principal.username if principal else context.peer(),
                request.mode,
                time.time(),
                self._lock_lease_s,
                self._caller(principal),
            )
        except (PathNotFoundError, InvalidPathError, ConflictError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.LockResponse()
        return control_node_pb2.LockResponse(lock_id=committed_lock_id, lease_s=self._lock_lease_s)

    def RenewLock(self, request, context):
        self._authenticate(context)
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
        self._authenticate(context)
        try:
            self._commit(context, request.op_id, "release_lock", request.path, request.lock_id, time.time())
        except (PathNotFoundError, ConflictError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.UnlockResponse()

    # --- RF3 write: copy-on-write (B3) ---------------------------------------------------

    def BeginWrite(self, request, context):
        principal = self._authenticate(context)
        caller = self._caller(principal)
        # La propuesta se arma leyendo el árbol: tiene que estar al día (misma regla
        # que toda lectura). begin_write la vuelve a validar dentro de apply().
        self._read_barrier(context)
        try:
            # C3: write_layout exige `w` antes de proponer bloques, así el rechazo no
            # revela el tamaño del archivo; begin_write lo vuelve a exigir en apply().
            version, block_size, sizes = self._replicated.tree.write_layout(
                request.path, self._block_size_bytes, caller
            )
            plan = plan_write_slots(sizes, block_size, request.offset, request.length)
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
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
                caller,
            )
        except (PathNotFoundError, InvalidPathError, ConflictError, AccessDeniedError) as exc:
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
                    # ponytail: con `w` y sin `r`, la capability de lectura del bloque viejo
                    # igual sale: copy-on-write necesita ese contenido para completar el
                    # bloque nuevo. `w` sin `r` no oculta lo que la escritura toca.
                    old_capability=self._block_capability(old_block_id, "read") if old_block_id else "",
                    new_capability=self._block_capability(new_block_id, "write"),
                )
                for index, old_block_id, old_size, old_addresses, new_block_id, new_addresses, new_size in slots
            ],
        )

    def CommitWrite(self, request, context):
        principal = self._authenticate(context)
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
                self._caller(principal),
            )
        except (PathNotFoundError, InvalidPathError, ConflictError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.CommitWriteResponse()
        # Borrar la versión anterior es seguro recién ahora: el writer tiene el lock
        # exclusivo, así que no hay lectores usándola. Si falla o el líder cae antes,
        # quedan huérfanos para el recolector (A3).
        self._delete_blocks(replaced)
        return control_node_pb2.CommitWriteResponse(version=version)

    def AbortWrite(self, request, context):
        self._authenticate(context)
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

    # --- Usuarios y sesiones (C2) ------------------------------------------------------------

    def _auth_disabled(self, context: grpc.ServicerContext) -> None:
        context.abort(
            grpc.StatusCode.UNIMPLEMENTED,
            "este clúster no tiene autenticación: los ControlNodes arrancaron sin --jwt-secret-file",
        )

    @staticmethod
    def _check_new_password(password: str) -> None:
        if not password:
            raise InvalidPathError("la contraseña no puede estar vacía")
        if len(password) > MAX_PASSWORD_CHARS:
            raise InvalidPathError(f"la contraseña no puede pasar de {MAX_PASSWORD_CHARS} caracteres")

    def _verified_user(self, username: str, password: str):
        """El usuario si la contraseña coincide; None si no coincide o no existe.

        Compara fuera del lock del árbol (scrypt tarda ~20 ms). Con un usuario inexistente
        hace el mismo trabajo contra un señuelo, para tardar lo mismo."""
        user = self._replicated.tree.get_user(username)
        password_hash, salt = (user.password_hash, user.salt) if user else (_DECOY_HASH, _DECOY_SALT)
        matches = verify_password(password, password_hash, salt)
        return user if user is not None and matches else None

    def Login(self, request, context):
        # El único RPC sin _authenticate: es el que entrega el token.
        if self._jwt_secret is None:
            self._auth_disabled(context)
        self._read_barrier(context)
        user = self._verified_user(request.username, request.password)
        if user is None:
            # mismo mensaje para usuario inexistente y contraseña incorrecta
            _abort_on_domain_error(context, AuthError("usuario o contraseña incorrectos"))
            return control_node_pb2.LoginResponse()
        principal = Principal(request.username, user.groups, user.is_admin)
        return control_node_pb2.LoginResponse(
            token=issue_token(self._jwt_secret, principal, self._token_ttl_s, time.time()),
            expires_in_s=self._token_ttl_s,
            username=principal.username,
            groups=principal.groups,
            is_admin=principal.is_admin,
        )

    def CreateUser(self, request, context):
        principal = self._authenticate(context)
        if principal is None:
            self._auth_disabled(context)
        try:
            if not principal.is_admin:
                raise AccessDeniedError("solo un admin puede crear usuarios")
            self._check_new_password(request.password)
            if len(request.groups) > MAX_USER_GROUPS:
                raise InvalidPathError(f"un usuario no puede tener más de {MAX_USER_GROUPS} grupos")
            # Hash y sal se calculan acá, en el líder, y viajan en el comando: apply() no
            # genera azar. Un reintento con el mismo op_id calcula otra sal, pero el log
            # devuelve el resultado guardado y queda el primer hash.
            password_hash, salt = hash_password(request.password)
            self._commit(
                context,
                request.op_id,
                "create_user",
                request.username,
                password_hash,
                salt,
                list(request.groups) or [request.username],
                request.is_admin,
            )
        except (AccessDeniedError, InvalidPathError, PathExistsError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.CreateUserResponse()

    def ChangePassword(self, request, context):
        principal = self._authenticate(context)
        if principal is None:
            self._auth_disabled(context)
        target = request.username or principal.username
        try:
            if target != principal.username:
                if not principal.is_admin:
                    raise AccessDeniedError("solo un admin puede cambiar la contraseña de otro usuario")
            else:
                # La propia exige la actual: sin esto, un token robado (que dura hasta que
                # vence) alcanzaría para quedarse con la cuenta. Protege a un usuario común;
                # un token de admin robado igual puede crear otro admin o cambiarle la
                # contraseña a otro usuario: es el costo de no tener revocación.
                # AccessDeniedError y no
                # AuthError: el token es válido, y AuthError le haría a la shell pedir login.
                # ponytail: se verifica antes del commit, así que un reintento del mismo
                # op_id ya aplicado responde "contraseña actual incorrecta" aunque el cambio
                # se hizo. Si molesta, mirar applied_ops tras la barrera antes de verificar.
                self._read_barrier(context)
                if self._verified_user(target, request.current_password) is None:
                    raise AccessDeniedError("contraseña actual incorrecta")
            self._check_new_password(request.new_password)
            password_hash, salt = hash_password(request.new_password)
            self._commit(context, request.op_id, "change_password", target, password_hash, salt)
        except (AccessDeniedError, InvalidPathError, PathNotFoundError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ChangePasswordResponse()

    def ensure_admin(self, password_hash: bytes, salt: bytes) -> bool:
        """Crea al usuario admin si el clúster todavía no tiene usuarios. Devuelve True
        cuando ya no queda nada por hacer. Lo llama el hilo de arranque de serve().

        Mira el árbol local sin barrera a propósito: no es un RPC, y como no se borran
        usuarios, una lectura atrasada solo cuesta un intento de más, que el op_id fijo y
        el rechazo de duplicados de create_user vuelven inofensivo."""
        if self._replicated.tree.has_users():
            return True
        if not self._is_leader_raw():
            return False
        outcome = self._commit_raw(
            BOOTSTRAP_ADMIN_OP_ID, "create_user", ADMIN_USERNAME, password_hash, salt, [ADMIN_USERNAME], True
        )
        return outcome[0] in {"ok", "error"}

    # --- Permisos (C3) -----------------------------------------------------------------------
    #
    # Mutaciones: el árbol decide quién puede dentro de apply(), con quien llama como último
    # argumento del comando. Sin autenticación (caller None) no hay chequeo.

    def Chmod(self, request, context):
        principal = self._authenticate(context)
        try:
            self._commit(context, request.op_id, "chmod", request.path, request.mode, self._caller(principal))
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ChmodResponse()

    def Chown(self, request, context):
        principal = self._authenticate(context)
        try:
            self._commit(
                context, request.op_id, "chown", request.path, request.owner, request.group, self._caller(principal)
            )
        except (PathNotFoundError, InvalidPathError, AccessDeniedError) as exc:
            _abort_on_domain_error(context, exc)
        return control_node_pb2.ChownResponse()

    # --- Inventario de un DataNode (C3) ------------------------------------------------------

    def DataNodeInventory(self, request, context):
        """Inventario de un DataNode para `inspect huerfanos`, solo para admin.

        El ControlNode hace de proxy con su capability interna de `list`, que así nunca sale
        de los nodos. Solo acepta direcciones de --datanode-addresses: un admin no puede
        hacer que el ControlNode le presente su capability a un host cualquiera. No lee el
        árbol, así que no pasa por la barrera ni exige liderazgo."""
        principal = self._authenticate(context)
        try:
            if principal is not None and not principal.is_admin:
                raise AccessDeniedError("solo un admin puede ver el inventario de un DataNode")
            if request.address not in self._datanode_addresses:
                raise InvalidPathError(f"DataNode desconocido: {request.address!r}")
        except (AccessDeniedError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
            return control_node_pb2.DataNodeInventoryResponse()
        kwargs = {}
        if self._capability_key is not None:
            kwargs = capability_kwargs(issue_internal(self._capability_key, "list", INTERNAL_TTL_S, time.time()))
        # la llamada al DataNode usa lo que le queda a la del inspector; sin deadline, el del recolector
        remaining = context.time_remaining()
        timeout = DEFAULT_GC_RPC_TIMEOUT_S if remaining is None else remaining
        try:
            # ponytail: el inventario entero va en un solo mensaje unario, y gRPC limita el
            # tamaño de un mensaje (4 MiB por defecto según su documentación; no lo medí).
            # Sobra para este proyecto; paginar si llega a hacer falta.
            stored = [
                control_node_pb2.StoredBlockInfo(block_id=b.block_id, size_bytes=b.size_bytes, age_s=b.age_s)
                for b in self._datanode_stub(request.address).ListStoredBlocks(
                    data_node_pb2.ListStoredBlocksRequest(), timeout=timeout, **kwargs
                )
            ]
        except grpc.RpcError as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"no se pudo listar {request.address}: {exc.details()}")
            return control_node_pb2.DataNodeInventoryResponse()
        return control_node_pb2.DataNodeInventoryResponse(blocks=stored)
