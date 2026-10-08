from __future__ import annotations

import contextlib
import io
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from types import SimpleNamespace

import grpc

from dfsha.common.exceptions import (
    AccessDeniedError,
    AuthError,
    BlockCorruptedError,
    BlockNotFoundError,
    ConflictError,
    DFShaError,
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.common.tls import TlsConfig, channel_factory
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
_MAX_RETRY_BACKOFF_S = 2.0

# El deadline de un bloque cubre todo el stream. El piso de 1 MiB/s deja margen
# para un bloque de 128 MiB en enlaces lentos sin relajar los RPC cortos de control.
DEFAULT_BLOCK_TRANSFER_BASE_TIMEOUT_S = 5.0
DEFAULT_MIN_TRANSFER_THROUGHPUT_BYTES_PER_S = 1024 * 1024

# Bloques que send/receive transfieren a la vez. Cada hilo mueve un bloque completo por
# streaming (1 MiB en vuelo), así que la memoria no crece con el tamaño del bloque.
DEFAULT_PARALLEL_TRANSFERS = 4

# "Este nodo no puede atender ahora, probá otro": un follower (UNAVAILABLE), un nodo
# caído (UNAVAILABLE) o uno que no respondió a tiempo (DEADLINE_EXCEEDED). Reintentar
# una mutación es seguro solo porque la request lleva op_id.
_RETRYABLE_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})

# El fallback se conserva para servidores anteriores o metadata inválida. No distingue
# PERMISSION_DENIED de una ruta inválida y un permiso denegado; para eso se exige
# dfsha-error de una clase incluida explícitamente en ALLOWED_DOMAIN_ERROR_TYPES.
_STATUS_ERROR_MAP = {
    grpc.StatusCode.NOT_FOUND: PathNotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: PathExistsError,
    grpc.StatusCode.FAILED_PRECONDITION: NotEmptyError,
    grpc.StatusCode.PERMISSION_DENIED: InvalidPathError,
    grpc.StatusCode.INVALID_ARGUMENT: NotAFileError,
    grpc.StatusCode.DATA_LOSS: BlockCorruptedError,
    grpc.StatusCode.ABORTED: ConflictError,
    grpc.StatusCode.UNAUTHENTICATED: AuthError,
}

ALLOWED_DOMAIN_ERROR_TYPES = (
    InvalidPathError,
    PathNotFoundError,
    PathExistsError,
    NotEmptyError,
    NotAFileError,
    NotADirectoryError,
    BlockNotFoundError,
    BlockCorruptedError,
    ConflictError,
    AccessDeniedError,
    AuthError,
)
_ALLOWED_DOMAIN_ERROR_BY_NAME = {error_type.__name__: error_type for error_type in ALLOWED_DOMAIN_ERROR_TYPES}
_READ_FAILOVER_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.DATA_LOSS,
    }
)


def _metadata_error_type(rpc_error: grpc.RpcError) -> type[DFShaError] | None:
    """Acepta exactamente una clase de dominio conocida de la metadata gRPC."""
    try:
        metadata = rpc_error.trailing_metadata() or ()
    except AttributeError:
        return None
    names = [
        value
        for entry in metadata
        if isinstance(entry, tuple)
        and len(entry) == 2
        and entry[0] == "dfsha-error"
        and isinstance((value := entry[1]), str)
    ]
    if len(names) != 1:
        return None
    return _ALLOWED_DOMAIN_ERROR_BY_NAME.get(names[0])


def _translate(rpc_error: grpc.RpcError) -> DFShaError:
    error_type = _metadata_error_type(rpc_error)
    if error_type is None:
        error_type = _STATUS_ERROR_MAP.get(rpc_error.code(), DFShaError)
    return error_type(rpc_error.details())


def _is_recoverable_read_error(rpc_error: grpc.RpcError) -> bool:
    if rpc_error.code() in _READ_FAILOVER_CODES:
        return True
    return (
        rpc_error.code() == grpc.StatusCode.NOT_FOUND
        and _metadata_error_type(rpc_error) is BlockNotFoundError
    )


def _retry_delay(attempt: int) -> float:
    """Backoff exponencial con jitter para requests de control idempotentes."""
    base = min(_RETRY_BACKOFF_S * (2**attempt), _MAX_RETRY_BACKOFF_S)
    return base * random.uniform(0.5, 1.5)


def _new_op_id() -> str:
    return uuid.uuid4().hex


def _canonical_lock_path(path: str) -> str:
    """Replica la clave de locks del ControlTree sin aceptar ``..`` como válido."""
    return "/" + "/".join(part for part in path.strip("/").split("/") if part)


class _TransferCancelled(Exception):
    """Un bloque no se transfirió, o se cortó, porque otro bloque del mismo archivo ya
    había fallado. Nunca llega al usuario: se relanza el error original."""


class _RegionWriter:
    """Vista de un archivo abierto limitada a la región de un bloque.

    En una descarga paralela varios hilos escriben bloques distintos del mismo archivo.
    El failover de lectura descarta lo escrito por una réplica que falló con
    ``seek(inicio)`` + ``truncate()``; acá ``truncate`` no recorta el archivo, porque
    borraría los bloques que ya escribieron otros hilos: la réplica siguiente
    sobrescribe la región desde el principio. Escribir fuera de la región es un error."""

    def __init__(self, fh, size: int) -> None:
        self._fh = fh
        self._start = fh.tell()
        self._size = size

    def tell(self) -> int:
        return self._fh.tell() - self._start

    def seek(self, position: int) -> None:
        self._fh.seek(self._start + position)

    def truncate(self) -> None:
        pass

    def write(self, data: bytes) -> int:
        if self.tell() + len(data) > self._size:
            raise BlockCorruptedError("una réplica envió más bytes que el tamaño del bloque")
        return self._fh.write(data)


def _block_starts(blocks) -> list[int]:
    """Offset de cada bloque dentro del archivo: solo el último puede ser más corto."""
    starts, position = [], 0
    for block in blocks:
        starts.append(position)
        position += block.size_bytes
    return starts


class LeaseLock:
    """Handle local de un lock durable, y el handle de archivo de RF3 (D-P3): `open`
    lo devuelve, y `read`/`write` operan bajo este mismo lock en vez de tomar otro."""

    def __init__(self, client: "DistributedDFShaClient", path: str, mode: str, lock_id: str, lease_s: float):
        self._client = client
        self.path = path
        self.mode = mode
        self.lock_id = lock_id
        self.lease_s = lease_s
        self.lost = False
        self.released = False

    def renew(self) -> None:
        if self.released:
            return
        self._client._renew_held_lock(self)

    def release(self) -> None:
        if not self.released:
            self._client._release_held_lock(self)

    def close(self) -> None:
        self.release()

    def read(self, offset: int = 0, length: int | None = None) -> bytes:
        buffer = io.BytesIO()
        self._client._read_range(self.path, offset, length, buffer, held=self)
        return buffer.getvalue()

    def write(self, offset: int, data: bytes) -> int:
        return self._client._write_with_lock(self, offset, data)


class DistributedDFShaClient:
    def __init__(
        self,
        control_node_addresses: list[str],
        rpc_timeout_s: float = DEFAULT_RPC_TIMEOUT_S,
        failover_budget_s: float = DEFAULT_FAILOVER_BUDGET_S,
        transfer_base_timeout_s: float = DEFAULT_BLOCK_TRANSFER_BASE_TIMEOUT_S,
        minimum_transfer_throughput_bytes_per_s: float = DEFAULT_MIN_TRANSFER_THROUGHPUT_BYTES_PER_S,
        tls: TlsConfig | None = None,
        parallel_transfers: int = DEFAULT_PARALLEL_TRANSFERS,
    ) -> None:
        if isinstance(control_node_addresses, str):
            # un str se iteraría letra por letra como si fueran direcciones
            raise TypeError("control_node_addresses debe ser una lista de host:port")
        if not control_node_addresses:
            raise ValueError("hace falta al menos un ControlNode")
        if (
            rpc_timeout_s <= 0
            or failover_budget_s <= 0
            or transfer_base_timeout_s <= 0
            or minimum_transfer_throughput_bytes_per_s <= 0
        ):
            raise ValueError("los timeouts y throughput del cliente deben ser mayores que cero")
        if parallel_transfers < 1:
            raise ValueError(f"parallel_transfers debe ser >= 1, no {parallel_transfers}")
        self._parallel_transfers = parallel_transfers
        self._new_channel = channel_factory(tls)
        self._control_addresses = list(control_node_addresses)
        self._control_channels = {a: self._new_channel(a) for a in self._control_addresses}
        self._leader_index = 0  # último nodo que respondió: el líder más probable
        self._rpc_timeout_s = rpc_timeout_s
        self._failover_budget_s = failover_budget_s
        self._transfer_base_timeout_s = transfer_base_timeout_s
        self._minimum_transfer_throughput_bytes_per_s = minimum_transfer_throughput_bytes_per_s
        self._datanode_channels: dict[str, grpc.Channel] = {}
        self._datanode_channels_guard = threading.Lock()  # varios hilos de transferencia
        self._held_locks: dict[str, LeaseLock] = {}
        self._locks_guard = threading.RLock()
        self._lock_renewal_stop = threading.Event()
        self._lock_renewal_thread: threading.Thread | None = None
        # Sesión (C2). El token vive solo en memoria y la contraseña no se guarda: cuando
        # el token vence, hay que volver a llamar login(). None = sin sesión.
        self._auth_metadata: tuple[tuple[str, str], ...] | None = None
        self.username: str | None = None
        self.groups: tuple[str, ...] = ()
        self.is_admin = False

    def close(self) -> None:
        # Libera primero los leases para no dejar esperar al siguiente cliente; si la
        # red ya cayó, el vencimiento del servidor conserva la propiedad de liveness.
        for held in self.locks():
            try:
                held.release()
            except DFShaError:
                pass
        self._stop_lock_renewer()
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
        attempt = 0
        while True:
            for offset in range(total):
                index = (self._leader_index + offset) % total
                channel = self._control_channels[self._control_addresses[index]]
                stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)
                try:
                    # El token va en cada llamada, también desde los hilos de transferencia
                    # y el que renueva locks. Un UNAUTHENTICATED no es reintentable: los tres
                    # nodos comparten el secreto, probar otro no cambia nada.
                    response = getattr(stub, rpc_name)(
                        request, timeout=self._rpc_timeout_s, metadata=self._auth_metadata
                    )
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
            time.sleep(min(_retry_delay(attempt), max(0.0, deadline - time.monotonic())))
            attempt += 1

    def _datanode_stub(self, address: str):
        with self._datanode_channels_guard:
            if address not in self._datanode_channels:
                self._datanode_channels[address] = self._new_channel(address)
            channel = self._datanode_channels[address]
        return data_node_pb2_grpc.DataNodeServiceStub(channel)

    def _in_parallel(self, transfer, items: list[tuple]) -> list:
        """Corre ``transfer(*item, stop)`` con hasta ``parallel_transfers`` hilos y
        devuelve los resultados en el orden de ``items``.

        Al primer error se activa ``stop``: los bloques que no empezaron no arrancan y
        los que están en vuelo se cortan en el siguiente chunk. Se espera a que todos
        terminen antes de relanzar ese primer error, así quien llama (AbortUpload)
        limpia con todos los hilos quietos."""
        stop = threading.Event()
        if self._parallel_transfers == 1 or len(items) <= 1:
            return [transfer(*item, stop) for item in items]
        first_error: list[BaseException] = []
        guard = threading.Lock()

        def run(item):
            if stop.is_set():
                raise _TransferCancelled()
            try:
                return transfer(*item, stop)
            except BaseException as exc:
                with guard:
                    if not stop.is_set():
                        first_error.append(exc)
                        stop.set()
                raise

        pool = ThreadPoolExecutor(
            max_workers=min(self._parallel_transfers, len(items)), thread_name_prefix="dfsha-bloque"
        )
        try:
            futures = [pool.submit(run, item) for item in items]
            wait(futures)
        except BaseException:
            # Ctrl+C mientras se espera: cortar lo que está en vuelo y no esperar.
            stop.set()
            pool.shutdown(wait=False, cancel_futures=True)
            raise
        pool.shutdown(wait=True)
        if first_error:
            raise first_error[0]
        return [future.result() for future in futures]

    def _block_transfer_timeout(self, size_bytes: int) -> float:
        return self._transfer_base_timeout_s + (
            size_bytes / self._minimum_transfer_throughput_bytes_per_s
        )

    def login(self, username: str, password: str) -> None:
        """Inicia sesión. Si falla, la sesión anterior queda como estaba."""
        response = self._call("Login", control_node_pb2.LoginRequest(username=username, password=password))
        self._auth_metadata = (("authorization", f"Bearer {response.token}"),)
        self.username = response.username
        self.groups = tuple(response.groups)
        self.is_admin = response.is_admin

    def create_user(
        self, username: str, password: str, groups: list[str] | None = None, is_admin: bool = False
    ) -> None:
        self._call(
            "CreateUser",
            control_node_pb2.CreateUserRequest(
                username=username, password=password, groups=groups or [], is_admin=is_admin, op_id=_new_op_id()
            ),
        )

    def change_password(self, new_password: str, current_password: str = "", username: str = "") -> None:
        """Sin `username` cambia la propia, y hace falta la actual. Un admin cambia la de
        otro usuario sin ella."""
        self._call(
            "ChangePassword",
            control_node_pb2.ChangePasswordRequest(
                username=username,
                current_password=current_password,
                new_password=new_password,
                op_id=_new_op_id(),
            ),
        )

    def list_dir(self, path: str):
        response = self._call("ListDir", control_node_pb2.ListDirRequest(path=path))
        return list(response.entries)

    def make_dir(self, path: str) -> None:
        self._call("MakeDir", control_node_pb2.MakeDirRequest(path=path, op_id=_new_op_id()))

    def remove_dir(self, path: str) -> None:
        self._call("RemoveDir", control_node_pb2.RemoveDirRequest(path=path, op_id=_new_op_id()))

    def remove(self, path: str) -> None:
        self._call("Remove", control_node_pb2.RemoveRequest(path=path, op_id=_new_op_id()))

    def lock(self, path: str, mode: str) -> LeaseLock:
        if mode not in {"r", "w"}:
            raise InvalidPathError(f"modo de lock inválido: {mode!r}")
        canonical_path = _canonical_lock_path(path)
        response = self._call(
            "Lock", control_node_pb2.LockRequest(path=canonical_path, mode=mode, op_id=_new_op_id())
        )
        held = LeaseLock(self, canonical_path, mode, response.lock_id, response.lease_s)
        with self._locks_guard:
            self._held_locks[held.lock_id] = held
        self._start_lock_renewer()
        return held

    def open(self, path: str, mode: str) -> LeaseLock:
        """RF3 `open` (D-P3): "r" toma el lock compartido y "w" el exclusivo. El handle
        devuelto lee y escribe bajo ese lock hasta `close()`."""
        return self.lock(path, mode)

    def locks(self) -> list[LeaseLock]:
        with self._locks_guard:
            return list(self._held_locks.values())

    def unlock(self, path: str) -> None:
        canonical_path = _canonical_lock_path(path)
        for held in reversed(self.locks()):
            if held.path == canonical_path and not held.released:
                held.release()
                return
        raise ConflictError(f"no hay lock propio para: {canonical_path}")

    def _forget_held_lock(self, held: LeaseLock, *, lost: bool = False) -> None:
        """Quita un lease local y detiene el renovador si era el último."""
        with self._locks_guard:
            held.lost = held.lost or lost
            self._held_locks.pop(held.lock_id, None)
            empty = not self._held_locks
        if empty:
            self._stop_lock_renewer()

    def _renew_held_lock(self, held: LeaseLock) -> None:
        if held.lost or held.released:
            return
        try:
            self._call(
                "RenewLock",
                control_node_pb2.RenewLockRequest(
                    path=held.path, lock_id=held.lock_id, op_id=_new_op_id()
                ),
            )
        except (ConflictError, AuthError):
            # ConflictError: el servidor ya depuró el holder vencido. AuthError: el token
            # venció con el handle abierto y el lease ya no se puede renovar, así que va a
            # caducar en el servidor. En los dos casos: no mantener un handle que aparenta
            # proteger la lectura ni dejar vivo el hilo renovador.
            self._forget_held_lock(held, lost=True)
            raise

    def _release_held_lock(self, held: LeaseLock) -> None:
        if held.released:
            return
        try:
            if not held.lost:
                self._call(
                    "Unlock",
                    control_node_pb2.UnlockRequest(
                        path=held.path, lock_id=held.lock_id, op_id=_new_op_id()
                    ),
                )
        except ConflictError:
            # Puede vencer entre la última renovación y Unlock. La limpieza local
            # sigue siendo correcta e idempotente para un handle propio.
            held.lost = True
        finally:
            held.released = True
            self._forget_held_lock(held)

    def _start_lock_renewer(self) -> None:
        with self._locks_guard:
            if self._lock_renewal_thread is not None and self._lock_renewal_thread.is_alive():
                return
            self._lock_renewal_stop = threading.Event()
            self._lock_renewal_thread = threading.Thread(
                target=self._renew_locks_until_stopped, name="dfsha-lock-renewer", daemon=True
            )
            self._lock_renewal_thread.start()

    def _stop_lock_renewer(self) -> None:
        with self._locks_guard:
            thread = self._lock_renewal_thread
            self._lock_renewal_stop.set()
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        with self._locks_guard:
            if self._lock_renewal_thread is thread:
                self._lock_renewal_thread = None

    def _renew_locks_until_stopped(self) -> None:
        while True:
            with self._locks_guard:
                held = list(self._held_locks.values())
                stop_event = self._lock_renewal_stop
            if not held or stop_event.wait(min(lock.lease_s for lock in held) / 3):
                return
            for lock in held:
                try:
                    lock.renew()
                except DFShaError:
                    # Una renovación puede cruzar un failover; _call ya reintenta.
                    # Si el lease finalmente venció, la siguiente operación expone el
                    # conflicto al usuario sin dejar morir este hilo ni filtrar recursos.
                    pass

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
        blocks = list(begin_response.blocks)

        def send_block(block, start: int, stop: threading.Event) -> int:
            # cada hilo con su propio descriptor: comparten el archivo, no la posición
            with local_path.open("rb") as fh:
                fh.seek(start)
                checksum, bytes_written = self._write_block(block, fh, stop)
            if stop.is_set():
                raise _TransferCancelled()
            # El orden de los ConfirmBlock no importa: el archivo se publica recién
            # con CompleteUpload, cuando están todos.
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
            return bytes_written

        try:
            total_written = sum(self._in_parallel(send_block, list(zip(blocks, _block_starts(blocks)))))
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
        held = self.lock(remote_path, "r")
        tmp_path = local_path.parent / f"{local_path.name}.part-{uuid.uuid4().hex}"
        try:
            # La adquisición ocurre antes de ListBlocks y se conserva durante toda
            # la lectura: un writer/GC no puede publicar/borrar la versión leída.
            list_response = self._call("ListBlocks", control_node_pb2.ListBlocksRequest(path=remote_path))
            blocks = list(list_response.blocks)
            with tmp_path.open("wb") as fh:
                # tamaño final de una vez: cada hilo escribe su bloque en su lugar
                fh.truncate(sum(block.size_bytes for block in blocks))

            def fetch_block(block, start: int, stop: threading.Event) -> int:
                with tmp_path.open("r+b") as fh:
                    fh.seek(start)
                    bytes_written = self._read_block_with_failover(
                        block, _RegionWriter(fh, block.size_bytes), stop=stop
                    )
                if bytes_written != block.size_bytes:
                    raise BlockCorruptedError(
                        f"el bloque {block.block_id} llegó con {bytes_written} bytes, se esperaban {block.size_bytes}"
                    )
                return bytes_written

            bytes_written = sum(self._in_parallel(fetch_block, list(zip(blocks, _block_starts(blocks)))))
            os.replace(tmp_path, local_path)
        except grpc.RpcError as exc:
            tmp_path.unlink(missing_ok=True)
            raise _translate(exc) from exc
        except BaseException:
            # tras un Ctrl+C algún hilo puede tener el archivo abierto todavía (Windows
            # no deja borrarlo): queda el .part, que no pisa nada
            with contextlib.suppress(OSError):
                tmp_path.unlink(missing_ok=True)
            raise
        finally:
            try:
                held.release()
            except DFShaError:
                # El resultado de la descarga no cambia por un Unlock tardío o por
                # una red caída durante la limpieza; el lease la libera después.
                pass
        return bytes_written

    def read(self, path: str, offset: int = 0, length: int | None = None) -> bytes:
        """RF3 `read`: `length` bytes desde `offset` (None: hasta el final del archivo)."""
        buffer = io.BytesIO()
        self._read_range(path, offset, length, buffer)
        return buffer.getvalue()

    def read_to_file(self, path: str, offset: int, length: int | None, local_path: Path) -> int:
        """Como `read`, pero escribe a un archivo local: para rangos grandes. Mismo
        patrón atómico que `download`: nunca deja el destino a medias."""
        tmp_path = local_path.parent / f"{local_path.name}.part-{uuid.uuid4().hex}"
        try:
            with tmp_path.open("wb") as fh:
                bytes_written = self._read_range(path, offset, length, fh)
            os.replace(tmp_path, local_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise
        return bytes_written

    def _read_range(self, path: str, offset: int, length: int | None, fh, held: LeaseLock | None = None) -> int:
        if offset < 0 or (length is not None and length < 0):
            raise InvalidPathError(f"rango inválido: offset={offset}, length={length}")
        # D-P3: el lock compartido se toma antes de ListBlocks y dura toda la lectura,
        # igual que en download: un writer o el GC no pueden cambiar la versión leída.
        # Un handle abierto (`held`, o uno propio sobre la misma ruta) ya tiene su lock: se
        # lee bajo ese y no se suelta. Pedir otro "r" chocaría con el "w" propio.
        if held is None:
            canonical_path = _canonical_lock_path(path)
            held = next((h for h in self.locks() if h.path == canonical_path and not h.lost), None)
        own_lock = held is None
        if own_lock:
            held = self.lock(path, "r")
        try:
            blocks = self._call("ListBlocks", control_node_pb2.ListBlocksRequest(path=path)).blocks
            size = sum(block.size_bytes for block in blocks)
            if offset > size:
                raise InvalidPathError(f"offset {offset} fuera del archivo ({size} bytes)")
            end = size if length is None else min(size, offset + length)
            bytes_written = 0
            block_start = 0
            # Los bloques se ubican sumando tamaños: solo el último puede ser más corto (D-P4).
            for block in blocks:
                block_end = block_start + block.size_bytes
                if block_start < end and block_end > offset:
                    start_in_block = max(offset, block_start) - block_start
                    count = min(end, block_end) - block_start - start_in_block
                    bytes_written += self._read_block_with_failover(block, fh, start_in_block, count)
                block_start = block_end
            return bytes_written
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        finally:
            if own_lock:
                try:
                    held.release()
                except DFShaError:
                    # Un Unlock fallido no cambia el resultado de la lectura; el lease lo
                    # libera después.
                    pass

    def write(self, path: str, offset: int, data: bytes) -> int:
        """RF3 `write`: escribe `data` desde `offset`, sobrescribiendo o extendiendo (D-P4).
        Si ya hay un handle propio abierto en modo "w" sobre la ruta, escribe bajo ese
        lock; si no, toma el exclusivo solo para esta escritura."""
        canonical_path = _canonical_lock_path(path)
        for held in self.locks():
            if held.path == canonical_path and held.mode == "w" and not held.lost:
                return self._write_with_lock(held, offset, data)
        held = self.lock(path, "w")
        try:
            return self._write_with_lock(held, offset, data)
        finally:
            try:
                held.release()
            except DFShaError:
                pass  # el lease lo libera; no tapar el resultado de la escritura

    def _write_with_lock(self, held: LeaseLock, offset: int, data: bytes) -> int:
        if held.mode != "w" or held.released or held.lost:
            raise ConflictError(f"hace falta un handle abierto en modo w para escribir en {held.path}")
        begun = self._call(
            "BeginWrite",
            control_node_pb2.BeginWriteRequest(
                path=held.path, offset=offset, length=len(data), lock_id=held.lock_id, op_id=_new_op_id()
            ),
        )
        try:
            confirmed = []
            for slot in begun.slots:
                content = self._slot_content(slot, begun.block_size, offset, data)
                new_block = SimpleNamespace(
                    block_id=slot.new_block_id,
                    datanode_addresses=list(slot.new_addresses),
                    size_bytes=slot.new_size,
                )
                # el bloque nuevo viaja por el pipeline de siempre, con su checksum
                checksum, bytes_written = self._write_block(new_block, io.BytesIO(content))
                confirmed.append(
                    control_node_pb2.ConfirmedSlot(
                        index=slot.index, block_id=slot.new_block_id, checksum=checksum, size_bytes=bytes_written
                    )
                )
            self._call(
                "CommitWrite",
                control_node_pb2.CommitWriteRequest(
                    path=held.path,
                    write_id=begun.write_id,
                    base_version=begun.base_version,
                    lock_id=held.lock_id,
                    slots=confirmed,
                    op_id=_new_op_id(),
                ),
            )
        except (grpc.RpcError, OSError, DFShaError) as exc:
            # Si el commit llegó a aplicarse (resultado incierto), la reserva ya no existe
            # y el abort no borra nada: nunca destruye una escritura publicada.
            try:
                self._call(
                    "AbortWrite",
                    control_node_pb2.AbortWriteRequest(
                        path=held.path, write_id=begun.write_id, lock_id=held.lock_id, op_id=_new_op_id()
                    ),
                )
            except DFShaError:
                pass  # best-effort: no tapar la excepción original con la del abort
            if isinstance(exc, grpc.RpcError):
                raise _translate(exc) from exc
            raise
        return len(data)

    def _slot_content(self, slot, block_size: int, offset: int, data: bytes) -> bytes:
        """Contenido completo del bloque nuevo: los datos de la escritura que caen en él,
        y el resto copiado del bloque viejo (copy-on-write).

        ponytail: arma el bloque entero en memoria (hasta 128 MB); mezclar por streaming
        si llega a importar."""
        block_start = slot.index * block_size
        block_end = block_start + slot.new_size
        write_start = max(offset, block_start)
        write_end = min(offset + len(data), block_end)
        if write_start == block_start and write_end == block_end:
            return data[write_start - offset : write_end - offset]
        content = bytearray(slot.new_size)
        if slot.old_block_id:
            old_block = SimpleNamespace(
                block_id=slot.old_block_id,
                datanode_addresses=list(slot.old_addresses),
                size_bytes=slot.old_size,
            )
            old = io.BytesIO()
            self._read_block_with_failover(old_block, old)
            old_bytes = old.getvalue()[: slot.new_size]
            content[: len(old_bytes)] = old_bytes
        content[write_start - block_start : write_end - block_start] = data[write_start - offset : write_end - offset]
        return bytes(content)

    def _read_block_with_failover(
        self, block, fh, offset: int = 0, length: int = 0, stop: threading.Event | None = None
    ) -> int:
        """Prueba otra réplica solo cuando una falla recuperable invalida este bloque.

        offset y length piden un rango dentro del bloque; length 0 = hasta el final.
        ``stop`` corta la lectura entre chunks (otro bloque de la descarga falló)."""
        start = fh.tell()
        last_error: grpc.RpcError | None = None
        for address in block.datanode_addresses:
            try:
                request = data_node_pb2.ReadBlockRequest(
                    block_id=block.block_id, offset=offset, length=length
                )
                call = self._datanode_stub(address).ReadBlock(
                    request,
                    timeout=self._block_transfer_timeout(length or getattr(block, "size_bytes", 0)),
                )
                for chunk in call:
                    if stop is not None and stop.is_set():
                        call.cancel()
                        raise _TransferCancelled()
                    fh.write(chunk.data)
                return fh.tell() - start
            except grpc.RpcError as exc:
                # la réplica pudo haber fallado a mitad de bloque, con bytes ya
                # escritos: sin descartarlos el archivo final queda corrupto en silencio
                fh.seek(start)
                fh.truncate()
                if not _is_recoverable_read_error(exc):
                    raise
                last_error = exc
        if last_error is None:
            raise PathNotFoundError(f"el bloque {block.block_id} no tiene réplicas registradas")
        raise last_error

    def _write_block(self, block, fh, stop: threading.Event | None = None) -> tuple[str, int]:
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
                if stop is not None and stop.is_set():
                    # cortar el stream: gRPC cancela la llamada y el DataNode descarta
                    # el bloque a medias
                    raise _TransferCancelled()
                data = fh.read(min(CHUNK_SIZE_BYTES, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data_node_pb2.WriteBlockChunk(data=data)

        # el cliente sube una sola copia, al primero del pipeline; los DataNodes
        # se encargan de encadenar las réplicas restantes
        stub = self._datanode_stub(block.datanode_addresses[0])
        response = stub.WriteBlock(
            chunks(), timeout=self._block_transfer_timeout(block.size_bytes)
        )
        return response.checksum, response.bytes_written
