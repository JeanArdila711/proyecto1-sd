from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field

from dfsha.common.exceptions import (
    AccessDeniedError,
    ConflictError,
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)

# C3. Dueño, grupo y modo de un nodo. Estos tres defaults son formato persistido: un nodo
# de un snapshot anterior a C3 no los tiene en su __dict__ y los toma del atributo de
# clase. Cambiarlos cambiaría los permisos de todos los nodos viejos.
DEFAULT_OWNER = "admin"
DIR_MODE, FILE_MODE, ROOT_MODE = 0o755, 0o644, 0o777
R, W = 4, 2

# Quien llama, como viaja en el comando replicado: (usuario, grupos, es_admin). Una tupla
# plana y no un Principal: el journal se serializa con pickle y no tiene que depender de
# ninguna clase. None = sin identidad (autenticación apagada, journal anterior a C3 o un
# hilo interno del líder): no se chequea nada y lo que se crea queda de admin.
Caller = tuple[str, tuple[str, ...], bool]


@dataclass
class BlockRecord:
    block_id: str
    # en orden de pipeline: [0] es a quien escribe el cliente y el primero a quien
    # le intenta leer; el resto son las réplicas encadenadas
    datanode_addresses: list[str]
    checksum: str = ""
    size_bytes: int = 0
    confirmed: bool = False


@dataclass
class FileNode:
    state: str = "pending"  # "pending" | "committed"
    blocks: list[BlockRecord] = field(default_factory=list)
    # Lease de la subida pendiente: pasada esta hora (reloj del líder), otro BeginUpload
    # puede reemplazarla. Sin esto, un cliente que muere a mitad de subida deja el nombre
    # bloqueado para siempre. 0.0 = sin caducidad (entradas creadas antes de existir el lease).
    lease_expires_at: float = 0.0
    # B3. Defaults simples: un FileNode de un snapshot viejo los toma del atributo de
    # clase. version sube con cada commit_write (CAS optimista); block_size 0 = archivo
    # subido antes de B3, se infiere del primer bloque (ver _block_size_of).
    version: int = 0
    block_size: int = 0
    # C3. Mismo mecanismo: un archivo anterior a C3 es de admin con 0o644.
    owner: str = DEFAULT_OWNER
    group: str = DEFAULT_OWNER
    mode: int = FILE_MODE


@dataclass
class WriteSlotRecord:
    index: int
    new_block_id: str
    new_addresses: list[str]
    new_size: int


@dataclass
class WriteReservation:
    """Una escritura copy-on-write en curso: sus bloques nuevos todavía no son visibles."""

    write_id: str
    path: str  # canónica
    base_version: int
    lock_id: str
    block_size: int
    slots: list[WriteSlotRecord]
    lease_expires_at: float


def plan_write_slots(sizes: list[int], block_size: int, offset: int, length: int) -> list[tuple[int, int]]:
    """(índice, tamaño nuevo) de cada bloque que toca escribir `length` bytes desde `offset`.

    Una sola función para las dos puntas: el líder la usa para proponer bloques nuevos
    y begin_write para validar la propuesta, así nunca discrepan. D-P4: se sobrescribe
    o se extiende, nunca se inserta; todos los bloques menos el último quedan llenos."""
    size = sum(sizes)
    if length <= 0:
        raise InvalidPathError(f"el largo a escribir debe ser mayor a 0, no {length}")
    if offset < 0 or offset > size:
        raise InvalidPathError(f"offset {offset} fuera del archivo ({size} bytes); no hay huecos")
    new_size = max(size, offset + length)
    first = offset // block_size
    last = (offset + length - 1) // block_size
    return [(index, min(block_size, new_size - index * block_size)) for index in range(first, last + 1)]


def _block_size_of(node: FileNode, default: int) -> int:
    stored = getattr(node, "block_size", 0)
    if stored:
        return stored
    # archivo previo a B3: con más de un bloque, el primero está lleno (invariante D-P4)
    return node.blocks[0].size_bytes if len(node.blocks) > 1 else default


def _lease_expired(node, now: float | None) -> bool:
    # getattr: un FileNode restaurado de un snapshot viejo no tiene el atributo
    expires_at = getattr(node, "lease_expires_at", 0.0)
    return now is not None and 0.0 < expires_at <= now


@dataclass
class LockState:
    """Estado durable de una ruta bloqueada; las expiraciones las fija el líder."""

    mode: str  # "r" o "w"
    holders: dict[str, tuple[str, float]] = field(default_factory=dict)


@dataclass
class DirNode:
    children: dict = field(default_factory=dict)  # str -> DirNode | FileNode
    # C3. Un directorio anterior a C3 es de admin con 0o755. La raíz es la excepción
    # (0o777): no la crea ningún comando, así que se fija en ControlTree.
    owner: str = DEFAULT_OWNER
    group: str = DEFAULT_OWNER
    mode: int = DIR_MODE


# Nombres de usuario y de grupo, estilo Unix. Se validan acá y no solo en el servicer:
# el bootstrap del admin entra por _commit_raw sin pasar por un RPC.
_USERNAME_RE = re.compile(r"[a-z_][a-z0-9_-]{0,31}")


@dataclass(frozen=True)
class UserRecord:
    """Un usuario (C2). Inmutable: cambiar la contraseña reemplaza el registro entero.
    Hash y sal los calcula el líder y viajan en el comando replicado; quedan en el
    journal de Raft, que no va cifrado en disco."""

    password_hash: bytes
    salt: bytes
    groups: tuple[str, ...] = ()
    is_admin: bool = False


def _valid_name(value) -> bool:
    return isinstance(value, str) and _USERNAME_RE.fullmatch(value) is not None


# --- Permisos (C3) ---------------------------------------------------------------------------
#
# Funciones puras de (nodo, quien llama): corren dentro de apply() sin leer reloj, red ni
# disco. Una tupla de quien llama mal formada revienta al desempaquetar y apply() la
# convierte en un error, sin lanzar.


def _allows(node, caller: Caller | None, want: int) -> bool:
    """Clases excluyentes, como en Unix: el dueño usa solo sus bits, aunque esté en el
    grupo. El admin y None pasan siempre. No se exige `x` en ningún caso."""
    if caller is None:
        return True
    username, groups, is_admin = caller
    if is_admin:
        return True
    shift = 6 if username == node.owner else 3 if node.group in groups else 0
    return (node.mode >> shift) & want == want


def _require(node, caller: Caller | None, want: int, path: str) -> None:
    if not _allows(node, caller, want):
        raise AccessDeniedError(f"permiso denegado: falta {'r' if want == R else 'w'} en {path}")


def _owner_or_admin(node, caller: Caller | None) -> bool:
    if caller is None:
        return True
    username, _, is_admin = caller
    return bool(is_admin) or username == node.owner


def _ownership(caller: Caller | None) -> tuple[str, str]:
    """(dueño, grupo) de un nodo nuevo: quien lo crea y su primer grupo; sin grupos, su nombre."""
    if caller is None:
        return DEFAULT_OWNER, DEFAULT_OWNER
    username, groups, _ = caller
    return username, (groups[0] if groups else username)


def _valid_mode(mode) -> bool:
    # bool es un int en Python: True no puede pasar como modo 1
    return isinstance(mode, int) and not isinstance(mode, bool) and 0 <= mode <= 0o777


@dataclass
class DirEntryData:
    name: str
    is_dir: bool
    size_bytes: int
    owner: str = DEFAULT_OWNER
    group: str = DEFAULT_OWNER
    mode: int = 0


class ControlTree:
    def __init__(self) -> None:
        # C3: la raíz no pasa por ningún comando replicado; el default de clase de
        # DirNode (0o755) no le sirve. Un snapshot viejo la recupera en __setstate__.
        self._root = DirNode(mode=ROOT_MODE)
        # ponytail: un solo lock global sobre el árbol — ops en memoria, ~µs;
        # si algún día hay contención, pasar a locks por subárbol
        self._lock = threading.Lock()
        # Estado replicado: snapshots anteriores a B1 lo recuperan en __setstate__.
        self._locks: dict[str, LockState] = {}
        # Reservas de escritura COW por write_id (B3); también se recuperan en __setstate__.
        self._writes: dict[str, WriteReservation] = {}
        # Usuarios por nombre (C2); también se recuperan en __setstate__.
        self._users: dict[str, UserRecord] = {}

    # Raft guarda snapshots del árbol con pickle, y un Lock no se puede serializar:
    # se descarta al guardar y se crea uno nuevo al restaurar.
    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()
        # Snapshot pre-Hito 3: faltan los atributos fuera de dataclass.
        if not hasattr(self, "_locks"):
            self._locks = {}
        if not hasattr(self, "_writes"):
            self._writes = {}
        if not hasattr(self, "_users"):
            self._users = {}
        # Raíz anterior a C3: sin mode propio tomaría el 0o755 de la clase. Solo si no lo
        # trae: un chmod posterior sobre la raíz queda en su __dict__ y no se pisa.
        if "mode" not in self._root.__dict__:
            self._root.mode = ROOT_MODE

    def _parts(self, virtual_path: str) -> list[str]:
        if ".." in virtual_path.split("/"):
            raise InvalidPathError(f"la ruta contiene '..': {virtual_path!r}")
        return [p for p in virtual_path.strip("/").split("/") if p]

    def _canonical_path(self, virtual_path: str) -> str:
        """Representación única de una ruta válida para indexar locks."""
        return "/" + "/".join(self._parts(virtual_path))

    def _get_node(self, parts: list[str]):
        if not parts:
            return self._root
        parent = self._root
        for part in parts[:-1]:
            child = parent.children.get(part)
            if not isinstance(child, DirNode):
                return None
            parent = child
        return parent.children.get(parts[-1])

    def _walk_to_parent(self, parts: list[str], create: bool = False, caller: Caller | None = None) -> DirNode:
        """Con create (solo begin_upload), crea los directorios que falten y exige `w`
        en el último directorio que ya existía (C3): antes de la primera creación, para
        que una subida denegada no deje directorios a medias, o al final si no hubo que
        crear nada. Los de paso quedan de quien sube, con DIR_MODE."""
        current = self._root
        created = False
        for index, part in enumerate(parts):
            child = current.children.get(part)
            if child is None:
                if not create:
                    raise PathNotFoundError(f"no existe: {'/'.join(parts)}")
                if not created:
                    _require(current, caller, W, "/" + "/".join(parts[:index]))
                    created = True
                owner, group = _ownership(caller)
                child = DirNode(owner=owner, group=group, mode=DIR_MODE)
                current.children[part] = child
            if not isinstance(child, DirNode):
                raise NotADirectoryError(f"no es un directorio: {'/'.join(parts)}")
            current = child
        if create and not created:
            _require(current, caller, W, "/" + "/".join(parts))
        return current

    def list_dir(self, virtual_path: str, caller: Caller | None = None) -> list[DirEntryData]:
        """C3: `r` sobre el directorio. El servicer la llama después de la barrera."""
        with self._lock:
            parts = self._parts(virtual_path)
            node = self._get_node(parts)
            if node is None:
                raise PathNotFoundError(f"no existe: {virtual_path}")
            if not isinstance(node, DirNode):
                raise NotADirectoryError(f"no es un directorio: {virtual_path}")
            _require(node, caller, R, "/" + "/".join(parts))
            entries = []
            for name, child in sorted(node.children.items()):
                if isinstance(child, DirNode):
                    entries.append(
                        DirEntryData(
                            name=name, is_dir=True, size_bytes=0, owner=child.owner, group=child.group, mode=child.mode
                        )
                    )
                elif child.state == "committed":
                    size = sum(b.size_bytes for b in child.blocks)
                    entries.append(
                        DirEntryData(
                            name=name,
                            is_dir=False,
                            size_bytes=size,
                            owner=child.owner,
                            group=child.group,
                            mode=child.mode,
                        )
                    )
            return entries

    def make_dir(self, virtual_path: str, caller: Caller | None = None) -> None:
        """C3: `w` sobre el padre, antes de mirar si el nombre ya existe."""
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise PathExistsError("la raíz ya existe")
            parent = self._walk_to_parent(parts[:-1])
            _require(parent, caller, W, "/" + "/".join(parts[:-1]))
            name = parts[-1]
            if name in parent.children:
                raise PathExistsError(f"ya existe: {virtual_path}")
            owner, group = _ownership(caller)
            parent.children[name] = DirNode(owner=owner, group=group, mode=DIR_MODE)

    def remove_dir(self, virtual_path: str, caller: Caller | None = None) -> None:
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("no se puede borrar la raíz")
            parent = self._walk_to_parent(parts[:-1])
            _require(parent, caller, W, "/" + "/".join(parts[:-1]))
            name = parts[-1]
            node = parent.children.get(name)
            if node is None:
                raise PathNotFoundError(f"no existe: {virtual_path}")
            if not isinstance(node, DirNode):
                raise NotADirectoryError(f"no es un directorio: {virtual_path}")
            if node.children:
                raise NotEmptyError(f"directorio no vacío: {virtual_path}")
            del parent.children[name]

    def remove_file(
        self, virtual_path: str, now: float | None = None, caller: Caller | None = None
    ) -> list[BlockRecord]:
        """Devuelve los bloques que tenía el archivo, para que el servicer
        le avise al DataNode que los borre.

        ``now`` es opcional para reproducir journals anteriores a B1; ``caller`` (C3),
        para los anteriores a C3."""
        with self._lock:
            parts = self._parts(virtual_path)
            canonical_path = "/" + "/".join(parts)
            if not parts:
                raise InvalidPathError("no se puede borrar la raíz")
            parent = self._walk_to_parent(parts[:-1])
            _require(parent, caller, W, "/" + "/".join(parts[:-1]))
            name = parts[-1]
            node = parent.children.get(name)
            if node is None:
                raise PathNotFoundError(f"no existe: {virtual_path}")
            if not isinstance(node, FileNode):
                raise NotAFileError(f"no es un archivo: {virtual_path}")
            if node.state != "committed":
                # una subida pendiente es invisible (igual que en list_dir/list_blocks)
                raise PathNotFoundError(f"no existe: {virtual_path}")
            self._cleanup_expired_locks(canonical_path, now)
            if canonical_path in self._locks:
                raise ConflictError(f"el archivo tiene locks vigentes: {virtual_path}")
            del parent.children[name]
            return node.blocks

    def _cleanup_expired_locks(self, canonical_path: str, now: float | None) -> None:
        """Elimina holders vencidos usando exclusivamente ``now`` del líder."""
        if now is None:
            return
        state = self._locks.get(canonical_path)
        if state is None:
            return
        state.holders = {
            lock_id: holder
            for lock_id, holder in state.holders.items()
            if holder[1] > now
        }
        if not state.holders:
            del self._locks[canonical_path]

    def _get_committed_file(self, virtual_path: str) -> FileNode:
        parts = self._parts(virtual_path)
        node = self._get_node(parts)
        if not isinstance(node, FileNode) or node.state != "committed":
            raise PathNotFoundError(f"no existe: {virtual_path}")
        return node

    def acquire_lock(
        self,
        virtual_path: str,
        lock_id: str,
        owner: str,
        mode: str,
        now: float,
        lease_s: float,
        caller: Caller | None = None,
    ) -> str:
        """Toma un lock compartido o exclusivo con vencimiento determinado por líder.

        C3: el compartido exige `r` sobre el archivo y el exclusivo, `w`. Renovar y
        liberar no exigen nada más que el lock_id (D7)."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            if mode not in {"r", "w"}:
                raise InvalidPathError(f"modo de lock inválido: {mode!r}")
            if lease_s <= 0:
                raise InvalidPathError("lease_s debe ser mayor a 0")
            node = self._get_committed_file(canonical_path)
            _require(node, caller, R if mode == "r" else W, canonical_path)
            self._cleanup_expired_locks(canonical_path, now)
            state = self._locks.get(canonical_path)
            if state is not None and (state.mode != "r" or mode != "r"):
                raise ConflictError(f"lock en conflicto para: {virtual_path}")
            if state is None:
                state = LockState(mode=mode)
                self._locks[canonical_path] = state
            if lock_id in state.holders:
                raise ConflictError(f"lock_id ya existe para: {virtual_path}")
            state.holders[lock_id] = (owner, now + lease_s)
            return lock_id

    def renew_lock(self, virtual_path: str, lock_id: str, now: float, lease_s: float) -> None:
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            if lease_s <= 0:
                raise InvalidPathError("lease_s debe ser mayor a 0")
            self._get_committed_file(canonical_path)
            self._cleanup_expired_locks(canonical_path, now)
            state = self._locks.get(canonical_path)
            if state is None or lock_id not in state.holders:
                raise ConflictError(f"lock_id no vigente para: {virtual_path}")
            owner, _ = state.holders[lock_id]
            state.holders[lock_id] = (owner, now + lease_s)

    def release_lock(self, virtual_path: str, lock_id: str, now: float | None = None) -> None:
        """Libera un lock; con hora de líder tolera el lease que ya venció.

        ``now`` es opcional para conservar la firma de los journals previos a B1.
        Un lock_id ajeno sigue siendo un conflicto mientras exista otro holder vivo.
        """
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            self._get_committed_file(canonical_path)
            self._cleanup_expired_locks(canonical_path, now)
            state = self._locks.get(canonical_path)
            if state is None:
                if now is not None:
                    return
                raise ConflictError(f"lock_id no pertenece a: {virtual_path}")
            if lock_id not in state.holders:
                raise ConflictError(f"lock_id no pertenece a: {virtual_path}")
            del state.holders[lock_id]
            if not state.holders:
                del self._locks[canonical_path]

    # --- Permisos (C3) ----------------------------------------------------------------------
    #
    # Las dos mutaciones devuelven None: el resultado queda en applied_ops. Una subida
    # pendiente es invisible para las dos, como para list_dir y remove_file.

    def _get_visible_node(self, virtual_path: str):
        node = self._get_node(self._parts(virtual_path))
        if node is None or (isinstance(node, FileNode) and node.state != "committed"):
            raise PathNotFoundError(f"no existe: {virtual_path}")
        return node

    def chmod(self, virtual_path: str, mode: int, caller: Caller | None = None) -> None:
        """Solo el dueño o un admin. Modo entre 0 y 0o777: sin setuid, setgid ni sticky."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            node = self._get_visible_node(canonical_path)
            if not _owner_or_admin(node, caller):
                raise AccessDeniedError(
                    f"permiso denegado: solo el dueño o un admin cambia el modo de {canonical_path}"
                )
            if not _valid_mode(mode):
                raise InvalidPathError(f"modo inválido: {mode!r}; va de 0 a 0o777")
            node.mode = mode

    def chown(self, virtual_path: str, owner: str, group: str, caller: Caller | None = None) -> None:
        """Vacío = no cambia. El dueño lo cambia solo un admin, y el nuevo tiene que
        existir; el grupo, el dueño si pertenece al grupo nuevo, o un admin (P9, P10).
        Los grupos no tienen registro propio: del grupo solo se valida el formato."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            node = self._get_visible_node(canonical_path)
            if not owner and not group:
                raise InvalidPathError("chown: falta el dueño o el grupo")
            if caller is not None:
                username, groups, is_admin = caller
                if owner and not is_admin:
                    raise AccessDeniedError(f"permiso denegado: solo un admin cambia el dueño de {canonical_path}")
                if group and not is_admin and not (username == node.owner and group in groups):
                    raise AccessDeniedError(
                        f"permiso denegado: para cambiar el grupo de {canonical_path} "
                        "hay que ser su dueño y pertenecer al grupo nuevo"
                    )
            if owner and (not isinstance(owner, str) or owner not in self._users):
                raise PathNotFoundError(f"no existe el usuario: {owner}")
            if group and not _valid_name(group):
                raise InvalidPathError(f"nombre de grupo inválido: {group!r}")
            if owner:
                node.owner = owner
            if group:
                node.group = group

    # --- Usuarios (C2) ---------------------------------------------------------------------
    #
    # Las dos mutaciones devuelven None a propósito: el resultado de un comando replicado
    # queda en applied_ops, y ahí no puede terminar un hash.

    def create_user(
        self,
        username: str,
        password_hash: bytes,
        salt: bytes,
        groups: tuple[str, ...] = (),
        is_admin: bool = False,
    ) -> None:
        with self._lock:
            if not _valid_name(username):
                raise InvalidPathError(f"nombre de usuario inválido: {username!r}")
            invalid = [group for group in groups if not _valid_name(group)]
            if invalid:
                raise InvalidPathError(f"nombre de grupo inválido: {invalid[0]!r}")
            if username in self._users:
                # nunca pisa el hash: un segundo create_user no puede robar una cuenta
                raise PathExistsError(f"el usuario ya existe: {username}")
            self._users[username] = UserRecord(bytes(password_hash), bytes(salt), tuple(groups), bool(is_admin))

    def change_password(self, username: str, password_hash: bytes, salt: bytes) -> None:
        with self._lock:
            user = self._users.get(username) if isinstance(username, str) else None
            if user is None:
                raise PathNotFoundError(f"no existe el usuario: {username}")
            self._users[username] = UserRecord(bytes(password_hash), bytes(salt), user.groups, user.is_admin)

    def get_user(self, username: str) -> UserRecord | None:
        with self._lock:
            return self._users.get(username)

    def has_users(self) -> bool:
        with self._lock:
            return bool(self._users)

    def begin_upload(
        self,
        virtual_path: str,
        placements: list[tuple[str, list[str]]],
        now: float | None = None,
        lease_s: float | None = None,
        block_size: int | None = None,
        caller: Caller | None = None,
    ) -> tuple[list[tuple[str, list[str]]], list[BlockRecord]]:
        """placements: (block_id, direcciones de las réplicas en orden de pipeline).
        La política de selección vive en el servicer; el árbol solo la guarda.

        now y lease_s los fija el líder y viajan en el comando replicado: dentro de
        apply() no se puede leer el reloj, cada nodo calcularía una hora distinta.
        Sin ellos (entradas viejas del journal) no hay lease, como antes.
        block_size (B3) va último y opcional por la misma razón: un journal previo lo
        reproduce sin él, y el archivo lo infiere del primer bloque.

        Devuelve (placements guardados, bloques de una subida pendiente vencida que se
        reemplazó). Los placements: si un reintento con el mismo op_id llega con
        placements nuevos, la respuesta tiene que armarse con ESTOS. Los bloques
        reemplazados los borra el servicer después del commit.

        caller (C3) va último por lo mismo: hace falta `w` en el último directorio que
        ya existía, y el archivo queda de quien sube, con FILE_MODE, desde la reserva."""
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("ruta de destino inválida")
            parent = self._walk_to_parent(parts[:-1], create=True, caller=caller)
            name = parts[-1]
            stale_blocks: list[BlockRecord] = []
            existing = parent.children.get(name)
            if existing is not None:
                if isinstance(existing, FileNode) and existing.state == "pending" and _lease_expired(existing, now):
                    stale_blocks = existing.blocks
                else:
                    raise PathExistsError(f"ya existe: {virtual_path}")
            blocks = [
                BlockRecord(block_id=bid, datanode_addresses=list(addresses))
                for bid, addresses in placements
            ]
            expires_at = now + lease_s if now is not None and lease_s else 0.0
            owner, group = _ownership(caller)
            parent.children[name] = FileNode(
                state="pending",
                blocks=blocks,
                lease_expires_at=expires_at,
                block_size=block_size or 0,
                owner=owner,
                group=group,
                mode=FILE_MODE,
            )
            return [(b.block_id, list(b.datanode_addresses)) for b in blocks], stale_blocks

    def confirm_block(
        self,
        virtual_path: str,
        block_id: str,
        checksum: str,
        size_bytes: int,
        now: float | None = None,
        lease_s: float | None = None,
        caller: Caller | None = None,
    ) -> None:
        with self._lock:
            node = self._get_pending_file(virtual_path)
            self._require_upload_owner(node, caller, virtual_path)
            for block in node.blocks:
                if block.block_id == block_id:
                    block.checksum = checksum
                    block.size_bytes = size_bytes
                    block.confirmed = True
                    if now is not None and lease_s:
                        # cada bloque confirmado prueba que el cliente sigue vivo: renueva
                        node.lease_expires_at = now + lease_s
                    return
            raise PathNotFoundError(f"bloque {block_id} no reservado para {virtual_path}")

    def _require_upload_owner(self, node: FileNode, caller: Caller | None, virtual_path: str) -> None:
        """C3 (P6): una subida pendiente la confirman, completan o abortan solo quien la
        empezó o un admin. Con `w` en el padre no alcanza: otro usuario podría abortar
        una subida ajena en curso."""
        if not _owner_or_admin(node, caller):
            raise AccessDeniedError(
                f"permiso denegado: la subida de {self._canonical_path(virtual_path)} es de otro usuario"
            )

    def complete_upload(self, virtual_path: str, caller: Caller | None = None) -> None:
        with self._lock:
            node = self._get_pending_file(virtual_path)
            self._require_upload_owner(node, caller, virtual_path)
            unconfirmed = [b.block_id for b in node.blocks if not b.confirmed]
            if unconfirmed:
                raise InvalidPathError(f"bloques sin confirmar: {unconfirmed}")
            node.state = "committed"

    def abort_upload(self, virtual_path: str, caller: Caller | None = None) -> None:
        # No devuelve los bloques a propósito: el resultado de un comando replicado queda
        # en applied_ops, y reproducir un journal viejo con un retorno distinto daría otro
        # estado. El servicer los lee antes con pending_blocks().
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("no hay una subida pendiente para la raíz")
            parent = self._walk_to_parent(parts[:-1])
            name = parts[-1]
            node = parent.children.get(name)
            if not isinstance(node, FileNode) or node.state != "pending":
                raise PathNotFoundError(f"no hay una subida pendiente para: {virtual_path}")
            self._require_upload_owner(node, caller, virtual_path)
            del parent.children[name]

    def pending_blocks(self, virtual_path: str) -> list[BlockRecord]:
        """Copia de los bloques de una subida pendiente; [] si no hay ninguna. Solo lectura."""
        with self._lock:
            try:
                node = self._get_pending_file(virtual_path)
            except (PathNotFoundError, InvalidPathError):
                return []
            return [BlockRecord(b.block_id, list(b.datanode_addresses), b.checksum, b.size_bytes) for b in node.blocks]

    # --- Escritura copy-on-write (B3) ----------------------------------------------------
    #
    # Los bloques nunca se modifican en su lugar: una escritura reserva bloques nuevos
    # (begin_write), el cliente los escribe por el pipeline normal y commit_write los
    # publica con un compare-and-set de la versión del archivo. Hasta el commit, el
    # archivo visible es el anterior.

    def write_layout(
        self, virtual_path: str, default_block_size: int, caller: Caller | None = None
    ) -> tuple[int, int, list[int]]:
        """(versión, tamaño de bloque, tamaños de los bloques) de un archivo confirmado.
        Solo lectura: el líder la usa, tras la barrera, para armar la propuesta de begin_write.

        C3: exige `w`, antes de proponer bloques, para que un rechazo no revele el
        tamaño del archivo. begin_write y commit_write lo vuelven a exigir en apply()."""
        with self._lock:
            node = self._get_committed_file(virtual_path)
            _require(node, caller, W, self._canonical_path(virtual_path))
            return (
                getattr(node, "version", 0),
                _block_size_of(node, default_block_size),
                [block.size_bytes for block in node.blocks],
            )

    def _require_write_lock(self, canonical_path: str, lock_id: str, now: float) -> None:
        self._cleanup_expired_locks(canonical_path, now)
        state = self._locks.get(canonical_path)
        if state is None or state.mode != "w" or lock_id not in state.holders:
            raise ConflictError(f"hace falta el lock exclusivo vigente para escribir: {canonical_path}")

    def _cleanup_expired_writes(self, now: float | None) -> None:
        """Una reserva vencida deja de existir: sus bloques quedan para el recolector (A3)."""
        if now is None:
            return
        self._writes = {
            write_id: reservation
            for write_id, reservation in self._writes.items()
            if reservation.lease_expires_at > now
        }

    def begin_write(
        self,
        virtual_path: str,
        write_id: str,
        lock_id: str,
        base_version: int,
        offset: int,
        length: int,
        proposals: list[tuple[int, str, list[str]]],
        now: float,
        lease_s: float,
        default_block_size: int,
        caller: Caller | None = None,
    ) -> tuple[str, int, int, list[tuple]]:
        """Reserva los bloques nuevos de una escritura. `proposals` son (índice, block_id,
        réplicas) que el líder generó ANTES del commit (apply() no genera UUIDs ni elige
        réplicas); acá solo se valida que sigan correspondiendo al archivo actual.

        C3: `w` sobre el archivo, otra vez acá y otra en commit_write."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            node = self._get_committed_file(canonical_path)
            _require(node, caller, W, canonical_path)
            self._require_write_lock(canonical_path, lock_id, now)
            self._cleanup_expired_writes(now)
            if write_id in self._writes:
                raise ConflictError(f"write_id repetido: {write_id}")
            version = getattr(node, "version", 0)
            if version != base_version:
                raise ConflictError(f"el archivo cambió (versión {version}, se esperaba {base_version}); reintentá")
            block_size = _block_size_of(node, default_block_size)
            plan = plan_write_slots([b.size_bytes for b in node.blocks], block_size, offset, length)
            if [index for index, _ in plan] != [index for index, _, _ in proposals]:
                raise ConflictError("la propuesta de bloques no coincide con el archivo actual; reintentá")
            slots = [
                WriteSlotRecord(index, block_id, list(addresses), new_size)
                for (index, new_size), (_, block_id, addresses) in zip(plan, proposals)
            ]
            self._writes[write_id] = WriteReservation(
                write_id, canonical_path, version, lock_id, block_size, slots, now + lease_s
            )
            described = []
            for slot in slots:
                old = node.blocks[slot.index] if slot.index < len(node.blocks) else None
                described.append(
                    (
                        slot.index,
                        old.block_id if old else "",
                        old.size_bytes if old else 0,
                        list(old.datanode_addresses) if old else [],
                        slot.new_block_id,
                        list(slot.new_addresses),
                        slot.new_size,
                    )
                )
            return write_id, version, block_size, described

    def commit_write(
        self,
        virtual_path: str,
        write_id: str,
        lock_id: str,
        base_version: int,
        confirmed: list[tuple[int, str, str, int]],
        now: float,
        caller: Caller | None = None,
    ) -> tuple[int, list[BlockRecord]]:
        """Publica los bloques nuevos si la versión sigue siendo la reservada (CAS) y el
        writer conserva el lock. Devuelve (versión nueva, bloques reemplazados), para
        que el servicer del líder borre los viejos después del commit.

        C3: vuelve a exigir `w`. Un chmod entre el begin y el commit rechaza el commit
        y el archivo queda como estaba; la reserva la limpia abort_write o su lease."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            node = self._get_committed_file(canonical_path)
            _require(node, caller, W, canonical_path)
            self._cleanup_expired_writes(now)
            reservation = self._writes.get(write_id)
            if reservation is None or reservation.path != canonical_path:
                raise ConflictError(f"no hay una reserva vigente {write_id} para {canonical_path}")
            if reservation.lock_id != lock_id:
                raise ConflictError(f"la reserva {write_id} es de otro lock")
            self._require_write_lock(canonical_path, lock_id, now)
            version = getattr(node, "version", 0)
            if base_version != reservation.base_version or version != reservation.base_version:
                raise ConflictError(f"el archivo cambió (versión {version}); la escritura no se publica")
            expected = [(s.index, s.new_block_id, s.new_size) for s in reservation.slots]
            if [(index, block_id, size) for index, block_id, _, size in confirmed] != expected:
                raise ConflictError("los bloques confirmados no coinciden con la reserva")
            replaced: list[BlockRecord] = []
            for slot, (_, _, checksum, size) in zip(reservation.slots, confirmed):
                new_block = BlockRecord(slot.new_block_id, list(slot.new_addresses), checksum, size, True)
                if slot.index < len(node.blocks):
                    replaced.append(node.blocks[slot.index])
                    node.blocks[slot.index] = new_block
                else:
                    node.blocks.append(new_block)
            node.version = version + 1
            node.block_size = reservation.block_size
            del self._writes[write_id]
            return node.version, replaced

    def abort_write(self, virtual_path: str, write_id: str, lock_id: str, now: float | None = None) -> list[BlockRecord]:
        """Descarta una reserva y devuelve sus bloques nuevos, para borrarlos. Abortar
        una reserva que ya no existe (vencida, o commit ya aplicado) no es error y no
        devuelve nada: así un abort tras un commit con resultado incierto es inofensivo."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            self._cleanup_expired_writes(now)
            reservation = self._writes.get(write_id)
            if reservation is None or reservation.path != canonical_path:
                return []
            if reservation.lock_id != lock_id:
                raise ConflictError(f"la reserva {write_id} es de otro lock")
            del self._writes[write_id]
            return [
                BlockRecord(slot.new_block_id, list(slot.new_addresses), size_bytes=slot.new_size)
                for slot in reservation.slots
            ]

    def update_block_replicas(
        self,
        virtual_path: str,
        block_id: str,
        expected: list[str],
        new: list[str],
    ) -> None:
        """Reemplaza réplicas solo si la fotografía esperada todavía coincide."""
        with self._lock:
            node = self._get_committed_file(virtual_path)
            for block in node.blocks:
                if block.block_id != block_id:
                    continue
                if block.datanode_addresses != expected:
                    raise ConflictError(f"réplicas cambiaron para bloque {block_id}")
                block.datanode_addresses = list(new)
                return
            raise PathNotFoundError(f"bloque {block_id} no existe en {virtual_path}")

    def referenced_blocks(self, now: float) -> dict[str, set[str]]:
        """block_id -> DataNodes que la metadata le asigna, para el recolector (A3).

        Cuenta como en uso: archivos confirmados, TODAS las subidas pendientes (con el
        lease vencido también: complete_upload la acepta igual mientras nadie reemplace
        el nombre, así que borrarle los bloques perdería datos) y las reservas de
        escritura COW vigentes a `now`. Devuelve copias: nadie se lleva referencias."""
        with self._lock:
            referenced: dict[str, set[str]] = {}

            def visit(node: DirNode) -> None:
                for child in node.children.values():
                    if isinstance(child, DirNode):
                        visit(child)
                        continue
                    for block in child.blocks:
                        referenced.setdefault(block.block_id, set()).update(block.datanode_addresses)

            visit(self._root)
            for reservation in self._writes.values():
                if reservation.lease_expires_at > now:
                    for slot in reservation.slots:
                        referenced.setdefault(slot.new_block_id, set()).update(slot.new_addresses)
            return referenced

    def iter_blocks(self) -> list[tuple[str, BlockRecord]]:
        """Devuelve una fotografía independiente de los bloques confirmados."""
        with self._lock:
            snapshot: list[tuple[str, BlockRecord]] = []

            def visit(node: DirNode, prefix: str) -> None:
                for name, child in node.children.items():
                    path = f"{prefix}/{name}" if prefix else f"/{name}"
                    if isinstance(child, DirNode):
                        visit(child, path)
                    elif child.state == "committed":
                        for block in child.blocks:
                            snapshot.append(
                                (
                                    path,
                                    BlockRecord(
                                        block_id=block.block_id,
                                        datanode_addresses=list(block.datanode_addresses),
                                        checksum=block.checksum,
                                        size_bytes=block.size_bytes,
                                        confirmed=block.confirmed,
                                    ),
                                )
                            )

            visit(self._root, "")
            return snapshot

    def list_blocks(self, virtual_path: str, caller: Caller | None = None) -> list[BlockRecord]:
        """C3: `r` sobre el archivo. El servicer la llama después de la barrera."""
        with self._lock:
            parts = self._parts(virtual_path)
            node = self._get_node(parts)
            if not isinstance(node, FileNode) or node.state != "committed":
                raise PathNotFoundError(f"no existe: {virtual_path}")
            _require(node, caller, R, "/" + "/".join(parts))
            return node.blocks

    def _get_pending_file(self, virtual_path: str) -> FileNode:
        parts = self._parts(virtual_path)
        node = self._get_node(parts)
        if not isinstance(node, FileNode) or node.state != "pending":
            raise PathNotFoundError(f"no hay una subida pendiente para: {virtual_path}")
        return node
