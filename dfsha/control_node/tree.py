from __future__ import annotations

import threading
from dataclasses import dataclass, field

from dfsha.common.exceptions import (
    ConflictError,
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)


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


@dataclass
class DirEntryData:
    name: str
    is_dir: bool
    size_bytes: int


class ControlTree:
    def __init__(self) -> None:
        self._root = DirNode()
        # ponytail: un solo lock global sobre el árbol — ops en memoria, ~µs;
        # si algún día hay contención, pasar a locks por subárbol
        self._lock = threading.Lock()
        # Estado replicado: snapshots anteriores a B1 lo recuperan en __setstate__.
        self._locks: dict[str, LockState] = {}

    # Raft guarda snapshots del árbol con pickle, y un Lock no se puede serializar:
    # se descarta al guardar y se crea uno nuevo al restaurar.
    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_lock"]
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = threading.Lock()
        # Snapshot pre-Hito 3: falta el atributo fuera de dataclass.
        if not hasattr(self, "_locks"):
            self._locks = {}

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

    def _walk_to_parent(self, parts: list[str], create: bool = False) -> DirNode:
        current = self._root
        for part in parts:
            child = current.children.get(part)
            if child is None:
                if not create:
                    raise PathNotFoundError(f"no existe: {'/'.join(parts)}")
                child = DirNode()
                current.children[part] = child
            if not isinstance(child, DirNode):
                raise NotADirectoryError(f"no es un directorio: {'/'.join(parts)}")
            current = child
        return current

    def list_dir(self, virtual_path: str) -> list[DirEntryData]:
        with self._lock:
            parts = self._parts(virtual_path)
            node = self._get_node(parts)
            if node is None:
                raise PathNotFoundError(f"no existe: {virtual_path}")
            if not isinstance(node, DirNode):
                raise NotADirectoryError(f"no es un directorio: {virtual_path}")
            entries = []
            for name, child in sorted(node.children.items()):
                if isinstance(child, DirNode):
                    entries.append(DirEntryData(name=name, is_dir=True, size_bytes=0))
                elif child.state == "committed":
                    size = sum(b.size_bytes for b in child.blocks)
                    entries.append(DirEntryData(name=name, is_dir=False, size_bytes=size))
            return entries

    def make_dir(self, virtual_path: str) -> None:
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise PathExistsError("la raíz ya existe")
            parent = self._walk_to_parent(parts[:-1])
            name = parts[-1]
            if name in parent.children:
                raise PathExistsError(f"ya existe: {virtual_path}")
            parent.children[name] = DirNode()

    def remove_dir(self, virtual_path: str) -> None:
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("no se puede borrar la raíz")
            parent = self._walk_to_parent(parts[:-1])
            name = parts[-1]
            node = parent.children.get(name)
            if node is None:
                raise PathNotFoundError(f"no existe: {virtual_path}")
            if not isinstance(node, DirNode):
                raise NotADirectoryError(f"no es un directorio: {virtual_path}")
            if node.children:
                raise NotEmptyError(f"directorio no vacío: {virtual_path}")
            del parent.children[name]

    def remove_file(self, virtual_path: str, now: float | None = None) -> list[BlockRecord]:
        """Devuelve los bloques que tenía el archivo, para que el servicer
        le avise al DataNode que los borre.

        ``now`` es opcional para reproducir journals anteriores a B1."""
        with self._lock:
            parts = self._parts(virtual_path)
            canonical_path = "/" + "/".join(parts)
            if not parts:
                raise InvalidPathError("no se puede borrar la raíz")
            parent = self._walk_to_parent(parts[:-1])
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
    ) -> str:
        """Toma un lock compartido o exclusivo con vencimiento determinado por líder."""
        with self._lock:
            canonical_path = self._canonical_path(virtual_path)
            if mode not in {"r", "w"}:
                raise InvalidPathError(f"modo de lock inválido: {mode!r}")
            if lease_s <= 0:
                raise InvalidPathError("lease_s debe ser mayor a 0")
            self._get_committed_file(canonical_path)
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

    def begin_upload(
        self,
        virtual_path: str,
        placements: list[tuple[str, list[str]]],
        now: float | None = None,
        lease_s: float | None = None,
    ) -> tuple[list[tuple[str, list[str]]], list[BlockRecord]]:
        """placements: (block_id, direcciones de las réplicas en orden de pipeline).
        La política de selección vive en el servicer; el árbol solo la guarda.

        now y lease_s los fija el líder y viajan en el comando replicado: dentro de
        apply() no se puede leer el reloj, cada nodo calcularía una hora distinta.
        Sin ellos (entradas viejas del journal) no hay lease, como antes.

        Devuelve (placements guardados, bloques de una subida pendiente vencida que se
        reemplazó). Los placements: si un reintento con el mismo op_id llega con
        placements nuevos, la respuesta tiene que armarse con ESTOS. Los bloques
        reemplazados los borra el servicer después del commit."""
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("ruta de destino inválida")
            parent = self._walk_to_parent(parts[:-1], create=True)
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
            parent.children[name] = FileNode(state="pending", blocks=blocks, lease_expires_at=expires_at)
            return [(b.block_id, list(b.datanode_addresses)) for b in blocks], stale_blocks

    def confirm_block(
        self,
        virtual_path: str,
        block_id: str,
        checksum: str,
        size_bytes: int,
        now: float | None = None,
        lease_s: float | None = None,
    ) -> None:
        with self._lock:
            node = self._get_pending_file(virtual_path)
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

    def complete_upload(self, virtual_path: str) -> None:
        with self._lock:
            node = self._get_pending_file(virtual_path)
            unconfirmed = [b.block_id for b in node.blocks if not b.confirmed]
            if unconfirmed:
                raise InvalidPathError(f"bloques sin confirmar: {unconfirmed}")
            node.state = "committed"

    def abort_upload(self, virtual_path: str) -> None:
        with self._lock:
            parts = self._parts(virtual_path)
            if not parts:
                raise InvalidPathError("no hay una subida pendiente para la raíz")
            parent = self._walk_to_parent(parts[:-1])
            name = parts[-1]
            node = parent.children.get(name)
            if not isinstance(node, FileNode) or node.state != "pending":
                raise PathNotFoundError(f"no hay una subida pendiente para: {virtual_path}")
            del parent.children[name]

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

    def list_blocks(self, virtual_path: str) -> list[BlockRecord]:
        with self._lock:
            parts = self._parts(virtual_path)
            node = self._get_node(parts)
            if not isinstance(node, FileNode) or node.state != "committed":
                raise PathNotFoundError(f"no existe: {virtual_path}")
            return node.blocks

    def _get_pending_file(self, virtual_path: str) -> FileNode:
        parts = self._parts(virtual_path)
        node = self._get_node(parts)
        if not isinstance(node, FileNode) or node.state != "pending":
            raise PathNotFoundError(f"no hay una subida pendiente para: {virtual_path}")
        return node
