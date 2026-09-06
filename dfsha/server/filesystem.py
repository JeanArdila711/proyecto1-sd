from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dfsha.server.exceptions import (
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)


def resolve_path(root: Path, virtual_path: str) -> Path:
    root = root.resolve()
    relative = virtual_path.lstrip("/")
    candidate = (root / relative).resolve()
    if candidate != root and root not in candidate.parents:
        raise InvalidPathError(f"la ruta sale de la raíz: {virtual_path!r}")
    return candidate


@dataclass(frozen=True)
class DirEntryData:
    name: str
    is_dir: bool
    size_bytes: int


def list_dir(root: Path, virtual_path: str) -> list[DirEntryData]:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_dir():
        raise NotADirectoryError(f"no es un directorio: {virtual_path}")
    entries = []
    for child in sorted(target.iterdir(), key=lambda p: p.name):
        is_dir = child.is_dir()
        size = 0 if is_dir else child.stat().st_size
        entries.append(DirEntryData(name=child.name, is_dir=is_dir, size_bytes=size))
    return entries


def make_dir(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if target.exists():
        raise PathExistsError(f"ya existe: {virtual_path}")
    target.mkdir(parents=True)


def remove_dir(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_dir():
        raise NotADirectoryError(f"no es un directorio: {virtual_path}")
    try:
        target.rmdir()
    except OSError as exc:
        raise NotEmptyError(f"directorio no vacío: {virtual_path}") from exc


def remove_file(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_file():
        raise NotAFileError(f"no es un archivo: {virtual_path}")
    target.unlink()
