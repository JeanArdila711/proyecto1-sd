from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from dfsha.common.exceptions import (
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


def write_file_chunks(root: Path, virtual_path: str, chunks: Iterable[bytes]) -> int:
    target = resolve_path(root, virtual_path)
    if target == resolve_path(root, "/"):
        raise InvalidPathError(f"ruta de destino inválida: {virtual_path!r}")
    if target.is_dir():
        raise NotAFileError(f"no es un archivo: {virtual_path}")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.parent / f"{target.name}.part-{uuid.uuid4().hex}"
    bytes_written = 0
    try:
        with tmp_path.open("wb") as fh:
            for chunk in chunks:
                fh.write(chunk)
                bytes_written += len(chunk)
        os.replace(tmp_path, target)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return bytes_written


def read_file_chunks(root: Path, virtual_path: str, chunk_size: int) -> Iterator[bytes]:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_file():
        raise NotAFileError(f"no es un archivo: {virtual_path}")
    with target.open("rb") as fh:
        while True:
            data = fh.read(chunk_size)
            if not data:
                break
            yield data
