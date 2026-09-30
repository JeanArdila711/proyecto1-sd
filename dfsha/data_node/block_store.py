from __future__ import annotations

import hashlib
import os
import re
import uuid
from pathlib import Path
from typing import Iterable, Iterator

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError

_VALID_BLOCK_ID = re.compile(r"\A[0-9a-f]{32}\Z")


def validate_block_id(block_id: str) -> None:
    """Rechaza identificadores de red antes de que lleguen al filesystem."""
    if not _VALID_BLOCK_ID.match(block_id):
        raise BlockNotFoundError(f"block_id inválido: {block_id!r}")


def _block_path(root: Path, block_id: str) -> Path:
    validate_block_id(block_id)
    return root / block_id


def _checksum_path(root: Path, block_id: str) -> Path:
    return root / f"{block_id}.sha256"


def write_block(root: Path, block_id: str, chunks: Iterable[bytes]) -> tuple[str, int]:
    root.mkdir(parents=True, exist_ok=True)
    target = _block_path(root, block_id)
    tmp_path = root / f"{block_id}.part-{uuid.uuid4().hex}"
    bytes_written = 0
    hasher = hashlib.sha256()
    try:
        with tmp_path.open("wb") as fh:
            for chunk in chunks:
                fh.write(chunk)
                hasher.update(chunk)
                bytes_written += len(chunk)
        os.replace(tmp_path, target)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    checksum = hasher.hexdigest()
    _checksum_path(root, block_id).write_text(checksum)
    return checksum, bytes_written


def read_block(
    root: Path, block_id: str, chunk_size: int, offset: int = 0, length: int | None = None
) -> Iterator[bytes]:
    """Entrega el bloque, o solo `length` bytes desde `offset` (None: hasta el final).

    ponytail: verificar todo el bloque para leer un rango cuesta leerlo entero;
    checksums por chunk si llega a importar."""
    if offset < 0 or (length is not None and length < 0):
        raise ValueError(f"rango inválido: offset={offset}, length={length}")
    target = _block_path(root, block_id)
    checksum_path = _checksum_path(root, block_id)
    if not target.exists() or not checksum_path.exists():
        raise BlockNotFoundError(f"no existe el bloque: {block_id}")

    expected_checksum = checksum_path.read_text().strip()

    hasher = hashlib.sha256()
    with target.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            hasher.update(chunk)
    if hasher.hexdigest() != expected_checksum:
        raise BlockCorruptedError(
            f"checksum no coincide para el bloque {block_id}: "
            f"esperado {expected_checksum}, calculado {hasher.hexdigest()}"
        )

    with target.open("rb") as fh:
        fh.seek(offset)
        remaining = length
        while remaining is None or remaining > 0:
            chunk = fh.read(chunk_size if remaining is None else min(chunk_size, remaining))
            if not chunk:
                return
            if remaining is not None:
                remaining -= len(chunk)
            yield chunk


def delete_block(root: Path, block_id: str) -> None:
    target = _block_path(root, block_id)
    if not target.exists():
        raise BlockNotFoundError(f"no existe el bloque: {block_id}")
    target.unlink()
    _checksum_path(root, block_id).unlink(missing_ok=True)
