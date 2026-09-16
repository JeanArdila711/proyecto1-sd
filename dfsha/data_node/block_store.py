from __future__ import annotations

import hashlib
import os
import re
import uuid
from pathlib import Path
from typing import Iterable, Iterator

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError

_VALID_BLOCK_ID = re.compile(r"\A[0-9a-f]{32}\Z")


def _block_path(root: Path, block_id: str) -> Path:
    if not _VALID_BLOCK_ID.match(block_id):
        raise BlockNotFoundError(f"block_id inválido: {block_id!r}")
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


def read_block(root: Path, block_id: str, chunk_size: int) -> Iterator[bytes]:
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
        yield from iter(lambda: fh.read(chunk_size), b"")


def delete_block(root: Path, block_id: str) -> None:
    target = _block_path(root, block_id)
    if not target.exists():
        raise BlockNotFoundError(f"no existe el bloque: {block_id}")
    target.unlink()
    _checksum_path(root, block_id).unlink(missing_ok=True)
