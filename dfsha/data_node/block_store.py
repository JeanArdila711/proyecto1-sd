from __future__ import annotations

import hashlib
import os
import re
import struct
import time
import uuid
from pathlib import Path
from typing import Iterable, Iterator

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError

_VALID_BLOCK_ID = re.compile(r"\A[0-9a-f]{32}\Z")
_MAGIC = b"DFSE1"
_FORMAT_VERSION = 1
_CHUNK_SIZE_BYTES = 1024 * 1024
_TAG_SIZE_BYTES = 16
_HEADER = struct.Struct(">5sB8s")
_METADATA = struct.Struct(">QQ32s")
_METADATA_RECORD_SIZE = _METADATA.size + _TAG_SIZE_BYTES
_MAX_CHUNKS = 2**32 - 1


def validate_block_id(block_id: str) -> None:
    """Rechaza identificadores de red antes de que lleguen al filesystem."""
    if not _VALID_BLOCK_ID.match(block_id):
        raise BlockNotFoundError(f"block_id inválido: {block_id!r}")


def load_encryption_key(key_file: Path) -> bytes:
    """Carga una llave AES-256-GCM cruda de exactamente 32 bytes desde disco."""
    try:
        key = key_file.read_bytes()
    except FileNotFoundError as exc:
        raise RuntimeError(f"no existe la llave de cifrado: {key_file}") from exc
    if len(key) != 32:
        raise RuntimeError(
            f"la llave de cifrado debe tener exactamente 32 bytes, recibió {len(key)}: {key_file}"
        )
    return key


def _validate_key(key: bytes) -> AESGCM:
    if len(key) != 32:
        raise ValueError(f"la llave AES-256-GCM debe tener 32 bytes, recibió {len(key)}")
    return AESGCM(key)


def _block_path(root: Path, block_id: str) -> Path:
    validate_block_id(block_id)
    return root / block_id


def _nonce(prefix: bytes, counter: int) -> bytes:
    if not 0 <= counter <= _MAX_CHUNKS:
        raise ValueError("el bloque excede el máximo de chunks del formato DFSE1")
    return prefix + counter.to_bytes(4, "big")


def _chunk_aad(block_id: str, index: int, is_last: bool) -> bytes:
    return block_id.encode("ascii") + index.to_bytes(4, "big") + bytes((is_last,))


def _metadata_aad(block_id: str, header: bytes) -> bytes:
    return block_id.encode("ascii") + b"metadata" + header


def _container_error(block_id: str, detail: str) -> BlockCorruptedError:
    return BlockCorruptedError(f"contenedor DFSE1 inválido para el bloque {block_id}: {detail}")


def _read_metadata(target: Path, key: bytes, block_id: str) -> tuple[bytes, int, int, bytes]:
    """Valida header y metadata autenticada antes de localizar chunks de rango."""
    try:
        size_on_disk = target.stat().st_size
        if size_on_disk < _HEADER.size + _METADATA_RECORD_SIZE:
            raise _container_error(block_id, "truncado antes de metadata")
        with target.open("rb") as fh:
            header = fh.read(_HEADER.size)
            if len(header) != _HEADER.size:
                raise _container_error(block_id, "encabezado truncado")
            magic, version, prefix = _HEADER.unpack(header)
            if magic != _MAGIC:
                raise _container_error(block_id, "magic ausente; no se admiten bloques legacy/plaintext")
            if version != _FORMAT_VERSION:
                raise _container_error(block_id, f"versión no soportada: {version}")
            fh.seek(-_METADATA_RECORD_SIZE, os.SEEK_END)
            encrypted_metadata = fh.read(_METADATA_RECORD_SIZE)
        metadata = _validate_key(key).decrypt(
            _nonce(prefix, _MAX_CHUNKS), encrypted_metadata, _metadata_aad(block_id, header)
        )
        if len(metadata) != _METADATA.size:
            raise _container_error(block_id, "metadata con tamaño inválido")
        logical_size, chunk_count, checksum = _METADATA.unpack(metadata)
        expected_chunks = 0 if logical_size == 0 else (logical_size - 1) // _CHUNK_SIZE_BYTES + 1
        if chunk_count != expected_chunks:
            raise _container_error(block_id, "conteo de chunks no coincide con el tamaño lógico")
        if chunk_count > _MAX_CHUNKS:
            raise _container_error(block_id, "conteo de chunks fuera del formato")
        last_plaintext_size = 0 if not chunk_count else (
            logical_size - _CHUNK_SIZE_BYTES * (chunk_count - 1)
        )
        expected_size = (
            _HEADER.size
            + max(0, chunk_count - 1) * (_CHUNK_SIZE_BYTES + _TAG_SIZE_BYTES)
            + (last_plaintext_size + _TAG_SIZE_BYTES if chunk_count else 0)
            + _METADATA_RECORD_SIZE
        )
        if size_on_disk != expected_size:
            raise _container_error(block_id, "framing truncado o con bytes extra")
        return prefix, logical_size, chunk_count, checksum
    except InvalidTag as exc:
        raise _container_error(block_id, "tag inválido o llave incorrecta") from exc
    except FileNotFoundError as exc:
        raise BlockNotFoundError(f"no existe el bloque: {block_id}") from exc
    except (OSError, struct.error) as exc:
        raise _container_error(block_id, str(exc)) from exc


def _encrypt_chunk(
    cipher: AESGCM, prefix: bytes, block_id: str, index: int, is_last: bool, plaintext: bytes
) -> bytes:
    return cipher.encrypt(_nonce(prefix, index), plaintext, _chunk_aad(block_id, index, is_last))


def write_block(root: Path, key: bytes, block_id: str, chunks: Iterable[bytes]) -> tuple[str, int]:
    """Cifra chunks de texto plano a un contenedor DFSE1 y lo publica atómicamente."""
    cipher = _validate_key(key)
    root.mkdir(parents=True, exist_ok=True)
    target = _block_path(root, block_id)
    tmp_path = root / f"{block_id}.part-{uuid.uuid4().hex}"
    prefix = os.urandom(8)
    header = _HEADER.pack(_MAGIC, _FORMAT_VERSION, prefix)
    bytes_written = 0
    chunk_count = 0
    hasher = hashlib.sha256()
    pending = bytearray()

    def flush(fh, plaintext: bytes, is_last: bool) -> None:
        nonlocal chunk_count
        if chunk_count >= _MAX_CHUNKS:
            raise ValueError("el bloque excede el máximo de chunks del formato DFSE1")
        fh.write(_encrypt_chunk(cipher, prefix, block_id, chunk_count, is_last, plaintext))
        chunk_count += 1

    try:
        with tmp_path.open("wb") as fh:
            fh.write(header)
            for incoming in chunks:
                pending.extend(incoming)
                bytes_written += len(incoming)
                hasher.update(incoming)
                while len(pending) > _CHUNK_SIZE_BYTES:
                    flush(fh, bytes(pending[:_CHUNK_SIZE_BYTES]), False)
                    del pending[:_CHUNK_SIZE_BYTES]
            if pending:
                flush(fh, bytes(pending), True)
            metadata = _METADATA.pack(bytes_written, chunk_count, hasher.digest())
            fh.write(
                cipher.encrypt(
                    _nonce(prefix, _MAX_CHUNKS), metadata, _metadata_aad(block_id, header)
                )
            )
        os.replace(tmp_path, target)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return hasher.hexdigest(), bytes_written


def read_block(
    root: Path,
    key: bytes,
    block_id: str,
    chunk_size: int,
    offset: int = 0,
    length: int | None = None,
) -> Iterator[bytes]:
    """Entrega texto plano desde un contenedor DFSE1, opcionalmente en un rango.

    Cada chunk servido se autentica con AES-256-GCM y la metadata autenticada fija
    tamaño lógico y conteo. No se relee el bloque completo para un rango. El checksum
    SHA-256 lógico permanece en la metadata y se devuelve al ControlNode al escribir.
    """
    if offset < 0 or (length is not None and length < 0):
        raise ValueError(f"rango inválido: offset={offset}, length={length}")
    target = _block_path(root, block_id)
    if not target.exists():
        raise BlockNotFoundError(f"no existe el bloque: {block_id}")
    prefix, logical_size, chunk_count, _checksum = _read_metadata(target, key, block_id)
    if offset >= logical_size or length == 0:
        return
    end = logical_size if length is None else min(logical_size, offset + length)
    first_index = offset // _CHUNK_SIZE_BYTES
    last_index = (end - 1) // _CHUNK_SIZE_BYTES
    cipher = _validate_key(key)
    try:
        with target.open("rb") as fh:
            for index in range(first_index, last_index + 1):
                plaintext_size = (
                    _CHUNK_SIZE_BYTES
                    if index < chunk_count - 1
                    else logical_size - _CHUNK_SIZE_BYTES * (chunk_count - 1)
                )
                encrypted_size = plaintext_size + _TAG_SIZE_BYTES
                encrypted_offset = _HEADER.size + index * (_CHUNK_SIZE_BYTES + _TAG_SIZE_BYTES)
                fh.seek(encrypted_offset)
                encrypted = fh.read(encrypted_size)
                if len(encrypted) != encrypted_size:
                    raise _container_error(block_id, "chunk truncado")
                plaintext = cipher.decrypt(
                    _nonce(prefix, index), encrypted, _chunk_aad(block_id, index, index == chunk_count - 1)
                )
                if len(plaintext) != plaintext_size:
                    raise _container_error(block_id, "tamaño de chunk autenticado inválido")
                start_in_chunk = offset - index * _CHUNK_SIZE_BYTES if index == first_index else 0
                end_in_chunk = end - index * _CHUNK_SIZE_BYTES if index == last_index else plaintext_size
                for piece_start in range(start_in_chunk, end_in_chunk, chunk_size):
                    yield plaintext[piece_start : min(piece_start + chunk_size, end_in_chunk)]
    except InvalidTag as exc:
        raise _container_error(block_id, "tag inválido o llave incorrecta") from exc
    except FileNotFoundError as exc:
        raise BlockNotFoundError(f"no existe el bloque: {block_id}") from exc
    except OSError as exc:
        raise _container_error(block_id, str(exc)) from exc


def delete_block(root: Path, block_id: str) -> None:
    target = _block_path(root, block_id)
    if not target.exists():
        raise BlockNotFoundError(f"no existe el bloque: {block_id}")
    target.unlink()


def list_blocks(root: Path, key: bytes, now: float | None = None) -> Iterator[tuple[str, int, float]]:
    """Devuelve ``(block_id, tamaño lógico, edad en segundos)`` de cada DFSE1.

    El tamaño es plaintext lógico, no el tamaño físico del contenedor, salvo en un
    contenedor inválido: ese se lista igual con su tamaño físico, porque un bloque
    dañado no puede esconderle al recolector de A3 el resto del inventario. La edad
    la calcula el DataNode con su reloj mediante el mtime, para que A3 no compare
    relojes de máquinas distintas. Archivos temporales `.part-` se ignoran.
    """
    if not root.exists():
        return
    now = time.time() if now is None else now
    for entry in root.iterdir():
        if not _VALID_BLOCK_ID.match(entry.name):
            continue
        try:
            stat = entry.stat()
            _, size, _, _ = _read_metadata(entry, key, entry.name)
        except (FileNotFoundError, BlockNotFoundError):
            continue  # se borró entre el listado y la lectura
        except BlockCorruptedError:
            size = stat.st_size
        yield entry.name, size, max(0.0, now - stat.st_mtime)
