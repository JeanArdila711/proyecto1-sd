from __future__ import annotations

import errno
import os

import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.common.exceptions import BlockCorruptedError
from dfsha.data_node import block_store

BLOCK_ID = "1234567890abcdef1234567890abcdef"
CHUNK_SIZE = 1024 * 1024
KNOWN_PLAINTEXT = b"DFSha plaintext marker: never stored on disk."


def _write(root, data: bytes) -> None:
    block_store.write_block(root, TEST_ENCRYPTION_KEY, BLOCK_ID, [data])


@pytest.mark.parametrize("size", [CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 1, CHUNK_SIZE * 2])
def test_encrypted_container_round_trips_boundary_sizes(tmp_path, size):
    data = (b"a" * size) if size else b""

    _write(tmp_path, data)

    assert b"".join(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, 65537)) == data
    assert (tmp_path / BLOCK_ID).read_bytes().startswith(b"DFSE1")
    assert not (tmp_path / f"{BLOCK_ID}.sha256").exists()


def test_container_does_not_contain_known_plaintext(tmp_path):
    _write(tmp_path, KNOWN_PLAINTEXT * 32)

    assert KNOWN_PLAINTEXT not in (tmp_path / BLOCK_ID).read_bytes()


@pytest.mark.parametrize("mutation", ["header", "metadata", "ciphertext", "tag"])
def test_tampered_container_is_rejected(tmp_path, mutation):
    _write(tmp_path, b"x" * (CHUNK_SIZE + 1))
    path = tmp_path / BLOCK_ID
    raw = bytearray(path.read_bytes())
    positions = {
        "header": 5,
        "ciphertext": 32,
        "tag": 32 + CHUNK_SIZE,
        "metadata": len(raw) - 1,
    }
    raw[positions[mutation]] ^= 0x01
    path.write_bytes(raw)

    with pytest.raises(BlockCorruptedError):
        b"".join(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE))


def test_missing_last_full_chunk_is_rejected(tmp_path):
    _write(tmp_path, b"x" * (CHUNK_SIZE * 2))
    path = tmp_path / BLOCK_ID
    raw = path.read_bytes()
    path.write_bytes(raw[: -(CHUNK_SIZE + 16 + 64)])

    with pytest.raises(BlockCorruptedError):
        list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE))


def test_partial_truncation_is_rejected(tmp_path):
    _write(tmp_path, b"x" * (CHUNK_SIZE + 1))
    path = tmp_path / BLOCK_ID
    path.write_bytes(path.read_bytes()[:-3])

    with pytest.raises(BlockCorruptedError):
        list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE))


def test_wrong_key_is_rejected_without_yielding_data(tmp_path):
    _write(tmp_path, b"private data")

    with pytest.raises(BlockCorruptedError):
        list(block_store.read_block(tmp_path, b"z" * 32, BLOCK_ID, CHUNK_SIZE))


def test_range_crosses_encrypted_chunks(tmp_path):
    data = b"a" * (CHUNK_SIZE - 2) + b"WXYZ" + b"b" * 10
    _write(tmp_path, data)

    assert b"".join(
        block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, 17, CHUNK_SIZE - 2, 4)
    ) == b"WXYZ"


def test_range_inside_exact_chunk_multiple(tmp_path):
    data = b"a" * CHUNK_SIZE + b"b" * CHUNK_SIZE
    _write(tmp_path, data)

    assert b"".join(
        block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, 64, CHUNK_SIZE + 12, 44)
    ) == b"b" * 44


def test_plaintext_legacy_block_is_rejected(tmp_path):
    (tmp_path / BLOCK_ID).write_bytes(b"legacy plaintext")

    with pytest.raises(BlockCorruptedError, match="DFSE1"):
        list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE))


def test_empty_block_round_trips_with_authenticated_framing(tmp_path):
    _write(tmp_path, b"")

    assert list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE)) == []


def test_failed_replacement_keeps_previous_container_and_removes_temp(tmp_path, monkeypatch):
    _write(tmp_path, b"old")
    target = tmp_path / BLOCK_ID
    before = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("simulated crash before publication")

    monkeypatch.setattr(block_store.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated crash"):
        _write(tmp_path, b"new")

    assert target.read_bytes() == before
    assert list(tmp_path.glob("*.part-*")) == []


def test_list_blocks_reports_logical_size_and_datanode_age(tmp_path):
    _write(tmp_path, b"logical bytes")
    os.utime(tmp_path / BLOCK_ID, (100.0, 100.0))

    assert list(block_store.list_blocks(tmp_path, TEST_ENCRYPTION_KEY, now=103.5)) == [
        (BLOCK_ID, len(b"logical bytes"), 3.5)
    ]


def test_load_encryption_key_rejects_missing_or_wrong_length(tmp_path):
    with pytest.raises(RuntimeError, match="no existe la llave"):
        block_store.load_encryption_key(tmp_path / "missing.key")

    short_key = tmp_path / "short.key"
    short_key.write_bytes(b"short")
    with pytest.raises(RuntimeError, match="exactamente 32 bytes"):
        block_store.load_encryption_key(short_key)


def test_load_encryption_key_loads_exactly_32_raw_bytes(tmp_path):
    key_file = tmp_path / "key"
    key_file.write_bytes(TEST_ENCRYPTION_KEY)

    assert block_store.load_encryption_key(key_file) == TEST_ENCRYPTION_KEY


def test_list_blocks_survives_a_corrupted_block(tmp_path):
    # Un bloque dañado no puede tumbar el inventario: el recolector de A3 dejaría de
    # ver los huérfanos de todo el DataNode. Se lista con su tamaño físico.
    other = "f" * 32
    _write(tmp_path, b"sano")
    block_store.write_block(tmp_path, TEST_ENCRYPTION_KEY, other, [b"se va a romper"])
    (tmp_path / other).write_bytes(b"basura")

    listed = {block_id: size for block_id, size, _ in block_store.list_blocks(tmp_path, TEST_ENCRYPTION_KEY)}

    assert listed == {BLOCK_ID: 4, other: len(b"basura")}


def test_block_deleted_before_metadata_read_is_not_found(tmp_path, monkeypatch):
    # Un borrado concurrente es NOT_FOUND, no DATA_LOSS: DATA_LOSS marca al origen
    # como corrupto para el re-replicador.
    from dfsha.common.exceptions import BlockNotFoundError

    _write(tmp_path, b"efimero")
    real_stat = type(tmp_path).stat

    def vanished(self, *args, **kwargs):
        if self.name == BLOCK_ID:
            # con errno, como el error real: Path.exists() de Python 3.12 solo ignora ENOENT
            raise FileNotFoundError(errno.ENOENT, "No such file or directory", str(self))
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(type(tmp_path), "stat", vanished)
    with pytest.raises(BlockNotFoundError):
        list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, CHUNK_SIZE))


def test_read_block_honors_chunk_size(tmp_path):
    _write(tmp_path, b"0123456789")

    pieces = list(block_store.read_block(tmp_path, TEST_ENCRYPTION_KEY, BLOCK_ID, 4))

    assert pieces == [b"0123", b"4567", b"89"]
