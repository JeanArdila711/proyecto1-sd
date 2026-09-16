import hashlib
from pathlib import Path

import pytest

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError
from dfsha.data_node.block_store import delete_block, read_block, write_block

BLOCK_ID = "1234567890abcdef1234567890abcdef"  # formato real: uuid4().hex (32 hex)


def test_write_block_writes_file_and_returns_checksum(tmp_path):
    checksum, bytes_written = write_block(tmp_path, BLOCK_ID, [b"hello ", b"world"])

    assert bytes_written == 11
    assert (tmp_path / BLOCK_ID).read_bytes() == b"hello world"
    assert checksum == hashlib.sha256(b"hello world").hexdigest()


def test_write_block_creates_root_if_missing(tmp_path):
    root = tmp_path / "blocks"

    write_block(root, BLOCK_ID, [b"data"])

    assert (root / BLOCK_ID).exists()


def test_write_block_cleans_up_temp_on_failure(tmp_path):
    def broken_chunks():
        yield b"partial"
        raise RuntimeError("network died")

    with pytest.raises(RuntimeError):
        write_block(tmp_path, BLOCK_ID, broken_chunks())

    assert not (tmp_path / BLOCK_ID).exists()
    assert list(tmp_path.glob("*.part-*")) == []


def test_read_block_returns_written_bytes(tmp_path):
    write_block(tmp_path, BLOCK_ID, [b"hello world"])

    result = b"".join(read_block(tmp_path, BLOCK_ID, chunk_size=4))

    assert result == b"hello world"


def test_read_block_missing_raises(tmp_path):
    with pytest.raises(BlockNotFoundError):
        list(read_block(tmp_path, BLOCK_ID, chunk_size=4))


def test_read_block_corrupted_raises(tmp_path):
    write_block(tmp_path, BLOCK_ID, [b"hello world"])
    (tmp_path / BLOCK_ID).write_bytes(b"datos corruptos")

    with pytest.raises(BlockCorruptedError):
        list(read_block(tmp_path, BLOCK_ID, chunk_size=4))


def test_delete_block_removes_files(tmp_path):
    write_block(tmp_path, BLOCK_ID, [b"data"])

    delete_block(tmp_path, BLOCK_ID)

    assert not (tmp_path / BLOCK_ID).exists()
    assert not (tmp_path / f"{BLOCK_ID}.sha256").exists()


def test_delete_block_missing_raises(tmp_path):
    with pytest.raises(BlockNotFoundError):
        delete_block(tmp_path, BLOCK_ID)


@pytest.mark.parametrize("bad_block_id", ["/tmp/pwned", "../pwned", "../../etc/passwd"])
def test_write_block_rejects_path_like_block_id(tmp_path, bad_block_id):
    with pytest.raises(BlockNotFoundError):
        write_block(tmp_path, bad_block_id, [b"data"])

    # nada se escribió fuera (ni dentro) del root
    assert list(tmp_path.rglob("*")) == []
    assert not Path("/tmp/pwned").exists()
