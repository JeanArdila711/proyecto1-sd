import pytest

from dfsha.common.exceptions import (
    InvalidPathError,
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)
from dfsha.control_node.tree import ControlTree


def test_list_dir_empty_root():
    tree = ControlTree()
    assert tree.list_dir("/") == []


def test_make_dir_then_list():
    tree = ControlTree()
    tree.make_dir("/documentos")

    entries = tree.list_dir("/")

    assert len(entries) == 1
    assert entries[0].name == "documentos"
    assert entries[0].is_dir is True


def test_make_dir_existing_raises():
    tree = ControlTree()
    tree.make_dir("/documentos")

    with pytest.raises(PathExistsError):
        tree.make_dir("/documentos")


def test_make_dir_nested_without_parent_raises():
    tree = ControlTree()
    with pytest.raises(PathNotFoundError):
        tree.make_dir("/a/b")


def test_list_dir_missing_raises():
    tree = ControlTree()
    with pytest.raises(PathNotFoundError):
        tree.list_dir("/no-existe")


def test_remove_dir_empty():
    tree = ControlTree()
    tree.make_dir("/documentos")

    tree.remove_dir("/documentos")

    assert tree.list_dir("/") == []


def test_remove_dir_not_empty_raises():
    tree = ControlTree()
    tree.make_dir("/documentos")
    tree.make_dir("/documentos/sub")

    with pytest.raises(NotEmptyError):
        tree.remove_dir("/documentos")


def test_remove_dir_on_file_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:1")

    with pytest.raises(NotADirectoryError):
        tree.remove_dir("/archivo.txt")


def test_remove_file_missing_raises():
    tree = ControlTree()
    with pytest.raises(PathNotFoundError):
        tree.remove_file("/no-existe.txt")


def test_remove_file_on_dir_raises():
    tree = ControlTree()
    tree.make_dir("/documentos")

    with pytest.raises(NotAFileError):
        tree.remove_file("/documentos")


def test_remove_file_on_pending_upload_raises_not_found():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")

    with pytest.raises(PathNotFoundError):
        tree.remove_file("/archivo.txt")


def test_begin_upload_creates_pending_entry_not_visible_in_ls():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1", "b2"], "localhost:50061")

    assert tree.list_dir("/") == []  # pendiente, no aparece todavía


def test_begin_upload_existing_path_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")

    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", ["b2"], "localhost:50061")


def test_confirm_block_unknown_block_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")

    with pytest.raises(PathNotFoundError):
        tree.confirm_block("/archivo.txt", "block-que-no-existe", "checksum", 10)


def test_complete_upload_without_all_confirmed_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1", "b2"], "localhost:50061")
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)

    with pytest.raises(InvalidPathError):
        tree.complete_upload("/archivo.txt")


def test_complete_upload_with_all_confirmed_makes_it_visible():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1", "b2"], "localhost:50061")
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)
    tree.confirm_block("/archivo.txt", "b2", "checksum2", 3)

    tree.complete_upload("/archivo.txt")

    entries = tree.list_dir("/")
    assert len(entries) == 1
    assert entries[0].name == "archivo.txt"
    assert entries[0].size_bytes == 8


def test_list_blocks_on_pending_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")

    with pytest.raises(PathNotFoundError):
        tree.list_blocks("/archivo.txt")


def test_list_blocks_on_committed_returns_ordered_blocks():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1", "b2"], "localhost:50061")
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)
    tree.confirm_block("/archivo.txt", "b2", "checksum2", 3)
    tree.complete_upload("/archivo.txt")

    blocks = tree.list_blocks("/archivo.txt")

    assert [b.block_id for b in blocks] == ["b1", "b2"]
    assert blocks[0].checksum == "checksum1"


def test_abort_upload_removes_pending_entry():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")

    tree.abort_upload("/archivo.txt")

    with pytest.raises(PathNotFoundError):
        tree.list_blocks("/archivo.txt")
    with pytest.raises(PathNotFoundError):
        tree.confirm_block("/archivo.txt", "b1", "x", 1)


def test_abort_upload_on_committed_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", ["b1"], "localhost:50061")
    tree.confirm_block("/archivo.txt", "b1", "c", 1)
    tree.complete_upload("/archivo.txt")

    with pytest.raises(PathNotFoundError):
        tree.abort_upload("/archivo.txt")


def test_abort_upload_on_root_raises_invalid_path():
    tree = ControlTree()

    with pytest.raises(InvalidPathError):
        tree.abort_upload("/")
