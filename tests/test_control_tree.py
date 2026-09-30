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
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:1"])])

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
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])

    with pytest.raises(PathNotFoundError):
        tree.remove_file("/archivo.txt")


def test_begin_upload_creates_pending_entry_not_visible_in_ls():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"]), ("b2", ["localhost:50061"])])

    assert tree.list_dir("/") == []  # pendiente, no aparece todavía


def test_begin_upload_existing_path_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])

    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", [("b2", ["localhost:50061"])])


def test_confirm_block_unknown_block_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])

    with pytest.raises(PathNotFoundError):
        tree.confirm_block("/archivo.txt", "block-que-no-existe", "checksum", 10)


def test_complete_upload_without_all_confirmed_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"]), ("b2", ["localhost:50061"])])
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)

    with pytest.raises(InvalidPathError):
        tree.complete_upload("/archivo.txt")


def test_complete_upload_with_all_confirmed_makes_it_visible():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"]), ("b2", ["localhost:50061"])])
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)
    tree.confirm_block("/archivo.txt", "b2", "checksum2", 3)

    tree.complete_upload("/archivo.txt")

    entries = tree.list_dir("/")
    assert len(entries) == 1
    assert entries[0].name == "archivo.txt"
    assert entries[0].size_bytes == 8


def test_list_blocks_on_pending_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])

    with pytest.raises(PathNotFoundError):
        tree.list_blocks("/archivo.txt")


def test_list_blocks_on_committed_returns_ordered_blocks():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"]), ("b2", ["localhost:50061"])])
    tree.confirm_block("/archivo.txt", "b1", "checksum1", 5)
    tree.confirm_block("/archivo.txt", "b2", "checksum2", 3)
    tree.complete_upload("/archivo.txt")

    blocks = tree.list_blocks("/archivo.txt")

    assert [b.block_id for b in blocks] == ["b1", "b2"]
    assert blocks[0].checksum == "checksum1"


def test_abort_upload_removes_pending_entry():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])

    tree.abort_upload("/archivo.txt")

    with pytest.raises(PathNotFoundError):
        tree.list_blocks("/archivo.txt")
    with pytest.raises(PathNotFoundError):
        tree.confirm_block("/archivo.txt", "b1", "x", 1)


def test_abort_upload_on_committed_raises():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["localhost:50061"])])
    tree.confirm_block("/archivo.txt", "b1", "c", 1)
    tree.complete_upload("/archivo.txt")

    with pytest.raises(PathNotFoundError):
        tree.abort_upload("/archivo.txt")


def test_abort_upload_on_root_raises_invalid_path():
    tree = ControlTree()

    with pytest.raises(InvalidPathError):
        tree.abort_upload("/")


# ── lease de subidas pendientes ─────────────────────────────────────────────


def test_pending_upload_with_live_lease_still_blocks_the_name():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["dn1"])], now=100.0, lease_s=60.0)
    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", [("b2", ["dn1"])], now=159.0, lease_s=60.0)


def test_expired_pending_upload_is_replaced_and_its_blocks_returned():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["dn1", "dn2"])], now=100.0, lease_s=60.0)

    placements, stale = tree.begin_upload("/archivo.txt", [("b2", ["dn1"])], now=160.0, lease_s=60.0)

    assert placements == [("b2", ["dn1"])]
    assert [(b.block_id, b.datanode_addresses) for b in stale] == [("b1", ["dn1", "dn2"])]
    tree.confirm_block("/archivo.txt", "b2", "sum", 1, now=161.0, lease_s=60.0)
    tree.complete_upload("/archivo.txt")
    assert [b.block_id for b in tree.list_blocks("/archivo.txt")] == ["b2"]


def test_confirm_block_renews_the_lease():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["dn1"]), ("b2", ["dn1"])], now=100.0, lease_s=60.0)
    tree.confirm_block("/archivo.txt", "b1", "sum", 1, now=150.0, lease_s=60.0)  # vence a los 210

    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", [("b3", ["dn1"])], now=200.0, lease_s=60.0)


def test_committed_file_is_never_replaced_even_if_its_lease_expired():
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["dn1"])], now=100.0, lease_s=1.0)
    tree.confirm_block("/archivo.txt", "b1", "sum", 1)
    tree.complete_upload("/archivo.txt")
    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", [("b2", ["dn1"])], now=10_000.0, lease_s=1.0)


def test_pending_upload_without_lease_never_expires():
    # entradas del journal escritas antes de existir el lease: sin now ni lease_s
    tree = ControlTree()
    tree.begin_upload("/archivo.txt", [("b1", ["dn1"])])
    with pytest.raises(PathExistsError):
        tree.begin_upload("/archivo.txt", [("b2", ["dn1"])], now=10_000.0, lease_s=1.0)


# ── locks lectores/escritor con lease ───────────────────────────────────────


def _committed_file(tree: ControlTree, path: str = "/archivo.txt") -> str:
    tree.begin_upload(path, [("b1", ["dn1"])])
    tree.confirm_block(path, "b1", "sum", 1)
    tree.complete_upload(path)
    return path


def test_two_readers_coexist_and_writer_conflicts():
    from dfsha.common.exceptions import ConflictError

    tree = ControlTree()
    path = _committed_file(tree)
    tree.acquire_lock(path, "reader-1", "client-1", "r", now=100.0, lease_s=60.0)
    tree.acquire_lock(path, "reader-2", "client-2", "r", now=101.0, lease_s=60.0)

    assert tree._locks[path].mode == "r"
    assert set(tree._locks[path].holders) == {"reader-1", "reader-2"}
    with pytest.raises(ConflictError):
        tree.acquire_lock(path, "writer", "client-3", "w", now=102.0, lease_s=60.0)


def test_writer_blocks_reader_and_release_makes_lock_available():
    from dfsha.common.exceptions import ConflictError

    tree = ControlTree()
    path = _committed_file(tree)
    tree.acquire_lock(path, "writer", "client-1", "w", now=100.0, lease_s=60.0)

    with pytest.raises(ConflictError):
        tree.acquire_lock(path, "reader", "client-2", "r", now=101.0, lease_s=60.0)

    tree.release_lock(path, "writer")
    tree.acquire_lock(path, "reader", "client-2", "r", now=102.0, lease_s=60.0)


def test_expired_lock_is_reused_and_renewal_extends_lease():
    tree = ControlTree()
    path = _committed_file(tree)
    tree.acquire_lock(path, "old", "client-1", "r", now=100.0, lease_s=10.0)
    tree.acquire_lock(path, "writer", "client-2", "w", now=110.0, lease_s=10.0)
    tree.renew_lock(path, "writer", now=115.0, lease_s=10.0)

    assert tree._locks[path].holders["writer"] == ("client-2", 125.0)
    from dfsha.common.exceptions import ConflictError
    with pytest.raises(ConflictError):
        tree.acquire_lock(path, "reader", "client-3", "r", now=124.0, lease_s=10.0)


def test_foreign_lock_id_and_remove_with_live_lock_fail():
    from dfsha.common.exceptions import ConflictError

    tree = ControlTree()
    path = _committed_file(tree)
    tree.acquire_lock(path, "reader", "client-1", "r", now=100.0, lease_s=60.0)

    with pytest.raises(ConflictError):
        tree.renew_lock(path, "other", now=101.0, lease_s=60.0)
    with pytest.raises(ConflictError):
        tree.release_lock(path, "other")
    with pytest.raises(ConflictError):
        tree.remove_file(path, now=101.0)


def test_lock_operations_require_a_committed_file():
    tree = ControlTree()
    tree.begin_upload("/pending.txt", [("b1", ["dn1"])])

    with pytest.raises(PathNotFoundError):
        tree.acquire_lock("/pending.txt", "reader", "client", "r", now=1.0, lease_s=1.0)
    with pytest.raises(InvalidPathError):
        tree.acquire_lock("/", "reader", "client", "bad", now=1.0, lease_s=1.0)


# ── regresiones de claves canónicas de locks ─────────────────────────────────


def test_lock_path_variants_conflict_on_the_same_committed_file():
    from dfsha.common.exceptions import ConflictError

    tree = ControlTree()
    _committed_file(tree, "/docs/a.txt")
    tree.acquire_lock("/docs/a.txt", "reader", "client-1", "r", now=100.0, lease_s=60.0)

    with pytest.raises(ConflictError):
        tree.acquire_lock("docs//a.txt", "writer", "client-2", "w", now=101.0, lease_s=60.0)

    assert set(tree._locks) == {"/docs/a.txt"}


def test_remove_path_variant_respects_live_lock():
    from dfsha.common.exceptions import ConflictError

    tree = ControlTree()
    _committed_file(tree, "/docs/a.txt")
    tree.acquire_lock("docs/a.txt", "reader", "client-1", "r", now=100.0, lease_s=60.0)

    with pytest.raises(ConflictError):
        tree.remove_file("/docs//a.txt", now=101.0)



def test_renew_and_release_path_variants_use_the_canonical_lock_key():
    tree = ControlTree()
    _committed_file(tree, "/docs/a.txt")
    tree.acquire_lock("/docs//a.txt", "reader", "client-1", "r", now=100.0, lease_s=10.0)

    tree.renew_lock("docs/a.txt", "reader", now=105.0, lease_s=10.0)
    assert tree._locks["/docs/a.txt"].holders["reader"] == ("client-1", 115.0)

    tree.release_lock("/docs//a.txt", "reader")
    assert tree._locks == {}


def test_release_of_expired_lock_is_idempotent_with_leader_time():
    tree = ControlTree()
    path = _committed_file(tree)
    tree.acquire_lock(path, "reader", "client-1", "r", now=100.0, lease_s=10.0)

    tree.release_lock(path, "reader", now=110.0)
    tree.release_lock(path, "reader", now=111.0)

    assert tree._locks == {}
