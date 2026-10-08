"""Usuarios en el estado replicado (Hito 3, C2)."""

import pickle

import pytest

from dfsha.common.exceptions import InvalidPathError, PathExistsError, PathNotFoundError
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.tree import ControlTree, UserRecord

HASH = b"h" * 32
SALT = b"s" * 16
OTHER_HASH = b"H" * 32
OTHER_SALT = b"S" * 16


def test_tree_starts_without_users():
    tree = ControlTree()

    assert not tree.has_users()
    assert tree.get_user("alice") is None


def test_create_user_stores_the_record():
    tree = ControlTree()

    tree.create_user("alice", HASH, SALT, ("alice", "docentes"), True)

    assert tree.has_users()
    assert tree.get_user("alice") == UserRecord(HASH, SALT, ("alice", "docentes"), True)


def test_duplicate_user_is_rejected_and_keeps_the_original_hash():
    tree = ControlTree()
    tree.create_user("alice", HASH, SALT)

    with pytest.raises(PathExistsError):
        tree.create_user("alice", OTHER_HASH, OTHER_SALT, (), True)

    assert tree.get_user("alice") == UserRecord(HASH, SALT, (), False)


@pytest.mark.parametrize("username", ["", "Admin", "a b", "../x", "a" * 33, "ñandú", "9lives", None, 7])
def test_invalid_usernames_are_rejected(username):
    tree = ControlTree()

    with pytest.raises(InvalidPathError):
        tree.create_user(username, HASH, SALT)

    assert not tree.has_users()


def test_invalid_group_is_rejected():
    tree = ControlTree()

    with pytest.raises(InvalidPathError):
        tree.create_user("alice", HASH, SALT, ("alice", "Grupo Malo"))

    assert not tree.has_users()


def test_change_password_replaces_hash_and_keeps_privileges():
    tree = ControlTree()
    tree.create_user("alice", HASH, SALT, ("alice", "docentes"), True)

    tree.change_password("alice", OTHER_HASH, OTHER_SALT)

    assert tree.get_user("alice") == UserRecord(OTHER_HASH, OTHER_SALT, ("alice", "docentes"), True)


def test_change_password_of_unknown_user_is_rejected():
    tree = ControlTree()

    with pytest.raises(PathNotFoundError):
        tree.change_password("nadie", HASH, SALT)


def test_users_survive_a_snapshot_round_trip():
    tree = ControlTree()
    tree.create_user("alice", HASH, SALT, ("alice",), True)

    restored = pickle.loads(pickle.dumps(tree))

    assert restored.get_user("alice") == UserRecord(HASH, SALT, ("alice",), True)


def test_snapshot_from_before_c2_restores_with_no_users():
    tree = ControlTree()
    tree.make_dir("/docs")
    state = tree.__getstate__()
    del state["_users"]  # así se ve un snapshot anterior a C2

    restored = ControlTree.__new__(ControlTree)
    restored.__setstate__(state)

    assert not restored.has_users()
    assert [entry.name for entry in restored.list_dir("/")] == ["docs"]


# --- por el log replicado -----------------------------------------------------------------
# ReplicatedTree.apply se llama con _doApply=True: aplica local, sin clúster Raft.


def _apply(replicated: ReplicatedTree, op_id: str, method: str, *args):
    return replicated.apply(op_id, method, args, _doApply=True)


def test_user_mutations_go_through_the_replicated_log():
    replicated = ReplicatedTree()

    assert _apply(replicated, "op-1", "create_user", "alice", HASH, SALT, ["alice"], False) == ("ok", None)
    assert _apply(replicated, "op-2", "change_password", "alice", OTHER_HASH, OTHER_SALT) == ("ok", None)

    assert replicated.tree.get_user("alice") == UserRecord(OTHER_HASH, OTHER_SALT, ("alice",), False)


def test_retry_with_the_same_op_id_keeps_the_first_hash():
    # Un reintento del líder calcula otra sal; gana el primer comando aplicado.
    replicated = ReplicatedTree()
    _apply(replicated, "op-1", "create_user", "alice", HASH, SALT)

    assert _apply(replicated, "op-1", "create_user", "alice", OTHER_HASH, OTHER_SALT) == ("ok", None)

    assert replicated.tree.get_user("alice").password_hash == HASH


def test_replicated_errors_are_outcomes_not_exceptions():
    replicated = ReplicatedTree()
    _apply(replicated, "op-1", "create_user", "alice", HASH, SALT)

    duplicate = _apply(replicated, "op-2", "create_user", "alice", OTHER_HASH, OTHER_SALT)
    missing_args = _apply(replicated, "op-3", "create_user", "bob")
    unknown_user = _apply(replicated, "op-4", "change_password", "nadie", HASH, SALT)

    assert duplicate[:2] == ("error", "PathExistsError")
    assert missing_args[0] == "error"
    assert unknown_user[:2] == ("error", "PathNotFoundError")
    assert replicated.tree.get_user("bob") is None


def test_applied_ops_never_hold_a_hash_or_a_salt():
    replicated = ReplicatedTree()
    _apply(replicated, "op-1", "create_user", "alice", HASH, SALT)
    _apply(replicated, "op-2", "create_user", "alice", OTHER_HASH, OTHER_SALT)
    _apply(replicated, "op-3", "change_password", "alice", OTHER_HASH, OTHER_SALT)

    stored = repr(dict(replicated.applied_ops))

    for secret in (HASH, SALT, OTHER_HASH, OTHER_SALT):
        assert repr(secret) not in stored
        assert secret.hex() not in stored
