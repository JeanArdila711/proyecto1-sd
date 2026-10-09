"""Permisos por archivo en el árbol replicado (Hito 3, C3).

Quien llama viaja como una tupla plana (usuario, grupos, es_admin). None es "sin
identidad": no se chequea nada y lo creado queda de admin."""

import pickle

import pytest

from dfsha.common.exceptions import AccessDeniedError, InvalidPathError, PathNotFoundError
from dfsha.control_node.replicated_tree import MUTATIONS, ReplicatedTree
from dfsha.control_node.tree import ControlTree, DirNode, _allows

ALICE = ("alice", ("alice", "docentes"), False)
BOB = ("bob", ("bob",), False)
CAROL = ("carol", ("docentes",), False)  # comparte grupo con alice
ADMIN = ("root", ("root",), True)
NOGROUPS = ("dave", (), False)
B1, B2, B3 = "1" * 32, "2" * 32, "3" * 32


def _upload(tree, path, block_id=B1, caller=None, size=4, now=None, lease_s=None):
    tree.begin_upload(path, [(block_id, ["dn:1"])], now, lease_s, None, caller)
    tree.confirm_block(path, block_id, "sum", size, None, None, caller)
    tree.complete_upload(path, caller)


def _node(tree, path):
    return tree._get_node(tree._parts(path))


def _strip_permissions(tree):
    """Deja cada nodo como lo deja un pickle de 05bd4c4: sin owner, group ni mode."""

    def visit(node):
        for field in ("owner", "group", "mode"):
            node.__dict__.pop(field, None)
        if isinstance(node, DirNode):
            for child in node.children.values():
                visit(child)

    visit(tree._root)


# --- T1: campos, defaults y lecturas --------------------------------------------------------


def test_a_new_tree_has_an_admin_root_open_to_everyone():
    tree = ControlTree()

    assert (tree._root.owner, tree._root.group, tree._root.mode) == ("admin", "admin", 0o777)


def test_nodes_created_without_caller_belong_to_admin_with_default_modes():
    tree = ControlTree()
    tree.make_dir("/docs")
    _upload(tree, "/docs/a.bin")

    directory, file = _node(tree, "/docs"), _node(tree, "/docs/a.bin")

    assert (directory.owner, directory.group, directory.mode) == ("admin", "admin", 0o755)
    assert (file.owner, file.group, file.mode) == ("admin", "admin", 0o644)


def test_list_dir_returns_owner_group_and_mode_of_each_entry():
    tree = ControlTree()
    tree.make_dir("/docs", ALICE)
    _upload(tree, "/a.bin", caller=BOB)

    entries = {e.name: (e.is_dir, e.owner, e.group, e.mode) for e in tree.list_dir("/")}

    assert entries == {
        "docs": (True, "alice", "alice", 0o755),
        "a.bin": (False, "bob", "bob", 0o644),
    }


@pytest.mark.parametrize(
    ("mode", "caller", "want", "allowed"),
    [
        # 0o640: dueño rw, grupo r, otros nada
        (0o640, ALICE, 4, True),
        (0o640, ALICE, 2, True),
        (0o640, CAROL, 4, True),
        (0o640, CAROL, 2, False),
        (0o640, BOB, 4, False),
        (0o640, BOB, 2, False),
        # 0o604: dueño rw, grupo nada, otros r
        (0o604, ALICE, 4, True),
        (0o604, CAROL, 4, False),  # clases excluyentes: el grupo no hereda de otros
        (0o604, BOB, 4, True),
        (0o604, BOB, 2, False),
        # 0o060: solo el grupo; el dueño, aunque esté en el grupo, no lee ni escribe
        (0o060, ALICE, 4, False),
        (0o060, ALICE, 2, False),
        (0o060, CAROL, 4, True),
        (0o060, CAROL, 2, True),
        (0o060, BOB, 4, False),
        (0o060, BOB, 2, False),
    ],
)
def test_permission_matrix_uses_exclusive_classes(mode, caller, want, allowed):
    node = DirNode(owner="alice", group="docentes", mode=mode)

    assert _allows(node, caller, want) is allowed


@pytest.mark.parametrize("caller", [ADMIN, None])
@pytest.mark.parametrize("want", [4, 2])
def test_admin_and_no_identity_always_pass(caller, want):
    assert _allows(DirNode(owner="alice", group="alice", mode=0o000), caller, want) is True


def test_list_dir_without_r_is_denied():
    tree = ControlTree()
    tree.make_dir("/docs", ALICE)
    _node(tree, "/docs").mode = 0o700

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.list_dir("/docs", BOB)

    assert str(exc_info.value) == "permiso denegado: falta r en /docs"


def test_list_blocks_without_r_is_denied():
    tree = ControlTree()
    _upload(tree, "/a.bin", caller=ALICE)
    _node(tree, "/a.bin").mode = 0o600

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.list_blocks("a.bin", BOB)

    assert str(exc_info.value) == "permiso denegado: falta r en /a.bin"


def test_write_layout_without_w_is_denied():
    tree = ControlTree()
    _upload(tree, "/a.bin", caller=ALICE)

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.write_layout("/a.bin", 4, BOB)

    assert str(exc_info.value) == "permiso denegado: falta w en /a.bin"


def test_reads_with_permission_return_the_same_as_without_caller():
    tree = ControlTree()
    _upload(tree, "/a.bin", caller=ALICE)

    assert tree.list_blocks("/a.bin", BOB) == tree.list_blocks("/a.bin")
    assert tree.write_layout("/a.bin", 4, ALICE) == tree.write_layout("/a.bin", 4)
    assert tree.list_dir("/", BOB) == tree.list_dir("/")


def test_owner_group_and_mode_survive_a_pickle_round_trip():
    tree = ControlTree()
    tree.make_dir("/docs", ALICE)
    _upload(tree, "/docs/a.bin", caller=ALICE)
    _node(tree, "/docs/a.bin").mode = 0o600
    _node(tree, "/docs/a.bin").group = "docentes"

    restored = pickle.loads(pickle.dumps(tree))

    directory, file = _node(restored, "/docs"), _node(restored, "/docs/a.bin")
    assert (directory.owner, directory.group, directory.mode) == ("alice", "alice", 0o755)
    assert (file.owner, file.group, file.mode) == ("alice", "docentes", 0o600)
    assert restored._root.mode == 0o777


def test_a_snapshot_from_c2_restores_with_the_legacy_defaults():
    from dfsha.control_node.tree import UserRecord

    tree = ControlTree()
    tree.create_user("alice", b"h" * 32, b"s" * 16, ("alice",), False)
    tree.make_dir("/docs")
    tree.make_dir("/docs/sub")
    _upload(tree, "/docs/a.bin", B1)
    tree.begin_upload("/docs/pendiente.bin", [(B2, ["dn:1"])])
    tree.acquire_lock("/docs/a.bin", "l1", "alice", "r", 100.0, 30.0)
    _strip_permissions(tree)
    assert "mode" not in tree._root.__dict__  # así se ve un nodo de 05bd4c4

    restored = pickle.loads(pickle.dumps(tree))

    root = restored._root
    assert (root.owner, root.group, root.mode) == ("admin", "admin", 0o777)
    for path in ("/docs", "/docs/sub"):
        node = _node(restored, path)
        assert (node.owner, node.group, node.mode) == ("admin", "admin", 0o755)
    for path in ("/docs/a.bin", "/docs/pendiente.bin"):
        node = _node(restored, path)
        assert (node.owner, node.group, node.mode) == ("admin", "admin", 0o644)
    assert restored.get_user("alice") == UserRecord(b"h" * 32, b"s" * 16, ("alice",), False)
    assert "/docs/a.bin" in restored._locks
    # un usuario común lee y no escribe
    assert [e.name for e in restored.list_dir("/docs", BOB)] == ["a.bin", "sub"]
    assert [b.block_id for b in restored.list_blocks("/docs/a.bin", BOB)] == [B1]
    with pytest.raises(AccessDeniedError) as exc_info:
        restored.write_layout("/docs/a.bin", 4, BOB)
    assert str(exc_info.value) == "permiso denegado: falta w en /docs/a.bin"


def test_a_root_with_its_own_mode_keeps_it_on_restore():
    tree = ControlTree()
    tree._root.mode = 0o755

    restored = pickle.loads(pickle.dumps(tree))

    assert restored._root.mode == 0o755


# --- T2: quien llama en las mutaciones que ya existían ----------------------------------------

NOW = 100.0
LEASE = 30.0


def _dir_of_alice(tree):
    tree.make_dir("/docs", ALICE)


def _subdir_of_alice(tree):
    _dir_of_alice(tree)
    tree.make_dir("/docs/sub", ALICE)


def _file_of_alice(tree):
    _dir_of_alice(tree)
    _upload(tree, "/docs/a.bin", caller=ALICE)


def _private_file_of_alice(tree):
    _file_of_alice(tree)
    _node(tree, "/docs/a.bin").mode = 0o600


def _pending_of_alice(tree):
    # bob puede escribir en /docs: lo que lo frena es que la subida no es suya (P6)
    _dir_of_alice(tree)
    _node(tree, "/docs").mode = 0o777
    tree.begin_upload("/docs/p.bin", [(B1, ["dn:1"])], NOW, LEASE, 4, ALICE)


def _confirmed_pending_of_alice(tree):
    _pending_of_alice(tree)
    tree.confirm_block("/docs/p.bin", B1, "sum", 4, NOW, LEASE, ALICE)


def _file_with_write_lock(tree):
    _file_of_alice(tree)
    tree.acquire_lock("/docs/a.bin", "lw", "alice", "w", NOW, LEASE, ALICE)


def _reserved_write(tree):
    _file_with_write_lock(tree)
    tree.begin_write("/docs/a.bin", "w1", "lw", 0, 0, 2, [(0, B3, ["dn:1"])], NOW, LEASE, 4, ALICE)


_UPLOAD_OF_OTHER = "permiso denegado: la subida de /docs/p.bin es de otro usuario"

MUTATION_CASES = {
    "make_dir": (_dir_of_alice, lambda t, c: t.make_dir("/docs/x", c), "permiso denegado: falta w en /docs"),
    "remove_dir": (_subdir_of_alice, lambda t, c: t.remove_dir("/docs/sub", c), "permiso denegado: falta w en /docs"),
    "remove_file": (
        _file_of_alice,
        lambda t, c: t.remove_file("/docs/a.bin", NOW, c),
        "permiso denegado: falta w en /docs",
    ),
    "begin_upload": (
        _dir_of_alice,
        lambda t, c: t.begin_upload("/docs/n.bin", [(B2, ["dn:1"])], NOW, LEASE, 4, c),
        "permiso denegado: falta w en /docs",
    ),
    "confirm_block": (
        _pending_of_alice,
        lambda t, c: t.confirm_block("/docs/p.bin", B1, "sum", 4, NOW, LEASE, c),
        _UPLOAD_OF_OTHER,
    ),
    "complete_upload": (
        _confirmed_pending_of_alice,
        lambda t, c: t.complete_upload("/docs/p.bin", c),
        _UPLOAD_OF_OTHER,
    ),
    "abort_upload": (_pending_of_alice, lambda t, c: t.abort_upload("/docs/p.bin", c), _UPLOAD_OF_OTHER),
    "acquire_lock r": (
        _private_file_of_alice,
        lambda t, c: t.acquire_lock("/docs/a.bin", "l2", "x", "r", NOW, LEASE, c),
        "permiso denegado: falta r en /docs/a.bin",
    ),
    "acquire_lock w": (
        _file_of_alice,
        lambda t, c: t.acquire_lock("/docs/a.bin", "l2", "x", "w", NOW, LEASE, c),
        "permiso denegado: falta w en /docs/a.bin",
    ),
    "begin_write": (
        _file_with_write_lock,
        lambda t, c: t.begin_write("/docs/a.bin", "w1", "lw", 0, 0, 2, [(0, B3, ["dn:1"])], NOW, LEASE, 4, c),
        "permiso denegado: falta w en /docs/a.bin",
    ),
    "commit_write": (
        _reserved_write,
        lambda t, c: t.commit_write("/docs/a.bin", "w1", "lw", 0, [(0, B3, "sum2", 4)], NOW, c),
        "permiso denegado: falta w en /docs/a.bin",
    ),
}


def _state(tree) -> bytes:
    return pickle.dumps(tree.__getstate__())


@pytest.mark.parametrize("case", MUTATION_CASES)
@pytest.mark.parametrize("caller", [ALICE, ADMIN, None], ids=["dueño", "admin", "sin identidad"])
def test_the_owner_the_admin_and_no_identity_can_mutate(case, caller):
    setup, action, _ = MUTATION_CASES[case]
    tree = ControlTree()
    setup(tree)
    before = _state(tree)

    action(tree, caller)

    assert _state(tree) != before


@pytest.mark.parametrize("case", MUTATION_CASES)
def test_another_user_cannot_mutate_and_the_tree_stays_the_same(case):
    setup, action, message = MUTATION_CASES[case]
    tree = ControlTree()
    setup(tree)
    before = _state(tree)

    with pytest.raises(AccessDeniedError) as exc_info:
        action(tree, BOB)

    assert str(exc_info.value) == message
    assert _state(tree) == before


def test_the_permission_is_checked_before_the_name_is_looked_up():
    # P3: sin `w`, un nombre que ya existe responde permiso denegado y no "ya existe"
    tree = ControlTree()
    _subdir_of_alice(tree)

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.make_dir("/docs/sub", BOB)

    assert str(exc_info.value) == "permiso denegado: falta w en /docs"


def test_make_dir_belongs_to_the_caller_and_its_first_group():
    tree = ControlTree()
    tree.make_dir("/a", ALICE)
    tree.make_dir("/d", NOGROUPS)

    assert (_node(tree, "/a").owner, _node(tree, "/a").group, _node(tree, "/a").mode) == ("alice", "alice", 0o755)
    assert (_node(tree, "/d").owner, _node(tree, "/d").group, _node(tree, "/d").mode) == ("dave", "dave", 0o755)


def test_directories_created_by_an_upload_belong_to_the_uploader():
    tree = ControlTree()
    tree.make_dir("/a")
    tree.chmod("/a", 0o777)

    tree.begin_upload("/a/b/f", [(B1, ["dn:1"])], NOW, LEASE, 4, CAROL)

    b, f = _node(tree, "/a/b"), _node(tree, "/a/b/f")
    assert (b.owner, b.group, b.mode) == ("carol", "docentes", 0o755)
    assert (f.owner, f.group, f.mode) == ("carol", "docentes", 0o644)


def test_an_upload_without_w_on_the_last_existing_directory_creates_nothing():
    tree = ControlTree()
    tree.make_dir("/a")  # de admin, 0o755

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.begin_upload("/a/b/f", [(B1, ["dn:1"])], NOW, LEASE, 4, BOB)

    assert str(exc_info.value) == "permiso denegado: falta w en /a"
    assert _node(tree, "/a").children == {}


def test_the_upload_needs_w_on_the_last_existing_directory_not_on_an_ancestor():
    tree = ControlTree()
    tree.make_dir("/a")
    tree.chmod("/a", 0o777)
    tree.make_dir("/a/b")  # de admin, 0o755, dentro de un /a abierto

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.begin_upload("/a/b/c/f", [(B1, ["dn:1"])], NOW, LEASE, 4, BOB)

    assert str(exc_info.value) == "permiso denegado: falta w en /a/b"
    assert _node(tree, "/a/b").children == {}


def test_a_completed_upload_keeps_its_owner_and_file_mode():
    tree = ControlTree()
    _confirmed_pending_of_alice(tree)

    tree.complete_upload("/docs/p.bin", ALICE)

    node = _node(tree, "/docs/p.bin")
    assert (node.state, node.owner, node.group, node.mode) == ("committed", "alice", "alice", 0o644)


def test_an_expired_pending_upload_can_be_replaced_by_another_writer():
    tree = ControlTree()
    _pending_of_alice(tree)  # vence en NOW + LEASE

    _, stale = tree.begin_upload("/docs/p.bin", [(B2, ["dn:1"])], NOW + LEASE + 1, LEASE, 4, BOB)

    assert [b.block_id for b in stale] == [B1]
    node = _node(tree, "/docs/p.bin")
    assert (node.owner, node.group, [b.block_id for b in node.blocks]) == ("bob", "bob", [B2])
    tree.confirm_block("/docs/p.bin", B2, "sum", 4, NOW + LEASE + 1, LEASE, BOB)
    tree.complete_upload("/docs/p.bin", BOB)


def test_a_commit_is_rejected_if_the_writer_lost_w_after_the_reservation():
    tree = ControlTree()
    _reserved_write(tree)
    tree.chmod("/docs/a.bin", 0o444, ALICE)
    blocks_before = [b.block_id for b in tree.list_blocks("/docs/a.bin")]

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.commit_write("/docs/a.bin", "w1", "lw", 0, [(0, B3, "sum2", 4)], NOW, ALICE)

    assert str(exc_info.value) == "permiso denegado: falta w en /docs/a.bin"
    assert tree.write_layout("/docs/a.bin", 4)[0] == 0
    assert [b.block_id for b in tree.list_blocks("/docs/a.bin")] == blocks_before == [B1]


def test_renew_release_and_abort_write_only_need_the_lock_id():
    tree = ControlTree()
    _reserved_write(tree)
    tree.chmod("/docs/a.bin", 0o000, ALICE)

    tree.renew_lock("/docs/a.bin", "lw", NOW + 1, LEASE)
    assert [b.block_id for b in tree.abort_write("/docs/a.bin", "w1", "lw", NOW + 1)] == [B3]
    tree.release_lock("/docs/a.bin", "lw", NOW + 1)

    assert tree._locks == {} and tree._writes == {}


def test_update_block_replicas_does_not_check_permissions():
    tree = ControlTree()
    _private_file_of_alice(tree)

    tree.update_block_replicas("/docs/a.bin", B1, ["dn:1"], ["dn:2"])

    assert tree.list_blocks("/docs/a.bin")[0].datanode_addresses == ["dn:2"]


# --- por el log replicado -------------------------------------------------------------------
# ReplicatedTree.apply con _doApply=True: aplica local, sin clúster Raft.


def _apply(replicated, op_id, method, *args):
    return replicated.apply(op_id, method, args, _doApply=True)


def _replicated_with_dir_of_alice():
    replicated = ReplicatedTree()
    assert _apply(replicated, "op-docs", "make_dir", "/docs", ALICE) == ("ok", None)
    return replicated


def test_a_denied_mutation_is_an_outcome_not_an_exception():
    replicated = _replicated_with_dir_of_alice()

    outcome = _apply(replicated, "op-1", "make_dir", "/docs/x", BOB)

    assert outcome == ("error", "AccessDeniedError", "permiso denegado: falta w en /docs")
    assert replicated.tree.list_dir("/docs") == []


def test_retrying_a_denied_op_id_after_granting_the_permission_returns_the_stored_outcome():
    replicated = _replicated_with_dir_of_alice()
    denied = _apply(replicated, "op-1", "make_dir", "/docs/x", BOB)
    assert _apply(replicated, "op-chmod", "chmod", "/docs", 0o777, ALICE) == ("ok", None)

    assert _apply(replicated, "op-1", "make_dir", "/docs/x", BOB) == denied
    assert replicated.tree.list_dir("/docs") == []
    assert _apply(replicated, "op-2", "make_dir", "/docs/x", BOB) == ("ok", None)


def test_a_malformed_caller_is_a_generic_error_and_does_not_raise():
    replicated = _replicated_with_dir_of_alice()

    outcome = _apply(replicated, "op-1", "make_dir", "/docs/x", "alice")

    assert outcome[:2] == ("error", "DFShaError")
    assert outcome[2].startswith("error interno en make_dir: ")
    assert replicated.tree.list_dir("/docs") == []


def test_c2_journal_entries_replay_without_checks_on_foreign_closed_nodes():
    """Las tuplas exactas que arma el servicer de 05bd4c4, sin caller, sobre nodos 0o000
    de otro dueño: el replay de un journal de C2 da los mismos outcomes que antes."""
    replicated = ReplicatedTree()
    tree = replicated.tree
    tree.make_dir("/docs", ALICE)
    tree.make_dir("/docs/vacio", ALICE)
    _upload(tree, "/docs/a.bin", B1, caller=ALICE)
    _upload(tree, "/docs/b.bin", B2, caller=ALICE)
    for path in ("/docs", "/docs/vacio", "/docs/a.bin", "/docs/b.bin"):
        tree.chmod(path, 0o000, ALICE)
    _node(tree, "/").mode = 0o000
    placements = [("4" * 32, ["dn:1"])]

    entries = [
        ("make_dir", ("/docs/nuevo",)),
        ("remove_dir", ("/docs/vacio",)),
        ("remove_file", ("/docs/b.bin", NOW)),
        ("begin_upload", ("/docs/sub/p.bin", placements, NOW, LEASE, 4)),
        ("confirm_block", ("/docs/sub/p.bin", "4" * 32, "sum", 4, NOW, LEASE)),
        ("complete_upload", ("/docs/sub/p.bin",)),
        ("begin_upload", ("/docs/q.bin", [("5" * 32, ["dn:1"])], NOW, LEASE, 4)),
        ("abort_upload", ("/docs/q.bin",)),
        ("acquire_lock", ("/docs/a.bin", "lw", "alice", "w", NOW, LEASE)),
        ("begin_write", ("/docs/a.bin", "w1", "lw", 0, 0, 2, [(0, B3, ["dn:1"])], NOW, LEASE, 4)),
        ("commit_write", ("/docs/a.bin", "w1", "lw", 0, [(0, B3, "sum2", 4)], NOW)),
        ("renew_lock", ("/docs/a.bin", "lw", NOW, LEASE)),
        ("release_lock", ("/docs/a.bin", "lw", NOW)),
    ]
    outcomes = [replicated.apply(f"c2-{i}", method, args, _doApply=True) for i, (method, args) in enumerate(entries)]

    assert [outcome[0] for outcome in outcomes] == ["ok"] * len(entries), outcomes
    assert tree.write_layout("/docs/a.bin", 4)[0] == 1
    # lo que crea un comando sin caller queda de admin, como un nodo anterior a C3
    nuevo = _node(tree, "/docs/nuevo")
    assert (nuevo.owner, nuevo.group, nuevo.mode) == ("admin", "admin", 0o755)
    sub = _node(tree, "/docs/sub")
    assert (sub.owner, sub.group, sub.mode) == ("admin", "admin", 0o755)


# --- guardia de P25 ------------------------------------------------------------------------
# Una mutación nueva que llegue a un RPC no puede quedar sin chequeo por olvido: o recibe a
# quien llama como último parámetro, con default None, o está en esta lista con su motivo.

EXEMPT_FROM_CALLER = {
    "renew_lock": "D7: alcanza con el lock_id y estar autenticado",
    "release_lock": "D7: alcanza con el lock_id y estar autenticado",
    "abort_write": "P7: exige write_id y lock_id, y tiene que poder limpiar tras un chmod",
    "update_block_replicas": "ningún RPC la invoca; solo el re-replicador del líder",
    "create_user": "el servicer exige admin antes del commit",
    "change_password": "el servicer decide quién puede, antes del commit",
}


def _mutations_without_caller(tree_class, names) -> list[str]:
    import inspect

    missing = []
    for name in sorted(names):
        if name in EXEMPT_FROM_CALLER:
            continue
        params = list(inspect.signature(getattr(tree_class, name)).parameters.values())
        last = params[-1] if params else None
        if last is None or last.name != "caller" or last.default is not None:
            missing.append(name)
    return missing


def test_every_mutation_receives_the_caller_or_is_explicitly_exempt():
    assert len(MUTATIONS) == 18
    assert set(EXEMPT_FROM_CALLER) <= MUTATIONS
    assert len(MUTATIONS - set(EXEMPT_FROM_CALLER)) == 12
    assert _mutations_without_caller(ControlTree, MUTATIONS) == []


def test_the_guard_catches_a_new_mutation_without_caller():
    class Tree(ControlTree):
        def nueva(self, virtual_path):
            pass

        def otra(self, virtual_path, caller="nadie"):
            pass

        def tercera(self, caller=None, extra=None):
            pass

    assert _mutations_without_caller(Tree, {"make_dir", "nueva", "otra", "tercera", "renew_lock"}) == [
        "nueva",
        "otra",
        "tercera",
    ]


# --- T3: chmod y chown ------------------------------------------------------------------------


def _tree_with_users():
    tree = ControlTree()
    for user in (ALICE, BOB, CAROL):
        tree.create_user(user[0], b"h" * 32, b"s" * 16, user[1], False)
    _file_of_alice(tree)
    return tree


@pytest.mark.parametrize("caller", [ALICE, ADMIN], ids=["dueño", "admin"])
def test_the_owner_or_an_admin_change_the_mode(caller):
    tree = _tree_with_users()

    tree.chmod("/docs/a.bin", 0o640, caller)

    assert _node(tree, "/docs/a.bin").mode == 0o640


@pytest.mark.parametrize("caller", [BOB, CAROL], ids=["otro", "del grupo"])
def test_someone_else_cannot_change_the_mode(caller):
    tree = _tree_with_users()

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.chmod("/docs/a.bin", 0o777, caller)

    assert str(exc_info.value) == "permiso denegado: solo el dueño o un admin cambia el modo de /docs/a.bin"
    assert _node(tree, "/docs/a.bin").mode == 0o644


@pytest.mark.parametrize(
    ("mode", "shown"), [(-1, "-1"), (0o1000, "512"), (4095, "4095"), (True, "True"), ("644", "'644'")]
)
def test_an_invalid_mode_is_rejected(mode, shown):
    tree = _tree_with_users()

    with pytest.raises(InvalidPathError) as exc_info:
        tree.chmod("/docs/a.bin", mode, ALICE)

    assert str(exc_info.value) == f"modo inválido: {shown}; va de 0 a 0o777"
    assert _node(tree, "/docs/a.bin").mode == 0o644


@pytest.mark.parametrize("mode", [0, 0o777])
def test_the_mode_limits_are_valid(mode):
    tree = _tree_with_users()

    tree.chmod("/docs/a.bin", mode, ALICE)

    assert _node(tree, "/docs/a.bin").mode == mode


def test_an_admin_changes_the_owner():
    tree = _tree_with_users()

    tree.chown("/docs/a.bin", "bob", "", ADMIN)

    node = _node(tree, "/docs/a.bin")
    assert (node.owner, node.group) == ("bob", "alice")


def test_the_owner_cannot_give_the_file_away():
    tree = _tree_with_users()

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.chown("/docs/a.bin", "bob", "", ALICE)

    assert str(exc_info.value) == "permiso denegado: solo un admin cambia el dueño de /docs/a.bin"
    assert _node(tree, "/docs/a.bin").owner == "alice"


def test_the_new_owner_has_to_exist():
    tree = _tree_with_users()

    with pytest.raises(PathNotFoundError) as exc_info:
        tree.chown("/docs/a.bin", "jaen", "", ADMIN)

    assert str(exc_info.value) == "no existe el usuario: jaen"
    assert _node(tree, "/docs/a.bin").owner == "alice"


def test_chown_needs_an_owner_or_a_group():
    tree = _tree_with_users()

    with pytest.raises(InvalidPathError) as exc_info:
        tree.chown("/docs/a.bin", "", "", ADMIN)

    assert str(exc_info.value) == "chown: falta el dueño o el grupo"


def test_the_owner_changes_the_group_to_one_of_its_own():
    tree = _tree_with_users()

    tree.chown("/docs/a.bin", "", "docentes", ALICE)

    node = _node(tree, "/docs/a.bin")
    assert (node.owner, node.group) == ("alice", "docentes")


_GROUP_DENIED = (
    "permiso denegado: para cambiar el grupo de /docs/a.bin hay que ser su dueño y pertenecer al grupo nuevo"
)


def test_the_owner_cannot_move_the_file_to_a_group_it_is_not_in():
    tree = _tree_with_users()

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.chown("/docs/a.bin", "", "bob", ALICE)

    assert str(exc_info.value) == _GROUP_DENIED
    assert _node(tree, "/docs/a.bin").group == "alice"


def test_a_group_member_who_is_not_the_owner_cannot_change_the_group():
    tree = _tree_with_users()
    tree.chown("/docs/a.bin", "", "docentes", ALICE)

    with pytest.raises(AccessDeniedError) as exc_info:
        tree.chown("/docs/a.bin", "", "docentes", CAROL)

    assert str(exc_info.value) == _GROUP_DENIED


def test_a_group_with_an_invalid_name_is_rejected():
    tree = _tree_with_users()

    with pytest.raises(InvalidPathError) as exc_info:
        tree.chown("/docs/a.bin", "", "A B", ADMIN)

    assert str(exc_info.value) == "nombre de grupo inválido: 'A B'"
    assert _node(tree, "/docs/a.bin").group == "alice"


def test_chown_with_both_fields_by_an_admin():
    tree = _tree_with_users()

    tree.chown("/docs/a.bin", "bob", "docentes", ADMIN)

    node = _node(tree, "/docs/a.bin")
    assert (node.owner, node.group) == ("bob", "docentes")


def test_without_identity_chown_still_validates_owner_and_group():
    tree = _tree_with_users()

    with pytest.raises(PathNotFoundError):
        tree.chown("/docs/a.bin", "jaen", "", None)
    with pytest.raises(InvalidPathError):
        tree.chown("/docs/a.bin", "", "A B", None)
    tree.chown("/docs/a.bin", "bob", "", None)

    assert _node(tree, "/docs/a.bin").owner == "bob"


def test_a_chmod_of_the_root_by_the_admin_survives_a_snapshot():
    tree = ControlTree()

    tree.chmod("/", 0o755, ADMIN)
    restored = pickle.loads(pickle.dumps(tree))

    assert restored._root.mode == 0o755


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (lambda t: t.chmod("/docs/p.bin", 0o600, ADMIN), "no existe: /docs/p.bin"),
        (lambda t: t.chown("/docs/p.bin", "bob", "", ADMIN), "no existe: /docs/p.bin"),
        (lambda t: t.chmod("/docs/nada.bin", 0o600, ADMIN), "no existe: /docs/nada.bin"),
        (lambda t: t.chown("/docs/nada.bin", "", "docentes", ADMIN), "no existe: /docs/nada.bin"),
    ],
    ids=["chmod pendiente", "chown pendiente", "chmod inexistente", "chown inexistente"],
)
def test_chmod_and_chown_do_not_see_pending_uploads_or_missing_paths(call, message):
    tree = _tree_with_users()
    tree.begin_upload("/docs/p.bin", [(B2, ["dn:1"])], NOW, LEASE, 4, ALICE)

    with pytest.raises(PathNotFoundError) as exc_info:
        call(tree)

    assert str(exc_info.value) == message
    assert _node(tree, "/docs/p.bin").mode == 0o644


def test_the_effect_of_chmod_and_chown_is_visible():
    tree = _tree_with_users()
    assert tree.list_blocks("/docs/a.bin", BOB)

    tree.chmod("/docs/a.bin", 0o600, ALICE)
    with pytest.raises(AccessDeniedError):
        tree.list_blocks("/docs/a.bin", BOB)

    tree.chown("/docs/a.bin", "bob", "", ADMIN)
    assert [b.block_id for b in tree.list_blocks("/docs/a.bin", BOB)] == [B1]
    with pytest.raises(AccessDeniedError):
        tree.list_blocks("/docs/a.bin", ALICE)


def test_chmod_and_chown_go_through_the_replicated_log():
    replicated = ReplicatedTree()
    _apply(replicated, "op-user", "create_user", "bob", b"h" * 32, b"s" * 16, ("bob",), False)
    _apply(replicated, "op-dir", "make_dir", "/docs", ALICE)

    assert _apply(replicated, "op-1", "chmod", "/docs", 0o700, ALICE) == ("ok", None)
    assert _apply(replicated, "op-2", "chown", "/docs", "bob", "", ADMIN) == ("ok", None)
    denied = _apply(replicated, "op-3", "chmod", "/docs", 0o777, ALICE)
    # el mismo op_id no se vuelve a aplicar: el dueño cambió, pero el resultado es el guardado
    assert _apply(replicated, "op-1", "chmod", "/docs", 0o777, ALICE) == ("ok", None)

    node = replicated.tree._get_node(["docs"])
    assert (node.owner, node.mode) == ("bob", 0o700)
    assert denied == (
        "error",
        "AccessDeniedError",
        "permiso denegado: solo el dueño o un admin cambia el modo de /docs",
    )
