"""La máquina de estados replicada, sin red: apply(..., _doApply=True) ejecuta la
función directo, igual que hace pysyncobj al aplicar una entrada del log."""

import pickle

from dfsha.control_node import replicated_tree
from dfsha.control_node.replicated_tree import ReplicatedTree


def _apply(repl, op_id, method, *args):
    return repl.apply(op_id, method, args, _doApply=True)


def test_domain_error_is_returned_not_raised():
    repl = ReplicatedTree()
    assert _apply(repl, "op1", "make_dir", "/docs") == ("ok", None)

    outcome = _apply(repl, "op2", "make_dir", "/docs")

    assert outcome[0] == "error"
    assert outcome[1] == "PathExistsError"


def test_unexpected_exception_is_returned_not_raised(monkeypatch):
    repl = ReplicatedTree()

    def broken(*_):
        raise KeyError("bug")

    monkeypatch.setattr(repl.tree, "make_dir", broken)

    outcome = _apply(repl, "op1", "make_dir", "/docs")

    assert outcome[0] == "error"
    assert outcome[1] == "DFShaError"


def test_unknown_method_is_rejected_without_touching_the_tree():
    repl = ReplicatedTree()

    outcome = _apply(repl, "op1", "list_dir", "/")

    assert outcome[0] == "error"
    assert "desconocida" in outcome[2]


def test_same_op_id_is_not_applied_twice():
    repl = ReplicatedTree()
    first = _apply(repl, "op1", "make_dir", "/docs")

    # reintento de la misma operación lógica: misma respuesta, sin re-ejecutar
    assert _apply(repl, "op1", "make_dir", "/docs") == first == ("ok", None)
    # una operación distinta sí se ejecuta, y ve que ya existe
    assert _apply(repl, "op2", "make_dir", "/docs")[1] == "PathExistsError"


def test_retried_begin_upload_returns_original_placements():
    repl = ReplicatedTree()
    original = [("a" * 32, ["dn1", "dn2", "dn3"])]
    _, stored = _apply(repl, "op1", "begin_upload", "/f.bin", original)

    # tras un failover, el nuevo líder propone OTROS block_ids para el mismo op_id
    retry = [("b" * 32, ["dn2", "dn3", "dn1"])]
    _, returned = _apply(repl, "op1", "begin_upload", "/f.bin", retry)

    assert returned == stored == original
    # y el árbol guardó los originales: son los que se pueden confirmar
    assert _apply(repl, "op2", "confirm_block", "/f.bin", "a" * 32, "sum", 4) == ("ok", None)
    assert _apply(repl, "op3", "confirm_block", "/f.bin", "b" * 32, "sum", 4)[1] == "PathNotFoundError"


def test_applied_ops_is_bounded_fifo(monkeypatch):
    monkeypatch.setattr(replicated_tree, "MAX_APPLIED_OPS", 3)
    repl = ReplicatedTree()
    for i in range(5):
        _apply(repl, f"op{i}", "make_dir", f"/d{i}")

    assert list(repl.applied_ops) == ["op2", "op3", "op4"]


def test_snapshot_roundtrip_restores_tree_and_a_usable_lock():
    repl = ReplicatedTree()
    _apply(repl, "op1", "make_dir", "/docs")
    _apply(repl, "op2", "begin_upload", "/docs/f.bin", [("c" * 32, ["dn1"])])  # queda pendiente

    # lo que hace pysyncobj al compactar el log y al levantar un nodo desde el dump
    data = pickle.loads(pickle.dumps(repl._serialize()))
    restored = ReplicatedTree()
    restored._deserialize(data)

    assert [e.name for e in restored.tree.list_dir("/")] == ["docs"]
    assert "op2" in restored.applied_ops
    # la subida pendiente sobrevivió: se puede confirmar y completar
    assert _apply(restored, "op3", "confirm_block", "/docs/f.bin", "c" * 32, "sum", 4) == ("ok", None)
    assert _apply(restored, "op4", "complete_upload", "/docs/f.bin") == ("ok", None)
    assert [b.block_id for b in restored.tree.list_blocks("/docs/f.bin")] == ["c" * 32]
