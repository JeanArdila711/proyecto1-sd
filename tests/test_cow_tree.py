"""B3: estado replicado de la escritura copy-on-write (reserva, commit con CAS, abort)."""

from __future__ import annotations

import pickle

import pytest

from dfsha.common.exceptions import ConflictError, InvalidPathError, PathNotFoundError
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.tree import ControlTree, plan_write_slots

BS = 5
NOW = 1000.0
LEASE = 60.0


def _hex(n: int) -> str:
    return f"{n:032x}"


def _file(tree: ControlTree, sizes: list[int], path: str = "/a.bin", block_size: int | None = BS) -> list[str]:
    """Archivo confirmado con bloques de los tamaños dados; devuelve sus block_ids."""
    ids = [_hex(100 + i) for i in range(len(sizes))]
    placements = [(bid, ["dn1", "dn2"]) for bid in ids]
    if block_size is None:
        tree.begin_upload(path, placements, NOW, LEASE)  # firma vieja, sin block_size
    else:
        tree.begin_upload(path, placements, NOW, LEASE, block_size)
    for bid, size in zip(ids, sizes):
        tree.confirm_block(path, bid, f"sum-{bid}", size)
    tree.complete_upload(path)
    return ids


def _writer(tree: ControlTree, path: str = "/a.bin", lock_id: str = "L1", now: float = NOW) -> str:
    tree.acquire_lock(path, lock_id, "cliente", "w", now, LEASE)
    return lock_id


def _proposals(tree: ControlTree, path: str, offset: int, length: int) -> list:
    version, block_size, sizes = tree.write_layout(path, BS)
    return [
        (index, _hex(900 + index), ["dn3", "dn1"])
        for index, _ in plan_write_slots(sizes, block_size, offset, length)
    ]


def _begin(tree, path="/a.bin", offset=0, length=1, lock_id="L1", write_id="W1", now=NOW, base_version=None):
    version, _, _ = tree.write_layout(path, BS)
    return tree.begin_write(
        path,
        write_id,
        lock_id,
        version if base_version is None else base_version,
        offset,
        length,
        _proposals(tree, path, offset, length),
        now,
        LEASE,
        BS,
    )


def _confirm(result) -> list:
    _, _, _, slots = result
    return [(s[0], s[4], f"new-{s[4]}", s[6]) for s in slots]


# --- plan_write_slots: qué bloques toca una escritura --------------------------------


@pytest.mark.parametrize(
    "sizes,offset,length,expected",
    [
        ([5, 5, 5, 5, 3], 6, 2, [(1, 5)]),  # dentro de un bloque
        ([5, 5, 5, 5, 3], 8, 4, [(1, 5), (2, 5)]),  # cruza dos bloques
        ([5, 5, 5, 5, 3], 21, 10, [(4, 5), (5, 5), (6, 1)]),  # extiende el último y agrega
        ([5, 5, 5, 5, 3], 23, 2, [(4, 5)]),  # append que completa el último bloque
        ([5, 5, 5, 5], 20, 2, [(4, 2)]),  # append con tamaño múltiplo del bloque
        ([5, 5, 3], 0, 13, [(0, 5), (1, 5), (2, 3)]),  # reescribe todo, mismo tamaño
    ],
)
def test_plan_write_slots(sizes, offset, length, expected):
    assert plan_write_slots(sizes, BS, offset, length) == expected


@pytest.mark.parametrize("offset,length", [(24, 1), (-1, 2), (0, 0), (0, -3)])
def test_plan_write_slots_rejects_invalid_ranges(offset, length):
    with pytest.raises(InvalidPathError):
        plan_write_slots([5, 5, 5, 5, 3], BS, offset, length)


# --- begin_write ------------------------------------------------------------------------


def test_begin_write_reserves_new_blocks_and_describes_old_ones():
    tree = ControlTree()
    ids = _file(tree, [5, 5, 5, 5, 3])
    _writer(tree)

    write_id, base_version, block_size, slots = _begin(tree, offset=8, length=4)

    assert (write_id, base_version, block_size) == ("W1", 0, BS)
    assert slots == [
        (1, ids[1], 5, ["dn1", "dn2"], _hex(901), ["dn3", "dn1"], 5),
        (2, ids[2], 5, ["dn1", "dn2"], _hex(902), ["dn3", "dn1"], 5),
    ]
    # la reserva no cambia el archivo visible
    assert [b.block_id for b in tree.list_blocks("/a.bin")] == ids


def test_begin_write_for_new_blocks_has_no_old_block():
    tree = ControlTree()
    _file(tree, [5, 5, 5, 5, 3])
    _writer(tree)

    _, _, _, slots = _begin(tree, offset=21, length=10)

    assert [(s[0], s[1], s[2], s[6]) for s in slots] == [(4, _hex(104), 3, 5), (5, "", 0, 5), (6, "", 0, 1)]


@pytest.mark.parametrize("mode", [None, "r"])
def test_begin_write_requires_the_exclusive_lock(mode):
    tree = ControlTree()
    _file(tree, [5, 5])
    if mode:
        tree.acquire_lock("/a.bin", "L1", "cliente", mode, NOW, LEASE)
    with pytest.raises(ConflictError):
        _begin(tree)


def test_begin_write_rejects_an_expired_lock():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    with pytest.raises(ConflictError):
        _begin(tree, now=NOW + LEASE + 1)


def test_begin_write_rejects_a_stale_proposal():
    """El líder armó la propuesta leyendo una versión que ya cambió."""
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    with pytest.raises(ConflictError):
        _begin(tree, base_version=7)


def test_begin_write_rejects_proposals_that_do_not_match_the_file():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    with pytest.raises(ConflictError):
        tree.begin_write("/a.bin", "W1", "L1", 0, 0, 1, [(1, _hex(901), ["dn3"])], NOW, LEASE, BS)


# --- commit_write ----------------------------------------------------------------------


def test_commit_write_publishes_new_blocks_bumps_version_and_returns_old_ones():
    tree = ControlTree()
    ids = _file(tree, [5, 5, 5, 5, 3])
    _writer(tree)
    begun = _begin(tree, offset=21, length=10)

    version, old = tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(begun), NOW + 1)

    assert version == 1
    assert [b.block_id for b in old] == [ids[4]]
    blocks = tree.list_blocks("/a.bin")
    assert [b.block_id for b in blocks] == ids[:4] + [_hex(904), _hex(905), _hex(906)]
    assert [b.size_bytes for b in blocks] == [5, 5, 5, 5, 5, 5, 1]
    assert all(b.confirmed for b in blocks)
    assert blocks[5].checksum == f"new-{_hex(905)}"


def test_commit_write_with_a_stale_version_conflicts_and_publishes_nothing():
    tree = ControlTree()
    ids = _file(tree, [5, 5])
    _writer(tree)
    first = _begin(tree, offset=0, length=2, write_id="W1")
    second = _begin(tree, offset=6, length=2, write_id="W2")
    tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(first), NOW + 1)
    after_first = [b.block_id for b in tree.list_blocks("/a.bin")]

    with pytest.raises(ConflictError):
        tree.commit_write("/a.bin", "W2", "L1", 0, _confirm(second), NOW + 2)

    assert [b.block_id for b in tree.list_blocks("/a.bin")] == after_first != ids


def test_commit_write_rejects_a_lost_lock_and_keeps_the_file():
    tree = ControlTree()
    ids = _file(tree, [5, 5])
    _writer(tree)
    begun = _begin(tree)
    with pytest.raises(ConflictError):
        tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(begun), NOW + LEASE + 1)
    assert [b.block_id for b in tree.list_blocks("/a.bin")] == ids


def test_commit_write_rejects_an_expired_reservation():
    tree = ControlTree()
    _file(tree, [5, 5])
    tree.acquire_lock("/a.bin", "L1", "cliente", "w", NOW, LEASE * 10)
    begun = _begin(tree)
    with pytest.raises(ConflictError):
        tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(begun), NOW + LEASE + 1)


def test_commit_write_rejects_confirmed_slots_that_do_not_match_the_reservation():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    begun = _begin(tree)
    tampered = [(index, _hex(555), checksum, size) for index, _, checksum, size in _confirm(begun)]
    with pytest.raises(ConflictError):
        tree.commit_write("/a.bin", "W1", "L1", 0, tampered, NOW + 1)


def test_commit_write_fixes_the_block_size_of_legacy_files():
    """Un archivo subido antes de B3 no guarda block_size: se infiere del primer
    bloque y queda fijado al primer commit."""
    tree = ControlTree()
    _file(tree, [5, 5, 3], block_size=None)
    _writer(tree)
    assert tree.write_layout("/a.bin", 999)[1] == BS
    tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(_begin(tree, offset=13, length=2)), NOW + 1)
    assert tree.write_layout("/a.bin", 999)[1] == BS


# --- abort_write -----------------------------------------------------------------------


def test_abort_write_releases_the_reservation_and_returns_its_new_blocks():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    begun = _begin(tree, offset=3, length=4)

    reserved = tree.abort_write("/a.bin", "W1", "L1", NOW + 1)

    assert sorted(b.block_id for b in reserved) == [_hex(900), _hex(901)]
    with pytest.raises(ConflictError):
        tree.commit_write("/a.bin", "W1", "L1", 0, _confirm(begun), NOW + 2)
    assert tree.abort_write("/a.bin", "W1", "L1", NOW + 3) == []  # idempotente


def test_abort_write_of_someone_elses_reservation_conflicts():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    _begin(tree)
    with pytest.raises(ConflictError):
        tree.abort_write("/a.bin", "W1", "OTRO", NOW + 1)


def test_begin_write_on_a_missing_file_is_not_found():
    tree = ControlTree()
    with pytest.raises(PathNotFoundError):
        tree.write_layout("/no-existe.bin", BS)


# --- persistencia y replicación ---------------------------------------------------------


def test_snapshot_roundtrip_keeps_reservations():
    tree = ControlTree()
    _file(tree, [5, 5])
    _writer(tree)
    begun = _begin(tree)
    restored = pickle.loads(pickle.dumps(tree))
    assert restored.commit_write("/a.bin", "W1", "L1", 0, _confirm(begun), NOW + 1)[0] == 1


def test_snapshot_from_before_b3_restores_with_empty_reservations():
    tree = ControlTree()
    _file(tree, [5, 5], block_size=None)
    state = tree.__getstate__()
    del state["_writes"]
    restored = ControlTree.__new__(ControlTree)
    restored.__setstate__(state)
    assert restored._writes == {}
    assert restored.write_layout("/a.bin", BS)[0] == 0


def test_begin_write_is_a_replicated_mutation_deduplicated_by_op_id():
    replicated = ReplicatedTree()

    def apply(op_id, method, *args):
        return replicated.apply(op_id, method, args, _doApply=True)

    apply("up", "begin_upload", "/a.bin", [(_hex(100), ["dn1"]), (_hex(101), ["dn1"])], NOW, LEASE, BS)
    apply("c0", "confirm_block", "/a.bin", _hex(100), "s0", 5)
    apply("c1", "confirm_block", "/a.bin", _hex(101), "s1", 5)
    apply("done", "complete_upload", "/a.bin")
    apply("lock", "acquire_lock", "/a.bin", "L1", "cliente", "w", NOW, LEASE)

    first = apply("bw", "begin_write", "/a.bin", "W1", "L1", 0, 0, 2, [(0, _hex(900), ["dn2"])], NOW, LEASE, BS)
    # reintento con el mismo op_id y OTRA propuesta: devuelve la reserva original
    retry = apply("bw", "begin_write", "/a.bin", "W9", "L1", 0, 0, 2, [(0, _hex(999), ["dn9"])], NOW, LEASE, BS)
    assert first[0] == "ok" and retry == first
    assert first[1][0] == "W1"
