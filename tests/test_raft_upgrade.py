"""Spike de Raft cifrado y compatibilidad de estado persistido de Hito 2.

Los fixtures son binarios reales de ``9985c6d``. Este archivo es también el
punto de extensión de B1, B3, C2 y C3: cada paquete agrega sus aserciones de
estado nuevo a ``assert_legacy_upgrade`` sin cambiar el replay legacy.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from pysyncobj import SyncObj

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.control_node.main import build_raft_conf
from dfsha.control_node.replicated_tree import ReplicatedTree


WORKTREE = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = Path(__file__).with_name("fixtures") / "raft_legacy_9985c6d"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"
SPIKE_PATH = WORKTREE / "scripts" / "spikes" / "raft_password_spike.py"

# Contrato de extensión deliberadamente verificable: los paquetes posteriores
# añaden la comprobación concreta al mismo replay, sin fabricar otro fixture.
FOLLOW_UP_CONTRACT = {
    "B1": {
        "state": "ControlTree._locks",
        "recovery": "__setstate__ inicializa {} cuando falta en el snapshot",
        "legacy_outcome": "abort_upload -> None",
    },
    "B3": {
        "state": "FileNode.version y FileNode.block_size",
        "recovery": "defaults simples por atributo de clase; begin_upload conserva argumentos legacy",
        "legacy_outcome": "applied_ops conserva resultados existentes",
    },
    "C2": {
        "state": "ControlTree._users",
        "recovery": "__setstate__ inicializa {} cuando falta en el snapshot",
        "legacy_outcome": "replay no convierte firmas legacy en TypeError",
    },
    "C3": {
        "state": "DirNode/FileNode owner, group y mode",
        "recovery": "defaults simples admin, 0o755 y 0o644 para objetos legacy",
        "legacy_outcome": "applied_ops conserva abort_upload -> None",
    },
}


def _canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _block_projection(
    block_id: str, checksum: str, size_bytes: int, confirmed: bool
) -> dict[str, object]:
    return {
        "block_id": block_id,
        "checksum": checksum,
        "confirmed": confirmed,
        "datanode_addresses": ["legacy-dn:50061"],
        "size_bytes": size_bytes,
    }


def _begin_upload_outcome(block_id: str) -> list[object]:
    return ["ok", [[[block_id, ["legacy-dn:50061"]]], []]]


_SNAPSHOT_BLOCK_ID = "a" * 32
_JOURNAL_BLOCK_ID = "c" * 32
EXPECTED_LEGACY_PROJECTION = _canonical_json(
    {
        "tree": {
            "directories": ["/", "/snapshot"],
            "files": {
                "/snapshot/file.bin": {
                    "blocks": [
                        _block_projection(
                            _SNAPSHOT_BLOCK_ID,
                            "snapshot-checksum",
                            8,
                            True,
                        )
                    ],
                    "state": "committed",
                }
            },
        },
        "applied_ops": {
            "legacy-snapshot-mkdir": ["ok", None],
            "legacy-snapshot-begin": _begin_upload_outcome(_SNAPSHOT_BLOCK_ID),
            "legacy-snapshot-confirm": ["ok", None],
            "legacy-snapshot-complete": ["ok", None],
            "legacy-snapshot-begin-abort": _begin_upload_outcome("b" * 32),
            "legacy-snapshot-abort": ["ok", None],
            "legacy-journal-mkdir": ["ok", None],
            "legacy-journal-begin": _begin_upload_outcome(_JOURNAL_BLOCK_ID),
            "legacy-journal-confirm": ["ok", None],
            "legacy-journal-complete": ["ok", None],
            "legacy-journal-begin-abort": _begin_upload_outcome("d" * 32),
            "legacy-journal-abort": ["ok", None],
            "legacy-journal-remove": [
                "ok",
                [_block_projection(_JOURNAL_BLOCK_ID, "journal-checksum", 7, True)],
            ],
            "legacy-journal-rmdir": ["ok", None],
        },
    }
)


def _canonical_legacy_value(value):
    from dfsha.control_node.tree import BlockRecord

    if isinstance(value, BlockRecord):
        return {
            "block_id": value.block_id,
            "checksum": value.checksum,
            "confirmed": value.confirmed,
            "datanode_addresses": list(value.datanode_addresses),
            "size_bytes": value.size_bytes,
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_legacy_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _canonical_legacy_value(item) for key, item in value.items()}
    return value


def _canonical_legacy_projection(replica) -> str:
    """Proyecta todo el árbol restaurado y los outcomes legacy sin depender de pickle."""
    from dfsha.control_node.tree import DirNode, FileNode

    directories: list[str] = []
    files: dict[str, object] = {}

    def visit(directory: DirNode, path: str) -> None:
        directories.append(path)
        for name, child in sorted(directory.children.items()):
            child_path = f"/{name}" if path == "/" else f"{path}/{name}"
            if isinstance(child, DirNode):
                visit(child, child_path)
            elif isinstance(child, FileNode):
                files[child_path] = {
                    "blocks": [_canonical_legacy_value(block) for block in child.blocks],
                    "state": child.state,
                }
            else:
                raise AssertionError(f"nodo legacy desconocido en {child_path}: {type(child)!r}")

    with replica.tree._lock:
        visit(replica.tree._root, "/")

    return _canonical_json(
        {
            "tree": {"directories": sorted(directories), "files": files},
            "applied_ops": {
                op_id: _canonical_legacy_value(outcome)
                for op_id, outcome in replica.applied_ops.items()
            },
        }
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _start_restored_cluster(tmp_path: Path):
    """Restaura los tres journals/dumps reales con direcciones nuevas y libres."""
    fixture = _fixture_manifest()
    data_dirs = []
    for node_name in fixture["nodes"]:
        target = tmp_path / node_name
        shutil.copytree(FIXTURE_ROOT / node_name, target)
        data_dirs.append(target)

    addresses = [f"localhost:{free_port()}" for _ in range(3)]
    conf_overrides = {**FAST_RAFT_CONF, "connectionRetryTime": 0.05}
    nodes: list[tuple[SyncObj, ReplicatedTree]] = []
    for index, address in enumerate(addresses):
        replicated = ReplicatedTree()
        raft = SyncObj(
            address,
            [peer for peer in addresses if peer != address],
            conf=build_raft_conf(data_dirs[index], conf_overrides),
            consumers=[replicated],
        )
        nodes.append((raft, replicated))

    def leader_index():
        leaders = [index for index, (raft, _) in enumerate(nodes) if raft._isLeader()]
        return leaders[0] if len(leaders) == 1 else None

    assert wait_for(lambda: leader_index() is not None), "el fixture restaurado no eligió líder"
    return nodes, leader_index


def _stop_cluster(nodes: list[tuple[SyncObj, ReplicatedTree]]) -> None:
    for raft, _ in nodes:
        raft.destroy()


def assert_legacy_upgrade(
    replicas: list[ReplicatedTree], manifest: dict, extension_checks=()
) -> None:
    """Comprueba el contrato estable que reutilizan B1/B3/C2/C3.

    ``extension_checks`` recibe cada réplica restaurada. Los paquetes que agreguen
    estado persistido lo usan para validar sus defaults sin reemplazar este replay.
    """
    legacy_operations = manifest["legacy_operations"]
    for replica in replicas:
        for operation in legacy_operations:
            outcome = replica.applied_ops[operation["op_id"]]
            assert outcome[0] == "ok", (operation, outcome)
            assert "TypeError" not in repr(outcome)
        assert replica.applied_ops[manifest["abort_upload_op_id"]] == ("ok", None)
        for check in extension_checks:
            check(replica)


def test_raft_password_spike_reports_three_node_restart_and_wrong_password(tmp_path):
    completed = subprocess.run(
        [sys.executable, str(SPIKE_PATH), "--workdir", str(tmp_path / "spike")],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "three_nodes=ok" in completed.stdout
    assert "restart=ok" in completed.stdout
    assert "wrong_password_isolated=ok" in completed.stdout


def test_legacy_fixture_is_from_main_9985c6d_and_hashes_match():
    manifest = _fixture_manifest()

    assert manifest["source_commit"] == "9985c6d"
    assert manifest["generator"] == "scripts/spikes/generate_legacy_raft_fixture.py"
    assert set(manifest["nodes"]) == {"cn0", "cn1", "cn2"}
    assert manifest["follow_up_contract"] == FOLLOW_UP_CONTRACT
    expected_artifacts = {
        f"{node}/{filename}"
        for node in manifest["nodes"]
        for filename in ("raft.dump", "raft.journal", "raft.journal.meta")
    }
    assert set(manifest["sha256"]) == expected_artifacts
    for relative_path, expected_hash in manifest["sha256"].items():
        artifact = FIXTURE_ROOT / relative_path
        assert artifact.is_file() and artifact.stat().st_size > 0
        assert _sha256(artifact) == expected_hash


def test_legacy_fixture_restores_replays_original_signatures_and_converges(tmp_path):
    manifest = _fixture_manifest()
    nodes, leader_index = _start_restored_cluster(tmp_path)
    try:
        replicas = [replicated for _, replicated in nodes]
        expected_op_ids = {operation["op_id"] for operation in manifest["legacy_operations"]}
        assert wait_for(lambda: all(expected_op_ids <= set(replica.applied_ops) for replica in replicas))
        assert_legacy_upgrade(replicas, manifest)

        projections = [_canonical_legacy_projection(replica) for replica in replicas]
        assert projections == [EXPECTED_LEGACY_PROJECTION] * len(replicas)

        leader = replicas[leader_index()]
        placements = [("f" * 32, ["legacy-dn:50061"])]
        assert leader.apply(
            "legacy-replay-begin", "begin_upload", ("/replayed.bin", placements), sync=True, timeout=1.0
        )[0] == "ok"
        assert leader.apply(
            "legacy-replay-confirm",
            "confirm_block",
            ("/replayed.bin", "f" * 32, "legacy-checksum", 7),
            sync=True,
            timeout=1.0,
        ) == ("ok", None)
        assert leader.apply(
            "legacy-replay-complete", "complete_upload", ("/replayed.bin",), sync=True, timeout=1.0
        ) == ("ok", None)

        replayed_ops = {"legacy-replay-begin", "legacy-replay-confirm", "legacy-replay-complete"}
        assert wait_for(lambda: all(replayed_ops <= set(replica.applied_ops) for replica in replicas))
        for replica in replicas:
            assert [block.block_id for block in replica.tree.list_blocks("/replayed.bin")] == ["f" * 32]
            assert all(replica.applied_ops[op_id][0] == "ok" for op_id in replayed_ops)
            assert "TypeError" not in repr([replica.applied_ops[op_id] for op_id in replayed_ops])
    finally:
        _stop_cluster(nodes)


@pytest.mark.parametrize("tampering", ("snapshot_metadata", "aborted_upload"))
def test_canonical_legacy_projection_rejects_tampered_legacy_state(tampering):
    """El verificador no acepta ni metadata alterada ni uploads abortados visibles."""
    from types import SimpleNamespace

    from dfsha.control_node.tree import ControlTree

    tree = ControlTree()
    tree.make_dir("/snapshot")
    tree.begin_upload("/snapshot/file.bin", [(_SNAPSHOT_BLOCK_ID, ["legacy-dn:50061"])])
    tree.confirm_block("/snapshot/file.bin", _SNAPSHOT_BLOCK_ID, "snapshot-checksum", 8)
    tree.complete_upload("/snapshot/file.bin")

    if tampering == "snapshot_metadata":
        tree.list_blocks("/snapshot/file.bin")[0].checksum = "altered-checksum"
    else:
        tree.begin_upload("/snapshot/aborted.bin", [("b" * 32, ["legacy-dn:50061"])])

    replica = SimpleNamespace(tree=tree, applied_ops=json.loads(EXPECTED_LEGACY_PROJECTION)["applied_ops"])
    with pytest.raises(AssertionError):
        assert _canonical_legacy_projection(replica) == EXPECTED_LEGACY_PROJECTION


def test_follow_up_upgrade_contract_is_precise_and_the_hook_runs():
    checked = []

    class Replica:
        applied_ops = {"legacy-abort": ("ok", None)}

    manifest = {
        "legacy_operations": [{"op_id": "legacy-abort"}],
        "abort_upload_op_id": "legacy-abort",
    }
    assert_legacy_upgrade([Replica()], manifest, extension_checks=(lambda replica: checked.append(replica),))
    assert checked and FOLLOW_UP_CONTRACT.keys() == {"B1", "B3", "C2", "C3"}



def test_legacy_fixture_initializes_locks_and_preserves_legacy_outcomes(tmp_path):
    manifest = _fixture_manifest()
    nodes, _ = _start_restored_cluster(tmp_path)
    try:
        replicas = [replicated for _, replicated in nodes]
        expected_op_ids = {operation["op_id"] for operation in manifest["legacy_operations"]}
        assert wait_for(lambda: all(expected_op_ids <= set(replica.applied_ops) for replica in replicas))
        assert_legacy_upgrade(
            replicas,
            manifest,
            extension_checks=(assert_legacy_locks,),
        )
    finally:
        _stop_cluster(nodes)


def assert_legacy_locks(replica) -> None:
    assert replica.tree._locks == {}


def test_legacy_fixture_supports_cow_writes_on_old_files(tmp_path):
    """B3 sobre estado real de 9985c6d: sin reservas, archivos sin version/block_size
    (los toman del atributo de clase) y write_layout que infiere el tamaño de bloque."""
    manifest = _fixture_manifest()
    nodes, _ = _start_restored_cluster(tmp_path)
    try:
        replicas = [replicated for _, replicated in nodes]
        expected_op_ids = {operation["op_id"] for operation in manifest["legacy_operations"]}
        assert wait_for(lambda: all(expected_op_ids <= set(replica.applied_ops) for replica in replicas))
        assert_legacy_upgrade(replicas, manifest, extension_checks=(assert_legacy_writes,))
    finally:
        _stop_cluster(nodes)


def assert_legacy_writes(replica) -> None:
    tree = replica.tree
    assert tree._writes == {}
    committed = [path for path, _ in tree.iter_blocks()]
    assert committed, "el fixture legacy tiene que traer archivos confirmados"
    for path in set(committed):
        version, block_size, sizes = tree.write_layout(path, 5)
        assert version == 0
        assert block_size > 0
        assert all(size == block_size for size in sizes[:-1])


def test_legacy_fixture_starts_without_users_and_accepts_new_ones(tmp_path):
    """C2 sobre estado real de 9985c6d: el snapshot no trae `_users`, el replay no cambia
    nada del árbol, y un usuario creado después converge en las tres réplicas."""
    manifest = _fixture_manifest()
    nodes, leader_index = _start_restored_cluster(tmp_path)
    try:
        replicas = [replicated for _, replicated in nodes]
        expected_op_ids = {operation["op_id"] for operation in manifest["legacy_operations"]}
        assert wait_for(lambda: all(expected_op_ids <= set(replica.applied_ops) for replica in replicas))
        assert_legacy_upgrade(replicas, manifest, extension_checks=(assert_legacy_users,))
        assert [_canonical_legacy_projection(replica) for replica in replicas] == [
            EXPECTED_LEGACY_PROJECTION
        ] * len(replicas)

        leader = replicas[leader_index()]
        assert leader.apply(
            "c2-upgrade-user", "create_user", ("alice", b"h" * 32, b"s" * 16, ("alice",), False),
            sync=True, timeout=1.0,
        ) == ("ok", None)

        assert wait_for(lambda: all(replica.tree.get_user("alice") is not None for replica in replicas))
        records = [replica.tree.get_user("alice") for replica in replicas]
        assert records[0].password_hash == b"h" * 32
        assert records == [records[0]] * len(replicas)
    finally:
        _stop_cluster(nodes)


def assert_legacy_users(replica) -> None:
    assert replica.tree._users == {}
    assert not replica.tree.has_users()


def test_legacy_fixture_gets_default_permissions_and_accepts_callers(tmp_path):
    """C3 sobre estado real de 9985c6d: los nodos no traen dueño, grupo ni modo y toman
    los defaults (la raíz, 0o777); un usuario común lee y no escribe; un comando con
    caller converge en las tres réplicas, también cuando se rechaza."""
    manifest = _fixture_manifest()
    nodes, leader_index = _start_restored_cluster(tmp_path)
    try:
        replicas = [replicated for _, replicated in nodes]
        expected_op_ids = {operation["op_id"] for operation in manifest["legacy_operations"]}
        assert wait_for(lambda: all(expected_op_ids <= set(replica.applied_ops) for replica in replicas))
        assert_legacy_upgrade(replicas, manifest, extension_checks=(assert_legacy_permissions,))
        assert [_canonical_legacy_projection(replica) for replica in replicas] == [
            EXPECTED_LEGACY_PROJECTION
        ] * len(replicas)

        alice = ("alice", ("alice",), False)
        leader = replicas[leader_index()]
        denied = leader.apply("c3-upgrade-denied", "make_dir", ("/snapshot/x", alice), sync=True, timeout=1.0)
        assert denied == ("error", "AccessDeniedError", "permiso denegado: falta w en /snapshot")
        assert leader.apply(
            "c3-upgrade-mkdir", "make_dir", ("/alice", alice), sync=True, timeout=1.0
        ) == ("ok", None)

        c3_ops = {"c3-upgrade-denied", "c3-upgrade-mkdir"}
        assert wait_for(lambda: all(c3_ops <= set(replica.applied_ops) for replica in replicas))
        for replica in replicas:
            assert replica.applied_ops["c3-upgrade-denied"] == denied
            assert "x" not in replica.tree._root.children["snapshot"].children
            created = replica.tree._root.children["alice"]
            assert (created.owner, created.group, created.mode) == ("alice", "alice", 0o755)
    finally:
        _stop_cluster(nodes)


def assert_legacy_permissions(replica) -> None:
    tree = replica.tree
    root = tree._root
    snapshot_dir = root.children["snapshot"]
    snapshot_file = snapshot_dir.children["file.bin"]
    assert (root.owner, root.group, root.mode) == ("admin", "admin", 0o777)
    assert (snapshot_dir.owner, snapshot_dir.group, snapshot_dir.mode) == ("admin", "admin", 0o755)
    assert (snapshot_file.owner, snapshot_file.group, snapshot_file.mode) == ("admin", "admin", 0o644)
    bob = ("bob", ("bob",), False)
    assert [entry.name for entry in tree.list_dir("/snapshot", bob)] == ["file.bin"]
    assert [block.block_id for block in tree.list_blocks("/snapshot/file.bin", bob)] == [_SNAPSHOT_BLOCK_ID]
