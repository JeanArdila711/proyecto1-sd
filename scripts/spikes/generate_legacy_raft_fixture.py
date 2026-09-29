#!/usr/bin/env python3
"""Genera fixtures Raft desde el código de ``9985c6d`` anterior a Hito 3.

El script se niega a correr si ``HEAD`` no es exactamente esa base. Los comandos
replicados usan de forma explícita las firmas previas a Hito 3, sin argumentos de
los paquetes B1/B3/C2/C3.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pysyncobj import SyncObj

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.control_node.main import build_raft_conf
from dfsha.control_node.replicated_tree import ReplicatedTree


EXPECTED_COMMIT = "9985c6d"
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


def _head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def _start_cluster(output: Path):
    addresses = [f"localhost:{free_port()}" for _ in range(3)]
    conf_overrides = {**FAST_RAFT_CONF, "connectionRetryTime": 0.05}
    nodes: list[tuple[SyncObj, ReplicatedTree]] = []
    for index, address in enumerate(addresses):
        replicated = ReplicatedTree()
        raft = SyncObj(
            address,
            [peer for peer in addresses if peer != address],
            conf=build_raft_conf(output / f"cn{index}", conf_overrides),
            consumers=[replicated],
        )
        nodes.append((raft, replicated))

    def leader_index():
        leaders = [index for index, (raft, _) in enumerate(nodes) if raft._isLeader()]
        return leaders[0] if len(leaders) == 1 else None

    assert wait_for(lambda: leader_index() is not None), "el clúster de fixtures no eligió líder"
    return nodes, leader_index


def _stop_cluster(nodes: list[tuple[SyncObj, ReplicatedTree]]) -> None:
    for raft, _ in nodes:
        raft.destroy()


def _commit(replicated: ReplicatedTree, op_id: str, method: str, *args):
    outcome = replicated.apply(op_id, method, args, sync=True, timeout=2.0)
    assert outcome[0] == "ok", (op_id, outcome)
    return outcome


def _wait_for_ops(nodes: list[tuple[SyncObj, ReplicatedTree]], operations: list[dict]) -> None:
    op_ids = {operation["op_id"] for operation in operations}
    assert wait_for(lambda: all(op_ids <= set(replicated.applied_ops) for _, replicated in nodes))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def generate(output: Path) -> None:
    head = _head()
    if not head.startswith(EXPECTED_COMMIT):
        raise RuntimeError(f"los fixtures solo se generan desde {EXPECTED_COMMIT}; HEAD actual: {head}")
    if output.exists():
        raise RuntimeError(f"la salida ya existe; no sobrescribo artefactos versionados: {output}")

    output.mkdir(parents=True)
    nodes, leader_index = _start_cluster(output)
    snapshot_operations = [
        {"op_id": "legacy-snapshot-mkdir", "method": "make_dir", "args": ["/snapshot"]},
        {
            "op_id": "legacy-snapshot-begin",
            "method": "begin_upload",
            "args": ["/snapshot/file.bin", [["a" * 32, ["legacy-dn:50061"]]]],
        },
        {
            "op_id": "legacy-snapshot-confirm",
            "method": "confirm_block",
            "args": ["/snapshot/file.bin", "a" * 32, "snapshot-checksum", 8],
        },
        {"op_id": "legacy-snapshot-complete", "method": "complete_upload", "args": ["/snapshot/file.bin"]},
        {
            "op_id": "legacy-snapshot-begin-abort",
            "method": "begin_upload",
            "args": ["/snapshot/aborted.bin", [["b" * 32, ["legacy-dn:50061"]]]],
        },
        {"op_id": "legacy-snapshot-abort", "method": "abort_upload", "args": ["/snapshot/aborted.bin"]},
    ]
    journal_operations = [
        {"op_id": "legacy-journal-mkdir", "method": "make_dir", "args": ["/journal"]},
        {
            "op_id": "legacy-journal-begin",
            "method": "begin_upload",
            "args": ["/journal/replayed.bin", [["c" * 32, ["legacy-dn:50061"]]]],
        },
        {
            "op_id": "legacy-journal-confirm",
            "method": "confirm_block",
            "args": ["/journal/replayed.bin", "c" * 32, "journal-checksum", 7],
        },
        {"op_id": "legacy-journal-complete", "method": "complete_upload", "args": ["/journal/replayed.bin"]},
        {
            "op_id": "legacy-journal-begin-abort",
            "method": "begin_upload",
            "args": ["/journal/aborted.bin", [["d" * 32, ["legacy-dn:50061"]]]],
        },
        {"op_id": "legacy-journal-abort", "method": "abort_upload", "args": ["/journal/aborted.bin"]},
        {"op_id": "legacy-journal-remove", "method": "remove_file", "args": ["/journal/replayed.bin"]},
        {"op_id": "legacy-journal-rmdir", "method": "remove_dir", "args": ["/journal"]},
    ]
    all_operations = [*snapshot_operations, *journal_operations]
    try:
        leader = nodes[leader_index()][1]
        for operation in snapshot_operations:
            _commit(leader, operation["op_id"], operation["method"], *operation["args"])
        _wait_for_ops(nodes, snapshot_operations)

        for raft, _ in nodes:
            raft.forceLogCompaction()
        dumps = [output / f"cn{index}" / "raft.dump" for index in range(3)]
        assert wait_for(lambda: all(dump.is_file() and dump.stat().st_size > 0 for dump in dumps))

        for operation in journal_operations:
            _commit(leader, operation["op_id"], operation["method"], *operation["args"])
        _wait_for_ops(nodes, all_operations)
    finally:
        _stop_cluster(nodes)

    files = [
        output / f"cn{index}" / name
        for index in range(3)
        for name in ("raft.dump", "raft.journal", "raft.journal.meta")
    ]
    if not all(path.is_file() and path.stat().st_size > 0 for path in files):
        raise RuntimeError("faltan dump o journal reales después de cerrar el clúster")
    manifest = {
        "source_commit": EXPECTED_COMMIT,
        "source_commit_full": head,
        "generator": "scripts/spikes/generate_legacy_raft_fixture.py",
        "nodes": ["cn0", "cn1", "cn2"],
        "legacy_operations": all_operations,
        "abort_upload_op_id": "legacy-snapshot-abort",
        "follow_up_contract": FOLLOW_UP_CONTRACT,
        "sha256": {str(path.relative_to(output)): _sha256(path) for path in files},
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera fixture Raft legacy desde 9985c6d")
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "tests" / "fixtures" / "raft_legacy_9985c6d",
        help="directorio nuevo que recibirá cn0..cn2 y manifest.json",
    )
    args = parser.parse_args()
    generate(args.output)
    print(f"Fixture Raft legacy generado en {args.output}")


if __name__ == "__main__":
    main()
