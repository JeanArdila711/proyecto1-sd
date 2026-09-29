#!/usr/bin/env python3
"""Spike ejecutable de cifrado/authenticación Raft con ``SyncObjConf(password=...)``.

No usa puertos de DFSha. Cada ejecución reserva puertos efímeros y trabaja en el
``--workdir`` indicado, por lo que no interfiere con clústeres de desarrollo.
"""

from __future__ import annotations

import argparse
import socket
import time
from pathlib import Path

from pysyncobj import SyncObj, SyncObjConf, SyncObjConsumer, replicated


FAST_CONF = {
    "raftMinTimeout": 0.05,
    "raftMaxTimeout": 0.1,
    "appendEntriesPeriod": 0.01,
    "leaderFallbackTimeout": 0.5,
    "autoTickPeriod": 0.005,
    "connectionRetryTime": 0.05,
    # El spike también protege la configuración obligatoria del servicio real.
    "useFork": False,
}


class ProbeState(SyncObjConsumer):
    """Estado determinista mínimo para comprobar consenso y recuperación."""

    def __init__(self) -> None:
        super().__init__()
        self.values: list[str] = []

    @replicated
    def append(self, value: str) -> None:
        self.values.append(value)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]


def _port_is_free(port: int) -> bool:
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("localhost", port))
        except OSError:
            return False
    return True


def _wait_for(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def _start_cluster(addresses: list[str], data_root: Path, passwords: list[str]):
    nodes: list[tuple[SyncObj, ProbeState]] = []
    for index, address in enumerate(addresses):
        node_dir = data_root / f"cn{index}"
        node_dir.mkdir(parents=True, exist_ok=True)
        state = ProbeState()
        conf = SyncObjConf(
            password=passwords[index],
            journalFile=str(node_dir / "raft.journal"),
            fullDumpFile=str(node_dir / "raft.dump"),
            **FAST_CONF,
        )
        raft = SyncObj(address, [peer for peer in addresses if peer != address], conf=conf, consumers=[state])
        nodes.append((raft, state))
    return nodes


def _stop_cluster(nodes: list[tuple[SyncObj, ProbeState]], addresses: list[str]) -> None:
    for raft, _ in nodes:
        raft.destroy()
    for address in addresses:
        port = int(address.rsplit(":", 1)[1])
        if not _wait_for(lambda port=port: _port_is_free(port)):
            raise RuntimeError(f"Raft no liberó el puerto {port}")


def _leader(nodes: list[tuple[SyncObj, ProbeState]]) -> tuple[SyncObj, ProbeState]:
    leaders = [(raft, state) for raft, state in nodes if raft._isLeader()]
    if len(leaders) != 1:
        raise RuntimeError("el clúster no tiene un líder único")
    return leaders[0]


def _wait_for_leader(nodes: list[tuple[SyncObj, ProbeState]]) -> tuple[SyncObj, ProbeState]:
    if not _wait_for(lambda: len([raft for raft, _ in nodes if raft._isLeader()]) == 1):
        raise RuntimeError("el clúster no eligió líder")
    return _leader(nodes)


def _assert_values(nodes: list[tuple[SyncObj, ProbeState]], expected: list[str]) -> None:
    if not _wait_for(lambda: all(state.values == expected for _, state in nodes)):
        raise RuntimeError(f"réplicas no convergieron a {expected!r}: {[state.values for _, state in nodes]!r}")


def run_spike(workdir: Path) -> None:
    if workdir.exists() and any(workdir.iterdir()):
        raise ValueError(f"--workdir debe estar vacío: {workdir}")
    workdir.mkdir(parents=True, exist_ok=True)

    password = "raft-password-spike"
    addresses = [f"localhost:{_free_port()}" for _ in range(3)]
    correct_nodes = _start_cluster(addresses, workdir / "persistent", [password] * 3)
    try:
        leader_raft, leader_state = _wait_for_leader(correct_nodes)
        leader_state.append("snapshot-entry", sync=True, timeout=2.0)
        _assert_values(correct_nodes, ["snapshot-entry"])

        for raft, _ in correct_nodes:
            raft.forceLogCompaction()
        dumps = [workdir / "persistent" / f"cn{index}" / "raft.dump" for index in range(3)]
        if not _wait_for(lambda: all(dump.is_file() and dump.stat().st_size > 0 for dump in dumps)):
            raise RuntimeError("el spike no escribió snapshots")

        leader_state.append("journal-entry", sync=True, timeout=2.0)
        _assert_values(correct_nodes, ["snapshot-entry", "journal-entry"])
    finally:
        _stop_cluster(correct_nodes, addresses)

    restarted_nodes = _start_cluster(addresses, workdir / "persistent", [password] * 3)
    try:
        _, restarted_leader = _wait_for_leader(restarted_nodes)
        _assert_values(restarted_nodes, ["snapshot-entry", "journal-entry"])
        restarted_leader.append("after-restart", sync=True, timeout=2.0)
        _assert_values(restarted_nodes, ["snapshot-entry", "journal-entry", "after-restart"])
    finally:
        _stop_cluster(restarted_nodes, addresses)

    mixed_addresses = [f"localhost:{_free_port()}" for _ in range(3)]
    mixed_nodes = _start_cluster(
        mixed_addresses,
        workdir / "wrong-password",
        [password, password, "incorrect-password"],
    )
    try:
        _, leader_state = _wait_for_leader(mixed_nodes[:2])
        leader_state.append("majority-survives", sync=True, timeout=2.0)
        _assert_values(mixed_nodes[:2], ["majority-survives"])
        # Darle al tercero tiempo suficiente para intentar el handshake cifrado.
        time.sleep(0.2)
        if mixed_nodes[2][0]._isLeader() or mixed_nodes[2][1].values:
            raise RuntimeError("un nodo con password incorrecta no quedó aislado")
    finally:
        _stop_cluster(mixed_nodes, mixed_addresses)


def main() -> None:
    parser = argparse.ArgumentParser(description="Spike de password Fernet para Raft")
    parser.add_argument("--workdir", type=Path, required=True, help="directorio vacío para journals y snapshots")
    args = parser.parse_args()
    run_spike(args.workdir)
    print("three_nodes=ok restart=ok wrong_password_isolated=ok")


if __name__ == "__main__":
    main()
