"""Mide si el snapshot de Raft le cuesta el liderazgo al líder, según la cantidad de archivos.

    python scripts/medir_snapshot_raft.py
    python scripts/medir_snapshot_raft.py --archivos 10000,50000 --duracion 30 --salida resultados.csv

pysyncobj serializa el snapshot en el mismo hilo que manda los heartbeats de Raft (el
proyecto usa useFork=False). Mientras dura, el líder no le habla a nadie, y un seguidor
que pasa entre 0,4 y 1,4 segundos sin oírlo (los valores por defecto de pysyncobj, que el
proyecto no cambia) llama a elección.

Levanta tres nodos Raft reales, cada uno en su proceso, con la misma configuración que un
ControlNode pero sin gRPC, y les carga el mismo árbol sintético de
scripts/medir_escalabilidad.py. El que es líder fuerza un snapshot, espera y anota si
sigue siendo líder. Las elecciones dependen de timeouts aleatorios: dos corridas no dan
los mismos conteos. Resultados y análisis: docs/escalabilidad.md.
"""

from __future__ import annotations

import argparse
import csv
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pysyncobj import SyncObj  # noqa: E402

from dfsha.control_node.main import build_raft_conf  # noqa: E402
from dfsha.control_node.replicated_tree import ReplicatedTree  # noqa: E402
from medir_escalabilidad import build_tree  # noqa: E402

COLUMNS = ["archivos", "snapshots", "liderazgos_perdidos", "terminos_nuevos", "snapshot_ms"]


def run_node(me: str, others: list[str], files: int, data_dir: Path, duration: float, wait: float, seed: int) -> None:
    tree = build_tree(files, 2, 5, 3, seed)
    print("listo", flush=True)
    sys.stdin.readline()  # el proceso padre avisa cuando los tres terminaron de armar el árbol
    replicated = ReplicatedTree()
    node = SyncObj(me, others, conf=build_raft_conf(data_dir, None, "x" * 40), consumers=[replicated])
    replicated.tree = tree  # el mismo árbol en los tres: sale de la misma semilla

    # pysyncobj no dice cuánto tardó un snapshot: se envuelve su serializador, que es privado.
    serializer = node._SyncObj__serializer
    serialize = serializer.serialize
    durations: list[float] = []

    def timed(data, entry_id):
        started = time.perf_counter()
        serialize(data, entry_id)
        durations.append(time.perf_counter() - started)

    serializer.serialize = timed

    snapshots = lost = terms = attempt = 0
    end = time.monotonic() + duration
    while time.monotonic() < end:
        time.sleep(0.5)
        if node.getStatus()["leader"] != node.selfNode:
            continue
        attempt += 1
        try:  # el snapshot necesita una entrada aplicada después del anterior
            replicated.apply(f"{me}-{attempt}", "mkdir", (f"/x-{me.replace(':', '-')}-{attempt}",), sync=True, timeout=5)
        except Exception:  # noqa: BLE001 - perdió el liderazgo entre el chequeo y el commit
            continue
        term = node.getStatus()["raft_term"]
        taken = len(durations)
        node.forceLogCompaction()
        time.sleep(wait)
        if len(durations) == taken:
            continue
        status = node.getStatus()
        snapshots += 1
        lost += status["leader"] != node.selfNode
        terms += status["raft_term"] - term
    print(f"RES {snapshots} {lost} {terms} {statistics.median(durations) * 1000 if durations else 0:.0f}", flush=True)
    node.destroy()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def measure(files: int, duration: float, wait: float, seed: int) -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="dfsha-snapshot-"))
    addresses = [f"127.0.0.1:{_free_port()}" for _ in range(3)]
    try:
        nodes = [
            subprocess.Popen(
                [
                    sys.executable, __file__, "--nodo", address, "--otros", ",".join(a for a in addresses if a != address),
                    "--archivos", str(files), "--dir", str(tmp / f"n{i}"),
                    "--duracion", str(duration), "--espera", str(wait), "--semilla", str(seed),
                ],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            )
            for i, address in enumerate(addresses)
        ]
        for node in nodes:
            if node.stdout.readline().strip() != "listo":
                raise SystemExit("un nodo no arrancó")
        row = {"archivos": files, "snapshots": 0, "liderazgos_perdidos": 0, "terminos_nuevos": 0, "snapshot_ms": 0}
        # Los tres arrancan Raft a la vez, cuando todos terminaron de armar el árbol.
        for node in nodes:
            node.stdin.write("\n")
            node.stdin.flush()
        for node in nodes:
            result = next(line for line in node.communicate()[0].splitlines() if line.startswith("RES")).split()[1:]
            row["snapshots"] += int(result[0])
            row["liderazgos_perdidos"] += int(result[1])
            row["terminos_nuevos"] += int(result[2])
            if int(result[0]):
                row["snapshot_ms"] = max(row["snapshot_ms"], int(result[3]))
        return row
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="mide si el snapshot de Raft le cuesta el liderazgo al líder")
    parser.add_argument("--archivos", default="10000,25000,50000,100000", help="valores de N, separados por comas")
    parser.add_argument("--duracion", type=float, default=60.0, help="segundos que corre el clúster para cada N")
    parser.add_argument("--espera", type=float, default=4.0, help="segundos entre forzar el snapshot y mirar quién es líder")
    parser.add_argument("--semilla", type=int, default=2026)
    parser.add_argument("--salida", type=Path, help="archivo CSV donde guardar los resultados")
    parser.add_argument("--nodo", help=argparse.SUPPRESS)  # uso interno: este proceso es uno de los tres nodos
    parser.add_argument("--otros", help=argparse.SUPPRESS)
    parser.add_argument("--dir", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        sizes = [int(value) for value in args.archivos.split(",") if value.strip()]
    except ValueError:
        raise SystemExit("--archivos tiene que ser una lista de enteros separados por comas") from None
    if not sizes or min(sizes) < 1 or args.duracion <= 0 or args.espera <= 0:
        raise SystemExit("--archivos, --duracion y --espera tienen que ser mayores que cero")
    if args.nodo:
        run_node(args.nodo, args.otros.split(","), sizes[0], args.dir, args.duracion, args.espera, args.semilla)
        return

    print(f"3 nodos Raft, {args.duracion:g} s por valor de N, {args.espera:g} s de espera tras cada snapshot\n")
    print("| " + " | ".join(COLUMNS) + " |\n|" + "---:|" * len(COLUMNS))
    rows = []
    for files in sizes:
        rows.append(measure(files, args.duracion, args.espera, args.semilla))
        print("| " + " | ".join(str(rows[-1][column]) for column in COLUMNS) + " |", flush=True)
    if args.salida:
        with args.salida.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nguardado en {args.salida}")


if __name__ == "__main__":
    main()
