"""Mide cuánto cuestan los recorridos completos de la metadata según la cantidad de archivos.

    python scripts/medir_escalabilidad.py
    python scripts/medir_escalabilidad.py --archivos 1000,10000 --repeticiones 3 --salida resultados.csv
    python scripts/medir_escalabilidad.py --sin-gc      # separa el costo del recorrido del costo del GC de Python

El ControlNode guarda toda la metadata en memoria, en un árbol con un solo lock, y hay
tres operaciones que lo recorren entero: la foto que toma el re-replicador en cada ciclo
(`iter_blocks`), la que toma el recolector (`referenced_blocks`) y el snapshot de Raft,
que serializa el árbol completo. Las dos primeras corren con el lock del árbol tomado:
mientras duran, ninguna otra operación de metadata avanza. El snapshot corre en el hilo de
Raft (el proyecto usa useFork=False): mientras dura, ese nodo no aplica comandos.

No levanta nodos ni usa red: arma un `ControlTree` real con N archivos sintéticos y mide
esas operaciones sobre él. Los archivos salen de una semilla, así que dos corridas con los
mismos argumentos miden exactamente el mismo árbol. Resultados y análisis:
docs/escalabilidad.md.
"""

from __future__ import annotations

import argparse
import csv
import gc
import gzip
import io
import platform
import random
import statistics
import sys
import time
import tracemalloc
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pysyncobj.pickle as raft_pickle  # noqa: E402

from dfsha.control_node.rereplicator import ReReplicator  # noqa: E402
from dfsha.control_node.tree import ControlTree  # noqa: E402

BLOCK_SIZE = 128 * 1024 * 1024
FILES_PER_DIR = 1000
COLUMNS = [
    "archivos", "bloques", "memoria_mb", "iter_blocks_ms", "referenced_blocks_ms",
    "ciclo_rereplicador_ms", "snapshot_ms", "snapshot_mb",
]


def build_tree(files: int, blocks_per_file: int, datanodes: int, replicas: int, seed: int) -> ControlTree:
    """Un árbol con `files` archivos confirmados, repartidos de a FILES_PER_DIR por
    directorio. Cada bloque queda en `replicas` DataNodes consecutivos, rotando el primero,
    como hace el round-robin del ControlNode."""
    rng = random.Random(seed)
    addresses = [f"dn{i}:50061" for i in range(1, datanodes + 1)]
    tree = ControlTree()
    block = 0
    for i in range(files):
        path = f"/d{i // FILES_PER_DIR:05d}/f{i:08d}"
        placements = []
        for _ in range(blocks_per_file):
            placements.append(
                (f"{rng.getrandbits(128):032x}", [addresses[(block + r) % datanodes] for r in range(replicas)])
            )
            block += 1
        tree.begin_upload(path, placements, None, None, BLOCK_SIZE)
        for block_id, _ in placements:
            tree.confirm_block(path, block_id, "0" * 64, BLOCK_SIZE)
        tree.complete_upload(path)
    return tree


def raft_snapshot(tree: ControlTree) -> bytes:
    """Lo mismo que hace pysyncobj 0.3.17 al compactar el log (serializer.py): su propio
    `pickle.dump`, que fija el protocolo 2, comprimido con gzip. Con el protocolo por
    defecto de Python sale varias veces más rápido, pero no es lo que corre en los nodos.
    Acá va solo el árbol; el snapshot real suma applied_ops, que está acotado a 10.000
    entradas, y la escritura a disco."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as compressed:
        raft_pickle.dump(tree, compressed)
    return buffer.getvalue()


def _median_ms(operation, repetitions: int) -> float:
    samples = []
    for _ in range(repetitions):
        started = time.perf_counter()
        operation()
        samples.append((time.perf_counter() - started) * 1000)
    return statistics.median(samples)


def measure(files: int, blocks_per_file: int, datanodes: int, replicas: int, seed: int, repetitions: int) -> dict:
    # La memoria del árbol se mide armándolo con tracemalloc activo; los tiempos, después
    # de apagarlo, porque tracemalloc vuelve varias veces más lenta cada asignación.
    tracemalloc.start()
    tree = build_tree(files, blocks_per_file, datanodes, replicas, seed)
    memory_bytes = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    # Un ciclo real del re-replicador con el clúster sano: toma la foto y revisa cada
    # bloque sin encontrar nada que reparar. Es el costo fijo que paga cada 10 segundos.
    alive = [f"dn{i}:50061" for i in range(1, datanodes + 1)]
    servicer = SimpleNamespace(
        _is_leader_raw=lambda: True, _read_barrier_raw=lambda: True, _replicated=SimpleNamespace(tree=tree)
    )
    monitor = SimpleNamespace(alive_addresses=lambda: alive, dead_for=lambda address: None)
    rereplicator = ReReplicator(servicer, monitor, replication_factor=replicas)

    return {
        "archivos": files,
        "bloques": files * blocks_per_file,
        "memoria_mb": round(memory_bytes / 1e6, 1),
        "iter_blocks_ms": round(_median_ms(tree.iter_blocks, repetitions), 2),
        "referenced_blocks_ms": round(_median_ms(lambda: tree.referenced_blocks(0.0), repetitions), 2),
        "ciclo_rereplicador_ms": round(_median_ms(rereplicator.run_cycle, repetitions), 2),
        "snapshot_ms": round(_median_ms(lambda: raft_snapshot(tree), repetitions), 2),
        "snapshot_mb": round(len(raft_snapshot(tree)) / 1e6, 2),
    }


def _table(rows: list[dict]) -> str:
    lines = ["| " + " | ".join(COLUMNS) + " |", "|" + "---:|" * len(COLUMNS)]
    lines += ["| " + " | ".join(str(row[column]) for column in COLUMNS) + " |" for row in rows]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="mide los recorridos completos de la metadata según N archivos")
    parser.add_argument("--archivos", default="1000,10000,50000,100000", help="valores de N, separados por comas")
    parser.add_argument("--bloques-por-archivo", type=int, default=2)
    parser.add_argument("--datanodes", type=int, default=5)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--repeticiones", type=int, default=7, help="se informa la mediana")
    parser.add_argument("--semilla", type=int, default=2026)
    parser.add_argument("--salida", type=Path, help="archivo CSV donde guardar los resultados")
    parser.add_argument(
        "--sin-gc", action="store_true",
        help="apaga el recolector de basura de Python mientras mide (los nodos reales lo tienen activo)",
    )
    args = parser.parse_args()
    try:
        sizes = [int(value) for value in args.archivos.split(",") if value.strip()]
    except ValueError:
        raise SystemExit("--archivos tiene que ser una lista de enteros separados por comas") from None
    if not sizes or min(sizes) < 1 or args.bloques_por_archivo < 1 or args.repeticiones < 1:
        raise SystemExit("--archivos, --bloques-por-archivo y --repeticiones tienen que ser mayores que cero")
    if not 1 <= args.replicas <= args.datanodes:
        raise SystemExit("--replicas tiene que estar entre 1 y --datanodes")

    print(
        f"Python {platform.python_version()} en {platform.system()} {platform.machine()}; "
        f"{args.bloques_por_archivo} bloques por archivo, {args.replicas} réplicas sobre {args.datanodes} DataNodes, "
        f"semilla {args.semilla}, mediana de {args.repeticiones} repeticiones, "
        f"recolector de basura de Python {'apagado' if args.sin_gc else 'activo'}\n"
    )
    if args.sin_gc:
        gc.disable()
    rows = []
    for files in sizes:
        rows.append(
            measure(files, args.bloques_por_archivo, args.datanodes, args.replicas, args.semilla, args.repeticiones)
        )
        print(f"  medido N = {files}", file=sys.stderr)
    print(_table(rows))
    if args.salida:
        with args.salida.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nguardado en {args.salida}")


if __name__ == "__main__":
    main()
