from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc
from pysyncobj import SyncObj, SyncObjConf

from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.servicer import (
    DEFAULT_COMMIT_TIMEOUT_S,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_UPLOAD_LEASE_S,
    ControlNodeServicer,
)
from dfsha.generated import control_node_pb2_grpc

DEFAULT_BLOCK_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB

def build_raft_conf(data_dir: Path | None, raft_conf_overrides: dict | None = None) -> SyncObjConf:
    # Timeouts de Raft: los defaults de pysyncobj; los tests los acortan.
    conf = {
        **(raft_conf_overrides or {}),
        # La compactación del log por defecto hace fork() del proceso para serializar
        # el snapshot, y fork() con los hilos de gRPC corriendo no está soportado.
        "useFork": False,
    }
    if data_dir is not None:
        data_dir.mkdir(parents=True, exist_ok=True)
        conf["journalFile"] = str(data_dir / "raft.journal")
        conf["fullDumpFile"] = str(data_dir / "raft.dump")
    return SyncObjConf(**conf)


def serve(
    datanode_addresses: list[str],
    host: str,
    port: int,
    *,
    raft_self: str,
    raft_peers: list[str],
    data_dir: Path | None,
    block_size_bytes: int = DEFAULT_BLOCK_SIZE_BYTES,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    raft_conf_overrides: dict | None = None,
    commit_timeout_s: float = DEFAULT_COMMIT_TIMEOUT_S,
    upload_lease_s: float = DEFAULT_UPLOAD_LEASE_S,
) -> tuple[grpc.Server, int, SyncObj]:
    """Arranca un ControlNode: su nodo Raft y su servidor gRPC.

    data_dir=None deja el log solo en memoria (tests). En producción siempre va un
    directorio: sin él, reiniciar los 3 nodos pierde el árbol entero.
    Quien llama es dueño del SyncObj devuelto y tiene que hacerle destroy().
    """
    replicated = ReplicatedTree()
    raft = SyncObj(
        raft_self, raft_peers, conf=build_raft_conf(data_dir, raft_conf_overrides), consumers=[replicated]
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ControlNodeServicer(
        raft,
        replicated,
        datanode_addresses,
        block_size_bytes,
        replication_factor,
        commit_timeout_s,
        upload_lease_s,
    )
    control_node_pb2_grpc.add_ControlNodeServiceServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port, raft


def _split_addresses(value: str) -> list[str]:
    return [a.strip() for a in value.split(",") if a.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="ControlNode DFSha (Hito 2, clúster Raft)")
    parser.add_argument(
        "--node-id",
        type=int,
        required=True,
        help="posición de este nodo dentro de --raft-cluster (empieza en 0)",
    )
    parser.add_argument(
        "--raft-cluster",
        required=True,
        help="host:port de Raft de TODOS los ControlNodes, separados por comas; "
        "la misma lista y en el mismo orden en cada nodo",
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        help="directorio propio de este nodo para el journal y los snapshots de Raft",
    )
    parser.add_argument(
        "--datanode-addresses",
        required=True,
        help="host:port de los DataNodes, separados por comas",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--block-size-mb", type=int, default=128)
    parser.add_argument("--replication-factor", type=int, default=DEFAULT_REPLICATION_FACTOR)
    parser.add_argument(
        "--upload-lease-s",
        type=float,
        default=DEFAULT_UPLOAD_LEASE_S,
        help="segundos que una subida puede estar sin confirmar un bloque antes de que su "
        "nombre quede libre (cubre a un cliente que murió a mitad de subida)",
    )
    args = parser.parse_args()

    cluster = _split_addresses(args.raft_cluster)
    addresses = _split_addresses(args.datanode_addresses)
    if not addresses:
        raise SystemExit("--datanode-addresses no puede quedar vacío")
    if not 0 <= args.node_id < len(cluster):
        raise SystemExit(f"--node-id {args.node_id} fuera de rango para {len(cluster)} nodos en --raft-cluster")
    if len(set(cluster)) != len(cluster):
        raise SystemExit("--raft-cluster tiene direcciones repetidas")

    raft_self = cluster[args.node_id]
    raft_peers = [a for i, a in enumerate(cluster) if i != args.node_id]
    server, bound_port, _ = serve(
        addresses,
        args.host,
        args.port,
        raft_self=raft_self,
        raft_peers=raft_peers,
        data_dir=Path(args.data_dir),
        block_size_bytes=args.block_size_mb * 1024 * 1024,
        replication_factor=args.replication_factor,
        upload_lease_s=args.upload_lease_s,
    )
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    effective = min(args.replication_factor, len(addresses))
    print(
        f"ControlNode {args.node_id} escuchando en {args.host}:{bound_port}, "
        f"Raft={raft_self} peers={raft_peers}, data-dir={args.data_dir}, "
        f"DataNodes={addresses}, factor de replicación={effective}"
    )
    server.wait_for_termination()


if __name__ == "__main__":
    main()
