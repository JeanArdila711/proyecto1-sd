from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc
from pysyncobj import SyncObj, SyncObjConf

from dfsha.control_node.datanode_monitor import (
    DEFAULT_DATANODE_DEAD_AFTER_S,
    DEFAULT_HEARTBEAT_INTERVAL_S,
    DataNodeMonitor,
)
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.servicer import (
    DEFAULT_COMMIT_TIMEOUT_S,
    DEFAULT_MIN_WRITE_REPLICAS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_UPLOAD_LEASE_S,
    ControlNodeServicer,
)
from dfsha.generated import control_node_pb2_grpc

DEFAULT_BLOCK_SIZE_BYTES = 128 * 1024 * 1024


def build_raft_conf(data_dir: Path | None, raft_conf_overrides: dict | None = None) -> SyncObjConf:
    conf = {**(raft_conf_overrides or {}), "useFork": False}
    if data_dir is not None:
        data_dir.mkdir(parents=True, exist_ok=True)
        conf["journalFile"] = str(data_dir / "raft.journal")
        conf["fullDumpFile"] = str(data_dir / "raft.dump")
    return SyncObjConf(**conf)


def validate_datanode_configuration(
    datanode_addresses: list[str], replication_factor: int, min_write_replicas: int
) -> None:
    if not datanode_addresses:
        raise ValueError("hace falta al menos un DataNode")
    if replication_factor < 1:
        raise ValueError(f"el factor de replicación debe ser >= 1, no {replication_factor}")
    if not 1 <= min_write_replicas <= replication_factor:
        raise ValueError(
            "min_write_replicas debe estar entre 1 y replication_factor "
            f"({replication_factor}), no {min_write_replicas}"
        )
    if len(datanode_addresses) < min_write_replicas:
        raise ValueError(
            f"se configuraron {len(datanode_addresses)} DataNodes, menos que min_write_replicas={min_write_replicas}"
        )


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
    min_write_replicas: int = DEFAULT_MIN_WRITE_REPLICAS,
    heartbeat_interval_s: float = DEFAULT_HEARTBEAT_INTERVAL_S,
    datanode_dead_after_s: float = DEFAULT_DATANODE_DEAD_AFTER_S,
    raft_conf_overrides: dict | None = None,
    commit_timeout_s: float = DEFAULT_COMMIT_TIMEOUT_S,
    upload_lease_s: float = DEFAULT_UPLOAD_LEASE_S,
) -> tuple[grpc.Server, int, SyncObj]:
    """Arranca Raft, gRPC y el monitor pull local de DataNodes."""
    validate_datanode_configuration(datanode_addresses, replication_factor, min_write_replicas)
    replicated = ReplicatedTree()
    raft = SyncObj(
        raft_self, raft_peers, conf=build_raft_conf(data_dir, raft_conf_overrides), consumers=[replicated]
    )
    monitor = DataNodeMonitor(
        datanode_addresses,
        heartbeat_interval_s=heartbeat_interval_s,
        dead_after_s=datanode_dead_after_s,
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
        min_write_replicas,
        monitor,
    )
    control_node_pb2_grpc.add_ControlNodeServiceServicer_to_server(servicer, server)
    original_stop = server.stop

    def stop_with_cleanup(grace):
        termination = original_stop(grace)
        termination.wait()
        monitor.stop()
        servicer.close()
        return termination

    server.stop = stop_with_cleanup
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    monitor.start()
    # Atributos de diagnóstico para pruebas de lifecycle; no son parte del RPC.
    server._dfsha_datanode_monitor = monitor
    server._dfsha_control_servicer = servicer
    return server, bound_port, raft


def _split_addresses(value: str) -> list[str]:
    return [a.strip() for a in value.split(",") if a.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="ControlNode DFSha (Hito 3, liveness DataNode)")
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--raft-cluster", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--datanode-addresses", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--block-size-mb", type=int, default=128)
    parser.add_argument("--replication-factor", type=int, default=DEFAULT_REPLICATION_FACTOR)
    parser.add_argument("--min-write-replicas", type=int, default=DEFAULT_MIN_WRITE_REPLICAS)
    parser.add_argument("--heartbeat-interval-s", type=float, default=DEFAULT_HEARTBEAT_INTERVAL_S)
    parser.add_argument("--datanode-dead-after-s", type=float, default=DEFAULT_DATANODE_DEAD_AFTER_S)
    parser.add_argument("--upload-lease-s", type=float, default=DEFAULT_UPLOAD_LEASE_S)
    args = parser.parse_args()

    cluster = _split_addresses(args.raft_cluster)
    addresses = _split_addresses(args.datanode_addresses)
    if not 0 <= args.node_id < len(cluster):
        raise SystemExit(f"--node-id {args.node_id} fuera de rango para {len(cluster)} nodos en --raft-cluster")
    if len(set(cluster)) != len(cluster):
        raise SystemExit("--raft-cluster tiene direcciones repetidas")
    try:
        validate_datanode_configuration(addresses, args.replication_factor, args.min_write_replicas)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    raft_self = cluster[args.node_id]
    raft_peers = [a for i, a in enumerate(cluster) if i != args.node_id]
    server, bound_port, raft = serve(
        addresses,
        args.host,
        args.port,
        raft_self=raft_self,
        raft_peers=raft_peers,
        data_dir=Path(args.data_dir),
        block_size_bytes=args.block_size_mb * 1024 * 1024,
        replication_factor=args.replication_factor,
        min_write_replicas=args.min_write_replicas,
        heartbeat_interval_s=args.heartbeat_interval_s,
        datanode_dead_after_s=args.datanode_dead_after_s,
        upload_lease_s=args.upload_lease_s,
    )
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(
        f"ControlNode {args.node_id} escuchando en {args.host}:{bound_port}, "
        f"Raft={raft_self} peers={raft_peers}, DataNodes={addresses}, "
        f"factor={args.replication_factor}, mínimo escritura={args.min_write_replicas}"
    )
    try:
        server.wait_for_termination()
    finally:
        server.stop(grace=1).wait()
        raft.destroy()


if __name__ == "__main__":
    main()
