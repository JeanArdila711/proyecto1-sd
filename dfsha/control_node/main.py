from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc
from pysyncobj import SyncObj, SyncObjConf

from dfsha.common.tls import TlsConfig, add_port, add_tls_arguments, channel_factory, tls_from_args
from dfsha.control_node.datanode_monitor import (
    DEFAULT_DATANODE_DEAD_AFTER_S,
    DEFAULT_HEARTBEAT_INTERVAL_S,
    DataNodeMonitor,
)
from dfsha.control_node.garbage_collector import (
    DEFAULT_GC_GRACE_S,
    DEFAULT_GC_INTERVAL_S,
    GarbageCollector,
)
from dfsha.control_node.replicated_tree import ReplicatedTree
from dfsha.control_node.rereplicator import (
    DEFAULT_REREPLICATION_DELAY_S,
    DEFAULT_REREPLICATION_INTERVAL_S,
    DEFAULT_REREPLICATION_MAX_PER_CYCLE,
    ReReplicator,
)
from dfsha.control_node.servicer import (
    DEFAULT_COMMIT_TIMEOUT_S,
    DEFAULT_MIN_WRITE_REPLICAS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_UPLOAD_LEASE_S,
    DEFAULT_LOCK_LEASE_S,
    ControlNodeServicer,
)
from dfsha.generated import control_node_pb2_grpc

DEFAULT_BLOCK_SIZE_BYTES = 128 * 1024 * 1024


def build_raft_conf(
    data_dir: Path | None, raft_conf_overrides: dict | None = None, raft_password: str | None = None
) -> SyncObjConf:
    # Timeouts de Raft: los defaults de pysyncobj; los tests los acortan.
    conf = {
        **(raft_conf_overrides or {}),
        # La compactación del log por defecto hace fork() del proceso para serializar
        # el snapshot, y fork() con los hilos de gRPC corriendo no está soportado.
        "useFork": False,
    }
    if raft_password:
        # pysyncobj cifra y autentica cada mensaje entre ControlNodes con Fernet, con
        # una llave derivada de la password: un nodo sin ella no entra al clúster. El
        # journal en disco no cambia de formato.
        conf["password"] = raft_password
    if data_dir is not None:
        data_dir.mkdir(parents=True, exist_ok=True)
        conf["journalFile"] = str(data_dir / "raft.journal")
        conf["fullDumpFile"] = str(data_dir / "raft.dump")
    return SyncObjConf(**conf)


def load_raft_password(path: Path) -> str:
    password = path.read_text(encoding="utf-8").strip()
    if len(password) < 32:
        raise ValueError(f"la password de Raft en {path} es demasiado corta (mínimo 32 caracteres)")
    return password


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
    lock_lease_s: float = DEFAULT_LOCK_LEASE_S,
    rereplication_interval_s: float = DEFAULT_REREPLICATION_INTERVAL_S,
    rereplication_delay_s: float = DEFAULT_REREPLICATION_DELAY_S,
    rereplication_max_per_cycle: int = DEFAULT_REREPLICATION_MAX_PER_CYCLE,
    gc_interval_s: float = DEFAULT_GC_INTERVAL_S,
    gc_grace_s: float = DEFAULT_GC_GRACE_S,
    tls: TlsConfig | None = None,
    raft_password: str | None = None,
) -> tuple[grpc.Server, int, SyncObj]:
    """Arranca un ControlNode: su nodo Raft, su servidor gRPC y el monitor local de
    DataNodes, que se detiene junto con el servidor.

    data_dir=None deja el log solo en memoria (tests). En producción siempre va un
    directorio: sin él, reiniciar los 3 nodos pierde el árbol entero.
    Quien llama es dueño del SyncObj devuelto y tiene que hacerle destroy().
    """
    validate_datanode_configuration(datanode_addresses, replication_factor, min_write_replicas)
    replicated = ReplicatedTree()
    raft = SyncObj(
        raft_self,
        raft_peers,
        conf=build_raft_conf(data_dir, raft_conf_overrides, raft_password),
        consumers=[replicated],
    )
    # Todos los canales hacia DataNodes (Ping, DeleteBlock, ReplicateBlock, inventario)
    # usan el mismo TLS que el resto del clúster.
    datanode_channel = channel_factory(tls)
    monitor = DataNodeMonitor(
        datanode_addresses,
        heartbeat_interval_s=heartbeat_interval_s,
        dead_after_s=datanode_dead_after_s,
        channel_factory=datanode_channel,
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
        lock_lease_s,
        channel_factory=datanode_channel,
    )
    control_node_pb2_grpc.add_ControlNodeServiceServicer_to_server(servicer, server)
    rereplicator = ReReplicator(
        servicer,
        monitor,
        replication_factor=replication_factor,
        interval_s=rereplication_interval_s,
        delay_s=rereplication_delay_s,
        max_per_cycle=rereplication_max_per_cycle,
        channel_factory=datanode_channel,
    )
    garbage_collector = GarbageCollector(
        servicer,
        monitor,
        interval_s=gc_interval_s,
        grace_s=gc_grace_s,
        rereplicator=rereplicator,
        channel_factory=datanode_channel,
    )
    original_stop = server.stop

    def stop_with_cleanup(grace):
        termination = original_stop(grace)
        termination.wait()
        garbage_collector.stop()
        rereplicator.stop()
        monitor.stop()
        servicer.close()
        return termination

    server.stop = stop_with_cleanup
    bound_port = add_port(server, f"{host}:{port}", tls)
    server.start()
    monitor.start()
    rereplicator.start()
    garbage_collector.start()
    # Atributos de diagnóstico para pruebas de lifecycle; no son parte del RPC.
    server._dfsha_datanode_monitor = monitor
    server._dfsha_rereplicator = rereplicator
    server._dfsha_garbage_collector = garbage_collector
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
    parser.add_argument("--rereplication-interval-s", type=float, default=DEFAULT_REREPLICATION_INTERVAL_S)
    parser.add_argument("--rereplication-delay-s", type=float, default=DEFAULT_REREPLICATION_DELAY_S)
    parser.add_argument("--rereplication-max-per-cycle", type=int, default=DEFAULT_REREPLICATION_MAX_PER_CYCLE)
    parser.add_argument(
        "--upload-lease-s",
        type=float,
        default=DEFAULT_UPLOAD_LEASE_S,
        help="segundos que una subida puede estar sin confirmar un bloque antes de que su "
        "nombre quede libre (cubre a un cliente que murió a mitad de subida)",
    )
    parser.add_argument(
        "--lock-lease-s",
        type=float,
        default=DEFAULT_LOCK_LEASE_S,
        help="segundos de lease para locks lectores/escritor; el cliente renueva cada tercio",
    )
    parser.add_argument("--gc-interval-s", type=float, default=DEFAULT_GC_INTERVAL_S)
    parser.add_argument(
        "--gc-grace-s",
        type=float,
        default=DEFAULT_GC_GRACE_S,
        help="edad mínima de un bloque sin uso antes de borrarlo",
    )
    parser.add_argument(
        "--raft-password-file",
        help="password compartida por los ControlNodes: cifra y autentica el canal Raft",
    )
    add_tls_arguments(parser, server=True)
    args = parser.parse_args()
    tls = tls_from_args(args, server=True)
    raft_password = None
    if args.raft_password_file:
        try:
            raft_password = load_raft_password(Path(args.raft_password_file))
        except (OSError, ValueError) as exc:
            raise SystemExit(str(exc)) from exc
    else:
        print("AVISO: sin --raft-password-file, el canal Raft va en claro (solo para desarrollo)")

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
        rereplication_interval_s=args.rereplication_interval_s,
        rereplication_delay_s=args.rereplication_delay_s,
        rereplication_max_per_cycle=args.rereplication_max_per_cycle,
        upload_lease_s=args.upload_lease_s,
        lock_lease_s=args.lock_lease_s,
        gc_interval_s=args.gc_interval_s,
        gc_grace_s=args.gc_grace_s,
        tls=tls,
        raft_password=raft_password,
    )
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(
        f"ControlNode {args.node_id} escuchando en {args.host}:{bound_port}, "
        f"Raft={raft_self} peers={raft_peers}, DataNodes={addresses}, "
        f"factor={args.replication_factor}, mínimo escritura={args.min_write_replicas}, "
        f"TLS={tls is not None}, Raft cifrado={raft_password is not None}"
    )
    try:
        server.wait_for_termination()
    finally:
        server.stop(grace=1).wait()
        raft.destroy()


if __name__ == "__main__":
    main()
