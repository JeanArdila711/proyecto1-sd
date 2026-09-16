from __future__ import annotations

import argparse
from concurrent import futures

import grpc

from dfsha.control_node.servicer import DEFAULT_REPLICATION_FACTOR, ControlNodeServicer
from dfsha.generated import control_node_pb2_grpc

DEFAULT_BLOCK_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB


def serve(
    datanode_addresses: list[str],
    host: str,
    port: int,
    block_size_bytes: int = DEFAULT_BLOCK_SIZE_BYTES,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ControlNodeServicer(datanode_addresses, block_size_bytes, replication_factor)
    control_node_pb2_grpc.add_ControlNodeServiceServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(description="ControlNode DFSha (Hito 2, con replicación)")
    parser.add_argument(
        "--datanode-addresses",
        required=True,
        help="host:port de los DataNodes, separados por comas",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--block-size-mb", type=int, default=128)
    parser.add_argument("--replication-factor", type=int, default=DEFAULT_REPLICATION_FACTOR)
    args = parser.parse_args()

    addresses = [a.strip() for a in args.datanode_addresses.split(",") if a.strip()]
    if not addresses:
        raise SystemExit("--datanode-addresses no puede quedar vacío")

    server, bound_port = serve(
        addresses,
        args.host,
        args.port,
        args.block_size_mb * 1024 * 1024,
        args.replication_factor,
    )
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    effective = min(args.replication_factor, len(addresses))
    print(
        f"ControlNode escuchando en {args.host}:{bound_port}, "
        f"DataNodes={addresses}, factor de replicación={effective}"
    )
    server.wait_for_termination()


if __name__ == "__main__":
    main()
