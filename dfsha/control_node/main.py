from __future__ import annotations

import argparse
from concurrent import futures

import grpc

from dfsha.control_node.servicer import ControlNodeServicer
from dfsha.generated import control_node_pb2_grpc

DEFAULT_BLOCK_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB


def serve(
    datanode_address: str, host: str, port: int, block_size_bytes: int = DEFAULT_BLOCK_SIZE_BYTES
) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ControlNodeServicer(datanode_address, block_size_bytes)
    control_node_pb2_grpc.add_ControlNodeServiceServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(description="ControlNode DFSha (Hito 2, single-node)")
    parser.add_argument("--datanode-address", required=True, help="host:port del DataNode")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--block-size-mb", type=int, default=128)
    args = parser.parse_args()

    server, bound_port = serve(
        args.datanode_address, args.host, args.port, args.block_size_mb * 1024 * 1024
    )
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(f"ControlNode escuchando en {args.host}:{bound_port}, DataNode={args.datanode_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
