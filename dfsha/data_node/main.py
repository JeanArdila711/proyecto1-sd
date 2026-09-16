from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.data_node.servicer import DataNodeServicer
from dfsha.generated import data_node_pb2_grpc


def serve(root: Path, host: str, port: int) -> tuple[grpc.Server, int]:
    root.mkdir(parents=True, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_node_pb2_grpc.add_DataNodeServiceServicer_to_server(DataNodeServicer(root), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(description="DataNode DFSha (Hito 2, sin replicación)")
    parser.add_argument("--root", default="./dfsha-datanode-data")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50061)
    args = parser.parse_args()

    server, bound_port = serve(Path(args.root), args.host, args.port)
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(f"DataNode escuchando en {args.host}:{bound_port}, raíz={args.root}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
