from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2_grpc
from dfsha.server.servicer import DFShaServicer


def serve(root: Path, host: str, port: int) -> tuple[grpc.Server, int]:
    root.mkdir(parents=True, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfsha_pb2_grpc.add_DFShaServiceServicer_to_server(DFShaServicer(root), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor DFSha (Hito 1, monolítico)")
    parser.add_argument("--root", default="./dfsha-data", help="Carpeta raíz del árbol del DFS")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server, bound_port = serve(Path(args.root), args.host, args.port)
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(f"DFSha server escuchando en {args.host}:{bound_port}, raíz={args.root}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
