from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.common.tls import TlsConfig, add_port, add_tls_arguments, channel_factory, tls_from_args
from dfsha.data_node.block_store import load_encryption_key
from dfsha.data_node.servicer import DataNodeServicer
from dfsha.generated import data_node_pb2_grpc


def serve(
    root: Path, host: str, port: int, encryption_key: bytes, tls: TlsConfig | None = None
) -> tuple[grpc.Server, int]:
    root.mkdir(parents=True, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = DataNodeServicer(root, encryption_key, channel_factory(tls))
    data_node_pb2_grpc.add_DataNodeServiceServicer_to_server(servicer, server)
    original_stop = server.stop

    def stop_with_cleanup(grace):
        termination = original_stop(grace)
        termination.wait()
        servicer.close()
        return termination

    server.stop = stop_with_cleanup
    bound_port = add_port(server, f"{host}:{port}", tls)
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DataNode DFSha (Hito 3, replicación por pipeline y cifrado en reposo)"
    )
    parser.add_argument("--root", default="./dfsha-datanode-data")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50061)
    parser.add_argument(
        "--encryption-key-file",
        required=True,
        help="archivo con una llave AES-256-GCM cruda de exactamente 32 bytes",
    )
    add_tls_arguments(parser, server=True)
    args = parser.parse_args()
    tls = tls_from_args(args, server=True)

    encryption_key = load_encryption_key(Path(args.encryption_key_file))
    server, bound_port = serve(Path(args.root), args.host, args.port, encryption_key, tls)
    if bound_port == 0:
        raise RuntimeError(f"no se pudo abrir el puerto {args.port} en {args.host} (¿ya está en uso?)")
    print(f"DataNode escuchando en {args.host}:{bound_port}, raíz={args.root}, TLS={tls is not None}")
    try:
        server.wait_for_termination()
    finally:
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
