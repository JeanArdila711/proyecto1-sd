from __future__ import annotations

import argparse

from dfsha.client.distributed_client import DEFAULT_PARALLEL_TRANSFERS, DistributedDFShaClient
from dfsha.client.shell import run_repl
from dfsha.common.tls import TlsConfig, add_tls_arguments, tls_from_args


def build_client(
    control_node_addresses: list[str],
    tls: TlsConfig | None = None,
    parallel_transfers: int = DEFAULT_PARALLEL_TRANSFERS,
) -> DistributedDFShaClient:
    return DistributedDFShaClient(control_node_addresses, tls=tls, parallel_transfers=parallel_transfers)


def main() -> None:
    parser = argparse.ArgumentParser(description="Shell interactiva DFSha (cliente distribuido)")
    parser.add_argument(
        "--control-nodes",
        default="localhost:50051",
        help="host:port de los ControlNodes del clúster, separados por comas; "
        "el cliente sigue al líder automáticamente",
    )
    parser.add_argument(
        "--parallel-transfers",
        type=int,
        default=DEFAULT_PARALLEL_TRANSFERS,
        help="bloques que send y receive transfieren a la vez",
    )
    add_tls_arguments(parser, server=False)
    args = parser.parse_args()

    addresses = [a.strip() for a in args.control_nodes.split(",") if a.strip()]
    if not addresses:
        raise SystemExit("--control-nodes no puede quedar vacío")
    if args.parallel_transfers < 1:
        raise SystemExit("--parallel-transfers debe ser >= 1")
    client = build_client(addresses, tls_from_args(args, server=False), args.parallel_transfers)
    try:
        run_repl(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
