from __future__ import annotations

import argparse

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.client.shell import run_repl


def build_client(control_node_addresses: list[str]) -> DistributedDFShaClient:
    return DistributedDFShaClient(control_node_addresses)


def main() -> None:
    parser = argparse.ArgumentParser(description="Shell interactiva DFSha (Hito 2, distribuido)")
    parser.add_argument(
        "--control-nodes",
        default="localhost:50051",
        help="host:port de los ControlNodes del clúster, separados por comas; "
        "el cliente sigue al líder automáticamente",
    )
    args = parser.parse_args()

    addresses = [a.strip() for a in args.control_nodes.split(",") if a.strip()]
    if not addresses:
        raise SystemExit("--control-nodes no puede quedar vacío")
    client = build_client(addresses)
    try:
        run_repl(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
