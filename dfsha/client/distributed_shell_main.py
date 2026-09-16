from __future__ import annotations

import argparse

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.client.shell import run_repl


def build_client(control_node_address: str) -> DistributedDFShaClient:
    return DistributedDFShaClient(control_node_address)


def main() -> None:
    parser = argparse.ArgumentParser(description="Shell interactiva DFSha (Hito 2, distribuido)")
    parser.add_argument("--control-node-host", default="localhost")
    parser.add_argument("--control-node-port", type=int, default=50051)
    args = parser.parse_args()

    client = build_client(f"{args.control_node_host}:{args.control_node_port}")
    try:
        run_repl(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()
