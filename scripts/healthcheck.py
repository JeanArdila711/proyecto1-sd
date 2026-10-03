"""Healthcheck de Docker: el nodo acepta conexiones gRPC (con TLS si se pasa la CA).

    python scripts/healthcheck.py localhost:50061 --tls-ca-file /secrets/ca.crt
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import grpc  # noqa: E402

from dfsha.common.tls import channel_factory, load_tls  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument("--tls-ca-file")
    args = parser.parse_args()
    tls = load_tls(Path(args.tls_ca_file)) if args.tls_ca_file else None
    channel = channel_factory(tls)(args.address)
    try:
        grpc.channel_ready_future(channel).result(timeout=3)
    except grpc.FutureTimeoutError:
        sys.exit(f"{args.address} no responde")
    finally:
        channel.close()


if __name__ == "__main__":
    main()
