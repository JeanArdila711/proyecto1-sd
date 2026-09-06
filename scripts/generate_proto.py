"""Regenera el código gRPC desde proto/dfsha.proto.

Uso: python scripts/generate_proto.py
"""
from __future__ import annotations

import re
from pathlib import Path

from grpc_tools import protoc

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto"
OUT_DIR = REPO_ROOT / "dfsha" / "generated"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").touch(exist_ok=True)

    protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        str(PROTO_DIR / "dfsha.proto"),
    ])

    # grpc_tools genera un import absoluto (`import dfsha_pb2 as dfsha__pb2`)
    # que rompe porque el módulo vive dentro del paquete dfsha.generated.
    # Se reescribe como import relativo.
    grpc_file = OUT_DIR / "dfsha_pb2_grpc.py"
    content = grpc_file.read_text()
    fixed = re.sub(
        r"^import dfsha_pb2 as dfsha__pb2$",
        "from . import dfsha_pb2 as dfsha__pb2",
        content,
        flags=re.MULTILINE,
    )
    grpc_file.write_text(fixed)


if __name__ == "__main__":
    main()
