"""Genera el código gRPC/protobuf para todos los .proto del proyecto."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "dfsha" / "generated"

PROTO_FILES = [
    "dfsha.proto",
    "control_node.proto",
    "data_node.proto",
]


def _patch_broken_import(grpc_file: Path, module_stem: str) -> None:
    """grpc_tools genera un import absoluto que se rompe dentro de un paquete."""
    content = grpc_file.read_text()
    alias = f"{module_stem.replace('_', '__')}__pb2"
    broken = f"import {module_stem}_pb2 as {alias}"
    fixed = f"from . import {module_stem}_pb2 as {alias}"
    grpc_file.write_text(content.replace(broken, fixed))


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").touch(exist_ok=True)

    for proto_name in PROTO_FILES:
        proto_path = PROTO_DIR / proto_name
        subprocess.run(
            [
                sys.executable, "-m", "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={OUT_DIR}",
                f"--grpc_python_out={OUT_DIR}",
                str(proto_path),
            ],
            check=True,
        )
        module_stem = proto_name.removesuffix(".proto")
        grpc_file = OUT_DIR / f"{module_stem}_pb2_grpc.py"
        _patch_broken_import(grpc_file, module_stem)
        print(f"Generado: {module_stem}_pb2.py, {module_stem}_pb2_grpc.py")


if __name__ == "__main__":
    main()
