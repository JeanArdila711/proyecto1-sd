"""Genera las llaves crudas AES-256-GCM de los tres DataNodes de Compose."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

_KEY_FILES = ("dn1.key", "dn2.key", "dn3.key")


def _write_key(path: Path, force: bool) -> None:
    flags = os.O_WRONLY | os.O_CREAT | (os.O_TRUNC if force else os.O_EXCL)
    try:
        fd = os.open(path, flags, 0o600)
    except FileExistsError as exc:
        raise RuntimeError(f"ya existe {path}; use --force para reemplazarla") from exc
    try:
        if hasattr(os, "fchmod"):  # no existe en Windows con Python < 3.13
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(os.urandom(32))
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="genera solo las llaves AES-256-GCM de los DataNodes")
    parser.add_argument("--force", action="store_true", help="reemplaza llaves existentes")
    args = parser.parse_args()

    secrets_dir = Path("secrets")
    secrets_dir.mkdir(mode=0o700, exist_ok=True)
    for name in _KEY_FILES:
        path = secrets_dir / name
        _write_key(path, args.force)
        print(f"generada {path} (32 bytes, modo 0600)")


if __name__ == "__main__":
    main()
