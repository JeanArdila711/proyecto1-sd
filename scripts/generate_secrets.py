"""Crea los secretos del clúster en secrets/ (nunca van al repo).

    dn1.key dn2.key dn3.key   llave AES-256-GCM de cada DataNode (cifrado en reposo)
    raft.password             password del canal Raft entre ControlNodes
    ca.crt ca.key             CA privada de DFSha; un cliente solo necesita ca.crt
    node.crt node.key         certificado TLS que presentan los ControlNodes y DataNodes

Solo crea lo que falta: correrlo otra vez no cambia nada. Docker Compose lo corre solo
(servicio `init`) antes de levantar los nodos, como root, y deja los archivos con el
dueño del usuario de la imagen (--owner 1000:1000). En Linux eso importa: un archivo
0600 de otro dueño no se puede leer desde el contenedor.
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dfsha.common.tls import generate_ca, issue_node_cert  # noqa: E402

_DATANODE_KEYS = ("dn1.key", "dn2.key", "dn3.key")


def _write(path: Path, content: bytes, mode: int, force: bool) -> bool:
    """Escribe el archivo si no existe (o con --force). Devuelve si lo escribió."""
    if path.exists() and not force:
        return False
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, mode)
    try:
        if hasattr(os, "fchmod"):  # no existe en Windows con Python < 3.13
            os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    print(f"  creado   {path}")
    return True


def _chown(path: Path, owner: tuple[int, int] | None) -> None:
    if owner is None or not hasattr(os, "chown"):
        return
    try:
        os.chown(path, *owner)
    except OSError as exc:
        # p. ej. una carpeta de Windows montada en Docker Desktop: ahí los permisos no
        # se aplican y no hace falta
        print(f"  aviso: no se pudo cambiar el dueño de {path}: {exc}")


def generate(secrets_dir: Path, force: bool = False, owner: tuple[int, int] | None = None) -> None:
    secrets_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        secrets_dir.chmod(0o700)  # si Docker la creó antes, quedó 0755
    except OSError:
        pass

    for name in _DATANODE_KEYS:
        _write(secrets_dir / name, os.urandom(32), 0o600, force)

    _write(secrets_dir / "raft.password", secrets.token_hex(32).encode(), 0o600, force)

    ca_crt, ca_key = secrets_dir / "ca.crt", secrets_dir / "ca.key"
    new_ca = force or not ca_crt.exists() or not ca_key.exists()
    if new_ca:
        cert, key = generate_ca()
        _write(ca_key, key, 0o600, True)
        _write(ca_crt, cert, 0o644, True)

    node_crt, node_key = secrets_dir / "node.crt", secrets_dir / "node.key"
    if new_ca or not node_crt.exists() or not node_key.exists():
        # una CA nueva invalida el certificado anterior: se emite otro
        cert, key = issue_node_cert(ca_crt.read_bytes(), ca_key.read_bytes())
        _write(node_key, key, 0o600, True)
        _write(node_crt, cert, 0o644, True)

    for path in [secrets_dir, *sorted(secrets_dir.iterdir())]:
        _chown(path, owner)


def _prepare_writable_dir(path: Path, owner: tuple[int, int] | None) -> None:
    """Una carpeta montada que el contenedor escribe (intercambio/). Si no existía, Docker
    la crea con dueño root en Linux y la shell no podría guardar lo que baja."""
    path.mkdir(parents=True, exist_ok=True)
    if owner is not None and path.stat().st_uid == 0:
        _chown(path, owner)


def _parse_owner(value: str) -> tuple[int, int]:
    uid, _, gid = value.partition(":")
    return int(uid), int(gid or uid)


def main() -> None:
    parser = argparse.ArgumentParser(description="crea los secretos que falten en secrets/")
    parser.add_argument("--dir", default="secrets", help="carpeta de secretos (por defecto ./secrets)")
    parser.add_argument("--force", action="store_true", help="reemplaza TODOS los secretos (invalida los datos cifrados)")
    parser.add_argument("--owner", type=_parse_owner, help="uid:gid dueño de los archivos (requiere root)")
    parser.add_argument(
        "--writable-dir", action="append", default=[], type=Path,
        help="carpeta montada que el contenedor tiene que poder escribir",
    )
    args = parser.parse_args()

    print(f"Secretos en {args.dir}:")
    generate(Path(args.dir), args.force, args.owner)
    for path in args.writable_dir:
        _prepare_writable_dir(path, args.owner)
    print("  listo")


if __name__ == "__main__":
    main()
