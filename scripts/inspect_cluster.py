"""Inspector del clúster: muestra líder, árbol, bloques, réplicas y particionamiento.

Corre dentro de la red de Docker:

    docker compose run --rm inspect estado
    docker compose run --rm inspect lider
    docker compose run --rm inspect arbol
    docker compose run --rm inspect bloques /docs/tesis.pdf
    docker compose run --rm inspect mapa
    docker compose run --rm inspect huerfanos

Solo usa los RPC públicos de ControlNodes y DataNodes, salvo `huerfanos`, que lee
las carpetas de bloques (el servicio monta los volúmenes de los DataNodes).
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

DEFAULT_CN = "cn0:50051,cn1:50051,cn2:50051"
DEFAULT_DN = "dn1:50061,dn2:50061,dn3:50061"
DEFAULT_DN_ROOTS = "/dn1,/dn2,/dn3"


def _load_repo(repo: Path):
    if not (repo / "dfsha" / "generated" / "control_node_pb2.py").exists():
        sys.exit(
            f"No encuentro dfsha/generated en {repo}.\n"
            "Genera los stubs antes: python scripts/generate_proto.py"
        )
    sys.path.insert(0, str(repo))


def _split(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


# ── roles ────────────────────────────────────────────────────────────────────
def roles(cn_addresses: list[str]) -> list[str]:
    import grpc
    from dfsha.generated import control_node_pb2, control_node_pb2_grpc

    result = []
    for address in cn_addresses:
        stub = control_node_pb2_grpc.ControlNodeServiceStub(grpc.insecure_channel(address))
        try:
            stub.ListDir(control_node_pb2.ListDirRequest(path="/"), timeout=3)
            result.append("LIDER")
        except grpc.RpcError as exc:
            details = exc.details() or ""
            if exc.code() == grpc.StatusCode.UNAVAILABLE and "no es el líder" in details:
                result.append("seguidor")
            elif exc.code() == grpc.StatusCode.UNAVAILABLE and "confirmar el liderazgo" in details:
                result.append("líder SIN mayoría")
            else:
                result.append("CAÍDO")
    return result


def leader_stub(cn_addresses: list[str]):
    import grpc
    from dfsha.generated import control_node_pb2_grpc

    for address, role in zip(cn_addresses, roles(cn_addresses)):
        if role == "LIDER":
            return address, control_node_pb2_grpc.ControlNodeServiceStub(grpc.insecure_channel(address))
    sys.exit("No hay líder: ¿están caídos 2 de los 3 ControlNodes? Raft necesita mayoría.")


# ── recorrer el árbol ────────────────────────────────────────────────────────
def walk(stub, path: str = "/"):
    """Devuelve [(ruta, es_dir, tamaño)] recursivo."""
    from dfsha.generated import control_node_pb2

    out = []
    for entry in stub.ListDir(control_node_pb2.ListDirRequest(path=path), timeout=5).entries:
        child = f"{path.rstrip('/')}/{entry.name}"
        out.append((child, entry.is_dir, entry.size_bytes))
        if entry.is_dir:
            out.extend(walk(stub, child))
    return out


def blocks_of(stub, path: str):
    from dfsha.generated import control_node_pb2

    return list(stub.ListBlocks(control_node_pb2.ListBlocksRequest(path=path), timeout=5).blocks)


def replica_status(address: str, block_id: str, expected_checksum: str) -> str:
    """Pide el bloque al DataNode por gRPC y recalcula el SHA-256 del lado del inspector."""
    import grpc
    from dfsha.generated import data_node_pb2, data_node_pb2_grpc

    stub = data_node_pb2_grpc.DataNodeServiceStub(grpc.insecure_channel(address))
    hasher = hashlib.sha256()
    try:
        for chunk in stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=block_id), timeout=30):
            hasher.update(chunk.data)
    except grpc.RpcError as exc:
        return {
            grpc.StatusCode.UNAVAILABLE: "caído",
            grpc.StatusCode.NOT_FOUND: "NO TIENE el bloque",
            grpc.StatusCode.DATA_LOSS: "CORRUPTO",
        }.get(exc.code(), exc.code().name)
    return "ok" if hasher.hexdigest() == expected_checksum else "sha distinto"


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def short(address: str) -> str:
    return address.rsplit(":", 1)[-1] if address.startswith("localhost") else address.split(":")[0]


# ── comandos ────────────────────────────────────────────────────────────────
def cmd_estado(args):
    cns, dns = _split(args.control_nodes), _split(args.datanodes)
    print("\nCONTROLNODES (clúster Raft)")
    for i, (address, role) in enumerate(zip(cns, roles(cns))):
        mark = "★" if role == "LIDER" else " "
        print(f"  {mark} cn{i}  {address:<22} {role}")
    import grpc
    from dfsha.generated import data_node_pb2, data_node_pb2_grpc

    print("\nDATANODES")
    for i, address in enumerate(dns):
        stub = data_node_pb2_grpc.DataNodeServiceStub(grpc.insecure_channel(address))
        try:
            # un block_id válido que no existe: si responde NOT_FOUND, el nodo está vivo
            list(stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id="0" * 32), timeout=3))
            state = "vivo"
        except grpc.RpcError as exc:
            state = "vivo" if exc.code() == grpc.StatusCode.NOT_FOUND else "CAÍDO"
        print(f"    dn{i + 1}  {address:<22} {state}")
    print()


def cmd_lider(args):
    """Imprime el nombre del servicio líder (cn0, cn1, cn2), listo para `docker compose kill`."""
    for address, role in zip(_split(args.control_nodes), roles(_split(args.control_nodes))):
        if role == "LIDER":
            print(short(address))
            return
    sys.exit("no hay líder")


def cmd_arbol(args):
    address, stub = leader_stub(_split(args.control_nodes))
    print(f"\nÁrbol según el líder ({address})\n/")
    for path, is_dir, size in walk(stub):
        depth = path.count("/") - 1
        name = path.rsplit("/", 1)[-1]
        print(f"{'   ' * depth}{'├─ ' if depth >= 0 else ''}{name}{'/' if is_dir else f'   ({human(size)})'}")
    print()


def cmd_bloques(args):
    _, stub = leader_stub(_split(args.control_nodes))
    blocks = blocks_of(stub, args.ruta)
    total = sum(b.size_bytes for b in blocks)
    print(f"\n{args.ruta}  →  {human(total)} en {len(blocks)} bloque(s)\n")
    print(f"  {'#':>3}  {'block_id':<10} {'tamaño':>9}   réplicas (orden de pipeline) → estado verificado por SHA-256")
    print("  " + "─" * 96)
    for i, b in enumerate(blocks):
        cells = []
        for pos, address in enumerate(b.datanode_addresses):
            status = replica_status(address, b.block_id, b.checksum) if not args.sin_verificar else "?"
            role = "cabeza" if pos == 0 else f"réplica {pos + 1}"
            cells.append(f"{short(address)}[{role}]={status}")
        print(f"  b{i:<2}  {b.block_id[:8]:<10} {human(b.size_bytes):>9}   " + "  ".join(cells))
    print(f"\n  checksum registrado del b0: {blocks[0].checksum}" if blocks else "")
    print()


def cmd_mapa(args):
    cns, dns = _split(args.control_nodes), _split(args.datanodes)
    _, stub = leader_stub(cns)
    files = [p for p, is_dir, _ in walk(stub) if not is_dir]
    if not files:
        print("\n(no hay archivos)\n")
        return
    labels = [short(a) for a in dns]
    header = " ".join(f"{label:^11}" for label in labels)
    print(f"\nMAPA DE PARTICIONAMIENTO  (C = cabeza del pipeline, r = réplica, · = no está)\n")
    print(f"  {'archivo / bloque':<29} {header}")
    print("  " + "─" * (30 + len(header)))
    per_node = {a: 0 for a in dns}
    for path in files:
        print(f"  {path}")
        for i, b in enumerate(blocks_of(stub, path)):
            row = []
            for address in dns:
                if address not in b.datanode_addresses:
                    row.append(f"{'·':^11}")
                    continue
                per_node[address] += 1
                row.append(f"{('C' if b.datanode_addresses[0] == address else 'r'):^11}")
            print(f"    b{i:<3}{b.block_id[:8]}  {human(b.size_bytes):>9}     " + " ".join(row))
    print("  " + "─" * (30 + len(header)))
    print(f"  {'bloques por DataNode':<29} " + " ".join(f"{per_node[a]:^11}" for a in dns))
    print()


def cmd_huerfanos(args):
    cns = _split(args.control_nodes)
    roots = [Path(r) for r in _split(args.datanode_roots)]
    _, stub = leader_stub(cns)
    referenced = set()
    for path, is_dir, _ in walk(stub):
        if not is_dir:
            referenced.update(b.block_id for b in blocks_of(stub, path))
    print(f"\nBloques referenciados por archivos visibles: {len(referenced)}\n")
    total_orphans = 0
    for root in roots:
        on_disk = sorted(p.name for p in root.iterdir() if len(p.name) == 32) if root.exists() else []
        orphans = [b for b in on_disk if b not in referenced]
        total_orphans += len(orphans)
        size = sum((root / b).stat().st_size for b in orphans)
        print(f"  {str(root):<45} {len(on_disk):>4} en disco   {len(orphans):>4} huérfanos ({human(size)})")
        for b in orphans[: args.limite]:
            print(f"      huérfano {b}")
    print(f"\n  Total huérfanos: {total_orphans}"
          + ("  ← espacio que nadie va a liberar" if total_orphans else "")
          + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspector del clúster DFSha")
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parent.parent), help="raíz del repo")
    parser.add_argument("--control-nodes", default=DEFAULT_CN)
    parser.add_argument("--datanodes", default=DEFAULT_DN)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("estado", help="rol de cada ControlNode y si cada DataNode está vivo")
    sub.add_parser("lider", help="imprime el nombre del líder (cn0, cn1 o cn2)")
    sub.add_parser("arbol", help="árbol completo de directorios, según el líder")
    p = sub.add_parser("bloques", help="bloques de un archivo y estado real de cada réplica")
    p.add_argument("ruta")
    p.add_argument("--sin-verificar", action="store_true", help="no descargar los bloques para verificar el SHA")
    sub.add_parser("mapa", help="matriz archivos × DataNodes: cómo quedó particionado todo")
    p = sub.add_parser("huerfanos", help="bloques en disco que ningún archivo visible usa")
    p.add_argument("--datanode-roots", default=DEFAULT_DN_ROOTS, help="carpetas de bloques de los DataNodes, separadas por comas")
    p.add_argument("--limite", type=int, default=5)
    args = parser.parse_args()

    _load_repo(Path(args.repo))
    import grpc

    try:
        {
            "estado": cmd_estado, "lider": cmd_lider, "arbol": cmd_arbol,
            "bloques": cmd_bloques, "mapa": cmd_mapa, "huerfanos": cmd_huerfanos,
        }[args.cmd](args)
    except grpc.RpcError as exc:
        sys.exit(f"\n  ✗ {exc.code().name}: {exc.details()}\n")


if __name__ == "__main__":
    main()
