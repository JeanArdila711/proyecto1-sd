from __future__ import annotations

from pathlib import Path

from dfsha.server.exceptions import InvalidPathError


def resolve_path(root: Path, virtual_path: str) -> Path:
    root = root.resolve()
    relative = virtual_path.lstrip("/")
    candidate = (root / relative).resolve()
    if candidate != root and root not in candidate.parents:
        raise InvalidPathError(f"la ruta sale de la raíz: {virtual_path!r}")
    return candidate
