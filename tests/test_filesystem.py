from pathlib import Path

import pytest

from dfsha.server import filesystem
from dfsha.server.exceptions import InvalidPathError


def test_resolve_path_dentro_de_la_raiz(tmp_path):
    resolved = filesystem.resolve_path(tmp_path, "/docs/reporte.txt")
    assert resolved == tmp_path / "docs" / "reporte.txt"


def test_resolve_path_raiz_vacia(tmp_path):
    assert filesystem.resolve_path(tmp_path, "/") == tmp_path
    assert filesystem.resolve_path(tmp_path, "") == tmp_path


def test_resolve_path_bloquea_traversal(tmp_path):
    with pytest.raises(InvalidPathError):
        filesystem.resolve_path(tmp_path, "/../../etc/passwd")


def test_resolve_path_bloquea_traversal_interno(tmp_path):
    with pytest.raises(InvalidPathError):
        filesystem.resolve_path(tmp_path, "/docs/../../etc/passwd")
