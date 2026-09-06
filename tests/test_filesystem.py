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


from dfsha.server.exceptions import (
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)


def test_list_dir_vacio(tmp_path):
    assert filesystem.list_dir(tmp_path, "/") == []


def test_list_dir_con_contenido(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    (tmp_path / "carpeta").mkdir()
    entries = filesystem.list_dir(tmp_path, "/")
    names = {e.name: e for e in entries}
    assert names["archivo.txt"].is_dir is False
    assert names["archivo.txt"].size_bytes == 4
    assert names["carpeta"].is_dir is True
    assert names["carpeta"].size_bytes == 0


def test_list_dir_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.list_dir(tmp_path, "/no-existe")


def test_list_dir_sobre_archivo(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    with pytest.raises(NotADirectoryError):
        filesystem.list_dir(tmp_path, "/archivo.txt")


def test_make_dir_crea_directorio(tmp_path):
    filesystem.make_dir(tmp_path, "/nueva")
    assert (tmp_path / "nueva").is_dir()


def test_make_dir_anidado(tmp_path):
    filesystem.make_dir(tmp_path, "/a/b/c")
    assert (tmp_path / "a" / "b" / "c").is_dir()


def test_make_dir_ya_existe(tmp_path):
    filesystem.make_dir(tmp_path, "/nueva")
    with pytest.raises(PathExistsError):
        filesystem.make_dir(tmp_path, "/nueva")


def test_remove_dir_vacio(tmp_path):
    filesystem.make_dir(tmp_path, "/vacia")
    filesystem.remove_dir(tmp_path, "/vacia")
    assert not (tmp_path / "vacia").exists()


def test_remove_dir_no_vacio(tmp_path):
    filesystem.make_dir(tmp_path, "/con-cosas")
    (tmp_path / "con-cosas" / "archivo.txt").write_text("hola")
    with pytest.raises(NotEmptyError):
        filesystem.remove_dir(tmp_path, "/con-cosas")


def test_remove_dir_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.remove_dir(tmp_path, "/no-existe")


def test_remove_file_elimina_archivo(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    filesystem.remove_file(tmp_path, "/archivo.txt")
    assert not (tmp_path / "archivo.txt").exists()


def test_remove_file_sobre_directorio(tmp_path):
    filesystem.make_dir(tmp_path, "/carpeta")
    with pytest.raises(NotAFileError):
        filesystem.remove_file(tmp_path, "/carpeta")


def test_remove_file_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.remove_file(tmp_path, "/no-existe")
