from __future__ import annotations

from pathlib import Path

import pytest

from dfsha.client.shell import handle_command, resolve_relative, split_command
from dfsha.common.exceptions import PathNotFoundError


class FakeClient:
    """Cliente falso en memoria, con la misma interfaz que DFShaClient."""

    def __init__(self):
        self.dirs = {"/"}
        self.files = {}

    def list_dir(self, path):
        if path not in self.dirs:
            raise PathNotFoundError(f"no existe: {path}")
        return []

    def make_dir(self, path):
        self.dirs.add(path)

    def remove_dir(self, path):
        self.dirs.discard(path)

    def remove(self, path):
        self.files.pop(path, None)

    def upload(self, local_path: Path, remote_path: str) -> int:
        data = local_path.read_bytes()
        self.files[remote_path] = data
        return len(data)

    def download(self, remote_path: str, local_path: Path) -> int:
        data = self.files[remote_path]
        local_path.write_bytes(data)
        return len(data)


def test_resolve_relative_absoluta():
    assert resolve_relative("/actual", "/otra/ruta") == "/otra/ruta"


def test_resolve_relative_relativa():
    assert resolve_relative("/a/b", "c") == "/a/b/c"


def test_resolve_relative_subir_nivel():
    assert resolve_relative("/a/b", "..") == "/a"


def test_resolve_relative_raiz():
    assert resolve_relative("/a", "..") == "/"


def test_pwd():
    client = FakeClient()
    new_dir, output = handle_command(client, "/actual", "pwd")
    assert new_dir == "/actual"
    assert output == "/actual"


def test_cd_a_directorio_existente():
    client = FakeClient()
    client.make_dir("/docs")
    new_dir, output = handle_command(client, "/", "cd docs")
    assert new_dir == "/docs"
    assert output == ""


def test_cd_a_directorio_inexistente_no_cambia_el_directorio():
    client = FakeClient()
    new_dir, output = handle_command(client, "/", "cd no-existe")
    assert new_dir == "/"
    assert "no existe" in output


def test_mkdir():
    client = FakeClient()
    new_dir, output = handle_command(client, "/", "mkdir docs")
    assert new_dir == "/"
    assert "/docs" in client.dirs


def test_send_y_receive(tmp_path):
    client = FakeClient()
    local_origen = tmp_path / "origen.txt"
    local_origen.write_text("hola mundo")

    _, output_send = handle_command(client, "/", f"send {local_origen} archivo.txt")
    assert "enviados" in output_send

    local_destino = tmp_path / "destino.txt"
    _, output_receive = handle_command(client, "/", f"receive archivo.txt {local_destino}")
    assert "recibidos" in output_receive
    assert local_destino.read_text() == "hola mundo"


def test_comando_no_reconocido():
    client = FakeClient()
    _, output = handle_command(client, "/", "volar")
    assert "no reconocido" in output


def test_receive_a_ruta_local_invalida_no_revienta_el_shell():
    client = FakeClient()
    client.files["/archivo.txt"] = b"contenido"
    ruta_invalida = "/no/existe/destino.txt"
    _, output = handle_command(client, "/", f"receive archivo.txt {ruta_invalida}")
    assert "receive:" in output


def test_split_command_respeta_comillas_y_barras_de_windows():
    partes = split_command(r'send "C:\Users\yo\8 SEMESTRE\tesis.pdf" ..\datos\otro.pdf')
    assert partes == ["send", r"C:\Users\yo\8 SEMESTRE\tesis.pdf", r"..\datos\otro.pdf"]


def test_send_con_espacios_en_la_ruta_local(tmp_path):
    client = FakeClient()
    carpeta = tmp_path / "8 SEMESTRE"
    carpeta.mkdir()
    origen = carpeta / "mi tesis.txt"
    origen.write_text("hola")
    _, output = handle_command(client, "/", f'send "{origen}" "tesis final.txt"')
    assert "enviados" in output
    assert client.files["/tesis final.txt"] == b"hola"


def test_comilla_sin_cerrar_no_revienta_el_shell():
    _, output = handle_command(FakeClient(), "/", 'send "abierta destino')
    assert "error de sintaxis" in output
