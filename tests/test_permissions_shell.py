"""Comandos de permisos de la shell (Hito 3, C3), con un cliente falso."""

from types import SimpleNamespace

import pytest

from dfsha.client.shell import handle_command
from dfsha.common.exceptions import AccessDeniedError

_CHMOD_USAGE = "chmod: uso: chmod <modo octal> <ruta>   (por ejemplo: chmod 640 notas.txt)"
_CHOWN_USAGE = "chown: uso: chown <usuario>[:<grupo>] <ruta>   (o chown :<grupo> <ruta>)"


def _entry(name, is_dir, size, owner, group, mode):
    return SimpleNamespace(name=name, is_dir=is_dir, size_bytes=size, owner=owner, group=group, mode=mode)


class FakePermissionsClient:
    """La parte de permisos de DistributedDFShaClient, en memoria."""

    def __init__(self, denied=None):
        self.calls = []
        self.denied = denied
        self.username = "alice"

    def list_dir(self, path):
        self.calls.append(("list_dir", path))
        return [
            _entry("docs", True, 0, "alice", "alice", 0o755),
            _entry("notas.txt", False, 1234, "bob", "docentes", 0o640),
        ]

    def chmod(self, path, mode):
        self.calls.append(("chmod", path, mode))
        if self.denied:
            raise AccessDeniedError(self.denied)

    def chown(self, path, owner="", group=""):
        self.calls.append(("chown", path, owner, group))


class MonolithicClient:
    """Como el cliente del Hito 1: sus entradas no traen modo y no tiene chmod ni chown."""

    def __init__(self):
        self.calls = []

    def list_dir(self, path):
        self.calls.append(("list_dir", path))
        return [SimpleNamespace(name="a.txt", is_dir=False, size_bytes=3)]


def _run(client, line, current_dir="/"):
    asked = []

    def prompt(text):
        asked.append(text)
        raise EOFError

    _, output = handle_command(client, current_dir, line, prompt)
    return output, asked


def test_ls_l_prints_mode_owner_group_size_and_name():
    output, _ = _run(FakePermissionsClient(), "ls -l")

    assert output.split("\n") == [
        "drwxr-xr-x alice    alice             0  docs",
        "-rw-r----- bob      docentes       1234  notas.txt",
    ]


def test_ls_without_l_prints_what_it_always_did():
    output, _ = _run(FakePermissionsClient(), "ls")

    assert output.split("\n") == ["d          0  docs", "-       1234  notas.txt"]


@pytest.mark.parametrize("line", ["ls -l /docs", "ls /docs -l", "ls -l docs"])
def test_ls_l_takes_the_path_before_or_after_the_flag(line):
    client = FakePermissionsClient()

    _run(client, line)

    assert client.calls == [("list_dir", "/docs")]


def test_ls_l_with_a_client_whose_entries_have_no_mode_says_so():
    client = MonolithicClient()

    output, _ = _run(client, "ls -l")

    assert output == "ls: -l no disponible con este cliente (los permisos requieren el cliente distribuido)"
    assert _run(client, "ls")[0] == "-          3  a.txt"


def test_chmod_parses_the_octal_mode_and_resolves_the_path():
    client = FakePermissionsClient()

    output, _ = _run(client, "chmod 640 a.txt", current_dir="/docs")

    assert output == ""
    assert client.calls == [("chmod", "/docs/a.txt", 0o640)]


@pytest.mark.parametrize("line", ["chmod 999 a.txt", "chmod rw a.txt", "chmod 640", "chmod 0640 a.txt", "chmod"])
def test_chmod_with_a_bad_mode_or_missing_path_shows_the_usage(line):
    client = FakePermissionsClient()

    output, _ = _run(client, line)

    assert output == _CHMOD_USAGE
    assert client.calls == []


@pytest.mark.parametrize(
    ("line", "owner", "group"),
    [("chown bob a.txt", "bob", ""), ("chown bob:docentes a.txt", "bob", "docentes"), ("chown :docentes a.txt", "", "docentes")],
)
def test_chown_passes_owner_and_group(line, owner, group):
    client = FakePermissionsClient()

    output, _ = _run(client, line)

    assert output == ""
    assert client.calls == [("chown", "/a.txt", owner, group)]


@pytest.mark.parametrize("line", ["chown a.txt", "chown : a.txt", "chown", "chown bob a.txt b.txt"])
def test_chown_without_owner_or_group_shows_the_usage(line):
    client = FakePermissionsClient()

    output, _ = _run(client, line)

    assert output == _CHOWN_USAGE
    assert client.calls == []


@pytest.mark.parametrize("line", ["chmod 640 a.txt", "chown bob a.txt"])
def test_chmod_and_chown_are_not_available_with_the_monolithic_client(line):
    output, _ = _run(MonolithicClient(), line)

    cmd = line.split()[0]
    assert output == f"{cmd}: no disponible con este cliente (los permisos requieren el cliente distribuido)"


def test_a_denied_command_prints_the_reason_and_does_not_ask_for_the_password():
    client = FakePermissionsClient(denied="permiso denegado: solo el dueño o un admin cambia el modo de /a.txt")

    output, asked = _run(client, "chmod 600 a.txt")

    assert output == "chmod: permiso denegado: solo el dueño o un admin cambia el modo de /a.txt"
    assert asked == []


def test_help_lists_the_permission_commands():
    output, _ = _run(FakePermissionsClient(), "help")

    for command in ("ls -l [ruta]", "chmod <modo> <ruta>", "chown <usuario>[:<grupo>] <ruta>"):
        assert command in output
    assert "  ls [ruta]                  lista un directorio (por defecto, el actual)" in output
