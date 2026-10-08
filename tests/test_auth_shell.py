"""Comandos de sesión de la shell (Hito 3, C2), con un cliente falso."""

import pytest

from dfsha.client.shell import handle_command
from dfsha.common.exceptions import AuthError


class FakeSessionClient:
    """La parte de sesión de DistributedDFShaClient, en memoria."""

    def __init__(self, passwords=None):
        self.passwords = dict(passwords or {"alice": "clave-de-alice"})
        self.username = None
        self.groups = ()
        self.is_admin = False
        self.expired = False
        self.calls = []

    def login(self, username, password):
        self.calls.append(("login", username, password))
        if self.passwords.get(username) != password:
            raise AuthError("usuario o contraseña incorrectos")
        self.username, self.groups, self.expired = username, (username,), False

    def create_user(self, username, password, groups=None, is_admin=False):
        self.calls.append(("create_user", username, password, groups, is_admin))

    def change_password(self, new_password, current_password="", username=""):
        self.calls.append(("change_password", new_password, current_password, username))

    def list_dir(self, path):
        if self.username is None:
            raise AuthError("falta el token de sesión")
        if self.expired:
            raise AuthError("token vencido")
        return []


class Prompts:
    """Respuestas para prompt_password, en orden; guarda lo que se preguntó."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.asked = []

    def __call__(self, text):
        self.asked.append(text)
        if not self.answers:
            raise EOFError
        return self.answers.pop(0)


def _run(client, line, *answers):
    prompts = Prompts(*answers)
    _, output = handle_command(client, "/", line, prompts)
    return output, prompts


def test_login_asks_for_the_password_and_never_echoes_it():
    client = FakeSessionClient()

    output, prompts = _run(client, "login alice", "clave-de-alice")

    assert client.calls == [("login", "alice", "clave-de-alice")]
    assert len(prompts.asked) == 1
    assert output == "sesión iniciada como alice"
    assert "clave-de-alice" not in output


def test_login_rejects_a_password_on_the_command_line():
    client = FakeSessionClient()

    output, prompts = _run(client, "login alice clave-de-alice")

    assert output.startswith("login: uso: login <usuario>")
    assert client.calls == []
    assert prompts.asked == []


def test_a_failed_login_reports_it_without_asking_again():
    client = FakeSessionClient()

    output, prompts = _run(client, "login alice", "incorrecta")

    assert output == "login: usuario o contraseña incorrectos"
    assert len(prompts.asked) == 1
    assert "incorrecta" not in output.replace("incorrectos", "")


def test_whoami_with_and_without_a_session():
    client = FakeSessionClient()

    before, _ = _run(client, "whoami")
    _run(client, "login alice", "clave-de-alice")
    client.groups, client.is_admin = ("alice", "docentes"), True
    after, _ = _run(client, "whoami")

    assert before.startswith("sin sesión")
    assert after == "alice  grupos: alice, docentes  admin: sí"


def test_adduser_asks_for_the_password_twice():
    client = FakeSessionClient()

    output, prompts = _run(client, "adduser bob --admin grupo1 grupo2", "clave-de-bob", "clave-de-bob")

    assert client.calls == [("create_user", "bob", "clave-de-bob", ["grupo1", "grupo2"], True)]
    assert len(prompts.asked) == 2
    assert output == "usuario bob creado"


def test_adduser_does_nothing_when_the_passwords_differ():
    client = FakeSessionClient()

    output, _ = _run(client, "adduser bob", "una", "otra")

    assert client.calls == []
    assert "no coinciden" in output


@pytest.mark.parametrize("line", ["adduser", "adduser --admin", "adduser bob --otra-cosa"])
def test_adduser_with_bad_arguments_shows_its_usage(line):
    client = FakeSessionClient()

    output, prompts = _run(client, line)

    assert output.startswith("adduser: uso:")
    assert client.calls == [] and prompts.asked == []


def test_passwd_for_yourself_asks_for_the_current_one():
    client = FakeSessionClient()
    _run(client, "login alice", "clave-de-alice")
    client.calls.clear()

    output, prompts = _run(client, "passwd", "clave-de-alice", "la-nueva", "la-nueva")

    assert client.calls == [("change_password", "la-nueva", "clave-de-alice", "")]
    assert len(prompts.asked) == 3
    assert output == "contraseña cambiada"


def test_passwd_for_someone_else_asks_only_for_the_new_one():
    client = FakeSessionClient()
    _run(client, "login alice", "clave-de-alice")
    client.calls.clear()

    output, prompts = _run(client, "passwd bob", "la-de-bob", "la-de-bob")

    assert client.calls == [("change_password", "la-de-bob", "", "bob")]
    assert len(prompts.asked) == 2
    assert output == "contraseña cambiada"


def test_passwd_does_nothing_when_the_new_passwords_differ():
    client = FakeSessionClient()

    output, _ = _run(client, "passwd", "actual", "una", "otra")

    assert client.calls == []
    assert "no coinciden" in output


def test_an_expired_session_is_renewed_and_the_user_repeats_the_command():
    client = FakeSessionClient()
    _run(client, "login alice", "clave-de-alice")
    client.expired = True
    client.calls.clear()

    output, prompts = _run(client, "ls", "clave-de-alice")

    assert client.calls == [("login", "alice", "clave-de-alice")]
    assert "repite el comando" in output
    assert "venció" in prompts.asked[0] and "alice" in prompts.asked[0]
    assert _run(client, "ls")[0] == ""  # ahora sí funciona


def test_a_failed_renewal_is_reported_and_does_not_crash_the_shell():
    client = FakeSessionClient()
    _run(client, "login alice", "clave-de-alice")
    client.expired = True

    wrong, _ = _run(client, "ls", "incorrecta")
    cancelled, _ = _run(client, "ls")  # sin respuesta: EOF en el prompt

    assert wrong == "ls: token vencido; no se pudo renovar la sesión: usuario o contraseña incorrectos"
    assert cancelled == "ls: token vencido; no se pudo renovar la sesión: cancelado"
    assert client.username == "alice"


def test_without_a_session_the_shell_suggests_login():
    client = FakeSessionClient()

    output, prompts = _run(client, "ls")

    assert "login <usuario>" in output
    assert prompts.asked == []


def test_cancelling_a_password_prompt_does_not_crash_the_shell():
    client = FakeSessionClient()

    output, _ = _run(client, "login alice")  # EOF en el prompt

    assert output == "login: cancelado"
    assert client.calls == []


def test_session_commands_are_unavailable_with_the_monolithic_client():
    class MonolithicClient:
        def list_dir(self, path):
            return []

    for line in ("login alice", "whoami", "adduser bob", "passwd"):
        _, output = handle_command(MonolithicClient(), "/", line, Prompts())
        assert "no disponible con este cliente" in output, line


def test_help_lists_the_session_commands():
    _, output = handle_command(FakeSessionClient(), "/", "help")

    for command in ("login <usuario>", "whoami", "adduser <usuario>", "passwd [usuario]"):
        assert command in output
