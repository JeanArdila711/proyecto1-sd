"""El inspector y el ayudante de tests contra un clúster con autenticación (Hito 3, C2)."""

import importlib.util
import uuid
from pathlib import Path

import grpc
import pytest

from conftest import FAST_RAFT_CONF, free_port, wait_for, wait_until_datanode_excluded
from dfsha.control_node.main import serve as serve_control_node
from dfsha.generated import control_node_pb2 as pb

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "inspect_cluster.py"
SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"


@pytest.fixture
def inspector(monkeypatch):
    """El módulo del inspector recién cargado: su sesión es estado global del módulo."""
    spec = importlib.util.spec_from_file_location("inspect_cluster_under_test", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    channels = []

    def new_channel(address):
        channels.append(grpc.insecure_channel(address))
        return channels[-1]

    module._new_channel = new_channel
    monkeypatch.setattr(module, "_can_prompt", lambda: False)  # sin terminal, salvo que el test diga otra cosa
    yield module
    for channel in channels:
        channel.close()


def _with_terminal(monkeypatch, inspector, *answers):
    """Simula una terminal: getpass devuelve `answers` en orden. Devuelve lo preguntado."""
    asked, pending = [], list(answers)

    def fake_getpass(prompt):
        asked.append(prompt)
        return pending.pop(0)

    monkeypatch.setattr(inspector, "_can_prompt", lambda: True)
    monkeypatch.setattr(inspector.getpass, "getpass", fake_getpass)
    return asked


@pytest.fixture
def start_node():
    started = []

    def _start(authenticated=True):
        kwargs = {"jwt_secret": SECRET, "admin_password": ADMIN_PASSWORD} if authenticated else {}
        server, port, raft = serve_control_node(
            ["localhost:1"],
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            raft_conf_overrides=FAST_RAFT_CONF,
            replication_factor=1,
            min_write_replicas=1,
            **kwargs,
        )
        started.append((server, raft))
        assert wait_for(raft._isLeader)
        if authenticated:
            replicated = server._dfsha_control_servicer._replicated
            assert wait_for(lambda: replicated.tree.get_user("admin") is not None)
        return f"localhost:{port}"

    yield _start

    for server, raft in started:
        server.stop(grace=None)
        raft.destroy()


def test_with_good_credentials_the_inspector_finds_the_leader_and_walks_the_tree(inspector, start_node):
    address = start_node()
    inspector._credentials = ("admin", ADMIN_PASSWORD)

    leader_address, stub = inspector.leader_stub([address])
    stub.MakeDir(pb.MakeDirRequest(path="/docs", op_id=uuid.uuid4().hex), metadata=inspector._auth_metadata)

    assert leader_address == address
    assert inspector.roles([address]) == ["LIDER"]
    assert inspector.walk(stub) == [("/docs", True, 0)]


def test_rejected_credentials_without_a_terminal_stop_the_inspector(inspector, start_node):
    address = start_node()
    inspector._credentials = ("admin", "ya-no-es-esta")

    with pytest.raises(SystemExit) as exc_info:
        inspector.roles([address])

    assert "credenciales del inspector rechazadas" in str(exc_info.value)
    assert "ya-no-es-esta" not in str(exc_info.value)


def test_a_stale_password_file_falls_back_to_asking(inspector, start_node, monkeypatch):
    # La guía recomienda cambiar la contraseña del admin: la del archivo queda vieja.
    address = start_node()
    inspector._credentials = ("admin", "la-del-archivo-ya-vieja")
    asked = _with_terminal(monkeypatch, inspector, ADMIN_PASSWORD)

    assert inspector.roles([address]) == ["LIDER"]
    assert len(asked) == 1 and "rechazada" in asked[0]


def test_asking_and_getting_a_wrong_password_stops_the_inspector(inspector, start_node, monkeypatch):
    address = start_node()
    asked = _with_terminal(monkeypatch, inspector, "tampoco-es-esta")

    with pytest.raises(SystemExit) as exc_info:
        inspector.roles([address])

    assert "credenciales del inspector rechazadas" in str(exc_info.value)
    assert len(asked) == 1  # pregunta una vez, no en un ciclo


def test_without_credentials_or_terminal_the_role_says_what_is_missing(inspector, start_node):
    address = start_node()

    assert inspector.roles([address]) == [inspector.NEEDS_CREDENTIALS]
    with pytest.raises(SystemExit) as exc_info:
        inspector.leader_stub([address])
    assert "pide credenciales" in str(exc_info.value)


def test_lider_says_credentials_are_missing_instead_of_no_leader(inspector, start_node):
    from types import SimpleNamespace

    address = start_node()

    with pytest.raises(SystemExit) as exc_info:
        inspector.cmd_lider(SimpleNamespace(control_nodes=address))

    assert "pide credenciales" in str(exc_info.value)


def test_without_a_password_file_the_inspector_asks(inspector, start_node, monkeypatch):
    address = start_node()
    asked = _with_terminal(monkeypatch, inspector, ADMIN_PASSWORD)

    assert inspector.roles([address]) == ["LIDER"]
    assert len(asked) == 1 and "admin" in asked[0]


@pytest.mark.parametrize("credentials", [None, ("admin", ADMIN_PASSWORD)])
def test_a_cluster_without_authentication_works_as_before(inspector, start_node, credentials):
    address = start_node(authenticated=False)
    inspector._credentials = credentials

    _, stub = inspector.leader_stub([address])

    assert inspector.roles([address]) == ["LIDER"]
    assert inspector.walk(stub) == []


def test_a_dead_node_is_still_reported_as_down(inspector):
    inspector._credentials = ("admin", ADMIN_PASSWORD)

    assert inspector.roles([f"localhost:{free_port()}"]) == ["CAÍDO"]


# --- conftest.wait_until_datanode_excluded -------------------------------------------------


def test_the_datanode_probe_works_with_a_token_and_fails_without_it(inspector, start_node):
    address = start_node()
    inspector._credentials = ("admin", ADMIN_PASSWORD)
    inspector.roles([address])  # deja el token del admin en _auth_metadata
    channel = grpc.insecure_channel(address)
    try:
        # "localhost:9" no es un DataNode del clúster: con token, la sonda lo ve excluido
        assert wait_until_datanode_excluded(channel, "localhost:9", metadata=inspector._auth_metadata)
        assert not wait_until_datanode_excluded(channel, "localhost:9", timeout=0.3)
    finally:
        channel.close()
