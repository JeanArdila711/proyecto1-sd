"""Permisos desde el cliente distribuido (Hito 3, C3): un ControlNode con autenticación y
admin, y tres DataNodes reales."""

import pytest

from conftest import FAST_RAFT_CONF, TEST_ENCRYPTION_KEY, free_port, wait_for
from dfsha.client import distributed_client
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import AccessDeniedError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node

SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"
BLOCK = 5
CONTENT = b"contenido de alice"  # cuatro bloques de 5 bytes


@pytest.fixture
def cluster(tmp_path):
    """start() devuelve la dirección de un ControlNode nuevo, de un nodo; login(dirs,
    usuario, clave) arma un cliente con sesión."""
    datanodes, addresses, control_nodes, clients = [], [], [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        datanodes.append(server)
        addresses.append(f"localhost:{port}")

    def start():
        server, port, raft = serve_control_node(
            addresses,
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            block_size_bytes=BLOCK,
            raft_conf_overrides=FAST_RAFT_CONF,
            jwt_secret=SECRET,
            admin_password=ADMIN_PASSWORD,
        )
        control_nodes.append((server, raft))
        assert wait_for(raft._isLeader)
        tree = server._dfsha_control_servicer._replicated
        assert wait_for(lambda: tree.tree.get_user("admin") is not None), "no se creó el admin"
        return f"localhost:{port}"

    def login(control_addresses, username, password):
        made = DistributedDFShaClient(control_addresses, rpc_timeout_s=2.0, failover_budget_s=4.0)
        clients.append(made)
        made.login(username, password)
        return made

    yield start, login

    for made in clients:
        made.close()
    for server, raft in control_nodes:
        server.stop(grace=None)
        raft.destroy()
    for server in datanodes:
        server.stop(grace=None)


@pytest.fixture
def people(cluster, tmp_path):
    """admin, alice y bob con sesión en el mismo ControlNode; alice subió /alice/a.txt."""
    start, login = cluster
    address = start()
    admin = login([address], "admin", ADMIN_PASSWORD)
    admin.create_user("alice", "clave-de-alice")
    admin.create_user("bob", "clave-de-bob")
    alice = login([address], "alice", "clave-de-alice")
    bob = login([address], "bob", "clave-de-bob")
    source = tmp_path / "origen.txt"
    source.write_bytes(CONTENT)
    alice.make_dir("/alice")
    alice.upload(source, "/alice/a.txt")
    return admin, alice, bob


def test_chmod_takes_reading_away_from_others(people, tmp_path):
    _, alice, bob = people
    assert bob.download("/alice/a.txt", tmp_path / "antes.txt") == len(CONTENT)
    assert (tmp_path / "antes.txt").read_bytes() == CONTENT

    alice.chmod("/alice/a.txt", 0o600)

    for call in (
        lambda: bob.download("/alice/a.txt", tmp_path / "despues.txt"),
        lambda: bob.read("/alice/a.txt", 2, 5),
        lambda: bob.open("/alice/a.txt", "r"),
    ):
        with pytest.raises(AccessDeniedError) as exc_info:
            call()
        assert str(exc_info.value) == "permiso denegado: falta r en /alice/a.txt"
    assert not (tmp_path / "despues.txt").exists()
    assert bob.locks() == []
    assert alice.read("/alice/a.txt", 2, 5) == CONTENT[2:7]


def test_others_cannot_remove_or_write_but_the_admin_can(people, tmp_path):
    admin, alice, bob = people

    with pytest.raises(AccessDeniedError) as removing:
        bob.remove("/alice/a.txt")
    with pytest.raises(AccessDeniedError) as writing:
        bob.write("/alice/a.txt", 0, b"BOB")

    assert str(removing.value) == "permiso denegado: falta w en /alice"
    assert str(writing.value) == "permiso denegado: falta w en /alice/a.txt"
    assert alice.read("/alice/a.txt") == CONTENT
    assert admin.write("/alice/a.txt", 0, b"ADM") == 3
    assert alice.read("/alice/a.txt") == b"ADM" + CONTENT[3:]
    admin.remove("/alice/a.txt")
    assert alice.list_dir("/alice") == []


def test_ls_shows_the_owner_and_mode_from_the_server(people):
    _, alice, bob = people
    alice.chmod("/alice/a.txt", 0o640)

    entries = bob.list_dir("/alice")

    assert [(e.name, e.owner, e.group, e.mode, e.size_bytes) for e in entries] == [
        ("a.txt", "alice", "alice", 0o640, len(CONTENT))
    ]


def test_an_access_denied_error_does_not_try_another_control_node(cluster, monkeypatch):
    start, login = cluster
    first, second = start(), start()
    admin = login([first], "admin", ADMIN_PASSWORD)
    admin.create_user("bob", "clave-de-bob")
    admin.make_dir("/privado")
    admin.chmod("/privado", 0o700)
    bob = login([first, second], "bob", "clave-de-bob")
    calls = []
    real_stub = distributed_client.control_node_pb2_grpc.ControlNodeServiceStub
    address_of = {id(channel): address for address, channel in bob._control_channels.items()}

    def recording_stub(channel):
        calls.append(address_of[id(channel)])
        return real_stub(channel)

    monkeypatch.setattr(distributed_client.control_node_pb2_grpc, "ControlNodeServiceStub", recording_stub)

    with pytest.raises(AccessDeniedError) as exc_info:
        bob.list_dir("/privado")

    assert str(exc_info.value) == "permiso denegado: falta r en /privado"
    # los nodos aplican el mismo log: preguntarle a otro no cambia la respuesta
    assert calls == [first]


def test_after_chown_by_the_admin_the_new_owner_can(people, tmp_path):
    admin, alice, bob = people
    alice.chmod("/alice/a.txt", 0o600)
    with pytest.raises(AccessDeniedError):
        bob.read("/alice/a.txt")

    admin.chown("/alice/a.txt", "bob")

    assert bob.read("/alice/a.txt") == CONTENT
    assert bob.write("/alice/a.txt", 0, b"BOB") == 3
    bob.chmod("/alice/a.txt", 0o644)
    assert alice.read("/alice/a.txt") == b"BOB" + CONTENT[3:]
    with pytest.raises(AccessDeniedError) as exc_info:
        alice.chown("/alice/a.txt", "alice")
    assert str(exc_info.value) == "permiso denegado: solo un admin cambia el dueño de /alice/a.txt"
