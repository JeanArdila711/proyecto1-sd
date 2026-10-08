"""El cliente con sesión (Hito 3, C2): login, token en cada llamada y vencimiento."""

import pytest

from conftest import FAST_RAFT_CONF, TEST_ENCRYPTION_KEY, free_port, wait_for
from dfsha.client import distributed_client
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import AccessDeniedError, AuthError, DFShaError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node

SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"
BLOCK = 5


@pytest.fixture
def cluster(tmp_path):
    """Tres DataNodes y ControlNodes de un nodo cada uno. start(**kwargs) devuelve
    (dirección, servidor) de un ControlNode nuevo; client(direcciones) arma un cliente."""
    datanodes, addresses, control_nodes, clients = [], [], [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY)
        datanodes.append(server)
        addresses.append(f"localhost:{port}")

    def start(authenticated=True, **kwargs):
        if authenticated:
            kwargs.setdefault("jwt_secret", SECRET)
            kwargs.setdefault("admin_password", ADMIN_PASSWORD)
        server, port, raft = serve_control_node(
            addresses,
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            block_size_bytes=BLOCK,
            raft_conf_overrides=FAST_RAFT_CONF,
            **kwargs,
        )
        control_nodes.append((server, raft))
        assert wait_for(raft._isLeader)
        if kwargs.get("admin_password"):
            tree = server._dfsha_control_servicer._replicated
            assert wait_for(lambda: tree.tree.get_user("admin") is not None), "no se creó el admin"
        return f"localhost:{port}", server

    def client(control_addresses, **kwargs):
        made = DistributedDFShaClient(control_addresses, rpc_timeout_s=2.0, failover_budget_s=4.0, **kwargs)
        clients.append(made)
        return made

    yield start, client

    for made in clients:
        made.close()
    for server, raft in control_nodes:
        server.stop(grace=None)
        raft.destroy()
    for server in datanodes:
        server.stop(grace=None)


@pytest.fixture
def calls_per_node(monkeypatch):
    """Dirección del ControlNode de cada intento de _call, en orden."""
    calls = []
    real_stub = distributed_client.control_node_pb2_grpc.ControlNodeServiceStub

    def install(client):
        address_of = {id(channel): address for address, channel in client._control_channels.items()}

        def recording_stub(channel):
            calls.append(address_of[id(channel)])
            return real_stub(channel)

        monkeypatch.setattr(distributed_client.control_node_pb2_grpc, "ControlNodeServiceStub", recording_stub)
        return calls

    return install


def test_login_opens_a_session(cluster):
    start, make_client = cluster
    client = make_client([start()[0]])

    client.login("admin", ADMIN_PASSWORD)

    assert client.list_dir("/") == []
    assert (client.username, client.groups, client.is_admin) == ("admin", ("admin",), True)


def test_without_a_session_the_call_fails_once_and_does_not_try_another_node(cluster, calls_per_node):
    start, make_client = cluster
    first, second = start()[0], start()[0]
    client = make_client([first, second])
    calls = calls_per_node(client)

    with pytest.raises(AuthError) as exc_info:
        client.list_dir("/")

    assert type(exc_info.value) is AuthError
    # los nodos comparten el secreto: probar el otro no cambia nada
    assert calls == [first]


def test_a_failed_login_keeps_the_previous_session(cluster, calls_per_node):
    start, make_client = cluster
    first, second = start()[0], start()[0]
    client = make_client([first, second])
    client.login("admin", ADMIN_PASSWORD)
    calls = calls_per_node(client)

    with pytest.raises(AuthError) as exc_info:
        client.login("admin", "incorrecta")

    assert type(exc_info.value) is AuthError
    assert calls == [first]
    assert client.username == "admin"
    assert client.list_dir("/") == []


def test_an_expired_token_needs_a_new_login(cluster):
    start, make_client = cluster
    client = make_client([start(token_ttl_s=2)[0]])
    client.login("admin", ADMIN_PASSWORD)
    client.make_dir("/docs")

    def expired() -> bool:
        try:
            client.list_dir("/")
        except AuthError:
            return True
        return False

    assert wait_for(expired, timeout=6)
    client.login("admin", ADMIN_PASSWORD)
    assert [entry.name for entry in client.list_dir("/")] == ["docs"]


def test_the_token_travels_from_the_transfer_threads(cluster, tmp_path):
    start, make_client = cluster
    client = make_client([start()[0]], parallel_transfers=4)
    client.login("admin", ADMIN_PASSWORD)
    content = bytes(range(47))  # 10 bloques de 5 bytes: varios ConfirmBlock a la vez
    source = tmp_path / "origen.bin"
    source.write_bytes(content)

    assert client.upload(source, "/datos.bin") == len(content)
    client.download("/datos.bin", tmp_path / "vuelta.bin")

    assert (tmp_path / "vuelta.bin").read_bytes() == content


def test_the_token_travels_from_the_lock_renewer(cluster, tmp_path):
    start, make_client = cluster
    lease_s = 0.6
    address, server = start(lock_lease_s=lease_s)
    client = make_client([address])
    client.login("admin", ADMIN_PASSWORD)
    (tmp_path / "a.txt").write_bytes(b"hola")
    client.upload(tmp_path / "a.txt", "/a.txt")

    held = client.lock("/a.txt", "r")

    def expires_at() -> float:
        return server._dfsha_control_servicer._replicated.tree._locks["/a.txt"].holders[held.lock_id][1]

    first = expires_at()
    # El vencimiento pasó más allá del lease original: hubo renovaciones aceptadas, y las
    # manda el hilo renovador. Sin token, la primera fallaría y el lock caducaría.
    assert wait_for(lambda: expires_at() > first + lease_s, timeout=10)
    assert not held.lost


def test_a_lock_is_marked_lost_when_the_token_expires_under_it(cluster, tmp_path):
    start, make_client = cluster
    client = make_client([start(token_ttl_s=2, lock_lease_s=0.6)[0]])
    client.login("admin", ADMIN_PASSWORD)
    (tmp_path / "a.txt").write_bytes(b"hola")
    client.upload(tmp_path / "a.txt", "/a.txt")

    held = client.lock("/a.txt", "w")

    # Con el token vencido el lease ya no se renueva y caduca en el servidor: el handle
    # no puede seguir aparentando que protege el archivo.
    assert wait_for(lambda: held.lost, timeout=8)
    assert held not in client.locks()


def test_admin_creates_a_user_who_then_changes_its_password(cluster):
    start, make_client = cluster
    address = start()[0]
    admin, alice = make_client([address]), make_client([address])
    admin.login("admin", ADMIN_PASSWORD)

    admin.create_user("alice", "la-primera", groups=["alice", "docentes"])
    alice.login("alice", "la-primera")
    alice.change_password("la-segunda", current_password="la-primera")

    assert (alice.username, alice.groups, alice.is_admin) == ("alice", ("alice", "docentes"), False)
    fresh = make_client([address])
    fresh.login("alice", "la-segunda")
    with pytest.raises(AuthError):
        fresh.login("alice", "la-primera")


def test_admin_resets_someone_elses_password(cluster):
    start, make_client = cluster
    address = start()[0]
    admin = make_client([address])
    admin.login("admin", ADMIN_PASSWORD)
    admin.create_user("alice", "la-olvidada")

    admin.change_password("la-nueva", username="alice")

    make_client([address]).login("alice", "la-nueva")


def test_a_regular_user_cannot_create_users(cluster):
    start, make_client = cluster
    address = start()[0]
    admin, alice = make_client([address]), make_client([address])
    admin.login("admin", ADMIN_PASSWORD)
    admin.create_user("alice", "clave-de-alice")
    alice.login("alice", "clave-de-alice")

    with pytest.raises(AccessDeniedError) as exc_info:
        alice.create_user("bob", "clave-de-bob")

    assert type(exc_info.value) is AccessDeniedError


def test_login_against_a_cluster_without_authentication_says_so(cluster):
    start, make_client = cluster
    client = make_client([start(authenticated=False)[0]])

    with pytest.raises(DFShaError) as exc_info:
        client.login("admin", ADMIN_PASSWORD)

    assert type(exc_info.value) is DFShaError
    assert "no tiene autenticación" in str(exc_info.value)
    assert client.username is None
    assert client.list_dir("/") == []  # sin autenticación, todo sigue funcionando sin sesión
