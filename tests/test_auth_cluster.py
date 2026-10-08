"""Bootstrap del admin y flags de autenticación (Hito 3, C2), con tres ControlNodes y
Raft real. Los DataNodes no existen: estos tests no mueven datos."""

import subprocess
import sys
import threading
import uuid
from pathlib import Path

import grpc
import pytest

from conftest import free_port, wait_for
from dfsha.control_node.main import load_admin_password, load_jwt_secret
from dfsha.control_node.main import serve as serve_control_node
from dfsha.control_node.servicer import BOOTSTRAP_ADMIN_OP_ID
from dfsha.generated import control_node_pb2 as pb
from test_raft_cluster import CLUSTER_RAFT_CONF, RaftCluster

_REPO = Path(__file__).resolve().parent.parent
SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"
FAKE_DATANODES = ["localhost:1", "localhost:2"]


class AuthCluster(RaftCluster):
    """RaftCluster con autenticación. RaftCluster.start no acepta argumentos extra para
    serve() y no se toca: acá se repite su cuerpo con los de C2."""

    def __init__(self, tmp_path, admin_password=ADMIN_PASSWORD, raft_conf=None):
        super().__init__(tmp_path, FAKE_DATANODES, persistent=True, raft_conf=raft_conf)
        self._admin_password = admin_password

    def start(self, i):
        server, port, raft = serve_control_node(
            self._datanode_addresses,
            "localhost",
            self.grpc_ports[i],
            raft_self=self.raft_addresses[i],
            raft_peers=[a for j, a in enumerate(self.raft_addresses) if j != i],
            data_dir=self._tmp_path / f"cn{i}",
            block_size_bytes=5,
            raft_conf_overrides=self._raft_conf,
            commit_timeout_s=1.0,
            jwt_secret=SECRET,
            admin_password=self._admin_password,
        )
        assert port == self.grpc_ports[i]
        self.nodes[i] = (server, raft)

    def start_all(self):
        for i in range(3):
            self.start(i)
        return self.wait_leader()

    def tree(self, i):
        return self.nodes[i][0]._dfsha_control_servicer._replicated.tree

    def replicated(self, i):
        return self.nodes[i][0]._dfsha_control_servicer._replicated

    def live(self):
        return [i for i, node in enumerate(self.nodes) if node is not None]

    def wait_admin(self):
        assert wait_for(
            lambda: all(self.tree(i).get_user("admin") is not None for i in self.live())
        ), "el admin no llegó a todas las réplicas"


@pytest.fixture
def make_auth_cluster(tmp_path):
    clusters, channels = [], []

    def _make(**kwargs):
        cluster = AuthCluster(tmp_path, **kwargs)
        clusters.append(cluster)
        cluster.start_all()
        return cluster

    def _stub(cluster, i):
        channel, stub = cluster.stub(i)
        channels.append(channel)
        return stub

    yield _make, _stub

    for channel in channels:
        channel.close()
    for cluster in clusters:
        cluster.close()


def _login(stub, password, username="admin"):
    return stub.Login(pb.LoginRequest(username=username, password=password), timeout=5)


def _login_at_leader(cluster, stub_of, password):
    """Login contra el líder actual, esperando a que termine una elección en curso."""
    result = []

    def attempt() -> bool:
        try:
            result.append(_login(stub_of(cluster, cluster.wait_leader()), password))
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAVAILABLE:
                raise
            return False
        return True

    assert wait_for(attempt, timeout=10), "ningún líder aceptó el login"
    return result[-1]


def _bearer(response):
    return (("authorization", f"Bearer {response.token}"),)


def test_the_cluster_creates_exactly_one_admin(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make()
    cluster.wait_admin()

    response = _login_at_leader(cluster, stub_of, ADMIN_PASSWORD)

    assert response.is_admin
    records = [cluster.tree(i).get_user("admin") for i in range(3)]
    assert records == [records[0]] * 3
    assert records[0].is_admin
    for i in range(3):
        assert set(cluster.tree(i)._users) == {"admin"}
        # los tres nodos lo intentan; el op_id fijo deja un solo resultado
        assert cluster.replicated(i).applied_ops[BOOTSTRAP_ADMIN_OP_ID] == ("ok", None)


def test_the_admin_survives_a_full_restart_unchanged(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make()
    cluster.wait_admin()
    before = cluster.tree(0).get_user("admin")

    for i in range(3):
        cluster.kill(i)
    cluster.start_all()
    cluster.wait_admin()

    for i in range(3):
        assert set(cluster.tree(i)._users) == {"admin"}
        # mismos bytes: el reinicio no recalculó el hash con otra sal
        assert cluster.tree(i).get_user("admin") == before
    assert _login_at_leader(cluster, stub_of, ADMIN_PASSWORD).is_admin


def test_a_leader_crash_keeps_the_admin_and_the_open_sessions(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make()
    cluster.wait_admin()
    before = cluster.tree(0).get_user("admin")
    old_session = _bearer(_login_at_leader(cluster, stub_of, ADMIN_PASSWORD))

    cluster.kill(cluster.wait_leader())
    new_leader = cluster.wait_leader()

    assert _login_at_leader(cluster, stub_of, ADMIN_PASSWORD).is_admin
    for i in cluster.live():
        assert cluster.tree(i).get_user("admin") == before
    # el token lo emitió el líder anterior: los tres comparten el secreto

    def old_token_works() -> bool:
        try:
            stub_of(cluster, cluster.wait_leader()).ListDir(pb.ListDirRequest(path="/"), metadata=old_session, timeout=5)
        except grpc.RpcError as exc:
            assert exc.code() == grpc.StatusCode.UNAVAILABLE, exc
            return False
        return True

    assert new_leader in cluster.live()
    assert wait_for(old_token_works, timeout=10)


def test_the_bootstrap_does_not_restore_a_changed_admin_password(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make()
    cluster.wait_admin()
    session = _bearer(_login_at_leader(cluster, stub_of, ADMIN_PASSWORD))
    stub_of(cluster, cluster.wait_leader()).ChangePassword(
        pb.ChangePasswordRequest(current_password=ADMIN_PASSWORD, new_password="la-nueva", op_id=uuid.uuid4().hex),
        metadata=session,
        timeout=5,
    )

    for i in range(3):
        cluster.kill(i)
    cluster.start_all()
    cluster.wait_admin()

    assert _login_at_leader(cluster, stub_of, "la-nueva").is_admin
    with pytest.raises(grpc.RpcError) as exc_info:
        _login_at_leader(cluster, stub_of, ADMIN_PASSWORD)
    assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_without_an_admin_password_no_user_is_created(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make(admin_password=None)

    with pytest.raises(grpc.RpcError) as exc_info:
        _login_at_leader(cluster, stub_of, ADMIN_PASSWORD)

    assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert all(not cluster.tree(i).has_users() for i in range(3))
    assert "dfsha-admin-bootstrap" not in {thread.name for thread in threading.enumerate()}


def test_a_follower_rejects_a_missing_token_before_saying_it_is_not_the_leader(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make()
    cluster.wait_admin()
    leader = cluster.wait_leader()
    follower = stub_of(cluster, next(i for i in range(3) if i != leader))

    with pytest.raises(grpc.RpcError) as without_token:
        follower.ListDir(pb.ListDirRequest(path="/"), timeout=5)
    with pytest.raises(grpc.RpcError) as login:
        _login(follower, ADMIN_PASSWORD)

    # la autenticación va primero: sin token no hay failover que valga
    assert without_token.value.code() == grpc.StatusCode.UNAUTHENTICATED
    # Login sí lee el árbol: un seguidor manda al cliente a buscar al líder
    assert login.value.code() == grpc.StatusCode.UNAVAILABLE


def test_a_leader_without_majority_does_not_log_anyone_in(make_auth_cluster):
    make, stub_of = make_auth_cluster
    cluster = make(raft_conf={**CLUSTER_RAFT_CONF, "leaderFallbackTimeout": 30.0})
    cluster.wait_admin()
    leader = cluster.wait_leader()
    for i in range(3):
        if i != leader:
            cluster.kill(i)

    assert cluster.nodes[leader][1]._isLeader(), "el test necesita un líder que no sabe que perdió la mayoría"
    with pytest.raises(grpc.RpcError) as exc_info:
        _login(stub_of(cluster, leader), ADMIN_PASSWORD)

    assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE


def test_stopping_a_node_stops_its_bootstrap_thread(tmp_path):
    # Un nodo solo, con un peer que no existe: nunca es líder y el hilo reintenta siempre.
    server, _, raft = serve_control_node(
        FAKE_DATANODES,
        "localhost",
        0,
        raft_self=f"localhost:{free_port()}",
        raft_peers=[f"localhost:{free_port()}"],
        data_dir=None,
        raft_conf_overrides=CLUSTER_RAFT_CONF,
        jwt_secret=SECRET,
        admin_password=ADMIN_PASSWORD,
    )
    try:
        assert wait_for(lambda: "dfsha-admin-bootstrap" in {t.name for t in threading.enumerate()})
    finally:
        server.stop(grace=None)
        raft.destroy()

    assert "dfsha-admin-bootstrap" not in {thread.name for thread in threading.enumerate()}


# --- carga de secretos y flags ---------------------------------------------------------------


def test_jwt_secret_file_must_be_long_enough(tmp_path):
    short = tmp_path / "jwt.secret"
    short.write_bytes(b"corto")
    good = tmp_path / "bueno.secret"
    good.write_bytes(b"x" * 40 + b"\n")

    with pytest.raises(ValueError, match="demasiado corto"):
        load_jwt_secret(short)
    assert load_jwt_secret(good) == b"x" * 40


def test_admin_password_file_must_not_be_empty(tmp_path):
    empty = tmp_path / "admin.password"
    empty.write_text("\n", encoding="utf-8")
    good = tmp_path / "buena.password"
    good.write_text("clave\n", encoding="utf-8")

    with pytest.raises(ValueError, match="vacía"):
        load_admin_password(empty)
    assert load_admin_password(good) == "clave"


def _run_control_node(tmp_path, *flags):
    return subprocess.run(
        [
            sys.executable, "-m", "dfsha.control_node.main",
            "--node-id", "0",
            "--raft-cluster", f"localhost:{free_port()}",
            "--data-dir", str(tmp_path / "raft"),
            "--datanode-addresses", ",".join(FAKE_DATANODES),
            "--port", "0",
            *flags,
        ],
        capture_output=True, text=True, check=False, cwd=_REPO, timeout=30,
    )


def test_incomplete_or_broken_authentication_flags_stop_the_node(tmp_path):
    good_secret = tmp_path / "jwt.secret"
    good_secret.write_bytes(b"x" * 40)
    short_secret = tmp_path / "corto.secret"
    short_secret.write_bytes(b"corto")
    password = tmp_path / "admin.password"
    password.write_text("clave", encoding="utf-8")
    cases = {
        "falta la contraseña del admin": (["--jwt-secret-file", str(good_secret)], "autenticación incompleta"),
        "falta el secreto": (["--admin-password-file", str(password)], "autenticación incompleta"),
        "el secreto no existe": (
            ["--jwt-secret-file", str(tmp_path / "no-existe"), "--admin-password-file", str(password)],
            "no-existe",
        ),
        "el secreto es corto": (
            ["--jwt-secret-file", str(short_secret), "--admin-password-file", str(password)],
            "demasiado corto",
        ),
        "TTL inválido": (
            ["--jwt-secret-file", str(good_secret), "--admin-password-file", str(password), "--token-ttl-s", "0"],
            "--token-ttl-s",
        ),
        "TTL que no es un número": (
            ["--jwt-secret-file", str(good_secret), "--admin-password-file", str(password), "--token-ttl-s", "nan"],
            "--token-ttl-s",
        ),
    }

    for name, (flags, expected) in cases.items():
        result = _run_control_node(tmp_path, *flags)
        assert result.returncode != 0, name
        assert expected in result.stderr, (name, result.stderr)
