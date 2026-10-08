"""Autenticación en el ControlNode (Hito 3, C2): token obligatorio, Login, CreateUser y
ChangePassword. Un ControlNode de un solo nodo, con un DataNode que no existe: estos
tests no mueven datos."""

import logging
import time
import uuid

import grpc
import pytest

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.common.auth import Principal, issue_token, verify_token
from dfsha.control_node.main import serve as serve_control_node
from dfsha.generated import control_node_pb2 as pb
from dfsha.generated import control_node_pb2_grpc

SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
OTHER_SECRET = b"otro-secreto-de-prueba-32-bytes!!"
SERVICE = pb.DESCRIPTOR.services_by_name["ControlNodeService"]
# Los únicos RPC que se pueden llamar sin token. Agregar uno acá es una decisión de
# seguridad: todo RPC nuevo que no esté en esta lista tiene que rechazar sin token.
PUBLIC_RPCS = {"Login"}

ADMIN = Principal("root", ("root",), True)
ALICE = Principal("alice", ("alice",), False)


def _op() -> str:
    return uuid.uuid4().hex


def _bearer(principal: Principal = ADMIN, secret: bytes = SECRET, ttl_s: float = 60, now: float | None = None):
    token = issue_token(secret, principal, ttl_s, time.time() if now is None else now)
    return (("authorization", f"Bearer {token}"),)


def _dfsha_error(rpc_error: grpc.RpcError) -> str | None:
    return dict(rpc_error.trailing_metadata() or ()).get("dfsha-error")


@pytest.fixture
def start_node():
    started = []

    def _start(**kwargs):
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
        channel = grpc.insecure_channel(f"localhost:{port}")
        started.append((server, raft, channel))
        assert wait_for(raft._isLeader)
        return control_node_pb2_grpc.ControlNodeServiceStub(channel), server

    yield _start

    for server, raft, channel in started:
        channel.close()
        server.stop(grace=None)
        raft.destroy()


@pytest.fixture
def stub(start_node):
    return start_node(jwt_secret=SECRET)[0]


def _protected_methods():
    return [method for method in SERVICE.methods if method.name not in PUBLIC_RPCS]


def _call_empty(stub, method, metadata=None):
    request = getattr(pb, method.input_type.name)()
    return getattr(stub, method.name)(request, metadata=metadata, timeout=5)


# --- T5: token obligatorio -------------------------------------------------------------------


def test_every_rpc_but_login_rejects_a_call_without_token(stub):
    # Recorre el .proto: un RPC nuevo que olvide _authenticate hace fallar este test.
    methods = _protected_methods()
    assert len(methods) >= 17

    for method in methods:
        with pytest.raises(grpc.RpcError) as exc_info:
            _call_empty(stub, method)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED, method.name
        assert _dfsha_error(exc_info.value) == "AuthError", method.name


@pytest.mark.parametrize(
    "metadata",
    [
        (("authorization", "Bearer esto-no-es-un-token"),),
        _bearer(secret=OTHER_SECRET),
        _bearer(now=time.time() - 3600),
        (("authorization", issue_token(SECRET, ADMIN, 60, time.time())),),
        (("authorization", "Bearer "),),
    ],
    ids=["basura", "otro secreto", "vencido", "sin Bearer", "Bearer vacío"],
)
def test_every_rpc_but_login_rejects_a_bad_token(stub, metadata):
    for method in _protected_methods():
        with pytest.raises(grpc.RpcError) as exc_info:
            _call_empty(stub, method, metadata)
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED, method.name
        assert _dfsha_error(exc_info.value) == "AuthError", method.name


def test_a_valid_token_lets_the_call_through(stub):
    stub.MakeDir(pb.MakeDirRequest(path="/docs", op_id=_op()), metadata=_bearer(ALICE))

    entries = stub.ListDir(pb.ListDirRequest(path="/"), metadata=_bearer(ALICE)).entries

    assert [entry.name for entry in entries] == ["docs"]


def test_a_rejected_mutation_changes_nothing(stub):
    with pytest.raises(grpc.RpcError):
        stub.MakeDir(pb.MakeDirRequest(path="/intruso", op_id=_op()))

    assert list(stub.ListDir(pb.ListDirRequest(path="/"), metadata=_bearer()).entries) == []


def test_without_a_secret_nothing_is_required(start_node):
    stub, _ = start_node()

    stub.MakeDir(pb.MakeDirRequest(path="/a", op_id=_op()))
    stub.MakeDir(pb.MakeDirRequest(path="/b", op_id=_op()), metadata=(("authorization", "basura"),))

    assert [entry.name for entry in stub.ListDir(pb.ListDirRequest(path="/")).entries] == ["a", "b"]


def test_the_lock_owner_is_the_user(start_node):
    stub, server = start_node(jwt_secret=SECRET)
    metadata = _bearer(ALICE)
    block = stub.BeginUpload(pb.BeginUploadRequest(path="/f", size_bytes=1, op_id=_op()), metadata=metadata).blocks[0]
    stub.ConfirmBlock(
        pb.ConfirmBlockRequest(path="/f", block_id=block.block_id, checksum="x", size_bytes=1, op_id=_op()),
        metadata=metadata,
    )
    stub.CompleteUpload(pb.CompleteUploadRequest(path="/f", op_id=_op()), metadata=metadata)

    lock_id = stub.Lock(pb.LockRequest(path="/f", mode="r", op_id=_op()), metadata=metadata).lock_id

    holders = server._dfsha_control_servicer._replicated.tree._locks["/f"].holders
    assert holders[lock_id][0] == "alice"


# --- T6: Login, CreateUser, ChangePassword ---------------------------------------------------


def _create(stub, username, password, metadata=None, **kwargs):
    return stub.CreateUser(
        pb.CreateUserRequest(username=username, password=password, op_id=kwargs.pop("op_id", _op()), **kwargs),
        metadata=_bearer() if metadata is None else metadata,
    )


def _login(stub, username, password, **kwargs):
    return stub.Login(pb.LoginRequest(username=username, password=password), **kwargs)


def _as(response):
    return (("authorization", f"Bearer {response.token}"),)


def test_login_returns_a_token_for_that_user(stub):
    _create(stub, "alice", "clave-de-alice", groups=["alice", "docentes"])

    response = _login(stub, "alice", "clave-de-alice")

    assert verify_token(SECRET, response.token) == Principal("alice", ("alice", "docentes"), False)
    assert response.expires_in_s == 1800
    assert (response.username, list(response.groups), response.is_admin) == ("alice", ["alice", "docentes"], False)
    assert stub.ListDir(pb.ListDirRequest(path="/"), metadata=_as(response)) is not None


def test_login_needs_no_token_and_ignores_a_bad_one(stub):
    _create(stub, "alice", "clave-de-alice")

    assert _login(stub, "alice", "clave-de-alice").token
    assert _login(stub, "alice", "clave-de-alice", metadata=(("authorization", "Bearer basura"),)).token


def test_wrong_password_and_unknown_user_look_the_same(stub):
    _create(stub, "alice", "clave-de-alice")
    failures = []

    for username, password in [("alice", "incorrecta"), ("nadie", "clave-de-alice"), ("", ""), ("alice", "")]:
        with pytest.raises(grpc.RpcError) as exc_info:
            _login(stub, username, password)
        failures.append((exc_info.value.code(), exc_info.value.details(), _dfsha_error(exc_info.value)))

    assert failures == [(grpc.StatusCode.UNAUTHENTICATED, "usuario o contraseña incorrectos", "AuthError")] * 4


def test_a_token_stops_working_when_it_expires(start_node):
    stub, _ = start_node(jwt_secret=SECRET, token_ttl_s=2)
    _create(stub, "alice", "clave-de-alice")
    metadata = _as(_login(stub, "alice", "clave-de-alice"))
    stub.ListDir(pb.ListDirRequest(path="/"), metadata=metadata)

    def rejected() -> bool:
        try:
            stub.ListDir(pb.ListDirRequest(path="/"), metadata=metadata)
        except grpc.RpcError as exc:
            return exc.code() == grpc.StatusCode.UNAUTHENTICATED and "vencido" in exc.details()
        return False

    assert wait_for(rejected, timeout=6)


def test_user_rpcs_are_unimplemented_without_authentication(start_node):
    stub, _ = start_node()

    for call in (
        lambda: _login(stub, "alice", "x"),
        lambda: stub.CreateUser(pb.CreateUserRequest(username="alice", password="x", op_id=_op())),
        lambda: stub.ChangePassword(pb.ChangePasswordRequest(new_password="x", op_id=_op())),
    ):
        with pytest.raises(grpc.RpcError) as exc_info:
            call()
        assert exc_info.value.code() == grpc.StatusCode.UNIMPLEMENTED


def test_admin_creates_a_user_whose_default_group_is_its_name(stub):
    _create(stub, "bob", "clave-de-bob")

    response = _login(stub, "bob", "clave-de-bob")

    assert list(response.groups) == ["bob"]
    assert not response.is_admin


def test_admin_can_create_another_admin(stub):
    _create(stub, "jefa", "clave-de-jefa", is_admin=True)

    assert _login(stub, "jefa", "clave-de-jefa").is_admin


def test_a_regular_user_cannot_create_users(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        _create(stub, "bob", "clave-de-bob", metadata=_bearer(ALICE))

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert _dfsha_error(exc_info.value) == "AccessDeniedError"
    with pytest.raises(grpc.RpcError):
        _login(stub, "bob", "clave-de-bob")


def test_a_duplicate_user_keeps_its_original_password(stub):
    _create(stub, "alice", "la-original")

    with pytest.raises(grpc.RpcError) as exc_info:
        _create(stub, "alice", "la-del-impostor")

    assert exc_info.value.code() == grpc.StatusCode.ALREADY_EXISTS
    assert _login(stub, "alice", "la-original").token
    with pytest.raises(grpc.RpcError):
        _login(stub, "alice", "la-del-impostor")


@pytest.mark.parametrize(
    ("username", "password"), [("Nombre Malo", "clave"), ("../x", "clave"), ("alice", ""), ("alice", "x" * 1025)]
)
def test_invalid_username_or_password_is_rejected(stub, username, password):
    with pytest.raises(grpc.RpcError) as exc_info:
        _create(stub, username, password)

    assert _dfsha_error(exc_info.value) == "InvalidPathError"


def test_create_user_needs_an_op_id(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        _create(stub, "alice", "clave", op_id="")

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_retrying_create_user_with_the_same_op_id_is_harmless(stub):
    op_id = _op()

    _create(stub, "alice", "clave-de-alice", op_id=op_id)
    _create(stub, "alice", "clave-de-alice", op_id=op_id)

    assert _login(stub, "alice", "clave-de-alice").token


def _change(stub, metadata, new_password, current_password="", username=""):
    return stub.ChangePassword(
        pb.ChangePasswordRequest(
            username=username, current_password=current_password, new_password=new_password, op_id=_op()
        ),
        metadata=metadata,
    )


def test_a_user_changes_its_own_password_with_the_current_one(stub):
    _create(stub, "alice", "la-vieja")
    old_session = _as(_login(stub, "alice", "la-vieja"))

    _change(stub, old_session, "la-nueva", current_password="la-vieja")

    assert _login(stub, "alice", "la-nueva").token
    with pytest.raises(grpc.RpcError) as exc_info:
        _login(stub, "alice", "la-vieja")
    assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
    # Costo declarado del token autocontenido: no hay revocación. Un token emitido antes
    # del cambio sigue sirviendo hasta que vence.
    assert stub.ListDir(pb.ListDirRequest(path="/"), metadata=old_session) is not None


@pytest.mark.parametrize("current_password", ["incorrecta", ""])
def test_changing_your_own_password_needs_the_current_one(stub, current_password):
    _create(stub, "alice", "la-vieja")
    session = _as(_login(stub, "alice", "la-vieja"))

    with pytest.raises(grpc.RpcError) as exc_info:
        _change(stub, session, "la-nueva", current_password=current_password)

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert _dfsha_error(exc_info.value) == "AccessDeniedError"
    assert _login(stub, "alice", "la-vieja").token


def test_a_regular_user_cannot_change_someone_elses_password(stub):
    _create(stub, "alice", "clave-de-alice")
    _create(stub, "bob", "clave-de-bob")
    alice = _as(_login(stub, "alice", "clave-de-alice"))

    with pytest.raises(grpc.RpcError) as exc_info:
        _change(stub, alice, "robada", current_password="clave-de-alice", username="bob")

    assert _dfsha_error(exc_info.value) == "AccessDeniedError"
    assert _login(stub, "bob", "clave-de-bob").token


def test_admin_changes_someone_elses_password_without_the_current_one(stub):
    _create(stub, "alice", "la-vieja")

    _change(stub, _bearer(), "la-que-puso-el-admin", username="alice")

    assert _login(stub, "alice", "la-que-puso-el-admin").token


def test_admin_cannot_change_the_password_of_an_unknown_user(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        _change(stub, _bearer(), "clave", username="nadie")

    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_an_empty_new_password_is_rejected(stub):
    _create(stub, "alice", "la-vieja")

    with pytest.raises(grpc.RpcError) as exc_info:
        _change(stub, _as(_login(stub, "alice", "la-vieja")), "", current_password="la-vieja")

    assert _dfsha_error(exc_info.value) == "InvalidPathError"


def test_passwords_hashes_and_salts_never_reach_logs_or_responses(start_node, caplog):
    stub, server = start_node(jwt_secret=SECRET)
    password = "clave-super-secreta-de-alice"
    visible = []

    with caplog.at_level(logging.DEBUG):
        visible.append(repr(_create(stub, "alice", password)))
        visible.append(repr(_login(stub, "alice", password)))
        for bad in ("otra-clave-secreta-equivocada", password + "x"):
            with pytest.raises(grpc.RpcError) as exc_info:
                _login(stub, "alice", bad)
            visible.append(exc_info.value.details())
        with pytest.raises(grpc.RpcError) as exc_info:
            _create(stub, "alice", password)
        visible.append(exc_info.value.details())

    record = server._dfsha_control_servicer._replicated.tree.get_user("alice")
    text = caplog.text + "\n".join(visible)
    for secret in (password, "otra-clave-secreta-equivocada"):
        assert secret not in text
    for secret in (record.password_hash, record.salt):
        assert secret.hex() not in text
        assert repr(secret) not in text


def test_a_user_cannot_have_so_many_groups_that_its_token_stops_fitting(stub):
    # Los grupos viajan en el token y el token en la metadata de cada llamada, que gRPC
    # limita: una cuenta con miles de grupos quedaría inutilizable, y no se puede borrar.
    with pytest.raises(grpc.RpcError) as exc_info:
        _create(stub, "alice", "clave-de-alice", groups=[f"g{i}" for i in range(17)])

    assert _dfsha_error(exc_info.value) == "InvalidPathError"
    with pytest.raises(grpc.RpcError):
        _login(stub, "alice", "clave-de-alice")
    _create(stub, "alice", "clave-de-alice", groups=[f"g{i}" for i in range(16)])
    assert len(_login(stub, "alice", "clave-de-alice").groups) == 16


@pytest.mark.parametrize("ttl", [float("nan"), float("inf"), 0, -1])
def test_the_token_ttl_must_be_a_positive_finite_number(start_node, ttl):
    with pytest.raises(ValueError, match="token_ttl_s"):
        start_node(jwt_secret=SECRET, token_ttl_s=ttl)

