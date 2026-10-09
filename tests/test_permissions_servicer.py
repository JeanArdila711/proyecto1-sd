"""Permisos por archivo en el ControlNode (Hito 3, C3). Un ControlNode de un solo nodo con
autenticación y tokens firmados en el test; el DataNode no existe: estos tests no mueven
datos (los bloques se confirman sin escribirse)."""

import time
import uuid

import grpc
import pytest

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.common.auth import Principal, issue_token
from dfsha.control_node.main import serve as serve_control_node
from dfsha.generated import control_node_pb2 as pb
from dfsha.generated import control_node_pb2_grpc

SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN = Principal("root", ("root",), True)
ALICE = Principal("alice", ("alice", "docentes"), False)
BOB = Principal("bob", ("bob",), False)
SIZE = 4321  # tamaño del archivo de prueba; no puede aparecer en un rechazo


def _op() -> str:
    return uuid.uuid4().hex


def _as(principal: Principal):
    return (("authorization", f"Bearer {issue_token(SECRET, principal, 60, time.time())}"),)


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
            # el DataNode no existe: que el monitor no lo saque del pipeline a mitad del test
            datanode_dead_after_s=3600,
            **kwargs,
        )
        channel = grpc.insecure_channel(f"localhost:{port}")
        started.append((server, raft, channel))
        assert wait_for(raft._isLeader)
        return control_node_pb2_grpc.ControlNodeServiceStub(channel), server._dfsha_control_servicer

    yield _start

    for server, raft, channel in started:
        channel.close()
        server.stop(grace=None)
        raft.destroy()


@pytest.fixture
def node(start_node):
    return start_node(jwt_secret=SECRET)


def _create_user(stub, username, groups=()):
    stub.CreateUser(
        pb.CreateUserRequest(username=username, password="clave", groups=list(groups), op_id=_op()),
        metadata=_as(ADMIN),
    )


def _upload(stub, path, principal=ALICE, size=SIZE):
    metadata = _as(principal)
    block = stub.BeginUpload(pb.BeginUploadRequest(path=path, size_bytes=size, op_id=_op()), metadata=metadata).blocks[0]
    stub.ConfirmBlock(
        pb.ConfirmBlockRequest(path=path, block_id=block.block_id, checksum="x", size_bytes=size, op_id=_op()),
        metadata=metadata,
    )
    stub.CompleteUpload(pb.CompleteUploadRequest(path=path, op_id=_op()), metadata=metadata)
    return block.block_id


def _alice_file(stub, mode=None):
    """/docs (de alice, 0o755) con /docs/a.bin (de alice, 0o644 o `mode`)."""
    stub.MakeDir(pb.MakeDirRequest(path="/docs", op_id=_op()), metadata=_as(ALICE))
    _upload(stub, "/docs/a.bin")
    if mode is not None:
        stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=mode, op_id=_op()), metadata=_as(ALICE))


def _denied(call):
    with pytest.raises(grpc.RpcError) as exc_info:
        call()
    error = exc_info.value
    assert error.code() == grpc.StatusCode.PERMISSION_DENIED, error
    assert _dfsha_error(error) == "AccessDeniedError", error
    return error.details()


def _names(stub, path, principal=ADMIN):
    return [entry.name for entry in stub.ListDir(pb.ListDirRequest(path=path), metadata=_as(principal)).entries]


def test_list_dir_brings_owner_group_and_mode(node):
    stub, _ = node
    _alice_file(stub)

    root = {e.name: (e.is_dir, e.owner, e.group, e.mode) for e in stub.ListDir(pb.ListDirRequest(path="/"), metadata=_as(BOB)).entries}
    docs = stub.ListDir(pb.ListDirRequest(path="/docs"), metadata=_as(BOB)).entries

    assert root == {"docs": (True, "alice", "alice", 0o755)}
    assert [(e.name, e.is_dir, e.size_bytes, e.owner, e.group, e.mode) for e in docs] == [
        ("a.bin", False, SIZE, "alice", "alice", 0o644)
    ]


def test_a_user_without_r_gets_no_block_locations(node):
    stub, _ = node
    _alice_file(stub, mode=0o600)

    details = _denied(lambda: stub.ListBlocks(pb.ListBlocksRequest(path="/docs/a.bin"), metadata=_as(BOB)))

    assert details == "permiso denegado: falta r en /docs/a.bin"
    assert len(stub.ListBlocks(pb.ListBlocksRequest(path="/docs/a.bin"), metadata=_as(ALICE)).blocks) == 1


def test_a_user_without_r_cannot_list_a_directory(node):
    stub, _ = node
    _alice_file(stub)
    stub.Chmod(pb.ChmodRequest(path="/docs", mode=0o700, op_id=_op()), metadata=_as(ALICE))

    details = _denied(lambda: stub.ListDir(pb.ListDirRequest(path="/docs"), metadata=_as(BOB)))

    assert details == "permiso denegado: falta r en /docs"


def _held_write(stub, servicer):
    """alice tiene el lock exclusivo y una reserva de escritura sobre /docs/a.bin."""
    lock_id = stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="w", op_id=_op()), metadata=_as(ALICE)).lock_id
    begun = stub.BeginWrite(
        pb.BeginWriteRequest(path="/docs/a.bin", offset=0, length=2, lock_id=lock_id, op_id=_op()),
        metadata=_as(ALICE),
    )
    slots = [pb.ConfirmedSlot(index=s.index, block_id=s.new_block_id, checksum="y", size_bytes=s.new_size) for s in begun.slots]
    return lock_id, begun, slots


def test_without_w_nothing_is_created_or_written(node):
    stub, servicer = node
    _alice_file(stub)
    stub.MakeDir(pb.MakeDirRequest(path="/docs/vacio", op_id=_op()), metadata=_as(ALICE))
    lock_id, begun, slots = _held_write(stub, servicer)
    bob = _as(BOB)
    tree = lambda: servicer._replicated.tree  # noqa: E731  (siempre el árbol vigente)

    cases = {
        "MakeDir": (lambda: stub.MakeDir(pb.MakeDirRequest(path="/docs/x", op_id=_op()), metadata=bob), "/docs"),
        "BeginUpload": (
            lambda: stub.BeginUpload(pb.BeginUploadRequest(path="/docs/x.bin", size_bytes=1, op_id=_op()), metadata=bob),
            "/docs",
        ),
        "Remove": (lambda: stub.Remove(pb.RemoveRequest(path="/docs/a.bin", op_id=_op()), metadata=bob), "/docs"),
        "RemoveDir": (lambda: stub.RemoveDir(pb.RemoveDirRequest(path="/docs/vacio", op_id=_op()), metadata=bob), "/docs"),
        "Lock w": (lambda: stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="w", op_id=_op()), metadata=bob), "/docs/a.bin"),
        "BeginWrite": (
            lambda: stub.BeginWrite(
                pb.BeginWriteRequest(path="/docs/a.bin", offset=0, length=2, lock_id=lock_id, op_id=_op()), metadata=bob
            ),
            "/docs/a.bin",
        ),
        "CommitWrite": (
            lambda: stub.CommitWrite(
                pb.CommitWriteRequest(
                    path="/docs/a.bin",
                    write_id=begun.write_id,
                    base_version=begun.base_version,
                    lock_id=lock_id,
                    slots=slots,
                    op_id=_op(),
                ),
                metadata=bob,
            ),
            "/docs/a.bin",
        ),
    }
    for name, (call, path) in cases.items():
        assert _denied(call) == f"permiso denegado: falta w en {path}", name

    assert _names(stub, "/docs") == ["a.bin", "vacio"]
    assert tree().write_layout("/docs/a.bin", 1)[0] == 0
    assert list(tree()._writes) == [begun.write_id]
    assert set(tree()._locks["/docs/a.bin"].holders) == {lock_id}


def test_a_user_without_r_cannot_take_a_shared_lock(node):
    stub, servicer = node
    _alice_file(stub, mode=0o600)

    details = _denied(lambda: stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="r", op_id=_op()), metadata=_as(BOB)))

    assert details == "permiso denegado: falta r en /docs/a.bin"
    assert servicer._replicated.tree._locks == {}


def _foreign_pending(stub):
    """Subida pendiente de alice en /pub (0o777): bob puede escribir ahí, pero no es suya."""
    stub.MakeDir(pb.MakeDirRequest(path="/pub", op_id=_op()), metadata=_as(ALICE))
    stub.Chmod(pb.ChmodRequest(path="/pub", mode=0o777, op_id=_op()), metadata=_as(ALICE))
    response = stub.BeginUpload(pb.BeginUploadRequest(path="/pub/p.bin", size_bytes=3, op_id=_op()), metadata=_as(ALICE))
    return response.blocks[0].block_id


def test_someone_else_cannot_confirm_complete_or_abort_an_upload(node, monkeypatch):
    stub, servicer = node
    block_id = _foreign_pending(stub)
    deleted = []
    monkeypatch.setattr(servicer, "_delete_blocks", lambda blocks: deleted.append(list(blocks)))
    bob = _as(BOB)
    message = "permiso denegado: la subida de /pub/p.bin es de otro usuario"

    assert _denied(
        lambda: stub.ConfirmBlock(
            pb.ConfirmBlockRequest(path="/pub/p.bin", block_id=block_id, checksum="x", size_bytes=3, op_id=_op()),
            metadata=bob,
        )
    ) == message
    assert _denied(lambda: stub.CompleteUpload(pb.CompleteUploadRequest(path="/pub/p.bin", op_id=_op()), metadata=bob)) == message
    assert _denied(lambda: stub.AbortUpload(pb.AbortUploadRequest(path="/pub/p.bin", op_id=_op()), metadata=bob)) == message

    pending = servicer._replicated.tree.pending_blocks("/pub/p.bin")
    assert [(b.block_id, b.size_bytes) for b in pending] == [(block_id, 0)]
    assert deleted == []
    stub.AbortUpload(pb.AbortUploadRequest(path="/pub/p.bin", op_id=_op()), metadata=_as(ALICE))
    assert [[b.block_id for b in blocks] for blocks in deleted] == [[block_id]]


def test_begin_write_without_w_reserves_nothing_and_does_not_reveal_the_size(node):
    stub, servicer = node
    _alice_file(stub)
    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o666, op_id=_op()), metadata=_as(ALICE))
    lock_id = stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="w", op_id=_op()), metadata=_as(BOB)).lock_id
    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o644, op_id=_op()), metadata=_as(ALICE))

    details = _denied(
        lambda: stub.BeginWrite(
            pb.BeginWriteRequest(path="/docs/a.bin", offset=SIZE, length=10, lock_id=lock_id, op_id=_op()),
            metadata=_as(BOB),
        )
    )

    assert details == "permiso denegado: falta w en /docs/a.bin"
    assert str(SIZE) not in details
    assert servicer._replicated.tree._writes == {}


def test_renew_and_unlock_only_need_the_lock_id(node):
    stub, servicer = node
    _alice_file(stub, mode=0o600)
    lock_id = stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="r", op_id=_op()), metadata=_as(ALICE)).lock_id

    stub.RenewLock(pb.RenewLockRequest(path="/docs/a.bin", lock_id=lock_id, op_id=_op()), metadata=_as(BOB))
    stub.Unlock(pb.UnlockRequest(path="/docs/a.bin", lock_id=lock_id, op_id=_op()), metadata=_as(BOB))

    assert servicer._replicated.tree._locks == {}


def test_retrying_a_denied_op_id_is_denied_again(node):
    stub, _ = node
    _alice_file(stub)
    request = pb.MakeDirRequest(path="/docs/x", op_id=_op())

    first = _denied(lambda: stub.MakeDir(request, metadata=_as(BOB)))
    stub.Chmod(pb.ChmodRequest(path="/docs", mode=0o777, op_id=_op()), metadata=_as(ALICE))
    second = _denied(lambda: stub.MakeDir(request, metadata=_as(BOB)))

    assert first == second == "permiso denegado: falta w en /docs"
    assert _names(stub, "/docs") == ["a.bin"]


# --- matriz: dueño, otro usuario y admin, antes y después de Chmod y Chown -----------------


def _allowed(call) -> bool:
    try:
        call()
    except grpc.RpcError as exc:
        assert exc.code() == grpc.StatusCode.PERMISSION_DENIED, exc
        assert _dfsha_error(exc) == "AccessDeniedError", exc
        return False
    return True


def _matrix(stub):
    def read(principal):
        return lambda: stub.ListBlocks(pb.ListBlocksRequest(path="/docs/a.bin"), metadata=_as(principal))

    def write(principal):
        def call():
            lock_id = stub.Lock(pb.LockRequest(path="/docs/a.bin", mode="w", op_id=_op()), metadata=_as(principal)).lock_id
            stub.Unlock(pb.UnlockRequest(path="/docs/a.bin", lock_id=lock_id, op_id=_op()), metadata=_as(principal))

        return call

    return {
        name: (_allowed(read(principal)), _allowed(write(principal)))
        for name, principal in (("alice", ALICE), ("bob", BOB), ("admin", ADMIN))
    }


def test_the_permission_matrix_follows_chmod_and_chown(node):
    stub, _ = node
    _create_user(stub, "bob")
    _alice_file(stub)

    assert _matrix(stub) == {"alice": (True, True), "bob": (True, False), "admin": (True, True)}

    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o600, op_id=_op()), metadata=_as(ALICE))
    assert _matrix(stub) == {"alice": (True, True), "bob": (False, False), "admin": (True, True)}

    stub.Chown(pb.ChownRequest(path="/docs/a.bin", owner="bob", op_id=_op()), metadata=_as(ADMIN))
    assert _matrix(stub) == {"alice": (False, False), "bob": (True, True), "admin": (True, True)}


# --- Chmod y Chown ------------------------------------------------------------------------


def test_chmod_and_chown_happy_path(node):
    stub, servicer = node
    _create_user(stub, "bob")
    _alice_file(stub)

    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o640, op_id=_op()), metadata=_as(ALICE))
    stub.Chown(pb.ChownRequest(path="/docs/a.bin", group="docentes", op_id=_op()), metadata=_as(ALICE))
    stub.Chown(pb.ChownRequest(path="/docs", owner="bob", group="bob", op_id=_op()), metadata=_as(ADMIN))

    entries = {e.name: (e.owner, e.group, e.mode) for e in stub.ListDir(pb.ListDirRequest(path="/docs"), metadata=_as(ADMIN)).entries}
    root = {e.name: (e.owner, e.group, e.mode) for e in stub.ListDir(pb.ListDirRequest(path="/"), metadata=_as(ADMIN)).entries}
    assert entries == {"a.bin": ("alice", "docentes", 0o640)}
    assert root == {"docs": ("bob", "bob", 0o755)}


def test_chmod_and_chown_by_someone_who_is_not_the_owner_are_denied(node):
    stub, _ = node
    _create_user(stub, "bob")
    _alice_file(stub)

    chmod = _denied(lambda: stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o777, op_id=_op()), metadata=_as(BOB)))
    chown_owner = _denied(
        lambda: stub.Chown(pb.ChownRequest(path="/docs/a.bin", owner="bob", op_id=_op()), metadata=_as(ALICE))
    )
    chown_group = _denied(
        lambda: stub.Chown(pb.ChownRequest(path="/docs/a.bin", group="bob", op_id=_op()), metadata=_as(BOB))
    )

    assert chmod == "permiso denegado: solo el dueño o un admin cambia el modo de /docs/a.bin"
    assert chown_owner == "permiso denegado: solo un admin cambia el dueño de /docs/a.bin"
    assert chown_group == (
        "permiso denegado: para cambiar el grupo de /docs/a.bin hay que ser su dueño y pertenecer al grupo nuevo"
    )
    entry = stub.ListDir(pb.ListDirRequest(path="/docs"), metadata=_as(ADMIN)).entries[0]
    assert (entry.owner, entry.group, entry.mode) == ("alice", "alice", 0o644)


@pytest.mark.parametrize("rpc", ["Chmod", "Chown"])
def test_chmod_and_chown_need_an_op_id(node, rpc):
    stub, _ = node
    _alice_file(stub)
    request = (
        pb.ChmodRequest(path="/docs/a.bin", mode=0o600, op_id="")
        if rpc == "Chmod"
        else pb.ChownRequest(path="/docs/a.bin", group="docentes", op_id="")
    )

    with pytest.raises(grpc.RpcError) as exc_info:
        getattr(stub, rpc)(request, metadata=_as(ALICE))

    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert exc_info.value.details() == "falta op_id"


def test_chmod_with_an_invalid_mode_is_rejected(node):
    stub, _ = node
    _alice_file(stub)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o1000, op_id=_op()), metadata=_as(ALICE))

    assert _dfsha_error(exc_info.value) == "InvalidPathError"
    assert exc_info.value.details() == "modo inválido: 512; va de 0 a 0o777"


def test_chown_to_an_unknown_user_is_not_found(node):
    stub, _ = node
    _alice_file(stub)

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.Chown(pb.ChownRequest(path="/docs/a.bin", owner="jaen", op_id=_op()), metadata=_as(ADMIN))

    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
    assert exc_info.value.details() == "no existe el usuario: jaen"


# --- sin autenticación ----------------------------------------------------------------------


def test_without_authentication_everything_works_and_belongs_to_admin(start_node):
    stub, _ = start_node()

    stub.MakeDir(pb.MakeDirRequest(path="/docs", op_id=_op()))
    block = stub.BeginUpload(pb.BeginUploadRequest(path="/docs/a.bin", size_bytes=3, op_id=_op())).blocks[0]
    stub.ConfirmBlock(pb.ConfirmBlockRequest(path="/docs/a.bin", block_id=block.block_id, checksum="x", size_bytes=3, op_id=_op()))
    stub.CompleteUpload(pb.CompleteUploadRequest(path="/docs/a.bin", op_id=_op()))
    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o000, op_id=_op()))

    root = stub.ListDir(pb.ListDirRequest(path="/")).entries
    docs = stub.ListDir(pb.ListDirRequest(path="/docs")).entries
    assert [(e.name, e.owner, e.group, e.mode) for e in root] == [("docs", "admin", "admin", 0o755)]
    assert [(e.name, e.owner, e.group, e.mode) for e in docs] == [("a.bin", "admin", "admin", 0o000)]
    # sin identidad no hay chequeo: un 0o000 no impide nada
    assert len(stub.ListBlocks(pb.ListBlocksRequest(path="/docs/a.bin")).blocks) == 1


# --- toda mutación que llega por RPC lleva a quien llama ------------------------------------


def test_every_mutation_reached_by_an_rpc_carries_the_caller(node, monkeypatch):
    """Complementa la guardia de firmas de test_permissions_tree.py: acá se mira qué le
    pasa el servicer al árbol. Si un RPC olvida el caller, el último argumento no es él."""
    from test_permissions_tree import EXEMPT_FROM_CALLER

    from dfsha.control_node.replicated_tree import MUTATIONS

    stub, servicer = node
    _create_user(stub, "bob")
    recorded = []
    real_commit_raw = servicer._commit_raw

    def recording(op_id, method, *args):
        recorded.append((method, args))
        return real_commit_raw(op_id, method, *args)

    monkeypatch.setattr(servicer, "_commit_raw", recording)
    alice = _as(ALICE)

    stub.MakeDir(pb.MakeDirRequest(path="/docs", op_id=_op()), metadata=alice)
    stub.MakeDir(pb.MakeDirRequest(path="/docs/vacio", op_id=_op()), metadata=alice)
    stub.RemoveDir(pb.RemoveDirRequest(path="/docs/vacio", op_id=_op()), metadata=alice)
    _upload(stub, "/docs/a.bin")
    stub.BeginUpload(pb.BeginUploadRequest(path="/docs/p.bin", size_bytes=1, op_id=_op()), metadata=alice)
    stub.AbortUpload(pb.AbortUploadRequest(path="/docs/p.bin", op_id=_op()), metadata=alice)
    lock_id, begun, slots = _held_write(stub, servicer)
    stub.CommitWrite(
        pb.CommitWriteRequest(
            path="/docs/a.bin", write_id=begun.write_id, base_version=begun.base_version, lock_id=lock_id, slots=slots, op_id=_op()
        ),
        metadata=alice,
    )
    stub.Unlock(pb.UnlockRequest(path="/docs/a.bin", lock_id=lock_id, op_id=_op()), metadata=alice)
    stub.Chmod(pb.ChmodRequest(path="/docs/a.bin", mode=0o600, op_id=_op()), metadata=alice)
    stub.Chown(pb.ChownRequest(path="/docs/a.bin", group="docentes", op_id=_op()), metadata=alice)
    stub.Remove(pb.RemoveRequest(path="/docs/a.bin", op_id=_op()), metadata=alice)

    checked = {method for method, _ in recorded if method not in EXEMPT_FROM_CALLER}
    assert checked == MUTATIONS - set(EXEMPT_FROM_CALLER)
    for method, args in recorded:
        if method not in EXEMPT_FROM_CALLER:
            assert args[-1] == ("alice", ("alice", "docentes"), False), method


def test_reusing_someone_elses_op_id_does_not_return_their_result(node):
    # La deduplicación devuelve el resultado guardado sin volver a ejecutar, y por lo tanto
    # sin volver a mirar permisos. Si la clave fuera solo el op_id, quien repitiera el de
    # otro usuario recibiría SU resultado: acá, los bloques de una subida ajena.
    stub, _ = node
    stub.MakeDir(pb.MakeDirRequest(path="/alice", op_id=_op()), metadata=_as(ALICE))
    request = pb.BeginUploadRequest(path="/alice/secreto.bin", size_bytes=3, op_id=_op())
    mine = stub.BeginUpload(request, metadata=_as(ALICE)).blocks[0]

    with pytest.raises(grpc.RpcError) as exc_info:
        stub.BeginUpload(request, metadata=_as(BOB))

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert exc_info.value.details() == "permiso denegado: falta w en /alice"
    # el reintento de la propia alice sigue deduplicando: mismo bloque, sin PathExistsError
    assert stub.BeginUpload(request, metadata=_as(ALICE)).blocks[0].block_id == mine.block_id


def test_the_same_op_id_from_two_users_is_two_operations(node):
    stub, _ = node
    op_id = _op()

    stub.MakeDir(pb.MakeDirRequest(path="/de-alice", op_id=op_id), metadata=_as(ALICE))
    stub.MakeDir(pb.MakeDirRequest(path="/de-bob", op_id=op_id), metadata=_as(BOB))

    names = {e.name: e.owner for e in stub.ListDir(pb.ListDirRequest(path="/"), metadata=_as(ADMIN)).entries}
    assert names == {"de-alice": "alice", "de-bob": "bob"}
