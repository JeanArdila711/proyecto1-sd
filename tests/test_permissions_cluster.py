"""Permisos por archivo con tres ControlNodes y Raft real (Hito 3, C3). Los DataNodes no
existen: los archivos se crean confirmando bloques por el log, sin escribirlos."""

import time
import uuid

import grpc
import pytest

from conftest import wait_for
from dfsha.common.auth import Principal, issue_token
from dfsha.control_node.tree import DirNode
from dfsha.generated import control_node_pb2 as pb
from test_auth_cluster import SECRET, AuthCluster
from test_raft_cluster import CLUSTER_RAFT_CONF

ALICE = Principal("alice", ("alice",), False)
BOB = Principal("bob", ("bob",), False)
ADMIN = Principal("admin", ("admin",), True)
ALICE_CALLER = ("alice", ("alice",), False)


def _op() -> str:
    return uuid.uuid4().hex


def _as(principal: Principal):
    return (("authorization", f"Bearer {issue_token(SECRET, principal, 60, time.time())}"),)


def _dfsha_error(rpc_error: grpc.RpcError) -> str | None:
    return dict(rpc_error.trailing_metadata() or ()).get("dfsha-error")


@pytest.fixture
def make_cluster(tmp_path):
    clusters, channels = [], []

    def _make(**kwargs):
        cluster = AuthCluster(tmp_path, **kwargs)
        clusters.append(cluster)
        cluster.start_all()
        cluster.wait_admin()
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


def _at_leader(cluster, stub_of, call):
    """Llama al líder actual; reintenta con la misma request si una elección está en curso."""
    result = []

    def attempt() -> bool:
        try:
            result.append(call(stub_of(cluster, cluster.wait_leader())))
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAVAILABLE:
                raise
            return False
        return True

    assert wait_for(attempt, timeout=10), "ningún líder respondió"
    return result[-1]


def _commit_file(cluster, path, block_id):
    """Un archivo confirmado de alice, por el log del líder (los DataNodes no existen)."""
    replicated = cluster.replicated(cluster.wait_leader())
    for method, args in (
        ("begin_upload", (path, [(block_id, ["localhost:1"])], None, None, None, ALICE_CALLER)),
        ("confirm_block", (path, block_id, "x", 1, None, None, ALICE_CALLER)),
        ("complete_upload", (path, ALICE_CALLER)),
    ):
        assert replicated.apply(_op(), method, args, sync=True, timeout=5)[0] == "ok", method


def _private_setup(cluster, stub_of):
    """/priv (de alice, 0o700) y /f.bin (de alice, 0o600): bob no lee ninguno."""
    alice = _as(ALICE)
    mkdir = pb.MakeDirRequest(path="/priv", op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.MakeDir(mkdir, metadata=alice, timeout=5))
    chmod_dir = pb.ChmodRequest(path="/priv", mode=0o700, op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chmod(chmod_dir, metadata=alice, timeout=5))
    _commit_file(cluster, "/f.bin", "f" * 32)
    chmod_file = pb.ChmodRequest(path="/f.bin", mode=0o600, op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chmod(chmod_file, metadata=alice, timeout=5))


def _bob_reads(stub):
    """(código de ListDir /priv, código de ListBlocks /f.bin) para bob."""
    codes = []
    for call in (
        lambda: stub.ListDir(pb.ListDirRequest(path="/priv"), metadata=_as(BOB), timeout=5),
        lambda: stub.ListBlocks(pb.ListBlocksRequest(path="/f.bin"), metadata=_as(BOB), timeout=5),
    ):
        with pytest.raises(grpc.RpcError) as exc_info:
            call()
        codes.append(exc_info.value.code())
    return codes


def _permissions(tree) -> dict:
    """ruta -> (es directorio, dueño, grupo, modo) de todo el árbol de una réplica."""
    found = {}

    def visit(node, path):
        found[path] = (isinstance(node, DirNode), node.owner, node.group, node.mode)
        if isinstance(node, DirNode):
            for name, child in node.children.items():
                visit(child, f"{path.rstrip('/')}/{name}")

    with tree._lock:
        visit(tree._root, "/")
    return found


def test_a_follower_answers_unavailable_without_looking_at_permissions(make_cluster):
    make, stub_of = make_cluster
    cluster = make()
    _private_setup(cluster, stub_of)
    leader = cluster.wait_leader()
    follower = next(i for i in range(3) if i != leader)

    # el permiso se mira después de la barrera: un seguidor no llega a mirarlo
    assert _bob_reads(stub_of(cluster, follower)) == [grpc.StatusCode.UNAVAILABLE] * 2
    assert _bob_reads(stub_of(cluster, leader)) == [grpc.StatusCode.PERMISSION_DENIED] * 2


def test_a_leader_without_majority_answers_unavailable_without_looking_at_permissions(make_cluster):
    make, stub_of = make_cluster
    cluster = make(raft_conf={**CLUSTER_RAFT_CONF, "leaderFallbackTimeout": 30.0})
    _private_setup(cluster, stub_of)
    leader = cluster.wait_leader()
    assert _bob_reads(stub_of(cluster, leader)) == [grpc.StatusCode.PERMISSION_DENIED] * 2
    for i in range(3):
        if i != leader:
            cluster.kill(i)

    assert cluster.nodes[leader][1]._isLeader(), "el test necesita un líder que no sabe que perdió la mayoría"
    assert _bob_reads(stub_of(cluster, leader)) == [grpc.StatusCode.UNAVAILABLE] * 2


def test_a_chmod_converges_and_survives_the_leader(make_cluster):
    make, stub_of = make_cluster
    cluster = make()
    _commit_file(cluster, "/f.bin", "f" * 32)
    listing = pb.ListBlocksRequest(path="/f.bin")
    assert len(_at_leader(cluster, stub_of, lambda s: s.ListBlocks(listing, metadata=_as(BOB), timeout=5)).blocks) == 1

    chmod = pb.ChmodRequest(path="/f.bin", mode=0o600, op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chmod(chmod, metadata=_as(ALICE), timeout=5))
    assert wait_for(lambda: all(cluster.tree(i)._root.children["f.bin"].mode == 0o600 for i in range(3)))
    cluster.kill(cluster.wait_leader())

    with pytest.raises(grpc.RpcError) as exc_info:
        _at_leader(cluster, stub_of, lambda s: s.ListBlocks(listing, metadata=_as(BOB), timeout=5))
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert exc_info.value.details() == "permiso denegado: falta r en /f.bin"
    assert len(_at_leader(cluster, stub_of, lambda s: s.ListBlocks(listing, metadata=_as(ALICE), timeout=5)).blocks) == 1


def test_a_full_restart_keeps_owners_groups_and_modes(make_cluster):
    make, stub_of = make_cluster
    cluster = make()
    alice = _as(ALICE)
    for request in (
        pb.MakeDirRequest(path="/alice", op_id=_op()),
        pb.MakeDirRequest(path="/alice/sub", op_id=_op()),
    ):
        _at_leader(cluster, stub_of, lambda s, r=request: s.MakeDir(r, metadata=alice, timeout=5))
    _commit_file(cluster, "/alice/f.bin", "f" * 32)
    chmod_file = pb.ChmodRequest(path="/alice/f.bin", mode=0o640, op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chmod(chmod_file, metadata=alice, timeout=5))
    chmod_root = pb.ChmodRequest(path="/", mode=0o755, op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chmod(chmod_root, metadata=_as(ADMIN), timeout=5))
    chown = pb.ChownRequest(path="/alice/sub", owner="admin", group="staff", op_id=_op())
    _at_leader(cluster, stub_of, lambda s: s.Chown(chown, metadata=_as(ADMIN), timeout=5))
    expected = {
        "/": (True, "admin", "admin", 0o755),
        "/alice": (True, "alice", "alice", 0o755),
        "/alice/sub": (True, "admin", "staff", 0o755),
        "/alice/f.bin": (False, "alice", "alice", 0o640),
    }
    assert wait_for(lambda: all(_permissions(cluster.tree(i)) == expected for i in range(3)))

    for i in range(3):
        cluster.kill(i)
    cluster.start_all()

    assert wait_for(lambda: all(_permissions(cluster.tree(i)) == expected for i in range(3)), timeout=10)


def test_a_denied_mutation_leaves_the_same_outcome_in_every_replica(make_cluster):
    make, stub_of = make_cluster
    cluster = make()
    _private_setup(cluster, stub_of)
    op_id = _op()
    request = pb.MakeDirRequest(path="/priv/x", op_id=op_id)

    with pytest.raises(grpc.RpcError) as exc_info:
        _at_leader(cluster, stub_of, lambda s: s.MakeDir(request, metadata=_as(BOB), timeout=5))

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert _dfsha_error(exc_info.value) == "AccessDeniedError"
    expected = ("error", "AccessDeniedError", "permiso denegado: falta w en /priv")
    # la clave del log lleva delante a quien llamó (ver ControlNodeServicer._commit)
    key = f"{BOB.username}:{op_id}"
    assert wait_for(lambda: all(cluster.replicated(i).applied_ops.get(key) == expected for i in range(3)))
    assert all(cluster.tree(i)._root.children["priv"].children == {} for i in range(3))
