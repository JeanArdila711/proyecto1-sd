"""Inventario por el ControlNode e inspector con capabilities (Hito 3, C3, T11)."""

import importlib.util
import os
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import grpc
import pytest

from conftest import FAST_RAFT_CONF, TEST_ENCRYPTION_KEY, free_port, wait_for
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.auth import Principal, issue_token
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node import block_store
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2 as cn
from dfsha.generated import control_node_pb2_grpc

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "inspect_cluster.py"
KEY = b"clave-de-capabilities-de-32-bytes!"
JWT_SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"
BLOCK = 5
CONTENT = b"doce bytes!!"
ORPHAN = "0f" * 16


def _as(principal):
    return (("authorization", f"Bearer {issue_token(JWT_SECRET, principal, 60, time.time())}"),)


ADMIN = Principal("admin", ("admin",), True)
ALICE = Principal("alice", ("alice",), False)


@pytest.fixture
def inspector(monkeypatch):
    """El módulo del inspector recién cargado. `opened` anota a qué direcciones abrió canal."""
    spec = importlib.util.spec_from_file_location("inspect_cluster_capabilities", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    channels = []
    module.opened = []

    def new_channel(address):
        module.opened.append(address)
        channels.append(grpc.insecure_channel(address))
        return channels[-1]

    module._new_channel = new_channel
    monkeypatch.setattr(module, "_can_prompt", lambda: False)
    yield module
    for channel in channels:
        channel.close()


@pytest.fixture
def cluster(tmp_path):
    """start(secure) -> SimpleNamespace(address, stub, servicer, datanodes, roots, servers, client)."""
    servers, rafts, channels, clients = [], [], [], []

    def start(secure=True):
        datanodes, roots, dn_servers = [], [], []
        for i in range(3):
            root = tmp_path / f"s{len(rafts)}-dn{i}"
            server, port = serve_data_node(
                root, "localhost", 0, TEST_ENCRYPTION_KEY, capability_key=KEY if secure else None
            )
            servers.append(server)
            dn_servers.append(server)
            datanodes.append(f"localhost:{port}")
            roots.append(root)
        extra = {"jwt_secret": JWT_SECRET, "admin_password": ADMIN_PASSWORD, "capability_key": KEY} if secure else {}
        server, port, raft = serve_control_node(
            datanodes,
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            raft_conf_overrides=FAST_RAFT_CONF,
            block_size_bytes=BLOCK,
            gc_interval_s=3600,
            **extra,
        )
        servers.insert(0, server)
        rafts.append(raft)
        assert wait_for(raft._isLeader)
        if secure:
            replicated = server._dfsha_control_servicer._replicated
            assert wait_for(lambda: replicated.tree.get_user("admin") is not None)
        address = f"localhost:{port}"
        channel = grpc.insecure_channel(address)
        channels.append(channel)

        def client(username=None, password=None):
            made = DistributedDFShaClient([address], rpc_timeout_s=2.0, failover_budget_s=4.0)
            clients.append(made)
            if username is not None:
                made.login(username, password)
            return made

        return SimpleNamespace(
            address=address,
            stub=control_node_pb2_grpc.ControlNodeServiceStub(channel),
            servicer=server._dfsha_control_servicer,
            datanodes=datanodes,
            roots=roots,
            servers=dn_servers,
            client=client,
        )

    yield start

    for made in clients:
        made.close()
    for channel in channels:
        channel.close()
    for server in servers:
        server.stop(grace=None)
    for raft in rafts:
        raft.destroy()


def _upload(client, tmp_path, path, data=CONTENT):
    source = tmp_path / f"f-{uuid.uuid4().hex}"
    source.write_bytes(data)
    client.upload(source, path)


def _seed_orphan(root, age_s=500):
    block_store.write_block(root, TEST_ENCRYPTION_KEY, ORPHAN, [b"huerfano"])
    past = time.time() - age_s
    os.utime(root / ORPHAN, (past, past))


def _rejected(call, code) -> grpc.RpcError:
    with pytest.raises(grpc.RpcError) as exc_info:
        call()
    assert exc_info.value.code() == code, exc_info.value.details()
    return exc_info.value


def _dfsha_error(exc) -> str | None:
    return dict(exc.trailing_metadata() or ()).get("dfsha-error")


# --- DataNodeInventory -------------------------------------------------------------------------


def test_the_inventory_as_admin_lists_the_blocks_with_size_and_age(cluster, tmp_path):
    system = cluster()
    _upload(system.client("admin", ADMIN_PASSWORD), tmp_path, "/a.bin")
    _seed_orphan(system.roots[0])

    response = system.stub.DataNodeInventory(
        cn.DataNodeInventoryRequest(address=system.datanodes[0]), timeout=10, metadata=_as(ADMIN)
    )

    by_id = {b.block_id: b for b in response.blocks}
    assert len(by_id) == 4  # tres bloques del archivo y el huérfano
    assert by_id[ORPHAN].size_bytes == len(b"huerfano")
    assert by_id[ORPHAN].age_s >= 499
    assert sorted(b.size_bytes for b in response.blocks if b.block_id != ORPHAN) == [2, 5, 5]


def test_the_inventory_is_only_for_admins(cluster):
    system = cluster()

    exc = _rejected(
        lambda: system.stub.DataNodeInventory(
            cn.DataNodeInventoryRequest(address=system.datanodes[0]), timeout=10, metadata=_as(ALICE)
        ),
        grpc.StatusCode.PERMISSION_DENIED,
    )

    assert _dfsha_error(exc) == "AccessDeniedError"
    assert exc.details() == "solo un admin puede ver el inventario de un DataNode"


def test_the_inventory_of_an_unknown_address_opens_no_channel(cluster):
    system = cluster()
    stranger = f"localhost:{free_port()}"

    exc = _rejected(
        lambda: system.stub.DataNodeInventory(
            cn.DataNodeInventoryRequest(address=stranger), timeout=10, metadata=_as(ADMIN)
        ),
        grpc.StatusCode.PERMISSION_DENIED,
    )

    assert _dfsha_error(exc) == "InvalidPathError"
    assert exc.details() == f"DataNode desconocido: {stranger!r}"
    assert stranger not in system.servicer._channels


def test_the_inventory_of_a_dead_datanode_is_unavailable(cluster):
    system = cluster()
    system.servers[1].stop(grace=None)

    exc = _rejected(
        lambda: system.stub.DataNodeInventory(
            cn.DataNodeInventoryRequest(address=system.datanodes[1]), timeout=10, metadata=_as(ADMIN)
        ),
        grpc.StatusCode.UNAVAILABLE,
    )

    assert exc.details().startswith(f"no se pudo listar {system.datanodes[1]}: ")


def test_the_inventory_without_a_token_is_unauthenticated(cluster):
    system = cluster()

    exc = _rejected(
        lambda: system.stub.DataNodeInventory(cn.DataNodeInventoryRequest(address=system.datanodes[0]), timeout=10),
        grpc.StatusCode.UNAUTHENTICATED,
    )

    assert _dfsha_error(exc) == "AuthError"


# --- inspector ---------------------------------------------------------------------------------


def test_replica_status_needs_the_capability_from_list_blocks(cluster, inspector, tmp_path):
    system = cluster()
    _upload(system.client("admin", ADMIN_PASSWORD), tmp_path, "/a.bin")
    inspector._credentials = ("admin", ADMIN_PASSWORD)
    _, stub = inspector.leader_stub([system.address])
    first = inspector.blocks_of(stub, "/a.bin")[0]

    assert inspector.replica_status(first.datanode_addresses[0], first.block_id, first.checksum, first.capability) == "ok"
    assert inspector.replica_status(first.datanode_addresses[0], first.block_id, first.checksum) == "PERMISSION_DENIED"


def test_huerfanos_counts_a_seeded_orphan_through_the_control_node(cluster, inspector, tmp_path, capsys):
    system = cluster()
    _upload(system.client("admin", ADMIN_PASSWORD), tmp_path, "/a.bin")
    _seed_orphan(system.roots[2])
    inspector._credentials = ("admin", ADMIN_PASSWORD)

    inspector.cmd_huerfanos(
        SimpleNamespace(control_nodes=system.address, datanodes=",".join(system.datanodes), gracia_s=100, limite=5)
    )

    out = capsys.readouterr().out
    assert "Total a borrar en el próximo ciclo del recolector: 1" in out
    assert f"a borrar {ORPHAN}" in out
    assert "no responde" not in out
    # el inspector no le habló a ningún DataNode: todo el inventario vino del ControlNode
    assert set(inspector.opened) == {system.address}


def test_bloques_verifies_a_private_file_of_another_user(cluster, inspector, tmp_path, capsys):
    system = cluster()
    admin = system.client("admin", ADMIN_PASSWORD)
    admin.create_user("alice", "clave-de-alice")
    alice = system.client("alice", "clave-de-alice")
    alice.make_dir("/alice")
    _upload(alice, tmp_path, "/alice/a.bin")
    alice.chmod("/alice/a.bin", 0o600)
    inspector._credentials = ("admin", ADMIN_PASSWORD)

    inspector.cmd_bloques(SimpleNamespace(control_nodes=system.address, ruta="/alice/a.bin", sin_verificar=False))

    out = capsys.readouterr().out
    assert out.count("=ok") == 9  # tres bloques, tres réplicas cada uno
    assert "PERMISSION_DENIED" not in out


def test_both_commands_work_as_before_without_authentication_or_capabilities(cluster, inspector, tmp_path, capsys):
    system = cluster(secure=False)
    _upload(system.client(), tmp_path, "/a.bin")
    _seed_orphan(system.roots[0])

    inspector.cmd_bloques(SimpleNamespace(control_nodes=system.address, ruta="/a.bin", sin_verificar=False))
    inspector.cmd_huerfanos(
        SimpleNamespace(control_nodes=system.address, datanodes=",".join(system.datanodes), gracia_s=100, limite=5)
    )

    out = capsys.readouterr().out
    assert out.count("=ok") == 9
    assert "Total a borrar en el próximo ciclo del recolector: 1" in out
