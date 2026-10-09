"""El cliente manda las capabilities (Hito 3, C3, T10): autenticación, permisos y
capabilities activos a la vez, con un ControlNode y tres DataNodes reales."""

import io
from types import SimpleNamespace

import grpc
import pytest

from conftest import FAST_RAFT_CONF, TEST_ENCRYPTION_KEY, free_port, wait_for, wait_until_datanode_excluded
from dfsha.client import distributed_client
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import AccessDeniedError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2 as cn

KEY = b"clave-de-capabilities-de-32-bytes!"
JWT_SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
ADMIN_PASSWORD = "clave-inicial-del-admin"
BLOCK = 5
CONTENT = bytes(range(23))  # cinco bloques: 5 + 5 + 5 + 5 + 3


@pytest.fixture
def cluster(tmp_path):
    """start(key) -> (dirección del ControlNode, servidores de los DataNodes, direcciones);
    login(dirección, usuario, clave) -> cliente con sesión y 4 transferencias en paralelo."""
    servers, rafts, clients = [], [], []

    def start(key=KEY, authenticated=True):
        datanodes, addresses = [], []
        for i in range(3):
            server, port = serve_data_node(
                tmp_path / f"s{len(rafts)}-dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY, capability_key=key
            )
            servers.append(server)
            datanodes.append(server)
            addresses.append(f"localhost:{port}")
        auth = {"jwt_secret": JWT_SECRET, "admin_password": ADMIN_PASSWORD} if authenticated else {}
        server, port, raft = serve_control_node(
            addresses,
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            block_size_bytes=BLOCK,
            raft_conf_overrides=FAST_RAFT_CONF,
            heartbeat_interval_s=0.05,
            datanode_dead_after_s=0.2,
            capability_key=key,
            **auth,
        )
        servers.insert(0, server)
        rafts.append(raft)
        assert wait_for(raft._isLeader)
        if authenticated:
            replicated = server._dfsha_control_servicer._replicated
            assert wait_for(lambda: replicated.tree.get_user("admin") is not None)
        return f"localhost:{port}", datanodes, addresses

    def login(address, username=None, password=None):
        made = DistributedDFShaClient([address], rpc_timeout_s=2.0, failover_budget_s=4.0, parallel_transfers=4)
        clients.append(made)
        if username is not None:
            made.login(username, password)
        return made

    yield start, login

    for made in clients:
        made.close()
    for server in servers:
        server.stop(grace=None)
    for raft in rafts:
        raft.destroy()


@pytest.fixture
def alice(cluster, tmp_path):
    """alice, con sesión, subió /alice/a.bin y /alice/b.bin."""
    start, login = cluster
    address, _, _ = start()
    admin = login(address, "admin", ADMIN_PASSWORD)
    admin.create_user("alice", "clave-de-alice")
    made = login(address, "alice", "clave-de-alice")
    made.make_dir("/alice")
    source = tmp_path / "origen.bin"
    source.write_bytes(CONTENT)
    made.upload(source, "/alice/a.bin")
    source.write_bytes(CONTENT[::-1])
    made.upload(source, "/alice/b.bin")
    return made


def _blocks(client, path):
    return list(client._call("ListBlocks", cn.ListBlocksRequest(path=path)).blocks)


def _recording_datanode_stubs(client, monkeypatch):
    """Anota (dirección, RPC, kwargs) de cada llamada a un DataNode."""
    calls = []
    real = client._datanode_stub

    def recording(address):
        stub = real(address)

        class _Stub:
            def ReadBlock(self, request, **kwargs):
                calls.append((address, "ReadBlock", kwargs))
                return stub.ReadBlock(request, **kwargs)

            def WriteBlock(self, chunks, **kwargs):
                calls.append((address, "WriteBlock", kwargs))
                return stub.WriteBlock(chunks, **kwargs)

        return _Stub()

    monkeypatch.setattr(client, "_datanode_stub", recording)
    return calls


# --- camino feliz ------------------------------------------------------------------------------


def test_upload_download_range_and_cow_writes_work_with_everything_on(alice, tmp_path):
    assert alice.download("/alice/a.bin", tmp_path / "bajado.bin") == len(CONTENT)
    assert (tmp_path / "bajado.bin").read_bytes() == CONTENT
    assert alice.read("/alice/a.bin", 7, 9) == CONTENT[7:16]

    # sobrescribe parte del bloque 1: el resto se lee del bloque viejo con old_capability
    assert alice.write("/alice/a.bin", 6, b"XY") == 2
    # completa el último bloque (3 bytes) y agrega uno nuevo
    assert alice.write("/alice/a.bin", 21, b"1234567") == 7

    expected = CONTENT[:6] + b"XY" + CONTENT[8:21] + b"1234567"
    assert alice.read("/alice/a.bin") == expected
    assert alice.download("/alice/a.bin", tmp_path / "de-nuevo.bin") == len(expected)
    assert (tmp_path / "de-nuevo.bin").read_bytes() == expected


def test_every_datanode_call_carries_a_capability(alice, monkeypatch, tmp_path):
    calls = _recording_datanode_stubs(alice, monkeypatch)
    source = tmp_path / "otro.bin"
    source.write_bytes(CONTENT)

    alice.upload(source, "/alice/c.bin")
    alice.download("/alice/c.bin", tmp_path / "c.bin")
    alice.write("/alice/c.bin", 6, b"XY")

    assert {name for _, name, _ in calls} == {"ReadBlock", "WriteBlock"}
    for _, _, kwargs in calls:
        ((key, capability),) = kwargs["metadata"]
        assert key == "dfsha-capability" and capability


# --- rechazos ----------------------------------------------------------------------------------


def test_the_capability_of_a_block_of_one_file_does_not_read_a_block_of_another(alice):
    a, b = _blocks(alice, "/alice/a.bin")[0], _blocks(alice, "/alice/b.bin")[0]
    forged = SimpleNamespace(
        block_id=b.block_id, datanode_addresses=list(b.datanode_addresses), size_bytes=b.size_bytes, capability=a.capability
    )

    with pytest.raises(grpc.RpcError) as exc_info:
        alice._read_block_with_failover(forged, io.BytesIO())

    translated = distributed_client._translate(exc_info.value)
    assert isinstance(translated, AccessDeniedError)
    assert str(translated) == "la capability no autoriza esta operación"


def test_a_replica_that_rejects_the_capability_does_not_trigger_failover(alice, monkeypatch):
    a, b = _blocks(alice, "/alice/a.bin")[0], _blocks(alice, "/alice/b.bin")[0]
    assert len(b.datanode_addresses) >= 2
    forged = SimpleNamespace(
        block_id=b.block_id, datanode_addresses=list(b.datanode_addresses), size_bytes=b.size_bytes, capability=a.capability
    )
    calls = _recording_datanode_stubs(alice, monkeypatch)

    with pytest.raises(grpc.RpcError) as exc_info:
        alice._read_block_with_failover(forged, io.BytesIO())

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    # la capability vale lo mismo en todas las réplicas: probar otra no cambia nada
    assert [address for address, _, _ in calls] == [b.datanode_addresses[0]]


def test_a_c2_client_without_capabilities_is_denied_by_datanodes_with_key(alice, monkeypatch, tmp_path):
    # Un cliente de C2 no conoce el campo `capability`: sus llamadas a los DataNodes salen
    # sin metadata.
    monkeypatch.setattr(distributed_client, "capability_kwargs", lambda *args, **kwargs: {})
    source = tmp_path / "nuevo.bin"
    source.write_bytes(CONTENT)

    with pytest.raises(AccessDeniedError) as downloading:
        alice.download("/alice/a.bin", tmp_path / "bajado.bin")
    with pytest.raises(AccessDeniedError) as uploading:
        alice.upload(source, "/alice/nuevo.bin")

    assert str(downloading.value) == "falta la capability"
    assert str(uploading.value) == "falta la capability"
    assert not (tmp_path / "bajado.bin").exists()
    assert [e.name for e in alice.list_dir("/alice")] == ["a.bin", "b.bin"]


def test_blocks_without_the_capability_attribute_are_denied(alice):
    # Bloques como los arma un cliente de C2 o un test: sin el atributo `capability`.
    a = _blocks(alice, "/alice/a.bin")[0]
    old_style = SimpleNamespace(block_id=a.block_id, datanode_addresses=list(a.datanode_addresses), size_bytes=a.size_bytes)

    with pytest.raises(grpc.RpcError) as exc_info:
        alice._read_block_with_failover(old_style, io.BytesIO())

    translated = distributed_client._translate(exc_info.value)
    assert isinstance(translated, AccessDeniedError)
    assert str(translated) == "falta la capability"


# --- sin clave ---------------------------------------------------------------------------------


def test_against_a_cluster_without_key_the_client_sends_no_metadata(cluster, monkeypatch, tmp_path):
    start, login = cluster
    address, _, _ = start(key=None, authenticated=False)
    client = login(address)
    calls = _recording_datanode_stubs(client, monkeypatch)
    source = tmp_path / "origen.bin"
    source.write_bytes(CONTENT)

    client.upload(source, "/a.bin")
    assert client.read("/a.bin", 3, 10) == CONTENT[3:13]
    client.write("/a.bin", 6, b"XY")

    assert calls
    assert all("metadata" not in kwargs for _, _, kwargs in calls)


# --- conftest.wait_until_datanode_excluded -----------------------------------------------------


def test_the_datanode_probe_works_for_a_common_user_with_everything_on(cluster):
    start, login = cluster
    address, datanodes, addresses = start()
    admin = login(address, "admin", ADMIN_PASSWORD)
    admin.create_user("alice", "clave-de-alice")
    alice = login(address, "alice", "clave-de-alice")

    datanodes[2].stop(grace=None)

    channel = grpc.insecure_channel(address)
    try:
        assert wait_until_datanode_excluded(channel, addresses[2], metadata=alice._auth_metadata)
    finally:
        channel.close()
