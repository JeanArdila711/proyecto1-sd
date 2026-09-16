"""Clúster real de 3 ControlNodes con Raft (en el mismo proceso, puertos reales)."""

import socket
import uuid

import grpc
import pytest

from conftest import FAST_RAFT_CONF, free_port, wait_for
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.control_node.main import build_raft_conf
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, control_node_pb2_grpc

# un nodo que reinicia tiene que reconectarse rápido con sus peers (default: 5 s)
CLUSTER_RAFT_CONF = {**FAST_RAFT_CONF, "connectionRetryTime": 0.1}


def _port_is_free(port):
    # con SO_REUSEADDR, igual que bindea pysyncobj: sin eso las conexiones viejas en
    # TIME_WAIT hacen parecer ocupado un puerto que ya nadie escucha
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("localhost", port))
        except OSError:
            return False
        return True


class RaftCluster:
    def __init__(self, tmp_path, datanode_addresses, persistent, raft_conf=None):
        self._tmp_path = tmp_path
        self._raft_conf = raft_conf or CLUSTER_RAFT_CONF
        self._datanode_addresses = datanode_addresses
        self._persistent = persistent
        self.raft_addresses = [f"localhost:{free_port()}" for _ in range(3)]
        self.grpc_ports = [free_port() for _ in range(3)]
        self.nodes = [None, None, None]  # (server, raft) o None si está caído

    @property
    def grpc_addresses(self):
        return [f"localhost:{p}" for p in self.grpc_ports]

    def start(self, i):
        server, port, raft = serve_control_node(
            self._datanode_addresses,
            "localhost",
            self.grpc_ports[i],
            raft_self=self.raft_addresses[i],
            raft_peers=[a for j, a in enumerate(self.raft_addresses) if j != i],
            data_dir=self._tmp_path / f"cn{i}" if self._persistent else None,
            block_size_bytes=5,
            raft_conf_overrides=self._raft_conf,
            commit_timeout_s=1.0,
        )
        assert port == self.grpc_ports[i]
        self.nodes[i] = (server, raft)

    def kill(self, i):
        server, raft = self.nodes[i]
        self.nodes[i] = None
        server.stop(grace=None)
        raft.destroy()
        # destroy() es asíncrono: esperar a que suelte el puerto de Raft
        raft_port = int(self.raft_addresses[i].rsplit(":", 1)[1])
        assert wait_for(lambda: _port_is_free(raft_port)), "Raft no liberó el puerto"

    def wait_leader(self, timeout=5.0):
        def single_leader():
            leaders = [i for i, n in enumerate(self.nodes) if n is not None and n[1]._isLeader()]
            return leaders[0] if len(leaders) == 1 else None

        assert wait_for(lambda: single_leader() is not None, timeout), "no hay un líder único"
        return single_leader()

    def client(self):
        return DistributedDFShaClient(self.grpc_addresses, rpc_timeout_s=2.0, failover_budget_s=10.0)

    def stub(self, i):
        channel = grpc.insecure_channel(self.grpc_addresses[i])
        return channel, control_node_pb2_grpc.ControlNodeServiceStub(channel)

    def close(self):
        for i, node in enumerate(self.nodes):
            if node is not None:
                self.kill(i)


@pytest.fixture
def datanodes(tmp_path):
    servers, addresses = [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    yield addresses
    for server in servers:
        server.stop(grace=None)


@pytest.fixture
def make_cluster(tmp_path, datanodes):
    clusters, clients = [], []

    def _make(persistent=False, raft_conf=None):
        cluster = RaftCluster(tmp_path, datanodes, persistent, raft_conf)
        for i in range(3):
            cluster.start(i)
        cluster.wait_leader()
        clusters.append(cluster)
        client = cluster.client()
        clients.append(client)
        return cluster, client

    yield _make

    for client in clients:
        client.close()
    for cluster in clusters:
        cluster.close()


def _write(tmp_path, content):
    local = tmp_path / f"subida-{uuid.uuid4().hex}"
    local.write_bytes(content)
    return local


def test_follower_rejects_reads_and_writes_with_unavailable(make_cluster):
    cluster, _ = make_cluster()
    leader = cluster.wait_leader()
    follower = next(i for i in range(3) if i != leader)
    channel, stub = cluster.stub(follower)
    try:
        for call in (
            lambda: stub.ListDir(control_node_pb2.ListDirRequest(path="/")),
            lambda: stub.MakeDir(control_node_pb2.MakeDirRequest(path="/x", op_id=uuid.uuid4().hex)),
        ):
            with pytest.raises(grpc.RpcError) as exc_info:
                call()
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        channel.close()


def test_client_keeps_working_after_leader_crash(make_cluster, tmp_path):
    cluster, client = make_cluster()
    content = bytes(range(23))  # 5 bloques
    client.make_dir("/docs")
    client.upload(_write(tmp_path, content), "/docs/a.bin")

    cluster.kill(cluster.wait_leader())
    cluster.wait_leader()

    # el llamador no se entera del failover: todo sigue funcionando
    assert [e.name for e in client.list_dir("/docs")] == ["a.bin"]
    destino = tmp_path / "salida.bin"
    client.download("/docs/a.bin", destino)
    assert destino.read_bytes() == content
    client.make_dir("/despues")
    client.upload(_write(tmp_path, b"nuevo"), "/despues/b.txt")
    assert [e.name for e in client.list_dir("/")] == ["despues", "docs"]


def test_upload_survives_leader_crash_before_complete(make_cluster, tmp_path):
    """La subida pendiente y sus bloques confirmados están replicados: si el líder
    muere justo antes de CompleteUpload, el nuevo líder la puede completar."""
    cluster, client = make_cluster()
    content = b"0123456789abc"  # 3 bloques
    real_call = client._call
    killed = []

    def call_killing_leader_before_complete(rpc_name, request):
        if rpc_name == "CompleteUpload" and not killed:
            leader = cluster.wait_leader()
            cluster.kill(leader)
            killed.append(leader)
        return real_call(rpc_name, request)

    client._call = call_killing_leader_before_complete
    client.upload(_write(tmp_path, content), "/a.bin")

    assert killed, "el test no llegó a matar al líder"
    destino = tmp_path / "salida.bin"
    client.download("/a.bin", destino)
    assert destino.read_bytes() == content


def test_leader_without_majority_does_not_serve_stale_reads(make_cluster):
    """Un líder que perdió a la mayoría se sigue creyendo líder un rato
    (leaderFallbackTimeout). Si leyera su árbol local, podría servir datos viejos;
    con la barrera de lectura no logra confirmar y responde UNAVAILABLE.

    Es la misma garantía que evita la ventana de un líder recién elegido que todavía
    no aplicó las últimas entradas confirmadas (bug encontrado con la CPU cargada:
    `ls` no mostraba un archivo cuya subida ya se había confirmado)."""
    cluster, client = make_cluster(raft_conf={**CLUSTER_RAFT_CONF, "leaderFallbackTimeout": 30.0})
    client.make_dir("/docs")
    leader = cluster.wait_leader()
    for i in range(3):
        if i != leader:
            cluster.kill(i)

    channel, stub = cluster.stub(leader)
    try:
        assert cluster.nodes[leader][1]._isLeader(), "el test necesita un líder que no sabe que perdió la mayoría"
        for call in (
            lambda: stub.ListDir(control_node_pb2.ListDirRequest(path="/")),
            lambda: stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/docs")),
        ):
            with pytest.raises(grpc.RpcError) as exc_info:
                call()
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
    finally:
        channel.close()


def test_retry_with_same_op_id_after_commit_is_not_an_error(make_cluster):
    """Simula una respuesta perdida: la operación se confirmó, el cliente no se
    enteró y reintenta. Con el mismo op_id no puede fallar por PathExistsError."""
    cluster, _ = make_cluster()
    channel, stub = cluster.stub(cluster.wait_leader())
    try:
        request = control_node_pb2.MakeDirRequest(path="/docs", op_id=uuid.uuid4().hex)
        stub.MakeDir(request)
        stub.MakeDir(request)  # mismo op_id: sin error

        with pytest.raises(grpc.RpcError) as exc_info:
            stub.MakeDir(control_node_pb2.MakeDirRequest(path="/docs", op_id=uuid.uuid4().hex))
        assert exc_info.value.code() == grpc.StatusCode.ALREADY_EXISTS
    finally:
        channel.close()


def test_mutation_without_op_id_is_rejected(make_cluster):
    cluster, _ = make_cluster()
    channel, stub = cluster.stub(cluster.wait_leader())
    try:
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.MakeDir(control_node_pb2.MakeDirRequest(path="/docs"))
        assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert list(stub.ListDir(control_node_pb2.ListDirRequest(path="/")).entries) == []
    finally:
        channel.close()


def test_full_cluster_restart_recovers_state_from_snapshot_and_journal(make_cluster, tmp_path):
    """Snapshot forzado (con useFork=False y el árbol serializable) + entradas
    posteriores que solo están en el journal; después se apagan los 3 nodos."""
    cluster, client = make_cluster(persistent=True)
    content = bytes(range(12))
    client.make_dir("/antes")
    client.upload(_write(tmp_path, content), "/antes/a.bin")

    for _, raft in cluster.nodes:
        raft.forceLogCompaction()
    dumps = [tmp_path / f"cn{i}" / "raft.dump" for i in range(3)]
    assert wait_for(lambda: all(d.exists() and d.stat().st_size > 0 for d in dumps)), "no se escribió el snapshot"

    client.make_dir("/despues")  # solo en el journal, posterior al snapshot

    for i in range(3):
        cluster.kill(i)
    for i in range(3):
        cluster.start(i)
    cluster.wait_leader()

    assert [e.name for e in client.list_dir("/")] == ["antes", "despues"]
    destino = tmp_path / "salida.bin"
    client.download("/antes/a.bin", destino)
    assert destino.read_bytes() == content


def test_log_compaction_never_forks(tmp_path):
    """Con useFork=True, pysyncobj hace fork() del proceso para escribir el snapshot.
    El proceso tiene los hilos de gRPC y de Raft corriendo, y Python advierte que
    fork() ahí puede dejar al hijo bloqueado: el snapshot no se escribiría nunca y
    la compactación se frenaría sin error visible. No se reproduce de forma
    determinística, así que se protege la configuración, incluso contra overrides."""
    assert build_raft_conf(tmp_path / "cn").useFork is False
    assert build_raft_conf(None, {"useFork": True}).useFork is False
