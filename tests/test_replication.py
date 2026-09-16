import os

import grpc
import pytest

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import DFShaError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.control_node.servicer import ControlNodeServicer
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, data_node_pb2


@pytest.fixture
def make_cluster(tmp_path):
    """Levanta N DataNodes + un ControlNode. Devuelve el cliente y, por cada
    DataNode, su dirección, su servidor y su raíz en disco."""
    servers = []
    clients = []

    def _make(num_datanodes, replication_factor=3, block_size_bytes=5):
        datanodes = []
        for i in range(num_datanodes):
            root = tmp_path / f"dn{i}"
            server, port = serve_data_node(root, "localhost", 0)
            servers.append(server)
            datanodes.append({"address": f"localhost:{port}", "server": server, "root": root})

        cn_server, cn_port = serve_control_node(
            [dn["address"] for dn in datanodes],
            "localhost",
            0,
            block_size_bytes=block_size_bytes,
            replication_factor=replication_factor,
        )
        servers.append(cn_server)
        client = DistributedDFShaClient(f"localhost:{cn_port}")
        clients.append(client)
        return client, datanodes

    yield _make

    for client in clients:
        client.close()
    for server in servers:
        server.stop(grace=None)


def _block_files(root):
    """block_ids guardados en una raíz de DataNode (sin .sha256 ni temporales)."""
    if not root.exists():
        return set()
    return {p.name for p in root.iterdir() if len(p.name) == 32}


def _blocks_of(client, path):
    return list(client._control_stub.ListBlocks(control_node_pb2.ListBlocksRequest(path=path)).blocks)


def _upload(client, tmp_path, remote, content):
    local = tmp_path / f"subida-{os.urandom(4).hex()}"
    local.write_bytes(content)
    client.upload(local, remote)


def test_pipeline_writes_block_to_all_three_replicas_with_same_checksum(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    _upload(client, tmp_path, "/a.txt", b"hola")

    [block] = _blocks_of(client, "/a.txt")
    assert len(block.datanode_addresses) == 3
    for dn in datanodes:
        assert (dn["root"] / block.block_id).read_bytes() == b"hola"
        assert (dn["root"] / f"{block.block_id}.sha256").read_text() == block.checksum


def test_download_fails_over_when_first_replica_is_down(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    content = bytes(range(40))  # 8 bloques de 5 bytes
    _upload(client, tmp_path, "/a.bin", content)

    # tirar el DataNode que es primera réplica del primer bloque
    first = _blocks_of(client, "/a.bin")[0].datanode_addresses[0]
    next(dn for dn in datanodes if dn["address"] == first)["server"].stop(grace=None)

    destino = tmp_path / "salida.bin"
    client.download("/a.bin", destino)
    assert destino.read_bytes() == content


def test_download_fails_over_when_first_replica_is_corrupted(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    _upload(client, tmp_path, "/a.txt", b"hola")

    [block] = _blocks_of(client, "/a.txt")
    first = next(dn for dn in datanodes if dn["address"] == block.datanode_addresses[0])
    (first["root"] / block.block_id).write_bytes(b"PODRIDO")

    destino = tmp_path / "salida.txt"
    client.download("/a.txt", destino)
    assert destino.read_bytes() == b"hola"


class _MidStreamError(grpc.RpcError):
    pass


def test_download_discards_partial_bytes_when_replica_dies_mid_block(make_cluster, tmp_path, monkeypatch):
    """Una réplica que se cae DESPUÉS de mandar parte del bloque deja bytes ya
    escritos en el archivo local. El failover tiene que descartarlos antes de
    reintentar, o el resultado queda con basura intercalada."""
    client, _ = make_cluster(3)
    content = bytes(range(15))  # 3 bloques de 5
    _upload(client, tmp_path, "/a.bin", content)

    # la primera réplica del bloque del medio manda 3 bytes y se cae
    middle = _blocks_of(client, "/a.bin")[1]
    dying_address = middle.datanode_addresses[0]

    class _DyingStub:
        def ReadBlock(self, request):
            if request.block_id != middle.block_id:
                yield from real_stub(dying_address).ReadBlock(request)
                return
            yield data_node_pb2.ReadBlockChunk(data=b"XYZ")
            raise _MidStreamError()

    real_stub = client._datanode_stub
    monkeypatch.setattr(
        client,
        "_datanode_stub",
        lambda address: _DyingStub() if address == dying_address else real_stub(address),
    )

    destino = tmp_path / "salida.bin"
    client.download("/a.bin", destino)
    assert destino.read_bytes() == content


def test_download_fails_when_every_replica_is_down(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    _upload(client, tmp_path, "/a.txt", b"hola")
    for dn in datanodes:
        dn["server"].stop(grace=None)

    destino = tmp_path / "salida.txt"
    with pytest.raises(DFShaError):
        client.download("/a.txt", destino)
    assert not destino.exists()
    assert not list(tmp_path.glob("salida.txt.part-*"))


def test_fewer_datanodes_than_factor_replicates_to_all_available(make_cluster, tmp_path):
    client, datanodes = make_cluster(2, replication_factor=3)
    _upload(client, tmp_path, "/a.txt", b"hola")

    [block] = _blocks_of(client, "/a.txt")
    assert len(block.datanode_addresses) == 2
    for dn in datanodes:
        assert block.block_id in _block_files(dn["root"])


def test_failed_replica_in_pipeline_aborts_upload(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    # el primer bloque de un ControlNode recién arrancado va a dn0 -> dn1 -> dn2
    datanodes[1]["server"].stop(grace=None)

    with pytest.raises(DFShaError):
        _upload(client, tmp_path, "/a.txt", b"hola")

    assert [e.name for e in client.list_dir("/")] == []
    # quórum 3 de 3: la cabeza del pipeline no se queda con una copia huérfana
    assert _block_files(datanodes[0]["root"]) == set()


def test_round_robin_spreads_blocks_across_all_datanodes(make_cluster, tmp_path):
    client, datanodes = make_cluster(4, replication_factor=3)
    _upload(client, tmp_path, "/a.bin", bytes(20))  # 4 bloques

    blocks = _blocks_of(client, "/a.bin")
    used = {address for b in blocks for address in b.datanode_addresses}
    assert used == {dn["address"] for dn in datanodes}
    # las 3 réplicas de un mismo bloque son siempre nodos distintos
    assert all(len(set(b.datanode_addresses)) == 3 for b in blocks)
    # y no todos los bloques arrancan el pipeline en el mismo nodo
    assert len({b.datanode_addresses[0] for b in blocks}) > 1


def test_remove_deletes_block_from_every_replica(make_cluster, tmp_path):
    client, datanodes = make_cluster(3)
    _upload(client, tmp_path, "/a.txt", b"hola")

    client.remove("/a.txt")
    for dn in datanodes:
        assert _block_files(dn["root"]) == set()


def test_pipeline_with_block_larger_than_forwarding_queue(make_cluster, tmp_path):
    """Un bloque de varios chunks llena la cola acotada del forwarding: tiene que
    aplicar backpressure y terminar, no colgarse."""
    client, datanodes = make_cluster(3, block_size_bytes=16 * 1024 * 1024)
    content = os.urandom(6 * 1024 * 1024 + 123)  # 7 chunks de 1 MiB, cola de 4
    _upload(client, tmp_path, "/grande.bin", content)

    [block] = _blocks_of(client, "/grande.bin")
    for dn in datanodes:
        assert (dn["root"] / block.block_id).read_bytes() == content

    destino = tmp_path / "salida.bin"
    client.download("/grande.bin", destino)
    assert destino.read_bytes() == content


def test_servicer_rejects_invalid_configuration():
    with pytest.raises(ValueError):
        ControlNodeServicer([], block_size_bytes=5)
    with pytest.raises(ValueError):
        ControlNodeServicer(["localhost:1"], block_size_bytes=5, replication_factor=0)
