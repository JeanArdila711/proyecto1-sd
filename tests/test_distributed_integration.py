import grpc
import pytest

from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import BlockCorruptedError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc


@pytest.fixture
def cluster(tmp_path):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0)
    datanode_address = f"localhost:{dn_port}"

    cn_server, cn_port = serve_control_node(datanode_address, "localhost", 0, block_size_bytes=5)
    client = DistributedDFShaClient(f"localhost:{cn_port}")

    yield client, datanode_address, dn_root

    client.close()
    cn_server.stop(grace=None)
    dn_server.stop(grace=None)


def test_single_block_roundtrip(cluster, tmp_path):
    client, _, _ = cluster
    local = tmp_path / "chico.txt"
    local.write_bytes(b"hola")
    client.upload(local, "/chico.txt")

    destino = tmp_path / "salida.txt"
    client.download("/chico.txt", destino)

    assert destino.read_bytes() == b"hola"


def test_multi_block_roundtrip(cluster, tmp_path):
    client, _, _ = cluster
    contenido = bytes(range(50)) * 3  # 150 bytes, block_size_bytes=5 -> 30 bloques
    local = tmp_path / "grande.bin"
    local.write_bytes(contenido)
    client.upload(local, "/grande.bin")

    destino = tmp_path / "salida.bin"
    client.download("/grande.bin", destino)

    assert destino.read_bytes() == contenido


def test_abort_mid_upload_leaves_no_visible_file(cluster, tmp_path, monkeypatch):
    client, _, _ = cluster
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"0123456789")  # 10 bytes -> 2 bloques de 5

    original_write_block = client._write_block
    call_count = {"n": 0}

    def fail_on_second_block(block, fh):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise OSError("disco lleno simulado")
        return original_write_block(block, fh)

    monkeypatch.setattr(client, "_write_block", fail_on_second_block)

    with pytest.raises(OSError):
        client.upload(local, "/archivo.txt")

    assert client.list_dir("/") == []


def test_corruption_detected_on_download(cluster, tmp_path):
    client, _, dn_root = cluster
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"contenido valido")
    client.upload(local, "/archivo.txt")

    block_file = next(f for f in dn_root.glob("*") if not f.name.endswith(".sha256"))
    block_file.write_bytes(b"XXXXXXXXXXXXXXXXX")

    with pytest.raises(BlockCorruptedError):
        client.download("/archivo.txt", tmp_path / "salida.txt")


def test_remove_cleans_up_blocks_on_datanode(cluster, tmp_path):
    client, datanode_address, _ = cluster
    local = tmp_path / "archivo.txt"
    local.write_bytes(b"data")
    client.upload(local, "/archivo.txt")

    # capturar los block_ids antes de borrar
    cn_stub = control_node_pb2_grpc.ControlNodeServiceStub(client._control_channel)
    blocks = cn_stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/archivo.txt")).blocks

    client.remove("/archivo.txt")

    dn_channel = grpc.insecure_channel(datanode_address)
    dn_stub = data_node_pb2_grpc.DataNodeServiceStub(dn_channel)
    for block in blocks:
        with pytest.raises(grpc.RpcError) as exc_info:
            list(dn_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=block.block_id)))
        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
    dn_channel.close()
