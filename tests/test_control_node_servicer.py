import grpc
import pytest

from dfsha.control_node.main import serve as serve_control_node
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc


@pytest.fixture
def cluster(tmp_path):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0)
    datanode_address = f"localhost:{dn_port}"

    cn_server, cn_port = serve_control_node([datanode_address], "localhost", 0, block_size_bytes=5)
    channel = grpc.insecure_channel(f"localhost:{cn_port}")
    stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)

    yield stub, datanode_address

    channel.close()
    cn_server.stop(grace=None)
    dn_server.stop(grace=None)


def test_mkdir_then_ls(cluster):
    stub, _ = cluster
    stub.MakeDir(control_node_pb2.MakeDirRequest(path="/documentos"))

    response = stub.ListDir(control_node_pb2.ListDirRequest(path="/"))

    assert [e.name for e in response.entries] == ["documentos"]
    assert response.entries[0].is_dir is True


def test_begin_upload_reserves_blocks_by_configured_size(cluster):
    stub, datanode_address = cluster
    response = stub.BeginUpload(
        control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=12)
    )

    # block_size_bytes=5 en el fixture -> ceil(12/5) = 3 bloques: 5, 5, 2
    assert [b.size_bytes for b in response.blocks] == [5, 5, 2]
    assert all(list(b.datanode_addresses) == [datanode_address] for b in response.blocks)


def test_full_upload_flow_makes_file_visible(cluster):
    stub, datanode_address = cluster
    begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=3))
    block = begin.blocks[0]

    stub.ConfirmBlock(
        control_node_pb2.ConfirmBlockRequest(
            path="/archivo.txt", block_id=block.block_id, checksum="abc", size_bytes=3
        )
    )
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/archivo.txt"))

    entries = stub.ListDir(control_node_pb2.ListDirRequest(path="/")).entries
    assert [e.name for e in entries] == ["archivo.txt"]

    blocks = stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/archivo.txt")).blocks
    assert len(blocks) == 1
    assert blocks[0].checksum == "abc"


def test_remove_file_deletes_blocks_from_datanode(cluster):
    stub, datanode_address = cluster
    dn_channel = grpc.insecure_channel(datanode_address)
    dn_stub_for_check = data_node_pb2_grpc.DataNodeServiceStub(dn_channel)

    begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=3))
    block = begin.blocks[0]

    def chunks():
        yield data_node_pb2.WriteBlockChunk(header=data_node_pb2.WriteBlockHeader(block_id=block.block_id))
        yield data_node_pb2.WriteBlockChunk(data=b"abc")

    write_response = dn_stub_for_check.WriteBlock(chunks())
    stub.ConfirmBlock(
        control_node_pb2.ConfirmBlockRequest(
            path="/archivo.txt",
            block_id=block.block_id,
            checksum=write_response.checksum,
            size_bytes=write_response.bytes_written,
        )
    )
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/archivo.txt"))

    stub.Remove(control_node_pb2.RemoveRequest(path="/archivo.txt"))

    with pytest.raises(grpc.RpcError) as exc_info:
        list(dn_stub_for_check.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=block.block_id)))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    dn_channel.close()
