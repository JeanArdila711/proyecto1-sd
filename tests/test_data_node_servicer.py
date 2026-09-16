from pathlib import Path

import grpc
import pytest

from dfsha.common.exceptions import BlockCorruptedError, BlockNotFoundError
from dfsha.data_node.main import serve
from dfsha.generated import data_node_pb2, data_node_pb2_grpc

BLOCK_ID = "1234567890abcdef1234567890abcdef"  # formato real: uuid4().hex (32 hex)


@pytest.fixture
def datanode_stub(tmp_path):
    server, port = serve(tmp_path, "localhost", 0)
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = data_node_pb2_grpc.DataNodeServiceStub(channel)
    yield stub
    channel.close()
    server.stop(grace=None)


def _write(stub, block_id: str, data: bytes):
    def chunks():
        yield data_node_pb2.WriteBlockChunk(block_id=block_id)
        yield data_node_pb2.WriteBlockChunk(data=data)

    return stub.WriteBlock(chunks())


def test_write_then_read_roundtrip(datanode_stub):
    _write(datanode_stub, BLOCK_ID, b"contenido de prueba")

    chunks = list(datanode_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=BLOCK_ID)))
    result = b"".join(c.data for c in chunks)

    assert result == b"contenido de prueba"


def test_read_missing_block_raises_not_found(datanode_stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        list(datanode_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id="no-existe")))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_delete_then_read_raises_not_found(datanode_stub):
    _write(datanode_stub, BLOCK_ID, b"data")
    datanode_stub.DeleteBlock(data_node_pb2.DeleteBlockRequest(block_id=BLOCK_ID))

    with pytest.raises(grpc.RpcError) as exc_info:
        list(datanode_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=BLOCK_ID)))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_corrupted_block_raises_data_loss(datanode_stub, tmp_path):
    _write(datanode_stub, BLOCK_ID, b"data original")
    (tmp_path / BLOCK_ID).write_bytes(b"datos corruptos distintos")

    with pytest.raises(grpc.RpcError) as exc_info:
        list(datanode_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=BLOCK_ID)))
    assert exc_info.value.code() == grpc.StatusCode.DATA_LOSS


def test_write_empty_stream_returns_invalid_argument(datanode_stub):
    def empty_chunks():
        return iter(())

    with pytest.raises(grpc.RpcError) as exc_info:
        datanode_stub.WriteBlock(empty_chunks())
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
