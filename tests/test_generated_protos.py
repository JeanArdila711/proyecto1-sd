def test_control_node_messages_build():
    from dfsha.generated import control_node_pb2

    request = control_node_pb2.ListDirRequest(path="/")
    assert request.path == "/"

    location = control_node_pb2.BlockLocation(block_id="b1", datanode_address="localhost:1", size_bytes=10)
    assert location.size_bytes == 10


def test_data_node_messages_build():
    from dfsha.generated import data_node_pb2

    chunk = data_node_pb2.WriteBlockChunk(block_id="b1")
    assert chunk.block_id == "b1"

    data_chunk = data_node_pb2.WriteBlockChunk(data=b"hola")
    assert data_chunk.data == b"hola"


def test_dfsha_v1_still_generates():
    from dfsha.generated import dfsha_pb2

    request = dfsha_pb2.ListDirRequest(path="/")
    assert request.path == "/"
