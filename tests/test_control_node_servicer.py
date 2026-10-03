import uuid

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2, control_node_pb2_grpc, data_node_pb2, data_node_pb2_grpc


def _op():
    return uuid.uuid4().hex


@pytest.fixture
def cluster(tmp_path, start_control_node):
    dn_root = tmp_path / "datanode"
    dn_server, dn_port = serve_data_node(dn_root, "localhost", 0, TEST_ENCRYPTION_KEY)
    datanode_address = f"localhost:{dn_port}"

    # D-P2: las pruebas históricas de un único DataNode optan por mínimo 1.
    channel = grpc.insecure_channel(start_control_node([datanode_address], block_size_bytes=5, min_write_replicas=1))
    stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)

    yield stub, datanode_address

    channel.close()
    dn_server.stop(grace=None)


def test_mkdir_then_ls(cluster):
    stub, _ = cluster
    stub.MakeDir(control_node_pb2.MakeDirRequest(path="/documentos", op_id=_op()))

    response = stub.ListDir(control_node_pb2.ListDirRequest(path="/"))

    assert [e.name for e in response.entries] == ["documentos"]
    assert response.entries[0].is_dir is True


def test_begin_upload_reserves_blocks_by_configured_size(cluster):
    stub, datanode_address = cluster
    response = stub.BeginUpload(
        control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=12, op_id=_op())
    )

    # block_size_bytes=5 en el fixture -> ceil(12/5) = 3 bloques: 5, 5, 2
    assert [b.size_bytes for b in response.blocks] == [5, 5, 2]
    assert all(list(b.datanode_addresses) == [datanode_address] for b in response.blocks)


def test_full_upload_flow_makes_file_visible(cluster):
    stub, datanode_address = cluster
    begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=3, op_id=_op()))
    block = begin.blocks[0]

    stub.ConfirmBlock(
        control_node_pb2.ConfirmBlockRequest(
            op_id=_op(),
            path="/archivo.txt", block_id=block.block_id, checksum="abc", size_bytes=3
        )
    )
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/archivo.txt", op_id=_op()))

    entries = stub.ListDir(control_node_pb2.ListDirRequest(path="/")).entries
    assert [e.name for e in entries] == ["archivo.txt"]

    blocks = stub.ListBlocks(control_node_pb2.ListBlocksRequest(path="/archivo.txt")).blocks
    assert len(blocks) == 1
    assert blocks[0].checksum == "abc"


def test_remove_file_deletes_blocks_from_datanode(cluster):
    stub, datanode_address = cluster
    dn_channel = grpc.insecure_channel(datanode_address)
    dn_stub_for_check = data_node_pb2_grpc.DataNodeServiceStub(dn_channel)

    begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=3, op_id=_op()))
    block = begin.blocks[0]

    def chunks():
        yield data_node_pb2.WriteBlockChunk(header=data_node_pb2.WriteBlockHeader(block_id=block.block_id))
        yield data_node_pb2.WriteBlockChunk(data=b"abc")

    write_response = dn_stub_for_check.WriteBlock(chunks())
    stub.ConfirmBlock(
        control_node_pb2.ConfirmBlockRequest(
            op_id=_op(),
            path="/archivo.txt",
            block_id=block.block_id,
            checksum=write_response.checksum,
            size_bytes=write_response.bytes_written,
        )
    )
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/archivo.txt", op_id=_op()))

    stub.Remove(control_node_pb2.RemoveRequest(path="/archivo.txt", op_id=_op()))

    with pytest.raises(grpc.RpcError) as exc_info:
        list(dn_stub_for_check.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=block.block_id)))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND

    dn_channel.close()


def test_begin_upload_retry_with_same_op_id_returns_original_blocks(cluster):
    """Reintento de un BeginUpload ya confirmado (respuesta perdida en un failover):
    el servicer propone block_ids nuevos, pero tiene que responder con los que
    quedaron guardados, o el cliente escribiría bloques que nadie conoce."""
    stub, _ = cluster
    request = control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=12, op_id=_op())

    first = stub.BeginUpload(request)
    retry = stub.BeginUpload(request)

    assert [b.block_id for b in retry.blocks] == [b.block_id for b in first.blocks]
    assert [list(b.datanode_addresses) for b in retry.blocks] == [
        list(b.datanode_addresses) for b in first.blocks
    ]
    assert [b.size_bytes for b in retry.blocks] == [5, 5, 2]



def test_abandoned_upload_frees_the_name_after_lease_and_deletes_its_blocks(tmp_path, start_control_node):
    """Bug real: un cliente que muere a mitad de subida dejaba el nombre bloqueado para
    siempre (invisible en ls, rm decía 'no existe', send decía 'ya existe') y sus bloques
    huérfanos en disco."""
    import time

    dn_server, dn_port = serve_data_node(tmp_path / "dn", "localhost", 0, TEST_ENCRYPTION_KEY)
    datanode = f"localhost:{dn_port}"
    channel = grpc.insecure_channel(start_control_node([datanode], block_size_bytes=5, upload_lease_s=1.5, min_write_replicas=1))
    stub = control_node_pb2_grpc.ControlNodeServiceStub(channel)
    dn_stub = data_node_pb2_grpc.DataNodeServiceStub(grpc.insecure_channel(datanode))
    try:
        # el cliente reserva 2 bloques, escribe y confirma el primero... y se muere
        begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/tesis.bin", size_bytes=10, op_id=_op()))
        abandoned = begin.blocks[0]
        written = dn_stub.WriteBlock(iter([
            data_node_pb2.WriteBlockChunk(header=data_node_pb2.WriteBlockHeader(block_id=abandoned.block_id)),
            data_node_pb2.WriteBlockChunk(data=b"12345"),
        ]))
        stub.ConfirmBlock(control_node_pb2.ConfirmBlockRequest(
            path="/tesis.bin", block_id=abandoned.block_id, checksum=written.checksum, size_bytes=5, op_id=_op()))

        # con el lease vivo, el nombre sigue ocupado
        with pytest.raises(grpc.RpcError) as busy:
            stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/tesis.bin", size_bytes=3, op_id=_op()))
        assert busy.value.code() == grpc.StatusCode.ALREADY_EXISTS

        time.sleep(1.8)  # el lease de 1.5 s deja margen con la CPU cargada

        # vencido: otro cliente puede usar el nombre, y el bloque abandonado se borra
        retry = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/tesis.bin", size_bytes=3, op_id=_op()))
        assert len(retry.blocks) == 1
        with pytest.raises(grpc.RpcError) as gone:
            list(dn_stub.ReadBlock(data_node_pb2.ReadBlockRequest(block_id=abandoned.block_id)))
        assert gone.value.code() == grpc.StatusCode.NOT_FOUND
    finally:
        channel.close()
        dn_server.stop(grace=None)



def test_lock_renew_unlock_and_remove_conflict_are_translated(cluster):
    from dfsha.common.exceptions import ConflictError

    stub, _ = cluster
    begin = stub.BeginUpload(control_node_pb2.BeginUploadRequest(path="/archivo.txt", size_bytes=1, op_id=_op()))
    block = begin.blocks[0]
    stub.ConfirmBlock(control_node_pb2.ConfirmBlockRequest(
        path="/archivo.txt", block_id=block.block_id, checksum="sum", size_bytes=1, op_id=_op()))
    stub.CompleteUpload(control_node_pb2.CompleteUploadRequest(path="/archivo.txt", op_id=_op()))

    retry = stub.Lock(control_node_pb2.LockRequest(path="/archivo.txt", mode="r", op_id=_op()))
    assert retry.lock_id and retry.lease_s > 0
    same_op_id = _op()
    first = stub.Lock(control_node_pb2.LockRequest(path="/archivo.txt", mode="r", op_id=same_op_id))
    assert stub.Lock(control_node_pb2.LockRequest(path="/archivo.txt", mode="r", op_id=same_op_id)).lock_id == first.lock_id
    stub.RenewLock(control_node_pb2.RenewLockRequest(path="/archivo.txt", lock_id=retry.lock_id, op_id=_op()))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.Remove(control_node_pb2.RemoveRequest(path="/archivo.txt", op_id=_op()))
    assert exc_info.value.code() == grpc.StatusCode.ABORTED
    assert dict(exc_info.value.trailing_metadata())["dfsha-error"] == ConflictError.__name__
    stub.Unlock(control_node_pb2.UnlockRequest(path="/archivo.txt", lock_id=retry.lock_id, op_id=_op()))
    stub.Unlock(control_node_pb2.UnlockRequest(path="/archivo.txt", lock_id=first.lock_id, op_id=_op()))
    stub.Remove(control_node_pb2.RemoveRequest(path="/archivo.txt", op_id=_op()))


def test_lock_rejects_bad_mode_and_foreign_lock_id(cluster):
    stub, _ = cluster
    with pytest.raises(grpc.RpcError) as bad_mode:
        stub.Lock(control_node_pb2.LockRequest(path="/missing", mode="x", op_id=_op()))
    assert bad_mode.value.code() == grpc.StatusCode.PERMISSION_DENIED
    with pytest.raises(grpc.RpcError) as foreign:
        stub.RenewLock(control_node_pb2.RenewLockRequest(path="/missing", lock_id="foreign", op_id=_op()))
    assert foreign.value.code() == grpc.StatusCode.NOT_FOUND
