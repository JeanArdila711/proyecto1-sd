"""El ControlNode emite capabilities (Hito 3, C3, T8). ControlNode y DataNodes reales, los
dos con la clave; las llamadas van por stubs directos para ver cada capability."""

import math
import subprocess
import sys
import time
import uuid
from pathlib import Path

import grpc
import pytest

from conftest import FAST_RAFT_CONF, TEST_ENCRYPTION_KEY, free_port, wait_for
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.auth import Principal, issue_token
from dfsha.common.block_token import capability_kwargs, verify_block
from dfsha.common.exceptions import AccessDeniedError
from dfsha.control_node.main import serve as serve_control_node
from dfsha.control_node.servicer import ControlNodeServicer
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2 as cn
from dfsha.generated import control_node_pb2_grpc
from dfsha.generated import data_node_pb2 as dn
from dfsha.generated import data_node_pb2_grpc

_REPO = Path(__file__).resolve().parent.parent
KEY = b"clave-de-capabilities-de-32-bytes!"
JWT_SECRET = b"secreto-de-prueba-de-32-bytes-ok!"
BLOCK = 5
CONTENT = b"doce bytes!!"  # tres bloques: 5, 5 y 2
ADMIN = Principal("root", ("root",), True)
ALICE = Principal("alice", ("alice",), False)
BOB = Principal("bob", ("bob",), False)


def _op() -> str:
    return uuid.uuid4().hex


def _as(principal):
    if principal is None:
        return None
    return (("authorization", f"Bearer {issue_token(JWT_SECRET, principal, 60, time.time())}"),)


class _System:
    def __init__(self, stub, server, datanodes, client_factory):
        self.stub = stub
        self.server = server
        self.datanodes = datanodes  # address -> (stub, root, server)
        self.client = client_factory

    def dn(self, address):
        return self.datanodes[address][0]

    def root(self, address):
        return self.datanodes[address][1]

    def stop_datanode(self, address) -> None:
        self.datanodes[address][2].stop(grace=None)

    def stored(self, block_id) -> list[str]:
        return [address for address, (_, root, _) in self.datanodes.items() if (root / block_id).exists()]


@pytest.fixture
def start(tmp_path):
    """start(cn_key, dn_key, **serve_kwargs) -> _System con 3 DataNodes y un ControlNode."""
    servers, channels, rafts, clients = [], [], [], []

    def _start(cn_key=KEY, dn_key=KEY, **kwargs):
        datanodes = {}
        for i in range(3):
            root = tmp_path / f"s{len(rafts)}-dn{i}"
            server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY, capability_key=dn_key)
            servers.append(server)
            channel = grpc.insecure_channel(f"localhost:{port}")
            channels.append(channel)
            datanodes[f"localhost:{port}"] = (data_node_pb2_grpc.DataNodeServiceStub(channel), root, server)
        kwargs.setdefault("gc_interval_s", 3600)
        server, port, raft = serve_control_node(
            list(datanodes),
            "localhost",
            0,
            raft_self=f"localhost:{free_port()}",
            raft_peers=[],
            data_dir=None,
            raft_conf_overrides=FAST_RAFT_CONF,
            block_size_bytes=BLOCK,
            capability_key=cn_key,
            **kwargs,
        )
        servers.insert(0, server)
        rafts.append(raft)
        assert wait_for(raft._isLeader)
        channel = grpc.insecure_channel(f"localhost:{port}")
        channels.append(channel)

        def client_factory():
            made = DistributedDFShaClient([f"localhost:{port}"], rpc_timeout_s=2.0, failover_budget_s=4.0)
            clients.append(made)
            return made

        return _System(control_node_pb2_grpc.ControlNodeServiceStub(channel), server, datanodes, client_factory)

    yield _start

    for made in clients:
        made.close()
    for channel in channels:
        channel.close()
    for server in servers:
        server.stop(grace=None)
    for raft in rafts:
        raft.destroy()


def _write(stub, block_id, data, downstream=(), **kwargs):
    chunks = [
        dn.WriteBlockChunk(header=dn.WriteBlockHeader(block_id=block_id, downstream=list(downstream))),
        dn.WriteBlockChunk(data=data),
    ]
    return stub.WriteBlock(iter(chunks), timeout=10, **kwargs)


def _read(stub, block_id, **kwargs) -> bytes:
    return b"".join(c.data for c in stub.ReadBlock(dn.ReadBlockRequest(block_id=block_id), timeout=10, **kwargs))


def _write_blocks(system, blocks, data):
    """Escribe cada bloque por su pipeline con la capability que vino en la respuesta."""
    written, position = [], 0
    for block in blocks:
        piece = data[position : position + block.size_bytes]
        position += block.size_bytes
        response = _write(
            system.dn(block.datanode_addresses[0]),
            block.block_id,
            piece,
            downstream=block.datanode_addresses[1:],
            **capability_kwargs(block.capability),
        )
        written.append(response)
    return written


def _upload(system, path, data=CONTENT, principal=None):
    md = _as(principal)
    begun = system.stub.BeginUpload(cn.BeginUploadRequest(path=path, size_bytes=len(data), op_id=_op()), metadata=md)
    for block, response in zip(begun.blocks, _write_blocks(system, begun.blocks, data)):
        system.stub.ConfirmBlock(
            cn.ConfirmBlockRequest(
                path=path, block_id=block.block_id, checksum=response.checksum, size_bytes=response.bytes_written, op_id=_op()
            ),
            metadata=md,
        )
    system.stub.CompleteUpload(cn.CompleteUploadRequest(path=path, op_id=_op()), metadata=md)
    return list(begun.blocks)


def _list_blocks(system, path, principal=None):
    return list(system.stub.ListBlocks(cn.ListBlocksRequest(path=path), metadata=_as(principal)).blocks)


def _rejected(call) -> grpc.RpcError:
    with pytest.raises(grpc.RpcError) as exc_info:
        call()
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED, exc_info.value.details()
    return exc_info.value


# --- qué capability trae cada respuesta --------------------------------------------------------


def test_begin_upload_brings_a_write_capability_for_each_block(start):
    system = start()

    begun = system.stub.BeginUpload(cn.BeginUploadRequest(path="/a.bin", size_bytes=len(CONTENT), op_id=_op()))

    assert len(begun.blocks) == 3
    for block in begun.blocks:
        verify_block(KEY, block.capability, block.block_id, "write", time.time())


def test_list_blocks_brings_a_read_capability_that_reads_every_replica(start):
    system = start()
    _upload(system, "/a.bin")

    blocks = _list_blocks(system, "/a.bin")

    assert b"".join(_read(system.dn(b.datanode_addresses[0]), b.block_id, **capability_kwargs(b.capability)) for b in blocks) == CONTENT
    for block in blocks:
        verify_block(KEY, block.capability, block.block_id, "read", time.time())
        for address in block.datanode_addresses:
            _read(system.dn(address), block.block_id, **capability_kwargs(block.capability))


def test_begin_write_brings_read_for_the_old_block_and_write_for_the_new_one(start):
    system = start()
    _upload(system, "/a.bin")
    lock_id = system.stub.Lock(cn.LockRequest(path="/a.bin", mode="w", op_id=_op())).lock_id

    # offset 10 y 10 bytes: el bloque 2 (parcial, 2 bytes) se completa y el 3 extiende el archivo
    begun = system.stub.BeginWrite(
        cn.BeginWriteRequest(path="/a.bin", offset=10, length=10, lock_id=lock_id, op_id=_op())
    )

    assert [(s.index, bool(s.old_block_id)) for s in begun.slots] == [(2, True), (3, False)]
    overwritten, extending = begun.slots
    verify_block(KEY, overwritten.old_capability, overwritten.old_block_id, "read", time.time())
    assert extending.old_capability == ""
    for slot in begun.slots:
        verify_block(KEY, slot.new_capability, slot.new_block_id, "write", time.time())
    old = _read(system.dn(overwritten.old_addresses[0]), overwritten.old_block_id, **capability_kwargs(overwritten.old_capability))
    assert old == CONTENT[10:]


def test_a_retried_begin_upload_returns_the_same_blocks_with_valid_capabilities(start):
    system = start()
    request = cn.BeginUploadRequest(path="/a.bin", size_bytes=len(CONTENT), op_id=_op())

    first = system.stub.BeginUpload(request)
    again = system.stub.BeginUpload(request)

    assert [b.block_id for b in again.blocks] == [b.block_id for b in first.blocks]
    for block in again.blocks:
        verify_block(KEY, block.capability, block.block_id, "write", time.time())
    _write_blocks(system, again.blocks, CONTENT)


# --- una capability para cada cosa -------------------------------------------------------------


def test_the_upload_capability_does_not_read_and_the_listing_one_does_not_write(start):
    system = start()
    uploaded = _upload(system, "/a.bin")
    listed = _list_blocks(system, "/a.bin")
    head = system.dn(uploaded[0].datanode_addresses[0])

    reading = _rejected(lambda: _read(head, uploaded[0].block_id, **capability_kwargs(uploaded[0].capability)))
    writing = _rejected(lambda: _write(head, listed[0].block_id, b"otro", **capability_kwargs(listed[0].capability)))

    assert reading.details() == "la capability no autoriza esta operación"
    assert writing.details() == "la capability no autoriza esta operación"
    assert _read(head, listed[0].block_id, **capability_kwargs(listed[0].capability)) == CONTENT[:BLOCK]


def test_a_user_without_read_gets_neither_locations_nor_capability(start):
    system = start(jwt_secret=JWT_SECRET)
    _upload(system, "/a.bin", principal=ALICE)
    system.stub.Chmod(cn.ChmodRequest(path="/a.bin", mode=0o600, op_id=_op()), metadata=_as(ALICE))

    with pytest.raises(grpc.RpcError) as exc_info:
        system.stub.ListBlocks(cn.ListBlocksRequest(path="/a.bin"), metadata=_as(BOB))

    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert exc_info.value.details() == "permiso denegado: falta r en /a.bin"
    assert "b:read:" not in exc_info.value.details()
    for block in _list_blocks(system, "/a.bin", ALICE):
        verify_block(KEY, block.capability, block.block_id, "read", time.time())


# --- borrados que hace el propio ControlNode ---------------------------------------------------


def test_remove_deletes_the_blocks_on_datanodes_with_key(start):
    system = start()
    blocks = _upload(system, "/a.bin")
    assert all(len(system.stored(b.block_id)) == 3 for b in blocks)

    system.stub.Remove(cn.RemoveRequest(path="/a.bin", op_id=_op()))

    assert all(system.stored(b.block_id) == [] for b in blocks)


def test_abort_upload_deletes_the_written_blocks_on_datanodes_with_key(start):
    system = start()
    begun = system.stub.BeginUpload(cn.BeginUploadRequest(path="/a.bin", size_bytes=len(CONTENT), op_id=_op()))
    _write_blocks(system, begun.blocks, CONTENT)
    assert all(system.stored(b.block_id) for b in begun.blocks)

    system.stub.AbortUpload(cn.AbortUploadRequest(path="/a.bin", op_id=_op()))

    assert all(system.stored(b.block_id) == [] for b in begun.blocks)


def test_commit_write_deletes_the_replaced_blocks_on_datanodes_with_key(start):
    system = start()
    _upload(system, "/a.bin")
    lock_id = system.stub.Lock(cn.LockRequest(path="/a.bin", mode="w", op_id=_op())).lock_id
    begun = system.stub.BeginWrite(cn.BeginWriteRequest(path="/a.bin", offset=0, length=5, lock_id=lock_id, op_id=_op()))
    (slot,) = begun.slots
    response = _write(
        system.dn(slot.new_addresses[0]),
        slot.new_block_id,
        b"NUEVO",
        downstream=slot.new_addresses[1:],
        **capability_kwargs(slot.new_capability),
    )

    system.stub.CommitWrite(
        cn.CommitWriteRequest(
            path="/a.bin",
            write_id=begun.write_id,
            base_version=begun.base_version,
            lock_id=lock_id,
            slots=[
                cn.ConfirmedSlot(
                    index=slot.index, block_id=slot.new_block_id, checksum=response.checksum, size_bytes=response.bytes_written
                )
            ],
            op_id=_op(),
        )
    )

    assert system.stored(slot.old_block_id) == []
    assert len(system.stored(slot.new_block_id)) == 3


# --- duración ----------------------------------------------------------------------------------


def test_a_one_second_capability_stops_working_and_a_new_listing_brings_one_that_works(start):
    system = start(capability_ttl_s=1)
    _upload(system, "/a.bin")
    (first, *_) = _list_blocks(system, "/a.bin")
    head = system.dn(first.datanode_addresses[0])
    assert _read(head, first.block_id, **capability_kwargs(first.capability)) == CONTENT[:BLOCK]

    def expired() -> bool:
        try:
            _read(head, first.block_id, **capability_kwargs(first.capability))
        except grpc.RpcError as exc:
            return exc.details() == "capability vencida: repite la operación"
        return False

    assert wait_for(expired, timeout=5)
    fresh = _list_blocks(system, "/a.bin")[0]
    assert _read(head, fresh.block_id, **capability_kwargs(fresh.capability)) == CONTENT[:BLOCK]


# --- sin clave y configuraciones cruzadas ------------------------------------------------------


def test_without_key_the_fields_are_empty_and_everything_works(start, tmp_path):
    system = start(cn_key=None, dn_key=None)
    blocks = _upload(system, "/a.bin")
    lock_id = system.stub.Lock(cn.LockRequest(path="/a.bin", mode="w", op_id=_op())).lock_id
    slots = system.stub.BeginWrite(
        cn.BeginWriteRequest(path="/a.bin", offset=10, length=10, lock_id=lock_id, op_id=_op())
    ).slots

    assert [b.capability for b in blocks] == ["", "", ""]
    assert [b.capability for b in _list_blocks(system, "/a.bin")] == ["", "", ""]
    assert [(s.old_capability, s.new_capability) for s in slots] == [("", ""), ("", "")]
    system.stub.Unlock(cn.UnlockRequest(path="/a.bin", lock_id=lock_id, op_id=_op()))
    assert system.client().read("/a.bin") == CONTENT


def test_a_control_node_without_key_cannot_upload_to_datanodes_with_key(start, tmp_path):
    system = start(cn_key=None, dn_key=KEY)
    source = tmp_path / "origen.bin"
    source.write_bytes(CONTENT)

    with pytest.raises(AccessDeniedError) as exc_info:
        system.client().upload(source, "/a.bin")

    assert str(exc_info.value) == "falta la capability"


# --- configuración inválida --------------------------------------------------------------------


@pytest.mark.parametrize("ttl", [math.nan, math.inf, 0, -1])
def test_an_invalid_capability_ttl_is_rejected_by_the_servicer(ttl):
    with pytest.raises(ValueError) as exc_info:
        ControlNodeServicer(None, None, ["localhost:1"], BLOCK, 1, min_write_replicas=1, capability_ttl_s=ttl)

    assert str(exc_info.value) == f"capability_ttl_s debe ser un número finito mayor que cero, no {ttl}"


def _run_main(tmp_path, *extra):
    return subprocess.run(
        [
            sys.executable, "-m", "dfsha.control_node.main",
            "--node-id", "0", "--raft-cluster", f"localhost:{free_port()}",
            "--data-dir", str(tmp_path / "raft"), "--datanode-addresses", f"localhost:{free_port()}",
            "--host", "localhost", "--port", str(free_port()),
            "--replication-factor", "1", "--min-write-replicas", "1",
            *extra,
        ],
        cwd=_REPO, capture_output=True, text=True, timeout=60, check=False,
    )


@pytest.mark.parametrize("ttl", ["nan", "inf", "0", "-5"])
def test_main_rejects_an_invalid_capability_ttl(tmp_path, ttl):
    result = _run_main(tmp_path, f"--capability-ttl-s={ttl}")

    assert result.returncode != 0
    assert result.stderr.strip().endswith("--capability-ttl-s debe ser un número finito mayor que cero")
    assert "escuchando" not in result.stdout


@pytest.mark.parametrize("content", [None, b"corta"], ids=["inexistente", "corta"])
def test_main_with_a_bad_capability_key_file_stops(tmp_path, content):
    key_file = tmp_path / "capability.key"
    if content is not None:
        key_file.write_bytes(content)

    result = _run_main(tmp_path, "--capability-key-file", str(key_file))

    assert result.returncode != 0
    assert "escuchando" not in result.stdout
    if content is None:
        # el nombre y no la ruta entera: en Windows el mensaje de OSError duplica las barras
        assert "capability.key" in result.stderr and "Traceback" not in result.stderr
    else:
        assert result.stderr.strip().endswith(
            f"la clave de capabilities en {key_file} es demasiado corta (mínimo 32 bytes)"
        )
