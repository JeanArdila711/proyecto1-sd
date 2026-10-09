"""El DataNode exige capabilities (Hito 3, C3, T7). DataNodes reales con la clave y
capabilities firmadas en el test."""

import logging
import subprocess
import sys
import time
from pathlib import Path

import grpc
import pytest

from conftest import TEST_ENCRYPTION_KEY, free_port, wait_for
from dfsha.common.block_token import capability_kwargs, issue_block, issue_internal
from dfsha.data_node import block_store
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import data_node_pb2 as dn
from dfsha.generated import data_node_pb2_grpc

_REPO = Path(__file__).resolve().parent.parent
KEY = b"clave-de-capabilities-de-32-bytes!"
OTHER_KEY = b"otra-clave-de-capabilities-32-byt"
BLOCK = "0123456789abcdef0123456789abcdef"
OTHER_BLOCK = "fedcba9876543210fedcba9876543210"
DATA = b"contenido del bloque"


def _cap(block_id: str, op: str, ttl_s: float = 600, key: bytes = KEY, now: float | None = None) -> str:
    return issue_block(key, block_id, op, ttl_s, time.time() if now is None else now)


def _internal(op: str, ttl_s: float = 300, key: bytes = KEY) -> str:
    return issue_internal(key, op, ttl_s, time.time())


@pytest.fixture
def datanodes(tmp_path):
    """start(capability_key) -> (stub, root, address). Se apagan solos al terminar."""
    started = []

    def start(capability_key=KEY):
        root = tmp_path / f"dn{len(started)}"
        server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY, capability_key=capability_key)
        channel = grpc.insecure_channel(f"localhost:{port}")
        started.append((server, channel))
        return data_node_pb2_grpc.DataNodeServiceStub(channel), root, f"localhost:{port}"

    yield start

    for server, channel in started:
        channel.close()
        server.stop(grace=None)


def _write(stub, block_id=BLOCK, data=DATA, downstream=(), **kwargs):
    chunks = [
        dn.WriteBlockChunk(header=dn.WriteBlockHeader(block_id=block_id, downstream=list(downstream))),
        dn.WriteBlockChunk(data=data),
    ]
    return stub.WriteBlock(iter(chunks), timeout=10, **kwargs)


def _read(stub, block_id=BLOCK, offset=0, length=0, **kwargs) -> bytes:
    request = dn.ReadBlockRequest(block_id=block_id, offset=offset, length=length)
    return b"".join(chunk.data for chunk in stub.ReadBlock(request, timeout=10, **kwargs))


def _store(root, block_id=BLOCK, data=DATA):
    block_store.write_block(root, TEST_ENCRYPTION_KEY, block_id, [data])


def _has(root, block_id=BLOCK) -> bool:
    return (root / block_id).exists()


def _rejected(call) -> grpc.RpcError:
    with pytest.raises(grpc.RpcError) as exc_info:
        call()
    exc = exc_info.value
    assert exc.code() == grpc.StatusCode.PERMISSION_DENIED, exc.details()
    assert dict(exc.trailing_metadata() or ()).get("dfsha-error") == "AccessDeniedError"
    return exc


# --- camino feliz ------------------------------------------------------------------------------


def test_every_rpc_works_with_the_right_capability(datanodes):
    stub, root, _ = datanodes()
    target_stub, target_root, target = datanodes()

    response = _write(stub, **capability_kwargs(_cap(BLOCK, "write")))
    assert response.bytes_written == len(DATA)
    assert _read(stub, **capability_kwargs(_cap(BLOCK, "read"))) == DATA
    assert _read(stub, offset=3, length=5, **capability_kwargs(_cap(BLOCK, "read"))) == DATA[3:8]
    listed = list(stub.ListStoredBlocks(dn.ListStoredBlocksRequest(), timeout=10, **capability_kwargs(_internal("list"))))
    assert [b.block_id for b in listed] == [BLOCK]
    copied = stub.ReplicateBlock(
        dn.ReplicateBlockRequest(block_id=BLOCK, target=target),
        timeout=10,
        **capability_kwargs(_internal("replicate"), _cap(BLOCK, "write")),
    )
    assert copied.checksum == response.checksum
    assert _has(target_root)
    stub.DeleteBlock(dn.DeleteBlockRequest(block_id=BLOCK), timeout=10, **capability_kwargs(_cap(BLOCK, "delete")))
    assert not _has(root)


def test_ping_needs_no_capability(datanodes):
    stub, _, _ = datanodes()

    assert stub.Ping(dn.PingRequest(), timeout=5) == dn.PingResponse()


# --- sin capability ----------------------------------------------------------------------------


def test_each_rpc_without_a_capability_is_denied(datanodes):
    stub, root, _ = datanodes()
    _, target_root, target = datanodes()
    _store(root, OTHER_BLOCK)
    calls = {
        "WriteBlock": lambda: _write(stub),
        "ReadBlock": lambda: _read(stub, OTHER_BLOCK),
        "DeleteBlock": lambda: stub.DeleteBlock(dn.DeleteBlockRequest(block_id=OTHER_BLOCK), timeout=10),
        "ListStoredBlocks": lambda: list(stub.ListStoredBlocks(dn.ListStoredBlocksRequest(), timeout=10)),
        "ReplicateBlock": lambda: stub.ReplicateBlock(
            dn.ReplicateBlockRequest(block_id=OTHER_BLOCK, target=target), timeout=10
        ),
    }

    for name, call in calls.items():
        assert _rejected(call).details() == "falta la capability", name
    assert not _has(root, BLOCK)
    assert _has(root, OTHER_BLOCK)
    assert not _has(target_root, OTHER_BLOCK)


# --- capabilities que no sirven ----------------------------------------------------------------


def _manipulated(capability: str) -> str:
    return capability[:-1] + ("0" if capability[-1] != "0" else "1")


@pytest.mark.parametrize(
    "make, message",
    [
        (lambda op: _manipulated(_cap(BLOCK, op)), "capability inválida"),
        (lambda op: _cap(BLOCK, op, ttl_s=60, now=time.time() - 3600), "capability vencida: repite la operación"),
        (lambda op: _cap(BLOCK, "delete"), "la capability no autoriza esta operación"),
        (lambda op: _cap(OTHER_BLOCK, op), "la capability no autoriza esta operación"),
        (lambda op: _cap(BLOCK, op, key=OTHER_KEY), "capability inválida"),
    ],
    ids=["manipulada", "vencida", "otra operación", "otro bloque", "otra clave"],
)
def test_a_capability_that_does_not_fit_is_denied_for_read_and_write(datanodes, make, message):
    stub, root, _ = datanodes()

    assert _rejected(lambda: _write(stub, **capability_kwargs(make("write")))).details() == message
    assert not _has(root, BLOCK)
    _store(root)  # el bloque que se lee existe: lo único que falla es la capability
    assert _rejected(lambda: _read(stub, **capability_kwargs(make("read")))).details() == message


def test_the_capability_of_one_block_neither_reads_nor_writes_another(datanodes):
    stub, root, _ = datanodes()
    _store(root, BLOCK)

    for op, call in (
        ("read", lambda kwargs: _read(stub, BLOCK, **kwargs)),
        ("write", lambda kwargs: _write(stub, OTHER_BLOCK, **kwargs)),
    ):
        own = _cap(OTHER_BLOCK if op == "read" else BLOCK, op)
        assert _rejected(lambda: call(capability_kwargs(own))).details() == "la capability no autoriza esta operación"
    assert not _has(root, OTHER_BLOCK)
    assert _read(stub, BLOCK, **capability_kwargs(_cap(BLOCK, "read"))) == DATA


def test_internal_rpcs_reject_block_capabilities_and_block_rpcs_reject_internal_ones(datanodes):
    stub, root, _ = datanodes()
    _, target_root, target = datanodes()
    _store(root)
    not_authorized = "la capability no autoriza esta operación"

    assert _rejected(
        lambda: list(stub.ListStoredBlocks(dn.ListStoredBlocksRequest(), timeout=10, **capability_kwargs(_cap(BLOCK, "read"))))
    ).details() == not_authorized
    assert _rejected(
        lambda: stub.ReplicateBlock(
            dn.ReplicateBlockRequest(block_id=BLOCK, target=target),
            timeout=10,
            **capability_kwargs(_cap(BLOCK, "write"), _cap(BLOCK, "write")),
        )
    ).details() == not_authorized
    assert _rejected(lambda: _read(stub, **capability_kwargs(_internal("list")))).details() == not_authorized
    assert _rejected(lambda: _write(stub, OTHER_BLOCK, **capability_kwargs(_internal("replicate")))).details() == not_authorized
    assert not _has(root, OTHER_BLOCK)
    assert not _has(target_root)


def test_read_and_write_capabilities_do_not_delete(datanodes):
    stub, root, _ = datanodes()
    _store(root)

    for op in ("read", "write"):
        assert _rejected(
            lambda: stub.DeleteBlock(dn.DeleteBlockRequest(block_id=BLOCK), timeout=10, **capability_kwargs(_cap(BLOCK, op)))
        ).details() == "la capability no autoriza esta operación"
    assert _has(root)


# --- pipeline y réplica ------------------------------------------------------------------------


def test_one_write_capability_leaves_three_copies_through_the_pipeline(datanodes):
    (head, head_root, _), (_, second_root, second), (_, third_root, third) = datanodes(), datanodes(), datanodes()

    response = _write(head, downstream=[second, third], **capability_kwargs(_cap(BLOCK, "write")))

    assert response.bytes_written == len(DATA)
    assert all(_has(root) for root in (head_root, second_root, third_root))


def test_the_head_rejects_a_pipeline_with_the_capability_of_another_block(datanodes):
    (head, head_root, _), (_, second_root, second), (_, third_root, third) = datanodes(), datanodes(), datanodes()

    exc = _rejected(lambda: _write(head, downstream=[second, third], **capability_kwargs(_cap(OTHER_BLOCK, "write"))))

    assert exc.details() == "la capability no autoriza esta operación"
    assert not any(_has(root) for root in (head_root, second_root, third_root))


def test_replicate_block_copies_with_both_capabilities(datanodes):
    stub, root, _ = datanodes()
    _, target_root, target = datanodes()
    _store(root)

    stub.ReplicateBlock(
        dn.ReplicateBlockRequest(block_id=BLOCK, target=target),
        timeout=10,
        **capability_kwargs(_internal("replicate"), _cap(BLOCK, "write")),
    )

    assert b"".join(block_store.read_block(target_root, TEST_ENCRYPTION_KEY, BLOCK, 1024)) == DATA


@pytest.mark.parametrize(
    "target_capability, message",
    [("", "falta la capability"), (_cap(OTHER_BLOCK, "write"), "la capability no autoriza esta operación")],
    ids=["sin la del destino", "la del destino es de otro bloque"],
)
def test_replicate_block_needs_the_write_capability_of_that_block_for_the_target(datanodes, target_capability, message):
    stub, root, _ = datanodes()
    _, target_root, target = datanodes()
    _store(root)

    exc = _rejected(
        lambda: stub.ReplicateBlock(
            dn.ReplicateBlockRequest(block_id=BLOCK, target=target),
            timeout=10,
            **capability_kwargs(_internal("replicate"), target_capability),
        )
    )

    assert exc.details() == message
    assert not _has(target_root)


def test_a_target_that_rejects_the_copy_answers_permission_denied_through_the_source(datanodes):
    # El destino tiene otra clave: el origen acepta las dos capabilities y el destino no.
    # El rechazo no puede salir como UNAVAILABLE, o el re-replicador reintentaría.
    stub, root, _ = datanodes()
    _, target_root, target = datanodes(capability_key=OTHER_KEY)
    _store(root)

    exc = _rejected(
        lambda: stub.ReplicateBlock(
            dn.ReplicateBlockRequest(block_id=BLOCK, target=target),
            timeout=10,
            **capability_kwargs(_internal("replicate"), _cap(BLOCK, "write")),
        )
    )

    assert exc.details() == f"no se pudo replicar hacia {target}: capability inválida"
    assert not _has(target_root)


# --- tiempo ------------------------------------------------------------------------------------


def test_a_one_second_capability_reads_at_first_and_then_stops(datanodes):
    stub, root, _ = datanodes()
    _store(root)
    capability = _cap(BLOCK, "read", ttl_s=1)

    assert _read(stub, **capability_kwargs(capability)) == DATA

    def expired() -> bool:
        try:
            _read(stub, **capability_kwargs(capability))
        except grpc.RpcError as exc:
            return exc.code() == grpc.StatusCode.PERMISSION_DENIED and exc.details() == "capability vencida: repite la operación"
        return False

    assert wait_for(expired, timeout=5)


# --- sin clave ---------------------------------------------------------------------------------


def test_a_datanode_without_key_accepts_everything_with_or_without_metadata(datanodes):
    stub, root, _ = datanodes(capability_key=None)
    _, target_root, target = datanodes(capability_key=None)
    garbage = capability_kwargs("esto no es una capability", "tampoco esta")

    for kwargs in ({}, garbage):
        assert _write(stub, **kwargs).bytes_written == len(DATA)
        assert _read(stub, **kwargs) == DATA
        assert [b.block_id for b in stub.ListStoredBlocks(dn.ListStoredBlocksRequest(), timeout=10, **kwargs)] == [BLOCK]
        stub.ReplicateBlock(dn.ReplicateBlockRequest(block_id=BLOCK, target=target), timeout=10, **kwargs)
        assert _has(target_root)
        stub.DeleteBlock(dn.DeleteBlockRequest(block_id=BLOCK), timeout=10, **kwargs)
        assert not _has(root)


# --- main --------------------------------------------------------------------------------------


@pytest.mark.parametrize("content", [None, b"corta"], ids=["inexistente", "corta"])
def test_main_with_a_bad_capability_key_file_stops(tmp_path, content):
    encryption_key = tmp_path / "dn.key"
    encryption_key.write_bytes(TEST_ENCRYPTION_KEY)
    capability_key = tmp_path / "capability.key"
    if content is not None:
        capability_key.write_bytes(content)

    result = subprocess.run(
        [
            sys.executable, "-m", "dfsha.data_node.main",
            "--root", str(tmp_path / "datos"), "--host", "localhost", "--port", str(free_port()),
            "--encryption-key-file", str(encryption_key), "--capability-key-file", str(capability_key),
        ],
        cwd=_REPO, capture_output=True, text=True, timeout=60, check=False,
    )

    assert result.returncode != 0
    assert "escuchando" not in result.stdout
    if content is None:
        # el nombre y no la ruta entera: en Windows el mensaje de OSError duplica las barras
        assert "capability.key" in result.stderr and "Traceback" not in result.stderr
    else:
        assert result.stderr.strip().endswith(
            f"la clave de capabilities en {capability_key} es demasiado corta (mínimo 32 bytes)"
        )


# --- secretos fuera de logs y errores ----------------------------------------------------------


def test_no_capability_shows_up_in_logs_or_error_details(datanodes, caplog):
    caplog.set_level(logging.DEBUG)
    (head, _, _), (_, _, second) = datanodes(), datanodes()
    good = _cap(BLOCK, "write")
    wrong_block = _cap(OTHER_BLOCK, "write")
    expired = _cap(BLOCK, "write", ttl_s=60, now=time.time() - 3600)
    details = []

    _write(head, downstream=[second], **capability_kwargs(good))
    for bad in (wrong_block, expired, _manipulated(good)):
        details.append(_rejected(lambda: _write(head, BLOCK, **capability_kwargs(bad))).details())
        details.append(_rejected(lambda: _read(head, **capability_kwargs(bad))).details())

    for capability in (good, wrong_block, expired):
        signature = capability.rsplit(".", 1)[1]
        assert signature not in caplog.text
        assert all(signature not in detail for detail in details)
