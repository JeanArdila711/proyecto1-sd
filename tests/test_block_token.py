"""Capabilities de bloque (Hito 3, C3, T6): emisión y verificación de dfsha/common/block_token.py."""

import hashlib
import hmac
import re
import time

import pytest

from dfsha.common import block_token
from dfsha.common.block_token import (
    capability_kwargs,
    issue_block,
    issue_internal,
    load_capability_key,
    verify_block,
    verify_internal,
)
from dfsha.common.exceptions import AccessDeniedError

KEY = b"clave-de-capabilities-de-32-bytes!"
OTHER_KEY = b"otra-clave-de-capabilities-32-byt"
BLOCK = "0123456789abcdef0123456789abcdef"
OTHER_BLOCK = "fedcba9876543210fedcba9876543210"
NOW = 1_791_500_000.25


def _hand_signed(payload: str, key: bytes = KEY) -> str:
    return f"{payload}.{hmac.new(key, payload.encode('ascii'), hashlib.sha256).hexdigest()}"


def _denied(call) -> str:
    with pytest.raises(AccessDeniedError) as exc_info:
        call()
    return str(exc_info.value)


# --- emisión y formato -------------------------------------------------------------------------


def test_a_block_capability_round_trips_with_the_documented_format():
    capability = issue_block(KEY, BLOCK, "write", 3600, NOW)

    assert re.fullmatch(rf"b:write:{BLOCK}:1791503601\.[0-9a-f]{{64}}", capability)
    assert len(capability) == 116
    assert len(issue_block(KEY, BLOCK, "read", 3600, NOW)) == 115
    assert len(issue_block(KEY, BLOCK, "delete", 3600, NOW)) == 117
    verify_block(KEY, capability, BLOCK, "write", NOW)


def test_an_internal_capability_round_trips_with_the_documented_format():
    capability = issue_internal(KEY, "replicate", 300, NOW)

    assert re.fullmatch(r"i:replicate:1791500301\.[0-9a-f]{64}", capability)
    assert len(capability) == 87
    assert len(issue_internal(KEY, "list", 300, NOW)) == 82
    verify_internal(KEY, capability, "replicate", NOW)


def test_a_capability_with_a_two_second_lifetime_is_valid_when_just_issued():
    now = time.time()
    capability = issue_block(KEY, BLOCK, "read", 2, now)

    verify_block(KEY, capability, BLOCK, "read", time.time())
    verify_internal(KEY, issue_internal(KEY, "list", 2, now), "list", time.time())


@pytest.mark.parametrize("op", ["list", "READ", "", "replicate"])
def test_issuing_a_block_capability_for_an_unknown_operation_is_a_programming_error(op):
    with pytest.raises(ValueError):
        issue_block(KEY, BLOCK, op, 60, NOW)


@pytest.mark.parametrize("block_id", ["", BLOCK.upper(), BLOCK[:-1], BLOCK + "0", "../" + BLOCK[3:], None])
def test_issuing_a_block_capability_for_a_malformed_block_id_is_a_programming_error(block_id):
    with pytest.raises(ValueError):
        issue_block(KEY, block_id, "read", 60, NOW)


@pytest.mark.parametrize("op", ["read", "write", "delete", ""])
def test_issuing_an_internal_capability_for_an_unknown_operation_is_a_programming_error(op):
    with pytest.raises(ValueError):
        issue_internal(KEY, op, 60, NOW)


# --- rechazos, cada uno con todo lo demás válido -----------------------------------------------


@pytest.mark.parametrize("empty", ["", None])
def test_a_missing_capability_is_rejected(empty):
    assert _denied(lambda: verify_block(KEY, empty, BLOCK, "read", NOW)) == "falta la capability"
    assert _denied(lambda: verify_internal(KEY, empty, "list", NOW)) == "falta la capability"


def _with_one_hmac_char_changed(capability: str) -> str:
    last = capability[-1]
    return capability[:-1] + ("0" if last != "0" else "1")


def _with_the_operation_changed(capability: str) -> str:
    return capability.replace("b:read:", "b:write:", 1)


@pytest.mark.parametrize(
    "tamper",
    [
        lambda c: issue_block(OTHER_KEY, BLOCK, "read", 3600, NOW),
        _with_one_hmac_char_changed,
        _with_the_operation_changed,
        lambda c: c.replace(".", "", 1),
        lambda c: "esto no es una capability",
        lambda c: c[:-1] + "ñ",
        lambda c: c.encode("ascii"),
        lambda c: 12345,
    ],
    ids=["otra clave", "hmac cambiado", "operación cambiada", "sin punto", "basura", "no ascii", "bytes", "entero"],
)
def test_a_capability_that_does_not_verify_is_invalid(tamper):
    capability = tamper(issue_block(KEY, BLOCK, "read", 3600, NOW))

    # la verificación pide "write" en el caso de la operación cambiada: el texto dice
    # write, y aun así el HMAC original no coincide
    op = "write" if isinstance(capability, str) and capability.startswith("b:write:") else "read"
    assert _denied(lambda: verify_block(KEY, capability, BLOCK, op, NOW)) == "capability inválida"


def test_an_internal_capability_signed_with_another_key_is_invalid():
    capability = issue_internal(OTHER_KEY, "list", 300, NOW)

    assert _denied(lambda: verify_internal(KEY, capability, "list", NOW)) == "capability inválida"


def test_a_capability_for_another_operation_does_not_authorize():
    capability = issue_block(KEY, BLOCK, "read", 3600, NOW)

    for op in ("write", "delete"):
        assert _denied(lambda: verify_block(KEY, capability, BLOCK, op, NOW)) == "la capability no autoriza esta operación"
    listing = issue_internal(KEY, "list", 300, NOW)
    assert _denied(lambda: verify_internal(KEY, listing, "replicate", NOW)) == "la capability no autoriza esta operación"


def test_a_capability_for_another_block_does_not_authorize():
    capability = issue_block(KEY, BLOCK, "write", 3600, NOW)

    assert _denied(lambda: verify_block(KEY, capability, OTHER_BLOCK, "write", NOW)) == (
        "la capability no autoriza esta operación"
    )


def test_an_internal_capability_is_not_a_block_capability_and_vice_versa():
    internal = issue_internal(KEY, "replicate", 300, NOW)
    block = issue_block(KEY, BLOCK, "read", 3600, NOW)

    assert _denied(lambda: verify_block(KEY, internal, BLOCK, "read", NOW)) == "la capability no autoriza esta operación"
    assert _denied(lambda: verify_internal(KEY, block, "replicate", NOW)) == "la capability no autoriza esta operación"


def test_the_prefix_counts_even_when_everything_else_matches():
    # Firmados a mano con la clave buena: solo el prefijo está mal.
    as_internal = _hand_signed(f"i:read:{BLOCK}:{int(NOW) + 3600}")
    as_block = _hand_signed(f"b:list:{int(NOW) + 300}")

    assert _denied(lambda: verify_block(KEY, as_internal, BLOCK, "read", NOW)) == "la capability no autoriza esta operación"
    assert _denied(lambda: verify_internal(KEY, as_block, "list", NOW)) == "la capability no autoriza esta operación"


def test_a_signed_capability_with_a_malformed_expiry_does_not_authorize():
    capability = _hand_signed(f"b:read:{BLOCK}:pronto")

    assert _denied(lambda: verify_block(KEY, capability, BLOCK, "read", NOW)) == "la capability no autoriza esta operación"


def test_an_expired_capability_is_rejected():
    an_hour_ago = time.time() - 3600
    block = issue_block(KEY, BLOCK, "read", 60, an_hour_ago)
    internal = issue_internal(KEY, "list", 60, an_hour_ago)

    assert _denied(lambda: verify_block(KEY, block, BLOCK, "read", time.time())) == "capability vencida: repite la operación"
    assert _denied(lambda: verify_internal(KEY, internal, "list", time.time())) == "capability vencida: repite la operación"


def test_the_capability_expires_exactly_at_its_expiry_second():
    capability = issue_block(KEY, BLOCK, "read", 10, NOW)
    expiry = int(capability.split(".")[0].rsplit(":", 1)[1])

    verify_block(KEY, capability, BLOCK, "read", expiry - 0.001)
    assert _denied(lambda: verify_block(KEY, capability, BLOCK, "read", expiry)) == "capability vencida: repite la operación"


def test_no_error_message_contains_the_capability():
    capability = issue_block(KEY, BLOCK, "read", 60, NOW)
    attempts = [
        lambda: verify_block(OTHER_KEY, capability, BLOCK, "read", NOW),
        lambda: verify_block(KEY, capability, OTHER_BLOCK, "read", NOW),
        lambda: verify_block(KEY, capability, BLOCK, "write", NOW),
        lambda: verify_block(KEY, capability, BLOCK, "read", NOW + 3600),
        lambda: verify_internal(KEY, capability, "list", NOW),
    ]

    for attempt in attempts:
        message = _denied(attempt)
        assert capability not in message
        assert capability.rsplit(".", 1)[1] not in message


# --- metadata y clave --------------------------------------------------------------------------


def test_capability_kwargs_only_adds_metadata_when_there_is_a_capability():
    assert capability_kwargs("") == {}
    assert capability_kwargs("", "destino") == {}
    assert capability_kwargs("c") == {"metadata": (("dfsha-capability", "c"),)}
    assert capability_kwargs("c", "d") == {
        "metadata": (("dfsha-capability", "c"), ("dfsha-target-capability", "d"))
    }
    assert block_token.METADATA_KEY == "dfsha-capability"
    assert block_token.TARGET_METADATA_KEY == "dfsha-target-capability"


def test_load_capability_key_reads_the_file(tmp_path):
    path = tmp_path / "capability.key"
    path.write_bytes(b"a" * 64 + b"\n")

    assert load_capability_key(path) == b"a" * 64


def test_load_capability_key_rejects_a_short_key(tmp_path):
    path = tmp_path / "capability.key"
    path.write_bytes(b"a" * 31)

    with pytest.raises(ValueError) as exc_info:
        load_capability_key(path)

    assert str(exc_info.value) == f"la clave de capabilities en {path} es demasiado corta (mínimo 32 bytes)"


def test_load_capability_key_of_a_missing_file_is_an_os_error(tmp_path):
    with pytest.raises(OSError):
        load_capability_key(tmp_path / "no-existe.key")
