"""Hash de contraseñas y tokens JWT (Hito 3, C2)."""

import base64
import json
import time

import jwt
import pytest

from dfsha.common.auth import Principal, hash_password, issue_token, verify_password, verify_token
from dfsha.common.exceptions import AuthError

SECRET = b"s" * 32
OTHER_SECRET = b"o" * 32
ALICE = Principal("alice", ("alice", "docentes"), False)


def _b64(data: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(data).encode()).rstrip(b"=").decode()


def test_password_hash_round_trips():
    password_hash, salt = hash_password("clave correcta")

    assert len(password_hash) == 32
    assert len(salt) == 16
    assert verify_password("clave correcta", password_hash, salt)


def test_same_password_hashes_differently_each_time():
    assert hash_password("clave")[0] != hash_password("clave")[0]


def test_wrong_password_does_not_verify():
    password_hash, salt = hash_password("clave correcta")

    assert not verify_password("clave incorrecta", password_hash, salt)


def test_token_round_trips_the_principal():
    token = issue_token(SECRET, ALICE, 60, time.time())

    principal = verify_token(SECRET, token)

    assert principal == ALICE
    assert isinstance(principal.groups, tuple)


def test_short_lived_token_is_valid_right_after_issuing():
    assert verify_token(SECRET, issue_token(SECRET, ALICE, 2, time.time())) == ALICE


def test_expired_token_is_rejected():
    token = issue_token(SECRET, ALICE, 60, time.time() - 3600)

    with pytest.raises(AuthError, match="token vencido"):
        verify_token(SECRET, token)


def _valid_payload() -> dict:
    return {"sub": "alice", "groups": ["alice"], "admin": False, "exp": int(time.time()) + 60}


def _tampered() -> str:
    header, _, signature = issue_token(SECRET, ALICE, 60, time.time()).split(".")
    return ".".join([header, _b64({**_valid_payload(), "admin": True}), signature])


@pytest.mark.parametrize(
    "make_token",
    [
        _tampered,
        lambda: issue_token(OTHER_SECRET, ALICE, 60, time.time()),
        lambda: jwt.encode(_valid_payload(), SECRET, algorithm="HS512"),  # mismo secreto: solo cambia el algoritmo
        lambda: f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64(_valid_payload())}.",
        lambda: jwt.encode({"sub": "alice"}, SECRET, algorithm="HS256"),
        lambda: jwt.encode({"exp": int(time.time()) + 60}, SECRET, algorithm="HS256"),
        lambda: jwt.encode({**_valid_payload(), "groups": "alice"}, SECRET, algorithm="HS256"),
        lambda: jwt.encode({**_valid_payload(), "admin": "si"}, SECRET, algorithm="HS256"),
        lambda: "",
        lambda: "esto no es un token",
    ],
    ids=[
        "payload adulterado",
        "otro secreto",
        "HS512",
        "alg none",
        "sin exp",
        "sin sub",
        "groups no es lista",
        "admin no es bool",
        "vacío",
        "basura",
    ],
)
@pytest.mark.filterwarnings("ignore::jwt.InsecureKeyLengthWarning")  # el caso HS512 firma con 32 bytes a propósito
def test_invalid_tokens_are_rejected_without_leaking_them(make_token):
    token = make_token()

    with pytest.raises(AuthError) as exc_info:
        verify_token(SECRET, token)

    if token:
        assert token not in str(exc_info.value)
