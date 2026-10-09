"""Capabilities de bloque (Hito 3, C3): permisos firmados que el ControlNode entrega y
el DataNode verifica sin preguntarle a nadie.

Dos formas, texto ASCII con campos fijos y un HMAC-SHA256 en hexadecimal al final:

    de bloque:  b:<op>:<block_id>:<exp>.<hmac>    autoriza leer, escribir o borrar ESE bloque
    interna:    i:<op>:<exp>.<hmac>               autoriza listar o replicar; solo la usa el ControlNode

Ninguna nombra a un DataNode: una capability vale en cualquier réplica que tenga la
clave. Los nodos que comparten la clave son un único dominio de confianza: un DataNode
comprometido puede firmar cualquier capability.

Viajan como metadata gRPC (``dfsha-capability``), nunca en los mensajes del .proto, y no
se escriben en logs ni en mensajes de error. El vencimiento (``exp``, segundos Unix del
reloj de quien emite) se mira cuando la llamada llega: una transferencia que empezó a
tiempo termina aunque la capability venza a mitad.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import re
from pathlib import Path

from dfsha.common.exceptions import AccessDeniedError

METADATA_KEY = "dfsha-capability"
# Solo en ReplicateBlock: la capability de escritura que el origen le presenta al destino.
TARGET_METADATA_KEY = "dfsha-target-capability"
MIN_KEY_BYTES = 32
# Las que recibe el cliente (--capability-ttl-s). Cubre el arranque del último bloque de
# una transferencia larga; las internas se emiten justo antes de usarse.
DEFAULT_CAPABILITY_TTL_S = 3600.0
INTERNAL_TTL_S = 300.0
BLOCK_OPS = frozenset({"read", "write", "delete"})
INTERNAL_OPS = frozenset({"list", "replicate"})

# El formato que genera el ControlNode (uuid4().hex) y que valida el DataNode.
_BLOCK_ID = re.compile(r"[0-9a-f]{32}")
_EXP = re.compile(r"[0-9]+")

MISSING = "falta la capability"
INVALID = "capability inválida"
NOT_AUTHORIZED = "la capability no autoriza esta operación"
EXPIRED = "capability vencida: repite la operación"


def load_capability_key(path: Path) -> bytes:
    """Bytes del archivo de la clave, sin espacios ni saltos de línea en los bordes.
    OSError si no se puede leer; ValueError si mide menos de MIN_KEY_BYTES."""
    key = path.read_bytes().strip()
    if len(key) < MIN_KEY_BYTES:
        raise ValueError(
            f"la clave de capabilities en {path} es demasiado corta (mínimo {MIN_KEY_BYTES} bytes)"
        )
    return key


def _sign(key: bytes, payload: str) -> str:
    return f"{payload}.{hmac.new(key, payload.encode('ascii'), hashlib.sha256).hexdigest()}"


def _expiry(now: float, ttl_s: float) -> str:
    return str(math.ceil(now + ttl_s))


def issue_block(key: bytes, block_id: str, op: str, ttl_s: float, now: float) -> str:
    # Un valor fuera de lista acá es un error de programación, no una entrada de red.
    if op not in BLOCK_OPS:
        raise ValueError(f"operación de bloque desconocida: {op!r}")
    if not isinstance(block_id, str) or not _BLOCK_ID.fullmatch(block_id):
        raise ValueError(f"block_id con formato inválido: {block_id!r}")
    return _sign(key, f"b:{op}:{block_id}:{_expiry(now, ttl_s)}")


def issue_internal(key: bytes, op: str, ttl_s: float, now: float) -> str:
    if op not in INTERNAL_OPS:
        raise ValueError(f"operación interna desconocida: {op!r}")
    return _sign(key, f"i:{op}:{_expiry(now, ttl_s)}")


def _verified_fields(key: bytes, token) -> list[str]:
    """Campos de una capability con HMAC válido. Los mira recién después de la firma:
    un texto sin firmar no llega a compararse con nada."""
    if not token:
        raise AccessDeniedError(MISSING)
    try:
        payload, dot, signature = token.rpartition(".")
        expected = hmac.new(key, payload.encode("ascii"), hashlib.sha256).hexdigest().encode("ascii")
        valid = bool(dot) and hmac.compare_digest(signature.encode("ascii"), expected)
    except Exception:  # no es texto, o no es ASCII: nunca otra excepción que AccessDeniedError
        valid = False
    if not valid:
        raise AccessDeniedError(INVALID)
    return payload.split(":")


def _check_expiry(exp: str, now: float) -> None:
    if not _EXP.fullmatch(exp):
        raise AccessDeniedError(NOT_AUTHORIZED)
    if int(exp) <= now:
        raise AccessDeniedError(EXPIRED)


def verify_block(key: bytes, token: str, block_id: str, op: str, now: float) -> None:
    """Lanza AccessDeniedError si ``token`` no autoriza ``op`` sobre ``block_id`` ahora."""
    fields = _verified_fields(key, token)
    if len(fields) != 4 or fields[0] != "b" or fields[1] != op or fields[2] != block_id:
        raise AccessDeniedError(NOT_AUTHORIZED)
    _check_expiry(fields[3], now)


def verify_internal(key: bytes, token: str, op: str, now: float) -> None:
    """Lanza AccessDeniedError si ``token`` no autoriza la operación interna ``op`` ahora."""
    fields = _verified_fields(key, token)
    if len(fields) != 3 or fields[0] != "i" or fields[1] != op:
        raise AccessDeniedError(NOT_AUTHORIZED)
    _check_expiry(fields[2], now)


def capability_kwargs(capability: str, target_capability: str = "") -> dict:
    """Argumentos para una llamada a un DataNode. Sin capability devuelve {}: la llamada
    sale exactamente como antes de C3 (los stubs falsos de los tests no aceptan
    ``metadata``)."""
    if not capability:
        return {}
    metadata = [(METADATA_KEY, capability)]
    if target_capability:
        metadata.append((TARGET_METADATA_KEY, target_capability))
    return {"metadata": tuple(metadata)}
