"""Usuarios y sesiones (Hito 3, C2): hash de contraseñas y tokens de sesión.

El token es un JWT HS256 autocontenido: lleva usuario, grupos, si es admin y el
vencimiento, y el ControlNode le cree con solo verificar firma y fecha, sin consultar
el árbol. No hay revocación: un token robado sirve hasta que vence, y cambiar la
contraseña no lo corta. Quien tenga el secreto fabrica tokens de cualquier usuario.
"""

from __future__ import annotations

import hashlib
import hmac
import math
import os
from dataclasses import dataclass

import jwt

from dfsha.common.exceptions import AuthError

# ponytail: parámetros fijos del módulo, no guardados por usuario. Si algún día cambian,
# UserRecord gana un campo con default simple y los hashes viejos siguen verificando.
SCRYPT_N, SCRYPT_R, SCRYPT_P, SCRYPT_DKLEN, SALT_BYTES = 2**14, 8, 1, 32, 16

DEFAULT_TOKEN_TTL_S = 1800.0
# HS256 firma con SHA-256: un secreto más corto que el hash debilita la firma.
MIN_JWT_SECRET_BYTES = 32
_ALGORITHM = "HS256"


@dataclass(frozen=True)
class Principal:
    """Quien llama, tal como lo afirma un token ya verificado."""

    username: str
    groups: tuple[str, ...] = ()
    is_admin: bool = False


def _scrypt(password: str, salt: bytes) -> bytes:
    return hashlib.scrypt(
        password.encode("utf-8"), salt=salt, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=SCRYPT_DKLEN
    )


def hash_password(password: str) -> tuple[bytes, bytes]:
    """(hash, sal nueva). Se llama en el líder, nunca dentro de apply(): la sal es azar."""
    salt = os.urandom(SALT_BYTES)
    return _scrypt(password, salt), salt


def verify_password(password: str, password_hash: bytes, salt: bytes) -> bool:
    return hmac.compare_digest(_scrypt(password, salt), password_hash)


def issue_token(secret: bytes, principal: Principal, ttl_s: float, now: float) -> str:
    # Sin `iat` ni `nbf`: PyJWT rechaza un `iat` futuro, y con relojes apenas desfasados
    # un token recién emitido por un líder fallaría en el siguiente tras un failover.
    # ceil: PyJWT compara `exp` como entero; así el token vive al menos el TTL pedido.
    payload = {
        "sub": principal.username,
        "groups": list(principal.groups),
        "admin": principal.is_admin,
        "exp": math.ceil(now + ttl_s),
    }
    return jwt.encode(payload, secret, algorithm=_ALGORITHM)


def verify_token(secret: bytes, token: str) -> Principal:
    """Devuelve a quien llama o lanza AuthError. Los mensajes nunca incluyen el token."""
    if not token:
        raise AuthError("falta el token")
    try:
        # `algorithms` fija HS256: un token con `alg: none` o con otro algoritmo no pasa.
        # `require`: PyJWT solo valida `exp` si viene en el payload.
        payload = jwt.decode(token, secret, algorithms=[_ALGORITHM], options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError:
        raise AuthError("token vencido") from None
    except jwt.InvalidTokenError:
        raise AuthError("token inválido") from None
    groups = payload.get("groups", [])
    is_admin = payload.get("admin", False)
    if (
        not isinstance(groups, list)
        or not all(isinstance(group, str) for group in groups)
        or not isinstance(is_admin, bool)
    ):
        raise AuthError("token inválido")
    return Principal(username=payload["sub"], groups=tuple(groups), is_admin=is_admin)
