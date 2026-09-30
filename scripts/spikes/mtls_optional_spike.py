#!/usr/bin/env python3
"""Spike S1: ¿mTLS opcional en un solo puerto gRPC?

Pregunta: con ``grpc.ssl_server_credentials(..., root_certificates=CA,
require_client_auth=False)``, ¿el servidor pide y VERIFICA el certificado de cliente
cuando se presenta, y el servicer lo ve en ``context.auth_context()``?

Arma una CA de juguete y una CA ajena, levanta un servidor en un puerto efímero con
cada modo y llama con: sin certificado, certificado de la CA y certificado ajeno.
No usa puertos ni código de DFSha.

Resultado con grpcio 1.83.1 (2026-09-30): NEGATIVO. Con require_client_auth=False el
servidor ni siquiera pide el certificado: aunque el cliente presente uno válido,
``auth_context()`` solo trae security_level, ssl_session_reused y
transport_security_type, sin x509_*. Con require_client_auth=True sí verifica y expone
x509_common_name, pero rechaza a todo cliente sin certificado, así que no sirve para un
puerto compartido con clientes. Consecuencia para C1/C3: la autorización de las
operaciones internas del DataNode queda solo en las capabilities HMAC de D-P5.
"""

from __future__ import annotations

import datetime as dt
from concurrent import futures

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

METHOD = "/spike.Probe/Who"


def _pem(key, cert) -> tuple[bytes, bytes]:
    return (
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                          serialization.NoEncryption()),
        cert.public_bytes(serialization.Encoding.PEM),
    )


def _cert(cn: str, issuer=None, *, ca: bool = False, san: str | None = None):
    """(llave, cert). Sin issuer, el certificado es autofirmado (una CA)."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    issuer_key, issuer_cert = issuer or (key, None)
    now = dt.datetime.now(dt.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(issuer_cert.subject if issuer_cert else name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if san:
        builder = builder.add_extension(x509.SubjectAlternativeName([x509.DNSName(san)]),
                                        critical=False)
    return key, builder.sign(issuer_key, hashes.SHA256())


def _who(_request: bytes, context: grpc.ServicerContext) -> bytes:
    auth = context.auth_context()
    cn = [v.decode() for v in auth.get("x509_common_name", [])]
    return f"peer_identity={cn or None} ssl={auth.get('transport_security_type')}".encode()


def _serve(server_pem, ca_pem: bytes, require_client_auth: bool) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers([grpc.method_handlers_generic_handler(
        "spike.Probe", {"Who": grpc.unary_unary_rpc_method_handler(_who)})])
    creds = grpc.ssl_server_credentials([server_pem], root_certificates=ca_pem,
                                        require_client_auth=require_client_auth)
    port = server.add_secure_port("localhost:0", creds)
    server.start()
    return server, port


def _call(port: int, ca_pem: bytes, client_pem=None) -> str:
    key, chain = client_pem or (None, None)
    creds = grpc.ssl_channel_credentials(ca_pem, private_key=key, certificate_chain=chain)
    with grpc.secure_channel(f"localhost:{port}", creds) as channel:
        try:
            return channel.unary_unary(METHOD)(b"", timeout=3).decode()
        except grpc.RpcError as exc:
            return f"RECHAZADO {exc.code().name}"


def main() -> None:
    ca = _cert("dfsha-spike-ca", ca=True)
    rogue_ca = _cert("ca-ajena", ca=True)
    ca_pem = _pem(*ca)[1]
    server_pem = _pem(*_cert("localhost", ca, san="localhost"))
    clients = {
        "sin certificado": None,
        "cert de la CA (cn=dn1)": _pem(*_cert("dn1", ca)),
        "cert de CA ajena (cn=intruso)": _pem(*_cert("intruso", rogue_ca)),
    }
    for require in (False, True):
        server, port = _serve(server_pem, ca_pem, require)
        print(f"\nrequire_client_auth={require}")
        for label, pem in clients.items():
            print(f"  {label:32} -> {_call(port, ca_pem, pem)}")
        server.stop(None)


if __name__ == "__main__":
    main()
