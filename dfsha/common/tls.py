"""TLS de los enlaces gRPC (Hito 3, C1).

Una CA privada firma un único certificado de servidor que presentan todos los
ControlNodes y DataNodes. El certificado lleva el nombre fijo ``NODE_TLS_NAME`` y los
clientes lo verifican contra ese nombre (``grpc.ssl_target_name_override``), no contra
la dirección a la que se conectan: así el mismo certificado sirve en Docker
(``dn1:50061``), en procesos locales (``localhost``) y en AWS (IPs privadas), sin
regenerarlo por cada despliegue.

Es TLS de servidor: cifra el canal y prueba que del otro lado hay un nodo con la llave
de la CA. No identifica al cliente: el spike S1 mostró que grpcio no permite mTLS
opcional en un puerto que comparten clientes y nodos. La autorización de cada
operación queda para C2/C3.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import grpc

NODE_TLS_NAME = "dfsha-node"

_CA_VALIDITY = dt.timedelta(days=3650)
_NODE_VALIDITY = dt.timedelta(days=1825)


@dataclass(frozen=True)
class TlsConfig:
    """Material TLS de un proceso. Un nodo trae su certificado y su llave; un cliente
    solo necesita el certificado de la CA."""

    ca_cert: bytes
    cert_chain: bytes | None = None
    private_key: bytes | None = None

    @property
    def is_server(self) -> bool:
        return self.cert_chain is not None and self.private_key is not None


def load_tls(ca_file: Path, cert_file: Path | None = None, key_file: Path | None = None) -> TlsConfig:
    if (cert_file is None) != (key_file is None):
        raise ValueError("el certificado y la llave TLS van juntos")
    return TlsConfig(
        ca_cert=Path(ca_file).read_bytes(),
        cert_chain=Path(cert_file).read_bytes() if cert_file else None,
        private_key=Path(key_file).read_bytes() if key_file else None,
    )


def add_tls_arguments(parser, *, server: bool) -> None:
    """Flags de TLS comunes a nodos (certificado + llave + CA) y clientes (solo CA)."""
    parser.add_argument("--tls-ca-file", help="certificado de la CA de DFSha (sin él, canales en claro)")
    if server:
        parser.add_argument("--tls-cert-file", help="certificado TLS de este nodo")
        parser.add_argument("--tls-key-file", help="llave privada TLS de este nodo")


def tls_from_args(args, *, server: bool) -> TlsConfig | None:
    """None si no se pasó ningún flag de TLS (desarrollo y tests); si se pasó alguno,
    exige los que corresponden al rol."""
    files = [args.tls_ca_file] + ([args.tls_cert_file, args.tls_key_file] if server else [])
    if not any(files):
        print("AVISO: sin TLS, los canales gRPC van en claro (solo para desarrollo)")
        return None
    if not all(files):
        needed = "--tls-ca-file, --tls-cert-file y --tls-key-file" if server else "--tls-ca-file"
        raise SystemExit(f"TLS incompleto: hacen falta {needed}")
    if server:
        return load_tls(Path(args.tls_ca_file), Path(args.tls_cert_file), Path(args.tls_key_file))
    return load_tls(Path(args.tls_ca_file))


def channel_factory(tls: TlsConfig | None) -> Callable[[str], grpc.Channel]:
    """Fábrica de canales hacia ControlNodes o DataNodes. Sin TLS (tests y desarrollo)
    devuelve canales en claro."""
    if tls is None:
        return grpc.insecure_channel
    credentials = grpc.ssl_channel_credentials(root_certificates=tls.ca_cert)
    options = [("grpc.ssl_target_name_override", NODE_TLS_NAME)]
    return lambda address: grpc.secure_channel(address, credentials, options=options)


def add_port(server: grpc.Server, address: str, tls: TlsConfig | None) -> int:
    if tls is None:
        return server.add_insecure_port(address)
    if not tls.is_server:
        raise ValueError("un nodo con TLS necesita su certificado y su llave")
    credentials = grpc.ssl_server_credentials([(tls.private_key, tls.cert_chain)])
    return server.add_secure_port(address, credentials)


# ───────────────────────── generación (scripts/generate_secrets.py y tests) ─────────────────────────


def _pem_key(key) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def _pem_cert(cert) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return cert.public_bytes(serialization.Encoding.PEM)


def generate_ca() -> tuple[bytes, bytes]:
    """Devuelve (certificado, llave) PEM de una CA nueva, EC P-256."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "DFSha CA")])
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + _CA_VALIDITY)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False, key_encipherment=False,
                data_encipherment=False, key_agreement=False, key_cert_sign=True,
                crl_sign=True, encipher_only=False, decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return _pem_cert(cert), _pem_key(key)


def issue_node_cert(ca_cert_pem: bytes, ca_key_pem: bytes) -> tuple[bytes, bytes]:
    """Devuelve (certificado, llave) PEM del certificado de servidor de los nodos,
    firmado por la CA, con el nombre NODE_TLS_NAME."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem)
    ca_key = serialization.load_pem_private_key(ca_key_pem, password=None)
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, NODE_TLS_NAME)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + _NODE_VALIDITY)
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(NODE_TLS_NAME)]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_cert.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    return _pem_cert(cert), _pem_key(key)
