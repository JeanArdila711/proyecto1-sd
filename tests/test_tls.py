"""TLS en los enlaces gRPC (C1) y generación de secretos."""

import argparse
import importlib.util
from pathlib import Path

import pytest

from conftest import TEST_ENCRYPTION_KEY
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.exceptions import DFShaError
from dfsha.common.tls import generate_ca, issue_node_cert, load_tls, tls_from_args, TlsConfig
from dfsha.data_node.main import serve as serve_data_node

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "generate_secrets.py"
_spec = importlib.util.spec_from_file_location("generate_secrets", _SCRIPT)
generate_secrets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generate_secrets)


@pytest.fixture
def secrets_dir(tmp_path):
    path = tmp_path / "secrets"
    generate_secrets.generate(path)
    return path


def _node_tls(secrets_dir):
    return load_tls(secrets_dir / "ca.crt", secrets_dir / "node.crt", secrets_dir / "node.key")


@pytest.fixture
def tls_cluster(tmp_path, start_control_node, secrets_dir):
    """3 DataNodes y un ControlNode, todos con TLS. Devuelve la dirección del ControlNode."""
    node_tls = _node_tls(secrets_dir)
    servers, addresses = [], []
    for i in range(3):
        server, port = serve_data_node(tmp_path / f"dn{i}", "localhost", 0, TEST_ENCRYPTION_KEY, node_tls)
        servers.append(server)
        addresses.append(f"localhost:{port}")
    cn_address = start_control_node(addresses, block_size_bytes=5, tls=node_tls)
    yield cn_address
    for server in servers:
        server.stop(grace=None)


def _client(addresses, tls):
    return DistributedDFShaClient(addresses, tls=tls, rpc_timeout_s=1.0, failover_budget_s=2.0)


def test_cluster_works_end_to_end_over_tls(tls_cluster, secrets_dir, tmp_path):
    """Cliente→ControlNode, cliente→DataNode, el pipeline DataNode→DataNode y el Ping
    del monitor (sin DataNodes vivos la subida no arrancaría) van todos por TLS."""
    client = _client([tls_cluster], load_tls(secrets_dir / "ca.crt"))
    try:
        content = bytes(range(256)) * 2
        local = tmp_path / "a.bin"
        local.write_bytes(content)
        client.make_dir("/docs")
        client.upload(local, "/docs/a.bin")
        destination = tmp_path / "copia.bin"
        client.download("/docs/a.bin", destination)
        assert destination.read_bytes() == content
        client.remove("/docs/a.bin")
        assert client.list_dir("/docs") == []
    finally:
        client.close()


def test_certificate_does_not_depend_on_the_address(tls_cluster, secrets_dir):
    """El certificado lleva un nombre fijo: sirve igual por nombre o por IP, como en AWS."""
    port = tls_cluster.rsplit(":", 1)[1]
    client = _client([f"127.0.0.1:{port}"], load_tls(secrets_dir / "ca.crt"))
    try:
        assert client.list_dir("/") == []
    finally:
        client.close()


def test_plaintext_client_cannot_talk_to_a_tls_node(tls_cluster):
    client = _client([tls_cluster], None)
    try:
        with pytest.raises(DFShaError):
            client.list_dir("/")
    finally:
        client.close()


def test_client_rejects_a_node_signed_by_another_ca(tls_cluster, tmp_path):
    other_ca, _ = generate_ca()
    client = _client([tls_cluster], TlsConfig(ca_cert=other_ca))
    try:
        with pytest.raises(DFShaError):
            client.list_dir("/")
    finally:
        client.close()


def test_generate_secrets_creates_everything_once(tmp_path):
    path = tmp_path / "secrets"
    generate_secrets.generate(path)
    before = {f.name: f.read_bytes() for f in path.iterdir()}
    assert set(before) == {
        "dn1.key", "dn2.key", "dn3.key", "raft.password", "ca.crt", "ca.key", "node.crt", "node.key",
    }
    assert all(len(before[f"dn{i}.key"]) == 32 for i in (1, 2, 3))
    assert len(before["raft.password"]) >= 32

    generate_secrets.generate(path)  # correrlo otra vez no cambia nada
    assert {f.name: f.read_bytes() for f in path.iterdir()} == before


def test_a_new_ca_reissues_the_node_certificate(tmp_path):
    path = tmp_path / "secrets"
    generate_secrets.generate(path)
    old_node_cert = (path / "node.crt").read_bytes()
    dn_key = (path / "dn1.key").read_bytes()
    (path / "ca.key").unlink()

    generate_secrets.generate(path)

    assert (path / "node.crt").read_bytes() != old_node_cert
    assert (path / "dn1.key").read_bytes() == dn_key  # lo demás se conserva


def test_issued_node_certificate_verifies_against_its_ca():
    from cryptography import x509

    ca_cert, ca_key = generate_ca()
    node_cert, _ = issue_node_cert(ca_cert, ca_key)
    ca = x509.load_pem_x509_certificate(ca_cert)
    x509.load_pem_x509_certificate(node_cert).verify_directly_issued_by(ca)


def test_partial_tls_flags_are_rejected():
    args = argparse.Namespace(tls_ca_file="ca.crt", tls_cert_file=None, tls_key_file=None)
    with pytest.raises(SystemExit):
        tls_from_args(args, server=True)
    assert tls_from_args(argparse.Namespace(tls_ca_file=None), server=False) is None
