"""Direcciones públicas de los DataNodes para clientes de fuera de la red (Hito 3, D-P11)."""

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import TEST_ENCRYPTION_KEY, free_port
from dfsha.client import distributed_shell_main
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.addresses import (
    INTERNAL_VIEW_METADATA,
    parse_public_addresses,
    validate_public_addresses,
)
from dfsha.control_node.servicer import ControlNodeServicer
from dfsha.data_node.main import serve as serve_data_node
from dfsha.generated import control_node_pb2

_REPO = Path(__file__).resolve().parent.parent
_INTERNAL = ["dn1:50061", "dn2:50061", "dn3:50061"]
_VALID = {"dn1:50061": "203.0.113.7:50061", "dn2:50061": "203.0.113.7:50062", "dn3:50061": "nodo3.ejemplo.com:50061"}


# --- leer y validar el mapeo -----------------------------------------------------------------


def test_an_empty_mapping_means_the_cluster_is_not_announced():
    assert parse_public_addresses("") == {}
    assert parse_public_addresses(" , ") == {}
    validate_public_addresses({}, _INTERNAL)  # no lanza


def test_the_mapping_is_read_as_internal_equals_public_pairs():
    text = "dn1:50061=203.0.113.7:50061, dn2:50061=203.0.113.7:50062,\n dn3:50061 = nodo3.ejemplo.com:50061"

    assert parse_public_addresses(text) == _VALID
    validate_public_addresses(_VALID, _INTERNAL)  # no lanza


@pytest.mark.parametrize("pair", ["dn2:50061", "dn2:50061=", "=203.0.113.7:50062"])
def test_a_pair_that_is_not_internal_equals_public_is_rejected(pair):
    with pytest.raises(ValueError) as error:
        parse_public_addresses(f"dn1:50061=203.0.113.7:50061,{pair}")

    assert str(error.value) == f"mapeo de direcciones públicas inválido: {pair!r} no tiene la forma interna=pública"


def test_an_internal_address_listed_twice_is_rejected():
    with pytest.raises(ValueError) as error:
        parse_public_addresses("dn1:50061=203.0.113.7:50061,dn1:50061=203.0.113.7:50062")

    assert str(error.value) == "mapeo de direcciones públicas inválido: la dirección interna dn1:50061 aparece dos veces"


def test_a_datanode_without_public_address_is_rejected():
    mapping = {internal: public for internal, public in _VALID.items() if internal != "dn2:50061"}

    with pytest.raises(ValueError) as error:
        validate_public_addresses(mapping, _INTERNAL)

    assert str(error.value) == "mapeo de direcciones públicas incompleto: falta la dirección pública de dn2:50061"


def test_an_internal_address_that_is_not_a_configured_datanode_is_rejected():
    mapping = {**_VALID, "dn9:50061": "203.0.113.7:50069"}

    with pytest.raises(ValueError) as error:
        validate_public_addresses(mapping, _INTERNAL)

    assert str(error.value) == "mapeo de direcciones públicas inválido: no hay ningún DataNode configurado como dn9:50061"


def test_two_datanodes_behind_the_same_public_address_are_rejected():
    mapping = {**_VALID, "dn2:50061": _VALID["dn1:50061"]}

    with pytest.raises(ValueError) as error:
        validate_public_addresses(mapping, _INTERNAL)

    assert str(error.value) == (
        "mapeo de direcciones públicas inválido: dn1:50061 y dn2:50061 apuntan a la misma "
        "dirección pública 203.0.113.7:50061"
    )


@pytest.mark.parametrize(
    "public", ["203.0.113.7", "203.0.113.7:", ":50062", "203.0.113.7:abc", "203.0.113.7:0", "203.0.113.7:70000", "a b:50062"]
)
def test_a_public_address_that_is_not_host_port_is_rejected(public):
    mapping = {**_VALID, "dn2:50061": public}

    with pytest.raises(ValueError) as error:
        validate_public_addresses(mapping, _INTERNAL)

    assert str(error.value) == f"mapeo de direcciones públicas inválido: {public!r} no es host:puerto"


# --- arranque del ControlNode ----------------------------------------------------------------


def test_a_control_node_does_not_start_with_an_incomplete_mapping(start_control_node):
    with pytest.raises(ValueError) as error:
        start_control_node(["dn1:50061", "dn2:50061"], public_addresses={"dn1:50061": "203.0.113.7:50061"})

    assert str(error.value) == "mapeo de direcciones públicas incompleto: falta la dirección pública de dn2:50061"


def _run_control_node_main(tmp_path, *flags, env=None):
    command = [
        sys.executable, "-m", "dfsha.control_node.main", "--node-id", "0",
        "--raft-cluster", f"localhost:{free_port()}", "--data-dir", str(tmp_path / "raft"),
        "--datanode-addresses", ",".join(_INTERNAL), *flags,
    ]
    return subprocess.run(
        command, cwd=_REPO, env={**os.environ, **(env or {})}, capture_output=True, text=True, check=False, timeout=60
    )


def test_the_control_node_command_refuses_a_bad_mapping_before_opening_anything(tmp_path):
    result = _run_control_node_main(tmp_path, "--datanode-public-addresses", "dn1:50061=203.0.113.7:50061")

    assert result.returncode != 0
    assert result.stderr.strip().splitlines()[-1] == (
        "mapeo de direcciones públicas incompleto: falta la dirección pública de dn2:50061, dn3:50061"
    )
    assert not (tmp_path / "raft").exists()  # falló antes de tocar el disco


def test_the_control_node_command_reads_the_mapping_from_the_environment(tmp_path):
    # Así lo pasa docker-compose.publico.yml. Un valor mal formado demuestra que se leyó.
    result = _run_control_node_main(tmp_path, env={"DFSHA_DATANODE_PUBLIC_ADDRESSES": "dn1:50061"})

    assert result.returncode != 0
    assert result.stderr.strip().splitlines()[-1] == (
        "mapeo de direcciones públicas inválido: 'dn1:50061' no tiene la forma interna=pública"
    )


# --- qué direcciones recibe cada cliente -----------------------------------------------------
# Los DataNodes escuchan en localhost:<puerto>, y esa es su dirección interna. La "pública"
# es 127.0.0.1:<puerto>: llega al mismo proceso, pero es otro texto, que es lo único que
# distingue a las dos en estos tests.


def _public(internal: str) -> str:
    return internal.replace("localhost", "127.0.0.1")


@pytest.fixture
def make_cluster(tmp_path, start_control_node):
    servers, clients = [], []

    def _make(announced: bool = True, block_size_bytes: int = 5):
        internal, roots = [], []
        for i in range(3):
            root = tmp_path / f"dn{i}"
            server, port = serve_data_node(root, "localhost", 0, TEST_ENCRYPTION_KEY)
            servers.append(server)
            internal.append(f"localhost:{port}")
            roots.append(root)
        public_addresses = {address: _public(address) for address in internal} if announced else None
        control = start_control_node(internal, block_size_bytes=block_size_bytes, public_addresses=public_addresses)

        def client(**kwargs) -> tuple[DistributedDFShaClient, list[str]]:
            """Un cliente y la lista de direcciones de DataNode que va marcando."""
            new = DistributedDFShaClient([control], **kwargs)
            clients.append(new)
            dialed: list[str] = []
            real_channel = new._new_channel

            def recording_channel(address: str):
                dialed.append(address)
                return real_channel(address)

            new._new_channel = recording_channel  # los canales a ControlNodes ya están abiertos
            return new, dialed

        return SimpleNamespace(internal=internal, public=[_public(a) for a in internal], roots=roots, client=client)

    yield _make

    for client in clients:
        client.close()
    for server in servers:
        server.stop(grace=None)


def _upload(client, tmp_path, remote: str, content: bytes) -> None:
    local = tmp_path / f"subida-{os.urandom(4).hex()}"
    local.write_bytes(content)
    client.upload(local, remote)


def _blocks(client, path: str):
    return list(client._call("ListBlocks", control_node_pb2.ListBlocksRequest(path=path)).blocks)


def _stored(root: Path) -> set[str]:
    return {p.name for p in root.iterdir() if len(p.name) == 32} if root.exists() else set()


def test_a_write_gets_the_public_head_and_an_internal_downstream(make_cluster):
    cluster = make_cluster()
    client, _ = cluster.client()

    response = client._call(
        "BeginUpload", control_node_pb2.BeginUploadRequest(path="/a.bin", size_bytes=12, op_id="subida-1")
    )

    assert len(response.blocks) == 3
    for block in response.blocks:
        head, *downstream = block.datanode_addresses
        assert head in cluster.public
        assert len(downstream) == 2 and set(downstream) <= set(cluster.internal)
        # la cabeza y su downstream son tres DataNodes distintos
        assert len({head, *map(_public, downstream)}) == 3


def test_a_read_gets_every_replica_by_its_public_address(make_cluster, tmp_path):
    cluster = make_cluster()
    client, _ = cluster.client()
    _upload(client, tmp_path, "/a.bin", b"hola mundo!!")

    blocks = _blocks(client, "/a.bin")

    assert len(blocks) == 3
    for block in blocks:
        assert len(block.datanode_addresses) == 3
        assert set(block.datanode_addresses) == set(cluster.public)


def test_a_client_inside_the_network_asks_for_the_internal_addresses(make_cluster, tmp_path):
    cluster = make_cluster()
    outside, _ = cluster.client()
    inside, dialed = cluster.client(internal_addresses=True)
    _upload(inside, tmp_path, "/a.bin", b"hola mundo!!")

    assert inside.read("/a.bin") == b"hola mundo!!"
    assert dialed and set(dialed) <= set(cluster.internal)
    from_inside = [list(block.datanode_addresses) for block in _blocks(inside, "/a.bin")]
    from_outside = [list(block.datanode_addresses) for block in _blocks(outside, "/a.bin")]
    # Mismas réplicas y mismo orden: la metadata guarda la interna, y solo cambia cómo se entrega.
    assert all(set(replicas) == set(cluster.internal) for replicas in from_inside)
    assert from_outside == [[_public(address) for address in replicas] for replicas in from_inside]


def test_an_outside_client_works_end_to_end_dialing_only_public_addresses(make_cluster, tmp_path):
    cluster = make_cluster()
    client, dialed = cluster.client()
    content = b"hola mundo!!"  # 3 bloques de 5 bytes: cada DataNode es cabeza de alguno

    _upload(client, tmp_path, "/a.bin", content)
    downloaded = tmp_path / "bajada.bin"
    client.download("/a.bin", downloaded)
    assert downloaded.read_bytes() == content
    assert client.read("/a.bin", 3, 6) == content[3:9]
    assert client.write("/a.bin", 4, b"MUNDO") == 5  # copy-on-write: lee el bloque viejo y sube uno nuevo
    assert client.read("/a.bin") == b"holaMUNDOo!!"  # 5 bytes reemplazados desde el 4

    assert set(dialed) == set(cluster.public)
    # El pipeline llegó a los tres DataNodes: el downstream usó direcciones que sí funcionan entre nodos.
    block_ids = {block.block_id for block in _blocks(client, "/a.bin")}
    assert len(block_ids) == 3
    for root in cluster.roots:
        assert block_ids <= _stored(root)


def test_a_copy_on_write_gets_public_old_replicas_and_a_public_head_for_the_new_block(make_cluster, tmp_path):
    cluster = make_cluster()
    client, _ = cluster.client()
    _upload(client, tmp_path, "/a.bin", b"hola mundo!!")
    begun = []
    real_call = client._call

    def recording_call(rpc_name, request):
        response = real_call(rpc_name, request)
        if rpc_name == "BeginWrite":
            begun.append(response)
        return response

    client._call = recording_call
    client.write("/a.bin", 1, b"OLA")

    [response] = begun
    [slot] = response.slots
    assert set(slot.old_addresses) == set(cluster.public)
    head, *downstream = slot.new_addresses
    assert head in cluster.public
    assert len(downstream) == 2 and set(downstream) <= set(cluster.internal)


def test_without_a_mapping_every_client_gets_the_internal_addresses(make_cluster, tmp_path):
    cluster = make_cluster(announced=False)
    client, dialed = cluster.client()

    _upload(client, tmp_path, "/a.bin", b"hola mundo!!")

    assert client.read("/a.bin") == b"hola mundo!!"
    assert set(dialed) == set(cluster.internal)
    assert all(set(block.datanode_addresses) == set(cluster.internal) for block in _blocks(client, "/a.bin"))


def test_a_replica_that_is_no_longer_configured_goes_out_untranslated():
    # Un bloque viejo puede nombrar un DataNode que ya no está en la configuración.
    servicer = SimpleNamespace(_public_addresses={"dn1:50061": "203.0.113.7:50061"})
    context = SimpleNamespace(invocation_metadata=lambda: ())

    read = ControlNodeServicer._for_client(servicer, context, ["dn9:50061", "dn1:50061"])
    write = ControlNodeServicer._for_client(servicer, context, ["dn1:50061", "dn9:50061"], pipeline=True)

    assert read == ["dn9:50061", "203.0.113.7:50061"]
    assert write == ["203.0.113.7:50061", "dn9:50061"]


# --- shell e inspector -----------------------------------------------------------------------


@pytest.mark.parametrize("flags, expected", [([], False), (["--internal-addresses"], True)])
def test_the_shell_asks_for_internal_addresses_only_with_its_flag(monkeypatch, flags, expected):
    built = []

    def fake_build_client(addresses, tls, parallel_transfers, internal_addresses):
        built.append(internal_addresses)
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(distributed_shell_main, "build_client", fake_build_client)
    monkeypatch.setattr(distributed_shell_main, "run_repl", lambda client: None)
    monkeypatch.setattr(sys, "argv", ["shell", "--control-nodes", "localhost:50051", *flags])

    distributed_shell_main.main()

    assert built == [expected]


def test_the_inspector_always_asks_for_the_internal_addresses():
    spec = importlib.util.spec_from_file_location("inspect_cluster_addresses", _REPO / "scripts" / "inspect_cluster.py")
    inspector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(inspector)
    sent = []

    def list_blocks(request, timeout, metadata):
        sent.append(tuple(metadata))
        return control_node_pb2.ListBlocksResponse()

    inspector._auth_metadata = (("authorization", "Bearer token"),)
    inspector.blocks_of(SimpleNamespace(ListBlocks=list_blocks), "/a.bin")
    inspector._auth_metadata = None  # clúster sin autenticación
    inspector.blocks_of(SimpleNamespace(ListBlocks=list_blocks), "/a.bin")

    assert sent == [(("authorization", "Bearer token"), *INTERNAL_VIEW_METADATA), INTERNAL_VIEW_METADATA]


# --- docker-compose --------------------------------------------------------------------------
# Se leen como texto: PyYAML no es dependencia del proyecto. La imagen no los copia.


def _compose(name: str) -> str:
    path = _REPO / name
    if not path.is_file():
        pytest.skip(f"{name} no se copia a la imagen de Docker")
    return path.read_text(encoding="utf-8")


def test_the_public_override_publishes_the_port_the_mapping_announces():
    override = _compose("docker-compose.publico.yml")

    for i in (1, 2, 3):
        # el DataNode i sale por el puerto 5006i del host, y eso mismo dice el mapeo
        assert f'"${{DFSHA_BIND_IP:-0.0.0.0}}:5006{i}:50061"' in override
        assert f"}}:5006{i}" in override.split("x-mapeo:")[1].split("services:")[0]
        assert f"dn{i}:50061=${{DFSHA_PUBLIC_HOST" in override
    for i in (1, 2, 3):
        assert f'"${{DFSHA_BIND_IP:-0.0.0.0}}:5005{i}:50051"' in override  # un puerto por ControlNode
    assert override.count("environment: *mapeo") == 3  # los tres ControlNodes reciben el mismo mapeo
    assert "6000" not in override.split("services:")[1]  # Raft no se publica


def test_the_compose_shell_is_inside_and_the_external_client_is_not():
    compose = _compose("docker-compose.yml")
    shell = compose.split("\n  shell:\n")[1].split("\n  cliente:\n")[0]
    external = compose.split("\n  cliente:\n")[1].split("\n  inspect:\n")[0]

    assert "--internal-addresses" in shell
    assert "--internal-addresses" not in external
    assert "profiles: [externo]" in external
    assert "networks:" not in external and "<<: *dfsha" not in external  # fuera de la red del clúster
    assert "${DFSHA_CONTROL_NODES:-}" in external
    assert "ports:" not in compose  # el clúster local no publica nada si no se pide


def test_the_local_env_file_points_the_client_at_the_published_control_node_ports():
    lines = [line for line in _compose(".env.publico").splitlines() if line and not line.startswith("#")]
    env = dict(line.split("=", 1) for line in lines)

    host = env["DFSHA_PUBLIC_HOST"]
    assert env["DFSHA_CONTROL_NODES"] == ",".join(f"{host}:5005{i}" for i in (1, 2, 3))
    assert env["DFSHA_BIND_IP"] == "127.0.0.1"  # la prueba local no abre el clúster a la red
