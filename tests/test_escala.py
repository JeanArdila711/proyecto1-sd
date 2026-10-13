"""Evidencia de escalabilidad (Hito 3, X1): clúster con más DataNodes y script de medición."""

import csv
import gzip
import importlib.util
import pickle
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_COMPOSE = _REPO / "docker-compose.yml"
_ENV_ESCALA = _REPO / ".env.escala"


def _load(name: str):
    path = _REPO / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"{name}_escala", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, path


generate_secrets, _SECRETS_SCRIPT = _load("generate_secrets")
medir, _MEDIR_SCRIPT = _load("medir_escalabilidad")
_SNAPSHOT_SCRIPT = _REPO / "scripts" / "medir_snapshot_raft.py"

NODE_TLS = {"ca.crt", "node.crt", "node.key"}


def _files(root: Path) -> dict[str, bytes]:
    return {str(f.relative_to(root)): f.read_bytes() for f in sorted(root.rglob("*")) if f.is_file()}


def _generate(path: Path, datanodes: int) -> None:
    generate_secrets.generate(path, datanodes=datanodes)
    generate_secrets.generate_auth_secrets(path)
    generate_secrets.sync_mount_views(path, datanodes=datanodes)


# --- secretos para N DataNodes ---------------------------------------------------------------


def test_the_default_views_are_the_three_datanode_cluster():
    assert generate_secrets.MOUNT_VIEWS == generate_secrets.mount_views(3)
    assert set(generate_secrets.MOUNT_VIEWS) == {"dn1", "dn2", "dn3", "control", "client", "inspect"}


def test_five_datanodes_get_their_own_key_and_view(tmp_path):
    path = tmp_path / "secrets"

    _generate(path, 5)

    keys = [(path / f"dn{i}.key").read_bytes() for i in range(1, 6)]
    assert all(len(key) == 32 for key in keys)
    assert len(set(keys)) == 5  # una llave distinta por DataNode
    for i in range(1, 6):
        view = path / "mounts" / f"dn{i}"
        assert {f.name for f in view.iterdir()} == NODE_TLS | {f"dn{i}.key", "capability.key"}
    assert not list((path / "mounts").rglob("ca.key"))


def test_growing_from_three_to_five_keeps_every_existing_secret(tmp_path):
    # Si agregar DataNodes regenerara una llave, los bloques ya guardados quedarían ilegibles.
    path = tmp_path / "secrets"
    _generate(path, 3)
    before = _files(path)

    _generate(path, 5)

    after = _files(path)
    assert {name: after[name] for name in before} == before
    assert {name for name in after if name not in before} >= {"dn4.key", "dn5.key"}


def test_the_script_takes_the_number_of_datanodes(tmp_path):
    path = tmp_path / "secrets"

    result = subprocess.run(
        [sys.executable, str(_SECRETS_SCRIPT), "--dir", str(path), "--datanodes", "5"],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert {view.name for view in (path / "mounts").iterdir()} == set(generate_secrets.mount_views(5))


def test_the_script_rejects_a_cluster_without_datanodes(tmp_path):
    result = subprocess.run(
        [sys.executable, str(_SECRETS_SCRIPT), "--dir", str(tmp_path / "secrets"), "--datanodes", "0"],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode != 0
    assert "--datanodes debe ser >= 1" in result.stderr
    assert not (tmp_path / "secrets").exists()


# --- docker-compose.yml y .env.escala --------------------------------------------------------
# Se leen como texto: PyYAML no es dependencia del proyecto. La imagen no los copia.


def _compose_services() -> dict[str, str]:
    if not _COMPOSE.is_file() or not _ENV_ESCALA.is_file():
        pytest.skip("docker-compose.yml y .env.escala no se copian a la imagen de Docker")
    body = _COMPOSE.read_text(encoding="utf-8").split("\nservices:\n", 1)[1].split("\nnetworks:\n", 1)[0]
    services: dict[str, list[str]] = {}
    current = None
    for line in body.splitlines():
        if line.startswith("  ") and not line.startswith("   ") and line.rstrip().endswith(":"):
            current = line.strip().rstrip(":")
            services[current] = []
        elif current is not None:
            services[current].append(line)
    return {name: "\n".join(lines) for name, lines in services.items()}


def _env_escala() -> dict[str, str]:
    lines = [line for line in _ENV_ESCALA.read_text(encoding="utf-8").splitlines() if line and not line.startswith("#")]
    return dict(line.split("=", 1) for line in lines)


def test_the_extra_datanodes_only_start_with_the_escala_profile():
    services = _compose_services()

    for name in ("dn4", "dn5"):
        assert "profiles: [escala]" in services[name], name
        assert f"./secrets/mounts/{name}:/secrets:ro" in services[name], name
        assert f"/secrets/{name}.key" in services[name], name
        assert "--capability-key-file, /secrets/capability.key" in services[name], name
    for name in ("dn1", "dn2", "dn3", "cn0", "cn1", "cn2"):
        assert "profiles:" not in services[name], name


def test_the_control_nodes_take_the_datanode_list_from_the_environment():
    services = _compose_services()
    default = "--datanode-addresses=${DFSHA_DATANODES:-dn1:50061,dn2:50061,dn3:50061}"

    for name in ("cn0", "cn1", "cn2"):
        assert default in services[name], name
    assert '--datanodes, "${DFSHA_DATANODE_COUNT:-3}"' in services["init"]
    assert "DFSHA_DATANODES: ${DFSHA_DATANODES:-}" in services["inspect"]


def test_the_escala_env_file_names_exactly_the_datanodes_that_exist():
    services = _compose_services()
    env = _env_escala()

    names = [address.split(":")[0] for address in env["DFSHA_DATANODES"].split(",")]
    assert env["COMPOSE_PROFILES"] == "escala"
    assert int(env["DFSHA_DATANODE_COUNT"]) == len(names) == 5
    assert names == [f"dn{i}" for i in range(1, 6)]
    assert all(name in services for name in names)
    assert all(address.endswith(":50061") for address in env["DFSHA_DATANODES"].split(","))


# --- script de medición ----------------------------------------------------------------------


def _block_ids(tree) -> list[str]:
    return [block.block_id for _, block in tree.iter_blocks()]


def test_the_synthetic_tree_is_the_same_for_the_same_seed():
    first = medir.build_tree(50, 2, 5, 3, seed=7)
    again = medir.build_tree(50, 2, 5, 3, seed=7)
    other = medir.build_tree(50, 2, 5, 3, seed=8)

    assert _block_ids(first) == _block_ids(again)
    assert _block_ids(first) != _block_ids(other)


def test_the_synthetic_tree_spreads_the_replicas_over_every_datanode():
    tree = medir.build_tree(50, 2, 5, 3, seed=7)

    blocks = [block for _, block in tree.iter_blocks()]
    per_node: dict[str, int] = {}
    for block in blocks:
        assert len(set(block.datanode_addresses)) == 3
        for address in block.datanode_addresses:
            per_node[address] = per_node.get(address, 0) + 1

    assert len(blocks) == 100
    # 100 bloques x 3 réplicas sobre 5 nodos, rotando el primero: 60 en cada uno
    assert per_node == {f"dn{i}:50061": 60 for i in range(1, 6)}


def test_a_measurement_fills_every_column():
    row = medir.measure(200, 2, 5, 3, seed=7, repetitions=2)

    assert list(row) == medir.COLUMNS
    assert (row["archivos"], row["bloques"]) == (200, 400)
    for column in ("memoria_mb", "iter_blocks_ms", "referenced_blocks_ms", "ciclo_rereplicador_ms", "snapshot_ms"):
        assert row[column] > 0, column


def test_the_snapshot_is_what_pysyncobj_would_write_and_restores_the_tree():
    tree = medir.build_tree(20, 2, 5, 3, seed=7)

    restored = pickle.loads(gzip.decompress(medir.raft_snapshot(tree)))

    assert _block_ids(restored) == _block_ids(tree)


def test_the_measuring_script_prints_a_table_and_writes_the_csv(tmp_path):
    output = tmp_path / "medicion.csv"

    result = subprocess.run(
        [sys.executable, str(_MEDIR_SCRIPT), "--archivos", "20,40", "--repeticiones", "1", "--salida", str(output)],
        capture_output=True, text=True, check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "| archivos | bloques |" in result.stdout
    with output.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert [row["archivos"] for row in rows] == ["20", "40"]
    assert list(rows[0]) == medir.COLUMNS


def test_the_measuring_script_says_whether_the_garbage_collector_was_on():
    def header(*flags: str) -> str:
        command = [sys.executable, str(_MEDIR_SCRIPT), "--archivos", "20", "--repeticiones", "1", *flags]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        assert result.returncode == 0, result.stderr
        return result.stdout.splitlines()[0]

    assert header().endswith("recolector de basura de Python activo")
    assert header("--sin-gc").endswith("recolector de basura de Python apagado")


@pytest.mark.parametrize(
    "flags",
    [["--archivos", "diez"], ["--archivos", "0"], ["--replicas", "6", "--datanodes", "5"], ["--repeticiones", "0"]],
)
def test_the_measuring_script_rejects_bad_arguments(flags):
    result = subprocess.run([sys.executable, str(_MEDIR_SCRIPT), *flags], capture_output=True, text=True, check=False)

    assert result.returncode != 0
    assert "tiene" in result.stderr  # el mensaje dice qué argumento está mal


# --- script del snapshot de Raft -------------------------------------------------------------


def test_the_snapshot_script_runs_a_real_raft_cluster_and_reports_one_row_per_size(tmp_path):
    output = tmp_path / "snapshot.csv"

    result = subprocess.run(
        [
            sys.executable, str(_SNAPSHOT_SCRIPT), "--archivos", "20",
            "--duracion", "8", "--espera", "0.5", "--salida", str(output),
        ],
        capture_output=True, text=True, check=False, timeout=120,
    )

    assert result.returncode == 0, result.stderr
    with output.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert [row["archivos"] for row in rows] == ["20"]
    assert list(rows[0]) == ["archivos", "snapshots", "liderazgos_perdidos", "terminos_nuevos", "snapshot_ms"]
    # No se afirma nada sobre las elecciones: dependen de timeouts aleatorios y de la carga
    # de la máquina. Alcanza con que el líder haya tomado algún snapshot.
    assert int(rows[0]["snapshots"]) >= 1


def test_the_snapshot_script_rejects_bad_arguments():
    result = subprocess.run(
        [sys.executable, str(_SNAPSHOT_SCRIPT), "--duracion", "0"], capture_output=True, text=True, check=False
    )

    assert result.returncode != 0
    assert "tienen que ser mayores que cero" in result.stderr
