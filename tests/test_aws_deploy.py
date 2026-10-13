"""Despliegue en AWS en tres instancias (Hito 3, D1): Compose por nodo y deploy/aws/levantar.sh."""

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_COMPOSE = _REPO / "deploy" / "aws" / "docker-compose.yml"
_SCRIPT = _REPO / "deploy" / "aws" / "levantar.sh"

# Solo se copian a la imagen de Docker el código y los tests: deploy/ no.
pytestmark = pytest.mark.skipif(not _COMPOSE.is_file(), reason="deploy/ no se copia a la imagen de Docker")


def _compose() -> str:
    return _COMPOSE.read_text(encoding="utf-8")


# --- Compose de un nodo ----------------------------------------------------------------------
# Se lee como texto: PyYAML no es dependencia del proyecto.


def test_each_internal_name_resolves_to_the_private_ip_of_its_instance():
    compose = _compose()

    for n in (0, 1, 2):
        # el ControlNode n y el DataNode n+1 viven en la misma instancia
        assert f'- "cn{n}:${{DFSHA_IP_{n}' in compose
        assert f'- "dn{n + 1}:${{DFSHA_IP_{n}}}"' in compose
    assert "network_mode: host" in compose
    assert "--raft-cluster=cn0:6000,cn1:6000,cn2:6000" in compose
    assert "--datanode-addresses=dn1:50061,dn2:50061,dn3:50061" in compose


def test_each_datanode_is_announced_by_the_public_ip_of_its_instance():
    announced = _compose().split("DFSHA_DATANODE_PUBLIC_ADDRESSES:")[1].split("volumes:")[0]

    for n in (0, 1, 2):
        assert f"dn{n + 1}:50061=${{DFSHA_PUBLIC_{n}}}:50061" in announced


def test_the_node_this_instance_runs_comes_from_its_env_file():
    compose = _compose()

    assert "--node-id=${DFSHA_NODE}" in compose
    assert '"/secrets/dn${DFSHA_DN}.key"' in compose
    assert '"../../secrets/mounts/dn${DFSHA_DN}:/secrets:ro"' in compose
    assert "../../secrets/mounts/control:/secrets:ro" in compose


def test_the_nodes_come_back_after_a_reboot_and_the_tools_do_not():
    compose = _compose()
    tools = compose.split("\n  shell:\n")[1]

    assert compose.split("\nservices:\n")[0].count("restart: unless-stopped") == 1  # en el bloque común
    assert tools.count('restart: "no"') == 2  # shell e inspect
    assert "--internal-addresses" in tools.split("\n  inspect:\n")[0]
    assert "ports:" not in compose  # con la red de la instancia no se publica nada: decide el security group


def test_the_env_file_of_each_instance_is_not_versioned():
    assert "deploy/aws/.env" in (_REPO / ".gitignore").read_text(encoding="utf-8").splitlines()


# --- levantar.sh -----------------------------------------------------------------------------
# Se corre de verdad, con `aws` y `ssh` falsos delante en el PATH: no toca AWS ni la red.

_FAKE_AWS = r"""#!/usr/bin/env bash
args="$*"
case "$args" in
  *"wait "*) exit 0 ;;
  *"start-instances"*) echo "$args" >> "$FAKE_OUT/started"; exit 0 ;;
  *"tag:Name,Values=dfsha-nodo-"*)
    n="${args##*dfsha-nodo-}"; n="${n%% *}"
    state=running; [ "$n" = "${FAKE_STOPPED:-x}" ] && state=stopped
    [ "$n" = "${FAKE_MISSING:-x}" ] || printf 'i-%s\t%s\n' "$n" "$state" ;;
  *"--instance-ids i-"*)
    n="${args##*--instance-ids i-}"; n="${n%% *}"
    printf '10.0.0.%s\t203.0.113.%s\n' "$n" "$n" ;;
esac
"""

_FAKE_SSH = r"""#!/usr/bin/env bash
command="${!#}"            # el último argumento es el comando remoto
host="${@: -2:1}"          # y el anterior, ubuntu@<ip>
ip="${host#ubuntu@}"
case "$command" in
  *"cat > proyecto1-sd/deploy/aws/.env"*) cat > "$FAKE_OUT/env-$ip" ;;
  *"tar -C proyecto1-sd -cz"*) echo vistas ;;
  *"tar -C proyecto1-sd -xz"*) cat > /dev/null; echo "$ip" >> "$FAKE_OUT/copied" ;;
  *sha256sum*) if [ "$ip" = "203.0.113.0" ] || [ -n "${FAKE_SAME_SECRETS:-}" ]; then echo iguales; else echo distintas; fi ;;
  *'grep -c "(healthy)"'*) echo 2 ;;
  *"secrets/mounts/client/ca.crt"*) echo CERTIFICADO ;;
  *"secrets/admin.password"*) echo clave-de-prueba ;;
  *"inspect estado"*) echo "cn0 LIDER" ;;
esac
exit 0
"""


@pytest.fixture
def run_script(tmp_path):
    if sys.platform == "win32" or shutil.which("bash") is None:
        pytest.skip("levantar.sh es un script de bash")
    fake_bin, out, client = tmp_path / "bin", tmp_path / "out", tmp_path / "cliente local"
    fake_bin.mkdir()
    out.mkdir()
    for name, body in (("aws", _FAKE_AWS), ("ssh", _FAKE_SSH)):
        path = fake_bin / name
        path.write_text(body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
    key = tmp_path / "llave.pem"
    key.write_text("llave", encoding="utf-8")

    def _run(**env):
        full_env = {
            **{k: v for k, v in os.environ.items() if not k.startswith("DFSHA_")},
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "FAKE_OUT": str(out), "DFSHA_SSH_KEY": str(key), "DFSHA_CLIENT_DIR": str(client), **env,
        }
        result = subprocess.run(
            ["bash", str(_SCRIPT)], env=full_env, capture_output=True, text=True, check=False, timeout=120
        )
        return SimpleResult(result, out, client)

    return _run


class SimpleResult:
    def __init__(self, result, out: Path, client: Path) -> None:
        self.returncode, self.stdout, self.stderr = result.returncode, result.stdout, result.stderr
        self.out, self.client = out, client

    def env_of(self, n: int) -> dict[str, str]:
        lines = (self.out / f"env-203.0.113.{n}").read_text(encoding="utf-8").splitlines()
        return dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))


def test_the_script_writes_each_instance_its_own_node_and_the_same_addresses(run_script):
    result = run_script()

    assert result.returncode == 0, result.stderr
    for n in (0, 1, 2):
        env = result.env_of(n)
        assert (env["DFSHA_NODE"], env["DFSHA_DN"]) == (str(n), str(n + 1))
        assert [env[f"DFSHA_IP_{i}"] for i in (0, 1, 2)] == ["10.0.0.0", "10.0.0.1", "10.0.0.2"]
        assert [env[f"DFSHA_PUBLIC_{i}"] for i in (0, 1, 2)] == ["203.0.113.0", "203.0.113.1", "203.0.113.2"]
        assert "DFSHA_BLOCK_MB" not in env  # sin pedirlo, valen los valores por defecto del clúster


def test_the_script_passes_the_cluster_parameters_to_the_three_instances(run_script):
    result = run_script(DFSHA_BLOCK_MB="1", DFSHA_GC_GRACE_S="5")

    assert result.returncode == 0, result.stderr
    for n in (0, 1, 2):
        env = result.env_of(n)
        assert (env["DFSHA_BLOCK_MB"], env["DFSHA_GC_GRACE_S"]) == ("1", "5")
        assert "DFSHA_HEARTBEAT_INTERVAL_S" not in env


def test_the_script_leaves_what_the_client_needs_on_this_machine(run_script):
    result = run_script()

    assert result.returncode == 0, result.stderr
    client_env = (result.client / "cliente.env").read_text(encoding="utf-8").splitlines()
    assert "DFSHA_CONTROL_NODES=203.0.113.0:50051,203.0.113.1:50051,203.0.113.2:50051" in client_env
    assert f"DFSHA_CLIENT_SECRETS={result.client / 'cliente'}" in client_env
    assert (result.client / "cliente" / "ca.crt").read_text(encoding="utf-8").strip() == "CERTIFICADO"
    assert (result.client / "admin.password").read_text(encoding="utf-8").strip() == "clave-de-prueba"
    assert "clave-de-prueba" not in result.stdout  # la contraseña no se imprime
    if hasattr(os, "getuid"):
        assert stat.S_IMODE((result.client / "admin.password").stat().st_mode) == 0o600


def test_the_script_copies_the_secrets_only_to_the_instances_that_lack_them(run_script):
    first = run_script()
    assert first.returncode == 0, first.stderr
    assert (first.out / "copied").read_text(encoding="utf-8").split() == ["203.0.113.1", "203.0.113.2"]

    (first.out / "copied").unlink()
    again = run_script(FAKE_SAME_SECRETS="1")

    assert again.returncode == 0, again.stderr
    assert not (again.out / "copied").exists()  # ya tenían las mismas vistas que el nodo 0


def test_the_script_starts_an_instance_that_is_stopped(run_script):
    result = run_script(FAKE_STOPPED="1")

    assert result.returncode == 0, result.stderr
    assert "--instance-ids i-1" in (result.out / "started").read_text(encoding="utf-8")
    assert "dfsha-nodo-1 (i-1) estaba apagada: la enciendo" in result.stdout


def test_the_script_stops_if_an_instance_is_missing(run_script):
    result = run_script(FAKE_MISSING="2")

    assert result.returncode != 0
    assert result.stderr.strip() == (
        "tiene que haber exactamente una instancia llamada dfsha-nodo-2; encontré: ninguna"
    )
    assert not list(result.out.glob("env-*"))  # no configuró ninguna instancia


def test_the_script_stops_without_the_ssh_key(run_script, tmp_path):
    result = run_script(DFSHA_SSH_KEY=str(tmp_path / "no-existe.pem"))

    assert result.returncode != 0
    assert result.stderr.strip() == f"no encuentro la llave SSH en {tmp_path / 'no-existe.pem'} (DFSHA_SSH_KEY)"
