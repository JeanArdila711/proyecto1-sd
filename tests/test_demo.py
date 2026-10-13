"""Demo guiada (scripts/demo.py): lo que se puede comprobar sin levantar Docker.

La demo entera se prueba corriéndola: `python scripts/demo.py --sin-pausas` termina con
código 1 si alguna de sus comprobaciones no se cumple. Acá se cubre cómo lee la salida de
la shell y del inspector, que es donde un cambio de formato la rompería en silencio. Los
textos de ejemplo son salidas reales del clúster.
"""

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "demo.py"


@pytest.fixture
def demo():
    spec = importlib.util.spec_from_file_location("demo_under_test", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._color = False
    return module


# --- la shell --------------------------------------------------------------------------------

_SESSION = (
    "DFSha shell — 'help' para ver comandos, 'exit' para salir.\n"
    "dfsha:/$ sesión iniciada como alice\n"
    "dfsha:/$ dfsha:/$ 87 bytes enviados a /alice/nota.txt\n"
    "dfsha:/$ -rw-r--r-- alice    alice            87  nota.txt\n"
    "dfsha:/$ linea 1: DFSha guarda este archivo en bloques\nlinea 2: y cada bloque en tres DataNodes\n"
    "dfsha:/$ "
)
_COMMANDS = ["login alice", "mkdir /alice", "send /intercambio/demo/nota.txt /alice/nota.txt", "ls -l /alice", "cat /alice/nota.txt"]


def test_each_command_is_paired_with_what_the_shell_answered(demo):
    pairs = dict(demo.split_session(_SESSION, _COMMANDS))

    assert pairs["login alice"] == "sesión iniciada como alice"
    assert pairs["mkdir /alice"] == ""  # un comando sin salida no desplaza a los demás
    assert pairs["send /intercambio/demo/nota.txt /alice/nota.txt"] == "87 bytes enviados a /alice/nota.txt"
    assert pairs["ls -l /alice"] == "-rw-r--r-- alice    alice            87  nota.txt"
    assert pairs["cat /alice/nota.txt"].splitlines() == [
        "linea 1: DFSha guarda este archivo en bloques", "linea 2: y cada bloque en tres DataNodes",
    ]


def test_the_prompt_is_recognized_in_any_directory(demo):
    stdout = "saludo\ndfsha:/$ dfsha:/docs$ -          0  a.txt\ndfsha:/docs/sub$ "

    assert demo.split_session(stdout, ["cd /docs", "ls"]) == [("cd /docs", ""), ("ls", "-          0  a.txt")]


def test_a_session_that_died_early_leaves_the_missing_answers_empty(demo):
    pairs = demo.split_session("saludo\ndfsha:/$ sesión iniciada como admin\n", ["login admin", "ls /", "mkdir /x"])

    assert pairs == [("login admin", "sesión iniciada como admin"), ("ls /", ""), ("mkdir /x", "")]


def test_the_passwords_go_to_the_shell_but_not_to_the_list_of_commands(demo):
    demo._passwords.update(admin="clave-admin", alice="clave-alice")

    text, names = demo.session_input("admin", ["whoami", ("adduser alice", ["clave-alice", "clave-alice"])])

    assert text == "login admin\nclave-admin\nwhoami\nadduser alice\nclave-alice\nclave-alice\nexit\n"
    assert names == ["login admin", "whoami", "adduser alice"]


def test_a_session_without_user_does_not_log_in(demo):
    assert demo.session_input(None, ["ls /"]) == ("ls /\nexit\n", ["ls /"])


def test_a_successful_login_is_summarized_and_a_failed_one_is_shown(demo, capsys):
    demo.show_session("alice", [("login alice", "sesión iniciada como alice"), ("ls /", "d  0  docs")])
    quiet = capsys.readouterr().out
    demo.show_session("admin", [("login admin", "login: no se pudo confirmar el liderazgo para leer")])
    failed = capsys.readouterr().out
    demo.show_session("admin", [("login admin", "sesión iniciada como admin")], show_login=True)
    shown = capsys.readouterr().out

    assert "con la sesión iniciada" in quiet and "login alice" not in quiet and "dfsha$ ls /" in quiet
    assert "dfsha$ login admin" in failed and "no se pudo confirmar el liderazgo" in failed
    assert "dfsha$ login admin" in shown and "(contraseña oculta)" in shown


def test_no_password_is_ever_printed(demo, capsys):
    demo._passwords.update(admin="clave-admin-secreta", alice="clave-alice-secreta")
    _, names = demo.session_input("admin", [("adduser alice", ["clave-alice-secreta"] * 2)])

    demo.show_session("admin", demo.split_session("s\ndfsha:/$ sesión iniciada como admin\ndfsha:/$ usuario alice creado\n", names), True)

    assert "secreta" not in capsys.readouterr().out


# --- el inspector ----------------------------------------------------------------------------

_ESTADO = """
CONTROLNODES (clúster Raft)
    cn0  cn0:50051              seguidor
    cn1  cn1:50051              CAÍDO
  ★ cn2  cn2:50051              LIDER

DATANODES
    dn1  dn1:50061              vivo
    dn2  dn2:50061              CAÍDO
"""

_BLOQUES = """/docs/chico.bin  →  293.0 KB en 1 bloque(s)

    #  block_id      tamaño   réplicas (orden de pipeline) → estado verificado por SHA-256
  ────────────────────────────────────────────────────────────────────────────────────────────────
  b0   4fdfadc5    293.0 KB   dn4[cabeza]=CORRUPTO  dn5[réplica 2]=ok  dn1[réplica 3]=NO TIENE el bloque
  b1   7b76955e      1.0 MB   dn2[cabeza]=ok  dn3[réplica 2]=ok

  checksum registrado del b0: 28ec62d1afe0845bef1af10d9623b386d7d3ef1fd3fa3e0e5404bb3d475f7af3
"""

_HUERFANOS = """Bloques en uso por archivos visibles: 6   (gracia del recolector: 5 s)

  dn1         2 en disco      0 en uso      0 sin uso visible y jóvenes      2 a borrar (2.0 MB)
  dn4         6 en disco      6 en uso      0 sin uso visible y jóvenes      0 a borrar (0 B)
  dn5      no responde (UNAVAILABLE)

  Total a borrar en el próximo ciclo del recolector: 2
"""


def test_the_state_of_each_node_is_read_from_inspect_estado(demo):
    assert demo.parse_estado(_ESTADO) == {
        "cn0": "seguidor", "cn1": "CAÍDO", "cn2": "LIDER", "dn1": "vivo", "dn2": "CAÍDO",
    }


def test_a_leader_without_majority_is_not_taken_for_a_leader(demo):
    states = demo.parse_estado("  ★ cn0  cn0:50051              líder SIN mayoría\n")

    assert states == {"cn0": "líder SIN mayoría"} and "LIDER" not in states.values()


def test_the_replicas_of_each_block_are_read_from_inspect_bloques(demo):
    first, second = demo.parse_bloques(_BLOQUES)

    assert (first["index"], first["id"]) == (0, "4fdfadc5")
    assert first["replicas"] == [
        ("dn4", "cabeza", "CORRUPTO"), ("dn5", "réplica 2", "ok"), ("dn1", "réplica 3", "NO TIENE el bloque"),
    ]
    assert second["replicas"] == [("dn2", "cabeza", "ok"), ("dn3", "réplica 2", "ok")]  # un bloque con 2 réplicas


def test_the_blocks_on_disk_and_in_use_are_read_from_inspect_huerfanos(demo):
    # un DataNode que no responde no aparece: quien espera por él sigue esperando
    assert demo.parse_huerfanos(_HUERFANOS) == {"dn1": (2, 0), "dn4": (6, 6)}


# --- no tocar el clúster de quien la corre ---------------------------------------------------


def test_every_docker_command_targets_the_project_of_the_demo(demo):
    assert demo.compose_command("down", "-v") == ["docker", "compose", "-p", "dfsha-demo", "down", "-v"]
    source = _SCRIPT.read_text(encoding="utf-8")
    # `docker` se invoca en un solo lugar, compose_command; el resto son textos que se muestran
    assert len(re.findall(r'\[\s*"docker"', source)) == 1
    assert "subprocess.run(\n        compose_command(" in source and "subprocess.Popen(\n        compose_command(" in source


def test_the_demo_cluster_has_five_datanodes_and_demo_timings(demo):
    assert demo.DEMO_ENV["COMPOSE_PROFILES"] == "escala"
    assert demo.DEMO_ENV["DFSHA_DATANODE_COUNT"] == str(len(demo.DATANODES)) == "5"
    assert demo.DEMO_ENV["DFSHA_DATANODES"] == "dn1:50061,dn2:50061,dn3:50061,dn4:50061,dn5:50061"
    assert demo.DEMO_ENV["DFSHA_BLOCK_MB"] == "1"


def test_a_failed_check_is_remembered_and_a_passed_one_is_counted(demo, capsys):
    assert demo.check("esto se cumple", True) is True
    assert demo.check("esto no", False) is False

    out = capsys.readouterr().out
    assert (demo._passed, demo._failed) == (1, ["esto no"])
    assert "✔ esto se cumple" in out and "✘ NO SE CUMPLIÓ: esto no" in out


def test_the_script_explains_itself():
    result = subprocess.run([sys.executable, str(_SCRIPT), "--help"], capture_output=True, text=True, check=False)

    assert result.returncode == 0
    assert "--sin-pausas" in result.stdout and "--conservar" in result.stdout
