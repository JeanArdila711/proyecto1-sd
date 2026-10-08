"""Secretos de autenticación y qué ve cada contenedor (Hito 3, C2)."""

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO / "scripts" / "generate_secrets.py"
_spec = importlib.util.spec_from_file_location("generate_secrets_layout", _SCRIPT)
generate_secrets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generate_secrets)

BASE_SECRETS = {"dn1.key", "dn2.key", "dn3.key", "raft.password", "ca.crt", "ca.key", "node.crt", "node.key"}
AUTH_SECRETS = {"jwt.secret", "admin.password"}
NODE_TLS = {"ca.crt", "node.crt", "node.key"}
EXPECTED_VIEWS = {
    "dn1": NODE_TLS | {"dn1.key"},
    "dn2": NODE_TLS | {"dn2.key"},
    "dn3": NODE_TLS | {"dn3.key"},
    "control": NODE_TLS | {"raft.password", "jwt.secret", "admin.password"},
    "client": {"ca.crt"},
    "inspect": {"ca.crt", "admin.password"},
}


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(f.relative_to(root)): f.read_bytes() for f in sorted(root.rglob("*")) if f.is_file()}


@pytest.fixture
def secrets_dir(tmp_path):
    path = tmp_path / "secrets"
    generate_secrets.generate(path)
    generate_secrets.generate_auth_secrets(path)
    generate_secrets.sync_mount_views(path)
    return path


def test_auth_secrets_are_created_once(tmp_path):
    path = tmp_path / "secrets"

    generate_secrets.generate_auth_secrets(path)
    before = _snapshot(path)
    generate_secrets.generate_auth_secrets(path)

    assert set(before) == AUTH_SECRETS
    assert len(before["jwt.secret"]) >= 32
    assert before["admin.password"].strip()
    assert _snapshot(path) == before


def test_each_view_holds_exactly_what_its_role_uses(secrets_dir):
    mounts = secrets_dir / "mounts"

    assert {view.name for view in mounts.iterdir()} == set(EXPECTED_VIEWS)
    for view, names in EXPECTED_VIEWS.items():
        assert {f.name for f in (mounts / view).iterdir()} == names
        for name in names:
            assert (mounts / view / name).read_bytes() == (secrets_dir / name).read_bytes()


def test_the_ca_key_is_in_no_view(secrets_dir):
    assert (secrets_dir / "ca.key").is_file()
    assert not list((secrets_dir / "mounts").rglob("ca.key"))


def test_syncing_again_changes_nothing_and_keeps_the_view_directories(secrets_dir):
    mounts = secrets_dir / "mounts"
    before = _snapshot(secrets_dir)
    inodes = {view: (mounts / view).stat().st_ino for view in EXPECTED_VIEWS}

    generate_secrets.sync_mount_views(secrets_dir)

    assert _snapshot(secrets_dir) == before
    if os.name == "posix":
        # un contenedor ya creado tiene montada ESA carpeta: no se puede reemplazar
        assert {view: (mounts / view).stat().st_ino for view in EXPECTED_VIEWS} == inodes


def test_a_changed_secret_reaches_its_views(secrets_dir):
    (secrets_dir / "admin.password").write_bytes(b"otra-clave")

    generate_secrets.sync_mount_views(secrets_dir)

    for view in ("control", "inspect"):
        assert (secrets_dir / "mounts" / view / "admin.password").read_bytes() == b"otra-clave"


def test_a_secret_that_no_longer_belongs_to_a_view_is_removed(secrets_dir):
    leaked = secrets_dir / "mounts" / "client" / "raft.password"
    leaked.write_bytes(b"no deberia estar")

    generate_secrets.sync_mount_views(secrets_dir)

    assert not leaked.exists()
    assert {f.name for f in leaked.parent.iterdir()} == {"ca.crt"}


def test_syncing_without_the_source_secrets_fails(tmp_path):
    path = tmp_path / "secrets"
    generate_secrets.generate(path)  # sin los de autenticación

    with pytest.raises(FileNotFoundError, match="jwt.secret"):
        generate_secrets.sync_mount_views(path)


def test_the_script_creates_everything_and_is_idempotent(tmp_path):
    path = tmp_path / "secrets"
    command = [sys.executable, str(_SCRIPT), "--dir", str(path)]

    first = subprocess.run(command, capture_output=True, text=True, check=False)
    before = _snapshot(path)
    second = subprocess.run(command, capture_output=True, text=True, check=False)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert {name for name in before if "mounts" not in Path(name).parts} == BASE_SECRETS | AUTH_SECRETS
    assert {Path(name).parts[1] for name in before if "mounts" in Path(name).parts} == set(EXPECTED_VIEWS)
    assert _snapshot(path) == before


# --- docker-compose.yml ---------------------------------------------------------------------
# Se lee como texto: PyYAML no es dependencia del proyecto y no se agrega una por un test.

_COMPOSE = _REPO / "docker-compose.yml"
COMPOSE_VIEWS = {
    "dn1": "dn1", "dn2": "dn2", "dn3": "dn3",
    "cn0": "control", "cn1": "control", "cn2": "control",
    "shell": "client", "inspect": "inspect",
}


def _compose_services() -> dict[str, str]:
    """Texto de cada servicio de docker-compose.yml, por nombre."""
    if not _COMPOSE.is_file():
        pytest.skip("docker-compose.yml no se copia a la imagen de Docker")
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


def test_only_init_mounts_the_whole_secrets_folder():
    services = _compose_services()

    whole = [name for name, text in services.items() if "./secrets:/secrets" in text]

    assert whole == ["init"]


def test_every_other_service_mounts_its_own_view_read_only():
    services = _compose_services()

    for service, view in COMPOSE_VIEWS.items():
        assert f"./secrets/mounts/{view}:/secrets:ro" in services[service], service
        assert services[service].count("./secrets") == 1, service


def test_compose_views_exist_in_the_generator_and_the_ca_key_is_never_mounted():
    assert set(COMPOSE_VIEWS.values()) == set(generate_secrets.MOUNT_VIEWS)
    assert all("ca.key" not in names for names in generate_secrets.MOUNT_VIEWS.values())
    if _COMPOSE.is_file():
        assert "ca.key" not in _COMPOSE.read_text(encoding="utf-8")


def test_compose_turns_authentication_on_in_the_three_control_nodes():
    services = _compose_services()

    for name in ("cn0", "cn1", "cn2"):
        assert "--jwt-secret-file=/secrets/jwt.secret" in services[name], name
        assert "--admin-password-file=/secrets/admin.password" in services[name], name
        assert "--token-ttl-s=" in services[name], name
    assert "/secrets/admin.password" in services["inspect"]
    # los secretos de autenticación no llegan a DataNodes ni a la shell
    for name in ("dn1", "dn2", "dn3", "shell"):
        assert "jwt.secret" not in services[name] and "admin.password" not in services[name], name
