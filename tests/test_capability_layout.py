"""La clave de las capabilities en secrets/ y en docker-compose.yml (Hito 3, C3, T12,
variante A: archivo propio capability.key)."""

import importlib.util
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
_SCRIPT = _REPO / "scripts" / "generate_secrets.py"
_spec = importlib.util.spec_from_file_location("generate_secrets_capabilities", _SCRIPT)
generate_secrets = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generate_secrets)

_COMPOSE = _REPO / "docker-compose.yml"
NODE_VIEWS = ("dn1", "dn2", "dn3", "control")
CLIENT_VIEWS = ("client", "inspect")


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(f.relative_to(root)): f.read_bytes() for f in sorted(root.rglob("*")) if f.is_file()}


def _generate_all(path: Path) -> None:
    generate_secrets.generate(path)
    generate_secrets.generate_auth_secrets(path)
    generate_secrets.sync_mount_views(path)


def _compose_services() -> dict[str, str]:
    """Texto de cada servicio de docker-compose.yml, por nombre (mismo corte que
    tests/test_secrets_layout.py: PyYAML no es dependencia)."""
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


# --- secrets/ ----------------------------------------------------------------------------------


def test_the_capability_key_is_long_enough_and_only_the_nodes_see_it(tmp_path):
    path = tmp_path / "secrets"
    _generate_all(path)

    key = (path / "capability.key").read_bytes()
    assert len(key) >= 32
    for view in NODE_VIEWS:
        assert (path / "mounts" / view / "capability.key").read_bytes() == key, view
    for view in CLIENT_VIEWS:
        assert not (path / "mounts" / view / "capability.key").exists(), view
    assert all("capability.key" not in generate_secrets.MOUNT_VIEWS[view] for view in CLIENT_VIEWS)


def test_running_the_generator_twice_does_not_change_the_key(tmp_path):
    path = tmp_path / "secrets"
    _generate_all(path)
    before = _snapshot(path)

    _generate_all(path)

    assert _snapshot(path) == before


def test_over_the_secrets_of_c2_it_adds_the_key_without_touching_the_rest(tmp_path):
    # Un clúster que viene de C2 tiene todo menos capability.key: init lo agrega.
    path = tmp_path / "secrets"
    _generate_all(path)
    (path / "capability.key").unlink()
    for view in NODE_VIEWS:
        (path / "mounts" / view / "capability.key").unlink()
    before = _snapshot(path)

    _generate_all(path)

    after = _snapshot(path)
    added = set(after) - set(before)
    assert added == {"capability.key", *(str(Path("mounts") / view / "capability.key") for view in NODE_VIEWS)}
    assert {name: after[name] for name in before} == before


# --- docker-compose.yml ------------------------------------------------------------------------


def test_every_node_gets_the_capability_key_and_the_control_nodes_its_duration():
    services = _compose_services()

    for name in ("dn1", "dn2", "dn3"):
        assert "--capability-key-file, /secrets/capability.key" in services[name], name
    for name in ("cn0", "cn1", "cn2"):
        assert "--capability-key-file=/secrets/capability.key" in services[name], name
        assert "--capability-ttl-s=${DFSHA_CAPABILITY_TTL_S:-3600}" in services[name], name


def test_the_shell_and_the_inspector_do_not_name_the_capability_key():
    services = _compose_services()

    for name in ("shell", "inspect"):
        assert "capability" not in services[name], name
