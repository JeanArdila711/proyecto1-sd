"""Demo guiada de DFSha: recorre el sistema completo, paso a paso, con comandos reales.

    python scripts/demo.py                 # con pausas: Enter para pasar al paso siguiente
    python scripts/demo.py --sin-pausas    # de corrido; sirve como prueba de punta a punta
    python scripts/demo.py --conservar     # deja el clúster de la demo encendido al terminar

Solo necesita Docker y Python 3.10 o más nuevo. No toca el clúster que tengas levantado ni
sus datos: usa un proyecto de Compose aparte (`dfsha-demo`), con 5 DataNodes, bloques de
1 MB y fallos que se detectan y reparan en segundos, y lo borra al terminar. Comparte con
tu clúster la carpeta `secrets/` (no cambia nada de lo que ya exista ahí) y deja sus
archivos de trabajo en `intercambio/demo/`, que también borra.

Cada paso muestra el comando, su salida real y una comprobación (✔ o ✘). Si alguna
comprobación falla, el script termina con código 1.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PROJECT = "dfsha-demo"
DATANODES = [f"dn{i}" for i in range(1, 6)]
CONTROLNODES = ["cn0", "cn1", "cn2"]

# Lo mismo que .env.escala más los valores de demo de docs/GUIA.md, sin editar ningún archivo.
DEMO_ENV = {
    "COMPOSE_PROFILES": "escala",
    "DFSHA_DATANODE_COUNT": "5",
    "DFSHA_DATANODES": ",".join(f"{name}:50061" for name in DATANODES),
    "DFSHA_BLOCK_MB": "1",
    "DFSHA_HEARTBEAT_INTERVAL_S": "0.5",
    "DFSHA_DATANODE_DEAD_AFTER_S": "2",
    "DFSHA_REREPLICATION_INTERVAL_S": "1",
    "DFSHA_REREPLICATION_DELAY_S": "2",
    "DFSHA_GC_INTERVAL_S": "5",
    "DFSHA_GC_GRACE_S": "5",
}

PROMPT = re.compile(r"dfsha:/[^$\n]*\$ ")
_REPLICA = re.compile(r"(dn\d+)\[([^\]]+)\]=(.*?)(?=\s{2,}dn\d+\[|$)")

_color = sys.stdout.isatty() and "NO_COLOR" not in os.environ
_pause = True
_passed = 0
_failed: list[str] = []
# La de admin sale de secrets/admin.password; las de alice y bob se inventan en cada corrida.
# Ninguna se imprime.
_passwords: dict[str, str] = {name: secrets.token_urlsafe(12) for name in ("alice", "bob")}


def paint(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _color else text


# --- Docker ----------------------------------------------------------------------------------


def compose_command(*args: str) -> list[str]:
    """Todo pasa por acá: siempre el proyecto de la demo, nunca el que tengas levantado."""
    return ["docker", "compose", "-p", PROJECT, *args]


def compose(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        compose_command(*args), cwd=REPO, env={**os.environ, **DEMO_ENV}, input=stdin,
        capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
    )


def show_command(command: str) -> None:
    print(paint("36", f"  $ {command}"))


def show_output(text: str) -> None:
    for line in text.strip("\n").splitlines():
        print(f"    {line}")


def run(*args: str, shown: str | None = None) -> str:
    """Corre `docker compose <args>` en el clúster de la demo y muestra comando y salida."""
    show_command(shown or "docker compose " + " ".join(args))
    result = compose(*args)
    output = result.stdout if result.returncode == 0 else result.stdout + result.stderr
    show_output(output)
    return output


def inspect(*args: str, quiet: bool = False) -> str:
    if quiet:
        return compose("run", "--rm", "-T", "inspect", *args).stdout
    return run("run", "--rm", "-T", "inspect", *args, shown="docker compose run --rm inspect " + " ".join(args))


def tool(code: str) -> subprocess.CompletedProcess:
    """Python suelto dentro de la imagen, con ./intercambio montado: los archivos de la demo
    se crean, comparan y borran ahí, con el usuario del contenedor, en cualquier sistema."""
    return compose("run", "--rm", "-T", "--no-deps", "--entrypoint", "python", "shell", "-c", code)


# --- shell de DFSha --------------------------------------------------------------------------


def split_session(stdout: str, commands: list[str]) -> list[tuple[str, str]]:
    """Empareja cada comando con lo que respondió la shell. La shell imprime su prompt antes
    de leer cada línea, así que lo que hay entre dos prompts es la salida de un comando."""
    chunks = PROMPT.split(stdout)[1:]  # lo anterior al primer prompt es el saludo
    return [(command, chunks[i].strip("\n") if i < len(chunks) else "") for i, command in enumerate(commands)]


def session_input(user: str | None, commands: list) -> tuple[str, list[str]]:
    """El texto que se le escribe a la shell y la lista de comandos, sin las contraseñas."""
    lines, names = [], []
    if user:
        lines += [f"login {user}", _passwords[user]]
        names.append(f"login {user}")
    for item in commands:
        command, answers = (item, []) if isinstance(item, str) else item
        lines += [command, *answers]
        names.append(command)
    return "\n".join([*lines, "exit"]) + "\n", names


def show_session(who: str, pairs: list[tuple[str, str]], show_login: bool = False) -> None:
    """Un inicio de sesión que salió bien se resume en el encabezado, salvo que sea el tema
    del paso. Uno que falló se muestra siempre."""
    logged_in = bool(pairs) and pairs[0][0].startswith("login ") and "sesión iniciada" in pairs[0][1]
    if logged_in and not show_login:
        pairs = pairs[1:]
    print(paint("2", f"  shell de {who}" + (", con la sesión iniciada:" if logged_in and not show_login else ":")))
    for command, output in pairs:
        hidden = "   (contraseña oculta)" if command.split()[0] in ("login", "adduser", "passwd") else ""
        print(paint("36", f"  dfsha$ {command}") + paint("2", hidden))
        show_output(output)


def shell(user: str | None, commands: list, show_login: bool = False) -> list[str]:
    """Abre una shell de DFSha, entra como `user`, corre los comandos y devuelve la salida
    de cada uno. Un comando que pide algo por teclado va como (comando, [respuestas])."""
    text, names = session_input(user, commands)
    result = compose("run", "--rm", "-T", "shell", stdin=text)
    pairs = split_session(result.stdout, names)
    show_session(user or "alguien sin sesión", pairs, show_login)
    return [output for _, output in pairs][1 if user else 0:]


# --- lectura de la salida del inspector ------------------------------------------------------


def parse_estado(text: str) -> dict[str, str]:
    """{'cn0': 'LIDER', 'dn2': 'CAÍDO', ...} a partir de `inspect estado`."""
    states = {}
    for line in text.splitlines():
        match = re.match(r"\s*(?:★\s*)?((?:cn|dn)\d+)\s+\S+\s+(.+?)\s*$", line)
        if match:
            states[match.group(1)] = match.group(2)
    return states


def parse_bloques(text: str) -> list[dict]:
    """Una entrada por bloque de `inspect bloques`: su id corto y [(nodo, rol, estado)]."""
    blocks = []
    for line in text.splitlines():
        match = re.match(r"\s*b(\d+)\s+([0-9a-f]{8})\s", line)
        if match:
            replicas = [(m.group(1), m.group(2), m.group(3).strip()) for m in _REPLICA.finditer(line)]
            blocks.append({"index": int(match.group(1)), "id": match.group(2), "replicas": replicas})
    return blocks


def parse_huerfanos(text: str) -> dict[str, tuple[int, int]]:
    """{'dn2': (en disco, en uso)} a partir de `inspect huerfanos`."""
    counts = {}
    for line in text.splitlines():
        match = re.match(r"\s*(dn\d+)\s+(\d+) en disco\s+(\d+) en uso", line)
        if match:
            counts[match.group(1)] = (int(match.group(2)), int(match.group(3)))
    return counts


# --- narración -------------------------------------------------------------------------------


def step(number: int, total: int, title: str, *explanation: str) -> None:
    if _pause and number > 1:
        try:
            input(paint("2", "\n  Enter para seguir… "))
        except EOFError:
            pass
    print("\n" + paint("1;33", f"━━ {number} de {total} · {title} " + "━" * max(3, 70 - len(title))))
    for line in explanation:
        print(f"  {line}")
    print()


def check(description: str, ok: bool) -> bool:
    global _passed
    print(paint("32", f"  ✔ {description}") if ok else paint("1;31", f"  ✘ NO SE CUMPLIÓ: {description}"))
    if ok:
        _passed += 1
    else:
        _failed.append(description)
    return ok


def wait_until(what: str, probe, timeout: float = 90.0, every: float = 1.0):
    """Repite `probe` hasta que devuelva algo verdadero. None si se acaba el tiempo."""
    print(paint("2", f"  … esperando: {what}"))
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value or time.monotonic() >= deadline:
            return value or None
        time.sleep(every)


def same_file(a: str, b: str) -> bool:
    code = f"import filecmp, sys; sys.exit(0 if filecmp.cmp({a!r}, {b!r}, shallow=False) else 1)"
    return tool(code).returncode == 0


# --- los pasos -------------------------------------------------------------------------------

TOTAL = 12
DEMO_DIR = "/intercambio/demo"
_PREPARE = f"""
import pathlib, random
d = pathlib.Path({DEMO_DIR!r}); d.mkdir(exist_ok=True)
for old in d.iterdir(): old.unlink()
(d / "informe.bin").write_bytes(random.Random(2026).randbytes(3_500_000))
(d / "chico.bin").write_bytes(random.Random(7).randbytes(300_000))
(d / "nota.txt").write_text("linea 1: DFSha guarda este archivo en bloques\\nlinea 2: y cada bloque en tres DataNodes\\n", encoding="utf-8")
(d / "parche.txt").write_text("LINEA", encoding="utf-8")
"""
_CLEAN = f"""
import pathlib, shutil
shutil.rmtree({DEMO_DIR!r}, ignore_errors=True)
"""


def start_cluster() -> None:
    print(paint("1", "Preparando el clúster de la demo (proyecto de Compose aparte, no toca el tuyo)…"))
    compose("down", "-v", "--remove-orphans")
    result = compose("up", "-d", "--build")
    if result.returncode != 0:
        sys.exit("no se pudo levantar el clúster de la demo:\n" + result.stderr[-1500:])

    def all_healthy() -> bool:
        status = compose("ps", "--format", "{{.Service}} {{.Status}}").stdout
        return sum("(healthy)" in line for line in status.splitlines()) == len(DATANODES) + len(CONTROLNODES)

    if not wait_until("los 8 nodos sanos", all_healthy, timeout=180, every=2):
        sys.exit("los nodos no quedaron sanos:\n" + compose("ps", "-a").stdout)
    if tool(_PREPARE).returncode != 0:
        sys.exit("no se pudieron crear los archivos de la demo en intercambio/demo")
    password = compose("run", "--rm", "-T", "--no-deps", "--entrypoint", "cat", "inspect", "/secrets/admin.password").stdout.strip()
    if not password:
        sys.exit("no se pudo leer la contraseña inicial del admin")
    _passwords["admin"] = password
    wait_until("que haya líder", lambda: "LIDER" in parse_estado(inspect("estado", quiet=True)).values(), timeout=60)


def the_cluster() -> None:
    step(1, TOTAL, "El clúster", "8 contenedores: 3 ControlNodes (metadatos, con Raft) y 5 DataNodes (bloques).")
    states = parse_estado(inspect("estado"))
    check("hay un líder y dos seguidores", sorted(states[n] for n in CONTROLNODES) == ["LIDER", "seguidor", "seguidor"])
    check("los 5 DataNodes están vivos", all(states.get(n) == "vivo" for n in DATANODES))


def users() -> None:
    step(2, TOTAL, "Usuarios y sesión", "Sin iniciar sesión no se puede hacer nada. Solo un admin crea usuarios.")
    [refused] = shell(None, ["ls /"])
    check("sin sesión, `ls` se rechaza", "sesión" in refused.lower() or "login" in refused.lower())
    outputs = shell("admin", [
        "whoami",
        ("adduser alice", [_passwords["alice"]] * 2),
        ("adduser bob", [_passwords["bob"]] * 2),
    ], show_login=True)
    check("el admin creó a alice y a bob", "alice" in outputs[1] and "bob" in outputs[2])
    [denied] = shell("bob", [("adduser otro", ["x-12345678"] * 2)])
    check("bob no puede crear usuarios", "solo un admin" in denied)


def files() -> None:
    step(3, TOTAL, "Directorios y archivos (RF1 y RF2)", "mkdir, ls, rmdir, rm, y subir y bajar un archivo de 3,5 MB.")
    outputs = shell("admin", [
        "mkdir /docs",
        "mkdir /borrador",
        f"send {DEMO_DIR}/informe.bin /docs/informe.bin",
        f"send {DEMO_DIR}/chico.bin /borrador/chico.bin",
        "ls -l /docs",
        "rm /borrador/chico.bin",
        "rmdir /borrador",
        "ls /",
        f"receive /docs/informe.bin {DEMO_DIR}/informe-bajado.bin",
    ])
    check("el archivo quedó en /docs con sus 3.500.000 bytes", "3500000" in outputs[4] and "informe.bin" in outputs[4])
    check("/borrador ya no existe", "borrador" not in outputs[7] and "docs" in outputs[7])
    check("lo que bajó es idéntico a lo que subió", same_file(f"{DEMO_DIR}/informe.bin", f"{DEMO_DIR}/informe-bajado.bin"))


def partitioning() -> None:
    step(4, TOTAL, "Particionamiento y replicación",
         "El archivo se partió en bloques de 1 MB. Cada bloque está en 3 DataNodes distintos;",
         "en el mapa, C es la cabeza del pipeline y r una réplica.")
    inspect("mapa")
    blocks = parse_bloques(inspect("bloques", "/docs/informe.bin"))
    check("son 4 bloques", len(blocks) == 4)
    check("cada bloque tiene 3 réplicas y todas verifican su SHA-256",
          bool(blocks) and all(len(b["replicas"]) == 3 and all(s == "ok" for _, _, s in b["replicas"]) for b in blocks))
    used = {node for b in blocks for node, _, _ in b["replicas"]}
    check("los bloques se repartieron entre los 5 DataNodes", used == set(DATANODES))


_DIRECT_CALL = """
import grpc
from pathlib import Path
from dfsha.common.tls import channel_factory, load_tls
from dfsha.generated import data_node_pb2, data_node_pb2_grpc
stub = data_node_pb2_grpc.DataNodeServiceStub(channel_factory(load_tls(Path("/secrets/ca.crt")))("dn1:50061"))
try:
    blocks = list(stub.ListStoredBlocks(data_node_pb2.ListStoredBlocksRequest(), timeout=10))
    print(f"el DataNode respondio sin capability: {len(blocks)} bloques")
except grpc.RpcError as exc:
    print(f"{exc.code().name}: {exc.details()}")
"""


def encryption() -> None:
    step(5, TOTAL, "Cifrado y acceso directo a un DataNode",
         "En el disco de un DataNode no está el archivo: hay contenedores cifrados con AES-256-GCM.",
         "Entre nodos y con el cliente todo viaja por TLS, y Raft va cifrado con su propia clave.",
         "Y un DataNode no atiende a quien le hable directo, sin un permiso firmado por el ControlNode.")
    head = run("exec", "-T", "dn1", "sh", "-c", 'f=$(ls /data | head -1); head -c 5 /data/$f; echo',
               shown="docker compose exec dn1 sh -c 'f=$(ls /data | head -1); head -c 5 /data/$f; echo'")
    check("el bloque en disco empieza con la marca del contenedor cifrado (DFSE1)", head.strip() == "DFSE1")
    show_command("docker compose logs cn0 | grep escuchando")
    line = next((l for l in compose("logs", "cn0").stdout.splitlines() if "escuchando" in l), "")
    show_output("… " + line[line.find("TLS="):])  # la línea entera lista todos los nodos; acá importa el final
    check("el ControlNode arrancó con TLS, Raft cifrado, autenticación y capabilities",
          all(flag in line for flag in ("TLS=True", "Raft cifrado=True", "autenticación=True", "capabilities=True")))
    show_command("python: pedirle a dn1 su lista de bloques, por TLS pero sin capability")
    answer = tool(_DIRECT_CALL).stdout
    show_output(answer)
    check("el DataNode rechaza la llamada directa", answer.startswith("PERMISSION_DENIED"))


def permissions() -> None:
    step(6, TOTAL, "Permisos por archivo", "Dueño, grupo y modo, como en Unix. Alice cierra su archivo y bob deja de poder leerlo.")
    shell("alice", ["mkdir /alice", f"send {DEMO_DIR}/nota.txt /alice/nota.txt", "ls -l /alice"])
    [readable] = shell("bob", ["cat /alice/nota.txt"])
    check("con modo 644, bob puede leer el archivo de alice", "linea 1" in readable)
    shell("alice", ["chmod 600 /alice/nota.txt", "ls -l /alice"])
    denied = shell("bob", ["cat /alice/nota.txt", "rm /alice/nota.txt", "chmod 644 /alice/nota.txt"])
    check("con modo 600, bob ya no puede leerlo, borrarlo ni cambiarle el modo", all("permiso denegado" in out for out in denied))


def ranges() -> None:
    step(7, TOTAL, "Leer y escribir por rangos (RF3)",
         "read trae solo los bytes pedidos, aunque crucen de un bloque a otro. write cambia un tramo",
         "sin volver a subir el archivo: escribe bloques nuevos y los publica juntos.")
    outputs = shell("alice", [
        "cat /alice/nota.txt 0 7",
        f"write /alice/nota.txt 0 {DEMO_DIR}/parche.txt",
        "cat /alice/nota.txt",
        "chmod 644 /alice/nota.txt",
    ])
    check("cat con rango devolvió solo los primeros 7 bytes", outputs[0].strip() == "linea 1")
    check("write reemplazó 5 bytes al inicio y dejó el resto igual", outputs[2].startswith("LINEA 1: DFSha") and "linea 2" in outputs[2])
    shell("admin", [f"read /docs/informe.bin 1048000 5000 {DEMO_DIR}/rango.bin"])
    code = (f"import sys; whole = open({DEMO_DIR!r} + '/informe.bin', 'rb').read(); "
            f"sys.exit(0 if open({DEMO_DIR!r} + '/rango.bin', 'rb').read() == whole[1048000:1053000] else 1)")
    check("los 5.000 bytes leídos (que cruzan del bloque 0 al 1) son los del original", tool(code).returncode == 0)


def locks() -> None:
    step(8, TOTAL, "Locks (RF3)", "Una shell toma un lock de lectura y lo mantiene; otra intenta escribir y borrar.")
    text, _ = session_input("alice", ["lock /alice/nota.txt r"])
    holder = subprocess.Popen(
        compose_command("run", "--rm", "-T", "shell"), cwd=REPO, env={**os.environ, **DEMO_ENV},
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, encoding="utf-8",
    )
    seen: list[str] = []
    reader = threading.Thread(target=lambda: seen.extend(iter(lambda: holder.stdout.read(1), "")), daemon=True)
    reader.start()
    holder.stdin.write(text.removesuffix("exit\n"))
    holder.stdin.flush()
    # banner, login y lock: al tercer prompt la primera shell ya tiene el lock
    held = wait_until("que la primera shell tenga el lock", lambda: len(PROMPT.findall("".join(seen))) >= 3, timeout=60, every=0.3)
    show_session("alice (esta shell se queda abierta)", split_session("".join(seen), ["login alice", "lock /alice/nota.txt r"]))
    check("la primera shell tomó el lock de lectura", bool(held))
    blocked = shell("bob", ["cat /alice/nota.txt"])
    check("otro lector sí puede leer mientras tanto", "LINEA 1" in blocked[0])
    blocked = shell("alice", [f"write /alice/nota.txt 0 {DEMO_DIR}/parche.txt", "rm /alice/nota.txt"])
    check("escribir y borrar se rechazan mientras el lock está tomado", all("lock" in out.lower() for out in blocked))
    holder.stdin.write("unlock /alice/nota.txt\nexit\n")
    holder.stdin.flush()
    holder.wait(timeout=60)
    reader.join(timeout=10)
    print(paint("2", "  la primera shell hizo unlock y salió"))
    [written] = shell("alice", [f"write /alice/nota.txt 0 {DEMO_DIR}/parche.txt"])
    check("liberado el lock, la escritura funciona", "escritos" in written)


def integrity() -> None:
    step(9, TOTAL, "Un bloque dañado en disco",
         "Se cambia un byte de una réplica directamente en el disco de un DataNode. Cada bloque va",
         "autenticado: el DataNode lo detecta al leer y el cliente baja de otra réplica.")
    shell("admin", [f"send {DEMO_DIR}/chico.bin /docs/chico.bin"])
    [block] = parse_bloques(inspect("bloques", "/docs/chico.bin", quiet=True))
    node = block["replicas"][0][0]
    script = (f'f=$(ls /data | grep "^{block["id"]}" | head -1); '
              'printf "\\377" | dd of=/data/$f bs=1 seek=100 count=1 conv=notrunc 2>/dev/null; echo "byte 100 cambiado en $f"')
    run("exec", "-T", node, "sh", "-c", script, shown=f"docker compose exec {node} sh -c '… cambia el byte 100 del bloque {block['id']}…'")
    [after] = parse_bloques(inspect("bloques", "/docs/chico.bin"))
    statuses = dict((n, s) for n, _, s in after["replicas"])
    check(f"la réplica de {node} sale CORRUPTO y las otras dos siguen ok",
          statuses.get(node) == "CORRUPTO" and sorted(statuses.values()) == ["CORRUPTO", "ok", "ok"])
    shell("admin", [f"receive /docs/chico.bin {DEMO_DIR}/chico-bajado.bin"])
    check("el archivo baja idéntico, desde una réplica sana", same_file(f"{DEMO_DIR}/chico.bin", f"{DEMO_DIR}/chico-bajado.bin"))


def datanode_failure() -> None:
    step(10, TOTAL, "Cae un DataNode",
         "Se apaga un DataNode que tiene réplicas del archivo. El líder lo nota, el archivo se sigue",
         "leyendo, y las copias que faltan se reponen solas en otros DataNodes.")
    victim = parse_bloques(inspect("bloques", "/docs/informe.bin", quiet=True))[0]["replicas"][0][0]
    run("stop", victim)
    down = wait_until(f"que el líder dé a {victim} por caído", lambda: parse_estado(inspect("estado", quiet=True)).get(victim) == "CAÍDO")
    inspect("estado")
    check(f"{victim} aparece CAÍDO", bool(down))
    shell("admin", [f"receive /docs/informe.bin {DEMO_DIR}/informe-sin-un-nodo.bin"])
    check(f"con {victim} apagado, el archivo baja idéntico", same_file(f"{DEMO_DIR}/informe.bin", f"{DEMO_DIR}/informe-sin-un-nodo.bin"))

    def repaired():
        blocks = parse_bloques(inspect("bloques", "/docs/informe.bin", quiet=True))
        return blocks and all(
            len(b["replicas"]) == 3 and all(n != victim and s == "ok" for n, _, s in b["replicas"]) for b in blocks
        )

    fixed = wait_until("la re-replicación", repaired)
    inspect("bloques", "/docs/informe.bin")
    check(f"cada bloque volvió a tener 3 réplicas sanas, ninguna en {victim}", bool(fixed))

    run("start", victim)
    print(f"  {victim} vuelve con sus copias viejas, que ya sobran: el recolector las borra.")

    def collected():
        counts = parse_huerfanos(inspect("huerfanos", quiet=True))
        return victim in counts and counts[victim][0] == counts[victim][1]

    gone = wait_until(f"que el recolector limpie {victim}", collected)
    inspect("huerfanos")
    check(f"en {victim} no queda ningún bloque sin uso", bool(gone))


def leader_failure() -> None:
    step(11, TOTAL, "Cae el líder",
         "Se apaga el ControlNode líder. Los otros dos eligen uno nuevo y todo sigue funcionando,",
         "sin que el cliente tenga que saber cuál es.")
    leader = inspect("lider").strip().splitlines()[-1]
    run("stop", leader)

    def new_leader():
        states = parse_estado(inspect("estado", quiet=True))
        return next((n for n in CONTROLNODES if n != leader and states.get(n) == "LIDER"), None)

    elected = wait_until("la elección de un líder nuevo", new_leader)
    inspect("estado")
    check(f"hay un líder nuevo ({elected}) y {leader} aparece caído", bool(elected))
    outputs = shell("admin", ["ls -l /docs", f"send {DEMO_DIR}/chico.bin /docs/tras-la-eleccion.bin", "ls /docs"])
    check("se puede listar y subir con el líder nuevo", "informe.bin" in outputs[0] and "tras-la-eleccion.bin" in outputs[2])
    run("start", leader)
    back = wait_until(f"que {leader} vuelva", lambda: parse_estado(inspect("estado", quiet=True)).get(leader) == "seguidor")
    check(f"{leader} volvió como seguidor", bool(back))


def no_majority() -> None:
    step(12, TOTAL, "Sin mayoría",
         "Se apagan 2 de los 3 ControlNodes. Sin mayoría el clúster deja de responder en vez de",
         "arriesgarse a dar datos viejos o contradictorios: consistencia antes que disponibilidad.",
         "(El cliente insiste unos 15 segundos antes de rendirse.)")
    stopped = CONTROLNODES[:2]
    run("stop", *stopped)
    refused = login_without_majority()
    check("sin mayoría no se puede ni iniciar sesión", bool(refused))
    run("start", *stopped)
    wait_until("que vuelva a haber líder", lambda: "LIDER" in parse_estado(inspect("estado", quiet=True)).values())
    [listing] = shell("admin", ["ls /docs"])
    check("con mayoría otra vez, todo sigue ahí", "informe.bin" in listing and "tras-la-eleccion.bin" in listing)


def login_without_majority() -> str:
    """Una shell contra un clúster sin mayoría: devuelve lo que respondió el login, o vacío
    si el login funcionó (que acá sería el error)."""
    text, names = session_input("admin", [])
    pairs = split_session(compose("run", "--rm", "-T", "shell", stdin=text).stdout, names)
    show_session("admin", pairs)
    return "" if "sesión iniciada" in pairs[0][1] else pairs[0][1]


def main() -> None:
    global _pause
    parser = argparse.ArgumentParser(description="Demo guiada de DFSha, en un clúster propio que se borra al terminar")
    parser.add_argument("--sin-pausas", action="store_true", help="no espera Enter entre pasos")
    parser.add_argument("--conservar", action="store_true", help="no apaga ni borra el clúster de la demo al terminar")
    args = parser.parse_args()
    _pause = not args.sin_pausas
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if os.name == "nt":
        os.system("")  # activa los colores ANSI en la consola de Windows

    started = time.monotonic()
    try:
        start_cluster()
        the_cluster()
        users()
        files()
        partitioning()
        encryption()
        permissions()
        ranges()
        locks()
        integrity()
        datanode_failure()
        leader_failure()
        no_majority()
    finally:
        if args.conservar:
            print(paint("2", f"\nEl clúster de la demo sigue encendido. Para borrarlo: docker compose -p {PROJECT} down -v"))
        else:
            tool(_CLEAN)
            compose("down", "-v", "--remove-orphans")

    minutes = (time.monotonic() - started) / 60
    if _failed:
        print(paint("1;31", f"\n{len(_failed)} comprobación(es) no se cumplieron:"))
        for description in _failed:
            print(f"  ✘ {description}")
        sys.exit(1)
    print(paint("1;32", f"\nDemo completa: las {_passed} comprobaciones se cumplieron ({minutes:.1f} min)."))


if __name__ == "__main__":
    main()
