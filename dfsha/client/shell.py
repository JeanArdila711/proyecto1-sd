from __future__ import annotations

import getpass
import posixpath
import re
import shlex
from pathlib import Path

from dfsha.common.exceptions import AuthError, DFShaError

_HELP_TEXT = """Comandos disponibles:
  ls [ruta]                  lista un directorio (por defecto, el actual)
  cd <ruta>                  cambia el directorio actual
  pwd                        muestra el directorio actual
  mkdir <ruta>                crea un directorio
  rmdir <ruta>                elimina un directorio vacío
  rm <ruta>                   elimina un archivo
  send <local> <remota>       sube un archivo local al DFS
  receive <remota> <local>    descarga un archivo del DFS
  cat <ruta> [offset] [largo] muestra el archivo, o un rango de bytes, como texto
  read <ruta> <offset> <largo> <local>
                              guarda un rango de bytes del archivo en un archivo local
  write <ruta> <offset> <local>
                              escribe el contenido de un archivo local desde un offset
  open <ruta> r|w             abre un handle (toma el lock); close <ruta> lo cierra
  lock <ruta> r|w             toma un lock con lease
  unlock <ruta>               libera un lock propio de la ruta
  locks                       lista los locks propios
  login <usuario>             inicia sesión (pide la contraseña aparte)
  whoami                      muestra el usuario de la sesión, sus grupos y si es admin
  adduser <usuario> [--admin] [grupo ...]
                              crea un usuario (solo admin; pide su contraseña dos veces)
  passwd [usuario]            cambia la contraseña propia, o la de otro si eres admin
  ls -l [ruta]                lista con modo, dueño, grupo y tamaño
  chmod <modo> <ruta>         cambia el modo, en octal (640); solo el dueño o un admin
  chown <usuario>[:<grupo>] <ruta>
                              cambia el dueño (solo admin) o el grupo (chown :<grupo> <ruta>)
  help                        muestra esta ayuda
  exit / quit                 termina la sesión

Las rutas con espacios van entre comillas: send "C:\\mis docs\\a.pdf" a.pdf"""

# Comandos de RF3 y el método que necesitan. La shell es compartida con el cliente
# monolítico de Hito 1, que no los tiene: sin este chequeo, el AttributeError
# tumbaría la shell en vez de avisar.
_RF3_COMMANDS = {
    "cat": "read",
    "read": "read_to_file",
    "write": "write",
    "open": "open",
    "close": "unlock",
    "lock": "lock",
    "unlock": "unlock",
    "locks": "locks",
}


# Comandos de sesión (C2) y el método que necesitan; mismo motivo que los de RF3.
_SESSION_COMMANDS = {
    "login": "login",
    "whoami": "login",
    "adduser": "create_user",
    "passwd": "change_password",
}


# Comandos de permisos (C3) y el método que necesitan; mismo motivo que los de RF3.
_PERMISSION_COMMANDS = {
    "chmod": "chmod",
    "chown": "chown",
}

_CHMOD_USAGE = "chmod: uso: chmod <modo octal> <ruta>   (por ejemplo: chmod 640 notas.txt)"
_CHOWN_USAGE = "chown: uso: chown <usuario>[:<grupo>] <ruta>   (o chown :<grupo> <ruta>)"
_OCTAL_MODE_RE = re.compile(r"[0-7]{1,3}")


def _mode_string(is_dir: bool, mode: int) -> str:
    """drwxr-xr-x, como `ls -l` de Unix."""
    bits = "".join(
        letter if mode & (1 << (8 - index)) else "-" for index, letter in enumerate("rwxrwxrwx")
    )
    return ("d" if is_dir else "-") + bits


def _ask_new_password(prompt_password, whose: str) -> str | None:
    """Pide una contraseña nueva dos veces; None si no coinciden."""
    first = prompt_password(f"Contraseña nueva de {whose}: ")
    return first if prompt_password("Repítela: ") == first else None


def _parse_range(values: list[str]) -> tuple[int, int | None] | None:
    """offset y largo opcionales de `cat`; None si alguno no es un entero."""
    try:
        numbers = [int(v) for v in values]
    except ValueError:
        return None
    offset = numbers[0] if numbers else 0
    length = numbers[1] if len(numbers) > 1 else None
    return offset, length


def resolve_relative(current_dir: str, target: str) -> str:
    joined = target if target.startswith("/") else posixpath.join(current_dir, target)
    normalized = posixpath.normpath(joined)
    return "/" if normalized == "." else normalized


def split_command(line: str) -> list[str]:
    """Separa por espacios respetando comillas, para rutas como "C:\\8 SEMESTRE\\x.pdf".

    Sin escape: shlex.split trataría la barra invertida de las rutas de Windows como
    escape y convertiría ..\\datos\\a.pdf en ..datosa.pdf."""
    lexer = shlex.shlex(line, posix=True)
    lexer.whitespace_split = True
    lexer.escape = ""
    lexer.commenters = ""
    return list(lexer)


def handle_command(client, current_dir: str, line: str, prompt_password=None) -> tuple[str, str]:
    """prompt_password pide una contraseña sin mostrarla (por defecto getpass). Las
    contraseñas nunca van en la línea del comando: quedarían en pantalla y en el historial."""
    prompt_password = prompt_password or getpass.getpass
    try:
        parts = split_command(line)
    except ValueError as exc:
        return current_dir, f"error de sintaxis: {exc}"
    if not parts:
        return current_dir, ""
    cmd, *args = parts

    try:
        if cmd == "pwd":
            return current_dir, current_dir

        if cmd == "ls" and "-l" in args:
            # C3: `ls -l [ruta]` o `ls [ruta] -l`
            rest = [a for a in args if a != "-l"]
            target = resolve_relative(current_dir, rest[0]) if rest else current_dir
            entries = client.list_dir(target)
            if not all(hasattr(e, "mode") for e in entries):
                return current_dir, "ls: -l no disponible con este cliente (los permisos requieren el cliente distribuido)"
            return current_dir, "\n".join(
                f"{_mode_string(e.is_dir, e.mode)} {e.owner:<8} {e.group:<8} {e.size_bytes:>10}  {e.name}"
                for e in entries
            )

        if cmd == "ls":
            target = resolve_relative(current_dir, args[0]) if args else current_dir
            entries = client.list_dir(target)
            lines = [
                f"{'d' if e.is_dir else '-'} {e.size_bytes:>10}  {e.name}"
                for e in entries
            ]
            return current_dir, "\n".join(lines)

        if cmd == "cd":
            if not args:
                return current_dir, "cd: falta la ruta"
            new_dir = resolve_relative(current_dir, args[0])
            client.list_dir(new_dir)  # valida que exista y sea directorio
            return new_dir, ""

        if cmd == "mkdir":
            if not args:
                return current_dir, "mkdir: falta la ruta"
            client.make_dir(resolve_relative(current_dir, args[0]))
            return current_dir, ""

        if cmd == "rmdir":
            if not args:
                return current_dir, "rmdir: falta la ruta"
            client.remove_dir(resolve_relative(current_dir, args[0]))
            return current_dir, ""

        if cmd == "rm":
            if not args:
                return current_dir, "rm: falta la ruta"
            client.remove(resolve_relative(current_dir, args[0]))
            return current_dir, ""

        if cmd == "send":
            if len(args) < 2:
                return current_dir, "send: uso: send <local> <remota>"
            local_path = Path(args[0])
            remote_path = resolve_relative(current_dir, args[1])
            bytes_written = client.upload(local_path, remote_path)
            return current_dir, f"{bytes_written} bytes enviados a {remote_path}"

        if cmd == "receive":
            if len(args) < 2:
                return current_dir, "receive: uso: receive <remota> <local>"
            remote_path = resolve_relative(current_dir, args[0])
            local_path = Path(args[1])
            bytes_written = client.download(remote_path, local_path)
            return current_dir, f"{bytes_written} bytes recibidos en {local_path}"

        if cmd in _RF3_COMMANDS and not hasattr(client, _RF3_COMMANDS[cmd]):
            return current_dir, f"{cmd}: no disponible con este cliente (RF3 requiere el cliente distribuido)"

        if cmd == "cat":
            parsed = _parse_range(args[1:]) if 1 <= len(args) <= 3 else None
            if parsed is None:
                return current_dir, "cat: uso: cat <ruta> [offset] [largo]"
            offset, length = parsed
            data = client.read(resolve_relative(current_dir, args[0]), offset, length)
            return current_dir, data.decode("utf-8", errors="replace")

        if cmd == "read":
            parsed = _parse_range(args[1:3]) if len(args) == 4 else None
            if parsed is None:
                return current_dir, "read: uso: read <ruta> <offset> <largo> <local>"
            offset, length = parsed
            local_path = Path(args[3])
            bytes_read = client.read_to_file(
                resolve_relative(current_dir, args[0]), offset, length, local_path
            )
            return current_dir, f"{bytes_read} bytes leídos en {local_path}"

        if cmd == "write":
            parsed = _parse_range(args[1:2]) if len(args) == 3 else None
            if parsed is None:
                return current_dir, "write: uso: write <ruta> <offset> <local>"
            offset, _ = parsed
            # ponytail: lee el archivo local entero en memoria; streaming si hace falta
            data = Path(args[2]).read_bytes()
            remote_path = resolve_relative(current_dir, args[0])
            bytes_written = client.write(remote_path, offset, data)
            return current_dir, f"{bytes_written} bytes escritos en {remote_path} desde el byte {offset}"

        if cmd == "open":
            if len(args) != 2 or args[1] not in {"r", "w"}:
                return current_dir, "open: uso: open <ruta> r|w"
            handle = client.open(resolve_relative(current_dir, args[0]), args[1])
            return current_dir, f"abierto {handle.path} ({handle.mode}), lock {handle.lock_id}"

        if cmd == "close":
            if len(args) != 1:
                return current_dir, "close: uso: close <ruta>"
            client.unlock(resolve_relative(current_dir, args[0]))
            return current_dir, ""

        if cmd == "lock":
            if len(args) != 2 or args[1] not in {"r", "w"}:
                return current_dir, "lock: uso: lock <ruta> r|w"
            held = client.lock(resolve_relative(current_dir, args[0]), args[1])
            return current_dir, f"lock {held.lock_id} tomado para {held.path} ({held.mode})"

        if cmd == "unlock":
            if len(args) != 1:
                return current_dir, "unlock: uso: unlock <ruta>"
            client.unlock(resolve_relative(current_dir, args[0]))
            return current_dir, ""

        if cmd == "locks":
            return current_dir, "\n".join(
                f"{held.mode} {held.path} {held.lock_id}" for held in client.locks()
            )

        if cmd in _SESSION_COMMANDS and not hasattr(client, _SESSION_COMMANDS[cmd]):
            return current_dir, f"{cmd}: no disponible con este cliente (los usuarios requieren el cliente distribuido)"

        if cmd == "login":
            if len(args) != 1:
                return current_dir, "login: uso: login <usuario>   (la contraseña se pide aparte)"
            client.login(args[0], prompt_password(f"Contraseña de {args[0]}: "))
            return current_dir, f"sesión iniciada como {client.username}"

        if cmd == "whoami":
            if not client.username:
                return current_dir, "sin sesión: inicia sesión con login <usuario>"
            groups = ", ".join(client.groups) or "-"
            return current_dir, f"{client.username}  grupos: {groups}  admin: {'sí' if client.is_admin else 'no'}"

        if cmd == "adduser":
            is_admin = "--admin" in args
            names = [a for a in args if a != "--admin"]
            if not names or any(a.startswith("-") for a in names):
                return current_dir, "adduser: uso: adduser <usuario> [--admin] [grupo ...]"
            username, groups = names[0], names[1:]
            password = _ask_new_password(prompt_password, username)
            if password is None:
                return current_dir, "adduser: las contraseñas no coinciden; no se creó nada"
            client.create_user(username, password, groups=groups, is_admin=is_admin)
            return current_dir, f"usuario {username} creado"

        if cmd == "passwd":
            if len(args) > 1:
                return current_dir, "passwd: uso: passwd [usuario]"
            other = args[0] if args and args[0] != client.username else ""
            # la propia exige la actual; la de otro la cambia un admin sin ella
            current = "" if other else prompt_password("Contraseña actual: ")
            password = _ask_new_password(prompt_password, other or client.username or "tu usuario")
            if password is None:
                return current_dir, "passwd: las contraseñas no coinciden; no se cambió nada"
            client.change_password(password, current_password=current, username=other)
            return current_dir, "contraseña cambiada"

        if cmd in _PERMISSION_COMMANDS and not hasattr(client, _PERMISSION_COMMANDS[cmd]):
            return current_dir, f"{cmd}: no disponible con este cliente (los permisos requieren el cliente distribuido)"

        if cmd == "chmod":
            if len(args) != 2 or not _OCTAL_MODE_RE.fullmatch(args[0]):
                return current_dir, _CHMOD_USAGE
            client.chmod(resolve_relative(current_dir, args[1]), int(args[0], 8))
            return current_dir, ""

        if cmd == "chown":
            owner, _, group = args[0].partition(":") if len(args) == 2 else ("", "", "")
            if not owner and not group:
                return current_dir, _CHOWN_USAGE
            client.chown(resolve_relative(current_dir, args[1]), owner, group)
            return current_dir, ""

        if cmd == "help":
            return current_dir, _HELP_TEXT

        return current_dir, f"comando no reconocido: {cmd}"

    except AuthError as exc:
        username = getattr(client, "username", None)
        if cmd == "login":
            return current_dir, f"login: {exc}"
        if not username:
            return current_dir, f"{cmd}: {exc}. Inicia sesión con: login <usuario>"
        # Había sesión: el token venció (no hay renovación automática, el cliente no
        # guarda la contraseña). Se vuelve a pedir y el usuario repite el comando.
        try:
            client.login(username, prompt_password(f"La sesión venció. Contraseña de {username}: "))
        except (DFShaError, OSError, EOFError) as retry_exc:
            # str(): una excepción siempre es verdadera, aunque su mensaje esté vacío (EOF)
            return current_dir, f"{cmd}: {exc}; no se pudo renovar la sesión: {str(retry_exc) or 'cancelado'}"
        return current_dir, f"{cmd}: la sesión había vencido y ya se renovó; repite el comando"
    except (DFShaError, OSError) as exc:
        return current_dir, f"{cmd}: {exc}"
    except EOFError:
        return current_dir, f"{cmd}: cancelado"


def run_repl(client) -> None:
    current_dir = "/"
    print("DFSha shell — 'help' para ver comandos, 'exit' para salir.")
    while True:
        try:
            line = input(f"dfsha:{current_dir}$ ")
        except EOFError:
            break
        if line.strip() in ("exit", "quit"):
            break
        current_dir, output = handle_command(client, current_dir, line)
        if output:
            print(output)
    client.close()


def main() -> None:
    import argparse

    from dfsha.client.dfsha_client import DFShaClient

    parser = argparse.ArgumentParser(description="Cliente DFSha (Hito 1)")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    client = DFShaClient(args.host, args.port)
    run_repl(client)


if __name__ == "__main__":
    main()
