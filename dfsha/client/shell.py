from __future__ import annotations

import posixpath
import shlex
from pathlib import Path

from dfsha.common.exceptions import DFShaError

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


def handle_command(client, current_dir: str, line: str) -> tuple[str, str]:
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

        if cmd == "help":
            return current_dir, _HELP_TEXT

        return current_dir, f"comando no reconocido: {cmd}"

    except (DFShaError, OSError) as exc:
        return current_dir, f"{cmd}: {exc}"


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
