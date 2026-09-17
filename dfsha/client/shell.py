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
  help                        muestra esta ayuda
  exit / quit                 termina la sesión

Las rutas con espacios van entre comillas: send "C:\\mis docs\\a.pdf" a.pdf"""


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
