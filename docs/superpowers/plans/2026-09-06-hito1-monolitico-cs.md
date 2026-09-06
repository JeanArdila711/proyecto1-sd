# Hito 1 — DFSha monolítico C/S (RF1 + RF2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir la versión monolítica C/S de DFSha (un cliente, un servidor, sin distribución) que implemente RF1 (`ls`/`cd`/`mkdir`/`rmdir`/`rm`) y RF2 (`send`/`receive`) completos, con gRPC como transporte.

**Architecture:** Servidor gRPC de un solo proceso que espeja un árbol de archivos sobre una carpeta raíz del disco local. La lógica de filesystem vive separada del glue de gRPC (`filesystem.py` puro y testeable sin red; `servicer.py` traduce protobuf ↔ filesystem.py). El cliente es un shell interactivo (`shell.py`) sobre un wrapper delgado del stub gRPC (`dfsha_client.py`) que traduce errores de vuelta al mismo vocabulario de excepciones de dominio.

**Tech Stack:** Python 3, gRPC (`grpcio`, `grpcio-tools`), `pytest`.

**Spec:** `docs/superpowers/specs/2026-09-06-hito1-monolitico-cs-design.md`

## Global Constraints

- Lenguaje: Python (recomendado por el profesor).
- Transporte Cliente↔Servidor: gRPC exclusivamente — nada de REST/Flask en Hito 1.
- Sin autenticación ni namespaces por usuario en Hito 1.
- Almacenamiento del servidor: espejo directo sobre el filesystem local (una única carpeta raíz), sin metadata DB separada.
- Excepciones específicas de dominio en todo el código — nunca `except Exception` genérico ni `except: pass`.
- Escritura de archivos siempre atómica: escribir a un temporal y `os.replace()` al final, nunca sobrescribir en el lugar.
- `CHUNK_SIZE_BYTES = 1 MiB` (1 048 576 bytes) para el framing de streaming gRPC — no confundir con el bloque de 128 MB de Hito 2 (ese es el grano de distribución entre DataNodes; no aplica aquí).
- Fuera de alcance: RF3 (`open/close/read/write/lock`), distribución multi-nodo, replicación, seguridad/auth, locking/concurrencia entre clientes.

---

### Task 1: Scaffolding del repo, contrato `.proto` y generación de código

**Files:**
- Create: `requirements.txt`
- Create: `.gitignore`
- Create: `proto/dfsha.proto`
- Create: `scripts/generate_proto.py`
- Create: `dfsha/__init__.py`
- Create: `dfsha/server/__init__.py`
- Create: `dfsha/client/__init__.py`

**Interfaces:**
- Consumes: nada (primera tarea).
- Produces: paquete `dfsha.generated` (en disco, no versionado) con `dfsha_pb2` y `dfsha_pb2_grpc` importables; mensajes `ListDirRequest`, `ListDirResponse`, `DirEntry`, `MakeDirRequest`, `MakeDirResponse`, `RemoveDirRequest`, `RemoveDirResponse`, `RemoveRequest`, `RemoveResponse`, `UploadChunk`, `UploadResponse`, `DownloadRequest`, `DownloadChunk`; servicio `DFShaServiceServicer` (base a extender), `DFShaServiceStub` (cliente), función `add_DFShaServiceServicer_to_server`.

- [ ] **Step 1: Crear `requirements.txt`**

```
grpcio
grpcio-tools
pytest
```

- [ ] **Step 2: Crear `.gitignore`**

```
__pycache__/
*.pyc
.venv/
dfsha/generated/
dfsha-data/
*.part-*
```

- [ ] **Step 3: Crear entorno virtual e instalar dependencias**

Run: `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
Expected: instalación exitosa de `grpcio`, `grpcio-tools`, `pytest` sin errores.

- [ ] **Step 4: Escribir el contrato `proto/dfsha.proto`**

```protobuf
syntax = "proto3";

package dfsha;

service DFShaService {
  rpc ListDir(ListDirRequest) returns (ListDirResponse);
  rpc MakeDir(MakeDirRequest) returns (MakeDirResponse);
  rpc RemoveDir(RemoveDirRequest) returns (RemoveDirResponse);
  rpc Remove(RemoveRequest) returns (RemoveResponse);
  rpc Upload(stream UploadChunk) returns (UploadResponse);
  rpc Download(DownloadRequest) returns (stream DownloadChunk);
}

message ListDirRequest {
  string path = 1;
}

message DirEntry {
  string name = 1;
  bool is_dir = 2;
  int64 size_bytes = 3;
}

message ListDirResponse {
  repeated DirEntry entries = 1;
}

message MakeDirRequest  { string path = 1; }
message MakeDirResponse {}

message RemoveDirRequest  { string path = 1; }
message RemoveDirResponse {}

message RemoveRequest  { string path = 1; }
message RemoveResponse {}

message UploadChunk {
  oneof payload {
    string path = 1;
    bytes data = 2;
  }
}

message UploadResponse {
  int64 bytes_written = 1;
}

message DownloadRequest { string path = 1; }

message DownloadChunk { bytes data = 1; }
```

- [ ] **Step 5: Escribir el script de generación `scripts/generate_proto.py`**

```python
"""Regenera el código gRPC desde proto/dfsha.proto.

Uso: python scripts/generate_proto.py
"""
from __future__ import annotations

import re
from pathlib import Path

from grpc_tools import protoc

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto"
OUT_DIR = REPO_ROOT / "dfsha" / "generated"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "__init__.py").touch(exist_ok=True)

    protoc.main([
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        str(PROTO_DIR / "dfsha.proto"),
    ])

    # grpc_tools genera un import absoluto (`import dfsha_pb2 as dfsha__pb2`)
    # que rompe porque el módulo vive dentro del paquete dfsha.generated.
    # Se reescribe como import relativo.
    grpc_file = OUT_DIR / "dfsha_pb2_grpc.py"
    content = grpc_file.read_text()
    fixed = re.sub(
        r"^import dfsha_pb2 as dfsha__pb2$",
        "from . import dfsha_pb2 as dfsha__pb2",
        content,
        flags=re.MULTILINE,
    )
    grpc_file.write_text(fixed)


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Crear los `__init__.py` de los paquetes**

```bash
touch dfsha/__init__.py dfsha/server/__init__.py dfsha/client/__init__.py
```

- [ ] **Step 7: Ejecutar la generación y verificar que el código sea importable**

Run: `python scripts/generate_proto.py && python -c "from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc; print(dfsha_pb2_grpc.DFShaServiceStub)"`
Expected: imprime `<class 'dfsha.generated.dfsha_pb2_grpc.DFShaServiceStub'>` sin errores de import.

- [ ] **Step 8: Commit**

```bash
git add requirements.txt .gitignore proto/dfsha.proto scripts/generate_proto.py dfsha/__init__.py dfsha/server/__init__.py dfsha/client/__init__.py
git commit -m "chore: scaffolding, contrato gRPC y script de generación"
```

---

### Task 2: Excepciones de dominio y resolución segura de rutas

**Files:**
- Create: `dfsha/server/exceptions.py`
- Create: `dfsha/server/filesystem.py`
- Test: `tests/test_filesystem.py`

**Interfaces:**
- Consumes: nada de tareas anteriores.
- Produces: excepciones `DFShaError`, `InvalidPathError`, `PathNotFoundError`, `PathExistsError`, `NotEmptyError`, `NotAFileError`, `NotADirectoryError` (todas en `dfsha.server.exceptions`); función `resolve_path(root: Path, virtual_path: str) -> Path` en `dfsha.server.filesystem`.

- [ ] **Step 1: Escribir `dfsha/server/exceptions.py`**

```python
class DFShaError(Exception):
    """Excepción base para todos los errores de dominio de DFSha."""


class InvalidPathError(DFShaError):
    """La ruta virtual resuelve fuera de la raíz de almacenamiento."""


class PathNotFoundError(DFShaError):
    """La ruta virtual no existe."""


class PathExistsError(DFShaError):
    """La ruta virtual ya existe."""


class NotEmptyError(DFShaError):
    """Se intentó eliminar un directorio no vacío."""


class NotAFileError(DFShaError):
    """La operación esperaba un archivo y encontró otra cosa."""


class NotADirectoryError(DFShaError):
    """La operación esperaba un directorio y encontró otra cosa."""
```

- [ ] **Step 2: Escribir el test de `resolve_path` (falla primero)**

```python
# tests/test_filesystem.py
from pathlib import Path

import pytest

from dfsha.server import filesystem
from dfsha.server.exceptions import InvalidPathError


def test_resolve_path_dentro_de_la_raiz(tmp_path):
    resolved = filesystem.resolve_path(tmp_path, "/docs/reporte.txt")
    assert resolved == tmp_path / "docs" / "reporte.txt"


def test_resolve_path_raiz_vacia(tmp_path):
    assert filesystem.resolve_path(tmp_path, "/") == tmp_path
    assert filesystem.resolve_path(tmp_path, "") == tmp_path


def test_resolve_path_bloquea_traversal(tmp_path):
    with pytest.raises(InvalidPathError):
        filesystem.resolve_path(tmp_path, "/../../etc/passwd")


def test_resolve_path_bloquea_traversal_interno(tmp_path):
    with pytest.raises(InvalidPathError):
        filesystem.resolve_path(tmp_path, "/docs/../../etc/passwd")
```

- [ ] **Step 3: Correr el test y verificar que falla**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: FAIL — `ModuleNotFoundError` o `AttributeError: module 'dfsha.server.filesystem' has no attribute 'resolve_path'` (el módulo todavía no existe).

- [ ] **Step 4: Implementar `resolve_path` en `dfsha/server/filesystem.py`**

```python
from __future__ import annotations

from pathlib import Path

from dfsha.server.exceptions import InvalidPathError


def resolve_path(root: Path, virtual_path: str) -> Path:
    root = root.resolve()
    relative = virtual_path.lstrip("/")
    candidate = (root / relative).resolve()
    if candidate != root and root not in candidate.parents:
        raise InvalidPathError(f"la ruta sale de la raíz: {virtual_path!r}")
    return candidate
```

- [ ] **Step 5: Correr el test y verificar que pasa**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add dfsha/server/exceptions.py dfsha/server/filesystem.py tests/test_filesystem.py
git commit -m "feat: excepciones de dominio y resolución segura de rutas"
```

---

### Task 3: Operaciones de directorio (RF1: `ls`, `mkdir`, `rmdir`, `rm`)

**Files:**
- Modify: `dfsha/server/filesystem.py`
- Modify: `tests/test_filesystem.py`

**Interfaces:**
- Consumes: `resolve_path` (Task 2), excepciones de `dfsha.server.exceptions` (Task 2).
- Produces: `DirEntryData` (dataclass con `name: str`, `is_dir: bool`, `size_bytes: int`); funciones `list_dir(root, virtual_path) -> list[DirEntryData]`, `make_dir(root, virtual_path) -> None`, `remove_dir(root, virtual_path) -> None`, `remove_file(root, virtual_path) -> None`.

- [ ] **Step 1: Agregar los tests (fallan primero)**

```python
# agregar al final de tests/test_filesystem.py
from dfsha.server.exceptions import (
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)


def test_list_dir_vacio(tmp_path):
    assert filesystem.list_dir(tmp_path, "/") == []


def test_list_dir_con_contenido(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    (tmp_path / "carpeta").mkdir()
    entries = filesystem.list_dir(tmp_path, "/")
    names = {e.name: e for e in entries}
    assert names["archivo.txt"].is_dir is False
    assert names["archivo.txt"].size_bytes == 4
    assert names["carpeta"].is_dir is True
    assert names["carpeta"].size_bytes == 0


def test_list_dir_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.list_dir(tmp_path, "/no-existe")


def test_list_dir_sobre_archivo(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    with pytest.raises(NotADirectoryError):
        filesystem.list_dir(tmp_path, "/archivo.txt")


def test_make_dir_crea_directorio(tmp_path):
    filesystem.make_dir(tmp_path, "/nueva")
    assert (tmp_path / "nueva").is_dir()


def test_make_dir_anidado(tmp_path):
    filesystem.make_dir(tmp_path, "/a/b/c")
    assert (tmp_path / "a" / "b" / "c").is_dir()


def test_make_dir_ya_existe(tmp_path):
    filesystem.make_dir(tmp_path, "/nueva")
    with pytest.raises(PathExistsError):
        filesystem.make_dir(tmp_path, "/nueva")


def test_remove_dir_vacio(tmp_path):
    filesystem.make_dir(tmp_path, "/vacia")
    filesystem.remove_dir(tmp_path, "/vacia")
    assert not (tmp_path / "vacia").exists()


def test_remove_dir_no_vacio(tmp_path):
    filesystem.make_dir(tmp_path, "/con-cosas")
    (tmp_path / "con-cosas" / "archivo.txt").write_text("hola")
    with pytest.raises(NotEmptyError):
        filesystem.remove_dir(tmp_path, "/con-cosas")


def test_remove_dir_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.remove_dir(tmp_path, "/no-existe")


def test_remove_file_elimina_archivo(tmp_path):
    (tmp_path / "archivo.txt").write_text("hola")
    filesystem.remove_file(tmp_path, "/archivo.txt")
    assert not (tmp_path / "archivo.txt").exists()


def test_remove_file_sobre_directorio(tmp_path):
    filesystem.make_dir(tmp_path, "/carpeta")
    with pytest.raises(NotAFileError):
        filesystem.remove_file(tmp_path, "/carpeta")


def test_remove_file_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        filesystem.remove_file(tmp_path, "/no-existe")
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: FAIL — `AttributeError` porque `list_dir`/`make_dir`/`remove_dir`/`remove_file` no existen todavía.

- [ ] **Step 3: Implementar las operaciones de directorio**

```python
# agregar a dfsha/server/filesystem.py, debajo de resolve_path
from dataclasses import dataclass

from dfsha.server.exceptions import (
    NotADirectoryError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)


@dataclass(frozen=True)
class DirEntryData:
    name: str
    is_dir: bool
    size_bytes: int


def list_dir(root: Path, virtual_path: str) -> list[DirEntryData]:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_dir():
        raise NotADirectoryError(f"no es un directorio: {virtual_path}")
    entries = []
    for child in sorted(target.iterdir(), key=lambda p: p.name):
        is_dir = child.is_dir()
        size = 0 if is_dir else child.stat().st_size
        entries.append(DirEntryData(name=child.name, is_dir=is_dir, size_bytes=size))
    return entries


def make_dir(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if target.exists():
        raise PathExistsError(f"ya existe: {virtual_path}")
    target.mkdir(parents=True)


def remove_dir(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_dir():
        raise NotADirectoryError(f"no es un directorio: {virtual_path}")
    try:
        target.rmdir()
    except OSError as exc:
        raise NotEmptyError(f"directorio no vacío: {virtual_path}") from exc


def remove_file(root: Path, virtual_path: str) -> None:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_file():
        raise NotAFileError(f"no es un archivo: {virtual_path}")
    target.unlink()
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: PASS (17 tests en total).

- [ ] **Step 5: Commit**

```bash
git add dfsha/server/filesystem.py tests/test_filesystem.py
git commit -m "feat: operaciones de directorio (ls, mkdir, rmdir, rm)"
```

---

### Task 4: Operaciones de contenido de archivo con escritura atómica (RF2, parte servidor)

**Files:**
- Modify: `dfsha/server/filesystem.py`
- Modify: `tests/test_filesystem.py`

**Interfaces:**
- Consumes: `resolve_path` (Task 2), excepciones de `dfsha.server.exceptions` (Task 2).
- Produces: `write_file_chunks(root, virtual_path, chunks: Iterable[bytes]) -> int`, `read_file_chunks(root, virtual_path, chunk_size: int) -> Iterator[bytes]`.

- [ ] **Step 1: Agregar los tests (fallan primero)**

```python
# agregar al final de tests/test_filesystem.py

def test_write_and_read_roundtrip(tmp_path):
    contenido = b"contenido de prueba" * 1000
    written = filesystem.write_file_chunks(tmp_path, "/archivo.bin", [contenido])
    assert written == len(contenido)

    leido = b"".join(filesystem.read_file_chunks(tmp_path, "/archivo.bin", chunk_size=17))
    assert leido == contenido


def test_write_file_chunks_crea_directorios_padre(tmp_path):
    filesystem.write_file_chunks(tmp_path, "/a/b/archivo.txt", [b"hola"])
    assert (tmp_path / "a" / "b" / "archivo.txt").read_bytes() == b"hola"


def test_write_file_chunks_no_deja_temporales(tmp_path):
    filesystem.write_file_chunks(tmp_path, "/archivo.txt", [b"hola"])
    assert list(tmp_path.glob("archivo.txt.part-*")) == []


def test_write_file_chunks_atomico_ante_fallo(tmp_path):
    filesystem.write_file_chunks(tmp_path, "/existente.txt", [b"viejo"])

    def chunks_que_fallan():
        yield b"nuevo-parcial"
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        filesystem.write_file_chunks(tmp_path, "/existente.txt", chunks_que_fallan())

    assert (tmp_path / "existente.txt").read_bytes() == b"viejo"
    assert list(tmp_path.glob("existente.txt.part-*")) == []


def test_read_file_chunks_no_existe(tmp_path):
    with pytest.raises(PathNotFoundError):
        list(filesystem.read_file_chunks(tmp_path, "/no-existe", chunk_size=1024))


def test_read_file_chunks_sobre_directorio(tmp_path):
    filesystem.make_dir(tmp_path, "/carpeta")
    with pytest.raises(NotAFileError):
        list(filesystem.read_file_chunks(tmp_path, "/carpeta", chunk_size=1024))
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: FAIL — `AttributeError` porque `write_file_chunks`/`read_file_chunks` no existen todavía.

- [ ] **Step 3: Implementar las operaciones de contenido**

```python
# agregar a dfsha/server/filesystem.py
import os
import uuid
from typing import Iterable, Iterator


def write_file_chunks(root: Path, virtual_path: str, chunks: Iterable[bytes]) -> int:
    target = resolve_path(root, virtual_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.parent / f"{target.name}.part-{uuid.uuid4().hex}"
    bytes_written = 0
    try:
        with tmp_path.open("wb") as fh:
            for chunk in chunks:
                fh.write(chunk)
                bytes_written += len(chunk)
        os.replace(tmp_path, target)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return bytes_written


def read_file_chunks(root: Path, virtual_path: str, chunk_size: int) -> Iterator[bytes]:
    target = resolve_path(root, virtual_path)
    if not target.exists():
        raise PathNotFoundError(f"no existe: {virtual_path}")
    if not target.is_file():
        raise NotAFileError(f"no es un archivo: {virtual_path}")
    with target.open("rb") as fh:
        while True:
            data = fh.read(chunk_size)
            if not data:
                break
            yield data
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `python -m pytest tests/test_filesystem.py -v`
Expected: PASS (23 tests en total).

- [ ] **Step 5: Commit**

```bash
git add dfsha/server/filesystem.py tests/test_filesystem.py
git commit -m "feat: escritura atómica y lectura por chunks de archivos"
```

---

### Task 5: Servicer gRPC — RPCs unarios (ListDir, MakeDir, RemoveDir, Remove) y arranque del servidor

**Files:**
- Create: `dfsha/server/servicer.py`
- Create: `dfsha/server/main.py`
- Create: `tests/test_integration.py`

**Interfaces:**
- Consumes: `dfsha.server.filesystem` (Tasks 2-4), `dfsha.server.exceptions` (Task 2), `dfsha.generated.dfsha_pb2`/`dfsha_pb2_grpc` (Task 1).
- Produces: clase `DFShaServicer(root: Path)` (implementa `DFShaServiceServicer`); función `serve(root: Path, host: str, port: int) -> tuple[grpc.Server, int]` en `dfsha.server.main` (el `int` retornado es el puerto real asignado — usar `port=0` para que el SO elija uno libre, clave para que los tests no choquen entre sí).

- [ ] **Step 1: Escribir el test de integración (falla primero)**

```python
# tests/test_integration.py
from __future__ import annotations

from pathlib import Path

import grpc
import pytest

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.server.main import serve


@pytest.fixture
def running_server(tmp_path):
    server, port = serve(root=tmp_path / "data", host="localhost", port=0)
    yield port, tmp_path / "data"
    server.stop(grace=None)


@pytest.fixture
def stub(running_server):
    port, _ = running_server
    channel = grpc.insecure_channel(f"localhost:{port}")
    yield dfsha_pb2_grpc.DFShaServiceStub(channel)
    channel.close()


def test_list_dir_vacio(stub):
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    assert list(response.entries) == []


def test_make_dir_y_list_dir(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    names = [e.name for e in response.entries]
    assert names == ["documentos"]
    assert response.entries[0].is_dir is True


def test_make_dir_duplicado_da_already_exists(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/documentos"))
    assert exc_info.value.code() == grpc.StatusCode.ALREADY_EXISTS


def test_list_dir_inexistente_da_not_found(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ListDir(dfsha_pb2.ListDirRequest(path="/no-existe"))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


def test_remove_dir_no_vacio_da_failed_precondition(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/con-cosas"))
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/con-cosas/subcarpeta"))
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/con-cosas"))
    assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_remove_dir_y_luego_no_aparece(stub):
    stub.MakeDir(dfsha_pb2.MakeDirRequest(path="/temporal"))
    stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path="/temporal"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    assert list(response.entries) == []


def test_path_traversal_da_permission_denied(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        stub.ListDir(dfsha_pb2.ListDirRequest(path="/../../etc"))
    assert exc_info.value.code() == grpc.StatusCode.PERMISSION_DENIED
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dfsha.server.main'`.

- [ ] **Step 3: Implementar `dfsha/server/servicer.py`**

```python
from __future__ import annotations

from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.server import filesystem
from dfsha.server.exceptions import (
    InvalidPathError,
    NotAFileError,
    NotADirectoryError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

_ERROR_STATUS_MAP = {
    PathNotFoundError: grpc.StatusCode.NOT_FOUND,
    PathExistsError: grpc.StatusCode.ALREADY_EXISTS,
    NotEmptyError: grpc.StatusCode.FAILED_PRECONDITION,
    InvalidPathError: grpc.StatusCode.PERMISSION_DENIED,
    NotAFileError: grpc.StatusCode.INVALID_ARGUMENT,
    NotADirectoryError: grpc.StatusCode.INVALID_ARGUMENT,
}


def _abort_on_domain_error(context: grpc.ServicerContext, exc: Exception) -> None:
    status_code = _ERROR_STATUS_MAP.get(type(exc), grpc.StatusCode.UNKNOWN)
    context.abort(status_code, str(exc))


class DFShaServicer(dfsha_pb2_grpc.DFShaServiceServicer):
    def __init__(self, root: Path) -> None:
        self._root = root

    def ListDir(self, request, context):
        try:
            entries = filesystem.list_dir(self._root, request.path)
        except (PathNotFoundError, NotADirectoryError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.ListDirResponse(
            entries=[
                dfsha_pb2.DirEntry(name=e.name, is_dir=e.is_dir, size_bytes=e.size_bytes)
                for e in entries
            ]
        )

    def MakeDir(self, request, context):
        try:
            filesystem.make_dir(self._root, request.path)
        except (PathExistsError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.MakeDirResponse()

    def RemoveDir(self, request, context):
        try:
            filesystem.remove_dir(self._root, request.path)
        except (PathNotFoundError, NotADirectoryError, NotEmptyError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.RemoveDirResponse()

    def Remove(self, request, context):
        try:
            filesystem.remove_file(self._root, request.path)
        except (PathNotFoundError, NotAFileError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.RemoveResponse()
```

- [ ] **Step 4: Implementar `dfsha/server/main.py`**

```python
from __future__ import annotations

import argparse
from concurrent import futures
from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2_grpc
from dfsha.server.servicer import DFShaServicer


def serve(root: Path, host: str, port: int) -> tuple[grpc.Server, int]:
    root.mkdir(parents=True, exist_ok=True)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfsha_pb2_grpc.add_DFShaServiceServicer_to_server(DFShaServicer(root), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor DFSha (Hito 1, monolítico)")
    parser.add_argument("--root", default="./dfsha-data", help="Carpeta raíz del árbol del DFS")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server, bound_port = serve(Path(args.root), args.host, args.port)
    print(f"DFSha server escuchando en {args.host}:{bound_port}, raíz={args.root}")
    server.wait_for_termination()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Correr el test y verificar que pasa**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add dfsha/server/servicer.py dfsha/server/main.py tests/test_integration.py
git commit -m "feat: servicer gRPC para RF1 (ls, mkdir, rmdir, rm) y arranque del servidor"
```

---

### Task 6: Servicer gRPC — RPCs de streaming (Upload, Download)

**Files:**
- Modify: `dfsha/server/servicer.py`
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes: `write_file_chunks`/`read_file_chunks` (Task 4), `CHUNK_SIZE_BYTES` (definido en Task 5, en `dfsha.server.servicer`).
- Produces: métodos `Upload(request_iterator, context)` y `Download(request, context)` en `DFShaServicer`.

- [ ] **Step 1: Agregar los tests (fallan primero)**

```python
# agregar al final de tests/test_integration.py
import hashlib


def _upload_chunks(path: str, data: bytes, chunk_size: int = 4096):
    yield dfsha_pb2.UploadChunk(path=path)
    for i in range(0, len(data), chunk_size):
        yield dfsha_pb2.UploadChunk(data=data[i : i + chunk_size])


def test_upload_y_download_roundtrip(stub):
    contenido = b"x" * (5 * 1024 * 1024 + 123)  # fuerza varios chunks de 1 MiB
    response = stub.Upload(_upload_chunks("/grande.bin", contenido))
    assert response.bytes_written == len(contenido)

    recibido = b"".join(
        chunk.data for chunk in stub.Download(dfsha_pb2.DownloadRequest(path="/grande.bin"))
    )
    assert hashlib.sha256(recibido).hexdigest() == hashlib.sha256(contenido).hexdigest()


def test_upload_luego_aparece_en_list_dir(stub):
    stub.Upload(_upload_chunks("/nota.txt", b"hola mundo"))
    response = stub.ListDir(dfsha_pb2.ListDirRequest(path="/"))
    entry = next(e for e in response.entries if e.name == "nota.txt")
    assert entry.is_dir is False
    assert entry.size_bytes == len(b"hola mundo")


def test_download_inexistente_da_not_found(stub):
    with pytest.raises(grpc.RpcError) as exc_info:
        list(stub.Download(dfsha_pb2.DownloadRequest(path="/no-existe")))
    assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL — `grpc.RpcError` de tipo `UNIMPLEMENTED` porque `Upload`/`Download` no están implementados.

- [ ] **Step 3: Implementar `Upload` y `Download` en `dfsha/server/servicer.py`**

```python
# agregar dentro de la clase DFShaServicer, en dfsha/server/servicer.py
    def Upload(self, request_iterator, context):
        first = next(request_iterator)
        path = first.path

        def chunks():
            for msg in request_iterator:
                yield msg.data

        try:
            bytes_written = filesystem.write_file_chunks(self._root, path, chunks())
        except InvalidPathError as exc:
            _abort_on_domain_error(context, exc)
        return dfsha_pb2.UploadResponse(bytes_written=bytes_written)

    def Download(self, request, context):
        try:
            for chunk in filesystem.read_file_chunks(self._root, request.path, CHUNK_SIZE_BYTES):
                yield dfsha_pb2.DownloadChunk(data=chunk)
        except (PathNotFoundError, NotAFileError, InvalidPathError) as exc:
            _abort_on_domain_error(context, exc)
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS (10 tests en total).

- [ ] **Step 5: Commit**

```bash
git add dfsha/server/servicer.py tests/test_integration.py
git commit -m "feat: streaming de Upload/Download en el servicer (RF2, lado servidor)"
```

---

### Task 7: Cliente gRPC delgado con traducción de errores

**Files:**
- Create: `dfsha/client/dfsha_client.py`
- Modify: `tests/test_integration.py`

**Interfaces:**
- Consumes: `dfsha.generated.dfsha_pb2`/`dfsha_pb2_grpc` (Task 1), excepciones de `dfsha.server.exceptions` (Task 2, reutilizadas también del lado cliente).
- Produces: clase `DFShaClient(host: str, port: int)` con métodos `list_dir(path) -> list[DirEntry]`, `make_dir(path) -> None`, `remove_dir(path) -> None`, `remove(path) -> None`, `upload(local_path: Path, remote_path: str) -> int`, `download(remote_path: str, local_path: Path) -> int`, `close() -> None`.

- [ ] **Step 1: Agregar los tests de integración usando el cliente (fallan primero)**

```python
# agregar al final de tests/test_integration.py
from dfsha.client.dfsha_client import DFShaClient
from dfsha.server.exceptions import PathExistsError, PathNotFoundError


@pytest.fixture
def client(running_server):
    port, _ = running_server
    c = DFShaClient(host="localhost", port=port)
    yield c
    c.close()


def test_client_make_dir_y_list_dir(client):
    client.make_dir("/documentos")
    entries = client.list_dir("/")
    assert [e.name for e in entries] == ["documentos"]


def test_client_make_dir_duplicado_traduce_a_domain_error(client):
    client.make_dir("/documentos")
    with pytest.raises(PathExistsError):
        client.make_dir("/documentos")


def test_client_list_dir_inexistente_traduce_a_domain_error(client):
    with pytest.raises(PathNotFoundError):
        client.list_dir("/no-existe")


def test_client_upload_download_roundtrip(client, tmp_path):
    origen = tmp_path / "origen.bin"
    origen.write_bytes(b"contenido de prueba" * 10000)

    bytes_subidos = client.upload(origen, "/subido.bin")
    assert bytes_subidos == origen.stat().st_size

    destino = tmp_path / "destino.bin"
    bytes_bajados = client.download("/subido.bin", destino)
    assert bytes_bajados == bytes_subidos
    assert destino.read_bytes() == origen.read_bytes()
```

- [ ] **Step 2: Correr los tests y verificar que fallan**

Run: `python -m pytest tests/test_integration.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dfsha.client.dfsha_client'`.

- [ ] **Step 3: Implementar `dfsha/client/dfsha_client.py`**

```python
from __future__ import annotations

from pathlib import Path

import grpc

from dfsha.generated import dfsha_pb2, dfsha_pb2_grpc
from dfsha.server.exceptions import (
    DFShaError,
    InvalidPathError,
    NotAFileError,
    NotEmptyError,
    PathExistsError,
    PathNotFoundError,
)

CHUNK_SIZE_BYTES = 1024 * 1024  # 1 MiB

_STATUS_ERROR_MAP = {
    grpc.StatusCode.NOT_FOUND: PathNotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: PathExistsError,
    grpc.StatusCode.FAILED_PRECONDITION: NotEmptyError,
    grpc.StatusCode.PERMISSION_DENIED: InvalidPathError,
    # INVALID_ARGUMENT cubre tanto NotAFileError como NotADirectoryError del
    # lado servidor: el cliente solo necesita mostrar el mensaje, no
    # distinguir el subtipo exacto.
    grpc.StatusCode.INVALID_ARGUMENT: NotAFileError,
}


def _translate(rpc_error: grpc.RpcError) -> DFShaError:
    exc_cls = _STATUS_ERROR_MAP.get(rpc_error.code(), DFShaError)
    return exc_cls(rpc_error.details())


class DFShaClient:
    def __init__(self, host: str, port: int) -> None:
        self._channel = grpc.insecure_channel(f"{host}:{port}")
        self._stub = dfsha_pb2_grpc.DFShaServiceStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def list_dir(self, path: str) -> list[dfsha_pb2.DirEntry]:
        try:
            response = self._stub.ListDir(dfsha_pb2.ListDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return list(response.entries)

    def make_dir(self, path: str) -> None:
        try:
            self._stub.MakeDir(dfsha_pb2.MakeDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove_dir(self, path: str) -> None:
        try:
            self._stub.RemoveDir(dfsha_pb2.RemoveDirRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def remove(self, path: str) -> None:
        try:
            self._stub.Remove(dfsha_pb2.RemoveRequest(path=path))
        except grpc.RpcError as exc:
            raise _translate(exc) from exc

    def upload(self, local_path: Path, remote_path: str) -> int:
        def request_iterator():
            yield dfsha_pb2.UploadChunk(path=remote_path)
            with local_path.open("rb") as fh:
                while True:
                    data = fh.read(CHUNK_SIZE_BYTES)
                    if not data:
                        break
                    yield dfsha_pb2.UploadChunk(data=data)

        try:
            response = self._stub.Upload(request_iterator())
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return response.bytes_written

    def download(self, remote_path: str, local_path: Path) -> int:
        bytes_written = 0
        try:
            with local_path.open("wb") as fh:
                for chunk in self._stub.Download(dfsha_pb2.DownloadRequest(path=remote_path)):
                    fh.write(chunk.data)
                    bytes_written += len(chunk.data)
        except grpc.RpcError as exc:
            raise _translate(exc) from exc
        return bytes_written
```

- [ ] **Step 4: Correr los tests y verificar que pasan**

Run: `python -m pytest tests/test_integration.py -v`
Expected: PASS (14 tests en total).

- [ ] **Step 5: Commit**

```bash
git add dfsha/client/dfsha_client.py tests/test_integration.py
git commit -m "feat: cliente gRPC delgado con traducción de errores a excepciones de dominio"
```

---

### Task 8: Shell interactiva del cliente

**Files:**
- Create: `dfsha/client/shell.py`
- Create: `tests/test_shell.py`

**Interfaces:**
- Consumes: excepciones de `dfsha.server.exceptions` (Task 2); en producción consume `DFShaClient` (Task 7), pero los tests unitarios usan un cliente falso con la misma interfaz (`list_dir`, `make_dir`, `remove_dir`, `remove`, `upload`, `download`).
- Produces: `resolve_relative(current_dir: str, target: str) -> str`; `handle_command(client, current_dir: str, line: str) -> tuple[str, str]` (retorna `(nuevo_directorio, mensaje_de_salida)`); `run_repl(client) -> None`.

- [ ] **Step 1: Escribir el test con un cliente falso (falla primero)**

```python
# tests/test_shell.py
from __future__ import annotations

from pathlib import Path

import pytest

from dfsha.client.shell import handle_command, resolve_relative
from dfsha.server.exceptions import PathNotFoundError


class FakeClient:
    """Cliente falso en memoria, con la misma interfaz que DFShaClient."""

    def __init__(self):
        self.dirs = {"/"}
        self.files = {}

    def list_dir(self, path):
        if path not in self.dirs:
            raise PathNotFoundError(f"no existe: {path}")
        return []

    def make_dir(self, path):
        self.dirs.add(path)

    def remove_dir(self, path):
        self.dirs.discard(path)

    def remove(self, path):
        self.files.pop(path, None)

    def upload(self, local_path: Path, remote_path: str) -> int:
        data = local_path.read_bytes()
        self.files[remote_path] = data
        return len(data)

    def download(self, remote_path: str, local_path: Path) -> int:
        data = self.files[remote_path]
        local_path.write_bytes(data)
        return len(data)


def test_resolve_relative_absoluta():
    assert resolve_relative("/actual", "/otra/ruta") == "/otra/ruta"


def test_resolve_relative_relativa():
    assert resolve_relative("/a/b", "c") == "/a/b/c"


def test_resolve_relative_subir_nivel():
    assert resolve_relative("/a/b", "..") == "/a"


def test_resolve_relative_raiz():
    assert resolve_relative("/a", "..") == "/"


def test_pwd():
    client = FakeClient()
    new_dir, output = handle_command(client, "/actual", "pwd")
    assert new_dir == "/actual"
    assert output == "/actual"


def test_cd_a_directorio_existente():
    client = FakeClient()
    client.make_dir("/docs")
    new_dir, output = handle_command(client, "/", "cd docs")
    assert new_dir == "/docs"
    assert output == ""


def test_cd_a_directorio_inexistente_no_cambia_el_directorio():
    client = FakeClient()
    new_dir, output = handle_command(client, "/", "cd no-existe")
    assert new_dir == "/"
    assert "no existe" in output


def test_mkdir():
    client = FakeClient()
    new_dir, output = handle_command(client, "/", "mkdir docs")
    assert new_dir == "/"
    assert "/docs" in client.dirs


def test_send_y_receive(tmp_path):
    client = FakeClient()
    local_origen = tmp_path / "origen.txt"
    local_origen.write_text("hola mundo")

    _, output_send = handle_command(client, "/", f"send {local_origen} archivo.txt")
    assert "enviados" in output_send

    local_destino = tmp_path / "destino.txt"
    _, output_receive = handle_command(client, "/", f"receive archivo.txt {local_destino}")
    assert "recibidos" in output_receive
    assert local_destino.read_text() == "hola mundo"


def test_comando_no_reconocido():
    client = FakeClient()
    _, output = handle_command(client, "/", "volar")
    assert "no reconocido" in output
```

- [ ] **Step 2: Correr el test y verificar que falla**

Run: `python -m pytest tests/test_shell.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'dfsha.client.shell'`.

- [ ] **Step 3: Implementar `dfsha/client/shell.py`**

```python
from __future__ import annotations

import posixpath
from pathlib import Path

from dfsha.server.exceptions import DFShaError

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
  exit / quit                 termina la sesión"""


def resolve_relative(current_dir: str, target: str) -> str:
    joined = target if target.startswith("/") else posixpath.join(current_dir, target)
    normalized = posixpath.normpath(joined)
    return "/" if normalized == "." else normalized


def handle_command(client, current_dir: str, line: str) -> tuple[str, str]:
    parts = line.strip().split()
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

    except DFShaError as exc:
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
```

- [ ] **Step 4: Correr el test y verificar que pasa**

Run: `python -m pytest tests/test_shell.py -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add dfsha/client/shell.py tests/test_shell.py
git commit -m "feat: shell interactiva del cliente (RF1/RF2 end-to-end)"
```

---

### Task 9: Suite completa, README de uso y cierre de Hito 1

**Files:**
- Create: `README.md`
- Modify: `docs/superpowers/plans/2026-09-06-hito1-monolitico-cs.md` (marcar tareas completadas — lo hace quien ejecuta el plan)

**Interfaces:**
- Consumes: todo lo anterior. No produce interfaces nuevas — es la tarea de cierre.

- [ ] **Step 1: Correr toda la suite de tests junta**

Run: `python -m pytest tests/ -v`
Expected: PASS — 47 tests en total (23 en `test_filesystem.py` + 14 en `test_integration.py` + 10 en `test_shell.py`), 0 fallos.

- [ ] **Step 2: Escribir `README.md`**

```markdown
# DFSha — Hito 1 (versión monolítica C/S)

Sistema de archivos distribuido (proyecto de Sistemas Distribuidos, EAFIT).
Esta es la versión de **Hito 1**: un cliente y un servidor, sin distribución
todavía, con RF1 (`ls`, `cd`, `mkdir`, `rmdir`, `rm`) y RF2 (`send`,
`receive`) completos.

Diseño completo: `docs/superpowers/specs/2026-09-06-hito1-monolitico-cs-design.md`

## Instalación

\`\`\`bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_proto.py
\`\`\`

## Correr el servidor

\`\`\`bash
python -m dfsha.server.main --root ./dfsha-data --port 50051
\`\`\`

## Correr el cliente (shell interactiva)

\`\`\`bash
python -m dfsha.client.shell --host localhost --port 50051
\`\`\`

Dentro del shell:

\`\`\`
dfsha:/$ mkdir documentos
dfsha:/$ cd documentos
dfsha:/documentos$ send ./local.txt remoto.txt
dfsha:/documentos$ ls
dfsha:/documentos$ receive remoto.txt ./descargado.txt
\`\`\`

## Tests

\`\`\`bash
python -m pytest tests/ -v
\`\`\`

## Fuera de alcance en Hito 1

RF3 (acceso granular a archivos), distribución multi-nodo, replicación,
autenticación y particionamiento en bloques — ver el spec para el detalle
de qué llega en Hito 2 y Hito 3.
```

- [ ] **Step 3: Commit final**

```bash
git add README.md
git commit -m "docs: README de uso para Hito 1"
```

---

## Revisión final (no es una tarea de código)

Después de la Task 9, un revisor final debe:
1. Clonar el repo en un directorio limpio, seguir el README de punta a punta (instalación, generación de proto, correr servidor + cliente manualmente) y confirmar que funciona sin pasos ocultos.
2. Correr `python -m pytest tests/ -v` y confirmar 0 fallos.
3. Revisar que ningún archivo de `dfsha/server/` importe nada de `dfsha.generated` salvo `servicer.py` y `main.py` (la regla de dependencia del spec: `filesystem.py` permanece puro).
4. Confirmar contra el spec (`docs/superpowers/specs/2026-09-06-hito1-monolitico-cs-design.md`) que no quedó ningún ítem de RF1/RF2 sin cubrir.
