# Hito 1 — DFSha: versión monolítica C/S (RF1 + RF2)

## Contexto

DFSha es un DFS (Distributed File System) para el proyecto 1 de Sistemas
Distribuidos (ST0263/SI3007, EAFIT). El equipo tiene asignada la **Opción 1**
(C/S con composición y distribución del servicio, S2S), confirmada de forma
definitiva el 2026-09-06.

El cronograma del enunciado pide, para la semana 8, un **Hito 1**: una
versión **monolítica** de la arquitectura C/S — un único cliente y un único
servidor, sin distribución entre múltiples nodos todavía — que implemente
completos:

- **RF1** — Gestión del sistema de archivos: `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** — Transferencia de archivos: `send()` y `receive()`.

Referencia completa del proyecto y decisiones de arquitectura de fondo (Hito
2 en adelante): ver el vault de Obsidian, `Proyecto 1 - DFSha - Diseño y
Decisiones (Opción 1).md`.

## Objetivo de este documento

Fijar el diseño técnico del Hito 1 antes de escribir código: qué construye,
qué no, cómo se estructura el repo, el contrato gRPC exacto, el manejo de
errores y la estrategia de testing.

## Explícitamente fuera de alcance en Hito 1

- **RF3** (`open/close/read/write/lock` — acceso granular a archivos).
- **Distribución** entre ControlNode/DataNodes — eso es Hito 2.
- **Particionamiento en bloques de 128 MB** — no tiene sentido en un único
  nodo; se introduce en Hito 2 cuando hay DataNodes entre los que repartir.
- **Autenticación y namespaces por usuario** — un único árbol de archivos
  compartido, sin login. Seguridad es el foco explícito de Hito 3.
- **Concurrencia/locking** entre clientes — RF3 incluye `lock()`, no aplica aún.
- **Replicación, alta disponibilidad** — Hito 3.

## Decisiones ya tomadas (de la sesión de brainstorming)

| Decisión | Elegido |
|---|---|
| Protocolo Cliente↔Servidor | gRPC |
| Modo de cliente | Shell interactiva (mantiene directorio actual) |
| Auth | Ninguna en Hito 1 |
| Almacenamiento en servidor | Espejo directo sobre el filesystem local (una carpeta raíz) |
| Separación de código | Lógica de filesystem separada del glue de gRPC |
| Lenguaje | Python (recomendado por el profesor) |

## Arquitectura

```
proyecto1-sd/
├── proto/
│   └── dfsha.proto
├── dfsha/
│   ├── __init__.py
│   ├── server/
│   │   ├── __init__.py
│   │   ├── exceptions.py    # excepciones propias del dominio filesystem
│   │   ├── filesystem.py    # lógica pura: ls/mkdir/rmdir/rm/read/write sobre una raíz
│   │   ├── servicer.py      # clase gRPC: traduce protobuf <-> filesystem.py
│   │   └── main.py          # entrypoint: arranca el servidor gRPC
│   ├── client/
│   │   ├── __init__.py
│   │   ├── dfsha_client.py  # wrapper delgado sobre el stub gRPC generado
│   │   └── shell.py         # REPL interactivo (entrypoint del cliente)
│   └── generated/            # código generado por protoc (gitignored)
├── tests/
│   ├── test_filesystem.py    # unitarios, sin gRPC
│   └── test_integration.py   # servidor real + cliente real, roundtrip
├── requirements.txt
└── docs/superpowers/specs/   # este archivo
```

**Regla de dependencia:** `filesystem.py` no importa nada de gRPC ni de
`servicer.py`. `servicer.py` importa `filesystem.py` y el código generado,
nunca al revés. `shell.py` solo conoce `dfsha_client.py`, nunca el código
generado directamente.

## Contrato gRPC (`proto/dfsha.proto`)

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
  string path = 1;          // ruta virtual absoluta, ej: "/docs"
}

message DirEntry {
  string name = 1;
  bool is_dir = 2;
  int64 size_bytes = 3;      // 0 para directorios
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

// El primer mensaje del stream lleva la ruta destino; los siguientes,
// los bytes del archivo. Un `oneof` hace el contrato explícito en el tipo.
message UploadChunk {
  oneof payload {
    string path = 1;   // solo en el primer mensaje
    bytes data = 2;     // en los mensajes siguientes
  }
}

message UploadResponse {
  int64 bytes_written = 1;
}

message DownloadRequest { string path = 1; }

message DownloadChunk { bytes data = 1; }
```

**Tamaño de chunk para streaming:** 1 MiB (1 048 576 bytes), muy por debajo
del límite por defecto de gRPC (4 MB por mensaje). Constante nombrada
`CHUNK_SIZE_BYTES` en `dfsha/client/dfsha_client.py` y
`dfsha/server/servicer.py`. No confundir con el tamaño de bloque de 128 MB
del enunciado — ese es el grano de distribución entre DataNodes en Hito 2;
este es solo el tamaño de framing del streaming gRPC dentro de una única
transferencia.

**Convención de rutas virtuales:** siempre absolutas, separador `/`, la raíz
es `"/"`. `cd` y `pwd` son puramente del cliente — el shell mantiene el
directorio actual y resuelve rutas relativas a absolutas antes de invocar
cualquier RPC. El servidor no tiene sesión ni estado entre llamadas.

## Lógica de filesystem (`dfsha/server/filesystem.py`)

Funciones puras y deterministas, todas reciben la raíz física como
parámetro explícito (nunca una global ni un valor por defecto oculto):

```python
def resolve_path(root: Path, virtual_path: str) -> Path: ...
def list_dir(root: Path, virtual_path: str) -> list[DirEntryData]: ...
def make_dir(root: Path, virtual_path: str) -> None: ...
def remove_dir(root: Path, virtual_path: str) -> None: ...
def remove_file(root: Path, virtual_path: str) -> None: ...
def write_file_chunks(root: Path, virtual_path: str, chunks: Iterable[bytes]) -> int: ...
def read_file_chunks(root: Path, virtual_path: str, chunk_size: int) -> Iterator[bytes]: ...
```

`resolve_path` es el punto único de validación: normaliza la ruta virtual,
la resuelve contra `root`, y verifica que el resultado siga **dentro** de
`root` (usando `Path.resolve()` + comprobación de prefijo). Si no, levanta
`InvalidPathError`. Todas las demás funciones pasan por acá primero.

**Escritura atómica en `write_file_chunks`:** escribe a un archivo temporal
(`<destino>.part-<uuid>`) en el mismo directorio y solo hace `os.replace()`
al terminar de recibir todos los chunks. Si la transferencia se corta a
mitad de camino, el archivo definitivo nunca queda a medias — el `.part-*`
huérfano se puede limpiar aparte. Este patrón es el mismo de "escritura
atómica: temp + swap" ya documentado como aplicable en el diseño general.

### Excepciones (`dfsha/server/exceptions.py`)

```python
class DFShaError(Exception): ...
class InvalidPathError(DFShaError): ...      # traversal fuera de la raíz
class PathNotFoundError(DFShaError): ...
class PathExistsError(DFShaError): ...
class NotEmptyError(DFShaError): ...         # rmdir sobre directorio no vacío
class NotAFileError(DFShaError): ...
class NotADirectoryError(DFShaError): ...
```

Nunca `except Exception` genérico ni `except: pass` en ningún punto del
código de dominio.

## `servicer.py`: mapeo de errores a gRPC

| Excepción de dominio | `grpc.StatusCode` |
|---|---|
| `PathNotFoundError` | `NOT_FOUND` |
| `PathExistsError` | `ALREADY_EXISTS` |
| `NotEmptyError` | `FAILED_PRECONDITION` |
| `InvalidPathError` | `PERMISSION_DENIED` |
| `NotAFileError` / `NotADirectoryError` | `INVALID_ARGUMENT` |

Cada handler del servicer atrapa la excepción específica correspondiente,
llama a `context.set_code(...)` y `context.set_details(str(e))` con un
mensaje claro (nunca solo "error"), y retorna el mensaje de respuesta vacío
correspondiente.

## Cliente (`dfsha/client/`)

`dfsha_client.py` expone funciones delgadas (`list_dir`, `make_dir`,
`remove_dir`, `remove`, `upload`, `download`) que envuelven el stub gRPC y
traducen `grpc.RpcError` de vuelta a las mismas excepciones de dominio de
`dfsha/server/exceptions.py` (importadas, no duplicadas), para que
`shell.py` maneje un único vocabulario de errores sin importar si vienen
del lado cliente o del servidor.

`shell.py` es un REPL (`input()` en loop) que soporta:

```
ls [ruta]
cd <ruta>
pwd
mkdir <ruta>
rmdir <ruta>
rm <ruta>
send <ruta_local> <ruta_remota>
receive <ruta_remota> <ruta_local>
help
exit / quit
```

Errores del servidor se muestran como mensaje corto (`ls: /docs: no existe`),
nunca como traceback crudo al usuario.

## Testing

Dos niveles, ambos obligatorios:

**Unitarios — `tests/test_filesystem.py`** (pytest, fixture `tmp_path` como
raíz, sin gRPC):
- `ls` sobre directorio vacío y con contenido (archivos y subdirectorios).
- `mkdir` exitoso; `mkdir` sobre ruta existente → `PathExistsError`.
- `rmdir` sobre directorio vacío; `rmdir` sobre directorio no vacío →
  `NotEmptyError`.
- `rm` sobre archivo; `rm` sobre directorio → `NotAFileError`.
- Operación sobre ruta inexistente → `PathNotFoundError`.
- Intento de traversal (`../../etc/passwd`, rutas absolutas del SO) →
  `InvalidPathError`.
- `write_file_chunks` + `read_file_chunks` roundtrip: el contenido leído es
  idéntico al escrito, byte a byte.

**Integración — `tests/test_integration.py`** (fixture de sesión levanta el
servidor gRPC real en un puerto libre, cliente real se conecta):
- Flujo completo: `mkdir` → `ls` lo muestra → `send` un archivo (varios MB,
  para forzar múltiples chunks) → `receive` lo recupera → el hash SHA-256
  del archivo descargado coincide con el original → `rm` lo borra → `ls` ya
  no lo muestra.
- Al menos un caso de error de punta a punta: `receive` de una ruta que no
  existe debe propagar `PathNotFoundError` al cliente, no un `RpcError`
  genérico sin traducir.

## Dependencias (`requirements.txt`)

```
grpcio
grpcio-tools
pytest
```

Sin frameworks adicionales — no hace falta FastAPI/Flask en Hito 1 porque
todo el transporte es gRPC, no HTTP.

## Referencias

- Vault Obsidian: `Proyecto 1 - DFSha - Diseño y Decisiones (Opción 1).md`
- Clase-05a-gRPC.pdf (ejemplo de servidor/cliente Python con protobuf)
- Clase-04b-RPC-RMI.pdf (semántica de fallos, marshalling)
- `de-principios-de-diseno` (excepciones específicas, escritura atómica, sin `except: pass`)
