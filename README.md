# DFSha

Sistema de archivos distribuido con alta disponibilidad, rendimiento y
seguridad. Proyecto de Sistemas Distribuidos (ST0263/SI3007, EAFIT).

Un cliente sube y baja archivos grandes que quedan distribuidos entre
varios nodos, tanto en lectura como en escritura. Arquitectura elegida:
Opción 1, Cliente/Servidor con composición y distribución del servicio
(S2S) — el servicio corre como un sistema autónomo dentro de una red
propia, con mecanismos abiertos para el acceso cliente-servidor.

## Estado actual: Hito 1

Versión monolítica: un cliente y un servidor, todavía sin distribuir entre
varios nodos. Completo:

- **RF1** — gestión del sistema de archivos: `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** — transferencia de archivos: `send`/`receive`, con streaming gRPC
  para no cargar archivos grandes en memoria.

Todo el transporte cliente-servidor va sobre gRPC. El servidor guarda el
árbol de archivos como un espejo directo sobre el filesystem local.

## Roadmap

- **Hito 2** — arquitectura distribuida: separar el servidor en un
  ControlNode (metadatos) y varios DataNodes (bloques), particionar los
  archivos en bloques de 128 MB, y especificar las comunicaciones entre
  nodos.
- **Hito 3** — alta disponibilidad, replicación, consistencia de datos y
  seguridad (TLS entre nodos, autenticación, control de acceso).

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_proto.py
```

## Correr el servidor

```bash
python -m dfsha.server.main --root ./dfsha-data --port 50051
```

## Correr el cliente (shell interactiva)

```bash
python -m dfsha.client.shell --host localhost --port 50051
```

Dentro del shell:

```
dfsha:/$ mkdir documentos
dfsha:/$ cd documentos
dfsha:/documentos$ send ./local.txt remoto.txt
dfsha:/documentos$ ls
dfsha:/documentos$ receive remoto.txt ./descargado.txt
```

## Tests

```bash
python -m pytest tests/ -v
```

## Fuera de alcance en Hito 1

RF3 (acceso granular a archivos: `open`/`close`/`read`/`write`/`lock`),
distribución multi-nodo, replicación, autenticación y particionamiento
real en bloques. Ver el roadmap arriba para cuándo llega cada cosa.
