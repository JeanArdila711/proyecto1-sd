# DFSha — Hito 1 (versión monolítica C/S)

Sistema de archivos distribuido (proyecto de Sistemas Distribuidos, EAFIT).
Esta es la versión de **Hito 1**: un cliente y un servidor, sin distribución
todavía, con RF1 (`ls`, `cd`, `mkdir`, `rmdir`, `rm`) y RF2 (`send`,
`receive`) completos.

Diseño completo: `docs/superpowers/specs/2026-09-06-hito1-monolitico-cs-design.md`

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

RF3 (acceso granular a archivos), distribución multi-nodo, replicación,
autenticación y particionamiento en bloques — ver el spec para el detalle
de qué llega en Hito 2 y Hito 3.
