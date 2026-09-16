# DFSha

Sistema de archivos distribuido con alta disponibilidad, rendimiento y
seguridad. Proyecto de Sistemas Distribuidos (ST0263/SI3007, EAFIT).

Un cliente sube y baja archivos grandes que quedan distribuidos entre
varios nodos, tanto en lectura como en escritura. Arquitectura elegida:
Opción 1, Cliente/Servidor con composición y distribución del servicio
(S2S) — el servicio corre como un sistema autónomo dentro de una red
propia, con mecanismos abiertos para el acceso cliente-servidor.

## Estado actual: Hito 2 (sub-proyecto 1)

Hito 1 (versión monolítica, un cliente y un servidor) sigue disponible sin
cambios. Sobre eso, Hito 2 sub-proyecto 1 agrega la arquitectura distribuida
de un solo nodo por rol:

- **RF1** — gestión del sistema de archivos: `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** — transferencia de archivos: `send`/`receive`, con streaming gRPC
  para no cargar archivos grandes en memoria.
- **Hito 2 / sub-proyecto 1** — ControlNode (metadatos del árbol) separado
  de un DataNode (bloques), particionamiento de archivos en bloques (128 MB
  por defecto) y un cliente/shell distribuida que habla con ambos.

Todo el transporte va sobre gRPC.

## Roadmap

- **Hito 2 (siguientes sub-proyectos)** — más de un DataNode, distribución
  real de bloques entre nodos.
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

## Correr Hito 2 (ControlNode + DataNode + shell distribuida)

Orden de arranque: primero el DataNode, después el ControlNode (necesita
saber la dirección del DataNode al arrancar).

```bash
# 1. DataNode
python -m dfsha.data_node.main --root ./datanode-data --port <puerto>

# 2. ControlNode (--datanode-address es obligatorio)
python -m dfsha.control_node.main --datanode-address localhost:<puerto-datanode> --port <puerto-controlnode>

# 3. Shell distribuida
python -m dfsha.client.distributed_shell_main --control-node-host localhost --control-node-port <puerto-controlnode>
```

**Nota importante:** el puerto por defecto del ControlNode (50051) es el
mismo que el default del servidor de Hito 1 — si vas a correr ambos hitos
a la vez, usá `--port` para separarlos.

## Tests

```bash
python -m pytest tests/ -v
```

## Fuera de alcance en Hito 2 sub-proyecto 1

RF3 (acceso granular a archivos: `open`/`close`/`read`/`write`/`lock`),
más de un DataNode, replicación y autenticación. Ver el roadmap arriba
para cuándo llega cada cosa.
