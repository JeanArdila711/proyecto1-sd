# DFSha

Sistema de archivos distribuido con alta disponibilidad, rendimiento y
seguridad. Proyecto de Sistemas Distribuidos (ST0263/SI3007, EAFIT).

Un cliente sube y baja archivos grandes que quedan distribuidos entre
varios nodos, tanto en lectura como en escritura. Arquitectura elegida:
Opción 1, Cliente/Servidor con composición y distribución del servicio
(S2S) — el servicio corre como un sistema autónomo dentro de una red
propia, con mecanismos abiertos para el acceso cliente-servidor.

## Estado actual: Hito 2 (sub-proyectos 1 y 2)

Hito 1 (versión monolítica, un cliente y un servidor) sigue disponible sin
cambios. Sobre eso, Hito 2 agrega la arquitectura distribuida:

- **RF1** — gestión del sistema de archivos: `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** — transferencia de archivos: `send`/`receive`, con streaming gRPC
  para no cargar archivos grandes en memoria.
- **Hito 2 / sub-proyecto 1** — ControlNode (metadatos del árbol) separado
  de un DataNode (bloques), particionamiento de archivos en bloques (128 MB
  por defecto) y un cliente/shell distribuida que habla con ambos.
- **Hito 2 / sub-proyecto 2** — replicación de bloques con factor 3 mediante
  un pipeline DN1→DN2→DN3: el cliente sube una sola copia y los DataNodes
  encadenan las réplicas. La descarga hace failover a la siguiente réplica si
  una está caída o corrupta.

Todo el transporte va sobre gRPC.

## Roadmap

- **Hito 2 (siguientes sub-proyectos)** — clúster de 3 ControlNodes con
  Raft y contenerización con Docker.
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

## Correr Hito 2 (ControlNode + DataNodes + shell distribuida)

Orden de arranque: primero los DataNodes, después el ControlNode (necesita
saber sus direcciones al arrancar).

```bash
# 1. Tres DataNodes, cada uno con su raíz y su puerto
python -m dfsha.data_node.main --root ./dn1 --port 50061
python -m dfsha.data_node.main --root ./dn2 --port 50062
python -m dfsha.data_node.main --root ./dn3 --port 50063

# 2. ControlNode (--datanode-addresses es obligatorio, separado por comas)
python -m dfsha.control_node.main --port 50051 \
  --datanode-addresses localhost:50061,localhost:50062,localhost:50063

# 3. Shell distribuida
python -m dfsha.client.distributed_shell_main --control-node-host localhost --control-node-port 50051
```

`--replication-factor` (default 3) controla cuántas réplicas tiene cada
bloque. Con menos DataNodes que el factor, se replica en todos los que haya.

**Para ver la replicación funcionando:** subí un archivo con `send`, matá uno
de los DataNodes y bajalo con `receive` — sigue funcionando, y sigue
funcionando con dos caídos. Una subida nueva, en cambio, falla mientras haya
un DataNode caído en su pipeline: la escritura exige las 3 réplicas.

**Nota importante:** el puerto por defecto del ControlNode (50051) es el
mismo que el default del servidor de Hito 1 — si vas a correr ambos hitos
a la vez, usá `--port` para separarlos.

## Tests

```bash
python -m pytest tests/ -v
```

## Fuera de alcance por ahora

RF3 (acceso granular a archivos: `open`/`close`/`read`/`write`/`lock`),
alta disponibilidad del ControlNode, detección automática de DataNodes
caídos, re-replicación y autenticación. Ver el roadmap arriba para cuándo
llega cada cosa.
