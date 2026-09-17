# DFSha

Sistema de archivos distribuido con alta disponibilidad, rendimiento y
seguridad. Proyecto de Sistemas Distribuidos (ST0263/SI3007, EAFIT).

Un cliente sube y baja archivos grandes que quedan distribuidos entre
varios nodos, tanto en lectura como en escritura. Arquitectura elegida:
Opción 1, Cliente/Servidor con composición y distribución del servicio
(S2S) — el servicio corre como un sistema autónomo dentro de una red
propia, con mecanismos abiertos para el acceso cliente-servidor.

## Estado actual: Hito 2 (sub-proyectos 1, 2, 3 y 4)

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
- **Hito 2 / sub-proyecto 3** — clúster de 3 ControlNodes con consenso Raft
  (`pysyncobj`): elección de líder, failover automático, metadata replicada y
  persistida en disco. El cliente conoce los 3 nodos y sigue al líder solo; los
  reintentos son seguros porque cada operación lleva un `op_id`.
- **Hito 2 / sub-proyecto 4** — contenerización: una imagen para todos los roles y
  un `docker-compose.yml` con los 3 DataNodes, los 3 ControlNodes, la shell y los
  tests. Ver "Correr con Docker".

Todo el transporte va sobre gRPC.

## Roadmap

- **Hito 3** — alta disponibilidad, replicación, consistencia de datos y
  seguridad (TLS entre nodos, autenticación, control de acceso).

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/generate_proto.py
```

## Correr con Docker (recomendado)

Requiere Docker Desktop (o Docker Engine + Compose v2). No hace falta Python local.

```bash
docker compose up -d --build        # 3 DataNodes + 3 ControlNodes
docker compose ps                   # los 6 tienen que decir (healthy)
docker compose run --rm shell       # shell distribuida, dentro de la red de Docker
```

La carpeta `./intercambio` del host se ve como `/intercambio` dentro de la shell:

```
dfsha:/$ mkdir /docs
dfsha:/$ send "/intercambio/mi tesis.pdf" /docs/tesis.pdf
dfsha:/$ receive /docs/tesis.pdf /intercambio/bajada.pdf
```

Variables opcionales (en PowerShell: `$env:DFSHA_BLOCK_MB="1"`):

| Variable | Default | Para qué |
|---|---|---|
| `DFSHA_BLOCK_MB` | 128 | Tamaño de bloque. Con 1, un archivo de pocos MB se ve partido en varios bloques |
| `DFSHA_REPLICATION` | 3 | Factor de replicación. Con 3 DataNodes y factor 3 cada nodo guarda todo; con 2 se ve el reparto |
| `DFSHA_UPLOAD_LEASE_S` | 600 | Segundos que una subida puede pasar sin confirmar un bloque antes de liberar su nombre |

Tumbar y revivir nodos (`kill` manda SIGKILL, equivale a `kill -9`):

```bash
docker compose kill dn1        # un DataNode
docker compose kill cn0        # un ControlNode (si era el líder, se elige otro en ~2 s)
docker compose start dn1       # vuelve con sus mismos datos
docker compose logs -f cn0     # salida de un nodo
docker compose down            # apaga; los volúmenes (journal y bloques) se conservan
docker compose down -v         # apaga y borra todos los datos
docker compose run --rm tests  # la suite completa dentro de la imagen
```

**Por qué la shell corre dentro de la red y no en el host:** el ControlNode le entrega al
cliente las direcciones de los DataNodes tal como las tiene configuradas (`dn1:50061`), y
esos nombres solo resuelven dentro de la red `dfsha`. Un cliente en el host no podría
conectarse a los DataNodes aunque se publicaran los puertos.

Decisiones del compose que no conviene cambiar sin entenderlas:

- `--raft-cluster` usa nombres de servicio (`cn0:6000`), nunca `localhost`: pysyncobj escucha
  en la misma dirección que anuncia a los otros nodos.
- Cada ControlNode tiene su volumen para `--data-dir`: sin él, recrear el contenedor pierde el
  journal de Raft. Cada DataNode tiene el suyo para los bloques.
- Solo `dn1` tiene `build`: si todos los servicios construyeran la misma imagen, Compose las
  construiría en paralelo y chocarían.
- Sin política de reinicio a propósito: un nodo que se tumba para una prueba se queda caído.

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

Las rutas con espacios van entre comillas: `send "C:\mis docs\a.pdf" a.pdf`.

## Correr Hito 2 sin Docker (3 ControlNodes + DataNodes + shell distribuida)

Orden de arranque: primero los DataNodes, después los ControlNodes (necesitan
saber sus direcciones al arrancar).

```bash
# 1. Tres DataNodes, cada uno con su raíz y su puerto
python -m dfsha.data_node.main --root ./dn1 --port 50061
python -m dfsha.data_node.main --root ./dn2 --port 50062
python -m dfsha.data_node.main --root ./dn3 --port 50063

# 2. Tres ControlNodes. --raft-cluster es la MISMA lista, en el mismo orden,
#    en los 3; --node-id es la posición de cada uno en esa lista. Cada nodo
#    necesita su propio --data-dir (journal y snapshots de Raft).
for i in 0 1 2; do
  python -m dfsha.control_node.main --node-id $i --port 5005$((i+1)) \
    --raft-cluster localhost:6051,localhost:6052,localhost:6053 \
    --data-dir ./cn$i \
    --datanode-addresses localhost:50061,localhost:50062,localhost:50063 &
done

# 3. Shell distribuida: se le pasan los 3 ControlNodes, busca al líder sola
python -m dfsha.client.distributed_shell_main \
  --control-nodes localhost:50051,localhost:50052,localhost:50053
```

`--replication-factor` (default 3) controla cuántas réplicas tiene cada
bloque. Con menos DataNodes que el factor, se replica en todos los que haya.

**Para ver la replicación funcionando:** subí un archivo con `send`, matá uno
de los DataNodes y bajalo con `receive` — sigue funcionando, y sigue
funcionando con dos caídos. Una subida nueva, en cambio, falla mientras haya
un DataNode caído en su pipeline: la escritura exige las 3 réplicas.

**Para ver el clúster de ControlNodes funcionando:** con la shell abierta,
matá con `kill -9` al ControlNode líder (el que responde; los demás devuelven
`UNAVAILABLE`). La shell sigue funcionando sin reiniciarse: en unos 2 segundos
el clúster elige otro líder. Si matás los 3 y los volvés a levantar con los
mismos `--data-dir`, los archivos siguen ahí. Con 2 de 3 caídos el clúster
deja de atender: Raft necesita mayoría.

**Nota importante:** el puerto por defecto del ControlNode (50051) es el
mismo que el default del servidor de Hito 1 — si vas a correr ambos hitos
a la vez, usá `--port` para separarlos.

**En Windows, sin Docker:** los puertos 50051–50063 caen dentro del rango de puertos
efímeros (49152–65535) y cualquier programa puede ocuparlos al azar (pasó con Chrome).
Si un nodo falla con `Failed to bind`, usá puertos por debajo de 32768. El bloque `for`
de arriba es bash; en PowerShell hay que lanzar cada nodo en su propia terminal.

## Tests

```bash
python -m pytest tests/ -v
```

## Fuera de alcance por ahora

RF3 (acceso granular a archivos: `open`/`close`/`read`/`write`/`lock`),
detección automática de DataNodes
caídos, re-replicación y autenticación. Ver el roadmap arriba para cuándo
llega cada cosa.
