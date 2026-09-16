# Estado del proyecto — DFSha

Última actualización: 2026-09-15, tras cerrar el sub-proyecto 1 de Hito 2.

Este documento es para que cualquiera del equipo pueda entrar al repo, entender qué hay construido, qué falta y por qué se tomó cada decisión, sin tener que reconstruir el contexto desde cero. El `README.md` tiene las instrucciones de instalación y de cómo correr cada pieza — acá está el panorama completo.

## Qué es DFSha

Sistema de archivos distribuido: un cliente sube y baja archivos que quedan repartidos entre varios nodos, tanto en lectura como en escritura. Arquitectura elegida desde el principio: Cliente/Servidor con composición y distribución del servicio (el servicio corre como un sistema autónomo en su propia red, con una puerta de entrada abierta para clientes). Todo el transporte es gRPC — no hay HTTP REST ni sockets crudos en ningún punto.

## Qué está implementado

### Hito 1 — completo, sin cambios desde su entrega

Un cliente y un servidor, cada uno un solo proceso.

- **RF1** (gestión del árbol): `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** (transferencia de archivos): `send`/`receive`, con streaming gRPC para no cargar archivos completos en memoria ni en el cliente ni en el servidor.
- Escritura atómica en disco (temp file + `os.replace()`) en los dos lados — una descarga o subida que falla a mitad de camino nunca deja un archivo a medias con el nombre final.
- 54 tests, código en `dfsha/server/` y `dfsha/client/dfsha_client.py` + `dfsha/client/shell.py`.

### Hito 2, sub-proyecto 1 — completo, mergeado a `main`

Reemplaza el par cliente/servidor único por tres roles separados, todavía sobre un solo nodo por rol (sin réplicas, sin clúster):

- **ControlNode** (`dfsha/control_node/`) — dueño del árbol de directorios y de qué bloques componen cada archivo. Vive en memoria (nada se persiste a disco todavía; eso lo va a resolver Raft en el sub-proyecto 3). Reutiliza el mismo vocabulario de excepciones de Hito 1 (`PathNotFoundError`, `PathExistsError`, etc., ahora en `dfsha/common/exceptions.py`).
- **DataNode** (`dfsha/data_node/`) — dueño únicamente de los bytes de cada bloque. No sabe nada de rutas ni de nombres de archivo, solo de `block_id`. Cada bloque se guarda con su checksum SHA-256 calculado al escribir y reverificado al leer — si un bloque se corrompió en disco, el DataNode nunca lo sirve, devuelve un error explícito.
- **Cliente distribuido** (`dfsha/client/distributed_client.py`) — parte los archivos en bloques de 128 MB por defecto (configurable, nunca hardcodeado) y coordina: le pregunta al ControlNode dónde escribir cada bloque, se lo manda al DataNode correspondiente, y confirma cada bloque de vuelta al ControlNode antes de dar la subida por terminada.
- Shell interactiva distribuida (`dfsha/client/distributed_shell_main.py`) — es literalmente la misma shell de Hito 1 (`shell.py`, sin ningún cambio), solo apuntando al cliente distribuido en vez del cliente monolítico.
- 114 tests en total (los 54 de Hito 1 más 60 nuevos), todos pasando en `main`.

**Protocolo de subida**, en resumen: el cliente le dice al ControlNode cuánto pesa el archivo, el ControlNode reserva los `block_id` y le dice al cliente a qué DataNode escribir cada uno (`BeginUpload`); el cliente escribe cada bloque y confirma uno por uno (`ConfirmBlock`); recién cuando todos los bloques están confirmados, el archivo se hace visible (`CompleteUpload`). Si algo falla a mitad de camino, se aborta (`AbortUpload`) y el archivo nunca aparece a medias en `ls`.

## Qué falta en Hito 2

Hito 2 completo (según el enunciado) es un clúster de 3 ControlNodes con Raft más réplica de bloques. Eso se partió en 4 sub-proyectos independientes; solo el primero está hecho:

| # | Sub-proyecto | Estado |
|---|---|---|
| 1 | ControlNode + DataNode + cliente particionado (single-node) | ✅ Hecho |
| 2 | Replicación de bloques, factor 3 (pipeline DataNode1→2→3) | ⬜ Sin arrancar |
| 3 | Clúster de 3 ControlNodes con Raft (consenso, elección de líder) | ⬜ Sin arrancar |
| 4 | Contenerización (Docker) | ⬜ Sin arrancar |

Los sub-proyectos 2 y 4 pueden arrancar en cualquier momento — no dependen de nada más que del sub-proyecto 1, que ya está listo. El 3 (Raft) es el más grande; conviene hacer primero un spike corto (3 procesos locales con la librería de Raft elegida, matar el líder, confirmar que eligen otro y el log queda consistente) antes de comprometerse a integrarlo.

**Decisiones ya tomadas para lo que falta** (para no volver a discutirlas):

- **Consenso del clúster:** una librería de Raft embebida en el proceso del ControlNode (candidata: `pysyncobj`, sin validar madurez todavía con el spike). Nada de implementar Raft desde el paper, nada de etcd/ZooKeeper como sistema externo aparte — el costo operativo de correr y mantener un sistema externo, más el riesgo de reimplementar consenso a mano, no se justifican para el alcance de este proyecto.
- **Factor de replicación:** 3. Coincide con el default real de HDFS.
- **`op_id` (idempotencia de escrituras):** se genera una sola vez por operación lógica, del lado del cliente, antes del primer intento de red, y se reutiliza sin cambiar en cada reintento gRPC de esa misma operación (nunca un UUID nuevo por intento — eso anularía la idempotencia). Diferido a cuando exista Raft, porque solo tiene sentido probarlo de verdad con un escenario real de failover de líder.
- **`op_id` todavía no está implementado en el código** — el sub-proyecto 1 corre con un solo ControlNode, así que la ventana de fallo que `op_id` resolvería es mucho más chica y se decidió no construirlo antes de tener el clúster real.

## Cosas a tener en cuenta si vas a seguir sobre este código

Cosas que costó descubrir y que no vale la pena redescubrir:

- **Un identificador "opaco" sigue siendo una ruta si se une con `/`.** El DataNode valida `block_id` contra el formato exacto que genera el ControlNode (`uuid4().hex`, 32 hex minúsculas) antes de tocar el filesystem, en `dfsha/data_node/block_store.py::_block_path`. Esto no era así originalmente — se pensaba que un `block_id` "no es una ruta" y no necesitaba protección, hasta que una revisión encontró que sí se unía a una con `Path(root) / block_id`, y el operador `/` de `pathlib` descarta el lado izquierdo si el derecho es una ruta absoluta. Cualquier RPC nuevo que reciba un identificador de la red y lo use para armar una ruta en disco necesita la misma validación.
- **`ControlTree` tiene un lock global** (`dfsha/control_node/tree.py`) porque corre detrás de un `ThreadPoolExecutor` con varios workers gRPC simultáneos, y sin lock hay una condición de carrera real y reproducible en operaciones como `begin_upload`. Es un lock único y grueso sobre todo el árbol (a propósito — las operaciones son en memoria, del orden de microsegundos). Si en algún momento se vuelve un cuello de botella real, pasar a locks por subárbol, no antes.
- **El checksum del DataNode se escribe en un archivo `.sha256` aparte del bloque**, y esa escritura no es atómica como la del bloque en sí (el bloque sí usa temp-file + rename). No hay ningún camino hoy que reintente escribir el mismo bloque, así que la ventana no es alcanzable en la práctica — pero si el sub-proyecto de idempotencia agrega reintentos de bloque, hay que revisar esto primero.
- **El ControlNode confía en el checksum que le reporta el cliente en `ConfirmBlock`**, sin volver a preguntarle al DataNode. Es una decisión de diseño de este sub-proyecto, no un descuido — pero significa que hoy es posible (aunque nadie lo hace) confirmar y completar una subida sin haber escrito el bloque de verdad; recién falla al intentar descargarlo. El sub-proyecto de Raft va a replicar esta metadata tal cual, sin verificarla contra el almacenamiento real — tenerlo presente al diseñarlo.
- **Bloques huérfanos tras un `AbortUpload` parcial no se limpian todavía.** Si el cliente ya confirmó 2 de 3 bloques y el tercero falla, el `abort` borra la metadata del ControlNode pero los 2 bloques ya escritos quedan en el DataNode sin que nada los borre. Está fuera de alcance del sub-proyecto 1 a propósito; el sub-proyecto de Raft/replicación es el lugar natural para resolverlo.

## Convenciones del repo

- **Autor de todo commit:** `Jean Ardila <jardilaa@eafit.edu.co>` — es el identificador que reconoce GitHub para este repo, no usar un correo personal.
- **Mensajes de commit:** una línea, cortos, naturales — nada de listas exhaustivas entre paréntesis.
- **Tamaño de bloque:** 128 MB por defecto, siempre inyectable/configurable — nunca una constante hardcodeada suelta en el medio de una función.
- **Checksums:** SHA-256 en cada bloque, calculado al escribir y reverificado al leer, sin excepciones.
- **Excepciones de dominio:** viven en `dfsha/common/exceptions.py` y se reutilizan igual en todos los componentes (servidor de Hito 1, ControlNode, DataNode, ambos clientes) — no crear un código de error nuevo para algo que ya tiene uno.

## Estructura del código

```
dfsha/
  server/            Hito 1 — servidor monolítico
  client/
    dfsha_client.py         Hito 1 — cliente monolítico
    shell.py                shell interactiva (compartida por Hito 1 y Hito 2)
    distributed_client.py   Hito 2 — cliente que habla con ControlNode + DataNode
    distributed_shell_main.py
  control_node/      Hito 2 — árbol de directorios + metadata de bloques
  data_node/         Hito 2 — almacenamiento de bloques con checksum
  common/            Excepciones de dominio, compartidas por todo
  generated/         Código gRPC generado (no se versiona, se regenera con scripts/generate_proto.py)
proto/               Definiciones .proto (dfsha, control_node, data_node)
tests/               114 tests, un archivo por componente
```

## Cómo correr y probar

Ver `README.md` para los comandos exactos de instalación y de arranque de cada proceso (servidor de Hito 1, o el trío DataNode + ControlNode + shell distribuida de Hito 2). Para correr todos los tests:

```bash
python -m pytest tests/ -v
```
