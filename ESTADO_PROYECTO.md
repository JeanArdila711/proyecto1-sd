# Estado del proyecto — DFSha

Última actualización: 2026-09-16, tras cerrar el sub-proyecto 3 de Hito 2.

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

- **ControlNode** (`dfsha/control_node/`) — dueño del árbol de directorios y de qué bloques componen cada archivo. En el sub-proyecto 1 vivía solo en memoria; desde el sub-proyecto 3 se replica y persiste con Raft. Reutiliza el mismo vocabulario de excepciones de Hito 1 (`PathNotFoundError`, `PathExistsError`, etc., ahora en `dfsha/common/exceptions.py`).
- **DataNode** (`dfsha/data_node/`) — dueño únicamente de los bytes de cada bloque. No sabe nada de rutas ni de nombres de archivo, solo de `block_id`. Cada bloque se guarda con su checksum SHA-256 calculado al escribir y reverificado al leer — si un bloque se corrompió en disco, el DataNode nunca lo sirve, devuelve un error explícito.
- **Cliente distribuido** (`dfsha/client/distributed_client.py`) — parte los archivos en bloques de 128 MB por defecto (configurable, nunca hardcodeado) y coordina: le pregunta al ControlNode dónde escribir cada bloque, se lo manda al DataNode correspondiente, y confirma cada bloque de vuelta al ControlNode antes de dar la subida por terminada.
- Shell interactiva distribuida (`dfsha/client/distributed_shell_main.py`) — es literalmente la misma shell de Hito 1 (`shell.py`, sin ningún cambio), solo apuntando al cliente distribuido en vez del cliente monolítico.
- 114 tests en total (los 54 de Hito 1 más 60 nuevos), todos pasando en `main`.

**Protocolo de subida**, en resumen: el cliente le dice al ControlNode cuánto pesa el archivo, el ControlNode reserva los `block_id` y le dice al cliente a qué DataNode escribir cada uno (`BeginUpload`); el cliente escribe cada bloque y confirma uno por uno (`ConfirmBlock`); recién cuando todos los bloques están confirmados, el archivo se hace visible (`CompleteUpload`). Si algo falla a mitad de camino, se aborta (`AbortUpload`) y el archivo nunca aparece a medias en `ls`.

### Hito 2, sub-proyecto 2 — completo

Replicación de bloques con **factor 3**, el default real de HDFS, sobre el mismo protocolo de subida (no cambió ningún RPC del ControlNode, solo la forma de sus mensajes):

- **Pipeline de escritura DN1→DN2→DN3** (`dfsha/data_node/servicer.py`). El cliente sube **una sola copia**, a la cabeza del pipeline. El primer mensaje del stream es un `WriteBlockHeader` con el `block_id` y la lista `downstream` de los DataNodes que siguen; cada DataNode escribe el bloque en disco y **al mismo tiempo** lo reenvía al siguiente, chunk por chunk, pasándole la cola de la lista. El último recibe `downstream` vacío.
- **Quórum 3 de 3.** Si cualquier réplica del pipeline falla, falla la escritura entera y la subida se aborta. Además, cada eslabón compara su checksum con el del siguiente: si no coinciden, `DATA_LOSS`. No existen bloques sub-replicados, así que no hace falta re-replicador.
- **Selección de réplicas round-robin** en el ControlNode: cada bloque arranca el pipeline un nodo más adelante que el anterior, así los bloques de un archivo se reparten entre todos los DataNodes. Los DataNodes se configuran con una lista estática (`--datanode-addresses a,b,c`) y el factor con `--replication-factor` (default 3; con menos nodos que el factor, se replica en todos los que haya).
- **Failover en lectura** (`distributed_client.py::_read_block_with_failover`). La descarga prueba las réplicas en orden: una caída (`UNAVAILABLE`) o podrida (`DATA_LOSS`) hace caer a la siguiente. Solo falla si fallan todas.
- `Remove` borra el bloque de **todas** sus réplicas (best-effort, igual que antes).
- `BlockRecord.datanode_address` (string) pasó a `datanode_addresses` (lista en orden de pipeline). Se hizo antes que Raft a propósito: cambiar la forma de esa metadata es trivial ahora, y sería una migración de log replicado después.
- 125 tests en total (11 nuevos en `tests/test_replication.py`), todos pasando.

### Hito 2, sub-proyecto 3 — completo

El ControlNode deja de ser punto único de falla: **clúster de 3 ControlNodes con consenso Raft**, usando `pysyncobj==0.3.17` embebida en el proceso.

- **Máquina de estados replicada** (`dfsha/control_node/replicated_tree.py`). `ControlTree` no cambió: se envuelve en un `SyncObjConsumer` con un único comando replicado genérico, `apply(op_id, method, args)`, que aplica una de las 7 mutaciones permitidas (lista explícita). Los lock, los checks y las excepciones de dominio de siempre siguen igual.
- **Solo el líder atiende**, tanto lecturas como escrituras. Un follower responde `UNAVAILABLE`, que para el cliente significa "probá otro nodo". Las mutaciones se confirman por mayoría antes de responder (`_commit` en el servicer).
- **Barrera de lectura** (`ReplicatedTree.read_barrier`): antes de cada `ListDir`/`ListBlocks`, el líder confirma un comando vacío por Raft. Garantiza que el nodo sigue siendo líder para la mayoría y que ya aplicó todo lo confirmado. Ver "cosas a tener en cuenta": sin esto había un bug real.
- **Persistencia en disco**: journal + snapshots de Raft en `--data-dir`. Apagar los 3 nodos y volver a levantarlos recupera el árbol completo.
- **`op_id` implementado**, tal como estaba diseñado: el cliente lo genera una vez por operación lógica y lo reutiliza en cada reintento; el ControlNode deduplica por `op_id` (en el estado replicado, así que un nuevo líder también recuerda qué ya aplicó). Es obligatorio en las 7 requests que mutan: vacío → `INVALID_ARGUMENT`.
- **Cliente con failover** (`DistributedDFShaClient._call`): recibe la lista de ControlNodes, arranca por el último líder conocido y rota ante `UNAVAILABLE`/`DEADLINE_EXCEEDED`, con timeout por intento y un presupuesto total de reintentos. La shell se arranca con `--control-nodes a,b,c`.
- Arranque de cada ControlNode: `--node-id i --raft-cluster <los 3 host:port de Raft> --data-dir <propio>`. La lista es idéntica en los 3 nodos.
- 141 tests al cerrar el sub-proyecto 3 (16 nuevos: `tests/test_replicated_tree.py`, `tests/test_raft_cluster.py` y uno en `test_control_node_servicer.py`); 150 tras las correcciones posteriores. Los tests que ya existían corren contra un clúster Raft de 1 nodo con timeouts cortos (`conftest.py::start_control_node`): hay un solo camino de código, sin modo "sin Raft". La suite pasó de 0.5 s a ~10 s.
- Verificado con procesos reales y timeouts por defecto: tras `kill -9` al líder, el cliente vuelve a responder en ~2 s y `send`/`receive` siguen funcionando; con `kill -9` a los 3 y relevantándolos, todo se recupera desde el journal.

## Qué falta en Hito 2

Hito 2 completo (según el enunciado) es un clúster de 3 ControlNodes con Raft más réplica de bloques. Eso se partió en 4 sub-proyectos independientes; los tres primeros están hechos:

| # | Sub-proyecto | Estado |
|---|---|---|
| 1 | ControlNode + DataNode + cliente particionado (single-node) | ✅ Hecho |
| 2 | Replicación de bloques, factor 3 (pipeline DataNode1→2→3) | ✅ Hecho |
| 3 | Clúster de 3 ControlNodes con Raft (consenso, elección de líder) | ✅ Hecho |
| 4 | Contenerización (Docker) | ✅ Hecho |

### Hito 2, sub-proyecto 4 — completo

- **Una sola imagen** (`Dockerfile`, `python:3.12-slim`, usuario sin root) para todos los roles: DataNode, ControlNode, shell y tests. Los stubs gRPC se generan dentro de la imagen.
- **`docker-compose.yml`** con los 3 DataNodes, los 3 ControlNodes (arrancan cuando los DataNodes están `healthy`), y dos servicios con perfil: `shell` y `tests`.
- **Raft con nombres de servicio** (`--raft-cluster cn0:6000,cn1:6000,cn2:6000`). pysyncobj escucha en la misma dirección que anuncia, así que `localhost` no funciona entre contenedores.
- **Un volumen por nodo**: el journal de Raft y los bloques sobreviven a `docker compose down` (sin `-v`).
- **La shell corre dentro de la red de Docker**, con `./intercambio` montado. Motivo: el ControlNode entrega las direcciones de los DataNodes como `dn1:50061`, que desde el host no resuelven. Es el problema de la *advertised address* de HDFS; al desplegar en máquinas distintas (AWS) hay que configurar direcciones resolubles por los clientes.
- **Verificado:** los tests (150) pasan dentro de la imagen; con los 6 contenedores, subida y bajada idénticas, caída de un DataNode con `docker compose kill`, caída del líder (nuevo líder elegido), `down` + `up` conserva el árbol, y con `DFSHA_REPLICATION=2` cada DataNode guarda solo una parte de los bloques.

### Correcciones posteriores al sub-proyecto 3

- **Subidas pendientes con lease.** Un cliente que moría a mitad de subida (Ctrl+C, proceso matado) dejaba la entrada `pending` para siempre: el nombre no aparecía en `ls`, `rm` decía "no existe" y `send` decía "ya existe", también tras reiniciar el clúster. Ahora `BeginUpload` fija una caducidad (`--upload-lease-s`, 600 s por defecto) que cada `ConfirmBlock` renueva; vencida, otro `BeginUpload` puede reemplazar la entrada y el servicer borra los bloques de la subida abandonada. La hora la fija el líder y viaja en los argumentos del comando replicado: `apply()` sigue sin leer el reloj. Entradas del journal anteriores al cambio no tienen lease y se comportan como antes.
- **La shell acepta comillas** para rutas con espacios. Se separa con `shlex` **sin** carácter de escape: `shlex.split` normal destruye las rutas de Windows (`..\datos\a.pdf` → `..datosa.pdf`).
- **`.gitignore`** ahora ignora las carpetas `dn*/` y `cn*/` que crean los comandos del README, `intercambio/` y el archivo `nul` que aparece al correr `comando 2>nul` desde bash en Windows.

Con esto, **el Hito 2 queda completo en código**. Ojo con el alcance según el enunciado: ahí el Hito 2 es *"arquitectura distribuida + especificación de comunicaciones"*, y la replicación, la alta disponibilidad y la consistencia son del Hito 3. Buena parte del Hito 3 ya está adelantada; lo que falta del Hito 2 es **el documento de especificación de los 5 enlaces** (Cliente↔ControlNode, Cliente↔DataNode, ControlNode↔ControlNode, ControlNode↔DataNode, DataNode↔DataNode).

**Decisiones ya tomadas para lo que falta** (para no volver a discutirlas):

- **Consenso del clúster:** `pysyncobj` embebida en el proceso del ControlNode — implementado en el sub-proyecto 3, validada con un spike antes de integrarla (Python 3.14, failover, persistencia). Nada de implementar Raft desde el paper, nada de etcd/ZooKeeper como sistema externo aparte — el costo operativo de correr y mantener un sistema externo, más el riesgo de reimplementar consenso a mano, no se justifican para el alcance de este proyecto.
- **`op_id` (idempotencia de escrituras):** se genera una sola vez por operación lógica, del lado del cliente, antes del primer intento de red, y se reutiliza sin cambiar en cada reintento gRPC de esa misma operación (nunca un UUID nuevo por intento — eso anularía la idempotencia). Implementado en el sub-proyecto 3.
- **Heartbeats ControlNode↔DataNode:** se decidió dejarlos fuera del sub-proyecto 3 para mantenerlo enfocado en Raft. Siguen pendientes (ver "3 DataNodes y uno caído" abajo).

## Cosas a tener en cuenta si vas a seguir sobre este código

Cosas que costó descubrir y que no vale la pena redescubrir:

- **Un identificador "opaco" sigue siendo una ruta si se une con `/`.** El DataNode valida `block_id` contra el formato exacto que genera el ControlNode (`uuid4().hex`, 32 hex minúsculas) antes de tocar el filesystem, en `dfsha/data_node/block_store.py::_block_path`. Esto no era así originalmente — se pensaba que un `block_id` "no es una ruta" y no necesitaba protección, hasta que una revisión encontró que sí se unía a una con `Path(root) / block_id`, y el operador `/` de `pathlib` descarta el lado izquierdo si el derecho es una ruta absoluta. Cualquier RPC nuevo que reciba un identificador de la red y lo use para armar una ruta en disco necesita la misma validación.
- **`ControlTree` tiene un lock global** (`dfsha/control_node/tree.py`) porque corre detrás de un `ThreadPoolExecutor` con varios workers gRPC simultáneos, y sin lock hay una condición de carrera real y reproducible en operaciones como `begin_upload`. Es un lock único y grueso sobre todo el árbol (a propósito — las operaciones son en memoria, del orden de microsegundos). Si en algún momento se vuelve un cuello de botella real, pasar a locks por subárbol, no antes.
- **El checksum del DataNode se escribe en un archivo `.sha256` aparte del bloque**, y esa escritura no es atómica como la del bloque en sí (el bloque sí usa temp-file + rename). No hay ningún camino hoy que reintente escribir el mismo bloque, así que la ventana no es alcanzable en la práctica — pero si el sub-proyecto de idempotencia agrega reintentos de bloque, hay que revisar esto primero.
- **El ControlNode confía en el checksum que le reporta el cliente en `ConfirmBlock`**, sin volver a preguntarle al DataNode. Es una decisión de diseño del sub-proyecto 1, no un descuido — pero significa que hoy es posible (aunque nadie lo hace) confirmar y completar una subida sin haber escrito el bloque de verdad; recién falla al intentar descargarlo. Raft replica esta metadata tal cual, sin verificarla contra el almacenamiento real.
- **Bloques huérfanos tras un `AbortUpload` parcial no se limpian todavía**, y con replicación el problema se multiplica por 3. Si el cliente ya confirmó 2 de 3 bloques y el tercero falla, el `abort` borra la metadata del ControlNode pero los 2 bloques ya escritos quedan en sus 3 réplicas cada uno, sin que nada los borre. (Lo que sí se limpia: si un pipeline falla aguas abajo, cada eslabón descarta su copia local de *ese* bloque antes de devolver el error.) Con Raft se sumó otra fuente: si el líder muere después de confirmar un `Remove` pero antes de mandar los `DeleteBlock`, la metadata ya no está y los bloques quedan. Resolverlo requiere un recolector que compare lo que tienen los DataNodes contra la metadata; sigue pendiente.
- **Con exactamente 3 DataNodes y uno caído, fallan TODAS las subidas.** Quórum 3 de 3 + lista estática: el ControlNode no sabe que un nodo murió, y con 3 nodos y factor 3 todo pipeline incluye a los 3. Las **lecturas** siguen funcionando (failover), las **escrituras** no. Con 4+ nodos solo fallan los bloques cuyo pipeline caiga en el nodo muerto. Lo resuelven los heartbeats, que quedaron fuera del sub-proyecto 3 y siguen pendientes. Tenerlo presente al demostrar tolerancia a fallos: se demuestra bajando un nodo **después** de subir.
- **El forwarding del pipeline usa un `ThreadPoolExecutor` propio**, separado del del servidor gRPC. No es por prolijidad: si compartiera el pool del servidor, N escrituras simultáneas podrían ocupar todos los workers esperando a que el siguiente DataNode responda, sin dejar ninguno libre para reenviar, y el pipeline se auto-bloquea.
- **La cola del forwarding está acotada a 4 chunks (4 MiB)** a propósito — es lo que impide que un bloque de 128 MB se acumule entero en RAM si el siguiente nodo va más lento. El productor se bloquea hasta que haya lugar. `tests/test_replication.py::test_pipeline_with_block_larger_than_forwarding_queue` cubre que no se cuelgue.
- **El failover de lectura tiene que descartar bytes parciales.** Una réplica puede caerse a mitad de bloque con bytes ya escritos al archivo local; `_read_block_with_failover` hace `seek` + `truncate` a la posición donde arrancó el bloque antes de probar la siguiente réplica. Sin eso el archivo final sale corrupto **en silencio**. Ojo: ni un nodo caído ni un bloque podrido ejercitan ese camino (los dos fallan antes del primer byte), por eso hay un test específico que corta el stream a mitad.
- **Un comando replicado nunca puede lanzar una excepción.** `pysyncobj` no las atrapa al aplicar el log: la entrada queda sin aplicar y el nodo la reintenta para siempre. Como los 3 nodos aplican el mismo log, **un solo `mkdir` de una ruta existente bloquearía el clúster entero** (verificado en el spike: después de un comando que lanza, la siguiente escritura normal también se queda en timeout). Por eso `ReplicatedTree.apply` convierte todo en `("error", clase, mensaje)`, incluso excepciones inesperadas, y el servicer la vuelve a lanzar del lado del líder.
- **`apply` no puede tener efectos secundarios** (red, disco, DataNodes). Corre en los 3 nodos, y otra vez cada vez que un nodo reinicia y reproduce el journal. Los `DeleteBlock` de `Remove` están en el servicer del líder, después del commit, a propósito.
- **Nunca guardarse una referencia a `ReplicatedTree.tree`.** Restaurar un snapshot reemplaza el objeto árbol entero; una referencia vieja seguiría leyendo un árbol muerto. Siempre acceder vía `self._replicated.tree`.
- **Un líder recién elegido NO tiene su árbol al día, aunque `_isLeader()` diga True.** Esto fue un bug real, encontrado corriendo los tests con la CPU cargada: tras matar al líder, `ls` no mostraba un archivo cuya subida ya se le había confirmado al cliente. El nuevo líder tiene la entrada en su log, pero no la aplicó hasta confirmar un no-op de su propio término. Por eso cada lectura pasa por `read_barrier`. De paso resuelve que un líder aislado de la mayoría sirva lecturas viejas: no logra confirmar la barrera y responde `UNAVAILABLE`. Costo: una entrada de log por lectura. **Cualquier RPC nuevo que lea el árbol tiene que pasar por `_read_barrier`**, no solo por `_require_leader`.
- **Dos reglas en el servicer que no se ven fuera de contexto:** la respuesta de `BeginUpload` se arma con lo que *devuelve* el commit, no con los block_ids que el líder acaba de proponer (si el `op_id` ya se había aplicado, el resultado guardado trae los originales; con los propuestos, el cliente escribiría bloques que nadie conoce). Y un `op_id` vacío se rechaza en vez de inventar uno: vacío deduplicaría todas las requests entre sí.
- **`useFork=False` es obligatorio** (`control_node/main.py::build_raft_conf`, y pisa cualquier override). Con el default, la compactación del log hace `fork()` del proceso con los hilos de gRPC y Raft corriendo; Python mismo advierte que el hijo puede quedar bloqueado. No se pudo reproducir un fallo determinístico — la regla se apoya en esa advertencia — así que hay un test que protege la configuración. La compactación ocurre recién cada 5000 entradas: **nunca corre sola en los tests**, por eso `test_full_cluster_restart_...` la fuerza.
- **Durabilidad del journal:** `pysyncobj` escribe el journal por `mmap` sin `fsync` por entrada. Sobrevive a que se caiga el *proceso* (`kill -9`: lo escrito ya está en el page cache del SO), pero no garantiza durabilidad ante un corte de luz o caída del SO de un nodo — ahí depende de que la mayoría del clúster siga viva. Tenerlo presente al escribir el informe: no afirmar más que eso.
- **Config de `pysyncobj`:** exige `raftMinTimeout > 3 × appendEntriesPeriod` y `connectionTimeout >= raftMaxTimeout`, validado con `assert` (con `python -O` esas validaciones desaparecen en silencio). Los tests usan timeouts cortos (`conftest.py::FAST_RAFT_CONF`); con valores más agresivos que esos, las elecciones se vuelven inestables bajo carga.

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
  control_node/      Hito 2 — árbol de directorios + metadata de bloques + selección de réplicas, replicado con Raft
  data_node/         Hito 2 — almacenamiento de bloques con checksum + pipeline de replicación
  common/            Excepciones de dominio, compartidas por todo
  generated/         Código gRPC generado (no se versiona, se regenera con scripts/generate_proto.py)
proto/               Definiciones .proto (dfsha, control_node, data_node)
tests/               150 tests, un archivo por componente
Dockerfile           imagen única para todos los roles
docker-compose.yml   clúster completo + shell + tests
```

## Cómo correr y probar

Ver `README.md` para los comandos exactos de instalación y de arranque de cada proceso (servidor de Hito 1, o los 3 DataNodes + 3 ControlNodes + shell distribuida de Hito 2). Para correr todos los tests:

```bash
python -m pytest tests/ -v
```
