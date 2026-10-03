# Estado del proyecto — DFSha

Última actualización: 2026-10-03, tras agregar TLS, Raft cifrado y transferencia paralela de bloques (Hito 3, C1).

Este documento es para que cualquiera del equipo pueda entrar al repo, entender qué hay construido, qué falta y por qué se tomó cada decisión, sin tener que reconstruir el contexto desde cero. Cómo levantarlo y probarlo está en `docs/GUIA.md`; los protocolos entre componentes, en `docs/especificacion-comunicaciones.md`.

## Qué es DFSha

Sistema de archivos distribuido: un cliente sube y baja archivos que quedan repartidos entre varios nodos, tanto en lectura como en escritura. Arquitectura elegida desde el principio: Cliente/Servidor con composición y distribución del servicio (el servicio corre como un sistema autónomo en su propia red, con una puerta de entrada abierta para clientes). Todo el transporte es gRPC — no hay HTTP REST ni sockets crudos en ningún punto, salvo el canal de Raft entre ControlNodes, que impone la librería.

## Resumen

Hitos según el enunciado (`1_DFSha_Proyecto1.md`): Hito 1 monolítico (semana 8), Hito 2 arquitectura distribuida y especificación de comunicaciones (semana 10), Hito 3 alta disponibilidad, replicación, consistencia y seguridad (semana 12), entrega final con informe, repo y video (semana 13).

| Hito | Parte | Estado |
|---|---|---|
| 1 | RF1 (`ls cd mkdir rmdir rm`) y RF2 (`send receive`), cliente/servidor monolítico | ✅ |
| 2 | ControlNode + DataNodes + cliente particionado, replicación en pipeline, 3 ControlNodes con Raft, Docker | ✅ |
| 2 | Especificación de comunicaciones de los 5 enlaces | ✅ (`docs/especificacion-comunicaciones.md`) |
| 3 | P0 — errores de dominio con tipo exacto | ✅ |
| 3 | A1 — detección de DataNodes caídos, subida con 2 de 3 réplicas | ✅ |
| 3 | A2 — re-replicación automática | ✅ |
| 3 | A3 — recolector de bloques huérfanos | ✅ |
| 3 | B1 — locks lectores/escritor con lease | ✅ |
| 3 | B2 — `read` por rangos | ✅ |
| 3 | B3 — `write` copy-on-write | ✅ |
| 3 | C4 — cifrado en reposo AES-256-GCM | ✅ |
| 3 | S1, S2 — spikes de mTLS y de Raft cifrado | ✅ (mTLS opcional: resultado negativo) |
| 3 | C1 — TLS en los enlaces gRPC y canal Raft cifrado | ✅ |
| 3 | Transferencia paralela de bloques en `send`/`receive` | ✅ |
| 3 | C2 — usuarios y autenticación | ⬜ |
| 3 | C3 — permisos por archivo y autorización de operaciones internas | ⬜ |
| Final | Despliegue en AWS, informe, video de 10–15 min | ⬜ |

430 tests en verde, en Windows y dentro de la imagen de Docker.

## Qué falta

1. **Usuarios y permisos (Hito 3).**
   - **C2 — usuarios:** registro/login, token en la metadata de cada RPC y `_users` en el estado replicado. El test de upgrade de S2 ya deja el gancho (`extension_checks`) para verificarlo sobre un journal viejo.
   - **C3 — permisos:** dueño y ACL por ruta, y capability por bloque firmada por el ControlNode, para que un cliente solo lea o escriba los bloques que se le autorizaron y que las operaciones internas del DataNode (`ReplicateBlock`, `DeleteBlock`, `ListStoredBlocks`) solo las pida un ControlNode. Hoy TLS cifra todo, pero cualquiera que tenga `ca.crt` y llegue a la red puede llamarlas.
2. **Entrega final:** despliegue en AWS Academy (direcciones anunciadas resolubles desde el cliente, puerto de Raft solo en la red privada), informe técnico y video.
3. **Documentación:** actualizar la especificación de comunicaciones cuando entren C2 y C3.

## Qué está implementado

### Hito 1 — completo, sin cambios desde su entrega

Un cliente y un servidor, cada uno un solo proceso.

- **RF1** (gestión del árbol): `ls`, `cd`, `mkdir`, `rmdir`, `rm`.
- **RF2** (transferencia de archivos): `send`/`receive`, con streaming gRPC para no cargar archivos completos en memoria ni en el cliente ni en el servidor.
- Escritura atómica en disco (temp file + `os.replace()`) en los dos lados — una descarga o subida que falla a mitad de camino nunca deja un archivo a medias con el nombre final.
- 54 tests, código en `dfsha/server/` y `dfsha/client/dfsha_client.py` + `dfsha/client/shell.py`.

### Hito 2, sub-proyecto 1 — completo

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
- **Quórum 3 de 3 dentro del pipeline.** Si cualquier réplica del pipeline falla, falla la escritura entera y la subida se aborta. Además, cada eslabón compara su checksum con el del siguiente: si no coinciden, `DATA_LOSS`. *(Desde Hito 3 A1 el ControlNode arma el pipeline solo con los DataNodes vivos y acepta 2 de 3; las copias que faltan las repone el re-replicador de A2.)*
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

**Decisiones tomadas en Hito 2** (para no volver a discutirlas):

- **Consenso del clúster:** `pysyncobj` embebida en el proceso del ControlNode — implementado en el sub-proyecto 3, validada con un spike antes de integrarla (Python 3.14, failover, persistencia). Nada de implementar Raft desde el paper, nada de etcd/ZooKeeper como sistema externo aparte — el costo operativo de correr y mantener un sistema externo, más el riesgo de reimplementar consenso a mano, no se justifican para el alcance de este proyecto.
- **`op_id` (idempotencia de escrituras):** se genera una sola vez por operación lógica, del lado del cliente, antes del primer intento de red, y se reutiliza sin cambiar en cada reintento gRPC de esa misma operación (nunca un UUID nuevo por intento — eso anularía la idempotencia). Implementado en el sub-proyecto 3.
- **Liveness de DataNodes:** quedó fuera del sub-proyecto 3 para mantenerlo enfocado en Raft. Se resolvió en Hito 3 A1 con `Ping` iniciado por cada ControlNode (no con heartbeats del DataNode).

### Hito 3 — S2: spike de Raft cifrado y upgrade de estado legacy

- `scripts/spikes/raft_password_spike.py` verifica con `SyncObjConf(password=...)` un clúster de tres nodos: líder único, réplica, reinicio desde `raft.dump` + `raft.journal` y aislamiento de un tercer nodo con password distinta. Usa puertos efímeros y no toca el clúster de desarrollo.
- `tests/fixtures/raft_legacy_9985c6d/` versiona los tres directorios persistidos reales generados con `HEAD=9985c6d`; el manifiesto fija los hashes, las firmas legacy y el outcome histórico `abort_upload -> None`.
- `tests/test_raft_upgrade.py` restaura los binarios en tres nodos con direcciones nuevas, reproduce el journal, exige convergencia y rechaza outcomes que contengan `TypeError`. Deja `assert_legacy_upgrade(..., extension_checks=...)` para B1 (`_locks`), B3 (`version`/`block_size`), C2 (`_users`) y C3 (permisos).
- Revisión 1: antes de ejecutar comandos nuevos, el upgrade compara en cada réplica una proyección canónica del árbol y `applied_ops`: conserva `/snapshot/file.bin` con sus bloques y metadata, no expone los uploads abortados y deja el journal en su estado final idéntico en los tres nodos.

### Hito 3 — S1: spike de mTLS opcional en un solo puerto

- `scripts/spikes/mtls_optional_spike.py` levanta un servidor gRPC con una CA de juguete y prueba sin certificado, con certificado válido y con uno de otra CA.
- **Resultado negativo (grpcio 1.83.1):** con `require_client_auth=False` el servidor no pide el certificado del cliente, así que el servicer nunca ve quién llama. Con `require_client_auth=True` sí lo verifica, pero rechaza a los clientes sin certificado, y el puerto del DataNode es compartido con ellos.
- Consecuencia para C1/C3: TLS de servidor en todos los enlaces y, para autorizar las operaciones internas del DataNode, capabilities firmadas por el ControlNode en vez de mTLS.

### Hito 3 — P0: errores con tipo exacto

- Los servicers de Hito 1, ControlNode y DataNode adjuntan `dfsha-error` en la metadata final de cada excepción de dominio; la matriz completa vive en `docs/especificacion-comunicaciones.md`.
- El cliente distribuido acepta únicamente nombres de una lista explícita de excepciones de dominio y conserva un fallback seguro por `StatusCode` si la metadata falta o es inválida.
- La lectura solo intenta otra réplica para `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `DATA_LOSS` o un `NOT_FOUND` de bloque identificado por metadata. `WriteBlock` valida el identificador antes de iniciar forwarding.
- Verificado con 196 pruebas en verde (`python -m pytest tests/ -q`).

#### Correcciones de revisión, iteración 1 (P0)

- `filesystem._require_directory_parent` valida cada ancestro para que `MakeDir` y `Upload` anidados bajo un archivo traduzcan a `NotADirectoryError`, sin exponer `UNKNOWN`.
- `_translate` conserva el fallback seguro si `dfsha-error` aparece duplicado, incluso con valores iguales o conflictivos.
- Verificado con 200 pruebas en verde (`python -m pytest tests/ -q`).

#### Correcciones de revisión, iteración 3 (P0)

- `NOT_FOUND` sin `dfsha-error` o con un tipo desconocido no activa failover: se relanza el error y no se consulta la siguiente réplica.
- Verificado con 202 pruebas en verde (`python -m pytest tests/ -q`).

### Hito 3 — A1: liveness de DataNodes y pipeline degradado

- Cada ControlNode sondea `Ping` localmente y excluye del pipeline las direcciones internas que no responden durante `--datanode-dead-after-s`; el estado de liveness no se replica por Raft.
- Las subidas requieren `--min-write-replicas` (2 por defecto), no necesariamente el factor completo. Con menos del mínimo responden `UNAVAILABLE` antes de reservar metadata; con el mínimo, el bloque queda sub-replicado para la reposición de A2.
- Los monitores, sus canales y los recursos de forwarding se cierran al detener los servidores; el cliente y los RPC del plano de datos usan deadlines explícitos.
- Corrección de revisión: `ReadBlock` y `WriteBlock` calculan su deadline por bloque (`base + tamaño/throughput mínimo`, 1 MiB/s por defecto); el forwarding conserva el tiempo restante del deadline entrante, evitando cortar streams activos con el timeout corto de ControlNode.

### Hito 3 — B1: locks lectores/escritor con lease

- `ControlTree` replica por Raft locks compartidos (`r`) y exclusivos (`w`) con holders, dueño y vencimiento; el líder decide `now` antes del commit y los leases vencidos se limpian en las mutaciones.
- `Lock`, `RenewLock` y `Unlock` usan `op_id`; `Remove` rechaza archivos con holders vigentes mediante `ConflictError`/`ABORTED` y metadata `dfsha-error`.
- El cliente renueva locks cada tercio del lease, los libera al cerrar y mantiene un lock compartido durante `receive`; la shell expone `lock`, `unlock` y `locks`.
- El upgrade S2 verifica que snapshots legacy inicializan `_locks` sin alterar los outcomes históricos de `applied_ops`.

#### Correcciones de revisión B1 — rutas canónicas y lease perdido

- Los locks se indexan con la ruta canónica derivada de sus partes, por lo que variantes como `/docs/a.txt`, `docs/a.txt` y `/docs//a.txt` compiten por el mismo holder; `Remove` aplica la misma clave.
- El cliente guarda y libera rutas canónicas. Si `RenewLock` informa `ConflictError`, marca el `LeaseLock` como perdido, lo elimina de sus locks propios y detiene el renovador cuando corresponde; una liberación tardía limpia estado local sin alterar el resultado de una descarga exitosa.

### Hito 3 — A2: re-replicación

- El líder toma fotografías copiadas de bloques confirmados, identifica réplicas vivas únicamente con el monitor A1 y ordena `ReplicateBlock` desde una copia sana a un DataNode vivo que falte.
- La metadata se publica con `update_block_replicas` compare-and-set y `op_id`; conflictos, timeouts y archivos borrados durante la copia no detienen el hilo. La copia de ese último caso queda huérfana para que A3 la recoja.
- El re-replicador tiene barrera Raft, límites por ciclo, deadlines por tamaño, reintentos idempotentes con backoff+jitter y cierre coordinado con el ControlNode.

#### Correcciones de revisión A2

- **Delay:** se repara una réplica caída solo cuando lleva más de `--rereplication-delay-s` muerta (según `DataNodeMonitor.dead_for`), y el ciclo nunca duerme. Antes dormía el delay antes de cada reparación y después actuaba con una foto vieja: un nodo que revivía durante la espera igual se sacaba de la metadata. Un bloque escrito con menos copias (D-P2), sin réplicas muertas, se completa en el siguiente ciclo.
- **Origen corrupto:** `ReplicateBlock` verifica el bloque local antes de abrir el stream al destino, así un origen corrupto responde `DATA_LOSS` y uno sin el bloque `NOT_FOUND` (antes salían como `UNAVAILABLE`). El re-replicador prueba el siguiente origen vivo en vez de insistir con el primero, que dejaba un bloque sin reparar para siempre.
- **Límite conocido:** el re-replicador decide por liveness, no por integridad. Una réplica corrupta en un nodo vivo sigue contando como copia: la lectura la esquiva por failover, pero nadie la repone. Detectarla requiere un escaneo periódico de checksums, que no está en el plan.

### Hito 3 — B2: lectura por rangos

- `ReadBlockRequest` suma `offset` y `length` (0 = hasta el final del bloque, compatible con clientes viejos). El DataNode valida el rango antes de tocar el disco (`INVALID_ARGUMENT` si es negativo) y `block_store.read_block` conserva su firma anterior.
- La verificación del SHA-256 sigue siendo del bloque completo, aunque se pida un rango: nunca se sirve un pedazo de un bloque podrido. El costo es leer el bloque entero para servir un rango chico; está anotado con `ponytail:` y se resuelve con checksums por chunk si llega a importar.
- Cliente: `read(path, offset, length)` devuelve bytes y `read_to_file` escribe a un archivo local de forma atómica. Ubica los bloques sumando sus tamaños, pide a cada DataNode solo su parte, con un deadline calculado por esos bytes, y hace failover entre réplicas descartando bytes parciales, como `download`.
- Por D-P3, la lectura toma el lock compartido antes de `ListBlocks` y lo mantiene hasta terminar; un `Unlock` fallido al final no convierte en error una lectura exitosa.
- Shell: `cat <ruta> [offset] [largo]` y `read <ruta> <offset> <largo> <local>`. Los comandos de RF3 (`cat`, `read`, `lock`, `unlock`, `locks`) ahora avisan "no disponible" con el cliente de Hito 1 en vez de tumbar la shell con `AttributeError`, un bug de B1 corregido de paso.
- Implementado por Claude fuera de Kiro, en paralelo con A2, para ahorrar créditos.

### Hito 3 — B3: escritura copy-on-write (`write` de RF3)

- Tres RPC nuevos (`BeginWrite`, `CommitWrite`, `AbortWrite`) y tres mutaciones replicadas (`begin_write`, `commit_write`, `abort_write`). Los bloques siguen siendo inmutables: una escritura reserva bloques nuevos, el cliente los escribe por el pipeline normal y el commit los publica juntos. Hasta el commit, el archivo visible es el anterior.
- **Determinismo:** el líder arma la propuesta (block_ids y réplicas vivas) leyendo el árbol después de la barrera, y `begin_write` la vuelve a validar dentro de `apply()`. Si el archivo cambió entre medio, `ConflictError`. La cuenta de qué bloques toca una escritura vive en una sola función pura, `tree.plan_write_slots`, que usan las dos puntas.
- **Consistencia:** `commit_write` es un compare-and-set de `FileNode.version`, y además exige que la reserva siga vigente y que el writer conserve el lock exclusivo. Los bloques viejos se borran después del commit: es seguro porque el lock exclusivo impide lectores activos. Si el borrado falla o el líder cae antes, quedan huérfanos para A3.
- **Idempotencia:** reintentar `BeginWrite` o `CommitWrite` con el mismo `op_id` devuelve el resultado original. Un `AbortWrite` después de un commit con resultado incierto no borra nada.
- **Compatibilidad:** `FileNode` gana `version` y `block_size` con defaults simples; `ControlTree._writes` se inicializa en `__setstate__`; `begin_upload` recibe `block_size` como último argumento opcional, así los journals previos se reproducen igual. Los archivos viejos infieren el tamaño de bloque del primer bloque. Probado sobre el fixture real de `9985c6d` (S2).
- **Cliente:** `open(ruta, "r"|"w")` devuelve un handle (`LeaseLock`) con `read`/`write`/`close`; `write(ruta, offset, datos)` toma el exclusivo solo para esa escritura si no hay un handle propio abierto. Una lectura reutiliza el handle propio abierto sobre la misma ruta en vez de pedir otro lock. Ante cualquier falla antes del commit, `AbortWrite` sin tapar el error original.
- **Shell:** `write <ruta> <offset> <local>`, `open <ruta> r|w` y `close <ruta>`.
- **Límites conocidos, marcados con `ponytail:`:** la reserva vive un `--upload-lease-s` y no se renueva, así que una escritura que tarde más se rechaza en el commit; el cliente arma cada bloque nuevo entero en memoria; la shell lee el archivo local entero.
- Probado en Docker: escritura en el medio de un archivo, un lector que bloquea al escritor, una escritura de 20 bytes que cruza el borde de un bloque de 1 MiB (coincide byte a byte), 0 huérfanos después de los commits, y una escritura exitosa después de matar al líder.
- Implementado por Claude fuera de Kiro, para ahorrar créditos.

### Hito 3 — A3: recolector de bloques huérfanos y réplicas sobrantes

- `dfsha/control_node/garbage_collector.py`: hilo del líder, cada `--gc-interval-s` (60 s). Con lifecycle completo: `stop_event`, `join` y cierre de canales junto con el servidor. No guarda estado entre ciclos, así que tras un failover el líder nuevo parte de cero.
- **Qué está en uso** (`ControlTree.referenced_blocks`): archivos confirmados, TODAS las subidas pendientes y las reservas COW vigentes, más las copias en vuelo del re-replicador. Las subidas pendientes cuentan aunque su lease haya vencido, porque `complete_upload` las acepta mientras nadie reemplace el nombre, y borrarles los bloques perdería datos. *(Desvío del plan, que decía "con lease vigente".)*
- **Qué borra:** un bloque que nadie usa, o una réplica que la metadata ya no asigna a ese nodo (por ejemplo, un nodo que volvió después de que re-replicaron su contenido), si tiene más de `--gc-grace-s` (1200 s) de edad. La gracia cubre lo que el líder actual no ve: copias en vuelo de un líder anterior.
- **Orden:** primero lista los DataNodes y después lee la metadata tras la barrera, así un bloque reservado entre las dos lecturas no se borra. Un DataNode que no responde se salta; `NOT_FOUND` al borrar cuenta como éxito, y un segundo barrido no hace nada.
- **`ListStoredBlocks`** (DataNode) devuelve la **edad** de cada bloque calculada con el reloj del DataNode, no un `mtime`: así no se comparan relojes de máquinas distintas. *(Desvío del plan, que decía `mtime_unix`.)*
- **`AbortUpload`** borra al instante: el servicer lee los bloques de la subida (`pending_blocks`) antes del commit y los borra si sale bien. `abort_upload` sigue sin devolver nada A PROPÓSITO: la primera versión los devolvía, y el test de upgrade de S2 la rechazó. El resultado de un comando replicado queda guardado en `applied_ops`, y reproducir un journal viejo con un retorno distinto da otro estado. Cambiar el valor de retorno de un método replicado es tan riesgoso como cambiarle la firma.
- **`inspect huerfanos`** ahora pide el inventario por RPC en vez de leer los volúmenes, y separa los bloques en uso, los que no tienen uso visible pero son jóvenes, y los que el recolector va a borrar. El servicio `inspect` de compose ya no monta los volúmenes de los DataNodes.
- Implementado por Claude fuera de Kiro, para ahorrar créditos.

### Hito 3 — C4: cifrado en reposo del DataNode

- Cada bloque físico es un único contenedor `DFSE1` en `dfsha/data_node/block_store.py`: header con versión y prefijo aleatorio, chunks de 1 MiB AES-256-GCM y metadata final autenticada con tamaño lógico, conteo y SHA-256 del plaintext. Ya no existe sidecar `.sha256`.
- `ReadBlock` autentica primero la metadata y luego únicamente los chunks requeridos para el rango; corrupción, truncación, llave incorrecta o un archivo legacy/plaintext producen `BlockCorruptedError`/`DATA_LOSS` sin migración silenciosa. `list_blocks` informa tamaño lógico y edad calculada por el reloj del DataNode.
- `--encryption-key-file` es obligatorio: lee una llave cruda de exactamente 32 bytes. `scripts/generate_secrets.py` crea `secrets/dn1.key`, `dn2.key` y `dn3.key`, con modo 0600 y sin sobrescribir salvo `--force`; Compose los monta como solo lectura. *(Desde C1 lo corre el servicio `init` de Compose y crea también la password de Raft y los certificados TLS.)* No hay rotación de llaves.
- El formato cambió sin migración: usar `docker compose down -v` antes de crear volúmenes cifrados nuevos; un volumen previo falla explícitamente al leerse.

### Hito 3 — C1: TLS, Raft cifrado y transferencia paralela

- **TLS en los enlaces gRPC ① ② ④ ⑤** (`dfsha/common/tls.py`). Una CA privada firma un único certificado de servidor con el nombre fijo `dfsha-node`, que presentan todos los ControlNodes y DataNodes. Los clientes lo verifican contra ese nombre con `grpc.ssl_target_name_override`, no contra la dirección: el mismo certificado sirve con `dn1:50061`, `localhost` o una IP privada de AWS, sin regenerarlo por despliegue. Es TLS de servidor: por el spike S1, no hay mTLS. Todos los canales salen de una sola fábrica (`channel_factory`), inyectada en el cliente, el servicer del ControlNode, el monitor, el re-replicador, el recolector, el pipeline del DataNode y el inspector.
- **Canal Raft cifrado y autenticado** (enlace ③) con `SyncObjConf(password=...)`, leída de `--raft-password-file`. Un nodo con otra password no recibe el log ni vota. El journal no cambia de formato: un clúster con datos previos los conserva al activarla (hay un test).
- **Flags opcionales** (`--tls-ca-file`, `--tls-cert-file`, `--tls-key-file`, `--raft-password-file`): sin ellos los nodos avisan y van en claro, para tests y desarrollo. Compose los pasa siempre.
- **Secretos sin Python en el host:** el servicio `init` de Compose corre `scripts/generate_secrets.py` dentro de la imagen antes que los nodos, crea solo lo que falta (llaves de los DataNodes, `raft.password`, `ca.crt`/`ca.key`, `node.crt`/`node.key`) y deja todo con dueño `uid 1000`, el usuario de la imagen. También prepara `intercambio/`.
- **Healthchecks** con `scripts/healthcheck.py`, que se conecta con TLS (un canal en claro nunca quedaría listo).
- **Transferencia paralela:** `send` y `receive` mueven hasta `--parallel-transfers` bloques a la vez (4 por defecto, `DFSHA_PARALLEL_TRANSFERS`). Cada hilo usa su propio descriptor del archivo local. En la descarga, el archivo `.part` se crea con su tamaño final y cada hilo escribe su bloque en su región (`_RegionWriter`). Al primer error, los bloques que no empezaron no arrancan, los que están en vuelo se cortan en el siguiente chunk y, con todos los hilos quietos, se hace `AbortUpload`. `read`/`cat` por rangos y `write` siguen en serie.
- **Medido en Docker** (una sola máquina, 4 CPU para los 7 contenedores, 200 MB en bloques de 8 MB): subida de 41 a 74 MB/s y bajada de 163 a 261 MB/s con 4 hilos; con 8 casi no mejora. En máquinas separadas la ganancia debería ser mayor, porque cada DataNode tiene su propio disco y su propia red.
- **Verificado en Docker:** `openssl s_client` ve TLS 1.3 con el certificado de la CA; un cliente sin TLS es rechazado; subida degradada y re-replicación, caída del líder con Raft cifrado, borrado y recolector funcionan por los canales TLS.
- **Verificado con permisos de Linux** (volúmenes ext4 en vez de carpetas de Windows, partiendo de carpetas creadas por root): sin el `--owner` de `init`, el DataNode no puede leer su llave (`Permission denied`); con él, el clúster arranca y la shell escribe en `intercambio/`.

### Revisión de cierre del Hito 3 (2026-10-03)

Revisión de las diez entregas de Jean (P0, S2, A1, B1, A2, B2, B3, A3, S1, C4) con la suite completa y una demo de punta a punta sobre los 6 contenedores.

- **Correcto:** las mutaciones replicadas de `tree.py` siguen siendo deterministas (sin reloj, `uuid`, azar ni E/S dentro de `apply`); el CAS de versión de B3 y los leases de B1 son consistentes; el nonce de C4 (prefijo aleatorio de 8 bytes + contador de 4, metadata en el contador 2^32−1) no se repite dentro de un bloque y la llave es distinta por DataNode.
- **Verificado en Docker** con tiempos cortos: mapa de particionamiento, 0 apariciones del texto plano en los discos, `cat` por rango, `read` que cruza un borde de bloque (idéntico al original), `write` COW, un lector que bloquea `write` y `rm`, subida con un DataNode caído (2 réplicas) y re-replicación a 3 al volver, réplica corrupta → `CORRUPTO` y descarga idéntica desde otra, caída del líder, 2 ControlNodes caídos sin servicio, `down`/`up` conservando el árbol, y el recolector borrando un bloque huérfano inyectado.
- **Corregido:**
  - Tres tests de liveness dormían un tiempo fijo (`time.sleep(0.12)` / `1.2`) esperando que el monitor diera por muerto un DataNode. En Linux un connect a un puerto cerrado falla al instante; en Windows tarda ~2 s, así que fallaban. Ahora esperan la condición con `conftest.py::wait_until_datanode_excluded`.
  - `test_encrypted_block_store.py` simulaba un `FileNotFoundError` sin `errno`; en Python 3.12 `Path.exists()` solo ignora errores con `ENOENT`, así que el test fallaba dentro de la imagen.
  - `scripts/generate_secrets.py` llamaba `os.fchmod`, que no existe en Windows con Python < 3.13.
  - `inspect huerfanos` usaba una gracia fija de 1200 s; ahora toma `DFSHA_GC_GRACE_S` del `.env`, igual que los ControlNodes.
- Guía (`docs/GUIA.md`), README y diagrama (`docs/arquitectura-y-flujos.excalidraw`) reescritos con el estado actual.

## Cosas a tener en cuenta si vas a seguir sobre este código

Cosas que costó descubrir y que no vale la pena redescubrir:

- **Un identificador "opaco" sigue siendo una ruta si se une con `/`.** El DataNode valida `block_id` contra el formato exacto que genera el ControlNode (`uuid4().hex`, 32 hex minúsculas) antes de tocar el filesystem, en `dfsha/data_node/block_store.py::_block_path`. Esto no era así originalmente — se pensaba que un `block_id` "no es una ruta" y no necesitaba protección, hasta que una revisión encontró que sí se unía a una con `Path(root) / block_id`, y el operador `/` de `pathlib` descarta el lado izquierdo si el derecho es una ruta absoluta. Cualquier RPC nuevo que reciba un identificador de la red y lo use para armar una ruta en disco necesita la misma validación.
- **`ControlTree` tiene un lock global** (`dfsha/control_node/tree.py`) porque corre detrás de un `ThreadPoolExecutor` con varios workers gRPC simultáneos, y sin lock hay una condición de carrera real y reproducible en operaciones como `begin_upload`. Es un lock único y grueso sobre todo el árbol (a propósito — las operaciones son en memoria, del orden de microsegundos). Si en algún momento se vuelve un cuello de botella real, pasar a locks por subárbol, no antes.
- **El checksum del DataNode vive dentro del contenedor cifrado** (Hito 3 C4): la metadata final autenticada del archivo `DFSE1` lleva el tamaño, el número de chunks y el SHA-256 del texto plano. Antes iba en un `.sha256` aparte cuya escritura no era atómica; ese sidecar ya no existe y el bloque completo se escribe con temp-file + rename.
- **El ControlNode confía en el checksum que le reporta el cliente en `ConfirmBlock`**, sin volver a preguntarle al DataNode. Es una decisión de diseño del sub-proyecto 1, no un descuido — pero significa que hoy es posible (aunque nadie lo hace) confirmar y completar una subida sin haber escrito el bloque de verdad; recién falla al intentar descargarlo. Raft replica esta metadata tal cual, sin verificarla contra el almacenamiento real.
- **Bloques huérfanos.** Aparecen por varias vías: un `AbortUpload` parcial, un líder que muere entre el commit de un `Remove` y sus `DeleteBlock`, una copia del re-replicador sobre un archivo que se borró mientras tanto, o los bloques viejos de un `write` COW cuyo borrado falló. Desde Hito 3 A3 los limpia el recolector del líder, con una gracia (`--gc-grace-s`) que cubre lo que un líder recién elegido no ve; además `AbortUpload` borra al instante los bloques de la subida descartada.
- **Con 3 DataNodes y uno caído las subidas siguen funcionando**, con 2 réplicas por bloque (Hito 3 A1, `--min-write-replicas 2`). Pero hay una ventana: el monitor tarda `--datanode-dead-after-s` en dar el nodo por muerto, y una subida que empiece en ese intervalo falla porque el pipeline todavía lo incluye. Con dos DataNodes caídos, `BeginUpload` responde `UNAVAILABLE` sin reservar nada. El re-replicador repara solo lo que lleva caído más de `--rereplication-delay-s`, para no copiar bloques enteros por un reinicio corto.
- **El forwarding del pipeline usa un `ThreadPoolExecutor` propio**, separado del del servidor gRPC. No es por prolijidad: si compartiera el pool del servidor, N escrituras simultáneas podrían ocupar todos los workers esperando a que el siguiente DataNode responda, sin dejar ninguno libre para reenviar, y el pipeline se auto-bloquea.
- **La cola del forwarding está acotada a 4 chunks (4 MiB)** a propósito — es lo que impide que un bloque de 128 MB se acumule entero en RAM si el siguiente nodo va más lento. El productor se bloquea hasta que haya lugar. `tests/test_replication.py::test_pipeline_with_block_larger_than_forwarding_queue` cubre que no se cuelgue.
- **En Linux los permisos de `secrets/` e `intercambio/` sí importan.** Los contenedores corren como `uid 1000`. Docker Desktop en Windows y macOS no aplica permisos en las carpetas montadas, así que ahí todo funciona igual; en Linux (AWS), un archivo `0600` de otro dueño no se puede leer, y una carpeta que Docker crea sola queda de root. Por eso los secretos los crea el servicio `init` como root y los pasa a `uid 1000`. Si se cambia el `uid` del `Dockerfile`, hay que cambiar el `--owner` de `init`.
- **El certificado de los nodos no lleva sus direcciones**, lleva el nombre `dfsha-node`. Un cliente nuevo (otro lenguaje, `grpcurl`) tiene que verificar contra ese nombre, no contra el host al que se conecta.
- **El failover de lectura tiene que descartar bytes parciales.** Una réplica puede caerse a mitad de bloque con bytes ya escritos al archivo local; `_read_block_with_failover` hace `seek` + `truncate` a la posición donde arrancó el bloque antes de probar la siguiente réplica. Sin eso el archivo final sale corrupto **en silencio**. Con la descarga paralela, el archivo es compartido entre hilos y `truncate` borraría los bloques de los demás: cada hilo escribe a través de un `_RegionWriter` cuyo `truncate` no recorta, y la réplica siguiente sobrescribe la región. Ojo: ni un nodo caído ni un bloque podrido ejercitan ese camino (los dos fallan antes del primer byte), por eso hay un test específico que corta el stream a mitad.
- **Un comando replicado nunca puede lanzar una excepción.** `pysyncobj` no las atrapa al aplicar el log: la entrada queda sin aplicar y el nodo la reintenta para siempre. Como los 3 nodos aplican el mismo log, **un solo `mkdir` de una ruta existente bloquearía el clúster entero** (verificado en el spike: después de un comando que lanza, la siguiente escritura normal también se queda en timeout). Por eso `ReplicatedTree.apply` convierte todo en `("error", clase, mensaje)`, incluso excepciones inesperadas, y el servicer la vuelve a lanzar del lado del líder.
- **`apply` no puede tener efectos secundarios** (red, disco, DataNodes). Corre en los 3 nodos, y otra vez cada vez que un nodo reinicia y reproduce el journal. Los `DeleteBlock` de `Remove` están en el servicer del líder, después del commit, a propósito.
- **Nunca guardarse una referencia a `ReplicatedTree.tree`.** Restaurar un snapshot reemplaza el objeto árbol entero; una referencia vieja seguiría leyendo un árbol muerto. Siempre acceder vía `self._replicated.tree`.
- **Un líder recién elegido NO tiene su árbol al día, aunque `_isLeader()` diga True.** Esto fue un bug real, encontrado corriendo los tests con la CPU cargada: tras matar al líder, `ls` no mostraba un archivo cuya subida ya se le había confirmado al cliente. El nuevo líder tiene la entrada en su log, pero no la aplicó hasta confirmar un no-op de su propio término. Por eso cada lectura pasa por `read_barrier`. De paso resuelve que un líder aislado de la mayoría sirva lecturas viejas: no logra confirmar la barrera y responde `UNAVAILABLE`. Costo: una entrada de log por lectura. **Cualquier RPC nuevo que lea el árbol tiene que pasar por `_read_barrier`**, no solo por `_require_leader`.
- **Dos reglas en el servicer que no se ven fuera de contexto:** la respuesta de `BeginUpload` se arma con lo que *devuelve* el commit, no con los block_ids que el líder acaba de proponer (si el `op_id` ya se había aplicado, el resultado guardado trae los originales; con los propuestos, el cliente escribiría bloques que nadie conoce). Y un `op_id` vacío se rechaza en vez de inventar uno: vacío deduplicaría todas las requests entre sí.
- **`useFork=False` es obligatorio** (`control_node/main.py::build_raft_conf`, y pisa cualquier override). Con el default, la compactación del log hace `fork()` del proceso con los hilos de gRPC y Raft corriendo; Python mismo advierte que el hijo puede quedar bloqueado. No se pudo reproducir un fallo determinístico — la regla se apoya en esa advertencia — así que hay un test que protege la configuración. La compactación ocurre recién cada 5000 entradas: **nunca corre sola en los tests**, por eso `test_full_cluster_restart_...` la fuerza.
- **Durabilidad del journal:** `pysyncobj` escribe el journal por `mmap` sin `fsync` por entrada. Sobrevive a que se caiga el *proceso* (`kill -9`: lo escrito ya está en el page cache del SO), pero no garantiza durabilidad ante un corte de luz o caída del SO de un nodo — ahí depende de que la mayoría del clúster siga viva. Tenerlo presente al escribir el informe: no afirmar más que eso.
- **Config de `pysyncobj`:** exige `raftMinTimeout > 3 × appendEntriesPeriod` y `connectionTimeout >= raftMaxTimeout`, validado con `assert` (con `python -O` esas validaciones desaparecen en silencio). Los tests usan timeouts cortos (`conftest.py::FAST_RAFT_CONF`); con valores más agresivos que esos, las elecciones se vuelven inestables bajo carga.

## Convenciones del repo

- **Autor de cada commit:** cada integrante con su cuenta de GitHub; sin líneas `Co-Authored-By`.
- **Mensajes de commit:** una línea, cortos, naturales — nada de listas exhaustivas entre paréntesis.
- **Tamaño de bloque:** 128 MB por defecto, siempre inyectable/configurable — nunca una constante hardcodeada suelta en el medio de una función.
- **Checksums:** SHA-256 en cada bloque, calculado al escribir y reverificado al leer, sin excepciones.
- **Excepciones de dominio:** viven en `dfsha/common/exceptions.py` y se reutilizan igual en todos los componentes (servidor de Hito 1, ControlNode, DataNode, ambos clientes) — no crear un código de error nuevo para algo que ya tiene uno.
- **Secretos:** `secrets/` y `*.pem` nunca se versionan.

## Estructura del código

```
dfsha/
  server/            Hito 1 — servidor monolítico
  client/
    dfsha_client.py         Hito 1 — cliente monolítico
    shell.py                shell interactiva (Hito 1 y distribuida, con los comandos de RF3)
    distributed_client.py   cliente distribuido: failover entre ControlNodes y réplicas, transferencia paralela, locks, rangos, write COW
    distributed_shell_main.py
  control_node/
    tree.py                 árbol, bloques, subidas, locks y escrituras COW (lógica pura, determinista)
    replicated_tree.py      máquina de estados Raft (pysyncobj), lista blanca de 14 mutaciones
    servicer.py             RPC del ControlNode; solo el líder atiende
    datanode_monitor.py     A1 — Ping a cada DataNode, vista local de vivos
    rereplicator.py         A2 — repone copias de bloques sub-replicados
    garbage_collector.py    A3 — borra bloques huérfanos y réplicas sobrantes
    main.py
  data_node/
    block_store.py          bloques cifrados DFSE1 (AES-256-GCM)
    servicer.py             RPC del DataNode y pipeline de replicación
    main.py
  common/
    exceptions.py           excepciones de dominio, compartidas por todo
    tls.py                  TLS: carga de certificados, canales, puertos, generación de la CA
  generated/         código gRPC generado (no se versiona, se regenera con scripts/generate_proto.py)
proto/               definiciones .proto (dfsha, control_node, data_node)
scripts/
  generate_proto.py         stubs gRPC
  generate_secrets.py       secretos en secrets/ (lo corre el servicio init de Compose)
  healthcheck.py            healthcheck de Docker con TLS
  inspect_cluster.py        servicio inspect: estado, líder, árbol, mapa, bloques, huérfanos
  spikes/                   spikes S1 (mTLS) y S2 (Raft cifrado, fixture legacy)
tests/               430 tests, un archivo por componente; fixtures/ con el journal legacy de 9985c6d
docs/                GUIA.md, especificacion-comunicaciones.md, arquitectura-y-flujos.excalidraw
Dockerfile           imagen única para todos los roles
docker-compose.yml   init (secretos) + clúster completo + shell + inspect + tests
.env                 parámetros del clúster
```

## Cómo correr y probar

Ver `docs/GUIA.md`. Para correr todos los tests:

```bash
docker compose run --rm tests        # dentro de la imagen
python -m pytest tests/ -q           # con el venv local
```
