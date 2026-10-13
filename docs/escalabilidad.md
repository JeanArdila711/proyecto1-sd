# Escalabilidad

Qué pasa cuando DFSha crece en dos direcciones: más DataNodes y más archivos. Todo lo que hay acá se midió; lo que es un cálculo o una extrapolación lo dice.

Mediciones del 2026-10-12 sobre un Apple M5 de 10 núcleos y 16 GB, con Python 3.14.6 (Darwin arm64), `pysyncobj` 0.3.17 y Docker Compose v5.4.0. Los tiempos son de esa máquina: en otra cambian, las proporciones no deberían.

## Resumen

- **Más DataNodes:** el clúster pasa de 3 a 5 DataNodes sin perder datos ni llaves. Las subidas nuevas se reparten entre los 5. Con 5, cuando cae un DataNode el sistema repone solo las 3 copias de cada bloque; con 3 no puede, porque no queda un nodo libre.
- **Agregar un DataNode no es automático:** hay que editar la configuración y reiniciar los tres ControlNodes. Los bloques que ya existían no se mueven al nodo nuevo.
- **Más archivos:** la metadata ocupa unos 430 bytes de memoria por bloque y los recorridos completos crecen en proporción a la cantidad de bloques. Con 200.000 archivos pausan la metadata 0,4 s cada 10 s, lejos de los timeouts del cliente.
- **El primer límite es el snapshot de Raft, no los recorridos.** Desde unos 25.000 archivos, el snapshot tarda más que el timeout de elección y el líder puede perder el liderazgo cada vez que lo toma. Desde 50.000 pasa en la mitad de los snapshots o más. No se pierden datos, pero el clúster cambia de líder cada pocos minutos.

## 1. Más DataNodes

### Cómo se levanta

`docker-compose.yml` trae cinco DataNodes. `dn4` y `dn5` están en el perfil `escala`, que no arranca por defecto. El archivo `.env.escala` activa el perfil y les dice a los ControlNodes y al generador de secretos que hay cinco:

```bash
docker compose --env-file .env --env-file .env.escala up -d --build
docker compose --env-file .env --env-file .env.escala ps                         # 8 servicios healthy
docker compose --env-file .env --env-file .env.escala run --rm inspect mapa
```

Sirve tanto para arrancar de cero como para crecer un clúster de 3 que ya tiene datos. Mientras el clúster tenga 5 DataNodes, todos los comandos de Compose llevan los dos `--env-file`: sin el segundo, `up -d` vuelve a crear los ControlNodes con la lista de 3. Achicar el clúster no está probado.

### Qué se probó

Con bloques de 1 MB y los tiempos de demo de `docs/GUIA.md` (sección 4), en un proyecto de Compose aparte. La columna de cada DataNode es la fila `bloques por DataNode` de `inspect mapa`.

| Paso | dn1 | dn2 | dn3 | dn4 | dn5 |
|---|---:|---:|---:|---:|---:|
| 1. Clúster de 3. Se sube `a.bin`, de 20 MB (20 bloques) | 20 | 20 | 20 | | |
| 2. Se crece a 5 con `.env.escala`, sin borrar nada | 20 | 20 | 20 | 0 | 0 |
| 3. Se sube `b.bin`, de 40 MB (40 bloques) | 44 | 44 | 44 | 24 | 24 |
| 4. Se apaga `dn5`. A los 13 s | 52 | 52 | 52 | 24 | 0 |
| 5. Vuelve `dn5`. El recolector borra sus 24 copias, que ya sobran | 52 | 52 | 52 | 24 | 0 |

Lo que muestra cada paso:

- **Paso 2.** Los 8 servicios quedan `healthy`. Las llaves de `dn1` a `dn3`, la password de Raft, los certificados y los secretos de sesión no cambian: el generador solo crea `dn4.key`, `dn5.key` y sus vistas en `secrets/mounts/`. Esto importa porque regenerar una llave dejaría ilegibles los bloques ya cifrados con ella. `a.bin` sigue en el árbol.
- **Paso 3.** Los 40 bloques de `b.bin` son 120 copias, 24 por nodo: el reparto entre los 5 es parejo. `a.bin` sigue solo en `dn1` a `dn3`.
- **Paso 4.** Los 24 bloques que tenían una copia en `dn5` recuperan su tercera copia en otro nodo. `inspect bloques /b.bin` muestra los 40 bloques con 3 réplicas `ok` (el inspector las verifica con SHA-256) y ninguna en `dn5`. Con `dn5` apagado, `a.bin` y `b.bin` bajan idénticos a los originales.
- **Paso 5.** `inspect huerfanos` muestra `dn5` con 0 bloques en disco. Como cada bloque ya tenía sus 3 copias en otros nodos, las de `dn5` eran copias de más.

Los 13 s del paso 4 son con los tiempos de demo. Con los valores normales de `.env` (6 s para dar el nodo por caído, 30 s de espera, 4 copias por ciclo de 10 s), reponer 24 copias da por cuenta cerca de un minuto y medio. Eso no se midió.

### Por qué importa tener más de 3

Con 3 DataNodes y 3 copias por bloque, cada nodo tiene una copia de todo. Si uno cae, los archivos se siguen leyendo y las subidas se aceptan con 2 copias, pero no hay dónde poner la tercera hasta que el nodo vuelva. Con 5, el re-replicador encuentra un nodo vivo que no tiene el bloque y lo copia ahí: el sistema vuelve solo a 3 copias aunque el nodo caído no regrese.

### Límites de esta parte

- **La lista de DataNodes es estática.** Cada ControlNode la recibe al arrancar, en `--datanode-addresses`. Agregar o quitar un DataNode exige cambiar `DFSHA_DATANODES` y `DFSHA_DATANODE_COUNT`, tener el servicio en `docker-compose.yml`, y volver a crear los tres ControlNodes, que Compose reinicia porque les cambió el comando. Durante ese reinicio la metadata no responde; cuánto dura no se midió.
- **No hay rebalanceo.** Un DataNode nuevo solo recibe subidas nuevas y reparaciones. Después del paso 3 los nodos viejos tienen casi el doble que los nuevos, y así se quedan.
- **La reparación no mira la carga.** El re-replicador elige el primer nodo vivo de la lista que no tenga el bloque (`dfsha/control_node/rereplicator.py`, en `run_cycle`). En el paso 4 las 24 copias fueron a `dn1`, `dn2` y `dn3`, y ninguna a `dn4`, que era el más vacío.
- **Un nodo que vuelve no se rellena.** En el paso 5, `dn5` termina vacío.

Para el alcance del curso alcanza: el clúster de la entrega tiene un tamaño fijo, y lo que sí tiene que ser dinámico, que es reaccionar cuando un nodo cae, lo cubren el monitor de liveness y el re-replicador. Una membresía dinámica pediría guardar la lista de DataNodes en el estado replicado, un protocolo de alta que le entregue al nodo nuevo su llave y la clave de las capabilities, y un rebalanceador. Nada de eso está hecho.

## 2. Más archivos

Los ControlNodes guardan toda la metadata en memoria, en un árbol con un solo lock (`dfsha/control_node/tree.py`). Hay tres operaciones que lo recorren entero:

| Operación | Quién la hace | Cada cuánto | Qué frena mientras dura |
|---|---|---|---|
| `iter_blocks` | re-replicador, en el líder | 10 s | toda la metadata del líder (lock del árbol) |
| `referenced_blocks` | recolector, en el líder | 60 s | toda la metadata del líder (lock del árbol) |
| snapshot de Raft | cada ControlNode | cuando el log pasa de 5.000 entradas o pasan 300 s | el hilo de Raft de ese nodo: no aplica comandos ni manda heartbeats |

Las frecuencias son las de `.env` y, para el snapshot, los valores por defecto de `pysyncobj` 0.3.17, que el proyecto no cambia.

### Cómo se midió

```bash
python scripts/medir_escalabilidad.py --archivos 1000,10000,50000,100000,200000 \
    --salida docs/evidencia/escalabilidad-recorridos.csv
```

El script no levanta nodos. Arma un `ControlTree` real con N archivos sintéticos (1.000 por directorio, 2 bloques cada uno, 3 copias repartidas entre 5 DataNodes) y mide sobre él las mismas funciones que usa el sistema: `iter_blocks`, `referenced_blocks`, un ciclo completo de `ReReplicator.run_cycle()` con el clúster sano, y el snapshot tal como lo escribe `pysyncobj` (su `pickle` con protocolo 2, comprimido con gzip). Los identificadores de bloque salen de la semilla 2026, así que dos corridas con los mismos argumentos miden el mismo árbol. Cada tiempo es la mediana de 7 repeticiones. La memoria es la que `tracemalloc` atribuye al árbol, no la del proceso completo.

### Resultados

| archivos | bloques | memoria (MB) | `iter_blocks` (ms) | `referenced_blocks` (ms) | ciclo del re-replicador (ms) | snapshot (ms) | snapshot (MB) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1.000 | 2.000 | 0,9 | 0,67 | 0,24 | 0,97 | 15 | 0,08 |
| 10.000 | 20.000 | 8,5 | 7,61 | 3,58 | 10,73 | 155 | 0,84 |
| 50.000 | 100.000 | 42,7 | 76,38 | 20,06 | 92,08 | 760 | 4,20 |
| 100.000 | 200.000 | 85,4 | 186,37 | 129,87 | 217,23 | 1.561 | 8,40 |
| 200.000 | 400.000 | 170,8 | 411,36 | 271,82 | 480,52 | 3.169 | 16,81 |

Datos crudos: `docs/evidencia/escalabilidad-recorridos.csv`.

### Lectura

**Memoria.** Crece en línea recta: unos 430 bytes por bloque, 850 por archivo de 2 bloques. Con bloques de 128 MiB, 400.000 bloques son 48,8 TiB de datos descritos en 171 MB. La memoria no es lo primero que se acaba.

**Recorridos.** El algoritmo es lineal, pero los tiempos de la tabla crecen más rápido que eso: `referenced_blocks` se multiplica por 6,5 al pasar de 50.000 a 100.000 archivos. La causa es el recolector de basura de Python. Cada recorrido crea cientos de miles de objetos (una copia de cada bloque, un conjunto por bloque) y eso dispara pasadas del recolector sobre un heap que ya es grande. Con el recolector apagado (`--sin-gc`) la misma medición sale recta:

| archivos | `iter_blocks` (ms) | `referenced_blocks` (ms) | ciclo del re-replicador (ms) |
|---:|---:|---:|---:|
| 50.000 | 26,80 | 14,75 | 45,30 |
| 100.000 | 53,86 | 27,85 | 90,64 |
| 200.000 | 109,20 | 63,39 | 181,05 |

Datos crudos: `docs/evidencia/escalabilidad-recorridos-sin-gc.csv`. Los nodos reales corren con el recolector activo, así que los números que valen son los de la primera tabla; esta solo explica de dónde sale el salto. Con 200.000 archivos, tres cuartas partes del tiempo de `iter_blocks` son del recolector de basura.

**Qué le cuesta al sistema.** Con 200.000 archivos, el re-replicador gasta 0,48 s de cada 10 s en comprobar que no hay nada que reparar, y durante 0,41 s de esos tiene el lock del árbol: ninguna operación de metadata avanza en el líder. El recolector suma 0,27 s cada 60 s. Los timeouts que podrían notarlo son de 5 s por RPC en el cliente y 3 s para confirmar un comando, o sea que a esta escala las pausas quedan entre siete y doce veces por debajo. Extrapolando en línea recta, una pausa de 3 s aparecería cerca de los 3 millones de bloques. Es una extrapolación, no una medición, y por lo que muestra la tabla el recolector de basura la haría llegar antes.

### El snapshot de Raft es el primer límite

El snapshot es más caro que los recorridos (3,2 s con 200.000 archivos) y pega en un lugar peor. El proyecto usa `useFork=False`, porque `fork()` con los hilos de gRPC corriendo no está soportado, y así `pysyncobj` serializa el estado dentro del mismo hilo que manda los heartbeats de Raft. Mientras el líder serializa, no le habla a nadie. Un seguidor que pasa entre 0,4 y 1,4 s sin oír al líder llama a elección.

Para ver si eso pasa de verdad:

```bash
python scripts/medir_snapshot_raft.py --archivos 10000,25000,50000,100000,200000 \
    --salida docs/evidencia/escalabilidad-snapshot-lider.csv
```

Levanta tres nodos Raft reales, cada uno en su proceso, con la configuración de un ControlNode pero sin gRPC, y les carga el mismo árbol sintético. Durante 60 s, el que es líder fuerza un snapshot, espera 4 s y anota si sigue siendo líder y cuántos términos de Raft pasaron.

| archivos | snapshots del líder | veces que otro nodo quedó de líder | términos nuevos | duración del snapshot (ms) |
|---:|---:|---:|---:|---:|
| 10.000 | 13 | 0 | 0 | 191 |
| 25.000 | 14 | 1 | 1 | 410 |
| 50.000 | 25 | 13 | 32 | 780 |
| 100.000 | 31 | 15 | 71 | 1.557 |
| 200.000 | 29 | 26 | 81 | 3.197 |

Cada término nuevo es una elección. Con 10.000 archivos no hubo ninguna. Con 25.000 el snapshot ya roza el timeout mínimo y hubo una. De 50.000 en adelante hay más elecciones que snapshots, porque una elección puede necesitar varias rondas; a veces gana el mismo nodo, y por eso la tercera columna es menor que la cuarta. Las elecciones dependen de timeouts aleatorios: otra corrida da otros conteos, con la misma forma.

La duración medida acá coincide con la columna de snapshot de la primera tabla (3.197 ms contra 3.169 ms con 200.000 archivos), que se midió sin Raft.

Qué significa:

- En un clúster real nadie fuerza el snapshot: cada nodo lo toma cuando su log pasa de 5.000 entradas o pasaron 300 s. Con más de unos 25.000 archivos, cada snapshot del líder puede costarle el liderazgo, y con más de 50.000 lo normal es que se lo cueste. Eso es un cambio de líder cada 5 minutos como mucho, y más seguido con carga, porque cada lectura que pasa por la barrera de consistencia suma una entrada al log.
- No se pierden datos: es una elección normal de Raft y lo confirmado sigue confirmado. Lo que se pierde es disponibilidad: mientras no hay líder, las operaciones de metadata fallan o esperan, y el cliente tiene que encontrar al líder nuevo.
- Esto se midió con Raft solo. No se reprodujo en el clúster completo de Docker con decenas de miles de archivos, así que el efecto sobre un cliente a mitad de una operación no está medido. Los ControlNodes reales además tienen los hilos de gRPC compitiendo por el intérprete, lo que solo puede alargar el snapshot.

Con archivos de 2 bloques de 128 MiB, 25.000 archivos son unos 6 TiB de datos. Hasta ahí el diseño funciona sin este problema, en esta máquina.

### Qué habría que cambiar para pasar de ahí

Ninguna de estas cosas está hecha:

- **Subir los timeouts de elección de Raft** por encima de lo que tarda el snapshot. Es un cambio de configuración (`build_raft_conf` ya acepta valores distintos, hoy solo los usan los tests), y el costo es que el clúster tarda ese mismo tiempo de más en notar que el líder murió.
- **Sacar el snapshot del hilo de Raft.** `pysyncobj` solo ofrece hacerlo con `fork()`, que no convive con los hilos de gRPC. Habría que separar Raft y gRPC en procesos distintos.
- **Dejar de recorrer todo el árbol.** Un índice de bloques por DataNode, mantenido en cada cambio, le evitaría al re-replicador y al recolector el recorrido completo. No arregla el snapshot.
- **Repartir la metadata** entre varios grupos de ControlNodes, cada uno con una parte del árbol. Es otro sistema.

## Qué no se puede afirmar

DFSha no escala sin límite. El plano de datos crece agregando DataNodes, a mano y sin rebalanceo. El plano de control tiene un tamaño fijo de tres nodos con toda la metadata en memoria, y su techo práctico con la configuración actual está en el orden de las decenas de miles de archivos, por el snapshot. Para el tamaño de la entrega, que son decenas de archivos, sobra por varios órdenes de magnitud.
