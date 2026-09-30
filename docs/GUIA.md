# Guía de uso

Todo se hace con `docker compose` desde la raíz del repo. Los comandos son los mismos en Windows, macOS y Linux.

**Requisitos:** Docker Desktop (o Docker Engine con Compose v2) y git.

---

## 1. Levantar el clúster

```bash
git pull
docker compose up -d --build
docker compose ps
```

Tienen que aparecer 6 servicios `healthy`: `dn1`, `dn2`, `dn3` (DataNodes) y `cn0`, `cn1`, `cn2` (ControlNodes).

---

## 2. Usar la shell

```bash
docker compose run --rm shell
```

La carpeta `intercambio/` del repo se ve dentro de la shell como `/intercambio`. Pon ahí los archivos que quieras subir.

```
dfsha:/$ mkdir /docs
dfsha:/$ send /intercambio/tesis.pdf /docs/tesis.pdf
dfsha:/$ ls /docs
dfsha:/$ receive /docs/tesis.pdf /intercambio/copia.pdf
dfsha:/$ exit
```

| Comando | Qué hace |
|---|---|
| `ls [ruta]` | Lista un directorio |
| `cd <ruta>` · `pwd` | Cambia / muestra el directorio actual |
| `mkdir <ruta>` · `rmdir <ruta>` | Crea / borra un directorio vacío |
| `send <local> <remota>` | Sube un archivo |
| `receive <remota> <local>` | Descarga un archivo |
| `rm <ruta>` | Borra un archivo (falla mientras tenga un lock vigente) |
| `lock <ruta> r|w` · `unlock <ruta>` · `locks` | Toma, libera o lista locks propios con lease |

Las rutas con espacios van entre comillas: `send "/intercambio/mi tesis.pdf" /docs/tesis.pdf`.

---

## 3. Ver el sistema por dentro

```bash
docker compose run --rm inspect estado                     # quién es el líder y qué nodos están vivos
docker compose run --rm inspect lider                      # solo el nombre del líder: cn0, cn1 o cn2
docker compose run --rm inspect arbol                      # todos los directorios y archivos
docker compose run --rm inspect mapa                       # en qué DataNode está cada bloque
docker compose run --rm inspect bloques /docs/tesis.pdf    # réplicas de un archivo, verificando su SHA-256
docker compose run --rm inspect huerfanos                  # bloques en disco que ningún archivo usa
```

Ejemplo del mapa con bloques de 1 MB y factor 2:

```
  archivo / bloque                  dn1         dn2         dn3
  /docs/tesis.pdf
    b0  61ea2704     1.0 MB          C           r           ·
    b1  13be893a     1.0 MB          ·           C           r
    b2  abfda713     1.0 MB          r           ·           C
    b3  793ab22b   512.0 KB          C           r           ·
  bloques por DataNode               3           3           2
```

`C` = cabeza del pipeline de escritura, `r` = réplica, `·` = ese nodo no tiene el bloque.

---

## 4. Probar fallos

Deja la shell abierta en otra terminal (`docker compose run --rm shell`) para los pasos que dicen *en la shell*.

**Cae un DataNode** — el archivo baja completo desde otra réplica:

```bash
docker compose kill dn1
# en la shell: receive /docs/tesis.pdf /intercambio/copia.pdf
docker compose start dn1
```

**Cae el líder** — otro ControlNode es líder en ~2 s y la shell sigue funcionando:

```bash
docker compose run --rm inspect lider      # por ejemplo: cn1
docker compose kill cn1                    # el nombre que salió
docker compose run --rm inspect estado
# en la shell: ls /docs
docker compose start cn1
```

**Caen 2 ControlNodes** — no responde, porque Raft necesita mayoría (2 de 3):

```bash
docker compose kill cn0 cn1
# en la shell: ls /docs   → falla tras unos segundos
docker compose start cn0 cn1
```

**Subida con un DataNode caído y re-replicación** — esperá al menos 6 s tras la caída para que el sondeo pull lo declare muerto. Con los defaults (factor 3, mínimo 2), `send` confirma dos réplicas y el archivo queda sub-replicado. Al levantar el nodo, esperá un ciclo de 10 s: el re-replicador agrega la tercera copia. Las copias de un nodo que sigue caído se reponen en otro nodo recién cuando lleva más de 30 s muerto. Con menos de dos nodos vivos responde `UNAVAILABLE` y no hace visible el archivo:

```bash
docker compose kill dn3
sleep 7
# en la shell: send /intercambio/tesis.pdf /docs/otro.pdf
docker compose run --rm inspect bloques /docs/otro.pdf
docker compose start dn3
sleep 41
docker compose run --rm inspect bloques /docs/otro.pdf
```

**Apagar todo** — los archivos siguen ahí:

```bash
docker compose down
docker compose up -d
docker compose run --rm inspect arbol
```

**Cliente que muere a mitad de subida** — pon `DFSHA_UPLOAD_LEASE_S=15` en `.env` y `docker compose up -d`. En la shell empieza un `send` de un archivo grande y cierra esa terminal a mitad. Espera 15 s, abre otra shell y repite el mismo `send`: funciona, y los bloques abandonados se borran.

**Bloque corrupto** — la réplica aparece `CORRUPTO` y el archivo baja bien desde otra:

```bash
docker compose exec dn2 sh -c 'f=$(ls /data | grep -v sha256 | head -1); printf "\377" | dd of=/data/$f bs=1 seek=100 count=1 conv=notrunc 2>/dev/null; echo corrompido $f'
docker compose run --rm inspect bloques /docs/tesis.pdf
# en la shell: receive /docs/tesis.pdf /intercambio/copia2.pdf
```

---

## 5. Configurar

Los parámetros están en `.env`. Después de cambiarlos: `docker compose up -d`.

| Variable | Normal | Para la demo | Efecto |
|---|---|---|---|
| `DFSHA_BLOCK_MB` | 128 | 1 | Tamaño de bloque. Con 1, un archivo de pocos MB se parte en varios bloques |
| `DFSHA_REPLICATION` | 3 | 2 | Réplicas objetivo por bloque |
| `DFSHA_MIN_WRITE_REPLICAS` | 2 | 2 | Copias vivas mínimas para confirmar una subida; debe estar entre 1 y el factor |
| `DFSHA_HEARTBEAT_INTERVAL_S` | 2 | 0.5 | Período de sondeo `Ping` de cada ControlNode |
| `DFSHA_DATANODE_DEAD_AFTER_S` | 6 | 2 | Sin respuesta acumulada antes de excluir un DataNode del pipeline |
| `DFSHA_REREPLICATION_INTERVAL_S` | 10 | 1 | Período de búsqueda de bloques sub-replicados por el líder |
| `DFSHA_REREPLICATION_DELAY_S` | 30 | 2 | Cuánto tiene que llevar muerta una réplica antes de reponerla en otro nodo |
| `DFSHA_REREPLICATION_MAX_PER_CYCLE` | 4 | 4 | Máximo de copias que hace cada ciclo |
| `DFSHA_UPLOAD_LEASE_S` | 600 | 15 | Segundos hasta liberar una subida abandonada |

---

## 6. Apagar

```bash
docker compose down        # apaga y conserva los datos
docker compose down -v     # apaga y borra los datos
```

---

## 7. Tests

```bash
docker compose run --rm tests
```

### Spike S2: Raft con password y upgrade legacy

Fuera de Docker, el spike usa solo puertos efímeros y un directorio temporal;
comprueba un clúster Raft de tres nodos, reinicio desde snapshot+journal y que
un nodo con password distinta no afecta a la mayoría:

```bash
python scripts/spikes/raft_password_spike.py --workdir "$(mktemp -d)"
python -m pytest tests/test_raft_upgrade.py -q
```

Los `raft.dump`/`raft.journal` reales de `9985c6d`, sus hashes y la reproducción
controlada están en `tests/fixtures/README.md`.

---

## Ver también

- `docs/arquitectura-y-flujos.excalidraw` — arquitectura y cada flujo paso a paso (abrir en [excalidraw.com](https://excalidraw.com) o con la extensión de VS Code)
- `docs/especificacion-comunicaciones.md` — protocolos y contratos entre los componentes
- `ESTADO_PROYECTO.md` — qué está hecho, decisiones y detalles de implementación
- `docker compose logs -f cn0` — salida de un nodo


### Locks con lease (B1)

En una shell, `lock /docs/tesis.pdf r` toma un lock compartido y `lock /docs/tesis.pdf w` uno exclusivo; `locks` muestra los propios y `unlock /docs/tesis.pdf` libera uno. `receive` toma y renueva automáticamente un lock compartido hasta terminar, por lo que un escritor recibe conflicto mientras la descarga sigue activa. El ControlNode usa `--lock-lease-s` (30 s por defecto); el cliente renueva cada tercio.
