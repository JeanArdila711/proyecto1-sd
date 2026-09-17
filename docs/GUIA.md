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
| `rm <ruta>` | Borra un archivo |

Las rutas con espacios van entre comillas: `send "/intercambio/mi tesis.pdf" /docs/tesis.pdf`.

---

## 3. Ver el sistema por dentro

```bash
docker compose run --rm inspect estado
```

| Comando | Muestra |
|---|---|
| `inspect estado` | Quién es el líder y qué nodos están vivos |
| `inspect lider` | Solo el nombre del líder (`cn0`, `cn1` o `cn2`) |
| `inspect arbol` | Todos los directorios y archivos |
| `inspect mapa` | En qué DataNode está cada bloque de cada archivo |
| `inspect bloques /docs/tesis.pdf` | Los bloques de un archivo y el estado de cada réplica, verificando su SHA-256 |
| `inspect huerfanos` | Bloques en disco que ningún archivo usa |

Ejemplo de `inspect mapa` con bloques de 1 MB y factor 2:

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

| Prueba | Comandos | Resultado esperado |
|---|---|---|
| **Cae un DataNode** | `docker compose kill dn1` → `receive` en la shell | El archivo baja completo desde otra réplica |
| **Cae el líder** | `docker compose run --rm inspect lider` → `docker compose kill cn1` (el que salió) → `inspect estado` | Otro ControlNode es líder en ~2 s y la shell sigue funcionando |
| **Caen 2 ControlNodes** | `kill` a dos de `cn0`, `cn1`, `cn2` → `ls` en la shell | No responde: Raft necesita mayoría (2 de 3) |
| **Bloque corrupto** | ver abajo → `inspect bloques /docs/tesis.pdf` → `receive` | La réplica aparece `CORRUPTO` y el archivo baja bien desde otra |
| **Réplica caída al subir** | `docker compose kill dn3` → `send` de un archivo de varios bloques | La subida falla: toda escritura exige todas sus réplicas |
| **Apagar todo** | `docker compose down` → `docker compose up -d` → `inspect arbol` | Los archivos siguen ahí |
| **Cliente muere a mitad de subida** | poner `DFSHA_UPLOAD_LEASE_S=15` en `.env` → `send` de un archivo grande y cerrar la terminal a mitad → esperar 15 s → repetir el `send` | El segundo `send` funciona y los bloques abandonados se borran |

Revivir un nodo con sus mismos datos: `docker compose start dn1`.

Corromper un byte de un bloque en `dn2`:

```bash
docker compose exec dn2 sh -c 'f=$(ls /data | grep -v sha256 | head -1); printf "\377" | dd of=/data/$f bs=1 seek=100 count=1 conv=notrunc 2>/dev/null; echo corrompido $f'
```

---

## 5. Configurar

Los parámetros están en `.env`. Después de cambiarlos: `docker compose up -d`.

| Variable | Normal | Para la demo | Efecto |
|---|---|---|---|
| `DFSHA_BLOCK_MB` | 128 | 1 | Tamaño de bloque. Con 1, un archivo de pocos MB se parte en varios bloques |
| `DFSHA_REPLICATION` | 3 | 2 | Réplicas por bloque. Con 3 réplicas y 3 DataNodes, cada nodo guarda todos los bloques |
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

---

## Ver también

- `docs/especificacion-comunicaciones.md` — protocolos y contratos entre los componentes
- `ESTADO_PROYECTO.md` — qué está hecho, decisiones y detalles de implementación
- `docker compose logs -f cn0` — salida de un nodo
