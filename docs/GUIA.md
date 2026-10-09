# Guía de uso

Todo se hace desde la raíz del repo. Los comandos son los mismos en Windows (PowerShell), macOS y Linux.

**Requisitos:** Docker Desktop (o Docker Engine con Compose v2) y git. Nada más: todo corre dentro de los contenedores.

---

## 1. Levantar el clúster

```bash
git pull
docker compose up -d --build
docker compose ps
```

Tienen que aparecer 6 servicios `healthy`: `dn1`, `dn2`, `dn3` (DataNodes) y `cn0`, `cn1`, `cn2` (ControlNodes).

La primera vez, el servicio `init` crea `secrets/`: las llaves de cifrado de los DataNodes, la password de Raft, los certificados TLS, el secreto con el que se firman los tokens de sesión, la clave con la que se firman las capabilities de bloque (`secrets/capability.key`) y la contraseña inicial del usuario `admin` (`secrets/admin.password`). Las siguientes veces conserva lo que hay y crea lo que falte. `secrets/` nunca se sube al repo.

Cada contenedor monta solo los secretos que usa, desde su carpeta en `secrets/mounts/`: un DataNode no ve la password de Raft, la shell solo ve el certificado de la CA, la clave de las capabilities solo la ven los ControlNodes y los DataNodes, y la llave de la CA no se monta en ningún contenedor.

Si vienes de la versión con usuarios pero sin permisos, basta con `docker compose up -d --build`, sin `down`: `init` crea `capability.key` y los datos se conservan. Lo que ya existía queda de `admin`, con modo `755` los directorios y `644` los archivos.

Si vienes de una versión anterior a la del cifrado en reposo, borra los datos viejos antes del `up`: `docker compose down -v`. Los DataNodes no leen bloques sin cifrar.

---

## 2. Usar la shell

```bash
docker compose run --rm shell
```

La carpeta `intercambio/` del repo es `/intercambio` dentro de la shell.

```
dfsha:/$ login admin
Contraseña de admin:                 (la de secrets/admin.password; no se muestra al escribirla)
dfsha:/$ mkdir /docs
dfsha:/$ send /intercambio/tesis.pdf /docs/tesis.pdf
dfsha:/$ receive /docs/tesis.pdf /intercambio/copia.pdf
dfsha:/$ exit
```

| Comando | Qué hace |
|---|---|
| `ls [ruta]` · `cd <ruta>` · `pwd` | Navegar |
| `mkdir <ruta>` · `rmdir <ruta>` | Crear / borrar un directorio vacío |
| `send <local> <remota>` | Subir un archivo |
| `receive <remota> <local>` | Bajar un archivo |
| `rm <ruta>` | Borrar un archivo (falla si alguien tiene un lock sobre él) |
| `cat <ruta> [offset] [largo]` | Mostrar el archivo, o un rango de bytes |
| `read <ruta> <offset> <largo> <local>` | Guardar un rango de bytes en un archivo local |
| `write <ruta> <offset> <local>` | Escribir el contenido de un archivo local desde un offset |
| `open <ruta> r\|w` · `close <ruta>` | Abrir y cerrar un archivo (toma y suelta el lock) |
| `lock <ruta> r\|w` · `unlock <ruta>` · `locks` | Tomar, soltar y listar locks |
| `login <usuario>` · `whoami` | Iniciar sesión y ver con qué usuario estás |
| `adduser <usuario> [--admin] [grupo ...]` | Crear un usuario (solo un admin) |
| `passwd [usuario]` | Cambiar tu contraseña, o la de otro usuario si eres admin |
| `ls -l [ruta]` | Listar con modo, dueño, grupo y tamaño |
| `chmod <modo> <ruta>` | Cambiar el modo, en octal (`chmod 640 notas.txt`); solo el dueño o un admin |
| `chown <usuario>[:<grupo>] <ruta>` | Cambiar el dueño (solo un admin) o el grupo (`chown :<grupo> <ruta>`, el dueño si pertenece al grupo nuevo) |

`r` = lock compartido (varios lectores), `w` = lock exclusivo (un escritor). Las rutas con espacios van entre comillas: `send "/intercambio/mi tesis.pdf" /docs/tesis.pdf`.

**Sesiones.** Sin `login`, todos los comandos que hablan con el clúster son rechazados. Las contraseñas se piden aparte y nunca van en la línea del comando. Una sesión dura 30 minutos (`DFSHA_TOKEN_TTL_S`); cuando vence, la shell pide la contraseña otra vez y hay que repetir el comando. Si la sesión vence con un archivo abierto (`open`), su lock se pierde.

Conviene cambiar la contraseña del admin con `passwd` después del primer login: la inicial queda escrita en `secrets/admin.password`.

**Permisos.** Cada archivo y directorio tiene dueño, grupo y modo `rwx` para dueño, grupo y otros, como en Unix. Hace falta `r` en un directorio para listarlo, `w` en el directorio padre para crear o borrar algo, `r` en un archivo para leerlo y `w` para escribirlo. No se usa `x`. El admin se salta los chequeos. Lo que crea un usuario queda suyo, con su primer grupo: los directorios con `755` y los archivos con `644`. Sin permiso, el comando responde `permiso denegado: ...`, por ejemplo `rm: permiso denegado: falta w en /viejo`.

La raíz es `777`, así que cualquier usuario puede crear su carpeta en `/`, pero también borrar un archivo ajeno que esté directamente en `/`. Cuando el admin ya creó la carpeta de cada usuario, conviene cerrarla con `chmod 755 /`.

---

## 3. Ver el sistema por dentro

```bash
docker compose run --rm inspect estado                     # quién es el líder y qué nodos están vivos
docker compose run --rm inspect lider                      # solo el nombre del líder: cn0, cn1 o cn2
docker compose run --rm inspect arbol                      # todos los directorios y archivos
docker compose run --rm inspect mapa                       # en qué DataNode está cada bloque
docker compose run --rm inspect bloques /docs/tesis.pdf    # réplicas de un archivo y su estado real
docker compose run --rm inspect huerfanos                  # bloques sin uso y cuáles va a borrar el recolector
```

En Git Bash de Windows, antepón `MSYS_NO_PATHCONV=1` a los comandos que llevan una ruta como `/docs/...`.

El inspector entra como `admin` con la contraseña de `secrets/admin.password`. Si ya la cambiaste, te la pide por teclado.

---

## 4. Configurar

Los parámetros están en `.env`. Después de cambiarlos: `docker compose up -d`.

Para una demo, cambia estas líneas: los archivos se parten en bloques de 1 MB y los fallos se detectan y reparan en segundos.

```
DFSHA_BLOCK_MB=1
DFSHA_HEARTBEAT_INTERVAL_S=0.5
DFSHA_DATANODE_DEAD_AFTER_S=2
DFSHA_REREPLICATION_INTERVAL_S=1
DFSHA_REREPLICATION_DELAY_S=2
DFSHA_GC_INTERVAL_S=5
DFSHA_GC_GRACE_S=5
DFSHA_UPLOAD_LEASE_S=15
```

| Variable | Normal | Qué controla |
|---|---|---|
| `DFSHA_BLOCK_MB` | 128 | Tamaño de bloque |
| `DFSHA_REPLICATION` | 3 | Copias de cada bloque |
| `DFSHA_MIN_WRITE_REPLICAS` | 2 | Copias vivas mínimas para aceptar una subida |
| `DFSHA_HEARTBEAT_INTERVAL_S` | 2 | Cada cuánto los ControlNodes hacen `Ping` a los DataNodes |
| `DFSHA_DATANODE_DEAD_AFTER_S` | 6 | Segundos sin respuesta para dar un DataNode por caído |
| `DFSHA_REREPLICATION_INTERVAL_S` | 10 | Cada cuánto el líder busca bloques con copias de menos |
| `DFSHA_REREPLICATION_DELAY_S` | 30 | Cuánto tiene que llevar caído un nodo para reponer sus copias en otro |
| `DFSHA_REREPLICATION_MAX_PER_CYCLE` | 4 | Copias por ciclo |
| `DFSHA_GC_INTERVAL_S` | 60 | Cada cuánto el líder busca bloques huérfanos |
| `DFSHA_GC_GRACE_S` | 1200 | Edad mínima de un bloque sin uso para borrarlo |
| `DFSHA_UPLOAD_LEASE_S` | 600 | Segundos para liberar una subida o escritura abandonada |
| `DFSHA_PARALLEL_TRANSFERS` | 4 | Bloques que `send` y `receive` transfieren a la vez |
| `DFSHA_TOKEN_TTL_S` | 1800 | Segundos que dura una sesión |
| `DFSHA_CAPABILITY_TTL_S` | 3600 | Segundos que dura el permiso de un bloque que el cliente recibe para subir o bajar; una transferencia cuyos últimos bloques arranquen después falla y hay que repetirla |

---

## 5. Probar cada funcionalidad

Con los valores de demo del paso 4. Deja una shell abierta en otra terminal, con la sesión iniciada (`login admin`), para los pasos que dicen *en la shell*.

### Particionamiento y replicación

```bash
# en la shell: mkdir /docs
# en la shell: send /intercambio/tesis.pdf /docs/tesis.pdf
docker compose run --rm inspect mapa
docker compose run --rm inspect bloques /docs/tesis.pdf
```

Cada bloque aparece en 3 DataNodes y cada réplica sale `ok`.

### Cae un DataNode al bajar

```bash
docker compose kill dn1
# en la shell: receive /docs/tesis.pdf /intercambio/copia.pdf
docker compose start dn1
```

El archivo baja completo desde otra réplica.

### Cae un DataNode al subir, y se repara solo

```bash
docker compose kill dn3
docker compose run --rm inspect estado                     # esperar a que dn3 salga CAÍDO
# en la shell: send /intercambio/tesis.pdf /docs/otro.pdf
docker compose run --rm inspect bloques /docs/otro.pdf     # 2 réplicas por bloque
docker compose start dn3
docker compose run --rm inspect bloques /docs/otro.pdf     # a los pocos segundos: 3 réplicas
```

### Bloque corrupto

```bash
docker compose exec dn2 sh -c 'f=$(ls /data | head -1); printf "\377" | dd of=/data/$f bs=1 seek=100 count=1 conv=notrunc 2>/dev/null; echo corrompido $f'
docker compose run --rm inspect mapa                       # para saber de qué archivo es ese bloque
docker compose run --rm inspect bloques /docs/tesis.pdf    # la réplica de dn2 sale CORRUPTO
# en la shell: receive /docs/tesis.pdf /intercambio/copia2.pdf
```

El archivo baja bien desde otra réplica.

### Cifrado en reposo

```bash
docker compose exec dn1 sh -c 'f=$(ls /data | head -1); head -c 5 /data/$f; echo'
```

Imprime `DFSE1`: lo que hay en disco es un contenedor cifrado con AES-256-GCM, no el archivo.

### Cifrado en tránsito

```bash
docker compose exec dn1 sh -c "openssl s_client -connect dn2:50061 -CAfile /secrets/ca.crt -verify_hostname dfsha-node -brief </dev/null"
docker compose logs cn0 | grep escuchando
```

El primero muestra `TLSv1.3`, el certificado `CN=dfsha-node` y `Verification: OK` contra la CA de DFSha: todo el tráfico gRPC (shell, ControlNodes y DataNodes) va cifrado. El `unexpected eof` del final es normal: `openssl` no habla gRPC. El segundo muestra `TLS=True, Raft cifrado=True`: el canal Raft entre ControlNodes va cifrado y autenticado con la password de `secrets/raft.password`.

### Usuarios y sesiones

```bash
# en una shell nueva: ls                  # rechazado: falta el token de sesión
# en la shell: login admin                # la contraseña es la de secrets/admin.password
# en la shell: whoami
# en la shell: adduser jean               # pide la contraseña nueva dos veces
# en otra shell: login jean
# en otra shell: mkdir /de-jean           # funciona: jean tiene sesión
# en otra shell: adduser otro             # denegado: solo un admin crea usuarios
# en otra shell: passwd                   # pide la contraseña actual y la nueva dos veces
```

Para ver vencer una sesión, pon `DFSHA_TOKEN_TTL_S=20` en `.env`, corre `docker compose up -d`, inicia sesión y espera 20 segundos: el siguiente comando pide la contraseña otra vez. Si matas al líder con una sesión abierta (los pasos de *Cae el líder*, más abajo), la shell sigue funcionando sin volver a pedir login: los tres ControlNodes firman y verifican con el mismo secreto.

Límite: no hay revocación (un token sirve hasta que vence, aunque cambies la contraseña).

### Permisos y capabilities

Pon en `intercambio/` dos archivos cortos, `a.txt` y `b.txt`, y un `parche.txt`. Con la sesión de admin crea dos usuarios, y después entra con cada uno en su propia shell.

```bash
# admin: adduser alice
# admin: adduser bob
# alice: mkdir /alice
# alice: send /intercambio/a.txt /alice/a.txt
# alice: send /intercambio/b.txt /alice/b.txt
# alice: ls -l /alice                       # -rw-r--r-- alice alice
# bob:   cat /alice/a.txt                   # funciona: el archivo es 644
# alice: chmod 600 /alice/a.txt
# bob:   cat /alice/a.txt                   # cat: permiso denegado: falta r en /alice/a.txt
# bob:   rm /alice/a.txt                    # rm: permiso denegado: falta w en /alice
# bob:   chmod 644 /alice/a.txt             # chmod: permiso denegado: solo el dueño o un admin cambia el modo de /alice/a.txt
# bob:   write /alice/a.txt 0 /intercambio/parche.txt   # write: permiso denegado: falta w en /alice/a.txt
# admin: chown bob /alice/a.txt
# bob:   cat /alice/a.txt                   # funciona: ahora es suyo
# alice: chown bob /alice/b.txt             # chown: permiso denegado: solo un admin cambia el dueño de /alice/b.txt
docker compose run --rm inspect bloques /alice/a.txt   # todas las réplicas ok, aunque sea 600
```

Los DataNodes no saben de usuarios: atienden a quien trae una **capability**, un permiso firmado por el ControlNode para un bloque y una operación (leer, escribir o borrar). El cliente la recibe con la lista de bloques y la manda en cada llamada. Para ver que una capability no sirve para otra cosa, bob sube dos archivos (`mkdir /bob`, `send /intercambio/a.txt /bob/a.txt`, `send /intercambio/b.txt /bob/b.txt`) y se corre este script, guardado como `intercambio/cruzadas.py`:

```python
import getpass
from pathlib import Path
import grpc
from dfsha.client.distributed_client import DistributedDFShaClient
from dfsha.common.block_token import capability_kwargs
from dfsha.common.tls import load_tls
from dfsha.generated import control_node_pb2 as cn, data_node_pb2 as dn

c = DistributedDFShaClient(["cn0:50051", "cn1:50051", "cn2:50051"], tls=load_tls(Path("/secrets/ca.crt")))
c.login("bob", getpass.getpass("Contraseña de bob: "))
a = c._call("ListBlocks", cn.ListBlocksRequest(path="/bob/a.txt")).blocks[0]
b = c._call("ListBlocks", cn.ListBlocksRequest(path="/bob/b.txt")).blocks[0]
stub = c._datanode_stub(a.datanode_addresses[0])
header = dn.WriteBlockChunk(header=dn.WriteBlockHeader(block_id=a.block_id))
casos = {
    "leer a con la capability de a": lambda: list(stub.ReadBlock(dn.ReadBlockRequest(block_id=a.block_id), **capability_kwargs(a.capability))),
    "leer a sin capability": lambda: list(stub.ReadBlock(dn.ReadBlockRequest(block_id=a.block_id))),
    "leer b con la capability de a": lambda: list(stub.ReadBlock(dn.ReadBlockRequest(block_id=b.block_id), **capability_kwargs(a.capability))),
    "escribir a con la de lectura": lambda: stub.WriteBlock(iter([header]), **capability_kwargs(a.capability)),
    "borrar a con la de lectura": lambda: stub.DeleteBlock(dn.DeleteBlockRequest(block_id=a.block_id), **capability_kwargs(a.capability)),
    "inventario con la de lectura": lambda: list(stub.ListStoredBlocks(dn.ListStoredBlocksRequest(), **capability_kwargs(a.capability))),
}
for nombre, llamada in casos.items():
    try:
        llamada()
        print(f"{nombre}: ok")
    except grpc.RpcError as exc:
        print(f"{nombre}: {exc.code().name} ({exc.details()})")
c.close()
```

```bash
docker compose run --rm --entrypoint python shell /intercambio/cruzadas.py
```

Pide la contraseña de bob e imprime:

```
leer a con la capability de a: ok
leer a sin capability: PERMISSION_DENIED (falta la capability)
leer b con la capability de a: PERMISSION_DENIED (la capability no autoriza esta operación)
escribir a con la de lectura: PERMISSION_DENIED (la capability no autoriza esta operación)
borrar a con la de lectura: PERMISSION_DENIED (la capability no autoriza esta operación)
inventario con la de lectura: PERMISSION_DENIED (la capability no autoriza esta operación)
```

Para ver vencer una capability, pon en `.env` `DFSHA_CAPABILITY_TTL_S=2`, `DFSHA_BLOCK_MB=1` y `DFSHA_PARALLEL_TRANSFERS=1`, corre `docker compose up -d` y sube un archivo de unos 300 MB: falla a mitad con `send: capability vencida: repite la operación`, porque los permisos de todos los bloques se emiten al principio. Vuelve a los valores normales y otra vez `docker compose up -d`.

Límites: sin `x`, una carpeta privada no protege un archivo `644` que tenga adentro si se conoce su ruta; un `chmod` no corta las capabilities ya emitidas, que valen hasta que vencen; y los grupos y el admin que cuentan son los del token, que puede tener hasta 30 minutos.

### Cae el líder

```bash
docker compose run --rm inspect lider                      # por ejemplo: cn1
docker compose kill cn1
docker compose run --rm inspect estado                     # otro ControlNode es LIDER
# en la shell: ls /docs
docker compose start cn1
```

### Caen 2 ControlNodes

```bash
docker compose kill cn0 cn1
# en la shell: ls /docs                                    # falla: no hay mayoría
docker compose start cn0 cn1
```

### Lectura y escritura por rangos

Pon en `intercambio/` un `nota.txt` con algunas líneas y un `parche.txt` corto.

```bash
# en la shell: send /intercambio/nota.txt /docs/nota.txt
# en la shell: cat /docs/nota.txt 0 10
# en la shell: read /docs/tesis.pdf 1048000 5000 /intercambio/rango.bin
# en la shell: write /docs/nota.txt 0 /intercambio/parche.txt
# en la shell: cat /docs/nota.txt
```

`write` escribe bloques nuevos y los publica juntos al final: si algo falla a mitad, el archivo queda como estaba.

### Locks

En una shell:

```
dfsha:/$ lock /docs/nota.txt r
```

En otra shell:

```
dfsha:/$ write /docs/nota.txt 0 /intercambio/parche.txt     → lock en conflicto
dfsha:/$ rm /docs/nota.txt                                   → el archivo tiene locks vigentes
```

Después de `unlock /docs/nota.txt` en la primera, las dos operaciones funcionan.

### Cliente que muere a mitad de subida

En la shell, empieza un `send` de un archivo grande y cierra esa terminal a mitad. Pasados `DFSHA_UPLOAD_LEASE_S` segundos, el mismo `send` desde otra shell funciona.

### Recolector de huérfanos

```bash
docker compose run --rm inspect huerfanos
```

Muestra, por DataNode, los bloques en uso, los que no tienen uso pero son recientes, y los que el recolector va a borrar en su próximo ciclo.

### Apagar todo y volver a levantar

```bash
docker compose down
docker compose up -d
docker compose run --rm inspect arbol
```

Los archivos siguen ahí.

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

- `docs/arquitectura-y-flujos.excalidraw` — arquitectura y cada flujo paso a paso (abrir en [excalidraw.com](https://excalidraw.com) o con la extensión de VS Code)
- `docs/especificacion-comunicaciones.md` — protocolos, mensajes y errores entre componentes
- `ESTADO_PROYECTO.md` — qué está hecho, qué falta y detalles de implementación
- `docker compose logs -f cn0` — salida de un nodo
