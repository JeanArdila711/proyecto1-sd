# DFSha

Sistema de archivos distribuido con alta disponibilidad, rendimiento y seguridad. Proyecto 1 de Sistemas Distribuidos (ST0263/SI3007, EAFIT).

Un cliente sube y baja archivos que quedan partidos en bloques y replicados entre varios nodos. Arquitectura: **Opción 1**, Cliente/Servidor con composición y distribución del servicio (S2S).

## Arquitectura

```
                 ┌──────────── plano de control (metadatos) ────────────┐
   shell ──gRPC──►  cn0 ◄─Raft─► cn1 ◄─Raft─► cn2     (un líder, mayoría 2 de 3)
     │           └──────────────────────────────────────────────────────┘
     │                    │ Ping, re-replicación, recolector
     │           ┌────────▼──────── plano de datos (bloques) ───────────┐
     └──gRPC────►  dn1 ──pipeline──► dn2 ──pipeline──► dn3            │
                 │  bloques cifrados con AES-256-GCM en cada disco      │
                 └──────────────────────────────────────────────────────┘
```

- **ControlNodes:** árbol de directorios, qué bloques tiene cada archivo y dónde está cada réplica. Replicado con Raft (`pysyncobj`) y persistido en disco.
- **DataNodes:** guardan bloques cifrados. Replican en cascada (pipeline) y verifican la integridad al leer.
- **Cliente:** parte los archivos en bloques, los sube y los reensambla. Sigue al líder solo.

Todo el transporte es gRPC. Diagrama completo con cada flujo: [docs/arquitectura-y-flujos.excalidraw](docs/arquitectura-y-flujos.excalidraw).

## Estado

| Hito | Contenido | Estado |
|---|---|---|
| 1 | Versión monolítica C/S con RF1 (`ls cd mkdir rmdir rm`) y RF2 (`send receive`) | ✅ |
| 2 | Arquitectura distribuida: ControlNode + DataNodes, bloques, replicación en pipeline, 3 ControlNodes con Raft, Docker, especificación de comunicaciones | ✅ |
| 3 | Alta disponibilidad: detección de DataNodes caídos, escritura con 2 de 3 réplicas, re-replicación, recolector de huérfanos | ✅ |
| 3 | Consistencia y RF3: locks lectores/escritor con lease, `read` por rangos, `write` copy-on-write | ✅ |
| 3 | Seguridad: cifrado en reposo (AES-256-GCM) | ✅ |
| 3 | Seguridad: TLS en las comunicaciones, autenticación de usuarios, permisos por archivo | ⬜ |
| Final | Despliegue en AWS, informe, video | ⬜ |

Detalle de cada parte, decisiones y límites conocidos: [ESTADO_PROYECTO.md](ESTADO_PROYECTO.md).

## Uso rápido

```bash
python scripts/generate_secrets.py        # solo la primera vez: llaves de cifrado de los DataNodes
docker compose up -d --build              # 3 DataNodes + 3 ControlNodes
docker compose run --rm shell             # shell distribuida
docker compose run --rm inspect mapa      # dónde quedó cada bloque
docker compose down                       # apagar
```

Guía completa (comandos de la shell, inspector, prueba de cada funcionalidad, configuración): **[docs/GUIA.md](docs/GUIA.md)**.

## Documentación

| Documento | Contenido |
|---|---|
| [docs/GUIA.md](docs/GUIA.md) | Cómo levantarlo, usarlo y probar cada funcionalidad |
| [docs/arquitectura-y-flujos.excalidraw](docs/arquitectura-y-flujos.excalidraw) | Arquitectura y cada flujo paso a paso |
| [docs/especificacion-comunicaciones.md](docs/especificacion-comunicaciones.md) | Protocolos, RPC, mensajes y errores de los 5 enlaces |
| [ESTADO_PROYECTO.md](ESTADO_PROYECTO.md) | Qué está hecho, decisiones, límites conocidos y qué falta |

## Desarrollo sin Docker

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt           # Windows: .venv\Scripts\pip
.venv/bin/python scripts/generate_proto.py           # hay que repetirlo si cambia un .proto
.venv/bin/python -m pytest tests/ -q
```

Los componentes se arrancan con `python -m dfsha.data_node.main`, `python -m dfsha.control_node.main` y `python -m dfsha.client.distributed_shell_main`; `--help` lista sus parámetros. El servidor monolítico del Hito 1 sigue disponible en `python -m dfsha.server.main`.
