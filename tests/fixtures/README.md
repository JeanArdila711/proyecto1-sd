# Fixtures legacy de Raft

`raft_legacy_9985c6d/` es un conjunto de tres directorios persistidos reales (`cn0`, `cn1`, `cn2`) producidos con el código de `main` en el commit `9985c6d`, antes de cualquier cambio funcional del Hito 3. Cada directorio contiene un `raft.dump`, un `raft.journal` y su `raft.journal.meta` de `pysyncobj`; el `manifest.json` fija el commit de origen, las firmas de los comandos legacy y los SHA-256 de los nueve artefactos persistidos.

## Reproducción exacta

La generación se ejecutó en este worktree mientras `HEAD` era `9985c6d`. El generador se niega a ejecutarse con cualquier otro `HEAD`, por lo que reproduce los artefactos exclusivamente contra la base requerida:

```bash
"/Users/jeanardila/Developer/Universidad /Sistemas Distribuidos /proyecto1-sd/.venv/bin/python" scripts/generate_proto.py
"/Users/jeanardila/Developer/Universidad /Sistemas Distribuidos /proyecto1-sd/.venv/bin/python" scripts/spikes/generate_legacy_raft_fixture.py
```

No se deben regenerar desde una rama que ya incluya B1, B3, C2 o C3. Si algún día hay que recrearlos, se crea un worktree temporal en `9985c6d`, se lleva el generador sin modificar y se ejecutan los comandos anteriores; el chequeo de `git rev-parse HEAD` y los hashes del manifiesto deben pasar antes de versionar el resultado.

## Qué cubren

El snapshot registra `make_dir`, `begin_upload`, `confirm_block`, `complete_upload` y `abort_upload`; conserva el outcome legacy `abort_upload -> None` en `applied_ops`. El journal posterior al snapshot reproduce las siete mutaciones con sus firmas anteriores a Hito 3. `tests/test_raft_upgrade.py` arranca tres `SyncObj` desde los binarios, verifica convergencia, ausencia de `TypeError` convertido silenciosamente en outcome de error y vuelve a ejecutar las firmas antiguas.

Ese mismo test expone `assert_legacy_upgrade(..., extension_checks=...)`: B1 debe verificar `_locks`, B3 `version`/`block_size` y `begin_upload` legacy, C2 `_users`, y C3 los defaults simples de permisos. Todos deben conservar los outcomes legacy de `applied_ops`, especialmente `abort_upload -> None`.
