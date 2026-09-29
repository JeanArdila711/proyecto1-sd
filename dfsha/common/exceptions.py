from __future__ import annotations


class DFShaError(Exception):
    """Excepción base de dominio para DFSha."""


class InvalidPathError(DFShaError):
    pass


class PathNotFoundError(DFShaError):
    pass


class PathExistsError(DFShaError):
    pass


class NotEmptyError(DFShaError):
    pass


class NotAFileError(DFShaError):
    pass


class NotADirectoryError(DFShaError):
    pass


class BlockNotFoundError(DFShaError):
    """Un block_id que el DataNode no tiene."""


class BlockCorruptedError(DFShaError):
    """El checksum guardado no coincide con el contenido leído."""


class ConflictError(DFShaError):
    """La operación choca con un lock o una versión concurrente."""


class AccessDeniedError(DFShaError):
    """El usuario autenticado no puede ejecutar la operación."""


class AuthError(DFShaError):
    """La llamada no tiene credenciales válidas."""
