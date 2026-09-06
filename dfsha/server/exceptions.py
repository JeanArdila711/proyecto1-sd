class DFShaError(Exception):
    """Excepción base para todos los errores de dominio de DFSha."""


class InvalidPathError(DFShaError):
    """La ruta virtual resuelve fuera de la raíz de almacenamiento."""


class PathNotFoundError(DFShaError):
    """La ruta virtual no existe."""


class PathExistsError(DFShaError):
    """La ruta virtual ya existe."""


class NotEmptyError(DFShaError):
    """Se intentó eliminar un directorio no vacío."""


class NotAFileError(DFShaError):
    """La operación esperaba un archivo y encontró otra cosa."""


class NotADirectoryError(DFShaError):
    """La operación esperaba un directorio y encontró otra cosa."""
