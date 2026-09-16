from __future__ import annotations

import logging
from collections import OrderedDict

from pysyncobj import SyncObjConsumer, replicated

from dfsha.common.exceptions import DFShaError
from dfsha.control_node.tree import ControlTree

logger = logging.getLogger(__name__)

# Las únicas operaciones de ControlTree que se pueden aplicar por el log. Lista
# explícita: nada de getattr libre sobre un nombre que viene de la red.
MUTATIONS = frozenset(
    {
        "make_dir",
        "remove_dir",
        "remove_file",
        "begin_upload",
        "confirm_block",
        "complete_upload",
        "abort_upload",
    }
)

# ponytail: la deduplicación recuerda las últimas N operaciones; un reintento que
# llegue después de más de N operaciones posteriores se re-ejecutaría. Los
# reintentos del cliente duran segundos, así que no es alcanzable; si algún día lo
# es, expirar por tiempo lógico en vez de por cantidad.
MAX_APPLIED_OPS = 10_000


class ReplicatedTree(SyncObjConsumer):
    """El árbol de directorios como máquina de estados replicada por Raft.

    Todo lo que se crea después de super().__init__() entra en los snapshots.
    _deserialize reemplaza `tree` por un objeto nuevo: nadie debe guardarse una
    referencia a `tree`, siempre se accede a través de esta clase.
    """

    def __init__(self) -> None:
        super().__init__()
        self.tree = ControlTree()
        self.applied_ops: OrderedDict[str, tuple] = OrderedDict()

    @replicated
    def apply(self, op_id: str, method: str, args: tuple) -> tuple:
        """Aplica una mutación y devuelve ("ok", resultado) o ("error", clase, mensaje).

        NUNCA lanza. pysyncobj no atrapa excepciones al aplicar el log: una que se
        escape deja la entrada sin aplicar y el nodo la reintenta para siempre, y
        como los 3 nodos aplican el mismo log, bloquea el clúster entero.

        Tampoco toca red ni disco: esto corre en los 3 nodos y otra vez cada vez
        que un nodo reinicia y reproduce el journal. Los efectos secundarios van
        en el servicer del líder, después del commit.
        """
        if op_id in self.applied_ops:
            return self.applied_ops[op_id]

        if method not in MUTATIONS:
            outcome = ("error", "InvalidPathError", f"operación desconocida: {method!r}")
        else:
            try:
                outcome = ("ok", getattr(self.tree, method)(*args))
            except DFShaError as exc:
                outcome = ("error", type(exc).__name__, str(exc))
            except Exception as exc:
                # un bug no puede bloquear el clúster; queda registrado y el
                # llamador recibe un error genérico
                logger.exception("error inesperado aplicando %s", method)
                outcome = ("error", "DFShaError", f"error interno en {method}: {exc}")

        self.applied_ops[op_id] = outcome
        while len(self.applied_ops) > MAX_APPLIED_OPS:
            # FIFO en orden de aplicación: idéntico en todos los nodos
            self.applied_ops.popitem(last=False)
        return outcome

    @replicated
    def read_barrier(self) -> bool:
        """Comando vacío que el líder confirma antes de cada lectura.

        _isLeader() se vuelve True apenas se gana la elección, ANTES de que el nuevo
        líder confirme su no-op y aplique las últimas entradas que ya estaban
        confirmadas: leer el árbol local en ese momento devuelve datos viejos (un
        archivo cuya subida ya se le confirmó al cliente puede no aparecer).
        Cuando esta entrada se aplica localmente, todo lo anterior del log ya se
        aplicó. Además, un líder aislado de la mayoría no logra confirmarla, así
        que nunca sirve lecturas viejas."""
        return True
