"""Direcciones internas y públicas de los DataNodes (Hito 3, D-P11).

La dirección interna (``dn1:50061``) es la identidad de un DataNode: es la que guarda la
metadata y la que usan los nodos entre sí. Fuera de la red del clúster ese nombre no
resuelve, así que el ControlNode puede recibir un mapeo explícito ``interna=pública`` y
entregarle a cada cliente la dirección por la que ese cliente sí llega.

Nunca dos listas emparejadas por posición: un mapeo con nombre no se desordena al agregar
o quitar un nodo.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

# Metadata gRPC de un RPC del ControlNode. Con el valor INTERNAL_VIEW, las direcciones de
# la respuesta salen sin traducir. Lo mandan las herramientas que corren dentro de la red
# del clúster (la shell de Compose y el inspector).
ADDRESS_VIEW_METADATA_KEY = "dfsha-address-view"
INTERNAL_VIEW = "internal"
INTERNAL_VIEW_METADATA = ((ADDRESS_VIEW_METADATA_KEY, INTERNAL_VIEW),)

_PREFIX = "mapeo de direcciones públicas"


def _is_host_port(address: str) -> bool:
    host, separator, port = address.rpartition(":")
    return (
        bool(separator and host)
        and not any(char.isspace() or char in "=," for char in address)
        and port.isascii()
        and port.isdigit()
        and 1 <= int(port) <= 65535
    )


def parse_public_addresses(value: str) -> dict[str, str]:
    """``"dn1:50061=1.2.3.4:50061,dn2:50061=1.2.3.4:50062"`` -> dict. Vacío = sin mapeo."""
    mapping: dict[str, str] = {}
    for pair in (item.strip() for item in value.split(",")):
        if not pair:
            continue
        internal, separator, public = (part.strip() for part in pair.partition("="))
        if not separator or not internal or not public:
            raise ValueError(f"{_PREFIX} inválido: {pair!r} no tiene la forma interna=pública")
        if internal in mapping:
            raise ValueError(f"{_PREFIX} inválido: la dirección interna {internal} aparece dos veces")
        mapping[internal] = public
    return mapping


def validate_public_addresses(mapping: Mapping[str, str], internal_addresses: Sequence[str]) -> None:
    """Falla si el mapeo no cubre exactamente los DataNodes configurados, si dos DataNodes
    comparten dirección pública o si alguna dirección no es host:puerto. Un mapeo vacío es
    válido: el clúster no se anuncia hacia afuera."""
    if not mapping:
        return
    for address in (*mapping, *mapping.values()):
        if not _is_host_port(address):
            raise ValueError(f"{_PREFIX} inválido: {address!r} no es host:puerto")
    unknown = sorted(set(mapping) - set(internal_addresses))
    if unknown:
        raise ValueError(f"{_PREFIX} inválido: no hay ningún DataNode configurado como {', '.join(unknown)}")
    missing = sorted(set(internal_addresses) - set(mapping))
    if missing:
        raise ValueError(f"{_PREFIX} incompleto: falta la dirección pública de {', '.join(missing)}")
    owner: dict[str, str] = {}
    for internal, public in mapping.items():
        if public in owner:
            raise ValueError(
                f"{_PREFIX} inválido: {owner[public]} y {internal} apuntan a la misma dirección pública {public}"
            )
        owner[public] = internal
