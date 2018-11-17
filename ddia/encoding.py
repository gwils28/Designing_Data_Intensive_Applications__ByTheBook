"""Encodages de données et évolution de schéma (chapitre « Encoding and Evolution »).

Tous les encodeurs sont écrits à la main, en Python pur, pour être lus octet par octet :
chacun peut produire une **trace** (liste de `Segment`) qui dit à quoi sert chaque octet
(type, numéro de champ, longueur, valeur...). Les notebooks s'en servent pour dessiner
des cartes d'octets et pour *mesurer* les compromis entre formats.

Contenu
-------
- entiers de longueur variable : `encode_uvarint`, `decode_uvarint`, zigzag ;
- JSON compact (`json_bytes`) et sous-ensemble de MessagePack (`msgpack_encode`,
  `msgpack_decode`) ;
- Protocol Buffers simplifié : schéma (`PbField`, `PbMessage`), `pb_encode`, `pb_decode`
  avec conservation des champs inconnus ;
- Thrift BinaryProtocol et CompactProtocol (encodage seul) ;
- Avro : `avro_encode`, `avro_decode` avec résolution schéma de l'écrivain / schéma du
  lecteur, vérification statique `avro_compatible`, fichier conteneur simplifié,
  génération de schéma depuis une table SQLite ;
- registre de schémas avec modes de compatibilité (`SchemaRegistry`) ;
- données synthétiques (`make_people`) et carte d'octets (`draw_byte_map`) ;
- simulations : déploiement progressif et perte de champs inconnus
  (`simulate_rolling_upgrade`), relances d'appels distants (`simulate_retries`).
"""

from __future__ import annotations

import json
import os
import random
import re
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

# =====================================================================================
# Traces : à quoi sert chaque octet ?
# =====================================================================================

#: rôles possibles d'un segment d'octets, et leur signification
ROLES = {
    "header": "en-tête / type de conteneur",
    "name": "nom de champ",
    "tag": "type et numéro de champ",
    "length": "longueur",
    "str": "valeur chaîne",
    "int": "valeur entière",
    "count": "nombre d'éléments",
    "end": "fin",
    "union": "branche d'union",
    "other": "autre",
}


@dataclass
class Segment:
    """Plage d'octets [start, end) avec son rôle et une étiquette lisible."""

    start: int
    end: int
    role: str
    label: str = ""

    @property
    def size(self) -> int:
        return self.end - self.start


class _Out:
    """Tampon d'écriture qui enregistre (optionnellement) la trace des segments."""

    def __init__(self, trace: Optional[list] = None):
        self.buf = bytearray()
        self.trace = trace

    def put(self, data: bytes, role: str = "other", label: str = "") -> None:
        start = len(self.buf)
        self.buf += data
        if self.trace is not None and data:
            self.trace.append(Segment(start, len(self.buf), role, label))


def role_sizes(trace: Sequence[Segment]) -> Dict[str, int]:
    """Nombre d'octets par rôle dans une trace."""
    out: Dict[str, int] = {}
    for s in trace:
        out[s.role] = out.get(s.role, 0) + s.size
    return out


# =====================================================================================
# Entiers de longueur variable (varint) et zigzag
# =====================================================================================

def encode_uvarint(n: int) -> bytes:
    """Entier non négatif en base 128, 7 bits par octet, poids faibles d'abord.

    Le bit de poids fort de chaque octet vaut 1 s'il reste des octets à lire.
    """
    if n < 0:
        raise ValueError("encode_uvarint attend un entier >= 0")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def decode_uvarint(buf: bytes, pos: int = 0) -> Tuple[int, int]:
    """Décode un varint à partir de `pos` ; renvoie (valeur, position suivante)."""
    result, shift = 0, 0
    while True:
        if pos >= len(buf):
            raise ValueError("varint tronqué")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
