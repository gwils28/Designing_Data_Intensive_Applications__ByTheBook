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


def uvarint_size(n: int) -> int:
    """Nombre d'octets du varint de n (>= 0) : max(1, ceil(bits / 7))."""
    return max(1, (int(n).bit_length() + 6) // 7)


def zigzag_encode(n: int) -> int:
    """Entier signé -> non signé : 0, -1, 1, -2, 2... deviennent 0, 1, 2, 3, 4..."""
    return 2 * n if n >= 0 else -2 * n - 1


def zigzag_decode(z: int) -> int:
    return (z >> 1) if not z & 1 else -((z + 1) >> 1)


def encode_svarint(n: int) -> bytes:
    """Entier signé : zigzag puis varint (sint64 de Protocol Buffers, long d'Avro)."""
    return encode_uvarint(zigzag_encode(n))


def decode_svarint(buf: bytes, pos: int = 0) -> Tuple[int, int]:
    z, pos = decode_uvarint(buf, pos)
    return zigzag_decode(z), pos


def _twos(n: int, bits: int = 64) -> int:
    """Représentation en complément à deux sur `bits` bits (entier non signé)."""
    return n & ((1 << bits) - 1)


def _signed(u: int, bits: int) -> int:
    """Interprète les `bits` bits de poids faible de u comme un entier signé."""
    u &= (1 << bits) - 1
    return u - (1 << bits) if u >> (bits - 1) else u


# =====================================================================================
# JSON et MessagePack
# =====================================================================================

#: l'enregistrement d'exemple du chapitre
EXAMPLE_RECORD = {"userName": "Martin", "favoriteNumber": 1337, "interests": ["daydreaming", "hacking"]}


def json_bytes(obj: Any, pretty: bool = False) -> bytes:
    """JSON en UTF-8, sans espaces superflus (ou indenté si pretty=True)."""
    if pretty:
        return json.dumps(obj, indent=2, ensure_ascii=False).encode()
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode()


def _mp_str(out: _Out, s: str, role: str) -> None:
    b = s.encode("utf-8")
    n = len(b)
    if n < 32:
        out.put(bytes([0xA0 | n]), "length", f"str({n})")
    elif n < 256:
        out.put(bytes([0xD9, n]), "length", f"str8({n})")
    elif n < 65536:
        out.put(b"\xda" + struct.pack(">H", n), "length", f"str16({n})")
    else:
        out.put(b"\xdb" + struct.pack(">I", n), "length", f"str32({n})")
    out.put(b, role, s)


def _mp(out: _Out, obj: Any, role_str: str = "str") -> None:
    if obj is None:
        out.put(b"\xc0", "other", "nil")
    elif obj is True or obj is False:
        out.put(b"\xc3" if obj else b"\xc2", "int", str(obj).lower())
    elif isinstance(obj, int):
        n = obj
        if 0 <= n < 128:
            out.put(bytes([n]), "int", str(n))
        elif -32 <= n < 0:
            out.put(struct.pack(">b", n), "int", str(n))
        elif 0 <= n < 1 << 8:
            out.put(b"\xcc", "header", "uint8"); out.put(struct.pack(">B", n), "int", str(n))
        elif 0 <= n < 1 << 16:
            out.put(b"\xcd", "header", "uint16"); out.put(struct.pack(">H", n), "int", str(n))
        elif 0 <= n < 1 << 32:
            out.put(b"\xce", "header", "uint32"); out.put(struct.pack(">I", n), "int", str(n))
        elif 0 <= n < 1 << 64:
            out.put(b"\xcf", "header", "uint64"); out.put(struct.pack(">Q", n), "int", str(n))
        elif -(1 << 7) <= n:
            out.put(b"\xd0", "header", "int8"); out.put(struct.pack(">b", n), "int", str(n))
        elif -(1 << 15) <= n:
            out.put(b"\xd1", "header", "int16"); out.put(struct.pack(">h", n), "int", str(n))
        elif -(1 << 31) <= n:
            out.put(b"\xd2", "header", "int32"); out.put(struct.pack(">i", n), "int", str(n))
        else:
            out.put(b"\xd3", "header", "int64"); out.put(struct.pack(">q", n), "int", str(n))
    elif isinstance(obj, float):
        out.put(b"\xcb", "header", "float64"); out.put(struct.pack(">d", obj), "int", repr(obj))
    elif isinstance(obj, str):
        _mp_str(out, obj, role_str)
    elif isinstance(obj, (bytes, bytearray)):
        n = len(obj)
        if n < 256:
            out.put(bytes([0xC4, n]), "length", f"bin8({n})")
        elif n < 65536:
            out.put(b"\xc5" + struct.pack(">H", n), "length", f"bin16({n})")
        else:
            out.put(b"\xc6" + struct.pack(">I", n), "length", f"bin32({n})")
        out.put(bytes(obj), "str", f"{n} octets")
    elif isinstance(obj, (list, tuple)):
        n = len(obj)
        if n < 16:
            out.put(bytes([0x90 | n]), "header", f"array({n})")
        elif n < 65536:
            out.put(b"\xdc" + struct.pack(">H", n), "header", f"array16({n})")
        else:
            out.put(b"\xdd" + struct.pack(">I", n), "header", f"array32({n})")
        for x in obj:
            _mp(out, x)
    elif isinstance(obj, dict):
        n = len(obj)
        if n < 16:
            out.put(bytes([0x80 | n]), "header", f"map({n})")
        elif n < 65536:
            out.put(b"\xde" + struct.pack(">H", n), "header", f"map16({n})")
        else:
            out.put(b"\xdf" + struct.pack(">I", n), "header", f"map32({n})")
        for k, v in obj.items():
            if not isinstance(k, str):
                raise TypeError("ce MessagePack minimal n'accepte que des clés chaînes")
            _mp_str(out, k, "name")
            _mp(out, v)
    else:
        raise TypeError(f"type non pris en charge : {type(obj).__name__}")


def msgpack_encode(obj: Any, trace: Optional[list] = None) -> bytes:
    """Encode un objet JSON (dict, list, str, int, float, bool, None, bytes) en MessagePack."""
    out = _Out(trace)
    _mp(out, obj)
    return bytes(out.buf)


def msgpack_decode(buf: bytes) -> Any:
    obj, pos = _mp_read(buf, 0)
    if pos != len(buf):
        raise ValueError("octets en trop après l'objet MessagePack")
    return obj


def _mp_read(b: bytes, p: int) -> Tuple[Any, int]:
    t = b[p]
    p += 1
    if t < 0x80:
        return t, p
    if 0x80 <= t <= 0x8F:
        return _mp_map(b, p, t & 0x0F)
    if 0x90 <= t <= 0x9F:
        return _mp_arr(b, p, t & 0x0F)
    if 0xA0 <= t <= 0xBF:
        n = t & 0x1F
        return b[p:p + n].decode(), p + n
    if t >= 0xE0:
        return t - 256, p
    fixed = {0xCC: ">B", 0xCD: ">H", 0xCE: ">I", 0xCF: ">Q", 0xD0: ">b", 0xD1: ">h", 0xD2: ">i",
             0xD3: ">q", 0xCB: ">d", 0xCA: ">f"}
    if t in fixed:
        fmt = fixed[t]
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, b[p:p + size])[0], p + size
    if t == 0xC0:
        return None, p
    if t in (0xC2, 0xC3):
        return t == 0xC3, p
    lens = {0xD9: ">B", 0xDA: ">H", 0xDB: ">I", 0xC4: ">B", 0xC5: ">H", 0xC6: ">I",
            0xDC: ">H", 0xDD: ">I", 0xDE: ">H", 0xDF: ">I"}
    if t in lens:
        fmt = lens[t]
        size = struct.calcsize(fmt)
        n = struct.unpack(fmt, b[p:p + size])[0]
        p += size
        if t in (0xD9, 0xDA, 0xDB):
            return b[p:p + n].decode(), p + n
        if t in (0xC4, 0xC5, 0xC6):
            return bytes(b[p:p + n]), p + n
        if t in (0xDC, 0xDD):
            return _mp_arr(b, p, n)
        return _mp_map(b, p, n)
    raise ValueError(f"octet de type MessagePack inconnu : 0x{t:02x}")


def _mp_arr(b, p, n):
    out = []
    for _ in range(n):
        x, p = _mp_read(b, p)
        out.append(x)
    return out, p


def _mp_map(b, p, n):
    out = {}
    for _ in range(n):
        k, p = _mp_read(b, p)
        v, p = _mp_read(b, p)
        out[k] = v
    return out, p


# =====================================================================================
# Protocol Buffers (simplifié, syntaxe proto3)
# =====================================================================================

#: types d'onde (wire types) : 0 varint, 1 64 bits fixe, 2 longueur préfixée, 5 32 bits fixe
WIRE_TYPE = {
    "int32": 0, "int64": 0, "uint32": 0, "uint64": 0, "sint32": 0, "sint64": 0, "bool": 0,
    "enum": 0, "fixed64": 1, "sfixed64": 1, "double": 1, "string": 2, "bytes": 2,
    "fixed32": 5, "sfixed32": 5, "float": 5,
}


_WT_NAME = {0: "VAR", 1: "I64", 2: "LEN", 5: "I32"}


@dataclass
class PbField:
    name: str
    tag: int
    type: Any  # nom de type (str) ou PbMessage pour un message imbriqué
    repeated: bool = False


@dataclass
class PbMessage:
    name: str
    fields: List[PbField]
    reserved: List[int] = field(default_factory=list)

    def by_tag(self) -> Dict[int, PbField]:
        return {f.tag: f for f in self.fields}

    def to_proto(self) -> str:
        """Texte du schéma dans la syntaxe de l'IDL Protocol Buffers."""
        lines = [f"message {self.name} {{"]
        for f in self.fields:
            t = f.type.name if isinstance(f.type, PbMessage) else f.type
            lines.append(f"    {'repeated ' if f.repeated else ''}{t} {f.name} = {f.tag};")
        if self.reserved:
            lines.append(f"    reserved {', '.join(map(str, self.reserved))};")
        lines.append("}")
        return "\n".join(lines)


#: le schéma Person du chapitre
PERSON_PB = PbMessage("Person", [
    PbField("user_name", 1, "string"),
    PbField("favorite_number", 2, "int64"),
    PbField("interests", 3, "string", repeated=True),
])

#: correspondance entre les noms de l'exemple JSON et ceux du schéma Protocol Buffers
PB_NAMES = {"userName": "user_name", "favoriteNumber": "favorite_number", "interests": "interests"}


def to_pb_record(rec: dict) -> dict:
    """Renomme les champs camelCase de l'exemple en snake_case (convention protobuf)."""
    return {PB_NAMES.get(k, k): v for k, v in rec.items()}


def _wt(f: PbField) -> int:
    return 2 if isinstance(f.type, PbMessage) else WIRE_TYPE[f.type]


def _pb_scalar(t: str, v: Any) -> bytes:
    if t in ("int32", "int64", "enum"):
        return encode_uvarint(_twos(int(v)))          # négatif : 10 octets
    if t in ("uint32", "uint64"):
        return encode_uvarint(int(v))
    if t in ("sint32", "sint64"):
        return encode_svarint(int(v))
    if t == "bool":
        return b"\x01" if v else b"\x00"
    if t == "double":
        return struct.pack("<d", v)
    if t == "float":
        return struct.pack("<f", v)
    if t == "fixed64":
        return struct.pack("<Q", v)
    if t == "sfixed64":
        return struct.pack("<q", v)
    if t == "fixed32":
        return struct.pack("<I", v)
    if t == "sfixed32":
        return struct.pack("<i", v)
    raise ValueError(f"type scalaire inconnu : {t}")


def _pb_default(f: PbField) -> Any:
    if f.repeated:
        return []
    if isinstance(f.type, PbMessage):
        return None
    return {"string": "", "bytes": b"", "bool": False, "double": 0.0, "float": 0.0}.get(f.type, 0)


def pb_encode(record: dict, schema: PbMessage, trace: Optional[list] = None,
              packed: bool = True) -> bytes:
    """Encode un dict selon le schéma. Les champs absents (ou à leur valeur par défaut
    en proto3) ne sont pas écrits. La clé spéciale ``"__unknown__"`` (octets bruts) est
    recopiée telle quelle à la fin : c'est ainsi qu'un lecteur ancien préserve les champs
    qu'il ne connaît pas."""
    out = _Out(trace)
    for f in schema.fields:
        if f.name not in record:
            continue
        v = record[f.name]
        if v is None or (not f.repeated and not isinstance(f.type, PbMessage) and v == _pb_default(f)):
            continue
        values = list(v) if f.repeated else [v]
        if f.repeated and not values:
            continue
        if f.repeated and packed and _wt(f) != 2:
            body = b"".join(_pb_scalar(f.type, x) for x in values)
            out.put(encode_uvarint(f.tag << 3 | 2), "tag", f"#{f.tag} LEN")
            out.put(encode_uvarint(len(body)), "length", str(len(body)))
            out.put(body, "int", ", ".join(map(str, values)))
            continue
        for x in values:
            wt = _wt(f)
            out.put(encode_uvarint(f.tag << 3 | wt), "tag", f"#{f.tag} {_WT_NAME[wt]}")
            if isinstance(f.type, PbMessage):
                body = pb_encode(x, f.type)
                out.put(encode_uvarint(len(body)), "length", str(len(body)))
                out.put(body, "other", f.type.name)
            elif f.type == "string":
                b = str(x).encode("utf-8")
                out.put(encode_uvarint(len(b)), "length", str(len(b)))
                out.put(b, "str", str(x))
            elif f.type == "bytes":
                out.put(encode_uvarint(len(x)), "length", str(len(x)))
                out.put(bytes(x), "str", f"{len(x)} octets")
            else:
                out.put(_pb_scalar(f.type, x), "int", str(x))
    unknown = record.get("__unknown__")
    if unknown:
        out.put(bytes(unknown), "other", "champs inconnus préservés")
    return bytes(out.buf)


def _pb_read_raw(buf: bytes, pos: int, wt: int) -> Tuple[bytes, int]:
    """Lit la charge utile d'un champ selon son type d'onde ; renvoie (octets, position)."""
    start = pos
    if wt == 0:
        _, pos = decode_uvarint(buf, pos)
        return buf[start:pos], pos
    if wt == 1:
        return buf[pos:pos + 8], pos + 8
    if wt == 5:
        return buf[pos:pos + 4], pos + 4
    if wt == 2:
        n, pos = decode_uvarint(buf, pos)
        return buf[pos:pos + n], pos + n
    raise ValueError(f"type d'onde non pris en charge : {wt}")


def _pb_value(t: str, raw: bytes) -> Any:
    if t in ("int32", "enum"):
        return _signed(decode_uvarint(raw)[0], 32)    # troncature à 32 bits
    if t == "int64":
        return _signed(decode_uvarint(raw)[0], 64)
    if t == "uint32":
        return decode_uvarint(raw)[0] & 0xFFFFFFFF
    if t == "uint64":
        return decode_uvarint(raw)[0] & ((1 << 64) - 1)
    if t == "sint32":
        return _signed(zigzag_decode(decode_uvarint(raw)[0]), 32)
    if t == "sint64":
        return zigzag_decode(decode_uvarint(raw)[0])
    if t == "bool":
        return decode_uvarint(raw)[0] != 0
    fmts = {"double": "<d", "float": "<f", "fixed64": "<Q", "sfixed64": "<q",
            "fixed32": "<I", "sfixed32": "<i"}
    return struct.unpack(fmts[t], raw)[0]
