"""Moteurs de stockage et index du chapitre « Storage and Retrieval ».

Tout est écrit pour être lu : chaque structure compte ce qu'elle fait (octets écrits,
pages lues, SSTables consultées...) afin que les notebooks puissent *mesurer* les
compromis dont parle le livre au lieu de les affirmer.

Contenu
-------
- journal append-only à la « bash » (`BashDB`) ;
- format d'enregistrement binaire avec somme de contrôle (`encode_record`, `iter_records`) ;
- index de hachage en mémoire sur segments, compaction, tombstones (`HashIndexStore`) ;
- filtre de Bloom (`BloomFilter`, `bloom_fp_rate`, `bloom_optimal_k`) ;
- SSTable avec index épars et blocs (`SSTable`), fusion k-voies (`merge_runs`) ;
- arbre LSM complet : memtable, WAL, compaction par paliers ou par niveaux (`LSMTree`) ;
- arbre B+ avec éclatement de pages, WAL et simulation de panne (`BPlusTree`) ;
- modèle de disque (`DEVICES`, `io_time`) ;
- index inversé, distance d'édition, trie et recherche floue, n-grammes ;
- courbe de Morton (ordre Z) pour les index multidimensionnels ;
- stockage en colonnes : RLE, dictionnaire, bitmaps, table de faits synthétique, cube.
"""

from __future__ import annotations

import bisect
import copy
import hashlib
import heapq
import math
import os
import re
import struct
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import combinations
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import numpy as np

# =====================================================================================
# 1. Le journal le plus simple du monde
# =====================================================================================


class BashDB:
    """Traduction Python des deux fonctions bash `db_set` / `db_get`.

    `set` ajoute une ligne `clé,valeur` en fin de fichier ; `get` relit tout le fichier
    et garde la dernière occurrence. `bytes_scanned` compte les octets lus par `get`.
    """

    def __init__(self, path: str):
        self.path = path
        open(path, "a").close()
        self.bytes_scanned = 0

    def set(self, key: str, value: str) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(f"{key},{value}\n")

    def get(self, key: str) -> Optional[str]:
        prefix = f"{key},"
        found = None
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                self.bytes_scanned += len(line)
                if line.startswith(prefix):
                    found = line[len(prefix):-1]
        return found

    def size(self) -> int:
        return os.path.getsize(self.path)


# =====================================================================================
# 2. Format d'enregistrement binaire (somme de contrôle + longueurs)
# =====================================================================================

_HEADER = struct.Struct(">III")  # crc32, longueur de la clé, longueur de la valeur
TOMBSTONE_LEN = 0xFFFFFFFF       # longueur spéciale : enregistrement de suppression


def encode_record(key: str, value: Optional[str]) -> bytes:
    """Encode `crc | len(clé) | len(valeur) | clé | valeur` ; value=None : tombstone."""
    k = key.encode("utf-8")
    v = b"" if value is None else value.encode("utf-8")
    vlen = TOMBSTONE_LEN if value is None else len(v)
    body = struct.pack(">II", len(k), vlen) + k + v
    return struct.pack(">I", zlib.crc32(body)) + body


def iter_records(data: bytes) -> Iterator[Tuple[int, str, Optional[str], int]]:
    """Parcourt un journal : (offset, clé, valeur ou None, taille). S'arrête au premier
    enregistrement tronqué ou corrompu (somme de contrôle fausse)."""
    pos, n = 0, len(data)
    while pos + _HEADER.size <= n:
        crc, klen, vlen = _HEADER.unpack_from(data, pos)
        vl = 0 if vlen == TOMBSTONE_LEN else vlen
        end = pos + _HEADER.size + klen + vl
        if end > n:
            return  # enregistrement partiellement écrit
        body = data[pos + 4:end]
        if zlib.crc32(body) != crc:
            return  # corrompu
        k = data[pos + _HEADER.size:pos + _HEADER.size + klen].decode("utf-8")
        v = None if vlen == TOMBSTONE_LEN else data[pos + _HEADER.size + klen:end].decode("utf-8")
        yield pos, k, v, end - pos
        pos = end


def record_size(key: str, value: Optional[str]) -> int:
    """Taille encodée d'un enregistrement (sans l'encoder)."""
    return _HEADER.size + len(key.encode("utf-8")) + (0 if value is None else len(value.encode("utf-8")))


# =====================================================================================
# 3. Index de hachage sur segments append-only (à la Bitcask)
# =====================================================================================


class HashIndexStore:
    """Stockage clé-valeur : segments append-only sur disque + table de hachage en mémoire.

    - `put` / `delete` ajoutent un enregistrement au segment actif ; quand il dépasse
      `segment_bytes`, il est fermé (immuable) et un nouveau segment est ouvert ;
    - l'index en mémoire associe chaque clé à (segment, offset, taille) ;
    - `compact()` réécrit les segments fermés en ne gardant que la dernière valeur de
      chaque clé vivante (les valeurs écrasées et les tombstones disparaissent) ;
    - `HashIndexStore.open(dir)` reconstruit l'index en relisant tous les segments.
    """

    def __init__(self, directory: str, segment_bytes: int = 64 * 1024, _recover: bool = False):
        self.dir = directory
        self.segment_bytes = segment_bytes
        os.makedirs(directory, exist_ok=True)
        self.index: Dict[str, Tuple[int, int, int]] = {}
        self.segments: List[int] = []
        self.user_bytes = 0        # octets demandés par l'application
        self.compaction_bytes = 0  # octets réécrits par la compaction
        self.disk_reads = 0
        if _recover:
            self._rebuild()
        else:
            for f in os.listdir(directory):
                if f.endswith(".seg"):
                    os.remove(os.path.join(directory, f))
        if not _recover:
            self._next_id = 0
        self._open_new_segment()

    # -- fichiers
    def _path(self, seg: int) -> str:
        return os.path.join(self.dir, f"{seg:06d}.seg")

    def _open_new_segment(self) -> None:
        # identifiants espacés de 1000 : les segments compactés prennent les numéros
        # intermédiaires, ce qui conserve l'ordre chronologique des fichiers
        self.active = self._next_id
        self._next_id += 1000
        self.segments.append(self.active)
        self._fh = open(self._path(self.active), "ab")
        self._active_size = 0

    def _append(self, rec: bytes) -> Tuple[int, int]:
        if self._active_size + len(rec) > self.segment_bytes and self._active_size > 0:
            self._fh.close()
            self._open_new_segment()
        off = self._active_size
        self._fh.write(rec)
        self._fh.flush()
        self._active_size += len(rec)
        return self.active, off

    # -- API
    def put(self, key: str, value: str) -> None:
        rec = encode_record(key, value)
        seg, off = self._append(rec)
        self.user_bytes += len(rec)
        self.index[key] = (seg, off, len(rec))

    def delete(self, key: str) -> None:
        rec = encode_record(key, None)
        self._append(rec)
        self.user_bytes += len(rec)
        self.index.pop(key, None)

    def get(self, key: str) -> Optional[str]:
        loc = self.index.get(key)
        if loc is None:
            return None
        seg, off, size = loc
        with open(self._path(seg), "rb") as f:
            f.seek(off)
            data = f.read(size)
        self.disk_reads += 1
        for _, k, v, _ in iter_records(data):
            return v
        return None

    def keys(self):
        return self.index.keys()

    # -- maintenance
    def disk_bytes(self) -> int:
        return sum(os.path.getsize(self._path(s)) for s in self.segments)

    def live_bytes(self) -> int:
        return sum(size for _, _, size in self.index.values())

    def compact(self) -> dict:
        """Fusionne et compacte tous les segments fermés (le segment actif est laissé)."""
        closed = [s for s in self.segments if s != self.active]
        if not closed:
            return {"segments_avant": 0, "segments_apres": 0, "octets_avant": 0, "octets_apres": 0}
        before = sum(os.path.getsize(self._path(s)) for s in closed)
        closed_set = set(closed)
        live = sorted((loc for loc in self.index.items() if loc[1][0] in closed_set),
                      key=lambda kv: (kv[1][0], kv[1][1]))
        new_segs: List[int] = []
        fh, size, seg = None, 0, None
        for key, (s, off, sz) in live:
            with open(self._path(s), "rb") as f:
                f.seek(off)
                rec = f.read(sz)
            if fh is None or size + sz > self.segment_bytes:
                if fh:
                    fh.close()
                seg = closed[0] + len(new_segs)
                new_segs.append(seg)
                fh = open(self._path(seg) + ".tmp", "wb")
                size = 0
            fh.write(rec)
            self.index[key] = (seg, size, sz)
            size += sz
            self.compaction_bytes += sz
        if fh:
            fh.close()
        for s in closed:
            os.remove(self._path(s))
        for s in new_segs:  # remplacement atomique des anciens segments par les nouveaux
            os.replace(self._path(s) + ".tmp", self._path(s))
        # les nouveaux segments sont plus anciens que le segment actif : on les place avant
        self.segments = new_segs + [self.active]
        after = sum(os.path.getsize(self._path(s)) for s in new_segs)
        return {"segments_avant": len(closed), "segments_apres": len(new_segs),
                "octets_avant": before, "octets_apres": after}

    def close(self) -> None:
        self._fh.close()

    # -- reprise après panne
    def _rebuild(self) -> None:
        segs = sorted(int(f[:-4]) for f in os.listdir(self.dir) if f.endswith(".seg"))
        self.segments = segs
        self.records_scanned = 0
        self.bytes_scanned = 0
        for s in segs:
            with open(self._path(s), "rb") as f:
                data = f.read()
            self.bytes_scanned += len(data)
            good = 0
            for off, k, v, size in iter_records(data):
                self.records_scanned += 1
                good = off + size
                if v is None:
                    self.index.pop(k, None)
                else:
                    self.index[k] = (s, off, size)
            if good < len(data):  # fin de segment tronquée ou corrompue : on la coupe
                with open(self._path(s), "r+b") as f:
                    f.truncate(good)
        self._next_id = (max(segs) // 1000 + 1) * 1000 if segs else 0

    @classmethod
    def open(cls, directory: str, segment_bytes: int = 64 * 1024) -> "HashIndexStore":
        """Rouvre un répertoire existant en reconstruisant l'index (relecture complète)."""
        return cls(directory, segment_bytes, _recover=True)


# =====================================================================================
# 4. Filtre de Bloom
# =====================================================================================


def _hash2(key: str) -> Tuple[int, int]:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=16).digest()
    return int.from_bytes(h[:8], "little"), int.from_bytes(h[8:], "little") | 1


class BloomFilter:
    """Filtre de Bloom de m bits et k fonctions de hachage (double hachage de Kirsch et
    Mitzenmacher : h_i(x) = h1(x) + i * h2(x) mod m)."""

    def __init__(self, m_bits: int, k: int):
        self.m = max(8, int(m_bits))
        self.k = max(1, int(k))
        self.bits = np.zeros(self.m, dtype=bool)
        self.n = 0

    @classmethod
    def for_capacity(cls, n: int, bits_per_key: float) -> "BloomFilter":
        m = max(8, int(round(n * bits_per_key)))
        return cls(m, bloom_optimal_k(m, max(1, n)))

    def positions(self, key: str) -> List[int]:
        h1, h2 = _hash2(key)
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def add(self, key: str) -> None:
        self.bits[self.positions(key)] = True
        self.n += 1

    def __contains__(self, key: str) -> bool:
        return bool(self.bits[self.positions(key)].all())

    def fill_ratio(self) -> float:
        return float(self.bits.mean())

    def size_bytes(self) -> int:
        return (self.m + 7) // 8


def bloom_fp_rate(n: int, m: int, k: int) -> float:
    """Taux de faux positifs théorique : (1 - e^{-kn/m})^k."""
    return (1.0 - math.exp(-k * n / m)) ** k


def bloom_optimal_k(m: int, n: int) -> int:
    """Nombre de fonctions de hachage qui minimise les faux positifs : (m/n) ln 2."""
    return max(1, int(round(m / n * math.log(2))))


def bloom_bits_per_key(fp: float) -> float:
    """Bits par clé nécessaires (avec k optimal) pour un taux de faux positifs donné :
    m/n = -ln(p) / (ln 2)^2 ≈ 1,44 log2(1/p)."""
    return -math.log(fp) / math.log(2) ** 2


# =====================================================================================
# 5. SSTable et fusion
# =====================================================================================

Item = Tuple[str, Optional[str]]  # (clé, valeur) ; valeur None = tombstone


class SSTable:
    """Table triée immuable : blocs de `block_bytes`, index épars (première clé de chaque
    bloc) et filtre de Bloom. Les données restent en mémoire Python, mais tout accès est
    compté comme une lecture de bloc."""

    _seq = 0

    def __init__(self, items: Sequence[Item], block_bytes: int = 4096, bloom_bits_per_key: float = 10.0,
                 compress: bool = False):
        SSTable._seq += 1
        self.id = SSTable._seq
        self.blocks: List[List[Item]] = []
        self.sparse_index: List[str] = []
        cur, cur_bytes = [], 0
        self.size_bytes = 0
        raw = bytearray()
        for k, v in items:
            sz = record_size(k, v)
            if cur and cur_bytes + sz > block_bytes:
                self._close_block(cur, raw, compress)
                cur, cur_bytes, raw = [], 0, bytearray()
            cur.append((k, v))
            cur_bytes += sz
            if compress:
                raw += encode_record(k, v)
            else:
                self.size_bytes += sz
        if cur:
            self._close_block(cur, raw, compress)
        self.n = sum(len(b) for b in self.blocks)
        self.bloom = None
        if bloom_bits_per_key and self.n:
            self.bloom = BloomFilter.for_capacity(self.n, bloom_bits_per_key)
            for b in self.blocks:
                for k, _ in b:
                    self.bloom.add(k)
        self.min_key = self.blocks[0][0][0] if self.blocks else None
        self.max_key = self.blocks[-1][-1][0] if self.blocks else None

    def _close_block(self, cur, raw, compress) -> None:
        self.sparse_index.append(cur[0][0])
        self.blocks.append(cur)
        if compress:
            self.size_bytes += len(zlib.compress(bytes(raw), 6))

    def __len__(self) -> int:
        return self.n

    def __iter__(self) -> Iterator[Item]:
        for b in self.blocks:
            yield from b

    def overlaps(self, lo: str, hi: str) -> bool:
        return not (self.max_key < lo or self.min_key > hi)

    def get(self, key: str, stats: Optional[dict] = None) -> Tuple[bool, Optional[str]]:
        """(trouvé, valeur). Consulte le filtre de Bloom, puis l'index épars, puis un bloc."""
        st = stats if stats is not None else {}
        if self.n == 0 or key < self.min_key or key > self.max_key:
            return False, None
        if self.bloom is not None:
            st["bloom_checks"] = st.get("bloom_checks", 0) + 1
            if key not in self.bloom:
                st["bloom_negatives"] = st.get("bloom_negatives", 0) + 1
                return False, None
        i = bisect.bisect_right(self.sparse_index, key) - 1
        if i < 0:
            return False, None
        st["blocks_read"] = st.get("blocks_read", 0) + 1
        block = self.blocks[i]
        keys = [k for k, _ in block]
        j = bisect.bisect_left(keys, key)
        if j < len(keys) and keys[j] == key:
            return True, block[j][1]
        if self.bloom is not None:
            st["bloom_false_positives"] = st.get("bloom_false_positives", 0) + 1
        return False, None

    def scan(self, lo: str, hi: str) -> Iterator[Item]:
        i = max(0, bisect.bisect_right(self.sparse_index, lo) - 1)
        for b in self.blocks[i:]:
            for k, v in b:
                if k > hi:
                    return
                if k >= lo:
                    yield k, v
