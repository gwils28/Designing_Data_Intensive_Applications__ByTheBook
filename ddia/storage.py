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
