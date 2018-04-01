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
