"""Traitement par lots, notions ajoutées par la 2e édition du livre.

- `ObjectStore` : magasin d'objets en mémoire à la S3 (put/get immuables, liste par
  préfixe récursive, pas de répertoires, renommage non atomique par copie + suppression),
  avec compteur de requêtes ; protocole de validation par manifeste ;
- ordonnancement de ressources : simulation d'un cluster de cœurs avec plusieurs
  politiques (FIFO, partage équitable, gang scheduling) et allocation équitable
  multi-ressources DRF (*dominant resource fairness*) ;
- `LazyFrame` : DataFrame paresseux minimal sur une table découpée en groupes de lignes
  avec statistiques min/max (à la Parquet) ; l'optimiseur pousse les filtres et les
  projections vers la lecture ;
- MinHash et LSH pour détecter les quasi-doublons d'un corpus (préparation de données
  d'entraînement).
"""

from __future__ import annotations

import heapq
import math
import re
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

__all__ = ["ObjectStore", "RenameInterrupted", "commit_with_manifest", "read_committed",
           "simulate_cluster", "drf", "LazyFrame", "make_table", "shingles", "minhash",
           "lsh_candidates", "jaccard"]


# =====================================================================================
# Magasin d'objets
# =====================================================================================

class RenameInterrupted(RuntimeError):
    """Le processus qui renommait un « répertoire » est mort au milieu."""


class ObjectStore:
    """Magasin d'objets à plat : un espace de noms de clés par bucket, valeurs immuables."""

    def __init__(self):
        self.buckets: Dict[str, Dict[str, bytes]] = defaultdict(dict)
        self.requests: Dict[str, int] = defaultdict(int)

    def put(self, bucket: str, key: str, data: bytes) -> None:
        """Écrit (ou remplace entièrement) un objet ; atomique pour un objet donné."""
        self.requests["PUT"] += 1
        self.buckets[bucket][key] = bytes(data)

    def get(self, bucket: str, key: str) -> bytes:
        self.requests["GET"] += 1
        return self.buckets[bucket][key]

    def delete(self, bucket: str, key: str) -> None:
        self.requests["DELETE"] += 1
        self.buckets[bucket].pop(key, None)

    def copy(self, bucket: str, src: str, dst: str) -> None:
        self.requests["COPY"] += 1
        self.buckets[bucket][dst] = self.buckets[bucket][src]

    def list(self, bucket: str, prefix: str = "", page_size: int = 1000) -> List[str]:
        """Toutes les clés qui commencent par `prefix`, triées, sous-chemins compris
        (comme `ls -R`). Une requête LIST par page de `page_size` clés."""
        keys = sorted(k for k in self.buckets[bucket] if k.startswith(prefix))
        self.requests["LIST"] += max(1, math.ceil(len(keys) / page_size))
        return keys

    def list_dirs(self, bucket: str, prefix: str = "") -> List[str]:
        """« Sous-répertoires » immédiats déduits des clés (délimiteur « / »)."""
        out = set()
        for k in self.list(bucket, prefix):
            rest = k[len(prefix):]
            if "/" in rest:
                out.add(prefix + rest.split("/", 1)[0] + "/")
        return sorted(out)

    def rename_prefix(self, bucket: str, src: str, dst: str, crash_after: Optional[int] = None) -> int:
        """« Renomme un répertoire » : copie puis suppression, **objet par objet**.

        `crash_after=n` : le client meurt après avoir déplacé n objets (lève
        `RenameInterrupted`) ; l'état intermédiaire reste visible par tous.
        Renvoie le nombre d'objets déplacés.
        """
        moved = 0
        for k in self.list(bucket, src):
            if crash_after is not None and moved >= crash_after:
                raise RenameInterrupted(f"interrompu après {moved} objets")
            self.copy(bucket, k, dst + k[len(src):])
            self.delete(bucket, k)
            moved += 1
        return moved

    def reset_requests(self) -> None:
        self.requests = defaultdict(int)


def commit_with_manifest(store: ObjectStore, bucket: str, table: str, attempt: str,
                         parts: Dict[str, bytes], crash_after: Optional[int] = None) -> str:
    """Écrit une nouvelle version de `table` sans renommage :

    1. chaque fichier de données va sous un préfixe propre à la tentative
       (`table/data/<attempt>/...`), invisible des lecteurs ;
    2. un **manifeste** (liste des fichiers) est écrit en un seul PUT ;
    3. le pointeur `table/_latest` est remplacé en un seul PUT (atomique).

    `crash_after=n` : la tentative meurt après n fichiers de données (rien n'est publié).
    Renvoie la clé du manifeste.
    """
    written = []
    for i, (name, data) in enumerate(sorted(parts.items())):
        if crash_after is not None and i >= crash_after:
            raise RenameInterrupted(f"tentative {attempt} morte après {i} fichiers")
        key = f"{table}/data/{attempt}/{name}"
        store.put(bucket, key, data)
        written.append(key)
    manifest = f"{table}/manifests/{attempt}.txt"
    store.put(bucket, manifest, "\n".join(written).encode())
    store.put(bucket, f"{table}/_latest", manifest.encode())
    return manifest
