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


def read_committed(store: ObjectStore, bucket: str, table: str) -> List[bytes]:
    """Lit la dernière version publiée : pointeur -> manifeste -> fichiers listés."""
    try:
        manifest = store.get(bucket, f"{table}/_latest").decode()
    except KeyError:
        return []
    keys = store.get(bucket, manifest).decode().split("\n")
    return [store.get(bucket, k) for k in keys if k]


# =====================================================================================
# Allocation de ressources
# =====================================================================================

def simulate_cluster(jobs: Sequence[Dict[str, Any]], cores: int, policy: str = "fifo",
                     dt: float = 1.0, t_max: float = 10_000) -> Dict[str, Any]:
    """Cluster de `cores` cœurs identiques ; chaque job = {name, arrival, tasks, duration}
    (des tâches d'un cœur, toutes de même durée).

    Politiques :
    - "fifo" : le job arrivé le premier prend tout ce qu'il peut, les suivants le reste ;
    - "fair" : à chaque libération, le cœur va au job actif qui en occupe le moins ;
    - "gang" : FIFO, mais un job marqué `gang=True` ne démarre que si **toutes** ses
      tâches peuvent tourner ensemble ; en attendant, les cœurs qui se libèrent lui sont
      réservés (les jobs suivants ne le doublent pas).

    Renvoie l'occupation au cours du temps (par job) et les dates de fin.
    """
    state = {j["name"]: dict(j, left=j["tasks"], running=0, end=None) for j in jobs}
    events: List[Tuple[float, str]] = []     # (fin de tâche, job)
    t = 0.0
    timeline = []
    free = cores
    while t <= t_max:
        while events and events[0][0] <= t + 1e-9:
            _, name = heapq.heappop(events)
            state[name]["running"] -= 1
            free += 1
            if state[name]["left"] == 0 and state[name]["running"] == 0:
                state[name]["end"] = t
        active = [s for s in state.values() if s["arrival"] <= t + 1e-9 and s["left"] > 0]
        active.sort(key=lambda s: (s["arrival"], s["name"]))
        if policy == "fifo":
            for s in active:
                n = min(free, s["left"])
                for _ in range(n):
                    heapq.heappush(events, (t + s["duration"], s["name"]))
                s["left"] -= n
                s["running"] += n
                free -= n
        elif policy == "fair":
            while free > 0:
                cand = [s for s in active if s["left"] > 0]
                if not cand:
                    break
                s = min(cand, key=lambda s: (s["running"], s["arrival"]))
                heapq.heappush(events, (t + s["duration"], s["name"]))
                s["left"] -= 1
                s["running"] += 1
                free -= 1
        elif policy == "gang":
            for s in active:
                if not s.get("gang", False):
                    n = min(free, s["left"])
                    for _ in range(n):
                        heapq.heappush(events, (t + s["duration"], s["name"]))
                    s["left"] -= n
                    s["running"] += n
                    free -= n
                    continue
                if s["running"] > 0:
                    continue
                if s["tasks"] > cores:
                    raise ValueError(f"{s['name']} demande plus de cœurs que le cluster")
                if s["left"] <= free:
                    for _ in range(s["left"]):
                        heapq.heappush(events, (t + s["duration"], s["name"]))
                    s["running"] += s["left"]
                    free -= s["left"]
                    s["left"] = 0
                else:
                    break                      # réservation : les cœurs libres attendent ce job
        else:
            raise ValueError(policy)
        timeline.append({"t": t, **{name: s["running"] for name, s in state.items()}})
        if all(s["end"] is not None for s in state.values()):
            break
        t += dt
    return {"timeline": timeline, "end": {n: s["end"] for n, s in state.items()}}
