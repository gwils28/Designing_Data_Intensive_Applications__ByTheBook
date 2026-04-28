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


def drf(capacity: Sequence[float], demands: Dict[str, Sequence[float]],
        max_tasks: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """Dominant resource fairness par remplissage progressif (Ghodsi et al., 2011).

    `capacity` : quantité de chaque ressource (ex. [cœurs, Go de mémoire]) ;
    `demands[u]` : besoin d'une tâche de l'utilisateur u. À chaque pas, on donne une
    tâche à l'utilisateur dont la **part dominante** (max sur les ressources de sa part
    consommée) est la plus faible, tant que ses besoins tiennent dans le reste.
    """
    cap = np.asarray(capacity, dtype=float)
    used = np.zeros_like(cap)
    tasks = {u: 0 for u in demands}
    dem = {u: np.asarray(d, dtype=float) for u, d in demands.items()}
    blocked = set()
    while len(blocked) < len(demands):
        shares = {u: float(np.max(tasks[u] * dem[u] / cap)) for u in demands if u not in blocked}
        u = min(shares, key=lambda x: (shares[x], x))
        if np.all(used + dem[u] <= cap + 1e-9) and (max_tasks is None or tasks[u] < max_tasks.get(u, 10 ** 9)):
            used += dem[u]
            tasks[u] += 1
        else:
            blocked.add(u)
    dominant = {u: float(np.max(tasks[u] * dem[u] / cap)) for u in demands}
    return {"tasks": tasks, "dominant_share": dominant, "used": used, "utilisation": used / cap}


# =====================================================================================
# DataFrame paresseux
# =====================================================================================

def make_table(n_rows: int, row_group: int = 10_000, seed: int = 0) -> List[Dict[str, np.ndarray]]:
    """Table de ventes en groupes de lignes (colonnes numpy), triée par date comme le serait
    un fichier écrit au fil de l'eau : colonnes day, store, product, qty, price."""
    rng = np.random.default_rng(seed)
    day = np.sort(rng.integers(0, 365, n_rows))
    cols = {"day": day, "store": rng.integers(0, 50, n_rows), "product": rng.integers(0, 2000, n_rows),
            "qty": rng.integers(1, 6, n_rows), "price": rng.gamma(2.0, 15.0, n_rows).round(2)}
    return [{c: v[i:i + row_group] for c, v in cols.items()} for i in range(0, n_rows, row_group)]


@dataclass
class _Scan:
    columns: Optional[List[str]] = None
    predicates: List[Tuple[str, str, float]] = field(default_factory=list)


_OPS = {"<": np.less, "<=": np.less_equal, ">": np.greater, ">=": np.greater_equal, "==": np.equal}


class LazyFrame:
    """DataFrame paresseux : `filter`, `select`, `with_column`, `group_sum` empilent des
    opérations ; `explain(optimize)` montre le plan, `collect(optimize)` l'exécute.

    Optimisations (si `optimize=True`) : poussée des filtres simples (colonne, op,
    constante) dans la lecture, qui saute les groupes de lignes dont les statistiques
    min/max excluent toute ligne ; élagage des colonnes inutiles.
    Les compteurs `rows_read`, `values_read`, `groups_skipped` mesurent le travail.
    """

    def __init__(self, groups: List[Dict[str, np.ndarray]], ops: Optional[List[Tuple]] = None):
        self.groups = groups
        self.ops = ops or []
        self.stats: Dict[str, int] = {}

    def _with(self, op: Tuple) -> "LazyFrame":
        return LazyFrame(self.groups, self.ops + [op])

    def filter(self, col: str, op: str, value: float) -> "LazyFrame":
        return self._with(("filter", col, op, value))

    def with_column(self, name: str, fn: Callable[[Dict[str, np.ndarray]], np.ndarray], uses: Sequence[str]) -> "LazyFrame":
        return self._with(("with_column", name, fn, list(uses)))

    def select(self, *cols: str) -> "LazyFrame":
        return self._with(("select", list(cols)))

    def group_sum(self, key: str, value: str) -> "LazyFrame":
        return self._with(("group_sum", key, value))

    def _plan(self, optimize: bool) -> Tuple[_Scan, List[Tuple]]:
        scan, rest = _Scan(), []
        if not optimize:
            return scan, list(self.ops)
        defined = set()
        for op in self.ops:
            if op[0] == "filter" and op[1] not in defined:
                scan.predicates.append(op[1:])
            else:
                if op[0] == "with_column":
                    defined.add(op[1])
                rest.append(op)
        needed: set = set(p[0] for p in scan.predicates)
        for op in rest:
            if op[0] == "filter":
                needed.add(op[1])
            elif op[0] == "with_column":
                needed.update(op[3])
            elif op[0] == "select":
                needed.update(c for c in op[1] if c not in defined)
            elif op[0] == "group_sum":
                needed.update(c for c in (op[1], op[2]) if c not in defined)
        scan.columns = sorted(needed - defined)
        return scan, rest

    def explain(self, optimize: bool = True) -> str:
        scan, rest = self._plan(optimize)
        cols = "toutes" if scan.columns is None else ", ".join(scan.columns)
        preds = " ET ".join(f"{c} {o} {v}" for c, o, v in scan.predicates) or "aucun"
        lines = [f"LECTURE colonnes=[{cols}] filtres poussés=[{preds}]"]
        for op in rest:
            if op[0] == "filter":
                lines.append(f"FILTRE {op[1]} {op[2]} {op[3]}")
            elif op[0] == "with_column":
                lines.append(f"CALCUL {op[1]} := f({', '.join(op[3])})")
            elif op[0] == "select":
                lines.append(f"PROJECTION {', '.join(op[1])}")
            elif op[0] == "group_sum":
                lines.append(f"AGRÉGAT somme({op[2]}) par {op[1]}")
        return "\n".join(lines[::-1])

    def collect(self, optimize: bool = True):
        scan, rest = self._plan(optimize)
        st = {"groups_skipped": 0, "rows_read": 0, "values_read": 0}
        parts = []
        for g in self.groups:
            n = len(next(iter(g.values())))
            skip = False
            for c, o, v in scan.predicates:
                lo, hi = g[c].min(), g[c].max()
                if (o in ("<", "<=") and not _OPS[o](lo, v)) or (o in (">", ">=") and not _OPS[o](hi, v)) \
                        or (o == "==" and not (lo <= v <= hi)):
                    skip = True
            if skip:
                st["groups_skipped"] += 1
                continue
            cols = scan.columns if scan.columns is not None else list(g)
            data = {c: g[c] for c in cols}
            st["rows_read"] += n
            st["values_read"] += n * len(cols)
            mask = np.ones(n, dtype=bool)
            for c, o, v in scan.predicates:
                mask &= _OPS[o](data[c], v)
            parts.append({c: a[mask] for c, a in data.items()})
        data = {c: np.concatenate([p[c] for p in parts]) if parts else np.array([]) for c in (parts[0] if parts else {})}
        result: Any = data
        for op in rest:
            if op[0] == "filter":
                m = _OPS[op[2]](result[op[1]], op[3])
                result = {c: a[m] for c, a in result.items()}
            elif op[0] == "with_column":
                result = dict(result)
                result[op[1]] = op[2](result)
            elif op[0] == "select":
                result = {c: result[c] for c in op[1]}
            elif op[0] == "group_sum":
                keys, inv = np.unique(result[op[1]], return_inverse=True)
                sums = np.bincount(inv, weights=result[op[2]])
                result = {op[1]: keys, op[2]: sums}
        self.stats = st
        return result


# =====================================================================================
# Quasi-doublons : MinHash et LSH
# =====================================================================================
