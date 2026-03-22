"""Embeddings vectoriels et index de plus proches voisins (section *Vector Embeddings*).

Trois index, du plus simple au plus élaboré, qui comptent tous leurs calculs de distance :

- `FlatIndex` : recherche exhaustive (exacte, coût linéaire) ;
- `IVFIndex` : partition de l'espace par k-moyennes, on ne visite que `nprobe` listes ;
- `HNSWIndex` : graphe de proximité hiérarchique à plusieurs couches.

Les vecteurs sont normalisés : la similarité cosinus est alors un simple produit scalaire,
et la distance utilisée est 1 - cosinus.
"""

from __future__ import annotations

import heapq
import math
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np


def normalize(X: np.ndarray) -> np.ndarray:
    X = np.asarray(X, dtype=np.float32)
    n = np.linalg.norm(X, axis=-1, keepdims=True)
    return X / np.maximum(n, 1e-12)


def cosine_similarity(a, b) -> float:
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    return float(a @ b / (np.linalg.norm(a) * np.linalg.norm(b)))


def make_embeddings(n: int, dim: int = 32, n_topics: int = 20, spread: float = 0.35,
                    seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """Nuage synthétique d'« embeddings » : `n_topics` thèmes (directions aléatoires) et des
    documents dispersés autour de leur thème. Renvoie (vecteurs normalisés, thème)."""
    rng = np.random.default_rng(seed)
    centers = normalize(rng.normal(size=(n_topics, dim)))
    topic = rng.integers(0, n_topics, size=n)
    X = centers[topic] + spread * rng.normal(size=(n, dim)) / math.sqrt(dim) * 2.0
    return normalize(X), topic


def make_queries(X: np.ndarray, n: int, noise: float = 0.15, seed: int = 1) -> np.ndarray:
    """Requêtes proches de documents existants (comme une question reformulée)."""
    rng = np.random.default_rng(seed)
    base = X[rng.integers(0, len(X), size=n)]
    return normalize(base + noise * rng.normal(size=base.shape) / math.sqrt(X.shape[1]) * 2.0)


def recall_at_k(found: Sequence[int], truth: Sequence[int]) -> float:
    return len(set(found) & set(truth)) / max(1, len(truth))


class FlatIndex:
    """Index « plat » : on compare la requête à tous les vecteurs."""

    def __init__(self, X: np.ndarray):
        self.X = normalize(X)
        self.distance_computations = 0

    def search(self, q: np.ndarray, k: int = 10) -> List[int]:
        sims = self.X @ q
        self.distance_computations += len(self.X)
        idx = np.argpartition(-sims, min(k, len(sims) - 1))[:k]
        return [int(i) for i in idx[np.argsort(-sims[idx])]]


def kmeans(X: np.ndarray, k: int, n_iter: int = 15, seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """k-moyennes sphériques (centres renormalisés), initialisation k-means++ simplifiée."""
    rng = np.random.default_rng(seed)
    centers = [X[rng.integers(len(X))]]
    d2 = 1 - X @ centers[0]
    for _ in range(1, k):
        p = np.maximum(d2, 0) ** 2
        c = X[rng.choice(len(X), p=p / p.sum())]
        centers.append(c)
        d2 = np.minimum(d2, 1 - X @ c)
    C = np.array(centers)
    for _ in range(n_iter):
        assign = np.argmax(X @ C.T, axis=1)
        for j in range(k):
            m = X[assign == j]
            if len(m):
                C[j] = m.mean(axis=0)
        C = normalize(C)
    return C, np.argmax(X @ C.T, axis=1)


class IVFIndex:
    """Index à fichiers inversés : chaque vecteur est rangé dans la liste de son centroïde
    le plus proche ; une requête ne parcourt que les `nprobe` listes les plus proches."""

    def __init__(self, X: np.ndarray, nlist: int = 32, seed: int = 0):
        self.X = normalize(X)
        self.centroids, assign = kmeans(self.X, nlist, seed=seed)
        self.lists: List[np.ndarray] = [np.flatnonzero(assign == j) for j in range(nlist)]
        self.distance_computations = 0

    def search(self, q: np.ndarray, k: int = 10, nprobe: int = 1) -> List[int]:
        cs = self.centroids @ q
        self.distance_computations += len(cs)
        probe = np.argsort(-cs)[:nprobe]
        cand = np.concatenate([self.lists[j] for j in probe])
        if cand.size == 0:
            return []
        sims = self.X[cand] @ q
        self.distance_computations += cand.size
        order = np.argsort(-sims)[:k]
        return [int(i) for i in cand[order]]


class HNSWIndex:
    """Graphe HNSW simplifié (Malkov et Yashunin) : chaque vecteur reçoit un niveau
    aléatoire (loi géométrique), les couches hautes sont clairsemées et servent
    d'« autoroutes », la couche 0 contient tout le monde."""

    def __init__(self, X: np.ndarray, M: int = 12, ef_construction: int = 64, seed: int = 0,
                 metric: str = "cosine"):
        if metric not in ("cosine", "l2"):
            raise ValueError("metric doit valoir 'cosine' ou 'l2'")
        self.metric = metric
        self.X = normalize(X) if metric == "cosine" else np.asarray(X, dtype=np.float32)
        self.M = M
        self.M0 = 2 * M
        self.ef_construction = ef_construction
        self.mL = 1 / math.log(M)
        self.rng = np.random.default_rng(seed)
        self.layers: List[Dict[int, List[int]]] = []
        self.level_of: Dict[int, int] = {}
        self.entry: Optional[int] = None
        self.distance_computations = 0
        for i in range(len(self.X)):
            self._insert(i)
        self.build_distance_computations = self.distance_computations
        self.distance_computations = 0

    def _raw(self, q: np.ndarray, ids) -> np.ndarray:
        """Distances (1 - cosinus, ou carré de la distance euclidienne), sans comptage."""
        if self.metric == "cosine":
            return 1.0 - self.X[ids] @ q
        diff = self.X[ids] - q
        return np.einsum("ij,ij->i", diff, diff)

    def _dist(self, q: np.ndarray, ids) -> np.ndarray:
        ids = np.asarray(ids, dtype=np.int64)
        self.distance_computations += ids.size
        return self._raw(q, ids)

    def _search_layer(self, q, entries: List[int], ef: int, layer: int, trace: Optional[list] = None):
        visited = set(entries)
        d0 = self._dist(q, entries)
        cand = [(float(d), e) for d, e in zip(d0, entries)]
        heapq.heapify(cand)
        best = [(-float(d), e) for d, e in zip(d0, entries)]
        heapq.heapify(best)
        while len(best) > ef:
            heapq.heappop(best)
        graph = self.layers[layer]
        while cand:
            d, c = heapq.heappop(cand)
            if d > -best[0][0] and len(best) >= ef:
                break
            if trace is not None:
                trace.append((layer, c))
            nbrs = [n for n in graph.get(c, []) if n not in visited]
            if not nbrs:
                continue
            visited.update(nbrs)
            for dn, n in zip(self._dist(q, nbrs), nbrs):
                dn = float(dn)
                if len(best) < ef or dn < -best[0][0]:
                    heapq.heappush(cand, (dn, n))
                    heapq.heappush(best, (-dn, n))
                    if len(best) > ef:
                        heapq.heappop(best)
        return sorted((-d, e) for d, e in best)

    def _insert(self, i: int) -> None:
        q = self.X[i]
        level = int(-math.log(1 - self.rng.random()) * self.mL)
        self.level_of[i] = level
        while len(self.layers) <= level:
            self.layers.append({})
        if self.entry is None:
            for l in range(level + 1):
                self.layers[l][i] = []
            self.entry = i
            return
        ep = [self.entry]
        top = self.level_of[self.entry]
        for l in range(top, level, -1):
            ep = [self._search_layer(q, ep, 1, l)[0][1]]
        for l in range(min(level, top), -1, -1):
            res = self._search_layer(q, ep, self.ef_construction, l)
            mmax = self.M0 if l == 0 else self.M
            nbrs = self._select(i, [e for _, e in res], self.M)
            self.layers[l][i] = list(nbrs)
            for n in nbrs:
                lst = self.layers[l].setdefault(n, [])
                lst.append(i)
                if len(lst) > mmax:  # élagage, avec la même heuristique de diversité
                    self.layers[l][n] = self._select(n, lst, mmax)
            ep = [e for _, e in res]
        for l in range(top + 1, level + 1):
            self.layers[l][i] = []
        if level > top:
            self.entry = i

    def _select(self, base: int, cands: List[int], m: int) -> List[int]:
        """Heuristique de sélection des voisins de HNSW : on parcourt les candidats du plus
        proche au plus lointain et on garde un candidat seulement s'il est plus proche de
        `base` que de tous les voisins déjà gardés. Cela conserve des liens « longs » vers
        d'autres régions (sinon les groupes de points se referment sur eux-mêmes). Les
        places restantes sont complétées par les plus proches écartés."""
        cands = [c for c in dict.fromkeys(cands) if c != base]
        d = self._raw(self.X[base], cands)
        order = [cands[j] for j in np.argsort(d)]
        dist = {c: float(dd) for c, dd in zip(cands, d)}
        kept: List[int] = []
        dropped: List[int] = []
        for c in order:
            if len(kept) >= m:
                break
            if kept:
                dk = self._raw(self.X[c], kept)
                if float(dk.min()) < dist[c]:
                    dropped.append(c)
                    continue
            kept.append(c)
        for c in dropped:
            if len(kept) >= m:
                break
            kept.append(c)
        return kept

    def search(self, q: np.ndarray, k: int = 10, ef: int = 32, trace: Optional[list] = None) -> List[int]:
        q = np.asarray(q, dtype=np.float32)
        ep = [self.entry]
        for l in range(self.level_of[self.entry], 0, -1):
            ep = [self._search_layer(q, ep, 1, l, trace)[0][1]]
        res = self._search_layer(q, ep, max(ef, k), 0, trace)
        return [e for _, e in res[:k]]

    def layer_sizes(self) -> List[int]:
        return [len(g) for g in self.layers]
