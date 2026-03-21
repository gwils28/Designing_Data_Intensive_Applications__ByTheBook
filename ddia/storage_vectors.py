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
