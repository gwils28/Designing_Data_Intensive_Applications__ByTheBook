"""Sharding pour la multi-location (multitenancy) : notions de la 2e édition.

Fonctions du notebook « Pourquoi sharder ? Avantages, inconvénients et multi-location » :

- tailles de locataires (tenants) très inégales (loi log-normale) ;
- regroupement de petits locataires dans des shards (first-fit decreasing) et détection
  des locataires trop gros pour un seul nœud ;
- impact d'un « voisin bruyant » selon le nombre de locataires par shard ;
- croissance des locataires dans le temps et déplacements de locataires entre shards ;
- probabilité qu'une transaction touchant k enregistrements reste sur un seul shard.
"""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

import numpy as np


def tenant_sizes(n: int, median: float = 2.0, sigma: float = 1.6, seed: int = 0) -> np.ndarray:
    """Tailles (en Go) de `n` locataires : beaucoup de petits, quelques géants."""
    rng = np.random.default_rng(seed)
    return rng.lognormal(np.log(median), sigma, n)


def pack_tenants(sizes: Sequence[float], capacity: float) -> Tuple[List[List[int]], List[int]]:
    """Range les locataires dans des shards de capacité `capacity` (first-fit decreasing).

    Renvoie (shards, trop_gros) : chaque shard est la liste des indices de ses locataires ;
    `trop_gros` liste les locataires plus grands qu'un shard (à sharder eux-mêmes).
    """
    sizes = np.asarray(sizes, dtype=float)
    order = np.argsort(-sizes)
    bins: List[List[int]] = []
    free: List[float] = []
    too_big: List[int] = []
    for i in order:
        s = sizes[i]
        if s > capacity:
            too_big.append(int(i))
            continue
        for b, f in enumerate(free):
            if s <= f:
                bins[b].append(int(i))
                free[b] -= s
                break
        else:
            bins.append([int(i)])
            free.append(capacity - s)
    return bins, too_big


def noisy_neighbor_slowdown(loads: Sequence[float], shard_of: Sequence[int], heavy: int,
                            extra: float, capacity: float = 1.0) -> np.ndarray:
    """Ralentissement de chaque locataire quand le locataire `heavy` ajoute `extra` de charge.

    Chaque shard est une file M/M/1 de capacité `capacity` : temps de réponse ∝ 1/(1 - rho).
    Renvoie, pour chaque locataire, le rapport temps de réponse après / avant (inf si le
    shard sature).
    """
    loads = np.asarray(loads, dtype=float)
    shard_of = np.asarray(shard_of)
    n_shards = int(shard_of.max()) + 1
    base = np.bincount(shard_of, weights=loads, minlength=n_shards) / capacity
    after = base.copy()
    after[shard_of[heavy]] += extra / capacity

    def rt(rho):
        return np.where(rho < 1, 1.0 / np.maximum(1e-9, 1 - rho), np.inf)

    return rt(after)[shard_of] / rt(base)[shard_of]


def simulate_tenant_growth(sizes: Sequence[float], growth: Sequence[float], capacity: float,
                           periods: int, policy: str = "smallest") -> Dict[str, np.ndarray]:
    """Fait croître les locataires (taux `growth` par période) et rééquilibre les shards.

    Au départ, rangement first-fit decreasing. À chaque période, tout shard qui déborde
    déplace des locataires (les plus petits si policy="smallest", les plus gros si
    "largest") vers le shard le plus libre qui peut les accueillir, sinon vers un nouveau
    shard. Un locataire plus gros qu'un shard reste seul (il faudrait le sharder).
    Renvoie par période : nombre de shards, locataires déplacés, volume déplacé (Go),
    locataires trop gros.
    """
    sizes = np.asarray(sizes, dtype=float).copy()
    growth = np.asarray(growth, dtype=float)
    bins, too_big = pack_tenants(sizes, capacity)
    shard = np.full(sizes.size, -1)
    for b, members in enumerate(bins):
        shard[members] = b
    for t in too_big:
        shard[t] = len(bins)
        bins.append([t])
    out = {"shards": [], "deplaces": [], "volume": [], "trop_gros": []}
    for _ in range(periods):
        sizes *= 1 + growth
        moved, volume = 0, 0.0
        used = np.bincount(shard, weights=sizes, minlength=len(bins))
        for b in range(len(bins)):
            members = [i for i in range(sizes.size) if shard[i] == b]
            while used[b] > capacity and len(members) > 1:
                members.sort(key=lambda i: sizes[i], reverse=(policy == "largest"))
                i = members.pop(0)
                used[b] -= sizes[i]
                free = capacity - used
                free[b] = -1
                cand = np.flatnonzero(free >= sizes[i])
                if cand.size:
                    dest = int(cand[np.argmax(free[cand])])
                else:
                    dest = len(bins)
                    bins.append([])
                    used = np.append(used, 0.0)
                shard[i] = dest
                used[dest] += sizes[i]
                moved += 1
                volume += sizes[i]
        out["shards"].append(int(len(set(shard.tolist()))))
        out["deplaces"].append(moved)
        out["volume"].append(volume)
        out["trop_gros"].append(int((sizes > capacity).sum()))
    return {k: np.array(v) for k, v in out.items()}


def single_shard_probability(n_shards: int, k_records: int) -> float:
    """Probabilité qu'une transaction sur k enregistrements répartis au hasard tienne sur un seul shard."""
    return float((1.0 / n_shards) ** (k_records - 1))
