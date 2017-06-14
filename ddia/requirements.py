"""Exigences non fonctionnelles : charge, performance, fiabilité, scalabilité.

Fonctions réutilisables des notebooks du chapitre « exigences non fonctionnelles »
(2e édition : chapitre 2 ; 1re édition : chapitre 1) :

- **étude de cas des timelines** : graphe social à popularité en loi de puissance,
  coûts du fan-out à la lecture, à l'écriture et hybride, trois implémentations jouets ;
- **performance** : temps de réponse synthétiques, percentiles, histogramme log-linéaire
  à la HdrHistogram (fusionnable), fenêtre glissante, amplification de la latence de queue ;
- **files d'attente** : simulation FIFO à c serveurs, formules M/M/1, M/M/c (Erlang C),
  M/G/1 (Pollaczek-Khinchine) ;
- **fiabilité** : AFR et MTTF, pannes attendues, perte de données d'un groupe de
  répliques (formule et simulation, avec pannes corrélées), disponibilité et « neuf » ;
- **scalabilité** : loi d'Amdahl, Universal Scalability Law et son ajustement.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import heapq
import math
from collections import deque
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .sim import Network, Node

HOURS_PER_YEAR = 8760.0

# =====================================================================================
# 1. Étude de cas : timelines d'un réseau social
# =====================================================================================


def social_graph(n_users: int, mean_follows: float = 50.0, alpha: float = 1.2,
                 seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """Graphe « qui suit qui » réaliste : la popularité suit une loi de puissance.

    Chaque utilisateur u reçoit un poids de popularité w_u (loi de Pareto d'indice
    `alpha` : quelques célébrités, une immense majorité d'inconnus) ; il suit k_u comptes
    (k_u log-normal de moyenne ~ `mean_follows`) tirés proportionnellement à w.

    Renvoie deux tableaux de même longueur (follower, followee) : l'arête i signifie
    « follower[i] suit followee[i] ». Pas de boucle ni de doublon.
    """
    rng = np.random.default_rng(seed)
    w = rng.pareto(alpha, n_users) + 1.0
    w /= w.sum()
    sigma = 0.8
    k = rng.lognormal(math.log(mean_follows) - sigma ** 2 / 2, sigma, n_users)
    k = np.clip(np.round(k).astype(int), 1, n_users - 1)
    follower = np.repeat(np.arange(n_users), k)
    followee = rng.choice(n_users, size=follower.size, p=w)
    keep = follower != followee
    code = np.unique(follower[keep].astype(np.int64) * n_users + followee[keep])
    return (code // n_users).astype(np.int64), (code % n_users).astype(np.int64)


def degree_counts(follower: np.ndarray, followee: np.ndarray, n_users: int) -> Tuple[np.ndarray, np.ndarray]:
    """(nombre d'abonnés, nombre d'abonnements) de chaque utilisateur."""
    return (np.bincount(followee, minlength=n_users), np.bincount(follower, minlength=n_users))


def fanout_costs(follower: np.ndarray, followee: np.ndarray, n_users: int,
                 post_rate, read_rate, threshold: Optional[float] = None) -> Dict[str, float]:
    """Coût (opérations élémentaires par seconde) des trois stratégies de timeline.

    post_rate, read_rate : taux de publication et de lecture de timeline par utilisateur
    (scalaire ou tableau de taille n_users), en événements par seconde.

    - « lecture » (fan-out à la lecture, requête SQL) : 1 écriture par message ; une
      lecture de timeline consulte les messages récents de chaque compte suivi ;
    - « écriture » (timelines matérialisées) : un message est inséré dans la timeline de
      chacun de ses abonnés ; une lecture coûte 1 accès au cache ;
    - « hybride » (si `threshold` est donné) : les comptes ayant au moins `threshold`
      abonnés (célébrités) ne sont pas diffusés ; leurs messages sont fusionnés à la
      lecture (1 accès au cache + 1 par célébrité suivie).
    """
    n_followers, n_following = degree_counts(follower, followee, n_users)
    p = np.broadcast_to(np.asarray(post_rate, dtype=float), (n_users,))
    r = np.broadcast_to(np.asarray(read_rate, dtype=float), (n_users,))
    out = {
        "lecture_ecritures": float(p.sum()),
        "lecture_lectures": float((r * n_following).sum()),
        "ecriture_ecritures": float((p * n_followers).sum()),
        "ecriture_lectures": float(r.sum()),
    }
    if threshold is not None:
        celeb = n_followers >= threshold
        celeb_followed = np.bincount(follower[celeb[followee]], minlength=n_users)
        out["hybride_ecritures"] = float((p * np.where(celeb, 1, n_followers)).sum())
        out["hybride_lectures"] = float((r * (1 + celeb_followed)).sum())
        out["n_celebrites"] = int(celeb.sum())
    for s in ("lecture", "ecriture", "hybride"):
        if f"{s}_ecritures" in out:
            out[f"{s}_total"] = out[f"{s}_ecritures"] + out[f"{s}_lectures"]
    return out


class _Timelines:
    """Base commune : compteurs de travail et adjacences."""

    def __init__(self, follower: Sequence[int], followee: Sequence[int], n_users: int, k: int = 50):
        self.k = k
        self.followers: List[List[int]] = [[] for _ in range(n_users)]
        self.following: List[List[int]] = [[] for _ in range(n_users)]
        for a, b in zip(follower, followee):
            self.followers[int(b)].append(int(a))
            self.following[int(a)].append(int(b))
        self.posts: List[deque] = [deque(maxlen=k) for _ in range(n_users)]
        self.write_ops = 0   # écritures élémentaires (insertion d'un message quelque part)
        self.read_ops = 0    # lectures élémentaires (consultation d'une liste)

    def post(self, user: int, t: float, text: str = "") -> None:
        raise NotImplementedError

    def timeline(self, user: int) -> List[Tuple[float, int, str]]:
        raise NotImplementedError


class PullTimelines(_Timelines):
    """Fan-out à la lecture : on stocke chaque message une fois, on fusionne à la lecture."""

    def post(self, user, t, text=""):
        self.posts[user].appendleft((t, user, text))
        self.write_ops += 1

    def timeline(self, user):
        lists = []
        for f in self.following[user]:
            lists.append(self.posts[f])
            self.read_ops += 1
        return heapq.nlargest(self.k, (m for lst in lists for m in lst))


class PushTimelines(_Timelines):
    """Fan-out à l'écriture : chaque message est livré dans la boîte de chaque abonné."""

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.cache: List[deque] = [deque(maxlen=self.k) for _ in range(len(self.posts))]

    def post(self, user, t, text=""):
        self.posts[user].appendleft((t, user, text))
        self.write_ops += 1
        for f in self.followers[user]:
            self.cache[f].appendleft((t, user, text))
            self.write_ops += 1

    def timeline(self, user):
        self.read_ops += 1
        return list(self.cache[user])


class HybridTimelines(PushTimelines):
    """Hybride : diffusion pour les comptes ordinaires, fusion à la lecture pour les célébrités."""

    def __init__(self, follower, followee, n_users, k=50, threshold=1000):
        super().__init__(follower, followee, n_users, k=k)
        self.celebrity = [len(fs) >= threshold for fs in self.followers]
        self.celebs_followed = [[f for f in fl if self.celebrity[f]] for fl in self.following]

    def post(self, user, t, text=""):
        if self.celebrity[user]:
            self.posts[user].appendleft((t, user, text))
            self.write_ops += 1
        else:
            super().post(user, t, text)

    def timeline(self, user):
        self.read_ops += 1
        parts = [self.cache[user]]
        for c in self.celebs_followed[user]:
            parts.append(self.posts[c])
            self.read_ops += 1
        return heapq.nlargest(self.k, (m for lst in parts for m in lst))


# =====================================================================================
# 2. Performance : temps de réponse et percentiles
# =====================================================================================


def synthetic_response_times(n: int, seed: int = 0, median_ms: float = 40.0, sigma: float = 0.45,
                             p_slow: float = 0.015, slow_ms: Tuple[float, float] = (300.0, 1500.0)) -> np.ndarray:
    """Temps de réponse (ms) réalistes : corps log-normal + petite fraction de requêtes lentes.

    Les requêtes lentes modélisent les aléas du livre (pause du ramasse-miettes,
    retransmission TCP, défaut de page...) : avec probabilité `p_slow`, on ajoute un délai
    uniforme dans `slow_ms`.
    """
    rng = np.random.default_rng(seed)
    x = rng.lognormal(math.log(median_ms), sigma, n)
    slow = rng.random(n) < p_slow
    x[slow] += rng.uniform(slow_ms[0], slow_ms[1], slow.sum())
    return x
