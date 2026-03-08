"""Surcharge, tempêtes de relances et panne métastable (notions de la 2e édition).

Un serveur FIFO à capacité finie reçoit des requêtes de clients qui abandonnent au bout
d'un délai (timeout) et **relancent**. Quand la file dépasse ce délai, toutes les
réponses arrivent trop tard : le serveur travaille à plein pour rien, et les relances
augmentent encore la charge. Même après la fin du pic qui a déclenché la surcharge, le
système peut rester bloqué dans cet état : c'est une panne **métastable**.

Remèdes simulés : attente exponentielle avec gigue (exponential backoff, jitter),
budget de relances (à la manière d'un token bucket), délestage côté serveur (load
shedding), qui rejette vite les requêtes qu'il ne pourra pas servir à temps.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List

import numpy as np

__all__ = ["RetryPolicy", "simulate_overload"]


@dataclass
class RetryPolicy:
    """Comportement des clients.

    max_retries : nombre maximal de relances par requête (0 = aucune) ;
    backoff     : attente avant la 1re relance (s) ; doublée à chaque relance si
                  `exponential` ; tirée uniformément dans [0, attente] si `jitter` ;
    budget      : si non None, les relances ne sont permises que tant qu'elles restent
                  sous `budget` fois le nombre de requêtes neuves (compteur à jetons).
    """

    max_retries: int = 0
    backoff: float = 0.0
    exponential: bool = False
    jitter: bool = False
    budget: float | None = None

    def delay(self, attempt: int, rng: np.random.Generator) -> float:
        d = self.backoff * (2 ** (attempt - 1) if self.exponential else 1.0)
        return float(rng.uniform(0, d)) if self.jitter else d


def simulate_overload(rate: Callable[[float], float], capacity: float, duration: float,
                      timeout: float, policy: RetryPolicy, shed: bool = False,
                      reject_cost: float = 0.05, seed: int = 0) -> Dict[str, np.ndarray]:
    """Simule un serveur FIFO (service exponentiel de taux `capacity`) pendant `duration` s.

    rate(t) : taux d'arrivée des requêtes **neuves** (Poisson non homogène, par seconde).
    Un client attend au plus `timeout` ; sans réponse, il relance selon `policy`.
    Le serveur traite toute requête acceptée (même si le client est parti : travail
    perdu). Avec `shed=True`, il rejette d'emblée une requête dont l'attente prévue
    dépasse le timeout, pour un coût de `reject_cost` fois un service moyen.

    Renvoie des séries par seconde : `tentatives`, `neuves`, `succes` (réponses dans les
    temps), `gaspille` (requêtes servies trop tard), `rejets`, `attente` (attente en file
    vue par la dernière arrivée de la seconde, en s).
    """
    rng = np.random.default_rng(seed)
    T = int(math.ceil(duration))
    series = {k: np.zeros(T) for k in ("tentatives", "neuves", "succes", "gaspille", "rejets", "attente")}
    # arrivées neuves : amincissement d'un processus de Poisson
    rmax = max(rate(t) for t in np.linspace(0, duration, 2001)) * 1.05 + 1e-9
    events: List = []
    t = 0.0
    seq = 0
    while True:
        t += rng.exponential(1.0 / rmax)
        if t >= duration:
            break
        if rng.random() < rate(t) / rmax:
            heapq.heappush(events, (t, seq, 0))
            seq += 1
    free_at = 0.0          # instant où le serveur aura vidé sa file
    tokens = 10.0          # jetons de relance disponibles (budget)
    while events:
        t, _, attempt = heapq.heappop(events)
        if t >= duration:
            continue
        s = int(t)
        series["tentatives"][s] += 1
        if attempt == 0:
            series["neuves"][s] += 1
            if policy.budget is not None:
                tokens = min(tokens + policy.budget, 100.0)
        wait = max(0.0, free_at - t)
        series["attente"][s] = wait
        if shed and wait > timeout:
            free_at = max(free_at, t) + reject_cost * rng.exponential(1.0 / capacity)
            series["rejets"][s] += 1
            ok = False
            t_fail = t
        else:
            service = rng.exponential(1.0 / capacity)
            free_at = max(free_at, t) + service
            response = free_at - t
            ok = response <= timeout
            if ok:
                series["succes"][min(int(free_at), T - 1)] += 1
                continue
            series["gaspille"][min(int(free_at), T - 1)] += 1
            t_fail = t + timeout
        # échec (timeout ou rejet) : relance éventuelle
        if attempt < policy.max_retries:
            if policy.budget is not None:
                if tokens < 1.0:
                    continue
                tokens -= 1.0
            heapq.heappush(events, (t_fail + policy.delay(attempt + 1, rng), seq, attempt + 1))
            seq += 1
    return series
