"""Traitement de flux : courtiers, journaux partitionnés, CDC, event sourcing, fenêtres,
jointures et tolérance aux pannes.

Fonctions réutilisables des notebooks du chapitre « traitement de flux »
(2e édition : chapitre 12 ; 1re édition : chapitre 11) :

- **transmettre des flux** : politiques de débordement (perte, tampon, contre-pression),
  file partagée à la AMQP/JMS avec acquittements et relivraisons (réordonnancement),
  blocage en tête de file d'un journal partitionné ;
- **journal partitionné** à la Kafka : partitionneur murmur2 identique à celui de Kafka,
  segments, offsets, rétention, compaction, groupes de consommateurs et commits d'offsets ;
- **bases de données et flux** : course de la double écriture, capture des changements
  (CDC) sur SQLite par déclencheurs, index plein texte dérivé, compaction de journal ;
- **event sourcing** : magasin d'événements avec contrôle de version optimiste, rejeu,
  instantanés ;
- **traitement** : détection de motifs (CEP), HyperLogLog, percolateur (requêtes
  stockées), fenêtres (fixes, glissantes par saut, glissantes, sessions), compteur à
  filigrane (watermark) et retardataires ;
- **jointures** : flux-flux (recherche et clic), flux-table (enrichissement),
  table-table (timelines matérialisées) ;
- **tolérance aux pannes** : job à état avec pannes injectées (au plus une fois, au
  moins une fois, idempotence, commit atomique), puits protégé par jeton de fencing.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import bisect
import hashlib
import heapq
import json
import math
import random
import sqlite3
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

# =====================================================================================
# 1. Transmettre des flux : débordement, file partagée, blocage en tête de file
# =====================================================================================


def simulate_overflow(produced: Sequence[float], capacity: float, buffer_size: float,
                      policy: str) -> Dict[str, np.ndarray]:
    """Producteur plus rapide que le consommateur, modèle fluide pas à pas.

    produced : messages que le producteur *voudrait* émettre à chaque pas ;
    capacity : messages que le consommateur traite par pas ;
    policy   : "drop" (tampon borné, l'excédent est perdu), "buffer" (tampon non borné)
               ou "backpressure" (tampon borné, le producteur est ralenti et accumule
               son retard chez lui).

    Renvoie les séries : file (dans le courtier), livrés, perdus (cumulés), retard du
    producteur (messages non encore émis, contre-pression), latence estimée (pas).
    """
    produced = np.asarray(produced, dtype=float)
    n = produced.size
    queue = np.zeros(n)
    delivered = np.zeros(n)
    lost = np.zeros(n)
    backlog = np.zeros(n)
    q = b = l = 0.0
    for i in range(n):
        want = produced[i] + b
        if policy == "buffer":
            q += want
            b = 0.0
        elif policy == "drop":
            room = max(0.0, buffer_size - q)
            accepted = min(want, room + capacity)
            l += want - accepted
            q += accepted
            b = 0.0
        elif policy == "backpressure":
            room = max(0.0, buffer_size - q)
            accepted = min(want, room + capacity)
            b = want - accepted
            q += accepted
        else:
            raise ValueError(policy)
        d = min(q, capacity)
        q -= d
        queue[i], delivered[i], lost[i], backlog[i] = q, d, l, b
    latency = (queue + backlog) / capacity
    return {"queue": queue, "delivered": delivered, "lost": lost, "backlog": backlog,
            "latency": latency}


@dataclass
class Processing:
    """Un traitement de message par un consommateur (pour les diagrammes et l'analyse)."""

    msg: int
    consumer: int
    t_start: float
    t_end: float
    ok: bool


def simulate_shared_queue(n_messages: int, n_consumers: int, crash_prob: float = 0.0,
                          service: Tuple[float, float] = (0.8, 1.2), restart_delay: float = 2.0,
                          seed: int = 0) -> List[Processing]:
    """File partagée à la AMQP/JMS : répartition de charge, acquittements, relivraison.

    Les messages 0..n-1 sont dans la file, dans l'ordre. Chaque consommateur libre prend
    le premier message disponible. Avec probabilité `crash_prob`, il tombe en panne en
    cours de traitement : le message, non acquitté, est remis **en tête** de file (comme
    RabbitMQ) et le consommateur redémarre après `restart_delay`.
    Renvoie la liste chronologique des traitements (réussis ou interrompus).
    """
    rng = random.Random(seed)
    pending = deque(range(n_messages))
    out: List[Processing] = []
    # boucle d'événements : le consommateur qui se libère le plus tôt prend un message
    heap = [(0.0, c) for c in range(n_consumers)]
    heapq.heapify(heap)
    retry: List[Tuple[float, int]] = []          # (instant de remise en file, message)
    while pending or retry:
        t, c = heapq.heappop(heap)
        # remettre en tête les messages dont le consommateur a été déclaré mort
        retry.sort()
        while retry and retry[0][0] <= t:
            pending.appendleft(retry.pop(0)[1])
        if not pending:
            nxt = retry[0][0]
            heapq.heappush(heap, (nxt, c))
            continue
        m = pending.popleft()
        d = rng.uniform(*service)
        if rng.random() < crash_prob:
            t_crash = t + d * rng.uniform(0.3, 0.9)
            out.append(Processing(m, c, t, t_crash, False))
            retry.append((t_crash, m))
            heapq.heappush(heap, (t_crash + restart_delay, c))
        else:
            out.append(Processing(m, c, t, t + d, True))
            heapq.heappush(heap, (t + d, c))
    out.sort(key=lambda p: (p.t_end, p.msg))
    return out


def inversions(order: Sequence[int]) -> int:
    """Nombre de paires (i, j) traitées dans l'ordre inverse de leur envoi."""
    order = list(order)
    count = 0
    for i in range(len(order)):
        for j in range(i + 1, len(order)):
            if order[i] > order[j]:
                count += 1
    return count


def hol_latencies(arrivals: np.ndarray, services: np.ndarray, n_workers: int,
                  mode: str, keys: Optional[np.ndarray] = None) -> np.ndarray:
    """Temps de séjour de chaque message (attente + traitement).

    mode "shared"      : file unique, le premier consommateur libre prend le message
                         suivant (style AMQP, ordre non garanti) ;
    mode "partitioned" : chaque message va dans la partition `keys[i] % n_workers`, servie
                         par un seul consommateur dans l'ordre (style journal) : un
                         message lent bloque ceux qui le suivent dans sa partition.
    """
    arrivals = np.asarray(arrivals, float)
    services = np.asarray(services, float)
    n = arrivals.size
    out = np.empty(n)
    if mode == "shared":
        free = [0.0] * n_workers
        heapq.heapify(free)
        for i in range(n):
            f = heapq.heappop(free)
            start = max(f, arrivals[i])
            end = start + services[i]
            heapq.heappush(free, end)
            out[i] = end - arrivals[i]
    elif mode == "partitioned":
        keys = np.arange(n) if keys is None else np.asarray(keys)
        free = np.zeros(n_workers)
        for i in range(n):
            p = int(keys[i]) % n_workers
            start = max(free[p], arrivals[i])
            free[p] = start + services[i]
            out[i] = free[p] - arrivals[i]
    else:
        raise ValueError(mode)
    return out


# =====================================================================================
# 2. Journal partitionné à la Kafka
# =====================================================================================


def murmur2(data: bytes) -> int:
    """Hachage murmur2 32 bits, tel qu'implémenté par le client Java de Kafka."""
    length = len(data)
    seed, m, r = 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[4 * i: 4 * i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length & ~3
    extra = length % 4
    if extra >= 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h & 0xFFFFFFFF
