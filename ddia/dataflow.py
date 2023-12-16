"""Systèmes de flux de données : dérivation, dégroupage de la base et justesse de bout en bout.

Fonctions réutilisables du chapitre *A Philosophy of Streaming Systems* (2e édition,
chapitre 13 ; *The Future of Data Systems*, chapitre 12 de la 1re édition) :

- journal partitionné en ajout seul (offsets, consommateurs) ;
- double écriture contre journal unique, ordre causal (scénario « retrait d'ami ») ;
- disponibilité d'une mise à jour synchrone contre asynchrone ;
- construction d'un index en ligne (instantané + rattrapage), graphe de dataflow réactif
  façon tableur, journal de changements par déclencheurs SQLite ;
- jointure « as of » (taux de change) ;
- index inversé instrumenté (chemin d'écriture contre chemin de lecture) ;
- portée d'une URL calculée par diffusion entre shards ;
- virements avec identifiants de requête (SQLite), argument de bout en bout (sommes de
  contrôle à chaque couche), unicité par journal partitionné, virement multi-shard sans
  validation atomique avec pannes et rejeu ;
- surréservation, inventaire multi-régions avec ou sans coordination ;
- corruption silencieuse et vérification périodique, journal chaîné par hachage, arbres de
  Merkle (preuves d'inclusion, comparaison de répliques).
"""

from __future__ import annotations

import bisect
import hashlib
import math
import random
import sqlite3
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

__all__ = [
    "stable_hash", "Log", "dual_write_trial", "dual_write_divergence", "dual_write_divergence_formula",
    "outage_mask", "sync_vs_async", "unfriend_trial", "unfriend_experiment",
    "replay_table", "index_from_table", "IndexBuilder", "coupling_sim", "Dataflow",
    "make_changelog_db", "random_walk_rates", "asof_join",
    "make_corpus", "InvertedIndex", "grep_search", "zipf_queries",
    "reach_scatter_gather", "make_bank_db", "transfer", "retrying_client",
    "uuid_collision_prob", "e2e_pipeline", "checksum_detection_matrix",
    "UsernameShard", "claim_usernames", "multi_leader_claims",
    "PaymentSystem", "overbooking_profit", "best_overbooking", "inventory_regions",
    "scrub_loss_probability", "simulate_scrubbing", "HashChainLog", "MerkleTree",
]


# ----------------------------------------------------------------------------- journaux
def stable_hash(key: Any) -> int:
    """Hachage stable d'une clé (indépendant de PYTHONHASHSEED)."""
    return int(hashlib.md5(str(key).encode()).hexdigest()[:12], 16)


class Log:
    """Journal partitionné en ajout seul : chaque partition est une liste d'entrées
    (clé, valeur) adressées par leur offset."""

    def __init__(self, n_partitions: int = 1):
        self.partitions: list[list[tuple[Any, Any]]] = [[] for _ in range(n_partitions)]

    @property
    def n_partitions(self) -> int:
        return len(self.partitions)

    def partition_of(self, key: Any) -> int:
        return stable_hash(key) % self.n_partitions

    def append(self, key: Any, value: Any, partition: int | None = None) -> tuple[int, int]:
        p = self.partition_of(key) if partition is None else partition
        self.partitions[p].append((key, value))
        return p, len(self.partitions[p]) - 1

    def read(self, partition: int, offset: int, max_n: int | None = None) -> list[tuple[int, Any, Any]]:
        part = self.partitions[partition]
        end = len(part) if max_n is None else min(len(part), offset + max_n)
        return [(o, *part[o]) for o in range(offset, end)]

    def end_offset(self, partition: int) -> int:
        return len(self.partitions[partition])

    def __len__(self) -> int:
        return sum(len(p) for p in self.partitions)


# ---------------------------------------------------------- double écriture et causalité
def _delay(rng: random.Random, base: float, jitter: float) -> float:
    return base + rng.expovariate(1.0 / jitter) if jitter > 0 else base


def dual_write_trial(rng: random.Random, gap: float, jitter: float, base: float = 1.0,
                     via_log: bool = False) -> tuple[str, str]:
    """Deux clients écrivent la même clé : A à t=0, B à t=gap.

    Double écriture : chaque client envoie lui-même sa valeur à la base et à l'index, avec
    des délais réseau indépendants (base + exponentielle de moyenne jitter) ; chaque
    système garde la dernière valeur **reçue**.
    Journal : les deux écritures sont d'abord ajoutées à un journal (l'ordre d'arrivée dans
    le journal fait foi) puis appliquées dans cet ordre par les deux systèmes.
    Renvoie (valeur finale de la base, valeur finale de l'index).
    """
    if via_log:
        a, b = _delay(rng, base, jitter), gap + _delay(rng, base, jitter)
        last = "A" if a > b else "B"
        return last, last
    finals = []
    for _ in ("base", "index"):
        a, b = _delay(rng, base, jitter), gap + _delay(rng, base, jitter)
        finals.append("A" if a > b else "B")
    return finals[0], finals[1]


def dual_write_divergence(gap: float, jitter: float, n_trials: int = 20_000, seed: int = 0,
                          via_log: bool = False) -> float:
    """Fraction d'essais où la base et l'index finissent avec des valeurs différentes."""
    rng = random.Random(seed)
    bad = 0
    for _ in range(n_trials):
        x, y = dual_write_trial(rng, gap, jitter, via_log=via_log)
        bad += x != y
    return bad / n_trials
