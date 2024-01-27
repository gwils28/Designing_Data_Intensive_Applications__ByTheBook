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


def dual_write_divergence_formula(gap: float, jitter: float) -> float:
    """Probabilité exacte de divergence pour des délais exponentiels de même moyenne.

    La différence de deux exponentielles indépendantes de moyenne j suit une loi de Laplace :
    P(l'écriture A arrive après B dans un système) = p = exp(-gap/j) / 2 ; les deux
    systèmes divergent avec probabilité 2 p (1 - p).
    """
    p = 0.5 * math.exp(-gap / jitter)
    return 2 * p * (1 - p)


def unfriend_trial(rng: random.Random, gap: float, jitter: float, fix: bool = False,
                   base: float = 1.0) -> tuple[bool, float]:
    """Scénario du livre : retrait d'un ami à t=0, puis message méchant à t=gap.

    Les deux événements vivent dans deux journaux différents (amitiés, messages) et
    arrivent au service de notifications avec des délais indépendants. Sans correctif, le
    service notifie l'ex-ami s'il voit le message avant le retrait (fuite). Avec correctif,
    le message porte la version de la liste d'amis vue par l'expéditeur : le service attend
    d'avoir appliqué cette version avant de notifier.
    Renvoie (fuite ?, délai de notification du message après son arrivée).
    """
    t_unfriend = _delay(rng, base, jitter)
    t_message = gap + _delay(rng, base, jitter)
    if not fix:
        return t_unfriend > t_message, 0.0
    return False, max(0.0, t_unfriend - t_message)


def unfriend_experiment(gaps, jitter: float, n_trials: int = 20_000, seed: int = 0) -> dict[str, np.ndarray]:
    """Taux de fuite sans correctif, attente moyenne et p99 avec correctif, par écart."""
    rng = random.Random(seed)
    leak, wait_mean, wait_p99 = [], [], []
    for g in gaps:
        lk = [unfriend_trial(rng, g, jitter)[0] for _ in range(n_trials)]
        w = np.array([unfriend_trial(rng, g, jitter, fix=True)[1] for _ in range(n_trials)])
        leak.append(np.mean(lk))
        wait_mean.append(w.mean())
        wait_p99.append(np.quantile(w, 0.99))
    return {"gap": np.asarray(gaps, float), "leak": np.array(leak), "wait_mean": np.array(wait_mean),
            "wait_p99": np.array(wait_p99)}


# ------------------------------------------------------- synchrone contre asynchrone
def outage_mask(rng: np.random.Generator, n_steps: int, mtbf: float, mttr: float) -> np.ndarray:
    """Tableau booléen « en panne » par pas de temps : alternance de durées exponentielles
    de fonctionnement (moyenne mtbf) et de réparation (moyenne mttr)."""
    down = np.zeros(n_steps, dtype=bool)
    t = rng.exponential(mtbf)
    while t < n_steps:
        d = rng.exponential(mttr)
        down[int(t):int(min(n_steps, t + d)) + 1] = True
        t += d + rng.exponential(mtbf)
    return down


def sync_vs_async(n_derived: int = 4, n_steps: int = 30 * 24 * 60, mtbf: float = 5 * 24 * 60,
                  mttr: float = 60, catch_up: float = 3.0, seed: int = 0) -> dict[str, Any]:
    """Une base de référence et n systèmes dérivés (pas de temps : 1 minute).

    - synchrone (transaction distribuée) : une écriture n'est acceptée que si la base ET
      tous les systèmes dérivés sont disponibles ;
    - asynchrone (journal) : une écriture est acceptée dès que la base est disponible ; un
      système dérivé en panne accumule un retard qu'il rattrape ensuite à `catch_up` fois
      le débit d'écriture.
    Renvoie les taux de refus et le retard (en minutes) de chaque système dérivé.
    """
    rng = np.random.default_rng(seed)
    db_down = outage_mask(rng, n_steps, mtbf, mttr)
    derived_down = np.array([outage_mask(rng, n_steps, mtbf, mttr) for _ in range(n_derived)])
    any_down = db_down | derived_down.any(axis=0)
    lag = np.zeros((n_derived, n_steps))
    for i in range(n_derived):
        backlog = 0.0
        for t in range(n_steps):
            if not db_down[t]:
                backlog += 1.0
            if not derived_down[i, t]:
                backlog = max(0.0, backlog - catch_up)
            lag[i, t] = backlog
    return {"sync_refused": any_down.mean(), "async_refused": db_down.mean(), "lag": lag,
            "db_down": db_down, "derived_down": derived_down}


# --------------------------------------------------------- index construit en ligne
def replay_table(changes, upto: int | None = None) -> dict:
    """État d'une table clé -> valeur après les `upto` premiers changements
    (("put", clé, valeur) ou ("del", clé, None))."""
    table = {}
    for op, k, v in changes[:upto]:
        if op == "put":
            table[k] = v
        else:
            table.pop(k, None)
    return table


def index_from_table(table: dict) -> dict:
    """Index secondaire valeur -> liste triée des clés (construit d'un coup)."""
    idx = defaultdict(set)
    for k, v in table.items():
        idx[v].add(k)
    return {v: sorted(ks) for v, ks in sorted(idx.items())}


class IndexBuilder:
    """Index secondaire dérivé : construit sur un instantané, puis tenu à jour en
    consommant le journal des changements à partir de l'offset de l'instantané."""

    def __init__(self):
        self.current: dict = {}          # copie locale clé -> valeur (pour retirer l'ancienne entrée)
        self.idx: dict = defaultdict(set)
        self.offset = 0
        self.applied = 0

    def load_snapshot(self, table: dict, offset: int) -> None:
        for k, v in table.items():
            self.current[k] = v
            self.idx[v].add(k)
        self.offset = offset

    def apply(self, change) -> None:
        op, k, v = change
        old = self.current.pop(k, None)
        if old is not None:
            self.idx[old].discard(k)
            if not self.idx[old]:
                del self.idx[old]
        if op == "put":
            self.current[k] = v
            self.idx[v].add(k)
        self.applied += 1

    def catch_up(self, changes, upto: int | None = None) -> None:
        end = len(changes) if upto is None else upto
        for c in changes[self.offset:end]:
            self.apply(c)
        self.offset = max(self.offset, end)

    def as_dict(self) -> dict:
        return {v: sorted(ks) for v, ks in sorted(self.idx.items()) if ks}


# ----------------------------------------------------------- couplage lâche
def coupling_sim(mode: str, n_steps: int, rate: float, capacities, outages=None) -> dict[str, np.ndarray]:
    """Un producteur (débit `rate` par pas) et plusieurs consommateurs.

    capacities : capacité de traitement de chaque consommateur (par pas), ou matrice
                 (consommateurs x pas) de capacités variables ;
    outages    : liste (par consommateur) d'intervalles [t0, t1) pendant lesquels il est en
                 panne (capacité nulle).
    mode "sync"  : chaque écriture doit être appliquée par tous les consommateurs dans le
                   même pas (débit accepté = min des capacités disponibles) ;
    mode "async" : le producteur écrit dans un journal ; chaque consommateur lit à son rythme.
    Renvoie le débit accepté par pas et le retard (backlog) de chaque consommateur.
    """
    caps = np.asarray(capacities, float)
    outages = outages or [[] for _ in caps]
    cap_t = caps.copy() if caps.ndim == 2 else np.tile(caps[:, None], (1, n_steps))
    for i, ivs in enumerate(outages):
        for t0, t1 in ivs:
            cap_t[i, t0:t1] = 0.0
    accepted = np.zeros(n_steps)
    backlog = np.zeros((len(caps), n_steps))
    b = np.zeros(len(caps))
    for t in range(n_steps):
        if mode == "sync":
            accepted[t] = min(rate, cap_t[:, t].min())
        else:
            accepted[t] = rate
            b = np.maximum(0.0, b + rate - cap_t[:, t])
        backlog[:, t] = b
    return {"accepted": accepted, "backlog": backlog}


# --------------------------------------------------------- dataflow façon tableur
class Dataflow:
    """Graphe de cellules : des entrées et des formules ; modifier une entrée recalcule,
    dans l'ordre topologique, seulement les cellules qui en dépendent."""

    def __init__(self):
        self.values: dict[str, Any] = {}
        self.formulas: dict[str, tuple[Callable, tuple[str, ...]]] = {}
        self.dependents: dict[str, list[str]] = defaultdict(list)
        self.recomputations = 0
        self.order: list[str] = []

    def set(self, name: str, value: Any) -> list[str]:
        self.values[name] = value
        return self._propagate(name)

    def define(self, name: str, fn: Callable, *inputs: str) -> None:
        self.formulas[name] = (fn, inputs)
        for i in inputs:
            self.dependents[i].append(name)
        self._compute(name)
        self.order.append(name)

    def _compute(self, name: str) -> None:
        fn, inputs = self.formulas[name]
        self.values[name] = fn(*(self.values[i] for i in inputs))
        self.recomputations += 1

    def _propagate(self, name: str) -> list[str]:
        # ensemble des descendants, recalculés dans l'ordre de définition (topologique)
        todo, seen = [name], set()
        while todo:
            n = todo.pop()
            for d in self.dependents.get(n, []):
                if d not in seen:
                    seen.add(d)
                    todo.append(d)
        touched = [n for n in self.order if n in seen]
        for n in touched:
            self._compute(n)
        return touched

    def __getitem__(self, name: str) -> Any:
        return self.values[name]


def make_changelog_db() -> sqlite3.Connection:
    """Base SQLite dont la table `products` publie ses changements dans une table
    `changelog` (identifiant croissant) grâce à des déclencheurs : une capture de
    changements rudimentaire, lisible par offset."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.executescript("""
        CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
        CREATE TABLE changelog (seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, id INTEGER,
                                name TEXT, price REAL);
        CREATE TRIGGER t_ins AFTER INSERT ON products BEGIN
            INSERT INTO changelog (op, id, name, price) VALUES ('put', NEW.id, NEW.name, NEW.price); END;
        CREATE TRIGGER t_upd AFTER UPDATE ON products BEGIN
            INSERT INTO changelog (op, id, name, price) VALUES ('put', NEW.id, NEW.name, NEW.price); END;
        CREATE TRIGGER t_del AFTER DELETE ON products BEGIN
            INSERT INTO changelog (op, id, name, price) VALUES ('del', OLD.id, NULL, NULL); END;
    """)
    return conn


# ------------------------------------------------------- jointure temporelle
def random_walk_rates(seed: int, horizon: float, mean_interval: float, start: float = 1.10,
                      vol: float = 0.002) -> tuple[np.ndarray, np.ndarray]:
    """Instants et valeurs des mises à jour d'un taux de change (marche aléatoire)."""
    rng = np.random.default_rng(seed)
    n = int(horizon / mean_interval * 1.5) + 10
    t = np.cumsum(rng.exponential(mean_interval, n))
    t = np.concatenate([[0.0], t[t < horizon]])
    v = start * np.exp(np.cumsum(np.concatenate([[0.0], rng.normal(0, vol, len(t) - 1)])))
    return t, v
