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


def asof_join(times: np.ndarray, rate_times: np.ndarray, rate_values: np.ndarray) -> np.ndarray:
    """Valeur du taux en vigueur à chaque instant (dernière mise à jour <= instant)."""
    idx = np.searchsorted(rate_times, times, side="right") - 1
    return rate_values[np.clip(idx, 0, None)]


# -------------------------------------------- chemin d'écriture et chemin de lecture
def make_corpus(n_docs: int, vocab: int = 2000, doc_len: int = 60, zipf_a: float = 1.1,
                seed: int = 0) -> list[list[int]]:
    """Documents synthétiques : mots tirés selon une loi de Zipf sur `vocab` termes."""
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, vocab + 1)
    p = ranks ** -zipf_a
    p /= p.sum()
    return [list(rng.choice(vocab, size=doc_len, p=p)) for _ in range(n_docs)]


def zipf_queries(n: int, n_distinct: int, a: float = 1.0, seed: int = 0) -> np.ndarray:
    """Identifiants de requêtes (0 = la plus fréquente) suivant une loi de Zipf tronquée."""
    rng = np.random.default_rng(seed)
    p = np.arange(1, n_distinct + 1) ** -a
    p /= p.sum()
    return rng.choice(n_distinct, size=n, p=p)


class InvertedIndex:
    """Index inversé instrumenté : compte les opérations élémentaires (entrées de listes
    de postings écrites à l'indexation, lues à la recherche)."""

    def __init__(self):
        self.postings: dict[int, list[int]] = defaultdict(list)
        self.write_ops = 0
        self.read_ops = 0

    def add(self, doc_id: int, words) -> None:
        for w in sorted(set(words)):
            self.postings[w].append(doc_id)
            self.write_ops += 1

    def search_and(self, terms) -> list[int]:
        lists = sorted((self.postings.get(t, []) for t in set(terms)), key=len)
        self.read_ops += sum(len(lst) for lst in lists)
        if not lists:
            return []
        res = set(lists[0])
        for lst in lists[1:]:
            res &= set(lst)
        return sorted(res)


def grep_search(corpus, terms) -> tuple[list[int], int]:
    """Recherche sans index : parcourt tous les mots de tous les documents."""
    ts = set(terms)
    res, ops = [], 0
    for i, doc in enumerate(corpus):
        ops += len(doc)
        if ts <= set(doc):
            res.append(i)
    return res, ops


# --------------------------------------------------------- traitement multi-shard
def reach_scatter_gather(followers: dict, posters, n_shards: int) -> dict[str, Any]:
    """Nombre de personnes ayant vu une URL = taille de l'union des abonnés de ceux qui
    l'ont publiée, les listes d'abonnés étant réparties en shards par utilisateur.

    Étape 1 : chaque publication est routée vers le shard qui détient la liste d'abonnés
    de son auteur ; étape 2 : chaque abonné est routé vers le shard qui le possède, qui
    déduplique ; étape 3 : les comptes partiels sont additionnés.
    """
    def shard(u):
        return stable_hash(u) % n_shards
    stage2 = [set() for _ in range(n_shards)]
    msgs1 = msgs2 = 0
    for p in posters:
        msgs1 += 1                      # publication -> shard de l'auteur
        for f in followers.get(p, ()):
            stage2[shard(f)].add(f)
            msgs2 += 1                  # abonné -> shard de l'abonné
    partial = [len(s) for s in stage2]
    return {"reach": sum(partial), "messages": msgs1 + msgs2 + n_shards,
            "max_shard_load": max(partial) if partial else 0, "partial": partial}


# ------------------------------------------------------ identifiants de requête
def make_bank_db(accounts: dict[int, float]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("CREATE TABLE accounts (account_id INTEGER PRIMARY KEY, balance REAL NOT NULL)")
    conn.execute("CREATE TABLE requests (request_id TEXT UNIQUE, src INTEGER, dst INTEGER, amount REAL)")
    conn.executemany("INSERT INTO accounts VALUES (?, ?)", accounts.items())
    return conn


def transfer(conn: sqlite3.Connection, request_id: str | None, src: int, dst: int, amount: float) -> str:
    """Virement dans une transaction. Avec request_id : la requête est d'abord inscrite
    dans `requests` (contrainte UNIQUE) ; un doublon fait échouer l'insertion et annule la
    transaction. Renvoie "appliqué" ou "doublon ignoré"."""
    try:
        conn.execute("BEGIN")
        if request_id is not None:
            conn.execute("INSERT INTO requests VALUES (?, ?, ?, ?)", (request_id, src, dst, amount))
        conn.execute("UPDATE accounts SET balance = balance + ? WHERE account_id = ?", (amount, dst))
        conn.execute("UPDATE accounts SET balance = balance - ? WHERE account_id = ?", (amount, src))
        conn.execute("COMMIT")
        return "appliqué"
    except sqlite3.IntegrityError:
        conn.execute("ROLLBACK")
        return "doublon ignoré"


def retrying_client(conn: sqlite3.Connection, n_transfers: int, p_lost_reply: float, use_ids: bool,
                    max_attempts: int = 5, seed: int = 0, amount: float = 11.0) -> dict[str, Any]:
    """Un client fait `n_transfers` virements du compte 2 vers le compte 1. La réponse du
    serveur se perd avec probabilité `p_lost_reply` (le virement, lui, a bien eu lieu) ;
    le client, qui ne sait pas, réessaie (jusqu'à max_attempts envois)."""
    rng = random.Random(seed)
    attempts = 0
    for i in range(n_transfers):
        rid = f"req-{seed}-{i}" if use_ids else None
        for _ in range(max_attempts):
            attempts += 1
            transfer(conn, rid, 2, 1, amount)
            if rng.random() >= p_lost_reply:
                break
    bal = dict(conn.execute("SELECT account_id, balance FROM accounts"))
    return {"intended": n_transfers * amount, "credited": bal[1], "attempts": attempts}


def uuid_collision_prob(n: float, bits: int) -> float:
    """Probabilité d'au moins une collision parmi n identifiants aléatoires de `bits` bits
    (paradoxe des anniversaires : 1 - exp(-n^2 / 2^(bits+1)))."""
    return -math.expm1(-n * n / 2.0 ** (bits + 1))


# ---------------------------------------------------- argument de bout en bout
FAULTS = ("aucune", "réseau", "bogue émetteur", "bogue récepteur", "disque")
SCHEMES = ("sommes par lien (TCP)", "lien + disque", "bout en bout")


def _flip(rng: random.Random, buf: bytearray) -> None:
    i = rng.randrange(len(buf))
    buf[i] ^= 1 << rng.randrange(8)


def e2e_pipeline(payload: bytes, fault: str, rng: random.Random) -> dict[str, bool]:
    """Fait voyager un enregistrement : application émettrice -> tampon d'envoi -> réseau
    -> application réceptrice -> disque -> lecture par l'application finale.

    Une faute (au plus une) corrompt un bit à l'étape indiquée. Renvoie, pour chaque
    mécanisme de vérification, s'il a détecté la corruption, et si la donnée finale est
    effectivement corrompue.
    """
    h_e2e = hashlib.sha256(payload).digest()      # calculé par l'application d'origine
    buf = bytearray(payload)
    if fault == "bogue émetteur":                 # bogue dans le code qui prépare l'envoi
        _flip(rng, buf)
    crc_link = zlib.crc32(buf)                    # somme de contrôle du lien (TCP)
    if fault == "réseau":
        _flip(rng, buf)
    link_detect = zlib.crc32(buf) != crc_link
    if fault == "bogue récepteur":                # bogue après la vérification réseau
        _flip(rng, buf)
    crc_disk = zlib.crc32(buf)                    # somme de contrôle du bloc disque
    if fault == "disque":
        _flip(rng, buf)
    disk_detect = zlib.crc32(buf) != crc_disk
    e2e_detect = hashlib.sha256(bytes(buf)).digest() != h_e2e
    return {"corrompu": bytes(buf) != payload, "sommes par lien (TCP)": link_detect,
            "lien + disque": link_detect or disk_detect, "bout en bout": e2e_detect}


def checksum_detection_matrix(n_trials: int = 200, seed: int = 0) -> dict[tuple[str, str], float]:
    """Taux de détection de chaque mécanisme pour chaque type de faute."""
    rng = random.Random(seed)
    out = {}
    for f in FAULTS:
        res = [e2e_pipeline(rng.randbytes(256), f, rng) for _ in range(n_trials)]
        for s in SCHEMES:
            out[(f, s)] = float(np.mean([r[s] for r in res]))
        out[(f, "corrompu")] = float(np.mean([r["corrompu"] for r in res]))
    return out


# ------------------------------------------------- unicité par journal partitionné
class UsernameShard:
    """Processeur de flux mono-thread d'une partition : le premier qui réclame un nom
    dans l'ordre du journal l'obtient, les suivants sont refusés."""

    def __init__(self):
        self.taken: dict[str, str] = {}

    def process(self, req_id: str, username: str) -> tuple[str, str]:
        if username in self.taken:
            return req_id, "refusé"
        self.taken[username] = req_id
        return req_id, "accepté"


def claim_usernames(requests, n_partitions: int) -> dict[str, Any]:
    """requests : liste ordonnée (req_id, username). Chaque requête est ajoutée à la
    partition hash(username) ; chaque partition est consommée séquentiellement."""
    log = Log(n_partitions)
    for rid, name in requests:
        log.append(name, rid)
    shards = [UsernameShard() for _ in range(n_partitions)]
    results = {}
    for p in range(n_partitions):
        for _, name, rid in log.read(p, 0):
            results[rid] = shards[p].process(rid, name)[1]
    owners = defaultdict(list)
    for rid, name in requests:
        if results[rid] == "accepté":
            owners[name].append(rid)
    return {"results": results, "per_partition": [log.end_offset(p) for p in range(n_partitions)],
            "duplicates": sum(len(v) - 1 for v in owners.values())}


def multi_leader_claims(times, names, n_leaders: int, repl_delay: float, seed: int = 0) -> int:
    """Réclamations de noms réparties au hasard sur des leaders qui ne voient les noms
    acceptés par les autres qu'après `repl_delay` : renvoie le nombre de noms attribués à
    plusieurs personnes."""
    rng = random.Random(seed)
    by_name: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for t, name in sorted(zip(times, names)):
        leader = rng.randrange(n_leaders)
        visible = any(l2 == leader or t >= t2 + repl_delay for t2, l2 in by_name[name])
        if not visible:
            by_name[name].append((t, leader))
    return sum(len(v) - 1 for v in by_name.values())


# ------------------------------------------- virement multi-shard sans 2PC
FEES = "frais"


@dataclass
class _ShardProcessor:
    balances: dict
    reserved: dict = field(default_factory=lambda: defaultdict(float))
    seen: set = field(default_factory=set)
    offset: int = 0


class PaymentSystem:
    """Virements entre comptes répartis sur des shards de journal, sans validation
    atomique : chaque requête (identifiant unique fourni par le client) est ajoutée au
    shard du compte source ; le processeur de ce shard vérifie le solde disponible,
    réserve le montant et émet trois événements portant le même identifiant (débit vers
    son propre shard, crédit vers le shard du destinataire, frais vers le shard des
    frais), plus un statut. Chaque processeur est déterministe et déduplique par
    identifiant ; il peut tomber en panne après avoir émis une partie de ses sorties : il
    reprend alors depuis son dernier point de reprise (état + offset), et rejoue.
    """

    def __init__(self, balances: dict, n_shards: int = 3, fee: float = 1.0):
        self.n_shards = n_shards
        self.fee = fee
        self.log = Log(n_shards)
        self.status: dict[str, list[str]] = defaultdict(list)
        self.procs = []
        for p in range(n_shards):
            accts = {a: b for a, b in balances.items() if self.shard(a) == p}
            if self.shard(FEES) == p:
                accts.setdefault(FEES, 0.0)
            self.procs.append(_ShardProcessor(balances=accts))
        self.initial_total = sum(balances.values()) + (0.0 if FEES in balances else 0.0)
        self.crashes = 0
        self.processed_events = 0

    def shard(self, account) -> int:
        return stable_hash(account) % self.n_shards

    def submit(self, req_id: str, src, dst, amount: float) -> None:
        self.log.append(src, ("demande", req_id, src, dst, amount), partition=self.shard(src))

    def _outputs(self, proc: _ShardProcessor, ev) -> tuple[list, Callable[[], None]]:
        """Sorties (shard, événement) et mutation d'état, calculées sans effet de bord."""
        kind, rid = ev[0], ev[1]
        if kind == "demande":
            _, _, src, dst, amt = ev
            if ("demande", rid) in proc.seen:
                return [], lambda: None
            total = amt + self.fee
            available = proc.balances[src] - proc.reserved[src]
            if available >= total:
                outs = [(self.shard(src), ("débit", rid, src, total)),
                        (self.shard(dst), ("crédit", rid, dst, amt)),
                        (self.shard(FEES), ("crédit", rid, FEES, self.fee)),
                        (None, ("statut", rid, "accepté"))]

                def mut():
                    proc.reserved[src] += total
                    proc.seen.add(("demande", rid))
                return outs, mut
            return [(None, ("statut", rid, "refusé"))], lambda: proc.seen.add(("demande", rid))
        _, _, acct, amt = ev
        key = (kind, rid, acct)
        if key in proc.seen:
            return [], lambda: None

        def mut():
            proc.seen.add(key)
            if kind == "débit":
                proc.balances[acct] -= amt
                proc.reserved[acct] -= amt
            else:
                proc.balances[acct] += amt
        return [], mut

    def step(self, p: int, rng: random.Random | None = None, p_crash: float = 0.0) -> bool:
        """Traite l'événement suivant du shard p ; renvoie False si rien à traiter."""
        proc = self.procs[p]
        if proc.offset >= self.log.end_offset(p):
            return False
        _, _, ev = self.log.read(p, proc.offset, 1)[0]
        outs, mut = self._outputs(proc, ev)
        crash_after = None
        if rng is not None and rng.random() < p_crash:
            crash_after = rng.randrange(len(outs) + 1)   # panne après k sorties émises
        for i, (dest, out) in enumerate(outs):
            if crash_after is not None and i == crash_after:
                break
            if dest is None:
                self.status[out[1]].append(out[2])
            else:
                self.log.append(out[2], out, partition=dest)
        if crash_after is not None:
            self.crashes += 1                      # état et offset non validés : on rejouera
            return True
        mut()                                      # point de reprise : état + offset ensemble
        proc.offset += 1
        self.processed_events += 1
        return True

    def run(self, seed: int = 0, p_crash: float = 0.0, max_steps: int = 1_000_000) -> None:
        rng = random.Random(seed)
        for _ in range(max_steps):
            pending = [p for p in range(self.n_shards) if self.procs[p].offset < self.log.end_offset(p)]
            if not pending:
                return
            self.step(rng.choice(pending), rng, p_crash)

    def balances(self) -> dict:
        out = {}
        for proc in self.procs:
            out.update(proc.balances)
        return out

    def check(self) -> dict[str, Any]:
        bal = self.balances()
        return {"total": round(sum(bal.values()), 6), "min_balance": min(bal.values()),
                "accepted": sum(1 for v in self.status.values() if v[0] == "accepté"),
                "refused": sum(1 for v in self.status.values() if v[0] == "refusé"),
                "status_dups": sum(len(v) - 1 for v in self.status.values()),
                "log_events": len(self.log)}


# ---------------------------------------------- contraintes souples et coordination
def overbooking_profit(sold: int, capacity: int, show_prob: float, price: float, bump_cost: float) -> float:
    """Recette espérée d'un vol de `capacity` places quand on vend `sold` billets : chaque
    passager se présente avec probabilité show_prob (indépendamment) ; chaque passager
    refoulé coûte bump_cost (indemnité, excuse)."""
    from math import comb
    exp_bumped = sum(comb(sold, k) * show_prob ** k * (1 - show_prob) ** (sold - k) * (k - capacity)
                     for k in range(capacity + 1, sold + 1))
    return sold * price - bump_cost * exp_bumped


def best_overbooking(capacity: int, show_prob: float, price: float, bump_cost: float,
                     max_extra: int = 40) -> tuple[int, list[float]]:
    profits = [overbooking_profit(capacity + e, capacity, show_prob, price, bump_cost) for e in range(max_extra + 1)]
    return int(np.argmax(profits)), profits


def inventory_regions(threshold: float, stock: int = 1000, n_regions: int = 3, rate: float = 1.2,
                      horizon: float = 1000.0, repl_delay: float = 5.0, partitions=None,
                      seed: int = 0) -> dict[str, int]:
    """Vente d'un stock limité depuis plusieurs régions.

    Chaque région connaît les ventes des autres avec un retard `repl_delay`. Tant que son
    estimation du stock restant dépasse `threshold`, elle vend **sans coordination** ; en
    dessous, elle passe par une vérification **synchrone** auprès d'un coordinateur, qui
    échoue si la région est coupée du coordinateur (intervalles `partitions` par région).
    Renvoie : ventes, survente (excuses pour incohérence), refus pendant une coupure alors
    qu'il restait du stock (excuses pour indisponibilité), nombre de coordinations.
    """
    rng = np.random.default_rng(seed)
    partitions = partitions or {}
    orders = []
    for r in range(n_regions):
        t = np.cumsum(rng.exponential(1.0 / rate, int(rate * horizon * 1.3) + 10))
        orders += [(x, r) for x in t[t < horizon]]
    orders.sort()
    sale_times: list[list[float]] = [[] for _ in range(n_regions)]
    sold_true = 0
    oversold = refused_outage = coordinations = 0
    for t, r in orders:
        known = len(sale_times[r]) + sum(bisect.bisect_right(sale_times[r2], t - repl_delay)
                                         for r2 in range(n_regions) if r2 != r)
        estimate = stock - known
        if estimate > threshold:
            sale_times[r].append(t)
            sold_true += 1
            if sold_true > stock:
                oversold += 1
            continue
        coordinations += 1
        cut = any(a <= t < b for a, b in partitions.get(r, []))
        if cut:
            if sold_true < stock:
                refused_outage += 1
            continue
        if sold_true < stock:
            sale_times[r].append(t)
            sold_true += 1
    return {"sold": min(sold_true, stock), "oversold": oversold, "refused_outage": refused_outage,
            "coordinations": coordinations}


# ---------------------------------------------- confiance et vérification
def scrub_loss_probability(replicas: int, rate: float, interval: float, horizon: float) -> float:
    """Probabilité de perdre un bloc sur `horizon` quand chaque copie se corrompt
    silencieusement au taux `rate` et qu'une vérification tous les `interval` répare les
    copies corrompues à partir d'une copie saine (perte si toutes les copies sont
    corrompues au sein d'un même intervalle). interval >= horizon : jamais vérifié."""
    interval = min(interval, horizon)
    p_int = (-math.expm1(-rate * interval)) ** replicas
    n = horizon / interval
    return -math.expm1(n * math.log1p(-p_int)) if p_int < 1 else 1.0


def simulate_scrubbing(n_blocks: int, replicas: int, rate: float, interval: float, horizon: float,
                       seed: int = 0) -> int:
    """Simulation (par intervalles) du nombre de blocs perdus parmi n_blocks."""
    rng = np.random.default_rng(seed)
    interval = min(interval, horizon)
    n_int = int(round(horizon / interval))
    lost = np.zeros(n_blocks, dtype=bool)
    p = -math.expm1(-rate * interval)
    for _ in range(n_int):
        bad = rng.random((n_blocks, replicas)) < p
        lost |= bad.all(axis=1)
    return int(lost.sum())


class HashChainLog:
    """Journal d'événements chaîné : chaque entrée contient le hachage de la précédente.
    Modifier une entrée passée casse la chaîne à partir de cette entrée."""

    GENESIS = "0" * 64

    def __init__(self):
        self.entries: list[dict] = []

    @staticmethod
    def _digest(prev: str, payload: str) -> str:
        return hashlib.sha256((prev + "|" + payload).encode()).hexdigest()

    def append(self, payload: str) -> str:
        prev = self.entries[-1]["hash"] if self.entries else self.GENESIS
        h = self._digest(prev, payload)
        self.entries.append({"payload": payload, "prev": prev, "hash": h})
        return h

    def head(self) -> str:
        return self.entries[-1]["hash"] if self.entries else self.GENESIS

    def verify(self) -> int | None:
        """Indice de la première entrée incohérente, ou None si la chaîne est intacte."""
        prev = self.GENESIS
        for i, e in enumerate(self.entries):
            if e["prev"] != prev or self._digest(prev, e["payload"]) != e["hash"]:
                return i
            prev = e["hash"]
        return None


def _h(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()


class MerkleTree:
    """Arbre de Merkle à la RFC 6962 (transparence des certificats) : feuilles préfixées
    par 0x00, nœuds internes par 0x01, découpage au plus grand 2^k < n."""

    def __init__(self, leaves):
        self.leaves = [x if isinstance(x, bytes) else str(x).encode() for x in leaves]
        self.hash_count = 0
        self._cache: dict[tuple[int, int], bytes] = {}

    @staticmethod
    def _split(n: int) -> int:
        k = 1
        while k * 2 < n:
            k *= 2
        return k

    def node(self, lo: int, hi: int) -> bytes:
        """Hachage du sous-arbre des feuilles [lo, hi)."""
        key = (lo, hi)
        if key in self._cache:
            return self._cache[key]
        if hi - lo == 1:
            h = _h(b"\x00" + self.leaves[lo])
        else:
            k = self._split(hi - lo)
            h = _h(b"\x01" + self.node(lo, lo + k) + self.node(lo + k, hi))
        self.hash_count += 1
        self._cache[key] = h
        return h

    def root(self) -> bytes:
        return self.node(0, len(self.leaves)) if self.leaves else _h(b"")

    def proof(self, i: int) -> list[tuple[str, bytes]]:
        """Preuve d'inclusion de la feuille i : hachages frères, de la feuille vers la racine."""
        path = []

        def rec(lo, hi):
            if hi - lo == 1:
                return
            k = self._split(hi - lo)
            if i < lo + k:
                rec(lo, lo + k)
                path.append(("droite", self.node(lo + k, hi)))
            else:
                rec(lo + k, hi)
                path.append(("gauche", self.node(lo, lo + k)))
        rec(0, len(self.leaves))
        return path

    @staticmethod
    def verify_proof(leaf, proof, root: bytes) -> bool:
        leaf = leaf if isinstance(leaf, bytes) else str(leaf).encode()
        h = _h(b"\x00" + leaf)
        for side, sib in proof:
            h = _h(b"\x01" + (h + sib if side == "droite" else sib + h))
        return h == root

    def diff(self, other: "MerkleTree") -> tuple[list[int], int]:
        """Feuilles qui diffèrent entre deux arbres de même taille, en descendant seulement
        dans les sous-arbres dont les hachages diffèrent. Renvoie (indices, nombre de
        comparaisons de hachages)."""
        assert len(self.leaves) == len(other.leaves)
        out, comps = [], 0

        def rec(lo, hi):
            nonlocal comps
            comps += 1
            if self.node(lo, hi) == other.node(lo, hi):
                return
            if hi - lo == 1:
                out.append(lo)
                return
            k = self._split(hi - lo)
            rec(lo, lo + k)
            rec(lo + k, hi)
        rec(0, len(self.leaves))
        return out, comps
