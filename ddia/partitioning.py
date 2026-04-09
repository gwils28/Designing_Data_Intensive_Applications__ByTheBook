"""Sharding (partitionnement) : répartir les données et la charge entre plusieurs nœuds.

Fonctions réutilisables des notebooks du chapitre « Sharding »
(2e édition : chapitre 7 ; 1re édition : chapitre 6 *Partitioning*) :

- **hachage stable** : MD5 pour les chaînes, `mix64` (splitmix64) vectorisé pour les
  entiers, CRC16 et slots de Redis Cluster (avec les hash tags) ;
- **partitionnement par plage de clés** : frontières par quantiles, routage par
  dichotomie, partitionnement dynamique avec scissions (clés ou hachés) ;
- **partitionnement par hachage** : modulo N, nombre fixe de partitions affectées aux
  nœuds, anneau de hachage cohérent à jetons aléatoires (vnodes) ;
- **mesures** : charge par partition, déséquilibre (max / moyenne), fraction de clés
  déplacées lors d'un rééquilibrage ;
- **charges asymétriques** : requêtes en loi de Zipf, salage des clés chaudes ;
- **index secondaires** : index locaux (par document) et globaux (par terme) sur un
  catalogue de voitures d'occasion, latence d'un scatter/gather ;
- **routage des requêtes** : simulation (`ddia.sim`) des trois architectures (n'importe
  quel nœud, couche de routage, client informé) avec un coordinateur qui publie la carte
  des partitions, et diffusion épidémique (gossip) ;
- **opérations** : modèle de défaillance en cascade provoquée par un rééquilibrage
  automatique.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import bisect
import hashlib
import math
import random
from collections import Counter, defaultdict
from typing import Callable, Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .sim import Network, Node

U64 = np.uint64
MASK64 = (1 << 64) - 1

# =====================================================================================
# 1. Hachage stable
# =====================================================================================


def stable_hash(key, bits: int = 32) -> int:
    """Haché stable d'une clé (chaîne, octets ou entier) sur `bits` bits, via MD5.

    Contrairement à `hash()` de Python (randomisé pour les chaînes à chaque lancement de
    l'interpréteur), le résultat est le même dans tous les processus et sur toutes les
    machines : condition indispensable pour décider de la partition d'une clé.
    """
    if isinstance(key, bytes):
        data = key
    else:
        data = str(key).encode("utf-8")
    h = int.from_bytes(hashlib.md5(data).digest()[:8], "big")
    return h >> (64 - bits) if bits < 64 else h


def mix64(x) -> np.ndarray:
    """Hachage vectorisé d'entiers (finaliseur splitmix64), résultat en uint64.

    Très rapide sur de grands tableaux ; des entrées consécutives (0, 1, 2...) donnent
    des sorties d'apparence aléatoire et uniformes sur [0, 2^64).
    """
    z = np.asarray(x).astype(U64) + U64(0x9E3779B97F4A7C15)
    with np.errstate(over="ignore"):
        z = (z ^ (z >> U64(30))) * U64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> U64(27))) * U64(0x94D049BB133111EB)
        z = z ^ (z >> U64(31))
    return z


def hash_unit(keys) -> np.ndarray:
    """Position de chaque clé sur [0, 1) (anneau de hachage normalisé).

    Entiers : `mix64` vectorisé ; autres clés : `stable_hash` sur 52 bits.
    """
    arr = np.asarray(keys)
    if arr.dtype.kind in "iu":
        return (mix64(arr) >> U64(11)).astype(np.float64) / float(1 << 53)
    return np.array([stable_hash(k, 52) / float(1 << 52) for k in arr.ravel()]).reshape(arr.shape)


def redis_crc16(data: bytes) -> int:
    """CRC16-CCITT (variante XMODEM) utilisé par Redis Cluster."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def redis_hash_tag(key: str) -> str:
    """Partie de la clé effectivement hachée : le contenu du premier {...} non vide."""
    s = key.find("{")
    if s >= 0:
        e = key.find("}", s + 1)
        if e > s + 1:
            return key[s + 1:e]
    return key


def redis_slot(key: str, n_slots: int = 16384) -> int:
    """Slot Redis Cluster d'une clé : CRC16(hash tag) mod 16384."""
    return redis_crc16(redis_hash_tag(key).encode("utf-8")) % n_slots


# =====================================================================================
# 2. Mesures de répartition
# =====================================================================================


def shard_loads(shard_ids, n_shards: int, weights=None) -> np.ndarray:
    """Charge (nombre de clés ou somme des poids) reçue par chaque partition."""
    return np.bincount(np.asarray(shard_ids, dtype=np.int64), weights=weights, minlength=n_shards)


def imbalance(loads) -> float:
    """Déséquilibre = charge maximale / charge moyenne (1 = parfaitement équitable)."""
    loads = np.asarray(loads, dtype=float)
    return float(loads.max() / loads.mean())


def moved_fraction(before, after) -> float:
    """Fraction des clés dont le propriétaire change entre deux affectations."""
    before, after = np.asarray(before), np.asarray(after)
    return float(np.mean(before != after))


# =====================================================================================
# 3. Partitionnement par plage de clés
# =====================================================================================

#: fréquence approximative des initiales des entrées d'un dictionnaire anglais
#: (beaucoup de S, C, P ; très peu de X, Y, Z), pour des titres d'encyclopédie réalistes
INITIAL_FREQ = {
    "a": 6.0, "b": 5.5, "c": 9.5, "d": 5.8, "e": 4.0, "f": 4.2, "g": 3.4, "h": 3.9,
    "i": 3.6, "j": 0.9, "k": 1.0, "l": 3.2, "m": 5.6, "n": 2.2, "o": 2.4, "p": 8.3,
    "q": 0.5, "r": 5.4, "s": 11.5, "t": 5.3, "u": 2.6, "v": 1.6, "w": 2.6, "x": 0.1,
    "y": 0.3, "z": 0.3,
}


def synthetic_titles(n: int, seed: int = 0) -> List[str]:
    """`n` titres d'entrées distincts, avec une distribution réaliste des initiales."""
    rng = np.random.default_rng(seed)
    letters = np.array(list(INITIAL_FREQ))
    p = np.array(list(INITIAL_FREQ.values()))
    p /= p.sum()
    first = rng.choice(letters, size=n, p=p)
    alphabet = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    out = set()
    titles = []
    for f in first:
        while True:
            w = f + "".join(rng.choice(alphabet, size=int(rng.integers(3, 9))))
            if w not in out:
                out.add(w)
                titles.append(w)
                break
    return titles


def quantile_boundaries(keys: Sequence, n_shards: int) -> list:
    """Frontières qui découpent l'échantillon trié `keys` en `n_shards` parts égales.

    Renvoie les n_shards - 1 bornes inférieures des partitions 1..n-1 (la partition 0
    commence à moins l'infini).
    """
    s = sorted(keys)
    return [s[(i * len(s)) // n_shards] for i in range(1, n_shards)]


class KeyRangePartitioner:
    """Partitionnement par plage : la partition i contient [bornes[i-1], bornes[i]).

    `boundaries` : liste triée des bornes inférieures des partitions 1..n-1.
    """

    def __init__(self, boundaries: Sequence):
        self.boundaries = list(boundaries)
        if self.boundaries != sorted(self.boundaries):
            raise ValueError("les bornes doivent être triées")

    @property
    def n_shards(self) -> int:
        return len(self.boundaries) + 1

    def shard_of(self, key) -> int:
        return bisect.bisect_right(self.boundaries, key)

    def shards_for_range(self, lo, hi) -> List[int]:
        """Partitions à interroger pour une requête de plage [lo, hi]."""
        return list(range(self.shard_of(lo), self.shard_of(hi) + 1))

    @classmethod
    def from_sample(cls, keys: Sequence, n_shards: int) -> "KeyRangePartitioner":
        return cls(quantile_boundaries(keys, n_shards))


class DynamicRangeSharding:
    """Partitionnement dynamique : une partition qui dépasse `max_keys` est scindée.

    Les clés sont rangées par **position** : la clé elle-même (`hashed=False`,
    partitionnement par plage de clés) ou son haché sur [0, 1) (`hashed=True`,
    partitionnement par plage de hachés). Chaque scission coupe la partition à la clé
    médiane et réécrit toutes ses clés dans deux nouveaux fichiers : on en tient le compte
    dans `rewritten` (coût de la scission).

    `presplit` : bornes initiales (pré-découpage d'une base vide).
    """

    def __init__(self, max_keys: int, hashed: bool = False, presplit: Sequence = ()):
        self.max_keys = max_keys
        self.hashed = hashed
        self.bounds: list = list(presplit)          # bornes inférieures des partitions 1..
        self.parts: List[list] = [[] for _ in range(len(self.bounds) + 1)]
        self.writes: List[int] = [0] * len(self.parts)
        self.n_inserted = 0
        self.rewritten = 0
        self.splits: List[Tuple[int, int]] = []    # (clés insérées au moment, clés réécrites)

    def position(self, key):
        if self.hashed:
            if isinstance(key, (int, np.integer)):
                return float(hash_unit(np.array([key]))[0])
            return stable_hash(key, 52) / float(1 << 52)
        return key

    def shard_of(self, key) -> int:
        return bisect.bisect_right(self.bounds, self.position(key))

    def insert(self, key) -> int:
        """Insère une clé ; renvoie l'indice de la partition qui l'a reçue."""
        pos = self.position(key)
        i = bisect.bisect_right(self.bounds, pos)
        bisect.insort(self.parts[i], pos)
        self.writes[i] += 1
        self.n_inserted += 1
        if len(self.parts[i]) > self.max_keys:
            self._split(i)
        return i

    def _split(self, i: int) -> None:
        part = self.parts[i]
        mid = len(part) // 2
        cut = part[mid]
        if cut == part[0]:  # toutes les clés égales : impossible de couper
            return
        self.bounds.insert(i, cut)
        self.parts[i:i + 1] = [part[:mid], part[mid:]]
        w = self.writes[i]
        self.writes[i:i + 1] = [w // 2, w - w // 2]
        self.rewritten += len(part)
        self.splits.append((self.n_inserted, len(part)))

    @property
    def n_shards(self) -> int:
        return len(self.parts)

    def sizes(self) -> np.ndarray:
        return np.array([len(p) for p in self.parts])

    def reset_writes(self) -> None:
        self.writes = [0] * len(self.parts)


# =====================================================================================
# 4. Partitionnement par hachage et rééquilibrage
# =====================================================================================


def mod_n_assign(keys, n: int) -> np.ndarray:
    """Affectation naïve : nœud = haché(clé) mod N."""
    return (mix64(np.asarray(keys)) % U64(n)).astype(np.int64)


class FixedShardCluster:
    """Nombre fixe de partitions (par ex. 1 000), plusieurs partitions par nœud.

    La partition d'une clé ne change jamais (haché mod `n_shards`) ; seul change le
    placement partitions -> nœuds. Ajouter un nœud lui fait « voler » des partitions aux
    nœuds les plus chargés jusqu'à l'équilibre, sans rien déplacer d'autre.
    `weights` : capacité relative des nœuds (un nœud deux fois plus puissant reçoit deux
    fois plus de partitions).
    """

    def __init__(self, n_shards: int, nodes: Sequence[str], weights: Optional[Dict[str, float]] = None):
        self.n_shards = n_shards
        self.weights: Dict[str, float] = dict(weights or {})
        self.owner: List[str] = [nodes[i % len(nodes)] for i in range(n_shards)]
        for n in nodes:
            self.weights.setdefault(n, 1.0)
        self._rebalance()

    def nodes(self) -> List[str]:
        return list(self.weights)

    def shard_of(self, keys) -> np.ndarray:
        return (mix64(np.asarray(keys)) % U64(self.n_shards)).astype(np.int64)

    def node_of_shard(self) -> np.ndarray:
        idx = {n: i for i, n in enumerate(self.nodes())}
        return np.array([idx[o] for o in self.owner])

    def node_of(self, keys) -> np.ndarray:
        """Indice (dans `nodes()`) du nœud propriétaire de chaque clé."""
        return self.node_of_shard()[self.shard_of(keys)]

    def owner_names(self, keys) -> np.ndarray:
        names = np.array(self.owner)
        return names[self.shard_of(keys)]

    def shards_per_node(self) -> Dict[str, int]:
        c = Counter(self.owner)
        return {n: c.get(n, 0) for n in self.nodes()}

    def _targets(self) -> Dict[str, int]:
        total = sum(self.weights.values())
        raw = {n: self.n_shards * w / total for n, w in self.weights.items()}
        tgt = {n: int(math.floor(v)) for n, v in raw.items()}
        rest = self.n_shards - sum(tgt.values())
        for n in sorted(raw, key=lambda n: raw[n] - tgt[n], reverse=True)[:rest]:
            tgt[n] += 1
        return tgt

    def _rebalance(self) -> List[int]:
        """Déplace le minimum de partitions pour atteindre les cibles ; renvoie les partitions déplacées."""
        tgt = self._targets()
        count = Counter(self.owner)
        excess = {n: count[n] - tgt.get(n, 0) for n in count}
        to_move = []
        for s, o in enumerate(self.owner):
            if excess.get(o, 0) > 0:
                to_move.append(s)
                excess[o] -= 1
        need = [n for n in tgt for _ in range(max(0, tgt[n] - count.get(n, 0)))]
        for s, n in zip(to_move, need):
            self.owner[s] = n
        return to_move

    def add_node(self, name: str, weight: float = 1.0) -> List[int]:
        self.weights[name] = weight
        return self._rebalance()

    def remove_node(self, name: str) -> List[int]:
        del self.weights[name]
        return self._rebalance()


class HashRing:
    """Anneau de hachage cohérent à jetons aléatoires (à la Cassandra / Dynamo).

    Chaque nœud tire `vnodes` jetons uniformes sur [0, 1) ; une clé appartient au nœud du
    premier jeton rencontré en tournant dans le sens croissant à partir de son haché.
    Ajouter un nœud insère ses jetons : il récupère des morceaux des plages voisines,
    sans que les autres clés ne bougent.
    """

    def __init__(self, nodes: Sequence[str] = (), vnodes: int = 1, seed: int = 0):
        self.vnodes = vnodes
        self.rng = np.random.default_rng(seed)
        self.tokens = np.empty(0)
        self.token_owner: List[str] = []
        for n in nodes:
            self.add_node(n)

    def add_node(self, name: str, vnodes: Optional[int] = None) -> None:
        k = vnodes or self.vnodes
        new = self.rng.random(k)
        tokens = np.concatenate([self.tokens, new])
        owners = self.token_owner + [name] * k
        order = np.argsort(tokens)
        self.tokens = tokens[order]
        self.token_owner = [owners[i] for i in order]

    def remove_node(self, name: str) -> None:
        keep = [i for i, o in enumerate(self.token_owner) if o != name]
        self.tokens = self.tokens[keep]
        self.token_owner = [self.token_owner[i] for i in keep]

    def nodes(self) -> List[str]:
        return list(dict.fromkeys(sorted(set(self.token_owner), key=self.token_owner.index)))

    def owner_index(self, positions) -> np.ndarray:
        """Indice du jeton propriétaire de chaque position sur [0, 1)."""
        idx = np.searchsorted(self.tokens, np.asarray(positions), side="left")
        return idx % len(self.tokens)

    def node_of_positions(self, positions) -> np.ndarray:
        owners = np.array(self.token_owner)
        return owners[self.owner_index(positions)]

    def node_of(self, keys) -> np.ndarray:
        return self.node_of_positions(hash_unit(keys))

    def ownership(self) -> Dict[str, float]:
        """Fraction de l'anneau possédée par chaque nœud."""
        t = self.tokens
        arcs = np.diff(np.concatenate([[t[-1] - 1.0], t]))   # arc (jeton précédent, jeton]
        out: Dict[str, float] = defaultdict(float)
        for o, a in zip(self.token_owner, arcs):
            out[o] += float(a)
        return dict(out)


def rendezvous_node(key, nodes: Sequence[str]) -> str:
    """Hachage de rendez-vous (highest random weight) : le nœud de plus grand score h(clé, nœud)."""
    return max(nodes, key=lambda n: stable_hash(f"{key}|{n}", 64))


def jump_hash(key: int, n_buckets: int) -> int:
    """Jump consistent hash (Lamping et Veach, 2014) : seau dans [0, n_buckets).

    Passer de n à n + 1 seaux ne déplace que les clés qui vont dans le nouveau seau
    (une fraction 1 / (n + 1)) ; aucune table à stocker, mais les seaux sont numérotés
    et l'on ne peut retirer que le dernier.
    """
    k = int(key) & MASK64
    b, j = -1, 0
    while j < n_buckets:
        b = j
        k = (k * 2862933555777941757 + 1) & MASK64
        j = int((b + 1) * (float(1 << 31) / float((k >> 33) + 1)))
    return b


# =====================================================================================
# 5. Charges asymétriques et clés chaudes
# =====================================================================================


def zipf_weights(n_keys: int, s: float = 1.0) -> np.ndarray:
    """Probabilités d'une loi de Zipf : la clé de rang r est demandée ∝ 1 / r^s."""
    w = 1.0 / np.arange(1, n_keys + 1) ** s
    return w / w.sum()


def zipf_requests(n_keys: int, n_requests: int, s: float = 1.0, seed: int = 0) -> np.ndarray:
    """Suite de requêtes : indices de clés tirés selon Zipf (la clé 0 est la plus chaude)."""
    rng = np.random.default_rng(seed)
    return rng.choice(n_keys, size=n_requests, p=zipf_weights(n_keys, s))


def salted_key(key, salt: int, width: int = 2) -> str:
    """Clé salée : on ajoute un suffixe (00 à 99 pour width=2) pour éclater une clé chaude."""
    return f"{key}#{salt:0{width}d}"


def salted_variants(key, n_salts: int) -> List[str]:
    """Toutes les sous-clés d'une clé chaude salée (à lire et combiner en lecture)."""
    w = max(2, len(str(n_salts - 1)))
    return [salted_key(key, i, w) for i in range(n_salts)]


def sensor_readings(n_sensors: int, duration: float, rate: float, seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """Mesures de capteurs : (sensor_id, horodatage) triés par temps, `rate` mesures/s par capteur."""
    rng = np.random.default_rng(seed)
    n = rng.poisson(n_sensors * rate * duration)
    t = np.sort(rng.uniform(0, duration, n))
    sid = rng.integers(0, n_sensors, n)
    return sid, t


# =====================================================================================
# 6. Index secondaires locaux et globaux
# =====================================================================================

COLORS_CARS = ["rouge", "noir", "blanc", "gris", "bleu", "argent", "vert", "jaune"]
MAKES_CARS = ["Renault", "Peugeot", "Citroën", "Dacia", "Toyota", "Volkswagen", "Ford", "Fiat",
              "Audi", "BMW", "Honda", "Kia"]


def car_listings(n: int, seed: int = 0) -> List[dict]:
    """Annonces de voitures d'occasion : id, couleur, marque, prix (popularités inégales)."""
    rng = np.random.default_rng(seed)
    pc = zipf_weights(len(COLORS_CARS), 0.8)
    pm = zipf_weights(len(MAKES_CARS), 0.9)
    colors = rng.choice(COLORS_CARS, n, p=pc)
    makes = rng.choice(MAKES_CARS, n, p=pm)
    prices = np.round(rng.lognormal(np.log(12000), 0.5, n), -2)
    return [{"id": i, "color": str(c), "make": str(m), "price": float(p)}
            for i, (c, m, p) in enumerate(zip(colors, makes, prices))]


def doc_shard(doc_id: int, n_shards: int) -> int:
    return int(mix64(np.array([doc_id]))[0] % U64(n_shards))


def term_shard(term: str, n_shards: int) -> int:
    return stable_hash(term) % n_shards


class LocalIndexDB:
    """Index secondaires **locaux** (partitionnés par document).

    Chaque partition indexe seulement ses propres documents. Une écriture touche une seule
    partition ; une requête sur un terme doit interroger toutes les partitions.
    """

    def __init__(self, n_shards: int, fields: Sequence[str] = ("color", "make")):
        self.n = n_shards
        self.fields = list(fields)
        self.docs: List[Dict[int, dict]] = [dict() for _ in range(n_shards)]
        self.index: List[Dict[str, set]] = [defaultdict(set) for _ in range(n_shards)]

    def put(self, doc: dict) -> int:
        """Insère un document ; renvoie le nombre de partitions touchées (toujours 1)."""
        s = doc_shard(doc["id"], self.n)
        self.docs[s][doc["id"]] = doc
        for f in self.fields:
            self.index[s][f"{f}:{doc[f]}"].add(doc["id"])
        return 1

    def query(self, field: str, value) -> Tuple[set, int]:
        """IDs correspondant à field = value ; renvoie (ids, partitions interrogées)."""
        term = f"{field}:{value}"
        ids = set()
        for s in range(self.n):
            ids |= self.index[s].get(term, set())
        return ids, self.n

    def query_and(self, cond1: Tuple[str, object], cond2: Tuple[str, object]) -> Tuple[set, int, int]:
        """Conjonction : chaque partition intersecte localement ; (ids, partitions, IDs transférés)."""
        t1, t2 = f"{cond1[0]}:{cond1[1]}", f"{cond2[0]}:{cond2[1]}"
        ids = set()
        for s in range(self.n):
            ids |= self.index[s].get(t1, set()) & self.index[s].get(t2, set())
        return ids, self.n, len(ids)


class GlobalIndexDB:
    """Index secondaire **global** (partitionné par terme).

    Les documents sont répartis par ID ; l'index, lui, est réparti par terme : toutes les
    entrées « color:rouge » sont sur la même partition d'index. Une requête sur un terme
    lit une seule partition d'index ; une écriture touche la partition du document et
    celles de chacun de ses termes.
    """

    def __init__(self, n_shards: int, fields: Sequence[str] = ("color", "make")):
        self.n = n_shards
        self.fields = list(fields)
        self.docs: List[Dict[int, dict]] = [dict() for _ in range(n_shards)]
        self.index: List[Dict[str, set]] = [defaultdict(set) for _ in range(n_shards)]

    def put(self, doc: dict) -> int:
        """Insère un document ; renvoie le nombre de partitions distinctes touchées."""
        s = doc_shard(doc["id"], self.n)
        self.docs[s][doc["id"]] = doc
        touched = {s}
        for f in self.fields:
            term = f"{f}:{doc[f]}"
            ts = term_shard(term, self.n)
            self.index[ts][term].add(doc["id"])
            touched.add(ts)
        return len(touched)

    def query(self, field: str, value) -> Tuple[set, int]:
        term = f"{field}:{value}"
        return set(self.index[term_shard(term, self.n)].get(term, set())), 1

    def query_and(self, cond1, cond2) -> Tuple[set, int, int]:
        """Conjonction : les deux listes de postings sont rapatriées et intersectées.

        Renvoie (ids, partitions d'index lues, IDs transférés sur le réseau)."""
        t1, t2 = f"{cond1[0]}:{cond1[1]}", f"{cond2[0]}:{cond2[1]}"
        s1, s2 = term_shard(t1, self.n), term_shard(t2, self.n)
        p1 = self.index[s1].get(t1, set())
        p2 = self.index[s2].get(t2, set())
        moved = (len(p1) + len(p2)) if s1 != s2 else min(len(p1), len(p2))
        return p1 & p2, len({s1, s2}), moved


def scatter_gather_latency(n_shards: int, n_queries: int, sampler: Callable, seed: int = 0) -> np.ndarray:
    """Latence de requêtes envoyées en parallèle à `n_shards` partitions.

    `sampler(rng, size)` tire des temps de réponse d'une partition ; la requête complète
    attend la plus lente (maximum sur les partitions).
    """
    rng = np.random.default_rng(seed)
    t = sampler(rng, (n_queries, n_shards))
    return t.max(axis=1)


def lognormal_sampler(median: float = 5.0, sigma: float = 0.6, slow_p: float = 0.01,
                      slow_factor: float = 20.0) -> Callable:
    """Temps de réponse d'une partition : log-normal + 1 % de réponses très lentes (GC, disque)."""
    def sample(rng, size):
        t = rng.lognormal(np.log(median), sigma, size)
        slow = rng.random(size) < slow_p
        return np.where(slow, t * slow_factor, t)
    return sample


# =====================================================================================
# 7. Routage des requêtes (simulation sur ddia.sim)
# =====================================================================================


class ShardServer(Node):
    """Nœud de stockage : possède des partitions et une vue (peut-être périmée) de la carte.

    `forward=True` : une requête mal adressée est relayée au propriétaire (approche 1, tout
    nœud accepte les requêtes) ; sinon le nœud répond `moved` avec le propriétaire qu'il
    connaît (redirection à la Redis Cluster).
    """

    def __init__(self, name: str, owned: Iterable[int], view: Dict[int, str], forward: bool = False):
        super().__init__(name)
        self.owned = set(owned)
        self.view = dict(view)
        self.version = 0
        self.forward = forward
        self.relay: Dict[int, str] = {}
        self.served = 0

    def on_message(self, src, msg):
        kind = msg[0]
        if kind == "req":
            _, rid, shard = msg
            if shard in self.owned:
                self.served += 1
                self.send(src, ("resp", rid, self.name))
            elif self.forward and self.view[shard] != self.name:
                self.relay[rid] = src
                self.send(self.view[shard], ("req", rid, shard))
            else:
                owner = self.view[shard]
                if owner == self.name:     # vue en retard sur la passation : on ne sait pas
                    owner = None
                self.send(src, ("moved", rid, shard, owner))
        elif kind == "resp" and msg[1] in self.relay:
            self.send(self.relay.pop(msg[1]), msg)
        elif kind == "moved" and msg[1] in self.relay:
            # le nœud relayé ne possède plus la partition : on met à jour et on relaie encore
            _, rid, shard, owner = msg
            if owner:
                self.view[shard] = owner
                self.send(owner, ("req", rid, shard))
            else:
                self.send(self.relay.pop(rid), msg)
        elif kind == "map":
            _, version, mapping = msg
            if version > self.version:
                self.version, self.view = version, dict(mapping)


class RoutingTier(Node):
    """Couche de routage : relaie chaque requête au propriétaire d'après sa vue de la carte."""

    def __init__(self, name: str, view: Dict[int, str]):
        super().__init__(name)
        self.view = dict(view)
        self.version = 0
        self.pending: Dict[int, str] = {}

    def on_message(self, src, msg):
        kind = msg[0]
        if kind == "req":
            _, rid, shard = msg
            self.pending[rid] = src
            self.send(self.view[shard], ("req", rid, shard))
        elif kind == "resp" and msg[1] in self.pending:
            self.send(self.pending.pop(msg[1]), msg)
        elif kind == "moved" and msg[1] in self.pending:
            _, rid, shard, owner = msg
            if owner:
                self.view[shard] = owner
            self.set_timer(0.0 if owner else 1.0, self._retry, rid, shard)
        elif kind == "map":
            _, version, mapping = msg
            if version > self.version:
                self.version, self.view = version, dict(mapping)

    def _retry(self, rid, shard):
        self.send(self.view[shard], ("req", rid, shard))


class ShardClient(Node):
    """Client : envoie des requêtes selon le mode et mesure leur temps de réponse.

    mode = "any" (un nœud au hasard, qui relaie), "tier" (couche de routage) ou
    "aware" (le client connaît la carte et va directement au propriétaire).
    """

    def __init__(self, name: str, mode: str, servers: Sequence[str], view: Dict[int, str],
                 n_shards: int, n_requests: int, interval: float, seed: int = 0):
        super().__init__(name)
        self.mode = mode
        self.servers = list(servers)
        self.view = dict(view)
        self.version = 0
        self.n_shards = n_shards
        self.n_requests = n_requests
        self.interval = interval
        self.rng = random.Random(seed)
        self.start: Dict[int, float] = {}
        self.shard_of_req: Dict[int, int] = {}
        self.redirects: Counter = Counter()
        self.done: Dict[int, Tuple[float, str]] = {}

    def on_start(self):
        t = 0.0
        for rid in range(self.n_requests):
            t += self.rng.expovariate(1.0 / self.interval)
            self.net.at(t, self._issue, rid, self.rng.randrange(self.n_shards))

    def _target(self, shard):
        if self.mode == "any":
            return self.rng.choice(self.servers)
        if self.mode == "tier":
            return "routeur"
        return self.view[shard]

    def _issue(self, rid, shard):
        self.start[rid] = self.now
        self.shard_of_req[rid] = shard
        self.send(self._target(shard), ("req", rid, shard))

    def on_message(self, src, msg):
        kind = msg[0]
        if kind == "resp":
            self.done[msg[1]] = (self.now, msg[2])
        elif kind == "moved":
            _, rid, shard, owner = msg
            self.redirects[rid] += 1
            if owner:
                self.view[shard] = owner
                self.send(owner, ("req", rid, shard))
            else:
                self.set_timer(1.0, lambda: self.send(self._target(shard), ("req", rid, shard)))
        elif kind == "map":
            _, version, mapping = msg
            if version > self.version:
                self.version, self.view = version, dict(mapping)


class Coordinator(Node):
    """Service de coordination (rôle de ZooKeeper / etcd) : détient la carte de référence.

    `move(shard, new_owner)` : passation instantanée entre les deux nœuds de stockage
    (l'ancien cesse de servir, le nouveau commence, et tous deux connaissent aussitôt le
    nouveau propriétaire), puis notification de la nouvelle
    version de la carte à tous les abonnés (messages soumis aux délais du réseau).
    """

    def __init__(self, name: str, mapping: Dict[int, str], subscribers: Sequence[str]):
        super().__init__(name)
        self.mapping = dict(mapping)
        self.version = 0
        self.subscribers = list(subscribers)

    def move(self, shard: int, new_owner: str) -> None:
        old = self.mapping[shard]
        self.net[old].owned.discard(shard)
        self.net[new_owner].owned.add(shard)
        # les deux nœuds qui participent à la passation connaissent le nouveau propriétaire
        self.net[old].view[shard] = new_owner
        self.net[new_owner].view[shard] = new_owner
        self.mapping[shard] = new_owner
        self.version += 1
        for s in self.subscribers:
            self.send(s, ("map", self.version, dict(self.mapping)))


def routing_experiment(mode: str, n_servers: int = 4, n_shards: int = 16, n_requests: int = 400,
                       interval: float = 0.25, moves: Sequence[Tuple[float, int, str]] = (),
                       latency=(0.4, 0.6), notify=("servers", "router"), seed: int = 0):
    """Simule le routage de `n_requests` requêtes ; renvoie (enregistrements, réseau).

    moves : déplacements (instant, partition, nouveau propriétaire) décidés par le
    coordinateur. notify : abonnés notifiés par le coordinateur ("servers", "router",
    "client"). Chaque enregistrement : rid, t0, latence, sauts (messages réseau échangés
    pour la requête), redirections, nœud qui a servi.
    """
    servers = [f"n{i}" for i in range(n_servers)]
    mapping = {s: servers[s % n_servers] for s in range(n_shards)}
    net = Network(seed=seed, latency=latency)
    for name in servers:
        owned = [s for s, o in mapping.items() if o == name]
        net.add(ShardServer(name, owned, mapping, forward=(mode == "any")))
    if mode == "tier":
        net.add(RoutingTier("routeur", mapping))
    client = net.add(ShardClient("client", mode, servers, mapping, n_shards, n_requests, interval, seed))
    subs = []
    if "servers" in notify:
        subs += servers
    if "router" in notify and mode == "tier":
        subs.append("routeur")
    if "client" in notify:
        subs.append("client")
    coord = net.add(Coordinator("coord", mapping, subs))
    for t, shard, owner in moves:
        net.at(t, coord.move, shard, owner)
    net.run()
    hops = Counter(d.msg[1] for d in net.trace if d.msg[0] in ("req", "resp", "moved"))
    recs = []
    for rid, t0 in client.start.items():
        if rid in client.done:
            t1, by = client.done[rid]
            recs.append({"rid": rid, "t0": t0, "latence": t1 - t0, "sauts": hops[rid],
                         "redirections": client.redirects[rid], "servi_par": by,
                         "partition": client.shard_of_req[rid]})
    return recs, net


def gossip_rounds(n_nodes: int, fanout: int = 1, seed: int = 0) -> List[int]:
    """Diffusion épidémique (push) : nombre de nœuds informés après chaque tour.

    À chaque tour, chaque nœud informé contacte `fanout` nœuds tirés au hasard. Le nombre
    de tours pour informer tout le monde croît comme log2(n) + ln(n).
    """
    rng = np.random.default_rng(seed)
    informed = np.zeros(n_nodes, dtype=bool)
    informed[0] = True
    hist = [1]
    while not informed.all():
        src = np.flatnonzero(informed)
        targets = rng.integers(0, n_nodes, size=src.size * fanout)
        informed[targets] = True
        hist.append(int(informed.sum()))
        if len(hist) > 10 * (int(math.log2(n_nodes)) + 10):
            break
    return hist


# =====================================================================================
# 8. Opérations : rééquilibrage automatique et défaillance en cascade
# =====================================================================================


def rebalancing_cascade(n_nodes: int = 10, base_util: float = 0.70, spread: float = 0.10,
                        spike_factor: float = 1.45, spike: Tuple[int, int] = (10, 25),
                        timeout: float = 10.0, patience: int = 2, migration_cost: float = 0.04,
                        migration_steps: int = 15, auto: bool = True,
                        steps: int = 120) -> Dict[str, np.ndarray]:
    """Modèle à pas de temps (1 pas = 1 minute) d'un cluster de `n_nodes` nœuds.

    Le nœud i a une charge de base comprise entre base_util - spread et base_util + spread
    (les partitions ne sont jamais parfaitement égales) ; son temps de réponse relatif vaut
    1 / (1 - rho) (file M/M/1, en multiples du temps de service), infini si rho >= 1.
    Le nœud 0 subit un pic temporaire (`spike_factor` × sa charge pendant `spike`).

    Avec `auto=True`, un nœud dont le temps de réponse dépasse `timeout` pendant `patience`
    pas est déclaré mort (à tort : il est seulement lent). Sa charge est répartie sur les
    survivants, et le transfert de ses partitions ajoute `migration_cost` d'utilisation à
    chaque survivant pendant `migration_steps` pas. Avec `auto=False`, personne n'est
    exclu : le pic passe. Renvoie les séries : nœuds actifs, rho max et moyen, temps de
    réponse max (plafonné à 100).
    """
    alive = np.ones(n_nodes, dtype=bool)
    load = base_util + np.linspace(-spread, spread, n_nodes)[::-1]
    strikes = np.zeros(n_nodes, dtype=int)
    migrations: List[int] = []   # pas de fin de chaque transfert en cours
    out: Dict[str, list] = {"actifs": [], "rho_max": [], "rt_max": [], "rho_moyen": [], "exclusions": []}
    for t in range(steps):
        extra = migration_cost * sum(1 for end in migrations if t < end)
        rho = load + extra
        if spike[0] <= t < spike[1]:
            rho[0] = load[0] * spike_factor + extra
        rt = np.where(rho < 1, 1.0 / np.maximum(1e-9, 1 - rho), np.inf)
        excluded = []
        if auto and alive.any():
            strikes = np.where(alive & (rt > timeout), strikes + 1, 0)
            for d in np.flatnonzero(strikes >= patience):
                alive[d] = False
                strikes[d] = 0
                excluded.append(int(d))
                survivors = np.flatnonzero(alive)
                if survivors.size:
                    load[survivors] += load[d] / survivors.size
                    migrations.append(t + migration_steps)
                load[d] = 0.0
        a = alive.copy()
        out["actifs"].append(int(a.sum()))
        out["exclusions"].append(excluded)
        out["rho_max"].append(float(rho[a].max()) if a.any() else np.nan)
        out["rho_moyen"].append(float(rho[a].mean()) if a.any() else np.nan)
        out["rt_max"].append(float(min(100.0, rt[a].max())) if a.any() else np.nan)
    res = {k: np.array(v) for k, v in out.items() if k != "exclusions"}
    res["exclusions"] = out["exclusions"]
    return res
