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
