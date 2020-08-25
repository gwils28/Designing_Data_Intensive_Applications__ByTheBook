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
