"""Droit à l'effacement dans un journal immuable : réécriture contre crypto-effacement.

Un journal d'événements immuable (event sourcing, chapitre sur les flux) se prête mal à la
suppression des données d'une personne. Deux stratégies :

- **réécriture** : on recopie tout le journal (ou tous les segments concernés) sans les
  événements de la personne ; coût proportionnel à la taille des segments réécrits ;
- **crypto-effacement** (crypto-shredding) : chaque événement est chiffré avec une clé
  propre à la personne ; effacer la personne revient à détruire sa clé. Le journal reste
  intact, mais les octets deviennent illisibles.

Le chiffrement est ici un **jouet pédagogique** (flux XOR dérivé de SHA-256), sans
aucune prétention de sécurité : en production on utiliserait AES-GCM et un service de
gestion de clés.
"""

from __future__ import annotations

import hashlib
import json
import secrets

__all__ = ["toy_encrypt", "toy_decrypt", "ShreddableLog", "rewrite_cost", "segments_touched"]


def _keystream(key: bytes, nonce: bytes, n: int) -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < n:
        out += hashlib.sha256(key + nonce + counter.to_bytes(8, "big")).digest()
        counter += 1
    return bytes(out[:n])


def toy_encrypt(key: bytes, plaintext: bytes, nonce: bytes) -> bytes:
    ks = _keystream(key, nonce, len(plaintext))
    return bytes(a ^ b for a, b in zip(plaintext, ks))


toy_decrypt = toy_encrypt  # XOR : l'opération est sa propre inverse


class ShreddableLog:
    """Journal en ajout seul dont la charge utile est chiffrée par personne."""

    def __init__(self, seed: int | None = None):
        self.entries: list[tuple[int, str, bytes]] = []   # (offset, personne, octets chiffrés)
        self.keys: dict[str, bytes] = {}
        self._rand = secrets.token_bytes if seed is None else _seeded(seed)

    def append(self, person: str, event: dict) -> int:
        key = self.keys.setdefault(person, self._rand(32))
        off = len(self.entries)
        data = json.dumps(event, sort_keys=True).encode()
        self.entries.append((off, person, toy_encrypt(key, data, off.to_bytes(8, "big"))))
        return off

    def read(self, offset: int):
        off, person, blob = self.entries[offset]
        key = self.keys.get(person)
        if key is None:
            return None                                  # clé détruite : illisible
        return json.loads(toy_decrypt(key, blob, off.to_bytes(8, "big")))

    def shred(self, person: str) -> None:
        """Détruit la clé de la personne (un seul petit enregistrement à effacer)."""
        self.keys.pop(person, None)

    def replay(self):
        """Rejoue le journal : les événements dont la clé a été détruite sont ignorés."""
        for off, _, _ in self.entries:
            ev = self.read(off)
            if ev is not None:
                yield ev


def _seeded(seed: int):
    import random

    r = random.Random(seed)
    return lambda n: bytes(r.getrandbits(8) for _ in range(n))


def segments_touched(n_events_person: int, n_segments: int) -> float:
    """Nombre moyen de segments contenant au moins un des événements d'une personne,
    quand ses événements tombent uniformément au hasard dans `n_segments` segments."""
    return n_segments * (1 - (1 - 1 / n_segments) ** n_events_person)


def rewrite_cost(log_bytes: float, n_segments: int, n_events_person: int, requests: int) -> float:
    """Octets réécrits pour `requests` demandes d'effacement traitées une par une, par
    réécriture des segments touchés."""
    seg = log_bytes / n_segments
    return requests * segments_touched(n_events_person, n_segments) * seg
