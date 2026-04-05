"""Résolution automatique des conflits : CRDT et transformation opérationnelle (OT).

Briques du cours sur les moteurs de synchronisation et le logiciel local-first :

- compteurs `GCounter` et `PNCounter` (état fusionné par maximum composante par composante) ;
- ensemble `ORSet` (observed-remove set : une suppression n'efface que les ajouts qu'elle a
  observés, ce qui répare l'anomalie du panier qui ressuscite) ;
- registre `LWWRegister` (dernier écrivain gagnant, pour comparaison) ;
- texte `RGA` (Replicated Growable Array : chaque caractère a un identifiant unique et
  immuable, on insère « après tel identifiant ») ;
- transformation opérationnelle pour l'insertion et la suppression de caractères
  (`ot_transform`, `ot_transform_seq`, `ot_apply`).

Toutes les fusions d'état sont commutatives, associatives et idempotentes : c'est ce qui
garantit la convergence (cohérence à terme forte) quel que soit l'ordre des échanges.

Exemple
-------
>>> a, b = RGA("A"), RGA("B")
>>> ops = a.insert_text(0, "ice")
>>> b.apply_all(ops)
>>> op1 = a.insert(0, "n"); op2 = b.insert(3, "!")
>>> a.apply(op2); b.apply(op1)
True
>>> a.text(), b.text()
('nice!', 'nice!')
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

__all__ = ["GCounter", "PNCounter", "ORSet", "LWWRegister", "RGA", "ot_transform",
           "ot_transform_seq", "ot_apply"]


# ----------------------------------------------------------------------- compteurs
class GCounter:
    """Compteur croissant : une case par réplique, fusion par maximum."""

    def __init__(self, replica: str):
        self.replica = replica
        self.counts: dict[str, int] = {}

    def inc(self, n: int = 1) -> None:
        if n < 0:
            raise ValueError("un GCounter ne fait que croître")
        self.counts[self.replica] = self.counts.get(self.replica, 0) + n

    @property
    def value(self) -> int:
        return sum(self.counts.values())

    def merge(self, other: "GCounter") -> None:
        for r, c in other.counts.items():
            self.counts[r] = max(self.counts.get(r, 0), c)


class PNCounter:
    """Compteur qui monte et descend : deux GCounter (incréments, décréments)."""

    def __init__(self, replica: str):
        self.p, self.n = GCounter(replica), GCounter(replica)

    def inc(self, k: int = 1) -> None:
        self.p.inc(k)

    def dec(self, k: int = 1) -> None:
        self.n.inc(k)

    @property
    def value(self) -> int:
        return self.p.value - self.n.value

    def merge(self, other: "PNCounter") -> None:
        self.p.merge(other.p)
        self.n.merge(other.n)


# ------------------------------------------------------------------------ ensemble
class ORSet:
    """Ensemble à suppression observée : chaque ajout reçoit une étiquette unique ; une
    suppression marque comme mortes les étiquettes **observées** de l'élément. Un ajout
    concurrent d'une suppression (étiquette non observée) survit : « l'ajout gagne »."""

    def __init__(self, replica: str):
        self.replica = replica
        self._seq = itertools.count(1)
        self.adds: dict[Any, set] = {}
        self.removed: set = set()

    def add(self, elem) -> None:
        self.adds.setdefault(elem, set()).add((self.replica, next(self._seq)))

    def remove(self, elem) -> None:
        self.removed |= self.adds.get(elem, set())

    @property
    def value(self) -> set:
        return {e for e, tags in self.adds.items() if tags - self.removed}

    def merge(self, other: "ORSet") -> None:
        for e, tags in other.adds.items():
            self.adds.setdefault(e, set()).update(tags)
        self.removed |= other.removed
        # éviter de réutiliser une étiquette déjà émise par cette réplique
        mine = [s for tags in self.adds.values() for (r, s) in tags if r == self.replica]
        start = max(mine, default=0) + 1
        self._seq = itertools.count(start)
