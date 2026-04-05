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


class LWWRegister:
    """Registre « dernier écrivain gagnant » : garde la valeur au plus grand (horodatage, réplique)."""

    def __init__(self, replica: str):
        self.replica = replica
        self.stamp: tuple = (float("-inf"), "")
        self.value: Any = None

    def set(self, value, ts: float) -> None:
        if (ts, self.replica) > self.stamp:
            self.stamp, self.value = (ts, self.replica), value

    def merge(self, other: "LWWRegister") -> None:
        if other.stamp > self.stamp:
            self.stamp, self.value = other.stamp, other.value


# ---------------------------------------------------------------------- texte : RGA
@dataclass
class _Elem:
    id: tuple
    char: str
    deleted: bool = False


class RGA:
    """Séquence répliquée (Replicated Growable Array).

    Identifiant d'un caractère : (compteur de Lamport, réplique). Une insertion est
    ("ins", nouvel_id, id_précédent ou None, caractère), une suppression ("del", id) :
    la suppression laisse une pierre tombale. Les insertions concurrentes au même endroit
    sont ordonnées par identifiant décroissant, de la même façon sur toutes les répliques.
    """

    def __init__(self, replica: str):
        self.replica = replica
        self.clock = 0
        self.elems: list[_Elem] = []
        self.index: dict[tuple, int] = {}
        self.pending: list[tuple] = []
        self.log: list[tuple] = []

    # ---------------------------------------------------------------- lecture
    def text(self) -> str:
        return "".join(e.char for e in self.elems if not e.deleted)

    def visible_ids(self) -> list[tuple]:
        return [e.id for e in self.elems if not e.deleted]

    def _reindex(self) -> None:
        self.index = {e.id: i for i, e in enumerate(self.elems)}

    # ------------------------------------------------------- opérations locales
    def insert(self, pos: int, char: str) -> tuple:
        """Insère `char` à la position visible `pos` ; renvoie l'opération à diffuser."""
        ids = self.visible_ids()
        after = ids[pos - 1] if pos > 0 else None
        self.clock += 1
        op = ("ins", (self.clock, self.replica), after, char)
        self.apply(op)
        return op

    def insert_text(self, pos: int, text: str) -> list[tuple]:
        return [self.insert(pos + i, c) for i, c in enumerate(text)]

    def delete(self, pos: int) -> tuple:
        op = ("del", self.visible_ids()[pos])
        self.apply(op)
        return op

    # ------------------------------------------------------ intégration distante
    def apply(self, op: tuple) -> bool:
        """Intègre une opération (locale ou distante). Idempotent. Une opération dont la
        dépendance n'est pas encore là est mise en attente (renvoie False)."""
        if not self._integrate(op):
            if op not in self.pending:
                self.pending.append(op)
            return False
        progress = True
        while progress and self.pending:
            progress = False
            for p in list(self.pending):
                if self._integrate(p):
                    self.pending.remove(p)
                    progress = True
        return True

    def apply_all(self, ops) -> None:
        for op in list(ops):
            self.apply(op)

    def _integrate(self, op: tuple) -> bool:
        if op[0] == "ins":
            _, nid, after, char = op
            if nid in self.index:
                return True                     # déjà intégrée
            if after is not None and after not in self.index:
                return False
            i = 0 if after is None else self.index[after] + 1
            while i < len(self.elems) and self.elems[i].id > nid:
                i += 1                          # insertions concurrentes plus prioritaires
            self.elems.insert(i, _Elem(nid, char))
            self._reindex()
            self.clock = max(self.clock, nid[0])
        else:
            _, tid = op
            if tid not in self.index:
                return False
            if op in self.log:
                return True                     # déjà intégrée
            self.elems[self.index[tid]].deleted = True
        self.log.append(op)
        return True


# ------------------------------------------------- transformation opérationnelle
NOOP = ("noop",)
