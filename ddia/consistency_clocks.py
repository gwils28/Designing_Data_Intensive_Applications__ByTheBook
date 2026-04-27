"""Horloges logiques hybrides et générateurs d'identifiants linéarisables.

Compléments du chapitre « Consistency and Consensus » :

- `HybridLogicalClock` : horloge logique hybride (Kulkarni et al., 2014). L'horodatage
  (l, c) suit l'horloge physique (l) tout en respectant la causalité grâce à un compteur
  (c), comme une horloge de Lamport. Branchable sur `ddia.consistency.run_chat` via
  `extra_clocks={"hlc": hlc_factory}`.
- `TimestampOracle` : générateur d'identifiants linéarisable sur un nœud (l'« oracle
  d'horodatage » de Percolator / TiDB) qui persiste et réplique des **lots**
  d'identifiants plutôt que chaque identifiant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Tuple

__all__ = ["HybridLogicalClock", "hlc_factory", "TimestampOracle"]


class HybridLogicalClock:
    """Horodatage (l, c, nœud) : l suit max(horloge physique, horodatages vus)."""

    def __init__(self, node: str, physical: Callable[[], float], resolution: float = 1e-3):
        self.node = node
        self.physical = physical
        self.res = resolution
        self.l = 0
        self.c = 0

    def _pt(self) -> int:
        return int(self.physical() / self.res)

    def send(self) -> Tuple[int, int, str]:
        """Événement local ou émission : nouvel horodatage."""
        pt = self._pt()
        if pt > self.l:
            self.l, self.c = pt, 0
        else:
            self.c += 1
        return (self.l, self.c, self.node)

    def recv(self, ts: Tuple[int, int, str]) -> Tuple[int, int, str]:
        """Réception d'un horodatage : avance l et c pour rester causal."""
        ml, mc = ts[0], ts[1]
        pt = self._pt()
        new_l = max(self.l, ml, pt)
        if new_l == self.l == ml:
            self.c = max(self.c, mc) + 1
        elif new_l == self.l:
            self.c += 1
        elif new_l == ml:
            self.c = mc + 1
        else:
            self.c = 0
        self.l = new_l
        return (self.l, self.c, self.node)

    def drift_from_physical(self) -> float:
        """Avance de l sur l'horloge physique locale (en unités de temps)."""
        return (self.l - self._pt()) * self.res


def hlc_factory(resolution: float = 1e-3) -> Callable:
    """Fabrique pour `run_chat(extra_clocks=...)` : une HLC sur l'horloge du nœud."""
    def make(node):
        return HybridLogicalClock(node.name, node.local_time, resolution)
    return make


@dataclass
class TimestampOracle:
    """Générateur d'identifiants linéarisable par lots.

    Avant de distribuer les identifiants [hi, hi + batch), l'oracle persiste et réplique
    la borne `hi + batch` (coût `persist_cost`). Après une panne (ou une bascule vers un
    suiveur), il repart de la dernière borne persistée : les identifiants non distribués
    du lot en cours sont **sautés**, jamais réutilisés ni désordonnés.
    """

    batch: int = 1000
    persist_cost: float = 2.0        # ms par écriture durable + réplication
    next_id: int = 1
    persisted_hi: int = 1
    persists: int = 0
    skipped: int = 0
    issued: List[int] = field(default_factory=list)

    def get(self) -> Tuple[int, float]:
        """Renvoie (identifiant, coût de cet appel en ms)."""
        cost = 0.0
        if self.next_id >= self.persisted_hi:
            self.persisted_hi = self.next_id + self.batch
            self.persists += 1
            cost = self.persist_cost
        i = self.next_id
        self.next_id += 1
        self.issued.append(i)
        return i, cost

    def crash_and_recover(self) -> int:
        """Panne puis reprise : repart de la borne persistée. Renvoie le nombre sauté."""
        lost = self.persisted_hi - self.next_id
        self.skipped += lost
        self.next_id = self.persisted_hi
        return lost
