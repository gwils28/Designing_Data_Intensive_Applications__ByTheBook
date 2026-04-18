"""Méthodes formelles et tests aléatoires (notions de la 2e édition).

- `model_check` : petit model checker exhaustif (parcours en largeur de l'espace des
  états, contre-exemple le plus court) ;
- `counter_model`, `lease_model` : deux modèles de protocoles à vérifier ;
- simulation déterministe (DST) du service de verrous et du stockage à jetons de
  clôture de `ddia.distsys` : fautes tirées d'une graine, rejeu exact, recherche d'une
  graine fautive et réduction (« shrinking ») du scénario de fautes.
"""

from __future__ import annotations

import hashlib
import random
from collections import deque
from dataclasses import dataclass
from typing import Callable, Hashable, List, Optional, Sequence, Tuple

from .distsys import FencedStorage, LeaseClient, LockService, token_order_violations
from .sim import Network

# =====================================================================================
# Model checker
# =====================================================================================


@dataclass
class CheckResult:
    ok: bool
    states: int
    transitions: int
    depth: int
    path: Optional[List[Tuple[str, Hashable]]]   # [(action, état atteint), ...]


def model_check(init: Hashable, successors: Callable[[Hashable], Sequence[Tuple[str, Hashable]]],
                invariant: Callable[[Hashable], bool], max_states: int = 2_000_000) -> CheckResult:
    """Explore tous les états accessibles depuis `init` (parcours en largeur).

    Dès qu'un état viole l'invariant, renvoie le chemin d'actions qui y mène : comme le
    parcours est en largeur, c'est un contre-exemple **le plus court**.
    """
    parent = {init: None}
    depth = {init: 0}
    q = deque([init])
    transitions = 0

    def path_to(s):
        out = []
        while parent[s] is not None:
            prev, action = parent[s]
            out.append((action, s))
            s = prev
        return out[::-1]

    if not invariant(init):
        return CheckResult(False, 1, 0, 0, [])
    while q:
        s = q.popleft()
        for action, t in successors(s):
            transitions += 1
            if t in parent:
                continue
            parent[t] = (s, action)
            depth[t] = depth[s] + 1
            if not invariant(t):
                return CheckResult(False, len(parent), transitions, depth[t], path_to(t))
            if len(parent) >= max_states:
                raise RuntimeError("espace d'états trop grand")
            q.append(t)
    return CheckResult(True, len(parent), transitions, max(depth.values()), None)


def counter_model(n_procs: int = 2, atomic: bool = False):
    """n processus incrémentent un compteur partagé par lecture puis écriture.

    État : (pcs, locales, x) ; pc 0 = lire, 1 = écrire, 2 = fini. Invariant : quand tous
    ont fini, x = n_procs. Avec `atomic`, lecture et écriture forment une seule étape.
    """
    init = (tuple([0] * n_procs), tuple([0] * n_procs), 0)

    def succ(s):
        pcs, loc, x = s
        out = []
        for i in range(n_procs):
            if atomic and pcs[i] == 0:
                out.append((f"p{i} incrémente", (pcs[:i] + (2,) + pcs[i + 1:], loc, x + 1)))
            elif pcs[i] == 0:
                out.append((f"p{i} lit x={x}", (pcs[:i] + (1,) + pcs[i + 1:],
                                                 loc[:i] + (x,) + loc[i + 1:], x)))
            elif pcs[i] == 1:
                out.append((f"p{i} écrit x={loc[i] + 1}", (pcs[:i] + (2,) + pcs[i + 1:], loc, loc[i] + 1)))
        return out

    def inv(s):
        pcs, _, x = s
        return not all(p == 2 for p in pcs) or x == n_procs
    return init, succ, inv
