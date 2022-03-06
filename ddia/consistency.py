"""Cohérence et consensus : outils du chapitre « Consistency and Consensus ».

Le module regroupe quatre familles d'outils, toutes déterministes pour une graine donnée :

1. **Linéarisabilité** : historiques d'opérations (`Op`), vérificateur de linéarisabilité
   par recherche d'un ordre séquentiel valide (algorithme de Wing et Gong, mémoïsé à la
   manière de Lowe, de Knossos ou de Porcupine), tracé d'un historique avec ses points
   de linéarisation (`plot_history`), et un registre répliqué sur quorum simulé sur
   `ddia.sim` (mode « Dynamo » non linéarisable, mode ABD linéarisable).
2. **Ordre et horloges logiques** : horloges de Lamport et vectorielles, une messagerie
   simulée (`run_chat`) qui horodate chaque message de plusieurs façons, et des
   générateurs d'identifiants distribués (`assign_ids`) dont on mesure les inversions.
3. **Consensus** : diffusion à ordre total par séquenceur (`run_registry`) et une
   implémentation de **Raft** sur le simulateur (`RaftNode`, `RaftClient`,
   `raft_cluster`), avec ses vérifications de sûreté.
4. **Coordination** : un modèle de service de coordination à la etcd / ZooKeeper
   (`CoordService` : révisions, baux, clés éphémères et séquentielles, surveillances).
"""

from __future__ import annotations

import bisect
import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .sim import Clock, Network, Node

__all__ = [
    "Op", "LinResult", "register_step", "check_linearizable", "op_label", "plot_history",
    "QuorumReplica", "QuorumClient", "run_quorum_register", "lognormal_latency",
    "LamportClock", "VectorClock", "vc_compare", "run_chat", "causal_order_errors",
    "assign_ids", "inversion_curve",
    "Sequencer", "LogReplica", "run_registry",
    "PaxosAcceptor", "PaxosProposer", "run_paxos",
    "RaftNode", "RaftClient", "raft_cluster", "raft_leaders", "raft_check_safety",
    "majority_availability", "quorum_latency",
    "CoordService",
]

INF = float("inf")


# =====================================================================================
# 1. Historiques et linéarisabilité
# =====================================================================================

@dataclass
class Op:
    """Une opération vue par un client : appel à t_call, réponse à t_return.

    kind : "read" (result = valeur lue), "write" (arg = valeur, result = "ok"),
    "cas" (arg = (ancienne, nouvelle), result = True / False).
    done=False : le client n'a jamais reçu de réponse (délai expiré) ; l'opération a pu
    prendre effet ou non, à n'importe quel instant après t_call.
    """

    client: str
    kind: str
    arg: Any = None
    result: Any = None
    t_call: float = 0.0
    t_return: float = INF
    done: bool = True

    def __post_init__(self):
        if not self.done:
            self.t_return = INF


def register_step(state, op: Op):
    """Modèle séquentiel d'un registre : renvoie (opération valide ?, nouvel état)."""
    if op.kind == "read":
        return (not op.done) or op.result == state, state
    if op.kind == "write":
        return True, op.arg
    if op.kind == "cas":
        old, new = op.arg
        success = state == old
        if not op.done:
            return success, new if success else state
        return success == bool(op.result), new if success else state
    raise ValueError(f"opération inconnue : {op.kind}")


@dataclass
class LinResult:
    """Résultat du vérificateur.

    ok : l'historique est-il linéarisable ? order : indices des opérations dans l'ordre
    de linéarisation trouvé (ou le plus long préfixe valide si ok=False) ;
    points : instants de linéarisation proposés (même longueur que order) ;
    explored : nombre d'états (ensemble linéarisé, valeur du registre) visités.
    """

    ok: bool
    order: List[int]
    points: List[float]
    explored: int

    def blocking(self, history: Sequence[Op]) -> List[int]:
        """Opérations obligatoires que le meilleur préfixe n'a pas réussi à placer (par appel croissant)."""
        placed = set(self.order)
        rest = [i for i, o in enumerate(history) if o.done and i not in placed]
        return sorted(rest, key=lambda i: history[i].t_call)


def _points(history: Sequence[Op], order: Sequence[int]) -> List[float]:
    pts, prev = [], -INF
    for i in order:
        o = history[i]
        p = max(prev, o.t_call)
        hi = o.t_return if o.t_return < INF else p + 1.0
        # on place le point un peu après l'appel pour qu'il soit visible
        p = min(max(p, o.t_call + 0.15 * (hi - o.t_call) if hi > o.t_call else o.t_call), hi)
        p = max(p, prev)
        pts.append(p)
        prev = p
    return pts


def check_linearizable(history: Sequence[Op], init: Any = 0,
                       step: Callable = register_step, max_states: int = 2_000_000) -> LinResult:
    """Cherche un ordre séquentiel des opérations qui respecte le temps réel et le modèle.

    Une opération peut être placée en tête de ce qui reste si aucune opération restante ne
    s'est **terminée avant son appel** (sinon celle-ci doit passer devant). On explore ces
    choix en profondeur, en mémorisant les couples (ensemble déjà placé, état du registre)
    déjà essayés : c'est l'algorithme de Wing et Gong avec la mémoïsation de Lowe.
    Complexité exponentielle dans le pire cas (le problème est NP-complet), mais rapide
    sur les historiques courts ou peu concurrents.
    """
    ops = list(history)
    n = len(ops)
    by_call = sorted(range(n), key=lambda i: ops[i].t_call)
    required = 0
    for i, o in enumerate(ops):
        if o.done:
            required |= 1 << i
    seen = set()
    path: List[int] = []
    best: List[int] = []
    explored = 0

    def dfs(mask: int, state) -> bool:
        nonlocal explored, best
        if mask & required == required:
            return True
        key = (mask, state)
        if key in seen:
            return False
        seen.add(key)
        explored += 1
        if explored > max_states:
            raise RuntimeError("vérification abandonnée : trop d'états")
        min_ret = INF
        for i in range(n):
            if not (mask >> i) & 1 and ops[i].t_return < min_ret:
                min_ret = ops[i].t_return
        for i in by_call:
            if (mask >> i) & 1:
                continue
            if ops[i].t_call > min_ret:
                break
            ok, s2 = step(state, ops[i])
            if ok:
                path.append(i)
                if len(path) > len(best):
                    best = list(path)
                if dfs(mask | (1 << i), s2):
                    return True
                path.pop()
        return False

    ok = dfs(0, init)
    order = list(path) if ok else best
    return LinResult(ok, order, _points(ops, order), explored)


def op_label(op: Op) -> str:
    """Étiquette compacte : w(1), r→0, cas(0→2) ok, avec « ? » si sans réponse."""
    if op.kind == "write":
        s = f"w({op.arg})"
    elif op.kind == "read":
        s = f"r→{op.result}" if op.done else "r→?"
    else:
        old, new = op.arg
        s = f"cas({old}→{new})" + ((" ok" if op.result else " échec") if op.done else " ?")
    return s
