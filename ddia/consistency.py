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


def plot_history(history: Sequence[Op], title: str = "", subtitle: str = "", result: LinResult | None = None,
                 clients: Sequence[str] | None = None, t_max: float | None = None, width: float = 11,
                 highlight: Sequence[int] = (), show_points: bool = True, fontsize: float = 8.5):
    """Dessine un historique (une ligne par client) et, si `result` est fourni et valide,
    les points de linéarisation reliés dans l'ordre trouvé. Renvoie la toile SpaceTime."""
    from .plotting import GREEN, INDIGO, ROSE, SLATE, TEAL, VIOLET, AMBER
    from .timelines import SpaceTime

    clients = list(clients) if clients else list(dict.fromkeys(o.client for o in history))
    finite = [o.t_return for o in history if o.t_return < INF] + [o.t_call for o in history]
    t_max = t_max or max(finite) * 1.05
    st = SpaceTime(clients, t_max=t_max, title=title, subtitle=subtitle, width=width,
                   lane_height=1.0)
    colors = {"write": INDIGO, "read": TEAL, "cas": VIOLET}
    for i, o in enumerate(history):
        c = ROSE if i in highlight else colors[o.kind]
        t1 = o.t_return if o.t_return < INF else t_max
        st.op(o.client, o.t_call, t1, op_label(o), color=c, fontsize=fontsize)
        if not o.done:
            st.note(t_max * 0.995, o.client, "pas de réponse", dy=-0.28, fontsize=7.5)
    if result is not None and show_points and result.order:
        xs = result.points
        ys = [st.y[history[i].client] for i in result.order]
        col = GREEN if result.ok else AMBER
        st.ax.plot(xs, ys, color=col, lw=1.3, zorder=5, alpha=0.9)
        st.ax.plot(xs, ys, "o", color=col, markersize=5, zorder=6, markeredgecolor="white")
        for k, (x, y) in enumerate(zip(xs, ys)):
            st.ax.text(x, y - 0.2, str(k + 1), fontsize=7, color=col, ha="center", va="top", zorder=7)
    return st


# ------------------------------------------------------------- registre sur quorum

def lognormal_latency(base: float = 1.0, sigma: float = 0.8, slow: Dict[Tuple[str, str], float] | None = None):
    """Fonction de latence pour `Network` : base * LogNormale(0, sigma), éventuellement
    multipliée pour certains liens lents ({(src, dst): facteur})."""
    slow = slow or {}

    def f(rng, src, dst):
        return base * rng.lognormvariate(0.0, sigma) * slow.get((src, dst), 1.0)
    return f


class QuorumReplica(Node):
    """Réplique d'un registre : garde (horodatage, valeur) et applique la règle LWW."""

    def __init__(self, name: str, init: Any = 0):
        super().__init__(name)
        self.ts: Tuple = (0, "")
        self.value = init

    def on_message(self, src, msg):
        if msg[0] == "get":
            self.send(src, ("val", msg[1], self.ts, self.value))
        elif msg[0] == "set":
            _, rid, ts, v = msg
            if ts > self.ts:
                self.ts, self.value = ts, v
            self.send(src, ("ack", rid))


class QuorumClient(Node):
    """Client d'un registre répliqué sans leader (n répliques, quorums w et r).

    mode="dynamo" : écriture = envoi à toutes les répliques avec un horodatage local,
    attente de w accusés ; lecture = attente de r réponses, valeur la plus récente.
    mode="abd" (Attiya, Bar-Noy, Dolev) : l'écrivain lit d'abord l'horodatage maximal sur
    un quorum ; le lecteur réécrit la valeur lue sur un quorum (réparation **synchrone**)
    avant de répondre. C'est linéarisable, au prix d'un second aller-retour.
    """

    def __init__(self, name: str, replicas: Sequence[str], w: int, r: int, mode: str = "dynamo",
                 script: Sequence[Tuple] = (), history: List[Op] | None = None):
        super().__init__(name)
        self.replicas, self.w, self.r, self.mode = list(replicas), w, r, mode
        self.script = list(script)       # [(délai avant l'opération, "read"/"write", valeur)]
        self.history = history if history is not None else []
        self.counter = 0
        self.rid = 0
        self.cur: Optional[dict] = None

    def on_start(self):
        self._next()

    def _next(self):
        if not self.script:
            return
        delay, kind, value = self.script.pop(0)
        self.set_timer(delay, self._begin, kind, value)

    def _begin(self, kind, value):
        self.rid += 1
        op = Op(self.name, kind, value if kind == "write" else None, t_call=self.now)
        self.cur = dict(op=op, rid=self.rid, phase=1, got=[], acks=0)
        if kind == "write" and self.mode == "dynamo":
            self.counter += 1
            ts = (self.counter, self.name)
            self.cur.update(phase=2, ts=ts, v=value)
            for rep in self.replicas:
                self.send(rep, ("set", self.rid, ts, value))
        else:
            for rep in self.replicas:
                self.send(rep, ("get", self.rid))

    def _finish(self, result):
        op = self.cur["op"]
        op.result, op.t_return = result, self.now
        self.history.append(op)
        self.cur = None
        self._next()

    def on_message(self, src, msg):
        c = self.cur
        if c is None or msg[1] != c["rid"]:
            return
        if msg[0] == "val" and c["phase"] == 1:
            c["got"].append((msg[2], msg[3]))
            if len(c["got"]) == self.r:
                ts, v = max(c["got"], key=lambda x: x[0])
                op = c["op"]
                if op.kind == "read" and self.mode == "dynamo":
                    self._finish(v)
                    return
                if op.kind == "read":         # ABD : réparation synchrone avant de répondre
                    c.update(phase=2, ts=ts, v=v, result=v)
                else:                          # ABD : écriture avec un horodatage plus grand
                    ts = (ts[0] + 1, self.name)
                    c.update(phase=2, ts=ts, v=op.arg, result="ok")
                self.rid += 1
                c["rid"] = self.rid
                for rep in self.replicas:
                    self.send(rep, ("set", self.rid, c["ts"], c["v"]))
        elif msg[0] == "ack" and c["phase"] == 2:
            c["acks"] += 1
            if c["acks"] == self.w:
                self._finish(c.get("result", "ok"))
