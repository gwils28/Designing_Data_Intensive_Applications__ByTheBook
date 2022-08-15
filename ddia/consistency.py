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


def run_quorum_register(n: int = 3, w: int = 2, r: int = 2, mode: str = "dynamo", n_readers: int = 2,
                        n_ops: int = 6, sigma: float = 0.8, seed: int = 0, think: float = 1.0,
                        n_writers: int = 1, until: float = 1e4):
    """Simule un registre répliqué sur quorum ; renvoie (historique trié par appel, réseau).

    Les écrivains écrivent des valeurs entières distinctes (1, 2, 3... pour le premier,
    101, 102... pour le second), les lecteurs lisent en boucle.
    """
    rng = random.Random(seed)
    net = Network(seed=seed, latency=lognormal_latency(1.0, sigma))
    reps = [f"R{i + 1}" for i in range(n)]
    for name in reps:
        net.add(QuorumReplica(name))
    history: List[Op] = []
    for k in range(n_writers):
        script = [(rng.expovariate(1 / think), "write", 100 * k + j + 1) for j in range(n_ops)]
        net.add(QuorumClient(f"W{k + 1}" if n_writers > 1 else "W", reps, w, r, mode, script, history))
    for k in range(n_readers):
        script = [(rng.expovariate(1 / think), "read", None) for _ in range(n_ops + 2)]
        net.add(QuorumClient(chr(ord("A") + k), reps, w, r, mode, script, history))
    net.run(until=until)
    history.sort(key=lambda o: o.t_call)
    return history, net


# =====================================================================================
# 2. Ordre, horloges logiques et identifiants
# =====================================================================================

class LamportClock:
    """Horloge de Lamport : horodatage = (compteur, identifiant du nœud)."""

    def __init__(self, node: str):
        self.node = node
        self.counter = 0

    def tick(self) -> Tuple[int, str]:
        """Nouvel événement local (ou émission d'un message)."""
        self.counter += 1
        return (self.counter, self.node)

    def observe(self, ts: Tuple[int, str]) -> None:
        """Réception d'un horodatage : le compteur rattrape au moins sa valeur."""
        self.counter = max(self.counter, ts[0])


class VectorClock:
    """Horloge vectorielle : un compteur par nœud."""

    def __init__(self, node: str, nodes: Sequence[str]):
        self.node = node
        self.v = {x: 0 for x in nodes}

    def tick(self) -> Dict[str, int]:
        self.v[self.node] += 1
        return dict(self.v)

    def observe(self, other: Dict[str, int]) -> None:
        for k, x in other.items():
            self.v[k] = max(self.v.get(k, 0), x)


def vc_compare(a: Dict[str, int], b: Dict[str, int]) -> str:
    """'<' (a avant b), '>' (b avant a), '=' ou '||' (concurrents)."""
    keys = set(a) | set(b)
    le = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    ge = all(a.get(k, 0) >= b.get(k, 0) for k in keys)
    if le and ge:
        return "="
    if le:
        return "<"
    if ge:
        return ">"
    return "||"


class _ChatUser(Node):
    def __init__(self, name, users, rate, reply_prob, think, events, rng, extra):
        super().__init__(name)
        self.lamport = LamportClock(name)
        self.vc = VectorClock(name, users)
        self.rate, self.reply_prob, self.think = rate, reply_prob, think
        self.events, self.rng = events, rng
        self.extra = {k: f(self) for k, f in extra.items()}

    def on_start(self):
        self.set_timer(self.rng.expovariate(self.rate), self._spontaneous)

    def _spontaneous(self):
        self._post(None)
        self.set_timer(self.rng.expovariate(self.rate), self._spontaneous)

    def _post(self, reply_to):
        ev = dict(id=len(self.events), user=self.name, t_real=self.now, t_phys=self.local_time(),
                  lamport=self.lamport.tick(), vc=self.vc.tick(), reply_to=reply_to)
        for k, clk in self.extra.items():
            ev[k] = clk.send()
        self.events.append(ev)
        self.broadcast(("msg", ev))

    def on_message(self, src, msg):
        ev = msg[1]
        self.lamport.observe(ev["lamport"])
        self.vc.observe(ev["vc"])
        for k, clk in self.extra.items():
            clk.recv(ev[k])
        if self.rng.random() < self.reply_prob:
            self.set_timer(self.rng.expovariate(1 / self.think), self._post, ev["id"])


def run_chat(n_users: int = 4, duration: float = 60.0, rate: float = 0.05, reply_prob: float = 0.3,
             think: float = 2.0, offsets: Sequence[float] | None = None, latency=(0.5, 3.0),
             seed: int = 0, extra_clocks: Dict[str, Callable] | None = None) -> List[dict]:
    """Messagerie de groupe simulée : chaque utilisateur poste spontanément (Poisson de taux
    `rate`) et répond parfois aux messages reçus. Chaque message est horodaté par l'horloge
    physique du nœud (décalée de `offsets`), une horloge de Lamport et une horloge
    vectorielle (la vérité sur la causalité). `extra_clocks` : {nom: fabrique(nœud)} d'objets
    ayant send() et recv(ts), pour comparer d'autres horloges."""
    users = [f"u{i + 1}" for i in range(n_users)]
    offsets = list(offsets) if offsets is not None else [0.0] * n_users
    rng = random.Random(seed)
    net = Network(seed=seed, latency=latency)
    events: List[dict] = []
    for u, off in zip(users, offsets):
        net.add(_ChatUser(u, users, rate, reply_prob, think, events, random.Random(rng.random()),
                          extra_clocks or {}))
        net[u].clock = Clock(offset=off)
    net.run(until=duration)
    return events


def causal_order_errors(events: Sequence[dict], key: str) -> dict:
    """Compare l'ordre donné par events[i][key] à la causalité (horloges vectorielles).

    Renvoie le nombre de paires causalement liées, la part d'entre elles que la clé
    ordonne à l'envers, et la part de paires concurrentes."""
    causal = wrong = conc = total = 0
    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            a, b = events[i], events[j]
            rel = vc_compare(a["vc"], b["vc"])
            total += 1
            if rel == "||":
                conc += 1
                continue
            causal += 1
            first, second = (a, b) if rel == "<" else (b, a)
            if not first[key] < second[key]:
                wrong += 1
    return dict(paires=total, causales=causal, inversées=wrong,
                taux_inversion=wrong / causal if causal else 0.0, part_concurrentes=conc / total if total else 0.0)


def assign_ids(scheme: str, times: np.ndarray, nodes: np.ndarray, n_nodes: int, block: int = 100,
               offsets: Sequence[float] | None = None, tick: float = 1e-3) -> np.ndarray:
    """Attribue un identifiant à chaque requête (instants réels `times` croissants, nœud
    traitant `nodes`) selon un schéma :

    - "central" : un compteur unique (référence linéarisable) ;
    - "sharded" : le nœud k distribue k, k + n, k + 2n... (pair / impair pour n = 2) ;
    - "blocks" : chaque nœud réserve des blocs de `block` identifiants consécutifs ;
    - "wallclock" : (horloge du nœud arrondie à `tick`, nœud, séquence), horloge décalée
      de `offsets[k]`.
    """
    n = len(times)
    ids = np.empty(n, dtype=np.int64)
    if scheme == "central":
        ids[:] = np.arange(1, n + 1)
    elif scheme == "sharded":
        cnt = np.zeros(n_nodes, dtype=np.int64)
        for i, k in enumerate(nodes):
            ids[i] = cnt[k] * n_nodes + k + 1
            cnt[k] += 1
    elif scheme == "blocks":
        nxt = 1
        cur = np.zeros(n_nodes, dtype=np.int64)
        end = np.zeros(n_nodes, dtype=np.int64)
        for i, k in enumerate(nodes):
            if cur[k] >= end[k]:
                cur[k], end[k] = nxt, nxt + block
                nxt += block
            ids[i] = cur[k]
            cur[k] += 1
    elif scheme == "wallclock":
        offsets = np.zeros(n_nodes) if offsets is None else np.asarray(offsets, dtype=float)
        last = {}
        for i, (t, k) in enumerate(zip(times, nodes)):
            ms = int(math.floor((t + offsets[k]) / tick))
            seq = last.get((k, ms), -1) + 1
            last[(k, ms)] = seq
            ids[i] = (ms * 64 + int(k)) * 4096 + seq
    else:
        raise ValueError(scheme)
    return ids


def inversion_curve(times: np.ndarray, ids: np.ndarray, gaps: Sequence[float], n_pairs: int = 4000,
                    seed: int = 0) -> np.ndarray:
    """Pour chaque écart g : probabilité qu'une requête commencée g après une autre reçoive
    un identifiant **plus petit** (violation de l'ordre réel)."""
    rng = np.random.default_rng(seed)
    out = []
    n = len(times)
    for g in gaps:
        i = rng.integers(0, n, n_pairs)
        j = np.searchsorted(times, times[i] + g, side="left")
        ok = j < n
        out.append(float(np.mean(ids[j[ok]] < ids[i[ok]])) if ok.any() else np.nan)
    return np.array(out)


# =====================================================================================
# 3. Diffusion à ordre total et Raft
# =====================================================================================

class Sequencer(Node):
    """Séquenceur : numérote les demandes d'ajout et les diffuse à toutes les répliques."""

    def __init__(self, name: str, replicas: Sequence[str]):
        super().__init__(name)
        self.replicas = list(replicas)
        self.seq = 0

    def on_message(self, src, msg):
        if msg[0] == "append":
            self.seq += 1
            for r in self.replicas:
                self.send(r, ("deliver", self.seq, msg[1]))


class LogReplica(Node):
    """Réplique d'un registre de noms d'utilisateur alimentée par un journal.

    total_order=True : les entrées passent par le séquenceur et sont appliquées dans
    l'ordre des numéros (tampon pour les arrivées désordonnées) ; False : chaque réplique
    applique les demandes dans leur ordre d'arrivée (diffusion naïve)."""

    def __init__(self, name: str, total_order: bool = True, sequencer: str = "séquenceur"):
        super().__init__(name)
        self.total_order, self.sequencer = total_order, sequencer
        self.buffer: Dict[int, Any] = {}
        self.next_seq = 1
        self.log: List[Any] = []
        self.owner: Dict[str, str] = {}
        self.answers: Dict[Tuple[str, str], bool] = {}
        self.apply_times: List[float] = []

    def claim(self, username: str, user: str, peers: Sequence[str]):
        """Demande locale : réserver `username` pour `user`."""
        entry = ("claim", username, user)
        if self.total_order:
            self.send(self.sequencer, ("append", entry))
        else:
            self._apply(entry)
            for p in peers:
                if p != self.name:
                    self.send(p, ("raw", entry))

    def _apply(self, entry):
        self.log.append(entry)
        self.apply_times.append(self.now)
        _, username, user = entry
        if username not in self.owner:
            self.owner[username] = user
        self.answers[(username, user)] = self.owner[username] == user

    def on_message(self, src, msg):
        if msg[0] == "raw":
            self._apply(msg[1])
        elif msg[0] == "deliver":
            self.buffer[msg[1]] = msg[2]
            while self.next_seq in self.buffer:
                self._apply(self.buffer.pop(self.next_seq))
                self.next_seq += 1


def run_registry(claims: Sequence[Tuple[float, str, str, str]], n_replicas: int = 3,
                 total_order: bool = True, latency=(0.5, 4.0), seed: int = 0):
    """claims : [(instant, réplique, nom demandé, utilisateur)]. Renvoie (réseau, répliques)."""
    net = Network(seed=seed, latency=latency)
    names = [f"r{i + 1}" for i in range(n_replicas)]
    reps = [net.add(LogReplica(x, total_order)) for x in names]
    if total_order:
        net.add(Sequencer("séquenceur", names))
    for t, rep, username, user in claims:
        net.at(t, net[rep].claim, username, user, names)
    net.run()
    return net, reps


# ------------------------------------------------------------------ Paxos (une valeur)

class PaxosAcceptor(Node):
    """Accepteur de Paxos (une seule décision) : promet de ne plus écouter les numéros de
    scrutin inférieurs et se souvient de la dernière proposition acceptée."""

    def __init__(self, name: str):
        super().__init__(name)
        self.promised: Tuple = (0, "")
        self.accepted: Optional[Tuple[Tuple, Any]] = None

    def on_message(self, src, msg):
        kind, ballot = msg[0], msg[1]
        if kind == "prepare":
            if ballot > self.promised:
                self.promised = ballot
                self.send(src, ("promise", ballot, self.accepted))
            else:
                self.send(src, ("nack", ballot, self.promised))
        elif kind == "accept":
            if ballot >= self.promised:
                self.promised = ballot
                self.accepted = (ballot, msg[2])
                self.send(src, ("accepted", ballot))
            else:
                self.send(src, ("nack", ballot, self.promised))


class PaxosProposer(Node):
    """Proposeur de Paxos : phase 1 (prepare / promise) puis phase 2 (accept / accepted),
    chacune auprès d'une majorité d'accepteurs. En cas de refus ou de délai dépassé, il
    recommence avec un numéro plus grand après une attente `backoff` : (min, max) tirée au
    hasard, ou un nombre fixe (déterministe, ce qui peut provoquer un duel sans fin)."""

    def __init__(self, name: str, acceptors: Sequence[str], value: Any, start: float = 0.0,
                 backoff=(2.0, 6.0), timeout: float = 10.0, decisions: List[dict] | None = None,
                 peers: Sequence[str] = (), seed: int = 0):
        super().__init__(name)
        self.acceptors, self.value, self.start = list(acceptors), value, start
        self.backoff, self.timeout = backoff, timeout
        self.decisions = decisions if decisions is not None else []
        self.peers = [p for p in peers if p != name]
        self.rng = random.Random(f"{seed}-{name}")
        self.n = 0
        self.highest = 0
        self.ballot: Tuple = (0, name)
        self.phase = 0
        self.replies: List = []
        self.decided: Any = None
        self.rounds = 0

    @property
    def majority(self) -> int:
        return len(self.acceptors) // 2 + 1

    def on_start(self):
        self.set_timer(self.start, self._round)

    def _round(self):
        if self.decided is not None:
            return
        self.rounds += 1
        self.n = max(self.n, self.highest) + 1
        self.ballot = (self.n, self.name)
        self.phase, self.replies = 1, []
        for a in self.acceptors:
            self.send(a, ("prepare", self.ballot))
        b = self.ballot
        self.set_timer(self.timeout, self._expire, b)

    def _expire(self, ballot):
        if self.decided is None and ballot == self.ballot and self.phase in (1, 2):
            self._retry()

    def _retry(self):
        self.phase = 0
        wait = self.rng.uniform(*self.backoff) if isinstance(self.backoff, tuple) else float(self.backoff)
        self.set_timer(wait, self._round)

    def on_message(self, src, msg):
        kind = msg[0]
        if kind == "decided":
            if self.decided is None:
                self.decided = msg[1]
                self.phase = 3
            return
        if self.decided is not None or msg[1] != self.ballot:
            return
        if kind == "nack":
            self.highest = max(self.highest, msg[2][0])
            if self.phase in (1, 2):
                self._retry()
        elif kind == "promise" and self.phase == 1:
            self.replies.append(msg[2])
            if len(self.replies) == self.majority:
                prev = [acc for acc in self.replies if acc is not None]
                v = max(prev, key=lambda x: x[0])[1] if prev else self.value
                self.phase, self.replies, self.proposal = 2, [], v
                for a in self.acceptors:
                    self.send(a, ("accept", self.ballot, v))
        elif kind == "accepted" and self.phase == 2:
            self.replies.append(src)
            if len(self.replies) == self.majority:
                self.decided, self.phase = self.proposal, 3
                self.decisions.append(dict(t=self.now, proposer=self.name, value=self.proposal,
                                           ballot=self.ballot, rounds=self.rounds))
                for p in self.peers:
                    self.send(p, ("decided", self.proposal))


def run_paxos(values: Dict[str, Any], n_acceptors: int = 3, starts: Dict[str, float] | None = None,
              backoff=(2.0, 6.0), latency=(0.5, 1.5), seed: int = 0, until: float = 500.0,
              crashes: Sequence[Tuple[float, str]] = (), timeout: float = 10.0):
    """Paxos à une décision : `values` = {proposeur: valeur proposée}. Renvoie
    (réseau, décisions, proposeurs)."""
    net = Network(seed=seed, latency=latency)
    accs = [f"A{i + 1}" for i in range(n_acceptors)]
    for a in accs:
        net.add(PaxosAcceptor(a))
    decisions: List[dict] = []
    props = {}
    for p, v in values.items():
        props[p] = net.add(PaxosProposer(p, accs, v, start=(starts or {}).get(p, 0.0), backoff=backoff,
                                         timeout=timeout, decisions=decisions, peers=list(values), seed=seed))
    for t, name in crashes:
        net.at(t, net.crash, name)
    net.run(until=until)
    return net, decisions, props


# ------------------------------------------------------------------------- Raft

@dataclass
class Entry:
    term: int
    cmd: Any


class RaftNode(Node):
    """Un nœud Raft (Ongaro et Ousterhout, 2014) sur le simulateur `ddia.sim`.

    Implémente l'élection (mandats, votes, délais aléatoires), la réplication du journal
    (AppendEntries avec vérification de cohérence et retour arrière), la règle de
    validation par majorité et la machine à états (un dictionnaire clé-valeur). L'état
    persistant (mandat, vote, journal) survit aux pannes ; le reste est perdu.

    Paramètres pédagogiques (pour casser l'algorithme) :
    check_log=False supprime la restriction d'élection (un candidat au journal en retard
    peut gagner) ; current_term_only=False valide aussi par comptage les entrées d'un
    mandat précédent ; read_mode="local" fait répondre le leader aux lectures sans
    consulter de quorum (lectures potentiellement périmées).
    """

    def __init__(self, name: str, peers: Sequence[str], election_timeout=(15.0, 30.0), heartbeat: float = 5.0,
                 check_log: bool = True, current_term_only: bool = True, read_mode: str = "log",
                 trace: List[dict] | None = None, seed: int = 0):
        super().__init__(name)
        self.peers = [p for p in peers if p != name]
        self.cluster = list(peers)
        self.et, self.hb = election_timeout, heartbeat
        self.check_log, self.current_term_only, self.read_mode = check_log, current_term_only, read_mode
        self.trace = trace if trace is not None else []
        self.rng = random.Random(f"{seed}-{name}")
        # état persistant
        self.term = 0
        self.voted_for: Optional[str] = None
        self.log: List[Entry] = [Entry(0, None)]
        self._reset_volatile()

    # ------------------------------------------------------------------ outils
    def _reset_volatile(self):
        self.role = "follower"
        self.leader: Optional[str] = None
        self.commit = 0
        self.applied = 0
        self.kv: Dict[Any, Any] = {}
        self.votes: set = set()
        self.next_idx: Dict[str, int] = {}
        self.match_idx: Dict[str, int] = {}
        self.pending: Dict[int, Tuple[str, int, int]] = {}
        self._egen = 0
        self._hgen = 0

    @property
    def majority(self) -> int:
        return len(self.cluster) // 2 + 1

    def _ev(self, kind, **kw):
        self.trace.append(dict(t=self.now, node=self.name, kind=kind, term=self.term, **kw))

    def _last(self):
        return len(self.log) - 1, self.log[-1].term

    def _arm_election(self):
        self._egen += 1
        g = self._egen
        self.set_timer(self.rng.uniform(*self.et), self._election_timeout, g)

    def _step_down(self, term):
        if term > self.term:
            self.term, self.voted_for = term, None
        if self.role != "follower":
            self._ev("step_down")
        self.role = "follower"
        self.pending.clear()      # les clients en attente relanceront après leur délai
        self._hgen += 1
        self._arm_election()

    # ------------------------------------------------------------- cycle de vie
    def on_start(self):
        self._arm_election()

    def on_recover(self):
        self._reset_volatile()
        self._ev("recover")
        self._arm_election()

    def on_crash(self):
        self._ev("crash")

    # --------------------------------------------------------------- élection
    def _election_timeout(self, g):
        if g != self._egen or self.role == "leader":
            return
        self.term += 1
        self.role = "candidate"
        self.voted_for = self.name
        self.votes = {self.name}
        self._ev("candidate")
        li, lt = self._last()
        for p in self.peers:
            self.send(p, ("RequestVote", self.term, self.name, li, lt))
        self._arm_election()
        if len(self.votes) >= self.majority:  # grappe d'un seul nœud
            self._become_leader()

    def _become_leader(self):
        self.role = "leader"
        self.leader = self.name
        self._ev("leader")
        n = len(self.log)
        self.next_idx = {p: n for p in self.peers}
        self.match_idx = {p: 0 for p in self.peers}
        self.log.append(Entry(self.term, ("noop",)))   # entrée vide du nouveau mandat
        self._broadcast_append()
        self._hgen += 1
        self._heartbeat_loop(self._hgen)

    def _heartbeat_loop(self, g):
        if g != self._hgen or self.role != "leader":
            return
        self._broadcast_append()
        self.set_timer(self.hb, self._heartbeat_loop, g)

    def _send_append(self, p):
        ni = self.next_idx[p]
        prev = ni - 1
        entries = [(e.term, e.cmd) for e in self.log[ni:]]
        self.send(p, ("Append", self.term, self.name, prev, self.log[prev].term, entries, self.commit))

    def _broadcast_append(self):
        for p in self.peers:
            self._send_append(p)

    # ---------------------------------------------------------------- messages
    def on_message(self, src, msg):
        kind = msg[0]
        if kind in ("Submit", "Read"):
            return self._client(src, msg)
        term = msg[1]
        if term > self.term:
            self._step_down(term)
        if kind == "RequestVote":
            _, t, cand, li, lt = msg
            my_li, my_lt = self._last()
            up_to_date = (lt, li) >= (my_lt, my_li)
            grant = (t == self.term and self.voted_for in (None, cand)
                     and (up_to_date or not self.check_log))
            if grant:
                self.voted_for = cand
                self._arm_election()
            self.send(src, ("Vote", self.term, grant))
        elif kind == "Vote":
            if self.role == "candidate" and term == self.term and msg[2]:
                self.votes.add(src)
                if len(self.votes) >= self.majority:
                    self._become_leader()
        elif kind == "Append":
            _, t, leader, prev, prev_term, entries, lcommit = msg
            if t < self.term:
                self.send(src, ("AppendResp", self.term, False, 0))
                return
            if self.role != "follower":
                self._step_down(t)
            self.leader = leader
            self._arm_election()
            if prev >= len(self.log) or self.log[prev].term != prev_term:
                self.send(src, ("AppendResp", self.term, False, min(prev - 1, len(self.log) - 1)))
                return
            for k, (et, cmd) in enumerate(entries):
                idx = prev + 1 + k
                if idx < len(self.log) and self.log[idx].term != et:
                    if idx <= self.commit:
                        self._ev("truncate_committed", index=idx)
                    del self.log[idx:]
                if idx >= len(self.log):
                    self.log.append(Entry(et, cmd))
            last_new = prev + len(entries)
            if lcommit > self.commit:
                self.commit = min(lcommit, last_new)
                self._apply()
            self.send(src, ("AppendResp", self.term, True, last_new))
        elif kind == "AppendResp":
            if self.role != "leader" or term != self.term:
                return
            ok, idx = msg[2], msg[3]
            if ok:
                self.match_idx[src] = max(self.match_idx[src], idx)
                self.next_idx[src] = self.match_idx[src] + 1
                self._advance_commit()
            else:
                self.next_idx[src] = max(1, min(self.next_idx[src] - 1, idx + 1))
                self._send_append(src)

    def _advance_commit(self):
        for n in range(len(self.log) - 1, self.commit, -1):
            if self.current_term_only and self.log[n].term != self.term:
                break
            count = 1 + sum(1 for p in self.peers if self.match_idx[p] >= n)
            if count >= self.majority:
                self.commit = n
                self._ev("commit", index=n)
                self._apply()
                self._broadcast_append()   # propage l'indice de validation sans attendre
                break

    def _apply(self):
        while self.applied < self.commit:
            self.applied += 1
            cmd = self.log[self.applied].cmd
            result = None
            if cmd[0] == "set":
                self.kv[cmd[1]] = cmd[2]
                result = "ok"
            elif cmd[0] == "cas":
                _, k, old, new = cmd
                result = self.kv.get(k, 0) == old
                if result:
                    self.kv[k] = new
            elif cmd[0] == "read":
                result = self.kv.get(cmd[1], 0)
            if self.applied in self.pending:
                client, rid, term = self.pending.pop(self.applied)
                if self.log[self.applied].term == term:   # c'est bien l'entrée ajoutée pour ce client
                    self.send(client, ("Reply", rid, result))

    # ----------------------------------------------------------------- clients
    def _client(self, src, msg):
        rid = msg[1]
        if self.role != "leader":
            self.send(src, ("NotLeader", rid, self.leader))
            return
        if msg[0] == "Read" and self.read_mode == "local":
            self.send(src, ("Reply", rid, self.kv.get(msg[2], 0)))
            return
        cmd = ("read", msg[2]) if msg[0] == "Read" else msg[2]
        self.log.append(Entry(self.term, cmd))
        self.pending[len(self.log) - 1] = (src, rid, self.term)
        self._broadcast_append()


class RaftClient(Node):
    """Client d'une grappe Raft : exécute un script d'opérations l'une après l'autre,
    cherche le leader (indications NotLeader, essais successifs) et relance après un délai.
    Chaque opération est enregistrée comme `Op` (registre « x ») pour la vérification."""

    def __init__(self, name: str, servers: Sequence[str], script: Sequence[Tuple] = (), timeout: float = 20.0,
                 history: List[Op] | None = None, key: str = "x"):
        super().__init__(name)
        self.servers = list(servers)
        self.script = list(script)    # [(délai, "write"/"read"/"cas", argument)]
        self.timeout, self.key = timeout, key
        self.history = history if history is not None else []
        self.guess = 0
        self.rid = 0
        self.suspect: Optional[str] = None
        self.cur: Optional[dict] = None

    def on_start(self):
        self._next()

    def _next(self):
        if self.script:
            delay, kind, arg = self.script.pop(0)
            self.set_timer(delay, self._begin, kind, arg)

    def _begin(self, kind, arg):
        self.cur = dict(op=Op(self.name, kind, arg, t_call=self.now), kind=kind, arg=arg)
        self._try()

    def _try(self):
        self.rid += 1
        rid = self.rid
        self.cur["rid"] = rid
        kind, arg = self.cur["kind"], self.cur["arg"]
        dst = self.servers[self.guess % len(self.servers)]
        if kind == "read":
            self.send(dst, ("Read", rid, self.key))
        elif kind == "write":
            self.send(dst, ("Submit", rid, ("set", self.key, arg)))
        else:
            self.send(dst, ("Submit", rid, ("cas", self.key, arg[0], arg[1])))
        self.set_timer(self.timeout, self._expired, rid)

    def _expired(self, rid):
        if self.cur and self.cur["rid"] == rid:
            self.suspect = self.servers[self.guess % len(self.servers)]   # muet : on l'évite
            self.guess += 1
            self._try()

    def on_message(self, src, msg):
        if not self.cur or msg[1] != self.cur["rid"]:
            return
        if msg[0] == "NotLeader":
            hint = msg[2]
            usable = hint in self.servers and hint != src and hint != self.suspect
            self.guess = self.servers.index(hint) if usable else self.guess + 1
            self.set_timer(1.0, self._retry_if, msg[1])
        elif msg[0] == "Reply":
            op = self.cur["op"]
            op.result = "ok" if self.cur["kind"] == "write" else msg[2]
            op.t_return = self.now
            self.history.append(op)
            self.cur = None
            self._next()

    def _retry_if(self, rid):
        if self.cur and self.cur["rid"] == rid:
            self._try()


def raft_cluster(n: int = 5, seed: int = 0, latency=(1.0, 3.0), election_timeout=(15.0, 30.0),
                 heartbeat: float = 5.0, overrides: Dict[str, dict] | None = None, **kw):
    """Crée une grappe Raft de n nœuds (S1..Sn). Renvoie (réseau, {nom: nœud}, trace).

    overrides : paramètres particuliers par nœud (ex. {"S3": {"election_timeout": (40, 41)}})."""
    net = Network(seed=seed, latency=latency)
    names = [f"S{i + 1}" for i in range(n)]
    trace: List[dict] = []
    nodes = {}
    for x in names:
        params = dict(election_timeout=election_timeout, heartbeat=heartbeat, trace=trace, seed=seed, **kw)
        params.update((overrides or {}).get(x, {}))
        nodes[x] = net.add(RaftNode(x, names, **params))
    return net, nodes, trace


def raft_leaders(trace: Sequence[dict]) -> List[Tuple[float, str, int]]:
    """Liste des prises de pouvoir (instant, nœud, mandat)."""
    return [(e["t"], e["node"], e["term"]) for e in trace if e["kind"] == "leader"]


def raft_check_safety(nodes: Dict[str, RaftNode], trace: Sequence[dict]) -> dict:
    """Vérifie les propriétés de sûreté de Raft sur l'état final et la trace :
    un seul leader par mandat, préfixes validés identiques sur tous les nœuds, aucune
    entrée validée effacée."""
    per_term: Dict[int, set] = {}
    for _, node, term in raft_leaders(trace):
        per_term.setdefault(term, set()).add(node)
    two_leaders = [t for t, s in per_term.items() if len(s) > 1]
    logs = {x: [(e.term, e.cmd) for e in nd.log[1:nd.commit + 1]] for x, nd in nodes.items()}
    names = list(logs)
    divergent = []
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = logs[names[i]], logs[names[j]]
            m = min(len(a), len(b))
            if a[:m] != b[:m]:
                divergent.append((names[i], names[j]))
    truncated = [e for e in trace if e["kind"] == "truncate_committed"]
    return dict(mandats_a_deux_leaders=two_leaders, journaux_divergents=divergent,
                entrees_validees_effacees=len(truncated),
                sur=not two_leaders and not divergent and not truncated)


def majority_availability(n: int, p_up: float) -> float:
    """Probabilité qu'une majorité stricte de n nœuds indépendants soit en vie."""
    q = n // 2 + 1
    return sum(math.comb(n, k) * p_up ** k * (1 - p_up) ** (n - k) for k in range(q, n + 1))


def quorum_latency(n: int, q: int, samples: int = 20000, sigma: float = 0.5, base: float = 1.0,
                   local: bool = True, seed: int = 0) -> np.ndarray:
    """Temps d'attente d'un quorum de q réponses parmi n (aller-retour log-normal) ;
    local=True : le leader compte sa propre réponse (immédiate)."""
    rng = np.random.default_rng(seed)
    m = n - 1 if local else n
    need = q - 1 if local else q
    if need <= 0:
        return np.zeros(samples)
    rtt = base * rng.lognormal(0.0, sigma, size=(samples, m))
    return np.sort(rtt, axis=1)[:, need - 1]


# =====================================================================================
# 4. Service de coordination (modèle à la etcd / ZooKeeper)
# =====================================================================================

class CoordService:
    """Magasin clé-valeur linéarisable d'un service de coordination, sans le réseau.

    - chaque écriture incrémente une **révision** globale (le zxid de ZooKeeper, la
      révision d'etcd), utilisable comme jeton de clôture ;
    - un **bail** (lease) a une durée de vie ; il est prolongé par des battements
      (keepalive) et, s'il expire, les clés qui lui sont attachées (clés **éphémères**)
      disparaissent ;
    - `create` est une écriture conditionnelle (créer si absente) ; `create_sequential`
      ajoute un suffixe croissant (recette d'élection de ZooKeeper) ;
    - `watch(prefix ou clé, rappel)` notifie les changements ; on compte les
      notifications envoyées.
    Le temps est fourni par l'appelant (paramètre `now`).
    """

    def __init__(self):
        self.revision = 0
        self.data: Dict[str, dict] = {}
        self.leases: Dict[int, dict] = {}
        self._next_lease = 1
        self.watches: List[Tuple[str, bool, Callable, bool]] = []
        self.notifications = 0
        self.log: List[Tuple[int, str, str]] = []

    # ------------------------------------------------------------------ baux
    def grant(self, ttl: float, now: float) -> int:
        lid = self._next_lease
        self._next_lease += 1
        self.leases[lid] = dict(ttl=ttl, deadline=now + ttl, keys=set())
        return lid

    def keepalive(self, lease: int, now: float) -> bool:
        self.expire(now)
        if lease not in self.leases:
            return False
        self.leases[lease]["deadline"] = now + self.leases[lease]["ttl"]
        return True

    def revoke(self, lease: int) -> None:
        info = self.leases.pop(lease, None)
        if info:
            for k in sorted(info["keys"]):
                if k in self.data:
                    self._delete(k)

    def expire(self, now: float) -> List[int]:
        dead = [lid for lid, info in self.leases.items() if info["deadline"] <= now]
        for lid in dead:
            self.revoke(lid)
        return dead

    # -------------------------------------------------------------- écritures
    def _notify(self, kind: str, key: str):
        self.log.append((self.revision, kind, key))
        fired = []
        for w in list(self.watches):
            target, is_prefix, cb, once = w
            if (is_prefix and key.startswith(target)) or (not is_prefix and key == target):
                fired.append(w)
        for w in fired:
            if w[3] and w in self.watches:
                self.watches.remove(w)
            self.notifications += 1
            w[2](kind, key, self.revision)

    def put(self, key: str, value: Any, lease: int | None = None) -> int:
        self.revision += 1
        old = self.data.get(key)
        self.data[key] = dict(value=value, create=old["create"] if old else self.revision,
                              mod=self.revision, lease=lease)
        if lease is not None:
            self.leases[lease]["keys"].add(key)
        self._notify("put", key)
        return self.revision

    def create(self, key: str, value: Any, lease: int | None = None) -> Tuple[bool, int]:
        """Crée la clé si elle n'existe pas (CAS « absente -> valeur »)."""
        if key in self.data:
            return False, self.data[key]["create"]
        return True, self.put(key, value, lease)

    def create_sequential(self, prefix: str, value: Any, lease: int | None = None) -> str:
        key = f"{prefix}{self.revision + 1:08d}"
        self.put(key, value, lease)
        return key

    def _delete(self, key: str):
        info = self.data.pop(key)
        if info["lease"] in self.leases:
            self.leases[info["lease"]]["keys"].discard(key)
        self.revision += 1
        self._notify("delete", key)

    def delete(self, key: str) -> bool:
        if key not in self.data:
            return False
        self._delete(key)
        return True

    # ---------------------------------------------------------------- lectures
    def get(self, key: str):
        info = self.data.get(key)
        return None if info is None else info["value"]

    def keys(self, prefix: str) -> List[str]:
        return sorted(k for k in self.data if k.startswith(prefix))

    def watch(self, target: str, callback: Callable, prefix: bool = False, once: bool = True) -> None:
        self.watches.append((target, prefix, callback, once))
