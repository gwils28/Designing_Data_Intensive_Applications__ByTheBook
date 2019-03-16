"""Réplication : mini-systèmes pédagogiques du chapitre « Replication ».

Le module contient six briques indépendantes :

1. **Leader unique sur le simulateur** (`ReplNode`, `Controller`, `WriterClient`,
   `failover_scenario`) : réplication asynchrone, semi-synchrone, par quorum ou
   totalement synchrone ; rattrapage d'un suiveur ; basculement piloté par un contrôleur,
   numéros d'époque (fencing), split brain, écritures acquittées puis perdues.
   Latences d'écriture analytiques par statistiques d'ordre (`write_latency_samples`).
2. **Journaux de réplication** (`MiniDB`, `replicate_statements`, `replicate_rows`) :
   réplication par instructions (divergences dues à `NOW()`, `RAND()`, à l'ordre) contre
   réplication logique par lignes.
3. **Retard de réplication** (`LagCluster`, `lag_cluster`, `simulate_sessions`) : un
   leader, des suiveurs qui appliquent le journal dans l'ordre avec un retard aléatoire et
   des épisodes de blocage ; anomalies « lire ses écritures » et « lectures monotones »
   selon la stratégie de routage des lectures.
4. **Multi-leader** (`topology`, `propagate`, `TopologyLeader`, `run_topology`,
   `lww_resolve`, `merge_union`, `merge_with_tombstones`) : topologies circulaire, en
   étoile et tout-à-tout, boucles évitées par les identifiants de nœuds, messages qui se
   doublent, dernier écrivain gagnant et paniers fusionnés.
5. **Sans leader, à la Dynamo** (`p_stale_static`, `k_of_n`, `pbs_stale_probability`,
   `DynamoReplica`, `DynamoClient`, `merkle_tree`, `merkle_diff`) : quorums, probabilité
   de lecture périmée, lecture réparatrice, transfert différé (hinted handoff), quorum
   approximatif (sloppy quorum), anti-entropie par arbre de Merkle.
6. **Écritures concurrentes** (`VersionedKeyServer`, `vv_compare`, `vv_merge`,
   `DottedReplica`) : l'algorithme à numéro de version unique du livre (panier d'achat),
   vecteurs de versions et vecteurs de versions « pointés ».

Exemple
-------
>>> srv = VersionedKeyServer()
>>> v1, _ = srv.put(["lait"], None)
>>> v2, sib = srv.put(["oeufs"], None)          # écriture concurrente : deux frères
>>> sorted(map(tuple, sib.values()))
[('lait',), ('oeufs',)]
"""

from __future__ import annotations

import bisect
import hashlib
import itertools
import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from .sim import Network, Node

__all__ = [
    # 1. leader unique
    "Entry", "ReplNode", "Controller", "WriterClient", "failover_scenario", "write_latency_samples",
    "needed_acks",
    # 2. journaux
    "MiniDB", "replicate_statements", "replicate_rows",
    # 3. retard
    "LagCluster", "lag_cluster", "simulate_sessions", "STRATEGIES",
    # 4. multi-leader
    "topology", "propagate", "TopologyLeader", "run_topology", "lww_resolve", "merge_union",
    "merge_with_tombstones",
    # 5. sans leader
    "p_stale_static", "k_of_n", "pbs_stale_probability", "DynamoReplica", "DynamoClient",
    "dynamo_cluster", "merkle_tree", "merkle_diff",
    # 6. concurrence
    "VersionedKeyServer", "vv_compare", "vv_merge", "vv_increment", "DottedReplica",
]


# =====================================================================================
# 1. Réplication à leader unique sur le simulateur ddia.sim
# =====================================================================================


@dataclass(frozen=True)
class Entry:
    """Une entrée du journal de réplication."""

    epoch: int      # époque du leader qui l'a créée
    lsn: int        # position dans le journal (1, 2, ...)
    key: str
    value: Any
    req: str        # identifiant de la requête cliente (pour retrouver les écritures perdues)


def needed_acks(mode: str, n_followers: int) -> int:
    """Nombre d'acquittements de suiveurs attendus avant de répondre au client.

    async : 0 ; semisync : 1 ; quorum : majorité du groupe (leader compris) moins le
    leader ; sync : tous les suiveurs.
    """
    if mode == "async":
        return 0
    if mode == "semisync":
        return min(1, n_followers)
    if mode == "quorum":
        return (n_followers + 1) // 2
    if mode == "sync":
        return n_followers
    raise ValueError(f"mode inconnu : {mode}")


class ReplNode(Node):
    """Nœud de données qui peut être leader ou suiveur.

    Messages : ("write", req, key, value) d'un client ; ("repl", epoch, Entry) et
    ("ack", epoch, lsn) entre leader et suiveurs ; ("fetch", from_lsn) pour le rattrapage ;
    ("pos?",) / ("pos", lsn, epoch) avec le contrôleur ; ("promote", epoch) ;
    ("new_leader", epoch, name) ; ("sync", epoch, log) pour aligner un ancien leader ;
    ("read", req, key) / ("value", req, value, lsn).
    """

    def __init__(self, name: str, peers: list[str], leader: str, mode: str = "async",
                 controller: str = "controleur", hb: float = 0.5, fencing: bool = True,
                 ship_delay: dict | None = None):
        super().__init__(name)
        self.ship_delay = ship_delay or {}      # retard d'expédition du journal par suiveur
        self.peers = [p for p in peers if p != name]
        self.leader = leader
        self.role = "leader" if name == leader else "follower"
        self.mode = mode
        self.controller = controller
        self.hb = hb
        self.fencing = fencing
        self.epoch = 0
        self.log: list[Entry] = []
        self.data: dict[str, Any] = {}
        self.pending: dict[int, list] = {}        # lsn -> [client, req, set(acks)]
        self.discarded: list[Entry] = []          # entrées jetées lors d'un réalignement
        self._ahead: dict[int, Entry] = {}         # entrées reçues avant leurs prédécesseurs
        self.roles: list[tuple[float, str, int]] = []   # (instant, rôle, époque)

    # ------------------------------------------------------------------ utilitaires
    @property
    def lsn(self) -> int:
        return len(self.log)

    def _set_role(self, role: str) -> None:
        self.role = role
        self.roles.append((self.now, role, self.epoch))

    def _apply(self, e: Entry) -> None:
        self.log.append(e)
        self.data[e.key] = e.value

    def on_start(self) -> None:
        self.roles.append((self.now, self.role, self.epoch))
        if self.role == "leader":
            self._heartbeat()

    def _heartbeat(self) -> None:
        if self.role == "leader" and self.up:
            self.send(self.controller, ("hb", self.epoch))
            self.set_timer(self.hb, self._heartbeat)

    def on_crash(self) -> None:
        self.roles.append((self.now, "down", self.epoch))

    def on_recover(self) -> None:
        self.roles.append((self.now, self.role, self.epoch))
        # le journal local a survécu à la panne : on reprend là où on s'était arrêté
        if self.role == "leader":
            self._heartbeat()
        elif self.leader:
            self._fetch(self.leader)

    def _fetch(self, leader: str) -> None:
        """Demande au leader les entrées après notre position (une seule demande en cours
        par position, renouvelée au bout d'une unité de temps si elle reste sans effet)."""
        last = getattr(self, "_last_fetch", None)
        if last and last[0] == self.lsn and self.now - last[1] < 1.0:
            return
        self._last_fetch = (self.lsn, self.now)
        self.send(leader, ("fetch", self.lsn))
        lsn0 = self.lsn

        def retry():
            if self.role == "follower" and self.lsn == lsn0 and self._ahead:
                self._fetch(self.leader)
        self.set_timer(1.05, retry)

    # ---------------------------------------------------------------------- messages
    def on_message(self, src: str, msg) -> None:
        kind = msg[0]
        if kind == "write":
            _, req, key, value = msg
            if self.role != "leader":
                self.send(src, ("redirect", req, self.leader))
                return
            e = Entry(self.epoch, self.lsn + 1, key, value, req)
            self._apply(e)
            for p in self.peers:
                d = self.ship_delay.get(p, 0.0)
                if d > 0:      # journal expédié plus tard (file d'envoi) : perdu si le leader meurt avant
                    self.set_timer(d, self.send, p, ("repl", self.epoch, e))
                else:
                    self.send(p, ("repl", self.epoch, e))
            need = needed_acks(self.mode, len(self.peers))
            if need == 0:
                self.send(src, ("ok", req, e.lsn))
            else:
                self.pending[e.lsn] = [src, req, set()]
        elif kind == "repl":
            _, epoch, e = msg
            if self.fencing and epoch < self.epoch:
                self.send(src, ("reject", self.epoch))          # fencing par numéro d'époque
                return
            if epoch > self.epoch:
                self.epoch, self.leader = epoch, src
                if self.role == "leader":
                    self._set_role("follower")
            if e.lsn == self.lsn + 1:
                self._apply(e)
                while self.lsn + 1 in self._ahead:              # entrées arrivées en avance
                    self._apply(self._ahead.pop(self.lsn + 1))
                self.send(src, ("ack", epoch, self.lsn))
            elif e.lsn > self.lsn + 1:
                self._ahead[e.lsn] = e
                self._fetch(src)                                 # trou : on demande la suite
            else:
                self.send(src, ("ack", epoch, self.lsn))
        elif kind == "ack":
            _, epoch, lsn = msg
            if self.role != "leader" or epoch != self.epoch:
                return
            need = needed_acks(self.mode, len(self.peers))
            for l in sorted(self.pending):
                if l <= lsn:
                    self.pending[l][2].add(src)
                    if len(self.pending[l][2]) >= need:
                        client, req, _ = self.pending.pop(l)
                        self.send(client, ("ok", req, l))
        elif kind == "fetch":
            if self.role == "leader":
                for e in self.log[msg[1]:]:
                    self.send(src, ("repl", self.epoch, e))
        elif kind == "reject":
            if msg[1] > self.epoch and self.role == "leader":
                self.epoch = msg[1]
                self._set_role("follower")
        elif kind == "pos?":
            self.send(src, ("pos", self.lsn, self.epoch))
        elif kind == "promote":
            self.epoch, self.leader = msg[1], self.name
            self._set_role("leader")
            self.pending.clear()
            self._heartbeat()
        elif kind == "new_leader":
            _, epoch, name = msg
            if name == self.name:
                return                      # c'est nous : la promotion arrive par "promote"
            if epoch > self.epoch or (epoch == self.epoch and name != self.leader and name != self.name):
                self.epoch, self.leader = epoch, name
                if self.role == "leader":
                    self._set_role("follower")
                self.pending.clear()
                self.send(name, ("sync?", self.lsn))
        elif kind == "sync?":
            if self.role == "leader":
                self.send(src, ("sync", self.epoch, list(self.log)))
        elif kind == "sync":
            _, epoch, log = msg
            mine = set(self.log)
            theirs = set(log)
            self.discarded.extend(e for e in self.log if e not in theirs)
            self.log, self.data, self._ahead = [], {}, {}
            for e in log:
                self._apply(e)
            del mine
        elif kind == "read":
            _, req, key = msg
            self.send(src, ("value", req, self.data.get(key), self.lsn))


class Controller(Node):
    """Contrôleur de basculement : surveille les battements du leader et promeut le suiveur
    le plus à jour (`policy="best"`) ou un suiveur au hasard (`policy="random"`)."""

    def __init__(self, name: str, data_nodes: list[str], leader: str, clients: list[str],
                 timeout: float = 2.0, tick: float = 0.25, policy: str = "best", seed: int = 0):
        super().__init__(name)
        self.data_nodes = data_nodes
        self.leader = leader
        self.clients = clients
        self.timeout = timeout
        self.tick = tick
        self.policy = policy
        self.rng = random.Random(seed)
        self.epoch = 0
        self.last_hb = 0.0
        self.positions: dict[str, int] = {}
        self.electing = False
        self.failovers: list[tuple[float, str, int]] = []   # (instant, nouveau leader, époque)

    def on_start(self) -> None:
        self.last_hb = self.local_time()
        self.set_timer(self.tick, self._check)

    def _check(self) -> None:
        if not self.electing and self.local_time() - self.last_hb > self.timeout:
            self.electing = True
            self.positions = {}
            for n in self.data_nodes:
                if n != self.leader:
                    self.send(n, ("pos?",))
            self.set_timer(3 * self.tick, self._decide)
        # rappel périodique du leader courant (répare les nœuds qui l'ont manqué)
        if self.epoch > 0:
            for n in self.data_nodes:
                self.send(n, ("new_leader", self.epoch, self.leader))
        self.set_timer(self.tick, self._check)

    def _decide(self) -> None:
        self.electing = False
        if not self.positions:
            self.last_hb = self.local_time()
            return
        if self.policy == "best":
            new = max(sorted(self.positions), key=lambda n: self.positions[n])
        else:
            new = self.rng.choice(sorted(self.positions))
        self.epoch += 1
        self.leader = new
        self.failovers.append((self.now, new, self.epoch))
        self.send(new, ("promote", self.epoch))
        for n in self.data_nodes + self.clients:
            if n != new:
                self.send(n, ("new_leader", self.epoch, new))
        self.last_hb = self.local_time()

    def on_message(self, src: str, msg) -> None:
        if msg[0] == "hb":
            if src == self.leader and msg[1] >= self.epoch:
                self.last_hb = self.local_time()
        elif msg[0] == "pos":
            self.positions[src] = msg[1]


class WriterClient(Node):
    """Client qui écrit `key_i = i` à intervalle régulier et note les acquittements."""

    def __init__(self, name: str, leader: str, period: float = 0.4, start: float = 0.2,
                 stop: float = 20.0, prefix: str = "k"):
        super().__init__(name)
        self.leader = leader
        self.period, self.start, self.stop = period, start, stop
        self.prefix = prefix
        self.i = 0
        self.sent: dict[str, float] = {}
        self.acked: dict[str, tuple[float, int, str]] = {}   # req -> (instant, lsn, nœud)

    def on_start(self) -> None:
        self.set_timer(self.start, self._write)

    def _write(self) -> None:
        if self.now > self.stop:
            return
        self.i += 1
        req = f"{self.name}-{self.i}"
        self.sent[req] = self.now
        self.send(self.leader, ("write", req, f"{self.prefix}{self.i}", self.i))
        self.set_timer(self.period, self._write)

    def on_message(self, src: str, msg) -> None:
        if msg[0] == "ok":
            self.acked.setdefault(msg[1], (self.now, msg[2], src))
        elif msg[0] == "new_leader":
            self.leader = msg[2]
        elif msg[0] == "redirect" and msg[2]:
            self.leader = msg[2]
