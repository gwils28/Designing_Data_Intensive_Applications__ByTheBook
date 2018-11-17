"""Mini-simulateur à événements discrets pour expérimenter les systèmes distribués.

Le modèle est volontairement celui du livre : des nœuds qui ne partagent rien et ne
communiquent que par messages, sur un réseau **asynchrone** et **non fiable** (délais
variables, pertes, partitions), avec des **horloges imparfaites** (décalage et dérive).

Tout est déterministe pour une graine donnée : une expérience se rejoue à l'identique.

Exemple
-------
>>> class Echo(Node):
...     def on_message(self, src, msg):
...         if msg == "ping":
...             self.send(src, "pong")
>>> net = Network(seed=1, latency=(1.0, 3.0))
>>> a, b = net.add(Node("a")), net.add(Echo("b"))
>>> a.send("b", "ping")
>>> net.run()
>>> [(d.src, d.msg) for d in net.trace]
[('a', 'ping'), ('b', 'pong')]
"""

from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = ["Network", "Node", "Delivery", "Clock"]


@dataclass
class Delivery:
    """Un message tel qu'enregistré dans la trace du réseau."""

    src: str
    dst: str
    msg: Any
    t_send: float
    t_recv: float
    lost: bool = False


@dataclass
class Clock:
    """Horloge physique d'un nœud : lecture = (1 + drift) * t_réel + offset.

    drift est en fraction (ex. 200e-6 = 200 ppm, l'ordre de grandeur d'un quartz bon
    marché) ; offset en unités de temps de simulation.
    """

    offset: float = 0.0
    drift: float = 0.0

    def read(self, t: float) -> float:
        return (1.0 + self.drift) * t + self.offset

    def adjust(self, t: float, target: float) -> None:
        """Synchronisation de type NTP : la lecture à l'instant t devient `target`
        (l'horloge peut donc reculer : c'est une horloge *time-of-day*)."""
        self.offset += target - self.read(t)


class Node:
    """Nœud de base : surcharger `on_message` (et éventuellement `on_start`)."""

    def __init__(self, name: str, clock: Clock | None = None):
        self.name = name
        self.net: Network | None = None
        self.clock = clock or Clock()
        self.up = True
        self.log: list[tuple[float, str]] = []

    # --- à surcharger ---------------------------------------------------------
    def on_start(self) -> None:
        pass

    def on_message(self, src: str, msg: Any) -> None:
        pass

    def on_crash(self) -> None:
        pass

    def on_recover(self) -> None:
        pass

    # --- services offerts --------------------------------------------------------
    @property
    def now(self) -> float:
        """Temps réel de la simulation (inconnu des nœuds dans la vraie vie !)."""
        return self.net.now

    def local_time(self) -> float:
        """Lecture de l'horloge physique du nœud (seule notion de temps légitime)."""
        return self.clock.read(self.net.now)

    def send(self, dst: str, msg: Any) -> None:
        if self.up:
            self.net.send(self.name, dst, msg)

    def broadcast(self, msg: Any, include_self: bool = False) -> None:
        for other in self.net.nodes:
            if other != self.name or include_self:
                self.send(other, msg)

    def set_timer(self, delay: float, fn: Callable, *args) -> None:
        """Programme fn(*args) après `delay` (ignoré si le nœud est en panne à ce moment)."""
        def fire():
            if self.up:
                fn(*args)
        self.net.schedule(delay, fire)

    def record(self, what: str) -> None:
        self.log.append((self.net.now, what))


class Network:
    """Réseau simulé + ordonnanceur d'événements.

    latency : (min, max) pour un délai uniforme, ou fonction(rng, src, dst) -> délai.
    drop    : probabilité de perte de chaque message.
    """

    def __init__(self, seed: int = 0, latency=(1.0, 1.0), drop: float = 0.0,
                 duplicate: float = 0.0):
        self.rng = random.Random(seed)
        self.latency = latency
        self.drop = drop
        self.duplicate = duplicate
        self.now = 0.0
        self.nodes: dict[str, Node] = {}
        self.trace: list[Delivery] = []
        self._queue: list = []
        self._seq = itertools.count()
        self._blocked: set[tuple[str, str]] = set()

    # --- topologie ---------------------------------------------------------------
    def add(self, node: Node) -> Node:
        node.net = self
        self.nodes[node.name] = node
        self.schedule(0.0, node.on_start)
        return node

    def __getitem__(self, name: str) -> Node:
        return self.nodes[name]

    def partition(self, *groups) -> None:
        """Coupe le réseau entre les groupes (ex. partition({"a","b"}, {"c"}))."""
        groups = [set(g) for g in groups]
        for g1, g2 in itertools.permutations(groups, 2):
            for a in g1:
                for b in g2:
                    self._blocked.add((a, b))

    def block(self, src: str, dst: str) -> None:
        """Coupe un seul sens (src -> dst) : utile pour les pannes asymétriques."""
        self._blocked.add((src, dst))

    def heal(self) -> None:
        self._blocked.clear()

    def crash(self, name: str) -> None:
        node = self.nodes[name]
        node.up = False
        node.on_crash()

    def recover(self, name: str) -> None:
        node = self.nodes[name]
        node.up = True
        node.on_recover()

    # --- ordonnancement ------------------------------------------------------------
    def schedule(self, delay: float, fn: Callable, *args) -> None:
        heapq.heappush(self._queue, (self.now + delay, next(self._seq), fn, args))

    def at(self, t: float, fn: Callable, *args) -> None:
        """Programme fn(*args) à l'instant absolu t."""
        heapq.heappush(self._queue, (t, next(self._seq), fn, args))

    def _delay(self, src: str, dst: str) -> float:
        if callable(self.latency):
            return float(self.latency(self.rng, src, dst))
        lo, hi = self.latency
        return self.rng.uniform(lo, hi)

    def send(self, src: str, dst: str, msg: Any) -> None:
        copies = 2 if self.rng.random() < self.duplicate else 1
        for _ in range(copies):
            d = self._delay(src, dst)
            lost = (src, dst) in self._blocked or self.rng.random() < self.drop
            rec = Delivery(src, dst, msg, self.now, self.now + d, lost)
            self.trace.append(rec)
            if not lost:
                self.schedule(d, self._deliver, rec)

    def _deliver(self, rec: Delivery) -> None:
        node = self.nodes.get(rec.dst)
        if node is None or not node.up or (rec.src, rec.dst) in self._blocked:
            rec.lost = True
            return
        node.on_message(rec.src, rec.msg)

    def run(self, until: float = float("inf"), max_events: int = 1_000_000) -> None:
        n = 0
        while self._queue and self._queue[0][0] <= until and n < max_events:
            t, _, fn, args = heapq.heappop(self._queue)
            self.now = t
            fn(*args)
            n += 1
        if until != float("inf"):
            self.now = max(self.now, until)

    # --- statistiques ----------------------------------------------------------------
    def messages(self, kind: Callable[[Any], bool] | None = None) -> list[Delivery]:
        return [d for d in self.trace if kind is None or kind(d.msg)]
