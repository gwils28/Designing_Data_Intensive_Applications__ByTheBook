"""Mini-moteur d'exécution durable (section « Durable Execution and Workflows »).

Le principe des moteurs comme Temporal ou Restate, réduit à l'essentiel :

- un **workflow** est une fonction Python ordinaire `workflow(ctx, *args)` ;
- chaque appel à un service (une **activité**) passe par `ctx.activity(fn, *args)` ;
- le résultat de chaque activité, de chaque lecture d'horloge (`ctx.now()`) et de chaque
  tirage aléatoire (`ctx.random()`) est **journalisé** dans un stockage durable ;
- après une panne, le moteur **réexécute** la fonction depuis le début : les étapes déjà
  journalisées ne sont pas refaites, leur résultat est relu dans le journal (rejeu) ;
- si, au rejeu, le code ne refait pas les mêmes appels dans le même ordre, le moteur lève
  `NonDeterminismError`.

Les pannes sont injectées par une politique `crash(phase, name, seq, attempt) -> bool`
appelée juste **avant** une activité (phase "before") et juste **après** son effet mais
avant l'écriture dans le journal (phase "after") : c'est la fenêtre dangereuse.
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


class Crash(Exception):
    """Panne simulée du processus qui exécute le workflow."""


class NonDeterminismError(RuntimeError):
    """Le code rejoué ne refait pas les mêmes appels que ceux du journal."""


@dataclass
class Event:
    seq: int
    kind: str      # "activity", "now", "random"
    name: str
    result: Any


@dataclass
class LogLine:
    """Trace chronologique (pour les diagrammes) : t, tentative, action, étape."""
    t: float
    attempt: int
    action: str    # "exec", "replay", "crash", "done", "start"
    name: str = ""
    seq: int = -1


class ExternalService:
    """Service externe à effet de bord (passerelle de paiement, banque...).

    Avec idempotent=True, une requête portant une clé déjà vue n'est pas réexécutée : on
    renvoie la réponse mémorisée.
    """

    def __init__(self, name: str, idempotent: bool = False):
        self.name = name
        self.idempotent = idempotent
        self.calls = 0
        self.effects: List[Any] = []
        self._seen: Dict[str, Any] = {}

    def __call__(self, key: Optional[str], *args) -> Any:
        self.calls += 1
        if self.idempotent and key is not None and key in self._seen:
            return self._seen[key]
        self.effects.append(args)
        result = f"{self.name}-{len(self.effects)}"
        if key is not None:
            self._seen[key] = result
        return result


class WorkflowContext:
    def __init__(self, engine: "Engine", wf_id: str, history: List[Event], attempt: int):
        self.engine = engine
        self.wf_id = wf_id
        self.history = history
        self.attempt = attempt
        self.seq = 0
        self.replayed = 0

    def _next(self, kind: str, name: str):
        seq = self.seq
        self.seq += 1
        if seq < len(self.history):
            ev = self.history[seq]
            if ev.kind != kind or ev.name != name:
                raise NonDeterminismError(
                    f"étape {seq} : le journal contient {ev.kind} {ev.name!r}, le code demande {kind} {name!r}")
            return seq, ev
        return seq, None

    def activity(self, fn: Callable, *args, name: Optional[str] = None, with_key: bool = False) -> Any:
        """Exécute (ou rejoue) une activité. with_key=True : fn reçoit en premier argument
        une clé d'idempotence stable, `wf_id/étape`, identique à chaque réexécution."""
        name = name or getattr(fn, "name", None) or fn.__name__
        seq, ev = self._next("activity", name)
        eng = self.engine
        if ev is not None:
            self.replayed += 1
            eng._log("replay", name, seq, self.attempt, dt=0.15)
            return ev.result
        if eng.crash("before", name, seq, self.attempt):
            eng._log("crash", name, seq, self.attempt, dt=0.0)
            raise Crash(f"panne avant {name}")
        key = f"{self.wf_id}/{seq}" if with_key else None
        result = fn(key, *args) if with_key else fn(*args)
        eng.executions[name] += 1
        eng._log("exec", name, seq, self.attempt, dt=1.0)
        if eng.crash("after", name, seq, self.attempt):
            eng._log("crash", name, seq, self.attempt, dt=0.0)
            raise Crash(f"panne après l'effet de {name}, avant la journalisation")
        if eng.durable:
            self.history.append(Event(seq, "activity", name, result))
            eng.journal_writes += 1
        return result

    def now(self) -> float:
        """Horloge déterministe : la valeur est journalisée au premier passage."""
        seq, ev = self._next("now", "now")
        if ev is not None:
            return ev.result
        v = self.engine.t
        if self.engine.durable:
            self.history.append(Event(seq, "now", "now", v))
        return v

    def random(self) -> float:
        """Tirage aléatoire déterministe : journalisé au premier passage."""
        seq, ev = self._next("random", "random")
        if ev is not None:
            return ev.result
        v = self.engine.rng.random()
        if self.engine.durable:
            self.history.append(Event(seq, "random", "random", v))
        return v
