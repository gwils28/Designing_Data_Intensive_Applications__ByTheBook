"""Modèles de données : notions propres à la 2e édition du livre.

- listes réordonnables et **indexation fractionnaire** (`key_between`) ;
- un mini-exécuteur **GraphQL** (sous-ensemble du langage) avec résolveurs naïfs ou
  regroupés par lots, pour mesurer le problème N+1 ;
- **event sourcing et CQRS** : journal d'événements immuables, traitement des
  commandes, projections (vues matérialisées) reconstruites par rejeu ;
- **DataFrames et matrices** : notes d'utilisateurs, matrice creuse au format CSR
  écrite à la main, similarité cosinus.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

__all__ = [
    "DIGITS", "key_between", "keys_between", "parse_graphql", "query_depth", "execute_graphql",
    "Event", "EventLog", "ConferenceService", "AvailableSeats", "BookingStatus", "DailyDashboard",
    "BadgeList", "replay", "make_conference_commands",
    "make_ratings", "to_csr", "csr_matvec", "csr_nbytes", "cosine_top_k",
]

# =====================================================================================
# 1. Indexation fractionnaire
# =====================================================================================

DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"


def _midpoint(a: str, b: str | None) -> str:
    """Chaîne strictement comprise entre a et b (ordre lexicographique), a < b.

    a = "" signifie « 0 », b = None signifie « 1 » : les clés sont les chiffres après la
    virgule d'un nombre en base 36. Aucune clé ne se termine par « 0 ».
    """
    if b is not None:
        if a >= b:
            raise ValueError(f"il faut a < b : {a!r} >= {b!r}")
        n = 0
        while (a[n] if n < len(a) else "0") == b[n]:
            n += 1
        if n > 0:
            return b[:n] + _midpoint(a[n:], b[n:])
    da = DIGITS.index(a[0]) if a else 0
    db = DIGITS.index(b[0]) if b else len(DIGITS)
    if db - da > 1:
        return DIGITS[(da + db + 1) // 2]
    if b is not None and len(b) > 1:
        return b[:1]
    return DIGITS[da] + _midpoint(a[1:], None)


def key_between(a: str | None, b: str | None) -> str:
    """Clé de tri à placer entre a et b (None : début ou fin de liste)."""
    return _midpoint(a or "", b)


def keys_between(a: str | None, b: str | None, n: int) -> list[str]:
    """n clés régulièrement réparties entre a et b."""
    if n == 0:
        return []
    if n == 1:
        return [key_between(a, b)]
    mid = n // 2
    m = key_between(a, b)
    return keys_between(a, m, mid) + [m] + keys_between(m, b, n - mid - 1)


# =====================================================================================
# 2. Mini-GraphQL
# =====================================================================================

_GQL_TOKEN = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*|-?\d+|\"[^\"]*\"|[{}():,])")


def parse_graphql(text: str) -> list[dict]:
    """Analyse une requête GraphQL (sous-ensemble : champs, arguments, sélections imbriquées).

    Renvoie la liste des champs de premier niveau, chacun {"name", "args", "fields"}.
    """
    toks = [t for t in _GQL_TOKEN.findall(text)]
    i = 0
    if toks and toks[0] == "query":
        i = 1
        if toks[i] != "{":
            i += 1                                 # nom de l'opération

    def selection():
        nonlocal i
        assert toks[i] == "{"
        i += 1
        out = []
        while toks[i] != "}":
            name = toks[i]
            i += 1
            args = {}
            if toks[i] == "(":
                i += 1
                while toks[i] != ")":
                    k = toks[i]
                    assert toks[i + 1] == ":"
                    v = toks[i + 2]
                    args[k] = int(v) if re.fullmatch(r"-?\d+", v) else v.strip('"')
                    i += 3
                    if toks[i] == ",":
                        i += 1
                i += 1
            sub = selection() if toks[i] == "{" else None
            out.append({"name": name, "args": args, "fields": sub})
        i += 1
        return out

    return selection()


def query_depth(fields: list[dict] | None) -> int:
    """Profondeur d'imbrication d'une requête (pour refuser les requêtes trop coûteuses)."""
    if not fields:
        return 0
    return 1 + max(query_depth(f["fields"]) for f in fields)


def execute_graphql(query: str, schema: dict, ctx: dict, batched: bool = False,
                    max_depth: int | None = None) -> dict:
    """Exécute une requête sur un schéma de résolveurs.

    schema[type][champ] = (type_renvoyé ou None pour un scalaire, résolveur, liste?)
    Un résolveur reçoit une **liste de parents** et renvoie une liste de valeurs (une par
    parent). En mode naïf, il est appelé une fois par parent (problème N+1) ; en mode
    regroupé (`batched=True`, à la DataLoader), une seule fois par champ et par niveau.
    ctx["calls"] compte les appels aux résolveurs non triviaux.
    """
    fields = parse_graphql(query)
    if max_depth is not None and query_depth(fields) > max_depth:
        raise ValueError(f"requête refusée : profondeur {query_depth(fields)} > {max_depth}")
    ctx.setdefault("calls", 0)

    def resolve(typ, parents, sel):
        outs = [dict() for _ in parents]
        for f in sel:
            rtype, fn, is_list = schema[typ][f["name"]]
            if batched:
                vals = fn(parents, f["args"], ctx)
            else:
                vals = [fn([p], f["args"], ctx)[0] for p in parents]
            if rtype is None:
                for o, v in zip(outs, vals):
                    o[f["name"]] = v
                continue
            # aplatir les enfants pour résoudre le niveau suivant d'un seul coup
            flat, shape = [], []
            for v in vals:
                if v is None:
                    shape.append(None)
                elif is_list:
                    shape.append(len(v))
                    flat.extend(v)
                else:
                    shape.append(-1)
                    flat.append(v)
            child = resolve(rtype, flat, f["fields"]) if flat else []
            k = 0
            for o, s in zip(outs, shape):
                if s is None:
                    o[f["name"]] = None
                elif s == -1:
                    o[f["name"]] = child[k]
                    k += 1
                else:
                    o[f["name"]] = child[k:k + s]
                    k += s
        return outs

    return {"data": resolve("Query", [None], fields)[0]}


# =====================================================================================
# 3. Event sourcing et CQRS
# =====================================================================================

@dataclass(frozen=True)
class Event:
    seq: int
    ts: float
    type: str
    data: dict = field(default_factory=dict)


class EventLog:
    """Journal d'événements en ajout seul ; les projections abonnées sont tenues à jour."""

    def __init__(self):
        self.events: list[Event] = []
        self.subscribers = []

    def append(self, type_: str, data: dict, ts: float) -> Event:
        ev = Event(len(self.events), ts, type_, dict(data))
        self.events.append(ev)
        for s in self.subscribers:
            s.apply(ev)
        return ev

    def subscribe(self, projection):
        for ev in self.events:                     # rattrapage depuis le début du journal
            projection.apply(ev)
        self.subscribers.append(projection)
        return projection


class AvailableSeats:
    """Vue : nombre de places encore disponibles."""

    def __init__(self):
        self.capacity = 0
        self.booked = 0
        self.bookings = {}

    def apply(self, ev: Event):
        d = ev.data
        if ev.type in ("RegistrationOpened", "CapacityChanged"):
            self.capacity = d["capacity"]
        elif ev.type == "SeatsBooked":
            self.bookings[d["booking"]] = d["seats"]
            self.booked += d["seats"]
        elif ev.type == "BookingCancelled":
            self.booked -= self.bookings.pop(d["booking"])

    @property
    def available(self) -> int:
        return self.capacity - self.booked


class BookingStatus:
    """Vue : état de chaque réservation."""

    def __init__(self):
        self.status = {}

    def apply(self, ev: Event):
        if ev.type == "SeatsBooked":
            self.status[ev.data["booking"]] = {"client": ev.data["client"], "places": ev.data["seats"],
                                               "etat": "confirmée"}
        elif ev.type == "BookingCancelled":
            self.status[ev.data["booking"]]["etat"] = "annulée"


class DailyDashboard:
    """Vue : réservations et annulations par jour (tableau de bord de l'organisateur)."""

    def __init__(self):
        self.rows = defaultdict(lambda: {"reservations": 0, "places": 0, "annulations": 0})

    def apply(self, ev: Event):
        day = int(ev.ts // 86400)
        if ev.type == "SeatsBooked":
            self.rows[day]["reservations"] += 1
            self.rows[day]["places"] += ev.data["seats"]
        elif ev.type == "BookingCancelled":
            self.rows[day]["annulations"] += 1

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(dict(self.rows), orient="index").sort_index()
