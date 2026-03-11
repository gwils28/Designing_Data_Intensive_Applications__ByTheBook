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


class BadgeList:
    """Vue : badges à imprimer (une ligne par place des réservations actives)."""

    def __init__(self):
        self.by_booking = {}

    def apply(self, ev: Event):
        if ev.type == "SeatsBooked":
            self.by_booking[ev.data["booking"]] = [f"{ev.data['client']} #{k + 1}" for k in range(ev.data["seats"])]
        elif ev.type == "BookingCancelled":
            self.by_booking.pop(ev.data["booking"], None)

    @property
    def badges(self) -> list[str]:
        return [b for lst in self.by_booking.values() for b in lst]


class ConferenceService:
    """Traitement des commandes : une commande est validée avant de devenir un événement.

    Seules les commandes valides produisent un événement (nommé au passé) : le journal
    ne contient donc que des faits, qu'aucune projection n'a le droit de refuser.
    """

    def __init__(self, log: EventLog | None = None):
        self.log = log or EventLog()
        self.state = self.log.subscribe(AvailableSeats())      # modèle d'écriture minimal
        self.rejected = []

    def handle(self, cmd: dict, ts: float) -> bool:
        kind = cmd["cmd"]
        st = self.state
        if kind == "open":
            self.log.append("RegistrationOpened", {"capacity": cmd["capacity"]}, ts)
        elif kind == "book":
            if cmd["booking"] in st.bookings or cmd["seats"] > st.available:
                self.rejected.append((ts, cmd))
                return False
            self.log.append("SeatsBooked", {"booking": cmd["booking"], "client": cmd["client"],
                                            "seats": cmd["seats"]}, ts)
        elif kind == "cancel":
            if cmd["booking"] not in st.bookings:
                self.rejected.append((ts, cmd))
                return False
            self.log.append("BookingCancelled", {"booking": cmd["booking"]}, ts)
        elif kind == "capacity":
            if cmd["capacity"] < st.booked:
                self.rejected.append((ts, cmd))
                return False
            self.log.append("CapacityChanged", {"capacity": cmd["capacity"]}, ts)
        else:
            raise ValueError(kind)
        return True


def replay(events, projection):
    """Reconstruit une projection en rejouant les événements dans l'ordre donné."""
    for ev in events:
        projection.apply(ev)
    return projection


def make_conference_commands(n: int = 2000, capacity: int = 500, seed: int = 0, days: int = 60):
    """Flux de commandes réaliste : ouverture, réservations (individuelles ou par lots),
    annulations, et un changement de salle à mi-parcours. Renvoie [(ts, commande)]."""
    rng = np.random.default_rng(seed)
    cmds = [(0.0, {"cmd": "open", "capacity": capacity})]
    ts = np.sort(rng.uniform(3600, days * 86400, n))
    made = []
    clients = ["Acme", "Globex", "Initech", "Umbrella", "Hooli", "Stark", "Wayne", "Cyberdyne"]
    for i, t in enumerate(ts):
        if i == n // 2:
            cmds.append((float(t), {"cmd": "capacity", "capacity": int(capacity * 1.5)}))
            continue
        if made and rng.random() < 0.12:
            b = made[int(rng.integers(len(made)))]
            cmds.append((float(t), {"cmd": "cancel", "booking": b}))
        else:
            bulk = rng.random() < 0.1
            seats = int(rng.integers(5, 30)) if bulk else 1
            client = str(rng.choice(clients)) if bulk else f"participant {i}"
            b = f"R{i:05d}"
            made.append(b)
            cmds.append((float(t), {"cmd": "book", "booking": b, "client": client, "seats": seats}))
    return cmds


# =====================================================================================
# 4. DataFrames, matrices creuses
# =====================================================================================

def make_ratings(n_users: int = 2000, n_movies: int = 500, per_user: float = 25, seed: int = 0) -> pd.DataFrame:
    """Table relationnelle de notes (1 à 5) : user_id, movie_id, rating, date.

    Popularité des films en loi de Zipf ; deux « goûts » latents rendent les notes
    corrélées (utile pour la recommandation).
    """
    rng = np.random.default_rng(seed)
    pop = 1.0 / np.arange(1, n_movies + 1) ** 0.9
    pop /= pop.sum()
    taste_u = rng.normal(size=(n_users, 2))
    taste_m = rng.normal(size=(n_movies, 2))
    rows = []
    for u in range(n_users):
        k = max(1, int(rng.poisson(per_user)))
        movies = rng.choice(n_movies, size=min(k, n_movies), replace=False, p=pop)
        for m in movies:
            score = 3 + 0.9 * taste_u[u] @ taste_m[m] + rng.normal(scale=0.6)
            rows.append((u, int(m), int(np.clip(np.rint(score), 1, 5)), int(rng.integers(0, 3 * 365))))
    df = pd.DataFrame(rows, columns=["user_id", "movie_id", "rating", "jour"])
    return df


def to_csr(rows, cols, vals, shape):
    """Matrice creuse au format CSR (Compressed Sparse Row) à partir de triplets (i, j, v).

    Renvoie (indptr, indices, data) : les valeurs non nulles de la ligne i sont
    data[indptr[i]:indptr[i + 1]], en colonnes indices[indptr[i]:indptr[i + 1]].
    """
    rows, cols, vals = np.asarray(rows), np.asarray(cols), np.asarray(vals, dtype=np.float32)
    order = np.lexsort((cols, rows))
    rows, cols, vals = rows[order], cols[order], vals[order]
    indptr = np.zeros(shape[0] + 1, dtype=np.int64)
    np.add.at(indptr, rows + 1, 1)
    return np.cumsum(indptr), cols.astype(np.int32), vals


def csr_matvec(csr, x):
    """Produit matrice creuse x vecteur dense."""
    indptr, indices, data = csr
    out = np.zeros(len(indptr) - 1)
    prod = data * x[indices]
    nz = np.diff(indptr) > 0
    out[nz] = np.add.reduceat(prod, indptr[:-1][nz])
    return out


def csr_nbytes(csr) -> int:
    return sum(a.nbytes for a in csr)


def cosine_top_k(matrix: np.ndarray, j: int, k: int = 5) -> list[tuple[int, float]]:
    """Les k colonnes les plus similaires (cosinus) à la colonne j d'une matrice dense."""
    norms = np.linalg.norm(matrix, axis=0)
    sims = matrix.T @ matrix[:, j] / np.where(norms * norms[j] == 0, 1, norms * norms[j])
    sims[j] = -np.inf
    best = np.argsort(-sims)[:k]
    return [(int(b), float(sims[b])) for b in best]
