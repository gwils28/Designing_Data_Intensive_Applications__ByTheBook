"""Traitement « exactement une fois » sans transaction distribuée (2e édition).

Un courtier de messages livre « au moins une fois » : tant qu'un message n'est pas
acquitté, il est relivré. Le consommateur applique l'effet du message dans une base
SQLite. Une panne peut survenir à différents points :

- ``avant_commit``      : la transaction locale est annulée, le message sera relivré ;
- ``avant_ack``         : la transaction est validée mais l'acquittement n'est pas parti ;
- ``avant_nettoyage``   : acquitté, mais l'identifiant n'a pas encore été purgé.

Mode ``naif`` : on applique l'effet, on valide, on acquitte (doublons possibles).
Mode ``dedup`` : l'identifiant du message est inséré dans une table ``processed`` dans la
**même transaction** que l'effet (clé primaire) ; un message déjà vu est simplement
acquitté. Le traitement devient idempotent.
"""

from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass, field

__all__ = ["CRASH_POINTS", "Crash", "make_db", "handle", "Broker", "run_stream", "crash_matrix"]

CRASH_POINTS = ("avant_commit", "avant_ack", "avant_nettoyage")


class Crash(Exception):
    """Panne simulée du consommateur."""


def make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("CREATE TABLE account (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)")
    conn.execute("CREATE TABLE processed (msg_id TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO account VALUES (1, 0)")
    return conn


@dataclass
class Broker:
    """File de messages à livraison « au moins une fois »."""
    messages: list                       # [(msg_id, montant)]
    acked: set = field(default_factory=set)
    deliveries: int = 0

    def next(self):
        for m in self.messages:
            if m[0] not in self.acked:
                self.deliveries += 1
                return m
        return None

    def ack(self, msg_id) -> None:
        self.acked.add(msg_id)


def handle(conn: sqlite3.Connection, broker: Broker, msg, mode: str = "dedup",
           crash_at: str | None = None, cleanup: bool = True) -> str:
    """Traite un message. Lève Crash au point demandé. Renvoie 'appliqué' ou 'doublon'."""
    msg_id, amount = msg
    conn.execute("BEGIN")
    if mode == "dedup":
        seen = conn.execute("SELECT 1 FROM processed WHERE msg_id = ?", (msg_id,)).fetchone()
        if seen:
            conn.execute("COMMIT")
            broker.ack(msg_id)
            if cleanup:
                conn.execute("DELETE FROM processed WHERE msg_id = ?", (msg_id,))
            return "doublon"
        conn.execute("INSERT INTO processed VALUES (?)", (msg_id,))
    conn.execute("UPDATE account SET balance = balance + ? WHERE id = 1", (amount,))
    if crash_at == "avant_commit":
        conn.execute("ROLLBACK")
        raise Crash(crash_at)
    conn.execute("COMMIT")
    if crash_at == "avant_ack":
        raise Crash(crash_at)
    broker.ack(msg_id)
    if mode == "dedup" and cleanup:
        if crash_at == "avant_nettoyage":
            raise Crash(crash_at)
        conn.execute("DELETE FROM processed WHERE msg_id = ?", (msg_id,))
    return "appliqué"


def run_stream(n_messages: int = 200, mode: str = "dedup", crash_prob: float = 0.1,
               seed: int = 0, cleanup: bool = True) -> dict:
    """Traite un flux de messages avec des pannes aléatoires du consommateur (à chaque
    tentative, probabilité `crash_prob` de tomber à un point tiré uniformément)."""
    rng = random.Random(seed)
    msgs = [(f"m{i}", rng.randint(1, 100)) for i in range(n_messages)]
    broker = Broker(msgs)
    conn = make_db()
    crashes = {p: 0 for p in CRASH_POINTS}
    while True:
        m = broker.next()
        if m is None:
            break
        points = CRASH_POINTS if mode == "dedup" else CRASH_POINTS[:2]
        point = rng.choice(points) if rng.random() < crash_prob else None
        try:
            handle(conn, broker, m, mode, point, cleanup)
        except Crash as e:
            crashes[str(e)] += 1
    balance = conn.execute("SELECT balance FROM account").fetchone()[0]
    leftover = conn.execute("SELECT COUNT(*) FROM processed").fetchone()[0]
    expected = sum(a for _, a in msgs)
    return dict(mode=mode, attendu=expected, obtenu=balance, écart=balance - expected,
                livraisons=broker.deliveries, ids_restants=leftover, **{f"pannes_{k}": v for k, v in crashes.items()})


def crash_matrix(modes=("naif", "dedup")) -> list:
    """Pour un message unique de montant 10, chaque point de panne suivi d'une relivraison
    sans panne : solde final obtenu (10 = exactement une fois)."""
    rows = []
    for mode in modes:
        for point in (None,) + CRASH_POINTS:
            if mode == "naif" and point == "avant_nettoyage":
                continue
            conn, broker = make_db(), Broker([("m1", 10)])
            try:
                handle(conn, broker, broker.next(), mode, point)
            except Crash:
                pass
            m = broker.next()
            if m is not None:
                handle(conn, broker, m, mode, None)
            bal = conn.execute("SELECT balance FROM account").fetchone()[0]
            rows.append(dict(mode=mode, panne=point or "aucune", livraisons=broker.deliveries,
                             solde=bal, exactement_une_fois=bal == 10))
    return rows
