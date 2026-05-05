"""Traitement de flux : compléments propres à la 2e édition.

- **files de lettres mortes** (dead letter queues) face à un message empoisonné ;
- **boîte d'envoi** (outbox pattern) comparée à la double écriture, avec pannes ;
- **effacement cryptographique** (crypto-shredding) d'un journal immuable ;
- **stockage hiérarchisé** d'un journal (disques locaux + stockage objet).

Le chiffrement de `CryptoShredder` est un jouet pédagogique (flux de clé SHA-256 en
mode compteur) : il illustre le principe, il ne doit pas servir en production.
"""

from __future__ import annotations

import hashlib
import json
import random
import sqlite3
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from .streams import PartitionedLog


def simulate_poison(n_messages: int, poison: Tuple[int, ...], policy: str, max_attempts: int = 3,
                    n_steps: Optional[int] = None) -> Dict[str, Any]:
    """Un consommateur traite un message par pas de temps ; un message « empoisonné »
    le fait planter à chaque tentative (il n'est donc jamais acquitté).

    policy "strict"   : ordre strict, le message en échec est retenté indéfiniment
                        en tête de file (blocage) ;
    policy "requeue"  : le message en échec est remis en fin de file (on progresse, mais
                        on gaspille une tentative à chaque passage) ;
    policy "dlq"      : après `max_attempts` échecs, le message part dans la file de
                        lettres mortes, surveillée par un opérateur.
    Renvoie la progression (messages traités cumulés), les tentatives gaspillées et la
    file de lettres mortes.
    """
    n_steps = n_steps or 3 * n_messages
    queue = deque(range(n_messages))
    attempts: Dict[int, int] = {}
    done = 0
    wasted = 0
    dlq: List[int] = []
    progress = []
    wasted_series = []
    for _ in range(n_steps):
        if queue:
            m = queue.popleft()
            if m in poison:
                wasted += 1
                attempts[m] = attempts.get(m, 0) + 1
                if policy == "strict":
                    queue.appendleft(m)
                elif policy == "requeue":
                    queue.append(m)
                elif policy == "dlq":
                    if attempts[m] >= max_attempts:
                        dlq.append(m)
                    else:
                        queue.appendleft(m)
                else:
                    raise ValueError(policy)
            else:
                done += 1
        progress.append(done)
        wasted_series.append(wasted)
    return {"progress": progress, "wasted": wasted, "wasted_series": wasted_series, "dlq": dlq,
            "done": done}


class _Crash(Exception):
    pass


def outbox_vs_dual(n_orders: int, crash_prob: float, mode: str, seed: int = 0) -> Dict[str, int]:
    """Un service enregistre des commandes et publie un événement pour chacune.

    mode "dual"   : INSERT dans la base, validation, puis publication dans le journal ;
                    une panne entre les deux perd l'événement (et une relance du client
                    après une panne *avant* la réponse crée parfois un doublon).
    mode "outbox" : INSERT de la commande et de l'événement (table outbox) dans la même
                    transaction ; un relais lit l'outbox dans l'ordre, publie, puis note
                    sa position. Une panne du relais provoque au pire une republication
                    (au moins une fois), détectable par l'identifiant de l'événement.
    Renvoie les commandes validées, les événements publiés distincts, les événements
    manquants et les doublons.
    """
    rng = random.Random(seed)
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)")
    conn.execute("CREATE TABLE outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT)")
    log = PartitionedLog(1)
    relay_pos = 0

    def relay():
        nonlocal relay_pos
        rows = conn.execute("SELECT seq, event FROM outbox WHERE seq > ? ORDER BY seq", (relay_pos,)).fetchall()
        for seq, ev in rows:
            log.append(None, json.loads(ev))
            if rng.random() < crash_prob:      # panne du relais avant d'avoir noté sa position
                return
            relay_pos = seq

    for i in range(n_orders):
        amount = rng.randint(1, 100)
        if mode == "dual":
            conn.execute("BEGIN")
            conn.execute("INSERT INTO orders VALUES (?, ?)", (i, amount))
            conn.execute("COMMIT")
            if rng.random() < crash_prob:      # panne entre la validation et la publication
                continue
            log.append(None, {"order": i, "amount": amount})
        elif mode == "outbox":
            conn.execute("BEGIN")
            conn.execute("INSERT INTO orders VALUES (?, ?)", (i, amount))
            conn.execute("INSERT INTO outbox (event) VALUES (?)", (json.dumps({"order": i, "amount": amount}),))
            conn.execute("COMMIT")
            if i % 10 == 9:
                relay()
        else:
            raise ValueError(mode)
    if mode == "outbox":
        while relay_pos < conn.execute("SELECT COALESCE(MAX(seq), 0) FROM outbox").fetchone()[0]:
            relay()
    published = [r.value["order"] for r in log.read(0, 0)]
    committed = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    distinct = set(published)
    return {"committed": committed, "published": len(distinct),
            "missing": committed - len(distinct), "duplicates": len(published) - len(distinct)}
