"""Transactions : mini-moteurs pédagogiques du chapitre « Transactions ».

Le module contient quatre briques indépendantes :

1. **Journal d'écriture anticipée (WAL)** : enregistrements avec somme de contrôle, record
   de validation, récupération après une panne à n'importe quel octet (`wal_encode`,
   `wal_recover`, `apply_in_place`).
2. **Moteur transactionnel MVCC** (`Engine`) avec plusieurs niveaux d'isolation :
   ``none`` (aucune protection), ``read_uncommitted``, ``read_committed``, ``snapshot``
   (avec ou sans détection des mises à jour perdues), ``serializable_ssi`` (isolation par
   instantané sérialisable, version simplifiée) et ``serializable_2pl`` (verrouillage à
   deux phases avec verrous de prédicat).
3. **Ordonnanceur** (`Scheduler`, `run`) : des transactions écrites comme des générateurs
   Python sont entrelacées selon un ordre imposé (« ABAB... ») ou aléatoire ; attentes de
   verrous, réveils, interblocages et réessais sont gérés. Outils d'analyse : test de
   sérialisabilité par force brute, graphe de précédence, frise d'exécution.
4. **Validation à deux phases (2PC)** sur le simulateur `ddia.sim` (`run_2pc`).

Exemple
-------
>>> def incr(key):
...     def prog():
...         v = yield R(key)
...         yield W(key, v + 1)
...     return prog
>>> res = run({"x": 42}, {"A": incr("x"), "B": incr("x")}, "ABABAB", level="read_committed")
>>> res.final["x"]          # mise à jour perdue : 43 au lieu de 44
43
"""

from __future__ import annotations

import itertools
import random
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd

from .sim import Network, Node

__all__ = [
    "wal_encode", "wal_recover", "apply_in_place", "transfer_txs",
    "R", "W", "D", "S", "INC", "RFU", "SFU", "ABORT", "DELETED",
    "LEVELS", "LEVEL_LABELS", "Engine", "Blocked", "TxAborted", "Scheduler", "RunResult", "run",
    "serial_results", "is_serializable", "precedence_graph", "plot_precedence", "plot_history",
    "simulate_load", "Participant2PC", "Coordinator2PC", "run_2pc",
]

# =====================================================================================
# 1. Journal d'écriture anticipée (atomicité et durabilité sur un seul nœud)
# =====================================================================================


def _record(*fields) -> bytes:
    body = " ".join(str(f) for f in fields)
    return f"{body} {zlib.crc32(body.encode()):08x}\n".encode()


def wal_encode(txs) -> bytes:
    """Encode une suite de transactions en journal.

    txs : liste de (txid, [(clé, valeur), ...]). Chaque écriture donne un enregistrement
    ``txid W clé valeur crc``, puis un enregistrement de validation ``txid C - - crc``.
    L'ordre « d'abord les données, ensuite le record de validation » est ce qui rend la
    validation atomique.
    """
    out = bytearray()
    for txid, writes in txs:
        for k, v in writes:
            out += _record(txid, "W", k, v)
        out += _record(txid, "C", "-", "-")
    return bytes(out)


def wal_recover(initial: dict, log: bytes):
    """Rejoue un journal (éventuellement tronqué ou corrompu) sur l'état initial.

    Seules les transactions dont le record de validation est intact sont appliquées.
    La lecture s'arrête au premier enregistrement illisible (somme de contrôle fausse ou
    ligne incomplète) : c'est la « queue déchirée » d'une écriture interrompue.
    Renvoie (état, liste des txid validés).
    """
    pending: dict[str, list] = {}
    committed = []
    state = dict(initial)
    for line in log.split(b"\n"):
        if not line:
            continue
        try:
            *fields, crc = line.decode().split(" ")
            body = " ".join(fields)
            ok = len(fields) == 4 and int(crc, 16) == zlib.crc32(body.encode())
        except (UnicodeDecodeError, ValueError):
            ok = False
        if not ok:
            break
        txid, kind, k, v = fields
        if kind == "W":
            pending.setdefault(txid, []).append((k, int(v)))
        elif kind == "C":
            for kk, vv in pending.pop(txid, []):
                state[kk] = vv
            committed.append(int(txid))
    return state, committed


def apply_in_place(initial: dict, txs, n_writes: int) -> dict:
    """Moteur naïf sans journal : les écritures modifient directement les données.
    Une panne après `n_writes` écritures laisse ce qui a été écrit, même à moitié."""
    state = dict(initial)
    done = 0
    for _, writes in txs:
        for k, v in writes:
            if done >= n_writes:
                return state
            state[k] = v
            done += 1
    return state


def transfer_txs(n: int = 3, seed: int = 0, accounts=("A", "B", "C"), start: int = 100):
    """Virements aléatoires (txid, [(compte_débité, v1), (compte_crédité, v2)])
    à partir d'un solde initial `start` sur chaque compte. Renvoie (initial, txs)."""
    rng = random.Random(seed)
    state = {a: start for a in accounts}
    txs = []
    for t in range(1, n + 1):
        src, dst = rng.sample(list(accounts), 2)
        amount = rng.randint(1, 30)
        state[src] -= amount
        state[dst] += amount
        txs.append((t, [(src, state[src]), (dst, state[dst])]))
    return {a: start for a in accounts}, txs


# =====================================================================================
# 2. Moteur transactionnel MVCC
# =====================================================================================

class _Deleted:
    def __repr__(self):
        return "<supprimé>"


DELETED = _Deleted()
ACTIVE, COMMITTED, ABORTED = "active", "committed", "aborted"

LEVELS = ["none", "read_uncommitted", "read_committed", "snapshot", "serializable_ssi",
          "serializable_2pl"]
LEVEL_LABELS = {
    "none": "aucune isolation",
    "read_uncommitted": "read uncommitted",
    "read_committed": "read committed",
    "snapshot": "snapshot isolation",
    "snapshot_nodetect": "snapshot (sans détection)",
    "serializable_ssi": "sérialisable (SSI)",
    "serializable_2pl": "sérialisable (2PL)",
}
_SNAPSHOT = ("snapshot", "serializable_ssi")


# --- opérations (ce que les programmes « yield ») -----------------------------------
def R(key):
    """Lecture d'une clé (renvoie None si elle n'existe pas)."""
    return ("read", key)


def W(key, value):
    """Écriture (insertion ou mise à jour) d'une clé."""
    return ("write", key, value)


def D(key):
    """Suppression d'une clé."""
    return ("write", key, DELETED)


def S(pred, label: str = "scan"):
    """Requête par prédicat : renvoie {clé: valeur} des lignes visibles qui satisfont
    pred(clé, valeur). Exemple : S(lambda k, v: k.startswith("doc:") and v, "de garde")."""
    return ("scan", pred, label)


def INC(key, delta=1):
    """Incrément atomique côté base (UPDATE t SET v = v + 1) : verrou exclusif puis lecture
    de la dernière version validée. Renvoie la nouvelle valeur."""
    return ("incr", key, delta)


def RFU(key):
    """SELECT ... FOR UPDATE sur une clé : verrou exclusif puis lecture."""
    return ("read_for_update", key)


def SFU(pred, label: str = "scan for update"):
    """SELECT ... WHERE pred FOR UPDATE : verrouille exclusivement les lignes trouvées
    (mais pas celles qui n'existent pas encore : les fantômes passent au travers)."""
    return ("scan", pred, label, True)


ABORT = ("abort",)
_COMMIT = ("commit",)


@dataclass
class Version:
    value: Any
    xmin: int                    # transaction qui a créé la version
    xmax: int | None = None      # transaction qui l'a remplacée (affichage seulement)
