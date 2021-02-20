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


@dataclass
class Blocked:
    """Résultat d'une opération qui doit attendre un verrou détenu par `waits_for`."""
    waits_for: set


class TxAborted(Exception):
    """La base abandonne la transaction (conflit, interblocage, SSI...)."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class Tx:
    txid: int
    name: str
    level: str
    snap_xmax: int
    snap_active: frozenset
    status: str = ACTIVE
    reads: set = field(default_factory=set)
    preds: list = field(default_factory=list)
    writeset: dict = field(default_factory=dict)      # clé -> (ancienne valeur, nouvelle)
    trace: list = field(default_factory=list)          # pour le graphe de précédence
    held: set = field(default_factory=set)
    commit_seq: int | None = None


def _match(pred, key, val) -> bool:
    if val is None or val is DELETED:
        return False
    try:
        return bool(pred(key, val))
    except Exception:  # noqa: BLE001 (prédicat écrit pour d'autres types de lignes)
        return False


class Engine:
    """Base clé-valeur multiversion.

    level : niveau d'isolation par défaut des transactions.
    detect_lost_updates : en snapshot isolation, abandonner une transaction qui écrit une
    clé modifiée par une transaction concurrente déjà validée (comportement de PostgreSQL ;
    False imite le « repeatable read » de MySQL/InnoDB).
    """

    def __init__(self, initial: dict | None = None, level: str = "read_committed",
                 detect_lost_updates: bool = True):
        if level not in LEVELS:
            raise ValueError(f"niveau inconnu : {level}")
        self.level = level
        self.detect_lost_updates = detect_lost_updates
        self.versions: dict[Any, list[Version]] = {k: [Version(v, 0)] for k, v in (initial or {}).items()}
        self.status: dict[int, str] = {0: COMMITTED}
        self.txs: dict[int, Tx] = {}
        self.next_txid = 1
        self.commit_counter = 0
        self.locks: dict[Any, dict] = {}
        self.pred_locks: list[tuple[int, Callable]] = []

    # ------------------------------------------------------------------ cycle de vie
    def begin(self, name: str | None = None, level: str | None = None) -> Tx:
        level = level or self.level
        txid = self.next_txid
        self.next_txid += 1
        active = frozenset(t for t, s in self.status.items() if s == ACTIVE)
        tx = Tx(txid, name or f"T{txid}", level, txid, active)
        self.status[txid] = ACTIVE
        self.txs[txid] = tx
        return tx

    def commit(self, tx: Tx) -> None:
        if tx.level == "serializable_ssi" and tx.writeset:
            for other in self.txs.values():
                if other.status != COMMITTED or other is tx or self._in_snapshot(tx, other.txid):
                    continue
                for key, (old, new) in other.writeset.items():
                    if key in tx.reads or any(_match(p, key, old) or _match(p, key, new)
                                              for p in tx.preds):
                        self.abort(tx)
                        raise TxAborted(f"SSI : prémisse périmée ({key} écrit par {other.name})")
        self.commit_counter += 1
        tx.commit_seq = self.commit_counter
        tx.status = COMMITTED
        self.status[tx.txid] = COMMITTED
        self._release(tx)

    def abort(self, tx: Tx) -> None:
        tx.status = ABORTED
        self.status[tx.txid] = ABORTED
        self._release(tx)

    # ------------------------------------------------------------------ visibilité
    def _in_snapshot(self, tx: Tx, writer: int) -> bool:
        return writer < tx.snap_xmax and writer not in tx.snap_active

    def _sees(self, tx: Tx, writer: int) -> bool:
        if writer == tx.txid:
            return True
        st = self.status[writer]
        if tx.level in ("none", "read_uncommitted"):
            return st != ABORTED
        if tx.level in ("read_committed", "serializable_2pl"):
            return st == COMMITTED
        return st == COMMITTED and self._in_snapshot(tx, writer)

    def _visible(self, tx: Tx, key):
        """(version visible ou None)."""
        for v in reversed(self.versions.get(key, [])):
            if self._sees(tx, v.xmin):
                return v
        return None

    def _latest_committed(self, key):
        for v in reversed(self.versions.get(key, [])):
            if self.status[v.xmin] == COMMITTED:
                return v
        return None

    @staticmethod
    def _val(v):
        return None if v is None or v.value is DELETED else v.value

    # ------------------------------------------------------------------ verrous
    def _lock(self, tx: Tx, key, mode: str):
        L = self.locks.setdefault(key, {"S": set(), "X": None})
        if mode == "S":
            if L["X"] not in (None, tx.txid):
                return Blocked({L["X"]})
            L["S"].add(tx.txid)
        else:
            others = (L["S"] - {tx.txid}) | ({L["X"]} - {None, tx.txid})
            if others:
                return Blocked(set(others))
            L["X"] = tx.txid
        tx.held.add(key)
        return None

    def _release(self, tx: Tx) -> None:
        for key in tx.held:
            L = self.locks[key]
            L["S"].discard(tx.txid)
            if L["X"] == tx.txid:
                L["X"] = None
        tx.held.clear()
        self.pred_locks = [(t, p) for t, p in self.pred_locks if t != tx.txid]

    def _check_lost_update(self, tx: Tx, key) -> None:
        if tx.level in _SNAPSHOT and (self.detect_lost_updates or tx.level == "serializable_ssi"):
            last = self._latest_committed(key)
            if last is not None and last.xmin != tx.txid and not self._sees(tx, last.xmin):
                self.abort(tx)
                writer = self.txs[last.xmin].name if last.xmin in self.txs else last.xmin
                raise TxAborted(f"mise à jour concurrente de {key} (par {writer})")

    # ------------------------------------------------------------------ opérations
    def read(self, tx: Tx, key):
        if tx.level == "serializable_2pl":
            blk = self._lock(tx, key, "S")
            if blk:
                return blk
        v = self._visible(tx, key)
        tx.reads.add(key)
        tx.trace.append(("r", key, v.xmin if v else None))
        return self._val(v)

    def write(self, tx: Tx, key, value):
        if tx.level != "none":
            blk = self._lock(tx, key, "X")
            if blk:
                return blk
        if tx.level == "serializable_2pl":
            old = self._val(self._latest_committed(key))
            holders = {t for t, p in self.pred_locks
                       if t != tx.txid and (_match(p, key, old) or _match(p, key, value))}
            if holders:
                return Blocked(holders)
        self._check_lost_update(tx, key)
        vs = self.versions.setdefault(key, [])
        old_v = self._latest_committed(key)
        if vs and vs[-1].xmin == tx.txid:
            vs[-1].value = value
        else:
            for v in reversed(vs):
                if self.status[v.xmin] != ABORTED:
                    v.xmax = tx.txid
                    break
            vs.append(Version(value, tx.txid))
        prev_old = tx.writeset.get(key, (self._val(old_v), None))[0]
        tx.writeset[key] = (prev_old, value)
        tx.trace.append(("w", key, tx.txid))
        return None

    def incr(self, tx: Tx, key, delta):
        blk = self._lock(tx, key, "X") if tx.level != "none" else None
        if blk:
            return blk
        self._check_lost_update(tx, key)
        mine = self._visible(tx, key)
        base = mine if (mine is not None and mine.xmin == tx.txid) else self._latest_committed(key)
        new = (self._val(base) or 0) + delta
        tx.reads.add(key)
        tx.trace.append(("r", key, base.xmin if base else None))
        r = self.write(tx, key, new)
        return r if isinstance(r, Blocked) else new

    def read_for_update(self, tx: Tx, key):
        blk = self._lock(tx, key, "X") if tx.level != "none" else None
        if blk:
            return blk
        self._check_lost_update(tx, key)
        if tx.level in _SNAPSHOT:
            return self.read(tx, key)
        v = self._latest_committed(key)
        mine = self._visible(tx, key)
        if mine is not None and mine.xmin == tx.txid:
            v = mine
        tx.reads.add(key)
        tx.trace.append(("r", key, v.xmin if v else None))
        return self._val(v)

    def scan(self, tx: Tx, pred, label="scan", for_update=False):
        if tx.level == "serializable_2pl":
            for key, L in self.locks.items():
                o = L["X"]
                if o is None or o == tx.txid:
                    continue
                cand = [v.value for v in self.versions.get(key, []) if v.xmin == o]
                cand.append(self._val(self._latest_committed(key)))
                if any(_match(pred, key, c) for c in cand):
                    return Blocked({o})
            if (tx.txid, pred) not in self.pred_locks:
                self.pred_locks.append((tx.txid, pred))
        seen = {k: self._visible(tx, k) for k in self.versions}
        result = {k: self._val(v) for k, v in seen.items() if _match(pred, k, self._val(v))}
        if for_update and tx.level != "none":
            for k in result:
                blk = self._lock(tx, k, "X")
                if blk:
                    return blk
            for k in list(result):
                self._check_lost_update(tx, k)
                if tx.level not in _SNAPSHOT:   # read committed : relire la dernière version
                    v = self._latest_committed(k)
                    mine = self._visible(tx, k)
                    if mine is not None and mine.xmin == tx.txid:
                        v = mine
                    if _match(pred, k, self._val(v)):
                        result[k] = self._val(v)
                        seen[k] = v
                    else:
                        del result[k]
        tx.preds.append(pred)
        tx.trace.append(("scan", pred, {k: (v.xmin if v else None) for k, v in seen.items()}))
        return result

    def execute(self, tx: Tx, op):
        kind = op[0]
        if kind == "read":
            return self.read(tx, op[1])
        if kind == "write":
            return self.write(tx, op[1], op[2])
        if kind == "incr":
            return self.incr(tx, op[1], op[2])
        if kind == "read_for_update":
            return self.read_for_update(tx, op[1])
        if kind == "scan":
            return self.scan(tx, op[1], op[2], for_update=len(op) > 3 and op[3])
        if kind == "commit":
            return self.commit(tx)
        if kind == "abort":
            self.abort(tx)
            raise TxAborted("abandon demandé par l'application")
        raise ValueError(f"opération inconnue : {op!r}")

    # ------------------------------------------------------------------ inspection
    def snapshot_state(self) -> dict:
        """État validé courant (sans les clés supprimées)."""
        out = {}
        for k in self.versions:
            v = self._latest_committed(k)
            if v is not None and v.value is not DELETED:
                out[k] = v.value
        return out

    def versions_table(self) -> pd.DataFrame:
        """Toutes les versions physiques, à la PostgreSQL (created_by = xmin, deleted_by = xmax)."""
        rows = []
        for k, vs in self.versions.items():
            for v in vs:
                rows.append(dict(clé=k, valeur=v.value, created_by=v.xmin,
                                 deleted_by=v.xmax if v.xmax is not None else "",
                                 statut_créateur=self.status[v.xmin]))
        return pd.DataFrame(rows)

    def vacuum(self, horizon: bool = False) -> int:
        """Ramasse-miettes : supprime les versions qu'aucune transaction active ne peut plus
        voir (versions avortées, et versions validées masquées pour tous). Renvoie le
        nombre de versions supprimées.

        horizon=True imite PostgreSQL : une version n'est supprimable que si une version
        plus récente est visible de **toutes** les transactions actives (horizon de la plus
        vieille transaction) ; une transaction longue retient alors tout ce qui a été écrit
        depuis son début."""
        active = [t for t in self.txs.values() if t.status == ACTIVE]
        removed = 0
        for k, vs in self.versions.items():
            keep = [v for v in vs if self.status[v.xmin] != ABORTED]
            removed += len(vs) - len(keep)
            if horizon:
                cut = 0
                for i, v in enumerate(keep):
                    if self.status[v.xmin] == COMMITTED and all(self._sees(t, v.xmin) for t in active):
                        cut = i
                removed += cut
                self.versions[k] = keep[cut:]
                continue
            # plus ancienne version encore utile : celle que verrait chaque transaction active,
            # et la dernière version validée (pour les futures transactions)
            needed = set()
            last = None
            for v in keep:
                if self.status[v.xmin] == COMMITTED:
                    last = v
            if last is not None:
                needed.add(id(last))
            for t in active:
                for v in reversed(keep):
                    if self._sees(t, v.xmin):
                        needed.add(id(v))
                        break
            new = [v for v in keep if id(v) in needed or self.status[v.xmin] == ACTIVE]
            removed += len(keep) - len(new)
            self.versions[k] = new
        return removed


# =====================================================================================
# 3. Ordonnanceur : entrelacer des transactions écrites comme des générateurs
# =====================================================================================

def _fmt(op, result=None) -> str:
    kind = op[0]
    if kind == "read":
        return f"r({op[1]})={result}"
    if kind == "read_for_update":
        return f"r⁺({op[1]})={result}"
    if kind == "write":
        if op[2] is DELETED:
            return f"supprime({op[1]})"
        val = str(op[2])
        return f"w({op[1]})" if len(val) > 10 else f"w({op[1]}={val})"
    if kind == "incr":
        return f"{op[1]}+={op[2]}→{result}"
    if kind == "scan":
        n = len(result) if isinstance(result, dict) else "?"
        return f"{op[2]} → {n}"
    if kind == "commit":
        return "commit"
    if kind == "abort":
        return "abort"
    return str(op)


def _short(op) -> str:
    kind = op[0]
    if kind in ("read", "read_for_update"):
        return f"r({op[1]})"
    if kind in ("write", "incr"):
        return f"w({op[1]})"
    if kind == "scan":
        return op[2]
    return kind


@dataclass
class _Slot:
    name: str
    factory: Callable
    level: str | None
    gen: Any = None
    tx: Tx | None = None
    pending: Any = None
    blocked: Blocked | None = None
    done: bool = False
    outcome: str = ""
    reads: list = field(default_factory=list)
    attempts: int = 0
    retries_left: int = 0
    waited: int = 0
    wait_since: int = 0


@dataclass
class RunResult:
    engine: Engine
    history: pd.DataFrame
    outcomes: dict
    reads: dict
    attempts: dict

    @property
    def final(self) -> dict:
        return self.engine.snapshot_state()

    @property
    def committed(self) -> list:
        return [n for n, o in self.outcomes.items() if o == "commit"]

    @property
    def aborted(self) -> list:
        return [n for n, o in self.outcomes.items() if o != "commit"]
