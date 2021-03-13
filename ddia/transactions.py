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


class Scheduler:
    """Exécute des programmes transactionnels pas à pas.

    Un programme est une fonction génératrice sans argument qui « yield » des opérations
    (R, W, S, INC, RFU, SFU, ABORT) et reçoit leur résultat ; la validation a lieu quand
    le générateur se termine (elle occupe un pas). Une opération bloquée attend : elle est
    retentée à chaque libération de verrous (réveil). Un cycle dans le graphe d'attente
    est un interblocage : la transaction qui ferme le cycle est abandonnée.
    """

    def __init__(self, engine: Engine, retries: int = 0):
        self.engine = engine
        self.retries = retries
        self.slots: dict[str, _Slot] = {}
        self.events: list[dict] = []
        self.step_no = 0
        self._wait_counter = 0

    def add(self, name: str, factory: Callable, level: str | None = None) -> None:
        self.slots[name] = _Slot(name, factory, level, retries_left=self.retries)

    # ------------------------------------------------------------------ mécanique
    def _log(self, slot, text, kind, wake=False):
        self.events.append(dict(pas=self.step_no, tx=slot.name,
                                txid=slot.tx.txid if slot.tx else None,
                                op=text, type=kind, réveil=wake))

    def _start(self, slot):
        slot.attempts += 1
        slot.tx = self.engine.begin(slot.name, slot.level)
        slot.gen = slot.factory()
        slot.reads = []
        try:
            slot.pending = next(slot.gen)
        except StopIteration:
            slot.pending = _COMMIT

    def _deadlock(self, start: str):
        """Cycle du graphe d'attente passant par `start` (liste de noms) ou None."""
        by_txid = {s.tx.txid: s for s in self.slots.values() if s.tx is not None}
        parent = {start: None}
        stack = [start]
        while stack:
            n = stack.pop()
            s = self.slots[n]
            if s.blocked is None:
                continue
            for t in s.blocked.waits_for:
                o = by_txid.get(t)
                if o is None or o.tx.status != ACTIVE:
                    continue
                if o.name == start:
                    cycle, m = [], n
                    while m is not None:
                        cycle.append(m)
                        m = parent[m]
                    return cycle
                if o.name not in parent:
                    parent[o.name] = n
                    stack.append(o.name)
        return None

    def _finish(self, slot, outcome):
        slot.outcome = outcome
        if outcome != "commit" and slot.retries_left > 0:
            slot.retries_left -= 1
            slot.tx = None
            slot.blocked = None
            return
        slot.done = True
        slot.blocked = None

    def _exec(self, slot, wake=False) -> str:
        """Exécute l'opération en attente de `slot`. Renvoie 'ok', 'blocked', 'end'."""
        op = slot.pending
        try:
            res = self.engine.execute(slot.tx, op)
        except TxAborted as e:
            self._log(slot, f"ABANDON : {e.reason}", "abort", wake)
            self._finish(slot, f"abort ({e.reason})")
            self._wake()
            return "end"
        if isinstance(res, Blocked):
            first = slot.blocked is None
            slot.blocked = res
            slot.waited += 1
            if first:
                self._wait_counter += 1
                slot.wait_since = self._wait_counter
                holders = ", ".join(sorted(self.engine.txs[t].name for t in res.waits_for))
                self._log(slot, f"{_short(op)} attend {holders}", "wait", wake)
            cycle = self._deadlock(slot.name)
            if cycle:
                # victime : la transaction la plus jeune du cycle (garantit la progression)
                victim = max((self.slots[n] for n in cycle), key=lambda s: s.tx.txid)
                self.engine.abort(victim.tx)
                self._log(victim, "ABANDON : interblocage", "abort", wake)
                self._finish(victim, "abort (interblocage)")
                self._wake()
                return "end" if victim is slot else ("blocked" if slot.blocked else "ok")
            return "blocked"
        slot.blocked = None
        kind = op[0]
        self._log(slot, _fmt(op, res), {"read": "read", "read_for_update": "read", "scan": "read",
                                        "commit": "commit"}.get(kind, "write"), wake)
        if kind == "commit":
            self._finish(slot, "commit")
            self._wake()
            return "end"
        if kind in ("read", "read_for_update", "scan", "incr"):
            slot.reads.append(res)
        try:
            slot.pending = slot.gen.send(res)
        except StopIteration:
            slot.pending = _COMMIT
        return "ok"

    def _wake(self):
        progress = True
        while progress:
            progress = False
            waiting = sorted((s for s in self.slots.values()
                              if s.blocked is not None and not s.done and s.tx is not None),
                             key=lambda s: s.wait_since)
            for s in waiting:
                if s.blocked is not None and not s.done and s.tx is not None:
                    r = self._exec(s, wake=True)
                    if r != "blocked":
                        progress = True
                        break

    def step(self, name: str) -> str:
        """Fait avancer `name` d'une opération. Renvoie 'ok', 'blocked', 'end' ou 'done'."""
        self.step_no += 1
        slot = self.slots[name]
        if slot.done:
            return "done"
        if slot.tx is None:
            self._start(slot)
        if slot.blocked is not None:
            r = self._exec(slot)
            return r
        return self._exec(slot)

    def runnable(self) -> list[str]:
        return [n for n, s in self.slots.items() if not s.done and s.blocked is None]

    def active(self) -> list[str]:
        return [n for n, s in self.slots.items() if not s.done]

    def run(self, schedule=None, max_steps: int = 10_000) -> RunResult:
        for name in (schedule or []):
            self.step(name)
        n = 0
        while self.active() and n < max_steps:
            r = self.runnable()
            if not r:  # tout le monde attend sans cycle détecté : ne devrait pas arriver
                raise RuntimeError("blocage sans interblocage détecté")
            for name in r:
                self.step(name)
            n += 1
        return self.result()

    def result(self) -> RunResult:
        return RunResult(self.engine, pd.DataFrame(self.events),
                         {n: s.outcome for n, s in self.slots.items()},
                         {n: s.reads for n, s in self.slots.items()},
                         {n: s.attempts for n, s in self.slots.items()})


def run(initial: dict, programs: dict, schedule=None, level: str = "read_committed",
        retries: int = 0, detect_lost_updates: bool = True, levels: dict | None = None) -> RunResult:
    """Exécute `programs` ({nom: fonction génératrice}) sur une base initialisée à `initial`.

    schedule : suite de noms (ex. "ABAB" ou ["A", "B"]) donnant l'ordre des pas ; une fois
    épuisée, les transactions restantes avancent à tour de rôle. levels : niveau propre à
    certaines transactions (sinon `level`)."""
    eng = Engine(initial, level=level, detect_lost_updates=detect_lost_updates)
    sch = Scheduler(eng, retries=retries)
    for name, prog in programs.items():
        sch.add(name, prog, (levels or {}).get(name))
    return sch.run(list(schedule) if schedule is not None else None)


# --- analyse ------------------------------------------------------------------------

def serial_results(initial: dict, programs: dict, names=None) -> list:
    """Résultats de toutes les exécutions en série : [(ordre, état final, lectures)]."""
    names = list(names or programs)
    out = []
    for order in itertools.permutations(names):
        sched = []
        for n in order:
            sched += [n] * 200
        res = run(initial, {n: programs[n] for n in order}, sched, level="read_committed")
        out.append((order, res.final, {n: res.reads[n] for n in order}))
    return out


def is_serializable(res: RunResult, initial: dict, programs: dict):
    """Renvoie un ordre série équivalent (état final et valeurs lues identiques pour les
    transactions validées), ou None si l'exécution n'est équivalente à aucun ordre série."""
    names = res.committed
    for order, final, reads in serial_results(initial, programs, names):
        if final == res.final and all(reads[n] == res.reads[n] for n in names):
            return order
    return None


def precedence_graph(res: RunResult):
    """Graphe de précédence (conflits) entre transactions validées.

    Arcs : wr (T lit une version écrite par U : U -> T), ww (U écrit une version que T
    remplace : U -> T), rw (T lit une version que U remplace ensuite : T -> U, dite
    anti-dépendance). Une exécution est sérialisable par conflits ssi le graphe est
    acyclique. Renvoie un networkx.DiGraph (attribut 'kind' sur les arcs).
    """
    import networkx as nx

    eng = res.engine
    committed = {t.txid: t for t in eng.txs.values() if t.status == COMMITTED}
    order = {}   # clé -> liste des xmin validés dans l'ordre des versions
    for k, vs in eng.versions.items():
        order[k] = [v.xmin for v in vs if eng.status[v.xmin] == COMMITTED]
    g = nx.DiGraph()
    g.add_nodes_from(t.name for t in committed.values())

    def add(a, b, kind, key):
        if a == b or a not in committed or b not in committed:
            return
        u, v = committed[a].name, committed[b].name
        if g.has_edge(u, v):
            g[u][v]["kind"] = ", ".join(sorted(set(g[u][v]["kind"].split(", ")) | {kind}))
            g[u][v]["keys"].add(key)
        else:
            g.add_edge(u, v, kind=kind, keys={key})

    def after(key, xmin):
        seq = order.get(key, [])
        if xmin is None:
            return seq
        return seq[seq.index(xmin) + 1:] if xmin in seq else []

    for t in committed.values():
        for item in t.trace:
            if item[0] == "r":
                _, key, xmin = item
                if xmin is not None:
                    add(xmin, t.txid, "wr", key)
                for later in after(key, xmin):
                    add(t.txid, later, "rw", key)
            elif item[0] == "w":
                _, key, _ = item
                seq = order.get(key, [])
                if t.txid in seq:
                    i = seq.index(t.txid)
                    if i > 0:
                        add(seq[i - 1], t.txid, "ww", key)
            elif item[0] == "scan":
                _, pred, seen = item
                for key, xmin in seen.items():
                    vs = {v.xmin: v.value for v in eng.versions.get(key, [])}
                    if xmin is not None and _match(pred, key, vs.get(xmin)):
                        add(xmin, t.txid, "wr", key)
                    old = vs.get(xmin) if xmin is not None else None
                    for later in after(key, xmin):
                        if _match(pred, key, old) or _match(pred, key, vs.get(later)):
                            add(t.txid, later, "rw", key)
                        old = vs.get(later)
    return g


def plot_precedence(g, ax=None, title: str | None = None, pos=None):
    """Dessine un graphe de précédence (arcs étiquetés wr / ww / rw ; arcs d'un cycle en
    rose). Renvoie l'axe."""
    import matplotlib.pyplot as plt
    import networkx as nx
    from matplotlib.patches import FancyArrowPatch

    from .plotting import INDIGO, LIGHT, ROSE, SLATE

    if ax is None:
        _, ax = plt.subplots(figsize=(4.5, 3.2))
    pos = pos or nx.circular_layout(g)
    in_cycle = set()
    for cyc in nx.simple_cycles(g):
        for i in range(len(cyc)):
            in_cycle.add((cyc[i], cyc[(i + 1) % len(cyc)]))
    for u, v, data in g.edges(data=True):
        c = ROSE if (u, v) in in_cycle else SLATE
        rad = 0.25 if g.has_edge(v, u) else 0.0
        ax.add_patch(FancyArrowPatch(pos[u], pos[v], arrowstyle="-|>", mutation_scale=16, color=c,
                                     lw=1.8, connectionstyle=f"arc3,rad={rad}", shrinkA=22, shrinkB=22))
        mx, my = (pos[u][0] + pos[v][0]) / 2, (pos[u][1] + pos[v][1]) / 2
        dx, dy = pos[v][0] - pos[u][0], pos[v][1] - pos[u][1]
        mx, my = mx + dy * rad * 0.5, my - dx * rad * 0.5
        keys = ",".join(sorted(map(str, data.get("keys", []))))
        ax.text(mx, my, f"{data['kind']} ({keys})", fontsize=8, color=c, ha="center", va="center",
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.9))
    for n, (x, y) in pos.items():
        ax.scatter([x], [y], s=900, color=LIGHT[INDIGO], edgecolor=INDIGO, lw=1.8, zorder=3)
        ax.text(x, y, n, ha="center", va="center", fontsize=10, fontweight="bold", color=SLATE, zorder=4)
    ax.set_xlim(-1.5, 1.5)
    ax.set_ylim(-1.4, 1.4)
    ax.set_aspect("equal")
    ax.axis("off")
    if title:
        ax.set_title(title, fontsize=10.5)
    return ax


def plot_history(res: RunResult, title: str | None = None, subtitle: str | None = None,
                 width: float = 11, names=None, t_max=None):
    """Frise d'une exécution : une ligne par transaction, un pas par colonne.
    Lectures en sarcelle, écritures en ambre, validations en vert, attentes en gris,
    abandons en rose."""
    from .plotting import AMBER, GRAY, GREEN, ROSE, TEAL
    from .timelines import SpaceTime

    h = res.history
    names = names or list(res.outcomes)
    t_max = t_max or (h["pas"].max() + 1 if len(h) else 2)
    st = SpaceTime(names, t_max=t_max, title=None, subtitle=subtitle, width=width,
                   time_label="pas d'exécution")
    if title:
        st.ax.set_title(title, pad=18 if subtitle else 6)
    colors = {"read": TEAL, "write": AMBER, "commit": GREEN, "wait": GRAY, "abort": ROSE}
    count: dict = {}
    for _, e in h.iterrows():
        if e["tx"] not in names:
            continue
        key = (e["tx"], e["pas"])
        k = count.get(key, 0)
        count[key] = k + 1
        t = e["pas"] + 0.22 * k
        c = colors[e["type"]]
        if e["type"] == "abort":
            st.event(e["tx"], t, "", kind="crash")
            st.note(t, e["tx"], e["op"].replace("ABANDON : ", "abandon : "), dy=-0.33,
                    color=ROSE, fontsize=7.5)
        elif e["type"] == "wait":
            st.span(e["tx"], t - 0.35, t + 0.35, "", color=GRAY, alpha=0.35)
            st.note(t, e["tx"], e["op"], dy=0.3, color="#64748B", fontsize=7.5)
        else:
            label = e["op"] + (" (réveil)" if e["réveil"] else "")
            st.op(e["tx"], t - 0.32, t + 0.32, "", color=c)
            st.note(t, e["tx"], label, dy=0.3 if (e["pas"] % 2 == 0 or len(names) == 1) else -0.32,
                    color=c, fontsize=7.5)
    return st


# --- charge aléatoire ---------------------------------------------------------------
