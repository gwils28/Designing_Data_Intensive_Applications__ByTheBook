"""Traitement de flux : courtiers, journaux partitionnés, CDC, event sourcing, fenêtres,
jointures et tolérance aux pannes.

Fonctions réutilisables des notebooks du chapitre « traitement de flux »
(2e édition : chapitre 12 ; 1re édition : chapitre 11) :

- **transmettre des flux** : politiques de débordement (perte, tampon, contre-pression),
  file partagée à la AMQP/JMS avec acquittements et relivraisons (réordonnancement),
  blocage en tête de file d'un journal partitionné ;
- **journal partitionné** à la Kafka : partitionneur murmur2 identique à celui de Kafka,
  segments, offsets, rétention, compaction, groupes de consommateurs et commits d'offsets ;
- **bases de données et flux** : course de la double écriture, capture des changements
  (CDC) sur SQLite par déclencheurs, index plein texte dérivé, compaction de journal ;
- **event sourcing** : magasin d'événements avec contrôle de version optimiste, rejeu,
  instantanés ;
- **traitement** : détection de motifs (CEP), HyperLogLog, percolateur (requêtes
  stockées), fenêtres (fixes, glissantes par saut, glissantes, sessions), compteur à
  filigrane (watermark) et retardataires ;
- **jointures** : flux-flux (recherche et clic), flux-table (enrichissement),
  table-table (timelines matérialisées) ;
- **tolérance aux pannes** : job à état avec pannes injectées (au plus une fois, au
  moins une fois, idempotence, commit atomique), puits protégé par jeton de fencing.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import bisect
import hashlib
import heapq
import json
import math
import random
import sqlite3
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

# =====================================================================================
# 1. Transmettre des flux : débordement, file partagée, blocage en tête de file
# =====================================================================================


def simulate_overflow(produced: Sequence[float], capacity: float, buffer_size: float,
                      policy: str) -> Dict[str, np.ndarray]:
    """Producteur plus rapide que le consommateur, modèle fluide pas à pas.

    produced : messages que le producteur *voudrait* émettre à chaque pas ;
    capacity : messages que le consommateur traite par pas ;
    policy   : "drop" (tampon borné, l'excédent est perdu), "buffer" (tampon non borné)
               ou "backpressure" (tampon borné, le producteur est ralenti et accumule
               son retard chez lui).

    Renvoie les séries : file (dans le courtier), livrés, perdus (cumulés), retard du
    producteur (messages non encore émis, contre-pression), latence estimée (pas).
    """
    produced = np.asarray(produced, dtype=float)
    n = produced.size
    queue = np.zeros(n)
    delivered = np.zeros(n)
    lost = np.zeros(n)
    backlog = np.zeros(n)
    q = b = l = 0.0
    for i in range(n):
        want = produced[i] + b
        if policy == "buffer":
            q += want
            b = 0.0
        elif policy == "drop":
            room = max(0.0, buffer_size - q)
            accepted = min(want, room + capacity)
            l += want - accepted
            q += accepted
            b = 0.0
        elif policy == "backpressure":
            room = max(0.0, buffer_size - q)
            accepted = min(want, room + capacity)
            b = want - accepted
            q += accepted
        else:
            raise ValueError(policy)
        d = min(q, capacity)
        q -= d
        queue[i], delivered[i], lost[i], backlog[i] = q, d, l, b
    latency = (queue + backlog) / capacity
    return {"queue": queue, "delivered": delivered, "lost": lost, "backlog": backlog,
            "latency": latency}


@dataclass
class Processing:
    """Un traitement de message par un consommateur (pour les diagrammes et l'analyse)."""

    msg: int
    consumer: int
    t_start: float
    t_end: float
    ok: bool


def simulate_shared_queue(n_messages: int, n_consumers: int, crash_prob: float = 0.0,
                          service: Tuple[float, float] = (0.8, 1.2), restart_delay: float = 2.0,
                          seed: int = 0) -> List[Processing]:
    """File partagée à la AMQP/JMS : répartition de charge, acquittements, relivraison.

    Les messages 0..n-1 sont dans la file, dans l'ordre. Chaque consommateur libre prend
    le premier message disponible. Avec probabilité `crash_prob`, il tombe en panne en
    cours de traitement : le message, non acquitté, est remis **en tête** de file (comme
    RabbitMQ) et le consommateur redémarre après `restart_delay`.
    Renvoie la liste chronologique des traitements (réussis ou interrompus).
    """
    rng = random.Random(seed)
    pending = deque(range(n_messages))
    out: List[Processing] = []
    # boucle d'événements : le consommateur qui se libère le plus tôt prend un message
    heap = [(0.0, c) for c in range(n_consumers)]
    heapq.heapify(heap)
    retry: List[Tuple[float, int]] = []          # (instant de remise en file, message)
    while pending or retry:
        t, c = heapq.heappop(heap)
        # remettre en tête les messages dont le consommateur a été déclaré mort
        retry.sort()
        while retry and retry[0][0] <= t:
            pending.appendleft(retry.pop(0)[1])
        if not pending:
            nxt = retry[0][0]
            heapq.heappush(heap, (nxt, c))
            continue
        m = pending.popleft()
        d = rng.uniform(*service)
        if rng.random() < crash_prob:
            t_crash = t + d * rng.uniform(0.3, 0.9)
            out.append(Processing(m, c, t, t_crash, False))
            retry.append((t_crash, m))
            heapq.heappush(heap, (t_crash + restart_delay, c))
        else:
            out.append(Processing(m, c, t, t + d, True))
            heapq.heappush(heap, (t + d, c))
    out.sort(key=lambda p: (p.t_end, p.msg))
    return out


def inversions(order: Sequence[int]) -> int:
    """Nombre de paires (i, j) traitées dans l'ordre inverse de leur envoi."""
    order = list(order)
    count = 0
    for i in range(len(order)):
        for j in range(i + 1, len(order)):
            if order[i] > order[j]:
                count += 1
    return count


def hol_latencies(arrivals: np.ndarray, services: np.ndarray, n_workers: int,
                  mode: str, keys: Optional[np.ndarray] = None) -> np.ndarray:
    """Temps de séjour de chaque message (attente + traitement).

    mode "shared"      : file unique, le premier consommateur libre prend le message
                         suivant (style AMQP, ordre non garanti) ;
    mode "partitioned" : chaque message va dans la partition `keys[i] % n_workers`, servie
                         par un seul consommateur dans l'ordre (style journal) : un
                         message lent bloque ceux qui le suivent dans sa partition.
    """
    arrivals = np.asarray(arrivals, float)
    services = np.asarray(services, float)
    n = arrivals.size
    out = np.empty(n)
    if mode == "shared":
        free = [0.0] * n_workers
        heapq.heapify(free)
        for i in range(n):
            f = heapq.heappop(free)
            start = max(f, arrivals[i])
            end = start + services[i]
            heapq.heappush(free, end)
            out[i] = end - arrivals[i]
    elif mode == "partitioned":
        keys = np.arange(n) if keys is None else np.asarray(keys)
        free = np.zeros(n_workers)
        for i in range(n):
            p = int(keys[i]) % n_workers
            start = max(free[p], arrivals[i])
            free[p] = start + services[i]
            out[i] = free[p] - arrivals[i]
    else:
        raise ValueError(mode)
    return out


# =====================================================================================
# 2. Journal partitionné à la Kafka
# =====================================================================================


def murmur2(data: bytes) -> int:
    """Hachage murmur2 32 bits, tel qu'implémenté par le client Java de Kafka."""
    length = len(data)
    seed, m, r = 0x9747B28C, 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[4 * i: 4 * i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length & ~3
    extra = length % 4
    if extra >= 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h & 0xFFFFFFFF


def kafka_partition(key: bytes | str, n_partitions: int) -> int:
    """Partition choisie par le partitionneur par défaut de Kafka pour une clé non nulle."""
    if isinstance(key, str):
        key = key.encode()
    return (murmur2(key) & 0x7FFFFFFF) % n_partitions


@dataclass
class Record:
    offset: int
    key: Any
    value: Any
    timestamp: float = 0.0


class OffsetOutOfRange(Exception):
    """L'offset demandé a été supprimé par la rétention (consommateur trop en retard)."""


class PartitionedLog:
    """Sujet (topic) découpé en partitions ; chaque partition est un journal en ajout seul,
    lui-même découpé en segments de `segment_size` enregistrements.

    - `append(key, value)` choisit la partition par hachage murmur2 de la clé (comme
      Kafka), ou à tour de rôle si la clé est None, et renvoie (partition, offset) ;
    - `read(p, offset, max_records)` lit séquentiellement à partir d'un offset ;
    - `enforce_retention(max_segments)` supprime les plus vieux segments ;
    - `compact(p)` ne garde, dans les segments fermés, que la dernière valeur de chaque
      clé (et supprime les pierres tombales, valeur None) : les offsets sont conservés ;
    - `commit(group, p, offset)` / `committed(group, p)` : offsets des groupes.
    """

    def __init__(self, n_partitions: int = 1, segment_size: int = 1000):
        self.n = n_partitions
        self.segment_size = segment_size
        self.segments: List[List[List[Record]]] = [[[]] for _ in range(n_partitions)]
        self.next_offset = [0] * n_partitions
        self.log_start = [0] * n_partitions
        self._rr = 0
        self.offsets: Dict[Tuple[str, int], int] = {}

    # --- écriture --------------------------------------------------------------------
    def partition_for(self, key) -> int:
        if key is None:
            p = self._rr % self.n
            self._rr += 1
            return p
        return kafka_partition(str(key), self.n)

    def append(self, key, value, timestamp: float = 0.0, partition: Optional[int] = None) -> Tuple[int, int]:
        p = self.partition_for(key) if partition is None else partition
        segs = self.segments[p]
        if len(segs[-1]) >= self.segment_size:
            segs.append([])
        off = self.next_offset[p]
        segs[-1].append(Record(off, key, value, timestamp))
        self.next_offset[p] += 1
        return p, off

    # --- lecture ---------------------------------------------------------------------
    def start_offset(self, p: int) -> int:
        """Premier offset lisible (avancé par la rétention, pas par la compaction)."""
        return self.log_start[p]

    def end_offset(self, p: int) -> int:
        """Offset du prochain message à écrire (« high watermark » d'un journal non répliqué)."""
        return self.next_offset[p]

    def read(self, p: int, offset: int, max_records: Optional[int] = None) -> List[Record]:
        if offset < self.start_offset(p):
            raise OffsetOutOfRange(f"partition {p} : offset {offset} < début {self.start_offset(p)}")
        out: List[Record] = []
        for seg in self.segments[p]:
            if not seg or seg[-1].offset < offset:
                continue
            i = bisect.bisect_left([r.offset for r in seg], offset)
            for r in seg[i:]:
                out.append(r)
                if max_records is not None and len(out) >= max_records:
                    return out
        return out

    def __len__(self) -> int:
        return sum(self.size(p) for p in range(self.n))

    def size(self, p: int) -> int:
        return sum(len(s) for s in self.segments[p])

    def lag(self, group: str, p: int) -> int:
        return self.end_offset(p) - self.committed(group, p)

    # --- rétention et compaction --------------------------------------------------------
    def enforce_retention(self, max_segments: int) -> int:
        """Ne garde que les `max_segments` derniers segments par partition ; renvoie le
        nombre d'enregistrements supprimés."""
        removed = 0
        for p in range(self.n):
            segs = self.segments[p]
            while len(segs) > max_segments:
                removed += len(segs.pop(0))
            self.log_start[p] = next((seg[0].offset for seg in segs if seg), self.next_offset[p])
        return removed

    def compact(self, p: int) -> int:
        """Compaction des segments fermés de la partition p ; renvoie le nombre
        d'enregistrements supprimés. Le segment actif n'est pas touché (comme Kafka)."""
        segs = self.segments[p]
        closed, active = segs[:-1], segs[-1]
        if not closed:
            return 0
        latest: Dict[Any, int] = {}
        for seg in segs:                 # la dernière occurrence peut être dans le segment actif
            for r in seg:
                latest[r.key] = r.offset
        before = sum(len(s) for s in closed)
        kept = [r for seg in closed for r in seg
                if latest[r.key] == r.offset and r.value is not None]
        # regroupement des petits segments
        new = [kept[i:i + self.segment_size] for i in range(0, len(kept), self.segment_size)] or [[]]
        self.segments[p] = new + [active]
        return before - len(kept)

    # --- offsets des groupes de consommateurs ----------------------------------------
    def commit(self, group: str, p: int, offset: int) -> None:
        self.offsets[(group, p)] = offset

    def committed(self, group: str, p: int) -> int:
        return self.offsets.get((group, p), 0)


def range_assign(n_partitions: int, members: Sequence[str]) -> Dict[str, List[int]]:
    """Affectation « range » des partitions aux membres d'un groupe (triés par nom) :
    chaque membre reçoit un bloc contigu ; au-delà de n_partitions membres, les suivants
    restent inactifs."""
    members = sorted(members)
    out: Dict[str, List[int]] = {m: [] for m in members}
    k = len(members)
    base, extra = divmod(n_partitions, k)
    start = 0
    for i, m in enumerate(members):
        cnt = base + (1 if i < extra else 0)
        out[m] = list(range(start, start + cnt))
        start += cnt
    return out


def simulate_offset_commits(n_messages: int, commit_every: int, crash_prob: float,
                            mode: str = "after", seed: int = 0) -> Dict[str, Any]:
    """Consommateur d'une partition qui valide (commit) son offset tous les
    `commit_every` messages et tombe en panne avec probabilité `crash_prob` après
    chaque message. Au redémarrage, il reprend à l'offset validé.

    mode "after"  : traiter puis valider (au moins une fois : doublons possibles) ;
    mode "before" : valider le lot puis le traiter (au plus une fois : pertes possibles).
    Renvoie le nombre de traitements de chaque message, les doublons, les pertes et les
    pannes.
    """
    rng = random.Random(seed)
    times = np.zeros(n_messages, dtype=int)
    committed = 0
    crashes = 0
    pos = 0
    while pos < n_messages:
        if mode == "before":
            batch_end = min(n_messages, pos + commit_every)
            committed = batch_end
            for i in range(pos, batch_end):
                times[i] += 1
                if rng.random() < crash_prob:
                    crashes += 1
                    break
            pos = committed
            continue
        # mode "after"
        times[pos] += 1
        pos += 1
        if pos % commit_every == 0 or pos == n_messages:
            committed = pos
        if pos < n_messages and rng.random() < crash_prob:
            crashes += 1
            pos = committed
    return {"times": times, "duplicates": int((times - 1).clip(min=0).sum()),
            "lost": int((times == 0).sum()), "crashes": crashes}


# =====================================================================================
# 3. Bases de données et flux : double écriture, CDC, compaction
# =====================================================================================


def simulate_dual_writes(n_writes: int, n_keys: int, rate: float, delay: Tuple[float, float] = (1.0, 5.0),
                         fail_prob: float = 0.0, mode: str = "dual", seed: int = 0) -> Dict[str, Any]:
    """Des clients mettent à jour des clés dans une base puis dans un index de recherche.

    mode "dual" : chaque client écrit dans la base, attend l'accusé, puis écrit dans
                  l'index ; chaque écriture subit un délai réseau aléatoire, et l'écriture
                  dans l'index échoue avec probabilité `fail_prob` ;
    mode "cdc"  : le client n'écrit que dans la base ; l'index applique le journal des
                  changements dans l'ordre de validation de la base.
    Renvoie la fraction de clés dont la valeur finale diffère entre base et index.
    """
    rng = random.Random(seed)
    db_writes: List[Tuple[float, int, int]] = []
    idx_writes: List[Tuple[float, int, int]] = []
    t = 0.0
    for v in range(n_writes):
        t += rng.expovariate(rate)
        k = rng.randrange(n_keys)
        t_db = t + rng.uniform(*delay)
        db_writes.append((t_db, k, v))
        if mode == "dual":
            t_ack = t_db + rng.uniform(*delay)
            t_idx = t_ack + rng.uniform(*delay)
            if rng.random() >= fail_prob:
                idx_writes.append((t_idx, k, v))
    db: Dict[int, int] = {}
    for _, k, v in sorted(db_writes):
        db[k] = v
    if mode == "cdc":
        idx = dict(db)
    else:
        idx = {}
        for _, k, v in sorted(idx_writes):
            idx[k] = v
    diverged = sum(1 for k in db if idx.get(k) != db[k])
    return {"db": db, "index": idx, "diverged": diverged, "frac_diverged": diverged / max(1, len(db))}


class CdcDatabase:
    """Base SQLite dont chaque table suivie alimente un journal des changements
    `_changelog` par des déclencheurs (triggers) : c'est une capture des changements
    (CDC) minimale. Le numéro `lsn` du journal joue le rôle de position dans le journal
    de réplication (log sequence number).
    """

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", isolation_level=None)
        self.conn.execute("CREATE TABLE _changelog (lsn INTEGER PRIMARY KEY AUTOINCREMENT, "
                          "tbl TEXT, op TEXT, pk, row TEXT)")
        self.tables: Dict[str, Tuple[str, List[str]]] = {}

    def create_table(self, name: str, pk: str, columns: Sequence[str]) -> None:
        cols = [pk] + [c for c in columns if c != pk]
        self.conn.execute(f"CREATE TABLE {name} ({pk} PRIMARY KEY, " + ", ".join(cols[1:]) + ")")
        self.tables[name] = (pk, cols)
        obj = lambda pref: "json_object(" + ", ".join(f"'{c}', {pref}.{c}" for c in cols) + ")"
        for op, pref in (("INSERT", "NEW"), ("UPDATE", "NEW"), ("DELETE", "OLD")):
            row = "NULL" if op == "DELETE" else obj(pref)
            self.conn.execute(
                f"CREATE TRIGGER {name}_{op.lower()} AFTER {op} ON {name} BEGIN "
                f"INSERT INTO _changelog (tbl, op, pk, row) VALUES ('{name}', '{op.lower()}', "
                f"{pref}.{pk}, {row}); END")

    def execute(self, sql: str, params: Sequence = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def changes(self, since: int = 0, limit: int = -1) -> List[Dict[str, Any]]:
        rows = self.conn.execute("SELECT lsn, tbl, op, pk, row FROM _changelog WHERE lsn > ? "
                                 "ORDER BY lsn LIMIT ?", (since, limit)).fetchall()
        return [{"lsn": l, "table": t, "op": o, "key": k, "row": json.loads(r) if r else None}
                for l, t, o, k, r in rows]

    def snapshot(self, table: str) -> Tuple[int, List[Tuple]]:
        """Instantané cohérent : (position du journal, lignes) lus dans une même transaction."""
        self.conn.execute("BEGIN")
        lsn = self.conn.execute("SELECT COALESCE(MAX(lsn), 0) FROM _changelog").fetchone()[0]
        rows = self.conn.execute(f"SELECT * FROM {table}").fetchall()
        self.conn.execute("COMMIT")
        return lsn, rows


class SearchIndex:
    """Index inversé minimal, système dérivé alimenté par des événements de changement."""

    def __init__(self, text_field: str = "text"):
        self.text_field = text_field
        self.docs: Dict[Any, str] = {}
        self.postings: Dict[str, set] = defaultdict(set)
        self.position = 0          # dernier lsn appliqué

    @staticmethod
    def tokens(text: str) -> List[str]:
        return [t for t in "".join(ch.lower() if ch.isalnum() else " " for ch in text).split() if t]

    def _remove(self, key) -> None:
        old = self.docs.pop(key, None)
        if old is not None:
            for t in set(self.tokens(old)):
                self.postings[t].discard(key)

    def upsert(self, key, text: str) -> None:
        self._remove(key)
        self.docs[key] = text
        for t in set(self.tokens(text)):
            self.postings[t].add(key)

    def delete(self, key) -> None:
        self._remove(key)

    def apply(self, change: Dict[str, Any]) -> None:
        """Applique un événement de changement produit par `CdcDatabase.changes`."""
        if change["op"] == "delete" or change["row"] is None:
            self.delete(change["key"])
        else:
            self.upsert(change["key"], str(change["row"].get(self.text_field, "")))
        self.position = change["lsn"]

    def search(self, term: str) -> List[Any]:
        return sorted(self.postings.get(term.lower(), set()), key=str)


def compact_records(records: Iterable[Tuple[Any, Any]]) -> List[Tuple[int, Any, Any]]:
    """Compaction d'une suite (clé, valeur) : garde la dernière valeur de chaque clé,
    supprime les clés dont la dernière valeur est None (pierre tombale). Renvoie des
    triplets (offset d'origine, clé, valeur) dans l'ordre des offsets."""
    records = list(records)
    last: Dict[Any, int] = {}
    for i, (k, _) in enumerate(records):
        last[k] = i
    return [(i, k, v) for i, (k, v) in enumerate(records) if last[k] == i and v is not None]


# =====================================================================================
# 4. Event sourcing
# =====================================================================================


class ConcurrencyError(Exception):
    """La version attendue du flux ne correspond pas (écriture concurrente)."""


@dataclass
class Event:
    stream: str
    version: int
    type: str
    data: Dict[str, Any]
    position: int = 0          # position dans le journal global


class EventStore:
    """Magasin d'événements en ajout seul, découpé en flux (un par agrégat).

    `append(stream, events, expected_version)` refuse l'écriture si la version courante
    du flux n'est pas celle attendue : c'est un contrôle de concurrence optimiste qui
    suffit tant qu'une commande ne touche qu'un seul flux.
    """

    def __init__(self):
        self.log: List[Event] = []
        self.versions: Dict[str, int] = defaultdict(int)
        self.subscribers: List[Callable[[Event], None]] = []

    def append(self, stream: str, events: Sequence[Tuple[str, Dict[str, Any]]],
               expected_version: Optional[int] = None) -> int:
        cur = self.versions[stream]
        if expected_version is not None and expected_version != cur:
            raise ConcurrencyError(f"{stream} : version {cur}, attendue {expected_version}")
        for typ, data in events:
            cur += 1
            ev = Event(stream, cur, typ, dict(data), len(self.log))
            self.log.append(ev)
            for s in self.subscribers:
                s(ev)
        self.versions[stream] = cur
        return cur

    def read(self, stream: Optional[str] = None, from_position: int = 0) -> List[Event]:
        return [e for e in self.log[from_position:] if stream is None or e.stream == stream]

    def subscribe(self, fn: Callable[[Event], None], from_start: bool = True) -> None:
        if from_start:
            for e in self.log:
                fn(e)
        self.subscribers.append(fn)


def replay(events: Iterable[Event], apply: Callable[[Any, Event], Any], state: Any = None) -> Any:
    """Plie (fold) une suite d'événements : état = apply(...apply(apply(état0, e1), e2)...)."""
    for e in events:
        state = apply(state, e)
    return state


# =====================================================================================
# 5. Usages : CEP, HyperLogLog, percolateur
# =====================================================================================


def detect_brute_force(events: Iterable[Tuple[float, str, str]], n_fail: int = 3,
                       within: float = 60.0) -> List[Tuple[float, str, int]]:
    """Motif CEP : au moins `n_fail` échecs de connexion d'un même utilisateur suivis
    d'un succès, le tout en moins de `within` secondes. Les événements sont des triplets
    (t, utilisateur, "fail" | "ok"), dans l'ordre. Une petite machine à états par
    utilisateur (file des instants d'échec récents) suffit. Renvoie (t, utilisateur,
    nombre d'échecs) pour chaque détection : c'est l'« événement complexe »."""
    fails: Dict[str, deque] = defaultdict(deque)
    out = []
    for t, user, kind in events:
        q = fails[user]
        while q and q[0] < t - within:
            q.popleft()
        if kind == "fail":
            q.append(t)
        elif kind == "ok":
            if len(q) >= n_fail:
                out.append((t, user, len(q)))
            q.clear()
    return out


def hash64(item) -> int:
    """Hachage 64 bits (blake2b) de la représentation textuelle d'un élément."""
    return int.from_bytes(hashlib.blake2b(str(item).encode(), digest_size=8).digest(), "big")


class HyperLogLog:
    """Estimateur de cardinalité HyperLogLog (Flajolet et al., 2007).

    2^p registres ; chacun garde le rang du premier bit à 1 maximal observé parmi les
    hachages qui tombent dans ce registre. Erreur relative typique 1,04 / sqrt(2^p),
    pour une mémoire de 2^p registres de 6 bits, quel que soit le nombre d'éléments.
    """

    def __init__(self, p: int = 12):
        self.p = p
        self.m = 1 << p
        self.reg = np.zeros(self.m, dtype=np.uint8)

    def add(self, item) -> None:
        h = hash64(item)
        j = h >> (64 - self.p)
        w = h & ((1 << (64 - self.p)) - 1)
        rank = (64 - self.p) - w.bit_length() + 1
        if rank > self.reg[j]:
            self.reg[j] = rank

    def add_hashes(self, hashes: np.ndarray) -> None:
        """Version vectorisée de `add` pour des hachages 64 bits déjà calculés (`hash64`)."""
        h = np.asarray(hashes, dtype=np.uint64)
        j = (h >> np.uint64(64 - self.p)).astype(np.int64)
        w = h & np.uint64((1 << (64 - self.p)) - 1)
        bitlen = np.where(w > 0, np.frexp(w.astype(np.float64))[1], 0)
        rank = ((64 - self.p) - bitlen + 1).astype(np.uint8)
        np.maximum.at(self.reg, j, rank)

    def count(self) -> float:
        m = self.m
        alpha = 0.7213 / (1 + 1.079 / m)
        est = alpha * m * m / np.sum(2.0 ** (-self.reg.astype(float)))
        zeros = int(np.sum(self.reg == 0))
        if est <= 2.5 * m and zeros:
            est = m * math.log(m / zeros)          # correction des petites cardinalités
        return float(est)

    def merge(self, other: "HyperLogLog") -> "HyperLogLog":
        out = HyperLogLog(self.p)
        out.reg = np.maximum(self.reg, other.reg)
        return out

    @property
    def memory_bytes(self) -> int:
        return self.m * 6 // 8


class Percolator:
    """Recherche sur un flux : les requêtes sont stockées, les documents passent.

    Une requête est un ensemble de termes qui doivent tous figurer dans le document.
    `match_naive` teste chaque requête ; `match_indexed` n'examine que les requêtes
    indexées sous un de leurs termes (le plus rare, d'après `df`) présent dans le
    document. Les deux renvoient (requêtes satisfaites, nombre de requêtes examinées).
    """

    def __init__(self, df: Optional[Dict[str, float]] = None):
        self.queries: Dict[Any, frozenset] = {}
        self.by_term: Dict[str, List[Any]] = defaultdict(list)
        self.df = df or {}

    def add(self, qid, terms: Iterable[str]) -> None:
        terms = frozenset(terms)
        self.queries[qid] = terms
        rare = min(terms, key=lambda t: (self.df.get(t, 0.0), t))
        self.by_term[rare].append(qid)

    def match_naive(self, doc_terms: Iterable[str]) -> Tuple[List[Any], int]:
        d = set(doc_terms)
        hits = [q for q, ts in self.queries.items() if ts <= d]
        return hits, len(self.queries)

    def match_indexed(self, doc_terms: Iterable[str]) -> Tuple[List[Any], int]:
        d = set(doc_terms)
        cand = [q for t in d for q in self.by_term.get(t, ())]
        hits = [q for q in cand if self.queries[q] <= d]
        return sorted(hits, key=str), len(cand)


# =====================================================================================
# 6. Le temps : fenêtres, filigranes, retardataires
# =====================================================================================


def tumbling_counts(ts: Sequence[float], size: float, origin: float = 0.0) -> Dict[float, int]:
    """Fenêtres fixes (tumbling) : chaque événement dans exactement une fenêtre
    [origin + k*size, origin + (k+1)*size)."""
    out: Dict[float, int] = defaultdict(int)
    for t in ts:
        out[origin + math.floor((t - origin) / size) * size] += 1
    return dict(sorted(out.items()))


def hopping_counts(ts: Sequence[float], size: float, hop: float, origin: float = 0.0) -> Dict[float, int]:
    """Fenêtres glissantes par saut (hopping) de longueur `size` démarrant tous les
    `hop` : calculées comme somme de size/hop fenêtres fixes de longueur `hop`."""
    k = int(round(size / hop))
    if abs(k * hop - size) > 1e-9:
        raise ValueError("size doit être un multiple de hop")
    base = tumbling_counts(ts, hop, origin)
    if not base:
        return {}
    lo, hi = min(base), max(base)
    out = {}
    start = lo - (k - 1) * hop
    while start <= hi + 1e-9:
        c = sum(base.get(round(start + i * hop, 9), 0) for i in range(k))
        if c:
            out[round(start, 9)] = c
        start += hop
    return out


def sliding_counts(ts: Sequence[float], width: float) -> np.ndarray:
    """Fenêtre glissante (sliding) : pour chaque événement, nombre d'événements dont
    l'horodatage est dans ]t - width, t]. Implémentée avec un tampon trié dont on retire
    les événements expirés (les événements sont supposés arriver dans l'ordre)."""
    buf: deque = deque()
    out = np.empty(len(ts), dtype=int)
    for i, t in enumerate(ts):
        buf.append(t)
        while buf[0] <= t - width:
            buf.popleft()
        out[i] = len(buf)
    return out


def sessionize(events: Iterable[Tuple[Any, float]], gap: float) -> List[Tuple[Any, float, float, int]]:
    """Fenêtres de session : regroupe les événements (clé, t) d'une même clé séparés de
    moins de `gap` ; renvoie (clé, début, fin, nombre d'événements), trié par début."""
    by_key: Dict[Any, List[float]] = defaultdict(list)
    for k, t in events:
        by_key[k].append(t)
    out = []
    for k, ts in by_key.items():
        ts.sort()
        start = prev = ts[0]
        n = 1
        for t in ts[1:]:
            if t - prev > gap:
                out.append((k, start, prev, n))
                start, n = t, 0
            prev = t
            n += 1
        out.append((k, start, prev, n))
    out.sort(key=lambda s: (s[1], str(s[0])))
    return out


@dataclass
class WindowResult:
    window: float
    count: int
    emitted_at: float
    kind: str            # "final" ou "correction"


class WatermarkCounter:
    """Compteur par fenêtres fixes en temps d'événement, déclenché par un filigrane.

    Les événements (temps d'événement, temps d'arrivée) sont traités dans l'ordre
    d'arrivée. Le filigrane vaut « plus grand temps d'événement vu - `lateness` » : on
    parie qu'aucun événement plus ancien n'arrivera. Une fenêtre [w, w + size) est émise
    quand le filigrane dépasse w + size. Un événement qui arrive après l'émission de sa
    fenêtre est un **retardataire** : politique "drop" (ignoré, compté) ou "update"
    (correction émise avec la nouvelle valeur).
    """

    def __init__(self, size: float, lateness: float, policy: str = "drop"):
        self.size, self.lateness, self.policy = size, lateness, policy
        self.open: Dict[float, int] = {}
        self.closed: Dict[float, int] = {}
        self.watermark = -math.inf
        self.results: List[WindowResult] = []
        self.dropped = 0
        self.max_open = 0

    def process(self, event_time: float, arrival: float) -> None:
        w = math.floor(event_time / self.size) * self.size
        if w in self.closed:
            if self.policy == "drop":
                self.dropped += 1
            else:
                self.closed[w] += 1
                self.results.append(WindowResult(w, self.closed[w], arrival, "correction"))
        else:
            self.open[w] = self.open.get(w, 0) + 1
        self.watermark = max(self.watermark, event_time - self.lateness)
        for ws in sorted(self.open):
            if ws + self.size <= self.watermark:
                c = self.open.pop(ws)
                self.closed[ws] = c
                self.results.append(WindowResult(ws, c, arrival, "final"))
        self.max_open = max(self.max_open, len(self.open))

    def flush(self, at: float) -> None:
        for ws in sorted(self.open):
            c = self.open.pop(ws)
            self.closed[ws] = c
            self.results.append(WindowResult(ws, c, at, "final"))

    def run(self, event_times: Sequence[float], arrivals: Sequence[float]) -> "WatermarkCounter":
        order = np.argsort(arrivals, kind="stable")
        for i in order:
            self.process(float(event_times[i]), float(arrivals[i]))
        self.flush(float(np.max(arrivals)))
        return self


# =====================================================================================
# 7. Jointures de flux
# =====================================================================================


class SearchClickJoin:
    """Jointure flux-flux (fenêtrée) entre recherches et clics d'une même session.

    Événements (kind, session, t) traités dans l'ordre d'arrivée, kind = "search" ou
    "click". L'état garde, par session, les recherches et les clics de la dernière
    `window` (en temps d'événement). Sorties : ("clicked", session, délai) dès qu'une
    paire est complète, ("not_clicked", session, None) quand une recherche expire sans
    clic. `state_size` suit le nombre d'événements en mémoire.
    """

    def __init__(self, window: float):
        self.window = window
        self.searches: Dict[Any, float] = {}
        self.clicks: Dict[Any, float] = {}
        self.out: List[Tuple[str, Any, Optional[float]]] = []
        self.state_size: List[int] = []
        self.clock = -math.inf
        self._expiry: List[Tuple[float, int, str, Any]] = []     # tas (instant, n°, type, session)
        self._n = 0

    def _expire(self) -> None:
        limit = self.clock - self.window
        while self._expiry and self._expiry[0][0] < limit:
            t, _, kind, s = heapq.heappop(self._expiry)
            if kind == "search" and self.searches.get(s) == t:
                del self.searches[s]
                self.out.append(("not_clicked", s, None))
            elif kind == "click" and self.clicks.get(s) == t:
                del self.clicks[s]

    def process(self, kind: str, session, t: float) -> None:
        self.clock = max(self.clock, t)
        if kind == "search":
            if session in self.clicks:                  # le clic est arrivé avant la recherche
                self.out.append(("clicked", session, self.clicks.pop(session) - t))
            else:
                self.searches[session] = t
                heapq.heappush(self._expiry, (t, self._n, "search", session))
        else:
            if session in self.searches:
                self.out.append(("clicked", session, t - self.searches.pop(session)))
            else:
                self.clicks[session] = t
                heapq.heappush(self._expiry, (t, self._n, "click", session))
        self._n += 1
        self._expire()
        self.state_size.append(len(self.searches) + len(self.clicks))

    def flush(self) -> None:
        for s in list(self.searches):
            self.out.append(("not_clicked", s, None))
        self.searches.clear()
        self.clicks.clear()
        self._expiry.clear()


def enrich(stream: Iterable[Tuple[str, Any, Any]]) -> List[Tuple[Any, Any, Any]]:
    """Jointure flux-table : le flux mêle des changements de la table des profils
    ("profile", user, profil) et des événements d'activité ("activity", user, action),
    dans l'ordre où l'opérateur les reçoit. L'opérateur garde une copie locale de la
    table (fenêtre « depuis l'origine des temps ») et enrichit chaque activité avec le
    profil connu *à cet instant*. Renvoie (user, action, profil)."""
    table: Dict[Any, Any] = {}
    out = []
    for kind, user, payload in stream:
        if kind == "profile":
            if payload is None:
                table.pop(user, None)
            else:
                table[user] = payload
        else:
            out.append((user, payload, table.get(user)))
    return out
