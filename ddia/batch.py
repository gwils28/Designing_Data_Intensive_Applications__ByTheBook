"""Traitement par lots : outils du chapitre « Batch Processing ».

Tout est écrit « à la main », en Python pur (plus numpy pour générer les données), pour
que chaque mécanisme soit lisible :

- données synthétiques : journal d'accès d'un serveur web (format NGINX), utilisateurs et
  événements d'activité, documents textuels, clés asymétriques (loi de Zipf) ;
- outils Unix : pipeline `awk | sort | uniq -c | sort -rn | head` lancé par `subprocess`,
  équivalent en table de hachage, tri externe à mémoire bornée (passes de fusion) ;
- tuyaux et contre-pression : simulation d'un producteur et d'un consommateur reliés par
  un tampon borné ;
- systèmes de fichiers distribués : placement des blocs à la HDFS, localité des tâches,
  mémoire du NameNode, parité XOR, probabilité de perte (réplication, codes d'effacement) ;
- workflows : DAG de jobs, ordonnancement avec relances, chemin critique ; tâches
  préemptées et relancées depuis le début ;
- MapReduce : moteur miniature avec vrais fichiers de shuffle (un fichier trié par couple
  mappeur/réducteur), combineur, tri secondaire, exécution multiprocessus ;
- jointures : tri-fusion, hachage diffusé (broadcast), hachage partitionné, modèle de coût ;
  asymétrie et salage des clés chaudes ;
- moteur dataflow paresseux (`Dataset`) : fusion des opérateurs étroits, shuffle aux
  dépendances larges, lignage et recalcul d'une partition perdue ;
- Pregel (BSP) : PageRank et composantes connexes par passage de messages ;
- sorties : index inversé construit par lots, fichier clé-valeur trié avec index creux,
  publication atomique d'une nouvelle version par lien symbolique.
"""

from __future__ import annotations

import bisect
import heapq
import itertools
import json
import math
import multiprocessing
import os
import shutil
import subprocess
import tempfile
import time
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import numpy as np

__all__ = [
    "zipf_weights", "make_urls", "generate_access_log", "write_lines", "url_of", "top_k_hash",
    "unix_top_k", "external_sort", "SortStats", "uniq_count", "merge_passes",
    "simulate_pipe", "namenode_memory", "place_blocks", "rack_of", "schedule_locality",
    "xor_parity", "xor_recover", "p_loss_replication", "p_loss_erasure", "Workflow",
    "expected_time_with_restarts", "simulate_restarts", "simulate_job", "stable_hash",
    "partition_of", "MRStats", "run_mapreduce", "make_users", "make_events", "sort_merge_join",
    "broadcast_hash_join", "partitioned_hash_join", "join_network_cost", "zipf_keys",
    "reducer_loads", "salted_loads", "Dataset", "Engine", "pregel", "pagerank_pregel",
    "connected_components_pregel", "make_documents", "write_sorted_kv", "SortedKVFile",
    "publish_version", "current_version",
]


# =====================================================================================
# Données synthétiques
# =====================================================================================

def zipf_weights(n: int, s: float = 1.0) -> np.ndarray:
    """Poids normalisés d'une loi de Zipf : le k-ième élément a un poids en 1/k^s."""
    w = 1.0 / np.arange(1, n + 1) ** s
    return w / w.sum()


_WORDS = ["distributed", "locking", "consensus", "replication", "streams", "batch", "crdt",
          "local", "first", "logs", "clocks", "byzantine", "isolation", "snapshot", "sharding",
          "encoding", "schema", "evolution", "index", "btree", "lsm", "compaction", "raft",
          "paxos", "quorum", "leases", "fencing", "tokens", "elliptic", "curves", "privacy",
          "ethics", "automerge", "collaboration", "offline", "sync", "join", "shuffle"]


def make_urls(n: int, seed: int = 0) -> List[str]:
    """`n` chemins d'URL plausibles : quelques ressources statiques puis des articles datés."""
    rng = np.random.default_rng(seed)
    fixed = ["/", "/favicon.ico", "/css/typography.css", "/css/pygments.css", "/feed.xml",
             "/robots.txt", "/papers/", "/talks/", "/contact.html", "/img/profile.jpg"]
    urls = list(fixed[: min(n, len(fixed))])
    seen = set(urls)
    while len(urls) < n:
        y, m, d = rng.integers(2010, 2023), rng.integers(1, 13), rng.integers(1, 29)
        k = rng.integers(2, 5)
        slug = "-".join(rng.choice(_WORDS, size=k, replace=False))
        u = f"/{y}/{m:02d}/{d:02d}/{slug}.html"
        if u not in seen:
            seen.add(u)
            urls.append(u)
    # les ressources statiques ne sont pas forcément en tête de popularité : on mélange
    # un peu l'ordre tout en gardant « / » et la favicon parmi les plus demandées
    head, tail = urls[:2], urls[2:]
    perm = rng.permutation(len(tail))
    return head + [tail[i] for i in perm]


_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]


def generate_access_log(n_lines: int, n_urls: int = 2000, s: float = 1.1, n_ips: int = 5000,
                        seed: int = 0, day: str = "15/Nov/2022") -> List[str]:
    """Journal d'accès au format « combined » de NGINX (une chaîne par ligne).

    Les URL suivent une loi de Zipf d'exposant `s` (quelques pages très demandées, une
    longue traîne), les adresses IP aussi (exposant 0,9). Le 7e champ séparé par des
    espaces est l'URL demandée, comme dans le livre.
    """
    rng = np.random.default_rng(seed)
    urls = make_urls(n_urls, seed)
    u_idx = rng.choice(n_urls, size=n_lines, p=zipf_weights(n_urls, s))
    ip_pool = [f"{a}.{b}.{c}.{d}" for a, b, c, d in rng.integers(1, 255, size=(n_ips, 4))]
    ip_idx = rng.choice(n_ips, size=n_lines, p=zipf_weights(n_ips, 0.9))
    secs = np.sort(rng.integers(0, 86400, size=n_lines))
    status = rng.choice([200, 200, 200, 200, 200, 200, 304, 404], size=n_lines)
    sizes = rng.integers(200, 40000, size=n_lines)
    agents = rng.integers(0, len(_AGENTS), size=n_lines)
    lines = []
    for i in range(n_lines):
        t = int(secs[i])
        ts = f"[{day}:{t // 3600:02d}:{t // 60 % 60:02d}:{t % 60:02d} +0000]"
        url = urls[u_idx[i]]
        st = int(status[i])
        size = 0 if st == 304 else int(sizes[i])
        lines.append(f'{ip_pool[ip_idx[i]]} - - {ts} "GET {url} HTTP/1.1" {st} {size} '
                     f'"https://example.org/" "{_AGENTS[agents[i]]}"')
    return lines


def write_lines(path: str, lines: Iterable[str]) -> int:
    """Écrit les lignes dans un fichier texte ; renvoie la taille en octets."""
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(line)
            f.write("\n")
    return os.path.getsize(path)


def url_of(line: str) -> str:
    """URL demandée : 7e champ séparé par des espaces (indice 6)."""
    return line.split()[6]


def top_k_hash(lines: Iterable[str], k: int = 5, field: int = 6) -> List[Tuple[int, str]]:
    """Les k valeurs les plus fréquentes du champ `field`, par table de hachage en mémoire.

    En cas d'égalité de compte, l'ordre est celui de `sort -r -n` en locale C (valeur
    décroissante), pour pouvoir comparer avec le pipeline Unix.
    """
    counts: Dict[str, int] = defaultdict(int)
    for line in lines:
        counts[line.split()[field]] += 1
    return sorted(((c, u) for u, c in counts.items()), reverse=True)[:k]


def unix_top_k(path: str, k: int = 5, awk_prog: str = "{print $7}",
               sort_opts: str = "") -> Tuple[List[Tuple[int, str]], float]:
    """Lance le vrai pipeline Unix du livre sur `path` ; renvoie (résultat, durée en s).

    `sort_opts` est passé au premier `sort` (par ex. "-S 1M" pour borner sa mémoire).
    La locale C rend le tri déterministe et rapide (comparaison octet par octet).
    """
    cmd = (f"cat {path} | awk '{awk_prog}' | sort {sort_opts} | uniq -c | "
           f"sort -r -n | head -n {k}")
    env = dict(os.environ, LC_ALL="C")
    t0 = time.perf_counter()
    out = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, env=env).stdout
    dt = time.perf_counter() - t0
    res = []
    for line in out.strip().splitlines():
        c, u = line.split(None, 1)
        res.append((int(c), u))
    return res, dt


# =====================================================================================
# Tri externe
# =====================================================================================

@dataclass
class SortStats:
    """Bilan d'un tri externe."""
    n_records: int = 0
    n_runs: int = 0              # segments triés écrits lors de la première phase
    merge_passes: int = 0        # passes de fusion (la dernière comprise)
    max_in_memory: int = 0       # nombre maximal d'éléments gardés en mémoire
    records_written: int = 0     # éléments écrits sur disque (toutes passes)
    records_read: int = 0        # éléments relus depuis le disque


def merge_passes(n_runs: int, fan_in: int) -> int:
    """Nombre de passes de fusion pour réduire `n_runs` segments avec `fan_in` voies."""
    if n_runs <= 1:
        return 0
    return math.ceil(math.log(n_runs) / math.log(fan_in) - 1e-12)


def _write_run(path: str, items: Sequence[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for x in items:
            f.write(x)
            f.write("\n")


def _read_run(path: str, stats: SortStats) -> Iterator[str]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            stats.records_read += 1
            yield line[:-1]


def external_sort(items: Iterable[str], max_in_memory: int, fan_in: int = 8,
                  tmpdir: Optional[str] = None) -> Tuple[List[str], SortStats]:
    """Tri externe de chaînes (sans retour à la ligne) avec au plus `max_in_memory` en RAM.

    Phase 1 : on lit des paquets de `max_in_memory` éléments, on les trie en mémoire et on
    les écrit comme segments triés (« runs »). Phase 2 : on fusionne au plus `fan_in`
    segments à la fois (fusion à k voies par tas), en plusieurs passes si nécessaire.
    Renvoie la liste triée (relue depuis le dernier segment) et les statistiques.
    """
    if max_in_memory < 1 or fan_in < 2:
        raise ValueError("max_in_memory >= 1 et fan_in >= 2")
    own = tmpdir is None
    tmpdir = tmpdir or tempfile.mkdtemp(prefix="extsort_")
    stats = SortStats()
    runs: List[str] = []
    buf: List[str] = []

    def flush():
        buf.sort()
        p = os.path.join(tmpdir, f"run_{len(runs):05d}.txt")
        _write_run(p, buf)
        stats.records_written += len(buf)
        runs.append(p)
        buf.clear()

    for x in items:
        buf.append(x)
        stats.n_records += 1
        stats.max_in_memory = max(stats.max_in_memory, len(buf))
        if len(buf) >= max_in_memory:
            flush()
    if buf or not runs:
        flush()
    stats.n_runs = len(runs)
    gen = 0
    while len(runs) > 1:
        gen += 1
        new_runs = []
        for g in range(0, len(runs), fan_in):
            group = runs[g:g + fan_in]
            if len(group) == 1:
                new_runs.append(group[0])
                continue
            p = os.path.join(tmpdir, f"merge_{gen}_{g:05d}.txt")
            with open(p, "w", encoding="utf-8") as f:
                for x in heapq.merge(*[_read_run(r, stats) for r in group]):
                    f.write(x)
                    f.write("\n")
                    stats.records_written += 1
            new_runs.append(p)
        runs = new_runs
        stats.merge_passes += 1
    out = [line[:-1] for line in open(runs[0], encoding="utf-8")]
    if own:
        shutil.rmtree(tmpdir, ignore_errors=True)
    return out, stats


def uniq_count(sorted_items: Iterable[Any]) -> Iterator[Tuple[Any, int]]:
    """Équivalent de `uniq -c` : compte les répétitions **adjacentes** d'un flux trié."""
    for key, grp in itertools.groupby(sorted_items):
        yield key, sum(1 for _ in grp)


# =====================================================================================
# Tuyaux et contre-pression
# =====================================================================================

def simulate_pipe(produce: Sequence[float], consume: Sequence[float], buffer_size: int) -> Dict[str, np.ndarray]:
    """Producteur et consommateur reliés par un tampon de `buffer_size` éléments.

    produce[i] : temps pour fabriquer l'élément i ; consume[i] : temps pour le traiter.
    Le producteur se bloque quand le tampon est plein (contre-pression) ; le consommateur
    attend quand il est vide. Renvoie les instants de dépôt (`put`), de prise (`take`), de
    fin de traitement (`done`) et le temps total passé bloqué par le producteur.
    """
    n = len(produce)
    put, take, done = np.zeros(n), np.zeros(n), np.zeros(n)
    blocked = 0.0
    t_prod = 0.0
    for i in range(n):
        ready = t_prod + produce[i]
        slot = take[i - buffer_size] if i >= buffer_size else 0.0   # place libérée
        put[i] = max(ready, slot)
        blocked += put[i] - ready
        t_prod = put[i]
        take[i] = max(put[i], done[i - 1] if i else 0.0)
        done[i] = take[i] + consume[i]
    return {"put": put, "take": take, "done": done, "producer_blocked": np.array(blocked)}


# =====================================================================================
# Systèmes de fichiers distribués
# =====================================================================================

def namenode_memory(total_bytes: float, block_size: float, file_size: float,
                    bytes_per_object: int = 150) -> float:
    """Mémoire de métadonnées (octets) : ~150 octets par fichier et par bloc (règle HDFS)."""
    n_files = total_bytes / file_size
    blocks_per_file = math.ceil(file_size / block_size)
    return n_files * (1 + blocks_per_file) * bytes_per_object


def rack_of(node: int, nodes_per_rack: int) -> int:
    return node // nodes_per_rack


def place_blocks(n_blocks: int, n_nodes: int, replication: int = 3, nodes_per_rack: int = 10,
                 seed: int = 0) -> np.ndarray:
    """Placement des répliques à la HDFS : 1re réplique sur un nœud au hasard, 2e sur un
    autre rack, 3e sur le même rack que la 2e (autre nœud), suivantes au hasard.

    Renvoie un tableau (n_blocks, replication) d'identifiants de nœuds.
    """
    rng = np.random.default_rng(seed)
    n_racks = math.ceil(n_nodes / nodes_per_rack)
    out = np.zeros((n_blocks, replication), dtype=int)
    for b in range(n_blocks):
        first = int(rng.integers(n_nodes))
        chosen = [first]
        if replication >= 2 and n_racks > 1:
            r1 = rack_of(first, nodes_per_rack)
            others = [n for n in range(n_nodes) if rack_of(n, nodes_per_rack) != r1]
            second = int(rng.choice(others))
            chosen.append(second)
            if replication >= 3:
                r2 = rack_of(second, nodes_per_rack)
                same = [n for n in range(n_nodes) if rack_of(n, nodes_per_rack) == r2 and n not in chosen]
                if same:
                    chosen.append(int(rng.choice(same)))
        while len(chosen) < replication:
            c = int(rng.integers(n_nodes))
            if c not in chosen:
                chosen.append(c)
        out[b] = chosen
    return out


def schedule_locality(placement: np.ndarray, n_nodes: int, slots_per_node: int,
                      busy_fraction: float = 0.0, nodes_per_rack: int = 10, seed: int = 0) -> Dict[str, float]:
    """Affecte une tâche par bloc en préférant un nœud qui détient une réplique.

    Une fraction `busy_fraction` des emplacements est déjà occupée par d'autres jobs.
    Renvoie la part de tâches locales au nœud, locales au rack et distantes.
    """
    rng = np.random.default_rng(seed)
    free = np.full(n_nodes, slots_per_node)
    occupied = rng.random((n_nodes, slots_per_node)) < busy_fraction
    free -= occupied.sum(axis=1)
    counts = {"nœud": 0, "rack": 0, "distant": 0}
    order = rng.permutation(len(placement))
    for b in order:
        reps = [int(r) for r in placement[b] if free[r] > 0]
        if reps:
            free[reps[0]] -= 1
            counts["nœud"] += 1
            continue
        racks = {rack_of(int(r), nodes_per_rack) for r in placement[b]}
        cands = [n for n in range(n_nodes) if free[n] > 0 and rack_of(n, nodes_per_rack) in racks]
        if cands:
            free[cands[0]] -= 1
            counts["rack"] += 1
            continue
        cands = np.flatnonzero(free > 0)
        if len(cands):
            free[cands[0]] -= 1
        counts["distant"] += 1          # (ou en attente si le cluster est plein)
    tot = sum(counts.values())
    return {k: v / tot for k, v in counts.items()}


def xor_parity(blocks: Sequence[bytes]) -> bytes:
    """Bloc de parité : XOR octet à octet de blocs de même taille (RAID 5, code (k, 1))."""
    arr = np.frombuffer(blocks[0], dtype=np.uint8).copy()
    for b in blocks[1:]:
        arr ^= np.frombuffer(b, dtype=np.uint8)
    return arr.tobytes()


def xor_recover(surviving: Sequence[bytes], parity: bytes) -> bytes:
    """Reconstruit l'unique bloc manquant à partir des survivants et de la parité."""
    return xor_parity(list(surviving) + [parity])


def p_loss_replication(p: float, r: int) -> float:
    """Probabilité de perdre un bloc à r copies si chaque copie est perdue avec proba p
    (indépendamment) avant d'avoir été réparée."""
    return p ** r


def p_loss_erasure(p: float, k: int, m: int) -> float:
    """Code d'effacement (k données + m parités) : perte si plus de m fragments sur k+m."""
    n = k + m
    return float(sum(math.comb(n, i) * p ** i * (1 - p) ** (n - i) for i in range(m + 1, n + 1)))


# =====================================================================================
# Workflows de jobs et pannes
# =====================================================================================

class Workflow:
    """DAG de jobs : chaque job a une durée et des dépendances (jobs dont il lit la sortie)."""

    def __init__(self):
        self.duration: Dict[str, float] = {}
        self.deps: Dict[str, List[str]] = {}

    def add(self, name: str, duration: float, deps: Sequence[str] = ()) -> "Workflow":
        for d in deps:
            if d not in self.duration:
                raise ValueError(f"dépendance inconnue : {d}")
        self.duration[name] = float(duration)
        self.deps[name] = list(deps)
        return self

    def topo_order(self) -> List[str]:
        """Ordre topologique (algorithme de Kahn, stable dans l'ordre d'ajout)."""
        indeg = {j: len(d) for j, d in self.deps.items()}
        children = defaultdict(list)
        for j, ds in self.deps.items():
            for d in ds:
                children[d].append(j)
        ready = [j for j in self.duration if indeg[j] == 0]
        out = []
        while ready:
            j = ready.pop(0)
            out.append(j)
            for c in children[j]:
                indeg[c] -= 1
                if indeg[c] == 0:
                    ready.append(c)
        if len(out) != len(self.duration):
            raise ValueError("le graphe contient un cycle")
        return out

    def downstream(self, job: str) -> List[str]:
        """Jobs qui dépendent (directement ou non) de `job`."""
        res, todo = [], [job]
        while todo:
            x = todo.pop()
            for j, ds in self.deps.items():
                if x in ds and j not in res:
                    res.append(j)
                    todo.append(j)
        return [j for j in self.topo_order() if j in res]

    def schedule(self, slots: Optional[int] = None, attempts: Optional[Dict[str, int]] = None,
                 retry_delay: float = 0.0) -> List[Dict[str, Any]]:
        """Simule l'exécution : un job démarre quand toutes ses entrées existent et qu'un
        emplacement est libre. `attempts[j]` = nombre d'échecs avant succès (chaque échec
        consomme toute la durée du job, puis `retry_delay`).

        Renvoie une ligne par tentative : job, tentative, début, fin, succès.
        """
        attempts = attempts or {}
        slots = slots or len(self.duration)
        finish: Dict[str, float] = {}
        running: List[Tuple[float, str]] = []        # (fin, job)
        free_at = [0.0] * slots
        rows = []
        for j in self.topo_order():
            # simple ordonnancement de liste : l'ordre topologique est la priorité
            ready = max([finish[d] for d in self.deps[j]], default=0.0)
            k = int(np.argmin(free_at))
            t = max(ready, free_at[k])
            for a in range(attempts.get(j, 0)):
                rows.append(dict(job=j, tentative=a + 1, debut=t, fin=t + self.duration[j], succes=False))
                t += self.duration[j] + retry_delay
            rows.append(dict(job=j, tentative=attempts.get(j, 0) + 1, debut=t,
                             fin=t + self.duration[j], succes=True))
            finish[j] = t + self.duration[j]
            free_at[k] = finish[j]
        return rows

    def critical_path(self) -> Tuple[List[str], float]:
        """Plus long chemin du DAG (durée minimale du workflow avec slots illimités)."""
        best: Dict[str, Tuple[float, Optional[str]]] = {}
        for j in self.topo_order():
            prev = max(((best[d][0], d) for d in self.deps[j]), default=(0.0, None))
            best[j] = (prev[0] + self.duration[j], prev[1])
        end = max(best, key=lambda j: best[j][0])
        path, x = [], end
        while x is not None:
            path.append(x)
            x = best[x][1]
        return path[::-1], best[end][0]
