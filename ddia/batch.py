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
