"""Compromis d'architecture des systèmes de données (chapitre 1 de la 2e édition).

Fonctions réutilisables des notebooks du chapitre :

- systèmes opérationnels et analytiques : base de ventes synthétique sur SQLite, mesure du
  travail d'une requête (instructions de la machine virtuelle SQLite), file FIFO à
  plusieurs serveurs, formules M/M/c ;
- entrepôts et données dérivées : fraction d'historique capturée par des instantanés
  périodiques, ordre de reconstruction des données dérivées (graphe de dérivation) ;
- cloud et auto-hébergement : profil de charge synthétique, coûts comparés (TCO),
  ordres de grandeur des couches de stockage, coût d'un object store, cache LRU, seau à
  jetons (quotas par locataire) ;
- distribué ou mono-nœud : distances et temps d'aller-retour sur fibre, modèle de temps
  d'un cluster et métrique COST, disponibilité d'une chaîne de services, chemin critique
  d'une trace, simulation serverless (démarrages à froid), points de reprise (HPC) ;
- données et droit : journal à effacement cryptographique (« crypto-shredding ») et
  compaction d'un journal.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import hashlib
import heapq
import math
import sqlite3
from collections import OrderedDict

import numpy as np

# ====================================================================================
# 1. Systèmes opérationnels et analytiques
# ====================================================================================

CATEGORIES = ["fruits", "légumes", "épicerie", "boissons", "hygiène", "bébé", "surgelés", "boulangerie"]


def make_shop_db(con: sqlite3.Connection, n_customers: int = 2000, n_products: int = 400,
                 n_stores: int = 20, n_sales: int = 100_000, n_days: int = 90, seed: int = 0) -> None:
    """Crée une base de ventes de supermarché (schéma OLTP simple) dans `con`.

    Tables : customers(id, name, city), stores(id, city), products(id, name, category,
    price), sales(id, customer_id, store_id, product_id, qty, amount, day).
    Index : sales(customer_id), sales(day).
    """
    rng = np.random.default_rng(seed)
    cities = ["Paris", "Lyon", "Lille", "Nantes", "Bordeaux", "Toulouse", "Marseille", "Rennes"]
    con.executescript("""
    DROP TABLE IF EXISTS customers; DROP TABLE IF EXISTS stores;
    DROP TABLE IF EXISTS products;  DROP TABLE IF EXISTS sales;
    CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, city TEXT);
    CREATE TABLE stores    (id INTEGER PRIMARY KEY, city TEXT);
    CREATE TABLE products  (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL);
    CREATE TABLE sales     (id INTEGER PRIMARY KEY, customer_id INTEGER, store_id INTEGER,
                            product_id INTEGER, qty INTEGER, amount REAL, day INTEGER);
    """)
    con.executemany("INSERT INTO customers VALUES (?, ?, ?)",
                    ((i, f"client{i}", cities[int(rng.integers(len(cities)))]) for i in range(n_customers)))
    con.executemany("INSERT INTO stores VALUES (?, ?)", ((i, cities[i % len(cities)]) for i in range(n_stores)))
    prices = np.round(rng.lognormal(1.0, 0.6, n_products), 2)
    con.executemany("INSERT INTO products VALUES (?, ?, ?, ?)",
                    ((i, f"produit{i}", CATEGORIES[i % len(CATEGORIES)], float(prices[i]))
                     for i in range(n_products)))
    # popularité des produits et des clients en loi de Zipf (quelques produits très vendus)
    pw = 1.0 / np.arange(1, n_products + 1) ** 0.9
    prod = rng.choice(n_products, n_sales, p=pw / pw.sum())
    cust = rng.integers(0, n_customers, n_sales)
    store = rng.integers(0, n_stores, n_sales)
    qty = rng.integers(1, 6, n_sales)
    day = np.sort(rng.integers(0, n_days, n_sales))
    amount = np.round(qty * prices[prod], 2)
    con.executemany("INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ((i, int(cust[i]), int(store[i]), int(prod[i]), int(qty[i]), float(amount[i]), int(day[i]))
                     for i in range(n_sales)))
    con.executescript("""
    CREATE INDEX sales_by_customer ON sales (customer_id);
    CREATE INDEX sales_by_day ON sales (day);
    """)
    con.commit()


def vm_steps(con: sqlite3.Connection, sql: str, params=(), every: int = 10) -> int:
    """Nombre (approché à `every` près) d'instructions de la machine virtuelle SQLite
    exécutées par la requête : une mesure du travail, déterministe, contrairement au temps."""
    count = [0]

    def handler():
        count[0] += 1
        return 0

    con.set_progress_handler(handler, every)
    try:
        con.execute(sql, params).fetchall()
    finally:
        con.set_progress_handler(None, every)
    return count[0] * every


def fifo_servers(arrivals, services, c: int = 1):
    """File FIFO commune devant `c` serveurs identiques.

    arrivals : instants d'arrivée (triés) ; services : durées de service.
    Renvoie (début, fin) de chaque tâche (tableaux numpy).
    """
    arrivals = np.asarray(arrivals, dtype=float)
    services = np.asarray(services, dtype=float)
    free = [0.0] * c                     # instants de libération des serveurs (tas)
    heapq.heapify(free)
    start = np.empty_like(arrivals)
    end = np.empty_like(arrivals)
    for i, (a, s) in enumerate(zip(arrivals, services)):
        f = heapq.heappop(free)
        st = a if a > f else f
        start[i] = st
        end[i] = st + s
        heapq.heappush(free, end[i])
    return start, end


def erlang_c(c: int, a: float) -> float:
    """Probabilité d'attendre dans une file M/M/c de charge offerte a = lambda / mu (Erlang C)."""
    if a >= c:
        return 1.0
    s = 0.0
    term = 1.0
    for k in range(c):
        if k > 0:
            term *= a / k
        s += term
    top = term * a / c * c / (c - a)      # a^c / c! * c / (c - a)
    return top / (s + top)


def mmc_response_time(lam: float, mu: float, c: int) -> float:
    """Temps de réponse moyen (attente + service) d'une file M/M/c ; inf si saturée."""
    a = lam / mu
    if a >= c:
        return math.inf
    return erlang_c(c, a) / (c * mu - lam) + 1.0 / mu


# ====================================================================================
# 2. Entrepôts, lacs et données dérivées
# ====================================================================================

def snapshot_capture_fraction(rate: float, period: float) -> float:
    """Fraction des versions d'un enregistrement visibles par un instantané périodique.

    L'enregistrement change selon un processus de Poisson de taux `rate` ; un instantané
    tous les `period` ne voit que la dernière version de chaque période. Espérance du
    nombre de périodes contenant au moins un changement, divisée par l'espérance du
    nombre de changements : (1 - exp(-rate * period)) / (rate * period).
    """
    x = rate * period
    if x == 0:
        return 1.0
    return (1.0 - math.exp(-x)) / x


def rebuild_order(graph, changed) -> list:
    """Données dérivées à recalculer (dans l'ordre) quand les nœuds `changed` changent.

    graph : networkx.DiGraph dont les arcs vont de la source vers la donnée dérivée.
    Renvoie les descendants de `changed` dans un ordre topologique (chaque donnée est
    recalculée après toutes celles dont elle dépend).
    """
    import networkx as nx

    if isinstance(changed, str):
        changed = [changed]
    impacted = set()
    for c in changed:
        impacted |= nx.descendants(graph, c)
    return [n for n in nx.topological_sort(graph) if n in impacted]


# ====================================================================================
# 3. Cloud et auto-hébergement
# ====================================================================================

def load_profile(hours: int = 24 * 7, mean: float = 100.0, peak_ratio: float = 3.0,
                 floor: float = 0.15, noise: float = 0.05, seed: int = 0) -> np.ndarray:
    """Charge horaire synthétique (cycle quotidien, creux la nuit, pic l'après-midi).

    La moyenne vaut `mean` et le pic environ `peak_ratio` fois la moyenne ; `floor` est la
    part constante de la charge (fraction de la moyenne) ; `noise` le bruit multiplicatif.
    """
    if peak_ratio <= 1.0:
        raise ValueError("peak_ratio doit être > 1")
    h = np.arange(hours)
    g0 = (1 + np.cos(2 * np.pi * (h % 24 - 15) / 24)) / 2          # 1 à 15 h, 0 à 3 h
    target = (1 - floor) / (peak_ratio - floor)                    # moyenne voulue de g0**p
    lo, hi = 1e-3, 500.0
    for _ in range(80):                                            # bissection sur p
        p = math.sqrt(lo * hi)
        if np.mean(g0 ** p) > target:
            lo = p
        else:
            hi = p
    g = g0 ** p
    shape = floor + (1 - floor) * g / g.mean()
    rng = np.random.default_rng(seed)
    shape = shape * rng.lognormal(0.0, noise, hours)
    return mean * shape / shape.mean()


def servers_needed(load, capacity: float, headroom: float = 0.0) -> np.ndarray:
    """Nombre de serveurs de capacité `capacity` pour servir `load` avec une marge."""
    return np.maximum(1, np.ceil(np.asarray(load, dtype=float) * (1 + headroom) / capacity)).astype(int)


def cost_self_hosted(load, capacity: float, server_month: float, ops_month: float,
                     headroom: float = 0.3, hours_per_month: float = 730.0) -> float:
    """Coût mensuel d'une flotte possédée, dimensionnée pour le pic (+ marge).

    server_month : coût mensuel amorti d'un serveur (achat, hébergement, électricité) ;
    ops_month : coût mensuel de l'équipe d'exploitation imputé à ce système.
    """
    n = int(servers_needed(np.max(load), capacity, headroom))
    return n * server_month + ops_month


def cost_cloud(load, capacity: float, instance_hour: float, ops_month: float = 0.0,
               headroom: float = 0.3, autoscale: bool = True, hours_per_month: float = 730.0) -> float:
    """Coût mensuel en instances à l'heure (charge horaire `load`).

    autoscale=True : on paie heure par heure le nombre d'instances nécessaire ;
    autoscale=False : flotte fixe dimensionnée pour le pic, comme en auto-hébergement.
    """
    load = np.asarray(load, dtype=float)
    if autoscale:
        inst_hours = servers_needed(load, capacity, headroom).sum() * hours_per_month / load.size
    else:
        inst_hours = int(servers_needed(load.max(), capacity, headroom)) * hours_per_month
    return inst_hours * instance_hour + ops_month


#: ordres de grandeur (latence du premier octet en s, débit d'un flux en octets/s)
STORAGE_TIERS = {
    "RAM locale": (1e-7, 10e9),
    "SSD NVMe local": (1e-4, 2e9),
    "disque virtuel réseau": (1e-3, 250e6),
    "object store": (3e-2, 100e6),
    "object store (autre région)": (1e-1, 50e6),
}


def transfer_time(size_bytes, latency: float, bandwidth: float):
    """Temps pour lire `size_bytes` : latence du premier octet + taille / débit."""
    return latency + np.asarray(size_bytes, dtype=float) / bandwidth


def object_store_monthly_cost(total_bytes: float, object_bytes: float, reads_per_month: float = 1.0,
                              gb_month: float = 0.023, put_per_1000: float = 0.005,
                              get_per_1000: float = 0.0004) -> dict:
    """Coût mensuel d'un jeu de données découpé en objets de `object_bytes` octets.

    Le jeu est réécrit une fois par mois (un PUT par objet) et lu en entier
    `reads_per_month` fois (un GET par objet). Tarifs : ordres de grandeur publics d'un
    object store en 2025, à titre indicatif.
    """
    n = math.ceil(total_bytes / object_bytes)
    stockage = total_bytes / 1e9 * gb_month
    ecritures = n / 1000 * put_per_1000
    lectures = n * reads_per_month / 1000 * get_per_1000
    return {"objets": n, "stockage": stockage, "ecritures": ecritures, "lectures": lectures,
            "total": stockage + ecritures + lectures}


class LRUCache:
    """Cache LRU de capacité fixe (en nombre d'entrées), avec compteurs."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self.data: OrderedDict = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key) -> bool:
        """Accède à `key` ; renvoie True si présent (hit), sinon l'insère (miss)."""
        if key in self.data:
            self.data.move_to_end(key)
            self.hits += 1
            return True
        self.misses += 1
        if self.capacity > 0:
            self.data[key] = True
            if len(self.data) > self.capacity:
                self.data.popitem(last=False)
        return False

    @property
    def hit_ratio(self) -> float:
        n = self.hits + self.misses
        return self.hits / n if n else 0.0
