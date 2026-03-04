"""Tests du module ddia.architecture (compromis d'architecture, chapitre 1)."""
import math
import sqlite3

import networkx as nx
import numpy as np
import pytest

from ddia import architecture as ar


# --- opérationnel / analytique ---------------------------------------------------------

def test_base_de_ventes_et_travail_des_requetes():
    con = sqlite3.connect(":memory:")
    ar.make_shop_db(con, n_customers=200, n_products=50, n_sales=5000, seed=1)
    assert con.execute("SELECT COUNT(*) FROM sales").fetchone()[0] == 5000
    point = ar.vm_steps(con, "SELECT * FROM sales WHERE customer_id = ?", (7,))
    agg = ar.vm_steps(con, "SELECT store_id, SUM(amount) FROM sales GROUP BY store_id")
    assert agg > 50 * point                  # l'agrégat parcourt toute la table
    # montant cohérent avec le prix et la quantité
    row = con.execute("SELECT s.qty * p.price, s.amount FROM sales s JOIN products p "
                      "ON p.id = s.product_id LIMIT 1").fetchone()
    assert row[0] == pytest.approx(row[1], abs=0.01)


def test_fifo_un_et_deux_serveurs():
    s, e = ar.fifo_servers([0, 0, 0], [1, 1, 1], c=1)
    assert list(e) == [1, 2, 3]
    s, e = ar.fifo_servers([0, 0, 0], [1, 1, 1], c=2)
    assert list(e) == [1, 1, 2]


def test_erlang_c_et_mmc():
    assert ar.erlang_c(1, 0.5) == pytest.approx(0.5)            # M/M/1 : P(attente) = rho
    assert ar.mmc_response_time(0.5, 1.0, 1) == pytest.approx(2.0)
    assert ar.mmc_response_time(3.0, 1.0, 2) == math.inf
    # simulation M/M/2 cohérente avec la formule
    rng = np.random.default_rng(0)
    arr = np.cumsum(rng.exponential(1 / 1.5, 200_000))
    srv = rng.exponential(1.0, arr.size)
    s, e = ar.fifo_servers(arr, srv, c=2)
    assert np.mean(e - arr) == pytest.approx(ar.mmc_response_time(1.5, 1.0, 2), rel=0.05)


# --- entrepôts et données dérivées ----------------------------------------------------

def test_fraction_capturee_par_instantanes():
    assert ar.snapshot_capture_fraction(0.0, 1.0) == 1.0
    assert ar.snapshot_capture_fraction(1e-6, 1.0) == pytest.approx(1.0, abs=1e-5)
    assert ar.snapshot_capture_fraction(10.0, 1.0) == pytest.approx(0.1, rel=1e-3)
    rng = np.random.default_rng(2)
    t = np.cumsum(rng.exponential(1 / 3.0, 300_000))
    periods = np.unique(np.floor(t))
    assert periods.size / t.size == pytest.approx(ar.snapshot_capture_fraction(3.0, 1.0), rel=0.02)


def test_ordre_de_reconstruction():
    g = nx.DiGraph([("db", "cache"), ("db", "index"), ("db", "entrepot"), ("entrepot", "modele"),
                    ("index", "suggestions"), ("modele", "suggestions")])
    order = ar.rebuild_order(g, "db")
    assert set(order) == {"cache", "index", "entrepot", "modele", "suggestions"}
    assert order.index("modele") > order.index("entrepot")
    assert order[-1] == "suggestions"
    assert ar.rebuild_order(g, "entrepot") == ["modele", "suggestions"]


# --- cloud et auto-hébergement ----------------------------------------------------------

def test_profil_de_charge():
    load = ar.load_profile(24 * 14, mean=200, peak_ratio=4, noise=0.0)
    assert load.mean() == pytest.approx(200)
    assert load.max() / load.mean() == pytest.approx(4, rel=0.02)
    assert load.min() > 0
    with pytest.raises(ValueError):
        ar.load_profile(peak_ratio=0.5)


def test_couts_cloud_et_auto_hebergement():
    load = np.array([10.0, 10.0, 10.0, 90.0])
    assert ar.servers_needed(95, 10).item() == 10
    own = ar.cost_self_hosted(load, capacity=10, server_month=100, ops_month=0, headroom=0)
    assert own == 9 * 100
    fixed = ar.cost_cloud(load, capacity=10, instance_hour=1, headroom=0, autoscale=False)
    auto = ar.cost_cloud(load, capacity=10, instance_hour=1, headroom=0, autoscale=True)
    assert fixed == pytest.approx(9 * 730)
    assert auto == pytest.approx((1 + 1 + 1 + 9) / 4 * 730)


def test_couches_de_stockage_et_object_store():
    lat, bw = ar.STORAGE_TIERS["object store"]
    assert ar.transfer_time(0, lat, bw) == pytest.approx(lat)
    assert ar.transfer_time(bw, lat, bw) == pytest.approx(lat + 1)
    small = ar.object_store_monthly_cost(1e12, 1e3)
    big = ar.object_store_monthly_cost(1e12, 1e8)
    assert small["objets"] == 10**9 and big["objets"] == 10**4
    assert small["stockage"] == pytest.approx(big["stockage"])
    assert small["total"] > 100 * big["total"]


def test_cache_lru():
    c = ar.LRUCache(2)
    assert [c.get(k) for k in "abacb"] == [False, False, True, False, False]
    assert c.hits == 1 and c.misses == 4
    keys = ar.zipf_keys(1000, 20_000, s=1.0, seed=0)
    big, small = ar.LRUCache(200), ar.LRUCache(20)
    for k in keys:
        big.get(int(k)); small.get(int(k))
    assert big.hit_ratio > small.hit_ratio > 0.2


def test_seau_a_jetons():
    t = np.arange(0, 10, 0.1)               # 10 requêtes par seconde pendant 10 s
    ok = ar.token_bucket(t, rate=2.0, burst=5)
    assert ok[:5].all()
    assert ok.sum() == pytest.approx(5 + 2 * 10, abs=2)


# --- distribué ou mono-nœud -----------------------------------------------------------

def test_distances_et_rtt():
    p, ny = ar.CITIES["Paris"], ar.CITIES["New York"]
    d = ar.haversine_km(*p, *ny)
    assert 5800 < d < 5900
    assert ar.fiber_rtt_ms(1000, stretch=1.0) == pytest.approx(10.0)


def test_cluster_et_cost():
    ns = [1, 2, 4, 8, 16]
    t = ar.cluster_time(ns, t_single=100, overhead=5)
    assert t[0] == pytest.approx(500)
    assert ar.cost_metric(ns, t, 100) == 8
    assert ar.cost_metric(ns, t, 1) is None


def test_disponibilite_chaine():
    assert ar.chain_availability([0.999] * 10) == pytest.approx(0.999**10)


def test_chemin_critique():
    spans = {"req": (None, 0, 10), "auth": ("req", 0, 2), "cache": ("req", 0.5, 1),
             "db": ("req", 2, 9), "disque": ("db", 3, 8)}
    assert ar.critical_path(spans, "req") == ["req", "auth", "db", "disque"]


def test_serverless():
    # requêtes très espacées : toujours à froid ; très rapprochées : presque jamais
    r = ar.simulate_serverless(np.arange(0, 1000, 100.0), 0.1, keep_alive=10, cold_start=1)
    assert r["cold_fraction"] == 1.0
    r = ar.simulate_serverless(np.arange(0, 100, 1.0), 0.1, keep_alive=10, cold_start=1)
    # la 1re requête démarre à froid (1,1 s) : la 2e arrive pendant ce temps, à froid aussi
    assert r["cold"].sum() == 2
    assert r["billed"] == pytest.approx(10.0)
    assert r["instance_time"] > r["billed"]


def test_points_de_reprise():
    assert ar.young_daly_interval(0.5, 100) == pytest.approx(10.0)
    # sans pannes : travail + points de reprise intermédiaires
    t = ar.simulate_checkpointing(10, 2, 0.1, 0.5, mtbf=1e12)
    assert t == pytest.approx(10 + 4 * 0.1)
    # l'intervalle de Young-Daly fait mieux que des intervalles extrêmes
    def mean_t(iv):
        return np.mean([ar.simulate_checkpointing(200, iv, 0.2, 0.2, 10, seed=s) for s in range(30)])
    best = mean_t(ar.young_daly_interval(0.2, 10))
    assert best < mean_t(0.2) and best < mean_t(20)


# --- données et droit -----------------------------------------------------------------

def test_chiffrement_et_crypto_shredding():
    msg = "visite le 3 mars".encode("utf-8")
    c = ar.xor_cipher(b"k" * 32, msg)
    assert c != msg and ar.xor_cipher(b"k" * 32, c) == msg
    log = ar.ShreddableLog(seed=0)
    for i in range(6):
        log.append("alice" if i % 2 else "bob", f"événement {i}")
    size = log.size_bytes
    assert len(log.read()) == 6
    assert log.forget("alice") == 0
    assert [u for u, _ in log.read()] == ["bob"] * 3
    assert log.size_bytes == size            # rien n'a été réécrit


def test_compaction():
    entries = [("a", b"xx"), ("b", b"yyy"), ("a", b"z")]
    kept, rewritten = ar.compact_without(entries, "a")
    assert kept == [("b", b"yyy")] and rewritten == 3
