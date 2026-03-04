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
