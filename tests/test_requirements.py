"""Tests du module ddia.requirements (exigences non fonctionnelles)."""
import math

import numpy as np
import pytest

from ddia import requirements as rq


# --- étude de cas des timelines ------------------------------------------------------

def test_graphe_social_sans_boucle_ni_doublon():
    f, g = rq.social_graph(2000, mean_follows=20, seed=3)
    assert np.all(f != g)
    assert len(set(zip(f.tolist(), g.tolist()))) == f.size
    nf, ng = rq.degree_counts(f, g, 2000)
    assert nf.sum() == ng.sum() == f.size
    assert nf.max() > 10 * np.median(nf)            # queue en loi de puissance


def test_couts_fanout():
    # 0 suit 1 et 2 ; 1 suit 2
    f, g = np.array([0, 0, 1]), np.array([1, 2, 2])
    c = rq.fanout_costs(f, g, 3, post_rate=1.0, read_rate=10.0, threshold=2)
    assert c["lecture_ecritures"] == 3 and c["lecture_lectures"] == 10 * 3
    assert c["ecriture_ecritures"] == 3 and c["ecriture_lectures"] == 30   # abonnés : 0, 1, 2
    # 2 a deux abonnés -> célébrité : non diffusé ; 0 et 1 la suivent
    assert c["n_celebrites"] == 1
    assert c["hybride_ecritures"] == 0 + 1 + 1
    assert c["hybride_lectures"] == 10 * (2 + 2 + 1)


def test_trois_strategies_meme_timeline():
    f, g = rq.social_graph(300, mean_follows=15, seed=1)
    pull = rq.PullTimelines(f, g, 300, k=20)
    push = rq.PushTimelines(f, g, 300, k=20)
    hyb = rq.HybridTimelines(f, g, 300, k=20, threshold=30)
    rng = np.random.default_rng(0)
    for i, u in enumerate(rng.integers(0, 300, 3000)):
        for s in (pull, push, hyb):
            s.post(int(u), float(i), "")
    for u in range(0, 300, 7):
        assert pull.timeline(u) == push.timeline(u) == hyb.timeline(u)
    assert push.write_ops > pull.write_ops


# --- percentiles -------------------------------------------------------------------

def test_percentile_rang_le_plus_proche():
    x = list(range(1, 101))
    assert rq.percentile(x, 50) == 50
    assert rq.percentile(x, 99) == 99
    assert rq.percentile(x, 100) == 100
    assert rq.percentile([7.0], 99.9) == 7.0


def test_histogramme_erreur_relative_bornee_et_fusion():
    x = rq.synthetic_response_times(50_000, seed=2)
    h = rq.LogLinearHistogram(sub_bits=5)
    h.record_many(x)
    for q in (50, 90, 99, 99.9):
        exact = rq.percentile(x, q)
        assert abs(h.percentile(q) - exact) / exact <= h.max_relative_error() + 1e-9
    a, b = rq.LogLinearHistogram(5), rq.LogLinearHistogram(5)
    a.record_many(x[:20_000]); b.record_many(x[20_000:])
    m = rq.LogLinearHistogram.merged([a, b])
    assert m.counts == h.counts and m.total == h.total
    assert math.isclose(m.mean(), x.mean())


def test_fenetre_glissante_oublie_le_passe():
    r = rq.RollingPercentiles(window=2, slot=60)
    r.add_many([10, 20], [1000.0, 1000.0])          # minute 0 : lent
    r.add_many([70, 80, 130, 140], [10.0, 10.0, 10.0, 10.0])
    assert r.percentile(110, 100) > 500             # fenêtre = minutes 0 et 1
    assert r.percentile(150, 100) < 20              # fenêtre = minutes 1 et 2 : la minute 0 est oubliée
    assert 0 not in r.slots


def test_amplification_queue():
    assert math.isclose(rq.p_any_slow(0.01, 1), 0.01)
    assert math.isclose(rq.p_any_slow(0.01, 100), 1 - 0.99 ** 100)
    assert math.isclose(rq.per_call_quantile_for(0.99, 1), 0.99)
    assert rq.per_call_quantile_for(0.99, 100) > 0.9998
    s = np.arange(1.0, 101.0)
    m = rq.fanout_response_times(s, 5, 1000, seed=0)
    assert m.shape == (1000,) and m.min() >= 1


# --- files d'attente ---------------------------------------------------------------

def test_fifo_lindley_et_ordre():
    st, en = rq.simulate_fifo(np.array([0.0, 1.0, 1.5]), np.array([2.0, 1.0, 1.0]))
    assert np.allclose(st, [0, 2, 3]) and np.allclose(en, [2, 3, 4])
    # arrivées non triées : servies dans l'ordre des arrivées
    st2, en2 = rq.simulate_fifo(np.array([1.5, 0.0, 1.0]), np.array([1.0, 2.0, 1.0]))
    assert np.allclose(en2, [4, 2, 3])
    st3, en3 = rq.simulate_fifo(np.array([0.0, 0.0, 0.0]), np.array([1.0, 1.0, 1.0]), c=2)
    assert np.allclose(en3, [1, 1, 2])
