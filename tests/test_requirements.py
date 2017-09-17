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


def test_mm1_simulation_et_little():
    lam, mu, n = 70.0, 100.0, 100_000
    arr = rq.poisson_arrivals(lam, n, seed=1)
    svc = np.random.default_rng(2).exponential(1 / mu, n)
    st, en = rq.simulate_fifo(arr, svc)
    w = (en - arr)[n // 10:].mean()
    th = rq.mm1(lam, mu)
    assert abs(w - th["W"]) / th["W"] < 0.08
    assert math.isclose(th["L"], lam * th["W"])
    assert math.isclose(rq.mm1_response_quantile(lam, mu, 0.99), math.log(100) / 30)
    assert rq.mm1(100, 100)["W"] == math.inf


def test_erlang_c_et_pk():
    assert math.isclose(rq.erlang_c(1, 0.7), 0.7)                 # M/M/1 : P(attente) = rho
    assert rq.mmc(70, 100, 1)["W"] == pytest.approx(rq.mm1(70, 100)["W"])
    assert rq.mmc(700, 100, 10)["W"] < rq.mm1(70, 100)["W"]     # mutualisation
    # P-K : service exponentiel -> attente M/M/1
    assert rq.mg1_wait(70, 0.01, 1.0) == pytest.approx(rq.mm1(70, 100)["Wq"])
    assert rq.mg1_wait(70, 0.01, 0.0) == pytest.approx(rq.mm1(70, 100)["Wq"] / 2)


# --- fiabilité -----------------------------------------------------------------------

def test_afr_mttf():
    assert rq.afr_from_mttf_hours(rq.mttf_hours_from_afr(0.03)) == pytest.approx(0.03)
    assert rq.expected_failures(10_000, 0.03, 365) == pytest.approx(-10_000 * math.log(0.97))
    assert rq.poisson_pmf(np.arange(50), 3.0).sum() == pytest.approx(1.0)


def test_mttdl_formule_et_simulation():
    lam = 1 / rq.mttf_hours_from_afr(0.03)
    assert rq.mttdl_hours(0.03, 2, 24) == pytest.approx(1 / (2 * lam ** 2 * 24))
    sim = rq.simulate_data_loss(2, 0.4, 168, 10, 4000, seed=1)
    p_sim = np.isfinite(sim).mean()
    p_th = 1 - math.exp(-10 * 8760 / rq.mttdl_hours(0.4, 2, 168))
    assert abs(p_sim - p_th) < 0.025
    # un choc commun fréquent fait tout perdre
    assert np.isfinite(rq.simulate_data_loss(3, 0.01, 24, 10, 200, seed=2, shock_per_year=1.0)).mean() > 0.99


def test_disponibilites():
    assert rq.nines(0.999) == pytest.approx(3)
    assert rq.downtime_minutes(0.999, 30) == pytest.approx(43.2)
    assert rq.series_availability(0.99, 0.99) == pytest.approx(0.9801)
    assert rq.parallel_availability(0.99, 0.99) == pytest.approx(0.9999)
    assert rq.k_of_n_availability(0.9, 1, 1) == pytest.approx(0.9)
    assert rq.k_of_n_availability(0.9, 2, 3) == pytest.approx(0.972)


def test_injection_de_fautes_fanout():
    assert rq.fanout_fault_run("naif")[2] == {"perdues": 0, "dupliquees": 0}
    assert rq.fanout_fault_run("naif", crash_at=0.5)[2]["perdues"] > 0
    assert rq.fanout_fault_run("relance", crash_at=0.5)[2]["perdues"] == 0
    for t in (0.3, 0.9, 1.2, 2.0):
        assert rq.fanout_fault_run("idempotent", crash_at=t)[2] == {"perdues": 0, "dupliquees": 0}


# --- scalabilité ---------------------------------------------------------------------

def test_amdahl_usl_ajustement():
    assert rq.amdahl(1, 0.9) == pytest.approx(1)
    assert rq.amdahl(1e9, 0.9) == pytest.approx(10, rel=1e-6)
    assert rq.usl(1, 0.1, 0.01, 500) == pytest.approx(500)
    n = np.array([1, 2, 4, 8, 16, 32])
    lam, s, k = rq.fit_usl(n, rq.usl(n, 0.05, 0.002, 1000))
    assert lam == pytest.approx(1000) and s == pytest.approx(0.05) and k == pytest.approx(0.002)
    assert rq.usl_peak(0.05, 0.002) == pytest.approx(math.sqrt(0.95 / 0.002))
    assert rq.usl_peak(0.05, 0.0) == math.inf


def test_human_number():
    assert rq.human_number(0.01) == "0,01"
    assert rq.human_number(1e-6) == "1e-6"
    assert rq.human_number(2.5e6) == "2,5 M"
    assert rq.human_number(10_000) == "10 k"
