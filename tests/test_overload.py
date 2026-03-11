"""Tests du module ddia.overload (tempêtes de relances, notions de la 2e édition)."""
import numpy as np

from ddia.overload import RetryPolicy, simulate_overload


def _run(policy, shed=False):
    return simulate_overload(lambda t: 1600.0 if 10 <= t < 15 else 800.0, capacity=1000, duration=70,
                             timeout=1.0, policy=policy, shed=shed, seed=1)


def test_sans_surcharge_tout_reussit():
    r = simulate_overload(lambda t: 500.0, capacity=1000, duration=20, timeout=1.0, policy=RetryPolicy(3), seed=0)
    assert r["gaspille"].sum() == 0
    assert abs(r["succes"].sum() - r["neuves"].sum()) <= 5


def test_relances_immediates_metastables():
    r = _run(RetryPolicy(max_retries=3))
    assert r["succes"][50:].mean() < 50              # ne se rétablit pas
    assert r["tentatives"][50:].mean() > 2500        # charge amplifiée


def test_sans_relance_et_delestage_se_retablissent():
    for pol, shed in [(RetryPolicy(), False), (RetryPolicy(max_retries=3), True)]:
        r = _run(pol, shed)
        assert r["succes"][50:].mean() > 700


def test_budget_borne_les_relances():
    r = _run(RetryPolicy(max_retries=3, budget=0.1))
    relances = r["tentatives"].sum() - r["neuves"].sum()
    assert relances <= 0.1 * r["neuves"].sum() + 20


def test_backoff_exponentiel_et_gigue():
    rng = np.random.default_rng(0)
    p = RetryPolicy(max_retries=3, backoff=0.1, exponential=True)
    assert [p.delay(k, rng) for k in (1, 2, 3)] == [0.1, 0.2, 0.4]
    pj = RetryPolicy(max_retries=3, backoff=0.1, exponential=True, jitter=True)
    d = [pj.delay(3, rng) for _ in range(200)]
    assert 0 <= min(d) and max(d) <= 0.4 and np.std(d) > 0.05
