import numpy as np
import pandas as pd

from ddia import ethics as et


def test_exclusion_shared_score_concentrates():
    c0, _ = et.exclusion_counts(n_people=4000, n_orgs=10, rho=0.0, seed=1)
    c1, _ = et.exclusion_counts(n_people=4000, n_orgs=10, rho=1.0, seed=1)
    assert (c1 == 10).mean() > 0.09
    assert (c0 == 10).mean() < 0.01
    assert abs(c0.mean() - 1.0) < 0.05 and abs(c1.mean() - 1.0) < 0.05


def test_logistic_recovers_coefficients():
    rng = np.random.default_rng(0)
    X = np.column_stack([np.ones(5000), rng.normal(size=5000)])
    y = (rng.random(5000) < et.sigmoid(0.5 + 2 * X[:, 1])).astype(float)
    w = et.fit_logistic(X, y)
    assert abs(w[0] - 0.5) < 0.15 and abs(w[1] - 2) < 0.2


def test_proxy_reintroduces_bias():
    df = et.make_credit_population(n=8000, seed=2)
    assert abs(df[df.group == 0].repaid.mean() - df[df.group == 1].repaid.mean()) < 0.05
    rates = {}
    for uz in (True, False):
        X = et.design_matrix(df, use_group=False, use_zip=uz)
        s = et.predict_proba(X, et.fit_logistic(X, df.label.values))
        m = et.group_metrics(df.repaid.values, s, df.group.values, np.quantile(s, 0.3))
        rates[uz] = m.loc["B", "taux d'acceptation"] / m.loc["A", "taux d'acceptation"]
    assert rates[True] < 0.8 < rates[False]


def test_calibration_table_shape():
    rng = np.random.default_rng(0)
    s = rng.random(2000)
    y = (rng.random(2000) < s).astype(int)
    g = (rng.random(2000) < 0.5).astype(int)
    t = et.calibration_table(y, s, g, bins=5)
    assert set(t["groupe"]) == {"A", "B"}
    assert np.allclose(t["score moyen"], t["taux observé"], atol=0.12)


def test_credit_spiral_screening_hurts():
    a = et.credit_spiral(n=2000, months=60, screening=False, seed=0)
    b = et.credit_spiral(n=2000, months=60, screening=True, seed=0)
    assert b["unemployment"][-1] > a["unemployment"][-1]


def test_patrol_feedback():
    hot = et.patrol_feedback(policy="hotspot", days=300, seed=0)
    assert hot[-1, 0] > 0.95
    cor = [et.patrol_feedback(policy="proportional", correct=True, days=600, seed=s)[-1, 0] for s in range(4)]
    assert all(abs(x - 0.5) < 0.08 for x in cor)


def test_recommender_collapses():
    r = et.recommender_loop(rounds=300, seed=0)
    assert r["pref"][-1].max() > 0.9
    assert len(r["shown"]) == 300


def test_privacy_uniqueness_and_generalization():
    pop = et.make_population(n=20000, n_zip=100, seed=0)
    qi = ["zip", "birth", "sex"]
    assert et.uniqueness(pop, qi) > 0.9
    g = et.generalize(pop, zip_digits=3, birth="decade")
    assert et.uniqueness(g, qi) == 0.0
    assert et.k_anonymity(g, qi) >= 2
    assert et.linkage_attack(pop, pop.sample(500, random_state=0), qi) > 0.9


def test_expected_unique_fraction():
    assert abs(et.expected_unique_fraction(1, 100) - 1.0) < 1e-12
    assert abs(et.expected_unique_fraction(101, 100) - (0.99 ** 100)) < 1e-12


def test_shadow_profiles_increase_with_adoption():
    f1, _ = et.shadow_profiles(n=1500, adoption=0.1, seed=0)
    f2, _ = et.shadow_profiles(n=1500, adoption=0.5, seed=0)
    assert 0 < f1 < f2 <= 1


def test_randomized_response_unbiased():
    rng = np.random.default_rng(0)
    truth = (rng.random(50000) < 0.2).astype(int)
    est = et.rr_estimate(et.randomized_response(truth, 0.5, rng), 0.5)
    assert abs(est - 0.2) < 4 * np.sqrt(et.rr_variance(0.2, 0.5, 50000))
    assert 0.2 < et.rr_posterior_yes(0.2, 0.5) < 1


def test_breach_exposure_grows_with_retention():
    a = et.breach_exposure(30, n_runs=500)
    b = et.breach_exposure(365, n_runs=500)
    assert b[0] > a[0] and a[2] == b[2]
