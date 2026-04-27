"""Tests du module ddia.distsys_verif (model checking et simulation déterministe, 2e édition)."""
from ddia import distsys_verif as dv


def test_model_checker_mise_a_jour_perdue():
    r = dv.model_check(*dv.counter_model(2))
    assert not r.ok
    assert [a for a, _ in r.path] == ["p0 lit x=0", "p1 lit x=0", "p0 écrit x=1", "p1 écrit x=1"]
    assert dv.model_check(*dv.counter_model(3, atomic=True)).ok


def test_model_checker_bail():
    r = dv.model_check(*dv.lease_model())
    assert not r.ok and len(r.path) == 8
    assert r.path[-1][0].startswith("write c0")
    assert dv.model_check(*dv.lease_model(fencing=True)).ok
    amn = dv.model_check(*dv.lease_model(fencing=True, lock_crash=True, persist=False))
    assert not amn.ok and any(a.startswith("crash") for a, _ in amn.path)
    assert dv.model_check(*dv.lease_model(fencing=True, lock_crash=True, persist=True)).ok


def test_espace_d_etats_croit():
    a = dv.model_check(*dv.lease_model(fencing=True, t_max=3)).states
    b = dv.model_check(*dv.lease_model(fencing=True, t_max=6)).states
    assert b > a


def test_dst_rejeu_deterministe():
    f = dv.random_faults(7)
    assert f == dv.random_faults(7)
    a = dv.run_lease_system(f, seed=7)
    b = dv.run_lease_system(f, seed=7)
    c = dv.run_lease_system(f, seed=8)
    assert a.digest == b.digest != c.digest


def test_dst_trouve_et_reduit_le_bogue():
    found = dv.find_failing_seed(range(50), persist_counter=False)
    assert found is not None
    seed, faults, res = found
    mini = dv.shrink_faults(faults, seed, persist_counter=False)
    assert len(mini) <= len(faults)
    assert not dv.run_lease_system(mini, seed=seed, persist_counter=False).ok
    assert any(f.kind == "crash_verrous" for f in mini)


def test_dst_compteur_durable_sans_violation():
    assert all(dv.run_lease_system(dv.random_faults(s), seed=s).ok for s in range(40))
