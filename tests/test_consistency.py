"""Tests du module ddia.consistency (linéarisabilité, horloges, identifiants, Raft, coordination)."""
import matplotlib

matplotlib.use("Agg")

import numpy as np

from ddia import consistency as cs
from ddia.consistency import Op


# ------------------------------------------------------------------ linéarisabilité

def test_lecture_concurrente_peut_voir_ancienne_ou_nouvelle_valeur():
    for v in (0, 1):
        h = [Op("C", "write", 1, "ok", 0, 10), Op("A", "read", None, v, 2, 4)]
        assert cs.check_linearizable(h).ok


def test_retour_en_arriere_interdit():
    # A lit 1, puis B (qui commence après) lit 0 pendant que l'écriture dure encore
    h = [Op("C", "write", 1, "ok", 0, 10), Op("A", "read", None, 1, 1, 2), Op("B", "read", None, 0, 3, 4)]
    res = cs.check_linearizable(h)
    assert not res.ok
    assert 2 in res.blocking(h)


def test_lecture_perimee_apres_ecriture_terminee():
    h = [Op("C", "write", 1, "ok", 0, 2), Op("A", "read", None, 0, 3, 4)]
    assert not cs.check_linearizable(h).ok


def test_cas_et_operation_sans_reponse():
    h = [Op("A", "cas", (0, 5), True, 0, 2), Op("B", "cas", (0, 7), False, 1, 3), Op("C", "read", None, 5, 4, 5)]
    assert cs.check_linearizable(h).ok
    # une écriture sans réponse a pu prendre effet... ou non
    for v in (0, 9):
        h = [Op("A", "write", 9, None, 0, done=False), Op("B", "read", None, v, 5, 6)]
        assert cs.check_linearizable(h).ok


def test_points_de_linearisation_dans_les_intervalles():
    h = [Op("C", "write", 1, "ok", 0, 10), Op("A", "read", None, 0, 1, 3), Op("B", "read", None, 1, 4, 6)]
    res = cs.check_linearizable(h)
    assert res.ok
    for i, p in zip(res.order, res.points):
        assert h[i].t_call <= p <= h[i].t_return
    assert all(a <= b for a, b in zip(res.points, res.points[1:]))


def test_quorum_dynamo_non_linearisable_et_abd_linearisable():
    bad = {"dynamo": 0, "abd": 0}
    for mode in bad:
        for s in range(40):
            h, _ = cs.run_quorum_register(mode=mode, w=2, r=2, seed=s, sigma=1.0, n_writers=2)
            bad[mode] += not cs.check_linearizable(h).ok
    assert bad["dynamo"] > 0
    assert bad["abd"] == 0


# ------------------------------------------------------------------ horloges et identifiants

def test_horloge_de_lamport():
    a, b = cs.LamportClock("a"), cs.LamportClock("b")
    t1 = a.tick()
    b.observe(t1)
    t2 = b.tick()
    assert t1 < t2 and t2 == (2, "b")


def test_horloges_vectorielles():
    assert cs.vc_compare({"a": 1, "b": 0}, {"a": 1, "b": 1}) == "<"
    assert cs.vc_compare({"a": 2, "b": 0}, {"a": 1, "b": 1}) == "||"
    assert cs.vc_compare({"a": 1}, {"a": 1}) == "="


def test_lamport_respecte_la_causalite_pas_horloge_decalee():
    ev = cs.run_chat(offsets=[0, 3, -2, 1], duration=150, seed=2)
    assert cs.causal_order_errors(ev, "lamport")["inversées"] == 0
    assert cs.causal_order_errors(ev, "t_phys")["inversées"] > 0


def test_generateurs_identifiants():
    rng = np.random.default_rng(0)
    times = np.sort(rng.uniform(0, 10, 2000))
    nodes = rng.integers(0, 3, 2000)
    for scheme in ("central", "sharded", "blocks", "wallclock"):
        ids = cs.assign_ids(scheme, times, nodes, 3, offsets=[0, 0.05, -0.05])
        assert len(set(ids.tolist())) == len(ids)
    central = cs.assign_ids("central", times, nodes, 3)
    assert cs.inversion_curve(times, central, [0.01])[0] == 0
    sharded = cs.assign_ids("sharded", times, nodes, 3)
    assert cs.inversion_curve(times, sharded, [0.01])[0] > 0.1
    wall = cs.assign_ids("wallclock", times, nodes, 3, offsets=[0, 0.05, -0.05])
    assert cs.inversion_curve(times, wall, [0.5])[0] == 0


# ------------------------------------------------------------------ diffusion et Raft

def test_diffusion_a_ordre_total_fait_converger_les_repliques():
    claims = [(1, "r1", "bob", "u1"), (1.2, "r2", "bob", "u2"), (1.1, "r3", "bob", "u3")]
    _, reps = cs.run_registry(claims, total_order=True)
    assert len({r.owner["bob"] for r in reps}) == 1
    assert len({tuple(r.log) for r in reps}) == 1
    _, reps = cs.run_registry(claims, total_order=False)
    assert len({r.owner["bob"] for r in reps}) > 1
