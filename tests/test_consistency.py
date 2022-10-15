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


def test_raft_elit_un_leader_et_valide():
    net, nodes, trace = cs.raft_cluster(5, seed=1)
    hist = []
    net.add(cs.RaftClient("client", list(nodes), [(60, "write", 1), (2, "cas", (1, 2)), (2, "read", None)],
                          history=hist))
    leader0 = None

    def crash_leader():
        nonlocal leader0
        leader0 = [x for x, nd in nodes.items() if nd.role == "leader"][0]
        net.crash(leader0)
    net.at(120, crash_leader)
    net.run(until=400)
    assert len(hist) == 3 and hist[-1].result == 2
    assert cs.check_linearizable(hist).ok
    assert cs.raft_check_safety(nodes, trace)["sur"]
    leaders = cs.raft_leaders(trace)
    assert len({term for _, _, term in leaders}) == len(leaders)


def test_raft_survit_a_une_partition():
    net, nodes, trace = cs.raft_cluster(5, seed=3)
    net.run(until=80)
    leader = [x for x, nd in nodes.items() if nd.role == "leader"][0]
    others = [x for x in nodes if x != leader]
    net.partition({leader, others[0]}, set(others[1:]))
    net.run(until=250)
    net.heal()
    net.run(until=400)
    assert cs.raft_check_safety(nodes, trace)["sur"]
    assert sum(nd.role == "leader" for nd in nodes.values()) == 1
    assert nodes[leader].term > 1


def test_disponibilite_et_latence_de_quorum():
    assert abs(cs.majority_availability(3, 0.9) - (0.9 ** 3 + 3 * 0.9 ** 2 * 0.1)) < 1e-12
    assert cs.majority_availability(5, 0.99) > cs.majority_availability(3, 0.99)
    lat3 = np.median(cs.quorum_latency(3, 2))
    lat7 = np.median(cs.quorum_latency(7, 4))
    assert lat7 > lat3


# ------------------------------------------------------------------ coordination

def test_service_de_coordination_bail_et_cle_ephemere():
    svc = cs.CoordService()
    events = []
    lease = svc.grant(5, now=0)
    ok, rev = svc.create("/lock", "c1", lease)
    assert ok
    assert svc.create("/lock", "c2")[0] is False
    svc.watch("/lock", lambda kind, key, r: events.append((kind, key)))
    assert svc.keepalive(lease, now=4)
    svc.expire(now=8)
    assert svc.get("/lock") == "c1"
    svc.expire(now=9.5)
    assert svc.get("/lock") is None and events == [("delete", "/lock")]
    ok2, rev2 = svc.create("/lock", "c2")
    assert ok2 and rev2 > rev


def test_cles_sequentielles_ordonnees():
    svc = cs.CoordService()
    keys = [svc.create_sequential("/election/n-", f"c{i}") for i in range(5)]
    assert keys == sorted(keys) and svc.keys("/election/") == keys


def test_paxos_accord_malgre_pannes():
    for s in range(30):
        _, dec, props = cs.run_paxos({"P1": "x", "P2": "y", "P3": "z"}, n_acceptors=5, seed=s,
                                     crashes=[(2.0, "A1"), (3.0, "A2")])
        vals = {p.decided for p in props.values()}
        assert len(vals) == 1 and vals <= {"x", "y", "z"}


def test_raft_ecriture_sur_ancien_leader_isole_n_est_pas_confirmee_a_tort():
    net, nodes, trace = cs.raft_cluster(5, seed=2)
    hist = []
    net.run(until=60)
    old = [x for x, nd in nodes.items() if nd.role == "leader"][0]
    others = [x for x in nodes if x != old]
    net.partition({old, others[0]}, set(others[1:]))
    net.add(cs.RaftClient("a", [old] + others, [(1, "write", 1)], history=hist, timeout=60))
    net.add(cs.RaftClient("b", others[1:], [(50, "write", 2)], history=hist, timeout=60))
    net.run(until=140)
    net.heal()
    net.run(until=400)
    assert cs.raft_check_safety(nodes, trace)["sur"]
    leader = [x for x, nd in nodes.items() if nd.role == "leader"][0]
    sets = [e.cmd for e in nodes[leader].log[1:nodes[leader].commit + 1] if e.cmd[0] == "set"]
    # chaque écriture confirmée au client est bien dans le journal validé
    for op in hist:
        assert ("set", "x", op.arg) in sets


def test_raft_sans_restriction_d_election_perd_une_ecriture():
    pertes = 0
    for check in (True, False):
        net, nodes, trace = cs.raft_cluster(5, seed=0, check_log=check)
        hist = []
        net.run(until=60)
        L = [x for x, nd in nodes.items() if nd.role == "leader"][0]
        autres = [x for x in nodes if x != L]
        for x in autres[:2]:
            net.partition({x}, set(nodes) - {x})
        net.add(cs.RaftClient("client", [L], [(1, "write", 7)], history=hist, timeout=50))
        net.run(until=100)
        net.heal(); net.crash(L)
        net.run(until=400)
        s = cs.raft_check_safety(nodes, trace)
        assert s["sur"] == check
        pertes += s["entrees_validees_effacees"]
    assert pertes > 0
