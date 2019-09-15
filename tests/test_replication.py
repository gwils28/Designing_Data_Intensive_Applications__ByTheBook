"""Tests du module ddia.replication (leader unique, journaux, retard, multi-leader,
sans leader, écritures concurrentes)."""

import math

import numpy as np
import pytest

from ddia.replication import (STRATEGIES, DottedReplica, MiniDB, VersionedKeyServer, dynamo_cluster,
                              failover_scenario, k_of_n, lag_cluster, merge_union,
                              merge_with_tombstones, merkle_diff, merkle_tree, needed_acks,
                              p_stale_static, pbs_stale_probability, propagate, replicate_rows,
                              replicate_statements, run_topology, simulate_sessions, topology,
                              vv_compare, vv_merge, write_latency_samples, lww_resolve)

SHIP = {"n2": 0.5, "n3": 1.5}


# --- leader unique ----------------------------------------------------------------------
def test_needed_acks():
    assert [needed_acks(m, 4) for m in ("async", "semisync", "quorum", "sync")] == [0, 1, 2, 4]


def test_async_partition_perd_des_ecritures_acquittees():
    r = failover_scenario("async", "partition", ship_delay=SHIP, seed=1)
    assert len(r["lost"]) > 5
    assert r["split_brain"] is not None
    assert r["leaders_at_end"] == [r["final_leader"]]


@pytest.mark.parametrize("mode", ["semisync", "quorum", "sync"])
def test_modes_synchrones_ne_perdent_rien(mode):
    for fault in ("crash", "partition"):
        r = failover_scenario(mode, fault, ship_delay=SHIP, seed=1)
        assert r["lost"] == []


def test_tous_les_noeuds_convergent_apres_basculement():
    r = failover_scenario("async", "crash", ship_delay=SHIP, seed=1)
    logs = {n: [e.req for e in x.log] for n, x in r["nodes"].items()}
    assert len({tuple(v) for v in logs.values()}) == 1
    assert r["controller"].failovers and r["final_leader"] != "n1"
    # l'ancien leader a dû jeter ce qu'il était seul à avoir
    assert {e.req for e in r["nodes"]["n1"].discarded} >= set(r["lost"])


def test_latence_statistiques_ordre():
    rtt = np.array([[1.0, 5.0, 3.0]])
    assert write_latency_samples("async", 3, rtt)[0] == 0
    assert write_latency_samples("semisync", 3, rtt)[0] == 1
    assert write_latency_samples("quorum", 3, rtt)[0] == 3
    assert write_latency_samples("sync", 3, rtt)[0] == 5


# --- journaux ---------------------------------------------------------------------------
def stmts():
    return [lambda db, t: db.insert("evt", {"at": db.now(t), "tirage": db.rand()}),
            lambda db, t: [db.update("cpt", r["id"], {"solde": r["solde"] * 2}) for r in db.select("cpt")],
            lambda db, t: [db.update("cpt", r["id"], {"solde": r["solde"] + 10}) for r in db.select("cpt")]]


def fresh(name, off, seed):
    db = MiniDB(name, clock_offset=off, seed=seed)
    db.insert("cpt", {"id": 1, "solde": 100})
    db.changes.clear()
    return db


def test_instructions_divergent_lignes_convergent():
    L, F = fresh("L", 0, 1), fresh("F", 0.7, 2)
    replicate_statements(stmts(), L, [F], [1.0, 2.0, 3.0], follower_order=[[0, 2, 1]])
    assert L.state() != F.state()
    L, F = fresh("L", 0, 1), fresh("F", 0.7, 2)
    log = replicate_rows(stmts(), L, [F], [1.0, 2.0, 3.0])
    assert L.state() == F.state() and len(log) == 3


# --- retard -----------------------------------------------------------------------------
def make_cluster():
    wt = np.cumsum(np.random.default_rng(0).exponential(0.05, 1500))
    cl = lag_cluster(wt, 3, mean_lag=[0.02, 0.05, 0.3], stalls=[(2, 20, 30)], seed=0)
    owners = np.random.default_rng(1).integers(0, 100, len(wt)).tolist()
    return cl, owners


def test_lag_cluster_dans_l_ordre():
    cl, _ = make_cluster()
    for f in range(3):
        a = np.array(cl.apply_times[f])
        assert np.all(np.diff(a) >= 0) and np.all(a >= np.array(cl.write_times))
    assert cl.lag_seconds(2, 29.0) > 5


def test_strategies_de_session():
    cl, owners = make_cluster()
    res = {s: simulate_sessions(cl, owners, s) for s in STRATEGIES}
    assert res["aleatoire"]["anomalies_ryw"] > 0.1 and res["aleatoire"]["anomalies_monotones"] > 0.01
    assert res["leader"]["anomalies_ryw"] == 0 and res["leader"]["part_leader"] == 1
    assert res["collant"]["anomalies_monotones"] == 0
    assert res["jeton"]["anomalies_ryw"] == 0 and res["jeton"]["anomalies_monotones"] == 0
    assert res["jeton"]["part_leader"] < 0.2
    assert res["fenetre"]["anomalies_ryw"] == 0


# --- multi-leader -----------------------------------------------------------------------
def test_topologies_et_pannes():
    nodes = ["A", "B", "C", "D"]
    assert len(propagate(topology("circulaire", nodes), "A")) == 4
    assert set(propagate(topology("circulaire", nodes), "A", failed={"B"})) == {"A"}
    assert set(propagate(topology("etoile", nodes), "B", failed={"A"})) == {"B"}
    assert len(propagate(topology("tout", nodes), "B", failed={"A"})) == 3


def test_depassement_tout_a_tout():
    assert run_topology("circulaire", 4, 80, seed=0)["violations"] == 0
    r = run_topology("tout", 4, 80, seed=0)
    assert r["violations"] > 0 and not r["etat_final_correct"]
    assert run_topology("tout", 4, 80, buffer=True, seed=0)["etat_final_correct"]


def test_lww_et_paniers():
    assert lww_resolve([(5, "A", "B"), (7, "B", "C"), (7, "A", "D")])[2] == "C"
    assert merge_union([{"savon", "dvd"}, {"savon", "livre"}]) == {"savon", "dvd", "livre"}
    assert merge_with_tombstones([({"savon", "dvd", "livre"}, {"livre"}),
                                  ({"savon", "dvd", "livre"}, {"dvd"})]) == {"savon"}


# --- sans leader ------------------------------------------------------------------------
def test_quorums_formules():
    assert p_stale_static(3, 2, 2) == 0
    assert p_stale_static(3, 1, 1) == pytest.approx(2 / 3)
    assert p_stale_static(5, 2, 2) == pytest.approx(math.comb(3, 2) / math.comb(5, 2))
    assert k_of_n(0.9, 2, 3) == pytest.approx(0.972)


def test_pbs_decroit_et_nul_avec_quorum_strict():
    p = pbs_stale_probability(3, 1, 1, [0, 1, 3], trials=5000)
    assert p[0] > p[1] > p[2]
    assert pbs_stale_probability(3, 2, 2, [0], trials=5000)[0] == 0


def test_dynamo_lecture_reparatrice_et_handoff():
    net, reps, cl = dynamo_cluster(3, 2, 2, n_fallback=2, sloppy=True, latency=(0.1, 0.5), seed=3)
    c = cl["client"]
    net.at(0.0, net.crash, "r2")
    net.at(0.0, net.crash, "r3")
    net.at(5.0, net.recover, "r2")
    net.at(0.1, c.put, "x", 1, "v1")
    net.at(5.0, net.recover, "r3")
    net.run(until=4.0)
    assert reps["r3"].store.get("x") is None
    net.run(until=10.0)
    assert reps["r3"].store["x"] == (1, "v1")        # transfert de l'indice
    assert [x["ok"] for x in c.results] == [True]     # quorum approximatif : écriture acceptée
    net2, reps2, cl2 = dynamo_cluster(3, 2, 2, seed=4)
    c2 = cl2["client"]
    net2.at(0.0, net2.crash, "r3")
    net2.at(0.1, c2.put, "x", 1, "v1")
    net2.at(3.0, net2.recover, "r3")
    net2.at(4.0, c2.get, "x")
    net2.run()
    assert all(r.store["x"] == (1, "v1") for r in reps2.values())   # lecture réparatrice
    assert [x["value"] for x in c2.results if x["type"] == "get"] == ["v1"]


def test_merkle():
    a = {f"k{i}": i for i in range(2000)}
    b = dict(a)
    b["k5"] = -1
    ta, tb = merkle_tree(a, 256), merkle_tree(b, 256)
    diff, compared = merkle_diff(ta, tb)
    assert len(diff) == 1 and compared < 30
    assert merkle_diff(ta, merkle_tree(a, 256)) == ([], 1)


# --- écritures concurrentes -------------------------------------------------------------
def test_panier_du_livre():
    s = VersionedKeyServer()
    v1, _ = s.put(["lait"], None)
    v2, sib = s.put(["oeufs"], None)
    assert sorted(map(tuple, sib.values())) == [("lait",), ("oeufs",)]
    v3, sib = s.put(["lait", "farine"], v1)
    assert sib == {2: ["oeufs"], 3: ["lait", "farine"]}
    v4, sib = s.put(["oeufs", "lait", "jambon"], v2)
    assert sib == {3: ["lait", "farine"], 4: ["oeufs", "lait", "jambon"]}
    v5, sib = s.put(["lait", "farine", "oeufs", "lard"], v3)
    assert sib == {4: ["oeufs", "lait", "jambon"], 5: ["lait", "farine", "oeufs", "lard"]}


def test_vecteurs_de_versions():
    assert vv_compare({"A": 1}, {"A": 2}) == "avant"
    assert vv_compare({"A": 2, "B": 1}, {"A": 2}) == "apres"
    assert vv_compare({"A": 1}, {"B": 1}) == "concurrent"
    assert vv_compare({}, {}) == "egal"
    assert vv_merge({"A": 1, "B": 3}, {"A": 2}) == {"A": 2, "B": 3}


def test_dotted_garde_les_concurrents_et_converge():
    a, b = DottedReplica("A"), DottedReplica("B")
    a.put("x", "a", None)
    b.put("x", "b", None)
    a.sync_from(b, "x")
    b.sync_from(a, "x")
    assert sorted(a.get("x")[0]) == sorted(b.get("x")[0]) == ["a", "b"]
    vals, ctx = a.get("x")
    a.put("x", "ab", ctx)
    b.sync_from(a, "x")
    assert b.get("x")[0] == ["ab"]
    # deux clients sur la même réplique, sans contexte : pas d'écrasement
    c = DottedReplica("C")
    c.put("y", 1, None)
    c.put("y", 2, None)
    assert sorted(c.get("y")[0]) == [1, 2]
