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
