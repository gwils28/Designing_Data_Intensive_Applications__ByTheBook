"""Tests rapides de ddia/dataflow.py (chapitre 13 en 2e édition, 12 en 1re)."""

import random

import numpy as np
import pytest

from ddia import dataflow as df


def test_log_partitions_et_offsets():
    log = df.Log(4)
    for i in range(20):
        log.append(f"k{i % 5}", i)
    assert len(log) == 20
    p = log.partition_of("k1")
    vals = [v for _, k, v in log.read(p, 0) if k == "k1"]
    assert vals == [1, 6, 11, 16]
    assert [o for o, _, _ in log.read(p, 2, 2)] == [2, 3]


def test_double_ecriture_formule_et_journal():
    g, j = 0.5, 1.0
    sim = df.dual_write_divergence(g, j, n_trials=40_000, seed=1)
    assert sim == pytest.approx(df.dual_write_divergence_formula(g, j), abs=0.01)
    assert df.dual_write_divergence(g, j, n_trials=2000, via_log=True) == 0.0


def test_retrait_d_ami():
    r = df.unfriend_experiment([0.0, 2.0], jitter=1.0, n_trials=20_000, seed=0)
    assert r["leak"][0] == pytest.approx(0.5, abs=0.02)
    assert r["leak"][1] == pytest.approx(0.5 * np.exp(-2.0), abs=0.01)
    leak, _ = df.unfriend_trial(random.Random(0), 0.0, 1.0, fix=True)
    assert not leak


def test_synchrone_refuse_plus_que_asynchrone():
    r = df.sync_vs_async(n_derived=4, n_steps=20 * 24 * 60, seed=2)
    assert r["sync_refused"] > r["async_refused"]
    assert r["lag"].shape == (4, 20 * 24 * 60)
    assert r["lag"][:, -1].max() >= 0


def test_index_en_ligne_rattrapage():
    rng = random.Random(0)
    changes = []
    for _ in range(500):
        k = rng.randrange(50)
        changes.append(("del", k, None) if rng.random() < 0.1 else ("put", k, rng.choice("abcde")))
    snap_at = 200
    b = df.IndexBuilder()
    b.load_snapshot(df.replay_table(changes, snap_at), snap_at)
    naive = b.as_dict()
    b.catch_up(changes)
    assert b.as_dict() == df.index_from_table(df.replay_table(changes))
    assert naive != b.as_dict()


def test_couplage():
    sync = df.coupling_sim("sync", 100, 10, [20, 20, 5])
    asyn = df.coupling_sim("async", 100, 10, [20, 20, 5])
    assert sync["accepted"].max() == 5
    assert asyn["accepted"].min() == 10
    assert asyn["backlog"][0].max() == 0 and asyn["backlog"][2, -1] == pytest.approx(500)


def test_dataflow_incremental():
    g = df.Dataflow()
    g.set("a", 1); g.set("b", 2); g.set("c", 10)
    g.define("s", lambda a, b: a + b, "a", "b")
    g.define("t", lambda s: s * 2, "s")
    g.define("u", lambda c: c + 1, "c")
    n0 = g.recomputations
    touched = g.set("a", 5)
    assert touched == ["s", "t"] and g["t"] == 14 and g["u"] == 11
    assert g.recomputations - n0 == 2


def test_changelog_sqlite():
    conn = df.make_changelog_db()
    conn.execute("INSERT INTO products VALUES (1, 'stylo', 2.0)")
    conn.execute("UPDATE products SET price = 2.5 WHERE id = 1")
    conn.execute("DELETE FROM products WHERE id = 1")
    rows = conn.execute("SELECT op, id, price FROM changelog ORDER BY seq").fetchall()
    assert rows == [("put", 1, 2.0), ("put", 1, 2.5), ("del", 1, None)]


def test_asof_join():
    t = np.array([0.0, 10.0, 20.0])
    v = np.array([1.0, 2.0, 3.0])
    assert list(df.asof_join(np.array([0.0, 9.9, 10.0, 25.0]), t, v)) == [1.0, 1.0, 2.0, 3.0]


def test_index_inverse_et_grep():
    corpus = df.make_corpus(200, vocab=300, doc_len=30, seed=1)
    idx = df.InvertedIndex()
    for i, d in enumerate(corpus):
        idx.add(i, d)
    for terms in ([0, 1], [3], [5, 7, 11]):
        res, ops = df.grep_search(corpus, terms)
        assert idx.search_and(terms) == res
        assert ops == 200 * 30


def test_portee_url():
    followers = {"a": {1, 2, 3}, "b": {3, 4}, "c": {4, 5, 6}}
    r = df.reach_scatter_gather(followers, ["a", "b", "c"], n_shards=3)
    assert r["reach"] == 6
    assert sum(r["partial"]) == 6


def test_identifiants_de_requete():
    naive = df.retrying_client(df.make_bank_db({1: 0.0, 2: 1000.0}), 50, 0.3, use_ids=False, seed=3)
    dedup = df.retrying_client(df.make_bank_db({1: 0.0, 2: 1000.0}), 50, 0.3, use_ids=True, seed=3)
    assert naive["credited"] > naive["intended"]
    assert dedup["credited"] == dedup["intended"]
    assert df.uuid_collision_prob(1e9, 122) < 1e-18
    assert df.uuid_collision_prob(2 ** 16, 32) == pytest.approx(1 - np.exp(-0.5), rel=1e-6)


def test_bout_en_bout():
    m = df.checksum_detection_matrix(n_trials=100, seed=0)
    assert m[("réseau", "sommes par lien (TCP)")] == 1.0
    assert m[("bogue émetteur", "sommes par lien (TCP)")] == 0.0
    assert m[("disque", "lien + disque")] == 1.0
    assert m[("bogue récepteur", "lien + disque")] == 0.0
    for f in df.FAULTS[1:]:
        assert m[(f, "bout en bout")] == 1.0
    assert m[("aucune", "bout en bout")] == 0.0


def test_unicite_par_journal():
    reqs = [(f"r{i}", f"user{i % 30}") for i in range(100)]
    r = df.claim_usernames(reqs, 4)
    assert r["duplicates"] == 0
    assert sum(v == "accepté" for v in r["results"].values()) == 30
    assert r["results"]["r0"] == "accepté" and r["results"]["r30"] == "refusé"
    rng = np.random.default_rng(0)
    times = np.sort(rng.uniform(0, 100, 400))
    names = [f"n{x}" for x in rng.integers(0, 100, 400)]
    assert df.multi_leader_claims(times, names, 3, repl_delay=5.0) > 0
    assert df.multi_leader_claims(times, names, 1, repl_delay=5.0) == 0


@pytest.mark.parametrize("p_crash", [0.0, 0.3])
def test_virements_multishard(p_crash):
    rng = random.Random(1)
    accts = {f"c{i}": 100.0 for i in range(8)}
    ps = df.PaymentSystem(dict(accts), n_shards=3, fee=1.0)
    for i in range(200):
        a, b = rng.sample(sorted(accts), 2)
        ps.submit(f"req{i}", a, b, float(rng.randint(1, 40)))
    ps.run(seed=2, p_crash=p_crash)
    chk = ps.check()
    assert chk["total"] == pytest.approx(800.0)
    assert chk["min_balance"] >= 0
    assert chk["accepted"] + chk["refused"] == 200
    if p_crash:
        assert ps.crashes > 0


def test_surreservation():
    best, profits = df.best_overbooking(100, 0.9, 200.0, 600.0)
    assert 3 <= best <= 15
    assert profits[best] > profits[0]


def test_inventaire_regions():
    coord = df.inventory_regions(threshold=float("inf"), stock=300, horizon=300, seed=1)
    free = df.inventory_regions(threshold=0, stock=300, horizon=300, seed=1)
    assert coord["oversold"] == 0
    assert free["oversold"] > 0 and free["coordinations"] < coord["coordinations"]


def test_verification_periodique():
    p_no = df.scrub_loss_probability(2, 0.1, 10.0, 10.0)
    p_scrub = df.scrub_loss_probability(2, 0.1, 0.5, 10.0)
    assert p_scrub < p_no
    sim = df.simulate_scrubbing(20_000, 2, 0.1, 0.5, 10.0, seed=0) / 20_000
    assert sim == pytest.approx(p_scrub, abs=0.01)


def test_chaine_de_hachage():
    log = df.HashChainLog()
    for i in range(10):
        log.append(f"evt{i}")
    assert log.verify() is None
    log.entries[4]["payload"] = "evt4-falsifié"
    assert log.verify() == 4


@pytest.mark.parametrize("n", [1, 2, 5, 8, 13])
def test_merkle(n):
    leaves = [f"x{i}" for i in range(n)]
    t = df.MerkleTree(leaves)
    root = t.root()
    for i in range(n):
        assert df.MerkleTree.verify_proof(leaves[i], t.proof(i), root)
    if n > 1:
        assert not df.MerkleTree.verify_proof("autre", t.proof(0), root)
    other = list(leaves)
    other[n // 2] = "corrompu"
    diff, comps = t.diff(df.MerkleTree(other))
    assert diff == [n // 2]
