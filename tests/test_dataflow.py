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
