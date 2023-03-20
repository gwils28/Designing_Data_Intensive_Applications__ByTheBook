"""Tests rapides de ddia/batch.py (chapitre Batch Processing)."""

import math
import os

import numpy as np
import pytest

from ddia import batch as B


@pytest.fixture(scope="module")
def lines():
    return B.generate_access_log(5000, n_urls=300, seed=3)


def test_journal_et_top_k(lines, tmp_path):
    assert len(lines) == 5000
    assert all(len(l.split()) >= 7 for l in lines[:50])
    top = B.top_k_hash(lines, 5)
    assert top[0][1] == "/"
    p = str(tmp_path / "a.log")
    B.write_lines(p, lines)
    res, dt = B.unix_top_k(p, 5)
    assert res == top
    res2, _ = B.unix_top_k(p, 5, sort_opts="-S 64K")
    assert res2 == top


def test_tri_externe(lines):
    urls = [B.url_of(l) for l in lines]
    out, st = B.external_sort(urls, max_in_memory=300, fan_in=4)
    assert out == sorted(urls)
    assert st.n_runs == math.ceil(5000 / 300)
    assert st.merge_passes == B.merge_passes(st.n_runs, 4) == 3
    assert st.max_in_memory == 300
    counts = dict(B.uniq_count(out))
    assert sum(counts.values()) == 5000


def test_contre_pression():
    n = 200
    r = B.simulate_pipe(np.full(n, 1.0), np.full(n, 2.0), buffer_size=5)
    occ = np.arange(n) - np.searchsorted(r["take"], r["put"], side="right") + 1
    assert occ.max() <= 5 + 1
    assert r["done"][-1] == pytest.approx(1 + 2 * n)
    assert float(r["producer_blocked"]) > 0


def test_placement_hdfs():
    pl = B.place_blocks(200, 30, 3, nodes_per_rack=10, seed=1)
    for reps in pl:
        assert len(set(reps)) == 3
        racks = {B.rack_of(int(x), 10) for x in reps}
        assert len(racks) == 2
    loc = B.schedule_locality(pl, 30, 8, busy_fraction=0.0)
    assert loc["nœud"] == 1.0


def test_parite_et_pertes():
    rng = np.random.default_rng(0)
    blocks = [rng.integers(0, 256, 64, dtype=np.uint8).tobytes() for _ in range(4)]
    par = B.xor_parity(blocks)
    assert B.xor_recover(blocks[:2] + blocks[3:], par) == blocks[2]
    assert B.p_loss_replication(0.01, 3) == pytest.approx(1e-6)
    assert B.p_loss_erasure(0.01, 6, 3) == pytest.approx(math.comb(9, 4) * 1e-8, rel=0.05)


def test_workflow():
    w = B.Workflow().add("a", 2).add("b", 3, ["a"]).add("c", 1, ["a"]).add("d", 2, ["b", "c"])
    assert w.topo_order() == ["a", "b", "c", "d"]
    assert w.critical_path() == (["a", "b", "d"], 7.0)
    rows = w.schedule(attempts={"b": 1})
    assert max(r["fin"] for r in rows) == 10.0
    assert w.downstream("b") == ["d"]


def test_relances():
    assert B.expected_time_with_restarts(1.0, 0.0) == 1.0
    sim = B.simulate_restarts(2.0, 0.5, 4000, seed=1).mean()
    assert sim == pytest.approx(B.expected_time_with_restarts(2.0, 0.5), rel=0.05)
    assert B.simulate_job(100, 100, 10, 0.0) == pytest.approx(10.0)


def _m(line):
    yield B.url_of(line), 1


def _r(k, vs):
    yield [k, sum(vs)]


def _c(k, vs):
    yield sum(vs)


def test_mapreduce_sequentiel_parallele_combineur(lines):
    shards = [lines[i::4] for i in range(4)]
    attendu = {u: c for c, u in B.top_k_hash(lines, 10_000)}
    for kw in (dict(), dict(processes=3), dict(combiner=_c)):
        outs, st = B.run_mapreduce(shards, _m, _r, n_reducers=3, **kw)
        got = {u: c for o in outs for u, c in o}
        assert got == attendu
        assert st.spill_files == 12
        assert sum(st.records_per_reducer) == st.shuffle_records
    assert st.shuffle_records < 5000          # le combineur réduit le shuffle
