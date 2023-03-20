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


def test_tri_secondaire():
    users = [{"user_id": 1, "birth": 1990}, {"user_id": 2, "birth": 1970}]
    events = [{"user_id": 1, "url": "/a", "ts": 5}, {"user_id": 1, "url": "/b", "ts": 1},
              {"user_id": 2, "url": "/a", "ts": 3}]

    def mapper(rec):
        if "birth" in rec:
            yield (rec["user_id"], 0, 0), rec["birth"]
        else:
            yield (rec["user_id"], 1, rec["ts"]), rec["url"]

    def reducer(uid, vals):
        birth = next(vals)
        for url in vals:
            yield [uid, url, birth]

    outs, _ = B.run_mapreduce([users, events], mapper, reducer, n_reducers=2, group_by=lambda k: k[0])
    rows = sorted(tuple(x) for o in outs for x in o)
    assert rows == [(1, "/b", 1990), (1, "/a", 1990), (2, "/a", 1970)] or \
        rows == sorted([(1, "/b", 1990), (1, "/a", 1990), (2, "/a", 1970)])
    per_user = [x for o in outs for x in o if x[0] == 1]
    assert [x[1] for x in per_user] == ["/b", "/a"]      # ordre des horodatages


def test_jointures_equivalentes():
    users = B.make_users(300, seed=1)
    events = B.make_events(3000, 300, seed=2)
    ref = sorted((e["ts"], e["user_id"], u["birth"]) for e, u in B.sort_merge_join(events, users, "user_id"))
    assert len(ref) == 3000
    shards = [events[i::5] for i in range(5)]
    bh, st = B.broadcast_hash_join(shards, users, "user_id")
    assert sorted((e["ts"], e["user_id"], u["birth"]) for e, u in bh) == ref
    assert st["enregistrements_diffuses"] == 1500
    big = [[e for e in events if B.partition_of(e["user_id"], 5) == i] for i in range(5)]
    small = [[u for u in users if B.partition_of(u["user_id"], 5) == i] for i in range(5)]
    ph, _ = B.partitioned_hash_join(big, small, "user_id")
    assert sorted((e["ts"], e["user_id"], u["birth"]) for e, u in ph) == ref
    with pytest.raises(ValueError):
        B.partitioned_hash_join(big, small[::-1], "user_id")
    assert B.join_network_cost(100, 1, 10, "broadcast") == 9
    assert B.join_network_cost(100, 1, 10, "shuffle") == pytest.approx(90.9)


def test_asymetrie_et_salage():
    keys = B.zipf_keys(20_000, 1000, s=1.2, seed=0)
    loads = B.reducer_loads(keys, 10)
    assert loads.sum() == 20_000
    salted = B.salted_loads(keys, 10, hot=[0, 1, 2], salt=10)
    assert salted.sum() == 20_000
    assert salted.max() < loads.max()


def test_dataset_fusion_et_lignage(lines):
    shards = [lines[i::4] for i in range(4)]
    ds = (B.Dataset.source(shards).map(B.url_of, "url").map(lambda u: (u, 1), "paire")
          .reduce_by_key(lambda a, b: a + b, 3, "compte"))
    assert len(ds.stages()) == 2
    e1, e2 = B.Engine(fuse=True), B.Engine(fuse=False)
    r1, r2 = sorted(e1.collect(ds)), sorted(e2.collect(ds))
    assert r1 == r2 == sorted((u, c) for c, u in B.top_k_hash(lines, 10_000))
    assert e1.materialized == 0 and e2.materialized > 0
    assert e1.tasks == 4 + 3 and e2.tasks == 4 * 2 + 3
    key = next(iter(e1.shuffle_store))
    lost = len(e1.shuffle_store[key][1])
    e1.shuffle_store[key].pop(1)
    e1.reset_counters()
    assert sorted(e1.collect(ds)) == r1
    assert e1.shuffled == lost and e1.recomputed == [("paire", 1)]
    j = B.Dataset.source([[(1, "a"), (2, "b")]]).join(B.Dataset.source([[(1, "x"), (1, "y")]]), 2)
    assert sorted(B.Engine().collect(j)) == [(1, ("a", "x")), (1, ("a", "y"))]


def test_pregel():
    g = {0: [1, 2], 1: [2], 2: [0], 3: [2]}
    pr, hist = B.pagerank_pregel(g, n_iter=50)
    assert sum(pr.values()) == pytest.approx(1.0)
    assert pr[2] > pr[1] > pr[3]
    cc, h = B.connected_components_pregel({0: [1], 1: [2], 3: [4], 5: []})
    assert cc == {0: 0, 1: 0, 2: 0, 3: 3, 4: 3, 5: 5}
    assert h[-1]["messages"] == 0


def test_kv_trie_et_publication(tmp_path):
    items = [(f"k{i:05d}", i * i) for i in range(1000)]
    v1 = tmp_path / "v1"
    v1.mkdir()
    st = B.write_sorted_kv(str(v1 / "data"), items, index_every=50)
    assert st["records"] == 1000 and st["index_entries"] == 20
    f = B.SortedKVFile(str(v1 / "data"))
    assert f.get("k00042") == 42 * 42 and f.get("zzz") is None and f.get("a") is None
    assert f.lines_scanned <= 2 * 50
    f.close()
    with pytest.raises(ValueError):
        B.write_sorted_kv(str(tmp_path / "bad"), [("b", 1), ("a", 2)])
    v2 = tmp_path / "v2"
    v2.mkdir()
    B.publish_version(str(tmp_path), str(v1))
    assert B.current_version(str(tmp_path)) == "v1"
    B.publish_version(str(tmp_path), str(v2))
    assert B.current_version(str(tmp_path)) == "v2"
    assert os.path.islink(tmp_path / "current")


def test_documents():
    docs = B.make_documents(20, seed=1)
    assert len(docs) == 20 and all(len(d.split()) == 80 for d in docs.values())
