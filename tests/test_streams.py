"""Tests du module ddia.streams (chapitre « traitement de flux »)."""

import math

import numpy as np
import pytest

from ddia import streams as s


def test_murmur2_identique_a_kafka():
    from kafka.partitioner.default import murmur2 as ref
    for key in [b"", b"a", b"ab", b"abc", b"abcd", b"user-42", "clé".encode(), bytes(range(37))]:
        assert s.murmur2(key) == ref(key) & 0xFFFFFFFF
    assert s.kafka_partition("user-42", 6) == (ref(b"user-42") & 0x7FFFFFFF) % 6


def test_journal_partitionne_offsets_et_ordre_par_cle():
    log = s.PartitionedLog(4, segment_size=5)
    for i in range(40):
        log.append(f"u{i % 7}", i)
    assert len(log) == 40
    for p in range(4):
        recs = log.read(p, 0)
        assert [r.offset for r in recs] == list(range(len(recs)))
    # tous les messages d'une clé dans la même partition, dans l'ordre d'écriture
    p = log.partition_for("u3")
    vals = [r.value for r in log.read(p, 0) if r.key == "u3"]
    assert vals == sorted(vals) and len(vals) == len(range(3, 40, 7))


def test_retention_et_offset_hors_limites():
    log = s.PartitionedLog(1, segment_size=10)
    for i in range(100):
        log.append(None, i)
    assert log.enforce_retention(3) == 70
    assert log.start_offset(0) == 70
    with pytest.raises(s.OffsetOutOfRange):
        log.read(0, 10)
    assert log.read(0, 95)[0].value == 95


def test_compaction_garde_la_derniere_valeur():
    log = s.PartitionedLog(1, segment_size=4)
    data = [("a", 1), ("b", 1), ("a", 2), ("c", 1), ("b", None), ("a", 3), ("d", 1), ("c", 2), ("e", 1)]
    for k, v in data:
        log.append(k, v, partition=0)
    log.compact(0)
    state = {}
    for r in log.read(0, 0):
        state[r.key] = r.value
    state = {k: v for k, v in state.items() if v is not None}
    assert state == {"a": 3, "c": 2, "d": 1, "e": 1}
    assert s.compact_records(data) == [(5, "a", 3), (6, "d", 1), (7, "c", 2), (8, "e", 1)]


def test_range_assign():
    a = s.range_assign(5, ["c2", "c1"])
    assert a == {"c1": [0, 1, 2], "c2": [3, 4]}
    a = s.range_assign(2, ["a", "b", "c"])
    assert a["c"] == []


def test_commits_doublons_et_pertes():
    after = s.simulate_offset_commits(2000, 50, 0.002, mode="after", seed=1)
    assert after["lost"] == 0 and after["duplicates"] > 0
    before = s.simulate_offset_commits(2000, 50, 0.002, mode="before", seed=1)
    assert before["duplicates"] == 0 and before["lost"] > 0
    sans = s.simulate_offset_commits(500, 10, 0.0, seed=0)
    assert sans["duplicates"] == 0 and sans["lost"] == 0


def test_file_partagee_reordonne_apres_panne():
    ok = s.simulate_shared_queue(30, 3, crash_prob=0.0, seed=0)
    assert sorted(p.msg for p in ok) == list(range(30))
    tr = s.simulate_shared_queue(60, 3, crash_prob=0.15, seed=3)
    done = [p.msg for p in tr if p.ok]
    assert sorted(done) == list(range(60))
    assert s.inversions(done) > 0
    assert s.inversions([0, 1, 2]) == 0 and s.inversions([2, 1, 0]) == 3


def test_blocage_en_tete_de_file():
    arr = np.arange(200) * 1.0
    serv = np.full(200, 0.5)
    serv[10] = 50.0
    shared = s.hol_latencies(arr, serv, 4, "shared")
    part = s.hol_latencies(arr, serv, 4, "partitioned")
    assert part[14] > 30 and shared[14] < 1


def test_debordement():
    prod = [10] * 20 + [0] * 20
    d = s.simulate_overflow(prod, 5, 20, "drop")
    assert d["lost"][-1] > 0
    b = s.simulate_overflow(prod, 5, 20, "backpressure")
    assert b["lost"][-1] == 0 and b["delivered"].sum() == pytest.approx(200)
    u = s.simulate_overflow(prod, 5, 20, "buffer")
    assert u["queue"].max() == pytest.approx(100)


def test_double_ecriture_contre_cdc():
    dual = s.simulate_dual_writes(3000, 200, rate=50, seed=2)
    cdc = s.simulate_dual_writes(3000, 200, rate=50, mode="cdc", seed=2)
    assert dual["frac_diverged"] > 0
    assert cdc["diverged"] == 0


def test_cdc_sqlite_et_index_derive():
    db = s.CdcDatabase()
    db.create_table("docs", "id", ["id", "text"])
    db.execute("INSERT INTO docs VALUES (1, 'chat noir')")
    db.execute("INSERT INTO docs VALUES (2, 'chien blanc')")
    db.execute("UPDATE docs SET text = 'chat blanc' WHERE id = 1")
    db.execute("DELETE FROM docs WHERE id = 2")
    ch = db.changes()
    assert [c["op"] for c in ch] == ["insert", "insert", "update", "delete"]
    idx = s.SearchIndex()
    for c in ch:
        idx.apply(c)
    assert idx.search("blanc") == [1] and idx.search("chien") == []
    lsn, rows = db.snapshot("docs")
    assert lsn == 4 and rows == [(1, "chat blanc")]


def test_event_store_concurrence_optimiste():
    es = s.EventStore()
    es.append("cart-1", [("added", {"item": "a"})], expected_version=0)
    es.append("cart-1", [("added", {"item": "b"}), ("removed", {"item": "a"})], expected_version=1)
    with pytest.raises(s.ConcurrencyError):
        es.append("cart-1", [("added", {"item": "c"})], expected_version=1)

    def apply(st, e):
        st = set() if st is None else set(st)
        (st.add if e.type == "added" else st.discard)(e.data["item"])
        return st
    assert s.replay(es.read("cart-1"), apply) == {"b"}
    seen = []
    es.subscribe(seen.append)
    es.append("cart-2", [("added", {"item": "z"})])
    assert len(seen) == 4


def test_cep_hll_percolateur():
    ev = [(0, "u", "fail"), (5, "u", "fail"), (9, "u", "fail"), (10, "u", "ok"),
          (100, "v", "fail"), (200, "v", "fail"), (300, "v", "fail"), (301, "v", "ok")]
    assert s.detect_brute_force(ev, 3, 60) == [(10, "u", 3)]
    h = s.HyperLogLog(12)
    for i in range(50_000):
        h.add(i % 20_000)
    assert abs(h.count() / 20_000 - 1) < 0.05
    hv = s.HyperLogLog(12)
    hv.add_hashes(np.array([s.hash64(i % 20_000) for i in range(50_000)], dtype=np.uint64))
    assert np.array_equal(hv.reg, h.reg)
    small = s.HyperLogLog(10)
    for i in range(100):
        small.add(i)
    assert abs(small.count() - 100) < 10
    p = s.Percolator(df={"maison": 0.3, "jardin": 0.05, "lyon": 0.1})
    p.add("q1", ["maison", "jardin"])
    p.add("q2", ["maison", "lyon"])
    assert p.match_naive(["maison", "jardin", "piscine"])[0] == ["q1"]
    hits, examined = p.match_indexed(["maison", "jardin", "piscine"])
    assert hits == ["q1"] and examined == 1


def test_fenetres():
    ts = [0.5, 1.2, 1.9, 3.1, 7.0]
    assert s.tumbling_counts(ts, 2.0) == {0.0: 3, 2.0: 1, 6.0: 1}
    hop = s.hopping_counts(ts, 2.0, 1.0)
    assert hop[1.0] == 2 and hop[0.0] == 3 and hop[-1.0] == 1
    assert list(s.sliding_counts(ts, 2.0)) == [1, 2, 3, 3, 1]
    sess = s.sessionize([("a", 0), ("a", 10), ("a", 50), ("b", 5)], gap=30)
    assert sess == [("a", 0, 10, 2), ("b", 5, 5, 1), ("a", 50, 50, 1)]


def test_filigrane_et_retardataires():
    et = np.array([1, 2, 11, 3, 12, 25, 4])
    arr = np.arange(len(et), dtype=float)
    wc = s.WatermarkCounter(10, lateness=0, policy="drop").run(et, arr)
    finals = {r.window: r.count for r in wc.results if r.kind == "final"}
    assert finals[0] == 2 and wc.dropped == 2          # 3 et 4 arrivent trop tard
    wc2 = s.WatermarkCounter(10, lateness=0, policy="update").run(et, arr)
    corr = [r for r in wc2.results if r.kind == "correction"]
    assert corr[-1].count == 4
    wc3 = s.WatermarkCounter(10, lateness=20, policy="drop").run(et, arr)
    assert wc3.dropped == 0


def test_jointures():
    j = s.SearchClickJoin(window=10)
    for ev in [("search", 1, 0), ("click", 1, 3), ("search", 2, 4), ("click", 3, 5),
               ("search", 3, 6), ("search", 4, 30)]:
        j.process(*ev)
    j.flush()
    res = {(o[0], o[1]) for o in j.out}
    assert ("clicked", 1) in res and ("not_clicked", 2) in res and ("clicked", 3) in res
    out = s.enrich([("profile", "u", "Paris"), ("activity", "u", "a1"), ("profile", "u", "Lyon"),
                    ("activity", "u", "a2")])
    assert out == [("u", "a1", "Paris"), ("u", "a2", "Lyon")]
    tv = s.TimelineView()
    for ev in [("post", "b", 1), ("follow", "a", "b"), ("post", "b", 2), ("post", "c", 3),
               ("follow", "a", "c"), ("unfollow", "a", "b"), ("delete_post", "c", 3)]:
        tv.apply(*ev)
    assert tv.view() == {}
    tv.apply("post", "c", 4)
    assert tv.view() == {"a": {4}}


def test_job_a_etat_semantiques():
    rng = np.random.default_rng(0)
    keys = list(rng.integers(0, 5, 300))
    crashes = [37, 120, 205]
    amo = s.run_stateful_job(keys, "at_most_once", crashes)
    alo = s.run_stateful_job(keys, "at_least_once", crashes)
    idem = s.run_stateful_job(keys, "idempotent", crashes)
    tx = s.run_stateful_job(keys, "transactional", crashes)
    assert amo["missing"] > 0 and amo["excess"] == 0
    assert alo["excess"] > 0 and alo["missing"] == 0
    assert idem["excess"] == idem["missing"] == 0
    assert tx["excess"] == tx["missing"] == 0


def test_fencing_et_young():
    sink = s.FencedSink(fencing=True)
    assert sink.write("k", 1, epoch=1)
    assert sink.write("k", 2, epoch=2)
    assert not sink.write("k", "zombie", epoch=1)
    assert sink.data["k"] == 2
    naive = s.FencedSink(fencing=False)
    naive.write("k", 2, 2)
    naive.write("k", "zombie", 1)
    assert naive.data["k"] == "zombie"
    assert s.young_interval(2.0, 3600) == pytest.approx(math.sqrt(14400))
