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
