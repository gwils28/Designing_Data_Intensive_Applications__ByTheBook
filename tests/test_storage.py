"""Tests du module ddia.storage (chapitre Storage and Retrieval)."""
import random

import numpy as np
import pandas as pd
import pytest

from ddia import storage as S


# ------------------------------------------------------------------ journal et hachage
def test_bashdb_garde_la_derniere_valeur(tmp_path):
    db = S.BashDB(str(tmp_path / "database"))
    db.set("42", '{"ville":"Lyon"}')
    db.set("12", "x")
    db.set("42", '{"ville":"Lille"}')
    assert db.get("42") == '{"ville":"Lille"}'
    assert db.get("99") is None
    assert db.bytes_scanned == 2 * db.size()  # deux lectures complètes du fichier


def test_enregistrement_et_troncature():
    data = S.encode_record("a", "1") + S.encode_record("b", None) + S.encode_record("c", "333")
    recs = list(S.iter_records(data))
    assert [(k, v) for _, k, v, _ in recs] == [("a", "1"), ("b", None), ("c", "333")]
    assert len(list(S.iter_records(data[:-2]))) == 2  # dernier enregistrement tronqué
    corrupt = bytearray(data)
    corrupt[-1] ^= 0xFF
    assert len(list(S.iter_records(bytes(corrupt)))) == 2  # somme de contrôle fausse
    assert S.record_size("a", "1") == len(S.encode_record("a", "1"))


def test_hash_index_compaction_et_reprise(tmp_path):
    d = str(tmp_path / "bitcask")
    st = S.HashIndexStore(d, segment_bytes=2048)
    rng = random.Random(0)
    ref = {}
    for i in range(3000):
        k = f"k{rng.randrange(200)}"
        st.put(k, f"v{i}")
        ref[k] = f"v{i}"
        if i % 700 == 699:
            st.compact()
    st.delete("k3")
    ref.pop("k3", None)
    before = st.disk_bytes()
    st.compact()
    assert st.disk_bytes() < before
    assert all(st.get(k) == v for k, v in ref.items())
    st.close()
    # écriture partielle simulée à la fin du segment actif
    with open(st._path(st.active), "ab") as f:
        f.write(S.encode_record("k7", "perdu")[:9])
    st2 = S.HashIndexStore.open(d, segment_bytes=2048)
    assert st2.get("k3") is None
    assert all(st2.get(k) == v for k, v in ref.items())


# ------------------------------------------------------------------------ Bloom
def test_bloom_sans_faux_negatif_et_taux_theorique():
    n = 5000
    bf = S.BloomFilter.for_capacity(n, 10)
    for i in range(n):
        bf.add(f"in{i}")
    assert all(f"in{i}" in bf for i in range(n))
    fp = np.mean([f"out{i}" in bf for i in range(20000)])
    theo = S.bloom_fp_rate(n, bf.m, bf.k)
    assert abs(fp - theo) < 0.005
    assert 0.005 < theo < 0.012  # règle empirique : 10 bits par clé -> environ 1 %
    assert S.bloom_optimal_k(10 * n, n) == 7
    assert S.bloom_bits_per_key(0.01) == pytest.approx(9.585, abs=1e-3)


# ------------------------------------------------------------------- SSTable, LSM
def test_merge_runs_garde_la_plus_recente():
    new = [("a", "2"), ("c", None)]
    old = [("a", "1"), ("b", "1"), ("c", "1")]
    assert S.merge_runs([new, old]) == [("a", "2"), ("b", "1"), ("c", None)]
    assert S.merge_runs([new, old], drop_tombstones=True) == [("a", "2"), ("b", "1")]


def test_sstable_index_epars():
    items = [(S.kv_key(i), S.kv_value(i)) for i in range(0, 2000, 2)]
    t = S.SSTable(items, block_bytes=1024)
    assert len(t.sparse_index) == len(t.blocks) > 50
    st = {}
    assert t.get(S.kv_key(500), st) == (True, S.kv_value(500))
    assert t.get(S.kv_key(501), st)[0] is False
    assert st["blocks_read"] <= 2
    assert [k for k, _ in t.scan(S.kv_key(10), S.kv_key(16))] == [S.kv_key(i) for i in (10, 12, 14, 16)]


@pytest.mark.parametrize("strategy", ["leveled", "tiered", "none"])
def test_lsm_correct_quelle_que_soit_la_strategie(strategy):
    lsm = S.LSMTree(memtable_bytes=8 * 1024, strategy=strategy)
    keys = S.zipf_keys(8000, 2000, s=0.9, seed=3)
    ref = {}
    for n, i in enumerate(keys):
        k = S.kv_key(int(i))
        if n % 17 == 0:
            lsm.delete(k)
            ref[k] = None
        else:
            v = S.kv_value(int(i), n)
            lsm.put(k, v)
            ref[k] = v
    assert all(lsm.get(k) == v for k, v in ref.items())
    lo, hi = S.kv_key(100), S.kv_key(300)
    expected = sorted((k, v) for k, v in ref.items() if lo <= k <= hi and v is not None)
    assert lsm.scan(lo, hi) == expected
    assert lsm.write_amplification() > 1
