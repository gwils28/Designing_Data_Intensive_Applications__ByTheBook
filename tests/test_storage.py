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


def test_lsm_niveaux_disjoints_et_amplification():
    lsm = S.LSMTree(memtable_bytes=8 * 1024, strategy="leveled", fanout=4)
    for n, i in enumerate(S.zipf_keys(15000, 5000, s=0.5, seed=0)):
        lsm.put(S.kv_key(int(i)), S.kv_value(int(i), n))
    for lvl in lsm.levels[1:]:
        for a, b in zip(lvl, lvl[1:]):
            assert a.max_key < b.min_key
    tiered = S.LSMTree(memtable_bytes=8 * 1024, strategy="tiered")
    for n, i in enumerate(S.zipf_keys(15000, 5000, s=0.5, seed=0)):
        tiered.put(S.kv_key(int(i)), S.kv_value(int(i), n))
    assert tiered.write_amplification() < lsm.write_amplification()
    assert lsm.space_amplification() < tiered.space_amplification()


def test_lsm_reprise_depuis_le_wal():
    lsm = S.LSMTree(memtable_bytes=1 << 20)
    for i in range(50):
        lsm.put(S.kv_key(i), S.kv_value(i))
    full = lsm.crash()
    assert full.replayed == 50 and full.get(S.kv_key(49)) == S.kv_value(49)
    cut = lsm.crash(keep_wal_bytes=len(lsm.wal) - 5)  # dernier enregistrement à moitié écrit
    assert cut.replayed == 49 and cut.get(S.kv_key(49)) is None


# --------------------------------------------------------------------------- B-tree
def test_btree_insertion_recherche_intervalle():
    bt = S.BPlusTree(max_keys=8)
    keys = list(range(5000))
    random.Random(1).shuffle(keys)
    for k in keys:
        bt.insert(k, -k)
    assert len(bt) == 5000
    assert bt.get(1234) == -1234 and bt.get(99999) is None
    assert [k for k, _ in bt.range(100, 110)] == list(range(100, 111))
    assert [k for k, _ in bt.items()] == list(range(5000))
    assert 4 <= bt.height <= 6
    assert 0.6 < bt.fill_factor() < 0.8  # environ ln 2 pour des insertions aléatoires
    assert BPlusTree_ok(bt)


def BPlusTree_ok(bt):
    r = S.BPlusTree.check(bt.pages, bt.root)
    return not r["problemes"] and not r["cles_perdues"] and not r["pages_orphelines"]


def test_btree_panne_pendant_un_eclatement_et_wal():
    bt = S.BPlusTree(max_keys=4, track_disk=True)
    for k in [10, 20, 30, 40]:
        bt.insert(k, k)
    with pytest.raises(S.SimulatedCrash):
        bt.insert(25, 25, crash_after=2)
    r = S.BPlusTree.check(bt.disk, bt.disk_root())
    assert r["cles_perdues"] and r["pages_orphelines"]
    bt = S.BPlusTree(max_keys=4, track_disk=True, wal=True)
    for k in [10, 20, 30, 40]:
        bt.insert(k, k)
    with pytest.raises(S.SimulatedCrash):
        bt.insert(25, 25, crash_after=2)
    pages = bt.recover_from_wal()
    r = S.BPlusTree.check(pages, pages[0].keys[0])
    assert r["cles_atteignables"] == 5 and not r["cles_perdues"]


def test_btree_amplification_et_cache():
    direct = S.BPlusTree(max_keys=32, page_bytes=4096, record_bytes=100)
    cached = S.BPlusTree(max_keys=32, page_bytes=4096, record_bytes=100, checkpoint_every=500)
    for k in random.Random(2).sample(range(100000), 5000):
        direct.insert(k, k)
        cached.insert(k, k)
    assert direct.write_amplification() > 40
    assert cached.write_amplification() < direct.write_amplification() / 2


def test_capacite_btree():
    c = S.btree_capacity(4096, key_bytes=0, ref_bytes=8)
    assert c["branchement"] == 512
    assert S.btree_capacity(4096, 0, 8192 // 1000)["branchement"] >= 500


def test_modele_disque():
    assert S.io_time("HDD", n_random=100) > 100 * S.io_time("SSD NVMe", n_random=1)
    assert S.io_time("HDD", seq_bytes=180e6) == pytest.approx(1.0)


# ------------------------------------------------------------------- plein texte
def test_index_inverse_et_intersection():
    idx = S.InvertedIndex()
    docs = ["pommes rouges et poires", "pommes vertes", "cerises rouges", "Pommes ROUGES"]
    for i, d in enumerate(docs):
        idx.add(i, d)
    res, comps = idx.search_and(["rouges", "pommes"])
    assert res == [0, 3] and comps > 0
    assert (idx.bitmap("rouges") & idx.bitmap("pommes")).nonzero()[0].tolist() == [0, 3]


def test_levenshtein_et_trie():
    assert S.levenshtein("chat", "chats") == 1
    assert S.levenshtein("kitten", "sitting") == 3
    vocab = S.make_vocabulary(3000, seed=0)
    trie = S.Trie(vocab)
    w = vocab[123]
    q = w[:-1] + ("x" if w[-1] != "x" else "y")
    res, visited = trie.fuzzy(q, 1)
    brute = sorted((v, S.levenshtein(q, v)) for v in vocab if S.levenshtein(q, v) <= 1)
    assert sorted(res) == brute and (w, 1) in res
    assert visited < trie.n_nodes / 3


def test_ngrammes():
    assert S.ngrams("hello") == ["hel", "ell", "llo"]
    assert S.ngrams("ab") == []


def test_documents_synthetiques():
    vocab = S.make_vocabulary(500, seed=1)
    docs = S.make_documents(50, vocab, words_per_doc=20)
    assert len(docs) == 50 and all(len(d.split()) == 20 for d in docs)


def test_zorder():
    assert S.zorder(0b11, 0b00) == 0b0101
    assert S.zorder(0b00, 0b11) == 0b1010
    x = np.array([3, 1]); y = np.array([5, 2])
    assert list(S.zorder(x, y)) == [S.zorder(3, 5), S.zorder(1, 2)]


# ---------------------------------------------------------------------- colonnes
def test_rle_dictionnaire_bitmaps():
    a = np.array([5, 5, 5, 2, 2, 7, 5, 5])
    v, l = S.rle_encode(a)
    assert v.tolist() == [5, 2, 7, 5] and l.tolist() == [3, 2, 1, 2]
    assert S.rle_decode(v, l).tolist() == a.tolist()
    d, codes = S.dict_encode(np.array(["b", "a", "b"]))
    assert d.tolist() == ["a", "b"] and codes.tolist() == [1, 0, 1]
    bm = S.bitmap_index(a)
    assert bm[2].tolist() == [0, 0, 0, 1, 1, 0, 0, 0]
    assert S.bitmap_rle_runs(bm[2]) == [3, 2, 3]
    assert S.bitmap_rle_runs(bm[5]) == [0, 3, 3, 2]
    assert S.bits_for(100000) == 17
    assert S.varint_size(127) == 1 and S.varint_size(128) == 2


def test_make_sales_et_cube():
    fact, prod, date, store = S.make_sales(5000, n_extra_cols=3, seed=0)
    assert len(fact) == 5000 and fact["date_key"].is_monotonic_increasing
    assert set(fact["product_sk"]).issubset(set(prod["product_sk"]))
    cube = S.data_cube(fact, ["date_key", "product_sk", "store_sk"], "net_price")
    assert len(cube) == 8
    assert cube[()] == pytest.approx(fact["net_price"].sum())
    assert cube[("store_sk",)].sum() == pytest.approx(fact["net_price"].sum())


def test_interpretation_et_compilation():
    rng = np.random.default_rng(0)
    cols = {"product_sk": rng.integers(0, 10, 2000).tolist(), "store_sk": rng.integers(0, 5, 2000).tolist(),
            "quantity": rng.integers(1, 5, 2000).tolist()}
    where = [("product_sk", "=", 3), ("store_sk", "in", (1, 2))]
    expected = sum(q for p, s, q in zip(cols["product_sk"], cols["store_sk"], cols["quantity"])
                   if p == 3 and s in (1, 2))
    assert S.interpret_query(cols, where, "quantity") == expected
    fn, src = S.compile_query(where, "quantity")
    assert fn(cols) == expected and "product_sk[i] == 3" in src
