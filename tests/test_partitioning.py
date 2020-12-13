"""Tests du module ddia.partitioning (chapitre « Sharding », 1re éd. « Partitioning »)."""
import subprocess
import sys

import numpy as np
import pytest

from ddia import partitioning as P


def test_hachage_stable_entre_processus():
    code = "from ddia.partitioning import stable_hash; print(stable_hash('clé'))"
    outs = {subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env={"PYTHONHASHSEED": str(s)}).stdout.strip() for s in (1, 2)}
    assert len(outs) == 1 and int(outs.pop()) == P.stable_hash("clé")


def test_mix64_uniforme():
    u = P.hash_unit(np.arange(100_000))
    assert 0 <= u.min() and u.max() < 1
    counts = np.histogram(u, bins=10, range=(0, 1))[0]
    assert counts.min() > 9_500 and counts.max() < 10_500


def test_slots_redis():
    assert P.redis_crc16(b"123456789") == 0x31C3
    assert P.redis_slot("foo") == 12182
    assert P.redis_slot("{user1000}.following") == P.redis_slot("{user1000}.followers")
    assert P.redis_hash_tag("foo{}{bar}") == "foo{}{bar}"
    assert P.redis_hash_tag("{a}{b}") == "a"


def test_partition_par_plage():
    kr = P.KeyRangePartitioner(["d", "m", "t"])
    assert [kr.shard_of(k) for k in ["a", "d", "lz", "m", "zz"]] == [0, 1, 1, 2, 3]
    assert kr.shards_for_range("b", "n") == [0, 1, 2]
    titles = P.synthetic_titles(4_000, seed=1)
    kr = P.KeyRangePartitioner.from_sample(titles, 8)
    loads = P.shard_loads([kr.shard_of(t) for t in titles], 8)
    assert P.imbalance(loads) < 1.01


def test_partitionnement_dynamique():
    d = P.DynamicRangeSharding(max_keys=100, hashed=True)
    for k in range(5_000):
        d.insert(k)
    assert d.sizes().sum() == 5_000 and d.sizes().max() <= 100
    assert d.n_shards >= 50 and d.rewritten > 5_000
    d2 = P.DynamicRangeSharding(max_keys=100, presplit=[1000, 2000])
    assert d2.n_shards == 3 and d2.shard_of(1500) == 1


def test_modulo_deplace_presque_tout():
    keys = np.arange(50_000)
    f = P.moved_fraction(P.mod_n_assign(keys, 3), P.mod_n_assign(keys, 4))
    assert 0.73 < f < 0.77


def test_nombre_fixe_de_partitions():
    keys = np.arange(50_000)
    c = P.FixedShardCluster(120, ["a", "b", "c"])
    before = c.owner_names(keys)
    moved = c.add_node("d")
    assert len(moved) == 30 and set(c.shards_per_node().values()) == {30}
    assert abs(P.moved_fraction(before, c.owner_names(keys)) - 0.25) < 0.02
    c.remove_node("a")
    assert sorted(c.shards_per_node().values()) == [40, 40, 40]
    w = P.FixedShardCluster(90, ["x", "y"], weights={"x": 2.0, "y": 1.0})
    assert w.shards_per_node() == {"x": 60, "y": 30}


def test_anneau_coherent():
    keys = np.arange(40_000)
    r = P.HashRing(["a", "b", "c"], vnodes=64, seed=3)
    assert abs(sum(r.ownership().values()) - 1) < 1e-9
    before = r.node_of(keys)
    r.add_node("d")
    after = r.node_of(keys)
    assert set(after[before != after]) == {"d"}          # seules les clés vers d bougent
    assert 0.18 < P.moved_fraction(before, after) < 0.32


def test_zipf_et_sel():
    w = P.zipf_weights(1000, 1.0)
    assert abs(w.sum() - 1) < 1e-12 and w[0] > 100 * w[-1]
    v = P.salted_variants("post:42", 100)
    assert len(set(v)) == 100 and v[7] == "post:42#07"


def test_index_locaux_et_globaux():
    cars = P.car_listings(2_000, seed=2)
    loc, glo = P.LocalIndexDB(8), P.GlobalIndexDB(8)
    assert all(loc.put(c) == 1 for c in cars)
    touched = [glo.put(c) for c in cars]
    assert max(touched) <= 3 and np.mean(touched) > 1.5
    ids_l, n_l = loc.query("color", "rouge")
    ids_g, n_g = glo.query("color", "rouge")
    assert ids_l == ids_g == {c["id"] for c in cars if c["color"] == "rouge"}
    assert (n_l, n_g) == (8, 1)
    a, _, _ = loc.query_and(("color", "rouge"), ("make", "Renault"))
    b, _, moved = glo.query_and(("color", "rouge"), ("make", "Renault"))
    assert a == b and moved >= len(a)
