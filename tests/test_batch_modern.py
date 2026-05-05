"""Tests de ddia/batch_modern.py (notions de la 2e édition)."""

import numpy as np
import pytest

from ddia import batch_modern as M


def test_object_store_prefixes_et_renommage():
    s = M.ObjectStore()
    for i in range(5):
        s.put("b", f"out/tmp/part-{i}", b"x")
    s.put("b", "out/tmp/sub/a", b"y")
    assert len(s.list("b", "out/")) == 6          # liste récursive
    assert s.list_dirs("b", "out/") == ["out/tmp/"]
    with pytest.raises(M.RenameInterrupted):
        s.rename_prefix("b", "out/tmp/", "out/final/", crash_after=3)
    assert len(s.list("b", "out/final/")) == 3 and len(s.list("b", "out/tmp/")) == 3
    s.delete("b", "out/tmp/sub/a")
    assert s.list_dirs("b", "out/tmp/") == []      # pas de répertoire vide


def test_manifeste():
    s = M.ObjectStore()
    assert M.read_committed(s, "b", "t") == []
    M.commit_with_manifest(s, "b", "t", "a1", {"p0": b"1", "p1": b"2"})
    with pytest.raises(M.RenameInterrupted):
        M.commit_with_manifest(s, "b", "t", "a2", {"p0": b"3", "p1": b"4"}, crash_after=1)
    assert M.read_committed(s, "b", "t") == [b"1", b"2"]
    M.commit_with_manifest(s, "b", "t", "a3", {"p0": b"5"})
    assert M.read_committed(s, "b", "t") == [b"5"]


def test_ordonnancement():
    jobs = [dict(name="A", arrival=0, tasks=100, duration=10), dict(name="B", arrival=0, tasks=100, duration=10)]
    assert M.simulate_cluster(jobs, 160, "fifo")["end"] == {"A": 10.0, "B": 20.0}
    assert M.simulate_cluster(jobs, 160, "fair")["end"] == {"A": 20.0, "B": 20.0}
    jobs[1]["gang"] = True
    g = M.simulate_cluster(jobs, 160, "gang")
    assert g["timeline"][0]["B"] == 0 and g["end"]["B"] == 20.0


def test_drf_exemple_de_l_article():
    r = M.drf([9, 18], {"A": [1, 4], "B": [3, 1]})
    assert r["tasks"] == {"A": 3, "B": 2}
    assert r["dominant_share"]["A"] == pytest.approx(2 / 3)
    assert r["dominant_share"]["B"] == pytest.approx(2 / 3)


def test_lazyframe_pushdown():
    g = M.make_table(100_000, row_group=10_000, seed=1)
    lf = (M.LazyFrame(g).filter("day", ">=", 300)
          .with_column("rev", lambda d: d["qty"] * d["price"], ["qty", "price"]).group_sum("store", "rev"))
    a = lf.collect(True)
    sa = dict(lf.stats)
    b = lf.collect(False)
    sb = dict(lf.stats)
    assert np.allclose(a["rev"], b["rev"])
    assert sa["groups_skipped"] > 0 and sa["values_read"] < sb["values_read"]
    assert "filtres poussés=[day >= 300]" in lf.explain(True)


def test_minhash_lsh():
    a = M.shingles("le chat noir dort sur le canapé rouge du salon ce soir", 3)
    b = M.shingles("le chat noir dort sur le canapé bleu du salon ce soir", 3)
    c = M.shingles("une phrase sans aucun rapport avec les deux autres textes ici", 3)
    j = M.jaccard(a, b)
    sigs = {i: M.minhash(x, 256, seed=0) for i, x in enumerate([a, b, c])}
    assert abs((sigs[0] == sigs[1]).mean() - j) < 0.12
    assert (sigs[0] == sigs[2]).mean() < 0.05
    cands = M.lsh_candidates(sigs, bands=64)
    assert (0, 1) in cands and (0, 2) not in cands
