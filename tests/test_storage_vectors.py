"""Tests de ddia.storage_vectors (index vectoriels, 2e édition)."""
import numpy as np

from ddia import storage_vectors as V


def _data():
    X, _ = V.make_embeddings(1500, dim=16, n_topics=10, seed=0)
    Q = V.make_queries(X, 30, seed=1)
    return X, Q


def test_normalisation_et_cosinus():
    X, _ = _data()
    assert np.allclose(np.linalg.norm(X, axis=1), 1, atol=1e-5)
    assert V.cosine_similarity([1, 0], [0, 2]) == 0
    assert abs(V.cosine_similarity([1, 1], [2, 2]) - 1.0) < 1e-12


def test_index_plat_exact():
    X, Q = _data()
    flat = V.FlatIndex(X)
    res = flat.search(Q[0], 5)
    sims = X @ Q[0]
    assert res == list(np.argsort(-sims)[:5])
    assert flat.distance_computations == len(X)


def test_ivf_rappel_croit_avec_nprobe():
    X, Q = _data()
    flat = V.FlatIndex(X)
    truth = [flat.search(q, 10) for q in Q]
    ivf = V.IVFIndex(X, nlist=20)
    rec = [np.mean([V.recall_at_k(ivf.search(q, 10, p), t) for q, t in zip(Q, truth)]) for p in (1, 4, 20)]
    assert rec[0] <= rec[1] <= rec[2] == 1.0


def test_hnsw_rappel_et_economie():
    X, Q = _data()
    flat = V.FlatIndex(X)
    truth = [flat.search(q, 10) for q in Q]
    h = V.HNSWIndex(X, M=8, ef_construction=40)
    rec = np.mean([V.recall_at_k(h.search(q, 10, ef=40), t) for q, t in zip(Q, truth)])
    assert rec > 0.85
    assert h.distance_computations / len(Q) < len(X) / 2
    sizes = h.layer_sizes()
    assert sizes[0] == len(X) and all(a >= b for a, b in zip(sizes, sizes[1:]))


def test_hnsw_metrique_euclidienne():
    rng = np.random.default_rng(2)
    P = rng.random((800, 2))
    h = V.HNSWIndex(P, M=6, ef_construction=30, metric="l2")
    q = np.array([0.3, 0.7])
    exact = int(np.argmin(((P - q) ** 2).sum(axis=1)))
    assert h.search(q, k=1, ef=20)[0] == exact
