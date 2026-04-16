"""Tests de ddia.partitioning_tenants (multi-location, notions de la 2e édition)."""
import numpy as np

from ddia import partitioning_tenants as T


def test_rangement():
    sizes = T.tenant_sizes(500, seed=1)
    bins, big = T.pack_tenants(sizes, 100.0)
    placed = sorted(i for b in bins for i in b)
    assert sorted(placed + big) == list(range(500))
    assert all(sizes[b].sum() <= 100.0 for b in bins)
    assert all(sizes[i] > 100.0 for i in big)
    assert len(bins) <= np.ceil(sizes[sizes <= 100].sum() / 100) * 1.25 + 1


def test_voisin_bruyant():
    loads = np.full(12, 0.05)
    shard_of = np.repeat(np.arange(3), 4)
    s = T.noisy_neighbor_slowdown(loads, shard_of, heavy=0, extra=0.5)
    assert (s[1:4] > 1.5).all() and np.allclose(s[4:], 1.0)


def test_croissance():
    sizes = T.tenant_sizes(200, seed=2)
    growth = np.random.default_rng(0).uniform(0, 0.15, 200)
    small = T.simulate_tenant_growth(sizes, growth, 200.0, 8, policy="smallest")
    large = T.simulate_tenant_growth(sizes, growth, 200.0, 8, policy="largest")
    assert small["shards"][-1] >= small["shards"][0]
    assert small["volume"].sum() <= large["volume"].sum()


def test_transactions_mono_shard():
    assert T.single_shard_probability(10, 1) == 1.0
    assert abs(T.single_shard_probability(10, 3) - 0.01) < 1e-12
