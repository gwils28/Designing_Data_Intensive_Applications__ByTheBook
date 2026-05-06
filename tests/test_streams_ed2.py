"""Tests du module ddia.streams_ed2 (compléments 2e édition du chapitre « traitement de flux »)."""

from ddia import streams_ed2 as e


def test_lettres_mortes():
    strict = e.simulate_poison(100, (10,), "strict")
    assert strict["done"] == 10
    dlq = e.simulate_poison(100, (10, 50), "dlq", max_attempts=3)
    assert dlq["done"] == 98 and dlq["dlq"] == [10, 50] and dlq["wasted"] == 6
    req = e.simulate_poison(100, (10,), "requeue", n_steps=300)
    assert req["done"] == 99 and req["wasted"] > 100


def test_outbox_contre_double_ecriture():
    dual = e.outbox_vs_dual(500, 0.05, "dual", seed=1)
    box = e.outbox_vs_dual(500, 0.05, "outbox", seed=1)
    assert dual["missing"] > 0
    assert box["missing"] == 0 and box["committed"] == 500


def test_crypto_shredding():
    cs = e.CryptoShredder(seed=0)
    r1 = cs.encrypt("alice", "adresse : 1 rue des Lilas")
    r2 = cs.encrypt("bob", "adresse : 2 rue des Roses")
    assert b"Lilas" not in r1[2]
    assert cs.decrypt(r1) == "adresse : 1 rue des Lilas"
    cs.shred("alice")
    assert cs.decrypt(r1) is None and cs.decrypt(r2).endswith("Roses")


def test_stockage_hierarchise():
    c = e.tiered_storage_cost(50, 30, 24)
    assert c["tiered_eur"] < c["all_local_eur"]
