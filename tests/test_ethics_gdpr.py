from ddia.ethics_gdpr import ShreddableLog, rewrite_cost, segments_touched, toy_decrypt, toy_encrypt


def test_toy_cipher_roundtrip():
    k, n = b"k" * 32, b"\x00" * 8
    c = toy_encrypt(k, b"bonjour", n)
    assert c != b"bonjour" and toy_decrypt(k, c, n) == b"bonjour"


def test_shredding_hides_only_one_person():
    log = ShreddableLog(seed=0)
    log.append("alice", {"type": "inscription"})
    log.append("bob", {"type": "inscription"})
    log.append("alice", {"type": "achat", "montant": 12})
    before = len(log.entries)
    log.shred("alice")
    assert len(log.entries) == before            # rien n'est réécrit
    assert log.read(0) is None and log.read(1) == {"type": "inscription"}
    assert list(log.replay()) == [{"type": "inscription"}]


def test_rewrite_cost():
    assert abs(segments_touched(1, 100) - 1) < 1e-9
    assert segments_touched(1000, 100) > 99
    assert abs(rewrite_cost(1e12, 1000, 1, 10) - 1e10) < 1
