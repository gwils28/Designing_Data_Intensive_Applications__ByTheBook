"""Tests du module ddia.crdt (CRDT et transformation opérationnelle)."""

import random

from ddia.crdt import RGA, GCounter, LWWRegister, ORSet, PNCounter, ot_apply, ot_transform_seq


def test_compteurs_convergent():
    a, b = PNCounter("A"), PNCounter("B")
    a.inc(3); b.inc(2); b.dec(1)
    a.merge(b); b.merge(a); a.merge(b)
    assert a.value == b.value == 4
    g = GCounter("A"); g.inc(); g.merge(g)
    assert g.value == 1


def test_orset_repare_le_panier():
    a, b = ORSet("A"), ORSet("B")
    for x in ("savon", "livre", "dvd"):
        a.add(x)
    b.merge(a)
    a.remove("livre"); b.remove("dvd")
    a.merge(b); b.merge(a)
    assert a.value == b.value == {"savon"}
    # ajout concurrent d'une suppression : l'ajout gagne
    a.add("dvd"); b.remove("savon")
    a.merge(b); b.merge(a)
    assert a.value == b.value == {"dvd"}


def test_lww():
    a, b = LWWRegister("A"), LWWRegister("B")
    a.set("B", 10); b.set("C", 12)
    a.merge(b); b.merge(a)
    assert a.value == b.value == "C"


def test_rga_exemple_du_livre():
    a, b = RGA("A"), RGA("B")
    b.apply_all(a.insert_text(0, "ice"))
    o1, o2 = a.insert(0, "n"), b.insert(3, "!")
    a.apply(o2); b.apply(o1)
    assert a.text() == b.text() == "nice!"


def test_rga_aleatoire_converge():
    rng = random.Random(0)
    for trial in range(30):
        reps = [RGA(n) for n in "ABC"]
        ops = []
        for _ in range(40):
            r = rng.choice(reps)
            t = r.text()
            if t and rng.random() < 0.3:
                ops.append(r.delete(rng.randrange(len(t))))
            else:
                ops.append(r.insert(rng.randrange(len(t) + 1), rng.choice("xyz")))
            if rng.random() < 0.3:                          # synchronisation partielle
                other = rng.choice(reps)
                other.apply_all(r.log)
        for r in reps:
            shuffled = ops[:]
            rng.shuffle(shuffled)
            r.apply_all(shuffled)
        assert len({r.text() for r in reps}) == 1
        assert all(not r.pending for r in reps)


def test_ot_exemple_et_aleatoire():
    A, B = [("ins", 0, "n", "A")], [("ins", 3, "!", "B")]
    A2, B2 = ot_transform_seq(A, B)
    s1 = ot_apply(ot_apply("ice", A[0]), B2[0])
    s2 = ot_apply(ot_apply("ice", B[0]), A2[0])
    assert s1 == s2 == "nice!"
    rng = random.Random(1)
    for _ in range(300):
        base = "".join(rng.choice("abc") for _ in range(rng.randint(0, 6)))

        def gen(site, txt):
            ops = []
            for _ in range(rng.randint(1, 4)):
                if txt and rng.random() < 0.4:
                    p = rng.randrange(len(txt)); op = ("del", p)
                else:
                    p = rng.randrange(len(txt) + 1); op = ("ins", p, rng.choice("XYZ"), site)
                ops.append(op); txt = ot_apply(txt, op)
            return ops
        A, B = gen("A", base), gen("B", base)
        A2, B2 = ot_transform_seq(A, B)
        x = base
        for op in A + B2:
            x = ot_apply(x, op)
        y = base
        for op in B + A2:
            y = ot_apply(y, op)
        assert x == y
