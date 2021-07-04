"""Tests du module ddia.transactions (journal, moteur MVCC, ordonnanceur, 2PC)."""

import networkx as nx
import pytest

from ddia.transactions import (DELETED, INC, LEVELS, R, RFU, S, SFU, W, Engine, apply_in_place,
                               is_serializable, precedence_graph, run, run_2pc, simulate_load,
                               transfer_txs, wal_encode, wal_recover)


def incr(key):
    def prog():
        v = yield R(key)
        yield W(key, v + 1)
    return prog


def off_call(me, for_update=False):
    def prog():
        op = SFU if for_update else S
        r = yield op(lambda k, v: v is True, "de garde")
        if len(r) >= 2:
            yield W(me, False)
    return prog


DOCS = {"alice": True, "bob": True}


# --- journal --------------------------------------------------------------------------

def test_wal_atomique_a_chaque_octet():
    init, txs = transfer_txs(4, seed=3)
    log = wal_encode(txs)
    total = sum(init.values())
    committed_counts = []
    for b in range(len(log) + 1):
        state, committed = wal_recover(init, log[:b])
        assert sum(state.values()) == total
        committed_counts.append(len(committed))
    assert committed_counts[0] == 0 and committed_counts[-1] == 4
    assert committed_counts == sorted(committed_counts)


def test_wal_detecte_corruption():
    init, txs = transfer_txs(2, seed=0)
    log = bytearray(wal_encode(txs))
    log[5] = ord("9") if log[5] != ord("9") else ord("8")   # corrompt la 1re ligne
    state, committed = wal_recover(init, bytes(log))
    assert committed == [] and state == init


def test_ecriture_en_place_non_atomique():
    init, txs = transfer_txs(2, seed=0)
    sums = {sum(apply_in_place(init, txs, k).values()) for k in range(5)}
    assert len(sums) > 1


# --- anomalies et niveaux -------------------------------------------------------------

def test_mise_a_jour_perdue_selon_niveau():
    res = {lvl: run({"x": 42}, {"A": incr("x"), "B": incr("x")}, "ABABAB", level=lvl) for lvl in LEVELS}
    assert res["read_committed"].final["x"] == 43
    assert res["read_committed"].committed == ["A", "B"]
    for lvl in ("snapshot", "serializable_ssi", "serializable_2pl"):
        assert res[lvl].final["x"] == 43 and len(res[lvl].aborted) == 1
    nodetect = run({"x": 42}, {"A": incr("x"), "B": incr("x")}, "ABABAB", level="snapshot",
                   detect_lost_updates=False)
    assert nodetect.final["x"] == 43 and nodetect.committed == ["A", "B"]


def test_reessai_donne_le_bon_resultat():
    for lvl in ("snapshot", "serializable_ssi", "serializable_2pl"):
        r = run({"x": 42}, {"A": incr("x"), "B": incr("x")}, "ABABAB", level=lvl, retries=2)
        assert r.final["x"] == 44 and r.committed == ["A", "B"]


def test_increment_atomique_et_for_update():
    def atomic():
        yield INC("x")

    def fu():
        v = yield RFU("x")
        yield W("x", v + 1)
    for prog in (atomic, fu):
        r = run({"x": 0}, {"A": prog, "B": prog}, "ABABAB", level="read_committed")
        assert r.final["x"] == 2 and r.committed == ["A", "B"]


def test_lecture_sale():
    def writer():
        yield W("x", 3)
        yield W("y", 3)

    def reader():
        a = yield R("x")
        b = yield R("y")
        return (a, b)
    ru = run({"x": 2, "y": 2}, {"W": writer, "L": reader}, "WLLW", level="read_uncommitted")
    rc = run({"x": 2, "y": 2}, {"W": writer, "L": reader}, "WLLW", level="read_committed")
    assert ru.reads["L"] == [3, 2]          # incohérent : lecture sale
    assert rc.reads["L"] == [2, 2]


def test_ecriture_sale_sans_verrous():
    def buy(who):
        def prog():
            yield W("listing", who)
            yield W("invoice", who)
        return prog
    r = run({"listing": None, "invoice": None}, {"A": buy("A"), "B": buy("B")}, "ABBA", level="none")
    assert r.final["listing"] != r.final["invoice"]
    rc = run({"listing": None, "invoice": None}, {"A": buy("A"), "B": buy("B")}, "ABBA", level="read_committed")
    assert rc.final["listing"] == rc.final["invoice"]


def test_read_skew_et_snapshot():
    def transfer():
        a = yield R("a1")
        yield W("a1", a + 100)
        b = yield R("a2")
        yield W("a2", b - 100)

    def audit():
        x = yield R("a1")
        y = yield R("a2")
        return x + y
    init = {"a1": 500, "a2": 500}
    rc = run(init, {"L": audit, "T": transfer}, "LTTTTTL", level="read_committed")
    si = run(init, {"L": audit, "T": transfer}, "LTTTTTL", level="snapshot")
    assert sum(rc.reads["L"]) == 900
    assert sum(si.reads["L"]) == 1000


def test_write_skew_medecins():
    progs = {"A": off_call("alice"), "B": off_call("bob")}
    si = run(DOCS, progs, "ABABAB", level="snapshot")
    assert si.final == {"alice": False, "bob": False}
    assert is_serializable(si, DOCS, progs) is None
    g = precedence_graph(si)
    assert not nx.is_directed_acyclic_graph(g)
    for lvl in ("serializable_ssi", "serializable_2pl"):
        r = run(DOCS, progs, "ABABAB", level=lvl)
        assert list(r.final.values()).count(True) == 1
        assert is_serializable(r, DOCS, progs) is not None
    fu = run(DOCS, {"A": off_call("alice", True), "B": off_call("bob", True)}, "ABABAB", level="read_committed")
    assert list(fu.final.values()).count(True) == 1


def test_fantome_reservation():
    def book(user):
        def prog():
            r = yield S(lambda k, v: k.startswith("resa:") and v["salle"] == 1, "salle 1 ?")
            if not r:
                yield W(f"resa:{user}", {"salle": 1, "user": user})
        return prog
    progs = {"A": book("a"), "B": book("b")}
    si = run({}, progs, "ABABAB", level="snapshot")
    assert len(si.final) == 2               # double réservation
    for lvl in ("serializable_ssi", "serializable_2pl"):
        r = run({}, progs, "ABABAB", level=lvl, retries=1)
        assert len(r.final) == 1
    fu = run({}, {"A": book("a"), "B": book("b")}, "ABABAB", level="read_committed")
    assert len(fu.final) == 2


def test_ssi_lecture_seule_jamais_abandonnee():
    def ro():
        yield R("x")
        yield R("y")

    def wr():
        yield W("x", 5)
    r = run({"x": 1, "y": 1}, {"L": ro, "E": wr}, "LEEL", level="serializable_ssi")
    assert r.committed == ["L", "E"]


def test_mvcc_versions_et_vacuum():
    eng = Engine({"x": 1}, level="snapshot")
    t1 = eng.begin("lecteur")
    assert eng.read(t1, "x") == 1
    for i in range(3):
        t = eng.begin()
        eng.write(t, "x", 10 + i)
        eng.commit(t)
    assert eng.read(t1, "x") == 1           # l'instantané ne bouge pas
    assert len(eng.versions["x"]) == 4
    assert eng.vacuum() == 2                # garde la version de t1 et la dernière
    eng.commit(t1)
    assert eng.vacuum() == 1
    assert eng.snapshot_state() == {"x": 12}
    t = eng.begin()
    eng.write(t, "x", DELETED)
    eng.commit(t)
    assert eng.snapshot_state() == {}


def test_simulate_load_correct_si_serialisable():
    def make(rng, i):
        keys = rng.sample(["a", "b", "c"], 2)

        def prog():
            for k in keys:
                v = yield R(k)
                yield W(k, v + 1)
        return prog
    init = {"a": 0, "b": 0, "c": 0}
    for lvl in ("serializable_2pl", "serializable_ssi", "snapshot"):
        out = simulate_load(make, 30, lvl, concurrency=4, seed=1, initial=init)
        assert sum(out["final"].values()) == 60
    rc = simulate_load(make, 30, "read_committed", concurrency=4, seed=1, initial=init)
    assert sum(rc["final"].values()) < 60


# --- 2PC ------------------------------------------------------------------------------

def test_2pc_succes_et_abandon():
    ok = run_2pc()
    assert ok.atomic and set(ok.summary["issue"]) == {"commit"}
    no = run_2pc(votes=["yes", "no"])
    assert no.atomic and set(no.summary["issue"]) == {"abort"}


def test_1pc_non_atomique():
    r = run_2pc(protocol="1pc", votes=["yes", "no"])
    assert not r.atomic


@pytest.mark.parametrize("t", [0.2, 1.0, 2.2, 2.9, 3.6, 4.6, 5.0, 6.0])
def test_2pc_atomique_quelle_que_soit_la_panne(t):
    r = run_2pc(crash={"coord": (t, t + 8)}, t_max=40)
    assert r.atomic
    assert "en doute" not in set(r.summary["issue"])


def test_2pc_bloque_sans_coordinateur():
    r = run_2pc(crash={"coord": (2.9, None)}, t_max=30)
    assert set(r.summary["issue"]) == {"en doute"}
    assert r.summary["durée_en_doute"].min() > 20
