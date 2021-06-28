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
