"""Tests de ddia.consistency_clocks (horloges logiques hybrides, oracle d'horodatage)."""
from ddia import consistency as cs
from ddia.consistency_clocks import HybridLogicalClock, TimestampOracle, hlc_factory


def test_hlc_monotone_meme_si_l_horloge_recule():
    t = {"v": 10.0}
    clk = HybridLogicalClock("a", lambda: t["v"], resolution=1.0)
    a = clk.send()
    t["v"] = 5.0          # saut arrière (NTP)
    b = clk.send()
    assert b > a and b[0] == 10


def test_hlc_rattrape_un_horodatage_en_avance():
    t = {"v": 100.0}
    clk = HybridLogicalClock("b", lambda: t["v"], resolution=1.0)
    out = clk.recv((150, 3, "a"))
    assert out == (150, 4, "b")
    t["v"] = 200.0
    assert clk.send() == (200, 0, "b")


def test_hlc_respecte_la_causalite_et_reste_proche_du_temps_physique():
    ev = cs.run_chat(offsets=[0, 3, -2, 1], duration=150, seed=2, extra_clocks={"hlc": hlc_factory()})
    assert cs.causal_order_errors(ev, "hlc")["inversées"] == 0
    ecart = max(e["hlc"][0] * 1e-3 - e["t_phys"] for e in ev)
    assert 0 <= ecart <= 5.01   # au plus l'avance de l'horloge la plus en avance


def test_oracle_saute_mais_ne_desordonne_jamais():
    o = TimestampOracle(batch=100)
    ids = [o.get()[0] for _ in range(150)]
    lost = o.crash_and_recover()
    ids += [o.get()[0] for _ in range(10)]
    assert lost == 50 and ids == sorted(ids) and len(set(ids)) == len(ids)
    assert o.persists == 3
