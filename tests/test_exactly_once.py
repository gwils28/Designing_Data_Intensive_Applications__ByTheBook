"""Tests du module ddia.exactly_once (traitement exactement une fois par déduplication)."""

from ddia.exactly_once import crash_matrix, run_stream


def test_matrice_des_pannes():
    rows = crash_matrix()
    dedup = [r for r in rows if r["mode"] == "dedup"]
    naif = {r["panne"]: r for r in rows if r["mode"] == "naif"}
    assert all(r["exactement_une_fois"] for r in dedup)
    assert not naif["avant_ack"]["exactement_une_fois"]
    assert naif["avant_commit"]["exactement_une_fois"]


def test_flux_avec_pannes():
    naif = run_stream(150, "naif", crash_prob=0.2, seed=2)
    dedup = run_stream(150, "dedup", crash_prob=0.2, seed=2)
    assert naif["écart"] > 0
    assert dedup["écart"] == 0
    assert dedup["ids_restants"] == dedup["pannes_avant_nettoyage"]
