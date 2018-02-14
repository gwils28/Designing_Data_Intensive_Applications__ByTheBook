"""Tests du module ddia.datamodels (modèles de données et langages de requête)."""
import sqlite3

import pytest

from ddia import datamodels as dm


# --- relationnel contre documents --------------------------------------------------------

@pytest.fixture(scope="module")
def profils():
    orgs = dm.make_orgs(50, seed=2)
    p = dm.make_profiles(150, seed=2, orgs=orgs)
    con = sqlite3.connect(":memory:")
    dm.create_relational(con, p, orgs)
    dm.create_documents(con, p)
    return p, con


def test_trois_lectures_donnent_le_meme_profil(profils):
    p, con = profils
    cc = dm.CountingConnection(con)
    for uid in (1, 17, 150):
        attendu = p[uid - 1]
        assert dm.get_profile_doc(cc, uid) == attendu
        assert dm.get_profile_multi(cc, uid) == attendu
        assert dm.get_profile_join(cc, uid) == attendu


def test_jointure_multiple_renvoie_le_produit_cartesien(profils):
    p, con = profils
    cc = dm.CountingConnection(con)
    for uid in range(1, 40):
        prof = p[uid - 1]
        cc.reset()
        dm.get_profile_join(cc, uid)
        attendu = (max(1, len(prof["positions"])) * max(1, len(prof["education"]))
                   * max(1, len(prof["contact_info"])))
        assert cc.queries == 1 and cc.rows == attendu
        cc.reset()
        dm.get_profile_multi(cc, uid)
        assert cc.queries == 4
        cc.reset()
        dm.get_profile_doc(cc, uid)
        assert (cc.queries, cc.rows) == (1, 1)


def test_cout_aller_retour():
    assert dm.round_trip_cost(4, 10, rtt_ms=1.0, per_row_ms=0.1) == pytest.approx(5.0)


# --- pipeline d'agrégation et MapReduce -------------------------------------------------------

def test_pipeline_egale_calcul_direct():
    obs = dm.make_observations(1500, seed=3)
    res = dm.aggregate(obs, [
        {"$match": {"family": "Sharks"}},
        {"$group": {"_id": {"month": {"$month": "$observationTimestamp"}},
                    "total": {"$sum": "$numAnimals"}, "n": {"$sum": 1}}},
        {"$sort": {"_id.month": 1}},
    ])
    direct = {}
    for o in obs:
        if o["family"] == "Sharks":
            m = int(o["observationTimestamp"][5:7])
            direct[m] = direct.get(m, 0) + o["numAnimals"]
    assert {r["_id"]["month"]: r["total"] for r in res} == direct
    assert [r["_id"]["month"] for r in res] == sorted(direct)


def test_pipeline_lookup_unwind_et_comparaisons():
    users = [{"_id": 1, "region": "a", "tags": ["x", "y"]}, {"_id": 2, "region": "b", "tags": []},
             {"_id": 3, "region": "a", "tags": ["y"]}]
    regions = [{"_id": "a", "name": "Alpha"}, {"_id": "b", "name": "Beta"}]
    out = dm.aggregate(users, [{"$match": {"_id": {"$gte": 2}}},
                               {"$lookup": {"from": "regions", "localField": "region",
                                            "foreignField": "_id", "as": "r"}}],
                       collections={"regions": regions})
    assert [(d["_id"], d["r"][0]["name"]) for d in out] == [(2, "Beta"), (3, "Alpha")]
    unw = dm.aggregate(users, [{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}}},
                               {"$sort": {"_id": 1}}])
    assert unw == [{"_id": "x", "n": 1}, {"_id": "y", "n": 2}]
    assert dm.aggregate(users, [{"$match": {"tags": "y"}}, {"$count": "n"}]) == [{"n": 2}]


def test_map_reduce_egale_pipeline():
    obs = dm.make_observations(800, seed=4)
    stats = {}
    mr = dm.map_reduce(obs, lambda d, emit: emit(d["observationTimestamp"][:7], d["numAnimals"]),
                       lambda k, vs: sum(vs), query={"family": "Sharks"}, stats=stats)
    pipe = dm.aggregate(obs, [{"$match": {"family": "Sharks"}},
                              {"$group": {"_id": {"year": {"$year": "$observationTimestamp"},
                                                  "month": {"$month": "$observationTimestamp"}},
                                          "t": {"$sum": "$numAnimals"}}}])
    assert {f"{r['_id']['year']}-{r['_id']['month']:02d}": r["t"] for r in pipe} == mr
    assert stats["emissions"] == stats["documents"]


# --- graphes de propriétés -------------------------------------------------------------------
