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

def test_exemple_emigres_trois_moteurs():
    g = dm.example_graph()
    assert dm.emigrants_forward(g, "Europe", "Amérique du Nord") == {"Inès"}
    assert dm.emigrants_backward(g, "Europe", "Amérique du Nord") == {"Inès"}
    assert dm.emigrants_backward(g, "Asie", "Canada") == {"Kenji"}
    con = sqlite3.connect(":memory:")
    g.to_sqlite(con)
    assert dm.emigrants_sql(con, "Europe", "Amérique du Nord") == {"Inès"}
    assert dm.emigrants_sql(con, "France", "France") == {"Léa"}


def test_strategies_equivalentes_sur_graphe_synthetique():
    g = dm.make_world_graph(n_people=1500, seed=5, countries_per_continent=3,
                            regions_per_country=3, cities_per_region=4)
    c1, c2 = {}, {}
    a = dm.emigrants_forward(g, "Europe", "Asie", c1)
    b = dm.emigrants_backward(g, "Europe", "Asie", c2)
    assert a == b and len(a) > 0
    assert c2["aretes"] < c1["aretes"]
    con = sqlite3.connect(":memory:")
    g.to_sqlite(con)
    assert dm.emigrants_sql(con, "Europe", "Asie") == a


def test_fermeture_gere_les_cycles():
    g = dm.PropertyGraph()
    for v in "abc":
        g.add_vertex(v, "X", name=v)
    g.add_edge("a", "b", "R"); g.add_edge("b", "c", "R"); g.add_edge("c", "a", "R")
    assert g.closure(["a"], "R", "out") == {"a", "b", "c"}
    assert g.closure(["a"], "S", "out") == {"a"}


# --- triplets -------------------------------------------------------------------------------

def test_triplets_et_chemins():
    ts = dm.graph_to_triples(dm.example_graph())
    assert ("_:ines", "bornIn", "_:lyon") in set(ts.match(s="_:ines", p="bornIn"))
    assert ts.path_objects("_:lyon", ("within*", "name")) >= {"Lyon", "Rhône", "France", "Europe"}
    sols = ts.query([("?p", ("bornIn", "within*", "name"), "Europe"),
                     ("?p", ("livesIn", "within*", "name"), "Amérique du Nord"),
                     ("?p", "name", "?nom")])
    assert [s["?nom"] for s in sols] == ["Inès"]
    # l'ordre des motifs change le travail, pas le résultat
    st1, st2 = [], []
    q1 = [("?p", "a", "Person"), ("?p", "livesIn", "?l"), ("?l", "name", "Lyon")]
    q2 = [("?l", "name", "Lyon"), ("?p", "livesIn", "?l"), ("?p", "a", "Person")]
    r1, r2 = ts.query(q1, st1), ts.query(q2, st2)
    assert [s["?p"] for s in r1] == [s["?p"] for s in r2] == ["_:lea"]
    assert sum(st2) < sum(st1)
    assert "_:ines a :Person" in ts.to_turtle()


# --- Datalog --------------------------------------------------------------------------------

PROG = """
location(1, "Amérique du Nord", continent).  location(2, "Canada", pays).
location(3, "Québec", province).  location(4, "Montréal", ville).
location(5, "Europe", continent). location(6, "France", pays). location(7, "Lyon", ville).
within(2, 1). within(3, 2). within(4, 3). within(6, 5). within(7, 6).
person(100, "Inès"). born_in(100, 7). lives_in(100, 4).
person(101, "Léa").  born_in(101, 6). lives_in(101, 7).
within_rec(L, N) :- location(L, N, _).                 /* règle 1 */
within_rec(L, N) :- within(L, V), within_rec(V, N).    /* règle 2 */
migrated(P, B, V) :- person(I, P), born_in(I, X), within_rec(X, B),
                     lives_in(I, Y), within_rec(Y, V).
eu_to_na(P) :- migrated(P, "Europe", "Amérique du Nord").
"""


def test_datalog_requete_du_cours():
    d = dm.Datalog(PROG)
    d.evaluate()
    assert d.query("eu_to_na") == [("Inès",)]
    assert (4, "Amérique du Nord") in d.query("within_rec", 4, None)
    assert d.query("migrated", "Léa", "France", "France") == [("Léa", "France", "France")]


def test_naif_et_semi_naif_meme_point_fixe():
    n = 30
    prog = " ".join(f"edge({i}, {i + 1})." for i in range(n))
    prog += " path(X, Y) :- edge(X, Y). path(X, Z) :- path(X, Y), edge(Y, Z)."
    a, b = dm.Datalog(prog), dm.Datalog(prog)
    sa, sb = a.evaluate("naive"), b.evaluate("seminaive")
    assert a.rel["path"] == b.rel["path"]
    assert len(a.rel["path"]) == n * (n + 1) // 2
    assert sa["iterations"] == sb["iterations"] == n + 1
    assert sb["derivations"] == n * (n + 1) // 2           # chaque chemin dérivé une seule fois
    assert sa["derivations"] > 5 * sb["derivations"]


def test_datalog_syntaxe():
    facts, rules = dm.parse_datalog('p(1, "a b"). % commentaire\n q(X) :- p(X, _).')
    assert facts == [("p", (1, "a b"))]
    assert rules[0][0] == ("q", ("X",))
    with pytest.raises(SyntaxError):
        dm.parse_datalog("p(X).")
