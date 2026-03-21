"""Tests de ddia.datamodels_ed2 (indexation fractionnaire, GraphQL, event sourcing, matrices)."""
import random

import numpy as np
import pytest

from ddia import datamodels_ed2 as e2


def test_indexation_fractionnaire_insertions_aleatoires():
    rnd = random.Random(1)
    keys = [e2.key_between(None, None)]
    for _ in range(3000):
        j = rnd.randint(0, len(keys))
        a = keys[j - 1] if j > 0 else None
        b = keys[j] if j < len(keys) else None
        k = e2.key_between(a, b)
        assert (a is None or a < k) and (b is None or k < b) and not k.endswith("0")
        keys.insert(j, k)
    assert keys == sorted(keys) and len(set(keys)) == len(keys)
    assert max(map(len, keys)) <= 8
    ks = e2.keys_between("1", "2", 50)
    assert ks == sorted(ks) and all("1" < k < "2" for k in ks)
    with pytest.raises(ValueError):
        e2.key_between("b", "a")


SCHEMA_DATA = {
    "users": {1: {"fullName": "Aya", "imageUrl": "/a.png"}, 2: {"fullName": "Bruno", "imageUrl": "/b.png"}},
    "messages": [{"id": i, "channel": i % 2, "sender": 1 + i % 2, "content": f"m{i}",
                  "replyTo": i - 1 if i % 3 == 2 else None} for i in range(12)],
}


def _schema():
    msgs = {m["id"]: m for m in SCHEMA_DATA["messages"]}

    def users(parents, args, ctx):
        ctx["calls"] += 1
        return [SCHEMA_DATA["users"][p["sender"]] for p in parents]

    def reply(parents, args, ctx):
        ctx["calls"] += 1
        return [msgs.get(p["replyTo"]) for p in parents]

    def recent(parents, args, ctx):
        ctx["calls"] += 1
        return [[m for m in SCHEMA_DATA["messages"] if m["channel"] == p["id"]][-args["latest"]:] for p in parents]

    scalar = lambda k: (None, lambda ps, a, c: [p[k] for p in ps], False)
    return {
        "Query": {"channels": ("Channel", lambda ps, a, c: [[{"id": 0, "name": "#general"},
                                                               {"id": 1, "name": "#random"}]], True)},
        "Channel": {"name": scalar("name"), "recentMessages": ("Message", recent, True)},
        "Message": {"content": scalar("content"), "sender": ("User", users, False),
                    "replyTo": ("Message", reply, False)},
        "User": {"fullName": scalar("fullName"), "imageUrl": scalar("imageUrl")},
    }


Q = """query Chat { channels { name recentMessages(latest: 4) { content sender { fullName }
       replyTo { content sender { fullName } } } } }"""


def test_graphql_naif_et_par_lots_meme_reponse():
    c1, c2 = {}, {}
    r1 = e2.execute_graphql(Q, _schema(), c1)
    r2 = e2.execute_graphql(Q, _schema(), c2, batched=True)
    assert r1 == r2
    ch = r1["data"]["channels"]
    assert [c["name"] for c in ch] == ["#general", "#random"]
    assert len(ch[0]["recentMessages"]) == 4
    assert c2["calls"] < c1["calls"]
    assert e2.query_depth(e2.parse_graphql(Q)) == 5
    with pytest.raises(ValueError):
        e2.execute_graphql(Q, _schema(), {}, max_depth=3)


def test_event_sourcing_rejeu_et_validation():
    svc = e2.ConferenceService()
    views = [svc.log.subscribe(v) for v in (e2.BookingStatus(), e2.BadgeList(), e2.DailyDashboard())]
    for ts, c in e2.make_conference_commands(800, capacity=900, seed=2):
        svc.handle(c, ts)
    assert svc.state.available >= 0
    # rejouer le journal reconstruit exactement les mêmes vues
    again = e2.replay(svc.log.events, e2.AvailableSeats())
    assert again.available == svc.state.available
    badges = e2.replay(svc.log.events, e2.BadgeList()).badges
    assert badges == views[1].badges and len(badges) == svc.state.booked
    # aucune commande invalide n'a produit d'événement
    assert not svc.handle({"cmd": "cancel", "booking": "inconnue"}, 1e9)
    assert svc.log.events[-1].type != "BookingCancelled" or svc.log.events[-1].data["booking"] != "inconnue"


def test_rejeu_dans_le_desordre_echoue():
    svc = e2.ConferenceService()
    svc.handle({"cmd": "open", "capacity": 10}, 0)
    svc.handle({"cmd": "book", "booking": "A", "client": "x", "seats": 2}, 1)
    svc.handle({"cmd": "cancel", "booking": "A"}, 2)
    with pytest.raises(KeyError):
        e2.replay(list(reversed(svc.log.events)), e2.AvailableSeats())


def test_csr_produit_et_taille():
    df = e2.make_ratings(300, 120, per_user=10, seed=1)
    csr = e2.to_csr(df.user_id, df.movie_id, df.rating, (300, 120))
    dense = np.zeros((300, 120))
    dense[df.user_id, df.movie_id] = df.rating
    x = np.random.default_rng(0).random(120)
    assert np.allclose(e2.csr_matvec(csr, x), dense @ x)
    assert e2.csr_nbytes(csr) < dense.astype(np.float32).nbytes
    top = e2.cosine_top_k(dense, 0, k=3)
    assert len(top) == 3 and all(j != 0 for j, _ in top)
