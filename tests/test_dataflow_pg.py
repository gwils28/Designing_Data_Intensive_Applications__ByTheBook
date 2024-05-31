from ddia.dataflow_pg import InvertedIndexView, parse_change, rebuild_index, tokenize


def test_parse_insert_avec_apostrophes_et_null():
    ch = parse_change("table public.doc: INSERT: id[integer]:1 titre[text]:'Bonjour l''ami' "
                      "corps[text]:'les flux de données' auteur[text]:null")
    assert ch["op"] == "INSERT" and ch["table"] == "doc"
    assert ch["row"] == {"id": 1, "titre": "Bonjour l'ami", "corps": "les flux de données", "auteur": None}


def test_parse_update_old_key_et_delete():
    ch = parse_change("table public.doc: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:2 "
                      "titre[text]:'a b' montant[numeric]:11.50")
    assert ch["old"] == {"id": 1} and ch["row"]["montant"] == 11.5 and ch["row"]["titre"] == "a b"
    d = parse_change("table public.doc: DELETE: id[integer]:7")
    assert d["op"] == "DELETE" and d["row"] == {"id": 7}
    assert parse_change("BEGIN 742") is None and parse_change("COMMIT 742") is None


def test_tokenize():
    assert tokenize("Les Flux d'Événements") == {"flux", "evenement"}
    assert tokenize(None) == set()


def test_vue_idempotente_et_egale_au_recalcul():
    rows = [{"id": 1, "titre": "flux de données", "corps": "journal", "auteur": "ana"},
            {"id": 2, "titre": "index", "corps": "journal partitionné", "auteur": "bo"}]
    v = InvertedIndexView()
    changes = [{"op": "INSERT", "row": r, "old": None} for r in rows]
    changes.append({"op": "UPDATE", "row": {"id": 1, "titre": "cache", "corps": "vue", "auteur": "bo"}, "old": None})
    changes.append({"op": "DELETE", "row": {"id": 2}, "old": None})
    for c in changes + changes[1:]:          # relivraison partielle : sans effet
        v.apply(c)
    v.apply(changes[2]); v.apply(changes[3])
    ref = rebuild_index([{"id": 1, "titre": "cache", "corps": "vue", "auteur": "bo"}])
    assert dict(v.postings) == dict(ref.postings)
    assert v.authors() == {"bo": 1}
    assert v.search("vue cache") == {1}
