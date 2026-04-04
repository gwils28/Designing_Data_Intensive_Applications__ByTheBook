"""Tests des encodeurs et de l'évolution de schéma (chapitre « Encoding and Evolution »)."""
import copy
import random
import sqlite3

import matplotlib

matplotlib.use("Agg")

import pytest

from ddia import encoding as E

R = E.EXAMPLE_RECORD


# --------------------------------------------------------------------- varints
def test_varint_aller_retour_et_taille():
    rng = random.Random(1)
    for n in [0, 1, 127, 128, 300, 1337, 2 ** 32, 2 ** 63 - 1] + [rng.randrange(2 ** 40) for _ in range(200)]:
        b = E.encode_uvarint(n)
        assert E.decode_uvarint(b) == (n, len(b))
        assert len(b) == E.uvarint_size(n)
    assert E.encode_uvarint(300) == bytes([0xAC, 0x02])


def test_zigzag():
    assert [E.zigzag_encode(x) for x in [0, -1, 1, -2, 2]] == [0, 1, 2, 3, 4]
    for x in range(-1000, 1000):
        assert E.zigzag_decode(E.zigzag_encode(x)) == x
    # -64..63 tiennent sur un octet, -8192..8191 sur deux
    assert len(E.encode_svarint(-64)) == 1 and len(E.encode_svarint(63)) == 1
    assert len(E.encode_svarint(64)) == 2 and len(E.encode_svarint(-8192)) == 2
    assert len(E.encode_svarint(8192)) == 3


# --------------------------------------------------------- tailles du chapitre
def test_tailles_de_l_exemple():
    assert len(E.json_bytes(R)) == 81
    assert len(E.msgpack_encode(R)) == 66
    assert len(E.thrift_binary_encode(R)) == 59
    assert len(E.thrift_compact_encode(R)) == 34
    assert len(E.pb_encode(E.to_pb_record(R), E.PERSON_PB)) == 33
    assert len(E.avro_encode(R, E.PERSON_AVRO)) == 32


def test_traces_couvrent_tous_les_octets():
    for enc in [lambda t: E.msgpack_encode(R, t), lambda t: E.avro_encode(R, E.PERSON_AVRO, t),
                lambda t: E.pb_encode(E.to_pb_record(R), E.PERSON_PB, t),
                lambda t: E.thrift_binary_encode(R, trace=t), lambda t: E.thrift_compact_encode(R, trace=t)]:
        tr = []
        b = enc(tr)
        assert sum(s.size for s in tr) == len(b)
        assert tr[0].start == 0 and all(a.end == c.start for a, c in zip(tr, tr[1:]))


def test_msgpack_aller_retour():
    obj = {"a": [1, -5, 300, -70000, 2 ** 40, 1.5, None, True, False], "b": "é" * 40, "c": b"\x00\x01",
           "d": {"x": list(range(20))}}
    assert E.msgpack_decode(E.msgpack_encode(obj)) == obj


# ----------------------------------------------------------- Protocol Buffers
def test_pb_aller_retour_et_defauts():
    rec = E.pb_decode(E.pb_encode({"user_name": "Ana"}, E.PERSON_PB), E.PERSON_PB)
    assert rec == {"user_name": "Ana", "favorite_number": 0, "interests": []}


def test_pb_champ_inconnu_ignore_puis_preserve():
    v2 = E.PbMessage("Person", E.PERSON_PB.fields + [E.PbField("photo_url", 4, "string")])
    data = E.pb_encode({**E.to_pb_record(R), "photo_url": "http://x/p.jpg"}, v2)
    old = E.pb_decode(data, E.PERSON_PB)                # compatibilité ascendante (forward)
    assert old["user_name"] == "Martin" and "__unknown__" in old
    old["favorite_number"] = 42
    rewritten = E.pb_encode(old, E.PERSON_PB)          # l'ancien code réécrit...
    assert E.pb_decode(rewritten, v2)["photo_url"] == "http://x/p.jpg"   # ...sans perte
    lossy = E.pb_decode(data, E.PERSON_PB, keep_unknown=False)
    assert E.pb_decode(E.pb_encode(lossy, E.PERSON_PB), v2)["photo_url"] == ""


def test_pb_troncature_int64_vers_int32():
    s64 = E.PbMessage("M", [E.PbField("n", 1, "int64")])
    s32 = E.PbMessage("M", [E.PbField("n", 1, "int32")])
    assert E.pb_decode(E.pb_encode({"n": 2 ** 31 + 5}, s64), s32)["n"] == -(2 ** 31) + 5
    assert E.pb_decode(E.pb_encode({"n": 123}, s32), s64)["n"] == 123
    assert len(E.pb_encode({"n": -1}, s64)) == 11       # négatif en int64 : 10 octets de varint


def test_pb_type_d_onde_incompatible():
    s1 = E.PbMessage("M", [E.PbField("n", 1, "int64")])
    s2 = E.PbMessage("M", [E.PbField("n", 1, "string")])
    with pytest.raises(E.PbDecodeError):
        E.pb_decode(E.pb_encode({"n": 5}, s1), s2, strict=True)
    rec = E.pb_decode(E.pb_encode({"n": 5}, s1), s2)       # traité comme champ inconnu
    assert rec["n"] == "" and "__unknown__" in rec


def test_pb_repeated_packed():
    s = E.PbMessage("M", [E.PbField("xs", 1, "sint64", repeated=True)])
    xs = [0, -1, 5, -300, 10 ** 9]
    assert E.pb_decode(E.pb_encode({"xs": xs}, s), s)["xs"] == xs
    assert E.pb_decode(E.pb_encode({"xs": xs}, s, packed=False), s)["xs"] == xs


# ------------------------------------------------------------------------ Avro
V2 = {"type": "record", "name": "Person", "fields": [
    {"name": "userName", "type": "string"},
    {"name": "interests", "type": {"type": "array", "items": "string"}},
    {"name": "photoURL", "type": ["null", "string"], "default": None},
]}


def test_avro_aller_retour():
    for rec in E.make_people(300, seed=3):
        assert E.avro_decode(E.avro_encode(rec, E.PERSON_AVRO), E.PERSON_AVRO) == rec


def test_avro_resolution_ordre_defaut_et_champ_ignore():
    data = E.avro_encode(R, E.PERSON_AVRO)
    got = E.avro_decode(data, E.PERSON_AVRO, V2)
    assert got == {"userName": "Martin", "interests": ["daydreaming", "hacking"], "photoURL": None}
    back = E.avro_decode(E.avro_encode({**got, "photoURL": "p.jpg"}, V2), V2, E.PERSON_AVRO)
    assert back["favoriteNumber"] is None


def test_avro_champ_sans_defaut_casse_la_compatibilite():
    bad = copy.deepcopy(V2)
    bad["fields"][2] = {"name": "photoURL", "type": "string"}
    with pytest.raises(E.AvroError):
        E.avro_decode(E.avro_encode(R, E.PERSON_AVRO), E.PERSON_AVRO, bad)
    assert not E.avro_compatible(E.PERSON_AVRO, bad)[0]
    assert E.avro_compatible(bad, E.PERSON_AVRO)[0]     # l'ancien lecteur ignore photoURL
    assert E.avro_compatible(E.PERSON_AVRO, V2)[0] and E.avro_compatible(V2, E.PERSON_AVRO)[0]


def test_avro_alias_et_promotion():
    w = {"type": "record", "name": "P", "fields": [{"name": "n", "type": "int"}]}
    r = {"type": "record", "name": "P", "fields": [{"name": "num", "aliases": ["n"], "type": "double"}]}
    assert E.avro_decode(E.avro_encode({"n": 7}, w), w, r) == {"num": 7.0}
    assert E.avro_compatible(w, r)[0] and not E.avro_compatible(r, w)[0]


def test_avro_union_elargie():
    w = {"type": "record", "name": "P", "fields": [{"name": "x", "type": ["null", "long", "string"]}]}
    r = {"type": "record", "name": "P", "fields": [{"name": "x", "type": ["null", "long"]}]}
    assert E.avro_compatible(r, w)[0] and not E.avro_compatible(w, r)[0]
    assert E.avro_decode(E.avro_encode({"x": 5}, w), w, r) == {"x": 5}
    with pytest.raises(E.AvroError):
        E.avro_decode(E.avro_encode({"x": "a"}, w), w, r)


def test_avro_mauvais_schema_ecrivain_detecte_ou_corrompu():
    data = E.avro_encode(R, E.PERSON_AVRO)
    with pytest.raises(E.AvroError):
        E.avro_decode(data, V2)


def test_avro_conteneur():
    recs = E.make_people(250, seed=1)
    buf = E.avro_container_write(recs, E.PERSON_AVRO, block_size=100)
    w, got = E.avro_container_read(buf)
    assert w == E.PERSON_AVRO and got == recs
    _, got2 = E.avro_container_read(buf, V2)
    assert all(g["photoURL"] is None for g in got2)


def test_avro_schema_depuis_sqlite():
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE clients (id INTEGER NOT NULL, nom TEXT, solde REAL)")
    s = E.avro_schema_from_sqlite(con, "clients")
    assert [f["name"] for f in s["fields"]] == ["id", "nom", "solde"]
    assert s["fields"][0]["type"] == "long" and s["fields"][1]["type"] == ["null", "string"]
    rec = {"id": 1, "nom": None, "solde": 3.5}
    assert E.avro_decode(E.avro_encode(rec, s), s) == rec


def test_registre_de_schemas():
    reg = E.SchemaRegistry("BACKWARD")
    i1 = reg.register("person", E.PERSON_AVRO)
    i2 = reg.register("person", V2)
    assert reg.register("person", V2) == i2
    msg = reg.encode(i1, R)
    assert msg[0] == 0 and len(msg) == 5 + 32
    assert reg.decode(msg, V2)["photoURL"] is None
    bad = copy.deepcopy(V2)
    bad["fields"].append({"name": "age", "type": "int"})
    with pytest.raises(E.IncompatibleSchema):
        reg.register("person", bad)
    fwd = E.SchemaRegistry("FORWARD")
    fwd.register("p", E.PERSON_AVRO)
    fwd.register("p", bad)                               # l'ancien lecteur ignore les ajouts


def test_registre_transitif():
    a = {"type": "record", "name": "P", "fields": [{"name": "x", "type": "int"}]}
    b = {"type": "record", "name": "P", "fields": [{"name": "x", "type": "int", "default": 0}]}
    c = {"type": "record", "name": "P", "fields": [{"name": "y", "type": "int", "default": 0}]}
    reg = E.SchemaRegistry("FORWARD")
    for s in (a, b, c):
        reg.register("p", s)
    reg2 = E.SchemaRegistry("FORWARD_TRANSITIVE")
    reg2.register("p", a)
    reg2.register("p", b)
    with pytest.raises(E.IncompatibleSchema):
        reg2.register("p", c)


# ---------------------------------------------------------------- simulations
def test_simulation_deploiement_progressif():
    lossy = E.simulate_rolling_upgrade(seed=1)
    safe = E.simulate_rolling_upgrade(seed=1, preserve_unknown=True)
    assert lossy["photos_perdues"].iloc[-1] > 0
    assert safe["photos_perdues"].iloc[-1] == 0
    assert lossy["part_v2"].iloc[-1] == 1.0


def test_simulation_relances():
    a = E.simulate_retries(20000, 0.05, 0.05, max_attempts=3, seed=2)
    b = E.simulate_retries(20000, 0.05, 0.05, max_attempts=3, idempotent=True, seed=2)
    assert a["exécutée 2 fois ou plus"] > 0.03 and b["exécutée 2 fois ou plus"] == 0
    assert abs(a["succès"] - b["succès"]) < 1e-9


def test_byte_map_se_dessine():
    tr = []
    b = E.avro_encode(R, E.PERSON_AVRO, tr)
    fig = E.draw_byte_map(b, tr, title="avro")
    assert fig is not None


# ------------------------------------------------------------ JSON Schema
def test_json_schema_open_closed():
    ouvert = {"type": "object", "properties": {"port": {"type": "integer", "minimum": 1, "maximum": 65535}}}
    ferme = {**ouvert, "additionalProperties": False}
    assert E.json_schema_validate({"port": 80, "host": "a"}, ouvert) == []
    assert len(E.json_schema_validate({"port": 80, "host": "a"}, ferme)) == 1
    assert len(E.json_schema_validate({"port": 0}, ouvert)) == 1
    carte = {"type": "object", "patternProperties": {"^[0-9]+$": {"type": "string"}},
             "additionalProperties": False}
    assert E.json_schema_validate({"12": "a", "7": "b"}, carte) == []
    assert len(E.json_schema_validate({"12": 3, "x": "b"}, carte)) == 2
