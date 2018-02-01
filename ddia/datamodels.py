"""Modèles de données et langages de requête (chapitre « Data Models and Query Languages »).

Ce module rassemble les implémentations « à la main » utilisées par les notebooks :

- un générateur de CV synthétiques, leur chargement en tables normalisées et en
  documents JSON sur SQLite, et un connecteur qui compte les allers-retours ;
- un mini-moteur de pipeline d'agrégation à la MongoDB ($match, $group, $lookup...)
  et un mini-MapReduce ;
- un graphe de propriétés (sommets, arêtes, index entrants et sortants), son export
  vers deux tables SQL, et la requête « émigrés » exécutée de deux façons ;
- un magasin de triplets avec motifs à variables et chemins de propriétés (à la SPARQL)
  et une sérialisation Turtle ;
- un mini-moteur Datalog (analyse syntaxique, évaluation naïve et semi-naïve).
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime

import numpy as np

__all__ = [
    "REGIONS", "make_orgs", "make_profiles", "create_relational", "create_documents",
    "CountingConnection", "get_profile_multi", "get_profile_join", "get_profile_doc",
    "round_trip_cost", "make_observations", "aggregate", "map_reduce",
    "PropertyGraph", "example_graph", "make_world_graph", "emigrants_forward",
    "emigrants_backward", "emigrants_sql", "EMIGRANTS_SQL",
    "TripleStore", "graph_to_triples", "Datalog", "parse_datalog",
]

# =====================================================================================
# 1. CV synthétiques : relationnel contre documents
# =====================================================================================

#: régions normalisées : identifiant -> nom lisible
REGIONS = {
    "fr:69": "Lyon, Auvergne-Rhône-Alpes, France",
    "fr:75": "Paris, Île-de-France, France",
    "fr:31": "Toulouse, Occitanie, France",
    "fr:33": "Bordeaux, Nouvelle-Aquitaine, France",
    "fr:44": "Nantes, Pays de la Loire, France",
    "fr:59": "Lille, Hauts-de-France, France",
    "fr:13": "Marseille, Provence-Alpes-Côte d'Azur, France",
    "fr:67": "Strasbourg, Grand Est, France",
    "be:bru": "Bruxelles, Belgique",
    "ch:ge": "Genève, Suisse",
    "ca:qc": "Montréal, Québec, Canada",
    "sn:dk": "Dakar, Sénégal",
}

_FIRST = ["Inès", "Hugo", "Léa", "Malik", "Chloé", "Yanis", "Manon", "Théo", "Aïcha", "Lucas",
          "Camille", "Nathan", "Sarah", "Karim", "Emma", "Jules", "Zoé", "Omar", "Louise", "Adam"]
_LAST = ["Martin", "Bernard", "Diallo", "Petit", "Robert", "Nguyen", "Richard", "Durand", "Moreau",
         "Haddad", "Laurent", "Simon", "Michel", "Lefebvre", "Garcia", "Fontaine", "Rousseau", "Blanc"]
_JOBS = ["Ingénieure logiciel", "Data engineer", "Administrateur de bases", "Cheffe de projet",
         "Développeur web", "Architecte SI", "Analyste", "Responsable produit", "SRE", "Stagiaire"]
_SCHOOLS = ["INSA Lyon", "Université Paris-Saclay", "École polytechnique", "Université de Lille",
            "IMT Atlantique", "Université de Bordeaux", "EPFL", "Université de Montréal",
            "Université Cheikh Anta Diop", "Sorbonne Université"]


def make_orgs(n: int = 200, seed: int = 0) -> list[dict]:
    """Organisations (employeurs) : identifiant, nom, URL de logo."""
    rng = np.random.default_rng(seed)
    radicals = ["Data", "Brio", "Nova", "Cloud", "Ouest", "Alpha", "Ciel", "Rhône", "Kilo", "Pixel",
                "Atlas", "Delta", "Orion", "Vert", "Azur", "Hexa", "Lumen", "Terra", "Quanta", "Mistral"]
    suffixes = ["tech", "soft", "sys", "lab", "net", "ware", "corp", "io", "metrics", "base"]
    names, out = set(), []
    while len(out) < n:
        name = rng.choice(radicals) + rng.choice(suffixes)
        if name in names:
            name = f"{name} {len(out)}"
        names.add(name)
        oid = 500 + len(out)
        out.append({"org_id": oid, "name": name, "logo_url": f"/logos/{oid}.png"})
    return out


def make_profiles(n: int = 1000, seed: int = 0, orgs: list[dict] | None = None,
                  max_positions: int = 6) -> list[dict]:
    """Profils de type CV, sous forme d'arbres JSON (relations un-à-plusieurs).

    Chaque profil a des champs uniques (nom, titre, région) et trois listes :
    postes occupés (1 à `max_positions`), formations (0 à 3), coordonnées (0 à 3).
    Les postes portent à la fois le nom de l'organisation (dénormalisé) et son
    identifiant `org_id` (normalisé), pour comparer les deux représentations.
    """
    rng = np.random.default_rng(seed)
    orgs = orgs if orgs is not None else make_orgs(seed=seed)
    # popularité des employeurs en loi de Zipf : quelques gros employeurs, une longue traîne
    w = 1.0 / np.arange(1, len(orgs) + 1) ** 1.1
    w /= w.sum()
    region_ids = list(REGIONS)
    profiles = []
    for uid in range(1, n + 1):
        first, last = rng.choice(_FIRST), rng.choice(_LAST)
        n_pos = int(rng.integers(1, max_positions + 1))
        year = int(rng.integers(1995, 2012))
        positions = []
        for _ in range(n_pos):
            o = orgs[int(rng.choice(len(orgs), p=w))]
            dur = int(rng.integers(1, 6))
            positions.append({"job_title": str(rng.choice(_JOBS)), "organization": o["name"],
                              "org_id": o["org_id"], "start": year, "end": year + dur})
            year += dur
        positions.reverse()                              # le poste le plus récent d'abord
        education = []
        for _ in range(int(rng.integers(0, 4))):
            s = int(rng.integers(1985, 2010))
            education.append({"school_name": str(rng.choice(_SCHOOLS)), "start": s, "end": s + int(rng.integers(1, 6))})
        contact = {}
        for kind in rng.choice(["website", "blog", "mastodon", "github"], int(rng.integers(0, 4)), replace=False):
            contact[str(kind)] = f"https://{kind}.example/{uid}"
        profiles.append({
            "user_id": uid, "first_name": str(first), "last_name": str(last),
            "headline": f"{positions[0]['job_title']} chez {positions[0]['organization']}",
            "region_id": str(rng.choice(region_ids)), "photo_url": f"/p/{uid:06d}.jpg",
            "positions": positions, "education": education, "contact_info": contact,
        })
    return profiles


def create_relational(con: sqlite3.Connection, profiles: list[dict], orgs: list[dict] | None = None,
                      indexes: bool = True) -> None:
    """Range les profils dans un schéma normalisé (une table par relation un-à-plusieurs)."""
    con.executescript("""
    DROP TABLE IF EXISTS users; DROP TABLE IF EXISTS regions; DROP TABLE IF EXISTS positions;
    DROP TABLE IF EXISTS education; DROP TABLE IF EXISTS contact_info; DROP TABLE IF EXISTS organizations;
    CREATE TABLE regions (id TEXT PRIMARY KEY, name TEXT);
    CREATE TABLE organizations (id INTEGER PRIMARY KEY, name TEXT, logo_url TEXT);
    CREATE TABLE users (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, headline TEXT,
                        region_id TEXT REFERENCES regions(id), photo_url TEXT);
    CREATE TABLE positions (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id),
                            job_title TEXT, org_id INTEGER REFERENCES organizations(id),
                            start INTEGER, "end" INTEGER);
    CREATE TABLE education (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id),
                            school_name TEXT, start INTEGER, "end" INTEGER);
    CREATE TABLE contact_info (user_id INTEGER REFERENCES users(id), kind TEXT, value TEXT);
    """)
    con.executemany("INSERT INTO regions VALUES (?, ?)", REGIONS.items())
    if orgs is None:
        seen = {}
        for p in profiles:
            for pos in p["positions"]:
                seen[pos["org_id"]] = pos["organization"]
        orgs = [{"org_id": k, "name": v, "logo_url": f"/logos/{k}.png"} for k, v in seen.items()]
    con.executemany("INSERT INTO organizations VALUES (?, ?, ?)",
                    [(o["org_id"], o["name"], o["logo_url"]) for o in orgs])
    con.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)",
                    [(p["user_id"], p["first_name"], p["last_name"], p["headline"], p["region_id"],
                      p["photo_url"]) for p in profiles])
    con.executemany('INSERT INTO positions (user_id, job_title, org_id, start, "end") VALUES (?, ?, ?, ?, ?)',
                    [(p["user_id"], q["job_title"], q["org_id"], q["start"], q["end"])
                     for p in profiles for q in p["positions"]])
    con.executemany('INSERT INTO education (user_id, school_name, start, "end") VALUES (?, ?, ?, ?)',
                    [(p["user_id"], e["school_name"], e["start"], e["end"])
                     for p in profiles for e in p["education"]])
    con.executemany("INSERT INTO contact_info VALUES (?, ?, ?)",
                    [(p["user_id"], k, v) for p in profiles for k, v in p["contact_info"].items()])
    if indexes:
        con.executescript("""
        CREATE INDEX positions_user ON positions (user_id);
        CREATE INDEX education_user ON education (user_id);
        CREATE INDEX contact_user ON contact_info (user_id);
        """)
    con.commit()


def create_documents(con: sqlite3.Connection, profiles: list[dict]) -> None:
    """Range chaque profil comme un document JSON autonome (une ligne, une colonne texte)."""
    con.executescript("DROP TABLE IF EXISTS profiles;"
                      "CREATE TABLE profiles (user_id INTEGER PRIMARY KEY, doc TEXT);")
    con.executemany("INSERT INTO profiles VALUES (?, ?)",
                    [(p["user_id"], json.dumps(p, ensure_ascii=False)) for p in profiles])
    con.commit()


class CountingConnection:
    """Enveloppe une connexion SQLite et compte les requêtes et les lignes renvoyées.

    Sert de modèle d'un client distant : chaque requête coûte un aller-retour réseau.
    """

    def __init__(self, con: sqlite3.Connection):
        self.con = con
        self.queries = 0
        self.rows = 0

    def execute(self, sql: str, params=()) -> list[tuple]:
        self.queries += 1
        out = self.con.execute(sql, params).fetchall()
        self.rows += len(out)
        return out

    def reset(self) -> None:
        self.queries = self.rows = 0


def get_profile_multi(cc: CountingConnection, uid: int) -> dict:
    """Recompose un profil par plusieurs requêtes simples (une par table)."""
    (u,) = cc.execute("SELECT u.id, first_name, last_name, headline, region_id, photo_url "
                      "FROM users u WHERE u.id = ?", (uid,))
    pos = cc.execute('SELECT p.job_title, o.name, p.org_id, p.start, p."end" FROM positions p '
                     "JOIN organizations o ON o.id = p.org_id WHERE p.user_id = ? ORDER BY p.start DESC", (uid,))
    edu = cc.execute('SELECT school_name, start, "end" FROM education WHERE user_id = ? ORDER BY id', (uid,))
    con = cc.execute("SELECT kind, value FROM contact_info WHERE user_id = ? ORDER BY rowid", (uid,))
    return _assemble(u, pos, edu, con)


def _assemble(u, pos, edu, con) -> dict:
    return {
        "user_id": u[0], "first_name": u[1], "last_name": u[2], "headline": u[3],
        "region_id": u[4], "photo_url": u[5],
        "positions": [{"job_title": a, "organization": b, "org_id": c, "start": d, "end": e}
                      for a, b, c, d, e in pos],
        "education": [{"school_name": a, "start": b, "end": c} for a, b, c in edu],
        "contact_info": {k: v for k, v in con},
    }


JOIN_SQL = """
SELECT u.id, u.first_name, u.last_name, u.headline, u.region_id, u.photo_url,
       p.id, p.job_title, o.name, p.org_id, p.start, p."end",
       e.id, e.school_name, e.start, e."end",
       c.rowid, c.kind, c.value
  FROM users u
  LEFT JOIN positions p     ON p.user_id = u.id
  LEFT JOIN organizations o ON o.id = p.org_id
  LEFT JOIN education e     ON e.user_id = u.id
  LEFT JOIN contact_info c  ON c.user_id = u.id
 WHERE u.id = ?
"""


def get_profile_join(cc: CountingConnection, uid: int) -> dict:
    """Recompose un profil par une seule jointure multiple.

    Les trois listes étant indépendantes, la jointure renvoie leur **produit cartésien** :
    |postes| x |formations| x |coordonnées| lignes, qu'il faut dédoublonner côté client.
    """
    rows = cc.execute(JOIN_SQL, (uid,))
    u = rows[0][:6]
    pos, edu, con = {}, {}, {}
    for r in rows:
        if r[6] is not None:
            pos[r[6]] = r[7:12]
        if r[12] is not None:
            edu[r[12]] = r[13:16]
        if r[16] is not None:
            con[r[16]] = r[17:19]
    pos_l = sorted(pos.values(), key=lambda t: -t[3])
    return _assemble(u, pos_l, [edu[k] for k in sorted(edu)], [con[k] for k in sorted(con)])


def get_profile_doc(cc: CountingConnection, uid: int) -> dict:
    """Lit un profil rangé comme un document JSON : une requête, une ligne."""
    (row,) = cc.execute("SELECT doc FROM profiles WHERE user_id = ?", (uid,))
    return json.loads(row[0])


def round_trip_cost(n_queries: int, n_rows: int, rtt_ms: float = 0.5, per_row_ms: float = 0.002) -> float:
    """Modèle simple du temps vu par un client distant : allers-retours + transfert des lignes."""
    return n_queries * rtt_ms + n_rows * per_row_ms


# =====================================================================================
# 2. Pipeline d'agrégation à la MongoDB et mini-MapReduce
# =====================================================================================

_FAMILIES = {
    "Sharks": ["Grand requin blanc", "Requin-baleine", "Requin marteau", "Requin-tigre"],
    "Cetaceans": ["Dauphin commun", "Baleine à bosse", "Orque", "Marsouin"],
    "Turtles": ["Tortue verte", "Tortue luth", "Caouanne"],
    "Rays": ["Raie manta", "Pastenague"],
}


def make_observations(n: int = 5000, seed: int = 0, year: int = 2017) -> list[dict]:
    """Observations d'animaux marins (documents JSON), étalées sur une année.

    La saison compte : les requins sont plus souvent observés l'été.
    """
    rng = np.random.default_rng(seed)
    fams = list(_FAMILIES)
    out = []
    for i in range(n):
        day = int(rng.integers(0, 365))
        fam = fams[int(rng.choice(len(fams), p=[0.3, 0.35, 0.2, 0.15]))]
        if fam == "Sharks" and rng.random() < 0.5:
            day = int(np.clip(rng.normal(200, 30), 0, 364))   # pic estival
        ts = datetime.fromordinal(datetime(year, 1, 1).toordinal() + day).replace(
            hour=int(rng.integers(6, 20)), minute=int(rng.integers(0, 60)))
        out.append({"_id": i, "observationTimestamp": ts.isoformat(), "family": fam,
                    "species": str(rng.choice(_FAMILIES[fam])),
                    "numAnimals": int(rng.geometric(0.45)),
                    "observer_id": int(rng.integers(1, 40))})
    return out


def _get(doc, path: str):
    cur = doc
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _as_dt(v):
    return v if isinstance(v, datetime) else datetime.fromisoformat(v)


def _eval(expr, doc):
    """Évalue une expression d'agrégation : "$champ", constante, ou {"$op": arg}."""
    if isinstance(expr, str) and expr.startswith("$"):
        return _get(doc, expr[1:])
    if isinstance(expr, dict):
        if len(expr) == 1 and next(iter(expr)).startswith("$"):
            op, arg = next(iter(expr.items()))
            if op == "$year":
                return _as_dt(_eval(arg, doc)).year
            if op == "$month":
                return _as_dt(_eval(arg, doc)).month
            if op == "$multiply":
                v = 1
                for a in arg:
                    v *= _eval(a, doc)
                return v
            if op == "$add":
                return sum(_eval(a, doc) for a in arg)
            if op == "$size":
                return len(_eval(arg, doc) or [])
            raise ValueError(f"opérateur d'expression inconnu : {op}")
        return {k: _eval(v, doc) for k, v in expr.items()}
    return expr


_CMP = {"$gt": lambda a, b: a is not None and a > b, "$gte": lambda a, b: a is not None and a >= b,
        "$lt": lambda a, b: a is not None and a < b, "$lte": lambda a, b: a is not None and a <= b,
        "$ne": lambda a, b: a != b, "$in": lambda a, b: a in b, "$nin": lambda a, b: a not in b}


def _matches(doc, cond: dict) -> bool:
    for field, want in cond.items():
        if field == "$or":
            if not any(_matches(doc, c) for c in want):
                return False
            continue
        val = _get(doc, field)
        if isinstance(want, dict) and want and all(k.startswith("$") for k in want):
            if not all(_CMP[op](val, arg) for op, arg in want.items()):
                return False
        elif isinstance(val, list) and not isinstance(want, list):
            if want not in val:                 # égalité sur un tableau : « contient »
                return False
        elif val != want:
            return False
    return True


def _freeze(v):
    return json.dumps(v, sort_keys=True, default=str)


def aggregate(docs: list[dict], pipeline: list[dict], collections: dict | None = None,
              stats: dict | None = None) -> list[dict]:
    """Exécute un pipeline d'agrégation (sous-ensemble du langage de MongoDB).

    Étapes : $match, $group (accumulateurs $sum, $avg, $min, $max, $push, $first),
    $sort, $limit, $project, $unwind, $lookup, $count. `collections` fournit les
    collections référencées par $lookup. `stats`, si fourni, reçoit le nombre de
    documents en sortie de chaque étape.
    """
    collections = collections or {}
    cur = list(docs)
    for i, stage in enumerate(pipeline):
        (op, arg), = stage.items()
        if op == "$match":
            cur = [d for d in cur if _matches(d, arg)]
        elif op == "$group":
            groups, order = {}, []
            for d in cur:
                key = _eval(arg["_id"], d)
                k = _freeze(key)
                if k not in groups:
                    groups[k] = {"_id": key, "_acc": defaultdict(list)}
                    order.append(k)
                for name, acc in arg.items():
                    if name == "_id":
                        continue
                    (aop, aexpr), = acc.items()
                    groups[k]["_acc"][name].append(_eval(aexpr, d))
            out = []
            for k in order:
                g = groups[k]
                row = {"_id": g["_id"]}
                for name, acc in arg.items():
                    if name == "_id":
                        continue
                    (aop, _), = acc.items()
                    vals = g["_acc"][name]
                    row[name] = {"$sum": sum, "$avg": lambda v: sum(v) / len(v), "$min": min, "$max": max,
                                 "$push": list, "$first": lambda v: v[0]}[aop](vals)
                out.append(row)
            cur = out
        elif op == "$sort":
            for field, direction in reversed(list(arg.items())):
                cur.sort(key=lambda d: _freeze(_get(d, field)) if isinstance(_get(d, field), dict)
                         else _get(d, field), reverse=direction < 0)
        elif op == "$limit":
            cur = cur[:arg]
        elif op == "$project":
            cur = [{**({"_id": d.get("_id")} if arg.get("_id", 1) else {}),
                    **{k: (_get(d, k) if v in (1, True) else _eval(v, d))
                       for k, v in arg.items() if k != "_id" and v not in (0, False)}} for d in cur]
        elif op == "$unwind":
            field = arg[1:]
            cur = [{**d, field: item} for d in cur for item in (_get(d, field) or [])]
        elif op == "$lookup":
            foreign = defaultdict(list)
            for f in collections[arg["from"]]:
                foreign[_freeze(_get(f, arg["foreignField"]))].append(f)
            cur = [{**d, arg["as"]: list(foreign.get(_freeze(_get(d, arg["localField"])), []))} for d in cur]
        elif op == "$count":
            cur = [{arg: len(cur)}]
        else:
            raise ValueError(f"étape inconnue : {op}")
        if stats is not None:
            stats[f"{i}:{op}"] = len(cur)
    return cur


def map_reduce(docs: list[dict], mapper, reducer, query: dict | None = None,
               stats: dict | None = None) -> dict:
    """MapReduce sur une collection : `mapper(doc, emit)` puis `reducer(clé, valeurs)`.

    `query` filtre d'abord les documents (comme le paramètre `query` de MongoDB).
    """
    groups = defaultdict(list)
    n_in = n_emit = 0

    def emit(k, v):
        nonlocal n_emit
        n_emit += 1
        groups[k].append(v)

    for d in docs:
        if query is None or _matches(d, query):
            n_in += 1
            mapper(d, emit)
    out = {k: (reducer(k, vs) if len(vs) > 1 else vs[0]) for k, vs in groups.items()}
    if stats is not None:
        stats.update(documents=n_in, emissions=n_emit, cles=len(groups))
    return out


# =====================================================================================
# 3. Graphe de propriétés
# =====================================================================================

class PropertyGraph:
    """Graphe de propriétés : sommets et arêtes étiquetés, chacun avec ses propriétés.

    Deux index (arêtes sortantes et entrantes de chaque sommet) permettent de parcourir
    le graphe dans les deux sens, comme les index `edges_tails` et `edges_heads`
    d'un schéma relationnel à deux tables.
    """

    def __init__(self):
        self.vertices: dict = {}                  # id -> (label, props)
        self.edges: list = []                     # (edge_id, tail, head, label, props)
        self._out = defaultdict(list)             # tail -> [edge_id]
        self._in = defaultdict(list)              # head -> [edge_id]
        self._by_name = defaultdict(list)         # props["name"] -> [vid] (index secondaire)

    def add_vertex(self, vid, label: str, **props):
        self.vertices[vid] = (label, props)
        if "name" in props:
            self._by_name[props["name"]].append(vid)
        return vid

    def add_edge(self, tail, head, label: str, **props) -> int:
        eid = len(self.edges)
        self.edges.append((eid, tail, head, label, props))
        self._out[tail].append(eid)
        self._in[head].append(eid)
        return eid

    def label(self, vid) -> str:
        return self.vertices[vid][0]

    def props(self, vid) -> dict:
        return self.vertices[vid][1]

    def out(self, vid, label: str | None = None, counter: dict | None = None) -> list:
        res = []
        for e in self._out.get(vid, ()):
            if counter is not None:
                counter["aretes"] = counter.get("aretes", 0) + 1
            if label is None or self.edges[e][3] == label:
                res.append(self.edges[e][2])
        return res

    def inn(self, vid, label: str | None = None, counter: dict | None = None) -> list:
        res = []
        for e in self._in.get(vid, ()):
            if counter is not None:
                counter["aretes"] = counter.get("aretes", 0) + 1
            if label is None or self.edges[e][3] == label:
                res.append(self.edges[e][1])
        return res

    def find(self, label: str | None = None, **props) -> list:
        """Sommets ayant ce label et ces propriétés (index sur `name` si fourni)."""
        cands = self._by_name.get(props["name"], []) if "name" in props else list(self.vertices)
        return [v for v in cands if (label is None or self.vertices[v][0] == label)
                and all(self.vertices[v][1].get(k) == val for k, val in props.items())]

    def closure(self, starts, label: str, direction: str = "in", counter: dict | None = None) -> set:
        """Sommets atteignables depuis `starts` en suivant 0, 1, 2... arêtes `label`.

        direction="out" suit les arêtes dans leur sens (de la queue vers la tête),
        direction="in" les remonte. C'est le `-[:WITHIN*0..]->` de Cypher.
        """
        step = self.out if direction == "out" else self.inn
        seen = set(starts)
        frontier = list(starts)
        while frontier:
            nxt = []
            for v in frontier:
                for w in step(v, label, counter):
                    if w not in seen:
                        seen.add(w)
                        nxt.append(w)
            frontier = nxt
        return seen

    def to_sqlite(self, con: sqlite3.Connection, indexes: bool = True) -> None:
        """Exporte le graphe dans deux tables `vertices` et `edges` (propriétés en JSON)."""
        con.executescript("""
        DROP TABLE IF EXISTS vertices; DROP TABLE IF EXISTS edges;
        CREATE TABLE vertices (vertex_id INTEGER PRIMARY KEY, label TEXT, properties TEXT);
        CREATE TABLE edges (edge_id INTEGER PRIMARY KEY,
                            tail_vertex INTEGER REFERENCES vertices (vertex_id),
                            head_vertex INTEGER REFERENCES vertices (vertex_id),
                            label TEXT, properties TEXT);
        """)
        ids = {v: i for i, v in enumerate(self.vertices)}
        con.executemany("INSERT INTO vertices VALUES (?, ?, ?)",
                        [(ids[v], lab, json.dumps(p, ensure_ascii=False)) for v, (lab, p) in self.vertices.items()])
        con.executemany("INSERT INTO edges VALUES (?, ?, ?, ?, ?)",
                        [(e, ids[t], ids[h], lab, json.dumps(p, ensure_ascii=False))
                         for e, t, h, lab, p in self.edges])
        if indexes:
            con.executescript("""
            CREATE INDEX edges_tails ON edges (tail_vertex, label);
            CREATE INDEX edges_heads ON edges (head_vertex, label);
            CREATE INDEX vertices_name ON vertices (label, properties ->> 'name');
            """)
        con.commit()


def example_graph() -> PropertyGraph:
    """Le petit graphe fil rouge du cours : trois personnes, des lieux emboîtés.

    Les granularités varient volontairement : Inès est née dans une ville, Léa dans une
    région ; le Québec a des « régions administratives », la France des régions et des
    départements.
    """
    g = PropertyGraph()
    L = lambda vid, name, typ: g.add_vertex(vid, "Location", name=name, type=typ)
    L("europe", "Europe", "continent"); L("amnord", "Amérique du Nord", "continent"); L("asie", "Asie", "continent")
    L("france", "France", "pays"); L("canada", "Canada", "pays"); L("japon", "Japon", "pays")
    L("aura", "Auvergne-Rhône-Alpes", "région"); L("bretagne", "Bretagne", "région")
    L("rhone", "Rhône", "département"); L("lyon", "Lyon", "ville")
    L("quebec", "Québec", "province"); L("montreal", "Montréal", "ville")
    L("kansai", "Kansai", "région"); L("osaka", "Osaka", "ville")
    for a, b in [("france", "europe"), ("canada", "amnord"), ("japon", "asie"), ("aura", "france"),
                 ("bretagne", "france"), ("rhone", "aura"), ("lyon", "rhone"), ("quebec", "canada"),
                 ("montreal", "quebec"), ("kansai", "japon"), ("osaka", "kansai")]:
        g.add_edge(a, b, "WITHIN")
    g.add_vertex("ines", "Person", name="Inès", birth_year=1988)
    g.add_vertex("kenji", "Person", name="Kenji", birth_year=1985)
    g.add_vertex("lea", "Person", name="Léa", birth_year=1993)
    g.add_edge("ines", "lyon", "BORN_IN"); g.add_edge("ines", "montreal", "LIVES_IN")
    g.add_edge("kenji", "osaka", "BORN_IN"); g.add_edge("kenji", "montreal", "LIVES_IN")
    g.add_edge("lea", "bretagne", "BORN_IN"); g.add_edge("lea", "lyon", "LIVES_IN")
    g.add_edge("ines", "kenji", "MARRIED", since=2016); g.add_edge("kenji", "ines", "MARRIED", since=2016)
    return g


def make_world_graph(n_people: int = 20_000, seed: int = 0, countries_per_continent: int = 8,
                     regions_per_country: int = 6, cities_per_region: int = 10,
                     coarse: float = 0.1) -> PropertyGraph:
    """Graphe synthétique : continents > pays > régions > villes, et des personnes.

    Chaque personne est née et vit dans un lieu ; une fraction `coarse` des lieux est
    renseignée seulement au niveau de la région ou du pays (granularité variable).
    Environ 70 % des personnes vivent sur leur continent de naissance.
    """
    rng = np.random.default_rng(seed)
    g = PropertyGraph()
    continents = ["Europe", "Amérique du Nord", "Amérique du Sud", "Afrique", "Asie", "Océanie"]
    cities_by_cont, coarse_by_cont = {}, {}
    for ci, cname in enumerate(continents):
        cid = f"c{ci}"
        g.add_vertex(cid, "Location", name=cname, type="continent")
        cities_by_cont[ci], coarse_by_cont[ci] = [], []
        for k in range(countries_per_continent):
            kid = f"{cid}k{k}"
            g.add_vertex(kid, "Location", name=f"Pays {ci}.{k}", type="pays")
            g.add_edge(kid, cid, "WITHIN")
            coarse_by_cont[ci].append(kid)
            for r in range(regions_per_country):
                rid = f"{kid}r{r}"
                g.add_vertex(rid, "Location", name=f"Région {ci}.{k}.{r}", type="région")
                g.add_edge(rid, kid, "WITHIN")
                coarse_by_cont[ci].append(rid)
                for v in range(cities_per_region):
                    vid = f"{rid}v{v}"
                    g.add_vertex(vid, "Location", name=f"Ville {ci}.{k}.{r}.{v}", type="ville")
                    g.add_edge(vid, rid, "WITHIN")
                    cities_by_cont[ci].append(vid)

    def place(ci):
        pool = coarse_by_cont[ci] if rng.random() < coarse else cities_by_cont[ci]
        return pool[int(rng.integers(len(pool)))]

    w_cont = np.array([0.22, 0.12, 0.1, 0.18, 0.33, 0.05])
    for i in range(n_people):
        pid = f"p{i}"
        g.add_vertex(pid, "Person", name=f"Personne {i}")
        born = int(rng.choice(6, p=w_cont))
        lives = born if rng.random() < 0.7 else int(rng.choice(6, p=w_cont))
        g.add_edge(pid, place(born), "BORN_IN")
        g.add_edge(pid, place(lives), "LIVES_IN")
    return g


def emigrants_forward(g: PropertyGraph, born: str, lives: str, counter: dict | None = None) -> set:
    """Stratégie 1 : partir de chaque personne et remonter ses lieux (WITHIN sortants)."""
    res = set()
    for v, (lab, p) in g.vertices.items():
        if lab != "Person":
            continue
        if counter is not None:
            counter["sommets"] = counter.get("sommets", 0) + 1
        ok = True
        for rel, target in (("BORN_IN", born), ("LIVES_IN", lives)):
            places = g.out(v, rel, counter)
            anc = g.closure(places, "WITHIN", "out", counter)
            if not any(g.props(a).get("name") == target for a in anc):
                ok = False
                break
        if ok:
            res.add(p["name"])
    return res


def emigrants_backward(g: PropertyGraph, born: str, lives: str, counter: dict | None = None) -> set:
    """Stratégie 2 : partir des deux lieux (index sur `name`) et descendre (WITHIN entrants)."""
    sets = []
    for rel, target in (("BORN_IN", born), ("LIVES_IN", lives)):
        start = g.find("Location", name=target)
        if counter is not None:
            counter["sommets"] = counter.get("sommets", 0) + len(start)
        inside = g.closure(start, "WITHIN", "in", counter)
        people = set()
        for loc in inside:
            people.update(g.inn(loc, rel, counter))
        sets.append(people)
    return {g.props(v)["name"] for v in sets[0] & sets[1]}


EMIGRANTS_SQL = """
WITH RECURSIVE
  -- tous les lieux situés (directement ou non) dans le lieu de naissance cherché
  in_born(vertex_id) AS (
      SELECT vertex_id FROM vertices
       WHERE label = 'Location' AND properties ->> 'name' = :born
    UNION
      SELECT edges.tail_vertex FROM edges
        JOIN in_born ON edges.head_vertex = in_born.vertex_id
       WHERE edges.label = 'WITHIN'
  ),
  -- tous les lieux situés dans le lieu de résidence cherché
  in_lives(vertex_id) AS (
      SELECT vertex_id FROM vertices
       WHERE label = 'Location' AND properties ->> 'name' = :lives
    UNION
      SELECT edges.tail_vertex FROM edges
        JOIN in_lives ON edges.head_vertex = in_lives.vertex_id
       WHERE edges.label = 'WITHIN'
  ),
  born_there(vertex_id) AS (
      SELECT edges.tail_vertex FROM edges
        JOIN in_born ON edges.head_vertex = in_born.vertex_id
       WHERE edges.label = 'BORN_IN'
  ),
  living_there(vertex_id) AS (
      SELECT edges.tail_vertex FROM edges
        JOIN in_lives ON edges.head_vertex = in_lives.vertex_id
       WHERE edges.label = 'LIVES_IN'
  )
SELECT vertices.properties ->> 'name'
  FROM vertices
  JOIN born_there   ON vertices.vertex_id = born_there.vertex_id
  JOIN living_there ON vertices.vertex_id = living_there.vertex_id
"""


def emigrants_sql(con: sqlite3.Connection, born: str, lives: str) -> set:
    return {r[0] for r in con.execute(EMIGRANTS_SQL, {"born": born, "lives": lives})}


# =====================================================================================
# 4. Magasin de triplets et requêtes à la SPARQL
# =====================================================================================

class TripleStore:
    """Triplets (sujet, prédicat, objet) avec trois index : SPO, POS et OSP.

    Les objets sont soit des littéraux (str, int...), soit des sommets. Pour distinguer
    les deux, les sommets sont des chaînes commençant par « _: » (nœuds anonymes de
    Turtle) ou des URI entre chevrons.
    """

    def __init__(self):
        self.spo = defaultdict(lambda: defaultdict(set))
        self.pos = defaultdict(lambda: defaultdict(set))
        self.osp = defaultdict(lambda: defaultdict(set))
        self.n = 0

    def add(self, s, p, o) -> None:
        if o in self.spo[s][p]:
            return
        self.spo[s][p].add(o)
        self.pos[p][o].add(s)
        self.osp[o][s].add(p)
        self.n += 1

    def __len__(self) -> int:
        return self.n

    def match(self, s=None, p=None, o=None):
        """Triplets correspondant au motif (None = n'importe quoi), via le meilleur index."""
        if s is not None:
            preds = [p] if p is not None else list(self.spo.get(s, {}))
            for pp in preds:
                for oo in self.spo.get(s, {}).get(pp, ()):
                    if o is None or oo == o:
                        yield s, pp, oo
        elif p is not None:
            objs = [o] if o is not None else list(self.pos.get(p, {}))
            for oo in objs:
                for ss in self.pos.get(p, {}).get(oo, ()):
                    yield ss, p, oo
        elif o is not None:
            for ss, preds in self.osp.get(o, {}).items():
                for pp in preds:
                    yield ss, pp, o
        else:
            for ss, d in self.spo.items():
                for pp, objs in d.items():
                    for oo in objs:
                        yield ss, pp, oo

    # -------- chemins de propriétés : ":bornIn / :within* / :name"
    def _step_fwd(self, nodes, pred):
        if pred.endswith("*"):
            base, seen, frontier = pred[:-1], set(nodes), list(nodes)
            while frontier:
                nxt = []
                for n in frontier:
                    for o in self.spo.get(n, {}).get(base, ()):
                        if o not in seen:
                            seen.add(o)
                            nxt.append(o)
                frontier = nxt
            return seen
        return {o for n in nodes for o in self.spo.get(n, {}).get(pred, ())}

    def _step_bwd(self, nodes, pred):
        if pred.endswith("*"):
            base, seen, frontier = pred[:-1], set(nodes), list(nodes)
            while frontier:
                nxt = []
                for n in frontier:
                    for s in self.pos.get(base, {}).get(n, ()):
                        if s not in seen:
                            seen.add(s)
                            nxt.append(s)
                frontier = nxt
            return seen
        return {s for n in nodes for s in self.pos.get(pred, {}).get(n, ())}

    def path_objects(self, s, path) -> set:
        nodes = {s}
        for pred in path:
            nodes = self._step_fwd(nodes, pred)
        return nodes

    def path_subjects(self, o, path) -> set:
        nodes = {o}
        for pred in reversed(path):
            nodes = self._step_bwd(nodes, pred)
        return nodes

    def query(self, patterns, stats: list | None = None) -> list[dict]:
        """Évalue un motif de graphe : liste de triplets dont les termes « ?x » sont des variables.

        Le prédicat peut être un chemin : un tuple de prédicats, « p* » signifiant « zéro,
        une ou plusieurs fois p ». Les motifs sont joints **dans l'ordre donné** (pas
        d'optimiseur) ; `stats` reçoit le nombre de solutions partielles après chaque motif.
        """
        isvar = lambda t: isinstance(t, str) and t.startswith("?")
        sols = [{}]
        for s, p, o in patterns:
            new = []
            for b in sols:
                ss = b.get(s, s) if isvar(s) else s
                oo = b.get(o, o) if isvar(o) else o
                s_free, o_free = isvar(ss), isvar(oo)
                if isinstance(p, tuple):
                    if not s_free:
                        cands = [(ss, x) for x in self.path_objects(ss, p)]
                    elif not o_free:
                        cands = [(x, oo) for x in self.path_subjects(oo, p)]
                    else:
                        cands = [(x, y) for x in list(self.spo) for y in self.path_objects(x, p)]
                    for x, y in cands:
                        if (o_free or y == oo) and (s_free or x == ss):
                            nb = dict(b)
                            if s_free:
                                nb[s] = x
                            if o_free:
                                nb[o] = y
                            new.append(nb)
                else:
                    pp = b.get(p, p) if isvar(p) else p
                    for x, y, z in self.match(None if s_free else ss, None if isvar(pp) else pp,
                                              None if o_free else oo):
                        nb = dict(b)
                        if s_free:
                            nb[s] = x
                        if isvar(pp):
                            nb[p] = y
                        if o_free:
                            if o in nb and nb[o] != z:
                                continue
                            nb[o] = z
                        new.append(nb)
            sols = new
            if stats is not None:
                stats.append(len(sols))
        return sols

    def to_turtle(self, prefix: str = "urn:example:") -> str:
        """Sérialisation Turtle compacte (un sujet par ligne, « ; » entre prédicats)."""
        lines = [f"@prefix : <{prefix}>."]
        fmt_o = lambda o: o if isinstance(o, str) and o.startswith("_:") else json.dumps(o, ensure_ascii=False)
        for s, preds in self.spo.items():
            parts = []
            for p in sorted(preds, key=lambda x: (x != "a", x)):
                for o in sorted(preds[p], key=str):
                    parts.append(("a " + ":" + o) if p == "a" else f":{p} {fmt_o(o)}")
            lines.append(f"{s} " + "; ".join(parts) + ".")
        return "\n".join(lines)


def graph_to_triples(g: PropertyGraph) -> TripleStore:
    """Traduit un graphe de propriétés en triplets.

    Sommet v de label L : (_:v, a, L) ; propriété k=x : (_:v, k, x) ; arête de label E :
    (_:tail, e, _:head) avec e = E en camelCase (BORN_IN -> bornIn).
    """
    ts = TripleStore()
    camel = lambda lab: re.sub(r"_([a-z])", lambda m: m.group(1).upper(), lab.lower())
    for v, (lab, props) in g.vertices.items():
        ts.add(f"_:{v}", "a", lab)
        for k, x in props.items():
            ts.add(f"_:{v}", camel(k.upper()), x)
    for _, t, h, lab, _p in g.edges:
        ts.add(f"_:{t}", camel(lab), f"_:{h}")
    return ts


# =====================================================================================
# 5. Datalog
# =====================================================================================

_TOKEN = re.compile(r'\s*(?:(:-)|("(?:[^"\\]|\\.)*")|(-?\d+)|([A-Za-z_][A-Za-z0-9_]*)|([(),.]))')


def _is_var(t) -> bool:
    return isinstance(t, str) and not t.startswith('"') and (t[0].isupper() or t[0] == "_")


def parse_datalog(text: str):
    """Analyse un programme Datalog : faits `p(a, 1, "x").` et règles `h(X) :- b(X, Y), c(Y).`

    Variables : identifiants commençant par une majuscule ou « _ » (le « _ » seul est
    anonyme). Constantes : nombres, chaînes entre guillemets, identifiants en minuscules.
    Commentaires : `/* ... */` et `% ...`. Renvoie (faits, règles) où un fait est
    (prédicat, tuple) et une règle (tête, [atomes du corps]), un atome étant (prédicat, tuple).
    """
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.S)
    text = re.sub(r"%[^\n]*", " ", text)
    toks, pos = [], 0
    while pos < len(text):
        if text[pos:].strip() == "":
            break
        m = _TOKEN.match(text, pos)
        if not m:
            raise SyntaxError(f"Datalog : caractère inattendu près de {text[pos:pos + 20]!r}")
        pos = m.end()
        arrow, s, num, ident, punct = m.groups()
        toks.append(("op", ":-") if arrow else ("str", json.loads(s)) if s else
                    ("num", int(num)) if num else ("id", ident) if ident else ("p", punct))
    i = 0
    anon = [0]

    def atom():
        nonlocal i
        kind, name = toks[i]
        assert kind == "id", f"prédicat attendu, trouvé {name!r}"
        i += 1
        assert toks[i] == ("p", "("), f"« ( » attendu après {name}"
        i += 1
        args = []
        while toks[i] != ("p", ")"):
            k, v = toks[i]
            if k == "id" and v == "_":
                anon[0] += 1
                v = f"_{anon[0]}"
            elif k == "str":
                v = '"' + v                     # marque les constantes chaînes
            args.append(v)
            i += 1
            if toks[i] == ("p", ","):
                i += 1
        i += 1
        return name, tuple(args)

    facts, rules = [], []
    while i < len(toks):
        head = atom()
        if toks[i] == ("op", ":-"):
            i += 1
            body = [atom()]
            while toks[i] == ("p", ","):
                i += 1
                body.append(atom())
            rules.append((head, body))
        else:
            if any(_is_var(a) for a in head[1]):
                raise SyntaxError(f"un fait ne peut pas contenir de variable : {head}")
            facts.append((head[0], tuple(_const(a) for a in head[1])))
        assert toks[i] == ("p", "."), "« . » attendu en fin de clause"
        i += 1
    return facts, rules
