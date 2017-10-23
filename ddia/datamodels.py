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
