"""Outils du lab « dégrouper la base » : lecture du flux de changements de PostgreSQL.

Le plugin de décodage logique ``test_decoding`` produit une ligne de texte par changement,
par exemple ::

    table public.doc: INSERT: id[integer]:1 titre[text]:'Bonjour l''ami' auteur[text]:null
    table public.doc: UPDATE: id[integer]:1 titre[text]:'Autre'
    table public.doc: DELETE: id[integer]:1

``parse_change`` transforme une telle ligne en dictionnaire ; ``tokenize`` et
``InvertedIndexView`` décrivent la fonction de dérivation d'un index inversé, indépendamment
du stockage (Redis dans le lab, un dictionnaire Python dans les tests).
"""

from __future__ import annotations

import re
import unicodedata
from collections import Counter, defaultdict

__all__ = ["parse_change", "parse_values", "tokenize", "InvertedIndexView", "rebuild_index"]

_HEAD = re.compile(r"^table (?P<schema>[^.]+)\.(?P<table>[^:]+): (?P<op>INSERT|UPDATE|DELETE): ?(?P<rest>.*)$", re.S)
_COL = re.compile(r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\[(?P<type>[^\]]+)\]:")

_INT_TYPES = {"integer", "bigint", "smallint"}
_FLOAT_TYPES = {"numeric", "double precision", "real"}


def _convert(raw: str, typ: str):
    if raw == "null":
        return None
    if raw.startswith("'"):
        return raw[1:-1].replace("''", "'")
    if typ in _INT_TYPES:
        return int(raw)
    if typ in _FLOAT_TYPES:
        return float(raw)
    if typ == "boolean":
        return raw == "true"
    return raw


def parse_values(text: str) -> dict:
    """Analyse une suite ``col[type]:valeur`` (valeurs textuelles entre apostrophes)."""
    out, i, n = {}, 0, len(text)
    while i < n:
        while i < n and text[i] == " ":
            i += 1
        if i >= n:
            break
        m = _COL.match(text, i)
        if not m:
            raise ValueError(f"colonne illisible à la position {i} : {text[i:i + 30]!r}")
        i = m.end()
        if i < n and text[i] == "'":
            j = i + 1
            while True:
                k = text.index("'", j)
                if k + 1 < n and text[k + 1] == "'":
                    j = k + 2
                    continue
                break
            raw, i = text[i:k + 1], k + 1
        else:
            j = text.find(" ", i)
            j = n if j < 0 else j
            raw, i = text[i:j], j
        out[m.group("name")] = _convert(raw, m.group("type"))
    return out


def parse_change(line: str) -> dict | None:
    """Ligne ``test_decoding`` -> {"table", "op", "row", "old"} ; None pour BEGIN/COMMIT."""
    m = _HEAD.match(line)
    if not m:
        return None
    rest, old = m.group("rest"), None
    if rest.startswith("old-key: "):
        k = rest.index(" new-tuple: ")
        old = parse_values(rest[len("old-key: "):k])
        rest = rest[k + len(" new-tuple: "):]
    return {"table": m.group("table"), "op": m.group("op"), "row": parse_values(rest), "old": old}


_STOP = {"le", "la", "les", "de", "des", "du", "un", "une", "et", "en", "a", "au", "aux", "l", "d"}


def tokenize(text: str | None, stem: bool = True) -> set[str]:
    """Analyse simplifiée d'un moteur de recherche : minuscules, accents retirés, mots vides
    éliminés et racinisation très rudimentaire (pluriel en -s retiré)."""
    if not text:
        return set()
    t = unicodedata.normalize("NFKD", text.lower())
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    words = re.findall(r"[a-z0-9]+", t)
    out = set()
    for w in words:
        if w in _STOP or len(w) < 2:
            continue
        if stem and len(w) > 3 and w[-1] == "s":
            w = w[:-1]
        out.add(w)
    return out


class InvertedIndexView:
    """Vue dérivée en mémoire : index inversé terme -> ensemble d'identifiants, et compteur
    de documents par auteur. ``apply`` est idempotent : rejouer un changement ne modifie
    rien, car on mémorise les termes et l'auteur de chaque document."""

    def __init__(self, tokenizer=tokenize):
        self.tokenizer = tokenizer
        self.postings: dict[str, set] = defaultdict(set)
        self.doc_terms: dict = {}
        self.doc_author: dict = {}

    def apply(self, change: dict) -> None:
        row = change["row"]
        doc_id = row["id"]
        for term in self.doc_terms.pop(doc_id, set()):
            self.postings[term].discard(doc_id)
            if not self.postings[term]:
                del self.postings[term]
        self.doc_author.pop(doc_id, None)
        if change["op"] == "DELETE":
            return
        terms = self.tokenizer(row.get("titre")) | self.tokenizer(row.get("corps"))
        self.doc_terms[doc_id] = terms
        self.doc_author[doc_id] = row.get("auteur")
        for term in terms:
            self.postings[term].add(doc_id)

    def authors(self) -> Counter:
        return Counter(a for a in self.doc_author.values() if a is not None)

    def search(self, query: str) -> set:
        terms = self.tokenizer(query)
        if not terms:
            return set()
        return set.intersection(*(self.postings.get(t, set()) for t in terms))


def rebuild_index(rows, tokenizer=tokenize) -> InvertedIndexView:
    """Recalcul complet de la vue à partir des lignes actuelles de la table source."""
    v = InvertedIndexView(tokenizer)
    for r in rows:
        v.apply({"op": "INSERT", "row": r, "old": None})
    return v
