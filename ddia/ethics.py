"""Faire ce qui est juste : simulations pour l'éthique des systèmes de données.

Le livre ne contient ni code ni chiffres sur ces sujets ; ce module donne des modèles
jouets, volontairement simples et synthétiques, pour rendre les arguments mesurables :

- prédiction et « prison algorithmique » : plusieurs organisations qui utilisent un même
  score excluent toujours les mêmes personnes (:func:`exclusion_counts`) ;
- biais et discrimination : population synthétique avec un attribut protégé, une
  variable proxy (le quartier) et des étiquettes historiques biaisées
  (:func:`make_credit_population`), régression logistique en numpy
  (:func:`fit_logistic`), métriques d'équité par groupe (:func:`group_metrics`,
  :func:`calibration_table`) ;
- boucles de rétroaction : spirale score de crédit / emploi (:func:`credit_spiral`),
  allocation de patrouilles (:func:`patrol_feedback`), recommandeur glouton
  (:func:`recommender_loop`) ;
- vie privée : population avec quasi-identifiants (:func:`make_population`), unicité et
  k-anonymat (:func:`uniqueness`, :func:`k_anonymity`, :func:`generalize`), attaque par
  jointure (:func:`linkage_attack`), profils fantômes (:func:`shadow_profiles`), réponse
  randomisée (:func:`randomized_response`, :func:`rr_estimate`), exposition aux fuites en
  fonction de la durée de conservation (:func:`breach_exposure`).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

__all__ = [
    "sigmoid", "exclusion_counts", "make_credit_population", "design_matrix", "fit_logistic",
    "predict_proba", "group_metrics", "calibration_table", "credit_spiral", "patrol_feedback",
    "recommender_loop", "make_population", "uniqueness", "k_anonymity", "class_sizes",
    "generalize", "linkage_attack", "shadow_profiles", "randomized_response", "rr_estimate",
    "rr_variance", "rr_posterior_yes", "breach_exposure", "expected_unique_fraction",
]


def sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.asarray(z, dtype=float)))


# --------------------------------------------------------------------------------------
# Prison algorithmique
# --------------------------------------------------------------------------------------
def exclusion_counts(n_people: int = 10_000, n_orgs: int = 10, reject_frac: float = 0.1,
                     rho: float = 0.0, signal: float = 1.0, seed: int = 0):
    """Nombre de refus subis par chaque personne quand `n_orgs` organisations refusent
    chacune la fraction `reject_frac` la plus « risquée » selon leur propre score.

    score_k = signal * risque_vrai + erreur_k, avec erreur_k = sqrt(rho) * erreur commune
    + sqrt(1 - rho) * erreur propre (variance 1). rho = 1 : tout le monde utilise le même
    score (même fournisseur de données, même modèle).

    Renvoie (refus par personne, risque vrai).
    """
    rng = np.random.default_rng(seed)
    risk = rng.normal(size=n_people)
    shared = rng.normal(size=n_people)
    counts = np.zeros(n_people, dtype=int)
    for _ in range(n_orgs):
        err = math.sqrt(rho) * shared + math.sqrt(1 - rho) * rng.normal(size=n_people)
        score = signal * risk + err
        cut = np.quantile(score, 1 - reject_frac)
        counts += score > cut
    return counts, risk


# --------------------------------------------------------------------------------------
# Biais et discrimination
# --------------------------------------------------------------------------------------
def make_credit_population(n: int = 20_000, frac_b: float = 0.3, n_zip: int = 20,
                           segregation: float = 0.85, label_bias: float = 0.25,
                           income_gap: float = 0.0, seed: int = 0) -> pd.DataFrame:
    """Population synthétique de demandeurs de crédit.

    - ``group`` : attribut protégé (0 = A, 1 = B), B est minoritaire ;
    - ``zip`` : quartier ; avec probabilité `segregation`, une personne de B habite l'un
      des quartiers 0 à 4, une personne de A l'un des quartiers 5 à n_zip - 1 ;
    - ``ability`` : capacité réelle de remboursement, **même loi dans les deux groupes** ;
    - ``income``, ``debt`` : caractéristiques observées, bruitées (``income_gap`` décale le
      revenu de B pour modéliser une inégalité structurelle sans effet sur le remboursement) ;
    - ``repaid`` : remboursement réel (ne dépend que de ``ability``) ;
    - ``label`` : étiquette **historique** : une personne de B qui a remboursé est quand
      même enregistrée comme mauvais payeur avec probabilité `label_bias` (décisions
      passées discriminatoires, recouvrements plus agressifs, historique manquant).
    """
    rng = np.random.default_rng(seed)
    group = (rng.random(n) < frac_b).astype(int)
    seg = rng.random(n) < segregation
    zip_b = rng.integers(0, 5, size=n)
    zip_a = rng.integers(5, n_zip, size=n)
    zip_any = rng.integers(0, n_zip, size=n)
    zipc = np.where(seg, np.where(group == 1, zip_b, zip_a), zip_any)
    ability = rng.normal(size=n)
    income = ability + 0.6 * rng.normal(size=n) - income_gap * group
    debt = -0.5 * ability + 0.8 * rng.normal(size=n)
    p_true = sigmoid(1.2 + 1.3 * ability)
    repaid = (rng.random(n) < p_true).astype(int)
    flip = (group == 1) & (repaid == 1) & (rng.random(n) < label_bias)
    label = np.where(flip, 0, repaid)
    return pd.DataFrame({"group": group, "zip": zipc, "ability": ability, "income": income,
                         "debt": debt, "p_true": p_true, "repaid": repaid, "label": label})


def design_matrix(df: pd.DataFrame, use_group: bool = False, use_zip: bool = True,
                  n_zip: int | None = None) -> np.ndarray:
    """Variables explicatives : constante, revenu, dette, [groupe], [indicatrices de quartier]."""
    cols = [np.ones(len(df)), df["income"].to_numpy(), df["debt"].to_numpy()]
    if use_group:
        cols.append(df["group"].to_numpy().astype(float))
    if use_zip:
        k = n_zip or int(df["zip"].max()) + 1
        z = df["zip"].to_numpy()
        for j in range(1, k):                      # quartier 0 = référence
            cols.append((z == j).astype(float))
    return np.column_stack(cols)
