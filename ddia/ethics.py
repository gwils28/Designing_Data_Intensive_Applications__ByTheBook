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


def fit_logistic(X: np.ndarray, y: np.ndarray, l2: float = 1e-3, iters: int = 30) -> np.ndarray:
    """Régression logistique par la méthode de Newton (moindres carrés repondérés)."""
    w = np.zeros(X.shape[1])
    reg = l2 * np.eye(X.shape[1])
    reg[0, 0] = 0.0
    for _ in range(iters):
        p = sigmoid(X @ w)
        g = X.T @ (p - y) + reg @ w
        H = (X * (p * (1 - p))[:, None]).T @ X + reg
        step = np.linalg.solve(H, g)
        w -= step
        if np.max(np.abs(step)) < 1e-9:
            break
    return w


def predict_proba(X: np.ndarray, w: np.ndarray) -> np.ndarray:
    return sigmoid(X @ w)


def group_metrics(y: np.ndarray, score: np.ndarray, group: np.ndarray, threshold: float,
                  names=("A", "B")) -> pd.DataFrame:
    """Métriques par groupe pour la décision « accepter si score >= seuil ».

    y = 1 : la personne rembourse réellement (« bon payeur »).
    - taux d'acceptation ; TPR (bons payeurs acceptés) ; FPR (mauvais payeurs acceptés) ;
    - taux de refus à tort = 1 - TPR ; précision (PPV) des acceptations.
    """
    y, score, group = map(np.asarray, (y, score, group))
    rows = {}
    for g, name in enumerate(names):
        m = group == g
        acc = score[m] >= threshold
        yy = y[m]
        rows[name] = {
            "effectif": int(m.sum()),
            "taux de bons payeurs": yy.mean(),
            "taux d'acceptation": acc.mean(),
            "TPR (bons acceptés)": acc[yy == 1].mean(),
            "bons payeurs refusés": 1 - acc[yy == 1].mean(),
            "FPR (mauvais acceptés)": acc[yy == 0].mean(),
            "PPV (acceptés qui remboursent)": yy[acc].mean() if acc.any() else float("nan"),
            "score moyen": score[m].mean(),
        }
    out = pd.DataFrame(rows).T
    return out


def calibration_table(y: np.ndarray, score: np.ndarray, group: np.ndarray, bins: int = 8,
                      names=("A", "B")) -> pd.DataFrame:
    """Fréquence observée de y = 1 par tranche de score et par groupe."""
    edges = np.quantile(score, np.linspace(0, 1, bins + 1))
    edges[0], edges[-1] = -np.inf, np.inf
    idx = np.digitize(score, edges[1:-1])
    rows = []
    for g, name in enumerate(names):
        for b in range(bins):
            m = (group == g) & (idx == b)
            if m.sum() >= 20:
                rows.append({"groupe": name, "tranche": b, "score moyen": score[m].mean(),
                             "taux observé": y[m].mean(), "effectif": int(m.sum())})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------------------
# Boucles de rétroaction
# --------------------------------------------------------------------------------------
def credit_spiral(n: int = 5_000, months: int = 120, screening: bool = True, shock: float = 0.01,
                  p_lose_job: float = 0.01, p_find_job: float = 0.15, screen_penalty: float = 0.25,
                  threshold: float = 600, seed: int = 0) -> dict:
    """Spirale score de crédit / emploi.

    Chaque mois : un coup dur (maladie, accident) frappe avec probabilité `shock` et fait
    perdre l'emploi ; un emploi se perd aussi avec probabilité `p_lose_job`. Un chômeur
    manque ses échéances (score -30 par mois), un employé les honore (+5, plafond 850).
    Un chômeur retrouve un emploi avec probabilité `p_find_job`, multipliée par
    `screen_penalty` si les employeurs filtrent sur le score et que celui-ci est sous
    `threshold`.

    Renvoie un dict : ``unemployment`` (taux mensuel), ``long_term`` (part des chômeurs
    depuis plus de 24 mois), ``scores`` (matrice mois x personnes, échantillon de 200).
    """
    rng = np.random.default_rng(seed)
    score = np.full(n, 720.0)
    employed = np.ones(n, dtype=bool)
    spell = np.zeros(n, dtype=int)
    unemp, longt, trace = [], [], []
    for _ in range(months):
        lose = employed & ((rng.random(n) < shock) | (rng.random(n) < p_lose_job))
        p_find = np.full(n, p_find_job)
        if screening:
            p_find = np.where(score < threshold, p_find_job * screen_penalty, p_find_job)
        find = (~employed) & (rng.random(n) < p_find)
        employed = (employed & ~lose) | find
        spell = np.where(employed, 0, spell + 1)
        score = np.where(employed, np.minimum(850, score + 5), np.maximum(300, score - 30))
        unemp.append(1 - employed.mean())
        longt.append((spell > 24).mean())
        trace.append(score[:200].copy())
    return {"unemployment": np.array(unemp), "long_term": np.array(longt),
            "scores": np.array(trace), "final_score": score, "employed": employed}


def patrol_feedback(true_rates=(1.0, 1.0), init=(11, 10), days: int = 1_000, patrols: int = 10,
                    policy: str = "proportional", detect: float = 0.1, correct: bool = False,
                    seed: int = 0) -> np.ndarray:
    """Allocation de patrouilles guidée par les incidents **enregistrés**.

    Les incidents réels sont identiques en moyenne dans tous les quartiers (`true_rates`),
    mais on n'enregistre que ceux que voit une patrouille : chaque patrouille découvre un
    incident avec probabilité ``detect * taux``.

    policy = "proportional" : patrouilles réparties au prorata des incidents enregistrés ;
    policy = "hotspot" : toutes les patrouilles vont au quartier qui a le plus d'incidents
    enregistrés. correct=True : chaque incident découvert est pondéré par l'inverse de la
    part de patrouilles du quartier (correction de l'échantillonnage).

    Renvoie la part (estimée) d'incidents attribuée à chaque quartier, jour par jour.
    """
    rng = np.random.default_rng(seed)
    rates = np.asarray(true_rates, dtype=float)
    k = len(rates)
    rec = np.asarray(init, dtype=float).copy()
    shares = np.zeros((days, k))
    for d in range(days):
        if policy == "hotspot":
            alloc = np.zeros(k, dtype=int)
            alloc[int(np.argmax(rec))] = patrols
        else:
            alloc = rng.multinomial(patrols, rec / rec.sum())
        found = rng.binomial(alloc, np.clip(detect * rates, 0, 1))
        if correct:
            frac = alloc / patrols
            w = np.where(frac > 0, 1.0 / np.maximum(frac, 1e-12), 0.0) / k
            rec += found * w
        else:
            rec += found
        shares[d] = rec / rec.sum()
    return shares


def recommender_loop(rounds: int = 300, n_topics: int = 8, epsilon: float = 0.0,
                     reinforcement: float = 0.02, seed: int = 0) -> np.ndarray:
    """Recommandeur glouton face à un utilisateur aux goûts presque uniformes.

    À chaque tour, le système montre le thème dont le taux de clic estimé est le plus haut
    (ou un thème au hasard avec probabilité `epsilon`). L'utilisateur clique avec une
    probabilité égale à son intérêt pour le thème ; chaque clic renforce cet intérêt de
    `reinforcement` (l'exposition façonne les goûts). Renvoie un dict : ``pref`` (matrice
    tours x thèmes de l'intérêt de l'utilisateur) et ``shown`` (thème montré à chaque tour).
    """
    rng = np.random.default_rng(seed)
    pref = np.clip(0.3 + 0.02 * rng.normal(size=n_topics), 0.01, 0.99)
    shown = np.ones(n_topics)
    clicks = np.full(n_topics, 0.3)
    hist = np.zeros((rounds, n_topics))
    seq = np.zeros(rounds, dtype=int)
    for t in range(rounds):
        if rng.random() < epsilon:
            j = int(rng.integers(n_topics))
        else:
            j = int(np.argmax(clicks / shown + 1e-9 * rng.random(n_topics)))
        shown[j] += 1
        seq[t] = j
        if rng.random() < pref[j]:
            clicks[j] += 1
            pref[j] = min(0.95, pref[j] + reinforcement)
            others = np.arange(n_topics) != j
            pref[others] = np.maximum(0.02, pref[others] - reinforcement / (n_topics - 1))
        hist[t] = pref
    return {"pref": hist, "shown": seq}


# --------------------------------------------------------------------------------------
# Vie privée
# --------------------------------------------------------------------------------------
def make_population(n: int = 100_000, n_zip: int = 400, seed: int = 0) -> pd.DataFrame:
    """Population synthétique : code postal (tailles très inégales), date de naissance,
    sexe, plus quelques attributs (dont un attribut sensible, ``condition``)."""
    rng = np.random.default_rng(seed)
    sizes = rng.lognormal(mean=0.0, sigma=1.0, size=n_zip)
    zipc = rng.choice(n_zip, size=n, p=sizes / sizes.sum())
    year = rng.integers(1940, 2006, size=n)
    doy = rng.integers(1, 366, size=n)
    sex = rng.integers(0, 2, size=n)
    return pd.DataFrame({
        "zip": 10_000 + zipc, "zip_id": zipc, "year": year, "doy": doy, "sex": sex,
        "birth": year * 1000 + doy,
        "age_band": ((2024 - year) // 10) * 10,
        "runner": (rng.random(n) < 0.15).astype(int),
        "parent": (rng.random(n) < 0.35).astype(int),
        "condition": (rng.random(n) < 0.02).astype(int),
    })


def class_sizes(df: pd.DataFrame, cols) -> pd.Series:
    """Taille de la classe d'équivalence (même valeur des colonnes `cols`) de chaque ligne."""
    return df.groupby(list(cols))[list(cols)[0]].transform("size")


def uniqueness(df: pd.DataFrame, cols) -> float:
    """Fraction des lignes uniques sur la combinaison de colonnes `cols`."""
    return float((class_sizes(df, cols) == 1).mean())


def k_anonymity(df: pd.DataFrame, cols) -> int:
    """Plus petite classe d'équivalence : le jeu de données est k-anonyme pour ce k."""
    return int(class_sizes(df, cols).min())


def generalize(df: pd.DataFrame, zip_digits: int = 5, birth: str = "day") -> pd.DataFrame:
    """Généralise les quasi-identifiants : code postal tronqué (5 à 1 chiffres) et date de
    naissance au jour (« day »), à l'année (« year ») ou à la décennie (« decade »)."""
    out = df.copy()
    out["zip"] = df["zip"] // 10 ** (5 - zip_digits)
    out["birth"] = {"day": df["year"] * 1000 + df["doy"], "year": df["year"],
                    "decade": df["year"] // 10 * 10}[birth]
    return out


def linkage_attack(released: pd.DataFrame, public: pd.DataFrame, cols) -> float:
    """Fraction des lignes publiques qui correspondent à **exactement une** ligne publiée
    (ré-identification certaine par jointure sur les quasi-identifiants `cols`)."""
    cnt = released.groupby(list(cols)).size().rename("n").reset_index()
    m = public.merge(cnt, on=list(cols), how="left")
    return float((m["n"] == 1).mean())


def expected_unique_fraction(pop_per_cell: float, n_combos: float) -> float:
    """Probabilité qu'une personne soit seule de sa combinaison quand `pop_per_cell`
    personnes se répartissent uniformément sur `n_combos` combinaisons."""
    return (1 - 1 / n_combos) ** max(pop_per_cell - 1, 0)


def shadow_profiles(n: int = 5_000, m: int = 4, adoption: float = 0.3, seed: int = 0):
    """Graphe social (Barabási-Albert) ; une fraction `adoption` des personnes utilise le
    service et téléverse son carnet d'adresses. Renvoie (part des non-utilisateurs connus
    du service, nombre moyen de contacts connus par non-utilisateur connu)."""
    import networkx as nx

    g = nx.barabasi_albert_graph(n, m, seed=seed)
    rng = np.random.default_rng(seed)
    users = rng.random(n) < adoption
    known = np.zeros(n, dtype=int)
    for a, b in g.edges():
        if users[a] and not users[b]:
            known[b] += 1
        if users[b] and not users[a]:
            known[a] += 1
    non = ~users
    seen = non & (known > 0)
    frac = seen.sum() / max(non.sum(), 1)
    mean_k = known[seen].mean() if seen.any() else 0.0
    return float(frac), float(mean_k)


def randomized_response(truth: np.ndarray, p: float, rng: np.random.Generator) -> np.ndarray:
    """Chaque répondant dit la vérité avec probabilité p, sinon répond « oui » ou « non »
    à pile ou face."""
    truth = np.asarray(truth)
    honest = rng.random(truth.size) < p
    coin = rng.random(truth.size) < 0.5
    return np.where(honest, truth, coin).astype(int)


def rr_estimate(answers: np.ndarray, p: float) -> float:
    """Estimateur sans biais de la proportion de « oui » réels."""
    lam = float(np.mean(answers))
    return (lam - (1 - p) / 2) / p


def rr_variance(pi: float, p: float, n: int) -> float:
    """Variance de l'estimateur : lambda (1 - lambda) / (n p^2), lambda = p pi + (1-p)/2."""
    lam = p * pi + (1 - p) / 2
    return lam * (1 - lam) / (n * p * p)


def rr_posterior_yes(pi: float, p: float) -> float:
    """P(vrai « oui » | réponse « oui ») : ce que l'enquêteur apprend d'un « oui »."""
    yes_given_yes = p + (1 - p) / 2
    yes_given_no = (1 - p) / 2
    return pi * yes_given_yes / (pi * yes_given_yes + (1 - pi) * yes_given_no)


def breach_exposure(retention_days: float, records_per_day: float = 10_000,
                    breaches_per_year: float = 0.2, years: int = 10, fraction_leaked: float = 1.0,
                    n_runs: int = 2_000, seed: int = 0):
    """Nombre d'enregistrements exposés sur `years` ans quand les fuites arrivent selon un
    processus de Poisson et emportent `fraction_leaked` du stock conservé (stock en régime
    permanent = débit x durée de conservation). Renvoie (moyenne, p95, P(au moins une fuite))."""
    rng = np.random.default_rng(seed)
    stock = records_per_day * retention_days
    k = rng.poisson(breaches_per_year * years, size=n_runs)
    # une personne exposée deux fois ne compte qu'une fois : on borne par le total collecté
    total = records_per_day * 365 * years + stock
    exposed = np.minimum(k * stock * fraction_leaked, total)
    return float(exposed.mean()), float(np.quantile(exposed, 0.95)), float((k > 0).mean())
