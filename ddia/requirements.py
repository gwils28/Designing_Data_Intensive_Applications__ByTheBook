"""Exigences non fonctionnelles : charge, performance, fiabilité, scalabilité.

Fonctions réutilisables des notebooks du chapitre « exigences non fonctionnelles »
(2e édition : chapitre 2 ; 1re édition : chapitre 1) :

- **étude de cas des timelines** : graphe social à popularité en loi de puissance,
  coûts du fan-out à la lecture, à l'écriture et hybride, trois implémentations jouets ;
- **performance** : temps de réponse synthétiques, percentiles, histogramme log-linéaire
  à la HdrHistogram (fusionnable), fenêtre glissante, amplification de la latence de queue ;
- **files d'attente** : simulation FIFO à c serveurs, formules M/M/1, M/M/c (Erlang C),
  M/G/1 (Pollaczek-Khinchine) ;
- **fiabilité** : AFR et MTTF, pannes attendues, perte de données d'un groupe de
  répliques (formule et simulation, avec pannes corrélées), disponibilité et « neuf » ;
- **scalabilité** : loi d'Amdahl, Universal Scalability Law et son ajustement.

Tout est déterministe pour une graine donnée.
"""

from __future__ import annotations

import heapq
import math
from collections import deque
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from .sim import Network, Node

HOURS_PER_YEAR = 8760.0

# =====================================================================================
# 1. Étude de cas : timelines d'un réseau social
# =====================================================================================


def social_graph(n_users: int, mean_follows: float = 50.0, alpha: float = 1.2,
                 seed: int = 0) -> Tuple[np.ndarray, np.ndarray]:
    """Graphe « qui suit qui » réaliste : la popularité suit une loi de puissance.

    Chaque utilisateur u reçoit un poids de popularité w_u (loi de Pareto d'indice
    `alpha` : quelques célébrités, une immense majorité d'inconnus) ; il suit k_u comptes
    (k_u log-normal de moyenne ~ `mean_follows`) tirés proportionnellement à w.

    Renvoie deux tableaux de même longueur (follower, followee) : l'arête i signifie
    « follower[i] suit followee[i] ». Pas de boucle ni de doublon.
    """
    rng = np.random.default_rng(seed)
    w = rng.pareto(alpha, n_users) + 1.0
    w /= w.sum()
    sigma = 0.8
    k = rng.lognormal(math.log(mean_follows) - sigma ** 2 / 2, sigma, n_users)
    k = np.clip(np.round(k).astype(int), 1, n_users - 1)
    follower = np.repeat(np.arange(n_users), k)
    followee = rng.choice(n_users, size=follower.size, p=w)
    keep = follower != followee
    code = np.unique(follower[keep].astype(np.int64) * n_users + followee[keep])
    return (code // n_users).astype(np.int64), (code % n_users).astype(np.int64)


def degree_counts(follower: np.ndarray, followee: np.ndarray, n_users: int) -> Tuple[np.ndarray, np.ndarray]:
    """(nombre d'abonnés, nombre d'abonnements) de chaque utilisateur."""
    return (np.bincount(followee, minlength=n_users), np.bincount(follower, minlength=n_users))


def fanout_costs(follower: np.ndarray, followee: np.ndarray, n_users: int,
                 post_rate, read_rate, threshold: Optional[float] = None) -> Dict[str, float]:
    """Coût (opérations élémentaires par seconde) des trois stratégies de timeline.

    post_rate, read_rate : taux de publication et de lecture de timeline par utilisateur
    (scalaire ou tableau de taille n_users), en événements par seconde.

    - « lecture » (fan-out à la lecture, requête SQL) : 1 écriture par message ; une
      lecture de timeline consulte les messages récents de chaque compte suivi ;
    - « écriture » (timelines matérialisées) : un message est inséré dans la timeline de
      chacun de ses abonnés ; une lecture coûte 1 accès au cache ;
    - « hybride » (si `threshold` est donné) : les comptes ayant au moins `threshold`
      abonnés (célébrités) ne sont pas diffusés ; leurs messages sont fusionnés à la
      lecture (1 accès au cache + 1 par célébrité suivie).
    """
    n_followers, n_following = degree_counts(follower, followee, n_users)
    p = np.broadcast_to(np.asarray(post_rate, dtype=float), (n_users,))
    r = np.broadcast_to(np.asarray(read_rate, dtype=float), (n_users,))
    out = {
        "lecture_ecritures": float(p.sum()),
        "lecture_lectures": float((r * n_following).sum()),
        "ecriture_ecritures": float((p * n_followers).sum()),
        "ecriture_lectures": float(r.sum()),
    }
    if threshold is not None:
        celeb = n_followers >= threshold
        celeb_followed = np.bincount(follower[celeb[followee]], minlength=n_users)
        out["hybride_ecritures"] = float((p * np.where(celeb, 1, n_followers)).sum())
        out["hybride_lectures"] = float((r * (1 + celeb_followed)).sum())
        out["n_celebrites"] = int(celeb.sum())
    for s in ("lecture", "ecriture", "hybride"):
        if f"{s}_ecritures" in out:
            out[f"{s}_total"] = out[f"{s}_ecritures"] + out[f"{s}_lectures"]
    return out


class _Timelines:
    """Base commune : compteurs de travail et adjacences."""

    def __init__(self, follower: Sequence[int], followee: Sequence[int], n_users: int, k: int = 50):
        self.k = k
        self.followers: List[List[int]] = [[] for _ in range(n_users)]
        self.following: List[List[int]] = [[] for _ in range(n_users)]
        for a, b in zip(follower, followee):
            self.followers[int(b)].append(int(a))
            self.following[int(a)].append(int(b))
        self.posts: List[deque] = [deque(maxlen=k) for _ in range(n_users)]
        self.write_ops = 0   # écritures élémentaires (insertion d'un message quelque part)
        self.read_ops = 0    # lectures élémentaires (consultation d'une liste)

    def post(self, user: int, t: float, text: str = "") -> None:
        raise NotImplementedError

    def timeline(self, user: int) -> List[Tuple[float, int, str]]:
        raise NotImplementedError


class PullTimelines(_Timelines):
    """Fan-out à la lecture : on stocke chaque message une fois, on fusionne à la lecture."""

    def post(self, user, t, text=""):
        self.posts[user].appendleft((t, user, text))
        self.write_ops += 1

    def timeline(self, user):
        lists = []
        for f in self.following[user]:
            lists.append(self.posts[f])
            self.read_ops += 1
        return heapq.nlargest(self.k, (m for lst in lists for m in lst))


class PushTimelines(_Timelines):
    """Fan-out à l'écriture : chaque message est livré dans la boîte de chaque abonné."""

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.cache: List[deque] = [deque(maxlen=self.k) for _ in range(len(self.posts))]

    def post(self, user, t, text=""):
        self.posts[user].appendleft((t, user, text))
        self.write_ops += 1
        for f in self.followers[user]:
            self.cache[f].appendleft((t, user, text))
            self.write_ops += 1

    def timeline(self, user):
        self.read_ops += 1
        return list(self.cache[user])


class HybridTimelines(PushTimelines):
    """Hybride : diffusion pour les comptes ordinaires, fusion à la lecture pour les célébrités."""

    def __init__(self, follower, followee, n_users, k=50, threshold=1000):
        super().__init__(follower, followee, n_users, k=k)
        self.celebrity = [len(fs) >= threshold for fs in self.followers]
        self.celebs_followed = [[f for f in fl if self.celebrity[f]] for fl in self.following]

    def post(self, user, t, text=""):
        if self.celebrity[user]:
            self.posts[user].appendleft((t, user, text))
            self.write_ops += 1
        else:
            super().post(user, t, text)

    def timeline(self, user):
        self.read_ops += 1
        parts = [self.cache[user]]
        for c in self.celebs_followed[user]:
            parts.append(self.posts[c])
            self.read_ops += 1
        return heapq.nlargest(self.k, (m for lst in parts for m in lst))


# =====================================================================================
# 2. Performance : temps de réponse et percentiles
# =====================================================================================


def synthetic_response_times(n: int, seed: int = 0, median_ms: float = 40.0, sigma: float = 0.45,
                             p_slow: float = 0.015, slow_ms: Tuple[float, float] = (300.0, 1500.0)) -> np.ndarray:
    """Temps de réponse (ms) réalistes : corps log-normal + petite fraction de requêtes lentes.

    Les requêtes lentes modélisent les aléas du livre (pause du ramasse-miettes,
    retransmission TCP, défaut de page...) : avec probabilité `p_slow`, on ajoute un délai
    uniforme dans `slow_ms`.
    """
    rng = np.random.default_rng(seed)
    x = rng.lognormal(math.log(median_ms), sigma, n)
    slow = rng.random(n) < p_slow
    x[slow] += rng.uniform(slow_ms[0], slow_ms[1], slow.sum())
    return x


def percentile(values: Iterable[float], q: float) -> float:
    """Percentile par la méthode du rang le plus proche (nearest rank) : la plus petite
    valeur v telle qu'au moins q % des observations soient <= v."""
    v = np.sort(np.asarray(list(values) if not isinstance(values, np.ndarray) else values, dtype=float))
    if v.size == 0:
        raise ValueError("aucune valeur")
    rank = max(1, int(math.ceil(q / 100.0 * v.size)))
    return float(v[min(rank, v.size) - 1])


class LogLinearHistogram:
    """Histogramme log-linéaire à la HdrHistogram (version simplifiée).

    Chaque puissance de deux [2^e, 2^(e+1)) est découpée en 2^sub_bits seaux de même
    largeur : l'erreur relative sur une valeur est donc bornée par 2^-sub_bits (et par
    2^-(sub_bits+1) en restituant le milieu du seau). La mémoire ne dépend que de la plage
    dynamique, pas du nombre de valeurs. Deux histogrammes de même paramétrage se
    **fusionnent** exactement en additionnant leurs compteurs.

    unit : résolution (ex. 1e-3 si les valeurs sont en ms et qu'on veut la µs).
    """

    def __init__(self, sub_bits: int = 5, unit: float = 1e-3):
        self.sub_bits = sub_bits
        self.unit = unit
        self.counts: Dict[int, int] = {}
        self.total = 0
        self._sum = 0.0

    # --- indexation -------------------------------------------------------------
    def _index(self, x: np.ndarray) -> np.ndarray:
        s = 1 << self.sub_bits
        u = np.maximum(np.asarray(x, dtype=float) / self.unit, 1.0)
        e = np.floor(np.log2(u)).astype(np.int64)
        frac = u / np.exp2(e) - 1.0
        sub = np.minimum((frac * s).astype(np.int64), s - 1)
        return e * s + sub

    def bucket_bounds(self, idx: int) -> Tuple[float, float]:
        s = 1 << self.sub_bits
        e, sub = divmod(int(idx), s)
        lo = 2.0 ** e * (1 + sub / s)
        hi = 2.0 ** e * (1 + (sub + 1) / s)
        return lo * self.unit, hi * self.unit

    def bucket_value(self, idx: int) -> float:
        lo, hi = self.bucket_bounds(idx)
        return (lo + hi) / 2

    # --- enregistrement ---------------------------------------------------------
    def record(self, value: float, count: int = 1) -> None:
        i = int(self._index(np.array([value]))[0])
        self.counts[i] = self.counts.get(i, 0) + count
        self.total += count
        self._sum += value * count

    def record_many(self, values) -> None:
        values = np.asarray(values, dtype=float)
        if values.size == 0:
            return
        idx, cnt = np.unique(self._index(values), return_counts=True)
        for i, c in zip(idx.tolist(), cnt.tolist()):
            self.counts[i] = self.counts.get(i, 0) + c
        self.total += int(values.size)
        self._sum += float(values.sum())

    def merge(self, other: "LogLinearHistogram") -> "LogLinearHistogram":
        """Ajoute les compteurs de `other` (même sub_bits et unit) à cet histogramme."""
        if (other.sub_bits, other.unit) != (self.sub_bits, self.unit):
            raise ValueError("histogrammes de paramétrages différents")
        for i, c in other.counts.items():
            self.counts[i] = self.counts.get(i, 0) + c
        self.total += other.total
        self._sum += other._sum
        return self

    @classmethod
    def merged(cls, hists: Sequence["LogLinearHistogram"]) -> "LogLinearHistogram":
        h0 = hists[0]
        out = cls(h0.sub_bits, h0.unit)
        for h in hists:
            out.merge(h)
        return out

    # --- requêtes ---------------------------------------------------------------
    def percentile(self, q: float) -> float:
        if self.total == 0:
            raise ValueError("histogramme vide")
        target = max(1, int(math.ceil(q / 100.0 * self.total)))
        acc = 0
        for i in sorted(self.counts):
            acc += self.counts[i]
            if acc >= target:
                return self.bucket_value(i)
        return self.bucket_value(max(self.counts))

    def mean(self) -> float:
        return self._sum / self.total

    @property
    def n_buckets(self) -> int:
        return len(self.counts)

    def max_relative_error(self) -> float:
        """Borne de l'erreur relative en restituant le milieu des seaux."""
        return 2.0 ** -(self.sub_bits + 1)


class RollingPercentiles:
    """Percentiles sur une fenêtre glissante : un histogramme par tranche de temps.

    `add(t, v)` range v dans la tranche floor(t / slot) ; `percentile(t, q)` fusionne les
    `window` dernières tranches (celle de t comprise). Les tranches trop vieilles sont
    oubliées : la mémoire reste bornée.
    """

    def __init__(self, window: int = 10, slot: float = 60.0, sub_bits: int = 5, unit: float = 1e-3):
        self.window, self.slot = window, slot
        self.sub_bits, self.unit = sub_bits, unit
        self.slots: Dict[int, LogLinearHistogram] = {}

    def add_many(self, t, values) -> None:
        t = np.asarray(t, dtype=float)
        values = np.asarray(values, dtype=float)
        k = np.floor(t / self.slot).astype(int)
        for s in np.unique(k):
            h = self.slots.setdefault(int(s), LogLinearHistogram(self.sub_bits, self.unit))
            h.record_many(values[k == s])

    def add(self, t: float, v: float) -> None:
        self.add_many([t], [v])

    def percentile(self, t: float, q: float) -> float:
        now = int(math.floor(t / self.slot))
        for s in [s for s in self.slots if s <= now - self.window]:
            del self.slots[s]
        hs = [h for s, h in self.slots.items() if now - self.window < s <= now]
        return LogLinearHistogram.merged(hs).percentile(q)


def p_any_slow(p: float, n) -> np.ndarray:
    """Probabilité qu'au moins un de n appels indépendants soit lent : 1 - (1 - p)^n."""
    return 1.0 - (1.0 - p) ** np.asarray(n, dtype=float)


def fanout_response_times(samples: np.ndarray, n: int, n_requests: int, seed: int = 0) -> np.ndarray:
    """Temps de réponse d'une requête qui attend n appels parallèles : max de n tirages."""
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, samples.size, size=(n_requests, n))
    return samples[idx].max(axis=1)


def per_call_quantile_for(q_global: float, n: int) -> float:
    """Quantile (en fraction) qu'un appel doit garantir pour que la requête qui en
    attend n respecte le quantile global : q_appel = q_global^(1/n)."""
    return q_global ** (1.0 / n)


# =====================================================================================
# 3. Files d'attente
# =====================================================================================


def poisson_arrivals(rate: float, n: int, seed: int = 0, start: float = 0.0) -> np.ndarray:
    """n instants d'arrivée d'un processus de Poisson de taux `rate`."""
    rng = np.random.default_rng(seed)
    return start + np.cumsum(rng.exponential(1.0 / rate, n))


def simulate_fifo(arrivals: np.ndarray, services: np.ndarray, c: int = 1) -> Tuple[np.ndarray, np.ndarray]:
    """File FIFO à c serveurs identiques. Renvoie (début de service, fin de service).

    Pour c = 1, c'est la récurrence de Lindley : début_i = max(arrivée_i, fin_{i-1}).
    Temps de réponse = fin - arrivée ; attente en file = début - arrivée.
    Les requêtes sont servies dans l'ordre des arrivées (les tableaux n'ont pas besoin
    d'être triés ; les résultats sont rendus dans l'ordre d'origine).
    """
    arrivals = np.asarray(arrivals, dtype=float)
    services = np.asarray(services, dtype=float)
    if arrivals.size > 1 and np.any(np.diff(arrivals) < 0):
        order = np.argsort(arrivals, kind="stable")
        st, en = simulate_fifo(arrivals[order], services[order], c)
        start, end = np.empty_like(st), np.empty_like(en)
        start[order], end[order] = st, en
        return start, end
    n = arrivals.size
    start = np.empty(n)
    end = np.empty(n)
    if c == 1:
        free = -math.inf
        for i in range(n):
            s = arrivals[i] if arrivals[i] > free else free
            start[i] = s
            free = s + services[i]
            end[i] = free
        return start, end
    servers = [0.0] * c
    heapq.heapify(servers)
    for i in range(n):
        free = heapq.heappop(servers)
        s = max(arrivals[i], free)
        start[i] = s
        end[i] = s + services[i]
        heapq.heappush(servers, end[i])
    return start, end


def mm1(lam: float, mu: float) -> Dict[str, float]:
    """File M/M/1 (arrivées de Poisson lam, service exponentiel mu, 1 serveur).

    rho = lam/mu ; nombre moyen dans le système L = rho/(1-rho) ; temps de réponse moyen
    W = 1/(mu-lam) ; attente moyenne en file Wq = rho/(mu-lam). Loi de Little : L = lam W.
    """
    if lam >= mu:
        return {"rho": lam / mu, "L": math.inf, "W": math.inf, "Wq": math.inf, "Lq": math.inf}
    rho = lam / mu
    W = 1.0 / (mu - lam)
    return {"rho": rho, "L": rho / (1 - rho), "W": W, "Wq": rho * W, "Lq": rho ** 2 / (1 - rho)}


def mm1_response_quantile(lam: float, mu: float, q: float) -> float:
    """Quantile q (fraction) du temps de réponse M/M/1 : loi exponentielle de taux mu - lam."""
    if lam >= mu:
        return math.inf
    return -math.log(1.0 - q) / (mu - lam)


def erlang_c(c: int, a: float) -> float:
    """Probabilité d'attendre dans une file M/M/c de charge offerte a = lam/mu (Erlang C)."""
    if a >= c:
        return 1.0
    b = 1.0
    for k in range(1, c + 1):
        b = a * b / (k + a * b)      # récurrence stable d'Erlang B
    return c * b / (c - a * (1 - b))


def mmc(lam: float, mu: float, c: int) -> Dict[str, float]:
    """File M/M/c : probabilité d'attente, attente moyenne, temps de réponse moyen."""
    a = lam / mu
    if a >= c:
        return {"rho": a / c, "P_attente": 1.0, "Wq": math.inf, "W": math.inf}
    pw = erlang_c(c, a)
    wq = pw / (c * mu - lam)
    return {"rho": a / c, "P_attente": pw, "Wq": wq, "W": wq + 1.0 / mu}


def mg1_wait(lam: float, mean_s: float, scv: float) -> float:
    """Attente moyenne en file M/G/1 (Pollaczek-Khinchine).

    scv : carré du coefficient de variation du temps de service (1 pour l'exponentielle,
    0 pour un service constant). Wq = lam E[S^2] / (2 (1 - rho)).
    """
    rho = lam * mean_s
    if rho >= 1:
        return math.inf
    es2 = mean_s ** 2 * (1.0 + scv)
    return lam * es2 / (2.0 * (1.0 - rho))


# =====================================================================================
# 4. Fiabilité
# =====================================================================================


def mttf_hours_from_afr(afr: float) -> float:
    """MTTF (heures) d'un composant à taux de panne constant ayant ce taux annuel (AFR)."""
    return -HOURS_PER_YEAR / math.log(1.0 - afr)


def afr_from_mttf_hours(mttf: float) -> float:
    """Taux annuel de panne (AFR) d'un composant de MTTF donné (heures)."""
    return 1.0 - math.exp(-HOURS_PER_YEAR / mttf)


def expected_failures(n: int, afr: float, days: float = 1.0) -> float:
    """Nombre moyen de pannes dans un parc de n composants pendant `days` jours."""
    lam = -math.log(1.0 - afr) / 365.0
    return n * lam * days


def poisson_pmf(k, mean: float) -> np.ndarray:
    k = np.asarray(k)
    logp = -mean + k * np.log(mean) - np.array([math.lgamma(int(x) + 1) for x in np.atleast_1d(k)])
    return np.exp(logp).reshape(k.shape)


def mttdl_hours(afr: float, r: int, rebuild_hours: float) -> float:
    """Durée moyenne avant perte de données d'un groupe de r copies (pannes indépendantes).

    Approximation (reconstruction courte devant le MTTF) : il y a perte quand les r
    copies sont en panne en même temps, c'est-à-dire quand, après une première panne
    (taux r lam), chacune des r - 1 autres copies tombe dans la fenêtre T qui suit
    (probabilité ~ lam T chacune). D'où un taux r lam^r T^(r-1) et
    MTTDL = 1 / (r lam^r T^(r-1)). Pour r = 2 : MTTF^2 / (2 T), la formule du RAID 1.
    """
    lam = 1.0 / mttf_hours_from_afr(afr)
    return 1.0 / (r * lam ** r * rebuild_hours ** (r - 1))


def p_loss_per_year(afr: float, r: int, rebuild_hours: float, n_groups: int = 1) -> float:
    """Probabilité de perdre au moins un groupe (sur n_groups) en un an."""
    rate = n_groups * HOURS_PER_YEAR / mttdl_hours(afr, r, rebuild_hours)
    return 1.0 - math.exp(-rate)


def simulate_data_loss(r: int, afr: float, rebuild_hours: float, years: float, n_groups: int,
                       seed: int = 0, shock_per_year: float = 0.0) -> np.ndarray:
    """Simulation Monte-Carlo de groupes de r copies (une par disque).

    Chaque disque sain tombe en panne au taux constant lam (déduit de l'AFR) ; un disque
    en panne est remplacé et reconstruit en `rebuild_hours`. `shock_per_year` ajoute des
    pannes **corrélées** : à ce taux, un choc commun (rack, alimentation, firmware...)
    détruit toutes les copies du groupe d'un coup.

    Renvoie, pour chaque groupe, l'instant (heures) de la première perte de données, ou
    inf s'il n'y en a pas eu sur l'horizon.
    """
    rng = np.random.default_rng(seed)
    lam = 1.0 / mttf_hours_from_afr(afr)
    shock = shock_per_year / HOURS_PER_YEAR
    horizon = years * HOURS_PER_YEAR
    out = np.full(n_groups, np.inf)
    for g in range(n_groups):
        t = 0.0
        repairs: List[float] = []        # instants de fin de reconstruction des disques en panne
        while True:
            healthy = r - len(repairs)
            rate = healthy * lam + shock
            t_fail = t + rng.exponential(1.0 / rate) if rate > 0 else math.inf
            t_rep = repairs[0] if repairs else math.inf
            if min(t_fail, t_rep) > horizon:
                break
            if t_rep <= t_fail:
                t = heapq.heappop(repairs)
                continue
            t = t_fail
            if rng.random() < shock / rate:           # choc commun : tout est perdu
                out[g] = t
                break
            heapq.heappush(repairs, t + rebuild_hours)
            if len(repairs) == r:
                out[g] = t
                break
    return out


def nines(availability: float) -> float:
    """Nombre de « neuf » : 0,999 -> 3."""
    return -math.log10(1.0 - availability)


def downtime_minutes(availability: float, period_days: float = 365.0) -> float:
    """Indisponibilité tolérée (minutes) sur la période pour une disponibilité donnée."""
    return (1.0 - availability) * period_days * 24 * 60


def series_availability(*a: float) -> float:
    """Disponibilité de composants **tous nécessaires** (en série) : produit."""
    return float(np.prod(a))


def parallel_availability(*a: float) -> float:
    """Disponibilité de composants redondants (un seul suffit), pannes indépendantes."""
    return 1.0 - float(np.prod([1.0 - x for x in a]))


def k_of_n_availability(a: float, k: int, n: int) -> float:
    """Disponibilité d'un système qui fonctionne si au moins k des n répliques (de
    disponibilité a, indépendantes) fonctionnent."""
    return float(sum(math.comb(n, i) * a ** i * (1 - a) ** (n - i) for i in range(k, n + 1)))


class TimelineStore(Node):
    """Stockage des timelines matérialisées : reçoit des ("insert", post, abonné).

    idempotent=True : une insertion déjà vue est ignorée (clé (post, abonné)), ce qui
    rend les livraisons répétées inoffensives.
    """

    def __init__(self, name: str = "timelines", idempotent: bool = False):
        super().__init__(name)
        self.idempotent = idempotent
        self.rows: List[Tuple[int, int]] = []
        self._seen = set()

    def on_message(self, src, msg):
        if msg[0] == "insert":
            key = (msg[1], msg[2])
            if self.idempotent and key in self._seen:
                return
            self._seen.add(key)
            self.rows.append(key)


class FanoutWorker(Node):
    """Travailleur de fan-out : livre un message à ses abonnés, un par `step`, puis acquitte."""

    def __init__(self, name: str, store: str = "timelines", step: float = 0.25):
        super().__init__(name)
        self.store, self.step = store, step
        self.epoch = 0                       # invalide les minuteries d'avant une panne

    def on_message(self, src, msg):
        if msg[0] == "task":
            _, task_id, post, followers = msg
            self._deliver(src, task_id, post, list(followers), 0, self.epoch)

    def _deliver(self, coord, task_id, post, followers, i, epoch):
        if epoch != self.epoch:
            return
        if i == len(followers):
            self.send(coord, ("done", task_id))
            return
        self.send(self.store, ("insert", post, followers[i]))
        self.set_timer(self.step, self._deliver, coord, task_id, post, followers, i + 1, epoch)

    def on_crash(self):
        self.epoch += 1                      # le travail en cours est perdu


class FanoutCoordinator(Node):
    """Distribue les tâches de fan-out ; si `timeout` est donné, réattribue une tâche non
    acquittée à un autre travailleur (sinon, une tâche confiée à un travailleur en panne
    est perdue)."""

    def __init__(self, name: str, tasks: Dict[int, Tuple[int, List[int]]], workers: List[str],
                 timeout: Optional[float] = None):
        super().__init__(name)
        self.tasks, self.workers, self.timeout = tasks, workers, timeout
        self.done: set = set()
        self.assignments: List[Tuple[float, int, str]] = []

    def on_start(self):
        for k, task_id in enumerate(sorted(self.tasks)):
            self._assign(task_id, k % len(self.workers))

    def _assign(self, task_id, w):
        worker = self.workers[w % len(self.workers)]
        post, followers = self.tasks[task_id]
        self.assignments.append((self.now, task_id, worker))
        self.send(worker, ("task", task_id, post, followers))
        if self.timeout is not None:
            self.set_timer(self.timeout, self._check, task_id, w)

    def _check(self, task_id, w):
        if task_id not in self.done:
            self._assign(task_id, w + 1)

    def on_message(self, src, msg):
        if msg[0] == "done":
            self.done.add(msg[1])


def fanout_fault_run(variant: str, crash_at: Optional[float] = None, crash_node: str = "w1",
                     n_posts: int = 4, n_followers: int = 5, seed: int = 0, recover_after: float = 3.0,
                     until: float = 60.0):
    """Rejoue un fan-out avec une panne de travailleur injectée.

    variant : "naif" (pas de réattribution), "relance" (réattribution après timeout, stockage
    non idempotent) ou "idempotent" (réattribution + stockage idempotent).
    Renvoie (réseau, stockage, verdict) où verdict compte les livraisons perdues et dupliquées.
    """
    net = Network(seed=seed, latency=(0.1, 0.3))
    tasks = {t: (100 + t, [1000 * (t + 1) + j for j in range(n_followers)]) for t in range(n_posts)}
    store = net.add(TimelineStore(idempotent=(variant == "idempotent")))
    workers = ["w1", "w2"]
    for w in workers:
        net.add(FanoutWorker(w))
    net.add(FanoutCoordinator("coordinateur", tasks, workers, timeout=None if variant == "naif" else 4.0))
    if crash_at is not None:
        net.at(crash_at, net.crash, crash_node)
        net.at(crash_at + recover_after, net.recover, crash_node)
    net.run(until=until)
    expected = {(post, f) for post, fl in tasks.values() for f in fl}
    counts: Dict[Tuple[int, int], int] = {}
    for key in store.rows:
        counts[key] = counts.get(key, 0) + 1
    verdict = {"perdues": sum(1 for k in expected if k not in counts),
               "dupliquees": sum(c - 1 for c in counts.values() if c > 1)}
    return net, store, verdict


# =====================================================================================
# 5. Scalabilité
# =====================================================================================


def amdahl(n, p: float) -> np.ndarray:
    """Accélération d'Amdahl sur n processeurs quand une fraction p du travail est parallèle."""
    n = np.asarray(n, dtype=float)
    return 1.0 / ((1.0 - p) + p / n)


def usl(n, sigma: float, kappa: float, lam: float = 1.0) -> np.ndarray:
    """Universal Scalability Law (Gunther) : débit X(n) = lam n / (1 + sigma (n-1) + kappa n (n-1)).

    sigma : contention (part sérialisée, comme Amdahl) ; kappa : cohérence (coût des
    échanges entre nœuds, qui croît en n^2 et fait **baisser** le débit au-delà d'un pic).
    """
    n = np.asarray(n, dtype=float)
    return lam * n / (1.0 + sigma * (n - 1.0) + kappa * n * (n - 1.0))


def fit_usl(n, x) -> Tuple[float, float, float]:
    """Ajuste (lam, sigma, kappa) de l'USL sur des mesures de débit x(n).

    lam est pris au plus petit n mesuré (x(n0) / n0, exact si n0 = 1) ; ensuite
    n lam / x - 1 = sigma (n - 1) + kappa n (n - 1) est linéaire en (sigma, kappa) :
    moindres carrés sans constante, coefficients contraints positifs.
    """
    n = np.asarray(n, dtype=float)
    x = np.asarray(x, dtype=float)
    i0 = int(np.argmin(n))
    lam = x[i0] / n[i0]
    y = n * lam / x - 1.0
    A = np.column_stack([n - 1.0, n * (n - 1.0)])
    coef, *_ = np.linalg.lstsq(A, y, rcond=None)
    sigma, kappa = (max(0.0, float(c)) for c in coef)
    return float(lam), sigma, kappa


def usl_peak(sigma: float, kappa: float) -> float:
    """Nombre de nœuds qui maximise le débit USL : sqrt((1 - sigma) / kappa)."""
    if kappa <= 0:
        return math.inf
    return math.sqrt((1.0 - sigma) / kappa)


# =====================================================================================
# 6. Petit utilitaire graphique
# =====================================================================================


def human_number(v: float) -> str:
    """Écriture lisible d'un nombre positif pour une graduation : 1e-6 ; 0,001 ; 50 ; 10 k ; 2 M."""
    if v <= 0:
        return "0"
    for div, suf in ((1e9, " G"), (1e6, " M"), (1e4, " k")):
        if v >= div:
            x = v / (div if suf != " k" else 1e3)
            return f"{x:g}{suf}".replace(".", ",")
    if v >= 1:
        return f"{v:g}"
    if v < 1e-3:
        m, e = f"{v:.0e}".split("e")
        return f"{m}e-{int(e[1:])}" if e.startswith("-") else f"{m}e{int(e)}"
    return f"{v:.10f}".rstrip("0").replace(".", ",")


def plain_log_axis(ax, which: str = "both") -> None:
    """Graduations logarithmiques écrites en clair (0,01 ; 1 ; 10 k) au lieu de 10^n.

    Évite aussi le signe moins typographique des exposants négatifs, absent de certaines
    polices installées.
    """
    from matplotlib.ticker import FuncFormatter, NullFormatter

    axes = {"x": [ax.xaxis], "y": [ax.yaxis], "both": [ax.xaxis, ax.yaxis]}[which]
    for axis in axes:
        if axis.get_scale() == "log":
            axis.set_major_formatter(FuncFormatter(lambda v, _: human_number(v)))
            axis.set_minor_formatter(NullFormatter())
