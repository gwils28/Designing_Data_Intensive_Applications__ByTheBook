"""Problèmes des systèmes distribués : réseaux, horloges, pauses, baux, fautes byzantines.

Fonctions et classes réutilisables du chapitre « The Trouble with Distributed Systems ».
Tout est déterministe pour une graine donnée.

Organisation
------------
- réseau : délais aller-retour synthétiques, détecteurs de pannes (délai fixe, phi
  accrual), estimateur de délai de retransmission de TCP, file d'un port de commutateur,
  commutation de circuits contre commutation de paquets ;
- horloges : dérive, calcul NTP, algorithme de Marzullo, lissage de la seconde
  intercalaire, horloge calendaire synchronisée par NTP, LWW et décalage, TrueTime ;
- pauses et baux : pauses de ramasse-miettes, service de verrous avec baux, stockage
  protégé par jetons de clôture (simulateur `ddia.sim`), appartenance par battements
  de cœur et vote majoritaire ;
- fautes byzantines : algorithme OM(m) de Lamport, sommes de contrôle ;
- correction : vérification des propriétés d'un générateur de jetons.
"""

from __future__ import annotations

import itertools
import math
import random
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import NormalDist
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .sim import Clock, Network, Node

# =====================================================================================
# 1. Réseau
# =====================================================================================


def partial_failure_probability(p: float, n: int) -> float:
    """Probabilité qu'au moins un des n nœuds touchés par une requête soit en faute."""
    return 1.0 - (1.0 - p) ** n


def rtt_samples(n: int, seed: int = 0, median_ms: float = 0.5, sigma: float = 0.35,
                spike_prob: float = 0.002, spike_scale_ms: float = 20.0,
                spike_alpha: float = 1.3) -> np.ndarray:
    """Délais aller-retour synthétiques (ms) d'un réseau de centre de données.

    Corps log-normal (médiane `median_ms`) et, avec probabilité `spike_prob`, un surcoût
    de Pareto (files d'attente, retransmissions, pause de VM) : la queue est lourde.
    """
    rng = np.random.default_rng(seed)
    body = median_ms * np.exp(sigma * rng.standard_normal(n))
    spikes = rng.random(n) < spike_prob
    extra = spike_scale_ms * (rng.pareto(spike_alpha, n) + 1.0)
    return body + np.where(spikes, extra, 0.0)


def heartbeat_arrivals(n: int, interval: float, delays: np.ndarray) -> np.ndarray:
    """Instants d'arrivée (triés) de n battements émis toutes les `interval` unités."""
    sent = np.arange(n) * interval
    return np.sort(sent + delays[:n])


def fixed_timeout_eval(arrivals: np.ndarray, timeout: float, interval: float,
                       crash_times: Optional[np.ndarray] = None,
                       seed: int = 0) -> Dict[str, float]:
    """Évalue un détecteur « pas de battement depuis `timeout` => suspect ».

    - faux positifs : nombre d'écarts entre arrivées successives supérieurs au délai,
      rapporté au nombre de battements ;
    - délai de détection : pour des pannes à des instants aléatoires, temps entre la panne
      et la suspicion (dernier battement reçu + timeout).
    """
    gaps = np.diff(arrivals)
    fp = float(np.mean(gaps > timeout))
    rng = np.random.default_rng(seed)
    if crash_times is None:
        crash_times = rng.uniform(arrivals[0] + 1, arrivals[-1] - 10 * interval, 2000)
    # dernier battement envoyé avant la panne, et son arrivée (approximation : il arrive)
    idx = np.searchsorted(arrivals, crash_times, side="right") - 1
    last = arrivals[np.clip(idx, 0, None)]
    detect = np.maximum(last + timeout, crash_times) - crash_times
    return {"faux_positifs": fp, "detection_moy": float(detect.mean()),
            "detection_p99": float(np.percentile(detect, 99))}


class PhiAccrualDetector:
    """Détecteur de pannes « phi accrual » (Hayashibara et al., 2004).

    Au lieu de répondre oui/non, il donne un niveau de suspicion continu
    phi = -log10(P(un battement arrive encore plus tard)), en modélisant les intervalles
    entre battements par une loi normale estimée sur une fenêtre glissante. phi = 1 :
    une chance sur 10 de se tromper en déclarant le nœud mort ; phi = 3 : une sur 1 000.
    """

    def __init__(self, window: int = 200, min_std: float = 0.05,
                 first_interval: Optional[float] = None):
        self.window = window
        self.min_std = min_std
        self.intervals: List[float] = [] if first_interval is None else [first_interval]
        self.last: Optional[float] = None

    def heartbeat(self, t: float) -> None:
        if self.last is not None:
            self.intervals.append(t - self.last)
            if len(self.intervals) > self.window:
                self.intervals.pop(0)
        self.last = t

    def stats(self) -> Tuple[float, float]:
        a = np.asarray(self.intervals)
        return float(a.mean()), max(float(a.std()), self.min_std)

    def phi(self, t: float) -> float:
        if self.last is None or not self.intervals:
            return 0.0
        mu, sd = self.stats()
        z = (t - self.last - mu) / (sd * math.sqrt(2.0))
        p_later = 0.5 * math.erfc(z)
        return float(-math.log10(max(p_later, 1e-300)))


def phi_trace(arrivals: Sequence[float], times: Sequence[float], **kw) -> np.ndarray:
    """phi évalué aux instants `times` pour une suite d'arrivées de battements."""
    det = PhiAccrualDetector(**kw)
    out = np.empty(len(times))
    j = 0
    for i, t in enumerate(times):
        while j < len(arrivals) and arrivals[j] <= t:
            det.heartbeat(arrivals[j])
            j += 1
        out[i] = det.phi(t)
    return out


def phi_detector_eval(arrivals: np.ndarray, threshold: float, interval: float,
                      n_crashes: int = 300, seed: int = 0, **kw) -> Dict[str, float]:
    """Faux positifs (par battement) et délai de détection d'un détecteur phi accrual.

    Le détecteur suspecte le nœud dès que phi dépasse `threshold`. Pour chaque intervalle
    entre deux battements, on calcule l'écart à partir duquel phi franchit le seuil, avec
    les statistiques connues à cet instant.
    """
    det = PhiAccrualDetector(**kw)
    fp = 0
    thresholds_gap = []
    for k, t in enumerate(arrivals):
        if det.last is not None and not det.intervals:
            thresholds_gap.append(np.inf)
        elif det.last is not None:
            mu, sd = det.stats()
            # phi(gap) = threshold  <=>  P_later = 10^-threshold
            g = mu + sd * NormalDist().inv_cdf(1 - 10 ** (-threshold))
            thresholds_gap.append(g)
            if t - det.last > g:
                fp += 1
        det.heartbeat(t)
    rng = np.random.default_rng(seed)
    lo = len(arrivals) // 4
    idx = rng.integers(lo, len(arrivals) - 2, n_crashes)
    u = rng.random(n_crashes)
    gaps_thr = np.asarray(thresholds_gap)
    # panne entre l'arrivée idx et l'émission suivante : détection à arrivée + seuil
    crash = arrivals[idx] + u * interval
    detect = arrivals[idx] + gaps_thr[idx] - crash
    return {"faux_positifs": fp / max(len(arrivals) - 1, 1),
            "detection_moy": float(np.maximum(detect, 0).mean())}


def jacobson_rto(samples: Sequence[float], alpha: float = 1 / 8, beta: float = 1 / 4,
                 k: float = 4.0, min_rto: float = 0.0) -> Dict[str, np.ndarray]:
    """Délai de retransmission adaptatif de TCP (Jacobson 1988, RFC 6298).

    SRTT <- (1-alpha) SRTT + alpha R ; RTTVAR <- (1-beta) RTTVAR + beta |SRTT - R| ;
    RTO = SRTT + k RTTVAR.
    """
    s = np.asarray(samples, dtype=float)
    srtt = np.empty_like(s)
    var = np.empty_like(s)
    srtt[0], var[0] = s[0], s[0] / 2
    for i in range(1, s.size):
        var[i] = (1 - beta) * var[i - 1] + beta * abs(srtt[i - 1] - s[i])
        srtt[i] = (1 - alpha) * srtt[i - 1] + alpha * s[i]
    return {"srtt": srtt, "rttvar": var, "rto": np.maximum(srtt + k * var, min_rto)}


def switch_port_sim(n_inputs: int = 3, load: float = 0.8, buffer: int = 64,
                    slots: int = 200_000, mean_burst: float = 8.0,
                    seed: int = 0) -> Dict[str, object]:
    """File d'attente du port de sortie d'un commutateur (temps discret).

    `n_inputs` ports envoient vers un même port de sortie, qui émet un paquet par
    créneau. Chaque entrée est une source on/off (rafales de longueur moyenne
    `mean_burst` créneaux, un paquet par créneau pendant une rafale) dont le débit moyen
    vaut `load / n_inputs`. Le tampon contient `buffer` paquets ; au-delà, perte.

    Renvoie les attentes (en créneaux) des paquets transmis, le taux de perte, la trace
    de la longueur de file.
    """
    rng = np.random.default_rng(seed)
    rate = load / n_inputs
    p_off_on = rate / (mean_burst * (1 - rate)) if rate < 1 else 1.0
    p_on_off = 1.0 / mean_burst
    on = rng.random(n_inputs) < rate
    queue: List[int] = []          # instants d'arrivée des paquets en file (FIFO)
    head = 0
    waits = []
    dropped = sent = 0
    qtrace = np.empty(slots, dtype=np.int32)
    u = rng.random((slots, n_inputs))
    for t in range(slots):
        for i in range(n_inputs):
            if on[i]:
                on[i] = u[t, i] >= p_on_off
            else:
                on[i] = u[t, i] < p_off_on
            if on[i]:
                sent += 1
                if len(queue) - head >= buffer:
                    dropped += 1
                else:
                    queue.append(t)
        if len(queue) > head:
            waits.append(t - queue[head])
            head += 1
        qtrace[t] = len(queue) - head
        if head > 4096:
            queue = queue[head:]
            head = 0
    return {"waits": np.asarray(waits), "drop_rate": dropped / max(sent, 1),
            "queue": qtrace, "offered": sent / slots}
