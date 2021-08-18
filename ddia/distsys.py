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
