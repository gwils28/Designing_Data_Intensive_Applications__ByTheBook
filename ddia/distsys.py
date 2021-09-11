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


def fifo_departures(arrivals: np.ndarray, service: np.ndarray) -> np.ndarray:
    """Instants de fin de service d'une file FIFO à un serveur (récurrence de Lindley)."""
    out = np.empty_like(arrivals, dtype=float)
    free = -np.inf
    for i, (a, s) in enumerate(zip(arrivals, service)):
        start = a if a > free else free
        free = start + s
        out[i] = free
    return out


def circuit_vs_packet(n_users: int = 10, load: float = 0.3, n_jobs: int = 20_000,
                      capacity: float = 1.0, mean_size: float = 1.0,
                      seed: int = 0) -> Dict[str, np.ndarray]:
    """Transferts de fichiers en rafales : circuits statiques contre partage dynamique.

    Chaque utilisateur émet des transferts (tailles exponentielles) selon un processus de
    Poisson, la charge totale valant `load` de la capacité du lien.

    - circuits : le lien est découpé en `n_users` circuits de débit capacity / n_users,
      chaque utilisateur a sa propre file ;
    - paquets : une seule file FIFO au débit `capacity` (partage dynamique).

    Renvoie les durées de transfert (attente + émission) dans les deux cas.
    """
    rng = np.random.default_rng(seed)
    lam_user = load * capacity / mean_size / n_users
    users = rng.integers(0, n_users, n_jobs)
    arrivals = np.cumsum(rng.exponential(1 / (lam_user * n_users), n_jobs))
    sizes = rng.exponential(mean_size, n_jobs)
    packet = fifo_departures(arrivals, sizes / capacity) - arrivals
    circuit = np.empty(n_jobs)
    for u in range(n_users):
        m = users == u
        circuit[m] = fifo_departures(arrivals[m], sizes[m] / (capacity / n_users)) - arrivals[m]
    return {"circuit": circuit, "paquets": packet, "sizes": sizes}


def voice_over_packets(load_cross: float, n_frames: int = 20_000, frame_size: float = 0.05,
                       seed: int = 0) -> np.ndarray:
    """Délai subi par un flux audio régulier qui partage une file avec du trafic en rafales.

    Le flux audio émet une trame de taille `frame_size` à chaque unité de temps ; le trafic
    concurrent (Poisson, tailles exponentielles de moyenne 1) occupe `load_cross` du lien.
    Sur un circuit réservé, le délai serait constant (frame_size).
    """
    rng = np.random.default_rng(seed)
    t_voice = np.arange(n_frames, dtype=float)
    n_cross = rng.poisson(load_cross * n_frames)
    t_cross = np.sort(rng.uniform(0, n_frames, n_cross))
    arr = np.concatenate([t_voice, t_cross])
    svc = np.concatenate([np.full(n_frames, frame_size), rng.exponential(1.0, n_cross)])
    is_voice = np.concatenate([np.ones(n_frames, bool), np.zeros(n_cross, bool)])
    order = np.argsort(arr, kind="stable")
    dep = fifo_departures(arr[order], svc[order])
    delay = dep - arr[order]
    return delay[is_voice[order]]


def drift_error(ppm: float, seconds: float) -> float:
    """Écart accumulé (en secondes) par une horloge qui dérive de `ppm` pendant `seconds`."""
    return ppm * 1e-6 * seconds


def ntp_offset_delay(t0: float, t1: float, t2: float, t3: float) -> Tuple[float, float]:
    """Calcul NTP à partir des quatre horodatages d'un échange.

    t0 : émission client (horloge client) ; t1 : réception serveur ; t2 : émission
    serveur (horloge serveur) ; t3 : réception client (horloge client).
    Renvoie (décalage estimé serveur - client, délai aller-retour réseau).
    """
    offset = ((t1 - t0) + (t2 - t3)) / 2
    delay = (t3 - t0) - (t2 - t1)
    return offset, delay


def ntp_exchange(true_offset: float, d_out: float, d_back: float, t0: float = 0.0,
                 server_time: float = 0.0) -> Tuple[float, float, float]:
    """Simule un échange NTP ; renvoie (décalage estimé, délai, erreur d'estimation).

    true_offset = horloge serveur - horloge client. L'erreur vaut (d_out - d_back) / 2 :
    elle est nulle si le réseau est symétrique et bornée par délai / 2.
    """
    t1 = t0 + d_out + true_offset
    t2 = t1 + server_time
    t3 = t2 - true_offset + d_back
    off, delay = ntp_offset_delay(t0, t1, t2, t3)
    return off, delay, off - true_offset


def marzullo(intervals: Sequence[Tuple[float, float]]) -> Tuple[int, float, float]:
    """Algorithme de Marzullo : le plus petit intervalle compatible avec le maximum de sources.

    intervals : liste de (bas, haut) annoncés par chaque source (« l'heure est dans
    [bas, haut] »). Renvoie (nombre de sources d'accord, bas, haut).
    """
    edges = []
    for lo, hi in intervals:
        edges.append((lo, -1))      # début : -1 trie avant les fins à égalité
        edges.append((hi, +1))
    edges.sort()
    best = cnt = 0
    best_lo = best_hi = float("nan")
    for i, (x, typ) in enumerate(edges):
        cnt -= typ
        if cnt > best:
            best = cnt
            best_lo = x
            best_hi = edges[i + 1][0]
    return best, best_lo, best_hi


def smeared_offset(t: np.ndarray, leap_at: float, window: float = 86_400.0) -> np.ndarray:
    """Écart (s) entre une horloge « lissée » et une horloge qui insère la seconde d'un coup.

    Lissage linéaire sur `window` secondes centrées sur l'instant de la seconde
    intercalaire (méthode « leap smear »).
    """
    t = np.asarray(t, dtype=float)
    return np.clip((t - (leap_at - window / 2)) / window, 0.0, 1.0)


def tod_clock_trace(duration: float = 3600.0, dt: float = 1.0, drift_ppm: float = 200.0,
                    sync_every: float = 600.0, sync_error_ms: float = 5.0,
                    step_threshold_ms: float = 128.0, slew_rate: float = 500e-6,
                    seed: int = 0) -> Dict[str, np.ndarray]:
    """Trajectoire de l'écart (ms) d'une horloge calendaire synchronisée par NTP.

    Entre deux synchronisations, l'horloge dérive de `drift_ppm`. À chaque
    synchronisation, NTP estime l'écart (avec une erreur gaussienne d'écart-type
    `sync_error_ms`) : si |écart| dépasse `step_threshold_ms`, l'horloge est **recalée
    d'un coup** (elle peut reculer) ; sinon NTP **ralentit ou accélère** l'horloge
    (« slew ») au plus de `slew_rate` jusqu'à corriger l'écart.

    Renvoie t (s), écart de l'horloge calendaire (ms), indicateur des sauts.
    """
    rng = np.random.default_rng(seed)
    n = int(duration / dt) + 1
    t = np.arange(n) * dt
    off = np.empty(n)
    jumps = np.zeros(n, bool)
    cur = 0.0
    pending = 0.0          # correction restant à appliquer par slew (ms)
    next_sync = sync_every
    for i in range(n):
        if i:
            cur += drift_ppm * 1e-6 * dt * 1e3
            step = min(abs(pending), slew_rate * dt * 1e3)
            corr = math.copysign(step, pending) if pending else 0.0
            cur += corr
            pending -= corr
        if t[i] >= next_sync:
            est = cur + rng.normal(0, sync_error_ms)
            if abs(est) > step_threshold_ms:
                cur -= est
                pending = 0.0
                jumps[i] = True
            else:
                pending = -est
            next_sync += sync_every
        off[i] = cur
    return {"t": t, "offset_ms": off, "jumps": jumps}


def lww_lost_writes(skew: float, n_writes: int = 10_000, mean_gap: float = 1.0,
                    seed: int = 0, n_nodes: int = 2) -> Dict[str, float]:
    """Écritures silencieusement perdues par « la dernière écriture gagne » (LWW).

    Des écritures sur une même clé arrivent l'une après l'autre (écarts exponentiels de
    moyenne `mean_gap`) sur des nœuds tirés au hasard ; chaque nœud horodate avec son
    horloge, décalée uniformément dans [0, skew]. Une écriture est perdue si une écriture
    **antérieure** (en temps réel) porte un horodatage plus grand : elle ne deviendra
    jamais la valeur courante, sans qu'aucune erreur ne soit signalée.
    """
    rng = np.random.default_rng(seed)
    offsets = rng.uniform(0, skew, n_nodes)
    offsets -= offsets.min()
    offsets[np.argmax(offsets)] = skew if n_nodes > 1 else 0.0
    t = np.cumsum(rng.exponential(mean_gap, n_writes))
    node = rng.integers(0, n_nodes, n_writes)
    ts = t + offsets[node]
    prev_max = np.maximum.accumulate(np.concatenate([[-np.inf], ts[:-1]]))
    lost = ts < prev_max
    return {"perdues": float(lost.mean()), "n": n_writes, "offsets": offsets}


class LWWReplica(Node):
    """Réplique multi-leader qui résout les conflits par LWW avec son horloge physique.

    Messages : ("set", valeur) ou ("incr",) d'un client ; ("repl", valeur, ts, origine)
    d'une autre réplique. L'état est (valeur, horodatage).
    """

    def __init__(self, name: str, peers: Sequence[str], clock: Optional[Clock] = None):
        super().__init__(name, clock)
        self.peers = list(peers)
        self.value = None
        self.ts = -math.inf
        self.history: List[Tuple[float, str, object, float, bool]] = []

    def _apply(self, value, ts, origin) -> None:
        kept = ts > self.ts
        if kept:
            self.value, self.ts = value, ts
        self.history.append((self.now, origin, value, ts, kept))

    def on_message(self, src, msg):
        kind = msg[0]
        if kind in ("set", "incr"):
            value = msg[1] if kind == "set" else (self.value or 0) + 1
            ts = self.local_time()
            self._apply(value, ts, self.name)
            for p in self.peers:
                self.send(p, ("repl", value, ts, self.name))
            self.send(src, ("ok", value, ts))
        elif kind == "repl":
            self._apply(msg[1], msg[2], msg[3])


class TrueTime:
    """Horloge à intervalle de confiance, à la TrueTime de Spanner.

    L'incertitude vaut `base` juste après une synchronisation puis grandit de
    `drift_ppm` (borne de dérive supposée) jusqu'à la synchronisation suivante.
    `true_offset` est l'écart réel (inconnu du nœud) de son horloge, supposé respecter
    |true_offset| <= incertitude.
    """

    def __init__(self, base: float = 1e-3, drift_ppm: float = 200.0, sync_every: float = 30.0,
                 true_offset: float = 0.0):
        self.base, self.drift, self.sync_every = base, drift_ppm * 1e-6, sync_every
        self.true_offset = true_offset

    def epsilon(self, t: float) -> float:
        return self.base + self.drift * (t % self.sync_every)

    def now(self, t: float) -> Tuple[float, float]:
        """Lecture à l'instant réel t : (au plus tôt, au plus tard)."""
        reading = t + self.true_offset
        e = self.epsilon(t)
        return reading - e, reading + e
