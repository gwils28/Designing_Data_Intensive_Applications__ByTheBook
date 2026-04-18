"""Méthodes formelles et tests aléatoires (notions de la 2e édition).

- `model_check` : petit model checker exhaustif (parcours en largeur de l'espace des
  états, contre-exemple le plus court) ;
- `counter_model`, `lease_model` : deux modèles de protocoles à vérifier ;
- simulation déterministe (DST) du service de verrous et du stockage à jetons de
  clôture de `ddia.distsys` : fautes tirées d'une graine, rejeu exact, recherche d'une
  graine fautive et réduction (« shrinking ») du scénario de fautes.
"""

from __future__ import annotations

import hashlib
import random
from collections import deque
from dataclasses import dataclass
from typing import Callable, Hashable, List, Optional, Sequence, Tuple

from .distsys import FencedStorage, LeaseClient, LockService, token_order_violations
from .sim import Network

# =====================================================================================
# Model checker
# =====================================================================================


@dataclass
class CheckResult:
    ok: bool
    states: int
    transitions: int
    depth: int
    path: Optional[List[Tuple[str, Hashable]]]   # [(action, état atteint), ...]


def model_check(init: Hashable, successors: Callable[[Hashable], Sequence[Tuple[str, Hashable]]],
                invariant: Callable[[Hashable], bool], max_states: int = 2_000_000) -> CheckResult:
    """Explore tous les états accessibles depuis `init` (parcours en largeur).

    Dès qu'un état viole l'invariant, renvoie le chemin d'actions qui y mène : comme le
    parcours est en largeur, c'est un contre-exemple **le plus court**.
    """
    parent = {init: None}
    depth = {init: 0}
    q = deque([init])
    transitions = 0

    def path_to(s):
        out = []
        while parent[s] is not None:
            prev, action = parent[s]
            out.append((action, s))
            s = prev
        return out[::-1]

    if not invariant(init):
        return CheckResult(False, 1, 0, 0, [])
    while q:
        s = q.popleft()
        for action, t in successors(s):
            transitions += 1
            if t in parent:
                continue
            parent[t] = (s, action)
            depth[t] = depth[s] + 1
            if not invariant(t):
                return CheckResult(False, len(parent), transitions, depth[t], path_to(t))
            if len(parent) >= max_states:
                raise RuntimeError("espace d'états trop grand")
            q.append(t)
    return CheckResult(True, len(parent), transitions, max(depth.values()), None)


def counter_model(n_procs: int = 2, atomic: bool = False):
    """n processus incrémentent un compteur partagé par lecture puis écriture.

    État : (pcs, locales, x) ; pc 0 = lire, 1 = écrire, 2 = fini. Invariant : quand tous
    ont fini, x = n_procs. Avec `atomic`, lecture et écriture forment une seule étape.
    """
    init = (tuple([0] * n_procs), tuple([0] * n_procs), 0)

    def succ(s):
        pcs, loc, x = s
        out = []
        for i in range(n_procs):
            if atomic and pcs[i] == 0:
                out.append((f"p{i} incrémente", (pcs[:i] + (2,) + pcs[i + 1:], loc, x + 1)))
            elif pcs[i] == 0:
                out.append((f"p{i} lit x={x}", (pcs[:i] + (1,) + pcs[i + 1:],
                                                 loc[:i] + (x,) + loc[i + 1:], x)))
            elif pcs[i] == 1:
                out.append((f"p{i} écrit x={loc[i] + 1}", (pcs[:i] + (2,) + pcs[i + 1:], loc, loc[i] + 1)))
        return out

    def inv(s):
        pcs, _, x = s
        return not all(p == 2 for p in pcs) or x == n_procs
    return init, succ, inv


def lease_model(n_clients: int = 2, lease: int = 2, t_max: int = 5, writes: int = 1,
                fencing: bool = False, lock_crash: bool = False, persist: bool = True):
    """Modèle abstrait du verrou à bail du livre, à temps discret.

    État : (t, détenteur, expiration, compteur, panne_utilisée, clients, stockage).
    Chaque client : (phase, jeton, échéance, écritures faites) avec phase 0 = sans bail,
    1 = bail obtenu, 2 = bail vérifié (va écrire), 3 = terminé.
    Stockage : (plus grand jeton accepté, propriétaires des jetons, corrompu ?).

    Actions : `tick` (le temps avance d'une unité, à n'importe quel moment : c'est ainsi
    que l'on modélise une pause entre la vérification du bail et l'écriture),
    `acquire ci`, `check ci`, `write ci`, et `crash verrous` (une fois, si `lock_crash`).
    Invariant : le stockage n'accepte jamais une écriture d'un jeton plus ancien qu'un jeton
    déjà accepté, ni deux clients avec le même jeton.
    """
    init = (0, None, 0, 0, False, tuple((0, None, 0, 0) for _ in range(n_clients)),
            (-1, frozenset(), False))

    def succ(s):
        t, holder, expiry, counter, crashed, cl, store = s
        out = []
        if t < t_max:
            out.append(("tick", (t + 1, holder, expiry, counter, crashed, cl, store)))
        if lock_crash and not crashed:
            out.append(("crash verrous", (t, None, 0, counter if persist else 0, True, cl, store)))
        for i, (ph, tok, dl, done) in enumerate(cl):
            if ph == 0 and done < writes and (holder is None or t >= expiry or holder == i):
                c = counter + 1
                ncl = cl[:i] + ((1, c, t + lease, done),) + cl[i + 1:]
                out.append((f"acquire c{i} (jeton {c})", (t, i, t + lease, c, crashed, ncl, store)))
            elif ph == 1:
                nph = 2 if t < dl else 0
                ncl = cl[:i] + ((nph, tok if nph else None, dl, done),) + cl[i + 1:]
                out.append((f"check c{i} ({'valide' if nph else 'expiré'})",
                            (t, holder, expiry, counter, crashed, ncl, store)))
            elif ph == 2:
                mx, owners, bad = store
                if fencing and tok < mx:
                    ncl = cl[:i] + ((0, None, 0, done),) + cl[i + 1:]
                    out.append((f"write c{i} (jeton {tok}) refusée",
                                (t, holder, expiry, counter, crashed, ncl, store)))
                    continue
                prev_owner = dict(owners).get(tok)
                nbad = bad or tok < mx or (prev_owner is not None and prev_owner != i)
                nowners = owners if prev_owner is not None else owners | {(tok, i)}
                nd = done + 1
                ncl = cl[:i] + ((1 if nd < writes else 3, tok, dl, nd),) + cl[i + 1:]
                out.append((f"write c{i} (jeton {tok}) acceptée",
                            (t, holder, expiry, counter, crashed, ncl, (max(mx, tok), nowners, nbad))))
        return out

    def inv(s):
        return not s[6][2]
    return init, succ, inv


# =====================================================================================
# Simulation déterministe (DST)
# =====================================================================================


@dataclass(frozen=True)
class Fault:
    kind: str          # "pause" (client gelé), "crash_verrous" (panne puis redémarrage)
    t: float
    target: str
    duration: float


def random_faults(seed: int, horizon: float = 120.0, n_clients: int = 3,
                  n_faults: int = 6) -> List[Fault]:
    """Tire un scénario de fautes à partir d'une graine (déterministe)."""
    rng = random.Random(seed)
    faults = []
    for _ in range(n_faults):
        if rng.random() < 0.7:
            faults.append(Fault("pause", round(rng.uniform(5, horizon - 10), 2),
                                f"c{rng.randrange(n_clients)}", round(rng.uniform(1, 12), 2)))
        else:
            faults.append(Fault("crash_verrous", round(rng.uniform(5, horizon - 10), 2),
                                "verrous", round(rng.uniform(0.5, 4), 2)))
    return sorted(faults, key=lambda f: f.t)


@dataclass
class DSTResult:
    ok: bool
    violations: list
    digest: str
    n_messages: int
    accepted: int
    rejected: int
    net: Network
    store: FencedStorage


def trace_digest(net: Network) -> str:
    """Empreinte de la trace complète des messages : deux exécutions identiques ont la même."""
    h = hashlib.sha256()
    for d in net.trace:
        h.update(f"{d.src}>{d.dst}:{d.msg!r}@{d.t_send:.9f}/{d.t_recv:.9f}/{d.lost}\n".encode())
    return h.hexdigest()[:16]


def run_lease_system(faults: Sequence[Fault], seed: int = 0, persist_counter: bool = True,
                     fencing: bool = True, n_clients: int = 3, horizon: float = 120.0,
                     lease: float = 5.0, drop: float = 0.02, grace: bool = False) -> DSTResult:
    """Exécute le système verrous + stockage + clients sous un scénario de fautes donné.

    Tout le non-déterminisme (latences, pertes, fautes) vient de `seed` et de `faults` :
    la même entrée produit exactement la même trace.
    """
    net = Network(seed=seed, latency=(0.2, 1.2), drop=drop)
    net.add(LockService(lease=lease, first_token=1, persist_counter=persist_counter, grace=grace))
    store = net.add(FencedStorage(fencing=fencing))
    for i in range(n_clients):
        net.add(LeaseClient(f"c{i}", start=0.3 * i, work=0.7, n_writes=1, retry=1.0, forever=True))
    for f in faults:
        if f.kind == "pause":
            net.at(f.t, net[f.target].freeze, f.duration)
        elif f.kind == "crash_verrous":
            net.at(f.t, net.crash, "verrous")
            net.at(f.t + f.duration, net.recover, "verrous")
    net.run(until=horizon)
    viol = token_order_violations(store)
    acc = sum(w.accepted for w in store.writes)
    return DSTResult(not viol, viol, trace_digest(net), len(net.trace), acc,
                     len(store.writes) - acc, net, store)


def find_failing_seed(seeds, **kw) -> Optional[Tuple[int, List[Fault], DSTResult]]:
    """Parcourt des graines ; renvoie la première dont le scénario viole l'invariant."""
    for s in seeds:
        faults = random_faults(s, horizon=kw.get("horizon", 120.0), n_clients=kw.get("n_clients", 3))
        res = run_lease_system(faults, seed=s, **kw)
        if not res.ok:
            return s, faults, res
    return None


def shrink_faults(faults: Sequence[Fault], seed: int, **kw) -> List[Fault]:
    """Réduit un scénario fautif : retire les fautes une à une tant que l'échec persiste."""
    cur = list(faults)
    changed = True
    while changed:
        changed = False
        for i in range(len(cur)):
            cand = cur[:i] + cur[i + 1:]
            if not run_lease_system(cand, seed=seed, **kw).ok:
                cur = cand
                changed = True
                break
    return cur
