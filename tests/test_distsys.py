"""Tests du module ddia.distsys (problèmes des systèmes distribués)."""
import math

import numpy as np
import pytest

from ddia import distsys as ds
from ddia.sim import Clock, Network


# --- réseau ----------------------------------------------------------------------------

def test_probabilite_panne_partielle():
    assert ds.partial_failure_probability(0.0, 100) == 0.0
    assert ds.partial_failure_probability(0.5, 2) == pytest.approx(0.75)
    assert ds.partial_failure_probability(1e-4, 40) == pytest.approx(40e-4, rel=0.01)


def test_rtt_queue_lourde():
    r = ds.rtt_samples(200_000, seed=1)
    assert np.median(r) == pytest.approx(0.5, rel=0.05)
    assert np.percentile(r, 99.99) > 50 * np.median(r)


def test_detecteur_delai_fixe_compromis():
    d = ds.rtt_samples(20_000, seed=2, median_ms=4, sigma=0.7) / 2
    arr = ds.heartbeat_arrivals(20_000, 100.0, d)
    court = ds.fixed_timeout_eval(arr, 105, 100.0)
    long = ds.fixed_timeout_eval(arr, 300, 100.0)
    assert court["faux_positifs"] > long["faux_positifs"]
    assert court["detection_moy"] < long["detection_moy"]


def test_phi_accrual():
    det = ds.PhiAccrualDetector(window=100, min_std=1.0)
    for k in range(50):
        det.heartbeat(k * 100.0 + (k % 3))
    last = det.last
    assert det.phi(last + 50) < 0.5
    assert det.phi(last + 300) > det.phi(last + 150) > 1
    # seuil phi = 8 : écart z = 5,61 écarts-types au-delà de la moyenne
    mu, sd = det.stats()
    assert det.phi(last + mu + 5.61 * sd) == pytest.approx(8, abs=0.1)


def test_phi_sans_historique():
    det = ds.PhiAccrualDetector()
    assert det.phi(10.0) == 0.0
    det.heartbeat(0.0)
    assert det.phi(10.0) == 0.0


def test_jacobson_rto_suit_le_reseau():
    s = np.r_[np.full(300, 1.0), np.full(300, 10.0)]
    est = ds.jacobson_rto(s)
    assert est["srtt"][299] == pytest.approx(1.0, rel=0.01)
    assert est["srtt"][-1] == pytest.approx(10.0, rel=0.01)
    assert np.all(est["rto"] >= est["srtt"])


def test_file_du_commutateur():
    faible = ds.switch_port_sim(load=0.3, slots=30_000, seed=1)
    forte = ds.switch_port_sim(load=0.95, slots=30_000, seed=1)
    assert faible["waits"].mean() < forte["waits"].mean()
    assert forte["drop_rate"] > faible["drop_rate"]
    assert forte["queue"].max() <= 64
    assert 0.85 < forte["offered"] < 1.05


def test_fifo_lindley():
    dep = ds.fifo_departures(np.array([0.0, 0.0, 5.0]), np.array([2.0, 2.0, 1.0]))
    assert list(dep) == [2.0, 4.0, 6.0]


def test_circuits_contre_paquets():
    r = ds.circuit_vs_packet(load=0.3, n_jobs=5000, seed=1)
    assert r["circuit"].mean() > 5 * r["paquets"].mean()


def test_voix_sur_paquets():
    v = ds.voice_over_packets(0.6, n_frames=5000, seed=1)
    assert v.min() == pytest.approx(0.05)
    assert np.percentile(v, 99) > 1.0


# --- horloges ----------------------------------------------------------------------------

def test_derive():
    assert ds.drift_error(200, 30) == pytest.approx(6e-3)
    assert ds.drift_error(200, 86_400) == pytest.approx(17.28)


def test_ntp_formules():
    off, delay = ds.ntp_offset_delay(1.0, 4.0, 5.0, 6.0)
    assert off == pytest.approx(1.0) and delay == pytest.approx(4.0)
    est, d, err = ds.ntp_exchange(true_offset=2.0, d_out=1.0, d_back=3.0, server_time=1.0)
    assert err == pytest.approx((1.0 - 3.0) / 2)
    assert abs(err) <= d / 2 + 1e-12
    _, _, err0 = ds.ntp_exchange(true_offset=-7.0, d_out=2.0, d_back=2.0)
    assert err0 == pytest.approx(0.0)


def test_marzullo_ecarte_le_menteur():
    k, lo, hi = ds.marzullo([(8, 12), (11, 13), (10, 12), (20, 22)])
    assert (k, lo, hi) == (3, 11, 12)


def test_lissage_seconde_intercalaire():
    s = ds.smeared_offset(np.array([-1e6, 0.0, 1e6]), 0.0, window=86_400)
    assert list(s) == [0.0, 0.5, 1.0]


def test_horloge_ntp_saute_et_ralentit():
    tr = ds.tod_clock_trace(duration=6 * 3600, seed=4)
    assert tr["jumps"].sum() >= 1
    assert np.abs(tr["offset_ms"]).max() < 200
    sans_saut = ds.tod_clock_trace(duration=3600, drift_ppm=20, seed=1)
    assert sans_saut["jumps"].sum() == 0


def test_lww_perd_des_ecritures():
    assert ds.lww_lost_writes(0.0, n_writes=5000)["perdues"] == 0.0
    a = ds.lww_lost_writes(0.3, n_writes=50_000, mean_gap=1.0, seed=1)["perdues"]
    assert a == pytest.approx(0.25 * (1 - math.exp(-0.3)), rel=0.2)


def test_replique_lww_perd_l_increment():
    net = Network(seed=1, latency=lambda r, s, d: {("n1", "n3"): 1.2, ("n1", "n2"): 4.0, ("n3", "n2"): 1.0}.get((s, d), 0.5))
    peers = ["n1", "n2", "n3"]
    reps = {p: net.add(ds.LWWReplica(p, [q for q in peers if q != p], clock=Clock(offset=o)))
            for p, o in zip(peers, [3.0, 1.0, 0.0])}
    from ddia.sim import Node
    a, b = net.add(Node("a")), net.add(Node("b"))
    net.at(1.0, a.send, "n1", ("set", 1))
    net.at(2.9, b.send, "n3", ("incr",))
    net.run()
    assert all(r.value == 1 for r in reps.values())        # l'incrément a disparu partout


def test_truetime_et_commit_wait():
    tt = ds.TrueTime(base=1e-3, drift_ppm=200, sync_every=30)
    assert tt.epsilon(0) == pytest.approx(1e-3)
    assert tt.epsilon(29.999) == pytest.approx(7e-3, rel=1e-3)
    lo, hi = tt.now(10.0)
    assert lo <= 10.0 <= hi
    sans = ds.commit_wait_experiment(eps=4e-3, commit_wait=False, seed=1)
    avec = ds.commit_wait_experiment(eps=4e-3, commit_wait=True, seed=1)
    assert sans["inversions"] > 0.1 and avec["inversions"] == 0.0
    assert avec["attente_moy"] == pytest.approx(8e-3)


# --- pauses, baux, jetons -------------------------------------------------------------------

def test_pauses_gc():
    p = ds.gc_pause_samples(500_000, seed=1)
    assert np.median(p) < 0.01
    assert (p > 10).mean() == pytest.approx(0.002 * (2 / 10) ** 1.5, rel=0.25)


def test_routage_conscient_du_gc():
    aveugle = ds.gc_aware_routing(n_requests=20_000, seed=1)
    prevenu = ds.gc_aware_routing(n_requests=20_000, aware=True, seed=1)
    assert np.percentile(aveugle, 99) > 0.1
    assert np.percentile(prevenu, 99.9) == pytest.approx(0.005)


def test_scenario_zombie_sans_et_avec_jetons():
    _, lock, store, _ = ds.lease_scenario(fencing=False, pause=15.0)
    assert [w.accepted for w in store.writes] == [True, True]
    assert ds.token_order_violations(store)                # 33 accepté après 34
    _, lock, store, (c1, _) = ds.lease_scenario(fencing=True, pause=15.0)
    assert [(w.token, w.accepted) for w in store.writes] == [(34, True), (33, False)]
    assert not ds.token_order_violations(store)
    assert any("refus" in e for _, e in c1.events)


def test_scenario_sans_pause_ok():
    _, lock, store, _ = ds.lease_scenario(fencing=False, pause=0.0)
    assert not ds.zombie_writes(store, lock.grants)
    assert [g.token for g in lock.grants] == [33, 34]


def test_requete_retardee():
    _, _, store, _ = ds.lease_scenario(fencing=True, delayed_write=20.0, until=30)
    assert [(w.client, w.accepted) for w in store.writes] == [("client2", True), ("client1", False)]


def test_service_de_verrous_amnesique():
    net = Network(seed=0, latency=(0.1, 0.2))
    lock = net.add(ds.LockService(lease=5.0, first_token=1, persist_counter=False))
    from ddia.sim import Node
    c = net.add(Node("c"))
    net.at(0.0, c.send, "verrous", ("acquire",))
    net.at(1.0, net.crash, "verrous")
    net.at(2.0, net.recover, "verrous")
    net.at(3.0, c.send, "verrous", ("acquire",))
    net.run()
    assert [g.token for g in lock.grants] == [1, 1]       # le jeton 1 est redonné


def test_delai_de_grace():
    net = Network(seed=0, latency=(0.1, 0.2))
    lock = net.add(ds.LockService(lease=5.0, first_token=1, grace=True))
    from ddia.sim import Node
    c = net.add(Node("c"))
    net.at(1.0, net.crash, "verrous")
    net.at(2.0, net.recover, "verrous")
    net.at(3.0, c.send, "verrous", ("acquire",))
    net.at(8.0, c.send, "verrous", ("acquire",))
    net.run()
    assert [g.t > 7.0 for g in lock.grants] == [True]


def test_majorite_et_byzantin():
    assert all(ds.majorities_intersect(n) for n in range(1, 8))
    assert [ds.tolerated_faults(n) for n in (3, 4, 5)] == [1, 1, 2]
    assert ds.tolerated_faults(4, byzantine=True) == 1
    assert ds.tolerated_faults(3, byzantine=True) == 0


def test_appartenance_faute_asymetrique():
    net = Network(seed=2, latency=(0.2, 0.6))
    grp = ["A", "B", "C"]
    nodes = {g: net.add(ds.HeartbeatMember(g, grp)) for g in grp}
    net.at(5.0, net.block, "A", "B")
    net.at(5.0, net.block, "A", "C")
    net.run(until=16)
    assert "A" in nodes["B"].declared and "A" in nodes["C"].declared
    assert not nodes["A"].suspects                         # A ne se doute de rien


# --- byzantin et sommes de contrôle ------------------------------------------------------------

def test_om1_trois_et_quatre_generaux():
    assert ds.byzantine_check(3, 1, {2})["echecs"] > 0
    assert ds.byzantine_check(4, 1, {3})["echecs"] == 0
    assert ds.byzantine_check(4, 1, {0})["echecs"] == 0
    assert ds.byzantine_check(4, 1, {2, 3})["echecs"] > 0


def test_om2_sept_generaux():
    assert ds.byzantine_check(7, 2, {5, 6}, n_strategies=300, seed=1)["echecs"] == 0


def test_somme_internet():
    # exemple de la RFC 1071 : 00 01 f2 03 f4 f5 f6 f7 -> somme 0xddf2, complément 0x220d
    assert ds.internet_checksum(bytes([0x00, 0x01, 0xF2, 0x03, 0xF4, 0xF5, 0xF6, 0xF7])) == 0x220D
    a = bytes(range(16))
    b = a[2:4] + a[0:2] + a[4:]                            # deux mots échangés
    assert ds.internet_checksum(a) == ds.internet_checksum(b)
    assert ds.crc32(a) != ds.crc32(b)


def test_taux_de_detection():
    r = ds.detection_rates(n_trials=300, size=128, seed=1)
    assert r["bit"]["somme Internet (16 bits)"] == 1.0
    assert r["echange"]["somme Internet (16 bits)"] == 0.0
    assert r["echange"]["CRC-32"] == 1.0


# --- correction ---------------------------------------------------------------------------------

def test_verification_historique_jetons():
    ok = [ds.TokenRequest("a", 0, 1, 1), ds.TokenRequest("b", 2, 3, 2), ds.TokenRequest("c", 2.5, None, None)]
    r = ds.check_token_history(ok)
    assert r["violations"] == [] and r["sans_reponse"] == 1
    ko = [ds.TokenRequest("a", 0, 1, 5), ds.TokenRequest("b", 2, 3, 5)]
    r = ds.check_token_history(ko)
    assert {v[0] for v in r["violations"]} == {"unicité", "monotonie"}
    assert r["premiere"] == 3


def test_generateur_amnesique():
    assert ds.check_token_history(ds.token_generator_history(persist=False))["violations"]
    assert not ds.check_token_history(ds.token_generator_history(persist=True))["violations"]


# --- notions de la 2e édition (fonctions dédiées) -------------------------------------------

def test_tcp_rupture_politiques():
    a = ds.tcp_failure_outcomes(policy="acquittements_tcp", n_trials=500)
    b = ds.tcp_failure_outcomes(policy="tout_renvoyer", n_trials=500)
    c = ds.tcp_failure_outcomes(policy="identifiants", n_trials=500)
    assert a["perdues"] > 0 and a["deux_fois"] == 0
    assert b["perdues"] == 0 and b["deux_fois"] > 0
    assert c == {"perdues": 0.0, "une_fois": 1.0, "deux_fois": 0.0}


def test_cloture_repliques_sans_leader():
    avec = ds.fenced_lww_demo(use_token=True)
    sans = ds.fenced_lww_demo(use_token=False)
    assert set(avec["lectures"].values()) == {"client2"}
    assert set(sans["lectures"].values()) == {"client1"}


def test_noeud_boiteux():
    tour = ds.limping_node_sim(policy="tourniquet", n_requests=40_000, seed=1)
    ejec = ds.limping_node_sim(policy="ejection", n_requests=40_000, seed=1)
    assert np.percentile(tour["latences"][20_000:], 99) > 5 * np.percentile(ejec["latences"][30_000:], 99)
    assert ejec["ejection"] is not None
