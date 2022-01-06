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
