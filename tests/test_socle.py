"""Tests du socle commun : style, diagrammes, diagrammes espace-temps, simulateur."""
import matplotlib

matplotlib.use("Agg")

import numpy as np

from ddia.diagrams import Diagram, _Node
from ddia.plotting import COLORS, set_style
from ddia.sim import Clock, Network, Node
from ddia.timelines import SpaceTime


def test_ancrage_rectangle():
    n = _Node(0, 0, 2, 1, "box")
    assert np.allclose(n.anchor(10, 0), (1, 0))
    assert np.allclose(n.anchor(0, -10), (0, -0.5))


def test_diagramme_se_construit():
    set_style()
    d = Diagram(6, 3, title="test")
    d.box("a", 1, 1, "A")
    d.box("b", 4, 1, "B", kind="db", color=COLORS[1])
    d.arrow("a", "b", "x")
    assert set(d.nodes) == {"a", "b"}


def test_espace_temps_se_construit():
    st = SpaceTime(["a", "b"], t_max=5)
    st.op("a", 0, 1, "op")
    st.msg("a", 0.5, "b", 2, "m")
    st.msg("b", 2.5, "a", 3, "perdu", lost=True)
    st.event("b", 4, "panne", kind="crash")
    assert st.y["a"] > st.y["b"]


class Echo(Node):
    def on_message(self, src, msg):
        if msg == "ping":
            self.send(src, "pong")


def test_ping_pong_deterministe():
    def run(seed):
        net = Network(seed=seed, latency=(1.0, 3.0))
        a = net.add(Node("a"))
        net.add(Echo("b"))
        net.at(0.0, a.send, "b", "ping")
        net.run()
        return [(d.src, d.msg, round(d.t_recv, 6)) for d in net.trace]

    t1 = run(7)
    assert [m for _, m, _ in t1] == ["ping", "pong"]
    assert t1 == run(7)


def test_partition_et_panne():
    net = Network(seed=0)
    a = net.add(Node("a"))
    net.add(Echo("b"))
    net.partition({"a"}, {"b"})
    net.at(0.0, a.send, "b", "ping")
    net.run()
    assert len(net.trace) == 1 and net.trace[0].lost
    net.heal()
    net.crash("b")
    net.at(net.now, a.send, "b", "ping")
    net.run()
    assert net.trace[-1].lost


def test_horloge_derive_et_reglage():
    c = Clock(offset=2.0, drift=1e-3)
    assert np.isclose(c.read(1000.0), 1003.0)
    c.adjust(1000.0, 1000.0)
    assert np.isclose(c.read(1000.0), 1000.0)
