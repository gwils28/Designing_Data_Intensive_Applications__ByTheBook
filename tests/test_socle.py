"""Tests du socle commun : style, diagrammes, diagrammes espace-temps."""
import matplotlib

matplotlib.use("Agg")

import numpy as np

from ddia.diagrams import Diagram, _Node
from ddia.plotting import COLORS, set_style
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
