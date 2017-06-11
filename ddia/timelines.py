"""Diagrammes espace-temps : une ligne horizontale par nœud, le temps de gauche à droite.

C'est la représentation favorite des systèmes distribués (et du livre) : elle montre
d'un coup d'oeil qui envoie quoi à qui, quand une opération commence et se termine,
quels messages se perdent et où un nœud tombe en panne.

Exemple
-------
>>> st = SpaceTime(["client", "leader", "suiveur"], t_max=10, title="Réplication asynchrone")
>>> st.op("client", 0.5, 3.0, "write x=1")
>>> st.msg("client", 0.6, "leader", 1.4, "x=1")
>>> st.msg("leader", 1.6, "client", 2.8, "ok")
>>> st.msg("leader", 1.8, "suiveur", 5.5, "x=1", color=AMBER)
>>> st.event("suiveur", 7, "panne", kind="crash")
>>> st.show()
"""

from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from .plotting import AMBER, GRAY, INDIGO, LIGHT, ROSE, SLATE, TEAL

__all__ = ["SpaceTime"]


class SpaceTime:
    """Toile espace-temps. Les nœuds sont empilés de haut en bas dans l'ordre donné."""

    def __init__(self, nodes, t_max: float = 10.0, title: str | None = None,
                 subtitle: str | None = None, width: float = 10.0, lane_height: float = 1.0,
                 time_label: str = "temps", show_axis: bool = True):
        self.nodes = list(nodes)
        self.t_max = t_max
        self.lane = lane_height
        n = len(self.nodes)
        height = max(2.2, n * lane_height * 0.95 + 0.9)
        self.fig, self.ax = plt.subplots(figsize=(width, height))
        self.y = {name: (n - 1 - i) * lane_height for i, name in enumerate(self.nodes)}
        ax = self.ax
        for name, y in self.y.items():
            ax.plot([0, t_max], [y, y], color="#CBD5E1", lw=1.4, zorder=1)
            ax.text(-0.015 * t_max, y, name, ha="right", va="center", fontsize=10,
                    fontweight="bold", color=SLATE)
        ax.set_xlim(-0.02 * t_max, t_max * 1.02)
        ax.set_ylim(-0.6 * lane_height, (n - 1) * lane_height + 0.6 * lane_height)
        for s in ("top", "right", "left"):
            ax.spines[s].set_visible(False)
        ax.set_yticks([])
        ax.grid(False)
        if show_axis:
            ax.set_xlabel(time_label)
        else:
            ax.set_xticks([])
            ax.spines["bottom"].set_visible(False)
        if title:
            ax.set_title(title, pad=20 if subtitle else 6)
        if subtitle:
            ax.text(0, 1.015, subtitle, transform=ax.transAxes, fontsize=9, color=GRAY, va="bottom")

    # ------------------------------------------------------------------ éléments
    def op(self, node: str, t0: float, t1: float, label: str = "", color: str = INDIGO,
           above: bool = True, fontsize: float = 8.5) -> None:
        """Opération qui dure de t0 à t1 sur le nœud (barre arrondie, étiquette au-dessus)."""
        y = self.y[node]
        h = 0.18 * self.lane
        self.ax.add_patch(FancyBboxPatch((t0, y - h / 2), t1 - t0, h,
                                         boxstyle="round,pad=0,rounding_size=0.04",
                                         facecolor=LIGHT.get(color, "#F1F5F9"), edgecolor=color,
                                         lw=1.6, zorder=3))
        if label:
            dy = 0.2 * self.lane if above else -0.2 * self.lane
            self.ax.text((t0 + t1) / 2, y + dy, label, ha="center",
                         va="bottom" if above else "top", fontsize=fontsize, color=color,
                         fontweight="bold", zorder=6)

    def span(self, node: str, t0: float, t1: float, label: str = "", color: str = ROSE,
             alpha: float = 0.25) -> None:
        """Zone colorée sur la ligne du nœud (pause, partition, indisponibilité...)."""
        y = self.y[node]
        h = 0.5 * self.lane
        self.ax.add_patch(plt.Rectangle((t0, y - h / 2), t1 - t0, h, color=color, alpha=alpha,
                                        lw=0, zorder=0.5))
        if label:
            self.ax.text((t0 + t1) / 2, y - h / 2 - 0.04 * self.lane, label, ha="center", va="top",
                         fontsize=8, color=color, style="italic")

    def msg(self, src: str, t_send: float, dst: str, t_recv: float | None = None,
            label: str = "", color: str = SLATE, lost: bool = False, style: str = "solid",
            label_pos: float = 0.5, fontsize: float = 8) -> None:
        """Message de src (émis à t_send) vers dst (reçu à t_recv).

        lost=True : le message se perd, la flèche s'arrête à mi-chemin sur une croix rouge
        (t_recv est alors l'instant où il « aurait » été reçu).
        """
        if t_recv is None:
            t_recv = t_send + 1.0
        y0, y1 = self.y[src], self.y[dst]
        ls = {"solid": "-", "dashed": (0, (4, 3)), "dotted": (0, (1, 2))}[style]
        if lost:
            tm, ym = (t_send + t_recv) / 2, (y0 + y1) / 2
            self.ax.plot([t_send, tm], [y0, ym], color=color, lw=1.4, linestyle=ls, zorder=4)
            self.ax.plot([tm], [ym], marker="x", color=ROSE, markersize=11, mew=2.5, zorder=5)
            px, py = t_send + label_pos * (tm - t_send), y0 + label_pos * (ym - y0)
        else:
            self.ax.add_patch(FancyArrowPatch((t_send, y0), (t_recv, y1), arrowstyle="-|>",
                                              mutation_scale=11, color=color, lw=1.4,
                                              linestyle=ls, shrinkA=0, shrinkB=0, zorder=4))
            px, py = t_send + label_pos * (t_recv - t_send), y0 + label_pos * (y1 - y0)
        if label:
            self.ax.text(px, py, label, ha="center", va="center", fontsize=fontsize, color=color,
                         zorder=6, bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.9))

    def event(self, node: str, t: float, label: str = "", kind: str = "point",
              color: str | None = None, fontsize: float = 8.5) -> None:
        """Événement ponctuel. kind : "point", "crash" (croix), "tick" (trait vertical), "star"."""
        y = self.y[node]
        if kind == "crash":
            color = color or ROSE
            self.ax.plot([t], [y], marker="X", color=color, markersize=13, zorder=5)
        elif kind == "tick":
            color = color or SLATE
            self.ax.plot([t, t], [y - 0.12 * self.lane, y + 0.12 * self.lane], color=color, lw=2, zorder=5)
        elif kind == "star":
            color = color or AMBER
            self.ax.plot([t], [y], marker="*", color=color, markersize=14, zorder=5)
        else:
            color = color or TEAL
            self.ax.plot([t], [y], marker="o", color=color, markersize=7, zorder=5,
                         markeredgecolor="white", markeredgewidth=1.2)
        if label:
            self.ax.text(t, y - 0.2 * self.lane, label, ha="center", va="top", fontsize=fontsize,
                         color=color)

    def vline(self, t: float, label: str = "", color: str = GRAY) -> None:
        """Repère vertical sur toutes les lignes (instant remarquable)."""
        top = max(self.y.values()) + 0.45 * self.lane
        self.ax.plot([t, t], [-0.45 * self.lane, top], color=color, lw=1, linestyle=(0, (3, 3)), zorder=0.8)
        if label:
            self.ax.text(t, top, label, ha="center", va="bottom", fontsize=8, color=color)

    def note(self, t: float, node: str, text: str, dy: float = 0.3, color: str = "#475569",
             fontsize: float = 8.5) -> None:
        self.ax.text(t, self.y[node] + dy * self.lane, text, ha="center", va="center",
                     fontsize=fontsize, color=color, style="italic", zorder=6)

    def from_trace(self, trace, color_of=None, label_of=None) -> None:
        """Dessine les messages d'une trace `ddia.sim` (liste de `Delivery`)."""
        for d in trace:
            if d.src not in self.y or d.dst not in self.y:
                continue
            c = color_of(d) if color_of else SLATE
            lab = label_of(d) if label_of else ""
            self.msg(d.src, d.t_send, d.dst, d.t_recv, lab, color=c, lost=d.lost)

    def show(self) -> None:
        self.fig.tight_layout()
        plt.show()
