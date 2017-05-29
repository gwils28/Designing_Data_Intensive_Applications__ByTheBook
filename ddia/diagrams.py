"""Petit moteur de diagrammes d'architecture fondé sur matplotlib.

Les diagrammes sont rendus comme des figures : ils s'affichent donc directement dans
les notebooks (et sur GitHub), sans dépendance externe.

Exemple
-------
>>> d = Diagram(10, 4, title="Un pipeline RAG minimal")
>>> d.box("q", 1, 2, "Requête", color=INDIGO, kind="user")
>>> d.box("r", 4, 2, "Retriever", sub="BM25 / embeddings", color=TEAL)
>>> d.box("db", 4, 0.6, "Index", color=SLATE, kind="db")
>>> d.box("llm", 7.5, 2, "LLM", sub="génération", color=VIOLET, kind="model")
>>> d.arrow("q", "r", "question")
>>> d.arrow("db", "r", style="dashed")
>>> d.arrow("r", "llm", "contexte")
>>> d.show()
"""

from __future__ import annotations

from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle, Ellipse, FancyArrowPatch, FancyBboxPatch, Polygon, Rectangle

from .plotting import AMBER, GRAY, INDIGO, LIGHT, ROSE, SKY, SLATE, TEAL, VIOLET

__all__ = ["Diagram", "INDIGO", "TEAL", "AMBER", "ROSE", "VIOLET", "SKY", "SLATE", "GRAY"]


def _light(color: str) -> str:
    return LIGHT.get(color, "#F8FAFC")


@dataclass
class _Node:
    x: float
    y: float
    w: float
    h: float
    kind: str

    def anchor(self, tx: float, ty: float) -> tuple[float, float]:
        """Point du bord du nœud situé sur le segment centre -> (tx, ty)."""
        dx, dy = tx - self.x, ty - self.y
        if dx == 0 and dy == 0:
            return self.x, self.y
        if self.kind in ("user", "circle"):
            r = self.w / 2
            n = np.hypot(dx, dy)
            return self.x + r * dx / n, self.y + r * dy / n
        hw, hh = self.w / 2, self.h / 2
        s = min(hw / abs(dx) if dx else np.inf, hh / abs(dy) if dy else np.inf)
        return self.x + s * dx, self.y + s * dy


class Diagram:
    """Toile de dessin en unités « données » : x dans [0, width], y dans [0, height]."""

    def __init__(self, width: float = 12, height: float = 6, title: str | None = None,
                 subtitle: str | None = None, scale: float = 0.9):
        self.width, self.height = width, height
        self.fig, self.ax = plt.subplots(figsize=(width * scale, height * scale))
        self.ax.set_xlim(0, width)
        header = (0.55 if title else 0) + (0.4 if subtitle else 0)
        self.ax.set_ylim(0, height + header)
        self.fig.set_size_inches(width * scale, (height + header) * scale)
        self.ax.set_aspect("equal")
        self.ax.axis("off")
        self.nodes: dict[str, _Node] = {}
        if title:
            self.ax.text(0.05, height + header - 0.05, title, fontsize=14, fontweight="bold",
                         color=SLATE, va="top", ha="left")
        if subtitle:
            self.ax.text(0.05, height + 0.3, subtitle, fontsize=10, color=GRAY, va="top", ha="left")

    # ------------------------------------------------------------------ nœuds
    def box(self, name: str, x: float, y: float, text: str, sub: str | None = None,
            color: str = INDIGO, w: float = 2.2, h: float = 0.9, kind: str = "box",
            fontsize: float = 10.5, filled: bool = False) -> str:
        """Ajoute un nœud centré en (x, y).

        kind : "box" (rectangle arrondi), "model" (rectangle arrondi épais, pour un modèle),
        "db" (cylindre, pour une base de données / un index), "user" ou "circle" (disque),
        "doc" (document au coin replié), "tag" (petite étiquette pleine).
        filled=True : fond de la couleur pleine et texte blanc (pour mettre un nœud en avant).
        """
        ax = self.ax
        face = color if filled else _light(color)
        txt_color = "white" if filled else SLATE
        if kind in ("user", "circle"):
            r = w / 2
            h = w
            ax.add_patch(Circle((x + 0.05, y - 0.05), r, color="#0F172A", alpha=0.08, zorder=1))
            ax.add_patch(Circle((x, y), r, facecolor=face, edgecolor=color, lw=1.8, zorder=2))
        elif kind == "db":
            eh = h * 0.28
            ax.add_patch(Rectangle((x - w / 2, y - h / 2 + eh / 2), w, h - eh, facecolor=face,
                                   edgecolor="none", zorder=2))
            ax.add_patch(Ellipse((x, y - h / 2 + eh / 2), w, eh, facecolor=face, edgecolor=color,
                                 lw=1.8, zorder=2))
            ax.add_patch(Rectangle((x - w / 2, y - h / 2 + eh / 2), w, h - eh, facecolor=face,
                                   edgecolor="none", zorder=2.1))
            ax.plot([x - w / 2, x - w / 2], [y - h / 2 + eh / 2, y + h / 2 - eh / 2], color=color, lw=1.8, zorder=2.2)
            ax.plot([x + w / 2, x + w / 2], [y - h / 2 + eh / 2, y + h / 2 - eh / 2], color=color, lw=1.8, zorder=2.2)
            ax.add_patch(Ellipse((x, y + h / 2 - eh / 2), w, eh, facecolor="white", edgecolor=color,
                                 lw=1.8, zorder=2.3))
            y = y - eh / 4
        elif kind == "doc":
            f = min(w, h) * 0.25
            pts = [(x - w / 2, y - h / 2), (x + w / 2, y - h / 2), (x + w / 2, y + h / 2 - f),
                   (x + w / 2 - f, y + h / 2), (x - w / 2, y + h / 2)]
            ax.add_patch(Polygon([(px + 0.05, py - 0.05) for px, py in pts], color="#0F172A", alpha=0.08, zorder=1))
            ax.add_patch(Polygon(pts, facecolor=face, edgecolor=color, lw=1.6, zorder=2))
            ax.add_patch(Polygon([(x + w / 2 - f, y + h / 2), (x + w / 2 - f, y + h / 2 - f),
                                  (x + w / 2, y + h / 2 - f)], facecolor="white", edgecolor=color, lw=1.2, zorder=3))
        else:
            lw = {"model": 2.8, "tag": 0}.get(kind, 1.6)
            if kind == "tag":
                face, txt_color = color, "white"
            pad = min(0.18, h / 4)
            ax.add_patch(FancyBboxPatch((x - w / 2 + 0.06, y - h / 2 - 0.06), w, h,
                                        boxstyle=f"round,pad=0,rounding_size={pad}",
                                        color="#0F172A", alpha=0.08, zorder=1))
            ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), w, h,
                                        boxstyle=f"round,pad=0,rounding_size={pad}",
                                        facecolor=face, edgecolor=color, lw=lw, zorder=2))
            if kind == "model":  # liseré coloré sur la gauche
                ax.add_patch(FancyBboxPatch((x - w / 2, y - h / 2), 0.12, h,
                                            boxstyle="round,pad=0,rounding_size=0.06",
                                            facecolor=color, edgecolor="none", zorder=3))
        if sub:
            ax.text(x, y + 0.13 * h / 0.9, text, ha="center", va="center", fontsize=fontsize,
                    fontweight="bold", color=txt_color, zorder=4)
            ax.text(x, y - 0.2 * h / 0.9, sub, ha="center", va="center", fontsize=fontsize * 0.78,
                    color=txt_color if filled else "#475569", zorder=4, style="italic")
        else:
            ax.text(x, y, text, ha="center", va="center", fontsize=fontsize, fontweight="bold",
                    color=txt_color, zorder=4)
        self.nodes[name] = _Node(x, y if kind != "db" else y + h * 0.07, w, h, kind)
        return name

    # ---------------------------------------------------------------- liaisons
    def arrow(self, a: str, b: str, label: str | None = None, color: str = SLATE,
              style: str = "solid", rad: float = 0.0, lw: float = 1.6, label_pos: float = 0.5,
              label_offset: tuple[float, float] = (0, 0.18), both: bool = False, fontsize: float = 8.5) -> None:
        """Flèche du nœud a vers le nœud b (rad != 0 : flèche courbée)."""
        na, nb = self.nodes[a], self.nodes[b]
        p1 = na.anchor(nb.x, nb.y)
        p2 = nb.anchor(na.x, na.y)
        ls = {"solid": "-", "dashed": (0, (4, 3)), "dotted": (0, (1, 2))}[style]
        arrowstyle = "<|-|>" if both else "-|>"
        self.ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle=arrowstyle, mutation_scale=14, color=color,
                                          lw=lw, linestyle=ls, connectionstyle=f"arc3,rad={rad}",
                                          shrinkA=4, shrinkB=4, zorder=5))
        if label:
            mx = p1[0] + label_pos * (p2[0] - p1[0])
            my = p1[1] + label_pos * (p2[1] - p1[1])
            # décalage perpendiculaire pour les flèches courbes
            dx, dy = p2[0] - p1[0], p2[1] - p1[1]
            mx += dy * rad * 0.5
            my += -dx * rad * 0.5
            self.ax.text(mx + label_offset[0], my + label_offset[1], label, ha="center", va="center",
                         fontsize=fontsize, color=color, zorder=6,
                         bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.9))

    def line(self, points, color: str = GRAY, style: str = "dashed", lw: float = 1.2, arrow: bool = True) -> None:
        """Polyligne libre (coordonnées données), flèche au bout si arrow=True."""
        pts = np.asarray(points, dtype=float)
        ls = {"solid": "-", "dashed": (0, (4, 3)), "dotted": (0, (1, 2))}[style]
        self.ax.plot(pts[:-1, 0], pts[:-1, 1], color=color, lw=lw, linestyle=ls, zorder=4)
        last = FancyArrowPatch(tuple(pts[-2]), tuple(pts[-1]), arrowstyle="-|>" if arrow else "-",
                               mutation_scale=12, color=color, lw=lw, linestyle=ls, zorder=4)
        self.ax.add_patch(last)

    # ------------------------------------------------------------ décorations
    def group(self, x0: float, y0: float, x1: float, y1: float, title: str | None = None,
              color: str = GRAY, alpha: float = 0.35) -> None:
        """Zone de regroupement (fond pâle + titre en haut à gauche)."""
        self.ax.add_patch(FancyBboxPatch((x0, y0), x1 - x0, y1 - y0,
                                         boxstyle="round,pad=0,rounding_size=0.25",
                                         facecolor=_light(color), edgecolor=color, lw=1.0,
                                         linestyle=(0, (5, 3)), alpha=alpha, zorder=0))
        if title:
            self.ax.text(x0 + 0.15, y1 - 0.12, title.upper(), ha="left", va="top", fontsize=8.5,
                         fontweight="bold", color=color, zorder=0.5)

    def note(self, x: float, y: float, text: str, color: str = "#475569", fontsize: float = 9,
             ha: str = "center", style: str = "italic") -> None:
        self.ax.text(x, y, text, ha=ha, va="center", fontsize=fontsize, color=color, style=style, zorder=6)

    def badge(self, x: float, y: float, text: str, color: str = AMBER) -> None:
        """Pastille numérotée (pour indiquer l'ordre des étapes)."""
        self.ax.add_patch(Circle((x, y), 0.2, facecolor=color, edgecolor="white", lw=1.5, zorder=7))
        self.ax.text(x, y, text, ha="center", va="center", fontsize=8.5, fontweight="bold", color="white", zorder=8)

    def flow(self, names_texts, y: float, x0: float = 1.2, dx: float = 2.6, colors=None,
             labels=None, **box_kw) -> list[str]:
        """Chaîne horizontale de boîtes reliées par des flèches.

        names_texts : liste de (nom, texte) ou (nom, texte, sous-texte).
        """
        colors = colors or [INDIGO, TEAL, VIOLET, AMBER, SKY, ROSE]
        names = []
        for i, item in enumerate(names_texts):
            name, text, *sub = item
            self.box(name, x0 + i * dx, y, text, sub=sub[0] if sub else None,
                     color=colors[i % len(colors)], **box_kw)
            names.append(name)
        for i in range(len(names) - 1):
            self.arrow(names[i], names[i + 1], labels[i] if labels else None)
        return names

    def show(self) -> None:
        self.fig.tight_layout()
        plt.show()
