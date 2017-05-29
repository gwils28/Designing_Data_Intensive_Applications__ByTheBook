"""Style graphique commun à tous les notebooks.

Une palette unique (indigo, sarcelle, ambre, rose, violet, ciel, ardoise) est utilisée
partout : un même concept garde la même couleur d'un chapitre à l'autre.
"""

from __future__ import annotations

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# --- Palette -----------------------------------------------------------------------
INDIGO = "#4F46E5"
TEAL = "#0D9488"
AMBER = "#F59E0B"
ROSE = "#E11D48"
VIOLET = "#7C3AED"
SKY = "#0284C7"
SLATE = "#334155"
GRAY = "#94A3B8"
GREEN = "#16A34A"

COLORS = [INDIGO, TEAL, AMBER, ROSE, VIOLET, SKY, SLATE, GREEN]

#: versions claires (fonds de boîtes, zones) associées à chaque couleur
LIGHT = {
    INDIGO: "#E0E7FF",
    TEAL: "#CCFBF1",
    AMBER: "#FEF3C7",
    ROSE: "#FFE4E6",
    VIOLET: "#EDE9FE",
    SKY: "#E0F2FE",
    SLATE: "#E2E8F0",
    GRAY: "#F1F5F9",
    GREEN: "#DCFCE7",
}

#: carte de couleurs séquentielle (blanc -> indigo) et divergente (rose <- blanc -> sarcelle)
SEQ = LinearSegmentedColormap.from_list("ddia_seq", ["#F8FAFC", "#C7D2FE", INDIGO, "#1E1B4B"])
DIV = LinearSegmentedColormap.from_list("ddia_div", [ROSE, "#FFF1F2", "#F8FAFC", "#CCFBF1", TEAL])


def set_style(font_scale: float = 1.0) -> None:
    """Applique le style graphique du dépôt (à appeler en tête de notebook)."""
    mpl.rcParams.update(
        {
            "figure.figsize": (8, 4.5),
            "figure.dpi": 110,
            "savefig.dpi": 150,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#CBD5E1",
            "axes.linewidth": 0.8,
            "axes.grid": True,
            "grid.color": "#E2E8F0",
            "grid.linewidth": 0.6,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titleweight": "bold",
            "axes.titlesize": 12 * font_scale,
            "axes.titlelocation": "left",
            "axes.labelsize": 10.5 * font_scale,
            "axes.labelcolor": SLATE,
            "axes.prop_cycle": mpl.cycler(color=COLORS),
            "xtick.color": SLATE,
            "ytick.color": SLATE,
            "xtick.labelsize": 9.5 * font_scale,
            "ytick.labelsize": 9.5 * font_scale,
            "legend.frameon": False,
            "legend.fontsize": 9.5 * font_scale,
            # DejaVu Sans : couvre tous les glyphes utiles, y compris le signe moins (U+2212)
            # que mathtext emploie pour les exposants des axes logarithmiques
            "font.family": ["DejaVu Sans"],
            "font.sans-serif": ["DejaVu Sans"],
            "text.color": SLATE,
            "lines.linewidth": 2.0,
            "axes.unicode_minus": False,
            "mathtext.fontset": "dejavusans",
            "image.cmap": "ddia_seq",
        }
    )
    try:
        mpl.colormaps.register(SEQ)
        mpl.colormaps.register(DIV)
    except ValueError:  # déjà enregistrées
        pass


def annotate_bars(ax, fmt: str = "{:.2f}", fontsize: int = 9, color: str = SLATE, padding: int = 3) -> None:
    """Écrit la valeur au bout de chaque barre d'un diagramme en barres."""
    for container in ax.containers:
        ax.bar_label(container, labels=[fmt.format(v) for v in container.datavalues],
                     fontsize=fontsize, color=color, padding=padding)


def subtitle(ax, text: str) -> None:
    """Ajoute un sous-titre gris sous le titre d'un axe."""
    ax.text(0, 1.01, text, transform=ax.transAxes, fontsize=9, color=GRAY, va="bottom")


def finish(fig=None, title: str | None = None) -> None:
    """Titre global optionnel + mise en page serrée."""
    fig = fig or plt.gcf()
    if title:
        fig.suptitle(title, x=0.01, ha="left", fontweight="bold", fontsize=13)
    fig.tight_layout()
