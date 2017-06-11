"""Construit et exécute un notebook à partir de sa source jupytext (format « percent »).

Usage : python tools/build_nb.py chemin/vers/source.py [autres sources...]
La 1re ligne de la source indique le notebook à produire :
`# target: notebooks/chXX_<theme>/chXX_cours_1_<sujet>`. Le notebook est exécuté avec le
kernel `ddia` ; toute erreur d'exécution fait échouer la construction.
"""
import sys
import time
from pathlib import Path

import jupytext
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

ROOT = Path(__file__).resolve().parents[1]


def build(src: Path, timeout: int = 1800) -> Path:
    lines = src.read_text(encoding="utf-8").splitlines()
    target = None
    for line in lines[:3]:
        if line.startswith("# target:"):
            target = line.split(":", 1)[1].strip()
    if target is None:
        raise SystemExit(f"{src}: ligne '# target: notebooks/chXX_.../nom' manquante")
    body = "\n".join(l for l in lines if not l.startswith("# target:"))
    nb = jupytext.reads(body, fmt="py:percent")
    nb.metadata = {"kernelspec": {"name": "ddia", "display_name": "Python (ddia)", "language": "python"},
                   "language_info": {"name": "python"}}
    out = ROOT / (target + ".ipynb")
    out.parent.mkdir(parents=True, exist_ok=True)
    t = time.time()
    ep = ExecutePreprocessor(timeout=timeout, kernel_name="ddia")
    ep.preprocess(nb, {"metadata": {"path": str(out.parent)}})
    for cell in nb.cells:
        cell.metadata.pop("execution", None)
    nbformat.write(nb, out)
    print(f"OK  {out.relative_to(ROOT)}  ({time.time() - t:.0f} s)")
    return out


if __name__ == "__main__":
    for arg in sys.argv[1:]:
        build(Path(arg))
