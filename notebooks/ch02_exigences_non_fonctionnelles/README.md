# Chapitre 2 - Exigences non fonctionnelles

Le chapitre 2 pose le vocabulaire de tout le livre à partir d'une étude de cas, les timelines d'un réseau social : comment décrire la charge et mesurer la performance (débit, temps de réponse, percentiles), ce qu'est un système fiable (fautes, défaillances, tolérance aux fautes, erreurs humaines), ce que signifie passer à l'échelle et ce qui rend un système maintenable. Les notebooks ajoutent la profondeur quantitative que le livre laisse de côté : simulation du fan-out sur un graphe en loi de puissance, histogrammes de percentiles, théorie des files d'attente, modèles de durabilité, injection de fautes, loi universelle de scalabilité.

Les fonctions réutilisables (graphe social et coûts du fan-out, timelines à la lecture, à l'écriture et hybrides, temps de réponse synthétiques, percentiles, histogramme log-linéaire à la HdrHistogram et fenêtre glissante, simulation de files FIFO à c serveurs, formules M/M/1, M/M/c et Pollaczek-Khinchine, AFR, MTTF et MTTDL, simulation de pertes de données avec pannes corrélées, disponibilités, fan-out tolérant aux fautes pour l'injection de fautes, lois d'Amdahl et USL avec ajustement) sont dans [`ddia/requirements.py`](../../ddia/requirements.py), testées par [`tests/test_requirements.py`](../../tests/test_requirements.py). La simulation des tempêtes de relances (panne métastable, backoff, budget de relances, délestage) est dans [`ddia/overload.py`](../../ddia/overload.py), testée par [`tests/test_overload.py`](../../tests/test_overload.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch02_cours_1_timelines_et_charge.ipynb`](ch02_cours_1_timelines_et_charge.ipynb) | *Case Study: Social Network Home Timelines* (*Representing Users, Posts, and Follows*, *Materializing and Updating Timelines*) ; *Understanding Load* | schéma relationnel et requête de timeline sur SQLite (plan de requête, coût linéaire dans le nombre de comptes suivis) ; ordres de grandeur du livre (polling contre fan-out) ; timelines matérialisées implémentées et vérifiées (diagramme) ; graphe social en loi de puissance, part du fan-out des plus gros comptes, délai de livraison d'un message de célébrité ; approche hybride : pourquoi elle ne réduit pas le travail moyen mais borne le pire cas ; paramètres de charge, moyenne contre pics sur une semaine simulée |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|


Pas de lab Docker pour ce chapitre : les notions sont mieux servies par des simulations déterministes.
