# Chapitre 11 - Traitement par lots

Le chapitre 11 présente les systèmes de traitement par lots : un job lit une entrée immuable et produit une sortie dérivée, recalculée de zéro, ce qui le rend rejouable et tolérant aux erreurs humaines. Il part des outils Unix (`awk | sort | uniq -c`), décrit les briques d'un « système d'exploitation distribué » (systèmes de fichiers distribués, magasins d'objets, orchestration des jobs et allocation des ressources, workflows, pannes et préemption), les modèles de traitement (MapReduce, moteurs dataflow, shuffle, jointures, SQL, DataFrames) et les cas d'usage (ETL, analytique, apprentissage automatique, service des données dérivées). Les notebooks implémentent tout cela à la main : tri externe, MapReduce miniature avec de vrais fichiers de shuffle, moteur dataflow paresseux avec lignage, Pregel, SSTable construite par lots, magasin d'objets, DRF, DataFrame paresseux, MinHash.

Les fonctions réutilisables (journaux synthétiques, pipeline Unix via `subprocess`, tri externe à mémoire bornée, simulation d'un tuyau avec contre-pression, placement des blocs à la HDFS et localité, parité XOR et probabilités de perte, DAG de jobs avec chemin critique et relances, tâches préemptées, MapReduce miniature avec combineur, tri secondaire et exécution multiprocessus, jointures tri-fusion, par hachage diffusé et partitionné, salage des clés chaudes, moteur dataflow `Dataset` avec fusion des opérateurs et recalcul par lignage, Pregel, PageRank, composantes connexes, index inversé, fichier clé-valeur trié avec index creux, publication atomique par lien symbolique) sont dans [`ddia/batch.py`](../../ddia/batch.py), testées par [`tests/test_batch.py`](../../tests/test_batch.py). Les notions propres à la 2e édition (magasin d'objets et protocole de validation par manifeste, simulation d'ordonnancement FIFO et gang scheduling, DRF, DataFrame paresseux avec poussée des filtres, MinHash et LSH) sont dans [`ddia/batch_modern.py`](../../ddia/batch_modern.py), testées par [`tests/test_batch_modern.py`](../../tests/test_batch_modern.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch11_cours_1_outils_unix_et_tri.ipynb`](ch11_cours_1_outils_unix_et_tri.ipynb) | introduction du chapitre ; *Batch Processing with Unix Tools* : *Simple Log Analysis*, *Chain of Commands Versus Custom Program*, *Sorting Versus In-Memory Aggregation* | en ligne, par lots, en flux (diagramme, ordres de grandeur) ; journal NGINX synthétique et vrai pipeline `awk \| sort \| uniq -c \| sort -rn \| head` lancé par `subprocess`, étape par étape ; script Python à table de hachage comparé (diagramme des processus) ; mémoire d'un dictionnaire selon le nombre de clés distinctes ; tri externe implémenté (segments, fusion à k voies, passes mesurées contre la formule) et `sort -S` ; tuyaux et contre-pression (diagramme espace-temps, débit selon la taille du tampon) |

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/batch/compose.yml -p ddia_batch up -d
docker compose -f docker/batch/compose.yml -p ddia_batch down -v
```

PostgreSQL écoute sur le port 11132 (utilisateur `ddia`, mot de passe `ddia`, base `lots`).
