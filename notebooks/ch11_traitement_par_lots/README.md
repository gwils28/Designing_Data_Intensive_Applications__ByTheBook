# Chapitre 11 - Traitement par lots

Le chapitre 11 présente les systèmes de traitement par lots : un job lit une entrée immuable et produit une sortie dérivée, recalculée de zéro, ce qui le rend rejouable et tolérant aux erreurs humaines. Il part des outils Unix (`awk | sort | uniq -c`), décrit les briques d'un « système d'exploitation distribué » (systèmes de fichiers distribués, magasins d'objets, orchestration des jobs et allocation des ressources, workflows, pannes et préemption), les modèles de traitement (MapReduce, moteurs dataflow, shuffle, jointures, SQL, DataFrames) et les cas d'usage (ETL, analytique, apprentissage automatique, service des données dérivées). Les notebooks implémentent tout cela à la main : tri externe, MapReduce miniature avec de vrais fichiers de shuffle, moteur dataflow paresseux avec lignage, Pregel, SSTable construite par lots, magasin d'objets, DRF, DataFrame paresseux, MinHash.

Les fonctions réutilisables (journaux synthétiques, pipeline Unix via `subprocess`, tri externe à mémoire bornée, simulation d'un tuyau avec contre-pression, placement des blocs à la HDFS et localité, parité XOR et probabilités de perte, DAG de jobs avec chemin critique et relances, tâches préemptées, MapReduce miniature avec combineur, tri secondaire et exécution multiprocessus, jointures tri-fusion, par hachage diffusé et partitionné, salage des clés chaudes, moteur dataflow `Dataset` avec fusion des opérateurs et recalcul par lignage, Pregel, PageRank, composantes connexes, index inversé, fichier clé-valeur trié avec index creux, publication atomique par lien symbolique) sont dans [`ddia/batch.py`](../../ddia/batch.py), testées par [`tests/test_batch.py`](../../tests/test_batch.py). Les notions propres à la 2e édition (magasin d'objets et protocole de validation par manifeste, simulation d'ordonnancement FIFO et gang scheduling, DRF, DataFrame paresseux avec poussée des filtres, MinHash et LSH) sont dans [`ddia/batch_modern.py`](../../ddia/batch_modern.py), testées par [`tests/test_batch_modern.py`](../../tests/test_batch_modern.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch11_cours_1_outils_unix_et_tri.ipynb`](ch11_cours_1_outils_unix_et_tri.ipynb) | introduction du chapitre ; *Batch Processing with Unix Tools* : *Simple Log Analysis*, *Chain of Commands Versus Custom Program*, *Sorting Versus In-Memory Aggregation* | en ligne, par lots, en flux (diagramme, ordres de grandeur) ; journal NGINX synthétique et vrai pipeline `awk \| sort \| uniq -c \| sort -rn \| head` lancé par `subprocess`, étape par étape ; script Python à table de hachage comparé (diagramme des processus) ; mémoire d'un dictionnaire selon le nombre de clés distinctes ; tri externe implémenté (segments, fusion à k voies, passes mesurées contre la formule) et `sort -S` ; tuyaux et contre-pression (diagramme espace-temps, débit selon la taille du tampon) |
| [`ch11_cours_2_stockage_et_orchestration.ipynb`](ch11_cours_2_stockage_et_orchestration.ipynb) | *Batch Processing in Distributed Systems* : *Distributed Filesystems*, *Object Stores*, *Distributed Job Orchestration* (*Resource allocation*, *Scheduling workflows*, *Handling faults*) | analogie du système d'exploitation distribué ; blocs de 128 Mo et mémoire du NameNode ; placement des répliques par rack et localité des tâches simulés ; parité XOR, réplication contre Reed-Solomon (probabilités de perte) ; workflow quotidien en DAG, chemin critique, Gantt avec échec et relance ; préemption, formule (e^{λT} - 1)/λ démontrée et simulée, granularité optimale, diagramme espace-temps d'une relance ; instances préemptibles ; magasin d'objets (préfixes, renommage non atomique interrompu) ; composants d'un orchestrateur, FIFO contre gang scheduling, DRF |
| [`ch11_cours_3_mapreduce_et_jointures.ipynb`](ch11_cours_3_mapreduce_et_jointures.ipynb) | *MapReduce* ; *Shuffling Data* ; *Joins and Grouping* ; algorithmes de jointure choisis par l'optimiseur (*Query Languages*) | modèle MapReduce et analogie Unix ; shuffle (diagramme), vrais fichiers m(i)r(j) triés, compteurs ; piège de `hash` randomisé ; accélération multiprocessus ; chaîne de deux jobs (diagramme espace-temps de la matérialisation) ; jointure tri-fusion avec tri secondaire et regroupement (carte de chaleur âges × pages) ; asymétrie et salage des clés chaudes ; jointures par hachage diffusé et partitionné, modèle de coût réseau et seuil |
| [`ch11_cours_4_moteurs_dataflow.ipynb`](ch11_cours_4_moteurs_dataflow.ipynb) | *Dataflow Engines* ; *Handling faults* ; *Query Languages* (encadré *Batch Processing and Cloud Data Warehouses Converge*) ; *DataFrames* ; Pregel (*Machine Learning*) | barrières contre pipeline (simulation avec traînards) ; moteur dataflow `Dataset` : plan en étapes (diagramme), fusion, comparaison avec un job par opérateur ; recalcul d'une partition perdue par lignage, non-déterminisme démontré, intervalle de sauvegarde de Young ; Pregel (diagramme des super-étapes), PageRank et composantes connexes ; SQL sur SQLite, ordre des jointures, exécution vectorisée ; DataFrame paresseux avec poussée des filtres et des projections |
| [`ch11_cours_5_sorties_et_usages.ipynb`](ch11_cours_5_sorties_et_usages.ipynb) | introduction du chapitre (sorties immuables) ; *Batch Use Cases* : *Extract-Transform-Load*, *Analytics*, *Machine Learning*, *Serving Derived Data* | sorties versionnées et retour arrière par pointeur contre mise à jour en place (diagramme) ; index inversé construit par MapReduce ; écrire dans la base de production (surcharge M/M/1, doublons après relance) contre fichier trié et publication atomique ; ETL avec rebut, cube préparé contre requêtes ad hoc ; déduplication de corpus par MinHash et LSH ; publication via un flux et visibilité tout ou rien |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch11_exercices_01-05.ipynb`](ch11_exercices_01-05.ipynb) | 11.1 à 11.5 |
| [`ch11_exercices_06-12.ipynb`](ch11_exercices_06-12.ipynb) | 11.6 à 11.12 |

- **Ex. 11.1** - Table de hachage ou tri : ordres de grandeur d'une agrégation de 2 To, tri externe en une passe, vérification du coût en log(N/M).
- **Ex. 11.2** - Régler la largeur de fusion d'un tri externe : passes contre accès disque, disque dur et SSD.
- **Ex. 11.3** - Dimensionner un DFS : mémoire des métadonnées et petits fichiers, réplication contre RS(6, 3), trafic de réparation.
- **Ex. 11.4** - Préemption et granularité des tâches : surcoût 1 + o/T + λT/2, optimum √(2o/λ), effet des vagues et du maximum (simulation).
- **Ex. 11.5** - Le combineur : réduction du shuffle, et pourquoi une moyenne ne peut pas servir de combineur (somme, effectif).
- **Ex. 11.6** - Une jointure tri-fusion qui ne produit rien : clé de tri, de partitionnement et de regroupement ; jointure interne ou externe.
- **Ex. 11.7** - Choisir l'algorithme de jointure pour quatre cas (diffusion, shuffle, hachage partitionné), seuil G/(W-1).
- **Ex. 11.8** - Une célébrité dans la jointure : charge ×11, salage, plancher du hachage (simulation).
- **Ex. 11.9** - Plus courts chemins en Pregel sur un réseau routier, vérification par Dijkstra, vague de messages.
- **Ex. 11.10** - Écrire dans la base de production ou construire et basculer : durées, états mélangés après échec, idempotence.
- **Ex. 11.11** - Allouer un cluster partagé : partage des cœurs irréalisable, DRF à trois ressources, interblocage du gang scheduling.
- **Ex. 11.12** - Publier une sortie sur un magasin d'objets : renommage interrompu, tâches spéculatives, protocole par manifeste testé panne par panne.

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/batch/compose.yml -p ddia_batch up -d
docker compose -f docker/batch/compose.yml -p ddia_batch down -v
```

PostgreSQL écoute sur le port 11132 (utilisateur `ddia`, mot de passe `ddia`, base `lots`).
