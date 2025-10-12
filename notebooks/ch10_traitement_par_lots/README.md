# Chapitre 10 - Traitement par lots

Le chapitre 10 ouvre la partie III du livre, consacrée aux données dérivées. Un job de lots lit une entrée immuable et produit une sortie dérivée, recalculée de zéro : c'est ce qui le rend rejouable et tolérant aux erreurs humaines. Le chapitre part des outils Unix (`awk | sort | uniq -c`), passe à MapReduce et à HDFS (exécution distribuée, shuffle, workflows, jointures côté reduce et côté map, asymétrie), compare Hadoop aux bases MPP, discute des sorties des workflows (index de recherche, bases clé-valeur construites par lots), puis va au-delà de MapReduce : moteurs dataflow, tolérance aux pannes par recalcul, Pregel et langages de haut niveau. Les notebooks implémentent tout cela à la main : tri externe, MapReduce miniature avec de vrais fichiers de shuffle, moteur dataflow paresseux avec lignage, Pregel, SSTable construite par lots.

Les fonctions réutilisables (journaux synthétiques, pipeline Unix via `subprocess`, tri externe à mémoire bornée, simulation d'un tuyau avec contre-pression, placement des blocs à la HDFS et localité, parité XOR et probabilités de perte, DAG de jobs avec chemin critique et relances, tâches préemptées, MapReduce miniature avec combineur, tri secondaire et exécution multiprocessus, jointures tri-fusion, par hachage diffusé et partitionné, salage des clés chaudes, moteur dataflow `Dataset` avec fusion des opérateurs et recalcul par lignage, Pregel, PageRank, composantes connexes, index inversé, fichier clé-valeur trié avec index creux, publication atomique par lien symbolique) sont dans [`ddia/batch.py`](../../ddia/batch.py), testées par [`tests/test_batch.py`](../../tests/test_batch.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch10_cours_1_outils_unix_et_tri.ipynb`](ch10_cours_1_outils_unix_et_tri.ipynb) | introduction de la partie III (*Systems of Record and Derived Data*) et du chapitre ; *Batch Processing with Unix Tools* : *Simple Log Analysis*, *Chain of commands versus custom program*, *Sorting versus in-memory aggregation*, *The Unix Philosophy* | système de référence et données dérivées ; services, lots et flux (diagramme, ordres de grandeur) ; journal NGINX synthétique et vrai pipeline `awk \| sort \| uniq -c \| sort -rn \| head` lancé par `subprocess`, étape par étape ; script Python à table de hachage comparé (diagramme des processus) ; mémoire d'un dictionnaire selon le nombre de clés distinctes ; tri externe implémenté et `sort -S` ; tuyaux et contre-pression (diagramme espace-temps) ; philosophie Unix : interface uniforme, logique séparée du câblage, recâblages et filtre Python |
| [`ch10_cours_2_stockage_et_orchestration.ipynb`](ch10_cours_2_stockage_et_orchestration.ipynb) | *MapReduce and Distributed Filesystems* ; *MapReduce workflows* ; *Designing for frequent faults* | MapReduce et HDFS comme système d'exploitation distribué ; blocs de 128 Mo et mémoire du NameNode ; placement des répliques par rack et localité des tâches simulés ; parité XOR, réplication contre Reed-Solomon ; workflow quotidien en DAG, chemin critique, Gantt avec échec et relance ; préemption, formule (e^{λT} - 1)/λ démontrée et simulée, granularité optimale, diagramme espace-temps d'une relance |
| [`ch10_cours_3_mapreduce_et_jointures.ipynb`](ch10_cours_3_mapreduce_et_jointures.ipynb) | *MapReduce Job Execution* (*Distributed execution of MapReduce*, *MapReduce workflows*) ; *Reduce-Side Joins and Grouping* (*Handling skew*) ; *Map-Side Joins* | modèle MapReduce et analogie Unix ; shuffle (diagramme), vrais fichiers m(i)r(j) triés, compteurs ; piège de `hash` randomisé ; accélération multiprocessus ; chaîne de deux jobs (diagramme espace-temps) ; jointure tri-fusion avec tri secondaire et regroupement ; asymétrie et salage ; jointures par hachage diffusé, par hachage partitionné et par fusion, modèle de coût réseau |
| [`ch10_cours_4_moteurs_dataflow.ipynb`](ch10_cours_4_moteurs_dataflow.ipynb) | *Beyond MapReduce* : *Materialization of Intermediate State* (*Dataflow engines*, *Fault tolerance*), *Graphs and Iterative Processing*, *High-Level APIs and Languages* | barrières contre pipeline (simulation avec traînards) ; moteur dataflow `Dataset` : plan en étapes (diagramme), fusion, comparaison avec un job par opérateur ; recalcul par lignage, non-déterminisme démontré, intervalle de sauvegarde ; Pregel (diagramme des super-étapes), PageRank et composantes connexes ; SQL sur SQLite, ordre des jointures, exécution vectorisée, spécialisation par domaine |
| [`ch10_cours_5_sorties_et_hadoop.ipynb`](ch10_cours_5_sorties_et_hadoop.ipynb) | *The Output of Batch Workflows* (*Building search indexes*, *Key-value stores as batch process output*, *Philosophy of batch process outputs*) ; *Comparing Hadoop to Distributed Databases* | sorties versionnées et retour arrière par pointeur contre mise à jour en place (diagramme) ; index inversé construit par MapReduce ; écrire dans la base de production (surcharge M/M/1, doublons après relance) contre fichier trié et publication atomique ; Hadoop contre bases MPP : diversité du stockage et des traitements, relance de la requête entière contre relance de la tâche |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch10_exercices_01-05.ipynb`](ch10_exercices_01-05.ipynb) | 10.1 à 10.5 |
| [`ch10_exercices_06-10.ipynb`](ch10_exercices_06-10.ipynb) | 10.6 à 10.10 |

- **Ex. 10.1** - Table de hachage ou tri : ordres de grandeur d'une agrégation de 2 To, tri externe en une passe, vérification du coût en log(N/M).
- **Ex. 10.2** - Régler la largeur de fusion d'un tri externe : passes contre accès disque, disque dur et SSD.
- **Ex. 10.3** - Dimensionner un DFS : mémoire des métadonnées et petits fichiers, réplication contre RS(6, 3), trafic de réparation.
- **Ex. 10.4** - Préemption et granularité des tâches : surcoût 1 + o/T + λT/2, optimum √(2o/λ), effet des vagues et du maximum (simulation).
- **Ex. 10.5** - Le combineur : réduction du shuffle, et pourquoi une moyenne ne peut pas servir de combineur (somme, effectif).
- **Ex. 10.6** - Une jointure tri-fusion qui ne produit rien : clé de tri, de partitionnement et de regroupement ; jointure interne ou externe.
- **Ex. 10.7** - Choisir l'algorithme de jointure pour quatre cas (diffusion, shuffle, hachage partitionné), seuil G/(W-1).
- **Ex. 10.8** - Une célébrité dans la jointure : charge ×11, salage, plancher du hachage (simulation).
- **Ex. 10.9** - Plus courts chemins en Pregel sur un réseau routier, vérification par Dijkstra, vague de messages.
- **Ex. 10.10** - Écrire dans la base de production ou construire et basculer : durées, états mélangés après échec, idempotence.

## Lab (Docker)

| Notebook | Contenu |
|---|---|
| [`ch10_lab_postgres_lots.ipynb`](ch10_lab_postgres_lots.ipynb) | PostgreSQL 16 comme moteur de lots : tri externe selon `work_mem` (aussi rapide, voire plus, que le tri en mémoire), agrégation par hachage qui déborde en partitions, Hash Join, Merge Join et Nested Loop forcés, jointure par hachage partitionnée (Batches), chargement ligne à ligne, `executemany` et `COPY`, publication en place contre bascule par `RENAME` transactionnel observée par un lecteur concurrent |

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/batch/compose.yml -p ddia_batch up -d
docker compose -f docker/batch/compose.yml -p ddia_batch down -v
```

PostgreSQL écoute sur le port 11132 (utilisateur `ddia`, mot de passe `ddia`, base `lots`).
