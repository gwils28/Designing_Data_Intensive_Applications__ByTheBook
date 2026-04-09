# Designing Data-Intensive Applications - *By The Book*

Notebooks Jupyter pédagogiques qui accompagnent la lecture de **Designing Data-Intensive Applications** (Martin Kleppmann et Chris Riccomini, O'Reilly, 2e édition, 2026) :
cours illustrés, implémentations « à la main » des structures et des protocoles qui font
fonctionner les bases de données et les systèmes distribués, simulations, diagrammes
d'architecture et **exercices corrigés en détail**, chapitre par chapitre.

Le dépôt a d'abord suivi la 1re édition du livre (2017, 12 chapitres). Il a été
réorganisé en 2026 selon la 2e édition (14 chapitres), et complété par les notions
qu'elle introduit : architectures cloud native, index vectoriels, DataFrames, sync
engines et logiciels local-first, exécution durable, méthodes formelles et tests
aléatoires, GraphQL, cadre légal...

## Objectif et public

Les notebooks sont écrits en français pour un lecteur qui **sait programmer en Python** et
connaît les bases de SQL, mais découvre l'ingénierie des systèmes de données : stockage,
réplication, partitionnement, transactions, cohérence, traitement par lots et de flux.

Le livre explique les idées sans code ni exercices. Ces notebooks ajoutent ce qu'il faut
pour **comprendre en profondeur** :

- chaque concept est motivé, expliqué par une intuition, puis formalisé (complexités,
  probabilités, quorums, files d'attente, ordres de grandeur) ;
- chaque mécanisme est **réimplémenté en Python** puis éprouvé : journaux et LSM-trees,
  B-trees, encodeurs binaires, réplication et quorums, moteur MVCC, horloges, consensus,
  MapReduce, fenêtres de flux... ;
- les protocoles sont rejoués sur un **simulateur de réseau déterministe** (délais,
  pertes, partitions, pannes, horloges imparfaites) et représentés par des **diagrammes
  espace-temps** ;
- les architectures sont représentées par des **diagrammes** ;
- chaque chapitre propose **8 à 12 exercices** conçus pour ce dépôt (compréhension,
  calculs de dimensionnement, mini-labs de code), tous corrigés.

Chaque exercice suit le même plan :

1. **énoncé** autonome ;
2. **rappels** des notions du cours utilisées (avec renvoi à la section du livre) ;
3. **solution** détaillée pas à pas ;
4. **vérification** numérique, expérimentale et/ou graphique ;
5. un court **bilan** (« À retenir »).

## Organisation du dépôt

```
notebooks/chXX_<theme>/   notebooks de cours (chXX_cours_*), d'exercices (chXX_exercices_*)
                          et labs sur de vrais systèmes (chXX_lab_*)
ddia/                     paquet Python : style graphique, diagrammes, diagrammes
                          espace-temps, simulateur de réseau, implémentations « à la
                          main » des mécanismes de chaque chapitre
docker/                   fichiers docker compose des labs
tests/                    tests unitaires (pytest)
tools/                    construction et exécution des notebooks
```

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m ipykernel install --user --name ddia
jupyter notebook
```

Aucun service externe n'est nécessaire pour les notebooks de cours et d'exercices : tout
tourne en local, en Python pur (plus SQLite). Les notebooks du dépôt sont fournis **déjà
exécutés** : sorties et figures sont visibles directement sur GitHub.

Les **labs** confrontent les simulations à de vrais systèmes (PostgreSQL, Kafka, Redis,
etcd...) lancés avec Docker. Chaque lab indique la commande à utiliser, par exemple :

```bash
docker compose -f docker/transactions/compose.yml -p ddia_transactions up -d
# ... exécuter le notebook du lab ...
docker compose -f docker/transactions/compose.yml -p ddia_transactions down -v
```

Lancer les tests : `pytest tests/`.

## Avancement

| Chapitre | Thème | Statut |
|---|---|---|
| | *Partie I - Fondements des systèmes de données* | |
| 1 | [Compromis dans l'architecture des systèmes de données](notebooks/ch01_compromis_architecture) | ✅ |
| 2 | [Définir les exigences non fonctionnelles](notebooks/ch02_exigences_non_fonctionnelles) | ✅ |
| 3 | [Modèles de données et langages de requête](notebooks/ch03_modeles_de_donnees) | ✅ |
| 4 | [Stockage et recherche](notebooks/ch04_stockage_et_recherche) | ✅ |
| 5 | [Encodage et évolution](notebooks/ch05_encodage_et_evolution) | ✅ |
| | *Partie II - Données distribuées* | |
| 6 | [Réplication](notebooks/ch06_replication) | ✅ |
| 7 | [Sharding](notebooks/ch07_sharding) | 🚧 |
| 8 | Transactions | 🚧 |
| 9 | Les problèmes des systèmes distribués | 🚧 |
| 10 | Cohérence et consensus | 🚧 |
| | *Partie III - Données dérivées* | |
| 11 | Traitement par lots | 🚧 |
| 12 | Traitement de flux | 🚧 |
| 13 | Une philosophie des systèmes de streaming | 🚧 |
| 14 | Faire ce qui est juste | 🚧 |

## Labs Docker

- [Chapitre 3 - Lab : tables, JSONB et requêtes récursives dans PostgreSQL](notebooks/ch03_modeles_de_donnees/ch03_lab_postgres_jsonb_et_recursif.ipynb)
- [Chapitre 4 - Lab : les index de PostgreSQL vus de l'intérieur](notebooks/ch04_stockage_et_recherche/ch04_lab_postgres_index.ipynb)
- [Chapitre 5 - Lab : évolution de schéma dans PostgreSQL](notebooks/ch05_encodage_et_evolution/ch05_lab_postgres_evolution.ipynb)
- [Chapitre 6 - Lab : Redis Cluster, un partitionnement par hachage grandeur nature](notebooks/ch06_partitionnement/ch06_lab_redis_cluster.ipynb)
- [Chapitre 6 - Lab : réplication en streaming avec PostgreSQL](notebooks/ch06_replication/ch06_lab_replication_postgresql.ipynb)
- [Chapitre 7 - Lab : niveaux d'isolation réels de PostgreSQL](notebooks/ch07_transactions/ch07_lab_isolation_postgresql.ipynb)
- [Chapitre 8 - Lab : verrou Redis, pause de processus et jetons de clôture](notebooks/ch08_problemes_systemes_distribues/ch08_lab_verrou_redis_et_clotures.ipynb)
- [Chapitre 9 - Lab : validation à deux phases avec PostgreSQL](notebooks/ch09_coherence_et_consensus/ch09_lab_2pc_postgresql.ipynb)
- [Chapitre 9 - Lab : consensus et coordination avec etcd](notebooks/ch09_coherence_et_consensus/ch09_lab_etcd.ipynb)
- [Chapitre 10 - Lab : les algorithmes du traitement par lots dans PostgreSQL](notebooks/ch10_traitement_par_lots/ch10_lab_postgres_lots.ipynb)
- [Chapitre 11 - Lab : capture des changements avec le décodage logique de PostgreSQL](notebooks/ch11_traitement_de_flux/ch11_lab_cdc_postgresql.ipynb)
- [Chapitre 11 - Lab : un journal partitionné réel avec Kafka](notebooks/ch11_traitement_de_flux/ch11_lab_kafka.ipynb)
- [Chapitre 12 - Lab : dégrouper la base avec PostgreSQL et Redis](notebooks/ch12_avenir_des_systemes_de_donnees/ch12_lab_degroupage_postgresql.ipynb)
