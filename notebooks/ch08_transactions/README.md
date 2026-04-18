# Chapitre 8 - Transactions

Le chapitre 8 explique ce que garantit vraiment une transaction : l'atomicité (abandon sûr, journal), la cohérence (invariants de l'application), l'isolation et la durabilité. L'essentiel porte sur l'isolation : les niveaux faibles (read committed, snapshot isolation et MVCC), les anomalies qu'ils laissent passer (lecture biaisée, mise à jour perdue, write skew, fantômes) et les trois façons d'obtenir la sérialisabilité (exécution en série, verrouillage à deux phases, SSI). La fin du chapitre traite des transactions distribuées : validation à deux phases (2PC), XA, transactions internes des bases NewSQL et traitement « exactement une fois » par idempotence. Les notebooks rejouent chaque anomalie sur un mini-moteur MVCC écrit à la main, simulent 2PC sous pannes, et reproduisent le tout sur un vrai PostgreSQL.

Les fonctions réutilisables (journal d'écriture anticipée avec sommes de contrôle, moteur transactionnel MVCC à six niveaux d'isolation avec verrous, verrous de prédicat et détection SSI, ordonnanceur de transactions écrites comme des générateurs, test de sérialisabilité, graphe de précédence, frises d'exécution, charge synthétique, simulation de 2PC sur `ddia.sim`) sont dans [`ddia/transactions.py`](../../ddia/transactions.py), testées par [`tests/test_transactions.py`](../../tests/test_transactions.py). Le traitement « exactement une fois » par déduplication est dans [`ddia/exactly_once.py`](../../ddia/exactly_once.py) ([`tests/test_exactly_once.py`](../../tests/test_exactly_once.py)).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch08_cours_1_acid_et_atomicite.ipynb`](ch08_cours_1_acid_et_atomicite.ipynb) | introduction ; *What Exactly Is a Transaction?* (*The Meaning of ACID*, *Replication and Durability*, *Single-Object and Multi-Object Operations*, *Handling errors and aborts*) | points de défaillance (diagramme) ; ACID lettre par lettre, la course de deux incréments ; journal d'écriture anticipée implémenté, panne injectée à chaque octet contre écriture en place ; fsync mesuré, validation groupée, pannes corrélées et réplication ; lecture sale d'un compteur dénormalisé, atomicité multi-objets avec SQLite ; emballement des réessais sous charge |
| [`ch08_cours_2_isolation_faible.ipynb`](ch08_cours_2_isolation_faible.ipynb) | *Weak Isolation Levels* : *Read Committed*, *Snapshot Isolation and Repeatable Read* | les 20 entrelacements de deux incréments à chaque niveau ; lecture sale, écriture sale (frises) ; verrous de lecture contre deux versions ; lecture biaisée et sauvegardes incohérentes (300 essais) ; MVCC (versions `created_by` / `deleted_by`, chaîne de versions), règles de visibilité (matrice), ramasse-miettes et transaction longue, B-arbre en copie sur écriture ; la confusion des noms |
| [`ch08_cours_3_mises_a_jour_perdues_et_write_skew.ipynb`](ch08_cours_3_mises_a_jour_perdues_et_write_skew.ipynb) | *Preventing Lost Updates* ; *Write Skew and Phantoms* | pertes selon la concurrence ; incrément atomique, `FOR UPDATE`, interblocage et graphe d'attente, détection automatique (PostgreSQL contre InnoDB), CAS et réessais, LWW contre CRDT ; médecins de garde, graphe de précédence cyclique ; fantômes de réservation, contrainte d'unicité, matérialisation des conflits ; tableau expérimental de six anomalies sous sept niveaux |
| [`ch08_cours_4_serialisabilite.ipynb`](ch08_cours_4_serialisabilite.ipynb) | *Serializability* : *Actual Serial Execution*, *Two-Phase Locking*, *Serializable Snapshot Isolation* | sérialisabilité par force brute et par graphe de précédence ; transaction interactive contre procédure stockée (espace-temps, débit), déterminisme et réplication, plafond des transactions multi-shards ; 2PL (compatibilité, deux phases, preuve), lecture longue qui bloque les écritures, verrous de prédicat et faux conflits des verrous d'intervalle ; SSI (prémisses périmées, algorithme simplifié et preuve) ; série, 2PL et SSI sous contention |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch08_exercices_01-04.ipynb`](ch08_exercices_01-04.ipynb) | 8.1 à 8.4 |
| [`ch08_exercices_05-08.ipynb`](ch08_exercices_05-08.ipynb) | 8.5 à 8.8 |
| [`ch08_exercices_09-12.ipynb`](ch08_exercices_09-12.ipynb) | 8.9 à 8.12 |

- **Ex. 8.1** - Journal et fsync : débit avec et sans validation groupée, simulation de la latence ; journal mal ordonné et journal sans somme de contrôle cassés par injection de pannes.
- **Ex. 8.2** - Diagnostiquer six historiques (anomalie, niveau minimal) et vérifier sur le moteur.
- **Ex. 8.3** - Visibilité MVCC à la main ; gonflement des tables par une transaction longue (formule et simulation).
- **Ex. 8.4** - Le « paradoxe des anniversaires » des mises à jour perdues (formule exacte, simulation) et le coût du CAS.
- **Ex. 8.5** - Atelier à places limitées : quand `FOR UPDATE` suffit, matérialisation du conflit.
- **Ex. 8.6** - Graphes de précédence à la main ; 400 entrelacements aléatoires, preuve expérimentale que 2PL est sérialisable.
- **Ex. 8.7** - Dimensionner une base à exécution en série shardée (modèle et simulation).
- **Ex. 8.8** - 2PL contre SSI selon la part de lectures seules (mini-lab).
- **Ex. 8.9** - Coût de 2PC (latence, fsync, verrous) et fréquence des transactions en doute.
- **Ex. 8.10** - Injection exhaustive de doubles pannes dans 2PC ; validation en une phase en comparaison.
- **Ex. 8.11** - Protocole de terminaison coopératif : quels blocages il lève, lesquels restent.
- **Ex. 8.12** - Exactement une fois par déduplication : l'ordre des étapes, une variante fautive, l'espace occupé.

## Labs (Docker)

Les labs utilisent PostgreSQL 16 (`docker/transactions/compose.yml`, port hôte 10832) :

```bash
docker compose -f docker/transactions/compose.yml -p ddia_transactions up -d
docker compose -f docker/transactions/compose.yml -p ddia_transactions down -v
```

| Notebook | Contenu |
|---|---|
| [`ch08_lab_isolation_postgresql.ipynb`](ch08_lab_isolation_postgresql.ipynb) | pas de lecture sale même en `READ UNCOMMITTED` ; lecture biaisée en `READ COMMITTED` et instantané en `REPEATABLE READ` ; `xmin`/`xmax`, pages brutes (`pageinspect`) et `VACUUM` bloqué par une transaction longue ; mises à jour perdues sous charge (8 fils) et quatre remèdes ; write skew des médecins et fantôme de réservation (`40001` en `SERIALIZABLE`) ; interblocage réel (`40P01`) |
