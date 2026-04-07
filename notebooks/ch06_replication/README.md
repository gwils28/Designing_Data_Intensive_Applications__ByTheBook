# Chapitre 6 - Réplication

Le chapitre 6 étudie comment garder une copie des mêmes données sur plusieurs machines, et tout ce qui se complique quand ces données changent : réplication à leader unique (synchrone ou asynchrone, ajout de suiveurs, basculement, split brain), formats du journal de réplication, anomalies dues au retard des suiveurs et garanties de session, réplication multi-leader et résolution des conflits, réplication sans leader et quorums, détection des écritures concurrentes par vecteurs de versions, moteurs de synchronisation, logiciel local-first et CRDT. Les notebooks ajoutent ce que le livre laisse de côté : un leader, ses suiveurs et un contrôleur de basculement simulés message par message, des statistiques d'ordre pour la latence des écritures, des simulations de sessions qui mesurent les anomalies, un magasin à la Dynamo avec lecture réparatrice, transfert différé et arbres de Merkle, le modèle PBS de péremption, des vecteurs de versions pointés implémentés et testés, des CRDT et une transformation opérationnelle passés au fuzzing, et un lab sur la réplication réelle de PostgreSQL.

Les fonctions réutilisables (nœuds leader/suiveur avec époques et contrôleur de basculement, scénarios de panne et de partition, latences par statistiques d'ordre, mini-base pour comparer réplication par instructions et par lignes, groupe de suiveurs en retard et simulation de sessions, topologies multi-leader et dépassements, LWW et fusions de paniers, probabilités de quorum et simulation PBS, répliques et clients à la Dynamo, arbres de Merkle, serveur à numéro de version, vecteurs de versions pointés) sont dans [`ddia/replication.py`](../../ddia/replication.py), testées par [`tests/test_replication.py`](../../tests/test_replication.py). Les CRDT (G-Counter, PN-Counter, OR-Set, registre LWW, séquence RGA) et la transformation opérationnelle sont dans [`ddia/crdt.py`](../../ddia/crdt.py), testés par [`tests/test_crdt.py`](../../tests/test_crdt.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch06_cours_1_leader_unique.ipynb`](ch06_cours_1_leader_unique.ipynb) | introduction, encadré *Backups and Replication* ; *Single-Leader Replication* : *Synchronous Versus Asynchronous Replication*, *Setting Up New Followers*, encadré *Databases Backed by Object Storage*, *Handling Node Outages* | schéma leader-suiveurs (diagramme) ; suiveur synchrone et asynchrone (diagramme espace-temps) ; latence médiane et p99 des écritures selon le mode (statistiques d'ordre), disponibilité des écritures ; durée du rattrapage d'un nouveau suiveur ; panne et rattrapage d'un suiveur simulés ; rétention du journal ; basculement simulé avec contrôleur, écritures acquittées perdues, choix du suiveur promu ; délai d'expiration et basculements inutiles ; split brain sous partition, asynchrone contre quorum, fencing ; incident des clés réutilisées ; coût des lots sur stockage objet |
| [`ch06_cours_2_journaux_et_retard.ipynb`](ch06_cours_2_journaux_et_retard.ipynb) | *Implementation of Replication Logs* ; *Problems with Replication Lag* : *Reading Your Own Writes*, encadré *Regions and Availability Zones*, *Monotonic Reads*, *Consistent Prefix Reads*, *Solutions for Replication Lag* | divergences de la réplication par instructions (NOW, RAND, ordre) ; WAL physique et couplage à la version ; journal logique par lignes et volume des trois formats (diagramme) ; retard de suiveurs simulé avec blocage ; cinq stratégies de lecture comparées (hasard, leader, fenêtre, suiveur fixe, jeton de LSN) ; anomalies de lectures monotones selon le retard ; préfixe cohérent entre shards (diagrammes espace-temps) |
| [`ch06_cours_3_multi_leader.ipynb`](ch06_cours_3_multi_leader.ipynb) | *Multi-Leader Replication* : *Geographically Distributed Operation*, *Multi-leader replication topologies*, *Dealing with Conflicting Writes* (*Conflict avoidance*, *Last write wins*, *Manual conflict resolution*, *Automatic conflict resolution*, *Types of conflict*) | un leader par région (diagramme) et latence des écritures ; unicité violée ; topologies (diagramme), pannes et dépassements simulés ; conflit d'écriture (diagramme espace-temps) et probabilité de conflit 1 - exp(-λL) ; identifiants pairs/impairs ; LWW et décalage d'horloge ; panier qui ressuscite, pierres tombales, fusion déterministe ; compteur fusionné ; double réservation de salle |
| [`ch06_cours_4_sans_leader.ipynb`](ch06_cours_4_sans_leader.ipynb) | *Leaderless Replication* : *Writing to the Database When a Node Is Down*, *Catching up on missed writes*, *Using quorums for reading and writing*, *Understanding the limitations of quorum consistency*, *Monitoring staleness*, *Single-Leader Versus Leaderless Replication Performance*, *Multi-Region Operation* | écriture et lecture par quorum avec lecture réparatrice simulées (diagramme espace-temps) ; lecture réparatrice contre anti-entropie ; arbres de Merkle ; probabilité de lecture périmée et disponibilité selon (w, r) ; modèle PBS ; lecture qui recule et écriture en échec qui survit ; quorum approximatif et transfert différé simulés ; pannes grises et couverture de requêtes ; niveaux de cohérence multi-régions |
| [`ch06_cours_5_ecritures_concurrentes.ipynb`](ch06_cours_5_ecritures_concurrentes.ipynb) | *Detecting Concurrent Writes* : *The happens-before relation and concurrency*, encadré *Concurrency, Time, and Relativity*, *Capturing the happens-before relationship*, *Version vectors* | divergence sans règle de résolution (simulation) ; relation « arrivé avant » calculée par vecteurs (diagramme espace-temps, matrice des relations) ; le panier du livre rejoué pas à pas et son graphe causal ; explosion des frères ; vecteurs de versions pointés ; pertes d'écritures comparées : LWW, vecteur naïf, vecteur pointé |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch06_exercices_01-06.ipynb`](ch06_exercices_01-06.ipynb) | 6.1 à 6.6 |
| [`ch06_exercices_07-12.ipynb`](ch06_exercices_07-12.ipynb) | 6.7 à 6.12 |

- **Ex. 6.1** - Latence d'écriture par mode de réplication (statistiques d'ordre), écritures perdues lors d'une panne, survivre à la perte d'une zone.
- **Ex. 6.2** - Monter un suiveur sur 3 To : copie, rattrapage, journal à conserver, pic de charge qui empêche de rattraper.
- **Ex. 6.3** - Délai de basculement optimal à partir d'une distribution des silences ; numéros de commande réattribués après basculement.
- **Ex. 6.4** - Quelles instructions répliquer telles quelles ; rendre la réplication par instructions déterministe (mini-lab).
- **Ex. 6.5** - Dimensionner « lire ses écritures » : charge du leader avec une fenêtre, fenêtre contre jeton sous blocage d'un suiveur.
- **Ex. 6.6** - Probabilités des anomalies de lectures monotones et de préfixe cohérent, formules et Monte-Carlo.
- **Ex. 6.7** - Multi-leader : LWW et horloges NTP, identifiants modulo k, conflits d'un agenda hors ligne (simulation).
- **Ex. 6.8** - Choisir une topologie : messages, sauts, délais, disponibilité, dépassements simulés.
- **Ex. 6.9** - Choisir n, w, r : recouvrement, pannes tolérées, PBS, latence de queue, quorum approximatif.
- **Ex. 6.10** - Vecteurs de versions pointés à la main sur deux répliques, comparaison avec LWW.
- **Ex. 6.11** - Propriétés de la fusion d'un CRDT, 2P-Set contre OR-Set, test de convergence aléatoire.
- **Ex. 6.12** - Transformation opérationnelle à la main, départage dans un RGA, fuzzing d'une transformation boguée.

## Lab (Docker)

| Notebook | Contenu |
|---|---|
| [`ch06_lab_replication_postgresql.ipynb`](ch06_lab_replication_postgresql.ipynb) | PostgreSQL 16, un primaire et deux répliques en streaming montées par `pg_basebackup` : état de la réplication, retard mesuré à vide et sous charge, lecture périmée (rejeu suspendu) et « lire ses écritures » par jeton de LSN, niveaux de `synchronous_commit` et suiveur synchrone qui bloque les validations, décodage logique avec `test_decoding`, basculement qui perd des écritures confirmées, split brain |

Démarrage et arrêt (depuis la racine du dépôt ; ports 10632 à 10634) :

```bash
docker compose -f docker/replication/compose.yml -p ddia_replication up -d
docker compose -f docker/replication/compose.yml -p ddia_replication down -v
```
