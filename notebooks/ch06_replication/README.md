# Chapitre 6 - Réplication

Le chapitre 6 étudie comment garder une copie des mêmes données sur plusieurs machines, et tout ce qui se complique quand ces données changent : réplication à leader unique (synchrone ou asynchrone, ajout de suiveurs, basculement, split brain), formats du journal de réplication, anomalies dues au retard des suiveurs et garanties de session, réplication multi-leader et résolution des conflits, réplication sans leader et quorums, détection des écritures concurrentes par vecteurs de versions, moteurs de synchronisation, logiciel local-first et CRDT. Les notebooks ajoutent ce que le livre laisse de côté : un leader, ses suiveurs et un contrôleur de basculement simulés message par message, des statistiques d'ordre pour la latence des écritures, des simulations de sessions qui mesurent les anomalies, un magasin à la Dynamo avec lecture réparatrice, transfert différé et arbres de Merkle, le modèle PBS de péremption, des vecteurs de versions pointés implémentés et testés, des CRDT et une transformation opérationnelle passés au fuzzing, et un lab sur la réplication réelle de PostgreSQL.

Les fonctions réutilisables (nœuds leader/suiveur avec époques et contrôleur de basculement, scénarios de panne et de partition, latences par statistiques d'ordre, mini-base pour comparer réplication par instructions et par lignes, groupe de suiveurs en retard et simulation de sessions, topologies multi-leader et dépassements, LWW et fusions de paniers, probabilités de quorum et simulation PBS, répliques et clients à la Dynamo, arbres de Merkle, serveur à numéro de version, vecteurs de versions pointés) sont dans [`ddia/replication.py`](../../ddia/replication.py), testées par [`tests/test_replication.py`](../../tests/test_replication.py). Les CRDT (G-Counter, PN-Counter, OR-Set, registre LWW, séquence RGA) et la transformation opérationnelle sont dans [`ddia/crdt.py`](../../ddia/crdt.py), testés par [`tests/test_crdt.py`](../../tests/test_crdt.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch06_cours_1_leader_unique.ipynb`](ch06_cours_1_leader_unique.ipynb) | introduction, encadré *Backups and Replication* ; *Single-Leader Replication* : *Synchronous Versus Asynchronous Replication*, *Setting Up New Followers*, encadré *Databases Backed by Object Storage*, *Handling Node Outages* | schéma leader-suiveurs (diagramme) ; suiveur synchrone et asynchrone (diagramme espace-temps) ; latence médiane et p99 des écritures selon le mode (statistiques d'ordre), disponibilité des écritures ; durée du rattrapage d'un nouveau suiveur ; panne et rattrapage d'un suiveur simulés ; rétention du journal ; basculement simulé avec contrôleur, écritures acquittées perdues, choix du suiveur promu ; délai d'expiration et basculements inutiles ; split brain sous partition, asynchrone contre quorum, fencing ; incident des clés réutilisées ; coût des lots sur stockage objet |

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt ; ports 10632 à 10634) :

```bash
docker compose -f docker/replication/compose.yml -p ddia_replication up -d
docker compose -f docker/replication/compose.yml -p ddia_replication down -v
```
