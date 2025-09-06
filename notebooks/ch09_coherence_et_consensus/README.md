# Chapitre 9 - Cohérence et consensus

Le chapitre 9 précise ce que veut dire « cohérence forte » et comment l'obtenir sans sacrifier la tolérance aux pannes. Il définit la **linéarisabilité** (l'illusion d'une copie unique), montre quand elle est indispensable et ce qu'elle coûte (CAP, latence) ; il étudie les **garanties d'ordre** (causalité, numéros de séquence, horloges de Lamport, diffusion à ordre total) ; il aborde enfin les **transactions distribuées et le consensus** : validation à deux phases, consensus tolérant aux pannes et services de coordination. Les notebooks ajoutent ce que le livre laisse de côté : un vérificateur de linéarisabilité, un registre sur quorum simulé, Paxos et Raft implémentés sur le simulateur et soumis à des pannes, un modèle de service de coordination, et des labs sur de vrais systèmes (PostgreSQL pour la validation à deux phases, etcd pour le consensus).

Les fonctions réutilisables (historiques et vérificateur de linéarisabilité, tracé des historiques avec leurs points de linéarisation, registre sur quorum en modes Dynamo et ABD, horloges de Lamport et vectorielles, messagerie simulée et mesure des inversions causales, générateurs d'identifiants distribués, diffusion à ordre total par séquenceur, Paxos à une décision, Raft avec client et vérifications de sûreté, disponibilité et latence des quorums, modèle de service de coordination avec révisions, baux, clés éphémères et séquentielles et surveillances) sont dans [`ddia/consistency.py`](../../ddia/consistency.py), testées par [`tests/test_consistency.py`](../../tests/test_consistency.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch09_cours_1_linearisabilite.ipynb`](ch09_cours_1_linearisabilite.ipynb) | *Consistency Guarantees* ; *Linearizability* : *What Makes a System Linearizable?*, encadré *Linearizability Versus Serializability*, *Relying on Linearizability*, *Implementing Linearizable Systems* (*Linearizability and quorums*), *The Cost of Linearizability* (*The CAP theorem*, encadré *The Unhelpful CAP Theorem*, *Linearizability and network delays*) | cohérence à terme contre cohérence forte ; le site sportif non linéarisable (diagramme espace-temps) ; registre, intervalles et points de linéarisation ; vérificateur de linéarisabilité (Wing et Gong mémoïsé) appliqué à des historiques avec CAS ; sérialisabilité stricte (diagramme) ; verrous, unicité, course entre deux canaux (transcodeur, probabilité d'incident) ; réplications linéarisables ou non, quorums stricts non linéarisables, algorithme ABD mesuré sur 1 200 simulations et son coût ; partition entre deux sites, CP contre AP sur une journée, latence en fonction de l'incertitude des délais |
| [`ch09_cours_2_ordre_et_horloges_logiques.ipynb`](ch09_cours_2_ordre_et_horloges_logiques.ipynb) | *Ordering Guarantees* : *Ordering and Causality* (*The causal order is not a total order*, *Linearizability is stronger than causal consistency*, *Capturing causal dependencies*), *Sequence Number Ordering* (*Noncausal sequence number generators*, *Lamport timestamps*, *Timestamp ordering is not sufficient*) | causalité et ordre partiel (matrice des relations), cohérence causale ; compteur central et latence par site ; générateurs par partition, par blocs, par horloge murale : inversions mesurées ; horloges de Lamport (diagramme, zéro inversion causale contre horloges décalées) ; horloges vectorielles et coût ; pourquoi les horodatages ne suffisent pas à une contrainte |
| [`ch09_cours_3_validation_atomique_2pc.ipynb`](ch09_cours_3_validation_atomique_2pc.ipynb) | *Distributed Transactions and Consensus* : *Atomic Commit and Two-Phase Commit (2PC)*, *Distributed Transactions in Practice* | transactions distribuées et validation atomique : de la validation locale à la validation distribuée, protocole 2PC, système de promesses, panne du coordinateur et participants en doute, validation à trois phases, transactions hétérogènes et XA, verrous tenus en doute, reprise, limites des transactions distribuées |
| [`ch09_cours_4_consensus.ipynb`](ch09_cours_4_consensus.ipynb) | *Ordering Guarantees* : *Total Order Broadcast* (*Using total order broadcast*, *Implementing linearizable storage using total order broadcast*, *Implementing total order broadcast using linearizable storage*) ; *Distributed Transactions and Consensus* : introduction, *Fault-Tolerant Consensus* (encadré *The Impossibility of Consensus*) | pourquoi le consensus ; accord, intégrité, validité, terminaison ; majorités et pannes tolérées ; Paxos à une décision sur le simulateur (diagramme, 300 exécutions avec pannes) ; FLP et duels de proposeurs, rôle du hasard ; diffusion à ordre total (registre de noms, réplication de machine à états), numéros sans trou par registre linéarisable ; équivalences CAS / diffusion à ordre total / consensus |
| [`ch09_cours_5_raft.ipynb`](ch09_cours_5_raft.ipynb) | *Fault-Tolerant Consensus* : *Consensus algorithms and total order broadcast*, *Single-leader replication and consensus*, *Epoch numbering and quorums*, *Limitations of consensus* | de la réplication à leader unique au consensus ; époques et quorums qui se recoupent (diagramme) ; Raft implémenté sur le simulateur : élection (diagramme espace-temps), frise des mandats, réplication du journal et validation à la majorité ; partition et leader isolé (journaux avant et après) ; lectures locales non linéarisables contre lectures par le journal ; restriction d'élection ; latence et messages selon la taille, délais d'élection et élections inutiles, lien défaillant |
| [`ch09_cours_6_services_de_coordination.ipynb`](ch09_cours_6_services_de_coordination.ipynb) | *Membership and Coordination Services* : *Allocating work to nodes*, *Service discovery*, *Membership services* | architecture d'un service de coordination (diagramme) ; verrou, bail expiré pendant une pause et jeton de clôture (diagramme espace-temps) ; durée des sessions : fausses expirations contre détection ; élection par clés éphémères séquentielles, effet de troupeau ; répartition du travail et réattribution après une panne ; pourquoi un petit ensemble fixe ; découverte de services, caches et observateurs ; services d'appartenance |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch09_exercices_01-05.ipynb`](ch09_exercices_01-05.ipynb) | 9.1 à 9.5 |
| [`ch09_exercices_06-10.ipynb`](ch09_exercices_06-10.ipynb) | 9.6 à 9.10 |

- **Ex. 9.1** - Linéarisable ou pas ? Quatre historiques (écritures concurrentes, CAS, écriture sans réponse, lecture longue) jugés à la main puis par le vérificateur.
- **Ex. 9.2** - Le coût d'un vérificateur de linéarisabilité : générateur d'historiques, recherche naïve contre mémoïsée, effet de la concurrence et des historiques fautifs.
- **Ex. 9.3** - Quorums sur 5 nœuds, délais variables et réparation synchrone : taux d'historiques non linéarisables et messages par opération (Dynamo contre ABD).
- **Ex. 9.4** - Placer le leader d'une base linéarisable sur trois centres de données : latences par site, partition, lectures locales.
- **Ex. 9.5** - Linéarisable, sérialisable ou les deux ? Et la course entre une notification et la réplication (formule exponentielle, simulation, correction par version).
- **Ex. 9.6** - Horloges de Lamport et vectorielles à la main sur une trace de trois nœuds.
- **Ex. 9.7** - Identifiants horodatés et décalage des horloges : probabilité d'inversion (loi triangulaire) démontrée et simulée.
- **Ex. 9.8** - Taille et placement d'une grappe de consensus face aux pannes de zones : calcul exact et Monte-Carlo.
- **Ex. 9.9** - Casser Raft : sans restriction d'élection, une écriture confirmée disparaît (10 graines).
- **Ex. 9.10** - Régler le délai d'élection : temps sans leader selon le délai et la dispersion du réseau (courbe en U).

## Labs (Docker)

| Notebook | Système | Contenu |
|---|---|---|
| [`ch09_lab_2pc_postgresql.ipynb`](ch09_lab_2pc_postgresql.ipynb) | PostgreSQL (`docker/transactions/compose.yml`) | validation à deux phases avec `PREPARE TRANSACTION` : coordinateur journalisé entre deux bases, transactions en doute et verrous tenus après une panne du coordinateur, survie à un redémarrage, reprise par le journal, décision heuristique, coût en latence |
| [`ch09_lab_etcd.ipynb`](ch09_lab_etcd.ipynb) | grappe etcd de 3 nœuds (`docker/consistency/compose.yml`) | révisions et CAS (20 réservations concurrentes, une seule gagne) ; baux et clés éphémères ; verrou dont le bail expire pendant une pause et transaction gardée ; lecture sérialisable périmée et lecture linéarisable impossible sur un nœud isolé, coût des lectures linéarisables ; gel du leader et nouvelle élection ; perte du quorum |

Démarrer et arrêter la grappe etcd (ports hôtes 11001 à 11003) :

```bash
docker compose -f docker/consistency/compose.yml -p ddia_consistency up -d
docker compose -f docker/consistency/compose.yml -p ddia_consistency down -v
```
