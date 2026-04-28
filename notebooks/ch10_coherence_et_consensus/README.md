# Chapitre 10 - Cohérence et consensus

Le chapitre 10 précise ce que veut dire « cohérence forte » et comment l'obtenir sans sacrifier la tolérance aux pannes. Il définit la **linéarisabilité** (l'illusion d'une copie unique), montre quand elle est indispensable et ce qu'elle coûte (CAP, latence) ; il relie ensuite la cohérence à la **génération d'identifiants et d'horodatages** (horloges de Lamport, horloges hybrides, générateurs linéarisables) ; il montre enfin que verrous, unicité, journaux, compteurs et validation atomique sont des formes d'un même problème, le **consensus**, résolu en pratique par Raft, Paxos ou Zab et exposé aux applications par les **services de coordination**. Les notebooks ajoutent ce que le livre laisse de côté : un vérificateur de linéarisabilité, un registre sur quorum simulé, Paxos et Raft implémentés sur le simulateur et soumis à des pannes, un modèle de service de coordination, et un lab sur une vraie grappe etcd.

Les fonctions réutilisables (historiques et vérificateur de linéarisabilité, tracé des historiques avec leurs points de linéarisation, registre sur quorum en modes Dynamo et ABD, horloges de Lamport et vectorielles, messagerie simulée et mesure des inversions causales, générateurs d'identifiants distribués, diffusion à ordre total par séquenceur, Paxos à une décision, Raft avec client et vérifications de sûreté, disponibilité et latence des quorums, modèle de service de coordination avec révisions, baux, clés éphémères et séquentielles et surveillances) sont dans [`ddia/consistency.py`](../../ddia/consistency.py), testées par [`tests/test_consistency.py`](../../tests/test_consistency.py). Les horloges logiques hybrides et l'oracle d'horodatage par lots sont dans [`ddia/consistency_clocks.py`](../../ddia/consistency_clocks.py), testés par [`tests/test_consistency_clocks.py`](../../tests/test_consistency_clocks.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch10_cours_1_linearisabilite.ipynb`](ch10_cours_1_linearisabilite.ipynb) | introduction du chapitre ; *Linearizability* : *What Makes a System Linearizable?*, encadré *Linearizability Versus Serializability*, *Relying on Linearizability*, *Implementing Linearizable Systems*, *The Cost of Linearizability* (*The CAP theorem*, encadré *The Unhelpful CAP Theorem*, *Linearizability and network delays*) | cohérence à terme contre cohérence forte ; le site sportif non linéarisable (diagramme espace-temps) ; registre, intervalles et points de linéarisation ; vérificateur de linéarisabilité (Wing et Gong mémoïsé) appliqué à des historiques avec CAS ; sérialisabilité stricte (diagramme) ; verrous, unicité, course entre deux canaux (transcodeur, probabilité d'incident) ; réplications linéarisables ou non, quorums stricts non linéarisables, algorithme ABD mesuré sur 1 200 simulations et son coût ; partition entre deux sites, CP contre AP sur une journée, PACELC, latence en fonction de l'incertitude des délais |
| [`ch10_cours_2_ordre_et_horloges_logiques.ipynb`](ch10_cours_2_ordre_et_horloges_logiques.ipynb) | *ID Generators and Logical Clocks* : *Logical Clocks*, *Lamport timestamps*, *Hybrid logical clocks*, *Lamport/hybrid logical clocks versus vector clocks*, *Linearizable ID Generators* (*Implementing a linearizable ID generator*, *Enforcing constraints using logical clocks*) | causalité et ordre partiel (matrice des relations) ; compteur central et latence par région ; générateurs par partition, par blocs, par horloge murale : inversions mesurées ; UUID v4, identifiants horodatés, paradoxe des anniversaires ; horloges de Lamport (diagramme, zéro inversion causale contre horloges décalées) ; horloges vectorielles et coût ; pourquoi les horodatages ne suffisent pas à une contrainte ; horloges logiques hybrides en simulation ; la photo privée, oracle d'horodatage par lots, attente de l'incertitude de Spanner |
| [`ch10_cours_3_consensus.ipynb`](ch10_cours_3_consensus.ipynb) | *Consensus* (encadré *The Impossibility of Consensus*) ; *The Many Faces of Consensus* : *Single-value consensus*, *Compare-and-set as consensus*, *Shared logs as consensus*, *Fetch-and-add as consensus*, *Atomic commitment as consensus* ; *Consensus in Practice* : *Using shared logs* | pourquoi le consensus ; accord, intégrité, validité, terminaison ; majorités et pannes tolérées ; Paxos à une décision sur le simulateur (diagramme, 300 exécutions avec pannes) ; FLP et duels de proposeurs, rôle du hasard ; journal partagé et diffusion à ordre total (registre de noms, réplication de machine à états) ; équivalences CAS / consensus ; fetch-and-add et nombre de consensus 2 ; validation atomique par consensus |
| [`ch10_cours_4_raft.ipynb`](ch10_cours_4_raft.ipynb) | *Consensus in Practice* : *From single-leader replication to consensus*, *Subtleties of consensus* (encadré *Consistency Versus Availability in Leader Election*), *Pros and cons of consensus* | de la réplication à leader unique au consensus ; mandats et quorums qui se recoupent (diagramme) ; Raft implémenté sur le simulateur : élection (diagramme espace-temps), frise des mandats, réplication du journal et validation à la majorité ; partition et leader isolé (journaux avant et après) ; lectures locales non linéarisables contre lectures par le journal ; restriction d'élection ; élection non propre ; latence et messages selon la taille, délais d'élection et élections inutiles, lien défaillant, pré-vote |
| [`ch10_cours_5_services_de_coordination.ipynb`](ch10_cours_5_services_de_coordination.ipynb) | *Coordination Services* : *Locks and leases*, *Support for fencing*, *Failure detection*, *Change notifications*, encadré *Managing Configuration with Coordination Services*, *Allocating work to nodes*, *Service discovery* | architecture d'un service de coordination (diagramme) ; verrou, bail expiré pendant une pause et jeton de clôture (diagramme espace-temps) ; durée des sessions : fausses expirations contre détection ; élection par clés éphémères séquentielles, effet de troupeau ; répartition du travail et réattribution après une panne ; pourquoi un petit ensemble fixe ; découverte de services, caches et observateurs |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch10_exercices_01-05.ipynb`](ch10_exercices_01-05.ipynb) | 10.1 à 10.5 |

- **Ex. 10.1** - Linéarisable ou pas ? Quatre historiques (écritures concurrentes, CAS, écriture sans réponse, lecture longue) jugés à la main puis par le vérificateur.
- **Ex. 10.2** - Le coût d'un vérificateur de linéarisabilité : générateur d'historiques, recherche naïve contre mémoïsée, effet de la concurrence et des historiques fautifs.
- **Ex. 10.3** - Quorums sur 5 nœuds, délais variables et réparation synchrone : taux d'historiques non linéarisables et messages par opération (Dynamo contre ABD).
- **Ex. 10.4** - Placer le leader d'une base linéarisable sur trois centres de données : latences par site, partition, lectures locales.
- **Ex. 10.5** - Linéarisable, sérialisable ou les deux ? Et la course entre une notification et la réplication (formule exponentielle, simulation, correction par version).

## Labs (Docker)

| Notebook | Système | Contenu |
|---|---|---|

Démarrer et arrêter la grappe etcd (ports hôtes 11001 à 11003) :

```bash
docker compose -f docker/consistency/compose.yml -p ddia_consistency up -d
docker compose -f docker/consistency/compose.yml -p ddia_consistency down -v
```


Les transactions distribuées et la validation à deux phases (2PC), traitées au chapitre 9 dans la 1re édition, sont au chapitre 8 dans cette édition (cours 5 et lab PostgreSQL) ; le cours 3 ci-dessus montre que la validation atomique est une forme de consensus.
