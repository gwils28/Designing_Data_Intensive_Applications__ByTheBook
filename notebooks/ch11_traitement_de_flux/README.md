# Chapitre 11 - Traitement de flux

Le chapitre 11 passe des entrées bornées du traitement par lots aux **flux** d'événements, qui ne se terminent jamais. Il compare les deux grandes familles de courtiers de messages (files de style AMQP/JMS et journaux partitionnés), montre qu'une base de données est elle-même une source de flux (capture des changements, event sourcing, dualité entre état et journal immuable), puis étudie le traitement des flux : usages, raisonnement sur le temps (temps de l'événement, filigranes, fenêtres), jointures et tolérance aux pannes. Les notebooks ajoutent ce que le livre laisse de côté : un journal partitionné implémenté avec le partitionneur exact de Kafka, des simulations de pannes, de courses et de retards, des mesures (HyperLogLog, vues incrémentales, percolateur), et deux labs sur de vrais systèmes.

Les fonctions réutilisables (politiques de débordement, file partagée avec relivraison, blocage en tête de file, partitionneur murmur2 de Kafka, journal partitionné avec segments, rétention, compaction et offsets de groupes, commits d'offsets avec pannes, double écriture contre CDC, CDC par déclencheurs sur SQLite et index plein texte dérivé, magasin d'événements à contrôle optimiste, détection de motifs, HyperLogLog, percolateur, fenêtres fixes, par sauts, glissantes et de session, compteur à filigrane, jointures flux-flux, flux-table et table-table, job à état avec quatre sémantiques de livraison, puits avec fencing, formule de Young) sont dans [`ddia/streams.py`](../../ddia/streams.py), testées par [`tests/test_streams.py`](../../tests/test_streams.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch11_cours_1_courtiers_de_messages.ipynb`](ch11_cours_1_courtiers_de_messages.ipynb) | *Transmitting Event Streams* : *Messaging Systems* (*Direct messaging from producers to consumers*, *Message brokers*, *Message brokers compared to databases*, *Multiple consumers*, *Acknowledgments and redelivery*) | événements, producteurs, consommateurs ; coût du sondage contre notification ; perte, tampon ou contre-pression face à un pic (simulation) ; messagerie directe et compteurs faussés par les pertes UDP ; courtier de messages (diagramme) et comparaison avec une base ; répartition de charge, diffusion et groupes (diagramme) ; acquittements, relivraison et réordonnancement (diagramme espace-temps, inversions mesurées) |
| [`ch11_cours_2_journaux_partitionnes.ipynb`](ch11_cours_2_journaux_partitionnes.ipynb) | *Partitioned Logs* : *Using logs for message storage*, *Logs compared to traditional messaging*, *Consumer offsets*, *Disk space usage*, *When consumers cannot keep up with producers*, *Replaying old messages* | sujet partitionné, offsets et clés (diagramme, partitionneur murmur2) ; affectation des partitions dans un groupe, plafond de parallélisme, blocage en tête de file simulé ; offsets validés : doublons ou pertes selon l'intervalle de validation ; calcul de la durée de rétention, consommateur en panne qui dépasse la rétention ; rejeu d'un consommateur v2 à côté de la v1 |
| [`ch11_cours_3_capture_des_changements.ipynb`](ch11_cours_3_capture_des_changements.ipynb) | *Databases and Streams* : *Keeping Systems in Sync*, *Change Data Capture* (*Implementing change data capture*, *Initial snapshot*, *Log compaction*, *API support for change streams*) | journal de réplication et machine à états ; systèmes à garder en phase (diagramme) ; course de la double écriture (diagramme espace-temps) et divergence mesurée selon la concurrence et les échecs ; CDC par déclencheurs sur SQLite et index plein texte dérivé (diagramme) ; instantané initial : trois méthodes comparées ; compaction de journal simulée et reconstruction depuis l'offset 0 |
| [`ch11_cours_4_event_sourcing_et_immutabilite.ipynb`](ch11_cours_4_event_sourcing_et_immutabilite.ipynb) | *Event Sourcing* (*Deriving current state from the event log*, *Commands and events*) ; *State, Streams, and Immutability* (*Advantages of immutable events*, *Deriving several views from the same event log*, *Concurrency control*, *Limitations of immutability*) | l'état comme intégrale du flux ; commandes et événements (diagramme), salle de spectacle en event sourcing, rejeu et instantanés, pourquoi on ne compacte pas par clé ; grand livre, reprise après un bogue, hésitations visibles dans le journal ; CQRS et nouvelle vue par rejeu (diagramme) ; lecture de ses propres écritures, contrôle optimiste ; taux de renouvellement et suppressions |
| [`ch11_cours_5_usages_du_traitement_de_flux.ipynb`](ch11_cours_5_usages_du_traitement_de_flux.ipynb) | *Processing Streams* ; *Uses of Stream Processing* (*Complex event processing*, *Stream analytics*, *Maintaining materialized views*, *Search on streams*, *Message passing and RPC*) | pipeline d'opérateurs (diagramme) ; CEP : détection d'attaques par force brute ; HyperLogLog (erreur contre mémoire) ; vue matérialisée incrémentale contre recalcul (SQLite) ; percolateur : requêtes indexées ; acteurs contre flux |
| [`ch11_cours_6_raisonner_sur_le_temps.ipynb`](ch11_cours_6_raisonner_sur_le_temps.ipynb) | *Reasoning About Time* : *Event time versus processing time*, *Knowing when you're ready*, *Whose clock are you using, anyway?*, *Types of windows* | faux pic dû au temps de traitement après un redéploiement ; filigrane : compromis fraîcheur, complétude, mémoire et corrections ; filigrane de plusieurs producteurs (diagramme espace-temps) ; correction des horloges d'appareils par trois horodatages ; quatre types de fenêtres visualisés, taille de l'état |
| [`ch11_cours_7_jointures_et_tolerance_aux_pannes.ipynb`](ch11_cours_7_jointures_et_tolerance_aux_pannes.ipynb) | *Stream Joins* (*Stream-stream join*, *Stream-table join*, *Table-table join*, *Time-dependence of joins*) ; *Fault Tolerance* (*Microbatching and checkpointing*, *Atomic commit revisited*, *Idempotence*, *Rebuilding state after a failure*) | recherches et clics : fenêtre contre exactitude et état ; enrichissement par copie locale (diagramme) ; timelines matérialisées vérifiées contre SQL (règle du produit) ; jointure non déterministe et dimensions à évolution lente ; micro-lots, checkpoints par barrières (diagramme espace-temps), formule de Young ; quatre sémantiques sous pannes ; idempotence et fencing d'un zombie (diagramme espace-temps) ; reconstruction de l'état |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch11_exercices_01-06.ipynb`](ch11_exercices_01-06.ipynb) | 11.1 à 11.6 |
| [`ch11_exercices_07-11.ipynb`](ch11_exercices_07-11.ipynb) | 11.7 à 11.11 |

- **Ex. 11.1** - Sondage contre notification : charge, requêtes vides, délai, seuil λT = 1 (calcul et simulation).
- **Ex. 11.2** - Dimensionner les partitions d'un sujet, partition la plus chargée avec des clés Zipf, clés déplacées par un changement du nombre de partitions.
- **Ex. 11.3** - Rétention d'un cluster répliqué, consommateur arrêté un week-end, durée du rattrapage (calcul et simulation).
- **Ex. 11.4** - Fréquence de validation des offsets, doublons par jour, effets externes.
- **Ex. 11.5** - Trouver l'anomalie d'un cache invalidé par double écriture (diagramme espace-temps, simulation) et la corriger par CDC et versions.
- **Ex. 11.6** - Taille d'un sujet compacté : seuil de saleté, amplification d'espace, pierres tombales (calcul et mini-lab).
- **Ex. 11.7** - Compte bancaire en event sourcing : écriture biaisée sans contrôle de version, instantanés, nouvelle projection.
- **Ex. 11.8** - Fenêtres fixes, par sauts, glissantes et sessions à la main, vérifiées par le code.
- **Ex. 11.9** - Marge d'un filigrane : borne par le quantile des délais contre retardataires réellement écartés.
- **Ex. 11.10** - Jointure recherches-clics : fenêtre pour 99 % des clics, taille de l'état par la loi de Little, simulation.
- **Ex. 11.11** - Idempotence par offset, traitement non déterministe et fencing d'une instance zombie (mini-lab).

## Labs (Docker)

Les deux labs partagent le fichier [`docker/streams/compose.yml`](../../docker/streams/compose.yml) (projet `ddia_streams`, Kafka sur le port 11292, PostgreSQL sur le port 11232) :

```bash
docker compose -f docker/streams/compose.yml -p ddia_streams up -d --wait
docker compose -f docker/streams/compose.yml -p ddia_streams down -v
```

| Notebook | Contenu |
|---|---|
| [`ch11_lab_cdc_postgresql.ipynb`](ch11_lab_cdc_postgresql.ipynb) | PostgreSQL 16, décodage logique (`test_decoding`) : slot de réplication et LSN, ordre des validations, `REPLICA IDENTITY`, index dérivé contre double écriture concurrente, instantané initial raccordé par l'instantané MVCC, WAL retenu par un slot non consommé |
