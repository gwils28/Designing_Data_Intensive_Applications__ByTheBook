# Chapitre 9 - Les problèmes des systèmes distribués

Le chapitre 9 pousse le pessimisme au maximum : dans un système distribué, une partie peut être en panne pendant que le reste fonctionne, le réseau perd et retarde les messages sans borne, les horloges dérivent et sautent, et un processus peut se figer n'importe où sans le savoir. Le chapitre en tire les conséquences : un nœud ne peut pas se fier à son propre jugement, les décisions se prennent à la majorité, un ancien détenteur de bail doit être neutralisé par des jetons de clôture, et la correction d'un algorithme se raisonne dans un modèle de système, par des propriétés de sûreté et de vivacité. La 2e édition ajoute les limites de TCP et les méthodes de vérification (model checking, injection de fautes, simulation déterministe). Les notebooks rendent tout cela tangible : simulateur à événements discrets, diagrammes espace-temps, détecteur phi accrual, horloges NTP et TrueTime simulées, scénario du zombie rejoué en simulation et sur un vrai verrou Redis.

Les fonctions réutilisables (délais aller-retour à queue lourde, détecteurs de pannes à délai fixe et phi accrual, estimateur de retransmission de TCP, file d'un port de commutateur, circuits contre paquets, calculs NTP et algorithme de Marzullo, horloge NTP simulée, LWW et ses pertes, TrueTime et commit wait, pauses de ramasse-miettes, service de verrous à baux, stockage à jetons de clôture, clients figeables, appartenance par battements et vote majoritaire, algorithme OM(m) des généraux byzantins, sommes de contrôle, vérification d'un générateur de jetons) sont dans [`ddia/distsys.py`](../../ddia/distsys.py), testées par [`tests/test_distsys.py`](../../tests/test_distsys.py). Le model checker et la simulation déterministe sont dans [`ddia/distsys_verif.py`](../../ddia/distsys_verif.py), testés par [`tests/test_distsys_verif.py`](../../tests/test_distsys_verif.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch09_cours_1_pannes_partielles_et_reseaux.ipynb`](ch09_cours_1_pannes_partielles_et_reseaux.ipynb) | *Faults and Partial Failures* ; *Unreliable Networks* : *The Limitations of TCP*, *Network Faults in Practice*, *Fault Detection* | machine seule contre panne partielle (diagramme) ; « un sur un million, c'est tous les jours » ; non-déterminisme d'une même requête mesuré sur le simulateur ; quatre causes d'absence de réponse (diagramme espace-temps) ; relances, doublons et identifiants de requête ; une année de pannes réseau ; coupures partielles et asymétriques (qui soupçonne qui ?) ; signaux explicites contre délai, mesurés avec de vraies sockets ; limites de TCP : l'accusé de réception n'est pas une réponse (sockets réelles, politiques de reprise) |
| [`ch09_cours_2_delais_et_detection_des_pannes.ipynb`](ch09_cours_2_delais_et_detection_des_pannes.ipynb) | *Timeouts and Unbounded Delays* (*Network congestion and queueing*, encadré *TCP Versus UDP*, *Variability of network delays*) ; *Synchronous Versus Asynchronous Networks* (*Can we not simply make network delays predictable?*, encadré *Latency and Resource Utilization*, *Combining circuit switching and packet switching*) | queue lourde des allers-retours ; frontière délai de détection / fausses suspicions ; défaillance en cascade par fausses déclarations ; file d'un commutateur (attente, pertes) ; pertes changées en retransmissions de 200 ms ; voix sur UDP et échéance de lecture ; estimateur RTO de TCP face à un voisin bruyant ; détecteur phi accrual implémenté et comparé ; RNIS, circuits contre paquets, coût de la latence garantie |
| [`ch09_cours_3_horloges_non_fiables.ipynb`](ch09_cours_3_horloges_non_fiables.ipynb) | *Unreliable Clocks* : *Monotonic Versus Time-of-Day Clocks*, *Clock Synchronization and Accuracy*, *Relying on Synchronized Clocks* (*Timestamps for ordering events*, *Clock readings with a confidence interval*, *Synchronized clocks for global snapshots*) | horloges de Python ; horloge NTP simulée (dérive, ralentissement, recalages) et durées faussées ; dérive en ppm ; échange NTP (diagramme espace-temps), borne δ/2, Marzullo contre un serveur menteur ; lissage des secondes intercalaires ; LWW qui perd un incrément (simulation, diagramme espace-temps) et taux de pertes selon l'écart ; TrueTime, intervalles disjoints, commit wait et son coût |
| [`ch09_cours_4_pauses_baux_et_jetons.ipynb`](ch09_cours_4_pauses_baux_et_jetons.ipynb) | *Process Pauses* (*Providing response time guarantees*, *Limiting the impact of garbage collection*) ; *Knowledge, Truth, and Lies* : *The Majority Rules*, *Distributed Locks and Leases* (*Fencing off zombies and delayed requests*, *Fencing with multiple replicas*) | boucle naïve du bail (diagramme espace-temps) ; vrai processus figé par SIGSTOP ; loi des pauses ; collectes cachées au répartiteur ; faute asymétrique et pause déclarées par la majorité (simulation) ; intersection des majorités ; zombie qui corrompt le stockage, requête retardée, jetons de clôture (simulations et diagrammes) ; balayage de la durée de pause ; jeton dans les bits de poids fort pour des répliques sans leader |
| [`ch09_cours_5_byzantin_et_modeles_de_systeme.ipynb`](ch09_cours_5_byzantin_et_modeles_de_systeme.ipynb) | *Byzantine Faults* (encadré *The Byzantine Generals Problem*, *Uses of Byzantine fault tolerance*, *Weak forms of lying*) ; *System Model and Reality* (*Defining the correctness of an algorithm*, *Distinguishing between safety and liveness*, *Mapping system models to the real world*) | deux généraux (diagramme espace-temps) ; OM(m) contre toutes les stratégies de mensonge, n > 3f ; coût en messages ; bogues communs et implémentations indépendantes ; somme d'Internet contre CRC-32 ; modèles de temps et de pannes, nœud boiteux ; sûreté et vivacité sur un générateur de jetons amnésique ; quorum cassé par un nœud amnésique |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch09_exercices_01-06.ipynb`](ch09_exercices_01-06.ipynb) | 9.1 à 9.6 |
| [`ch09_exercices_07-12.ipynb`](ch09_exercices_07-12.ipynb) | 9.7 à 9.12 |

- **Ex. 9.1** - Pannes partielles à l'échelle d'un parc : pannes quotidiennes, requêtes éclatées, effet de la redondance (calcul et Monte-Carlo).
- **Ex. 9.2** - Relances, idempotence et durée de rétention des identifiants de requête (raisonnement et mini-lab sur le simulateur).
- **Ex. 9.3** - Délai d'expiration face à une queue lourde : retards indépendants contre épisodes corrélés, vote de plusieurs observateurs (calcul et simulation).
- **Ex. 9.4** - Le détecteur phi accrual à la main, puis face à une gigue qui triple (calcul et mini-lab).
- **Ex. 9.5** - Tampon de commutateur : formule M/M/1/K, compromis pertes/délai, trafic en rafales (calcul et simulation).
- **Ex. 9.6** - Budget d'horloge, calcul NTP à partir de quatre horodatages, serveurs menteurs et Marzullo (calcul).
- **Ex. 9.7** - LWW et écritures perdues : approximation, simulation, correction par lecture préalable (calcul et simulation).
- **Ex. 9.8** - Commit wait de Spanner : incertitude en dent de scie, attente, débit d'une donnée chaude (calcul).
- **Ex. 9.9** - Bail, pause et marge : écritures hors bail par jour, effet de la marge et des jetons de clôture (calcul et simulation).
- **Ex. 9.10** - Majorités, menteurs et propriétés : intersections de quorums, n > 3f, sûreté ou vivacité (raisonnement).
- **Ex. 9.11** - Model checking d'un verrou : sans bail il se bloque, avec bail il n'exclut plus, la clôture réconcilie les deux (mini-lab).
- **Ex. 9.12** - Simulation déterministe : trouver, rejouer et réduire un bogue de reprise, réfuter une correction plausible (mini-lab).

## Labs (Docker)

| Notebook | Système | Contenu |
|---|---|---|

Services : [`docker/distsys/compose.yml`](../../docker/distsys/compose.yml) (projet `ddia_distsys`, ports 10901 et 10902).

```bash
docker compose -f docker/distsys/compose.yml -p ddia_distsys up -d     # démarrer
docker compose -f docker/distsys/compose.yml -p ddia_distsys down -v   # arrêter
```
