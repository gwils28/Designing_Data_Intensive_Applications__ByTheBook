# Chapitre 9 - Les problèmes des systèmes distribués

Le chapitre 9 pousse le pessimisme au maximum : dans un système distribué, une partie peut être en panne pendant que le reste fonctionne, le réseau perd et retarde les messages sans borne, les horloges dérivent et sautent, et un processus peut se figer n'importe où sans le savoir. Le chapitre en tire les conséquences : un nœud ne peut pas se fier à son propre jugement, les décisions se prennent à la majorité, un ancien détenteur de bail doit être neutralisé par des jetons de clôture, et la correction d'un algorithme se raisonne dans un modèle de système, par des propriétés de sûreté et de vivacité. La 2e édition ajoute les limites de TCP et les méthodes de vérification (model checking, injection de fautes, simulation déterministe). Les notebooks rendent tout cela tangible : simulateur à événements discrets, diagrammes espace-temps, détecteur phi accrual, horloges NTP et TrueTime simulées, scénario du zombie rejoué en simulation et sur un vrai verrou Redis.

Les fonctions réutilisables (délais aller-retour à queue lourde, détecteurs de pannes à délai fixe et phi accrual, estimateur de retransmission de TCP, file d'un port de commutateur, circuits contre paquets, calculs NTP et algorithme de Marzullo, horloge NTP simulée, LWW et ses pertes, TrueTime et commit wait, pauses de ramasse-miettes, service de verrous à baux, stockage à jetons de clôture, clients figeables, appartenance par battements et vote majoritaire, algorithme OM(m) des généraux byzantins, sommes de contrôle, vérification d'un générateur de jetons) sont dans [`ddia/distsys.py`](../../ddia/distsys.py), testées par [`tests/test_distsys.py`](../../tests/test_distsys.py). Le model checker et la simulation déterministe sont dans [`ddia/distsys_verif.py`](../../ddia/distsys_verif.py), testés par [`tests/test_distsys_verif.py`](../../tests/test_distsys_verif.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch09_cours_1_pannes_partielles_et_reseaux.ipynb`](ch09_cours_1_pannes_partielles_et_reseaux.ipynb) | *Faults and Partial Failures* ; *Unreliable Networks* : *The Limitations of TCP*, *Network Faults in Practice*, *Fault Detection* | machine seule contre panne partielle (diagramme) ; « un sur un million, c'est tous les jours » ; non-déterminisme d'une même requête mesuré sur le simulateur ; quatre causes d'absence de réponse (diagramme espace-temps) ; relances, doublons et identifiants de requête ; une année de pannes réseau ; coupures partielles et asymétriques (qui soupçonne qui ?) ; signaux explicites contre délai, mesurés avec de vraies sockets ; limites de TCP : l'accusé de réception n'est pas une réponse (sockets réelles, politiques de reprise) |
| [`ch09_cours_2_delais_et_detection_des_pannes.ipynb`](ch09_cours_2_delais_et_detection_des_pannes.ipynb) | *Timeouts and Unbounded Delays* (*Network congestion and queueing*, encadré *TCP Versus UDP*, *Variability of network delays*) ; *Synchronous Versus Asynchronous Networks* (*Can we not simply make network delays predictable?*, encadré *Latency and Resource Utilization*, *Combining circuit switching and packet switching*) | queue lourde des allers-retours ; frontière délai de détection / fausses suspicions ; défaillance en cascade par fausses déclarations ; file d'un commutateur (attente, pertes) ; pertes changées en retransmissions de 200 ms ; voix sur UDP et échéance de lecture ; estimateur RTO de TCP face à un voisin bruyant ; détecteur phi accrual implémenté et comparé ; RNIS, circuits contre paquets, coût de la latence garantie |

## Labs (Docker)

| Notebook | Système | Contenu |
|---|---|---|

Services : [`docker/distsys/compose.yml`](../../docker/distsys/compose.yml) (projet `ddia_distsys`, ports 10901 et 10902).

```bash
docker compose -f docker/distsys/compose.yml -p ddia_distsys up -d     # démarrer
docker compose -f docker/distsys/compose.yml -p ddia_distsys down -v   # arrêter
```
