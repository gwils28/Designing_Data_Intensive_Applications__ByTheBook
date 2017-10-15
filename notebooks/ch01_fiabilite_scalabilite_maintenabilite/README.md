# Chapitre 1 - Fiabilité, scalabilité et maintenabilité

Le chapitre 1 pose le vocabulaire de tout le livre : un système de données doit être fiable (continuer à fonctionner malgré les fautes matérielles, logicielles et humaines), scalable (faire face à la croissance de la charge, décrite par ses paramètres et mesurée par des percentiles de temps de réponse) et maintenable (opérable, simple, évolutif). L'exemple des timelines de Twitter sert de fil rouge. Les notebooks ajoutent la profondeur quantitative que le livre laisse de côté : simulation du fan-out sur un graphe en loi de puissance, histogrammes de percentiles, théorie des files d'attente, modèles de durabilité, injection de fautes, loi universelle de scalabilité.

Les fonctions réutilisables (graphe social et coûts du fan-out, timelines à la lecture, à l'écriture et hybrides, temps de réponse synthétiques, percentiles, histogramme log-linéaire à la HdrHistogram et fenêtre glissante, simulation de files FIFO à c serveurs, formules M/M/1, M/M/c et Pollaczek-Khinchine, AFR, MTTF et MTTDL, simulation de pertes de données avec pannes corrélées, disponibilités, fan-out tolérant aux fautes pour l'injection de fautes, lois d'Amdahl et USL avec ajustement) sont dans [`ddia/requirements.py`](../../ddia/requirements.py), testées par [`tests/test_requirements.py`](../../tests/test_requirements.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch01_cours_1_timelines_et_charge.ipynb`](ch01_cours_1_timelines_et_charge.ipynb) | *Thinking About Data Systems* ; *Scalability* : *Describing Load* | système de données composite (diagramme) ; schéma relationnel et requête de timeline sur SQLite (plan de requête, coût linéaire dans le nombre de comptes suivis) ; chiffres de Twitter (2012) : calcul à la lecture contre fan-out ; timelines matérialisées implémentées et vérifiées (diagramme) ; graphe social en loi de puissance, part du fan-out des plus gros comptes, délai de livraison d'un message de célébrité ; approche hybride : pourquoi elle ne réduit pas le travail moyen mais borne le pire cas ; paramètres de charge, moyenne contre pics sur une semaine simulée |
| [`ch01_cours_2_performance_et_percentiles.ipynb`](ch01_cours_2_performance_et_percentiles.ipynb) | *Scalability* : *Describing Performance*, encadré *Percentiles in Practice* | débit et temps de réponse ; anatomie d'un temps de réponse (diagramme espace-temps) et mesure côté client contre côté serveur (simulation avec retransmissions TCP) ; distributions, moyenne, médiane, p99, p999, sessions de plusieurs requêtes ; histogramme log-linéaire (précision contre mémoire), fenêtre glissante avec incident, agrégation par somme d'histogrammes ; amplification de la latence de queue (formule et simulation) ; SLO et SLA, fausses alertes dues au hasard, budget d'indisponibilité |
| [`ch01_cours_3_files_d_attente.ipynb`](ch01_cours_3_files_d_attente.ipynb) | *Scalability* : *Describing Performance* (attentes en file, blocage en tête de file, générateurs de charge) | courbe en crosse de hockey M/M/1 (théorie et simulation) ; hasard des arrivées et loi de Little vérifiée ; Pollaczek-Khinchine et blocage en tête de file (diagramme espace-temps, préjudice des requêtes rapides) ; file commune contre files séparées, dimensionnement par Erlang C ; générateur de charge en boucle ouverte contre boucle fermée (omission coordonnée) |
| [`ch01_cours_4_fiabilite.ipynb`](ch01_cours_4_fiabilite.ipynb) | *Reliability* : *Hardware Faults*, *Software Errors*, *Human Errors*, *How Important Is Reliability?* | fautes, défaillances, point de défaillance unique (diagramme), disponibilité en série et en redondance, « k sur n » ; fan-out tolérant aux fautes sur le simulateur (diagramme espace-temps d'une reprise) et injection de fautes aléatoires (pertes, doublons, idempotence) ; pannes de disques quotidiennes (simulation d'un an, loi de Poisson) ; MTTDL démontré et simulé, pannes corrélées, mises à jour progressives ; bogues communs et défaillances en cascade simulées ; erreurs humaines et déploiements progressifs simulés |
| [`ch01_cours_5_scalabilite_et_maintenabilite.ipynb`](ch01_cours_5_scalabilite_et_maintenabilite.ipynb) | *Scalability* : *Approaches for Coping with Load* ; introduction de la partie II (mémoire partagée, disques partagés, rien de partagé) ; *Maintainability* : *Operability*, *Simplicity*, *Evolvability* | ressources à performance constante (Erlang C, effet d'échelle) ; mémoire partagée, disques partagés, rien de partagé (diagramme), coût de la montée en gamme ; Amdahl et USL, ajustement sur des mesures et prévision du pic ; pas de sauce magique, élasticité simulée sur une journée ; opérabilité et automatisation ; simplicité mesurée par le rayon d'impact des modifications (graphes de dépendances) ; évolutivité et refactoring |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch01_exercices_01-06.ipynb`](ch01_exercices_01-06.ipynb) | 1.1 à 1.6 |
| [`ch01_exercices_07-10.ipynb`](ch01_exercices_07-10.ipynb) | 1.7 à 1.10 |

- **Ex. 1.1** - Ordres de grandeur du fan-out, seuil lectures/écritures démontré, retard et rattrapage après un pic (simulation fluide).
- **Ex. 1.2** - Seuil de l'approche hybride : indépendance vis-à-vis de F sous coût linéaire, seuil F* quand les comptes populaires publient plus, vérification sur graphe, contrainte de pire cas.
- **Ex. 1.3** - Percentiles à la main, effet d'une valeur aberrante, p99 global par somme d'histogrammes contre moyennes de percentiles.
- **Ex. 1.4** - Amplification de la latence de queue sur 40 appels, quantile par appel, requêtes de couverture (formule, simulation, surcoût).
- **Ex. 1.5** - Échantillonnage par réservoir (preuve et implémentation) contre histogramme log-linéaire pour les hauts percentiles.
- **Ex. 1.6** - Loi de Little et pool de connexions : Erlang C, simulation, effet d'un ralentissement de la base (cascade).
- **Ex. 1.7** - Blocage en tête de file : file commune, serveurs dédiés ou priorité (simulateur à événements).
- **Ex. 1.8** - Pannes de disques, MTTDL selon le nombre de copies et le temps de reconstruction, pannes corrélées de lot (calcul et simulation).
- **Ex. 1.9** - Disponibilité d'une chaîne de services, redondance, budget d'indisponibilité, mises à jour progressives, simulation d'états.
- **Ex. 1.10** - Ajuster l'USL sur un test de charge, plafond du cluster, comparaison avec une machine unique, changer d'architecture.

Pas de lab Docker pour ce chapitre : les notions sont mieux servies par des simulations déterministes.
