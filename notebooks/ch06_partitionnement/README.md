# Chapitre 6 - Partitionnement

Quand le volume de données ou le débit de requêtes dépasse ce qu'une machine peut absorber, on découpe les données en **partitions** placées sur des nœuds différents. Le chapitre 6 montre comment découper (par plage de clés ou par hachage), que faire des clés chaudes, comment indexer des données partitionnées, comment rééquilibrer quand le cluster change de taille et comment router les requêtes. Les notebooks ajoutent la profondeur quantitative que le livre laisse de côté : déséquilibre « balles dans des urnes », fraction de clés déplacées mesurée pour chaque stratégie, anneau de hachage cohérent, loi de Zipf, routage simulé avec cartes périmées, latence de queue du scatter/gather, défaillance en cascade d'un rééquilibrage automatique, et un vrai Redis Cluster.

Les fonctions réutilisables (hachage stable, `mix64`, CRC16 et slots de Redis Cluster, partitionnement par plage avec bornes par quantiles, partitionnement dynamique à scissions, haché modulo N, nombre fixe de morceaux avec poids, anneau de hachage à jetons aléatoires, mesures de déséquilibre et de clés déplacées, requêtes Zipf et salage, mesures de capteurs, index secondaires locaux et globaux, scatter/gather, simulation du routage sur `ddia.sim` avec coordinateur, diffusion épidémique, modèle de cascade de rééquilibrage) sont dans [`ddia/partitioning.py`](../../ddia/partitioning.py), testées par [`tests/test_partitioning.py`](../../tests/test_partitioning.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch06_cours_1_plages_et_hachage.ipynb`](ch06_cours_1_plages_et_hachage.ipynb) | introduction et encadré *Terminological confusion* ; *Partitioning and Replication* ; *Partitioning of Key-Value Data* : *Partitioning by Key Range*, *Partitioning by Hash of Key* | partitionnement et réplication combinés (placement calculé, diagramme) ; déséquilibre « balles dans des urnes » et gain n / déséquilibre ; encyclopédie : bornes fixes contre quantiles, requêtes par préfixe ; point chaud des horodatages (cartes de chaleur des écritures) et clé (capteur, horodatage) ; `hash()` de Python non stable entre processus ; hachage d'horodatages consécutifs ; partitions touchées par une requête de plage (mesure et formule) ; clé composée à la Cassandra |
| [`ch06_cours_2_reequilibrage.ipynb`](ch06_cours_2_reequilibrage.ipynb) | *Rebalancing Partitions* : *Strategies for Rebalancing* (*How not to do it: hash mod N*, *Fixed number of partitions*, *Dynamic partitioning*, *Partitioning proportionally to nodes*), encadré *Consistent Hashing*, *Operations: Automatic or Manual Rebalancing* | borne inférieure 1/(n+1) (diagramme) ; mod N : grille, preuve N/(N+1) et mesure ; nombre fixe de partitions : croissance de 3 à 12 nœuds, machines hétérogènes, divisibilité, taille des partitions contre volume (diagramme) ; partitionnement dynamique : nombre de partitions par vagues, amplification d'écriture, pré-découpage, clés croissantes ; anneau à jetons (dessin), déséquilibre selon le nombre de jetons, donneurs lors d'un ajout ; durée d'un transfert, cascade d'exclusions simulée et seuil de charge |
| [`ch06_cours_3_points_chauds_et_routage.ipynb`](ch06_cours_3_points_chauds_et_routage.ipynb) | *Skewed Workloads and Relieving Hot Spots* ; *Request Routing* : *Parallel Query Execution* | charge Zipf par partition et plancher n·w1 ; message viral et capacité d'un nœud ; salage dimensionné (collisions de sous-clés), compteur salé avec registre, détection par échantillonnage ; placement des clés chaudes et répartition de la clé n°1 ; trois architectures de routage (diagramme) simulées sur `ddia.sim` ; service de coordination (diagramme), redirection `MOVED` sur carte périmée (diagramme espace-temps), redirections selon le délai de notification ; diffusion épidémique ; accélération d'une requête analytique parallèle |
| [`ch06_cours_4_index_secondaires.ipynb`](ch06_cours_4_index_secondaires.ipynb) | *Partitioning and Secondary Indexes* : *Partitioning Secondary Indexes by Document*, *Partitioning Secondary Indexes by Term* | dispersion des valeurs indexées sur toutes les partitions (20 000 annonces de voitures) ; index locaux (diagramme, écriture sur une seule partition), dérive d'un index maintenu à la main quand une écriture échoue à moitié ; scatter/gather : 1-(1-p)^n, p50/p99 simulés jusqu'à 128 partitions, débit plafonné à C/f (loi d'Amdahl) ; index globaux (diagramme) : écritures sur 2 à 3 partitions, point chaud des termes populaires, conjonctions et listes de postings transférées, mise à jour asynchrone (diagramme espace-temps, probabilité d'une lecture périmée) ; modèle de coût local contre global, point de bascule selon le rapport lectures/écritures, tableau récapitulatif |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch06_exercices_01-06.ipynb`](ch06_exercices_01-06.ipynb) | 6.1 à 6.6 |
| [`ch06_exercices_07-10.ipynb`](ch06_exercices_07-10.ipynb) | 6.7 à 6.10 |

- **Ex. 6.1** - Choisir une clé de partitionnement (capteurs, commandes, réseau social) : déséquilibre en écriture, morceaux lus par requête, célébrité en loi de Zipf (simulation).
- **Ex. 6.2** - Balles dans des urnes : écart relatif en $\sqrt{2\ln n/\lambda}$, nombre de clés par morceau pour un équilibre à 10 % (calcul affiné et simulation).
- **Ex. 6.3** - Modulo N déplace $N/(N+1)$ des clés (démonstration par les restes chinois) contre $1/(N+1)$ ; part d'un nœud sur l'anneau en $1/\sqrt{v}$ (vérification).
- **Ex. 6.4** - Dimensionner un nombre fixe de partitions pour une croissance de 2 à 200 To, diviseurs de 5 040, coût des scissions générales.
- **Ex. 6.5** - Mini-lab : partitionnement dynamique avec fusion après une purge, et oscillation scission/fusion sans hystérésis.
- **Ex. 6.6** - Salage d'une clé chaude : nombre de suffixes avec collisions du hachage (probabilité exacte et simulation), amplification des lectures, agrégat en cache.
- **Ex. 6.7** - Trouver l'anomalie : écriture acquittée puis perdue chez l'ancien propriétaire (diagrammes espace-temps), correction par contrôle de propriété et époque de la carte (mini-lab).
- **Ex. 6.8** - Scatter/gather : $1-(1-p)^n$, percentiles simulés selon n, requêtes de couverture déclenchées au p95.
- **Ex. 6.9** - Index local ou global : modèle de coût, seuil de bascule selon la part d'écritures, vérification avec `LocalIndexDB` / `GlobalIndexDB`, limites du modèle.
- **Ex. 6.10** - Durée des transferts de rééquilibrage, carte de la défaillance en cascade (utilisation × surcoût de migration), patience du détecteur.

## Lab (Docker)

| Notebook | Contenu |
|---|---|
| [`ch06_lab_redis_cluster.ipynb`](ch06_lab_redis_cluster.ipynb) | Redis Cluster à trois puis quatre maîtres (`docker/partitioning/compose.yml`, ports 10701-10704) : les 16 384 slots comme nombre fixe de partitions (carte et diagramme) ; notre CRC16 identique à `CLUSTER KEYSLOT` sur 1 000 clés, répartition par nœud et par slot (CRC16 contre MD5 et loi de Poisson) ; redirections `MOVED` (diagramme espace-temps), correction slot par slot contre rechargement de la carte, coût d'une redirection ; hash tags, `CROSSSLOT` et transaction locale, point chaud d'un tag de 30 000 clés ; ajout d'un nœud avec `rebalance` : 25 % de clés déplacées contre 75 % pour un modulo ; migration manuelle d'un slot (`IMPORTING` / `MIGRATING`, `MIGRATE`, `ASK` / `ASKING`, puis `MOVED`) ; clé chaude Zipf et compteur salé lus dans `INFO commandstats` |

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning up -d     # démarrer
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning down -v   # arrêter et effacer
```

Les nœuds utilisent le réseau de l'hôte (Linux) pour que les redirections `MOVED` et `ASK` désignent des adresses joignables depuis le notebook.
