# Chapitre 7 - Sharding

Quand le volume de données ou le débit d'écriture dépasse ce qu'une machine peut absorber, on découpe les données en **shards** placés sur des nœuds différents. Le chapitre 7 montre pourquoi (et quand ne pas le faire), comment découper (par plage de clés ou par hachage), comment rééquilibrer quand le cluster change de taille, que faire des clés chaudes, comment router les requêtes et comment indexer des données découpées ; il traite aussi le sharding au service de la multi-location. Les notebooks ajoutent la profondeur quantitative que le livre laisse de côté : déséquilibre « balles dans des urnes », fraction de clés déplacées mesurée pour chaque stratégie, anneau de hachage cohérent, loi de Zipf, routage simulé avec cartes périmées, latence de queue du scatter/gather, défaillance en cascade d'un rééquilibrage automatique, et un vrai Redis Cluster.

Les fonctions réutilisables (hachage stable, `mix64`, CRC16 et slots de Redis Cluster, partitionnement par plage avec bornes par quantiles, partitionnement dynamique à scissions, haché modulo N, nombre fixe de morceaux avec poids, anneau de hachage à jetons aléatoires, mesures de déséquilibre et de clés déplacées, requêtes Zipf et salage, mesures de capteurs, index secondaires locaux et globaux, scatter/gather, simulation du routage sur `ddia.sim` avec coordinateur, diffusion épidémique, modèle de cascade de rééquilibrage) sont dans [`ddia/partitioning.py`](../../ddia/partitioning.py), testées par [`tests/test_partitioning.py`](../../tests/test_partitioning.py). Les fonctions de multi-location (tailles de locataires, rangement first-fit decreasing, voisin bruyant, croissance des locataires, transactions mono-shard) sont dans [`ddia/partitioning_tenants.py`](../../ddia/partitioning_tenants.py), testées par [`tests/test_partitioning_tenants.py`](../../tests/test_partitioning_tenants.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch07_cours_1_plages_et_hachage.ipynb`](ch07_cours_1_plages_et_hachage.ipynb) | introduction et encadré *Sharding and Partitioning* ; *Sharding of Key-Value Data* : *Sharding by Key Range*, *Sharding by Hash of Key*, encadré *Partitioning and Range Queries in Data Warehouses* | sharding et réplication combinés (placement calculé, diagramme) ; déséquilibre « balles dans des urnes » et gain n / déséquilibre ; encyclopédie : bornes fixes contre quantiles, requêtes par préfixe ; point chaud des horodatages (cartes de chaleur des écritures) et clé (capteur, horodatage) ; `hash()` de Python non stable entre processus ; hachage d'horodatages consécutifs ; morceaux touchés par une requête de plage (mesure et formule) ; clé composée à la Cassandra |
| [`ch07_cours_2_reequilibrage.ipynb`](ch07_cours_2_reequilibrage.ipynb) | *Rebalancing key-range sharded data*, *Hash modulo number of nodes*, *Fixed number of shards*, *Sharding by hash range*, *Consistent hashing* ; *Operations: Automatic Versus Manual Rebalancing* | borne inférieure 1/(n+1) (diagramme) ; mod N : grille, preuve N/(N+1) et mesure ; nombre fixe de shards : croissance de 3 à 12 nœuds, machines hétérogènes, divisibilité, taille des shards contre volume (diagramme) ; scissions : nombre de shards par vagues, amplification d'écriture, pré-découpage, clés croissantes ; anneau à jetons (dessin), déséquilibre selon le nombre de jetons, donneurs lors d'un ajout ; durée d'un transfert, cascade d'exclusions simulée et seuil de charge ; rendez-vous et jump hash comparés |
| [`ch07_cours_3_points_chauds_et_routage.ipynb`](ch07_cours_3_points_chauds_et_routage.ipynb) | *Skewed Workloads and Relieving Hot Spots* ; *Request Routing* | charge Zipf par shard et plancher n·w1 ; message viral et capacité d'un nœud ; salage dimensionné (collisions de sous-clés), compteur salé avec registre, détection par échantillonnage ; placement des clés chaudes et répartition de la clé n°1 ; trois architectures de routage (diagramme) simulées sur `ddia.sim` ; service de coordination (diagramme), redirection `MOVED` sur carte périmée (diagramme espace-temps), redirections selon le délai de notification ; diffusion épidémique ; accélération d'une requête analytique parallèle |
| [`ch07_cours_4_index_secondaires.ipynb`](ch07_cours_4_index_secondaires.ipynb) | *Sharding and Secondary Indexes* : *Local Secondary Indexes*, *Global Secondary Indexes* | dispersion des valeurs indexées sur tous les shards (20 000 annonces de voitures) ; index locaux (diagramme, écriture sur un seul shard), dérive d'un index maintenu à la main quand une écriture échoue à moitié ; scatter/gather : 1-(1-p)^n, p50/p99 simulés jusqu'à 128 shards, débit plafonné à C/f (loi d'Amdahl) ; index globaux (diagramme) : écritures sur 2 à 3 shards, point chaud des termes populaires, conjonctions et listes de postings transférées, mise à jour asynchrone (diagramme espace-temps, probabilité d'une lecture périmée) ; modèle de coût local contre global, point de bascule selon le rapport lectures/écritures, tableau récapitulatif |
| [`ch07_cours_5_pourquoi_sharder_et_multitenancy.ipynb`](ch07_cours_5_pourquoi_sharder_et_multitenancy.ipynb) | *Pros and Cons of Sharding* ; *Sharding for Multitenancy* | ordres de grandeur d'une machine et années avant saturation ; la réplication plafonne en écriture, le sharding croît (modèle de capacité) ; probabilité qu'une transaction reste sur un seul shard et latence d'une transaction 2PC simulée ; un shard par cœur contre verrous partagés (Amdahl), diagramme d'un serveur NUMA ; tailles de locataires log-normales, rangement first-fit decreasing et géants ; voisin bruyant selon le nombre de locataires par shard ; bogue d'autorisation sur base partagée contre base par locataire (SQLite) ; architecture cellulaire (diagramme) et rayon d'impact ; restauration et effacement d'un seul locataire, résidence des données ; migration de schéma par vagues simulée ; croissance des locataires et déplacements selon la politique |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch07_exercices_01-06.ipynb`](ch07_exercices_01-06.ipynb) | 7.1 à 7.6 |
| [`ch07_exercices_07-12.ipynb`](ch07_exercices_07-12.ipynb) | 7.7 à 7.12 |

- **Ex. 7.1** - Choisir une clé de sharding (capteurs, commandes, réseau social) : déséquilibre en écriture, morceaux lus par requête, célébrité en loi de Zipf (simulation).
- **Ex. 7.2** - Balles dans des urnes : écart relatif en $\sqrt{2\ln n/\lambda}$, nombre de clés par morceau pour un équilibre à 10 % (calcul affiné et simulation).
- **Ex. 7.3** - Modulo N déplace $N/(N+1)$ des clés (démonstration par les restes chinois) contre $1/(N+1)$ ; part d'un nœud sur l'anneau en $1/\sqrt{v}$ (vérification).
- **Ex. 7.4** - Dimensionner un nombre fixe de shards pour une croissance de 2 à 200 To, diviseurs de 5 040, coût des scissions générales.
- **Ex. 7.5** - Mini-lab : scissions de plages avec fusion après une purge, et oscillation scission/fusion sans hystérésis.
- **Ex. 7.6** - Salage d'une clé chaude : nombre de suffixes avec collisions du hachage (probabilité exacte et simulation), amplification des lectures, agrégat en cache.
- **Ex. 7.7** - Trouver l'anomalie : écriture acquittée puis perdue chez l'ancien propriétaire (diagrammes espace-temps), correction par contrôle de propriété et époque de la carte (mini-lab).
- **Ex. 7.8** - Scatter/gather : $1-(1-p)^n$, percentiles simulés selon n, requêtes de couverture déclenchées au p95.
- **Ex. 7.9** - Index local ou global : modèle de coût, seuil de bascule selon la part d'écritures, vérification avec `LocalIndexDB` / `GlobalIndexDB`, limites du modèle.
- **Ex. 7.10** - Durée des transferts de rééquilibrage, carte de la défaillance en cascade (utilisation × surcoût de migration), patience du détecteur.
- **Ex. 7.11** - Multi-location : rangement first-fit decreasing de 2 000 locataires, géants, voisin bruyant, croissance et politiques de déplacement.
- **Ex. 7.12** - Faut-il sharder ? Années avant de saturer une machine, probabilité qu'une transaction reste sur un seul shard, effet d'une clé « client ».

## Lab (Docker)

| Notebook | Contenu |
|---|---|
| [`ch07_lab_redis_cluster.ipynb`](ch07_lab_redis_cluster.ipynb) | Redis Cluster à trois puis quatre maîtres (`docker/partitioning/compose.yml`, ports 10701-10704) : les 16 384 slots comme nombre fixe de shards (carte et diagramme) ; notre CRC16 identique à `CLUSTER KEYSLOT` sur 1 000 clés, répartition par nœud et par slot (CRC16 contre MD5 et loi de Poisson) ; redirections `MOVED` (diagramme espace-temps), correction slot par slot contre rechargement de la carte, coût d'une redirection ; hash tags, `CROSSSLOT` et transaction locale, point chaud d'un tag de 30 000 clés ; ajout d'un nœud avec `rebalance` : 25 % de clés déplacées contre 75 % pour un modulo ; migration manuelle d'un slot (`IMPORTING` / `MIGRATING`, `MIGRATE`, `ASK` / `ASKING`, puis `MOVED`) ; clé chaude Zipf et compteur salé lus dans `INFO commandstats` |

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning up -d     # démarrer
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning down -v   # arrêter et effacer
```

Les nœuds utilisent le réseau de l'hôte (Linux) pour que les redirections `MOVED` et `ASK` désignent des adresses joignables depuis le notebook.
