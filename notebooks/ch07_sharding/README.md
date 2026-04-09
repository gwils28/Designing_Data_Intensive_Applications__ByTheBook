# Chapitre 7 - Sharding

Quand le volume de données ou le débit d'écriture dépasse ce qu'une machine peut absorber, on découpe les données en **shards** placés sur des nœuds différents. Le chapitre 7 montre pourquoi (et quand ne pas le faire), comment découper (par plage de clés ou par hachage), comment rééquilibrer quand le cluster change de taille, que faire des clés chaudes, comment router les requêtes et comment indexer des données découpées ; il traite aussi le sharding au service de la multi-location. Les notebooks ajoutent la profondeur quantitative que le livre laisse de côté : déséquilibre « balles dans des urnes », fraction de clés déplacées mesurée pour chaque stratégie, anneau de hachage cohérent, loi de Zipf, routage simulé avec cartes périmées, latence de queue du scatter/gather, défaillance en cascade d'un rééquilibrage automatique, et un vrai Redis Cluster.

Les fonctions réutilisables (hachage stable, `mix64`, CRC16 et slots de Redis Cluster, partitionnement par plage avec bornes par quantiles, partitionnement dynamique à scissions, haché modulo N, nombre fixe de morceaux avec poids, anneau de hachage à jetons aléatoires, mesures de déséquilibre et de clés déplacées, requêtes Zipf et salage, mesures de capteurs, index secondaires locaux et globaux, scatter/gather, simulation du routage sur `ddia.sim` avec coordinateur, diffusion épidémique, modèle de cascade de rééquilibrage) sont dans [`ddia/partitioning.py`](../../ddia/partitioning.py), testées par [`tests/test_partitioning.py`](../../tests/test_partitioning.py). Les fonctions de multi-location (tailles de locataires, rangement first-fit decreasing, voisin bruyant, croissance des locataires, transactions mono-shard) sont dans [`ddia/partitioning_tenants.py`](../../ddia/partitioning_tenants.py), testées par [`tests/test_partitioning_tenants.py`](../../tests/test_partitioning_tenants.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch07_cours_1_plages_et_hachage.ipynb`](ch07_cours_1_plages_et_hachage.ipynb) | introduction et encadré *Sharding and Partitioning* ; *Sharding of Key-Value Data* : *Sharding by Key Range*, *Sharding by Hash of Key*, encadré *Partitioning and Range Queries in Data Warehouses* | sharding et réplication combinés (placement calculé, diagramme) ; déséquilibre « balles dans des urnes » et gain n / déséquilibre ; encyclopédie : bornes fixes contre quantiles, requêtes par préfixe ; point chaud des horodatages (cartes de chaleur des écritures) et clé (capteur, horodatage) ; `hash()` de Python non stable entre processus ; hachage d'horodatages consécutifs ; morceaux touchés par une requête de plage (mesure et formule) ; clé composée à la Cassandra |
| [`ch07_cours_2_reequilibrage.ipynb`](ch07_cours_2_reequilibrage.ipynb) | *Rebalancing key-range sharded data*, *Hash modulo number of nodes*, *Fixed number of shards*, *Sharding by hash range*, *Consistent hashing* ; *Operations: Automatic Versus Manual Rebalancing* | borne inférieure 1/(n+1) (diagramme) ; mod N : grille, preuve N/(N+1) et mesure ; nombre fixe de shards : croissance de 3 à 12 nœuds, machines hétérogènes, divisibilité, taille des shards contre volume (diagramme) ; scissions : nombre de shards par vagues, amplification d'écriture, pré-découpage, clés croissantes ; anneau à jetons (dessin), déséquilibre selon le nombre de jetons, donneurs lors d'un ajout ; durée d'un transfert, cascade d'exclusions simulée et seuil de charge ; rendez-vous et jump hash comparés |

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning up -d     # démarrer
docker compose -f docker/partitioning/compose.yml -p ddia_partitioning down -v   # arrêter et effacer
```

Les nœuds utilisent le réseau de l'hôte (Linux) pour que les redirections `MOVED` et `ASK` désignent des adresses joignables depuis le notebook.
