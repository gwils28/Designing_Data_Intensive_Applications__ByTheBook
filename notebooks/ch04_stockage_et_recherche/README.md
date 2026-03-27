# Chapitre 4 - Stockage et recherche

Le chapitre 4 prend le point de vue de la base de données : comment ranger les données qu'on lui confie, et comment les retrouver vite ? Il présente les deux grandes familles de moteurs pour le transactionnel (OLTP), les moteurs à journal (index de hachage, SSTables, arbres LSM) et les B-trees qui modifient des pages en place, puis les index secondaires, multidimensionnels et plein texte, et enfin le stockage en colonnes pour l'analytique. La 2e édition y ajoute les entrepôts de données dans le cloud, la compilation de requêtes et les index vectoriels pour la recherche sémantique. Les notebooks implémentent chaque structure (en comptant octets écrits, pages et blocs lus), mesurent les compromis du livre (amplifications d'écriture, de lecture et d'espace) et les confrontent à un vrai PostgreSQL.

Les fonctions réutilisables (journal append-only, index de hachage sur segments, filtres de Bloom, SSTables et arbre LSM complet avec compaction par paliers ou par niveaux, arbre B+ à pages avec WAL et simulation de panne, modèle de disque, index inversé, trie et recherche floue, n-grammes, courbe de Morton, codages en colonnes, table de faits synthétique, cube de données, interprétation et compilation de requêtes) sont dans [`ddia/storage.py`](../../ddia/storage.py), testées par [`tests/test_storage.py`](../../tests/test_storage.py). Les embeddings synthétiques et les index vectoriels (plat, IVF, HNSW) sont dans [`ddia/storage_vectors.py`](../../ddia/storage_vectors.py), testés par [`tests/test_storage_vectors.py`](../../tests/test_storage_vectors.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch04_cours_1_journaux_et_hachage.ipynb`](ch04_cours_1_journaux_et_hachage.ipynb) | *Storage and Indexing for OLTP* ; *Log-Structured Storage* (index de hachage) | la base « bash » en Python, lecture en O(n) mesurée ; index dérivés et coût en écriture (mesuré de 0 à 6 index, diagramme) ; index de hachage en mémoire (diagramme, lecture en O(1)) ; segments, fusion et compaction (dents de scie de l'espace disque) ; format binaire avec somme de contrôle, tombstones, reprise après panne et écriture partielle tronquée ; un écrivain, des lecteurs, un compacteur (diagramme espace-temps) ; écritures séquentielles contre aléatoires (modèle HDD / SSD) ; RAM de l'index et limites |
| [`ch04_cours_2_sstables_et_lsm.ipynb`](ch04_cours_2_sstables_et_lsm.ipynb) | *The SSTable file format* ; *Constructing and merging SSTables* ; *Bloom filters* ; *Compaction strategies* ; *Embedded Storage Engines* | anatomie d'une SSTable (diagramme), index épars et taille de bloc, compression par bloc ; architecture LSM (diagramme : WAL, memtable, niveaux) ; reprise depuis le WAL, enregistrement tronqué ; fusion k-voies et tombstones, coût O(N log k) mesuré ; filtres de Bloom : visualisation, démonstration du k optimal et de la règle des 10 bits pour 1 %, mesures, effet sur les lectures ; size-tiered contre leveled : amplifications mesurées, forme des arbres ; moteurs embarqués et SSTables sur stockage objet (diagramme) |
| [`ch04_cours_3_btrees_et_comparaison.ipynb`](ch04_cours_3_btrees_et_comparaison.ipynb) | *B-Trees* (*Making B-trees reliable*, *Using B-tree variants*) ; *Comparing B-Trees and LSM-Trees* (*Read performance*, *Sequential versus random writes*, *Write amplification*, *Disk space usage*) | arbre B+ dessiné, chemin d'une recherche, éclatement de page avant/après ; hauteur logarithmique mesurée et capacité (250 To en 4 niveaux) ; remplissage à 69 % contre 50 % ; panne au milieu d'un éclatement (clés perdues, page orpheline) et réparation par WAL (diagramme espace-temps) ; clés abrégées et feuilles chaînées ; B-tree contre LSM sur la même charge : lectures, amplification d'écriture, débit soutenable, écritures aléatoires (HDD / SSD, ramasse-miettes des SSD), contre-pression et pics de latence simulés, fragmentation après suppressions |
| [`ch04_cours_4_index_secondaires_et_plein_texte.ipynb`](ch04_cours_4_index_secondaires_et_plein_texte.ipynb) | *Multicolumn and Secondary Indexes* ; *Storing Values Within the Index* ; *Keeping Everything in Memory* ; *Multidimensional and Full-Text Indexes* (*Full-Text Search*) | index secondaire sur B+ (clé + identifiant) ; tas, index groupé, index couvrant (diagramme), modèle de coût et seuil de sélectivité, plans SQLite ; index concaténé, courbe de Morton et requête rectangulaire sur 100 000 restaurants ; index inversé (loi de Zipf des postings lists, intersection, bitmaps), index de trigrammes ; table de Levenshtein et recherche floue dans un trie ; coût de l'encodage et bases en mémoire |
| [`ch04_cours_5_stockage_en_colonnes.ipynb`](ch04_cours_5_stockage_en_colonnes.ipynb) | *Data Storage for Analytics* (*Cloud Data Warehouses*) ; *Column-Oriented Storage* (*Column compression*, *Sort order in column storage*, *Writing to column-oriented storage*) ; *Query Execution: Compilation and Vectorization* ; *Materialized Views and Data Cubes* | rappels OLTP / OLAP et HTAP ; lignes contre colonnes (diagramme, octets et temps mesurés) ; dictionnaire, bitmaps et RLE (figure, tailles mesurées), filtres par OU / ET de bitmaps ; traitement vectorisé ; ordre de tri et compression ; écritures par lots (modèle LSM) ; cube de données, treillis des agrégats, gain et rigidité ; interprétation, compilation (code généré) et vectorisation mesurées ; entrepôt décomposé sur lac de données (diagramme), calcul élastique |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch04_exercices_01-06.ipynb`](ch04_exercices_01-06.ipynb) | 4.1 à 4.6 |

- **Ex. 4.1** - Dimensionner un index de hachage à la Bitcask : RAM, croissance du journal, fréquence de compaction, reprise et fichier d'indices.
- **Ex. 4.2** - Compaction : démonstration de WA = α / (α - 1), compromis espace / réécritures, simulation.
- **Ex. 4.3** - Filtres de Bloom d'un LSM : bits par clé, lectures inutiles, répartition optimale de la mémoire entre niveaux (lagrangien).
- **Ex. 4.4** - Choisir une stratégie de compaction selon la charge (size-tiered, leveled T = 4 et T = 10, mesurés).
- **Ex. 4.5** - Hauteur d'un B-tree d'un milliard de lignes, mémoire des pages internes, E/S par lecture.
- **Ex. 4.6** - Trouver l'anomalie : panne au milieu d'un éclatement, les six ordres d'écriture testés exhaustivement.

## Labs (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/storage/compose.yml -p ddia_storage up -d
docker compose -f docker/storage/compose.yml -p ddia_storage down -v
```

PostgreSQL écoute sur le port 10432 (`ddia` / `ddia`, base `stockage`).
