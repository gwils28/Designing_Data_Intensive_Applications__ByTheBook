# Chapitre 3 - Stockage et recherche

Le chapitre 3 prend le point de vue de la base de données : comment ranger les données qu'on lui confie, et comment les retrouver vite ? Il présente les deux grandes familles de moteurs pour le transactionnel (OLTP), les moteurs à journal (index de hachage, SSTables, arbres LSM) et les B-trees qui modifient des pages en place, puis les index secondaires, multidimensionnels et plein texte, et enfin le stockage en colonnes pour l'analytique. Les notebooks implémentent chaque structure (en comptant octets écrits, pages et blocs lus), mesurent les compromis du livre (amplifications d'écriture, de lecture et d'espace) et les confrontent à un vrai PostgreSQL.

Les fonctions réutilisables (journal append-only, index de hachage sur segments, filtres de Bloom, SSTables et arbre LSM complet avec compaction par paliers ou par niveaux, arbre B+ à pages avec WAL et simulation de panne, modèle de disque, index inversé, trie et recherche floue, courbe de Morton, codages en colonnes, table de faits synthétique et cube de données) sont dans [`ddia/storage.py`](../../ddia/storage.py), testées par [`tests/test_storage.py`](../../tests/test_storage.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch03_cours_1_journaux_et_hachage.ipynb`](ch03_cours_1_journaux_et_hachage.ipynb) | *Data Structures That Power Your Database* ; *Hash Indexes* | la base « bash » en Python, lecture en O(n) mesurée ; index dérivés et coût en écriture (mesuré de 0 à 6 index, diagramme) ; index de hachage en mémoire à la Bitcask (diagramme, lecture en O(1)) ; segments, fusion et compaction (dents de scie de l'espace disque) ; format binaire avec somme de contrôle, tombstones, reprise après panne et écriture partielle tronquée ; un écrivain, des lecteurs, un compacteur (diagramme espace-temps) ; écritures séquentielles contre aléatoires (modèle HDD / SSD) ; RAM de l'index et limites |
| [`ch03_cours_2_sstables_et_lsm.ipynb`](ch03_cours_2_sstables_et_lsm.ipynb) | *SSTables and LSM-Trees* (*Constructing and maintaining SSTables*, *Making an LSM-tree out of SSTables*, *Performance optimizations*) | anatomie d'une SSTable (diagramme), index épars et taille de bloc, compression par bloc ; architecture LSM (diagramme : WAL, memtable, niveaux) ; reprise depuis le WAL, enregistrement tronqué ; fusion k-voies et tombstones, coût O(N log k) mesuré ; filtres de Bloom : visualisation, démonstration du k optimal et de la règle des 10 bits pour 1 %, mesures, effet sur les lectures ; size-tiered contre leveled : amplifications d'écriture, d'espace et de lecture mesurées, forme des arbres |
| [`ch03_cours_3_btrees_et_comparaison.ipynb`](ch03_cours_3_btrees_et_comparaison.ipynb) | *B-Trees* (*Making B-trees reliable*, *B-tree optimizations*) ; *Comparing B-Trees and LSM-Trees* (*Advantages of LSM-trees*, *Downsides of LSM-trees*) | arbre B+ dessiné, chemin d'une recherche, éclatement de page avant/après ; hauteur logarithmique mesurée et capacité (250 To en 4 niveaux) ; remplissage à 69 % contre 50 % ; panne au milieu d'un éclatement (clés perdues, page orpheline) et réparation par WAL (diagramme espace-temps) ; clés abrégées et feuilles chaînées ; B-tree contre LSM sur la même charge : lectures, amplification d'écriture, débit soutenable, écritures aléatoires (HDD / SSD), contre-pression et pics de latence simulés, fragmentation après suppressions |
| [`ch03_cours_4_index_secondaires_et_plein_texte.ipynb`](ch03_cours_4_index_secondaires_et_plein_texte.ipynb) | *Other Indexing Structures* (*Storing values within the index*, *Multi-column indexes*, *Full-text search and fuzzy indexes*, *Keeping everything in memory*) | index secondaire sur B+ (clé + identifiant) ; tas, index groupé, index couvrant (diagramme), modèle de coût et seuil de sélectivité, plans SQLite ; index concaténé, courbe de Morton et requête rectangulaire sur 100 000 restaurants ; index inversé (loi de Zipf des postings lists, intersection, bitmaps) ; table de Levenshtein et recherche floue dans un trie ; coût de l'encodage et bases en mémoire |
| [`ch03_cours_5_stockage_en_colonnes.ipynb`](ch03_cours_5_stockage_en_colonnes.ipynb) | *Transaction Processing or Analytics?* (*Data Warehousing*, *Stars and Snowflakes: Schemas for Analytics*) ; *Column-Oriented Storage* (*Column Compression*, *Sort Order in Column Storage*, *Writing to Column-Oriented Storage*) ; *Aggregation: Data Cubes and Materialized Views* | OLTP contre OLAP, entrepôt et ETL (diagramme) ; schéma en étoile et en flocon (diagramme) ; lignes contre colonnes (diagramme, octets et temps mesurés) ; dictionnaire, bitmaps et RLE (figure, tailles mesurées), filtres par OU / ET de bitmaps ; boucle ligne à ligne contre traitement vectorisé ; ordre de tri et compression ; écritures par lots (modèle LSM) ; cube de données, treillis des agrégats, gain et rigidité |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch03_exercices_01-06.ipynb`](ch03_exercices_01-06.ipynb) | 3.1 à 3.6 |
| [`ch03_exercices_07-10.ipynb`](ch03_exercices_07-10.ipynb) | 3.7 à 3.10 |

- **Ex. 3.1** - Dimensionner un index de hachage à la Bitcask : RAM, croissance du journal, fréquence de compaction, reprise et fichier d'indices.
- **Ex. 3.2** - Compaction : démonstration de WA = α / (α - 1), compromis espace / réécritures, simulation.
- **Ex. 3.3** - Filtres de Bloom d'un LSM : bits par clé, lectures inutiles, répartition optimale de la mémoire entre niveaux (lagrangien).
- **Ex. 3.4** - Choisir une stratégie de compaction selon la charge (size-tiered, leveled T = 4 et T = 10, mesurés).
- **Ex. 3.5** - Hauteur d'un B-tree d'un milliard de lignes, mémoire des pages internes, E/S par lecture.
- **Ex. 3.6** - Trouver l'anomalie : panne au milieu d'un éclatement, les six ordres d'écriture testés exhaustivement.
- **Ex. 3.7** - LSM ou B-tree ? Bande passante, usure du SSD et IOPS pour une charge de télémétrie.
- **Ex. 3.8** - Choisir ses index secondaires et couvrants avec SQLite : plans, temps de requête et coût d'insertion.
- **Ex. 3.9** - Intersection de postings lists (fusion contre dichotomie) et recherche floue (variantes générées contre trie).
- **Ex. 3.10** - Stockage en colonnes : octets lus, dictionnaire, RLE, taille d'un cube et remplissage du treillis.

## Labs (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/storage/compose.yml -p ddia_storage up -d
docker compose -f docker/storage/compose.yml -p ddia_storage down -v
```

PostgreSQL écoute sur le port 10432 (`ddia` / `ddia`, base `stockage`).
