# Chapitre 4 - Stockage et recherche

Le chapitre 4 prend le point de vue de la base de données : comment ranger les données qu'on lui confie, et comment les retrouver vite ? Il présente les deux grandes familles de moteurs pour le transactionnel (OLTP), les moteurs à journal (index de hachage, SSTables, arbres LSM) et les B-trees qui modifient des pages en place, puis les index secondaires, multidimensionnels et plein texte, et enfin le stockage en colonnes pour l'analytique. La 2e édition y ajoute les entrepôts de données dans le cloud, la compilation de requêtes et les index vectoriels pour la recherche sémantique. Les notebooks implémentent chaque structure (en comptant octets écrits, pages et blocs lus), mesurent les compromis du livre (amplifications d'écriture, de lecture et d'espace) et les confrontent à un vrai PostgreSQL.

Les fonctions réutilisables (journal append-only, index de hachage sur segments, filtres de Bloom, SSTables et arbre LSM complet avec compaction par paliers ou par niveaux, arbre B+ à pages avec WAL et simulation de panne, modèle de disque, index inversé, trie et recherche floue, n-grammes, courbe de Morton, codages en colonnes, table de faits synthétique, cube de données, interprétation et compilation de requêtes) sont dans [`ddia/storage.py`](../../ddia/storage.py), testées par [`tests/test_storage.py`](../../tests/test_storage.py). Les embeddings synthétiques et les index vectoriels (plat, IVF, HNSW) sont dans [`ddia/storage_vectors.py`](../../ddia/storage_vectors.py), testés par [`tests/test_storage_vectors.py`](../../tests/test_storage_vectors.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch04_cours_1_journaux_et_hachage.ipynb`](ch04_cours_1_journaux_et_hachage.ipynb) | *Storage and Indexing for OLTP* ; *Log-Structured Storage* (index de hachage) | la base « bash » en Python, lecture en O(n) mesurée ; index dérivés et coût en écriture (mesuré de 0 à 6 index, diagramme) ; index de hachage en mémoire (diagramme, lecture en O(1)) ; segments, fusion et compaction (dents de scie de l'espace disque) ; format binaire avec somme de contrôle, tombstones, reprise après panne et écriture partielle tronquée ; un écrivain, des lecteurs, un compacteur (diagramme espace-temps) ; écritures séquentielles contre aléatoires (modèle HDD / SSD) ; RAM de l'index et limites |
| [`ch04_cours_2_sstables_et_lsm.ipynb`](ch04_cours_2_sstables_et_lsm.ipynb) | *The SSTable file format* ; *Constructing and merging SSTables* ; *Bloom filters* ; *Compaction strategies* ; *Embedded Storage Engines* | anatomie d'une SSTable (diagramme), index épars et taille de bloc, compression par bloc ; architecture LSM (diagramme : WAL, memtable, niveaux) ; reprise depuis le WAL, enregistrement tronqué ; fusion k-voies et tombstones, coût O(N log k) mesuré ; filtres de Bloom : visualisation, démonstration du k optimal et de la règle des 10 bits pour 1 %, mesures, effet sur les lectures ; size-tiered contre leveled : amplifications mesurées, forme des arbres ; moteurs embarqués et SSTables sur stockage objet (diagramme) |

## Labs (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt) :

```bash
docker compose -f docker/storage/compose.yml -p ddia_storage up -d
docker compose -f docker/storage/compose.yml -p ddia_storage down -v
```

PostgreSQL écoute sur le port 10432 (`ddia` / `ddia`, base `stockage`).
