# Chapitre 3 - Modèles de données et langages de requête

Le chapitre 3 compare les grands modèles de données généralistes et leurs langages de requête : le modèle relationnel et le modèle document (décalage objet-relationnel, localité, normalisation et jointures, relations plusieurs-à-plusieurs, schémas souples), les schémas en étoile de l'analytique, les graphes (graphes de propriétés et Cypher, requêtes récursives en SQL, triplets RDF et SPARQL, Datalog, GraphQL), puis l'event sourcing avec CQRS et les DataFrames. Les notebooks ajoutent ce que le livre laisse de côté : chaque modèle est implémenté à la main et mesuré (CV en tables et en JSON sur SQLite, mini-moteur de pipeline d'agrégation, graphe de propriétés, magasin de triplets, moteur Datalog semi-naïf, exécuteur GraphQL, journal d'événements et projections, matrices creuses), et un lab confronte le cours à PostgreSQL.

Les fonctions réutilisables (CV synthétiques et leur chargement en tables ou en documents, connecteur qui compte les allers-retours, pipeline d'agrégation et MapReduce, graphe de propriétés et graphe synthétique de lieux, requête récursive en SQL, magasin de triplets avec chemins de propriétés et sérialisation Turtle, moteur Datalog naïf et semi-naïf) sont dans [`ddia/datamodels.py`](../../ddia/datamodels.py), testées par [`tests/test_datamodels.py`](../../tests/test_datamodels.py). L'indexation fractionnaire, l'exécuteur GraphQL, le journal d'événements avec ses projections et les matrices creuses sont dans [`ddia/datamodels_ed2.py`](../../ddia/datamodels_ed2.py), testés par [`tests/test_datamodels_ed2.py`](../../tests/test_datamodels_ed2.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch03_cours_1_relationnel_et_documents.ipynb`](ch03_cours_1_relationnel_et_documents.ipynb) | introduction du chapitre ; *Relational Versus Document Models* : *The Object-Relational Mismatch* (*Object-relational mapping*, *The document data model for one-to-many relationships*) ; *Data locality for reads and writes* | modèles empilés en couches (diagramme) ; un demi-siècle de modèles concurrents (frise) ; un CV en objets, en tables (diagramme du schéma) et en document, son arbre ; recomposer un CV par 4 requêtes, une jointure multiple (produit cartésien mesuré) ou un document, modèle d'allers-retours ; coût d'une mise à jour et d'une lecture partielle selon la taille du document ; ORM et problème N+1 mesuré |
| [`ch03_cours_2_normalisation_et_relations.ipynb`](ch03_cours_2_normalisation_et_relations.ipynb) | *Normalization, Denormalization, and Joins* (*Trade-offs of normalization*, *Denormalization in the social networking case study*) ; *Many-to-One and Many-to-Many Relationships* ; *Stars and Snowflakes: Schemas for Analytics* | identifiant ou texte : renommage normalisé et dénormalisé, `$lookup` ; coût d'une mise à jour dénormalisée selon la popularité (Zipf), lectures avec ou sans jointure, panne au milieu d'un renommage avec et sans transaction ; table d'association indexée dans les deux sens (plans de requête), références entre documents (diagramme), relation stockée des deux côtés qui diverge ; timelines d'identifiants et hydratation contre dénormalisation des compteurs ; schéma en étoile (diagramme), flocon et OBT comparés en temps et en espace |
| [`ch03_cours_3_schemas_et_requetes.ipynb`](ch03_cours_3_schemas_et_requetes.ipynb) | *When to Use Which Model* (*Schema flexibility in the document model*, *Query languages for documents*, *Convergence of document and relational databases*) ; encadré *Terminology: Declarative Query Languages* | référence positionnelle à un élément imbriqué ; `ALTER TABLE` contre `UPDATE` mesurés, vieillissement des formats en schéma à la lecture (simulation), données hétérogènes ; déclaratif contre impératif : même requête, plan changé par un index, découpage parallèle (diagramme) ; pipeline d'agrégation contre SQL, flot de documents par étape ; index sur expression JSON dans SQLite ; listes réordonnables et indexation fractionnaire (coût des insertions, longueur des clés) |
| [`ch03_cours_4_graphes_et_cypher.ipynb`](ch03_cours_4_graphes_et_cypher.ipynb) | *Graph-Like Data Models* : *Property Graphs*, *The Cypher Query Language*, *Graph Queries in SQL* | liste contre matrice d'adjacence, PageRank par itération de puissance ; graphe de propriétés fil rouge (diagramme), deux tables SQL, extension par les allergies ; requête « émigrés » en Cypher et ses deux stratégies d'exécution mesurées sur un graphe de 50 000 personnes ; `WITH RECURSIVE` sur SQLite, cycles (`UNION` contre `UNION ALL`), effet de la profondeur |
| [`ch03_cours_5_triplets_et_datalog.ipynb`](ch03_cours_5_triplets_et_datalog.ipynb) | *Triple Stores and SPARQL* (encadré *The Semantic Web*, *The RDF data model*, *The SPARQL query language*) ; *Datalog: Recursive Relational Queries* ; *GraphQL* | triplets, Turtle, index SPO/POS/OSP ; espaces de noms RDF et fusion de jeux de données ; motifs et chemins de propriétés SPARQL, ordre des jointures mesuré ; faits et règles Datalog, dérivation pas à pas (diagramme) ; évaluation naïve contre semi-naïve ($n^3$ contre $n^2$) ; GraphQL : réponse en forme de requête, résolveurs naïfs (N+1) ou regroupés, duplication, limite de profondeur |
| [`ch03_cours_6_evenements_et_dataframes.ipynb`](ch03_cours_6_evenements_et_dataframes.ipynb) | *Event Sourcing and CQRS* ; *DataFrames, Matrices, and Arrays* ; *Summary* | commandes, événements et projections (diagramme d'architecture et diagramme espace-temps d'une vue en retard) ; inscriptions à une conférence simulées ; correction d'un bogue et nouvelle vue par rejeu, instantanés, rejeu dans le désordre ; données externes, crypto-shredding, effets de bord ; DataFrame, pivot en matrice creuse, CSR, one-hot, similarité cosinus |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch03_exercices_01-05.ipynb`](ch03_exercices_01-05.ipynb) | 3.1 à 3.5 |
| [`ch03_exercices_06-10.ipynb`](ch03_exercices_06-10.ipynb) | 3.6 à 3.10 |
| [`ch03_exercices_11-12.ipynb`](ch03_exercices_11-12.ipynb) | 3.11 et 3.12 |

- **Ex. 3.1** - Choisir un modèle pour cinq applications (catalogue, facturation, blog, fraude, liste réordonnable) ; cercle de fraude retrouvé comme composante connexe d'un graphe.
- **Ex. 3.2** - Jointure multiple ou requêtes séparées : seuil $pec - (1+p+e+c) > 3\,\text{RTT}/t$, carte du gagnant, mesure sur SQLite, variante par union.
- **Ex. 3.3** - Coût d'un renommage dénormalisé sous loi de Zipf (tirage uniforme ou proportionnel) et probabilité d'incohérence sans transaction (calcul et simulation).
- **Ex. 3.4** - Relation plusieurs-à-plusieurs stockée des deux côtés : divergence en $Np$, tâche de réconciliation, comparaison avec la représentation normalisée.
- **Ex. 3.5** - Migration immédiate, paresseuse ou combinée : durée, décroissance géométrique, récurrence affine résolue et simulée.
- **Ex. 3.6** - Deux rapports en pipeline d'agrégation (`$project`, `$group`, `$lookup`) et en SQL, résultats comparés.
- **Ex. 3.7** - Généalogie en `WITH RECURSIVE` : ancêtres par génération, ancêtre commun le plus proche, cycle dû à une erreur de saisie et sa détection.
- **Ex. 3.8** - « Vivre sur son continent de naissance » en Cypher, SPARQL, Datalog et SQL, exécuté et comparé.
- **Ex. 3.9** - Récursion linéaire ou non linéaire en Datalog : $n$ tours contre $\log_2 n$, dérivations mesurées.
- **Ex. 3.10** - Estimer la sélectivité de motifs de triplets pour choisir l'ordre des jointures, estimations contre mesures.
- **Ex. 3.11** - Event sourcing : fréquence optimale des instantanés $k^* = \sqrt{2ES/(rc)}$, reconstruction depuis un instantané, partitionnement tourniquet contre par clé.
- **Ex. 3.12** - Dimensionner matrices creuses (dense, COO, CSR), one-hot et OBT ; mesures de taille et de temps sur une instance réduite.

## Lab (Docker)

| Notebook | Contenu |
|---|---|
| [`ch03_lab_postgres_jsonb_et_recursif.ipynb`](ch03_lab_postgres_jsonb_et_recursif.ipynb) | PostgreSQL 16 : 50 000 CV en tables et en JSONB (tailles) ; quatre façons de lire un profil, dont un agrégat JSON côté serveur ; index GIN sur des valeurs imbriquées (`@>`) contre B-tree ; octets de WAL d'une petite modification selon la taille du document ; migrations (`ADD COLUMN` à défaut constant ou volatil, `UPDATE`) ; graphe en deux tables, `WITH RECURSIVE` et clause `CYCLE` |

Démarrage et arrêt (depuis la racine du dépôt), port hôte 10332 :

```bash
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels up -d
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels down -v
```
