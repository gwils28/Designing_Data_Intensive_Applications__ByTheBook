# Chapitre 2 - Modèles de données et langages de requête

Le chapitre 2 compare les grands modèles de données généralistes et leurs langages de requête : le modèle relationnel et le modèle document (naissance de NoSQL, décalage objet-relationnel, localité, relations plusieurs-à-un et plusieurs-à-plusieurs, retour sur les modèles hiérarchique et réseau, schémas souples), les langages de requête déclaratifs (SQL, CSS, MapReduce, pipeline d'agrégation) et les graphes (graphes de propriétés et Cypher, requêtes récursives en SQL, triplets RDF et SPARQL, Datalog). Les notebooks ajoutent ce que le livre laisse de côté : chaque modèle est implémenté à la main et mesuré (CV en tables et en JSON sur SQLite, mini-moteurs MapReduce et pipeline d'agrégation, base « réseau » à pointeurs, graphe de propriétés, magasin de triplets, moteur Datalog semi-naïf), et un lab confronte le cours à PostgreSQL.

Les fonctions réutilisables (CV synthétiques et leur chargement en tables ou en documents, connecteur qui compte les allers-retours, pipeline d'agrégation et MapReduce, graphe de propriétés et graphe synthétique de lieux, requête récursive en SQL, magasin de triplets avec chemins de propriétés et sérialisation Turtle, moteur Datalog naïf et semi-naïf) sont dans [`ddia/datamodels.py`](../../ddia/datamodels.py), testées par [`tests/test_datamodels.py`](../../tests/test_datamodels.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch02_cours_1_relationnel_et_documents.ipynb`](ch02_cours_1_relationnel_et_documents.ipynb) | introduction du chapitre ; *Relational Model Versus Document Model* : *The Birth of NoSQL*, *The Object-Relational Mismatch* ; *Data locality for queries* | modèles empilés en couches (diagramme) ; un demi-siècle de modèles concurrents (frise), les motivations de NoSQL ; un CV en objets, en tables (diagramme du schéma) et en document, son arbre ; recomposer un CV par 4 requêtes, une jointure multiple (produit cartésien mesuré) ou un document, modèle d'allers-retours ; coût d'une mise à jour et d'une lecture partielle selon la taille du document |
| [`ch02_cours_2_normalisation_et_relations.ipynb`](ch02_cours_2_normalisation_et_relations.ipynb) | *Many-to-One and Many-to-Many Relationships* ; *Are Document Databases Repeating History?* (*The network model*, *The relational model*, *Comparison to document databases*) | identifiant ou texte : renommage normalisé et dénormalisé, `$lookup` ; coût d'une mise à jour dénormalisée selon la popularité (Zipf), lectures avec ou sans jointure, panne au milieu d'un renommage avec et sans transaction ; table d'association indexée dans les deux sens (plans de requête), références entre documents (diagramme), relation stockée des deux côtés qui diverge ; modèles hiérarchique et réseau : une base à pointeurs et ses chemins d'accès (diagramme), contre l'optimiseur relationnel |
| [`ch02_cours_3_schemas_et_requetes.ipynb`](ch02_cours_3_schemas_et_requetes.ipynb) | *Relational Versus Document Databases Today* (*Which data model leads to simpler application code?*, *Schema flexibility in the document model*, *Convergence of document and relational databases*) ; *Query Languages for Data* (*Declarative Queries on the Web*, *MapReduce Querying*) | référence positionnelle à un élément imbriqué ; `ALTER TABLE` contre `UPDATE` mesurés, vieillissement des formats en schéma à la lecture (simulation), données hétérogènes ; déclaratif contre impératif : même requête, plan changé par un index, découpage parallèle (diagramme) ; sélecteur CSS contre manipulation du DOM ; MapReduce (diagramme), exécution par morceaux ; pipeline d'agrégation contre SQL, flot de documents par étape ; index sur expression JSON dans SQLite |
| [`ch02_cours_4_graphes_et_cypher.ipynb`](ch02_cours_4_graphes_et_cypher.ipynb) | *Graph-Like Data Models* : *Property Graphs*, *The Cypher Query Language*, *Graph Queries in SQL* | liste contre matrice d'adjacence, PageRank par itération de puissance ; graphe de propriétés fil rouge (diagramme), deux tables SQL, extension par les allergies ; requête « émigrés » en Cypher et ses deux stratégies d'exécution mesurées sur un graphe de 50 000 personnes ; `WITH RECURSIVE` sur SQLite, cycles (`UNION` contre `UNION ALL`), effet de la profondeur |
| [`ch02_cours_5_triplets_et_datalog.ipynb`](ch02_cours_5_triplets_et_datalog.ipynb) | *Triple-Stores and SPARQL* (*The semantic web*, *The RDF data model*, *The SPARQL query language*, encadré *Graph Databases Compared to the Network Model*) ; *The Foundation: Datalog* | triplets, Turtle, index SPO/POS/OSP ; espaces de noms RDF et fusion de jeux de données ; motifs et chemins de propriétés SPARQL, ordre des jointures mesuré ; bases de graphes face à CODASYL ; faits et règles Datalog, dérivation pas à pas (diagramme) ; évaluation naïve contre semi-naïve ($n^3$ contre $n^2$) |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch02_exercices_01-05.ipynb`](ch02_exercices_01-05.ipynb) | 2.1 à 2.5 |
| [`ch02_exercices_06-10.ipynb`](ch02_exercices_06-10.ipynb) | 2.6 à 2.10 |

- **Ex. 2.1** - Choisir un modèle pour cinq applications (catalogue, facturation, blog, fraude, liste réordonnable) ; cercle de fraude retrouvé comme composante connexe d'un graphe.
- **Ex. 2.2** - Jointure multiple ou requêtes séparées : seuil $pec - (1+p+e+c) > 3\,\text{RTT}/t$, carte du gagnant, mesure sur SQLite, variante par union.
- **Ex. 2.3** - Coût d'un renommage dénormalisé sous loi de Zipf (tirage uniforme ou proportionnel) et probabilité d'incohérence sans transaction (calcul et simulation).
- **Ex. 2.4** - Relation plusieurs-à-plusieurs stockée des deux côtés : divergence en $Np$, tâche de réconciliation, comparaison avec la représentation normalisée.
- **Ex. 2.5** - Migration immédiate, paresseuse ou combinée : durée, décroissance géométrique, récurrence affine résolue et simulée.
- **Ex. 2.6** - Deux rapports en pipeline d'agrégation (`$project`, `$group`, `$lookup`) et en SQL, résultats comparés.
- **Ex. 2.7** - Généalogie en `WITH RECURSIVE` : ancêtres par génération, ancêtre commun le plus proche, cycle dû à une erreur de saisie et sa détection.
- **Ex. 2.8** - « Vivre sur son continent de naissance » en Cypher, SPARQL, Datalog et SQL, exécuté et comparé.
- **Ex. 2.9** - Récursion linéaire ou non linéaire en Datalog : $n$ tours contre $\log_2 n$, dérivations mesurées.
- **Ex. 2.10** - Estimer la sélectivité de motifs de triplets pour choisir l'ordre des jointures, estimations contre mesures.

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt), port hôte 10332 :

```bash
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels up -d
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels down -v
```
