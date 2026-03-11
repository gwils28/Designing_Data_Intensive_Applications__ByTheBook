# Chapitre 3 - Modèles de données et langages de requête

Le chapitre 3 compare les grands modèles de données généralistes et leurs langages de requête : le modèle relationnel et le modèle document (décalage objet-relationnel, localité, normalisation et jointures, relations plusieurs-à-plusieurs, schémas souples), les schémas en étoile de l'analytique, les graphes (graphes de propriétés et Cypher, requêtes récursives en SQL, triplets RDF et SPARQL, Datalog, GraphQL), puis l'event sourcing avec CQRS et les DataFrames. Les notebooks ajoutent ce que le livre laisse de côté : chaque modèle est implémenté à la main et mesuré (CV en tables et en JSON sur SQLite, mini-moteur de pipeline d'agrégation, graphe de propriétés, magasin de triplets, moteur Datalog semi-naïf, exécuteur GraphQL, journal d'événements et projections, matrices creuses), et un lab confronte le cours à PostgreSQL.

Les fonctions réutilisables (CV synthétiques et leur chargement en tables ou en documents, connecteur qui compte les allers-retours, pipeline d'agrégation et MapReduce, graphe de propriétés et graphe synthétique de lieux, requête récursive en SQL, magasin de triplets avec chemins de propriétés et sérialisation Turtle, moteur Datalog naïf et semi-naïf) sont dans [`ddia/datamodels.py`](../../ddia/datamodels.py), testées par [`tests/test_datamodels.py`](../../tests/test_datamodels.py). L'indexation fractionnaire, l'exécuteur GraphQL, le journal d'événements avec ses projections et les matrices creuses sont dans [`ddia/datamodels_ed2.py`](../../ddia/datamodels_ed2.py), testés par [`tests/test_datamodels_ed2.py`](../../tests/test_datamodels_ed2.py).


## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch03_cours_1_relationnel_et_documents.ipynb`](ch03_cours_1_relationnel_et_documents.ipynb) | introduction du chapitre ; *Relational Versus Document Models* : *The Object-Relational Mismatch* (*Object-relational mapping*, *The document data model for one-to-many relationships*) ; *Data locality for reads and writes* | modèles empilés en couches (diagramme) ; un demi-siècle de modèles concurrents (frise) ; un CV en objets, en tables (diagramme du schéma) et en document, son arbre ; recomposer un CV par 4 requêtes, une jointure multiple (produit cartésien mesuré) ou un document, modèle d'allers-retours ; coût d'une mise à jour et d'une lecture partielle selon la taille du document ; ORM et problème N+1 mesuré |

## Lab (Docker)

| Notebook | Contenu |
|---|---|

Démarrage et arrêt (depuis la racine du dépôt), port hôte 10332 :

```bash
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels up -d
docker compose -f docker/datamodels/compose.yml -p ddia_datamodels down -v
```
