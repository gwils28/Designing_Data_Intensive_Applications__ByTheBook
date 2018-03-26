# Designing Data-Intensive Applications - *By The Book*

Notebooks Jupyter pédagogiques qui accompagnent la lecture de **Designing Data-Intensive Applications** (Martin Kleppmann, O'Reilly, 2017) :
cours illustrés, implémentations « à la main » des structures et des protocoles qui font
fonctionner les bases de données et les systèmes distribués, simulations, diagrammes
d'architecture et **exercices corrigés en détail**, chapitre par chapitre.

## Objectif et public

Les notebooks sont écrits en français pour un lecteur qui **sait programmer en Python** et
connaît les bases de SQL, mais découvre l'ingénierie des systèmes de données : stockage,
réplication, partitionnement, transactions, cohérence, traitement par lots et de flux.

Le livre explique les idées sans code ni exercices. Ces notebooks ajoutent ce qu'il faut
pour **comprendre en profondeur** :

- chaque concept est motivé, expliqué par une intuition, puis formalisé (complexités,
  probabilités, quorums, files d'attente, ordres de grandeur) ;
- chaque mécanisme est **réimplémenté en Python** puis éprouvé : journaux et LSM-trees,
  B-trees, encodeurs binaires, réplication et quorums, moteur MVCC, horloges, consensus,
  MapReduce, fenêtres de flux... ;
- les protocoles sont rejoués sur un **simulateur de réseau déterministe** (délais,
  pertes, partitions, pannes, horloges imparfaites) et représentés par des **diagrammes
  espace-temps** ;
- les architectures sont représentées par des **diagrammes** ;
- chaque chapitre propose **8 à 12 exercices** conçus pour ce dépôt (compréhension,
  calculs de dimensionnement, mini-labs de code), tous corrigés.

Chaque exercice suit le même plan :

1. **énoncé** autonome ;
2. **rappels** des notions du cours utilisées (avec renvoi à la section du livre) ;
3. **solution** détaillée pas à pas ;
4. **vérification** numérique, expérimentale et/ou graphique ;
5. un court **bilan** (« À retenir »).

## Organisation du dépôt

```
notebooks/chXX_<theme>/   notebooks de cours (chXX_cours_*), d'exercices (chXX_exercices_*)
ddia/                     paquet Python : style graphique, diagrammes, diagrammes
                          espace-temps, simulateur de réseau, implémentations « à la
                          main » des mécanismes de chaque chapitre
tests/                    tests unitaires (pytest)
tools/                    construction et exécution des notebooks
```

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python -m ipykernel install --user --name ddia
jupyter notebook
```

Aucun service externe n'est nécessaire pour les notebooks de cours et d'exercices : tout
tourne en local, en Python pur (plus SQLite). Les notebooks du dépôt sont fournis **déjà
exécutés** : sorties et figures sont visibles directement sur GitHub.

Lancer les tests : `pytest tests/`.

## Avancement

| Chapitre | Thème | Statut |
|---|---|---|
| | *Partie I - Fondements des systèmes de données* | |
| 1 | [Applications fiables, scalables et maintenables](notebooks/ch01_fiabilite_scalabilite_maintenabilite) | ✅ |
| 2 | [Modèles de données et langages de requête](notebooks/ch02_modeles_de_donnees) | ✅ |
| 3 | Stockage et recherche | ⬜ |
| 4 | Encodage et évolution | ⬜ |
| | *Partie II - Données distribuées* | |
| 5 | Réplication | ⬜ |
| 6 | Partitionnement | ⬜ |
| 7 | Transactions | ⬜ |
| 8 | Les problèmes des systèmes distribués | ⬜ |
| 9 | Cohérence et consensus | ⬜ |
| | *Partie III - Données dérivées* | |
| 10 | Traitement par lots | ⬜ |
| 11 | Traitement de flux | ⬜ |
| 12 | L'avenir des systèmes de données | ⬜ |
