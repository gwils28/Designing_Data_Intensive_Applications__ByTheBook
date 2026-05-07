# Chapitre 14 - Faire ce qui est juste

Le dernier chapitre du livre prend du recul : beaucoup de jeux de données décrivent des personnes, et les systèmes que l'on construit ont des conséquences voulues et non voulues sur leur vie. Après la philosophie des systèmes de flux du chapitre 13, le chapitre 14 examine l'analytique prédictive (biais, discrimination, responsabilité, boucles de rétroaction) puis la collecte elle-même (pistage, surveillance, consentement, vie privée, données comme actif toxique, législation). Le livre argumente sans chiffres ; les notebooks rendent chaque argument mesurable sur des modèles jouets entièrement synthétiques : prison algorithmique, variable proxy et calibration trompeuse, spirales et emballements, ré-identification par quasi-identifiants, profils fantômes, réponse randomisée, exposition aux fuites selon la durée de conservation.

Les fonctions réutilisables (scores partagés et exclusion, population de crédit synthétique avec variable proxy et étiquettes biaisées, régression logistique par la méthode de Newton, métriques d'équité et calibration par groupe, spirale score de crédit / emploi, allocation de patrouilles, recommandeur glouton, population à quasi-identifiants, unicité, k-anonymat et généralisation, attaque par jointure, profils fantômes, réponse randomisée, exposition aux fuites) sont dans [`ddia/ethics.py`](../../ddia/ethics.py), testées par [`tests/test_ethics.py`](../../tests/test_ethics.py). Le journal à crypto-effacement et le coût de la réécriture d'un journal immuable sont dans [`ddia/ethics_gdpr.py`](../../ddia/ethics_gdpr.py), testés par [`tests/test_ethics_gdpr.py`](../../tests/test_ethics_gdpr.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch14_cours_1_analytique_predictive.ipynb`](ch14_cours_1_analytique_predictive.ipynb) | *Predictive Analytics* : *Bias and Discrimination*, *Responsibility and Accountability*, *Feedback Loops* | asymétrie des coûts et seuil de refus ; prison algorithmique : 10 organisations, score partagé ou non (diagramme, simulation) ; biais et discrimination : population synthétique ségréguée, étiquettes historiques biaisées, trois régressions logistiques, la variable proxy (quartier) remplace l'attribut protégé, coefficients appris, calibration trompeuse contre les archives ; responsabilité : même dossier, score différent selon l'adresse, explication additive d'un refus, statistique contre individuel ; boucles de rétroaction : diagramme de boucle causale, spirale score de crédit / emploi simulée, patrouilles au prorata, au point chaud et corrigées, chambre d'écho d'un recommandeur, tarification algorithmique, pensée systémique |
| [`ch14_cours_2_vie_privee_et_surveillance.ipynb`](ch14_cours_2_vie_privee_et_surveillance.ipynb) | *Privacy and Tracking* : *Surveillance*, *Consent and Freedom of Choice*, *Privacy and Use of Data*, *Data as Assets and Power*, *Remembering the Industrial Revolution*, *Legislation and Self-Regulation* | qui est le client (diagramme) ; expérience de pensée données / surveillance ; unicité par quasi-identifiants (théorie et simulation), attaque par jointure, k-anonymat et coût de la généralisation ; profils fantômes selon l'adoption, exigences du RGPD sur le consentement ; ciblage publicitaire sur petites audiences, réponse randomisée (bruit contre déni plausible) ; exposition aux fuites selon la durée de conservation, gouvernements futurs ; analogie avec la révolution industrielle (diagramme) ; finalité et minimisation, catalogue de données, purge simulée, crypto-effacement dans un journal immuable |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|


Pas de lab Docker pour ce chapitre : les notions sont mieux servies par des simulations déterministes sur données synthétiques.
