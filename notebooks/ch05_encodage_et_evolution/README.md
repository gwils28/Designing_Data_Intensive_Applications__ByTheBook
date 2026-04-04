# Chapitre 5 - Encodage et évolution

Le chapitre 5 part d'un constat : le code change, les données restent, et pendant un déploiement progressif plusieurs versions du code lisent et écrivent les mêmes données. Il faut donc des encodages qui offrent la compatibilité descendante (le nouveau code lit l'ancien format) et ascendante (l'ancien code lit le nouveau). Les notebooks construisent à la main les encodeurs du livre (MessagePack, Protocol Buffers, Avro) jusqu'à l'octet près, testent automatiquement les règles d'évolution de schéma, puis étudient les modes de flux de données : bases, services REST et RPC (avec un vrai service HTTP local), messages et acteurs, workflows et exécution durable (avec un mini-moteur journalisé).

Les fonctions réutilisables (varints et zigzag, JSON et MessagePack, Protocol Buffers avec conservation des champs inconnus, Thrift BinaryProtocol et CompactProtocol, Avro avec résolution schéma de l'écrivain / schéma du lecteur et vérification statique de compatibilité, fichier conteneur, schéma généré depuis SQLite, registre de schémas à modes de compatibilité, cartes d'octets, simulations de déploiement progressif et de relances) sont dans [`ddia/encoding.py`](../../ddia/encoding.py), testées par [`tests/test_encoding.py`](../../tests/test_encoding.py). Le mini-moteur d'exécution durable (journal, rejeu, injection de pannes, détection du non-déterminisme) est dans [`ddia/durable.py`](../../ddia/durable.py), testé par [`tests/test_durable.py`](../../tests/test_durable.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch05_cours_1_formats_textuels_et_binaires.ipynb`](ch05_cours_1_formats_textuels_et_binaires.ipynb) | introduction du chapitre ; *Formats for Encoding Data* : *Language-Specific Formats*, *JSON, XML, and Binary Variants*, encadré *JSON Schema*, *Binary encoding* | compatibilités descendante et ascendante pendant un déploiement progressif (simulation, diagramme espace-temps du champ perdu) ; mémoire contre octets (diagramme) ; `pickle` : verrouillage, exécution de code au décodage, versions, taille ; JSON et les entiers au-delà de 2^53, Base64, échappement CSV, verbosité de XML ; MessagePack écrit à la main, carte d'octets, où passent les octets ; JSON Schema : validateur minimal, modèles ouverts et fermés, matrice de compatibilité |
| [`ch05_cours_2_protocol_buffers.ipynb`](ch05_cours_2_protocol_buffers.ipynb) | *Protocol Buffers* ; *Field tags and schema evolution* | IDL et génération de code (diagramme) ; varints et zigzag (tailles, distributions réalistes) ; l'exemple en 33 octets, octet de clé et type d'onde, listes compactées ; ajout, suppression, réutilisation de numéro, `reserved`, saut des champs inconnus ; préservation des champs inconnus (diagramme espace-temps, simulation de déploiement) ; troncature int64 vers int32, simple vers répété ; matrice de compatibilité testée automatiquement |
| [`ch05_cours_3_avro_et_schemas.ipynb`](ch05_cours_3_avro_et_schemas.ipynb) | *Avro* : *The writer's schema and the reader's schema*, *Schema evolution rules*, *But what is the writer's schema?*, *Dynamically generated schemas* ; *The Merits of Schemas* | l'exemple en 32 octets et le décodage avec un mauvais schéma ; résolution écrivain/lecteur (diagramme) ; règles d'évolution vérifiées statiquement et sur données ; fichier conteneur, préfixe de registre ou schéma par enregistrement (coûts mesurés), registre de schémas et modes de compatibilité ; export SQLite avec schéma généré, contre numérotation automatique protobuf ; bilan des tailles (brut et gzip) |
| [`ch05_cours_4_flux_par_les_bases.ipynb`](ch05_cours_4_flux_par_les_bases.ipynb) | *Modes of Dataflow* ; *Dataflow Through Databases* : *Different values written at different times*, *Archival storage* | les quatre modes de flux (diagramme) ; base partagée pendant un déploiement (diagramme espace-temps), réécriture d'un document JSON par un ancien modèle ; « les données survivent au code » : versions de schéma sur disque pendant trois ans, avec ou sans réencodage ; `ADD COLUMN` sans réécriture contre reconstruction (SQLite), migration en ligne en quatre étapes ; instantané d'archive avec le dernier schéma, lignes contre colonnes |
| [`ch05_cours_5_services_rest_et_rpc.ipynb`](ch05_cours_5_services_rest_et_rpc.ipynb) | *Dataflow Through Services: REST and RPC* : *Web services*, *The problems with remote procedure calls*, *Load balancers, service discovery, and service meshes*, *Data encoding and evolution for RPC* | architecture de services (diagramme) ; un vrai service REST local ; appel de fonction contre appel HTTP (mesuré) ; issue inconnue après un délai expiré (diagramme espace-temps) ; idempotence réelle : sans clé, clé tardive, clé réservée ; relances et doublons simulés ; serveurs ou clients d'abord (simulation) ; accumulation des versions d'API ; maillage de services (diagramme), DNS en cache contre registre à battements de cœur |
| [`ch05_cours_6_messages_et_acteurs.ipynb`](ch05_cours_6_messages_et_acteurs.ipynb) | *Event-Driven Architectures* : *Message brokers*, *Distributed actor frameworks* ; *Summary* | courtier écrit à la main : tampon pendant une panne (contre échecs RPC), files contre sujets, redistribution au moins une fois (diagramme espace-temps), requête-réponse par corrélation ; registre de schémas et republication qui perd ou préserve un champ ; acteurs répartis sur deux nœuds du simulateur, messages perdus ; bilan du chapitre |
| [`ch05_cours_7_execution_durable.ipynb`](ch05_cours_7_execution_durable.ipynb) | *Durable Execution and Workflows* | workflow de paiement et architecture d'un moteur (diagrammes) ; script relancé qui débite deux fois ; journal et rejeu (diagramme espace-temps tiré de la trace du moteur) ; fenêtre entre l'effet et la journalisation, clés d'idempotence, campagne de pannes aléatoires ; non-déterminisme (hasard) détecté au rejeu ; code réordonné et versions de workflows |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch05_exercices_01-06.ipynb`](ch05_exercices_01-06.ipynb) | 5.1 à 5.6 |
| [`ch05_exercices_07-12.ipynb`](ch05_exercices_07-12.ipynb) | 5.7 à 5.12 |

- **Ex. 5.1** - Compter les octets à la main en JSON, MessagePack, Protocol Buffers (`int64` contre `sint64`) et Avro.
- **Ex. 5.2** - Varints, zigzag et encodage différentiel d'horodatages : tailles et seuils.
- **Ex. 5.3** - Identifiants de 64 bits et doubles (dépassement de 2^53, probabilité de survie), surcoût de Base64.
- **Ex. 5.4** - Écrire un vérificateur d'évolution de schémas Protocol Buffers et l'appliquer à une revue de code.
- **Ex. 5.5** - Décoder un enregistrement Avro à la main, prédire la résolution (alias, promotion, défaut), effet d'un octet corrompu.
- **Ex. 5.6** - Registre de schémas : compatibilité non transitive, rétention et choix du mode.
- **Ex. 5.7** - Modèle de Markov exact des champs perdus pendant un déploiement progressif, effet de sa durée.
- **Ex. 5.8** - Dimensionner le stockage selon la façon de transporter le schéma de l'écrivain ; taille de bloc des fichiers conteneurs.
- **Ex. 5.9** - Quels changements d'API sont sûrs selon l'ordre de déploiement et pour des clients jamais mis à jour.
- **Ex. 5.10** - Délai d'expiration réglé sur la queue de latence, doublons des relances, amplification de charge.
- **Ex. 5.11** - Exécution durable : doublons p/(1-p) sans idempotence, écritures au journal et rejeux, bogue de non-déterminisme.
- **Ex. 5.12** - Faire évoluer un contrat JSON Schema : modèle fermé, modèle ouvert, « strict à l'écriture, tolérant à la lecture ».

## Labs (Docker)

| Notebook | Contenu |
|---|---|
| [`ch05_lab_postgres_evolution.ipynb`](ch05_lab_postgres_evolution.ipynb) | PostgreSQL 16 : `ADD COLUMN` sans réécriture (`attmissingval`, fichier inchangé) ; lignes de deux époques dans les mêmes pages (`pageinspect`) ; changements qui réécrivent (défaut volatil, changement de type) mesurés ; file de verrous d'un `ALTER TABLE` et `lock_timeout` ; documents JSONB de plusieurs versions, réécriture complète contre fusion ; protocole texte contre binaire (`COPY`) |

Démarrage : `docker compose -f docker/encoding/compose.yml -p ddia_encoding up -d` (port 10532) ; arrêt : `docker compose -f docker/encoding/compose.yml -p ddia_encoding down -v`.
