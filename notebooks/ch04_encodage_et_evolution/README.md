# Chapitre 4 - Encodage et évolution

Le chapitre 4 part d'un constat : le code change, les données restent, et pendant un déploiement progressif plusieurs versions du code lisent et écrivent les mêmes données. Il faut donc des encodages qui offrent la compatibilité descendante (le nouveau code lit l'ancien format) et ascendante (l'ancien code lit le nouveau). Les notebooks construisent à la main les encodeurs du livre (MessagePack, Thrift, Protocol Buffers, Avro) jusqu'à l'octet près, testent automatiquement les règles d'évolution de schéma, puis étudient les modes de flux de données : bases, services REST et RPC (avec un vrai service HTTP local), messages et acteurs.

Les fonctions réutilisables (varints et zigzag, JSON et MessagePack, Protocol Buffers avec conservation des champs inconnus, Thrift BinaryProtocol et CompactProtocol, Avro avec résolution schéma de l'écrivain / schéma du lecteur et vérification statique de compatibilité, fichier conteneur, schéma généré depuis SQLite, registre de schémas à modes de compatibilité, cartes d'octets, simulations de déploiement progressif et de relances) sont dans [`ddia/encoding.py`](../../ddia/encoding.py), testées par [`tests/test_encoding.py`](../../tests/test_encoding.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch04_cours_1_formats_textuels_et_binaires.ipynb`](ch04_cours_1_formats_textuels_et_binaires.ipynb) | introduction du chapitre ; *Formats for Encoding Data* : *Language-Specific Formats*, *JSON, XML, and Binary Variants*, *Binary encoding* | compatibilités descendante et ascendante pendant un déploiement progressif (simulation, diagramme espace-temps du champ perdu) ; mémoire contre octets (diagramme) ; `pickle` : verrouillage, exécution de code au décodage, versions, taille ; JSON et les entiers au-delà de 2^53, Base64, échappement CSV, verbosité de XML ; MessagePack écrit à la main, carte d'octets, où passent les octets |
| [`ch04_cours_2_thrift_et_protocol_buffers.ipynb`](ch04_cours_2_thrift_et_protocol_buffers.ipynb) | *Thrift and Protocol Buffers* : *Field tags and schema evolution*, *Datatypes and schema evolution* | IDL Thrift et protobuf, génération de code (diagramme) ; varints et zigzag ; l'exemple en Protocol Buffers (33 octets), Thrift BinaryProtocol (59) et CompactProtocol (34) octet par octet ; ajout, suppression, réutilisation de numéro, champs `required` ; préservation des champs inconnus (diagramme espace-temps, simulation) ; troncature, simple vers répété ; matrice de compatibilité testée automatiquement |
| [`ch04_cours_3_avro_et_schemas.ipynb`](ch04_cours_3_avro_et_schemas.ipynb) | *Avro* : *The writer's schema and the reader's schema*, *Schema evolution rules*, *But what is the writer's schema?*, *Dynamically generated schemas*, *Code generation and dynamically typed languages* ; *The Merits of Schemas* | l'exemple en 32 octets et le décodage avec un mauvais schéma ; résolution écrivain/lecteur (diagramme) ; règles d'évolution vérifiées statiquement et sur données ; fichier conteneur, préfixe de registre ou schéma par enregistrement, registre de schémas ; export SQLite avec schéma généré ; génération de code et langages dynamiques ; bilan des tailles (brut et gzip) |
| [`ch04_cours_4_flux_par_les_bases.ipynb`](ch04_cours_4_flux_par_les_bases.ipynb) | *Modes of Dataflow* ; *Dataflow Through Databases* : *Different values written at different times*, *Archival storage* | les trois modes de flux (diagramme) ; base partagée pendant un déploiement (diagramme espace-temps), réécriture d'un document JSON par un ancien modèle ; « les données survivent au code » : versions de schéma sur disque pendant trois ans ; `ADD COLUMN` sans réécriture contre reconstruction (SQLite), migration en ligne en quatre étapes ; instantané d'archive, lignes contre colonnes |
| [`ch04_cours_5_services_rest_et_rpc.ipynb`](ch04_cours_5_services_rest_et_rpc.ipynb) | *Dataflow Through Services: REST and RPC* : *Web services*, *The problems with remote procedure calls*, *Current directions for RPC*, *Data encoding and evolution for RPC* | architecture de services (diagramme) ; REST contre SOAP, un vrai service REST local ; appel de fonction contre appel HTTP (mesuré) ; issue inconnue après un délai expiré (diagramme espace-temps) ; idempotence réelle ; relances et doublons simulés ; futures et appels en parallèle ; serveurs ou clients d'abord (simulation) ; accumulation des versions d'API |
| [`ch04_cours_6_messages_et_acteurs.ipynb`](ch04_cours_6_messages_et_acteurs.ipynb) | *Message-Passing Dataflow* : *Message brokers*, *Distributed actor frameworks* ; *Summary* | courtier écrit à la main : tampon pendant une panne, files contre sujets, redistribution au moins une fois (diagramme espace-temps), requête-réponse par corrélation ; registre de schémas et republication ; acteurs répartis sur deux nœuds du simulateur ; bilan du chapitre |

## Notebooks d'exercices

| Notebook | Exercices |
|---|---|
| [`ch04_exercices_01-06.ipynb`](ch04_exercices_01-06.ipynb) | 4.1 à 4.6 |
| [`ch04_exercices_07-10.ipynb`](ch04_exercices_07-10.ipynb) | 4.7 à 4.10 |

- **Ex. 4.1** - Compter les octets à la main en JSON, MessagePack, Protocol Buffers (`int64` contre `sint64`), Thrift et Avro.
- **Ex. 4.2** - Varints, zigzag et encodage différentiel d'horodatages : tailles et seuils.
- **Ex. 4.3** - Identifiants de 64 bits et doubles (dépassement de 2^53, probabilité de survie), surcoût de Base64.
- **Ex. 4.4** - Écrire un vérificateur d'évolution de schémas Protocol Buffers et l'appliquer à une revue de code.
- **Ex. 4.5** - Décoder un enregistrement Avro à la main, prédire la résolution (alias, promotion, défaut), effet d'un octet corrompu.
- **Ex. 4.6** - Registre de schémas : compatibilité non transitive, rétention et choix du mode.
- **Ex. 4.7** - Modèle de Markov exact des champs perdus pendant un déploiement progressif, effet de sa durée.
- **Ex. 4.8** - Dimensionner le stockage selon la façon de transporter le schéma de l'écrivain ; taille de bloc des fichiers conteneurs.
- **Ex. 4.9** - Quels changements d'API sont sûrs selon l'ordre de déploiement et pour des clients jamais mis à jour.
- **Ex. 4.10** - Délai d'expiration réglé sur la queue de latence, doublons des relances, amplification de charge.

## Labs (Docker)

| Notebook | Contenu |
|---|---|

Démarrage : `docker compose -f docker/encoding/compose.yml -p ddia_encoding up -d` (port 10532) ; arrêt : `docker compose -f docker/encoding/compose.yml -p ddia_encoding down -v`.
