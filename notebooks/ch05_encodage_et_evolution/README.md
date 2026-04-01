# Chapitre 5 - Encodage et évolution

Le chapitre 5 part d'un constat : le code change, les données restent, et pendant un déploiement progressif plusieurs versions du code lisent et écrivent les mêmes données. Il faut donc des encodages qui offrent la compatibilité descendante (le nouveau code lit l'ancien format) et ascendante (l'ancien code lit le nouveau). Les notebooks construisent à la main les encodeurs du livre (MessagePack, Protocol Buffers, Avro) jusqu'à l'octet près, testent automatiquement les règles d'évolution de schéma, puis étudient les modes de flux de données : bases, services REST et RPC (avec un vrai service HTTP local), messages et acteurs, workflows et exécution durable (avec un mini-moteur journalisé).

Les fonctions réutilisables (varints et zigzag, JSON et MessagePack, Protocol Buffers avec conservation des champs inconnus, Thrift BinaryProtocol et CompactProtocol, Avro avec résolution schéma de l'écrivain / schéma du lecteur et vérification statique de compatibilité, fichier conteneur, schéma généré depuis SQLite, registre de schémas à modes de compatibilité, cartes d'octets, simulations de déploiement progressif et de relances) sont dans [`ddia/encoding.py`](../../ddia/encoding.py), testées par [`tests/test_encoding.py`](../../tests/test_encoding.py). Le mini-moteur d'exécution durable (journal, rejeu, injection de pannes, détection du non-déterminisme) est dans [`ddia/durable.py`](../../ddia/durable.py), testé par [`tests/test_durable.py`](../../tests/test_durable.py).

## Notebooks de cours

| Notebook | Sections du livre | Contenu |
|---|---|---|
| [`ch05_cours_1_formats_textuels_et_binaires.ipynb`](ch05_cours_1_formats_textuels_et_binaires.ipynb) | introduction du chapitre ; *Formats for Encoding Data* : *Language-Specific Formats*, *JSON, XML, and Binary Variants*, encadré *JSON Schema*, *Binary encoding* | compatibilités descendante et ascendante pendant un déploiement progressif (simulation, diagramme espace-temps du champ perdu) ; mémoire contre octets (diagramme) ; `pickle` : verrouillage, exécution de code au décodage, versions, taille ; JSON et les entiers au-delà de 2^53, Base64, échappement CSV, verbosité de XML ; MessagePack écrit à la main, carte d'octets, où passent les octets ; JSON Schema : validateur minimal, modèles ouverts et fermés, matrice de compatibilité |
| [`ch05_cours_2_protocol_buffers.ipynb`](ch05_cours_2_protocol_buffers.ipynb) | *Protocol Buffers* ; *Field tags and schema evolution* | IDL et génération de code (diagramme) ; varints et zigzag (tailles, distributions réalistes) ; l'exemple en 33 octets, octet de clé et type d'onde, listes compactées ; ajout, suppression, réutilisation de numéro, `reserved`, saut des champs inconnus ; préservation des champs inconnus (diagramme espace-temps, simulation de déploiement) ; troncature int64 vers int32, simple vers répété ; matrice de compatibilité testée automatiquement |

## Labs (Docker)

| Notebook | Contenu |
|---|---|

Démarrage : `docker compose -f docker/encoding/compose.yml -p ddia_encoding up -d` (port 10532) ; arrêt : `docker compose -f docker/encoding/compose.yml -p ddia_encoding down -v`.
