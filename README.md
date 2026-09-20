# Projet 1 — Pipeline météo automatisé : documentation complète

Ce document explique, étape par étape, la construction complète du Projet 1 : un pipeline de données qui récupère automatiquement la météo de dix villes françaises, transforme ces données, vérifie leur qualité, et répète cette opération chaque jour sans intervention humaine. Il est rédigé pour être compréhensible même sans connaissance préalable du domaine, et pour permettre à quiconque de reproduire l'ensemble du projet à l'identique.

---

## 1. Vue d'ensemble : à quoi sert ce projet et comment il fonctionne

Avant d'entrer dans le détail des étapes, voici le principe général du pipeline construit.

Chaque jour, un programme va :

1. Interroger un service météo sur internet (l'API Open-Meteo) pour récupérer la température, l'humidité et le vent de dix villes françaises.
2. Enregistrer ces informations brutes dans une base de données.
3. Transformer et nettoyer ces données pour qu'elles soient exploitables.
4. Vérifier automatiquement que les données ne contiennent pas d'erreurs (valeurs manquantes, doublons).
5. Répéter cette opération automatiquement, sans qu'aucune personne n'ait à lancer quoi que ce soit manuellement.

Pour réaliser cela, plusieurs outils ont été assemblés, chacun ayant un rôle précis :

- **Docker** : un outil qui permet de faire fonctionner des logiciels dans des environnements isolés et reproductibles, appelés conteneurs, sans avoir à les installer directement sur l'ordinateur.
- **Apache Airflow** : le chef d'orchestre du pipeline. C'est lui qui déclenche chaque étape au bon moment, dans le bon ordre, et qui réessaie automatiquement en cas d'échec.
- **PostgreSQL** : une base de données qui stocke les informations météo, aussi bien les données brutes que les données transformées.
- **dbt (data build tool)** : un outil qui transforme les données déjà stockées dans la base, à l'aide de requêtes SQL organisées et versionnées, et qui permet aussi de vérifier automatiquement leur qualité.
- **Python** : le langage de programmation utilisé pour écrire le script qui va chercher les données météo sur internet.
- **Metabase** : un outil open source de visualisation de données, utilisé pour construire un tableau de bord permettant de consulter les résultats du pipeline sous forme de graphiques, sans avoir à écrire de requête SQL.

Deux dossiers principaux ont été créés sur l'ordinateur pour organiser ce travail :

- `C:\data-projects\projet1-meteo\` : contient le script d'extraction des données et le projet dbt de transformation.
- `C:\airflow-project\` : contient la configuration d'Airflow, qui orchestre l'ensemble.

Ces deux dossiers sont, par défaut, complètement isolés l'un de l'autre puisqu'ils utilisent chacun leurs propres conteneurs Docker. Une partie importante du travail a donc consisté à les faire communiquer entre eux, comme expliqué plus loin.

---

## 2. Étape 1 — Installation de Docker Desktop et de WSL2

### Objectif de cette étape

Avant de pouvoir faire fonctionner Airflow, PostgreSQL ou tout autre outil, il faut disposer d'un logiciel capable de faire tourner des conteneurs sur l'ordinateur. C'est le rôle de Docker Desktop.

### Ce qui a été fait

Docker Desktop a été téléchargé et installé depuis le site officiel (docker.com), en choisissant la version correspondant au type de processeur de la machine (AMD64 pour un processeur Intel ou AMD classique, ce qui est le cas de la quasi-totalité des ordinateurs personnels).

Sur Windows, Docker Desktop a besoin d'un composant appelé WSL2 (Windows Subsystem for Linux, version 2) pour fonctionner, car les conteneurs Docker reposent techniquement sur un système Linux, même lorsqu'on travaille sous Windows. WSL2 crée donc une machine Linux légère à l'intérieur de Windows, invisible au quotidien, mais qui fait tourner tous les conteneurs en coulisses.

### Problèmes rencontrés à cette étape

**Choix de l'architecture du processeur.** Un doute est survenu au moment de choisir entre les versions AMD64 et ARM64 de Docker Desktop. Ce choix ne dépend pas du système d'exploitation, mais du type de processeur physique de l'ordinateur : AMD64 pour la quasi-totalité des ordinateurs (processeurs Intel ou AMD), et ARM64 uniquement pour des machines équipées de processeurs ARM, beaucoup plus rares sur PC.

**Message incompréhensible après l'installation de WSL2.** Après l'activation du composant Windows nécessaire à WSL2, une ligne de caractères sans signification apparente est apparue dans le terminal, suivie d'un message d'erreur. Il s'agissait simplement d'un résidu d'affichage du terminal, sans aucune conséquence réelle ; fermer puis rouvrir le terminal après redémarrage de l'ordinateur a suffi à repartir sur une base propre.

**Impossible de configurer la mémoire allouée à Docker.** Docker Desktop permet normalement de choisir combien de mémoire vive (RAM) et de processeur il est autorisé à utiliser. Or, lorsque Docker Desktop utilise WSL2 (ce qui est le cas par défaut), cette configuration ne se fait pas depuis l'interface de Docker, mais depuis un fichier texte nommé `.wslconfig`, à créer manuellement dans le dossier personnel de l'utilisateur Windows. Ce fichier contient des lignes telles que :

```
[wsl2]
memory=6GB
processors=4
swap=2GB
```

Ce fichier indique à WSL2 (et donc indirectement à Docker) qu'il ne doit pas utiliser plus de 6 gigaoctets de mémoire et 4 cœurs de processeur. Une fois ce fichier créé, il faut redémarrer WSL2 pour que le changement soit pris en compte, avec la commande `wsl --shutdown`, puis relancer Docker Desktop.

---

## 3. Étape 2 — Installation d'Apache Airflow

### Objectif de cette étape

Mettre en place l'outil qui va orchestrer automatiquement l'ensemble du pipeline : déclencher l'extraction des données, puis leur transformation, puis leurs tests de qualité, chaque jour, sans intervention humaine.

### Ce qui a été fait

Un dossier dédié, `C:\airflow-project\`, a été créé. Airflow étant lui-même composé de plusieurs programmes qui doivent fonctionner ensemble (une interface web, un planificateur de tâches, une base de données interne, un système de file d'attente, etc.), la méthode la plus simple pour l'installer consiste à utiliser un fichier de configuration nommé `docker-compose.yaml`, qui décrit tous ces programmes et la manière dont ils doivent être démarrés ensemble.

Ce fichier a été téléchargé directement depuis le site officiel d'Apache Airflow. Il décrit, entre autres, les services suivants :

- `postgres` : une base de données PostgreSQL réservée exclusivement au fonctionnement interne d'Airflow (elle stocke la liste des tâches, leur historique, les utilisateurs, etc.). Cette base est totalement différente de celle utilisée pour stocker les données météo.
- `redis` : un système de file d'attente qui permet de distribuer les tâches à exécuter entre plusieurs travailleurs.
- `airflow-apiserver` : le programme qui fait fonctionner l'interface web d'Airflow, accessible depuis un navigateur.
- `airflow-scheduler` : le planificateur, qui décide quand chaque tâche doit être lancée.
- `airflow-dag-processor` : le programme chargé de lire et d'analyser les fichiers Python qui décrivent les pipelines (appelés DAG, voir plus loin).
- `airflow-worker` : le programme qui exécute réellement les tâches.
- `airflow-triggerer` : un programme auxiliaire qui gère les tâches devant attendre un évènement avant de continuer.
- `airflow-init` : un programme qui ne s'exécute qu'une seule fois, au tout premier démarrage, pour préparer la base de données interne et créer l'utilisateur administrateur par défaut.

Quatre dossiers ont ensuite été créés à l'intérieur de `C:\airflow-project\`, tous nécessaires au fonctionnement d'Airflow :

- `dags\` : c'est dans ce dossier que doivent être placés les fichiers Python décrivant les pipelines à exécuter (appelés DAG, pour Directed Acyclic Graph, soit graphe orienté acyclique). Airflow scrute ce dossier en permanence pour détecter automatiquement les nouveaux pipelines ou leurs modifications.
- `logs\` : contient l'historique des exécutions de chaque tâche, utile pour comprendre ce qui s'est passé en cas de succès ou d'échec.
- `plugins\` : permet d'ajouter des fonctionnalités personnalisées à Airflow (non utilisé dans ce projet).
- `config\` : contient la configuration avancée d'Airflow.

Un fichier `.env` a également été créé, contenant une seule ligne (`AIRFLOW_UID=50000`), qui précise à Airflow quel identifiant utilisateur utiliser à l'intérieur des conteneurs Linux, afin d'éviter des problèmes de permissions sur les fichiers partagés.

Airflow a ensuite été démarré en deux temps : d'abord une commande d'initialisation (`docker compose up airflow-init`), qui prépare la base de données et crée l'utilisateur administrateur (identifiant et mot de passe par défaut : `airflow` / `airflow`), puis le démarrage de l'ensemble des services (`docker compose up -d`).

### Problème rencontré à cette étape

Aucun problème bloquant à cette étape précise ; la seule remarque est que la version installée (Airflow 3.3.1) utilise une architecture légèrement différente des versions plus anciennes que l'on trouve dans certains tutoriels : le service auparavant appelé `webserver` est désormais nommé `apiserver`, et le traitement des DAG a été séparé dans un service distinct, `dag-processor`.

---

## 4. Étape 3 — Création d'un environnement Python isolé

### Objectif de cette étape

Écrire un script Python capable d'aller chercher les données météo sur internet nécessite d'installer des outils Python spécifiques (dbt, des bibliothèques de connexion à PostgreSQL, etc.). Pour éviter tout conflit avec d'autres projets ou avec la version de Python déjà présente sur l'ordinateur, ces outils ont été installés dans un environnement isolé, appelé environnement virtuel (venv).

### Ce qui a été fait

Un dossier de projet, `C:\data-projects\projet1-meteo\`, a été créé pour accueillir l'ensemble du travail Python et dbt de ce projet, séparément du dossier `airflow-project`.

Python version 3.11 a été installé spécifiquement pour ce projet (en plus d'une version plus récente, Python 3.14, déjà présente sur l'ordinateur), car certains outils comme dbt ne prennent pas toujours en charge immédiatement les toutes dernières versions de Python.

Un environnement virtuel a ensuite été créé à l'intérieur du dossier du projet, avec la commande `py -3.11 -m venv venv`. Cette commande crée un dossier nommé `venv`, qui contient une copie isolée de Python et de ses outils, indépendante du reste de l'ordinateur. Toute bibliothèque installée à l'intérieur de cet environnement (avec la commande `pip install`) n'affecte que ce projet précis.

### Problèmes rencontrés à cette étape

**Message « No suitable Python runtime found ».** La création de l'environnement virtuel a d'abord échoué, car seule la version 3.14 de Python était installée sur l'ordinateur, alors que la commande demandait spécifiquement la version 3.11, qui n'existait pas encore. La solution a consisté à installer Python 3.11 depuis le site officiel avant de relancer la commande.

**Commande d'activation introuvable.** Une fois Python 3.11 installé, la commande servant à « activer » l'environnement virtuel (`.\venv\Scripts\Activate.ps1`) échouait également, simplement parce que l'environnement virtuel lui-même n'avait pas encore été créé avec succès à l'étape précédente. Ce problème s'est résolu automatiquement une fois l'environnement recréé correctement.

---

## 5. Étape 4 — Installation de dbt et de Great Expectations

### Objectif de cette étape

Installer, à l'intérieur de l'environnement virtuel Python, les deux outils qui serviront respectivement à transformer les données et à vérifier leur qualité.

### Ce qui a été fait

dbt a été installé avec la commande `pip install dbt-postgres`, qui installe à la fois le cœur de l'outil (dbt-core) et l'adaptateur spécifique permettant à dbt de se connecter à une base de données PostgreSQL (des adaptateurs différents existent pour d'autres bases de données, comme BigQuery ou Snowflake).

Great Expectations, un outil de test de qualité de données, a également été installé avec `pip install great_expectations`.

### Problème rencontré à cette étape

Une fois Great Expectations installé, la commande censée en vérifier la version (`great_expectations --version`) a échoué avec un message indiquant que la commande était introuvable, alors que l'installation s'était pourtant bien déroulée. L'explication est que Great Expectations a supprimé, à partir de sa version 1.0, l'interface en ligne de commande qui existait auparavant, au profit d'une utilisation exclusivement pilotée par du code Python. La vérification a donc dû se faire différemment, directement en Python :

```python
import great_expectations as gx
print(gx.__version__)
```

Compte tenu de la complexité supplémentaire introduite par cette nouvelle version de Great Expectations, il a été décidé de démarrer le projet avec uniquement les tests de qualité intégrés à dbt (qui sont amplement suffisants pour les besoins de ce projet), et de réserver l'intégration de Great Expectations à une phase ultérieure.

---

## 6. Étape 5 — Création de la base de données PostgreSQL du projet

### Objectif de cette étape

Mettre en place une base de données dédiée exclusivement au stockage des données météo, distincte de la base de données interne d'Airflow.

### Ce qui a été fait

Un fichier `docker-compose.yml` a été créé dans `C:\data-projects\projet1-meteo\`, décrivant un unique conteneur PostgreSQL :

```yaml
services:
  postgres_projet1:
    image: postgres:16
    container_name: postgres_projet1
    environment:
      POSTGRES_USER: dbt_user
      POSTGRES_PASSWORD: dbt_password
      POSTGRES_DB: meteo_db
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Ce fichier indique à Docker de démarrer une base de données PostgreSQL, avec un utilisateur nommé `dbt_user`, un mot de passe, et une base nommée `meteo_db`. Le port `5433` a volontairement été choisi (plutôt que le port standard `5432`) pour que cette base de données du projet ne rentre pas en conflit avec la base de données interne d'Airflow, qui utilise déjà le port 5432 de son côté. La ligne `volumes` garantit que les données ne sont pas perdues si le conteneur est arrêté ou redémarré : elles sont conservées sur le disque de l'ordinateur, dans un espace de stockage géré par Docker.

---

## 7. Étape 6 — Initialisation du projet dbt

### Objectif de cette étape

Créer la structure de dossiers et de fichiers nécessaire pour que dbt puisse transformer les données de la base `meteo_db`.

### Ce qui a été fait

Le projet dbt a été créé avec la commande `dbt init projet1_meteo`, exécutée depuis le dossier `C:\data-projects\projet1-meteo\`. Cette commande pose une série de questions pour configurer la connexion à la base de données, et crée automatiquement un dossier `projet1_meteo\` contenant la structure standard d'un projet dbt :

- `dbt_project.yml` : le fichier de configuration principal du projet dbt. Il indique notamment où se trouvent les modèles (les fichiers de transformation), et comment ils doivent être matérialisés dans la base de données (sous forme de vue ou de table, par exemple).
- `models\` : le dossier destiné à contenir les fichiers SQL de transformation des données (voir étape suivante).
- `macros\`, `seeds\`, `snapshots\`, `tests\`, `analyses\` : des dossiers standards de dbt, non utilisés en profondeur dans ce projet, mais présents par défaut (respectivement pour des fonctions SQL réutilisables, des données de référence statiques, un suivi historique des changements, des tests personnalisés et des requêtes d'analyse ponctuelles).
- `README.md` et `.gitignore` : des fichiers standards générés automatiquement par dbt.

Les informations de connexion saisies pendant l'initialisation (adresse du serveur, port, utilisateur, mot de passe, nom de la base) sont enregistrées par dbt dans un fichier séparé, `profiles.yml`, habituellement stocké dans le dossier personnel de l'utilisateur (`C:\Users\NomUtilisateur\.dbt\profiles.yml`), et non dans le dossier du projet lui-même. Ce point a eu son importance plus tard, lorsqu'il a fallu faire fonctionner dbt depuis l'intérieur d'un conteneur Airflow (voir étape 10).

### Problèmes rencontrés à cette étape

**Erreur « list index out of range ».** Lors de l'initialisation, dbt pose la question suivante : « Which database would you like to use ? [1] postgres — Enter a number ». Il s'agit ici de choisir un numéro dans une liste (ici, il n'y avait qu'un seul choix possible, le numéro 1, pour PostgreSQL), et non de renseigner un numéro de port ou toute autre information technique. En répondant par erreur avec un numéro de port (5433) plutôt qu'avec le choix de liste attendu (1), dbt a tenté d'aller chercher un élément inexistant dans sa liste de choix, provoquant une erreur technique.

**Message « A project called projet1_meteo already exists here ».** La première tentative d'initialisation, interrompue par l'erreur précédente, avait déjà eu le temps de créer un dossier partiel nommé `projet1_meteo`. Toute nouvelle tentative se heurtait donc à ce dossier déjà existant. La solution a consisté à supprimer entièrement ce dossier partiel avant de relancer la commande d'initialisation depuis le début.

**Confusion de dossier lors de la création des modèles.** Un dossier `models\staging\` a par erreur été créé directement dans `C:\data-projects\projet1-meteo\`, au lieu d'être créé à l'intérieur du dossier du projet dbt lui-même, `C:\data-projects\projet1-meteo\projet1_meteo\`. dbt exige que ses fichiers de configuration et ses modèles se trouvent au même niveau que le fichier `dbt_project.yml` ; le dossier mal placé a donc dû être déplacé au bon endroit avec la commande `Move-Item`.

---

## 8. Étape 7 — Écriture du script d'extraction des données météo

### Objectif de cette étape

Écrire le programme Python chargé d'aller chercher les données météo actuelles de dix villes françaises sur internet, et de les enregistrer dans la base de données PostgreSQL du projet.

### Contenu détaillé du fichier `extract_weather.py`

Ce fichier a été placé dans un dossier séparé, `C:\data-projects\projet1-meteo\extraction\`, afin de bien distinguer le code qui va chercher les données brutes (l'extraction) du code qui les transforme ensuite (dbt). Il contient plusieurs parties :

- Une liste nommée `VILLES`, qui contient les dix villes françaises choisies pour ce projet (Paris, Marseille, Lyon, Toulouse, Nice, Nantes, Strasbourg, Montpellier, Bordeaux, Lille), chacune associée à ses coordonnées géographiques précises (latitude et longitude), nécessaires pour interroger le service météo.
- Un dictionnaire nommé `DB_CONFIG`, qui contient les informations de connexion à la base de données PostgreSQL du projet (adresse, port, nom de la base, utilisateur, mot de passe).
- Une fonction `fetch_weather`, qui envoie une requête au service Open-Meteo (une API météorologique gratuite et sans inscription nécessaire) pour une ville donnée, en demandant précisément la température à deux mètres du sol, l'humidité relative, la vitesse du vent à dix mètres, et un code représentant les conditions météorologiques générales (ensoleillé, pluvieux, etc.).
- Une fonction `create_table_if_not_exists`, qui crée, si elle n'existe pas déjà, une table nommée `raw_weather` dans la base de données. Cette table contient une colonne pour chaque information récupérée (ville, coordonnées, température, humidité, vitesse du vent, code météo), ainsi qu'un identifiant unique et un horodatage indiquant précisément quand l'extraction a eu lieu.
- Une fonction `insert_weather_data`, qui enregistre, pour une ville donnée, les données récupérées dans la table `raw_weather`.
- Une fonction `main`, qui orchestre l'ensemble : elle se connecte à la base de données, s'assure que la table existe, puis parcourt la liste des dix villes une par une, récupère leurs données météo et les enregistre, en affichant un message de confirmation pour chaque ville traitée. Si une erreur survient pour une ville en particulier (par exemple, une coupure de connexion internet), le programme continue avec les villes suivantes plutôt que de s'arrêter complètement.

### Vérification effectuée

Après exécution du script (`python extract_weather.py`), une connexion directe à la base de données (via la commande `docker exec -it postgres_projet1 psql -U dbt_user -d meteo_db`) a permis de confirmer que les dix lignes de données étaient bien présentes dans la table `raw_weather`, avec des valeurs cohérentes.

### Problème rencontré à cette étape

Aucun problème technique n'est apparu lors de l'écriture et de la première exécution de ce script ; il a fonctionné correctement dès la première tentative complète.

---

## 9. Étape 8 — Transformation des données avec dbt

### Objectif de cette étape

Transformer les données brutes stockées dans `raw_weather` pour créer une version propre et enrichie de ces données, et définir des règles de vérification automatique de leur qualité.

### Contenu détaillé des fichiers créés

Trois fichiers ont été créés dans le dossier `C:\data-projects\projet1-meteo\projet1_meteo\models\staging\` :

**`sources.yml`** — Ce fichier déclare à dbt l'existence de la table `raw_weather`, qui n'a pas été créée par dbt lui-même mais par le script Python d'extraction. En la déclarant comme une « source », dbt peut ensuite y faire référence de façon fiable dans ses modèles de transformation, plutôt que d'écrire son nom en dur, ce qui facilite la maintenance si jamais le nom de la table ou de la base venait à changer.

**`stg_weather.sql`** — Il s'agit du premier modèle de transformation (le préfixe « stg » signifie « staging », c'est-à-dire une étape préliminaire de nettoyage). Ce fichier contient une requête SQL qui sélectionne l'ensemble des colonnes de la table brute, et ajoute une colonne calculée supplémentaire, `date_extraction`, qui extrait uniquement la date (sans l'heure précise) à partir de l'horodatage complet d'extraction. Ce modèle est ensuite exécuté par dbt avec la commande `dbt run`, qui crée automatiquement dans la base de données une vue nommée `stg_weather`, correspondant au résultat de cette requête. Une vue, contrairement à une table, ne stocke pas les données une seconde fois : elle recalcule le résultat de la requête à chaque fois qu'on la consulte, ce qui garantit qu'elle reflète toujours les données les plus récentes de la table brute.

**`schema.yml`** — Ce fichier définit les tests de qualité de données appliqués au modèle `stg_weather`. Quatre règles ont été définies : la colonne `id` doit être à la fois unique (aucun doublon) et jamais vide, et les colonnes `ville`, `temperature` et `humidite` ne doivent jamais être vides non plus. Ces règles sont ensuite vérifiées automatiquement par la commande `dbt test`, qui exécute une requête SQL de vérification pour chacune d'entre elles, et signale un échec si une règle n'est pas respectée sur au moins une ligne de données.

### Problèmes rencontrés à cette étape

**Erreur « No dbt_project.yml found ».** La commande `dbt run` a échoué à plusieurs reprises parce qu'elle était lancée depuis le mauvais dossier. dbt exige d'être exécuté depuis le dossier qui contient directement le fichier `dbt_project.yml` (c'est-à-dire `projet1_meteo\`, et non le dossier parent `projet1-meteo\`, dont le nom est très proche et prête facilement à confusion). Ce problème a nécessité plusieurs vérifications avec la commande `pwd` (qui affiche le dossier courant) avant d'être définitivement résolu.

**Avertissement sur une configuration inutilisée.** Après suppression du modèle d'exemple généré automatiquement par dbt (`models\example\`), un avertissement est resté affiché à chaque exécution, signalant qu'une configuration faisait toujours référence à ce dossier supprimé dans le fichier `dbt_project.yml`. La solution a consisté à retirer manuellement la ligne de configuration correspondante dans ce fichier.

---

## 10. Étape 9 — Mise en réseau : connecter Airflow à la base de données du projet

### Objectif de cette étape

Le dossier `airflow-project` et le dossier `projet1-meteo` utilisent chacun leur propre fichier `docker-compose`, ce qui signifie que Docker les considère comme deux ensembles totalement indépendants et isolés l'un de l'autre, y compris au niveau du réseau. Autrement dit, sans intervention supplémentaire, les conteneurs d'Airflow ne peuvent pas du tout communiquer avec le conteneur `postgres_projet1`, alors que c'est pourtant indispensable pour que le pipeline automatisé fonctionne.

### Ce qui a été fait

Un réseau Docker partagé, nommé `data_platform_net`, a été créé manuellement avec la commande `docker network create data_platform_net`. Contrairement aux réseaux créés automatiquement par chaque `docker-compose`, ce réseau n'appartient à aucune des deux stacks en particulier : il sert de passerelle commune entre elles.

Ce réseau a ensuite été rattaché à deux endroits :

- Dans le fichier `docker-compose.yml` du projet 1, le conteneur `postgres_projet1` a été explicitement rattaché à ce réseau, en plus de son réseau habituel.
- Dans le fichier `docker-compose.yaml` d'Airflow, l'ensemble des services Airflow (scheduler, worker, apiserver, triggerer, dag-processor, init) ont également été rattachés à ce même réseau. Comme tous ces services partagent une configuration commune appelée `x-airflow-common` dans le fichier, une seule modification à cet endroit a suffi à les connecter tous en même temps, plutôt que de devoir modifier chaque service individuellement.

Une fois cette configuration appliquée et les deux stacks redémarrées, un test de connectivité a été réalisé directement depuis l'intérieur d'un conteneur Airflow, en lui demandant de résoudre le nom `postgres_projet1` vers une adresse réseau, ce qui a confirmé que la communication entre les deux stacks fonctionnait correctement.

### Problème rencontré à cette étape

Après avoir ajouté ce second réseau, plusieurs conteneurs Airflow (le planificateur et surtout le processeur de DAG) sont passés au statut `unhealthy` (en mauvaise santé) dans Docker. En examinant les journaux détaillés du mécanisme de vérification de santé (healthcheck), il est apparu que les commandes de vérification internes d'Airflow fonctionnaient correctement, mais mettaient parfois légèrement plus de temps que prévu à répondre, très probablement à cause de la légère latence supplémentaire introduite par la présence d'un second réseau. La limite de temps autorisée pour cette vérification (fixée par défaut à dix secondes) a donc été augmentée à trente secondes dans le fichier `docker-compose.yaml`, pour les services concernés, ce qui a définitivement résolu le problème.

---

## 11. Étape 10 — Construction d'une image Airflow personnalisée

### Objectif de cette étape

L'image Airflow utilisée jusqu'ici est une image officielle « nue », qui ne contient ni dbt, ni les bibliothèques Python nécessaires pour se connecter à PostgreSQL ou pour envoyer des requêtes internet. Il fallait donc construire une version personnalisée de cette image, y ajoutant ces outils de façon permanente.

### Contenu détaillé du fichier `Dockerfile`

Un fichier nommé `Dockerfile` a été créé dans `C:\airflow-project\`. Un Dockerfile est une recette de construction d'image Docker, lue ligne par ligne :

```dockerfile
FROM apache/airflow:3.3.1

USER airflow

RUN pip install --no-cache-dir \
    dbt-postgres==1.11.0 \
    requests==2.34.2 \
    psycopg2-binary==2.9.13
```

La première ligne indique que la nouvelle image doit partir de l'image officielle d'Airflow, version 3.3.1, comme base de départ. La deuxième ligne précise que les commandes suivantes doivent s'exécuter avec l'utilisateur `airflow` (et non l'administrateur du système, pour des raisons de sécurité). La troisième instruction installe, à l'intérieur de cette image, les trois bibliothèques nécessaires : `dbt-postgres` (pour transformer les données), `requests` (pour interroger l'API météo, bien qu'elle ne soit en réalité utile qu'au script d'extraction) et `psycopg2-binary` (pour permettre à Python de se connecter à une base PostgreSQL).

Le fichier `docker-compose.yaml` d'Airflow a ensuite été modifié pour utiliser cette nouvelle image personnalisée : la ligne indiquant l'image officielle (`image: apache/airflow:3.3.1`) a été mise en commentaire, et remplacée par une instruction `build: .`, qui indique à Docker de construire l'image à partir du `Dockerfile` présent dans le dossier courant, plutôt que de télécharger une image toute prête.

### Problème rencontré à cette étape

La toute première tentative de construction de l'image a échoué avec le message « failed to read dockerfile: open Dockerfile: no such file or directory », alors que le fichier venait pourtant d'être créé avec Notepad. La cause de cette erreur, déjà rencontrée plus tôt dans le projet avec d'autres fichiers, est que Notepad ajoute automatiquement l'extension `.txt` à la fin des fichiers si elle n'est pas explicitement précisée à l'enregistrement, donnant ainsi un fichier nommé `Dockerfile.txt` plutôt que `Dockerfile`. La vérification du contenu réel du dossier (avec la commande `dir`) a permis de confirmer ce diagnostic, et le fichier a été recréé directement en ligne de commande pour éviter ce piège.

---

## 12. Étape 11 — Mise à disposition des fichiers du projet à l'intérieur des conteneurs Airflow

### Objectif de cette étape

Même une fois l'image Airflow personnalisée construite, les conteneurs n'ont, par défaut, accès qu'aux dossiers `dags`, `logs`, `config` et `plugins` d'Airflow. Ils n'ont aucune visibilité sur le script d'extraction ni sur le projet dbt, qui se trouvent physiquement dans un tout autre dossier de l'ordinateur (`C:\data-projects\projet1-meteo\`). Il fallait donc rendre ces dossiers visibles depuis l'intérieur des conteneurs.

### Ce qui a été fait

Deux lignes supplémentaires ont été ajoutées à la section `volumes` du fichier `docker-compose.yaml` d'Airflow :

```yaml
    - C:/data-projects/projet1-meteo/extraction:/opt/airflow/projet1/extraction
    - C:/data-projects/projet1-meteo/projet1_meteo:/opt/airflow/projet1/dbt
```

Chacune de ces lignes établit un lien direct (appelé montage de volume) entre un dossier réel de l'ordinateur Windows (à gauche des deux points) et un chemin à l'intérieur des conteneurs Linux d'Airflow (à droite des deux points). Concrètement, tout fichier présent dans `C:\data-projects\projet1-meteo\extraction` sur Windows devient immédiatement visible et modifiable depuis l'intérieur des conteneurs Airflow, à l'emplacement `/opt/airflow/projet1/extraction`, et de même pour le dossier du projet dbt. Cette approche permet d'utiliser directement les fichiers existants du projet, sans avoir à en dupliquer une copie dans le dossier `dags` d'Airflow.

Un fichier `profiles.yml` a également dû être créé directement dans le dossier du projet dbt (`C:\data-projects\projet1-meteo\projet1_meteo\profiles.yml`), afin qu'il soit lui aussi visible depuis les conteneurs Airflow (le fichier `profiles.yml` généré initialement par dbt se trouvait uniquement dans le dossier personnel de l'utilisateur Windows, invisible depuis les conteneurs). Ce nouveau fichier utilise des variables d'environnement plutôt que des valeurs écrites en dur, afin de pouvoir fonctionner aussi bien en local (sur Windows, avec le port 5433) que depuis l'intérieur d'un conteneur Airflow (avec le port interne 5432) :

```yaml
projet1_meteo:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('METEO_DB_HOST', 'localhost') }}"
      port: "{{ env_var('METEO_DB_PORT', '5433') | as_number }}"
      user: "{{ env_var('METEO_DB_USER', 'dbt_user') }}"
      password: "{{ env_var('METEO_DB_PASSWORD', 'dbt_password') }}"
      dbname: "{{ env_var('METEO_DB_NAME', 'meteo_db') }}"
      schema: public
      threads: 4
```

Pour la même raison, le script `extract_weather.py` a également été modifié afin que ses paramètres de connexion à la base de données (adresse, port, etc.) soient lus depuis des variables d'environnement plutôt qu'écrits en dur, avec des valeurs par défaut correspondant à une utilisation en local.

### Problème rencontré à cette étape

Aucune erreur technique particulière n'est survenue lors de cette étape ; la vérification, en listant le contenu des deux dossiers depuis l'intérieur d'un conteneur, a confirmé dès la première tentative que les fichiers étaient bien visibles.

---

## 13. Étape 12 — Écriture du pipeline Airflow (le DAG)

### Objectif de cette étape

Écrire le fichier qui décrit à Airflow les trois étapes du pipeline à exécuter chaque jour, dans le bon ordre : extraction, transformation, puis tests.

### Contenu détaillé du fichier `dag_meteo_pipeline.py`

Ce fichier a été placé dans le dossier `C:\airflow-project\dags\`, seul emplacement scruté automatiquement par Airflow pour détecter les pipelines à exécuter. Il contient :

- Deux constantes, `DBT_PROJECT_DIR` et `EXTRACTION_DIR`, qui indiquent les chemins, à l'intérieur des conteneurs, vers respectivement le projet dbt et le script d'extraction (les chemins définis à l'étape précédente).
- Un dictionnaire `ENV_VARS`, qui contient les informations de connexion à la base de données du projet, adaptées au contexte des conteneurs (adresse `postgres_projet1` et port `5432`, plutôt que `localhost` et `5433` utilisés en dehors des conteneurs).
- Un dictionnaire `default_args`, qui définit des paramètres appliqués par défaut à toutes les tâches du pipeline, notamment le nombre de nouvelles tentatives autorisées en cas d'échec (trois tentatives) et le délai d'attente entre chaque tentative (deux minutes).
- Un bloc principal `with DAG(...)`, qui définit le pipeline lui-même : son identifiant (`pipeline_meteo_projet1`), une description, la fréquence d'exécution automatique (`@daily`, c'est-à-dire une fois par jour), une date de départ, ainsi qu'un paramètre `catchup=False`, qui empêche Airflow de vouloir automatiquement rattraper toutes les exécutions passées qui auraient dû avoir lieu entre la date de départ choisie et aujourd'hui.
- Trois tâches, chacune définie avec un `BashOperator`, c'est-à-dire un type de tâche Airflow qui se contente d'exécuter une commande shell classique :
  - `extraction_meteo`, qui se déplace dans le dossier d'extraction et exécute le script Python.
  - `transformation_dbt`, qui se déplace dans le dossier du projet dbt et exécute la commande `dbt run`.
  - `tests_qualite_dbt`, qui exécute ensuite la commande `dbt test` dans ce même dossier.
- Une dernière ligne, `extraction >> transformation_dbt >> tests_dbt`, qui définit l'ordre d'exécution des trois tâches à l'aide d'un symbole représentant une flèche : l'extraction doit impérativement se terminer avant que la transformation ne débute, elle-même suivie des tests, et non l'inverse.

### Problèmes rencontrés à cette étape

**Le fichier n'apparaissait pas dans l'interface d'Airflow.** Après création du fichier, aucun pipeline n'apparaissait dans la liste des DAG affichée par l'interface web d'Airflow, même après plusieurs minutes d'attente. Après vérification, il s'est avéré que le composant chargé d'analyser périodiquement les fichiers du dossier `dags` (le `dag-processor`) n'avait tout simplement pas encore effectué son premier passage complet sur ce nouveau fichier au moment de la vérification. Un redémarrage manuel de ce composant (`docker restart`) a immédiatement forcé une nouvelle analyse complète, faisant apparaître le pipeline dans l'interface.

**Message d'erreur « dbt: command not found ».** Une fois le pipeline détecté et lancé une première fois, la tâche d'extraction s'est bien exécutée avec succès, mais la tâche de transformation a échoué avec un message indiquant que la commande `dbt` était introuvable, alors même que son bon fonctionnement avait pourtant été vérifié précédemment à l'intérieur des conteneurs. La cause de cette erreur est technique et assez subtile : lorsqu'un dictionnaire de variables d'environnement personnalisées (`env=ENV_VARS`) est transmis à un `BashOperator` sans autre précision, Airflow remplace intégralement l'environnement du programme exécuté, au lieu de simplement y ajouter les nouvelles variables. Or, cet environnement contenait justement la information indiquant où se trouve l'exécutable `dbt` sur le système (une variable appelée `PATH`). En la supprimant accidentellement, le système ne savait plus où chercher la commande. La solution a consisté à ajouter le paramètre `append_env=True` à chacune des trois tâches du pipeline, ce qui indique à Airflow de conserver l'environnement d'origine et d'y ajouter les nouvelles variables, plutôt que de le remplacer entièrement.

---

## 14. Étape 13 — Construction du dashboard de visualisation avec Metabase

### Objectif de cette étape

Une fois les données collectées, transformées et validées, il restait à les rendre lisibles pour un utilisateur humain, sous la forme d'un tableau de bord visuel. C'est le rôle de Metabase, un outil open source de visualisation de données, qui se connecte directement à une base de données et permet de construire des graphiques sans avoir à écrire de code.

### Ce qui a été fait

Un nouveau service a été ajouté au fichier `docker-compose.yml` du Projet 1, afin que le dashboard reste rattaché à ce projet précis et puisse être démontré de façon autonome, sans dépendre des autres projets du portfolio :

```yaml
  metabase_projet1:
    image: metabase/metabase:latest
    container_name: metabase_projet1
    ports:
      - "3002:3000"
    volumes:
      - metabase_data:/metabase-data
    networks:
      - data_platform_net
```

Ce service démarre un conteneur Metabase, accessible depuis le navigateur à l'adresse `http://localhost:3002`. Le port `3002` a été choisi spécifiquement pour ne pas entrer en conflit avec d'autres outils de visualisation déjà utilisés ailleurs dans le portfolio (Grafana sur le port 3000 pour le Projet 2, et une autre instance de Metabase sur le port 3001 pour le Projet 3). Le service a été rattaché au réseau partagé `data_platform_net`, ce qui lui permet de communiquer directement avec le conteneur `postgres_projet1`, exactement de la même façon que les conteneurs Airflow le font pour exécuter le pipeline.

Lors de la configuration initiale de Metabase, un compte administrateur local a été créé, puis une connexion a été établie vers la base de données du projet, avec les paramètres suivants : hôte `postgres_projet1` (et non `localhost`, puisque Metabase s'exécute lui-même à l'intérieur d'un conteneur, sur le même réseau que la base de données), port `5432` (le port interne du conteneur PostgreSQL, différent du port `5433` utilisé pour s'y connecter depuis l'extérieur), nom de base `meteo_db`, utilisateur `dbt_user`.

### Contenu du dashboard

Quatre visualisations ont été créées à partir de la vue `stg_weather` (la version nettoyée et enrichie des données, produite par dbt, plutôt que la table brute `raw_weather`) :

- **Température par ville** : un graphique en barres affichant la température moyenne relevée pour chacune des dix villes.
- **Humidité par ville** : un graphique en barres équivalent, appliqué à l'humidité relative.
- **Vitesse du vent par ville** : un graphique en barres équivalent, appliqué à la vitesse du vent.
- **Détail des relevés météo** : un tableau affichant l'ensemble des colonnes brutes (ville, température, humidité, vitesse du vent, date d'extraction), permettant de consulter chaque relevé individuellement.

Ces quatre visualisations ont ensuite été assemblées au sein d'un même tableau de bord, nommé « Météo France — Vue d'ensemble », avec les trois graphiques en barres disposés côte à côte, et le tableau détaillé affiché en dessous, sur toute la largeur.

Un point important de cette architecture est que le dashboard n'a besoin d'aucune configuration supplémentaire pour rester à jour : puisque Metabase interroge directement la vue `stg_weather` à chaque consultation, et que cette vue reflète toujours le contenu le plus récent de la base de données, chaque nouvelle exécution quotidienne du pipeline Airflow met automatiquement à jour l'ensemble des graphiques, sans qu'aucune action manuelle ne soit nécessaire sur Metabase lui-même.

### Problème rencontré à cette étape

Aucun problème technique particulier n'est survenu lors de cette étape ; la connexion à la base de données et la création des visualisations se sont déroulées sans erreur dès la première tentative.

---

## 15. Architecture finale détaillée des dossiers

### Dossier `C:\data-projects\projet1-meteo\`

```
projet1-meteo/
├── docker-compose.yml          Définit les conteneurs PostgreSQL (base meteo_db, port 5433) et Metabase (port 3002)
├── venv/                       Environnement Python isolé (Python 3.11), contient toutes les bibliothèques installées
├── extraction/
│   └── extract_weather.py      Script qui récupère la météo des dix villes et l'enregistre dans PostgreSQL
└── projet1_meteo/               Le projet dbt
    ├── dbt_project.yml          Fichier de configuration principal de dbt
    ├── profiles.yml              Informations de connexion à la base de données, compatibles local et conteneur
    ├── README.md                 Fichier généré automatiquement par dbt
    ├── .gitignore                 Fichier généré automatiquement par dbt
    ├── models/
    │   └── staging/
    │       ├── sources.yml        Déclare la table brute raw_weather comme source pour dbt
    │       ├── stg_weather.sql    Requête de transformation qui nettoie et enrichit les données brutes
    │       └── schema.yml         Définit les tests de qualité de données appliqués au modèle stg_weather
    ├── macros/                    Dossier standard dbt, non utilisé dans ce projet
    ├── seeds/                     Dossier standard dbt, non utilisé dans ce projet
    ├── snapshots/                 Dossier standard dbt, non utilisé dans ce projet
    ├── tests/                     Dossier standard dbt, non utilisé dans ce projet
    ├── analyses/                  Dossier standard dbt, non utilisé dans ce projet
    ├── target/                    Dossier généré automatiquement par dbt à chaque exécution (fichiers compilés)
    └── logs/                      Journal des exécutions de dbt
```

### Dossier `C:\airflow-project\`

```
airflow-project/
├── docker-compose.yaml         Définit l'ensemble des services Airflow, PostgreSQL interne et Redis
├── Dockerfile                   Recette de construction de l'image Airflow personnalisée avec dbt et les bibliothèques Python
├── .env                          Contient la variable AIRFLOW_UID nécessaire au bon fonctionnement des permissions
├── dags/
│   └── dag_meteo_pipeline.py     Le pipeline Airflow qui orchestre extraction, transformation et tests, chaque jour
├── logs/                          Historique détaillé de chaque exécution de chaque tâche
├── plugins/                       Extensions personnalisées d'Airflow (vide dans ce projet)
└── config/                        Configuration avancée d'Airflow, notamment le fichier airflow.cfg
```

### Le réseau partagé entre les deux dossiers

En dehors de ces deux dossiers, un réseau Docker nommé `data_platform_net` a été créé indépendamment (il n'appartient physiquement à aucun des deux dossiers). Ce réseau est ce qui permet aux conteneurs Airflow, définis dans `airflow-project`, de communiquer avec le conteneur `postgres_projet1`, défini dans `projet1-meteo`, alors même que ces deux ensembles de conteneurs sont démarrés et gérés de façon totalement indépendante l'un de l'autre.

---

## 16. Récapitulatif de l'ensemble des problèmes rencontrés

| Étape | Problème | Cause | Solution |
|---|---|---|---|
| Installation de Docker | Confusion entre AMD64 et ARM64 | Dépend du processeur, pas du système d'exploitation | Vérification du type de processeur dans les paramètres Windows |
| Installation de WSL2 | Ligne de caractères incompréhensible dans le terminal | Résidu d'affichage du terminal sans conséquence réelle | Fermeture et réouverture du terminal après redémarrage |
| Configuration de Docker | Impossible de régler la mémoire allouée depuis l'interface | Les ressources sont gérées par WSL2 et non par Docker Desktop | Création manuelle d'un fichier `.wslconfig` |
| Environnement Python | « No suitable Python runtime found » | Python 3.11 n'était pas encore installé | Installation explicite de Python 3.11 |
| Environnement Python | Commande d'activation du venv introuvable | Le venv n'avait pas pu être créé à cause du problème précédent | Résolu automatiquement une fois Python 3.11 installé |
| Great Expectations | Commande en ligne de commande introuvable | Interface en ligne de commande supprimée depuis la version 1.0 | Vérification via un script Python plutôt qu'en ligne de commande |
| Initialisation de dbt | Erreur « list index out of range » | Réponse incorrecte à la question de choix de base de données | Répondre par le numéro de choix (1) et non par un numéro de port |
| Initialisation de dbt | « A project called projet1_meteo already exists here » | Dossier partiel créé lors d'une tentative précédemment interrompue | Suppression du dossier partiel avant nouvelle tentative |
| Transformation dbt | « No dbt_project.yml found » | Commande lancée depuis le mauvais dossier | Se replacer dans le dossier contenant directement dbt_project.yml |
| Mise en réseau | Conteneurs Airflow passés en état « unhealthy » | Vérification de santé trop rapide compte tenu de la latence réseau ajoutée | Augmentation du délai de la vérification de santé |
| Image Airflow personnalisée | « failed to read dockerfile : no such file or directory » | Notepad avait enregistré le fichier sous le nom Dockerfile.txt | Recréation du fichier avec le nom exact, sans extension parasite |
| Pipeline Airflow | Le pipeline n'apparaissait pas dans l'interface | Le composant d'analyse des DAG n'avait pas encore scanné le nouveau fichier | Redémarrage manuel du composant d'analyse des DAG |
| Pipeline Airflow | « dbt : command not found » | Les variables d'environnement personnalisées avaient remplacé l'environnement d'origine, supprimant l'accès à dbt | Ajout du paramètre append_env=True à chaque tâche |

---

## 17. Résultat final

À l'issue de l'ensemble de ces étapes, le pipeline `pipeline_meteo_projet1` s'exécute intégralement et automatiquement chaque jour, sans aucune intervention manuelle : il récupère la météo actuelle de dix villes françaises, l'enregistre dans une base de données, la transforme et l'enrichit, puis vérifie automatiquement l'absence de valeurs manquantes ou de doublons. Les trois étapes du pipeline (extraction, transformation, tests) se terminent avec succès, ce qui est visible dans l'interface web d'Airflow par un statut vert pour chacune des trois tâches.

Les données ainsi produites sont enfin rendues visibles à travers un tableau de bord Metabase, accessible à l'adresse `http://localhost:3002`, qui se met à jour automatiquement à chaque nouvelle exécution du pipeline, sans aucune intervention manuelle supplémentaire.
