# Installation de la stack Data Engineering — Projet 1 (Pipeline météo)

Ce document retrace le processus complet d'installation et de configuration des outils utilisés pour le Projet 1 : un pipeline ETL batch orchestré avec Apache Airflow, dbt, PostgreSQL et Great Expectations, sur un environnement Windows. Il inclut les erreurs rencontrées pendant l'installation et la manière dont elles ont été résolues, afin de servir de référence pour de futures installations ou pour d'autres membres d'une équipe.

---

## 1. Docker Desktop et WSL2

### Utilité

**Docker** permet d'exécuter des applications dans des conteneurs isolés, reproductibles et portables. Dans un contexte de data engineering, il est essentiel pour faire tourner des outils complexes (Airflow, PostgreSQL, Kafka, etc.) sans avoir à les installer directement sur la machine, avec toutes les incompatibilités que cela peut engendrer.

**WSL2** (Windows Subsystem for Linux, version 2) est la couche de compatibilité qui permet à Docker Desktop de fonctionner sur Windows en exécutant un véritable noyau Linux léger. Docker Desktop en dépend obligatoirement sur Windows.

### Étapes d'installation

1. Téléchargement et installation de Docker Desktop depuis le site officiel (docker.com).
2. Activation automatique du backend WSL2 pendant l'installation.
3. Redémarrage de la machine pour finaliser l'activation du composant Windows `VirtualMachinePlatform`.
4. Vérification post-redémarrage avec les commandes PowerShell :
   ```powershell
   wsl --status
   wsl --version
   ```

### Erreurs rencontrées et solutions

**Erreur 1 — Choix de l'architecture (AMD64 ou ARM64)**
Au moment du téléchargement de Docker Desktop, une hésitation est survenue sur le choix entre les versions AMD64 et ARM64.
*Solution* : le choix dépend du processeur de la machine, et non du système d'exploitation. Pour la quasi-totalité des PC (processeurs Intel ou AMD Ryzen), la version **AMD64** est la bonne. La version ARM64 ne concerne que les machines équipées de processeurs ARM (ex. Surface Pro X, Copilot+ PC à puce Snapdragon). La vérification se fait via **Paramètres Windows > Système > Informations système**, à la ligne « Type de système ».

**Erreur 2 — Résidu de terminal après l'installation de WSL2**
Après l'installation du composant `VirtualMachinePlatform`, une ligne de caractères incompréhensible est apparue dans PowerShell (`[9;15;9;0;32;1_`), suivie d'une erreur `MissingTypename`.
*Solution* : il s'agissait d'un simple résidu d'une séquence d'échappement terminal mal interprétée par PowerShell, sans conséquence sur l'installation. La fermeture et la réouverture d'une nouvelle fenêtre PowerShell après redémarrage de la machine ont permis de repartir sur une base propre.

**Erreur 3 — Configuration des ressources (RAM/CPU) introuvable**
L'onglet **Settings > Resources > Advanced** de Docker Desktop affichait le message suivant : *« You are using the WSL 2 backend, so resource limits are managed by Windows »*, sans curseur de configuration disponible.
*Solution* : lorsque Docker Desktop utilise le backend WSL2, les ressources ne se configurent pas depuis l'interface Docker, mais via un fichier `.wslconfig` à créer manuellement dans le dossier utilisateur Windows (`%USERPROFILE%`) :
```ini
[wsl2]
memory=6GB
processors=4
swap=2GB
```
Après création du fichier, un redémarrage de WSL2 est nécessaire :
```powershell
wsl --shutdown
```
puis relance de Docker Desktop. La vérification de la prise en compte de la configuration se fait avec :
```powershell
wsl -d docker-desktop -- cat /proc/meminfo
```

---

## 2. Apache Airflow

### Utilité

Apache Airflow est un orchestrateur de workflows qui permet de planifier, exécuter et surveiller des pipelines de données sous forme de DAG (graphe orienté acyclique). Il gère les dépendances entre tâches, les tentatives automatiques en cas d'échec (retries), les alertes, et offre une interface web de suivi.

### Étapes d'installation (via Docker)

1. Création d'un dossier de projet dédié :
   ```powershell
   mkdir C:\airflow-project
   cd C:\airflow-project
   ```
2. Téléchargement du fichier `docker-compose.yaml` officiel :
   ```powershell
   curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
   ```
3. Création des dossiers requis par Airflow :
   ```powershell
   mkdir dags,logs,plugins,config
   ```
4. Création du fichier `.env` avec la variable d'utilisateur :
   ```powershell
   "AIRFLOW_UID=50000" | Out-File -Encoding ascii .env
   ```
5. Initialisation de la base de données interne d'Airflow :
   ```powershell
   docker compose up airflow-init
   ```
6. Démarrage de l'ensemble des services :
   ```powershell
   docker compose up -d
   ```
7. Vérification du bon fonctionnement des conteneurs :
   ```powershell
   docker ps
   ```
8. Accès à l'interface web via `http://localhost:8080`, identifiants par défaut : `airflow` / `airflow`.

### Points d'attention

- La version installée (Airflow 3.3.1) utilise une architecture légèrement différente des tutoriels basés sur Airflow 2.x : le service `webserver` est remplacé par `apiserver`, et un service `dag-processor` distinct a été introduit.
- Le conteneur `worker` peut afficher un statut `health: starting` plus longtemps que les autres services au démarrage ; cela ne constitue pas une anomalie.

---

## 3. Python 3.11 (environnement virtuel dédié)

### Utilité

Un environnement virtuel Python (venv) isole les dépendances d'un projet du reste du système, évitant les conflits de versions entre différents projets. Il est nécessaire ici car dbt-core ne supporte pas toujours immédiatement les toutes dernières versions de Python (ici, Python 3.14 déjà installé sur la machine), ce qui impose l'utilisation d'une version antérieure et stable (Python 3.11).

### Étapes d'installation

1. Téléchargement et installation de Python 3.11 depuis python.org, en cochant impérativement l'option **« Add python.exe to PATH »**.
2. Fermeture et réouverture de PowerShell afin de recharger la variable PATH.
3. Création de l'environnement virtuel :
   ```powershell
   cd C:\data-projects\projet1-meteo
   py -3.11 -m venv venv
   ```
4. Activation de l'environnement virtuel :
   ```powershell
   .\venv\Scripts\Activate.ps1
   ```

### Erreurs rencontrées et solutions

**Erreur 1 — `No suitable Python runtime found`**
La commande `py -3.11 -m venv venv` a échoué car seule la version Python 3.14 était installée sur la machine ; Python 3.11 n'existait pas encore.
*Solution* : installation explicite de Python 3.11 depuis python.org, en plus de la version 3.14 déjà présente (les deux versions coexistent sans conflit grâce au lanceur `py`).

**Erreur 2 — `Activate.ps1` non reconnu**
La commande d'activation du venv a échoué car le dossier `venv\Scripts\` n'existait pas encore, la création du venv ayant elle-même échoué à l'étape précédente.
*Solution* : résolue automatiquement une fois Python 3.11 correctement installé et le venv recréé avec succès.

**Erreur 3 — Politique d'exécution PowerShell**
Dans certains environnements, l'activation du venv peut être bloquée par la politique de sécurité PowerShell.
*Solution* :
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## 4. dbt-core (adaptateur PostgreSQL)

### Utilité

dbt (data build tool) permet de transformer des données directement dans l'entrepôt de données (ici PostgreSQL) à l'aide de requêtes SQL organisées en modèles versionnés. Il apporte également un système de tests de qualité de données intégré (valeurs nulles, unicité, cohérence des plages de valeurs) et une documentation générée automatiquement.

### Étapes d'installation

1. Installation du package incluant dbt-core et l'adaptateur PostgreSQL :
   ```powershell
   pip install dbt-postgres
   ```
2. Vérification de l'installation :
   ```powershell
   dbt --version
   ```
3. Initialisation du projet dbt :
   ```powershell
   dbt init projet1_meteo
   ```

### Erreurs rencontrées et solutions

**Erreur 1 — `IndexError: list index out of range` pendant `dbt init`**
Lors du choix de l'adaptateur de base de données, la question posée par dbt était :
```
Which database would you like to use?
[1] postgres
Enter a number:
```
La réponse `5433` (le numéro de port) a été saisie par erreur à la place du numéro de choix de la liste, provoquant une erreur car `5433` ne correspond à aucun index valide.
*Solution* : il fallait répondre `1` pour sélectionner PostgreSQL dans la liste ; les informations de connexion (host, port, utilisateur, mot de passe, etc.) sont demandées dans les questions suivantes.

**Erreur 2 — `A project called projet1_meteo already exists here`**
La première tentative de `dbt init` (interrompue par l'erreur précédente) avait déjà créé un dossier partiel `projet1_meteo`, empêchant toute nouvelle initialisation.
*Solution* : suppression du dossier partiel avant de relancer la commande :
```powershell
Remove-Item -Recurse -Force projet1_meteo
dbt init projet1_meteo
```

### Configuration finale validée

```
host: localhost
port: 5433
user: dbt_user
dbname: meteo_db
schema: public
threads: 4
```

Validation de la connexion avec `dbt debug`, confirmée par le message `All checks passed!`.

---

## 5. PostgreSQL (instance dédiée au projet)

### Utilité

PostgreSQL sert ici d'entrepôt de données (data warehouse) pour stocker les données météo brutes puis transformées par dbt. Une instance distincte de celle utilisée par Airflow a été mise en place afin de ne pas mélanger les métadonnées internes d'Airflow avec les données métier du projet.

### Étapes d'installation

Création d'un fichier `docker-compose.yml` dédié dans le dossier du projet :

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

Lancement du conteneur :
```powershell
docker compose up -d
```

### Point d'attention

Le port **5433** (et non 5432) a été utilisé pour exposer ce PostgreSQL sur la machine hôte, afin d'éviter tout conflit avec le PostgreSQL interne d'Airflow, qui occupe déjà le port 5432 par défaut.

---

## 6. Great Expectations

### Utilité

Great Expectations (GX) est un outil de test et de validation de la qualité des données. Il permet de définir des règles explicites (« expectations ») sur les données — par exemple l'absence de valeurs nulles, le respect d'une plage de valeurs, ou l'unicité d'une colonne — et de générer une documentation lisible des résultats de validation.

### Étapes d'installation

```powershell
pip install great_expectations
```

Vérification :
```powershell
python -c "import great_expectations as gx; print(gx.__version__)"
```

### Erreur rencontrée et solution

**Erreur — `great_expectations : The term 'great_expectations' is not recognized`**
La commande `great_expectations --version` échouait alors que le package était pourtant bien installé (confirmé par `pip install`).
*Cause* : depuis la version 1.0, Great Expectations a supprimé son interface en ligne de commande (CLI) au profit d'une utilisation exclusivement pilotée par du code Python, dans le cadre d'une refonte globale de son API.
*Solution* : vérifier l'installation via Python plutôt que via une commande CLI :
```python
import great_expectations as gx
print(gx.__version__)
```
Toute utilisation ultérieure de GX (création de contexte, définition d'expectations, validation) se fait désormais via des scripts Python, et non plus via des commandes terminal.

### Décision d'implémentation

Pour ce projet, il a été décidé de démarrer avec les tests natifs de dbt (`dbt test`) pour couvrir les besoins de qualité de données de base (valeurs nulles, doublons, plages de valeurs), et d'intégrer Great Expectations dans une phase ultérieure du projet, une fois le pipeline de base opérationnel.

---

## 7. Récapitulatif de la stack finale

| Outil | Rôle | Port(s) |
|---|---|---|
| Docker Desktop + WSL2 | Exécution des conteneurs | — |
| Apache Airflow 3.3.1 | Orchestration des pipelines | 8080 |
| PostgreSQL (Airflow) | Métadonnées internes d'Airflow | 5432 (interne) |
| PostgreSQL (projet) | Entrepôt de données météo | 5433 |
| Python 3.11 (venv) | Environnement d'exécution isolé | — |
| dbt-core 1.12.4 + adaptateur postgres | Transformation des données | — |
| Great Expectations 1.23.0 | Tests de qualité de données (phase ultérieure) | — |

---

## 8. Leçons retenues

- Toujours redémarrer PowerShell (voire la machine) après une modification touchant WSL2 ou une variable PATH, afin d'éviter des erreurs de commande introuvable.
- Bien lire l'intitulé exact de chaque question posée par un outil en ligne de commande (`dbt init` notamment) avant de répondre, pour éviter de fournir une valeur au mauvais endroit.
- Vérifier systématiquement l'état d'un dossier avant de relancer une commande d'initialisation ayant précédemment échoué, certains outils créant des fichiers partiels avant de planter.
- Se méfier des changements de version majeurs d'un outil (comme le passage de Great Expectations à la version 1.x) qui peuvent supprimer des fonctionnalités présentes dans la documentation ou les tutoriels plus anciens.
