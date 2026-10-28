# ClimaCity Paris -- Guide d'installation

Environnement Docker du projet PySpark « ClimaCity Paris » : JupyterLab, PySpark 3.5, Delta Lake 3.2 et MLflow 2.17.

**En bref :**

```bash
docker compose build jupyter   # 5-10 min, ~2 Go
docker compose up -d           # puis attendre 30 s
docker compose ps              # climacity-jupyter et climacity-mlflow : healthy
```

JupyterLab est ensuite accessible sur http://localhost:8888 (token : `climacity`).

---

## Prérequis

| Outil | Version minimale | Installation |
|-------|-----------------|--------------|
| Docker Desktop | 4.25 | https://www.docker.com/products/docker-desktop/ |
| Docker Compose | 2.20 (inclus dans Docker Desktop) | -- |
| RAM disponible | 8 Go (6 Go alloués à Docker) | -- |
| Espace disque | 10 Go libres | -- |

> **Windows** : Docker Desktop doit utiliser le backend WSL 2
> (Paramètres -> General -> « Use the WSL 2 based engine »).
> **macOS Apple Silicon** : l'image est compatible arm64/amd64 via Rosetta.

---

## Installation (première fois)

### 1. Récupérer le projet

```bash
# Via Git
git clone <url-du-depot> climacity-paris
cd climacity-paris

# Ou, avec l'archive ZIP fournie par l'enseignant : l'extraire, puis
cd climacity-paris
```

### 2. Ajuster les ressources

Adaptez le fichier `.env` à votre machine :

```
# Laptop 8 Go RAM (défaut)
DOCKER_MEMORY_LIMIT=6g
SPARK_DRIVER_MEMORY=4g

# Laptop 16 Go RAM
DOCKER_MEMORY_LIMIT=10g
SPARK_DRIVER_MEMORY=7g
```

> Sur macOS/Linux, `make help` liste toutes les commandes du `Makefile`.

### 3. Construire l'image

```bash
docker compose build jupyter
```

Durée : 5-10 minutes selon la connexion (~2 Go téléchargés). L'image embarque
les JARs Delta Lake : aucun téléchargement ne sera nécessaire à la création
des SparkSessions.

### 4. Démarrer et vérifier

```bash
docker compose up -d
docker compose ps
```

Attendez 30 secondes : `climacity-jupyter` et `climacity-mlflow` doivent
afficher le statut `healthy`.

---

## Accès aux services

| Service | URL | Identifiant |
|---------|-----|-------------|
| JupyterLab | http://localhost:8888 | Token : `climacity` |
| Spark UI | http://localhost:4040 | Aucun (disponible après `getOrCreate()`) |
| MLflow UI | http://localhost:5000 | Aucun |

> Le **Spark UI** n'existe qu'une fois une `SparkSession` créée dans un
> notebook. Si le navigateur affiche « Ce site est inaccessible », créez
> d'abord une session.

---

## Structure des répertoires

```
climacity-paris/
├── docker-compose.yml       # Orchestration des services
├── Dockerfile               # Image JupyterLab + PySpark personnalisée
├── requirements.txt         # Paquets Python ajoutés à l'image de base
├── .env                     # Configuration (ports, mémoire, token)
├── Makefile                 # Commandes de gestion (optionnel)
│
├── conf/
│   ├── spark-defaults.conf  # Configuration Spark globale (Delta, mémoire…)
│   └── start-climacity.sh   # Script de démarrage du conteneur
│
├── notebooks/               # Monté dans /home/jovyan/notebooks
│   └── Spark_DIA3_Session_1.ipynb ... Session_6.ipynb
│
├── scripts/                 # Monté dans /home/jovyan/scripts
│   ├── velib_prep.py        # Téléchargement et mise en forme des données
│   ├── simulateur_flux.py   # Simulateur de flux pour le Jour 2
│   ├── mesures.py           # Mesures de performance partagées
│   └── executer_carnets.py  # Rejeu des carnets par paires
│
└── data/                    # Monté dans /home/jovyan/data
    ├── velib/
    │   ├── raw/             # CSV.gz Vélib' historiques (téléchargés au Jour 1)
    │   └── parquet/         # Table Parquet consolidée (produite au Jour 1)
    ├── meteo/               # CSV météo horaire (Open-Meteo)
    └── output/              # Sorties : Delta, modèles, résultats
```

`notebooks/`, `scripts/` et `data/` sont des **volumes montés** : les
modifications faites dans le conteneur sont visibles immédiatement sur l'hôte,
et inversement. Vos données et notebooks survivent aux redémarrages.

---

## Commandes courantes

| Action | Commande |
|--------|----------|
| Démarrer | `docker compose up -d` |
| Arrêter (données conservées) | `docker compose stop` |
| Redémarrer | `docker compose restart` |
| Suivre les logs | `docker compose logs -f` |
| Ouvrir un terminal dans le conteneur | `docker exec -it climacity-jupyter bash` |
| Supprimer les conteneurs (volumes conservés) | `docker compose down` |
| Réinitialisation complète | `docker compose down --volumes` |

> ⚠️ `docker compose down --volumes` **supprime** les données MLflow du volume Docker.

---

## Dépannage

| Symptôme | Cause | Solution |
|----------|-------|----------|
| « Port already in use » au démarrage | Un autre service occupe le port 8888, 4040 ou 5000 | Changer les ports dans `.env` (voir ci-dessous), puis `docker compose up -d` |
| Spark UI (4040) inaccessible | Aucune SparkSession n'existe encore | Exécuter la cellule `SparkSession.builder...getOrCreate()`, attendre 5 s, recharger http://localhost:4040 |
| `OutOfMemoryError` dans un notebook | Mémoire allouée insuffisante | Augmenter la mémoire dans `.env` (voir ci-dessous), puis `docker compose down` et `docker compose up -d` |
| Le simulateur de flux ne produit aucun fichier (Jour 2) | Il n'est pas lancé dans un terminal séparé du conteneur | Voir ci-dessous |
| Échec de `docker compose build` | Le réseau bloque Maven Central (source des JARs Delta Lake) | Utiliser l'image pré-construite (voir ci-dessous) |

**Changer les ports** (`.env`) :

```
JUPYTER_PORT=8889
SPARK_UI_PORT=4041
MLFLOW_PORT=5001
```

**Augmenter la mémoire** (`.env`) :

```
DOCKER_MEMORY_LIMIT=8g
SPARK_DRIVER_MEMORY=6g
```

**Lancer le simulateur de flux** : dans JupyterLab, ouvrez un terminal
(File -> New -> Terminal) et exécutez :

```bash
python /home/jovyan/scripts/simulateur_flux.py \
    --output /home/jovyan/data/output/stream_input \
    --vitesse 3
```

Laissez ce terminal ouvert pendant toute la session de streaming.

**Utiliser l'image pré-construite** si le réseau bloque Maven Central :

```bash
# Enseignant : exporter l'image
docker save climacity-jupyter:latest | gzip > climacity-jupyter.tar.gz

# Étudiant : importer l'image (pas de build nécessaire)
docker load < climacity-jupyter.tar.gz
docker compose up -d
```

---

## Notes techniques

- **PySpark en mode local** : Spark tourne entièrement dans le conteneur Jupyter,
  sans cluster, et utilise tous les coeurs du conteneur (`local[*]`).
- **Delta Lake** : les JARs sont dans l'image (`${SPARK_HOME}/jars/`) et
  `spark-defaults.conf` active les extensions SQL Delta à chaque SparkSession.
- **MLflow** : le conteneur `jupyter` joint `http://mlflow:5000` par le réseau
  Docker interne. Les runs sont persistés dans le volume nommé `climacity_mlflow_data`.
- **Données** : `./data` (hôte) est monté dans `/home/jovyan/data`. Les notebooks
  utilisent des chemins relatifs (`../data/…`) depuis `/home/jovyan/notebooks`.
