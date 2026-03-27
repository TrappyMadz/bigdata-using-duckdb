# Municipales 2026 - Data Engine

Moteur d'analyse haute performance pour les élections municipales françaises de 2026. Ce projet utilise **DuckDB** pour interroger des fichiers **Parquet** massifs stockés sur un bucket S3 (Cellar), le tout exposé via une API **FastAPI**.

## Dépendances

Le projet nécessite Python 3.10+. Les bibliothèques principales sont :
* **FastAPI & Uvicorn** : Framework web et serveur pour l'API.
* **DuckDB** : Moteur SQL analytique (OLAP) ultra-rapide.
* **Botocore / Boto3** : Client pour la connexion au stockage S3 (S3-compatible).
* **Pydantic** : Validation des données et schémas de réponses JSON.
* **Python-dotenv** : Gestion des variables d'environnement.

## Architecture du Projet

| Fichier / Dossier | Utilité |
| :--- | :--- |
| `data/` | Dossier où placer vos fichiers sources bruts (CSV). |
| `output/` | Stockage local des fichiers `.parquet` générés avant upload. |
| `.env.exemple` | **Template crucial.** Contient la structure des clés S3 et le nom du bucket. À copier en `.env`. |
| `config.py` | **Cœur de la configuration.** Initialise la connexion DuckDB partagée, configure les accès S3 et définit les chemins vers les fichiers Parquet (`files`). |
| `main.py` | Point d'entrée de l'API FastAPI. Contient la déclaration des routes. |
| `metrics.py` | Logique SQL pour les indicateurs politiques (victoires, nuances, consolidation T1+T2). |
| `models.py` | Définition de tous les schémas Pydantic (modèles de données). |
| `results.py` | Logique d'agrégation des résultats par candidat et par liste. |
| `shared_functions.py` | Fonctions utilitaires, notamment le générateur dynamique de clauses `WHERE`. |
| `stats.py` | Logique SQL pour les statistiques descriptives (sièges, démographie). |
| `uploads.py` | **Pipeline de données.** Script pour convertir les fichiers de `/data` en Parquet et les téléverser sur le S3. |

## Installation et Lancement

### Préparation de l'environnement
Copiez le fichier d'exemple et remplissez vos accès S3 (Access Key, Secret Key, Endpoint) :
```bash
cp .env.exemple .env
```
### Pipeline : Conversion et Upload

Placez vos fichiers électoraux bruts dans le dossier /data, puis lancez le traitement automatique :
```bash
python uploads.py
```

Le script génère les fichiers Parquet dans /output et les pousse sur le bucket S3 configuré.
### Lancement de l'API

Une fois les données sur le S3, démarrez le serveur :
```bash
uvicorn main:app --reload
```
> L'API est alors accessible sur http://localhost:8000.
> La documentation interactive (Swagger) est disponible sur http://localhost:8000/docs.
## Quelques Endpoints clés

`/france/politique` : Synthèse nationale consolidée des nuances politiques.

`/communes/code/{code}` : Résultats détaillés par ville via le code INSEE.

`/france/stats-sieges` : Statistiques descriptives sur la taille des conseils municipaux.
