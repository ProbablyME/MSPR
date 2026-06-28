# API de Comparaison Train vs Avion

Cette API développée avec FastAPI permet de récupérer les données comparatives entre les trains et les avions pour des trajets spécifiques, notamment pour analyser l'empreinte carbone (CO2 par km et CO2 par passager).

## Prérequis

1. Python 3.11+ et [uv](https://docs.astral.sh/uv/).
2. Installer les dépendances (workspace, depuis la racine du dépôt) :
   ```bash
   uv sync --all-packages --group dev
   ```
3. Configurer les variables d'environnement dans le fichier `.env` (voir `.env.example`) avec les identifiants de la base de données PostgreSQL et la valeur de `API_TOKEN` (jamais committée).

## Lancement

Serveur de développement (depuis la racine du dépôt) :

```bash
uv run --package railcarbon-api uvicorn api.main:app --reload
```

> En production, l'API est lancée via Docker (`docker compose up`), pas en local.

## Utilisation

L'API est sécurisée par un token Bearer.
Une fois le serveur lancé, accédez à l'interface Swagger :
[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

* Configurez le token en cliquant sur le bouton **Authorize** en haut à droite.
* Saisissez la valeur de `API_TOKEN` définie dans votre `.env`.
* Testez les différents endpoints (ex: `/api/v1/compare/cities`).
