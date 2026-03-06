# API de Comparaison Train vs Avion

Cette API développée avec FastAPI permet de récupérer les données comparatives entre les trains et les avions pour des trajets spécifiques, notamment pour analyser l'empreinte carbone (CO2 par km et CO2 par passager).

## Prérequis

1. Avoir Python 3.9+ d'installé.
2. Installer les dépendances :
   ```bash
   pip install -r requirements.txt
   ```
3. Configurer les variables d'environnement dans le fichier `.env` avec les identifiants de la base de données PostgreSQL. Le token par défaut est `super-secret-temp-token`.

## Lancement

Lancer le serveur de développement :

```bash
cd api
uvicorn main:app --reload
```

## Utilisation

L'API est sécurisée par un token Bearer.
Une fois le serveur lancé, accédez à l'interface Swagger :
[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

* Configurez le token en cliquant sur le bouton **Authorize** en haut à droite.
* Saisissez le token de `.env` (`super-secret-temp-token`).
* Testez les différents endpoints (ex: `/api/v1/compare/cities`).
