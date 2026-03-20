from fastapi import FastAPI, Depends
from fastapi.openapi.utils import get_openapi
from .security import verify_token
from . import routers

tags_metadata = [
    {
        "name": "Stations",
        "description": "Gares ferroviaires et aeroports. Recherche par pays, ville, coordonnees GPS.",
    },
    {
        "name": "Routes Train",
        "description": "Routes ferroviaires (gare depart → gare arrivee). Trains de jour et de nuit.",
    },
    {
        "name": "Routes Avion",
        "description": "Routes aeriennes (aeroport depart → aeroport arrivee) avec donnees CO2.",
    },
    {
        "name": "Vehicules",
        "description": "Types de materiel roulant : trains et avions. Donnees EASA CO2 pour les avions.",
    },
    {
        "name": "Emissions",
        "description": "Table de faits des emissions CO2 par passager (train + avion).",
    },
    {
        "name": "Comparaison",
        "description": "Comparaison des emissions CO2 entre train et avion pour un meme trajet.",
    },
    {
        "name": "Recherche",
        "description": "Recherche de villes, destinations et villes communes train/avion.",
    },
    {
        "name": "Classements & Stats",
        "description": "Classements des routes les plus vertes, stats reseau, stats par pays, jour vs nuit.",
    },
    {
        "name": "Sources & ETL",
        "description": "Tracabilite des sources de donnees (RGPD) et monitoring des executions ETL.",
    },
    {
        "name": "Sante",
        "description": "Verification de l'etat de l'API et de la connexion a la base de donnees.",
    },
]

app = FastAPI(
    title="ObRail Europe API",
    description="""
## API REST - Observatoire du ferroviaire europeen

Cette API met a disposition les donnees collectees et transformees par le processus ETL d'ObRail Europe.
Elle permet de :

- **Consulter** les gares, aeroports, routes ferroviaires et aeriennes
- **Comparer** les emissions CO2 entre le train et l'avion pour un meme trajet
- **Explorer** les destinations disponibles et rechercher des villes
- **Analyser** les classements, statistiques reseau et comparaison jour/nuit
- **Tracer** les sources de donnees et l'historique des executions ETL

### Authentification
Toutes les requetes necessitent un token Bearer dans le header `Authorization`.

```
Authorization: Bearer <votre-token>
```
    """,
    version="2.0.0",
    openapi_tags=tags_metadata,
    dependencies=[Depends(verify_token)],
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(routers.router, prefix="/api/v1")


@app.get("/", tags=["Sante"], summary="Page d'accueil de l'API")
def read_root():
    return {
        "message": "Bienvenue sur l'API ObRail Europe.",
        "documentation": "/docs",
        "version": "2.0.0",
    }
