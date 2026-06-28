from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from . import routers
from .config import settings
from .observability import setup_observability
from .security import verify_token

# Rate limiting global : limite par IP, appliquée à toutes les routes via le
# middleware. Désactivable (tests) par RATE_LIMIT_ENABLED.
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[settings.RATE_LIMIT_DEFAULT],
    enabled=settings.RATE_LIMIT_ENABLED,
)

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

### Correspondance avec le cahier des charges
Certains endpoints portent un nom plus precis que celui du cahier des charges
(renommage autorise). Table de correspondance :

| Cahier des charges | Endpoint(s) reel(s)                          |
|--------------------|----------------------------------------------|
| `/trajets`         | `/routes/train` + `/routes/avion`            |
| `/stats/volumes`   | `/stats/network` (+ `/stats/by-country`)     |
    """,
    version="2.0.0",
    openapi_tags=tags_metadata,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Rate limiting : enregistre le limiter, le handler 429 et le middleware global.
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# CORS : autorise l'acces direct a l'API (hors reverse-proxy nginx) depuis les
# origines configurees. Derriere nginx ce middleware est inoffensif ; il evite
# de casser les appels directs (front en dev, outils externes).
_cors_origins = settings.cors_origins_list
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    # allow_credentials incompatible avec allow_origins=["*"] (spec CORS).
    allow_credentials=_cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# L'authentification protège uniquement les routes métier (/api/v1/*).
# La racine "/" et l'endpoint Prometheus "/metrics" restent publics : ce dernier
# doit être scrapable sans jeton par le service Prometheus interne.
app.include_router(
    routers.router,
    prefix="/api/v1",
    dependencies=[Depends(verify_token)],
)

# Logging applicatif (Loki) + export Prometheus /metrics.
setup_observability(app)


@app.get("/", tags=["Sante"], summary="Page d'accueil de l'API")
def read_root():
    return {
        "message": "Bienvenue sur l'API ObRail Europe.",
        "documentation": "/docs",
        "version": "2.0.0",
    }
