from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database settings
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "postgres"
    
    # Security settings — pas de valeur par défaut : doit venir de l'env / .env
    API_TOKEN: str

    # CORS : origines autorisees pour l'acces direct a l'API (hors nginx).
    # Liste separee par des virgules, ex: "http://localhost:5173,https://obrail.eu".
    # "*" autorise toutes les origines (pratique en dev, a restreindre en prod).
    CORS_ORIGINS: str = "*"

    # Rate limiting (slowapi) — protège l'API d'un abus / déni de service basique.
    # Désactivé en tests pour ne pas fausser les appels répétés (cf. conftest).
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_DEFAULT: str = "120/minute"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]


settings = Settings()
