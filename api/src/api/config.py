from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Database settings
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_NAME: str = "postgres"
    
    # Security settings
    API_TOKEN: str = "my-secret-token-123"  # A hardcoder temporairement, à supprimer plus tard

    class Config:
        env_file = ".env"

settings = Settings()
