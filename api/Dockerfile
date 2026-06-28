FROM python:3.11-slim

# Copier uv depuis l'image officielle
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copier les fichiers de dépendances du workspace en premier (cache Docker optimal)
COPY pyproject.toml uv.lock ./
COPY api/pyproject.toml ./api/

# Installer les dépendances de l'API (sans le projet, pour le cache)
RUN uv sync --frozen --no-dev --package railcarbon-api --no-install-project

# Copier le code source de l'API
COPY api/src ./api/src

# Installer le package railcarbon-api
RUN uv sync --frozen --no-dev --package railcarbon-api

# ── Durcissement : exécution en utilisateur non-root ──────────────────────────
# Crée un utilisateur dédié sans privilèges et lui donne /app. L'API tourne
# ensuite sans droits root (bonne pratique sécurité conteneur).
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Lancement direct via le binaire du venv (pas d'invocation `uv` au runtime :
# aucun cache à écrire, compatible utilisateur non-root).
CMD [".venv/bin/python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
