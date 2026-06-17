"""
Observabilité de l'API ObRail Europe.

Deux briques :
  1. Logging applicatif structuré, poussé vers Loki (mêmes conventions que l'ETL,
     tag ``application=railcarbon-api``) en plus de la sortie console.
  2. Export Prometheus des métriques HTTP (latence, taux d'erreurs, débit) sur
     l'endpoint public ``/metrics``, scrapé par Prometheus puis affiché dans Grafana.

Le handler Loki n'est attaché que si la variable d'environnement ``LOKI_URL`` est
définie (cas du docker-compose). En local / en CI elle est absente : les logs
restent en console et les tests ne tentent aucune connexion réseau.
"""

import logging
import os
import sys
import time

from fastapi import FastAPI, Request
from prometheus_fastapi_instrumentator import Instrumentator

# ── Logger applicatif ────────────────────────────────────────────────────────
logger = logging.getLogger("railcarbon-api")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _console = logging.StreamHandler(sys.stdout)
    _console.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(_console)

    # Handler Loki optionnel — activé uniquement si LOKI_URL est fournie.
    LOKI_URL = os.environ.get("LOKI_URL")
    if LOKI_URL:
        try:
            import logging_loki

            _loki = logging_loki.LokiHandler(
                url=LOKI_URL,
                tags={"application": "railcarbon-api"},
                version="1",
            )
            _loki.setLevel(logging.INFO)
            logger.addHandler(_loki)
            logger.info("[LOGGING] Handler Loki API actif -> %s", LOKI_URL)
        except ImportError:
            logger.info("[LOGGING] python-logging-loki absent — logs Loki désactivés.")
        except Exception as exc:  # pragma: no cover - dépend de l'infra
            logger.warning(
                "[LOGGING] Loki indisponible (%s) — logs console uniquement.", exc
            )


def setup_observability(app: FastAPI) -> None:
    """Branche le middleware de logging et l'export Prometheus sur ``/metrics``."""

    # ── Middleware : journalise chaque requête (méthode, path, statut, durée) ──
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000
        level = logging.WARNING if response.status_code >= 500 else logging.INFO
        logger.log(
            level,
            "%s %s -> %s (%.1f ms)",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            extra={
                "tags": {
                    "method": request.method,
                    "path": request.url.path,
                    "status": str(response.status_code),
                    "level": logging.getLevelName(level),
                }
            },
        )
        return response

    # ── Export Prometheus ─────────────────────────────────────────────────────
    # Métriques par défaut : http_request_duration_seconds (histogramme, labels
    # method/handler/status) -> débit, latence p95 et taux d'erreurs en dérivent.
    Instrumentator(
        should_group_status_codes=False,
        should_ignore_untemplated=True,
        excluded_handlers=["/metrics"],
    ).instrument(app).expose(
        app,
        endpoint="/metrics",
        include_in_schema=True,
        tags=["Sante"],
    )
