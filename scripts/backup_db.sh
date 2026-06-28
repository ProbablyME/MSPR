#!/bin/sh
# ============================================================
# Sauvegarde automatique PostgreSQL — ObRail Europe
# pg_dump compressé + rotation. Conçu pour un conteneur sidecar
# (image postgres:16-alpine) tournant à côté du service `postgres`.
#
# Variables (toutes optionnelles sauf le mot de passe via PGPASSWORD) :
#   DB_HOST (postgres)  DB_USER (postgres)  DB_NAME (postgres)
#   BACKUP_DIR (/backups)
#   BACKUP_INTERVAL  secondes entre 2 dumps (86400 = quotidien ; 0 = une seule fois)
#   BACKUP_KEEP_DAYS rétention en jours (7)
# ============================================================
set -eu

DB_HOST="${DB_HOST:-postgres}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
INTERVAL="${BACKUP_INTERVAL:-86400}"
KEEP_DAYS="${BACKUP_KEEP_DAYS:-7}"

mkdir -p "$BACKUP_DIR"

do_backup() {
  ts="$(date +%Y%m%d_%H%M%S)"
  out="$BACKUP_DIR/dump_${ts}.sql.gz"
  echo "[backup] $(date -Is) -> $out"
  if pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" | gzip > "$out.tmp"; then
    mv "$out.tmp" "$out"
    echo "[backup] OK ($(du -h "$out" | cut -f1))"
  else
    echo "[backup] ECHEC du pg_dump" >&2
    rm -f "$out.tmp"
    return 1
  fi
  # Rotation : supprime les dumps plus vieux que KEEP_DAYS jours
  find "$BACKUP_DIR" -name 'dump_*.sql.gz' -type f -mtime +"$KEEP_DAYS" -delete 2>/dev/null || true
}

# Un premier dump immédiat au démarrage, puis périodiquement si demandé.
do_backup || true
if [ "$INTERVAL" -gt 0 ]; then
  while true; do
    sleep "$INTERVAL"
    do_backup || true
  done
fi
