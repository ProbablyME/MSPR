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

# Attend que la base accepte les connexions avant le premier dump (évite un
# dump vide si le conteneur démarre avant que Postgres soit prêt).
wait_db() {
  i=0
  until pg_isready -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge 60 ] && { echo "[backup] Postgres injoignable après 60 tentatives" >&2; return 1; }
    sleep 2
  done
}

do_backup() {
  wait_db || return 1
  ts="$(date +%Y%m%d_%H%M%S)"
  tmp="$BACKUP_DIR/.dump_${ts}.sql"
  out="$BACKUP_DIR/dump_${ts}.sql.gz"
  echo "[backup] $(date -Is) -> $out"
  # On dump dans un fichier temporaire pour vérifier le CODE DE RETOUR de
  # pg_dump (un pipe `pg_dump | gzip` masquerait son échec derrière gzip).
  if pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" > "$tmp"; then
    gzip -c "$tmp" > "$out"
    rm -f "$tmp"
    echo "[backup] OK ($(du -h "$out" | cut -f1))"
  else
    echo "[backup] ECHEC du pg_dump" >&2
    rm -f "$tmp"
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
