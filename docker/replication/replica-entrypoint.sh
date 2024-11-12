#!/bin/sh
# Démarrage d'une réplique : copie de base du primaire (instantané + position du journal),
# puis démarrage en mode standby (fichier standby.signal écrit par pg_basebackup -R).
set -e
DATA=/var/lib/postgresql/data
until pg_isready -h primary -U ddia -d ddia >/dev/null 2>&1; do sleep 1; done
if [ ! -s "$DATA/PG_VERSION" ]; then
  rm -rf "$DATA"/*
  PGPASSWORD=ddia pg_basebackup -d "host=primary user=ddia password=ddia application_name=$REPLICA_NAME" \
    -D "$DATA" -Fp -Xs -R
  chown -R postgres:postgres "$DATA"
  chmod 700 "$DATA"
fi
exec su-exec postgres postgres -c hot_standby=on -c hot_standby_feedback=on
