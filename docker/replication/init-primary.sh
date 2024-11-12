#!/bin/sh
# Autorise les connexions de réplication (pg_basebackup, flux WAL) depuis le réseau Docker.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
