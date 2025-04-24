#!/bin/sh
# Client du lab : prend un bail, travaille, vérifie le bail, puis écrit dans le stockage.
# usage : sh /worker.sh NOM MODE TTL_MS TRAVAIL_S     (MODE = naif ou jetons)
NOM=$1; MODE=$2; TTL=$3; TRAVAIL=$4
# horloge monotone partagée par tous les conteneurs (même noyau) : /proc/uptime, en ms
ms() { awk '{printf "%d", $1 * 1000}' /proc/uptime; }
log() { redis-cli -h storage RPUSH evenements "$(ms) $NOM $1" > /dev/null; }

if [ "$MODE" = "jetons" ]; then
  # bail + jeton de clôture croissant, attribués atomiquement par le service de verrous
  JETON=$(redis-cli -h lock EVAL "if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return redis.call('INCR', KEYS[2]) else return 0 end" 2 verrou compteur "$NOM" "$TTL")
else
  R=$(redis-cli -h lock SET verrou "$NOM" NX PX "$TTL")
  if [ "$R" = "OK" ]; then JETON=1; else JETON=0; fi
fi
if [ "$JETON" = "0" ]; then log "refus_verrou"; exit 0; fi
log "verrou_obtenu $JETON"

sleep "$TRAVAIL"                                   # préparation de l'écriture
V=$(redis-cli -h lock GET verrou)                  # « le bail est-il encore à moi ? »
if [ "$V" != "$NOM" ]; then log "bail_perdu"; exit 0; fi
log "bail_verifie"

T0=$(ms)
sleep 1                                            # une seconde de travail... ou une pause
T1=$(ms)
DUREE=$(( T1 - T0 ))
if [ "$MODE" = "jetons" ]; then
  # le stockage refuse tout jeton plus petit que le plus grand déjà vu
  OK=$(redis-cli -h storage EVAL "local m = tonumber(redis.call('GET', KEYS[1]) or '0') if tonumber(ARGV[1]) < m then return 0 end redis.call('SET', KEYS[1], ARGV[1]) redis.call('SET', KEYS[2], ARGV[2]) return 1" 2 max_jeton fichier "$JETON" "contenu écrit par $NOM (jeton $JETON)")
else
  redis-cli -h storage SET fichier "contenu écrit par $NOM" > /dev/null
  OK=1
fi
if [ "$OK" = "1" ]; then log "ecriture_acceptee $DUREE"; else log "ecriture_refusee $DUREE"; fi
