#!/usr/bin/env bash
# check-close-wait.sh — échantillonne les sockets du démon DSH Eyes
# (port 8445), journalise CLOSE-WAIT/ESTAB et échoue si CLOSE-WAIT > 0.
#
# Utilisé par dsh-eyes-closewait.timer (toutes les 5 min). Le journal
# ~/.dsh-eyes/close-wait.log sert de preuve pour le critère
# « 0 CLOSE-WAIT sur 24 h ».

set -euo pipefail

PORT="${DSH_EYES_PORT:-8445}"
LOG="${DSH_EYES_HOME:-$HOME/.dsh-eyes}/close-wait.log"
mkdir -p "$(dirname "$LOG")"

read -r close_wait estab < <(
  ss -tn 2>/dev/null | awk -v port=":$PORT" '
    $1 == "CLOSE-WAIT" && ($4 ~ port "$" || $5 ~ port "$") { cw++ }
    $1 == "ESTAB"      && ($4 ~ port "$" || $5 ~ port "$") { es++ }
    END { printf "%d %d\n", cw + 0, es + 0 }
  '
)

printf '%s close_wait=%s estab=%s\n' "$(date -Iseconds)" "$close_wait" "$estab" >> "$LOG"

if [[ "$close_wait" -gt 0 ]]; then
  echo "ALERTE: $close_wait socket(s) CLOSE-WAIT sur $PORT à $(date -Iseconds)" >&2
  exit 1
fi
