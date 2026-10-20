#!/usr/bin/env bash
# lancer-chrome.sh — Chrome DÉDIÉ au pont dsh-eyes (modèle chrome-pmu).
#
# Profil Windows séparé (C:\Users\eddie\dsh-chrome) + extension DSH Eyes
# chargée au lancement : le pont se reconnecte au démon (WSL localhost
# forwarding) dès l'ouverture, SANS dépendre du Chrome personnel d'Eddie
# (leçon de la nuit du 20/10 : fenêtres/profils/comptes qui se mélangent).
# La session Google de ce profil se connecte UNE fois puis persiste.
set -eu
CHROME="/mnt/c/Program Files/Google/Chrome/Application/chrome.exe"
EXT="/mnt/c/Users/eddie/Downloads/dsh-eyes-extension-0.5.0"
PROFIL_WIN='C:\Users\eddie\dsh-chrome'

"$CHROME" \
  --user-data-dir="$PROFIL_WIN" \
  --load-extension="$(wslpath -w "$EXT")" \
  --no-first-run --no-default-browser-check \
  "https://analytics.google.com" >/dev/null 2>&1 &

echo "Chrome dédié lancé (profil dsh-chrome + extension chargée)"
echo "→ 1re fois : connecte-toi à Google (plot.eddie@gmail.com) — ensuite ça persiste"
