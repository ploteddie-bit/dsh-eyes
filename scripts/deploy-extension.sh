#!/usr/bin/env bash
# deploy-extension.sh — déploie l'extension DSH Eyes depuis le dépôt vers
# le dossier chargé par Chrome et vers la copie runtime.
#
# Usage :
#   scripts/deploy-extension.sh <dossier-chargé-par-Chrome>
#
# Exemple :
#   scripts/deploy-extension.sh \
#     /mnt/c/Users/eddie/Downloads/dsh-eyes-extension-1.0.0
#
# Le script sauvegarde les fichiers remplacés en `.bak.<horodatage>`,
# copie les 5 fichiers runtime, vérifie le md5 source/cible et rappelle
# qu'il faut recharger l'extension dans chrome://extensions.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/extension"
RUNTIME="${DSH_EYES_HOME:-$HOME/.dsh-eyes}/extension"
FILES=(background.js manifest.json options.html options.js humanize-math.js)

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <dossier-chargé-par-Chrome>" >&2
  echo "Exemple: $0 /mnt/c/Users/eddie/Downloads/dsh-eyes-extension-1.0.0" >&2
  exit 2
fi

TARGET="$1"
if [[ ! -d "$TARGET" ]]; then
  echo "Erreur : dossier cible introuvable : $TARGET" >&2
  exit 1
fi
if [[ ! -f "$SRC/manifest.json" ]]; then
  echo "Erreur : manifest source introuvable : $SRC/manifest.json" >&2
  exit 1
fi

VERSION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$SRC/manifest.json")"
STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUNTIME"

for f in "${FILES[@]}"; do
  [[ -f "$SRC/$f" ]] || { echo "Erreur : $SRC/$f manquant" >&2; exit 1; }
  if [[ -f "$TARGET/$f" ]]; then
    cp -p "$TARGET/$f" "$TARGET/$f.bak.$STAMP"
  fi
  cp -p "$SRC/$f" "$TARGET/$f"
  cp -p "$SRC/$f" "$RUNTIME/$f"
done

for f in background.js manifest.json; do
  a="$(md5sum "$SRC/$f" | awk '{print $1}')"
  b="$(md5sum "$TARGET/$f" | awk '{print $1}')"
  [[ "$a" == "$b" ]] || { echo "Erreur : md5 différent pour $f" >&2; exit 1; }
done

echo "Extension DSH Eyes $VERSION déployée."
echo "  source  : $SRC"
echo "  chargée : $TARGET"
echo "  runtime : $RUNTIME"
echo
echo "Recharge l'extension : chrome://extensions → DSH Eyes → Recharger."
