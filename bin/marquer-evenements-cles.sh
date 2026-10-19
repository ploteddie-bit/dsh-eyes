#!/usr/bin/env bash
# marquer-evenements-cles.sh — marque click_sortant et veille_creee comme
# événements clés dans GA4 (Admin > Affichage des données > Événements)
# dès que Google les a traités (délai ~24 h après le 1er envoi).
#
# Idempotent : lit l'état de l'étoile avant de cliquer (aria-pressed ou
# comparaison du path SVG) ; ne clique JAMAIS une étoile déjà active.
# Log : ~/.dsh-eyes/marquage-cles.log
set -u
TOKEN=$(cat ~/.dsh-eyes/token 2>/dev/null) || { echo "$(date -Is) KO: token introuvable" >> ~/.dsh-eyes/marquage-cles.log; exit 2; }
API=http://127.0.0.1:8445
LOG=~/.dsh-eyes/marquage-cles.log
URL="https://analytics.google.com/analytics/web/#/a376050186p559691236/admin/events/hub?r=194508599"
log() { echo "$(date -Is) $*" >> "$LOG"; }

# 1. Ouvrir/réutiliser l'onglet GA4 (dédup par origine+chemin côté extension)
OPEN=$(curl -s -m 20 -H "Authorization: Bearer $TOKEN" -X POST $API/v2/open \
  -H "Content-Type: application/json" -d "{\"url\":\"$URL\"}")
TAB=$(echo "$OPEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['result']['tab_id'])" 2>/dev/null)
[ -z "${TAB:-}" ] && { log "KO: /v2/open a échoué: $(echo "$OPEN" | head -c 120)"; exit 2; }
sleep 10   # chargement SPA Angular

# 2. Scanner les lignes + basculer les étoiles si besoin (dans la page)
python3 - "$TAB" <<'PY' > /tmp/ga4-eval-req.json
import json, sys
code = r'''
() => {
  const cibles = ["click_sortant", "veille_creee"];
  const rows = [...document.querySelectorAll("tr,[role=row]")];
  const out = {presents: [], actions: [], inconnus: []};
  for (const nom of cibles) {
    const row = rows.find(r => (r.textContent || "").includes(nom));
    if (!row) continue;
    out.presents.push(nom);
    const star = row.querySelector('button[aria-label*="clé"], [role="button"][aria-label*="clé"]');
    if (!star) { out.inconnus.push(nom + ": étoile introuvable"); continue; }
    const pressed = star.getAttribute("aria-pressed");
    if (pressed === "true") { out.actions.push(nom + ": DÉJÀ clé (rien fait)"); continue; }
    if (pressed === "false" || pressed === null) {
      star.click();
      out.actions.push(nom + ": étoile CLIQUÉE (aria-pressed=" + pressed + ")");
    } else {
      out.inconnus.push(nom + ": état inconnu (" + pressed + ") — pas cliqué");
    }
  }
  if (out.presents.length === 0) out.attente = "événements pas encore traités par GA4";
  return out;
}
'''
print(json.dumps({"cmd_id": "marquer-" + str(__import__("time").time_ns()),
                  "cmd": "eval", "params": {"tab_id": int(sys.argv[1]), "code": code}}))
PY
RES=$(curl -s -m 30 -H "Authorization: Bearer $TOKEN" -X POST $API/v2/cmd \
  -H "Content-Type: application/json" --data-binary @/tmp/ga4-eval-req.json)
echo "$RES" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d.get('result', {}).get('value') if d.get('ok') else None
if r is None:
    print('ERREUR: ' + str(d.get('error'))[:200]); raise SystemExit(2)
if r.get('attente'):
    print('ATTENTE'); raise SystemExit(1)
for a in r.get('actions', []) + r.get('inconnus', []): print(a)
if r.get('presents'): raise SystemExit(0)
print('ATTENTE'); raise SystemExit(1)
" 2>&1 | while read -r line; do log "$line"; echo "$line"; done
