[English](README.md) | **Français**

> Créé par DeepSeek et Z.ai.

# dsh-eyes

Démon d'observation des navigations Chrome + **mains** du navigateur
(modèle webbridge — REWORK 2026-10-05).

- Endpoint HTTP+WS : `http://127.0.0.1:8445` (health, stats, events, alerts, ws)
- Routes v1 (ouvertes) : `GET /health`, `GET /stats`, `GET /events`, `GET /alerts`, `WS /ws`
- Routes v2 (auth Bearer) : `GET /v2/info`, `GET /v2/metrics`, `GET /v2/ping`, `POST /v2/cmd`, `POST /v2/act`, `POST /v2/act/cancel`
- Source : `src/` (Rust, axum 0.7 + tokio + tokio-tungstenite)
- Tests : `tests/` — **141 tests exécutés** (21/10/2026), `cargo test` ; 2 ignorés (live Ollama + doc-test `ignore`)
- **v1.1.0** : auth WS par token + handshake `hello`, garde-fou SSRF centralisé sur `/v2/cmd`, humanisation CDP, rotation des logs, `GET /v2/info`, métriques `GET /v2/metrics`, act batch idempotent (`POST /v2/act`, annulation coopérative), rate limit 120/min (`429 rate_limited`)
- Extension Chrome : `extension/` (MV3, à charger via `chrome://extensions` mode développeur)
- Cockpit : `docs/cockpit.html`

## Architecture (REWORK 2026-10-05)

L'extension Chrome **déjà installée** dans le navigateur de l'utilisateur
(id `lmjgnljbpcahodolfncbnchomamnkjoi`) sert à la fois de **vigil**
(observation des navigations) et de **mains** (exécution de commandes
`goto`, `click`, `read`, `fill`, `tabs_create` via `chrome.tabs` et
`chrome.scripting.executeScript`). Le démon ne spawn plus de Chrome
dédié — il parle à l'extension via la même WS `ws://127.0.0.1:8445/ws`,
devenue **bidirectionnelle**.

Le modèle de spawn CDP + `chromiumoxide` a été **abandonné** au profit
de ce modèle webbridge. Voir `~/.superpowers/sdd/2026-10-05-dsh-eyes-v2-hands-solveur/task-2-report.md`
(section REWORK) pour le détail.

## Routes v2 — `POST /v2/cmd`

```bash
# Exemple : naviguer vers une URL
curl -X POST \
  -H "Authorization: Bearer $(cat ~/.dsh-eyes/token)" \
  -H "Content-Type: application/json" \
  -d '{"cmd":"goto","params":{"url":"https://example.com"}}' \
  http://127.0.0.1:8445/v2/cmd
```

Codes de retour :

- **200** + `{ok:true, result:...}` — l'extension a exécuté la commande.
- **401** — token absent ou invalide.
- **502** + `{ok:false, error:"..."}` — l'extension a renvoyé `ok:false`
  (sélecteur introuvable, page cross-origin, etc.).
- **504** + `{ok:false, error:"no_extension_connected"|"extension_timeout_30s"|"extension_disconnected"}`
  — pas d'extension connectée, ou l'extension n'a pas répondu dans
  les 30 s.

### Auth WebSocket (v1.1.0)

`/ws` exige le token d'API en query string (l'API WebSocket des navigateurs
ne permet pas de header `Authorization` personnalisé) :

```bash
TOKEN=$(cat ~/.dsh-eyes/token)
wscat -c "ws://127.0.0.1:8445/ws?token=$TOKEN"
```

L'extension lit le token dans `chrome.storage.local` (clé `dsh_token`).
Après installation : `chrome://extensions` → carte DSH Eyes → **Options** →
coller le contenu de `~/.dsh-eyes/token`. Le service worker envoie ensuite un
handshake `hello` (client, version, capacités) exposé par `GET /v2/info` :

```bash
curl -s -H "Authorization: Bearer $(cat ~/.dsh-eyes/token)" \
  http://127.0.0.1:8445/v2/info
```

### Act batch — idempotence et annulation (v1.1.0)

`POST /v2/act` exécute une séquence d'étapes dans l'ordre :

```bash
curl -X POST -H "Authorization: Bearer $(cat ~/.dsh-eyes/token)" \
  -H "Content-Type: application/json" \
  -d '{"action_id":"batch-1","steps":[
    {"cmd":"tabs_create","params":{"url":"https://example.com"}},
    {"cmd":"read","params":{"mode":"outline"}}
  ]}' http://127.0.0.1:8445/v2/act
```

- `action_id` optionnel : le résultat d'un batch réussi est mémorisé 5 min ;
  un second appel avec le même id renvoie `cached:true` sans rejouer les étapes.
- Le `tab_id` retourné par `tabs_create`/`goto` est propagé aux étapes suivantes
  qui n'en fournissent pas.
- `POST /v2/act/cancel {"action_id":"batch-1"}` pose un drapeau d'annulation
  coopérative : le batch s'arrête avant l'étape suivante et renvoie
  `409 {"error":"cancelled","step":N,"results":[…]}`.
- Maximum 50 étapes ; le garde-fou SSRF est appliqué à chaque étape
  `goto`/`tabs_create`/`open`.
- Sur erreur d'une étape, la réponse d'erreur du démon est renvoyée (les
  résultats partiels ne sont pas inclus).

Commandes supportées (cf. `extension/background.js`) :

- `tabs_create {url, group_title?}` — ouvre un onglet ; avec `group_title`, navigue l'onglet existant du groupe au lieu d'en créer un (1 onglet/groupe, fenêtre réutilisée) et renvoie `window_id`/`group_id`.
- `goto {tab_id?, url}` — navigue un onglet (existant ou nouveau).
- `read {tab_id, max_bytes?, mode?}` — `mode:"text"` (défaut) renvoie `document.body.innerText` (200 Ko par défaut) ; `mode:"outline"` renvoie la liste des éléments interactifs visibles (`tag`, `role`, `text`, `selector`, `x/y/w/h`).
- `find {tab_id?, text?, selector?, limit?}` — **v0.6.1** : liste les éléments visibles dont le texte contient `text` (ou qui matchent `selector`), avec géométrie viewport. Chaînon entre `read` et `click_native_text`.
- `click {tab_id, selector}` — clique sur un élément CSS.
- `fill {tab_id, selector, value, direct?}` — remplit un input et déclenche `input`/`change`. `direct:true` (v0.6.1) utilise le setter natif en une injection (<0,3 s) ; par défaut, frappe humaine caractère par caractère.
- `wait {tab_id, ms}` — attend.
- `tab_group {tab_id, title, color?}` — place l'onglet dans un groupe.
- `tab_activate {tab_id}` — active (et focus) l'onglet.
- `screenshot {tab_id?, format?}` — capture le viewport (`captureVisibleTab`), dataURL PNG/JPEG.
- `captcha_detect {tab_id?}` — détection captcha + capture.
- `eval {tab_id?, code, arg?, timeout_ms?}` — **exécute du JS arbitraire dans la page (monde MAIN)**. `code` = expression (`"document.title"`) ou fonction sérialisée (`"(a) => a.x + 1"`) recevant `arg` (JSON). Promesses attendues, résultat assaini (structure-cloneable). Dernier recours quand les sélecteurs CSS n'atteignent pas les SPA (Angular, GA4, widgets Fluent) : clic par texte, lecture de composants, injection d'overlay visuel. Ex. clic par texte : `"() => { const el=[...document.querySelectorAll('a,button,[role=link]')].find(e=>e.textContent.trim().startsWith('Événements')); if(!el) return 'introuvable'; el.click(); return el.textContent.trim(); }"`
- `click_native {tab_id?, selector}` — **clic NATIF** via `chrome.debugger` + `Input.dispatchMouseEvent` (événements `isTrusted=true`) : trajectoire 3-8 points depuis le dernier curseur connu, hésitation pré-clic, press/release espacés. Le JS ne fait que LIRE la géométrie (doctrine new-pmu règle 9 : un clic JS est identifiable). Bandeau « débogage » visible pendant le geste — assumé.
- `type_native {tab_id?, selector?, text, submit?}` — **frappe native** caractère par caractère (`Input.dispatchKeyEvent`), dwell 40-160 ms/touche. `selector` optionnel : focus + sélection du contenu existant (préparation d'état, pas un geste). `submit: true` ajoute Entrée.
- `scroll_native {tab_id?, dy}` — **molette native** par crans de 90-135 px (30-70 ms d'écart). Jamais de scroll JS (signature : 0 événement wheel + saut instantané).
- `click_native_text {tab_id?, text, exact?}` — clic natif sur un élément repéré **par libellé** (préfixe par défaut, égalité stricte si `exact:true`). LE pont vers les SPA sans sélecteur stable (GA4/Angular) : le JS ne fait que chercher/lire, le natif clique.
- `click_native_rect {tab_id?, x, y}` — clic natif à des coordonnées viewport brutes : brique universelle (eval cherche par n'importe quelle logique, ce clic exécute).
- `screen_pos {tab_id?, selector}` — active l'onglet et renvoie la géométrie **écran** de l'élément (`ecranX/ecranY`) — à passer à `bin/os-clic.sh` qui déplace le **VRAI pointeur Windows** (SetCursorPos + mouse_event via interop WSL→PowerShell). Contrôle exclusif du pointeur pendant ~300 ms ; offsets de calibrage `OS_OFFSET_X/Y` si zoom/DPI atypique.
- `wait_for {tab_id?, selector|text, timeout_ms?}` — attente polluante bornée (250 ms, max 60 s) d'un sélecteur OU d'un texte : remplace les sleep arbitraires des scripts sur SPA lentes.

> Les trois commandes de clic natif (`click_native`, `click_native_text`,
> `click_native_rect`) acceptent `focus:true` : la fenêtre Chrome est amenée
> au premier plan. Nécessaire si elle est minimisée ou occultée, sinon CDP
> Input peut geler jusqu'au garde de 15 s.
>
> Les gestes natifs acceptent aussi `human_profile` : `calme` (jitter 40,
> typos 5 %), `normal` (24, 3 %), `presse` (12, 1,5 %). Le profil module la
> trajectoire souris et la frappe ; `normal` par défaut.

Note v1.0.0 (changements introduits en 0.6.1, désormais figés dans la version majeure) :
- Les commandes sont **sérialisées par onglet** (`tab_id`) : deux appels concurrents sur le même onglet ne se percutent plus sur `chrome.debugger.attach`.
- `eval` accepte `timeout_ms` (défaut 8000, borné 1000-20000 ms) ; le garde du geste suit à +5 s (max 24 s).
- `click_native`, `click_native_text` et `click_native_rect` renvoient `verified` (effet observé sur URL, scroll, DOM, focus ou marqueurs `aria-*`) et `verify_wait_ms`.

### Enregistrement d'écran (v1.0.0)

Trois routes protégées par le token :

- `POST /v2/record/start` `{out?, fps?, resolution?, duration?, monitor?, cursor?}` — lance `rec_ecran_cli.py` via le Python Windows du venv dédié et renvoie `{out, stop_file}`.
- `POST /v2/record/stop` — crée le fichier sentinelle, attend le CLI et renvoie son JSON (`frames`, `duration_s`, `fps`, `resolution`).
- `GET /v2/record/status` — `{recording, out?, elapsed_s?}`.

Le CLI vit dans `bin/rec_ecran_cli.py` (source) et `%USERPROFILE%\Documents\Mon Enregistreur\rec_ecran_cli.py` (déploiement Windows). Dépendances : `mss`, `opencv-python`, `imageio`, `imageio-ffmpeg`, `numpy` dans `venv-rec`.

### Capture d'écran (v1.0.0)

`bin/capture-ecran.sh` capture l'écran physique Windows avec le curseur dessiné :

- `--region x,y,w,h` — une zone précise (ex. `0,0,800,600`).
- `--fenetre [titre]` — une fenêtre par titre partiel (ex. `"Chrome"`) ; sans titre, la fenêtre active.
- `--out <chemin>` — fichier de sortie Windows (défaut `%USERPROFILE%\Downloads\ecran-dsh.png`).

Sortie : `OK LARGEURxHAUTEUR mode curseur@(x,y)` puis le chemin WSL. Le crop réduit la perte de détail due à la réduction d'image du modèle.

### Doctrine d'emploi des gestes natifs (v0.4)

Issue de new-pmu (règles 9-10 du 12/10/2026, mesurées en A/B live) :
`el.click()` et `dispatchEvent` produisent des événements `isTrusted=false`
— identifiables par les plateformes qui surveillent (constat eBay 20/10 :
anti-bot déclenché par des clics JS). Les commandes `_native` émettent de
vrais événements d'entrée. **Usage réservé à NOS consoles et NOS comptes**
(GA4, GSC, Bing, Chrome de l'opérateur) — jamais pour contourner des CGU ou sur
des comptes en contentieux (eBay suspendu = à la main).

## Build statique (musl)

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
file target/x86_64-unknown-linux-musl/release/dsh-eyes  # ELF … static-pie linked
```

Binaire produit : binaire unique `statically linked`, ~2,7 Mo, aucune dépendance dynamique.

## Installation

```bash
mkdir -p ~/.dsh-eyes/bin
cp target/x86_64-unknown-linux-musl/release/dsh-eyes ~/.dsh-eyes/bin/
```

Déploiement de l'extension vers le dossier chargé par Chrome :

```bash
scripts/deploy-extension.sh /mnt/c/Users/eddie/Downloads/dsh-eyes-extension-1.0.0
```

Puis `chrome://extensions` → DSH Eyes → **Recharger**, et saisir le token
dans **Options** (voir « Auth WebSocket »).

Le serveur MCP est configuré pour VS Code dans `.vscode/mcp.json` (stdio,
`node mcp-server/server.js`). La CI GitHub Actions (`.github/workflows/ci.yml`)
exécute tests Rust, clippy, build musl et vérifications Node ; verte aux runs 39673427865 et 39675973329.
La surveillance CLOSE-WAIT tourne toutes les 5 min via
`deploy/systemd/dsh-eyes-closewait.{service,timer}` et journalise dans
`~/.dsh-eyes/close-wait.log` (échec si CLOSE-WAIT > 0).

Lancement : `~/.dsh-eyes/bin/dsh-eyes` (écoute sur `127.0.0.1:8445`).

Données runtime écrites dans `~/.dsh-eyes/` :
- `events.jsonl` — journal des événements reçus via WS
- `alerts.jsonl` — alertes (localhost_access, private_ip, new_domain, etc.)
- `allowlist.json` — domaines autorisés (persistance des décisions)
- `token` — token Bearer pour /v2 (chmod 600, généré au 1er run)
- `config.toml` — options non sensibles

## Solveur de captcha — cascade à deux tiers

Le solveur (`POST /v2/page/solve`) interroge les endpoints Ollama dans
l'ordre `ollama_tier1_url` → `ollama_tier2_url`, puis rend la main
(`human_required`) si les deux échouent. Chaque tier peut exiger un jeton :

| Clé `config.toml` | Sens |
|---|---|
| `ollama_tier1_url` / `ollama_tier2_url` | base Ollama ; `/api/generate` est appelé |
| `ollama_tier1_token_file` / `ollama_tier2_token_file` | fichier contenant le jeton envoyé en `Authorization: Bearer` (chemin absolu, ou résolu sous `$DSH_EYES_HOME`). Vide ou fichier absent → tier appelé sans authentification |
| `vision_model` | modèle vision (défaut `qwen2.5-vl`) |

Le secret ne vit jamais dans `config.toml` : le fichier de jeton doit être
en `chmod 600`. Ce mécanisme rend le routeur IAQ exploitable — il répond
`401` à `/api/generate` sans jeton.

Preuve automatisée (réseau local requis, test ignoré par défaut) :

```bash
cargo test --test solver -- --ignored live_iaq_token_accepted
```

## Recharger l'extension Chrome après mise à jour

1. Va sur `chrome://extensions/`.
2. Mode développeur activé (en haut à droite).
3. Trouve « DSH Eyes » dans la liste.
4. Clique sur le bouton **⟳ Recharger** (icone circulaire) sous la carte.
5. La nouvelle version de `background.js` est prise en compte — la
   connexion WS se réouvre automatiquement, le `ExtensionChannel` la
   re-découvre au prochain message.

## Test de la WS sans navigateur (preuve automatisée)

`extension/test-ws-client.js` simule l'extension — il se connecte sur
`/ws`, répond aux commandes reçues, et vérifie l'aller-retour :

```bash
# Démarre d'abord le daemon
DSH_EYES_HOME=/tmp/dsh-eyes-test ./target/release/dsh-eyes &
DAEMON_PID=$!

# Mode "command-loop" : attend 3 commandes du démon et y répond
node extension/test-ws-client.js --count 3 &

# Dans un 3e terminal, déclenche les 3 commandes
TOKEN=$(cat /tmp/dsh-eyes-test/token)
for i in 1 2 3; do
  curl -X POST -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"cmd\":\"goto\",\"params\":{\"url\":\"https://example.com?n=$i\"}}" \
    http://127.0.0.1:8445/v2/cmd
  echo
done

kill $DAEMON_PID
```

## Vue d'ensemble (inventaire)

Voir `~/.dsh/docs/ecosysteme-local.md` (section « DSH Eyes »).

## E2E automatisé

```bash
scripts/e2e.py               # contrôles standards
scripts/e2e.py --multiframe  # + shadow DOM / iframe same-origin
```

Le script vérifie `/health`, `/v2/info`, le handshake, le SSRF, `tabs_create`,
`read`, `screenshot`, `eval`, `fill`, `type_native`, `act` (cache + annulation),
les métriques et `click_native_text`. Il sort en code 1 au premier échec.

## Limites connues (v1.1.0)

- **Tests `record`** : les 141 tests couvrent l'existant ; aucune suite dédiée aux routes `/v2/record/*` ni au CLI Python.
- **Audio** : `rec_ecran_cli.py` capture la vidéo seule ; le micro et l'audio système de l'application d'origine ne sont pas intégrés.
- **Sécurité du démon** : `/v2/cmd` réapplique `is_forbidden_host` depuis la v1.1.0 ; `/ws` exige le token ; pas encore de rate limit ; `eval` reste une exécution JS arbitraire ; le handshake `hello` est informatif (pas de refus automatique en cas de version incompatible).
- **Doc-test ignoré** : `src/vigil.rs:283` est un exemple ` ```ignore `, non exécuté — il ne compte pas comme test.

## Licence

MIT — voir [LICENSE](LICENSE).

