# Parité WebBridge → DSH Eyes — contrat des commandes (2026-11-06)

Objectif : combler les écarts fonctionnels identifiés par l'audit
(`Edy:~/harness-analytics/reports/2026-11-06-audit-webbridge-plan-dsh-eyes.html`)
sans ajouter d'outil. Le démon (`src/api.rs`) expose des routes `/v2/*` ; chaque
route envoie une commande à l'extension (`extension/background.js`) via le
canal WS existant (`{"cmd_id","cmd","params"}` / `{"cmd_id","ok","result","error"}`).

## Règles communes

- `tab_id` optionnel : l'extension prend l'onglet actif si absent (comportement
  actuel de `click`/`fill`).
- Erreurs : `{"ok":false,"error":"<message>"}`. Codes attendus côté démon :
  `sélecteur introuvable: …` → 404, `wait_timeout: …` → 504,
  `commande inconnue: <cmd>` → 502.
- Toute commande doit être déclarée dans le `hello` (capabilities) : le démon
  refuse par `command_not_supported` avant envoi si l'extension ne l'annonce pas.
- Les refs `@eN` sont propres à un snapshot ; un re-snapshot invalide les refs.

## 1. `snapshot` — arbre d'accessibilité + refs

```json
{"cmd":"snapshot","params":{"tab_id":42,"max_chars":60000,"ref":"@e7"}}
```

Retour :

```json
{"url":"https://…","title":"…","tree":[
  {"ref":"@e1","role":"navigation","name":"Menu principal","level":1},
  {"ref":"@e2","role":"button","name":"Rechercher"}
],"truncated":false,"next_cursor":null}
```

- Construit depuis le DOM : rôle explicite (`role=`) ou implicite (balise →
  rôle ARIA), nom accessible (`aria-label`, `alt`, texte visible, `title`),
  profondeur/`level` pour les titres et listes.
- Ne renvoie que les nœuds visibles et nommés ; `max_chars` borne le JSON
  sérialisé (même sémantique que `snapshot format:aria` de WebBridge).
- `ref` fourni → sous-arbre de ce nœud.
- Les refs sont résolues par `click`/`fill`/`send_keys`/`wait` (sélecteur `@eN`).

## 2. `read` étendu — iframes + continuation

```json
{"cmd":"read","params":{"tab_id":42,"max_bytes":200000,"mode":"text","frame":"#f1","start":0}}
```

Retour : `{"text":"…","complete":true,"next_start":null,"frame":"#f1"}`

- `frame` : identifiant `#fN` fourni par `snapshot.frames` (liste des iframes
  accessibles), ou sélecteur CSS d'iframe ; `frame` est réémis dans la réponse.
- `start` : offset de continuation ; `next_start` non nul si `complete:false`.
- Sans `frame` : document principal (comportement actuel inchangé).

## 3. Cycle de vie des onglets

```json
{"cmd":"tabs_list","params":{}}
{"cmd":"tabs_list","params":{"all_windows":true}}
{"cmd":"tab_attach","params":{"tab_id":42}}
{"cmd":"tab_close","params":{"tab_id":42}}
{"cmd":"session_close","params":{"session":"ma-tache"}}
```

- `tabs_list` → `{"tabs":[{"tab_id","window_id","url","title","active","group_title"?,"borrowed"?}]}`
- `tab_attach` → `{"tab_id","url","title","borrowed":false}` (l'onglet devient
  l'onglet courant de la tâche).
- `tab_close` → `{"closed":1}` ; `session_close` → `{"closed":N}` en ne fermant
  que les onglets ouverts par la session (jamais les onglets « empruntés » par
  `tab_attach` d'un onglet préexistant).

## 4. `send_keys` — clavier natif

```json
{"cmd":"send_keys","params":{"tab_id":42,"keys":"Mod+A","focus_emulation":true}}
```

- `keys` : combinaison (`Enter`, `Escape`, `Mod+A`, `Shift+Tab`), `Mod` = Ctrl
  hors macOS.
- Retour : `{"dispatched":true,"os":"linux","verified":true}`.
- `focus_emulation` : active `Emulation.setFocusEmulationEnabled` via debugger
  pour un onglet en arrière-plan, puis la désactive.

## 5. `screenshot` étendu

```json
{"cmd":"screenshot","params":{"tab_id":42,"selector":"#main","full_page":true,"format":"png","path":"/tmp/capture.png"}}
```

- `selector` : capture de l'élément ; `full_page` : `captureBeyondViewport`
  (hauteur du document, plafonnée à 20 000 px).
- `path` : si fourni, l'extension n'écrit pas de fichier (elle n'a pas accès au
  disque) — le démon décode le base64 et écrit le fichier, puis renvoie
  `{path,size_bytes}`.

## 6. `network` — capture par onglet

```json
{"cmd":"network","params":{"op":"start","tab_id":42}}
{"cmd":"network","params":{"op":"list","tab_id":42,"filter":"api/"}}
{"cmd":"network","params":{"op":"detail","tab_id":42,"request_id":"1234.56"}}
{"cmd":"network","params":{"op":"stop","tab_id":42}}
```

- `start`/`stop` : active/désactive la capture pour l'onglet (buffer 200 entrées).
- `list` → `{"count":N,"requests":[{"request_id","url","method","status","mime_type","completed"}]}`
- `detail` → entrée + `request_headers`, `request_body?`, `body?` (borné à 256 Ko),
  `body_error?` si le corps n'est plus disponible.
- Les corps de réponse ne sont conservés que tant que la capture est active.

## 7. `pdf` — export PDF

```json
{"cmd":"pdf","params":{"tab_id":42,"paper_format":"a4","landscape":false,"scale":1,"print_background":true}}
```

- `Page.printToPDF` via debugger ; retour `{b64, page_title}`.
- Le démon écrit le fichier si `path` est fourni (même mécanisme que screenshot).

## 8. `upload` — fichier dans un input

```json
{"cmd":"upload","params":{"tab_id":42,"selector":"input[type=file]","files":["/home/eddie/x.pdf"]}}
```

- `DOM.setFileInputFiles` via debugger. Un chemin non lisible → erreur explicite.
- Retour : `{"file_count":1,"names":["x.pdf"]}`.

## 9. `cdp` — passe-plat contrôlé

```json
{"cmd":"cdp","params":{"tab_id":42,"method":"Page.getLayoutMetrics","params":{}}}
```

- Liste blanche de méthodes : `Page.*` (sauf `navigate`), `Network.*`, `DOM.*`,
  `Runtime.evaluate`, `Emulation.*`, `Accessibility.*`. Toute autre méthode →
  `cdp_method_not_allowed`.

## Routes démon correspondantes

| Commande | Route |
|---|---|
| `snapshot` | `POST /v2/snapshot` |
| `read` (frame/start) | `POST /v2/read` (champs ajoutés) |
| `tabs_list`/`tab_attach`/`tab_close`/`session_close` | `POST /v2/tabs` |
| `send_keys` | `POST /v2/send_keys` |
| `screenshot` étendu | `POST /v2/page/screenshot` (champs ajoutés) |
| `network` | `POST /v2/network` |
| `pdf` | `POST /v2/pdf` |
| `upload` | `POST /v2/upload` |
| `cdp` | `POST /v2/cdp` |

## Critères d'acceptation

1. `cargo test` vert (démon) et `pnpm`/aucun test JS propre : le contrat est
   couvert par `tests/parity_routes.rs` (routes, validation de capability,
   erreurs).
2. Smoke manuel après rechargement de l'extension : snapshot sur une page
   réelle, clic par ref, lecture d'une iframe, cycle onglet/session, capture
   plein écran, PDF, capture réseau, upload, cdp.
3. Aucun `commande inconnue` : chaque commande envoyée est annoncée dans
   `hello.capabilities`.

## 10. `captcha_apply` — appliquer une solution (P3 différenciation)

Le solveur existant détecte et propose une réponse ; cette commande **applique**
la solution fournie par l'appelant (agent ou modèle vision) sans dépendre d'un
service tiers.

```json
{"cmd":"captcha_apply","params":{
  "tab_id": 42,
  "kind": "grid" | "slider" | "text",
  "selector": "#captcha-zone",
  "cells": [0,4,8],
  "rows": 3, "cols": 3,
  "delta_x": 137,
  "input_selector": "#captcha-input",
  "text": "AB12C"
}}
```

- `grid` : `selector` désigne le conteneur (élément iframe ou div du captcha).
  `cells` = indices row-major `0..rows*cols-1`. L'extension calcule le rectangle
  du conteneur, le découpe en `rows`×`cols` et exécute un **clic natif**
  (`Input.dispatchMouseEvent`, `isTrusted=true`) au centre de chaque case.
  Réponse : `{applied:true, kind:"grid", cells, rect, clicked:n}`.
- `slider` : `selector` désigne la poignée à glisser ; `delta_x` = distance
  horizontale en pixels CSS. Glissement natif : `mousePressed`, mouvements
  intermédiaires (15-30 pas, léger jitter), `mouseReleased`, avec vérification
  du déplacement observé. Réponse : `{applied:true, kind:"slider", delta_x, verified}`.
- `text` : `input_selector` + `text` → saisie native (`type_native`). Réponse :
  `{applied:true, kind:"text", verified}`.
- Erreurs : `missing_selector`, `sélecteur introuvable: …`, `invalid_cells`,
  `missing_delta_x`, `kind_inconnu: …`.

Route démon : `POST /v2/page/captcha/apply` (mêmes champs, préfixe `captcha_apply`).
