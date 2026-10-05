# DSH Eyes v2 « Hands + Solveur » Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Étendre DSH Eyes d'observateur à agent : n'importe quelle inférence peut ouvrir une page web, lire son contenu, cliquer, remplir des formulaires, naviguer — via une API HTTP locale authentifiée — avec résolution locale des captchas (ia-general → serveur-ia → humain).

**Architecture:** Le démon Rust existant gagne un module `hands` : un client **Chrome DevTools Protocol** (`chromiumoxide`) vers une instance Chrome dédiée (profil jetable `~/.dsh-eyes/chrome-profile`, lancée et supervisée par le démon lui-même). Chaque action (open/text/click/fill/goto/screenshot) passe par l'API HTTP v2, protégée par un token, journalisée dans `actions.jsonl`, et soumise aux garde-fous (blocage localhost/réseaux privés pour la navigation sortante). Les captchas détectés dans le DOM sont résolus par un solveur hiérarchisé : Ollama **ia-general** (RX 9060 XT, `qwen2.5-vl`, ~5-10 s) → Ollama **serveur-ia** (iGPU Barcelo, ~15-40 s) → statut `human_required`.

**Tech Stack:** Rust (tokio, axum, serde), `chromiumoxide` (CDP), Ollama API (ia-general 10.0.0.223:11434, serveur-ia 10.0.0.245:11434), cwebp/base64 pour les images.

## Global Constraints

- Standard `dev-rust` : zéro unwrap/expect async, clippy -D warnings, docs.rs avant hypothèse d'API, Cargo.lock versionné.
- L'API v2 exige un **token** (`~/.dsh-eyes/token`, généré au premier run, chmod 600) — header `Authorization: Bearer <token>`. `/health` reste ouvert. Les endpoints v1 (/ws, /stats, /events) ne changent pas.
- **Navigation sortante uniquement** : `/open` et `/goto` refusent localhost, 127.0.0.0/8, ::1, 10/8, 192.168/16, 172.16/12 (réutilisation de la logique IP de `vigil.rs`, mutualisée). La page ouverte peut *afficher* du contenu, mais le démon ne pilote pas de navigation *vers* le réseau interne.
- **Chaque action est journalisée** dans `~/.dsh-eyes/actions.jsonl` : `{ts, action, url, page_id, result}`.
- Captchas : **détection + résolution image/locale uniquement**. Pas de service tiers (2captcha etc.). Échec après N tentatives → `{"status":"human_required"}`. reCAPTCHA v3 (score invisible) = hors capacité, retourné `unsupported`.
- Instance Chrome dédiée : user-data-dir `~/.dsh-eyes/chrome-profile`, JAMAIS le profil d'Eddie. Le démon la lance avec `--remote-debugging-port=<port libre>` et `--headless=new` (option `--headless=false` configurable).
- Résolution de captcha = lecture d'image → **l'inférence vision tourne sur les serveurs Ollama d'Eddie** (ia-general puis serveur-ia). Zéro service tiers, zéro clé.
- Cibles : ia-general `http://10.0.0.223:11434`, serveur-ia `http://10.0.0.245:11434` (MESURÉ actifs ; serveur-ia : 13 Go RAM dispo, load 0,00).
- Repo : `Edy:~/projets/dsh-eyes/`, branche main (base v1 = `5daa49a`).

---

### Task 1: Config + token + middleware d'auth v2

**Files:** Create `src/config.rs`, `src/auth.rs` ; Modify `src/main.rs`, `src/api.rs`

**Interfaces:**
- Produces: `config::Config::load()` lit `~/.dsh-eyes/config.toml` (ou défauts) : `api_token` (généré `openssl rand -hex 32` au premier run dans `~/.dsh-eyes/token`, chmod 600), `ollama_tier1_url` (défaut `http://10.0.0.223:11434`), `ollama_tier2_url` (défaut `http://10.0.0.245:11434`), `chrome_headless` (défaut true).
- Produces: `auth::require_token` (extracteur/middleware axum) : routes v2 montées derrière ; 401 sans/mauvais token. `/health`, `/ws`, `/stats`, `/events` inchangés (ouverts).

- [ ] **Step 1:** Écrire le test d'abord : route de test `GET /v2/ping` protégée → 401 sans token, 200 avec `Authorization: Bearer <token du fichier>` (test lit le fichier temporaire `DSH_EYES_HOME` de test).
- [ ] **Step 2:** Implémenter `config.rs` (génération + lecture token, toml via crate `toml`) et `auth.rs` (comparaison constante `subtle`-like via double parcours manuel, pas de panic).
- [ ] **Step 3:** `cargo test` vert + clippy vert + test manuel curl 401/200.
- [ ] **Step 4:** Commit `feat(v2): config + token + auth middleware`.

---

### Task 2: Cycle de vie du navigateur dédié (spawn + CDP)

**Files:** Create `src/hands/browser.rs` ; Modify `src/main.rs`, `src/api.rs`

**Interfaces:**
- Produces: `browser::BrowserPool` — au démarrage (lazy) : spawn Chrome (`<chrome path> --user-data-dir=~/.dsh-eyes/chrome-profile --remote-debugging-port=0 --headless=new`), parse le port réel depuis stderr (`DevTools listening on ws://…`), connexion `chromiumoxide::Browser::connect`. `GET /v2/browser/status` (auth) → `{"running":true,"pages":N}`. Kill propre au drop + gestion crash (relance au prochain appel).
- Chemin Chrome : détecté via `which google-chrome || which chromium-browser || which chromium` au runtime, sinon config `chrome_path` dans config.toml.

- [ ] **Step 1:** Vérifier la doc `chromiumoxide` sur docs.rs (version courante, `Browser::launch` vs `connect`, `Page` API) — avant tout code (règle dev-rust).
- [ ] **Step 2:** Test : `BrowserPool::ensure_running()` → status true → kill du process Chrome simulé → ensure_running relance. (test unitaire avec feature `test-util`.)
- [ ] **Step 3:** Implémentation + `cargo test` + clippy verts.
- [ ] **Step 4:** Test manuel : `/v2/browser/status` réel. Commit `feat(v2): pool navigateur dédié via CDP`.

---

### Task 3: `POST /v2/open` + `GET /v2/page/{id}/text` — les 2 fondamentaux

**Files:** Create `src/hands/actions.rs`, `tests/hands_open.rs`

**Interfaces:**
- `POST /v2/open {"url":"https://…"}` → crée un onglet CDP, attends `load`, retourne `{"page_id":"p1"}` (ids séquentiels en mémoire). Garde-fou : URL interne/privée → 403 `{"error":"forbidden_target"}` (réutilise la fonction IP mutualisée de vigil.rs, extraite en `pub fn is_forbidden_host(host) -> bool` dans `vigil.rs`).
- `GET /v2/page/{id}/text` → `{"url":"…","title":"…","text":"<innerText du body, tronqué à 200 Ko>"}` (CDP `Runtime.evaluate`).
- Erreurs CDP → 502 `{"error":"cdp: …"}`. Tout loggé dans actions.jsonl.

- [ ] **Step 1:** Test d'intégration : ouvrir une page `data:text/html,<h1>hello dsh</h1>` (pas de réseau externe en test) → `/text` contient « hello dsh ».
- [ ] **Step 2:** Implémentation. `cargo test` + clippy verts.
- [ ] **Step 3:** Test manuel réel : `/v2/open https://example.com` → `/text` contient « Example Domain » (preuve curl dans le rapport).
- [ ] **Step 4:** Commit `feat(v2): open + read text via CDP`.

---

### Task 4: Actions — `goto`, `click`, `fill`, attente d'élément

**Files:** Modify `src/hands/actions.rs`, `tests/hands_actions.rs`

**Interfaces:**
- `POST /v2/page/{id}/goto {"url"}` (garde-fou identique), `POST /v2/page/{id}/click {"selector"}` , `POST /v2/page/{id}/fill {"selector","value"}`, `POST /v2/page/{id}/wait {"selector","timeout_ms"≤10000}`. Réponses : `{"ok":true}` ou erreur CDP 502 / sélecteur introuvable 404.

- [ ] **Step 1:** Tests d'intégration sur page `data:` avec bouton/chargés inline (JS inline modifie le DOM au clic — assert sur le texte après clic).
- [ ] **Step 2:** Implémentation via CDP (`Page.evaluate` pour click/fill — injection JS standard `document.querySelector(sel).click()` / `.value = …` + event input, plus robuste que les domaines CDP dédiés pour la v2).
- [ ] **Step 3:** cargo test + clippy verts ; test manuel réel sur example.com (goto + click sur le lien « More information » si présent, sinon sur une page data:).
- [ ] **Step 4:** Commit `feat(v2): actions navigateur goto/click/fill/wait`.

---

### Task 5: Garde-fous actionnels + journal `actions.jsonl`

**Files:** Modify `src/hands/*`, `src/vigil.rs`

- [ ] **Step 1:** Test : `/open` vers `http://127.0.0.1:8445/health` → 403 ; vers `http://10.0.0.245:11434` → 403 ; chaque action réussie ajoute une ligne dans `actions.jsonl`.
- [ ] **Step 2:** Implémentation : `vigil::is_forbidden_host` mutualisée et réutilisée ; journalisation via le même pattern JSONL que events.
- [ ] **Step 3:** cargo test + clippy verts. Commit `feat(v2): garde-fous navigation + journal actions`.

---

### Task 6: Screenshot + détection de captcha

**Files:** Create `src/hands/captcha.rs`, `tests/captcha_detect.rs`

**Interfaces:**
- `GET /v2/page/{id}/screenshot` → `{"format":"png","b64":"…"}` (CDP `Page.captureScreenshot`).
- `GET /v2/page/{id}/captcha` → `{"detected":true,"type":"recaptcha_v2_image|hcaptcha|text|slider|recaptcha_v3_invisible|none","element_selector":"…","image_b64":"…"}` — détection par requêtes DOM standard : `iframe[src*="recaptcha"]`, `iframe[src*="hcaptcha"]`, `.g-recaptcha`, `.h-captcha`, `img[src*="captcha"]`, `input[name*="captcha"]`, sliders (`class*=slider`+canvas).
- [ ] **Step 1:** Tests sur pages data: contenant un faux marqueur captcha (iframe factice avec src contenant recaptcha).
- [ ] **Step 2:** Implémentation. cargo test + clippy verts.
- [ ] **Step 3:** Test manuel réel sur une page de démo captcha (ex. démo publique reCAPTCHA de Google — lecture seule, aucune résolution à cette étape). Commit `feat(v2): screenshot + détection captcha`.

---

### Task 7: Solveur hiérarchisé ia-general → serveur-ia → humain

**Files:** Create `src/solver.rs`, `tests/solver.rs`

**Interfaces:**
- `POST /v2/page/{id}/solve` (auth) → lance détection + résolution :
  1. capture l'image du captcha (`screenshot` de l'élément ou l'img cible)
  2. **Tier 1** : POST `ia-general:11434/api/generate` modèle vision (config `vision_model_tier1`, à puller une fois : `qwen2.5-vl` ~6 Go) avec l'image base64 + prompt d'extraction
  3. **Tier 2** : serveur-ia identique
  4. Échec/N-essais → `{"status":"human_required","captcha_type":"…"}`
- Réponse succès : `{"status":"solved","answer":"<texte/coord>","applied":true|false}` (application CDP pour les captchas texte/input ; pour les grilles hCaptcha : coords des tuiles → clics).
- Limite : reCAPTCHA v3 invisible → `{"status":"unsupported"}` direct.
- Prompt vision (template dans solver.rs, ajustable) : « This image contains a CAPTCHA. Answer with ONLY the solution: if text, the characters; if an image grid, describe which tiles to click as JSON coordinates. »

- [ ] **Step 1:** (prérequis ops, hors code) `ollama pull qwen2.5-vl` sur ia-general ET serveur-ia — via SSH, vérifier `api/tags`.
- [ ] **Step 2:** Test unitaire du parseur de réponse + du fallback de tiers (serveur Ollama mocké par un petit axum embarqué dans le test — pas de réseau réel en test).
- [ ] **Step 3:** Implémentation + tests verts.
- [ ] **Step 4:** Test manuel réel : démo captcha texte (site de démo ou page data: avec captcha texte auto-généré) → solved via tier 1. Preuve dans le rapport.
- [ ] **Step 5:** Commit `feat(v2): solveur captcha hiérarchisé ia-general → serveur-ia → humain`.

---

### Task 8: Skill `dsh-eyes-agent` + doc API pour toutes les inférences

**Files:** Create `Edy:~/.dsh/skills/dsh-eyes-agent/SKILL.md`, Modify `Edy:~/.dsh/docs/ecosysteme-local.md`

- [ ] **Step 1:** SKILL.md : objectif, prérequis (démon lancé, token), les 8 endpoints v2 avec exemples curl complets, les garde-fous, la sémantique captcha (human_required = ne pas insister), la règle « toute action est journalisée ».
- [ ] **Step 2:** Inventaire : §5 DSH Eyes enrichie (API v2, token, instance Chrome dédiée).
- [ ] **Step 3:** Commit + vérification qu'une session fictive peut suivre le skill (relecture à froid par Eddie ou un subagent).

---

### Task 9: E2E scénario autonome + revue finale

**Files:** Create `docs/test-e2e-v2-2026-MM-DD.md`

- [ ] **Step 1:** Scénario complet authentifié : démon + navigateur → `/open` une page de démo de formulaire publique (site de démo type httpbin forms ou page data: riche) → `/text` (lire) → `/fill` + `/click` (soumettre) → `/captcha` (détection none) → screenshot. Preuves curl complètes dans le rapport.
- [ ] **Step 2:** Scénario captcha : page de démo reCAPTCHA → `/captcha` détecte → `/solve` → verdict (solved OU human_required) documenté honnêtement.
- [ ] **Step 3:** Eddie valide le comportement (inspection des captures + logs).
- [ ] **Step 4:** Revue finale de branche (subagent, grille sécurité : token obligatoire, garde-fous, pas de fuite de contenu, zéro unwrap async) + commit docs.
- [ ] **Step 5:** Inventaire + cockpit mis à jour en « déployé v2 ».

---

## Checklist finale v2

- [ ] `/v2/open` + `/v2/page/{id}/text` : ouvrir et lire (fondamental 1) — preuve curl réelle
- [ ] `/goto`, `/click`, `/fill`, `/wait` : naviguer (fondamental 2) — preuve curl réelle
- [ ] Captcha : détection des 5 types, solveur local 2 tiers, human_required en dernier ressort
- [ ] Token obligatoire sur toute la v2 (401 sans token — preuve curl)
- [ ] actions.jsonl trace chaque action (preuve cat)
- [ ] Skill `dsh-eyes-agent` installé — toute inférence peut suivre la doc
- [ ] Revue finale verte + inventaire/cockpit « déployé v2 »
