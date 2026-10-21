# DSH Eyes v2.2 — Résolution chirurgicale des 10 dettes documentées

> **For agentic workers:** REQUIRED SUB-SKILL: subagent-driven-development. Steps use checkbox syntax.

**Goal:** Traiter les 10 dettes documentées du registre SDD en 5 lots à périmètres de fichiers disjoints (leçon des collisions du 05/10), 3 vagues d'exécution, sans régression des 119+ tests verts.

**Architecture:** Chaque lot touche des fichiers disjoints. `src/api.rs` (fichier partagé historique) est concentré dans le Lot E, exécuté en dernier. Vague 1 = 3 implémenteurs parallèles (A, B, C), Vague 2 = 2 implémenteurs (D, E), Vague 3 = revue finale + push.

**Tech Stack:** Rust (tokio, axum), extension MV3, crates : `url` (nouvelle, Lot E).

## Global Constraints

- Standard dev-rust : zéro unwrap/expect async, clippy -D warnings, docs.rs avant hypothèse.
- **Zéro collision** : chaque lot touche UNIQUEMENT ses fichiers listés. `src/api.rs` interdit aux Lots A-D.
- 119+ tests existants doivent rester verts après chaque lot (cargo test --all-targets + clippy).
- Un commit par lot, message conventionnel.
- Rapport au fil de l'eau : `task-<lot>-report.md` dans le workspace SDD.

---

### Lot A — D1 : bufferisation de `append_event` (src/ingest.rs)

**Files:** Modify `src/ingest.rs` uniquement. **Tests:** `tests/ingest.rs` (extend).

- [ ] Remplacer l'ouverture/fermeture par événement par un `EventWriter` : canal mpsc + tâche écrivain propriétaire d'un `BufWriter`, flush toutes les 10 lignes ou 500 ms d'idle (`recv_timeout`).
- [ ] `handle_socket` utilise `writer.append(evt)` (non-bloquant, fire-and-forget conservé).
- [ ] Test : 100 événements rapides → les 100 lignes présentes dans le fichier après flush (vert d'abord sans le buffer, puis vert avec).
- [ ] Clippy vert. Commit `perf(ingest): bufferise append_event (EventWriter + flush idle)`.

### Lot B — D2 : injection de la Config solveur (src/config.rs, src/solver.rs)

**Files:** Modify `src/config.rs`, `src/solver.rs`. **Tests:** `tests/solver.rs` (adapt).

- [ ] `Config` : ajouter constructeur `solver_config()` retournant `{tier1, tier2, model}` (cloné, léger).
- [ ] `api.rs` est INTERDIT — le `solve_handler` reçoit la config via un nouveau paramètre propagé depuis `router_with_v2` (signature : `router_with_v2(ctx, config, channel)` — `config` déjà présent, l'extraire et le passer à `solve_handler`).
- [ ] Supprimer le `spawn_blocking(Config::load)` du handler (la config vient du state).
- [ ] Tests solver.rs adaptés (URLs mockées passées via le constructeur, plus de config.toml écrit).
- [ ] cargo test + clippy verts. Commit `refactor(v2): injecte la config solveur (supprime Config::load par requête)`.

### Lot C — D4 : hot-reload de l'allowlist (src/vigil.rs)

**Files:** Modify `src/vigil.rs`. **Tests:** `tests/vigil.rs` (extend).

- [ ] `Allowlist` : stocker le mtime du fichier au chargement ; méthode `reload_if_changed()` comparant le mtime courant.
- [ ] L'alerme keep-alive (25 s, Task 2 du démon — via un `tokio::spawn` interval dans `main.rs` ou le tick existant) appelle `reload_if_changed()` : édition manuelle d'`allowlist.json` prise en compte en ≤ 25 s sans restart.
- [ ] Test : modifier le fichier (écrire une nouvelle entrée) → reload_if_changed → la nouvelle entrée est active.
- [ ] cargo test + clippy verts. Commit `feat(vigil): hot-reload allowlist par mtime`.

### Lot D — D6 + D7 : captcha appliqué + crop (extension/background.js)

**Files:** Modify `extension/background.js` uniquement. **Tests:** manuels (par l'opérateur) + syntaxe node.

- [ ] **D7 crop** : `cmdCaptchaDetect` — au lieu de la capture plein écran, injecter un script qui croppe l'élément captcha via `captureVisibleTab` + canvas off-screen (drawImage du rect getBoundingClientRect) → `image_b64` = crop de l'élément.
- [ ] **D6 grille** : nouvelle commande `captcha_apply_grid` `{tab_id, tile_indexes:[1,3]}` : cliquer les tuiles (sélecteurs calculés depuis la grille), puis le bouton verify — pour hCaptcha/reCAPTCHA image.
- [ ] `solve_handler` (côté démon — attention : modification MINIMALE autorisée dans `src/solver.rs` ligne de dispatch uniquement) : pour grille/slider, envoyer `captcha_apply_grid` au lieu de retourner `applied:false`.
- [ ] `node --check` + tests existants verts. Commit `feat(v2): captcha grille appliqué + crop élément`.

### Lot E — D3 + D8 + D9 + D10 + D5 (src/api.rs, src/main.rs)

**Files:** Modify `src/api.rs`, `src/main.rs`. **Tests:** `tests/endpoints.rs`, `tests/stats.rs` (adapt).

- [ ] **D3** : dépendance `url = "2"` ; `extract_host` réécrit avec `Url::parse` (origin + host) — supprime le split maison. URL exotiques couvertes.
- [ ] **D8** : `/v2/wait` `timeout_ms` > 10 000 → 400 `timeout_trop_grand` (au lieu du plafond silencieux). Test adapté.
- [ ] **D9** : `/v2/fill` sans `value` → 400 `missing_value` (au lieu du défaut ""). Test adapté. (Clear de champ = `value:""` explicite.)
- [ ] **D10** : `/v2/cmd` — valider `cmd` ∈ {tabs_create,goto,read,click,fill,wait,screenshot,captcha_detect,tab_group} (400 `unknown_cmd` sinon).
- [ ] **D5** : `/stats` persiste un snapshot `stats.json` à chaque requête (write atomique tmp+rename) ; au démarrage, recharge si présent → les compteurs survivent au restart. Test : stats → restart simulé (recréer le router) → stats restaurées.
- [ ] cargo test --all-targets + clippy verts. Commit `feat(v2): validations explicites + url crate + persistance stats`.

---

### Vague finale — revue de branche + push

- [ ] Revue complète des 5 commits (sécurité : pas de régression des garde-fous).
- [ ] cargo test --all-targets + clippy verts sur la branche entière.
- [ ] Push Forgejo (hash vérifié).
- [ ] Inventaire §5-bis + cockpit mis à jour (dettes soldées).
