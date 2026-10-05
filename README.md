# dsh-eyes

Démon d'observation des navigations Chrome (MVP).

- Endpoint HTTP+WS : `http://127.0.0.1:8445` (health, stats, events, alerts, ws)
- Routes principales : `GET /health`, `GET /stats`, `GET /events`, `GET /alerts`, `WS /ws`
- Source : `src/` (Rust, axum 0.7 + tokio + tokio-tungstenite)
- Tests : `tests/` (12+ tests, `cargo test`)
- Extension Chrome : `extension/` (MV3, à charger via `chrome://extensions` mode développeur)
- Cockpit : `docs/cockpit.html`

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
cp -r extension ~/.dsh-eyes/extension
```

Lancement : `~/.dsh-eyes/bin/dsh-eyes` (écoute sur `127.0.0.1:8445`).

Données runtime écrites dans `~/.dsh-eyes/` :
- `events.jsonl` — journal des événements reçus via WS
- `alerts.jsonl` — alertes (localhost_access, private_ip, new_domain, etc.)
- `allowlist.json` — domaines autorisés (persistance des décisions)

## Vue d'ensemble (inventaire)

Voir `Edy:~/.dsh/docs/ecosysteme-local.md` (section « DSH Eyes »).
