//! Module `hands` — interaction avec le navigateur (v2).
//!
//! ## Architecture REWORK (2026-10-05)
//!
//! L'extension DSH Eyes, déjà chargée dans le Chrome de l'utilisateur
//! (manifest MV3, service worker `background.js`), est devenue
//! **bidirectionnelle** : elle pousse ses événements de navigation via
//! `ws://127.0.0.1:8445/ws` (cf. `crate::ingest`) **et** reçoit des
//! commandes du démon (goto, click, read, fill, tabs_create, …) via le
//! même canal. L'extension les exécute avec `chrome.tabs` et
//! `chrome.scripting.executeScript`, puis renvoie le résultat.
//!
//! - `channel` : `ExtensionChannel` — registre du sink d'écriture WS de
//!   l'extension, file de requêtes en attente mappées par `cmd_id`,
//!   helpers `send_command` / `dispatch_response`. Mutex courts, `Arc`
//!   partagés entre `ingest::handle_socket` et `api::v2_cmd`.
//!
//! Le préfixe `hands` évoque la métaphore « mains » (les outils que le
//! daemon manipule) — par opposition à la « vue » (`api`) et au « jugement »
//! (`vigil`).

pub mod channel;
