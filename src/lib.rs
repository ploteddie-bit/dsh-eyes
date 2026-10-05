//! Bibliothèque `dsh-eyes`.
//!
//! Le binaire `main.rs` réutilise `api::router(VigilCtx)`. Les tests
//! d'intégration (cf. `tests/`) importent `dsh_eyes::api::router` et
//! `dsh_eyes::state::VigilCtx::for_tests`.
//!
//! Modules v2 (Task 1) :
//! - `config` : chargement/génération de la config + token.
//! - `auth` : middleware Bearer + comparaison constante.
//!
//! Module v2 (Task 7) :
//! - `solver` : solveur de captcha hiérarchisé (Ollama tier 1 → tier 2 →
//!   humain) + détection partagée entre `/v2/page/captcha` et
//!   `/v2/page/{id}/solve`.

pub mod api;
pub mod auth;
pub mod config;
pub mod hands;
pub mod ingest;
pub mod solver;
pub mod state;
pub mod stats;
pub mod vigil;
