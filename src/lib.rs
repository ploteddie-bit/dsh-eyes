//! Bibliothèque `dsh-eyes`.
//!
//! Le binaire `main.rs` réutilise `api::router(VigilCtx)`. Les tests
//! d'intégration (cf. `tests/`) importent `dsh_eyes::api::router` et
//! `dsh_eyes::state::VigilCtx::for_tests`.
//!
//! Modules v2 (Task 1) :
//! - `config` : chargement/génération de la config + token.
//! - `auth` : middleware Bearer + comparaison constante.

pub mod api;
pub mod auth;
pub mod config;
pub mod hands;
pub mod ingest;
pub mod state;
pub mod stats;
pub mod vigil;
