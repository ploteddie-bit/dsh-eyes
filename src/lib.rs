//! Bibliothèque `dsh-eyes`.
//!
//! Le binaire `main.rs` réutilise `api::router(VigilCtx)`. Les tests
//! d'intégration (cf. `tests/`) importent `dsh_eyes::api::router` et
//! `dsh_eyes::state::VigilCtx::for_tests`.

pub mod api;
pub mod ingest;
pub mod state;
pub mod stats;
pub mod vigil;
