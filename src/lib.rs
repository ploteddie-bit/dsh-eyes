//! Bibliothèque `dsh-eyes`.
//!
//! Le binaire `main.rs` réutilise `api::router()`. Les tests d'intégration
//! (cf. `tests/ingest.rs`) importent `dsh_eyes::api::router()`.

pub mod api;
pub mod ingest;