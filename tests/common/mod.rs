//! Garde de sérialisation : les tests qui manipulent DSH_EYES_HOME
//! partagent l'env du process — ils ne doivent jamais tourner en parallèle.
use std::sync::{Mutex, OnceLock};

pub fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}
