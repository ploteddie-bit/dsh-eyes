//! État partagé du daemon `dsh-eyes` (Task 4).
//!
//! Agrège les trois dépendances qui circulent entre les sous-routeurs :
//! - `Counters` : compteurs par domaine (Task 3, inchangé).
//! - `Allowlist` : domaines connus (Task 4, chargé lazy).
//! - `SharedSession` : ensemble des domaines déjà vus en session (Task 4).
//!
//! Pourquoi un agrégat et pas 3 `State<...>` séparés ? axum 0.7 n'autorise
//! qu'un seul `.with_state(...)` par `Router`. Plutôt que d'imbriquer
//! manuellement des extractors, on agrège et on déballe à la main avec
//! `Extension` (cf. `api.rs`).
//!
//! Compatibilité : `tests/stats.rs` construit `Counters::new()` et le passe
//! à `api::router` — on garde `Counters` accessible via `VigilCtx::counters()`
//! pour que `stats_router` puisse continuer à prendre son `Counters` direct
//! si besoin (mais ici on le passe via `VigilCtx` aussi pour uniformité).

use std::sync::Arc;
use tokio::sync::Mutex;

use crate::ingest::AlertBuffer;
use crate::stats::Counters;
use crate::vigil::{Allowlist, SessionState};

/// Vue partagée par le routeur racine (`/health`, `/ws`, `/stats`, `/events`).
#[derive(Clone)]
pub struct VigilCtx {
    pub counters: Counters,
    pub allowlist: Allowlist,
    pub session: Arc<Mutex<SessionState>>,
    pub alerts: AlertBuffer,
}

impl VigilCtx {
    /// Construit un contexte « production » : allowlist chargée depuis
    /// `~/.dsh-eyes/allowlist.json` (lazy, dégrade gracieusement vers vide).
    pub fn new() -> Self {
        Self {
            counters: Counters::new(),
            allowlist: Allowlist::load_or_default(),
            session: Arc::new(Mutex::new(SessionState::new())),
            alerts: AlertBuffer::new(),
        }
    }

    /// Construit un contexte « test » : allowlist injectée directement,
    /// session vide. `Counters` partagé (clone = même Arc intérieur).
    #[allow(dead_code)] // utilisé uniquement par les tests d'intégration
    pub fn for_tests(counters: Counters, allowlist: Allowlist) -> Self {
        Self {
            counters,
            allowlist,
            session: Arc::new(Mutex::new(SessionState::new())),
            alerts: AlertBuffer::new(),
        }
    }
}

impl Default for VigilCtx {
    fn default() -> Self {
        Self::new()
    }
}
