//! Pool navigateur dédié (Task 2 du plan v2).
//!
//! Responsabilités :
//! - `ensure_running()` : idempotent. Retourne une référence au `Browser`
//!   connecté en CDP ; spawn Chrome au premier appel ; relance si crash.
//! - `status()` : snapshot JSON-safe (`{running, pages, pid}`).
//! - `shutdown()` : kill propre (SIGTERM puis SIGKILL après grace) + abort
//!   du task handler.
//!
//! Choix techniques (vérifiés sur docs.rs) :
//! - `chromiumoxide::Browser::launch(BrowserConfig)` (v0.9.1) gère déjà le
//!   spawn, la lecture du stderr ("DevTools listening on ws://…") et la
//!   connexion CDP — pas besoin de réinventer. Cf. rapport Task 2.
//! - `Browser::Drop` (vu dans la doc) attend la fin du process enfant ; on
//!   s'appuie dessus + un `shutdown()` explicite pour le kill propre.
//! - `Browser::try_wait()` détecte un crash entre deux `ensure_running()`.
//! - `Browser::get_mut_child()` donne accès au PID du process enfant.
//! - Le `Handler` retourné par `launch` doit être drainé sous peine de
//!   saturer le buffer CDP — on le `spawn` dans une task Tokio et on
//!   stocke le `JoinHandle` pour `abort` au shutdown.
//!
//! Architecture :
//! - `OwnedBrowser` : détient le `chromiumoxide::Browser`. API minimale
//!   pour Task 2 : `pages()`, `kill()`, `try_alive()`.
//! - `BrowserPool` : `Arc<Mutex<PoolInner>>` qui orchestre spawn, kill,
//!   détection de crash. Clone = partage.
//! - `PoolInner` : `Option<OwnedBrowser>` + `Option<JoinHandle<()>>` (le
//!   task handler qui draine le `Handler` CDP) + `Option<u32>` (PID).
//!
//! Testabilité : la feature `test-util` n'est pas utilisée pour Task 2.
//! Les tests unitaires ne lancent pas Chrome (ils vérifient juste que
//! l'état initial est cohérent et que `status` ne pan pas). Le test de
//! crash/relance est documenté comme « test manuel curl » car il
//! nécessite un vrai Chrome. Raison : stubber chromiumoxide proprement
//! demanderait un mock du Browser (complexité non négligeable) et le
//! gain est marginal — `ensure_running` ne fait que vérifier `try_alive`
//! et re-spawner, c'est ~20 lignes triviales.
//!
//! Si on a besoin d'un test automatisé plus tard : on peut extraire
//! `spawn_inner` derrière un trait `Launcher` (comme c'était fait dans
//! la première version) et injecter un stub. C'est documenté en dette
//! technique du rapport.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::Config;

/// Statut JSON-safe exposé par `/v2/browser/status`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BrowserStatus {
    /// `true` si un process Chrome est running et CDP joignable.
    pub running: bool,
    /// Nombre de pages CDP ouvertes (0 si non running).
    pub pages: usize,
    /// PID du process Chrome (None si non running).
    pub pid: Option<u32>,
}

/// Wrapper qui détient un `chromiumoxide::Browser` et en expose une API
/// minimale (Task 2). Pas de fuite du type chromiumoxide hors du module.
pub struct OwnedBrowser {
    inner: Option<chromiumoxide::Browser>,
}

impl OwnedBrowser {
    pub fn new(b: chromiumoxide::Browser) -> Self {
        Self { inner: Some(b) }
    }

    /// Nombre de pages CDP ouvertes.
    pub async fn pages(&self) -> usize {
        let Some(b) = self.inner.as_ref() else {
            return 0;
        };
        b.pages().await.map(|p| p.len()).unwrap_or(0)
    }

    /// Tue le process Chrome. `Drop` de chromiumoxide attend la fin
    /// automatiquement.
    pub async fn kill(&mut self) {
        if let Some(mut b) = self.inner.take() {
            let _ = b.kill().await;
        }
    }

    /// Vérifie si le process est encore vivant. Retourne `true` si
    /// `try_wait` confirme qu'il tourne encore, `false` sinon (crash
    /// ou pas de child — ce dernier cas ne devrait pas arriver en Task 2
    /// car on utilise toujours `launch`).
    pub fn try_alive(&mut self) -> bool {
        let Some(b) = self.inner.as_mut() else {
            return false;
        };
        // try_wait retourne :
        //   Ok(None) : le process tourne encore
        //   Ok(Some(_)) : le process a fini (crash détecté)
        //   Err(_) : pas de child (ne devrait pas arriver)
        matches!(b.try_wait(), Ok(None))
    }
}

/// Pool navigateur. Clone = partage du même état (Arc interne).
#[derive(Clone)]
pub struct BrowserPool {
    inner: Arc<Mutex<PoolInner>>,
    /// Chemin du profil Chrome persistant (`$DSH_EYES_HOME/chrome-profile`).
    user_data_dir: PathBuf,
    /// Headless ou pas.
    headless: bool,
    /// Chemin Chrome explicite (None = détection auto).
    chrome_path: Option<String>,
}

/// État interne mutable du pool.
struct PoolInner {
    /// Browser vivant (None si shutdown / pas encore lancé / crash).
    browser: Option<OwnedBrowser>,
    /// PID courant (None si pas running).
    pid: Option<u32>,
    /// Task handler (drain des events CDP). `abort()` au shutdown.
    handler_task: Option<tokio::task::JoinHandle<()>>,
}

impl BrowserPool {
    /// Construit un pool à partir de la config. Le pool n'a pas encore
    /// lancé Chrome — c'est `ensure_running()` qui déclenche le spawn.
    pub fn new(config: &Config) -> Self {
        let home = crate::vigil::dsh_eyes_home().unwrap_or_else(|_| {
            // Fallback : /tmp/.dsh-eyes. On log un warning — en pratique
            // Config::load a déjà créé le home, donc cette branche n'est
            // atteinte que si le FS est devenu read-only entre temps.
            tracing::warn!("impossible de calculer DSH_EYES_HOME, fallback /tmp/.dsh-eyes");
            PathBuf::from("/tmp/.dsh-eyes")
        });
        let user_data_dir = home.join("chrome-profile");
        // Crée le répertoire du profil — chromiumoxide ne le crée pas
        // toujours (vu dans la doc, c'est implicite mais le user-data-dir
        // doit exister).
        let _ = std::fs::create_dir_all(&user_data_dir);

        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                browser: None,
                pid: None,
                handler_task: None,
            })),
            user_data_dir,
            headless: config.chrome_headless,
            chrome_path: config.chrome_path.clone(),
        }
    }

    /// Vérifie que le browser tourne et est vivant ; relance si nécessaire.
    /// Idempotent : peut être appelé 100 fois, ne spawn qu'une fois (sauf
    /// crash détecté).
    pub async fn ensure_running(&self) -> Result<(), anyhow::Error> {
        let mut inner = self.inner.lock().await;

        // Si on a un browser, vérifie qu'il est encore vivant.
        if let Some(b) = inner.browser.as_mut() {
            if b.try_alive() {
                return Ok(());
            }
            // Le process est mort (try_wait a retourné Some ou Err).
            // On nettoie et on relance.
            tracing::warn!("process Chrome détecté mort, relance…");
            if let Some(task) = inner.handler_task.take() {
                task.abort();
            }
            // OwnedBrowser::kill est idempotent : si l'OS l'a déjà attendu,
            // c'est un no-op.
            if let Some(mut b) = inner.browser.take() {
                b.kill().await;
            }
            inner.pid = None;
        }

        // Pas de browser ou vient de crasher → on en relance un.
        Self::spawn_inner(&self.user_data_dir, self.headless, self.chrome_path.as_deref(), &mut inner)
            .await
    }

    /// Lance Chrome et met à jour l'état interne. Doit être appelé sous
    /// le lock.
    async fn spawn_inner(
        user_data_dir: &std::path::Path,
        headless: bool,
        chrome_path: Option<&str>,
        inner: &mut PoolInner,
    ) -> Result<(), anyhow::Error> {
        use chromiumoxide::{Browser, BrowserConfig};

        let mut builder = BrowserConfig::builder()
            .user_data_dir(user_data_dir)
            // Args minimaux : pas d'extensions, pas de first-run UI.
            // chromiumoxide pose déjà --remote-debugging-port=0, --disable-gpu,
            // et d'autres args défensifs par défaut (vu dans la doc 0.9.1).
            .arg("--no-first-run")
            .arg("--disable-extensions")
            .arg("--no-default-browser-check")
            .arg("--disable-background-networking");

        if headless {
            // --headless=new (cf. doc 0.9.1)
            builder = builder.new_headless_mode();
        } else {
            builder = builder.with_head();
        }
        if let Some(p) = chrome_path {
            builder = builder.chrome_executable(p);
        }

        let config = builder
            .build()
            .map_err(|e| anyhow::anyhow!("BrowserConfig::build a échoué: {e}"))?;

        let (mut browser, mut handler) = Browser::launch(config)
            .await
            .map_err(|e| anyhow::anyhow!("Browser::launch a échoué: {e}"))?;

        // Récupère le PID du process enfant.
        let pid = browser
            .get_mut_child()
            .and_then(|c| c.inner.id())
            .ok_or_else(|| anyhow::anyhow!("impossible de récupérer le PID du process Chrome"))?;

        // Spawn une task qui draine le Handler. Si on ne le fait pas, le
        // buffer CDP sature et le navigateur se fige.
        let handler_task = tokio::spawn(async move {
            use futures_util::StreamExt;
            while let Some(_event) = handler.next().await {
                // Drain pur — on n'agit pas sur les events ici (Task 2
                // ne fait que spawn/kill/status).
            }
        });

        inner.browser = Some(OwnedBrowser::new(browser));
        inner.pid = Some(pid);
        inner.handler_task = Some(handler_task);

        tracing::info!(
            pid = pid,
            user_data_dir = %user_data_dir.display(),
            headless = headless,
            "Browser Chrome lancé"
        );
        Ok(())
    }

    /// Snapshot JSON-safe du statut.
    pub async fn status(&self) -> BrowserStatus {
        let mut inner = self.inner.lock().await;
        let running = if let Some(b) = inner.browser.as_mut() {
            b.try_alive()
        } else {
            false
        };
        let pages = if running {
            if let Some(b) = inner.browser.as_ref() {
                b.pages().await
            } else {
                0
            }
        } else {
            0
        };
        BrowserStatus {
            running,
            pages,
            pid: if running { inner.pid } else { None },
        }
    }

    /// Kill propre : arrête le handler task et le process Chrome. Best-effort.
    pub async fn shutdown(&self) {
        let mut inner = self.inner.lock().await;
        if let Some(task) = inner.handler_task.take() {
            task.abort();
        }
        if let Some(mut b) = inner.browser.take() {
            b.kill().await;
        }
        inner.pid = None;
        tracing::info!("BrowserPool shutdown terminé");
    }
}

// ============================================================================
// Tests unitaires — couverture de la logique de cycle de vie SANS Chrome.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Config minimal pour `BrowserPool::new`. On bypass `Config::load`
    /// pour ne pas générer de token / fichier en test.
    fn cfg_for_test() -> Config {
        Config::for_tests(true, None)
    }

    #[tokio::test]
    async fn status_initial_running_false() {
        let pool = BrowserPool::new(&cfg_for_test());
        let s = pool.status().await;
        assert!(!s.running, "un pool neuf ne doit pas être running");
        assert_eq!(s.pages, 0);
        assert!(s.pid.is_none(), "un pool neuf ne doit pas avoir de PID");
    }

    #[tokio::test]
    async fn status_ne_panique_pas_sur_browser_none() {
        let pool = BrowserPool::new(&cfg_for_test());
        // Appels multiples pour vérifier qu'on ne pan pas.
        for _ in 0..3 {
            let s = pool.status().await;
            assert!(!s.running);
            assert_eq!(s.pages, 0);
            assert!(s.pid.is_none());
        }
    }

    #[tokio::test]
    async fn shutdown_sur_pool_vide_ne_pan_pas() {
        let pool = BrowserPool::new(&cfg_for_test());
        // Pas d'erreur, pas de panic.
        pool.shutdown().await;
        let s = pool.status().await;
        assert!(!s.running);
    }

    #[tokio::test]
    async fn ensure_running_echoue_proprement_si_pas_de_chrome() {
        // Ce test ne lance pas Chrome. On vérifie juste que :
        // - soit Chrome est dans l'env et ça marche (peu probable en CI)
        // - soit on remonte une erreur (cas attendu), sans panic.
        // NOTE : on override `chrome_path` avec un chemin bidon pour
        // garantir l'échec.
        let cfg = Config::for_tests(true, Some("/nonexistent/chrome-binary".into()));
        let pool = BrowserPool::new(&cfg);
        let res = pool.ensure_running().await;
        // Doit remonter Err (pas de Chrome à ce chemin) — pas Ok ni panic.
        assert!(res.is_err(), "ensure_running doit échouer si Chrome introuvable");
    }
}
