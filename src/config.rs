//! Configuration v2 du daemon `dsh-eyes` (Task 1).
//!
//! Sources de configuration :
//! - `~/.dsh-eyes/config.toml` (ou `$DSH_EYES_HOME/config.toml`) : options
//!   non sensibles (URLs Ollama, chrome_headless, etc.). Fichier créé à la
//!   demande s'il est absent (tous défauts).
//! - `$DSH_EYES_HOME/token` : token d'API binaire **séparé**, jamais
//!   dans `config.toml` (règle : le token ne doit pas se retrouver dans
//!   un fichier world-readable par accident). Généré au premier run,
//!   `chmod 600` (UNIX).
//!
//! Stratégie de chargement (ordres) :
//! 1. Charge (ou crée) `config.toml`. Si absent, applique tous les défauts.
//! 2. Charge (ou génère + chmod 600) le fichier `token`.
//! 3. Retourne une `Config` avec `api_token` disponible pour le middleware.
//!
//! Aucune dépendance non vérifiée sur docs.rs :
//! - `toml = "1"` (1.1.6 tiré) : API `toml::from_str` / `toml::to_string_pretty`.
//! - `getrandom = "0.2"` (0.2.17 tiré) : `getrandom::getrandom(&mut [u8])`.
//! - `std::os::unix::fs::PermissionsExt` (stdlib) pour le chmod 600.
//!
//! Choix "DSH_EYES_HOME" :
//! - On réutilise `vigil::dsh_eyes_home()` qui mutualise déjà la logique
//!   avec `allowlist.json` et `alerts.jsonl`. Cohérence avec le reste du code.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Configuration runtime du daemon.
///
/// Tous les champs sont publics pour un accès direct ; un builder n'est
/// pas nécessaire (les défauts sont déjà choisis en dur dans `Default`).
///
/// **REWORK 2026-10-05** : `chrome_headless` et `chrome_path` ont été
/// retirés. Le modèle webbridge n'a plus besoin d'un Chrome dédié
/// piloté par le daemon — l'extension Chrome de l'utilisateur devient
/// elle-même les « mains ». Les clés restent **tolérées** dans
/// `config.toml` (cf. [`ConfigFile`]) pour ne pas casser les fichiers
/// existants, mais ne sont plus chargées en mémoire.
#[derive(Debug, Clone)]
pub struct Config {
    /// Token d'API chargé depuis `$DSH_EYES_HOME/token`.
    api_token: String,
    /// URL d'Ollama tier 1 (défaut : `http://10.0.0.223:11434`).
    pub ollama_tier1_url: String,
    /// URL d'Ollama tier 2 (défaut : `http://10.0.0.245:11434`).
    pub ollama_tier2_url: String,
}

/// Représentation sérialisable dans `config.toml`. Pas de `api_token`
/// ici — le token vit dans son propre fichier (cf. doc de module).
///
/// `chrome_headless` et `chrome_path` sont tolérés (lisibles depuis un
/// ancien `config.toml`) mais **ignorés** : le daemon n'instancie plus
/// de Chrome. Cf. REWORK 2026-10-05.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigFile {
    #[serde(default = "default_ollama_tier1")]
    pub ollama_tier1_url: String,
    #[serde(default = "default_ollama_tier2")]
    pub ollama_tier2_url: String,
    /// Toléré pour rétro-compat avec un ancien `config.toml`. Ignoré.
    #[serde(default, skip_serializing)]
    #[allow(dead_code)]
    pub chrome_headless: Option<bool>,
    /// Toléré pour rétro-compat avec un ancien `config.toml`. Ignoré.
    #[serde(default, skip_serializing)]
    #[allow(dead_code)]
    pub chrome_path: Option<String>,
}

fn default_ollama_tier1() -> String {
    "http://10.0.0.223:11434".to_string()
}
fn default_ollama_tier2() -> String {
    "http://10.0.0.245:11434".to_string()
}

impl Config {
    /// Charge la configuration complète. Crée les fichiers manquants
    /// (`config.toml` avec défauts, `token` avec 32 octets random en hex).
    ///
    /// Comportement :
    /// - `$DSH_EYES_HOME` (ou `~/.dsh-eyes/`) est créé si absent.
    /// - `config.toml` absent → écrit un fichier de défauts (rassurant
    ///   pour l'utilisateur qui ouvre le dossier).
    /// - `token` absent → génère 32 octets random (256 bits), les écrit
    ///   en hex (64 chars ASCII), chmod 600.
    /// - `config.toml` malformé → on log un warning et on prend les défauts
    ///   (dégradation gracieuse, comme pour `allowlist.json`).
    pub fn load() -> std::io::Result<Self> {
        let home = crate::vigil::dsh_eyes_home()?;

        // 1) Charge (ou crée) config.toml.
        let cfg_file = load_or_create_config_file(&home)?;

        // 2) Charge (ou génère) le token.
        let api_token = load_or_create_token(&home)?;

        Ok(Self {
            api_token,
            ollama_tier1_url: cfg_file.ollama_tier1_url,
            ollama_tier2_url: cfg_file.ollama_tier2_url,
        })
    }

    /// Token d'API (référence). À n'utiliser que dans le middleware d'auth
    /// pour la comparaison constante — jamais loggué, jamais sérialisé.
    pub fn api_token(&self) -> &str {
        &self.api_token
    }

    /// Constructeur pour les tests. Bypass le chargement réel (token
    /// généré / fichier `config.toml`). Le `api_token` vide est OK
    /// pour les tests qui ne touchent pas l'auth.
    ///
    /// Marqué `#[cfg(any(test, feature = "test-util"))]` pour rester
    /// accessible aux tests d'intégration (`tests/`) sans tirer la
    /// feature en production.
    #[cfg(any(test, feature = "test-util"))]
    #[allow(dead_code)] // utilisé par les tests d'intégration
    pub fn for_tests() -> Self {
        Self {
            api_token: String::new(),
            ollama_tier1_url: "http://localhost:11434".into(),
            ollama_tier2_url: "http://localhost:11434".into(),
        }
    }
}

/// Charge `config.toml` ou le crée avec les défauts s'il est absent.
/// Malformé → `tracing::warn!` + retour des défauts (pas d'erreur fatale).
fn load_or_create_config_file(home: &Path) -> std::io::Result<ConfigFile> {
    let path = home.join("config.toml");
    if !path.exists() {
        // Écrit un fichier de défauts pour que l'utilisateur ait un point
        // d'entrée éditable. Pas d'erreur si l'écriture échoue (FS read-only
        // par ex.) — on retourne juste les défauts en mémoire.
        let defaults = ConfigFile::default();
        if let Ok(raw) = toml::to_string_pretty(&defaults) {
            // best-effort : si ça échoue, on continue avec les défauts en RAM
            let _ = std::fs::write(&path, raw);
        }
        return Ok(defaults);
    }
    match std::fs::read_to_string(&path) {
        Ok(raw) => match toml::from_str::<ConfigFile>(&raw) {
            Ok(c) => Ok(c),
            Err(e) => {
                tracing::warn!(error = %e, path = %path.display(),
                    "config.toml malformé ; défauts appliqués");
                Ok(ConfigFile::default())
            }
        },
        Err(e) => {
            tracing::warn!(error = %e, path = %path.display(),
                "lecture config.toml impossible ; défauts appliqués");
            Ok(ConfigFile::default())
        }
    }
}

/// Charge le token depuis `home/token`. Si le fichier est absent, le
/// génère (32 octets random → hex 64 chars) et l'écrit avec `chmod 600`.
///
/// Garanties :
/// - Le fichier est créé en mode 0o600 (lecture/écriture owner only).
/// - Le contenu est un hex de 64 chars (256 bits d'entropie).
/// - En cas d'erreur `getrandom`, on remonte une `std::io::Error`
///   (kind `Other`) — pas de token faible fabriqué en fallback.
fn load_or_create_token(home: &Path) -> std::io::Result<String> {
    let path = home.join("token");
    if path.exists() {
        // Fichier présent : on le lit tel quel. Note : on ne re-applique
        // pas le chmod à chaque load pour ne pas surprendre l'utilisateur
        // (s'il l'a édité à la main, on respecte ses droits).
        return std::fs::read_to_string(&path).map(|s| s.trim().to_string());
    }

    // Génère 32 octets random.
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| std::io::Error::other(format!("getrandom a échoué: {e}")))?;

    // Hex 64 chars (lower-case). On utilise un encodeur minimaliste plutôt
    // que de tirer la crate `hex` — c'est 8 lignes, pas de dépendance
    // additionnelle.
    let mut hex = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{b:02x}");
    }

    // Écriture atomique : fichier temporaire + rename, pour éviter de
    // laisser un token vide sur disque si le process crash en cours.
    let tmp = path.with_extension("token.tmp");
    std::fs::write(&tmp, hex.as_bytes())?;
    chmod_600(&tmp)?;
    std::fs::rename(&tmp, &path)?;
    // rename préserve les permissions sous Linux — pas besoin de re-chmoder.
    Ok(hex)
}

/// Applique le mode `0o600` (lecture/écriture owner only) à un fichier.
/// No-op sur les plateformes non-UNIX (le test skip la vérif via `cfg(unix)`).
fn chmod_600(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(path)?.permissions();
        perm.set_mode(0o600);
        std::fs::set_permissions(path, perm)?;
    }
    // Sur Windows, on ne peut pas poser 0o600 POSIX ; le fichier reste
    // accessible seulement à l'utilisateur courant via les ACL par défaut.
    let _ = path; // silence unused
    Ok(())
}

/// Helper exposé pour les tests : chemin du fichier token dans un home donné.
/// (Principalement pour le rapport ; les tests d'intégration passent par
/// `Config::load()` directement.)
#[allow(dead_code)]
pub fn token_path(home: &Path) -> PathBuf {
    home.join("token")
}
