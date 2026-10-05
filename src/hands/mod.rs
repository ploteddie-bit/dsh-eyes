//! Module `hands` — cycle de vie des composants externes (v2).
//!
//! Sous-modules :
//! - `browser` : `BrowserPool` (spawn + CDP + kill propre).
//!
//! Le préfixe `hands` évoque la métaphore « mains » (les outils que le
//! daemon manipule) — par opposition à la « vue » (`api`) et au « jugement »
//! (`vigil`).

pub mod browser;
