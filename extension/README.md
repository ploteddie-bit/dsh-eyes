# DSH Eyes — Extension Chrome MV3

Vigil de navigation pour le harnais DSH. Observe les requêtes web terminées
et pousse les **métadonnées** (host, url, content-length, timestamp) au
démon local via WebSocket.

> **Métadonnées uniquement — aucun contenu de page, aucun envoi hors localhost.**

## Contrat

- **Cible WS** : `ws://127.0.0.1:8445/ws` (démon `dsh-eyes`, Task 1–4).
- **Format du message** (JSON, conforme au contrat Task 2 — `deny_unknown_fields`) :

  ```json
  {
    "ts":    1700000000000,
    "domain": "example.com",
    "url":    "https://example.com/path?q=1",
    "kind":   "request",
    "bytes":  1234
  }
  ```

  `bytes` est `null` (omis via `Option<u64>` côté Rust) si la réponse n'expose
  pas de header `Content-Length`.

- **Reconnexion** : backoff exponentiel 1 s → 30 s, plafonné.
- **File FIFO mémoire** : 1000 événements max si le démon est injoignable ;
  au-delà, perte assumée et loggée dans la console du service worker.
- **MV3** : service worker réveillé par `chrome.webRequest.onCompleted`,
  reconnexion initialisée depuis zéro à chaque réveil (état runtime réinitialisé
  — pas de variable globale persistente pour la connexion).

## Installation

1. **Lancer le démon** (à faire avant de charger l'extension) :

   ```sh
   cd ~/projets/dsh-eyes
   cargo run --release
   # → "dsh-eyes écoute sur 127.0.0.1:8445"
   ```

2. **Ouvrir la page des extensions** dans Chrome :

   ```
   chrome://extensions
   ```

3. **Activer le mode développeur** (toggle en haut à droite).

4. **Charger l'extension non empaquetée** :
   - Bouton « Charger l'extension non empaquetée ».
   - Sélectionner le dossier `~/projets/dsh-eyes/extension/`.
   - L'extension « DSH Eyes 0.1.0 » apparaît dans la liste.

5. **Vérifier** :
   - Ouvrir la console du service worker (carte de l'extension →
     « Inspecter le service worker » → onglet **Console**) : voir
     `[dsh-eyes] service worker démarré` puis `[dsh-eyes] WS ouverte`.
   - Naviguer sur quelques URLs : `curl http://127.0.0.1:8445/stats` doit
     montrer les domaines visités avec leurs compteurs.

## Test hors navigateur (vérification du contrat WS)

`extension/test-ws-client.js` simule exactement les messages émis par
`background.js` et vérifie l'acquittement `{"ok":true}` du démon.

```sh
cd ~/projets/dsh-eyes/extension
node test-ws-client.js
```

Ce test **n'instancie pas Chrome** ; il valide uniquement la conformité du
format JSON accepté par `ws://127.0.0.1:8445/ws`. Le test réel de bout en
bout (Chrome + extension + navigation) reste manuel (étape 6 du plan).

## Permissions (cf. `manifest.json`)

| Permission        | Pourquoi                                                      |
|-------------------|---------------------------------------------------------------|
| `webRequest`      | observer `onCompleted` sur `<all_urls>`.                       |
| `storage`         | réservée pour un futur allowlist / config côté extension.     |
| `<all_urls>`       | requis pour observer toutes les requêtes terminées.            |

**Aucune** permission `tabs`, `cookies`, `webRequestBlocking`, ni capture
de contenu de page.

## Limites connues

- **Pas de capture de réponse** : on ne lit que les headers HTTP (`content-length`),
  jamais le corps. Les URLs ne sont pas envoyées ailleurs que sur le WS localhost.
- **Pas d'allowlist dans l'extension** : toute requête HTTP(S) vue par Chrome
  produit un événement. Si Eddie veut filtrer côté client, prochaine itération :
  `chrome.storage.local` + filtre dans `buildEvent()`.
- **Service worker éphémère** : le contenu de la file FIFO est perdu à chaque
  réveil du worker (acceptable — c'est ce que dit le brief). Pour aller
  plus loin : persistance via `chrome.storage.local` (dégradation gracieuse).