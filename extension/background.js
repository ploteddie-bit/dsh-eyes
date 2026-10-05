// background.js — Service worker MV3 du vigil DSH Eyes.
//
// Rôle : observer la navigation via `chrome.webRequest.onCompleted` (<all_urls>),
// extraire des métadonnées (host, url, taille si présente dans les headers
// content-length), et pousser chaque observation au démon local
// `ws://127.0.0.1:8445/ws` au format Task 2 :
//
//   { "ts": <epoch_ms>, "domain": <host>, "url": <url>, "kind": "request"|"response", "bytes": <u64|null> }
//
// Notes MV3 importantes :
// - Le service worker peut être terminé à tout moment par Chrome ; il ne faut
//   AUCUN état persistant dans des variables globales pour la logique de
//   connexion (seule la file FIFO mémoire est tolérée — cf. brief).
// - `chrome.webRequest.onCompleted` réveille le service worker, donc la
//   reconnexion au démon est initialisée à chaque réveil (depuis zéro,
//   backoff compris). C'est volontaire : on évite toute dépendance entre
//   cycles de vie.
// - Aucune capture de contenu de page : on lit uniquement les headers
//   `content-length` (déjà présents en méta), `statusCode`, `url`, `host`.
//   Aucun envoi hors `ws://127.0.0.1:8445/ws`.

const WS_URL = "ws://127.0.0.1:8445/ws";

// Bornes de la file FIFO mémoire (brief : max 1000 événements si le démon
// est injoignable). Au-delà, perte assumée + log dans la console worker.
const QUEUE_MAX = 1000;

// Bornes du backoff de reconnexion (brief : 1 s → 30 s, exponentiel, plafond).
const BACKOFF_MIN_MS = 1000;
const BACKOFF_MAX_MS = 30000;

// État runtime du worker — réinitialisé à chaque réveil (cf. note MV3 ci-dessus).
let ws = null;
let queue = [];        // FIFO des événements non encore acquittés par le serveur.
let backoff = BACKOFF_MIN_MS;
let reconnectTimer = null;
let connecting = false;
let closedByWorker = false;

// Log dans la console du service worker (visible depuis chrome://extensions
// → "Inspecter le service worker" → onglet Console).
function log(...args) {
  console.log("[dsh-eyes]", ...args);
}

// Sérialise et envoie un événement. À l'appel, on garantit que `ws` est OPEN.
function sendEvent(evt) {
  try {
    ws.send(JSON.stringify(evt));
  } catch (e) {
    // Cas rare : OPEN côté client mais send lève (back-pressure, fermeture
    // concurrente). On remet l'événement en queue, on force la reconnexion.
    log("send a échoué, remise en queue :", e && e.message);
    enqueue(evt);
    scheduleReconnect();
  }
}

// Empile un événement en tête de FIFO. Si la file dépasse QUEUE_MAX, on
// perd le plus ancien (FIFO) et on le logge explicitement.
function enqueue(evt) {
  queue.push(evt);
  while (queue.length > QUEUE_MAX) {
    const dropped = queue.shift();
    log("FIFO pleine, événement perdu :", dropped);
  }
}

// ── Mains (REWORK 2026-10-05, modèle webbridge) ─────────────────────────

// Vide la FIFO en envoyant chaque événement. Si le send échoue, on remet
// l'événement en tête de file et on interrompt le drain (la reconnexion
// s'en chargera).
function drainQueue() {
  while (queue.length > 0 && ws && ws.readyState === WebSocket.OPEN) {
    const evt = queue[0];
    try {
      ws.send(JSON.stringify(evt));
      queue.shift();
    } catch (e) {
      log("drain: send a échoué, on reconnecte :", e && e.message);
      enqueue(queue.shift()); // re-pousse en queue (en tête, puis on sortira)
      // Ré-empile correctement : on a shift+retour, donc on remet à l'index 0.
      scheduleReconnect();
      return;
    }
  }
}

// Programme une tentative de reconnexion avec backoff exponentiel.
function scheduleReconnect() {
  if (reconnectTimer !== null) return;          // déjà programmée
  if (closedByWorker) return;                   // arrêt volontaire
  log(`reconnexion dans ${backoff} ms`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, backoff);
}

// Tente d'ouvrir la WS. Au succès → drain de la FIFO. À l'échec →
// backoff × 2 (plafonné à BACKOFF_MAX_MS) puis re-planification.
function connect() {
  if (connecting || (ws && ws.readyState === WebSocket.OPEN)) return;
  connecting = true;
  closedByWorker = false;
  try {
    ws = new WebSocket(WS_URL);
  } catch (e) {
    connecting = false;
    log("échec new WebSocket :", e && e.message);
    backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
    scheduleReconnect();
    return;
  }

  ws.addEventListener("open", () => {
    log("WS ouverte");
    connecting = false;
    backoff = BACKOFF_MIN_MS;     // reset du backoff sur succès
    drainQueue();
  });

  ws.addEventListener("message", (ev) => {
    let payload = null;
    try {
      payload = JSON.parse(ev.data);
    } catch (_) { /* message non-JSON, on ignore */ }
    if (!payload) return;

    // Commande du démon (REWORK 2026-10-05 — modèle webbridge).
    // Forme : {"cmd_id":N,"cmd":"goto"|"click"|"read"|"fill"|"tabs_create", "params":{...}}
    if (typeof payload.cmd_id === "number" && typeof payload.cmd === "string") {
      // Exécution asynchrone ; on protège avec un timeout local.
      handleCommand(payload.cmd_id, payload.cmd, payload.params || {})
        .catch((e) => log("handleCommand a crashé :", e && e.message));
      return;
    }

    // Ack legacy (réponse du serveur à un événement de navigation).
    if (payload.ok === false) {
      log("serveur a refusé un événement :", payload.error || "(sans détail)");
    }
  });

  ws.addEventListener("close", (ev) => {
    log(`WS fermée (code=${ev.code}, reason=${ev.reason || "(vide)"})`);
    connecting = false;
    ws = null;
    if (!closedByWorker) {
      backoff = Math.min(Math.max(backoff * 2, BACKOFF_MIN_MS), BACKOFF_MAX_MS);
      scheduleReconnect();
    }
  });

  ws.addEventListener("error", (ev) => {
    // L'événement `error` est suivi d'un `close` qui pilotera la reconnexion.
    // On logge juste pour le diagnostic.
    log("WS erreur :", ev && ev.message ? ev.message : "(détail indisponible)");
  });
}

// Construit un événement au format Task 2 à partir d'un détail webRequest.
// `details` est l'objet fourni par `chrome.webRequest.onCompleted`.
function buildEvent(details) {
  let host = "";
  try {
    // hostname (sans port) et non host : « 127.0.0.1:8445 » doit rester
    // parsable comme IP par le vigil côté démon, sinon la règle
    // localhost_access est contournée dès qu'un port est explicite.
    host = new URL(details.url).hostname;
  } catch (_) {
    return null; // URL malformée : on laisse tomber (cas rarissime).
  }
  if (!host) return null;

  // Taille : content-length dans les headers de réponse. Selon la spec
  // webRequest, c'est une string. On tente d'abord responseHeaders, puis
  // fallback sur requestHeaders si pertinent.
  let bytes = null;
  const headers = details.responseHeaders || details.requestHeaders || [];
  for (const h of headers) {
    if (!h || !h.name) continue;
    if (h.name.toLowerCase() === "content-length") {
      const n = parseInt(h.value, 10);
      if (Number.isFinite(n) && n >= 0) {
        bytes = n;
        break;
      }
    }
  }

  return {
    ts: Date.now(),
    domain: host,
    url: details.url,
    // kind libre côté serveur (string). `request` capture le départ de la
    // requête, `response` la fin — on choisit `request` pour onCompleted car
    // c'est l'observation terminale du cycle requête/réponse et c'est
    // cohérent avec le seul échantillon de message utilisé par les tests
    // Rust (`"kind": "request"`).
    kind: "request",
    bytes,
  };
}

// Handler webRequest : alimente la FIFO et tente d'envoyer si la WS est OPEN.
// Doit rester SYNCHRONE (MV3) — pas d'async ici.
function onRequestCompleted(details) {
  // Filet de sécurité : on ignore les schemes non-HTTP(S) (chrome://, file://,
  // about:, devtools://…) qui n'ont pas de host utile et qui ne correspondent
  // pas à de la "navigation web".
  if (details.url.startsWith("ws://") || details.url.startsWith("wss://")) return;

  const evt = buildEvent(details);
  if (!evt) return;

  if (ws && ws.readyState === WebSocket.OPEN) {
    sendEvent(evt);
  } else {
    enqueue(evt);
    // Si aucune tentative n'est en cours, on en lance une.
    if (!connecting && reconnectTimer === null) {
      scheduleReconnect();
    }
  }
}

// Inscription de l'observer webRequest. L'event page est réveille à chaque
// `onCompleted` ; le manifeste ne déclare pas `extraHostsSpecifiers`, donc
// <all_urls> est suffisant.
//
// On NE capture QUE `onCompleted` (volontairement), pas `onBeforeRequest` ni
// `onSendHeaders` — un seul événement par requête/réponse, métadonnées
// terminales (statusCode + headers visibles).
chrome.webRequest.onCompleted.addListener(
  onRequestCompleted,
  { urls: ["<all_urls>"] }
);

// ============================================================================
// REWORK 2026-10-05 — modèle webbridge : l'extension reçoit des commandes
// du démon et les exécute via chrome.tabs / chrome.scripting.executeScript.
//
// Commandes supportées :
//   - "tabs_create" {url} : ouvre un nouvel onglet sur `url` ; attend
//     `chrome.tabs.onUpdated` status=complete ; retourne {tab_id, url}.
//   - "goto" {tab_id?, url} : si `tab_id` est fourni, navigue cet onglet
//     via chrome.tabs.update ; sinon crée un onglet comme tabs_create.
//   - "read" {tab_id, max_bytes?} : exécute un script qui retourne
//     `document.body.innerText` (tronqué à `max_bytes` ou 200 Ko par
//     défaut) ; retourne {text}.
//   - "click" {tab_id?, selector} : exécute `document.querySelector(sel).click()`.
//     Si `tab_id` est omis, prend l'onglet actif (`chrome.tabs.query`).
//   - "fill" {tab_id?, selector, value} : exécute
//     `(el.value = value, el.dispatchEvent(new Event('input', {bubbles:true})))`.
//     `tab_id` optionnel (cf. click).
//   - "wait" {tab_id?, selector, timeout_ms} : poll toutes les 100 ms
//     `document.querySelector(sel)` jusqu'à ce qu'il renvoie non-null
//     ou que `timeout_ms` soit écoulé. Format d'erreur stable :
//     `"wait_timeout: '<sel>' introuvable après <N> ms"` — le démon
//     peut le matcher pour un 504 dédié (cf. spec Task 4).
//   - "screenshot" {tab_id?, format?} : `chrome.tabs.captureVisibleTab`
//     sur l'onglet ciblé (résolu sur l'onglet actif si `tab_id` est
//     omis). Retourne `{format:"png", data_url:<base64 dataURL>}`. Le
//     dataURL est renvoyé brut — le démon extrait le base64 si besoin.
//     Nécessite `<all_urls>` dans `host_permissions` (déjà déclaré) ;
//     `activeTab` n'est PAS requis quand `<all_urls>` est présent.
//   - "captcha_detect" {tab_id?} : `chrome.scripting.executeScript`
//     sur l'onglet qui retourne `{detected, type, selector, image_b64?}`
//     selon les marqueurs DOM. v1 simple : `image_b64` =
//     `captureVisibleTab` plein écran ; le crop viendra plus tard.
//
// Toutes les commandes sont enveloppées dans un timeout local de 25 s
// (légèrement en deçà du timeout serveur de 30 s pour éviter les races).
// En cas d'échec on renvoie `{cmd_id, ok:false, error:...}`.
// ============================================================================

const COMMAND_TIMEOUT_MS = 25_000;
const READ_MAX_BYTES_DEFAULT = 200 * 1024; // 200 Ko

/**
 * Dispatch principal d'une commande. Sécurisé par un timeout local.
 */
async function handleCommand(cmd_id, cmd, params) {
  log(`commande #${cmd_id} ${cmd}`, params);
  let timer;
  try {
    const result = await Promise.race([
      dispatchCommand(cmd, params),
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timeout local ${COMMAND_TIMEOUT_MS} ms`)),
          COMMAND_TIMEOUT_MS,
        );
      }),
    ]);
    sendResponse({ cmd_id, ok: true, result });
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    log(`commande #${cmd_id} échouée :`, msg);
    sendResponse({ cmd_id, ok: false, error: msg });
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Envoie une réponse JSON au démon sur la WS ouverte. */
function sendResponse(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    log("sendResponse : WS non ouverte, on ne peut pas répondre");
    return;
  }
  try {
    ws.send(JSON.stringify(payload));
  } catch (e) {
    log("sendResponse a échoué :", e && e.message);
  }
}

/** Dispatch selon `cmd`. */
async function dispatchCommand(cmd, params) {
  switch (cmd) {
    case "tabs_create":
      return cmdTabsCreate(params);
    case "goto":
      return cmdGoto(params);
    case "read":
      return cmdRead(params);
    case "click":
      return cmdClick(params);
    case "fill":
      return cmdFill(params);
    case "wait":
      return cmdWait(params);
    case "screenshot":
      return cmdScreenshot(params);
    case "captcha_detect":
      return cmdCaptchaDetect(params);
    default:
      throw new Error(`commande inconnue: ${cmd}`);
  }
}

/** Ouvre un onglet et attend la fin du chargement. */
async function cmdTabsCreate({ url }) {
  if (typeof url !== "string" || !url) {
    throw new Error("tabs_create: 'url' (string) requis");
  }
  const tab = await chrome.tabs.create({ url });
  await waitForTabComplete(tab.id);
  return { tab_id: tab.id, url: tab.url || url };
}

/** Navigue un onglet (ou en crée un) vers `url`. */
async function cmdGoto({ tab_id, url }) {
  if (typeof url !== "string" || !url) {
    throw new Error("goto: 'url' (string) requis");
  }
  if (tab_id != null) {
    await chrome.tabs.update(tab_id, { url });
    await waitForTabComplete(tab_id);
    return { tab_id, url };
  }
  return cmdTabsCreate({ url });
}

/** Lit le innerText d'un onglet via scripting.executeScript. */
async function cmdRead({ tab_id, max_bytes }) {
  if (tab_id == null) throw new Error("read: 'tab_id' requis");
  const cap = Number.isFinite(max_bytes) ? max_bytes : READ_MAX_BYTES_DEFAULT;
  const injection = (cap) => {
    const full = document.body ? document.body.innerText : "";
    if (full.length <= cap) return full;
    return full.slice(0, cap) + "\n[…tronqué à " + cap + " octets]";
  };
  const results = await chrome.scripting.executeScript({
    target: { tabId: tab_id },
    func: injection,
    args: [cap],
  });
  if (!Array.isArray(results) || results.length === 0) {
    throw new Error("read: executeScript n'a rien renvoyé");
  }
  const text = results[0].result;
  if (typeof text !== "string") {
    throw new Error("read: résultat non-string");
  }
  return { text };
}

/**
 * Résout un `tab_id` optionnel. Si `tab_id` est null/undefined, prend
 * l'onglet actif de la fenêtre courante. Factorisé pour click/fill/wait
 * — chaque endpoint peut donc être appelé après un `goto` sans connaître
 * l'ID retourné.
 */
async function resolveTabId(tab_id) {
  if (tab_id != null) return tab_id;
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tabs || tabs.length === 0) {
    throw new Error("aucun onglet actif trouvé (et tab_id non fourni)");
  }
  return tabs[0].id;
}

/** Clique sur un élément via son sélecteur CSS. */
async function cmdClick({ tab_id, selector }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("click: 'selector' (string) requis");
  }
  const real_tab_id = await resolveTabId(tab_id);
  const injection = (sel) => {
    const el = document.querySelector(sel);
    if (!el) throw new Error("sélecteur introuvable: " + sel);
    el.click();
    return true;
  };
  await chrome.scripting.executeScript({
    target: { tabId: real_tab_id },
    func: injection,
    args: [selector],
  });
  return { clicked: selector };
}

/** Remplit un input/textarea et déclenche l'event `input`. */
async function cmdFill({ tab_id, selector, value }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("fill: 'selector' (string) requis");
  }
  if (typeof value !== "string") {
    throw new Error("fill: 'value' (string) requis");
  }
  const real_tab_id = await resolveTabId(tab_id);
  const injection = (sel, val) => {
    const el = document.querySelector(sel);
    if (!el) throw new Error("sélecteur introuvable: " + sel);
    el.focus();
    el.value = val;
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  };
  await chrome.scripting.executeScript({
    target: { tabId: real_tab_id },
    func: injection,
    args: [selector, value],
  });
  return { filled: selector, length: value.length };
}

/**
 * Attend qu'un élément apparaisse dans le DOM (sélecteur CSS).
 *
 * Polling côté service worker : un `executeScript` toutes les
 * `WAIT_POLL_MS` (100 ms) jusqu'à ce que `document.querySelector(sel)`
 * renvoie non-null OU que `timeout_ms` soit écoulé. On borne le timeout
 * côté handler à 10 s (cf. brief Task 4) — au-delà on rejette avec une
 * erreur explicite plutôt que de laisser le `COMMAND_TIMEOUT_MS` (25 s)
 * global arbitrer. Le résultat est `{found, selector, elapsed_ms}`.
 *
 * Si `tab_id` est omis, on travaille sur l'onglet actif via
 * `chrome.tabs.query({active:true,currentWindow:true})`. Ça permet à
 * `POST /v2/wait` d'être appelé juste après un `goto` sans connaître
 * l'ID.
 */
const WAIT_POLL_MS = 100;
const WAIT_TIMEOUT_MAX_MS = 10_000;

async function cmdWait({ tab_id, selector, timeout_ms }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("wait: 'selector' (string) requis");
  }
  const tmo = Number.isFinite(timeout_ms) ? Math.floor(timeout_ms) : 5000;
  if (tmo < 0) {
    throw new Error("wait: 'timeout_ms' doit être >= 0");
  }
  if (tmo > WAIT_TIMEOUT_MAX_MS) {
    throw new Error(`wait: 'timeout_ms' > ${WAIT_TIMEOUT_MAX_MS} non supporté`);
  }

  // Résout le tab_id si absent (helper partagé avec click/fill).
  const real_tab_id = await resolveTabId(tab_id);

  const start = Date.now();
  const deadline = start + tmo;
  const injection = (sel) => Boolean(document.querySelector(sel));
  while (Date.now() < deadline) {
    const results = await chrome.scripting.executeScript({
      target: { tabId: real_tab_id },
      func: injection,
      args: [selector],
    });
    if (Array.isArray(results) && results.length > 0 && results[0].result === true) {
      return {
        found: true,
        selector,
        elapsed_ms: Date.now() - start,
      };
    }
    await new Promise((r) => setTimeout(r, WAIT_POLL_MS));
  }
  // Timeout écoulé : on signale une erreur typée pour que le handler
  // Rust puisse mapper en 504 (cf. `send_cmd`).
  throw new Error(
    `wait_timeout: '${selector}' introuvable après ${tmo} ms`,
  );
}

/**
 * Attend que l'onglet atteigne le statut `complete`. Poll sur
 * `chrome.tabs.get` (50 ms), timeout 20 s.
 */
async function waitForTabComplete(tab_id) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    const tab = await chrome.tabs.get(tab_id);
    if (tab.status === "complete") return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error("waitForTabComplete: timeout 20 s");
}

// ============================================================================
// Task 6 — screenshot + détection captcha
//
// Modèle : on réutilise `chrome.scripting.executeScript` (déjà autorisé par
// le manifeste) et `chrome.tabs.captureVisibleTab` (couvert par
// `<all_urls>` dans `host_permissions`). On NE touche PAS au manifeste :
// `scripting` + `tabs` + `<all_urls>` suffisent pour ces deux opérations.
//
// `chrome.tabs.captureVisibleTab` exige un `windowId` : on le récupère via
// `chrome.tabs.get(tab_id)`. Si la fenêtre est minimisée ou si l'onglet
// n'est pas la fenêtre active du navigateur, Chrome peut rejeter
// l'opération (cf. bug Chrome "Tabs cannot be edited" quand l'onglet est
// inactif). On accepte l'échec côté extension et on propage l'erreur.
// ============================================================================

/**
 * Capture le viewport visible d'un onglet. Retourne un dataURL base64
 * (`data:image/png;base64,...`) ou un dataURL JPEG selon `format`.
 *
 * Params: `{tab_id?, format?}` — `format` est `"png"` par défaut, on
 * accepte aussi `"jpeg"` (le dataURL commence par `data:image/jpeg;`).
 *
 * `chrome.tabs.captureVisibleTab` ne fonctionne que sur l'onglet actif de
 * la fenêtre — si l'utilisateur appelle `screenshot` sur un onglet en
 * arrière-plan, Chrome renvoie une erreur. v1 : on tente quand même
 * (Chrome 120+ assouplit parfois) et on propage l'erreur upstream si
 * échec. Le client peut alors passer le bon `tab_id` après `goto`.
 */
async function cmdScreenshot({ tab_id, format }) {
  const real_tab_id = await resolveTabId(tab_id);
  // Récupère le windowId via tabs.get (nécessaire par captureTabs).
  const tab = await chrome.tabs.get(real_tab_id);
  if (!tab || typeof tab.windowId !== "number") {
    throw new Error("screenshot: onglet introuvable ou sans windowId");
  }
  const fmt = format === "jpeg" ? "jpeg" : "png";
  const data_url = await chrome.tabs.captureVisibleTab(tab.windowId, {
    format: fmt,
  });
  if (typeof data_url !== "string" || !data_url.startsWith("data:image/")) {
    throw new Error("screenshot: captureVisibleTab n'a pas renvoyé de dataURL");
  }
  return { format: fmt, data_url };
}

/**
 * Détection de captcha v1 — heuristique DOM côté content-script via
 * `executeScript`. On inspecte des sélecteurs CSS connus (iframes
 * reCAPTCHA / hCaptcha, classes `.g-recaptcha` / `.h-captcha`,
 * `<input name*=captcha>`, `<img src*=captcha>`, slider+canvas).
 *
 * Si on détecte un type "image" (recaptcha_v2_image, hcaptcha, text),
 * on capture le viewport complet via `captureVisibleTab` et on le
 * place dans `image_b64` (le crop de l'élément précis viendra plus
 * tard — Chrome ne fournit pas de crop server-side dans MV3).
 *
 * Params: `{tab_id?}` — pas de validation de host (l'extension a
 * `<all_urls>`). Retourne :
 *   `{ detected: bool, type: string, selector?: string, image_b64?: string }`
 *
 * Format du `type` (aligné avec le brief Task 6) :
 *   - "none"               : rien détecté (detected=false).
 *   - "recaptcha_v2_invisible" : iframe[src*="recaptcha"] sans classe image.
 *   - "recaptcha_v2_image"     : iframe[src*="recaptcha"] + classe image.
 *   - "hcaptcha"               : iframe[src*="hcaptcha"] ou .h-captcha.
 *   - "text"                   : img[src*="captcha"] ou input[name*="captcha"].
 *   - "slider"                 : class*=slider + canvas.
 *
 * Note : l'ordre des vérifications reflète la précédence observée
 * (reCAPTCHA invisible = cas le plus fréquent en 2026).
 */
async function cmdCaptchaDetect({ tab_id }) {
  const real_tab_id = await resolveTabId(tab_id);
  const injection = () => {
    // Helper : retourne le premier élément matchant `sel` ou null.
    const pick = (sel) => document.querySelector(sel);

    // reCAPTCHA v2 (image OU invisible) : iframe injectée par Google.
    // L'attribut `src` contient `recaptcha`. La classe du body ou la
    // taille de l'iframe distinguent "image" (cases à cocher → images)
    // de "invisible" (badge flottant sans UI).
    const recaptcha_iframe = pick('iframe[src*="recaptcha"]');
    if (recaptcha_iframe) {
      // Heuristique v1 : `recaptcha/api2/audio` ou `recaptcha/api2/image`
      // → mode image. `recaptcha/api2/bframe` → invisible.
      const src = (recaptcha_iframe.getAttribute("src") || "").toLowerCase();
      const is_image =
        src.includes("/image") || src.includes("/audio") || src.includes("fallback");
      return {
        detected: true,
        type: is_image ? "recaptcha_v2_image" : "recaptcha_v2_invisible",
        selector: 'iframe[src*="recaptcha"]',
      };
    }

    // reCAPTCHA v2/v3 fallback : div .g-recaptcha (sans iframe —
    // cas rare, présent sur certains sites qui chargent le script en
    // async). On le classe en "image" par défaut (l'API publique ne
    // distingue pas v2 de v3 via le DOM).
    const grecaptcha_div = pick(".g-recaptcha");
    if (grecaptcha_div) {
      return {
        detected: true,
        type: "recaptcha_v2_image",
        selector: ".g-recaptcha",
      };
    }

    // hCaptcha : iframe OU div .h-captcha.
    const hcaptcha_iframe = pick('iframe[src*="hcaptcha"]');
    if (hcaptcha_iframe) {
      return {
        detected: true,
        type: "hcaptcha",
        selector: 'iframe[src*="hcaptcha"]',
      };
    }
    const hcaptcha_div = pick(".h-captcha");
    if (hcaptcha_div) {
      return {
        detected: true,
        type: "hcaptcha",
        selector: ".h-captcha",
      };
    }

    // Captcha textuel "old school" : image avec src contenant "captcha"
    // ou input dont le name contient "captcha".
    const captcha_img = pick('img[src*="captcha"]');
    if (captcha_img) {
      return {
        detected: true,
        type: "text",
        selector: 'img[src*="captcha"]',
      };
    }
    const captcha_input = pick('input[name*="captcha"]');
    if (captcha_input) {
      return {
        detected: true,
        type: "text",
        selector: 'input[name*="captcha"]',
      };
    }

    // Slider captcha : élément avec class*=slider ET un canvas dedans.
    const slider_el = Array.from(document.querySelectorAll('[class*="slider"]'))
      .find((el) => el.querySelector("canvas"));
    if (slider_el) {
      // Sélecteur synthétique : on remonte un selector CSS pour
      // l'élément slider. `tag.class` suffit pour un usage humain.
      const tag = slider_el.tagName.toLowerCase();
      const cls = slider_el.getAttribute("class") || "";
      const sel = cls
        ? `${tag}.${cls.split(/\s+/).filter(Boolean).join(".")}`
        : tag;
      return {
        detected: true,
        type: "slider",
        selector: sel,
      };
    }

    // Rien.
    return { detected: false, type: "none" };
  };
  const results = await chrome.scripting.executeScript({
    target: { tabId: real_tab_id },
    func: injection,
  });
  if (!Array.isArray(results) || results.length === 0) {
    throw new Error("captcha_detect: executeScript n'a rien renvoyé");
  }
  const det = results[0].result;
  if (!det || typeof det.detected !== "boolean") {
    throw new Error("captcha_detect: forme de réponse invalide");
  }
  // Si captcha "image" détecté : on capture le viewport pour fournir
  // `image_b64` au client. Pour les types invisibles (pas d'UI à
  // screenshotter), on omet `image_b64`.
  if (
    det.detected &&
    (det.type === "recaptcha_v2_image" ||
      det.type === "hcaptcha" ||
      det.type === "text")
  ) {
    try {
      const tab = await chrome.tabs.get(real_tab_id);
      const data_url = await chrome.tabs.captureVisibleTab(tab.windowId, {
        format: "png",
      });
      if (typeof data_url === "string" && data_url.startsWith("data:image/")) {
        // On extrait la partie base64 (après la virgule) — le démon
        // n'aura pas besoin du préfixe MIME. v1 : pas de crop, c'est
        // le viewport entier. La réduction viendra côté serveur
        // (downscaling) ou plus tard (crop via OffscreenCanvas).
        const b64 = data_url.includes(",")
          ? data_url.substring(data_url.indexOf(",") + 1)
          : data_url;
        det.image_b64 = b64;
      }
    } catch (e) {
      // Échec capture (onglet inactif, etc.) : on remonte le résultat
      // de détection SANS image_b64. Le client peut relancer
      // /v2/page/screenshot séparément.
      log("captcha_detect: captureVisibleTab a échoué, on continue sans image_b64 :", e && e.message);
    }
  }
  return det;
}

// Démarrage initial : on tente une connexion tout de suite. Si le démon est
// down, le backoff s'enclenche.
log("service worker démarré");
connect();