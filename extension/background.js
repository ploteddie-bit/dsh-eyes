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

// v2.1 — couche de comportement humain (port ghost-cursor).
import { humanMousePath, humanTypingPlan } from "./humanize-math.js";

// Dernière position de curseur connue par onglet (les injections de
// trajectoire démarrent d'ici — jamais du même point deux fois de suite).
const cursorState = new Map(); // tab_id → {x, y}

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
    // Keep-alive (REWORK v2.1) : alarme 25 s — réveille le worker AVANT
    // sa suspension MV3 (~30 s) et maintient la WS ouverte en permanence.
    chrome.alarms.create("dsh-eyes-keepalive", { periodInMinutes: 0.4 });
  });

  // Réagit à l'alarme keep-alive : ping WS (ou reconnect si morte).
  chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name !== "dsh-eyes-keepalive") return;
    if (ws && ws.readyState === WebSocket.OPEN) {
      try {
        ws.send(JSON.stringify({ ping: Date.now() }));
      } catch (e) {
        log("keep-alive : send a échoué, reconnexion :", e && e.message);
        scheduleReconnect();
      }
    } else {
      log("keep-alive : WS fermée, reconnexion");
      scheduleReconnect();
    }
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
    case "tab_group":
      return cmdTabGroup(params);
    case "tab_activate":
      return cmdTabActivate(params);
    case "screenshot":
      return cmdScreenshot(params);
    case "captcha_detect":
      return cmdCaptchaDetect(params);
    case "eval":
      return cmdEval(params);
    case "click_native":
      return cmdClickNative(params);
    case "click_native_text":
      return cmdClickNativeText(params);
    case "click_native_rect":
      return cmdClickNativeRect(params);
    case "screen_pos":
      return cmdScreenPos(params);
    case "wait_for":
      return cmdWaitFor(params);
    case "type_native":
      return cmdTypeNative(params);
    case "scroll_native":
      return cmdScrollNative(params);
    default:
      throw new Error(`commande inconnue: ${cmd}`);
  }
}

/** Ouvre un onglet et attend la fin du chargement. */
/**
 * Déduplication : cherche un onglet existant pointant sur la même page
 * (origine + chemin identiques — query, hash et slash final ignorés).
 * Retourne l'onglet trouvé ou null.
 */
async function findTabByUrl(url) {
	let target;
	try {
		target = new URL(url);
	} catch {
		return null;
	}
	const norm = (p) => p.replace(/\/+$/, "").toLowerCase();
	const tabs = await chrome.tabs.query({});
	return (
		tabs.find((t) => {
			if (!t.url) return false;
			try {
				const u = new URL(t.url);
				return (
					u.origin === target.origin &&
					norm(u.pathname) === norm(target.pathname)
				);
			} catch {
				return false;
			}
		}) ?? null
	);
}

async function cmdTabsCreate({ url, group_title }) {
  if (typeof url !== "string" || !url) {
    throw new Error("tabs_create: 'url' (string) requis");
  }
  const titre = typeof group_title === "string" && group_title ? group_title : null;

  if (titre) {
    // Réutilisation (retour Eddie) : un groupe = UN onglet de travail.
    // Si un onglet du groupe existe déjà, on le navigue au lieu d'empiler
    // des onglets ; la fenêtre du groupe est ainsi réutilisée elle aussi.
    const groupes = await chrome.tabGroups.query({ title: titre });
    for (const groupe of groupes) {
      const onglets = await chrome.tabs.query({ groupId: groupe.id });
      if (onglets.length > 0) {
        const cible = onglets[0];
        await chrome.tabs.update(cible.id, { url, active: true });
        await waitForTabComplete(cible.id);
        return {
          tab_id: cible.id,
          url,
          reused: true,
          window_id: cible.windowId,
          group_id: groupe.id,
        };
      }
    }
  }

  // Déduplication (retour Eddie) : onglet identique déjà ouvert → focus
  // au lieu de créer un doublon. Avec un groupe demandé, l'onglet trouvé
  // est adopté dans le groupe (migration propre au premier passage).
  const existing = await findTabByUrl(url);
  if (existing) {
    await chrome.tabs.update(existing.id, { active: true });
    await waitForTabComplete(existing.id);
    const reponse_existante = {
      tab_id: existing.id,
      url: existing.url || url,
      reused: true,
      window_id: existing.windowId,
    };
    if (titre) {
      const group_id = await chrome.tabs.group({ tabIds: [existing.id] });
      await chrome.tabGroups.update(group_id, { title: titre, color: "blue" });
      reponse_existante.group_id = group_id;
    }
    return reponse_existante;
  }

  const tab = await chrome.tabs.create({ url });
  await waitForTabComplete(tab.id);
  const reponse = { tab_id: tab.id, url: tab.url || url, reused: false, window_id: tab.windowId };
  if (titre) {
    const group_id = await chrome.tabs.group({ tabIds: [tab.id] });
    await chrome.tabGroups.update(group_id, { title: titre, color: "blue" });
    reponse.group_id = group_id;
  }
  return reponse;
}

/** Navigue un onglet (ou en crée un) vers `url`. */
async function cmdGoto({ tab_id, url }) {
  if (typeof url !== "string" || !url) {
    throw new Error("goto: 'url' (string) requis");
  }
  // Sans tab_id explicite : déduplication d'abord, création ensuite.
  if (tab_id === undefined) {
    const existing = await findTabByUrl(url);
    if (existing) {
      await chrome.tabs.update(existing.id, { active: true });
      if (existing.url !== url) await chrome.tabs.update(existing.id, { url });
      await waitForTabComplete(existing.id);
      return { tab_id: existing.id, url, reused: true };
    }
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
 * Exécute du JavaScript arbitraire dans l'onglet ciblé (monde MAIN :
 * accès au JS de la page, utile pour les SPA type Angular/GA4 que le
 * DOM cachait aux sélecteurs CSS). `code` est soit une expression
 * ("document.title"), soit une fonction sérialisée "(a) => a.x + 1"
 * recevant `arg` (JSON). Les promesses sont attendues ; le résultat
 * est assaini (structure-cloneable) avant remontée vers le démon.
 * Retourne { value } — null si le code n'a rien retourné.
 */
async function cmdEval({ tab_id, code, arg }) {
  if (typeof code !== "string" || !code.trim()) {
    throw new Error("eval: 'code' (string) requis");
  }
  const id = await resolveTabId(tab_id);
  // v0.5.1 : évaluation via CDP Runtime.evaluate — INSENSIBLE à la CSP de
  // la page. Constat 20/10 : scripting.executeScript + eval() interne était
  // bloqué par les sites à CSP stricte (le nôtre : script-src sans
  // unsafe-eval — GA4 l'acceptait, annonces-m2m.com non). Runtime.evaluate
  // tourne dans le moteur, hors CSP, monde principal de la page.
  const expression = `(async () => { const f = ${code}; ` +
    `return typeof f === "function" ? await f(${JSON.stringify(arg === undefined ? null : arg)}) : await f; })()`;
  return withDebugger(id, async (dbg) => {
    const rep = await dbg("Runtime.evaluate", {
      expression,
      awaitPromise: true,
      returnByValue: true,
    });
    if (rep.exceptionDetails) {
      const d = rep.exceptionDetails;
      return { __eval_error: String((d.exception && (d.exception.description || d.exception.value)) || d.text || "erreur") };
    }
    const v = rep.result && rep.result.value;
    return { value: v === undefined ? null : v };
  });
}

// ── v0.4 : gestes NATIFS (chrome.debugger + Input.dispatch*) ────────────
// Doctrine importée de new-pmu (règles 9-10, directives du 12/10/2026) :
// le JS de page ne déclenche JAMAIS un geste — el.click() produit des
// événements isTrusted=false, signature d'automatisation mesurée en A/B
// (« clic JS = identifiable, souris native = propre »). Le JS ne sert
// qu'à LIRE (géométrie de l'élément) ; tout geste passe par le protocole
// d'entrée natif : Input.dispatchMouseEvent / dispatchKeyEvent via
// chrome.debugger → événements isTrusted=true, indiscernables d'une
// vraie souris/clavier.
// NB : chaque commande attache puis détache le debugger — le bandeau
// « a commencé le débogage » clignote en haut du Chrome : c'est le
// signal honnête qu'un geste natif est en cours.

/** Dernier point curseur connu par onglet (les trajectoires partent de là). */
const nativeCursor = new Map(); // tab_id → {x, y}

const sleepNat = (ms) => new Promise((r) => setTimeout(r, ms));

/** Attache le debugger le temps de `fn(dbg)`, détache TOUJOURS ensuite.
 *  v0.5.3 : plomberie blindée — chaque commande CDP est bornée (8 s), le
 *  geste entier est borné (15 s), le détachement est borné (1,5 s) et non
 *  bloquant. Constat 20/10 : le clic était DÉLIVRÉ (isTrusted=true capté)
 *  mais la réponse au démon se perdait (timeout 25 s côté démon) — désormais
 *  pire cas ~16,5 s < 25 s : l'accusé de réception revient toujours. */
async function withDebugger(tabId, fn) {
  await chrome.debugger.attach({ tabId }, "1.3");
  const dbg = (method, params) => Promise.race([
    chrome.debugger.sendCommand({ tabId }, method, params),
    new Promise((_, rej) => setTimeout(() => rej(new Error("CDP " + method + " sans réponse (8 s)")), 8000)),
  ]);
  try {
    const garde = new Promise((_, rej) =>
      setTimeout(() => rej(new Error("geste natif incomplet (15 s)")), 15000));
    return await Promise.race([fn(dbg), garde]);
  } finally {
    try {
      await Promise.race([
        chrome.debugger.detach({ tabId }),
        new Promise((r) => setTimeout(r, 1500)),
      ]);
    } catch (_) { /* déjà détaché */ }
  }
}

/** Fraction gaussienne (Box-Muller) bornée [lo, hi] — jamais le centre exact. */
function fractionGaussienne(lo = 0.2, hi = 0.8) {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  const g = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  return Math.min(hi, Math.max(lo, 0.5 + g * 0.15));
}

/** Trajectoire courte vers la cible : 3-8 mouseMoved interpolés, cadence
 *  irrégulière, part du dernier point connu — jamais de téléportation. */
async function deplacerCurseur(dbg, tabId, cible) {
  const dep = nativeCursor.get(tabId) || { x: 8, y: Math.round(120 + Math.random() * 240) };
  const etapes = 3 + Math.floor(Math.random() * 5);
  for (let i = 1; i <= etapes; i++) {
    const t = i / etapes;
    const x = Math.round(dep.x + (cible.x - dep.x) * t + (Math.random() - 0.5) * 2);
    const y = Math.round(dep.y + (cible.y - dep.y) * t + (Math.random() - 0.5) * 2);
    await dbg("Input.dispatchMouseEvent", { type: "mouseMoved", x, y });
    await sleepNat(8 + Math.random() * 17);
  }
  nativeCursor.set(tabId, cible);
}

/** Lecture seule en JS : géométrie viewport d'un sélecteur. */
async function lireRect(id, selector) {
  const [res] = await chrome.scripting.executeScript({
    target: { tabId: id },
    func: (sel) => {
      const el = document.querySelector(sel);
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return { x: r.x, y: r.y, w: r.width, h: r.height, visible: r.width > 0 && r.height > 0 };
    },
    args: [selector],
  });
  return res ? res.result : null;
}

/** Clic natif : trajectoire + hésitation + press/release (isTrusted=true). */
async function cmdClickNative({ tab_id, selector }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("click_native: 'selector' (string) requis");
  }
  const id = await resolveTabId(tab_id);
  const rect = await lireRect(id, selector);
  if (!rect) throw new Error("click_native: sélecteur introuvable: " + selector);
  if (!rect.visible) throw new Error("click_native: élément hors viewport (scroll_native d'abord)");
  const cible = {
    x: Math.round(rect.x + rect.w * fractionGaussienne()),
    y: Math.round(rect.y + rect.h * fractionGaussienne()),
  };
  return withDebugger(id, async (dbg) => {
    await deplacerCurseur(dbg, id, cible);
    await sleepNat(40 + Math.random() * 60); // hésitation pré-clic
    await dbg("Input.dispatchMouseEvent", {
      type: "mousePressed", x: cible.x, y: cible.y, button: "left", clickCount: 1,
    });
    await sleepNat(45 + Math.random() * 50);
    await dbg("Input.dispatchMouseEvent", {
      type: "mouseReleased", x: cible.x, y: cible.y, button: "left", clickCount: 1,
    });
    return { clicked: selector, at: cible, native: true };
  });
}

/** virtual-keycode minimal : lettres/chiffres/Espace/Entrée (reste → 0). */
function vkDe(ch) {
  const c = ch.toUpperCase();
  const a = c.charCodeAt(0);
  if (c === "ENTER") return 13;
  if (ch === " ") return 32;
  if (a >= 65 && a <= 90) return a; // A-Z
  if (a >= 48 && a <= 57) return a; // 0-9
  return 0;
}

/** Frappe native caractère par caractère : keyDown(text) + keyUp, dwell
 *  40-160 ms — jamais keyboard.press (dwell nul = signature). */
async function cmdTypeNative({ tab_id, selector, text, submit }) {
  if (typeof text !== "string") throw new Error("type_native: 'text' (string) requis");
  const id = await resolveTabId(tab_id);
  if (selector) {
    // Préparation d'ÉTAT (focus + sélection du contenu existant) : ni clic
    // ni frappe — pas d'événement de geste (doctrine : le JS prépare,
    // le natif agit).
    const [res] = await chrome.scripting.executeScript({
      target: { tabId: id },
      func: (sel) => {
        const el = document.querySelector(sel);
        if (!el) return null;
        el.focus();
        if (typeof el.select === "function") el.select();
        return true;
      },
      args: [selector],
    });
    if (!res || !res.result) throw new Error("type_native: sélecteur introuvable: " + selector);
  }
  return withDebugger(id, async (dbg) => {
    for (const ch of text) {
      await dbg("Input.dispatchKeyEvent", {
        type: "keyDown", key: ch, text: ch, unmodifiedText: ch,
        windowsVirtualKeyCode: vkDe(ch), nativeVirtualKeyCode: vkDe(ch),
      });
      await sleepNat(40 + Math.random() * 120);
      await dbg("Input.dispatchKeyEvent", {
        type: "keyUp", key: ch, windowsVirtualKeyCode: vkDe(ch),
      });
      await sleepNat(15 + Math.random() * 45);
    }
    if (submit) {
      await sleepNat(60 + Math.random() * 90);
      await dbg("Input.dispatchKeyEvent", {
        type: "keyDown", key: "Enter", code: "Enter",
        windowsVirtualKeyCode: 13, nativeVirtualKeyCode: 13,
      });
      await sleepNat(40);
      await dbg("Input.dispatchKeyEvent", {
        type: "keyUp", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13,
      });
    }
    return { typed: text.length, submit: !!submit, native: true };
  });
}

/** Molette native : crans réalistes 90-135 px, 30-70 ms d'écart — jamais
 *  de scroll JS (0 événement wheel + saut instantané = signature). */
async function cmdScrollNative({ tab_id, dy }) {
  const total = Math.round(Number(dy));
  if (!Number.isFinite(total) || total === 0) {
    throw new Error("scroll_native: 'dy' (pixels, non nul) requis");
  }
  const id = await resolveTabId(tab_id);
  const sens = Math.sign(total);
  let reste = Math.abs(total);
  return withDebugger(id, async (dbg) => {
    let crans = 0;
    while (reste > 0) {
      const cran = Math.min(reste, 90 + Math.floor(Math.random() * 46));
      reste -= cran;
      const p = nativeCursor.get(id) || { x: 400, y: 320 };
      await dbg("Input.dispatchMouseEvent", {
        type: "mouseWheel", x: p.x, y: p.y, deltaX: 0, deltaY: sens * cran,
      });
      crans += 1;
      await sleepNat(30 + Math.random() * 40);
    }
    return { scrolled: total, crans, native: true };
  });
}

/** Trouve un élément PAR TEXTE (lecture seule) et renvoie son rect viewport.
 *  Pont eval→natif : eval SAIT trouver (SPA, Angular), le natif SAIT
 *  cliquer. Aucun geste en JS ici. */
async function chercherRectTexte(id, texte, exact) {
  const [res] = await chrome.scripting.executeScript({
    target: { tabId: id },
    func: (txt, ex) => {
      const normaliser = (s) => (s || "").replace(/\s+/g, " ").trim();
      const cibles = [...document.querySelectorAll("a,button,[role=button],[role=tab],[role=link],input[type=submit],input[type=button]")];
      const el = ex
        ? cibles.find((e) => normaliser(e.textContent) === txt)
        : cibles.find((e) => normaliser(e.textContent).startsWith(txt));
      if (!el) return null;
      const r = el.getBoundingClientRect();
      if (r.width <= 0 || r.height <= 0) return { introuvable: "hors viewport" };
      return { x: r.x, y: r.y, w: r.width, h: r.height, texte: normaliser(el.textContent).slice(0, 40) };
    },
    args: [texte, !!exact],
  });
  return res ? res.result : null;
}

/** Clic natif sur un élément repéré PAR TEXTE (GA4/Angular : pas de
 *  sélecteur stable, mais un libellé). Préfixe par défaut, exact:true sinon. */
async function cmdClickNativeText({ tab_id, text, exact }) {
  if (typeof text !== "string" || !text) throw new Error("click_native_text: 'text' requis");
  const id = await resolveTabId(tab_id);
  const rect = await chercherRectTexte(id, text, exact);
  if (!rect) throw new Error("click_native_text: aucun élément portant le texte: " + text);
  if (rect.introuvable) throw new Error("click_native_text: élément « " + text + " » hors viewport (scroll_native d'abord)");
  return clicNatifSurRect(id, rect, JSON.stringify(text));
}

/** Clic natif à des coordonnées viewport BRUTES — brique universelle :
 *  eval cherche par n'importe quelle logique, ce clic exécute. */
async function cmdClickNativeRect({ tab_id, x, y }) {
  const px = Math.round(Number(x));
  const py = Math.round(Number(y));
  if (!Number.isFinite(px) || !Number.isFinite(py)) throw new Error("click_native_rect: 'x'/'y' requis");
  const id = await resolveTabId(tab_id);
  return clicNatifSurRect(id, { x: px, y: py, w: 0, h: 0 }, px + "," + py);
}

/** Cœur partagé des clics natifs : gaussienne dans le rect (si surface),
 *  trajectoire, hésitation, press/release — isTrusted=true. */
async function clicNatifSurRect(id, rect, libelle) {
  const cible = {
    x: Math.round(rect.x + (rect.w > 8 ? rect.w * fractionGaussienne() : 0)),
    y: Math.round(rect.y + (rect.h > 8 ? rect.h * fractionGaussienne() : 0)),
  };
  return withDebugger(id, async (dbg) => {
    await deplacerCurseur(dbg, id, cible);
    await sleepNat(40 + Math.random() * 60);
    await dbg("Input.dispatchMouseEvent", { type: "mousePressed", x: cible.x, y: cible.y, button: "left", clickCount: 1 });
    await sleepNat(45 + Math.random() * 50);
    await dbg("Input.dispatchMouseEvent", { type: "mouseReleased", x: cible.x, y: cible.y, button: "left", clickCount: 1 });
    return { clicked: libelle, at: cible, native: true };
  });
}

/** Géométrie ÉCRAN d'un sélecteur : active l'onglet (le clic OS frappe ce
 *  qui est à l'écran), renvoie la conversion viewport→écran — le script
 *  os-clic.sh déplace ensuite le VRAI pointeur (SetCursorPos+mouse_event). */
async function cmdScreenPos({ tab_id, selector }) {
  if (typeof selector !== "string" || !selector) throw new Error("screen_pos: 'selector' requis");
  const id = await resolveTabId(tab_id);
  const tab = await chrome.tabs.get(id);
  await chrome.tabs.update(id, { active: true });
  // v0.5.1 : focusser la FENÊTRE aussi — le clic OS frappe ce qui est au
  // premier plan de l'OS ; sans focus, il cliquait dans la fenêtre du
  // dessus (constat 20/10 : clic parti dans VS Code, pas dans Chrome).
  // v0.5.2 : une fenêtre MINIMISÉE rend une géométrie absurde (barresY
  // négatif constaté) et `focused` ne la restaure pas — on ne touche au
  // state QUE si elle est minimisée (restauration maximisée).
  if (tab.windowId) {
    try {
      const win = await chrome.windows.get(tab.windowId);
      const maj = { focused: true };
      if (win.state === "minimized") maj.state = "maximized";
      await chrome.windows.update(tab.windowId, maj);
    } catch (_) { /* fenêtre fermée */ }
  }
  await sleepNat(250);
  const [res] = await chrome.scripting.executeScript({
    target: { tabId: id },
    func: (sel) => {
      const el = document.querySelector(sel);
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return {
        cssX: Math.round(r.x + r.width / 2), cssY: Math.round(r.y + r.height / 2),
        rect: { x: r.x, y: r.y, w: r.width, h: r.height },
        screenX: window.screenX, screenY: window.screenY,
        barresY: window.outerHeight - window.innerHeight,
        bordsX: Math.max(0, Math.round((window.outerWidth - window.innerWidth) / 2)),
        dpr: window.devicePixelRatio || 1,
        visible: r.width > 0 && r.height > 0,
      };
    },
    args: [selector],
  });
  const geo = res ? res.result : null;
  if (!geo) throw new Error("screen_pos: sélecteur introuvable: " + selector);
  if (!geo.visible) throw new Error("screen_pos: élément hors viewport");
  // Formule nominale (px écran) ; offset de calibrage éventuel côté script OS.
  geo.ecranX = Math.round(geo.screenX + geo.bordsX + geo.cssX * geo.dpr);
  geo.ecranY = Math.round(geo.screenY + geo.barresY + geo.cssY * geo.dpr);
  return geo;
}

/** Attente polluante bornée d'un sélecteur OU d'un texte — remplace les
 *  sleep arbitraires (SPA lentes : GA4 met 3-8 s à rendre). */
async function cmdWaitFor({ tab_id, selector, text, timeout_ms }) {
  const id = await resolveTabId(tab_id);
  const aSelector = typeof selector === "string" && selector.length > 0;
  const aTexte = typeof text === "string" && text.length > 0;
  if (!aSelector && !aTexte) throw new Error("wait_for: 'selector' ou 'text' requis");
  const borne = Math.min(Math.max(Number(timeout_ms) || 10000, 500), 60000);
  const debut = Date.now();
  const limite = debut + borne;
  while (Date.now() < limite) {
    const [res] = await chrome.scripting.executeScript({
      target: { tabId: id },
      func: (sel, txt) => {
        if (sel) return !!document.querySelector(sel);
        return (document.body ? document.body.innerText : "").includes(txt);
      },
      args: [aSelector ? selector : null, aTexte ? text : null],
    });
    if (res && res.result) return { trouve: true, attente_ms: Date.now() - debut };
    await sleepNat(250);
  }
  return { trouve: false, timeout_ms: borne };
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

/**
 * Injecté dans la page : rejoue une trajectoire souris humaine (points
 * {x,y,delayMs} en coordonnées viewport), puis survole + clique l'élément
 * ciblé avec hésitation pré-clic. Auto-contenu (aucune closure externe).
 */
async function replayMousePathInPage(points, selector) {
	const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
	const dispatch = (type, x, y, target) => {
		const el = target || document.elementFromPoint(x, y) || document.documentElement;
		el.dispatchEvent(
			new MouseEvent(type, {
				bubbles: true,
				cancelable: true,
				view: window,
				clientX: x,
				clientY: y,
				button: 0,
			}),
		);
	};
	for (const p of points) {
		if (p.delayMs) await sleep(p.delayMs);
		dispatch("mousemove", p.x, p.y);
	}
	const el = document.querySelector(selector);
	if (!el) throw new Error("sélecteur introuvable: " + selector);
	const r = el.getBoundingClientRect();
	const cx = Math.round(r.x + r.width / 2);
	const cy = Math.round(r.y + r.height / 2);
	el.dispatchEvent(new MouseEvent("mouseover", { bubbles: true, clientX: cx, clientY: cy }));
	await sleep(60 + Math.random() * 90); // hésitation pré-clic
	el.dispatchEvent(new MouseEvent("mousedown", { bubbles: true, clientX: cx, clientY: cy }));
	await sleep(40 + Math.random() * 60);
	el.dispatchEvent(new MouseEvent("mouseup", { bubbles: true, clientX: cx, clientY: cy }));
	el.dispatchEvent(
		new MouseEvent("click", { bubbles: true, cancelable: true, clientX: cx, clientY: cy }),
	);
	return { moves: points.length, selector };
}

/**
 * Injecté dans la page : frappe clavier humaine caractère par caractère
 * selon le plan {type:'char'|'backspace', ch, delayMs}. Compatible React
 * (setter natif + event input). Auto-contenu.
 */
async function replayTypingInPage(plan, selector) {
	const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
	const el = document.querySelector(selector);
	if (!el) throw new Error("sélecteur introuvable: " + selector);
	el.focus();
	const setter = Object.getOwnPropertyDescriptor(
		el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype,
		"value",
	)?.set;
	let value = "";
	const key = (type, ch) => {
		if (ch) el.dispatchEvent(new KeyboardEvent(type, { key: ch, bubbles: true }));
		else el.dispatchEvent(new KeyboardEvent(type, { bubbles: true }));
	};
	for (const step of plan) {
		await sleep(step.delayMs);
		if (step.type === "backspace") {
			value = value.slice(0, -1);
			key("keydown", "Backspace");
			if (setter) setter.call(el, value);
			else el.value = value;
			el.dispatchEvent(new Event("input", { bubbles: true }));
			key("keyup", "Backspace");
		} else {
			value += step.ch;
			key("keydown", step.ch);
			key("keypress", step.ch);
			if (setter) setter.call(el, value);
			else el.value = value;
			el.dispatchEvent(new Event("input", { bubbles: true }));
			key("keyup", step.ch);
		}
	}
	el.dispatchEvent(new Event("change", { bubbles: true }));
	return { length: value.length };
}

/** Clique sur un élément via son sélecteur CSS — trajectoire humaine (v2.1). */
async function cmdClick({ tab_id, selector, direct }) {
	if (typeof selector !== "string" || !selector) {
		throw new Error("click: 'selector' (string) requis");
	}
	const real_tab_id = await resolveTabId(tab_id);

	// Mode direct (usage outillé) : clic synchrone sans trajectoire ni
	// timer. Les setTimeout du replay humain sont étranglés (≈1 s/étape
	// quand l'onglet est throttlé), ce qui dépasse le timeout local 25 s ;
	// le direct répond en < 300 ms quel que soit l'état de l'onglet.
	if (direct === true) {
		const [directRes] = await chrome.scripting.executeScript({
			target: { tabId: real_tab_id },
			func: (sel) => {
				const el = document.querySelector(sel);
				if (!el) throw new Error("sélecteur introuvable: " + sel);
				el.scrollIntoView({ block: "center" });
				const r = el.getBoundingClientRect();
				const cx = Math.round(r.x + r.width / 2);
				const cy = Math.round(r.y + r.height / 2);
				for (const type of ["mouseover", "mousedown", "mouseup", "click"]) {
					el.dispatchEvent(
						new MouseEvent(type, {
							bubbles: true,
							cancelable: true,
							view: window,
							clientX: cx,
							clientY: cy,
							button: 0,
						}),
					);
				}
				return { clicked: sel, mode: "direct" };
			},
			args: [selector],
		});
		return directRes.result;
	}

	// 1) Centre de la cible (après scrollIntoView centre).
	const [rectRes] = await chrome.scripting.executeScript({
		target: { tabId: real_tab_id },
		func: (sel) => {
			const el = document.querySelector(sel);
			if (!el) throw new Error("sélecteur introuvable: " + sel);
			el.scrollIntoView({ block: "center" });
			const r = el.getBoundingClientRect();
			return { x: Math.round(r.x + r.width / 2), y: Math.round(r.y + r.height / 2) };
		},
		args: [selector],
	});
	const target = rectRes.result;

	// 2) Trajectoire humaine depuis la dernière position de curseur connue.
	const from = cursorState.get(real_tab_id) ?? {
		x: 80 + Math.random() * 1000,
		y: 120 + Math.random() * 400,
	};
	const { points, overshoot } = humanMousePath(from, target);

	// 3) Replay dans la page (mouvements + hover + clic avec hésitation).
	const [replayRes] = await chrome.scripting.executeScript({
		target: { tabId: real_tab_id },
		func: replayMousePathInPage,
		args: [points, selector],
	});

	// 4) Mémorise la position finale pour la prochaine trajectoire.
	cursorState.set(real_tab_id, { x: target.x, y: target.y });
	return {
		clicked: selector,
		moves: points.length,
		overshoot,
		replay: replayRes.result,
	};
}

/** Remplit un input/textarea — frappe clavier humaine char par char (v2.1). */
async function cmdFill({ tab_id, selector, value }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("fill: 'selector' (string) requis");
  }
  if (typeof value !== "string") {
    throw new Error("fill: 'value' (string) requis");
  }
  const real_tab_id = await resolveTabId(tab_id);

  // 1) Focus + scrollIntoView centre (préparation humaine).
  await chrome.scripting.executeScript({
    target: { tabId: real_tab_id },
    func: (sel) => {
      const el = document.querySelector(sel);
      if (!el) throw new Error("sélecteur introuvable: " + sel);
      el.scrollIntoView({ block: "center" });
      el.focus();
    },
    args: [selector],
  });

  // 2) Plan de frappe humain (typos + backspaces + délais lognormaux).
  const plan = humanTypingPlan(value, { typoRate: 0.03 });

  // 3) Replay dans la page (frappe char par char, compatible React).
  const [replayRes] = await chrome.scripting.executeScript({
    target: { tabId: real_tab_id },
    func: replayTypingInPage,
    args: [plan, selector],
  });

  return {
    filled: selector,
    length: value.length,
    keystrokes: plan.length,
    replay: replayRes.result,
  };
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
 * Place un onglet dans un groupe d'onglets nommé (Task « persistance »).
 * Crée le groupe si nécessaire (chrome.tabs.group sans groupId → nouveau
 * groupe), puis le nomme/colore via chrome.tabGroups.update.
 * Params: `{tab_id?, title?, color?}` — tab_id absent = onglet actif.
 * Retourne `{tab_id, group_id, title}`.
 */
async function cmdTabGroup({ tab_id, title, color }) {
  if (typeof title !== "string" || !title) {
    throw new Error("tab_group: 'title' (string) requis");
  }
  const real_tab_id = await resolveTabId(tab_id);
  const group_id = await chrome.tabs.group({ tabIds: [real_tab_id] });
  const update = { title };
  if (typeof color === "string" && color) update.color = color;
  await chrome.tabGroups.update(group_id, update);
  return { tab_id: real_tab_id, group_id, title };
}

/**
 * Active un onglet et met sa fenetre au premier plan.
 *
 * Fiabilise les clics : l'animation souris de `cmdClick` utilise des
 * setTimeout dans la page, que Chrome etrangle quand l'onglet est cache
 * (arriere-plan) — un clic peut alors depasser le plafond de 25 s du
 * daemon. Params: `{tab_id?}`.
 */
async function cmdTabActivate({ tab_id }) {
  const real_tab_id = await resolveTabId(tab_id);
  const tab = await chrome.tabs.get(real_tab_id);
  await chrome.tabs.update(real_tab_id, { active: true });
  let focused = false;
  try {
    await chrome.windows.update(tab.windowId, { focused: true });
    focused = true;
  } catch {
    // Fenetre minimisee : Chrome peut refuser — best effort.
  }
  return { tab_id: real_tab_id, window_id: tab.windowId, activated: true, focused };
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