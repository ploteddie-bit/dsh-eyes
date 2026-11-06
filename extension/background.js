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
import { humanMousePath, humanTypingPlan, humanScrollPlan, humanProfile } from "./humanize-math.js";

// Dernière position de curseur connue par onglet (les injections de
// trajectoire démarrent d'ici — jamais du même point deux fois de suite).
const cursorState = new Map(); // tab_id → {x, y}

// Onglets dont le debugger est attaché DURABLEMENT (capture `network`,
// contrat §6). `withDebugger` réutilise cette session au lieu de se
// l'attribuer (« Another debugger is already attached »), et ne la
// détache pas en sortie — sinon la capture en cours serait coupée.
const debuggerPersistant = new Set();

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
// Dernier pong applicatif reçu (heartbeat ping/pong, détection socket morte).
let lastPong = 0;

// Le token vit dans chrome.storage.local (clé `dsh_token`). Quand
// l'utilisateur le modifie dans les options, on force une reconnexion
// propre sans attendre le backoff.
chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== "local" || !changes.dsh_token) return;
  log("token modifié — reconnexion");
  closedByWorker = true;
  if (ws) {
    try { ws.close(); } catch (_) { /* déjà fermée */ }
  }
  ws = null;
  backoff = BACKOFF_MIN_MS;
  closedByWorker = false;
  setTimeout(connect, 100);
});

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
async function connect() {
  if (connecting || (ws && ws.readyState === WebSocket.OPEN)) return;
  connecting = true;
  closedByWorker = false;
  let token = "";
  try {
    const stored = await chrome.storage.local.get({ dsh_token: "" });
    token = String(stored.dsh_token || "").trim();
  } catch (e) {
    log("lecture du token impossible :", e && e.message);
  }
  if (!token) {
    connecting = false;
    log("token absent — renseigne-le dans les options de l'extension (clic droit sur l'icône → Options)");
    backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
    scheduleReconnect();
    return;
  }
  try {
    ws = new WebSocket(`${WS_URL}?token=${encodeURIComponent(token)}`);
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
    // Handshake de version/capacités : le démon l'expose via /v2/info.
    try {
      ws.send(JSON.stringify({
        hello: {
          client: "dsh-eyes-extension",
          version: chrome.runtime.getManifest().version,
          capabilities: [
            "read", "find", "click", "fill", "eval",
            "click_native", "click_native_text", "click_native_rect",
            "type_native", "scroll_native", "screen_pos",
            "screenshot", "captcha_detect", "record",
            "multiframe", "shadow_dom", "heartbeat", "human_profiles", "allframes_target",
            // Parité WebBridge (contrat docs/protocol-parity-2026-11-06.md) :
            // une entrée par commande — le démon refuse par `command_not_supported`
            // avant envoi si elle manque ici.
            "snapshot", "tabs_list", "tab_attach", "tab_close", "session_close",
            "send_keys", "network", "pdf", "upload", "cdp",
          ],
        },
      }));
    } catch (e) {
      log("hello: envoi impossible :", e && e.message);
    }
    lastPong = Date.now();
    try {
      ws.send(JSON.stringify({ ping: Date.now() }));
    } catch (_) {
      // La reconnexion est pilotée par l'événement close.
    }
    drainQueue();
    // Keep-alive (REWORK v2.1) : alarme 25 s — réveille le worker AVANT
    // sa suspension MV3 (~30 s) et maintient la WS ouverte en permanence.
    chrome.alarms.create("dsh-eyes-keepalive", { periodInMinutes: 0.4 });
  });

  // Réagit à l'alarme keep-alive : ping WS (ou reconnect si morte).
  // Garde d'idempotence : `connect()` est rappelé à chaque reconnexion ;
  // sans ce flag, chaque appel ajoutait un listener (fuite observée).
  if (!globalThis.__dshAlarmListenerInstalled) {
    globalThis.__dshAlarmListenerInstalled = true;
    chrome.alarms.onAlarm.addListener((alarm) => {
      if (alarm.name !== "dsh-eyes-keepalive") return;
      if (ws && ws.readyState === WebSocket.OPEN) {
        if (Date.now() - lastPong > 70000) {
          log("keep-alive : pas de pong depuis 70 s, reconnexion");
          try { ws.close(); } catch (_) { /* déjà fermée */ }
          return;
        }
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
  }

  ws.addEventListener("message", (ev) => {
    let payload = null;
    try {
      payload = JSON.parse(ev.data);
    } catch (_) { /* message non-JSON, on ignore */ }
    if (!payload) return;

    // Pong du heartbeat applicatif : la socket est vivante.
    if (typeof payload.pong === "number") {
      lastPong = Date.now();
      return;
    }

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
 * File d'attente par onglet (v0.6.1) : deux commandes concurrentes sur le
 * même onglet se percutent sur chrome.debugger.attach (« Another debugger
 * is already attached ») et brouillent les gestes. On chaîne les commandes
 * par clé (tab_id explicite, sinon « global »).
 */
const commandChains = new Map();

function withCommandLock(key, fn) {
  const previous = commandChains.get(key) || Promise.resolve();
  const run = previous.then(fn, fn);
  const tracked = run.catch(() => {});
  commandChains.set(key, tracked);
  tracked.then(() => {
    if (commandChains.get(key) === tracked) commandChains.delete(key);
  });
  return run;
}

/**
 * Dispatch principal d'une commande. Sécurisé par un timeout local et
 * sérialisé par onglet (v0.6.1).
 */
async function handleCommand(cmd_id, cmd, params) {
  log(`commande #${cmd_id} ${cmd}`, params);
  const cle = params && params.tab_id != null ? `tab:${params.tab_id}` : "global";
  let timer;
  try {
    const result = await Promise.race([
      withCommandLock(cle, () => dispatchCommand(cmd, params)),
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
      return noterOngletCree(await cmdTabsCreate(params), params);
    case "goto":
      return noterOngletCree(await cmdGoto(params), params);
    case "read":
      return cmdRead(params);
    case "find":
      return cmdFind(params);
    case "click":
      return cmdClick(await avecSelRefs(params));
    case "fill":
      return cmdFill(await avecSelRefs(params));
    case "wait":
      return cmdWait(await avecSelRefs(params));
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
    // ── Parité WebBridge (contrat docs/protocol-parity-2026-11-06.md) ──
    case "snapshot":
      return cmdSnapshot(params);
    case "tabs_list":
      return cmdTabsList(params);
    case "tab_attach":
      return cmdTabAttach(params);
    case "tab_close":
      return cmdTabClose(params);
    case "session_close":
      return cmdSessionClose(params);
    case "send_keys":
      return cmdSendKeys(params);
    case "network":
      return cmdNetwork(params);
    case "pdf":
      return cmdPdf(params);
    case "upload":
      return cmdUpload(params);
    case "cdp":
      return cmdCdp(params);
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
    // Réutilisation (retour opérateur) : un groupe = UN onglet de travail.
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

  // Déduplication (retour opérateur) : onglet identique déjà ouvert → focus
  // au lieu de créer un doublon. Avec un groupe demandé, l'onglet trouvé
  // est adopté dans le groupe (migration propre au premier passage).
  const existing = await findTabByUrl(url);
  if (existing) {
    // `findTabByUrl` déduplique sur origin+pathname (query/hash ignorés).
    // Il faut donc NAVIGUER l'onglet réutilisé vers l'URL demandée :
    // sinon `tabs_create` renvoie l'ancienne page et le démon pilote la
    // mauvaise URL (bug observé sur les SPA à query/hash).
    if (existing.url !== url) {
      await chrome.tabs.update(existing.id, { url, active: true });
      await waitForTabComplete(existing.id);
    } else {
      await chrome.tabs.update(existing.id, { active: true });
    }
    const reponse_existante = {
      tab_id: existing.id,
      url,
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

/** Lit un onglet. mode="text" (défaut) → innerText ; mode="outline" →
 *  liste structurée des éléments interactifs visibles (v0.6.1). */
async function cmdRead({ tab_id, max_bytes, mode, frame, start }) {
  if (tab_id == null) throw new Error("read: 'tab_id' requis");
  if (mode === "outline") {
    const injection = () => {
      const chemin = (el) => {
        if (el.id) return "#" + CSS.escape(el.id);
        const aria = el.getAttribute("aria-label");
        if (aria) return el.tagName.toLowerCase() + '[aria-label="' + aria.replace(/"/g, '\\"') + '"]';
        const name = el.getAttribute("name");
        if (name) return el.tagName.toLowerCase() + '[name="' + name.replace(/"/g, '\\"') + '"]';
        return el.tagName.toLowerCase();
      };
      // Auto-contenu : cette fonction est sérialisée dans chaque frame.
      const deepAll = (sel) => {
        const out = [];
        const vus = new Set();
        const walk = (root) => {
          if (!root || !root.querySelectorAll) return;
          let matches = [];
          try { matches = root.querySelectorAll(sel); } catch (_) { return; }
          for (const el of matches) {
            if (!vus.has(el)) { vus.add(el); out.push(el); }
          }
          for (const el of root.querySelectorAll("*")) {
            if (el.shadowRoot) walk(el.shadowRoot);
          }
        };
        walk(document);
        return out;
      };
      const out = [];
      const vus = new Set();
      deepAll("a,button,input,textarea,select,[role=button],[role=link],[role=tab],[contenteditable=true]").forEach((el) => {
        if (vus.has(el)) return;
        const r = el.getBoundingClientRect();
        if (r.width <= 0 || r.height <= 0) return;
        vus.add(el);
        out.push({
          tag: el.tagName.toLowerCase(),
          role: el.getAttribute("role") || "",
          text: (el.innerText || el.value || el.getAttribute("placeholder") || el.getAttribute("aria-label") || "").trim().replace(/\s+/g, " ").slice(0, 90),
          selector: chemin(el),
          x: Math.round(r.x),
          y: Math.round(r.y),
          w: Math.round(r.width),
          h: Math.round(r.height),
        });
      });
      return out.slice(0, 250);
    };
    const results = await chrome.scripting.executeScript({
      target: { tabId: tab_id, allFrames: true },
      func: injection,
    });
    const elements = [];
    for (const res of results || []) {
      if (!res || !Array.isArray(res.result)) continue;
      const frame_id = typeof res.frameId === "number" ? res.frameId : 0;
      for (const el of res.result) {
        elements.push({ ...el, frame_id });
        if (elements.length >= 250) break;
      }
      if (elements.length >= 250) break;
    }
    return { mode: "outline", elements, frames: (results || []).length };
  }
  const cap = Number.isFinite(max_bytes) ? max_bytes : READ_MAX_BYTES_DEFAULT;
  // Parité WebBridge (contrat §2) : `frame` (iframe ciblée) et `start`
  // (continuation) prennent le chemin dédié ci-dessous ; SANS eux, la
  // concaténation historique de toutes les frames reste INCHANGÉE.
  if (frame != null || start != null) {
    return lireEtendu(tab_id, frame, start, cap);
  }
  const injection = (cap) => {
    const full = document.body ? document.body.innerText : "";
    if (full.length <= cap) return full;
    return full.slice(0, cap) + "\n[…tronqué à " + cap + " octets]";
  };
  const results = await chrome.scripting.executeScript({
    target: { tabId: tab_id, allFrames: true },
    func: injection,
    args: [cap],
  });
  if (!Array.isArray(results) || results.length === 0) {
    throw new Error("read: executeScript n'a rien renvoyé");
  }
  const parts = results
    .map((r) => (r && typeof r.result === "string" ? r.result : ""))
    .filter((t) => t.trim().length > 0);
  let text = parts.join("\n\n");
  if (text.length > cap) {
    text = text.slice(0, cap) + "\n[…tronqué à " + cap + " octets]";
  }
  return { mode: "text", text, frames: parts.length };
}

/**
 * Cherche des éléments visibles par texte (v0.6.1) — le chaînon manquant
 * entre `read` (texte brut) et `click_native_text` (un seul élément).
 * Retourne une liste ordonnée avec géométrie viewport.
 */
async function cmdFind({ tab_id, text, selector, limit }) {
  const id = await resolveTabId(tab_id);
  const max = Math.min(Math.max(Number(limit) || 20, 1), 100);
  const injection = (txt, sel, max) => {
    const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
    // Auto-contenu : sérialisé dans chaque frame.
    const deepAll = (s) => {
      const out = [];
      const vus = new Set();
      const walk = (root) => {
        if (!root || !root.querySelectorAll) return;
        let matches = [];
        try { matches = root.querySelectorAll(s); } catch (_) { return; }
        for (const el of matches) if (!vus.has(el)) { vus.add(el); out.push(el); }
        for (const el of root.querySelectorAll("*")) if (el.shadowRoot) walk(el.shadowRoot);
      };
      walk(document);
      return out;
    };
    const base = sel
      ? deepAll(sel)
      : deepAll("a,button,input,textarea,select,[role=button],[role=link],[role=tab]");
    const out = [];
    for (const el of base) {
      const r = el.getBoundingClientRect();
      if (r.width <= 0 || r.height <= 0) continue;
      const t = norm(el.innerText || el.value || el.getAttribute("aria-label") || "");
      if (txt && !t.toLowerCase().includes(txt.toLowerCase())) continue;
      out.push({
        tag: el.tagName.toLowerCase(),
        text: t.slice(0, 120),
        x: Math.round(r.x),
        y: Math.round(r.y),
        w: Math.round(r.width),
        h: Math.round(r.height),
      });
      if (out.length >= max) break;
    }
    return out;
  };
  const results = await chrome.scripting.executeScript({
    target: { tabId: id, allFrames: true },
    func: injection,
    args: [typeof text === "string" ? text : "", typeof selector === "string" ? selector : "", max],
  });
  const elements = [];
  for (const res of results || []) {
    if (!res || !Array.isArray(res.result)) continue;
    const frame_id = typeof res.frameId === "number" ? res.frameId : 0;
    for (const el of res.result) {
      elements.push({ ...el, frame_id });
      if (elements.length >= max) break;
    }
    if (elements.length >= max) break;
  }
  return { find: text || selector || "", count: elements.length, elements, frames: (results || []).length };
}

/**
 * Exécute du JavaScript arbitraire dans l'onglet ciblé (monde MAIN :
 * accès au JS de la page, utile pour les SPA type Angular/GA4 que le
 * DOM cachait aux sélecteurs CSS). `code` est soit une expression
 * ("document.title"), soit une fonction sérialisée "(a) => a.x + 1"
 * recevant `arg` (JSON). Les promesses sont attendues ; le résultat
 * est assaini (structure-cloneable) avant remontée vers le démon.
 * Retourne { value } — null si le code n'a rien retourné.
 * v0.6.1 : `timeout_ms` paramétrable (défaut 8000, max 20000) — un script
 * multi-actions de 12 s n'est plus tué à 8 s.
 */
async function cmdEval({ tab_id, code, arg, timeout_ms }) {
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
  }, false, timeout_ms);
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
 *  pire cas ~16,5 s < 25 s : l'accusé de réception revient toujours.
 *  v0.5.4 : l'onglet est ACTIVÉ avant le geste — les Input.dispatch* pendent
 *  sur un onglet non actif (constat : « geste natif incomplet » en
 *  arrière-plan). L'activation reste INTERNE à la fenêtre Chrome : elle ne
 *  vole pas le focus de l'OS (la fenêtre peut rester derrière une autre).
 *  v0.6.0 : `focus` optionnel (défaut false) — amène AUSSI la fenêtre au
 *  premier plan de l'OS. Constat GA4 20/10 : les Input CDP gèlent quand la
 *  FENÊTRE entière est derrière une autre (rendu suspendu par Windows).
 *  Vol de focus explicite = sur demande seulement (l'opérateur travaille).
 *  v0.6.1 : `timeout_ms` optionnel — plafond CDP par appel (défaut 8 s,
 *  borné 1-20 s) et garde du geste à +5 s (max 24 s, sous le plafond 25 s).
 *  Un `eval` multi-actions de 12 s n'est plus tué à 8 s. */
async function withDebugger(tabId, fn, focus, timeout_ms) {
  try { await chrome.tabs.update(tabId, { active: true }); } catch (_) { /* fermé */ }
  if (focus) {
    try {
      const t = await chrome.tabs.get(tabId);
      const win = await chrome.windows.get(t.windowId);
      const maj = { focused: true };
      if (win.state === "minimized") maj.state = "maximized";
      await chrome.windows.update(t.windowId, maj);
    } catch (_) { /* fenêtre fermée */ }
  }
  await sleepNat(180); // laisser le rendu de l'activation
  // Session déjà ouverte par une capture réseau (contrat §6) : on la
  // RÉUTILISE. Sans ça, la première commande qui passe (clic, capture)
  // volait le debugger et cassait la capture en cours.
  const sessionPartagee = debuggerPersistant.has(tabId);
  if (!sessionPartagee) await chrome.debugger.attach({ tabId }, "1.3");
  const cdpMs = Math.min(Math.max(Number(timeout_ms) || 8000, 1000), 20000);
  const dbg = (method, params) => Promise.race([
    chrome.debugger.sendCommand({ tabId }, method, params),
    new Promise((_, rej) => setTimeout(() => rej(new Error(`CDP ${method} sans réponse (${cdpMs} ms)`)), cdpMs)),
  ]);
  try {
    const gesteMs = Number(timeout_ms) ? Math.min(cdpMs + 5000, 24000) : 15000;
    const garde = new Promise((_, rej) =>
      setTimeout(() => rej(new Error(`geste natif incomplet (${gesteMs} ms)`)), gesteMs));
    return await Promise.race([fn(dbg), garde]);
  } finally {
    // Session partagée : c'est la capture réseau qui la détachera.
    if (!sessionPartagee) {
      try {
        await Promise.race([
          chrome.debugger.detach({ tabId }),
          new Promise((r) => setTimeout(r, 1500)),
        ]);
      } catch (_) { /* déjà détaché */ }
    }
  }
}

/** Lecture seule de l'état de page (v0.6.1) — sert à vérifier qu'un geste
 *  natif a eu un effet observable (URL, scroll, DOM, focus, aria-*). */
async function lireEtatPage(tabId) {
  const [res] = await chrome.scripting.executeScript({
    target: { tabId },
    func: () => {
      const marqueurs = [...document.querySelectorAll("[aria-expanded],[aria-selected],[aria-checked]")]
        .slice(0, 300)
        .map((e) =>
          (e.getAttribute("aria-expanded") || "") +
          (e.getAttribute("aria-selected") || "") +
          (e.getAttribute("aria-checked") || ""))
        .join("|");
      return {
        url: location.href,
        scrollY: Math.round(window.scrollY),
        htmlLen: document.body ? document.body.innerHTML.length : 0,
        actif: document.activeElement
          ? document.activeElement.tagName + ":" + ((document.activeElement.innerText || "").trim().slice(0, 40))
          : "",
        marqueurs,
      };
    },
  });
  return res ? res.result : null;
}

/** Fraction gaussienne (Box-Muller) bornée [lo, hi] — jamais le centre exact. */
function fractionGaussienne(lo = 0.2, hi = 0.8) {
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  const g = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  return Math.min(hi, Math.max(lo, 0.5 + g * 0.15));
}

/** Trajectoire Bézier humaine (overshoot, bruit, vitesse en cloche) via
 *  humanMousePath — part du dernier point connu, jamais de téléportation.
 *  Les gestes CDP héritent ainsi de la même humanisation que les gestes JS,
 *  au lieu d'une interpolation linéaire en 3-8 points. */
async function deplacerCurseur(dbg, tabId, cible, jitter = 24) {
  const dep = nativeCursor.get(tabId) || { x: 8, y: Math.round(120 + Math.random() * 240) };
  const { points } = humanMousePath(dep, cible, { jitter });
  for (const p of points) {
    await dbg("Input.dispatchMouseEvent", { type: "mouseMoved", x: p.x, y: p.y });
    await sleepNat(p.delayMs);
  }
  nativeCursor.set(tabId, cible);
}

/** Lecture seule en JS : géométrie viewport d'un sélecteur, toutes
 *  frames et racines shadow ouvertes. Pour une iframe same-origin,
 *  l'offset de l'iframe est ajouté pour obtenir des coordonnées
 *  top-level utilisables par Input.dispatchMouseEvent. */
async function lireRect(id, selector) {
  const results = await chrome.scripting.executeScript({
    target: { tabId: id, allFrames: true },
    func: (sel) => {
      const deepAll = (s) => {
        const out = [];
        const vus = new Set();
        const walk = (root) => {
          if (!root || !root.querySelectorAll) return;
          let matches = [];
          try { matches = root.querySelectorAll(s); } catch (_) { return; }
          for (const el of matches) if (!vus.has(el)) { vus.add(el); out.push(el); }
          for (const el of root.querySelectorAll("*")) if (el.shadowRoot) walk(el.shadowRoot);
        };
        walk(document);
        return out;
      };
      const frameEl = window.frameElement;
      const cross = window !== window.top && !frameEl;
      const ox = frameEl ? frameEl.getBoundingClientRect().left + frameEl.clientLeft : 0;
      const oy = frameEl ? frameEl.getBoundingClientRect().top + frameEl.clientTop : 0;
      for (const el of deepAll(sel)) {
        const r = el.getBoundingClientRect();
        if (r.width <= 0 || r.height <= 0) continue;
        return {
          x: r.x + ox,
          y: r.y + oy,
          w: r.width,
          h: r.height,
          visible: true,
          cross_origin: cross,
          frame_url: location.href,
        };
      }
      return null;
    },
    args: [selector],
  });
  let fallback = null;
  for (const res of results || []) {
    const rect = res && res.result;
    if (!rect) continue;
    if (!rect.cross_origin) return rect;
    if (!fallback) fallback = rect;
  }
  return fallback;
}

/** Clic natif : trajectoire + hésitation + press/release (isTrusted=true).
 *  v0.6.1 : passe par le cœur partagé pour bénéficier de l'accusé vérifié. */
async function cmdClickNative({ tab_id, selector, focus, human_profile }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("click_native: 'selector' (string) requis");
  }
  const id = await resolveTabId(tab_id);
  const rect = await lireRect(id, selector);
  if (!rect) throw new Error("click_native: sélecteur introuvable: " + selector);
  if (!rect.visible) throw new Error("click_native: élément hors viewport (scroll_native d'abord)");
  if (rect.cross_origin) throw new Error("click_native: élément dans une iframe cross-origin (non supporté)");
  return clicNatifSurRect(id, rect, selector, focus, human_profile);
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

/** Frappe native caractère par caractère : keyDown(text) + keyUp, plan
 *  humain (délais lognormaux, typos, backspace) — jamais keyboard.press.
 *  `timeout_ms` optionnel : budget du geste (défaut calculé sur la
 *  longueur du texte, borné à 24 s par withDebugger). */
async function cmdTypeNative({ tab_id, selector, text, submit, timeout_ms, human_profile }) {
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
  // Budget temps : la frappe humaine coûte ~90-400 ms/caractère. Sans
  // budget explicite, la garde de 15 s coupait les textes > ~90 caractères.
  const budget = Number(timeout_ms) || Math.min(24000, Math.max(15000, 5000 + text.length * 90));
  return withDebugger(id, async (dbg) => {
    // Plan de frappe humain : délais lognormaux, typos QWERTY (~3 %) et
    // corrections backspace. Les gestes CDP ne sont plus une frappe
    // mécanique à dwell fixe.
    const profile = humanProfile(human_profile);
    const plan = humanTypingPlan(text, { typoRate: profile.typoRate });
    for (const step of plan) {
      if (step.type === "backspace") {
        await dbg("Input.dispatchKeyEvent", {
          type: "keyDown", key: "Backspace", code: "Backspace",
          windowsVirtualKeyCode: 8, nativeVirtualKeyCode: 8,
        });
        await sleepNat(25 + Math.random() * 35);
        await dbg("Input.dispatchKeyEvent", {
          type: "keyUp", key: "Backspace", code: "Backspace",
          windowsVirtualKeyCode: 8,
        });
      } else {
        await dbg("Input.dispatchKeyEvent", {
          type: "keyDown", key: step.ch, text: step.ch, unmodifiedText: step.ch,
          windowsVirtualKeyCode: vkDe(step.ch), nativeVirtualKeyCode: vkDe(step.ch),
        });
        await sleepNat(20 + Math.random() * 30);
        await dbg("Input.dispatchKeyEvent", {
          type: "keyUp", key: step.ch, windowsVirtualKeyCode: vkDe(step.ch),
        });
      }
      await sleepNat(step.delayMs);
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
  }, undefined, budget);
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
  // Plan de scroll humain : crans progressifs 80-300 px, accélération puis
  // freinage, délais 120-450 ms. Remplace la boucle mécanique 90-135 px.
  const plan = humanScrollPlan(Math.abs(total), { direction: sens > 0 ? "down" : "up" });
  // Budget temps proportionnel au plan (un cran = 120-450 ms).
  const budget = Math.min(24000, Math.max(15000, 5000 + plan.length * 450));
  return withDebugger(id, async (dbg) => {
    let crans = 0;
    for (const step of plan) {
      const p = nativeCursor.get(id) || { x: 400, y: 320 };
      if (!nativeCursor.has(id)) {
        await dbg("Input.dispatchMouseEvent", { type: "mouseMoved", x: p.x, y: p.y });
        await sleepNat(80 + Math.random() * 120);
        // Mémorise la position : sans ce set, le mouseMoved + son délai
        // étaient rejoués à CHAQUE cran et faisaient dépasser la garde.
        nativeCursor.set(id, p);
      }
      await dbg("Input.dispatchMouseEvent", {
        type: "mouseWheel", x: p.x, y: p.y, deltaX: 0,
        deltaY: step.direction === "up" ? -step.px : step.px,
      });
      crans += 1;
      await sleepNat(step.delayMs);
    }
    return { scrolled: total, crans, native: true };
  }, undefined, budget);
}

/** Trouve un élément PAR TEXTE (lecture seule) et renvoie son rect viewport,
 *  toutes frames et racines shadow ouvertes. Offset d'iframe same-origin
 *  ajouté pour un clic natif top-level. */
async function chercherRectTexte(id, texte, exact) {
  const results = await chrome.scripting.executeScript({
    target: { tabId: id, allFrames: true },
    func: (txt, ex) => {
      const normaliser = (s) => (s || "").replace(/\s+/g, " ").trim();
      const deepAll = (s) => {
        const out = [];
        const vus = new Set();
        const walk = (root) => {
          if (!root || !root.querySelectorAll) return;
          let matches = [];
          try { matches = root.querySelectorAll(s); } catch (_) { return; }
          for (const el of matches) if (!vus.has(el)) { vus.add(el); out.push(el); }
          for (const el of root.querySelectorAll("*")) if (el.shadowRoot) walk(el.shadowRoot);
        };
        walk(document);
        return out;
      };
      const frameEl = window.frameElement;
      const cross = window !== window.top && !frameEl;
      const ox = frameEl ? frameEl.getBoundingClientRect().left + frameEl.clientLeft : 0;
      const oy = frameEl ? frameEl.getBoundingClientRect().top + frameEl.clientTop : 0;
      const cibles = deepAll("a,button,[role=button],[role=tab],[role=link],input[type=submit],input[type=button]");
      const el = ex
        ? cibles.find((e) => normaliser(e.textContent) === txt)
        : cibles.find((e) => normaliser(e.textContent).startsWith(txt));
      if (!el) return null;
      const r = el.getBoundingClientRect();
      if (r.width <= 0 || r.height <= 0) return { introuvable: "hors viewport" };
      return {
        x: r.x + ox,
        y: r.y + oy,
        w: r.width,
        h: r.height,
        texte: normaliser(el.textContent).slice(0, 40),
        cross_origin: cross,
        frame_url: location.href,
      };
    },
    args: [texte, !!exact],
  });
  let fallback = null;
  for (const res of results || []) {
    const rect = res && res.result;
    if (!rect) continue;
    if (rect.introuvable) { if (!fallback) fallback = rect; continue; }
    if (!rect.cross_origin) return rect;
    if (!fallback) fallback = rect;
  }
  return fallback;
}

/** Clic natif sur un élément repéré PAR TEXTE (GA4/Angular : pas de
 *  sélecteur stable, mais un libellé). Préfixe par défaut, exact:true sinon. */
async function cmdClickNativeText({ tab_id, text, exact, focus, human_profile }) {
  if (typeof text !== "string" || !text) throw new Error("click_native_text: 'text' requis");
  const id = await resolveTabId(tab_id);
  const rect = await chercherRectTexte(id, text, exact);
  if (!rect) throw new Error("click_native_text: aucun élément portant le texte: " + text);
  if (rect.introuvable) throw new Error("click_native_text: élément « " + text + " » hors viewport (scroll_native d'abord)");
  if (rect.cross_origin) throw new Error("click_native_text: élément dans une iframe cross-origin (non supporté)");
  return clicNatifSurRect(id, rect, JSON.stringify(text), focus, human_profile);
}

/** Clic natif à des coordonnées viewport BRUTES — brique universelle :
 *  eval cherche par n'importe quelle logique, ce clic exécute. */
async function cmdClickNativeRect({ tab_id, x, y, focus, human_profile }) {
  const px = Math.round(Number(x));
  const py = Math.round(Number(y));
  if (!Number.isFinite(px) || !Number.isFinite(py)) throw new Error("click_native_rect: 'x'/'y' requis");
  const id = await resolveTabId(tab_id);
  return clicNatifSurRect(id, { x: px, y: py, w: 0, h: 0 }, px + "," + py, focus, human_profile);
}

/** Cœur partagé des clics natifs : gaussienne dans le rect (si surface),
 *  trajectoire, hésitation, press/release — isTrusted=true.
 *  v0.6.1 : accusé VÉRIFIÉ — l'état de page (URL, scroll, DOM, focus,
 *  aria-*) est capturé avant/après. `verified` dit si un effet a été
 *  observé, ce qui permet au client de ne pas rejouer un clic à l'aveugle. */
async function clicNatifSurRect(id, rect, libelle, focus, human_profile) {
  const profile = humanProfile(human_profile);
  let cible = {
    x: Math.round(rect.x + (rect.w > 8 ? rect.w * fractionGaussienne() : 0)),
    y: Math.round(rect.y + (rect.h > 8 ? rect.h * fractionGaussienne() : 0)),
  };
  // v0.6.2 — re-hit-test juste avant le press : la cible a pu bouger entre
  // la lecture du rect et le geste (montée d'un widget Turnstile qui décale
  // le formulaire, animation, sticky). Le clic frappait alors l'ancienne
  // position : « verified » restait vrai (le focus changeait) mais le
  // handler de la page ne se déclenchait jamais (constat 06/11/2026 sur
  // explodev.fr : submit register + confirmation newsletter). On relit
  // l'élément réellement sous le point ; s'il est interactif et que son
  // centre frais reste dans le rect d'origine, on clique CE centre —
  // jamais un overlay extérieur au rect (protection contre les bandeaux).
  const HOTE_ACTIF = ['BUTTON', 'A', 'INPUT', 'SELECT', 'TEXTAREA', 'SUMMARY', 'LABEL', 'SPAN'];
  try {
    const [hit] = await chrome.scripting.executeScript({
      target: { tabId: id },
      func: (x, y, rectOrig, hotes) => {
        const el = document.elementFromPoint(x, y);
        if (!el) return null;
        const r = el.getBoundingClientRect();
        const cx = Math.round(r.x + r.width / 2);
        const cy = Math.round(r.y + r.height / 2);
        const dansOrig = cx >= rectOrig.x - 6 && cx <= rectOrig.x + rectOrig.w + 6 &&
                         cy >= rectOrig.y - 6 && cy <= rectOrig.y + rectOrig.h + 6;
        let interactif = hotes.includes(el.tagName);
        let p = el.parentElement;
        for (let i = 0; p && i < 4 && !interactif; i++) { interactif = hotes.includes(p.tagName); p = p.parentElement; }
        return { tag: el.tagName, centre: { x: cx, y: cy }, dansOrig, interactif };
      },
      args: [cible.x, cible.y, { x: rect.x, y: rect.y, w: rect.w, h: rect.h }, HOTE_ACTIF],
    });
    const res = hit && hit.result;
    if (res && res.interactif && res.dansOrig) {
      cible = res.centre;
    }
  } catch (_) { /* hit-test indisponible : on conserve la cible initiale */ }
  const avant = await lireEtatPage(id);
  const rep = await withDebugger(id, async (dbg) => {
    await deplacerCurseur(dbg, id, cible, profile.jitter);
    await sleepNat(40 + Math.random() * 60);
    await dbg("Input.dispatchMouseEvent", { type: "mousePressed", x: cible.x, y: cible.y, button: "left", clickCount: 1 });
    await sleepNat(45 + Math.random() * 50);
    await dbg("Input.dispatchMouseEvent", { type: "mouseReleased", x: cible.x, y: cible.y, button: "left", clickCount: 1 });
    return { clicked: libelle, at: cible, native: true };
  }, focus);
  await sleepNat(450);
  const apres = await lireEtatPage(id);
  const change = !!(avant && apres) && (
    avant.url !== apres.url ||
    avant.scrollY !== apres.scrollY ||
    avant.htmlLen !== apres.htmlLen ||
    avant.actif !== apres.actif ||
    avant.marqueurs !== apres.marqueurs
  );
  return { ...rep, verified: change, verify_wait_ms: 450 };
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
async function cmdFill({ tab_id, selector, value, direct }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("fill: 'selector' (string) requis");
  }
  if (typeof value !== "string") {
    throw new Error("fill: 'value' (string) requis");
  }
  const real_tab_id = await resolveTabId(tab_id);

  // v0.6.1 : mode direct — une injection, setter natif + input/change,
  // pour les champs non sensibles (11 s → <0,3 s). Le mode humain reste
  // le défaut.
  if (direct === true) {
    const [res] = await chrome.scripting.executeScript({
      target: { tabId: real_tab_id },
      func: (sel, val) => {
        const el = document.querySelector(sel);
        if (!el) throw new Error("sélecteur introuvable: " + sel);
        el.scrollIntoView({ block: "center" });
        el.focus();
        const proto = el instanceof HTMLTextAreaElement
          ? HTMLTextAreaElement.prototype
          : HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, "value")?.set;
        if (setter) setter.call(el, val);
        else el.value = val;
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
        return { value: el.value, length: el.value.length };
      },
      args: [selector, value],
    });
    return { filled: selector, mode: "direct", ...(res && res.result ? res.result : {}) };
  }

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
async function cmdScreenshot({ tab_id, format, selector, full_page }) {
  const real_tab_id = await resolveTabId(tab_id);
  // Récupère le windowId via tabs.get (nécessaire par captureTabs).
  const tab = await chrome.tabs.get(real_tab_id);
  if (!tab || typeof tab.windowId !== "number") {
    throw new Error("screenshot: onglet introuvable ou sans windowId");
  }
  // Fix 21/10 (timeout 25 s constaté) : captureVisibleTab exige un rendu
  // VISIBLE — si la fenêtre est minimisée/occultée, la capture attend
  // indéfiniment et dépasse le timeout local (leçon l.1153 généralisée).
  // On restaure la fenêtre AVANT toute capture.
  try {
    const win = await chrome.windows.get(tab.windowId);
    if (win.state === "minimized" || win.state === "docked") {
      await chrome.windows.update(tab.windowId, { state: "maximized", focused: true });
      await new Promise((r) => setTimeout(r, 400));
    }
  } catch (_) { /* fenêtre fermée */ }
  // v0.6.0 : captureVisibleTab capture l'onglet ACTIF de la fenêtre —
  // MYSTÈRE RÉSOLU (20/10) : sans activation, on photographiait TOUJOURS
  // l'onglet actif (d'où les captures « figées », md5 identiques malgré
  // la navigation du bon onglet). On active l'onglet demandé, on attend
  // un rendu, on capture.
  if (tab.active !== true) {
    try { await chrome.tabs.update(real_tab_id, { active: true }); } catch (_) { /* fermé */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  const fmt = format === "jpeg" ? "jpeg" : "png";
  // Parité WebBridge (contrat §5) : `selector` (capture de l'élément) et
  // `full_page` (hauteur du document, plafond 20 000 px) passent par CDP.
  // `path` est IGNORÉ ici : l'extension n'a pas accès au disque, c'est le
  // démon qui décode le base64 et écrit le fichier.
  if ((typeof selector === "string" && selector) || full_page === true) {
    return captureCiblee(real_tab_id, fmt, selector, full_page === true);
  }
  // Fix 21/10 v1.0.2 : captureVisibleTab exige <all_urls> EFFECTIF (réglage
  // « accès aux sites » que Chrome réinitialise à chaque rechargement ↻) ou
  // activeTab (interaction). Fallback CDP : Page.captureScreenshot via le
  // debugger — permission "debugger" du manifest, TOUJOURS effective (le
  // read l'utilise déjà). Résultat identique (base64), zéro dépendance au
  // réglage d'accès.
  let data_url;
  try {
    data_url = await chrome.tabs.captureVisibleTab(tab.windowId, { format: fmt });
  } catch (err_visible) {
    const cdp = await withDebugger(real_tab_id, async (dbg) => {
      const r = await dbg("Page.captureScreenshot", { format: fmt });
      return r && r.data ? "data:image/" + fmt + ";base64," + r.data : null;
    });
    if (!cdp) {
      throw new Error("screenshot: échec captureVisibleTab (" + String(err_visible) + ") ET fallback CDP vide");
    }
    data_url = cdp;
  }
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

// ============================================================================
// PARITÉ WebBridge — contrat docs/protocol-parity-2026-11-06.md (2026-11-06)
//
// Commandes ajoutées côté extension SEULE (aucune nouvelle permission) :
//   snapshot, tabs_list, tab_attach, tab_close, session_close, send_keys,
//   network, pdf, upload, cdp — plus les extensions de read (§2) et de
//   screenshot (§5).
//
// Trois états vivent ici, tous rattachés à UN onglet et jamais partagés :
//   - snapshotState : refs @eN et frames #fN du DERNIER snapshot de l'onglet
//                     (contrat §1 : « un re-snapshot invalide les refs »).
//   - tabsRegistry  : quels onglets appartiennent à l'automatisation (donc
//                     fermables) et lesquels sont EMPRUNTÉS (jamais fermés
//                     par session_close) — contrat §3.
//   - capturesReseau: tampon borné (200 entrées) de la capture en cours — §6.
//
// Les deux premiers survivent à un réveil du service worker via
// chrome.storage.session (mémoire de session, effacée à la fermeture du
// navigateur), avec un cache mémoire pour éviter une lecture asynchrone à
// chaque commande.
// ============================================================================

// Bornes imposées par le contrat.
const SNAPSHOT_MAX_CHARS_DEFAULT = 60_000;
const SNAPSHOT_MAX_NODES = 4000;
const SNAPSHOT_MAX_VISITES = 20_000;
const NETWORK_MAX_ENTRIES = 200;
const NETWORK_BODY_MAX_BYTES = 256 * 1024;
const SCREENSHOT_FULL_PAGE_MAX_PX = 20_000;
const SESSION_DEFAUT = "default";

// ── Refs @eN / frames #fN (contrat §1) ──────────────────────────────────
//
// DÉCISION : une ref n'est pas un handle DOM (impossible à conserver dans
// un service worker) mais un CHEMIN CSS calculé pendant le snapshot.
// click / fill / wait réécrivent donc @eN → ce chemin AVANT d'agir : leur
// code natif reste intact. Aucun attribut n'est posé sur la page (doctrine
// du fichier : « le JS prépare, le natif agit » — pas de marqueur
// d'automatisation repérable par un site).
const snapshotState = new Map(); // tab_id → {ts, refs: {"@e1": {selector}}, frames}

/** Clé de stockage de session pour l'état de snapshot d'un onglet. */
function cleSnapshot(tabId) {
  return "dsh_snapshot_" + tabId;
}

/** État de snapshot d'un onglet (cache mémoire, puis storage.session). */
async function chargerSnapshot(tabId) {
  if (snapshotState.has(tabId)) return snapshotState.get(tabId);
  try {
    const cle = cleSnapshot(tabId);
    const lu = await chrome.storage.session.get(cle);
    const etat = lu ? lu[cle] : null;
    if (etat && etat.refs) {
      snapshotState.set(tabId, etat);
      return etat;
    }
  } catch (e) {
    log("snapshot: lecture storage.session impossible :", e && e.message);
  }
  return null;
}

/** Remplace la génération de refs de l'onglet (1 snapshot = 1 génération). */
async function enregistrerSnapshot(tabId, etat) {
  snapshotState.set(tabId, etat);
  try {
    const charge = {};
    charge[cleSnapshot(tabId)] = etat;
    await chrome.storage.session.set(charge);
  } catch (e) {
    log("snapshot: écriture storage.session impossible (cache mémoire seul) :", e && e.message);
  }
}

/** Traduit une ref @eN en sélecteur CSS ; renvoie tout le reste tel quel.
 *  Une ref inconnue lève « sélecteur introuvable: … » → 404 côté démon
 *  (contrat, règles communes). */
async function resoudreRef(tabId, selector) {
  if (typeof selector !== "string" || !selector.startsWith("@e")) return selector;
  const etat = await chargerSnapshot(tabId);
  const entree = etat && etat.refs ? etat.refs[selector] : null;
  if (!entree || !entree.selector) {
    throw new Error("sélecteur introuvable: " + selector + " (ref inconnue ou périmée — relancer snapshot)");
  }
  return entree.selector;
}

/** Normalise params.selector d'une commande HISTORIQUE (click/fill/wait)
 *  quand il porte une ref @eN. Sans ref, params est renvoyé TEL QUEL :
 *  aucun comportement existant n'est modifié. */
async function avecSelRefs(params) {
  if (!params || typeof params.selector !== "string" || !params.selector.startsWith("@e")) {
    return params;
  }
  const id = await resolveTabId(params.tab_id);
  return { ...params, selector: await resoudreRef(id, params.selector) };
}

/**
 * snapshot — arbre d'accessibilité construit depuis le DOM (contrat §1).
 *
 * Params : {tab_id?, max_chars?, ref?, cursor?}. max_chars borne le JSON
 * sérialisé ; cursor (notre ajout, pour consommer next_cursor) reprend la
 * fenêtre précédente. ref → sous-arbre, et les refs sont renumérotées :
 * un sous-arbre est un NOUVEAU snapshot, donc une nouvelle génération.
 *
 * Retour : {url, title, tree:[{ref, role, name, level?}], truncated,
 *           next_cursor, frames:[{id, index, selector, src, name, accessible}]}.
 *
 * DÉCISIONS : arbre PLAT (forme de l'exemple du contrat) ; seuls les nœuds
 * visibles ET nommés sont émis ; light DOM seulement (un chemin CSS à
 * travers un shadow root ne serait pas résolvable par click/fill) ; les
 * iframes ne sont pas fusionnées dans l'arbre, elles sont listées à part
 * dans frames (index → sélecteur), comme le demande le contrat §2.
 */
async function cmdSnapshot({ tab_id, max_chars, ref, cursor }) {
  const id = await resolveTabId(tab_id);
  const cap = Number(max_chars) > 0
    ? Math.min(Math.floor(Number(max_chars)), 500_000)
    : SNAPSHOT_MAX_CHARS_DEFAULT;
  const depuis = Math.max(0, Math.floor(Number(cursor) || 0));
  let racine = null;
  if (ref != null && String(ref).trim() !== "") {
    const demande = String(ref).trim();
    if (!demande.startsWith("@e")) {
      throw new Error("sélecteur introuvable: " + demande + " (ref attendue : @eN)");
    }
    racine = await resoudreRef(id, demande);
  }
  const injection = (racineSel, maxNodes, maxVisites) => {
    const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
    // Balises sans rôle ni contenu utile : on ne descend pas dedans.
    const IGNORES = {
      script: 1, style: 1, noscript: 1, template: 1, head: 1, meta: 1, link: 1,
      title: 1, base: 1, iframe: 1, frame: 1, frameset: 1, object: 1, embed: 1,
      source: 1, track: 1, param: 1,
    };
    // Rôles de CONTENEUR : leur nom accessible ne peut pas être leur texte
    // visible (sinon le nom d'une navigation = tout son menu).
    const SANS_TEXTE = {
      navigation: 1, main: 1, banner: 1, contentinfo: 1, complementary: 1,
      form: 1, region: 1, article: 1, list: 1, rowgroup: 1, table: 1,
      group: 1, dialog: 1, listbox: 1, menu: 1, menubar: 1, tablist: 1,
      toolbar: 1, tree: 1, grid: 1,
    };
    const roleImplicite = (el, tag) => {
      const type = String(el.getAttribute("type") || "text").toLowerCase();
      switch (tag) {
        case "a":
        case "area":
          return el.hasAttribute("href") ? "link" : null;
        case "button":
        case "summary":
          return "button";
        case "input":
          if (type === "hidden") return null;
          if (type === "checkbox") return "checkbox";
          if (type === "radio") return "radio";
          if (type === "range") return "slider";
          if (type === "number") return "spinbutton";
          if (type === "search") return "searchbox";
          if (type === "email" || type === "tel" || type === "url" ||
              type === "text" || type === "password") return "textbox";
          return "button";
        case "select":
          return el.multiple || el.size > 1 ? "listbox" : "combobox";
        case "textarea":
          return "textbox";
        case "h1": case "h2": case "h3": case "h4": case "h5": case "h6":
          return "heading";
        case "ul": case "ol": case "menu":
          return "list";
        case "li":
          return "listitem";
        case "nav":
          return "navigation";
        case "main":
          return "main";
        case "header":
          return "banner";
        case "footer":
          return "contentinfo";
        case "aside":
          return "complementary";
        case "form":
          return "form";
        case "article":
          return "article";
        case "section":
          return el.hasAttribute("aria-label") || el.hasAttribute("aria-labelledby") ? "region" : null;
        case "table":
          return "table";
        case "thead": case "tbody": case "tfoot":
          return "rowgroup";
        case "tr":
          return "row";
        case "td":
          return "cell";
        case "th":
          return "columnheader";
        case "caption":
          return "caption";
        case "img":
          return el.hasAttribute("alt") ? "img" : null;
        case "dialog":
          return "dialog";
        case "details":
        case "fieldset":
          return "group";
        case "progress":
          return "progressbar";
        case "option":
          return "option";
        case "optgroup":
          return "group";
        default:
          return null;
      }
    };
    const niveau = (el, role, tag, listes) => {
      const aria = parseInt(el.getAttribute("aria-level"), 10);
      if (Number.isFinite(aria) && aria > 0) return aria;
      if (role === "heading") {
        const m = /^h([1-6])$/.exec(tag);
        return m ? parseInt(m[1], 10) : 2;
      }
      if (role === "list") return listes + 1;
      if (role === "listitem") return Math.max(1, listes);
      return null;
    };
    const nomDe = (el, role, tag) => {
      const borne = (s) => s.slice(0, 200);
      const aria = norm(el.getAttribute("aria-label"));
      if (aria) return borne(aria);
      const labelledby = norm(el.getAttribute("aria-labelledby"));
      if (labelledby) {
        const texte = norm(labelledby.split(" ").map((rid) => {
          const noeud = document.getElementById(rid);
          return noeud ? (noeud.innerText || noeud.textContent || "") : "";
        }).join(" "));
        if (texte) return borne(texte);
      }
      if (tag === "img" || tag === "area") {
        const alt = norm(el.getAttribute("alt"));
        if (alt) return borne(alt);
      }
      const type = String(el.getAttribute("type") || "").toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select") {
        const placeholder = norm(el.getAttribute("placeholder"));
        if (placeholder) return borne(placeholder);
        const etiquette = el.labels && el.labels.length ? norm(el.labels[0].textContent || "") : "";
        if (etiquette) return borne(etiquette);
        const titreChamp = norm(el.getAttribute("title"));
        if (titreChamp) return borne(titreChamp);
        // Jamais la valeur d'un champ sensible dans l'arbre.
        if (type === "password") return "";
        if (typeof el.value === "string" && el.value.trim()) return borne(norm(el.value));
        return "";
      }
      if (SANS_TEXTE[role]) {
        const titre = norm(el.getAttribute("title"));
        return titre ? borne(titre) : "";
      }
      const texte = norm(el.innerText || el.textContent || "");
      if (texte) return borne(texte);
      const titre = norm(el.getAttribute("title"));
      return titre ? borne(titre) : "";
    };
    // Visibilité RÉELLE (display:none hérité, visibility, aria-hidden).
    const visible = (el) => {
      try {
        if (typeof el.checkVisibility === "function") {
          return el.checkVisibility({ checkOpacity: false, checkVisibilityCSS: true });
        }
      } catch (_) { /* API absente : repli ci-dessous */ }
      const st = getComputedStyle(el);
      if (st.display === "none" || st.visibility === "hidden" || st.visibility === "collapse") {
        return false;
      }
      return el.getClientRects().length > 0;
    };
    // Chemin CSS résolvable par document.querySelector : ancré sur le
    // premier id simple rencontré, sinon tag + :nth-of-type.
    const chemin = (el) => {
      const morceaux = [];
      let courant = el;
      let garde = 0;
      while (courant && courant.nodeType === 1 && garde < 8) {
        const id = courant.getAttribute("id") || "";
        if (id && /^[A-Za-z][A-Za-z0-9_-]*$/.test(id)) {
          morceaux.unshift("#" + id);
          return morceaux.join(" > ");
        }
        let sel = courant.tagName.toLowerCase();
        const parent = courant.parentElement;
        if (parent) {
          const freres = parent.children;
          let rang = 0;
          let total = 0;
          for (let i = 0; i < freres.length; i++) {
            if (freres[i].tagName === courant.tagName) {
              total += 1;
              if (freres[i] === courant) rang = total;
            }
          }
          if (total > 1) sel += ":nth-of-type(" + rang + ")";
        }
        morceaux.unshift(sel);
        courant = parent;
        garde += 1;
      }
      return morceaux.join(" > ");
    };
    const noeuds = [];
    let visites = 0;
    let coupe = false;
    const parcourir = (el, listes, ariaCache) => {
      if (!el || coupe || el.nodeType !== 1) return;
      if (noeuds.length >= maxNodes || visites >= maxVisites) {
        coupe = true;
        return;
      }
      const tag = el.tagName.toLowerCase();
      if (IGNORES[tag]) return;
      const ariaHidden = ariaCache || el.getAttribute("aria-hidden") === "true";
      if (ariaHidden) return; // sous-arbre hors arbre d'accessibilité
      visites += 1;
      if (!visible(el)) return;
      const roleBrut = norm(el.getAttribute("role")).split(" ")[0].toLowerCase();
      const role = roleBrut === "presentation" || roleBrut === "none"
        ? null
        : (roleBrut || roleImplicite(el, tag));
      let listesEnfants = listes;
      if (role) {
        const nom = nomDe(el, role, tag);
        if (nom) {
          noeuds.push({
            ref: "@e" + (noeuds.length + 1),
            role: role,
            name: nom,
            level: niveau(el, role, tag, listes),
            selector: chemin(el),
          });
        }
        if (role === "list") listesEnfants = listes + 1;
      }
      for (let i = 0; i < el.children.length && !coupe; i++) {
        parcourir(el.children[i], listesEnfants, ariaHidden);
      }
    };
    const depart = racineSel
      ? document.querySelector(racineSel)
      : (document.body || document.documentElement);
    if (!depart) {
      return { introuvable: true, url: location.href, title: document.title || "" };
    }
    parcourir(depart, 0, false);
    // Iframes accessibles du document principal (contrat §2 : index → handle).
    const frames = [];
    try {
      const liste = document.querySelectorAll("iframe, frame");
      for (let i = 0; i < liste.length; i++) {
        const el = liste[i];
        let accessible = false;
        try { accessible = !!el.contentDocument; } catch (_) { accessible = false; }
        frames.push({
          id: "#f" + (i + 1),
          index: i + 1,
          selector: chemin(el),
          src: el.getAttribute("src") || "",
          name: norm(el.getAttribute("name") || el.getAttribute("title") || ""),
          accessible: accessible,
        });
      }
    } catch (_) { /* document sans iframe : liste vide */ }
    return {
      url: location.href,
      title: document.title || "",
      nodes: noeuds,
      frames: frames,
      truncated: coupe,
    };
  };
  const [res] = await chrome.scripting.executeScript({
    target: { tabId: id },
    func: injection,
    args: [racine, SNAPSHOT_MAX_NODES, SNAPSHOT_MAX_VISITES],
  });
  const brut = res && res.result;
  if (!brut) throw new Error("snapshot: executeScript n'a rien renvoyé");
  if (brut.introuvable) {
    throw new Error("sélecteur introuvable: " + racine + " (arbre modifié depuis le snapshot)");
  }
  const noeuds = Array.isArray(brut.nodes) ? brut.nodes : [];
  const refs = {};
  for (const n of noeuds) {
    if (n && n.ref && n.selector) refs[n.ref] = { selector: n.selector };
  }
  const total = noeuds.length;
  const debut = Math.min(depuis, total);
  const arbre = [];
  let utilise = 0;
  let coupe = brut.truncated === true;
  let i = debut;
  for (; i < total; i++) {
    const n = noeuds[i];
    const rendu = { ref: n.ref, role: n.role, name: n.name };
    if (n.level != null) rendu.level = n.level;
    const taille = JSON.stringify(rendu).length + 1;
    if (utilise + taille > cap) {
      coupe = true;
      break;
    }
    utilise += taille;
    arbre.push(rendu);
  }
  const suivant = i < total ? String(i) : null;
  await enregistrerSnapshot(id, {
    ts: Date.now(),
    refs: refs,
    frames: Array.isArray(brut.frames) ? brut.frames : [],
  });
  return {
    url: brut.url || "",
    title: brut.title || "",
    tree: arbre,
    truncated: coupe,
    next_cursor: suivant,
    frames: Array.isArray(brut.frames) ? brut.frames : [],
  };
}

/**
 * read étendu (contrat §2) : iframe ciblée (#fN ou sélecteur CSS) et/ou
 * continuation (start). Retour {text, complete, next_start, frame}.
 *
 * Le chemin historique de cmdRead (concaténation de toutes les frames)
 * n'est utilisé que si NI frame NI start ne sont fournis : zéro régression.
 * Sans frame mais AVEC start, on pagine le DOCUMENT PRINCIPAL (le seul dont
 * les offsets restent stables d'un appel au suivant).
 */
async function lireEtendu(tabId, frame, start, cap) {
  const offset = Math.max(0, Math.floor(Number(start) || 0));
  const demande = frame != null ? String(frame).trim() : "";
  let selFrame = null;
  if (demande) {
    if (demande.startsWith("#f")) {
      const etat = await chargerSnapshot(tabId);
      const liste = etat && Array.isArray(etat.frames) ? etat.frames : [];
      const entree = liste.find((f) => f && f.id === demande);
      if (!entree) {
        throw new Error("sélecteur introuvable: " + demande + " (frame inconnue — relancer snapshot)");
      }
      if (entree.accessible === false) {
        throw new Error("read: frame " + demande + " inaccessible depuis l'extension (cross-origin)");
      }
      selFrame = entree.selector;
    } else {
      selFrame = demande;
    }
  }
  const injection = (sel, off, limite) => {
    if (sel) {
      // La bonne frame est celle dont frameElement correspond au sélecteur.
      // Une frame cross-origin n'a pas de frameElement : elle ne matchera
      // jamais, d'où l'erreur « sélecteur introuvable » explicite en sortie.
      const fe = window.frameElement;
      if (!fe) return null;
      let correspond = false;
      try { correspond = fe.matches(sel); } catch (_) { correspond = false; }
      if (!correspond) return null;
    } else if (window !== window.top) {
      return null; // sans frame : document principal uniquement
    }
    const plein = document.body ? document.body.innerText : "";
    const texte = plein.slice(off, off + limite);
    const suite = off + texte.length;
    return {
      text: texte,
      complete: suite >= plein.length,
      next_start: suite >= plein.length ? null : suite,
      length: plein.length,
      href: location.href,
    };
  };
  const results = await chrome.scripting.executeScript({
    target: selFrame ? { tabId: tabId, allFrames: true } : { tabId: tabId },
    func: injection,
    args: [selFrame, offset, cap],
  });
  let cible = null;
  for (const r of results || []) {
    if (r && r.result && typeof r.result.text === "string") {
      cible = r.result;
      break;
    }
  }
  if (!cible) {
    if (selFrame) throw new Error("sélecteur introuvable: " + (demande || selFrame));
    throw new Error("read: executeScript n'a rien renvoyé");
  }
  return {
    text: cible.text,
    complete: cible.complete === true,
    next_start: cible.complete === true ? null : cible.next_start,
    frame: demande || null,
  };
}

// ── Cycle de vie des onglets et des sessions (contrat §3) ──────────────
//
// DÉCISION : un onglet n'est fermable que s'il a été CRÉÉ par
// l'automatisation. tab_attach sur un onglet préexistant (celui que
// l'utilisateur avait ouvert) le marque EMPRUNTÉ : il devient l'onglet
// courant de la tâche, mais session_close ne le ferme JAMAIS. Un onglet
// absent du registre est traité comme emprunté (défaut sûr).
// La propriété est COLLANTE : un onglet créé par nous ne redevient jamais
// emprunté par simple réutilisation.
const tabsRegistry = new Map(); // tab_id → {session, borrowed, created_by, ts}
const CLE_REGISTRE = "dsh_tabs_registry";
let registreCharge = false;

async function chargerRegistre() {
  if (registreCharge) return tabsRegistry;
  registreCharge = true;
  try {
    const lu = await chrome.storage.session.get(CLE_REGISTRE);
    const liste = lu && Array.isArray(lu[CLE_REGISTRE]) ? lu[CLE_REGISTRE] : [];
    for (const e of liste) {
      if (e && typeof e.tab_id === "number") tabsRegistry.set(e.tab_id, e);
    }
  } catch (e) {
    log("onglets: lecture du registre impossible :", e && e.message);
  }
  return tabsRegistry;
}

async function sauverRegistre() {
  try {
    const charge = {};
    charge[CLE_REGISTRE] = [...tabsRegistry.values()];
    await chrome.storage.session.set(charge);
  } catch (e) {
    log("onglets: écriture du registre impossible :", e && e.message);
  }
}

/** Enregistre (ou met à jour) un onglet dans le registre de propriété. */
async function marquerOnglet(tabId, options) {
  const choix = options || {};
  const registre = await chargerRegistre();
  const ancien = registre.get(tabId) || {};
  const dejaPossede = ancien.borrowed === false;
  const session = (typeof choix.session === "string" && choix.session.trim())
    ? choix.session.trim()
    : (ancien.session || SESSION_DEFAUT);
  const entree = {
    tab_id: tabId,
    session: session,
    borrowed: dejaPossede ? false : choix.borrowed === true,
    created_by: choix.created_by || ancien.created_by || null,
    ts: Date.now(),
  };
  registre.set(tabId, entree);
  await sauverRegistre();
  return entree;
}

/** tabs_create / goto : note la propriété de l'onglet renvoyé.
 *  reused === false → créé par nous (fermable) ; reused === true → il
 *  préexistait (emprunté) ; reused absent (goto sur un tab_id fourni) →
 *  on ne touche pas au registre : l'onglet reste ce qu'il était. */
async function noterOngletCree(resultat, params) {
  if (!resultat || typeof resultat.tab_id !== "number" || resultat.reused === undefined) {
    return resultat;
  }
  await marquerOnglet(resultat.tab_id, {
    session: params && typeof params.session === "string" ? params.session : null,
    borrowed: resultat.reused === true,
    created_by: "tabs_create",
  });
  return resultat;
}

/** tabs_list — onglets de la fenêtre courante (ou de toutes avec
 *  all_windows). borrowed : onglet préexistant, jamais fermé par
 *  session_close ; owned : créé par l'automatisation. */
async function cmdTabsList({ all_windows }) {
  const registre = await chargerRegistre();
  const onglets = await chrome.tabs.query(all_windows === true ? {} : { currentWindow: true });
  const titresGroupes = new Map();
  const liste = [];
  for (const t of onglets) {
    const reg = registre.get(t.id);
    const entree = {
      tab_id: t.id,
      window_id: t.windowId,
      url: t.url || t.pendingUrl || "",
      title: t.title || "",
      active: t.active === true,
      borrowed: !(reg && reg.borrowed === false),
      owned: !!(reg && reg.borrowed === false),
    };
    if (reg && reg.session) entree.session = reg.session;
    const gid = typeof t.groupId === "number" ? t.groupId : -1;
    if (gid >= 0) {
      if (!titresGroupes.has(gid)) {
        let titre = "";
        try {
          const groupe = await chrome.tabGroups.get(gid);
          titre = (groupe && groupe.title) || "";
        } catch (_) { titre = ""; }
        titresGroupes.set(gid, titre);
      }
      const titreGroupe = titresGroupes.get(gid);
      if (titreGroupe) entree.group_title = titreGroupe;
    }
    liste.push(entree);
  }
  return { tabs: liste, count: liste.length, all_windows: all_windows === true };
}

/** tab_attach — l'onglet devient l'onglet courant de la tâche. */
async function cmdTabAttach({ tab_id, session }) {
  if (tab_id == null) throw new Error("tab_attach: 'tab_id' (entier) requis");
  let tab;
  try {
    tab = await chrome.tabs.get(tab_id);
  } catch (e) {
    throw new Error("tab_attach: onglet introuvable: " + tab_id);
  }
  const registre = await chargerRegistre();
  const ancien = registre.get(tab_id);
  // Créé par nous → reste à nous ; préexistant → EMPRUNTÉ (jamais fermé).
  const entree = await marquerOnglet(tab_id, {
    session: typeof session === "string" ? session : null,
    borrowed: !(ancien && ancien.borrowed === false),
    created_by: "tab_attach",
  });
  await chrome.tabs.update(tab_id, { active: true });
  return {
    tab_id: tab_id,
    url: tab.url || "",
    title: tab.title || "",
    borrowed: entree.borrowed === true,
    owned: entree.borrowed === false,
    session: entree.session,
    window_id: tab.windowId,
  };
}

/** tab_close — fermeture explicite d'UN onglet, emprunté ou non : c'est le
 *  client qui le demande nommément, on ne devine pas à sa place. */
async function cmdTabClose({ tab_id }) {
  if (tab_id == null) throw new Error("tab_close: 'tab_id' (entier) requis");
  const registre = await chargerRegistre();
  try {
    await chrome.tabs.remove(tab_id);
  } catch (e) {
    throw new Error("tab_close: fermeture impossible (onglet introuvable ou déjà fermé): " + (e && e.message));
  }
  if (registre.delete(tab_id)) await sauverRegistre();
  return { closed: 1 };
}

/** session_close — ferme les onglets OUVERTS PAR l'automatisation.
 *  DÉCISION : le démon est mono-tenant ; un nom de session inconnu du
 *  registre est traité comme un simple libellé et on ferme alors la session
 *  "default" (où vivent les onglets créés sans nom). Les onglets EMPRUNTÉS
 *  ne sont jamais fermés, quel que soit le nom demandé. */
async function cmdSessionClose({ session }) {
  const registre = await chargerRegistre();
  const nom = typeof session === "string" && session.trim() ? session.trim() : "";
  const possedes = [...registre.entries()].filter(([, r]) => r && r.borrowed === false);
  const nomConnu = nom !== "" && possedes.some(([, r]) => r.session === nom);
  const cibles = possedes
    .filter(([, r]) => !nom || r.session === nom || !nomConnu)
    .map(([id]) => id);
  const vivants = [];
  for (const id of cibles) {
    try {
      await chrome.tabs.get(id);
      vivants.push(id);
    } catch (_) {
      registre.delete(id); // déjà fermé à la main : rien à fermer
    }
  }
  let fermes = 0;
  if (vivants.length > 0) {
    try {
      await chrome.tabs.remove(vivants);
      fermes = vivants.length;
    } catch (e) {
      for (const id of vivants) {
        try {
          await chrome.tabs.remove(id);
          fermes += 1;
        } catch (_) { /* déjà fermé */ }
      }
    }
    for (const id of vivants) registre.delete(id);
  }
  await sauverRegistre();
  const empruntes = [...registre.values()].filter((r) => r && r.borrowed === true).length;
  return { closed: fermes, session: nom || null, kept_borrowed: empruntes };
}

/** Capture CDP d'un élément ou de la page entière (contrat §5).
 *  Le démon écrit le fichier à partir du base64 : l'extension n'a jamais
 *  accès au disque, donc path n'est même pas un paramètre ici. */
async function captureCiblee(tabId, fmt, selector, fullPage) {
  let clip = null;
  if (typeof selector === "string" && selector) {
    const sel = await resoudreRef(tabId, selector);
    const [res] = await chrome.scripting.executeScript({
      target: { tabId: tabId },
      func: (s) => {
        const el = document.querySelector(s);
        if (!el) return null;
        try { el.scrollIntoView({ block: "nearest", inline: "nearest" }); } catch (_) {}
        const r = el.getBoundingClientRect();
        return {
          x: r.x + window.scrollX,
          y: r.y + window.scrollY,
          width: r.width,
          height: r.height,
        };
      },
      args: [sel],
    });
    const rect = res && res.result;
    if (!rect) throw new Error("sélecteur introuvable: " + sel);
    if (!(rect.width > 0) || !(rect.height > 0)) {
      throw new Error("screenshot: élément de taille nulle (invisible ?): " + sel);
    }
    clip = {
      x: rect.x,
      y: rect.y,
      width: rect.width,
      height: Math.min(rect.height, SCREENSHOT_FULL_PAGE_MAX_PX),
      scale: 1,
    };
  }
  return withDebugger(tabId, async (dbg) => {
    const params = { format: fmt, captureBeyondViewport: true };
    if (fmt === "jpeg") params.quality = 85;
    if (clip) {
      params.clip = clip;
    } else if (fullPage) {
      const metriques = await dbg("Page.getLayoutMetrics", {});
      const taille = (metriques && (metriques.cssContentSize || metriques.contentSize)) || {};
      const largeur = Math.max(1, Math.ceil(Number(taille.width) || 0));
      const hauteur = Math.min(
        Math.max(1, Math.ceil(Number(taille.height) || 0)),
        SCREENSHOT_FULL_PAGE_MAX_PX,
      );
      params.clip = { x: 0, y: 0, width: largeur, height: hauteur, scale: 1 };
    }
    const rep = await dbg("Page.captureScreenshot", params);
    if (!rep || typeof rep.data !== "string" || !rep.data) {
      throw new Error("screenshot: Page.captureScreenshot n'a rien renvoyé");
    }
    return { format: fmt, data_url: "data:image/" + fmt + ";base64," + rep.data };
  }, false, 15000);
}

// ── send_keys : clavier natif (contrat §4) ─────────────────────────────
//
// Une combinaison est envoyée via Input.dispatchKeyEvent (isTrusted=true) :
// keyDown/rawKeyDown puis keyUp, avec le bitmask de modificateurs CDP
// (1=Alt, 2=Ctrl, 4=Meta, 8=Shift). Mod = Ctrl hors macOS (contrat §4).
// Le résultat n'est PAS cru sur parole : l'état de page (URL, focus, valeur,
// sélection, DOM, scroll) est comparé avant/après → verified.

/** Système hôte, vu du service worker (pas d'API dédiée : userAgent). */
function osCourant() {
  let ua = "";
  try { ua = navigator.userAgent || ""; } catch (_) { ua = ""; }
  if (/Windows/i.test(ua)) return "windows";
  if (/Mac OS X|Macintosh|iPhone|iPad/i.test(ua)) return "macos";
  return "linux";
}

/** Touches nommées → {key, code, vk, text?} (jeu minimal du contrat). */
const TOUCHES_CDP = {
  enter: { key: "Enter", code: "Enter", vk: 13 },
  return: { key: "Enter", code: "Enter", vk: 13 },
  escape: { key: "Escape", code: "Escape", vk: 27 },
  esc: { key: "Escape", code: "Escape", vk: 27 },
  tab: { key: "Tab", code: "Tab", vk: 9 },
  backspace: { key: "Backspace", code: "Backspace", vk: 8 },
  delete: { key: "Delete", code: "Delete", vk: 46 },
  space: { key: " ", code: "Space", vk: 32, text: " " },
  arrowup: { key: "ArrowUp", code: "ArrowUp", vk: 38 },
  arrowdown: { key: "ArrowDown", code: "ArrowDown", vk: 40 },
  arrowleft: { key: "ArrowLeft", code: "ArrowLeft", vk: 37 },
  arrowright: { key: "ArrowRight", code: "ArrowRight", vk: 39 },
  home: { key: "Home", code: "Home", vk: 36 },
  end: { key: "End", code: "End", vk: 35 },
  pageup: { key: "PageUp", code: "PageUp", vk: 33 },
  pagedown: { key: "PageDown", code: "PageDown", vk: 34 },
  insert: { key: "Insert", code: "Insert", vk: 45 },
  f1: { key: "F1", code: "F1", vk: 112 },
  f2: { key: "F2", code: "F2", vk: 113 },
  f3: { key: "F3", code: "F3", vk: 114 },
  f4: { key: "F4", code: "F4", vk: 115 },
  f5: { key: "F5", code: "F5", vk: 116 },
  f6: { key: "F6", code: "F6", vk: 117 },
  f7: { key: "F7", code: "F7", vk: 118 },
  f8: { key: "F8", code: "F8", vk: 119 },
  f9: { key: "F9", code: "F9", vk: 120 },
  f10: { key: "F10", code: "F10", vk: 121 },
  f11: { key: "F11", code: "F11", vk: 122 },
  f12: { key: "F12", code: "F12", vk: 123 },
};

/** Analyse "Mod+A" / "Shift+Tab" / "Enter" → touche + bitmask CDP. */
function analyserTouches(keys, os) {
  const jetons = String(keys).split("+").map((t) => t.trim()).filter(Boolean);
  if (jetons.length === 0) throw new Error("send_keys: 'keys' vide");
  const brut = jetons.pop().toLowerCase();
  let modifiers = 0;
  for (const modificateur of jetons) {
    const m = modificateur.toLowerCase();
    if (m === "mod") {
      // Mod = Ctrl hors macOS, Cmd sur macOS (contrat §4).
      modifiers |= os === "macos" ? 4 : 2;
    } else if (m === "control" || m === "ctrl") {
      modifiers |= 2;
    } else if (m === "cmd" || m === "command" || m === "meta") {
      modifiers |= 4;
    } else if (m === "alt" || m === "option") {
      modifiers |= 1;
    } else if (m === "shift") {
      modifiers |= 8;
    } else {
      throw new Error("send_keys: modificateur inconnu: " + modificateur);
    }
  }
  if (TOUCHES_CDP[brut]) return { ...TOUCHES_CDP[brut], modifiers: modifiers };
  if (Array.from(brut).length === 1) {
    const ch = brut;
    let vk = 0;
    let code = "";
    if (ch >= "a" && ch <= "z") {
      vk = ch.toUpperCase().charCodeAt(0);
      code = "Key" + ch.toUpperCase();
    } else if (ch >= "0" && ch <= "9") {
      vk = ch.charCodeAt(0);
      code = "Digit" + ch;
    }
    // Un raccourci ne PRODUIT pas de texte (Ctrl+A n'écrit pas « a »).
    const raccourci = (modifiers & 1) !== 0 || (modifiers & 2) !== 0 || (modifiers & 4) !== 0;
    return { key: ch, code: code, vk: vk, text: raccourci ? "" : ch, modifiers: modifiers };
  }
  throw new Error("send_keys: touche non gérée: " + brut);
}

/** État de page utile pour vérifier qu'un raccourci a eu un effet. */
async function lireEtatClavier(tabId) {
  try {
    const [res] = await chrome.scripting.executeScript({
      target: { tabId: tabId },
      func: () => {
        const el = document.activeElement;
        let selection = 0;
        try {
          selection = window.getSelection() ? window.getSelection().toString().length : 0;
        } catch (_) { selection = 0; }
        return {
          url: location.href,
          active: el ? el.tagName + ":" + String(el.innerText || el.value || "").slice(0, 40) : "",
          valeur: el && typeof el.value === "string" ? el.value.length : -1,
          selection: selection,
          htmlLen: document.body ? document.body.innerHTML.length : 0,
          scrollY: Math.round(window.scrollY),
        };
      },
    });
    return res ? res.result : null;
  } catch (e) {
    log("send_keys: lecture d'état impossible :", e && e.message);
    return null;
  }
}

/** Vrai si l'état de page a bougé entre deux lectures. */
function etatClavierChange(avant, apres) {
  if (!avant || !apres) return false;
  return avant.url !== apres.url ||
    avant.active !== apres.active ||
    avant.valeur !== apres.valeur ||
    avant.selection !== apres.selection ||
    avant.htmlLen !== apres.htmlLen ||
    avant.scrollY !== apres.scrollY;
}

/** send_keys — {keys, tab_id?, selector?, focus_emulation?, timeout_ms?}.
 *  focus_emulation : Emulation.setFocusEmulationEnabled activé puis
 *  TOUJOURS désactivé (finally), pour un onglet en arrière-plan. */
async function cmdSendKeys({ tab_id, keys, selector, focus_emulation, timeout_ms }) {
  if (typeof keys !== "string" || !keys.trim()) {
    throw new Error("send_keys: 'keys' (string) requis");
  }
  const os = osCourant();
  const touche = analyserTouches(keys, os);
  const id = await resolveTabId(tab_id);
  if (typeof selector === "string" && selector) {
    const sel = await resoudreRef(id, selector);
    // Préparation d'ÉTAT (focus), pas un geste : le natif agit ensuite.
    const [prep] = await chrome.scripting.executeScript({
      target: { tabId: id },
      func: (s) => {
        const el = document.querySelector(s);
        if (!el) return null;
        try { el.scrollIntoView({ block: "center" }); } catch (_) {}
        try { el.focus(); } catch (_) {}
        return true;
      },
      args: [sel],
    });
    if (!prep || !prep.result) throw new Error("sélecteur introuvable: " + sel);
  }
  const avant = await lireEtatClavier(id);
  const rep = await withDebugger(id, async (dbg) => {
    let emule = false;
    try {
      if (focus_emulation === true) {
        try {
          await dbg("Emulation.setFocusEmulationEnabled", { enabled: true });
          emule = true;
        } catch (e) {
          log("send_keys: focus emulation indisponible :", e && e.message);
        }
      }
      const base = {
        key: touche.key,
        code: touche.code,
        windowsVirtualKeyCode: touche.vk,
        nativeVirtualKeyCode: touche.vk,
        modifiers: touche.modifiers,
      };
      if (touche.text) {
        await dbg("Input.dispatchKeyEvent", { ...base, type: "keyDown", text: touche.text, unmodifiedText: touche.text });
      } else {
        await dbg("Input.dispatchKeyEvent", { ...base, type: "rawKeyDown" });
      }
      await sleepNat(30 + Math.random() * 40);
      await dbg("Input.dispatchKeyEvent", { ...base, type: "keyUp" });
      return { dispatched: true, emule: emule };
    } finally {
      if (emule) {
        try {
          await dbg("Emulation.setFocusEmulationEnabled", { enabled: false });
        } catch (_) { /* session détachée */ }
      }
    }
  }, false, timeout_ms);
  await sleepNat(250);
  const apres = await lireEtatClavier(id);
  const reponse = {
    dispatched: rep.dispatched === true,
    os: os,
    keys: keys,
    verified: etatClavierChange(avant, apres),
  };
  if (focus_emulation === true) reponse.focus_emulation = rep.emule === true;
  return reponse;
}

// ── network : capture par onglet (contrat §6) ───────────────────────────
//
// DÉCISION : la capture tient une session debugger PERSISTANTE sur l'onglet
// (Network.enable + événements Network.*), parce que les corps de réponse
// n'existent que par CDP. Cette session est déclarée dans
// debuggerPersistant : withDebugger la RÉUTILISE au lieu de la voler, donc
// les clics/captures continuent de fonctionner pendant une capture.
// Le tampon est borné à 200 entrées, les corps à 256 Ko, et ils sont
// JETÉS à l'arrêt de la capture (le contrat l'exige).
const capturesReseau = new Map(); // tab_id → {actif, depuis, entries, vues}

function captureReseau(tabId) {
  let c = capturesReseau.get(tabId);
  if (!c) {
    c = { actif: false, depuis: 0, entries: [], vues: new Map() };
    capturesReseau.set(tabId, c);
  }
  return c;
}

/** Entrée de list : strictement les 6 champs du contrat. */
function entreeReseauPublique(e) {
  return {
    request_id: e.request_id,
    url: e.url,
    method: e.method,
    status: e.status,
    mime_type: e.mime_type,
    completed: e.completed === true,
  };
}

/** En-têtes CDP (objet) → [{name, value}] : un nom peut se répéter
 *  (Set-Cookie), un objet JSON perdrait les doublons. */
function entetesReseau(entetes) {
  const out = [];
  if (!entetes) return out;
  for (const nom of Object.keys(entetes)) {
    out.push({ name: nom, value: String(entetes[nom]) });
  }
  return out;
}

/** Tronque un corps à 256 Ko (base64 : multiple de 4 pour rester valide). */
function bornerCorps(corps, base64) {
  const s = String(corps == null ? "" : corps);
  if (s.length <= NETWORK_BODY_MAX_BYTES) return { corps: s, tronque: false };
  let coupe = s.slice(0, NETWORK_BODY_MAX_BYTES);
  if (base64 === true) coupe = coupe.slice(0, Math.floor(coupe.length / 4) * 4);
  return { corps: coupe, tronque: true };
}

/** Empile une entrée en respectant la borne de 200 (FIFO). */
function ajouterEntreeReseau(c, entree) {
  c.entries.push(entree);
  c.vues.set(entree.request_id, entree);
  while (c.entries.length > NETWORK_MAX_ENTRIES) {
    const vieille = c.entries.shift();
    if (vieille) c.vues.delete(vieille.request_id);
    log("network: tampon plein (200 entrées), la plus ancienne est perdue");
  }
}

async function demarrerCaptureReseau(tabId) {
  const c = captureReseau(tabId);
  if (c.actif) {
    return { tab_id: tabId, capturing: true, started: false, count: c.entries.length };
  }
  c.entries = [];
  c.vues = new Map();
  try {
    await chrome.debugger.attach({ tabId: tabId }, "1.3");
    debuggerPersistant.add(tabId);
  } catch (e) {
    debuggerPersistant.delete(tabId);
    throw new Error("network: attache du debugger impossible (" + (e && e.message) + ")");
  }
  try {
    await chrome.debugger.sendCommand({ tabId: tabId }, "Network.enable", {});
  } catch (e) {
    debuggerPersistant.delete(tabId);
    try { await chrome.debugger.detach({ tabId: tabId }); } catch (_) { /* déjà détaché */ }
    throw new Error("network: Network.enable a échoué (" + (e && e.message) + ")");
  }
  c.actif = true;
  c.depuis = Date.now();
  return { tab_id: tabId, capturing: true, started: true, max_entries: NETWORK_MAX_ENTRIES };
}

async function arreterCaptureReseau(tabId) {
  const c = capturesReseau.get(tabId);
  if (c) {
    c.actif = false;
    for (const e of c.entries) {
      e.body = null;
      e.request_body = null;
      e.body_base64 = false;
      e.body_truncated = false;
      if (!e.body_error) e.body_error = "capture arrêtée";
    }
  }
  let detache = false;
  if (debuggerPersistant.has(tabId)) {
    debuggerPersistant.delete(tabId);
    try {
      await chrome.debugger.detach({ tabId: tabId });
      detache = true;
    } catch (_) { /* déjà détaché */ }
  }
  return {
    tab_id: tabId,
    capturing: false,
    stopped: true,
    detached: detache,
    count: c ? c.entries.length : 0,
  };
}

function listerReseau(tabId, filtre) {
  const c = capturesReseau.get(tabId);
  if (!c) return { tab_id: tabId, capturing: false, count: 0, requests: [] };
  const f = typeof filtre === "string" ? filtre : "";
  const requests = [];
  for (const e of c.entries) {
    if (f && !(e.url || "").includes(f)) continue;
    requests.push(entreeReseauPublique(e));
  }
  return { tab_id: tabId, capturing: c.actif === true, count: requests.length, requests: requests };
}

function detailReseau(tabId, requestId) {
  if (typeof requestId !== "string" || !requestId) {
    throw new Error("network: 'request_id' (string) requis pour op=detail");
  }
  const c = capturesReseau.get(tabId);
  const e = c ? c.vues.get(requestId) : null;
  if (!e) throw new Error("network: request_id inconnu: " + requestId);
  const out = entreeReseauPublique(e);
  out.request_headers = e.request_headers || [];
  out.response_headers = e.response_headers || [];
  if (e.request_body != null) out.request_body = e.request_body;
  if (e.error) out.error = e.error;
  if (e.body != null) out.body = e.body;
  if (e.body_base64 === true) out.body_base64 = true;
  if (e.body_truncated === true) out.body_truncated = true;
  if (out.body == null) out.body_error = e.body_error || "corps indisponible";
  return out;
}

/** network — {op: start|stop|list|detail, tab_id?, filter?, request_id?}. */
async function cmdNetwork({ op, tab_id, filter, request_id }) {
  const operation = typeof op === "string" ? op.trim().toLowerCase() : "";
  const id = await resolveTabId(tab_id);
  switch (operation) {
    case "start":
      return demarrerCaptureReseau(id);
    case "stop":
      return arreterCaptureReseau(id);
    case "list":
      return listerReseau(id, filter);
    case "detail":
      return detailReseau(id, request_id);
    default:
      throw new Error("network: 'op' inconnu (attendu start|stop|list|detail): " + String(op));
  }
}

/** Événements CDP de la capture : métadonnées puis corps des réponses.
 *  Listener SYNCHRONE enregistré au chargement du worker (exigence MV3) ;
 *  la récupération du corps est asynchrone et ignorée si la capture
 *  s'arrête entre-temps. */
chrome.debugger.onEvent.addListener((source, method, params) => {
  const tabId = source && typeof source.tabId === "number" ? source.tabId : null;
  if (tabId == null || !params) return;
  const c = capturesReseau.get(tabId);
  if (!c || c.actif !== true) return;
  if (method === "Network.requestWillBeSent") {
    const req = params.request || {};
    const existante = c.vues.get(params.requestId);
    if (existante) {
      // Redirection : même request_id, nouvelle URL.
      existante.url = req.url || existante.url;
      existante.method = req.method || existante.method;
      existante.request_headers = entetesReseau(req.headers);
      return;
    }
    const entree = {
      request_id: params.requestId,
      url: req.url || "",
      method: req.method || "",
      status: null,
      mime_type: "",
      completed: false,
      request_headers: entetesReseau(req.headers),
      response_headers: [],
      request_body: null,
      body: null,
      body_base64: false,
      body_error: null,
      body_truncated: false,
      error: null,
    };
    if (typeof req.postData === "string" && req.postData.length > 0) {
      entree.request_body = bornerCorps(req.postData, false).corps;
    }
    ajouterEntreeReseau(c, entree);
    return;
  }
  if (method === "Network.responseReceived") {
    const entree = c.vues.get(params.requestId);
    if (!entree) return;
    const rep = params.response || {};
    entree.status = typeof rep.status === "number" ? rep.status : null;
    entree.mime_type = rep.mimeType || "";
    entree.response_headers = entetesReseau(rep.headers);
    return;
  }
  if (method === "Network.loadingFinished") {
    const entree = c.vues.get(params.requestId);
    if (!entree) return;
    entree.completed = true;
    chrome.debugger
      .sendCommand({ tabId: tabId }, "Network.getResponseBody", { requestId: params.requestId })
      .then((rep) => {
        if (!rep || c.actif !== true) return;
        const base64 = rep.base64Encoded === true;
        const borne = bornerCorps(rep.body, base64);
        entree.body = borne.corps;
        entree.body_base64 = base64;
        entree.body_truncated = borne.tronque;
        entree.body_error = null;
      })
      .catch((err) => {
        entree.body_error = "corps indisponible (" + (err && err.message ? err.message : String(err)) + ")";
      });
    return;
  }
  if (method === "Network.loadingFailed") {
    const entree = c.vues.get(params.requestId);
    if (!entree) return;
    entree.completed = true;
    entree.error = params.errorText || "échec réseau";
    if (!entree.body_error) entree.body_error = "requête échouée: " + entree.error;
  }
});

/** Détachement subi (onglet fermé, DevTools ouvert, Chrome) : on remet
 *  l'état à zéro pour que withDebugger réattache normalement. */
chrome.debugger.onDetach.addListener((source) => {
  const tabId = source && typeof source.tabId === "number" ? source.tabId : null;
  if (tabId == null) return;
  debuggerPersistant.delete(tabId);
  const c = capturesReseau.get(tabId);
  if (c) {
    c.actif = false;
    for (const e of c.entries) {
      e.body = null;
      e.request_body = null;
    }
  }
});

/** Onglet fermé : aucun état fantôme (refs, capture, registre, propriété). */
chrome.tabs.onRemoved.addListener((tabId) => {
  snapshotState.delete(tabId);
  debuggerPersistant.delete(tabId);
  capturesReseau.delete(tabId);
  if (tabsRegistry.delete(tabId)) sauverRegistre();
  try {
    chrome.storage.session.remove(cleSnapshot(tabId));
  } catch (_) { /* storage.session indisponible */ }
});

// ── pdf : export via Page.printToPDF (contrat §7) ──────────────────────
const FORMATS_PDF = {
  a3: { paperWidth: 11.7, paperHeight: 16.54 },
  a4: { paperWidth: 8.27, paperHeight: 11.69 },
  a5: { paperWidth: 5.83, paperHeight: 8.27 },
  letter: { paperWidth: 8.5, paperHeight: 11 },
  legal: { paperWidth: 8.5, paperHeight: 14 },
  tabloid: { paperWidth: 11, paperHeight: 17 },
};

/** pdf — {tab_id?, paper_format?, landscape?, scale?, print_background?}.
 *  Retour {b64, page_title} ; le démon écrit le fichier si path est fourni.
 *  Budget CDP 18 s (sous le plafond local de 25 s) : un printToPDF de page
 *  lourde dépasse les 8 s par défaut de withDebugger. */
async function cmdPdf({ tab_id, paper_format, landscape, scale, print_background }) {
  const id = await resolveTabId(tab_id);
  const format = String(paper_format || "a4").trim().toLowerCase();
  const params = {
    printBackground: print_background !== false,
    landscape: landscape === true,
    displayHeaderFooter: false,
    scale: Math.min(Math.max(Number(scale) || 1, 0.1), 2),
  };
  if (FORMATS_PDF[format]) {
    params.paperWidth = FORMATS_PDF[format].paperWidth;
    params.paperHeight = FORMATS_PDF[format].paperHeight;
  } else if (format !== "auto") {
    throw new Error("pdf: 'paper_format' non supporté: " + paper_format);
  }
  let titre = "";
  try {
    const tab = await chrome.tabs.get(id);
    titre = tab.title || "";
  } catch (_) { titre = ""; }
  const b64 = await withDebugger(id, async (dbg) => {
    const rep = await dbg("Page.printToPDF", params);
    if (!rep || typeof rep.data !== "string" || !rep.data) {
      throw new Error("pdf: Page.printToPDF n'a rien renvoyé");
    }
    return rep.data;
  }, false, 18000);
  return { b64: b64, page_title: titre };
}

// ── upload : fichier dans un input[type=file] (contrat §8) ─────────────
/** upload — {tab_id?, selector, files:[chemins absolus]}.
 *  DOM.setFileInputFiles via debugger. Un chemin relatif est refusé AVANT
 *  l'appel (Chrome ne peut pas l'ouvrir) ; un chemin illisible côté Chrome
 *  remonte son erreur. Le retour est VÉRIFIÉ sur l'input après injection
 *  ({file_count, names} réellement portés), pas sur la demande. */
async function cmdUpload({ tab_id, selector, files }) {
  if (typeof selector !== "string" || !selector) {
    throw new Error("upload: 'selector' (string) requis");
  }
  if (!Array.isArray(files) || files.length === 0) {
    throw new Error("upload: 'files' (liste de chemins absolus) requis");
  }
  const chemins = files.map((f) => String(f).trim()).filter((f) => f.length > 0);
  if (chemins.length === 0) throw new Error("upload: 'files' vide");
  const relatif = chemins.find((c) => !c.startsWith("/") && !/^[A-Za-z]:[\\/]/.test(c));
  if (relatif) {
    throw new Error("upload: chemin non absolu (illisible depuis Chrome): " + relatif);
  }
  const id = await resolveTabId(tab_id);
  const sel = await resoudreRef(id, selector);
  const [prep] = await chrome.scripting.executeScript({
    target: { tabId: id },
    func: (s) => {
      const el = document.querySelector(s);
      if (!el) return null;
      try { el.scrollIntoView({ block: "center" }); } catch (_) {}
      try { el.focus(); } catch (_) {}
      return el.tagName.toLowerCase();
    },
    args: [sel],
  });
  if (!prep || !prep.result) throw new Error("sélecteur introuvable: " + sel);
  return withDebugger(id, async (dbg) => {
    await dbg("DOM.enable", {});
    const doc = await dbg("DOM.getDocument", { depth: 0 });
    if (!doc || !doc.root) throw new Error("upload: DOM.getDocument n'a rien renvoyé");
    const noeud = await dbg("DOM.querySelector", { nodeId: doc.root.nodeId, selector: sel });
    if (!noeud || !noeud.nodeId) throw new Error("sélecteur introuvable: " + sel);
    try {
      await dbg("DOM.setFileInputFiles", { nodeId: noeud.nodeId, files: chemins });
    } catch (e) {
      const detail = e && e.message ? e.message : String(e);
      throw new Error("upload: fichier illisible ou champ non-fichier (" + detail + ")");
    }
    const [verif] = await chrome.scripting.executeScript({
      target: { tabId: id },
      func: (s) => {
        const el = document.querySelector(s);
        if (!el || !el.files) return null;
        return {
          file_count: el.files.length,
          names: Array.from(el.files).map((f) => f.name),
        };
      },
      args: [sel],
    });
    const vu = verif && verif.result;
    return {
      file_count: vu && typeof vu.file_count === "number" ? vu.file_count : chemins.length,
      names: vu && Array.isArray(vu.names)
        ? vu.names
        : chemins.map((c) => c.split(/[\\/]/).pop()),
    };
  }, false, 15000);
}

// ── cdp : passe-plat debugger sous LISTE BLANCHE (contrat §9) ──────────
//
// Page.* sauf Page.navigate (le pilotage de navigation passe par goto),
// Network.*, DOM.*, Runtime.evaluate, Emulation.*, Accessibility.*.
// Tout le reste → erreur cdp_method_not_allowed (message exact, 502).
const METHODES_CDP_AUTORISEES = [
  /^Page\.(?!navigate$)[A-Za-z][A-Za-z0-9]*$/,
  /^Network\.[A-Za-z][A-Za-z0-9]*$/,
  /^DOM\.[A-Za-z][A-Za-z0-9]*$/,
  /^Runtime\.evaluate$/,
  /^Emulation\.[A-Za-z][A-Za-z0-9]*$/,
  /^Accessibility\.[A-Za-z][A-Za-z0-9]*$/,
];

function methodeCdpAutorisee(methode) {
  if (typeof methode !== "string" || !methode) return false;
  return METHODES_CDP_AUTORISEES.some((re) => re.test(methode));
}

/** cdp — {tab_id?, method, params?, timeout_ms?} → {method, result}. */
async function cmdCdp({ tab_id, method, params, timeout_ms }) {
  if (!methodeCdpAutorisee(method)) {
    throw new Error("cdp_method_not_allowed");
  }
  const id = await resolveTabId(tab_id);
  const retour = await withDebugger(id, async (dbg) => {
    const args = params && typeof params === "object" && !Array.isArray(params) ? params : {};
    const rep = await dbg(method, args);
    return rep === undefined ? null : rep;
  }, false, timeout_ms);
  return { method: method, result: retour };
}

// Démarrage initial : on tente une connexion tout de suite. Si le démon est
// down, le backoff s'enclenche.
log("service worker démarré");
connect();