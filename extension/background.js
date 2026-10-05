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
    // Le serveur répond {"ok":true} sur événement valide, {"ok":false,"error":...}
    // sur erreur. On logge les erreurs pour aider au debug (pas critique pour
    // la stabilité — la WS reste ouverte côté client).
    let payload = null;
    try {
      payload = JSON.parse(ev.data);
    } catch (_) { /* message non-JSON, on ignore */ }
    if (payload && payload.ok === false) {
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
    host = new URL(details.url).host;
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

// Démarrage initial : on tente une connexion tout de suite. Si le démon est
// down, le backoff s'enclenche.
log("service worker démarré");
connect();