// test-ws-client.js — Vérification hors-navigateur du contrat WS.
//
// Ce script NE teste PAS Chrome : il simule exactement les messages que
// produit `background.js` à partir de `chrome.webRequest.onCompleted` et
// vérifie que le démon `dsh-eyes` (Task 1-4) les accepte et répond
// `{"ok":true}`.
//
// C'est une preuve de conformité du format JSON du client vis-à-vis du
// serveur réel. Le test de bout en bout (Chrome chargeant l'extension et
// navigant) reste manuel (étape 6 du plan).
//
// Usage :
//   1. Démarrer le démon dans un autre terminal :
//        cd ~/projets/dsh-eyes && cargo run --release
//   2. Puis :
//        cd ~/projets/dsh-eyes/extension
//        node test-ws-client.js
//
// Dépendance : `ws` (paquet npm). Installer avec :
//   npm install ws     (à la racine du dépôt ou dans extension/)
//
// Le script échoue avec exit code != 0 sur la première incohérence.

const WebSocket = require("ws");

const WS_URL = "ws://127.0.0.1:8445/ws";

// Cas de test : exactement la même forme de payload que `buildEvent()` dans
// background.js. On couvre :
//   - un événement "standard" avec content-length
//   - un événement sans content-length (bytes omis, Option<u64> côté Rust)
//   - un événement sur localhost (qui doit déclencher le vigil "critical"
//     mais rester accepté côté ingestion)
const events = [
  {
    label: "standard (content-length=1234)",
    payload: {
      ts: Date.now(),
      domain: "example.com",
      url: "https://example.com/path?q=1",
      kind: "request",
      bytes: 1234,
    },
    expectedOk: true,
  },
  {
    label: "sans content-length (bytes omis)",
    payload: {
      ts: Date.now() + 1,
      domain: "other.org",
      url: "https://other.org/",
      kind: "request",
    },
    expectedOk: true,
  },
  {
    label: "localhost (vigil critical attendu, ingestion OK)",
    payload: {
      ts: Date.now() + 2,
      domain: "127.0.0.1",
      url: "http://127.0.0.1:8445/health",
      kind: "request",
      bytes: 18,
    },
    expectedOk: true,
  },
];

let failures = 0;

function fail(label, msg) {
  failures += 1;
  console.error(`✗ ${label} — ${msg}`);
}

function ok(label) {
  console.log(`✓ ${label}`);
}

const ws = new WebSocket(WS_URL);
let received = 0;

ws.on("open", () => {
  console.log(`connecté à ${WS_URL}`);
  for (const ev of events) {
    console.log(`envoi (${ev.label}): ${JSON.stringify(ev.payload)}`);
    ws.send(JSON.stringify(ev.payload));
  }
});

ws.on("message", (data) => {
  received += 1;
  const idx = received - 1;
  const ev = events[idx];
  let parsed = null;
  try {
    parsed = JSON.parse(data.toString("utf8"));
  } catch (e) {
    fail(ev.label, `réponse non-JSON : ${data}`);
    return finish();
  }
  if (parsed.ok !== ev.expectedOk) {
    fail(ev.label, `attendu ok=${ev.expectedOk}, reçu ${JSON.stringify(parsed)}`);
  } else {
    ok(`${ev.label} → ${JSON.stringify(parsed)}`);
  }
  if (received === events.length) finish();
});

ws.on("error", (err) => {
  fail("connexion", `erreur WS : ${err && err.message ? err.message : err}`);
  // On termine ici aussi — le démon est probablement down.
  process.exit(2);
});

ws.on("close", (code, reason) => {
  if (received < events.length) {
    fail("connexion", `WS fermée prématurément (code=${code}, reason=${reason || "(vide)"}), ${received}/${events.length} acquittements reçus`);
    process.exit(2);
  }
});

function finish() {
  try { ws.close(); } catch (_) { /* noop */ }
  if (failures === 0) {
    console.log(`\n✔ ${events.length}/${events.length} acquittements conformes (ack {"ok":true}).`);
    process.exit(0);
  } else {
    console.error(`\n✘ ${failures}/${events.length} échec(s).`);
    process.exit(1);
  }
}