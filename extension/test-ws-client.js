// test-ws-client.js — Vérification hors-navigateur du contrat WS (REWORK).
//
// Ce script simule le comportement de l'extension DSH Eyes (background.js)
// sur la WS `ws://127.0.0.1:8445/ws`. Il permet deux modes de test :
//
// 1. Mode "legacy" (option `--legacy`) : envoie 3 événements de
//    navigation comme dans la version d'origine, et vérifie que le
//    démon répond `{"ok":true}` à chaque. Sert à valider la
//    non-régression de l'ingestion legacy.
//
// 2. Mode "command-loop" (défaut) : se connecte, signale sa présence
//    (envoie 1 événement factice), puis BOUCLE : pour chaque commande
//    JSON reçue du démon (`{cmd_id, cmd, params}`), il répond
//    `{"cmd_id":N,"ok":true,"result":{...echo...}}`. S'arrête après N
//    réponses ou sur signal SIGINT. C'est ce mode qui prouve le
//    cycle `démon → commande → extension → réponse` sans dépendre
//    d'un vrai Chrome.
//
// Usage :
//   # Prérequis : daemon démarré
//   node test-ws-client.js                 # command-loop, 1 commande puis exit
//   node test-ws-client.js --count 5       # command-loop, 5 commandes
//   node test-ws-client.js --legacy        # mode legacy (3 événements)
//   node test-ws-client.js --result '{"foo":"bar"}'  # result custom
//
// Exit code 0 si toutes les interactions sont conformes, 1 sinon.

const WebSocket = require("ws");

const WS_URL = process.env.DSH_EYES_WS || "ws://127.0.0.1:8445/ws";

// Parse args simples (pas de dépendance yargs).
const args = process.argv.slice(2);
const isLegacy = args.includes("--legacy");
const countIdx = args.indexOf("--count");
const count = countIdx >= 0 ? parseInt(args[countIdx + 1], 10) : 1;
const resultIdx = args.indexOf("--result");
const customResult = resultIdx >= 0 ? args[resultIdx + 1] : null;

let failures = 0;
function fail(label, msg) {
  failures += 1;
  console.error(`✗ ${label} — ${msg}`);
}
function ok(label) {
  console.log(`✓ ${label}`);
}

if (isLegacy) {
  runLegacy();
} else {
  runCommandLoop();
}

// =============================================================================
// Mode legacy : on envoie 3 événements de navigation.
// =============================================================================
function runLegacy() {
  const events = [
    {
      label: "standard (content-length=1234)",
      payload: { ts: Date.now(), domain: "example.com", url: "https://example.com/path?q=1", kind: "request", bytes: 1234 },
      expectedOk: true,
    },
    {
      label: "sans content-length (bytes omis)",
      payload: { ts: Date.now() + 1, domain: "other.org", url: "https://other.org/", kind: "request" },
      expectedOk: true,
    },
    {
      label: "localhost (vigil critical)",
      payload: { ts: Date.now() + 2, domain: "127.0.0.1", url: "http://127.0.0.1:8445/health", kind: "request", bytes: 18 },
      expectedOk: true,
    },
  ];

  const ws = new WebSocket(WS_URL);
  let received = 0;
  ws.on("open", () => {
    console.log(`connecté à ${WS_URL} (mode legacy)`);
    for (const ev of events) {
      console.log(`envoi (${ev.label}): ${JSON.stringify(ev.payload)}`);
      ws.send(JSON.stringify(ev.payload));
    }
  });
  ws.on("message", (data) => {
    received += 1;
    const ev = events[received - 1];
    let parsed = null;
    try { parsed = JSON.parse(data.toString("utf8")); }
    catch (e) { fail(ev.label, `réponse non-JSON : ${data}`); return finish(ws); }
    if (parsed.ok !== ev.expectedOk) {
      fail(ev.label, `attendu ok=${ev.expectedOk}, reçu ${JSON.stringify(parsed)}`);
    } else { ok(`${ev.label} → ${JSON.stringify(parsed)}`); }
    if (received === events.length) finish(ws);
  });
  ws.on("error", (err) => { fail("connexion", `erreur WS : ${err && err.message ? err.message : err}`); process.exit(2); });
  ws.on("close", (code, reason) => {
    if (received < events.length) { fail("connexion", `WS fermée prématurément (${received}/${events.length})`); process.exit(2); }
  });
  function finish(ws) {
    try { ws.close(); } catch (_) {}
    if (failures === 0) console.log(`\n✔ ${events.length}/${events.length} acquittements conformes.`);
    else console.error(`\n✘ ${failures}/${events.length} échec(s).`);
    process.exit(failures === 0 ? 0 : 1);
  }
}

// =============================================================================
// Mode command-loop : on simule l'extension bidirectionnelle.
//   1. Connexion WS.
//   2. Envoi d'un événement factice (pour confirmer qu'on est enregistré).
//   3. Boucle : pour chaque commande reçue, on répond ok=true avec un
//      result qui « echo » les params (preuve qu'on a bien lu la
//      commande, et que le routeur HTTP la récupère).
//   4. Quitte après `count` réponses ou sur SIGINT.
// =============================================================================
function runCommandLoop() {
  console.log(`mode command-loop : cible ${WS_URL}, ${count} commande(s) attendue(s)`);
  const ws = new WebSocket(WS_URL);
  let ackReceived = 0;
  let commandsReceived = 0;
  let cmdIds = new Set();
  const startMs = Date.now();

  // Heartbeat pour éviter que le test attende indéfiniment si le
  // démon n'envoie pas de commande (timeout 35 s).
  const HEARTBEAT_MS = 35_000;

  const heartbeat = setTimeout(() => {
    fail("command-loop", `aucune commande reçue en ${HEARTBEAT_MS} ms — le démon a-t-il été déclenché ?`);
    try { ws.close(); } catch (_) {}
    process.exit(1);
  }, HEARTBEAT_MS);

  ws.on("open", () => {
    console.log(`connecté à ${WS_URL}`);
    const hello = {
      ts: Date.now(),
      domain: "fake.test",
      url: "http://fake.test/hello",
      kind: "request",
    };
    ws.send(JSON.stringify(hello));
    console.log("hello envoyé (événement factice)");
  });

  ws.on("message", (data) => {
    let parsed = null;
    try { parsed = JSON.parse(data.toString("utf8")); }
    catch (e) {
      fail("message", `non-JSON : ${data}`);
      return;
    }

    // Ack legacy d'un événement qu'on a envoyé.
    if (parsed && typeof parsed === "object" && "ok" in parsed) {
      ackReceived += 1;
      ok(`ack #${ackReceived} reçu : ${JSON.stringify(parsed)}`);
      return;
    }

    // Commande du démon.
    if (parsed && typeof parsed.cmd_id === "number" && typeof parsed.cmd === "string") {
      commandsReceived += 1;
      const cid = parsed.cmd_id;
      cmdIds.add(cid);
      console.log(`commande #${cid} reçue : ${parsed.cmd} ${JSON.stringify(parsed.params)}`);

      let result;
      if (customResult) {
        try { result = JSON.parse(customResult); }
        catch (_) { result = { raw: customResult }; }
      } else {
        result = {
          echoed_cmd: parsed.cmd,
          echoed_params: parsed.params,
          received_at_ms: Date.now() - startMs,
        };
      }
      const response = { cmd_id: cid, ok: true, result };
      ws.send(JSON.stringify(response));
      console.log(`réponse envoyée : ${JSON.stringify(response)}`);

      if (commandsReceived >= count) {
        clearTimeout(heartbeat);
        setTimeout(() => {
          try { ws.close(); } catch (_) {}
          if (cmdIds.size === count) {
            ok(`cmd_id tous distincts sur ${count} commande(s) : [${[...cmdIds].sort().join(", ")}]`);
            console.log(`\n✔ ${count}/${count} commande(s) traitées, ${ackReceived} ack legacy.`);
            process.exit(0);
          } else {
            fail("cmd_id", `doublons détectés : ${[...cmdIds].length} uniques pour ${count} commandes`);
            process.exit(1);
          }
        }, 100);
      }
      return;
    }

    fail("message", `forme inattendue : ${JSON.stringify(parsed)}`);
  });

  ws.on("error", (err) => {
    clearTimeout(heartbeat);
    fail("connexion", `erreur WS : ${err && err.message ? err.message : err}`);
    process.exit(2);
  });

  ws.on("close", (code, reason) => {
    clearTimeout(heartbeat);
    if (commandsReceived < count) {
      fail("command-loop", `WS fermée après seulement ${commandsReceived}/${count} commande(s) (code=${code}, reason=${reason || "(vide)"})`);
      process.exit(2);
    }
  });
}
