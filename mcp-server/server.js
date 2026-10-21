#!/usr/bin/env node
// dsh-eyes MCP — expose le harnais dsh-eyes (démon local 127.0.0.1:8445)
// comme outils MCP. L'extension exécute les commandes DANS le Chrome
// personnel d'Eddie (ses sessions, ses mots de passe).
// Protocole humain : EDDIE tape identifiants/captchas ; stop immédiat si
// anti-bot. (Créé le 21/10/2026 à la demande d'Eddie : « dsh-eyes devrait
// avoir son MCP connecté à VS Code ».)

const fs = require("fs");
const os = require("os");
const path = require("path");
const { McpServer } = require("@modelcontextprotocol/sdk/server/mcp.js");
const { StdioServerTransport } = require("@modelcontextprotocol/sdk/server/stdio.js");
const { z } = require("zod");

const BASE = process.env.DSH_URL || "http://127.0.0.1:8445";
const TOKEN = process.env.DSH_TOKEN ||
  fs.readFileSync(path.join(os.homedir(), ".dsh-eyes", "token"), "utf8").trim();

const TIMEOUT_MS = 35000;

async function appel(methode, chemin, corps) {
  const rep = await fetch(BASE + chemin, {
    method: methode,
    headers: {
      Authorization: "Bearer " + TOKEN,
      "Content-Type": "application/json",
    },
    body: corps ? JSON.stringify(corps) : undefined,
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const texte = await rep.text();
  return { status: rep.status, texte };
}

function texteOuErreur(r) {
  if (r.status >= 400) {
    throw new Error("dsh-eyes " + r.status + " : " + r.texte.slice(0, 500));
  }
  return r.texte;
}

async function appelJson(methode, chemin, corps) {
  const r = await appel(methode, chemin, corps);
  const t = texteOuErreur(r);
  try { return JSON.parse(t); } catch { return { brut: t }; }
}

const AVERTISSEMENT = "Exécuté dans le Chrome PERSONNEL d'Eddie. Ne JAMAIS taper un identifiant/mot de passe/captcha à sa place — demander à Eddie. Stop immédiat si détection anti-bot.";

const serveur = new McpServer({ name: "dsh-eyes", version: "1.1.0" });

serveur.registerTool(
  "dsh_ping",
  { description: "Santé du harnais dsh-eyes : version du démon et état réel de l'extension (connectée, version, capacités)." },
  async () => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("GET", "/v2/info")) }],
  })
);

serveur.registerTool(
  "dsh_open",
  {
    description: "Ouvre une URL dans un NOUVEL onglet du Chrome personnel d'Eddie — renvoie tab_id. " + AVERTISSEMENT,
    inputSchema: { url: z.string().describe("URL absolue (https)") },
  },
  async ({ url }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/open", { url })) }],
  })
);

serveur.registerTool(
  "dsh_goto",
  {
    description: "Navigue (comme dsh_open, alias du garde-fou allowlist). Préférer dsh_open. " + AVERTISSEMENT,
    inputSchema: { url: z.string().describe("URL absolue (https)") },
  },
  async ({ url }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/goto", { url })) }],
  })
);

serveur.registerTool(
  "dsh_read",
  {
    description: "Lit un onglet : `text` (innerText) ou `outline` (éléments interactifs avec géométrie) — pour décider la prochaine action.",
    inputSchema: {
      tab_id: z.number().describe("Identifiant de l'onglet (renvoyé par dsh_open)"),
      mode: z.enum(["text", "outline"]).optional().describe("text (défaut) ou outline"),
    },
  },
  async ({ tab_id, mode }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/cmd", { cmd: "read", params: { tab_id, mode } })) }],
  })
);

serveur.registerTool(
  "dsh_click",
  {
    description: "Clic sur un sélecteur CSS. `native:true` utilise CDP (isTrusted=true, trajectoire humaine) ; sinon clic JS détectable. " + AVERTISSEMENT,
    inputSchema: {
      tab_id: z.number(),
      selector: z.string().describe("Sélecteur CSS"),
      native: z.boolean().optional().describe("true = clic natif CDP (recommandé), false = clic JS"),
    },
  },
  async ({ tab_id, selector, native }) => {
    if (native) {
      return {
        content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/cmd", { cmd: "click_native", params: { tab_id, selector } })) }],
      };
    }
    return {
      content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/click", { tab_id, selector })) }],
    };
  }
);

serveur.registerTool(
  "dsh_fill",
  {
    description: "Saisit une valeur dans un champ (jamais un mot de passe — Eddie tape). " + AVERTISSEMENT,
    inputSchema: {
      tab_id: z.number(),
      selector: z.string().describe("Sélecteur CSS du champ"),
      value: z.string().describe("Valeur à saisir"),
    },
  },
  async ({ tab_id, selector, value }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/fill", { tab_id, selector, value })) }],
  })
);

serveur.registerTool(
  "dsh_wait",
  {
    description: "Attend qu'un sélecteur apparaisse dans l'onglet (timeout_ms par défaut côté démon, plafonné à 10 s).",
    inputSchema: {
      tab_id: z.number(),
      selector: z.string().describe("Sélecteur CSS à attendre"),
      timeout_ms: z.number().optional(),
    },
  },
  async ({ tab_id, selector, timeout_ms }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/wait", { tab_id, selector, timeout_ms })) }],
  })
);

serveur.registerTool(
  "dsh_screenshot",
  {
    description: "Capture d'écran de l'onglet (image retournée).",
    inputSchema: {
      tab_id: z.number(),
      format: z.enum(["png", "jpeg"]).optional().describe("png (défaut) ou jpeg"),
    },
  },
  async ({ tab_id, format }) => {
    const r = await appelJson("POST", "/v2/page/screenshot", { tab_id, format });
    const b64 = r.image || r.b64;
    if (b64) {
      const mime = (r.format || format) === "jpeg" ? "image/jpeg" : "image/png";
      return {
        content: [
          { type: "image", data: b64, mimeType: mime },
          { type: "text", text: "Capture de l'onglet " + tab_id },
        ],
      };
    }
    return { content: [{ type: "text", text: JSON.stringify(r) }] };
  }
);

serveur.registerTool(
  "dsh_captcha",
  {
    description: "Détection de captcha sur l'onglet (le solveur local propose une aide ; Eddie reste décisionnaire).",
    inputSchema: { tab_id: z.number() },
  },
  async ({ tab_id }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/page/captcha", { tab_id })) }],
  })
);

serveur.registerTool(
  "dsh_events",
  {
    description: "Derniers événements du vigil (domaines visités) — lecture seule.",
    inputSchema: { limit: z.number().optional().describe("Nombre max (défaut 20)") },
  },
  async ({ limit }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("GET", "/events?limit=" + (limit || 20))) }],
  })
);

serveur.registerTool(
  "dsh_act",
  {
    description: "Exécute un batch séquentiel de 1 à 50 actions avec idempotence optionnelle (`action_id`) : un second appel avec le même id renvoie le cache 5 min. Le `tab_id` produit par tabs_create/goto est propagé aux étapes suivantes. " + AVERTISSEMENT,
    inputSchema: {
      action_id: z.string().optional().describe("Identifiant d'idempotence (cache 5 min)"),
      steps: z.array(z.object({
        cmd: z.string().describe("Commande : goto, read, click, fill, wait, eval, screenshot, …"),
        params: z.record(z.any()).optional().describe("Paramètres de la commande"),
      })).min(1).max(50).describe("Étapes exécutées dans l'ordre"),
    },
  },
  async ({ action_id, steps }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/act", { action_id, steps })) }],
  })
);

serveur.registerTool(
  "dsh_act_cancel",
  {
    description: "Annule un batch `act` en cours : le handler s'arrête avant l'étape suivante et renvoie 409 cancelled avec les résultats partiels.",
    inputSchema: { action_id: z.string().describe("Identifiant du batch à annuler") },
  },
  async ({ action_id }) => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("POST", "/v2/act/cancel", { action_id })) }],
  })
);

serveur.registerTool(
  "dsh_metrics",
  {
    description: "Métriques du canal extension : volumes, issues, latence globale et par commande.",
  },
  async () => ({
    content: [{ type: "text", text: JSON.stringify(await appelJson("GET", "/v2/metrics")) }],
  })
);

async function main() {
  const transport = new StdioServerTransport();
  await serveur.connect(transport);
}
main().catch((e) => { console.error("dsh-eyes-mcp:", e); process.exit(1); });
