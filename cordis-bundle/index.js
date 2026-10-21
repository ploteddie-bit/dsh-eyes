/**
 * DSH Eyes — plugin hôte natif. Outils d'observation et de manipulation du
 * navigateur via le démon local DSH Eyes (http://127.0.0.1:8445, extension
 * MV3 « DSH Eyes » côté Chrome, couche de comportement humain simulé).
 *
 * Modèle : chaque outil construit un ToolDefinition (même forme que
 * defineTool) et proxifie vers l'API démon (auth Bearer, token lu dans
 * $HOME/.dsh-eyes/token au moment de l'appel — jamais loggé).
 */

import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

/** Cordis plugin identity. */
export const name = "dsh-eyes";
/** Le plugin a besoin du service tools du harnais. */
export const inject = ["tools"];

const DAEMON = "http://127.0.0.1:8445";

/** Token lu à chaque appel (jamais mis en cache module-wide). */
async function token() {
	const path = join(homedir(), ".dsh-eyes", "token");
	return (await readFile(path, "utf8")).trim();
}

/** Requête authentifiée vers le démon. Retourne le JSON parsé. */
async function api(path, method = "GET", body) {
	const auth = `Bearer ${await token()}`;
	const res = await fetch(`${DAEMON}${path}`, {
		method,
		headers: {
			Authorization: auth,
			...(body === undefined ? {} : { "Content-Type": "application/json" }),
		},
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	const data = (await res.json().catch(() => ({}))) ?? {};
	if (!res.ok) throw new Error(`dsh-eyes ${res.status} : ${JSON.stringify(data)}`);
	return data;
}

/** ToolDefinition : {name, description, parameters(JSON schema), output:{schema, render}, execute}. */
function tool(name, description, properties, required, execute, outputDescription = "Réponse JSON du démon DSH Eyes.") {
	const schema = {
		type: "object",
		additionalProperties: false,
		properties: {},
		required,
	};
	for (const key of required.concat(Object.keys(properties).filter((k) => !required.includes(k)))) {
		schema.properties[key] = properties[key];
	}
	return {
		name,
		description,
		parameters: schema,
		output: {
			schema: { type: "object", additionalProperties: true },
			render: (_args, value) => [{ type: "text", text: JSON.stringify(value, undefined, 2) }],
		},
		description_output: outputDescription,
		async execute(args) {
			return execute(args);
		},
	};
}

/** Construit un JSON schema pour un paramètre string. */
const str = (description) => ({ type: "string", description });
/** Construit un JSON schema pour un paramètre number optionnel. */
const num = (description) => ({ type: "number", description });

export function apply(ctx, _config) {
	ctx.tools.register(
		tool(
			"dsh_eyes_stats",
			"DSH Eyes — volumes de navigation par domaine observés dans Chrome (extension DSH Eyes). Aucun paramètre.",
			{},
			[],
			async () => api("/stats"),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_events",
			"DSH Eyes — dernières alertes du vigil (localhost_access critical, private_network, new_domain). Aucun paramètre.",
			{},
			[],
			async () => api("/events"),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_open",
			"DSH Eyes — ouvre une URL dans un nouvel onglet du navigateur de l'opérateur (trajectoire humaine simulée). Navigation sortante uniquement : localhost et réseaux privés refusés (403).",
			{ url: str("URL http(s) à ouvrir.") },
			["url"],
			async (args) => api("/v2/open", "POST", { url: args.url }),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_read",
			"DSH Eyes — lit le contenu texte d'un onglet (tronqué à 200 Ko). Sans tab_id : onglet actif.",
			{ tab_id: num("ID de l'onglet (renvoyé par dsh_eyes_open).") },
			[],
			async (args) => api("/v2/read", "POST", { tab_id: args.tab_id }),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_goto",
			"DSH Eyes — navigue un onglet vers une URL (garde-fou réseau interne : 403).",
			{ url: str("URL http(s) cible.") },
			["url"],
			async (args) => api("/v2/goto", "POST", { url: args.url }),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_click",
			"DSH Eyes — clique un élément avec trajectoire de souris humaine (Bézier cubique + bruit gaussien + overshoot ~20 %).",
			{ selector: str("Sélecteur CSS de l'élément."), tab_id: num("ID de l'onglet cible.") },
			["selector"],
			async (args) =>
				api("/v2/click", "POST", {
					selector: args.selector,
					...(args.tab_id === undefined ? {} : { tab_id: args.tab_id }),
				}),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_fill",
			"DSH Eyes — remplit un champ avec frappe clavier humaine (délais lognormaux 65-350 ms/char, typos simulées ~3 % avec correction backspace).",
			{
				selector: str("Sélecteur CSS du champ."),
				value: str("Texte à saisir."),
				tab_id: num("ID de l'onglet cible."),
			},
			["selector", "value"],
			async (args) =>
				api("/v2/fill", "POST", {
					selector: args.selector,
					value: args.value,
					...(args.tab_id === undefined ? {} : { tab_id: args.tab_id }),
				}),
		),
	);

	ctx.tools.register(
		tool(
			"dsh_eyes_solve",
			"DSH Eyes — détecte et résout un captcha localement (qwen2.5-vl sur les instances Ollama de l'opérateur : ia-general RX 9060 XT → serveur-ia iGPU → humain). human_required = ne pas insister, unsupported = reCAPTCHA v3.",
			{ tab_id: num("ID de l'onglet cible.") },
			[],
			async (args) =>
				api("/v2/page/solve", "POST", {
					...(args.tab_id === undefined ? {} : { tab_id: args.tab_id }),
				}),
		),
	);
}
