/**
 * humanize-math.js — mathématiques de comportement humain simulé (DSH Eyes v2.1).
 *
 * Port des patterns ghost-cursor (référence industrie scraping) :
 * - Trajectoires souris en courbes de Bézier cubiques + bruit gaussien
 *   sur les points de contrôle, vitesse en cloche (gaussienne), overshoot
 *   dans ~20 % des trajets puis correction.
 * - Frappe clavier : délais lognormaux (65–350 ms/caractère), typos
 *   occasionnelles (~3 %) avec correction backspace.
 * - Délais entre actions : gaussiens (moyenne 1,2 s, σ 0,4 s), plancher 350 ms.
 *
 * Module PUR (aucune API navigateur) — testable sous node.
 */

/** Gaussienne par Box-Muller (étape uniforme fournie par Math.random). */
export function gauss(mean = 0, stdDev = 1) {
	let u = 0;
	let v = 0;
	while (u === 0) u = Math.random();
	while (v === 0) v = Math.random();
	return mean + stdDev * Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

/**
 * Profils de navigation humaine. `jitter` = amplitude du bruit sur les
 * points de contrôle souris (px) ; `typoRate` = probabilité de typo par
 * caractère (plafonnée à 0,08 par humanTypingPlan).
 */
export const HUMAN_PROFILES = {
	calme: { jitter: 40, typoRate: 0.05 },
	normal: { jitter: 24, typoRate: 0.03 },
	presse: { jitter: 12, typoRate: 0.015 },
};

/** Retourne le profil demandé, ou `normal` si le nom est inconnu. */
export function humanProfile(name) {
	return HUMAN_PROFILES[name] || HUMAN_PROFILES.normal;
}

/** Point sur une courbe de Bézier cubique à t ∈ [0,1]. */
export function cubicBezier(p0, c1, c2, p1, t) {
	const it = 1 - t;
	const a = it * it * it;
	const b = 3 * it * it * t;
	const c = 3 * it * t * t;
	const d = t * t * t;
	return {
		x: a * p0.x + b * c1.x + c * c2.x + d * p1.x,
		y: a * p0.y + b * c1.y + c * c2.y + d * p1.y,
	};
}

/**
 * Génère une trajectoire souris humaine de `from` vers `to`.
 *
 * @param {{x:number,y:number}} from
 * @param {{x:number,y:number}} to
 * @param {{overshoot?:boolean, jitter?:number}} [options]
 *   - overshoot : force le dépassement + correction (défaut : 20 % de chance)
 *   - jitter : amplitude du bruit sur les points de contrôle en px (défaut 30)
 * @returns {{points: Array<{x:number,y:number,delayMs:number}>, overshoot: boolean}}
 *   points : à dispatcher dans l'ordre, delayMs = pause AVANT le point.
 */
export function humanMousePath(from, to, options = {}) {
	const jitter = options.jitter ?? 30;
	const distance = Math.hypot(to.x - from.x, to.y - from.y);
	// Densité de points : 1 par ~12 px, plancher 8, plafond 120.
	const steps = Math.max(8, Math.min(120, Math.round(distance / 12)));

	// Points de contrôle : décalés perpendiculairement à l'axe (courbure
	// naturelle) + bruit gaussien (jamais deux trajets identiques).
	const dx = to.x - from.x;
	const dy = to.y - from.y;
	const perpX = -dy / (distance || 1);
	const perpY = dx / (distance || 1);
	const bow = gauss(0, jitter) * 0.6;
	const c1 = {
		x: from.x + dx * 0.25 + perpX * bow + gauss(0, jitter * 0.4),
		y: from.y + dy * 0.25 + perpY * bow + gauss(0, jitter * 0.4),
	};
	const c2 = {
		x: from.x + dx * 0.75 + perpX * bow * 0.5 + gauss(0, jitter * 0.4),
		y: from.y + dy * 0.75 + perpY * bow * 0.5 + gauss(0, jitter * 0.4),
	};

	// Overshoot : ~20 % des trajets dépassent la cible de 5–30 px puis corrigent.
	const overshoot = (options.overshoot ?? Math.random() < 0.2) && distance > 80;
	const end = overshoot
		? {
				x: to.x + gauss(0, 8) + Math.sign(dx || 1) * (8 + Math.random() * 22),
				y: to.y + gauss(0, 8) + Math.sign(dy || 1) * (5 + Math.random() * 15),
			}
		: { x: to.x, y: to.y };

	const points = [];
	let previous = { x: from.x, y: from.y };
	for (let i = 1; i <= steps; i++) {
		// Vitesse en cloche : lent au départ, rapide au milieu, freinage en approche.
		const t = i / steps;
		const bell = Math.sin(Math.PI * Math.min(1, t * 1.15)) ** 0.7;
		const p = cubicBezier({ x: from.x, y: from.y }, c1, c2, end, t);
		const step = Math.hypot(p.x - previous.x, p.y - previous.y);
		// Délai par point : ~6-18 ms, modulé par la vitesse (points rapides = délais courts).
		const delayMs = Math.max(2, Math.round(gauss(11, 4) * (1.35 - bell * 0.7)));
		points.push({ x: Math.round(p.x * 10) / 10, y: Math.round(p.y * 10) / 10, delayMs });
		previous = p;
	}

	// Correction d'overshoot : retour doux vers la cible exacte.
	if (overshoot) {
		const corrections = Math.max(3, Math.round(steps / 8));
		for (let i = 1; i <= corrections; i++) {
			const t = i / corrections;
			const p = {
				x: end.x + (to.x - end.x) * t,
				y: end.y + (to.y - end.y) * t,
			};
			points.push({
				x: Math.round(p.x * 10) / 10,
				y: Math.round(p.y * 10) / 10,
				delayMs: Math.round(gauss(14, 4)),
			});
		}
		points.push({ x: to.x, y: to.y, delayMs: Math.round(gauss(30, 8)) });
	}

	return { points, overshoot };
}

// Voisinage QWERTY (lignes clavier) — pour la simulation de typos réalistes.
const QWERTY_NEIGHBORS = {
	q: 'wa', w: 'qes', e: 'wrd', r: 'etf', t: 'ryg', y: 'tuh', u: 'yij', i: 'uok',
	o: 'ipl', p: 'ol', a: 'qsz', s: 'awdxz', d: 'sefcx', f: 'drgv', g: 'fthb',
	h: 'gyjn', j: 'hukm', k: 'jil', l: 'ko', z: 'asx', x: 'zsdc', c: 'xdfv',
	v: 'cfgb', b: 'vghn', n: 'bhjm', m: 'njk',
};

/**
 * Génère le plan de frappe humain d'un texte.
 *
 * @param {string} text
 * @param {{typoRate?:number}} [options]
 *   - typoRate : probabilité de typo par caractère (défaut 0.03, plafonné 0.08)
 * @returns {Array<{type:'char'|'typo'|'backspace', ch:string, delayMs:number}>}
 *   type char : taper `ch`. type typo : taper `ch` (mauvais caractère) suivi
 *   d'un backspace. Les délais suivent une lognormale 65–350 ms ; pause
 *   supplémentaire après espace (mots) et ponctuation.
 */
export function humanTypingPlan(text, options = {}) {
	const typoRate = Math.min(0.08, options.typoRate ?? 0.03);
	const plan = [];
	for (const ch of text) {
		const base = Math.min(350, Math.max(65, Math.round(Math.exp(gauss(0, 0.55)) * 105)));
		// Typos : uniquement lettres, jamais le premier caractère ni les espaces.
		const isLetter = /[a-z]/i.test(ch);
		const doTypo = isLetter && plan.length > 2 && Math.random() < typoRate;
		if (doTypo) {
			const lower = ch.toLowerCase();
			const neighbors = QWERTY_NEIGHBORS[lower] ?? 'q';
			const wrong = neighbors[Math.floor(Math.random() * neighbors.length)];
			plan.push({ type: 'typo', ch: wrong, delayMs: Math.round(base * 0.8) });
			plan.push({ type: 'backspace', ch: '', delayMs: Math.round(gauss(180, 60)) });
		}
		plan.push({ type: 'char', ch, delayMs: base });
		if (ch === ' ') plan[plan.length - 1].delayMs += Math.round(gauss(40, 15));
		if (/[.!?:,]/.test(ch)) plan[plan.length - 1].delayMs += Math.round(gauss(120, 40));
	}
	return plan;
}

/**
 * Délai gaussien entre deux actions (jamais mécanique, jamais < 350 ms).
 * @param {{meanMs?:number, stdMs?:number}} [options]
 */
export function humanActionDelay(options = {}) {
	const mean = options.meanMs ?? 1200;
	const std = options.stdMs ?? 400;
	return Math.max(350, Math.round(gauss(mean, std)));
}

/**
 * Plan de scroll humain : étapes progressives de 80–300 px avec délais
 * 120–450 ms, freinage sur la dernière étape.
 * @param {number} totalPx distance à scroller (>0)
 * @param {{direction?:'down'|'up'}} [options]
 * @returns {Array<{px:number, delayMs:number, direction:'down'|'up'>}>}
 */
export function humanScrollPlan(totalPx, options = {}) {
	const direction = options.direction === 'up' ? 'up' : 'down';
	const plan = [];
	let remaining = Math.max(0, Math.round(totalPx));
	let first = true;
	while (remaining > 0) {
		const step = Math.min(remaining, Math.round(80 + Math.random() * 220));
		const delayMs = Math.round(
			(first ? 300 : 140) + gauss(120, 60) + (remaining - step < 200 ? 180 : 0),
		);
		plan.push({ px: step, delayMs: Math.max(80, delayMs), direction });
		remaining -= step;
		first = false;
	}
	return plan;
}
