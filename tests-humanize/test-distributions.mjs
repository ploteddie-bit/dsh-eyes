/**
 * tests humain-math : validation des distributions comportementales.
 * node tests-humanize/test-distributions.mjs
 */
import {
	gauss,
	cubicBezier,
	humanMousePath,
	humanTypingPlan,
	humanActionDelay,
	humanScrollPlan,
} from '../extension/humanize-math.js';

let failures = 0;
function check(name, condition, detail = '') {
	if (!condition) {
		failures++;
		console.error(`✗ ${name} ${detail}`);
	} else {
		console.log(`✓ ${name}`);
	}
}

// ─── gauss : moyenne et écart-type sur 10 000 tirages
{
	const n = 10000;
	const samples = Array.from({ length: n }, () => gauss(1200, 400));
	const mean = samples.reduce((a, b) => a + b, 0) / n;
	const std = Math.sqrt(
		samples.reduce((a, b) => a + (b - mean) ** 2, 0) / n,
	);
	check('gauss moyenne ~1200 (±40)', Math.abs(mean - 1200) < 40, `moyenne=${mean.toFixed(1)}`);
	check('gauss écart-type ~400 (±20)', Math.abs(std - 400) < 20, `std=${std.toFixed(1)}`);
	check('gauss distribution (aucun tir < 5σ)', samples.every((s) => s > 1200 - 5 * 400), `min=${Math.min(...samples).toFixed(0)}`);
}

// ─── cubicBezier : t=0 → p0, t=1 → p1
{
	const p = cubicBezier({ x: 0, y: 0 }, { x: 100, y: 50 }, { x: 200, y: 100 }, { x: 300, y: 150 }, 0);
	check('bezier t=0 = départ', p.x === 0 && p.y === 0);
	const q = cubicBezier({ x: 0, y: 0 }, { x: 100, y: 50 }, { x: 200, y: 100 }, { x: 300, y: 150 }, 1);
	check('bezier t=1 = arrivée', q.x === 300 && q.y === 150);
}

// ─── humanMousePath : structure, overshoot, lissage
{
	const from = { x: 40, y: 400 };
	const to = { x: 900, y: 120 };
	const { points, overshoot } = humanMousePath(from, to);
	check('path > 30 points (distance ~890px)', points.length > 30, `points=${points.length}`);
	check('path démarre près du départ', Math.hypot(points[0].x - from.x, points[0].y - from.y) < 40);
	check('path finit sur la cible', points[points.length - 1].x === to.x && points[points.length - 1].y === to.y);
	check('délais tous > 0', points.every((p) => p.delayMs > 0));
	// Lissage : aucun saut > 80 px entre points consécutifs (courbe, pas téléportation)
	let maxJump = 0;
	for (let i = 1; i < points.length; i++) {
		maxJump = Math.max(maxJump, Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y));
	}
	check('path lissé (aucun saut > 80px)', maxJump < 80, `maxJump=${maxJump.toFixed(1)}`);
	// Déterminisme de la cible finale : 20 runs → tous finissent sur `to`
	let allEndOnTarget = true;
	for (let r = 0; r < 20; r++) {
		const run = humanMousePath(from, to, { overshoot: true });
		if (run.points[run.points.length - 1].x !== to.x || run.points[run.points.length - 1].y !== to.y) {
			allEndOnTarget = false;
		}
	}
	check('20/20 trajets finissent sur la cible (overshoot corrigé)', allEndOnTarget);
	// Statistique overshoot : ~20 % sur 200 trajets (30-80 % toléré, 880px > 80px minimum)
	let overshootCount = 0;
	for (let r = 0; r < 200; r++) overshootCount += humanMousePath(from, to).overshoot ? 1 : 0;
	const rate = overshootCount / 200;
	check('overshoot rate ~20 % (10-35 %)', rate > 0.1 && rate < 0.35, `rate=${(rate * 100).toFixed(1)} %`);
	// Variabilité : deux trajets identiques en paramètres ne sont jamais identiques
	const a = humanMousePath(from, to);
	const b = humanMousePath(from, to);
	check('trajets toujours différents (variabilité humaine)', JSON.stringify(a.points) !== JSON.stringify(b.points));
}

// ─── humanTypingPlan : délais lognormaux, typos, structure
{
	const plan = humanTypingPlan('Bonjour le monde');
	check('plan ≥ longueur du texte', plan.filter((p) => p.type === 'char').length === 'Bonjour le monde'.length);
	check('délais entre 60 et 400 ms', plan.every((p) => p.delayMs >= 60 && p.delayMs <= 400));
	// Typos : taux ~3 % sur un texte long
	const longPlan = humanTypingPlan('abcdefghij'.repeat(20), { typoRate: 0.03 });
	const typos = longPlan.filter((p) => p.type === 'typo').length;
	const totalLetters = longPlan.filter((p) => p.type !== 'backspace').length;
	const rate = typos / totalLetters;
	check('typo rate ~3 % (1-6 %)', rate > 0.01 && rate < 0.06, `rate=${(rate * 100).toFixed(1)} %`);
	// Chaque typo est immédiatement suivie d'un backspace
	let paired = true;
	for (let i = 0; i < longPlan.length; i++) {
		if (longPlan[i].type === 'typo') {
			if (longPlan[i + 1]?.type !== 'backspace') paired = false;
		}
	}
	check('chaque typo suivie dun backspace', paired);
	// typoRate 0 → zéro typo
	const noTypo = humanTypingPlan('abc', { typoRate: 0 });
	check('typoRate 0 → zéro typo', noTypo.every((p) => p.type === 'char'));
}

// ─── humanActionDelay : plancher 350 ms
{
	const delays = Array.from({ length: 500 }, () => humanActionDelay());
	check('action delay ≥ 350 ms', delays.every((d) => d >= 350));
	const mean = delays.reduce((a, b) => a + b, 0) / delays.length;
	check('action delay moyenne ~1200 (±60)', Math.abs(mean - 1200) < 60, `moyenne=${mean.toFixed(0)}`);
}

// ─── humanScrollPlan : progression complète
{
	const plan = humanScrollPlan(1500);
	const total = plan.reduce((a, b) => a + b.px, 0);
	check('scroll 1500px couvert', total === 1500, `total=${total}`);
	check('scroll étapes 80-300 px', plan.every((p) => p.px >= 80 && p.px <= 300));
	check('scroll délais ≥ 80 ms', plan.every((p) => p.delayMs >= 80));
}

console.log(failures === 0 ? '\n✅ TOUS LES TESTS PASSENT' : `\n❌ ${failures} ÉCHEC(S)`);
process.exit(failures === 0 ? 0 : 1);
