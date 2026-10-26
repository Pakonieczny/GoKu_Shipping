/* SETSIM, the INCREMENTAL scenario (SETFIX, Paul 17 Oct 2026: "Set 2 was formed incorrectly ... I want you to fix it once and for all").
 *
 * The simulations that complete every sheet together (setsim-days.cjs "end", the batch looks of setsim-sets.cjs) hid the bug: Set 2 of 17 Oct was made by sheets that ARRIVE one after the other. Rose Gold
 * sheets are verified BEFORE their partners complete (or after, or in between), the groups of sheets that share orders complete one group at a time or one sheet at a time, partial drafts that share
 * orders with one group (or with several) fill up late, and a gold-family sheet is cut at some moment of the day.
 *
 *   node tests/charm-nest/setsim-incremental.cjs [--seed 1,2,3] [--days N] [--root DIR]
 *
 * Each seed builds a made-up day (3-6 groups of 1-4 GF/SS sheets tied by shared orders, a Rose Gold / 10K / 14K sheet on some groups, loose sheets, drafts that touch one group or two) and plays it
 * through the page's own set assembly (Gate.assemble, with the set rules, the cardinal rule and the owner's checker) in a random order of events, in BOTH settings of SetRules.PARTIAL_BRIDGE
 * ('hold' the rule as written · 'spread'). AFTER EVERY EVENT:
 *   I1  every set that ONE group of sheets makes by itself (a group that holds a completed GF and a completed SS sheet) is exactly the set the rules make of it (CharmNestSetInvariants.expected: the real
 *       formAll over the real units, a group with a partial sheet held whole). Which completed GF-only group pairs with which SS-only group is the order the sheets came in, not a rule: those pairs are
 *       judged by I2 (valid, minimal, nothing left that could still form a set), not compared.
 *   I2  the owner's own checker finds nothing wrong in the run's sets (Gate.selfCheck: every R-number)
 *   I3  no Rose Gold / 10K / 14K sheet is in a set unless its green line is cut (nobody presses Include in this play)
 *   I4  a set never loses or swaps a sheet (sets only grow, by a sheet that shares an order with them), except a spare unit the assembly takes out by itself (Decision 1, Paul 26 Oct 2026)
 * In 'spread' (SetRules.PARTIAL_BRIDGE) a partial sheet in the middle of a chain holds nothing: the sheets on either side form their own sets, and when it is full they are the flagged state "one group
 * in several sets" by design (a person takes the orders off): there I1 and the cardinal rule (R9) and minimality (R7) are not asked, everything else is (valid sets, no partial sheet in a set, no uncut gold sheet).
 * Exit 1 and the exact case (seed, setting, step, what) when one fails. Offline, deterministic; all orders are made up.      */
'use strict';
const path = require('node:path');
const Lib = require('./setsim-lib.cjs'), { setWorld, L } = Lib;
const argv = process.argv.slice(2), opt = (k, d) => { const i = argv.indexOf('--' + k); return i >= 0 ? argv[i + 1] : d; };
const GOLD_FAMILY = ['rose', 'gold14k', 'gold10k'];
const part = sets => sets.map(s => s.slice().sort()).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));

/** One made-up day: its sheets (all start filling), the events that play it and the groups they belong to. */
function makeDay(seed, bridge) {
  const r = L.rng(seed * 977 + 13), pick = l => l[Math.floor(r() * l.length)];
  let order = 5000000000 + seed * 100000; const next = () => String(++order);
  const pages = new Map(), groups = [];
  const mk = (tag, metal, pg, o = {}) => { const p = { metal, page: pg, runId: 'run-1', sheetId: tag, draft: true, outputs: { ai: 'a' }, persistedDone: true, verification: { ok: true }, status: 'complete', density: .5, placements: [], charms: [], releaseFull: false, ...o }; pages.set(tag, p); return p; };
  const piece = (p, id) => { p.placements.push({ id }); p.charms.push({ id, poolId: id, order: id.split('_')[0] }); };
  const filler = p => piece(p, `${next()}_1_1`);   // (a single-piece order of its own, so every sheet has something on it)
  const link = (a, b) => { const o = next(); piece(a, `${o}_1_1`); piece(b, `${o}_1_2`); };
  let page = 0; const pageOf = () => ++page;
  const nG = 3 + Math.floor(r() * 4);
  for (let g = 0; g < nG; g++) {
    const n = 1 + Math.floor(r() * 4), tags = [];
    for (let i = 0; i < n; i++) { const metal = r() < .5 ? 'gold' : 'silver', tag = `${metal === 'gold' ? 'g' : 's'}${g}_${i}`; const p = mk(tag, metal, pageOf()); filler(p); tags.push(tag); if (i) link(pages.get(tags[Math.floor(r() * i)]), p); }
    groups.push({ tags, extra: [] });
  }
  for (const [g, G] of groups.entries()) if (r() < .55) {       // a gold-family sheet on the group
    const metal = pick(['rose', 'rose', 'gold14k', 'gold10k']), tag = `${metal === 'rose' ? 'r' : metal === 'gold14k' ? 'k' : 't'}${g}`, p = mk(tag, metal, pageOf(), { density: .3 + r() * .5 }); filler(p); link(pages.get(pick(G.tags)), p); G.extra.push(tag);
  }
  for (let i = 0; i < 1 + Math.floor(r() * 3); i++) { const metal = r() < .5 ? 'gold' : 'silver', tag = `L${i}`, p = mk(tag, metal, pageOf()); filler(p); groups.push({ tags: [tag], extra: [], loose: true }); }
  const drafts = [];
  for (let i = 0; i < 2 + Math.floor(r() * 3); i++) { const tag = `d${i}`, p = mk(tag, r() < .5 ? 'gold' : 'silver', pageOf(), { density: .3 }); filler(p); drafts.push({ tag, touches: [], fills: false }); }
  const real = groups.filter(g => !g.loose);
  if (drafts[0] && real.length) { drafts[0].touches = [pick(real)]; link(pages.get(pick(drafts[0].touches[0].tags)), pages.get(drafts[0].tag)); drafts[0].fills = r() < .6; }
  if (drafts[1] && real.length > 1) {                              // a draft with orders on two groups: the open question (PARTIAL_BRIDGE)
    const a = pick(real); let b = pick(real); while (b === a) b = pick(real); drafts[1].touches = [a, b];
    link(pages.get(pick(a.tags)), pages.get(drafts[1].tag)); link(pages.get(pick(b.tags)), pages.get(drafts[1].tag)); drafts[1].fills = bridge === 'hold' && r() < .6;   // (in 'spread' a bridge that fills is the flagged state, not this play)
  }
  // the events: every GF/SS sheet of a group fills; gold-family sheets arrive and are cut at some moment; drafts that fill do
  const ev = [];
  for (const G of groups) for (const t of G.tags) ev.push({ k: 'full', t });
  for (const G of groups) for (const t of G.extra) { ev.push({ k: 'arrive', t }); if (r() < .6) ev.push({ k: 'cut', t }); }
  for (const d of drafts) if (d.fills) ev.push({ k: 'full', t: d.tag });
  // a random order, but a sheet is cut after it arrived
  for (let i = ev.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [ev[i], ev[j]] = [ev[j], ev[i]]; }
  const seq = [], later = [], arrived = new Set();
  for (const e of ev) { if (e.k === 'cut' && !arrived.has(e.t)) { later.push(e); continue; } seq.push(e); if (e.k === 'arrive') { arrived.add(e.t); for (let i = later.length - 1; i >= 0; i--) if (later[i].t === e.t) { seq.push(later[i]); later.splice(i, 1); } } }
  const initial = [...pages.values()].filter(p => !GOLD_FAMILY.includes(p.metal));
  return { pages, initial, events: seq, groups, drafts };
}

async function incremental({ root, seed, bridge }) {
  // (Decision 1, Paul 26 Oct 2026, "Remove automatically": the assembly takes a spare unit out of a formed set by itself, Gate.trimSurplus, and says so in the agent's log: caught here for I4)
  const trims = [], world = setWorld(root, 'const agent=(_c,_k,msg)=>{__trims.push(String(msg||""))};'), SR = world.SR, I = world.II; world.c.__trims = trims;
  if (!I || typeof world.selfCheck !== 'function') return { seed, bridge, steps: 0, skipped: 'this checkout has no set checker' };
  const was = SR.PARTIAL_BRIDGE; SR.PARTIAL_BRIDGE = bridge;
  const day = makeDay(seed, bridge), fails = []; let steps = 0; const memberOf = new Map();
  try {
    for (const p of day.initial) world.addPage(p);
    const judge = async (what) => {
      await world.assemble();   // (the trim says so once its own look is done, which can be steps later: a set that said it let a spare unit go may lose it; any other set may not)
      steps++;
      const pg = world.gatePages(), inSet = p => !p.draft && !!p.setId, res = world.selfCheck(), input = world.checkerInput(), fail = m => { if (fails.length < 5) fails.push({ seed, bridge, step: steps, what, msg: m }); };
      if (!res) return fail('no verdict from the Gate\'s self-check');
      const spread = bridge === 'spread', relaxed = new Set(['cardinal', 'surplus', 'closedJoin', 'unformed']);
      const bad = spread ? res.violations.filter(v => !relaxed.has(v.rule)) : res.violations;
      if (bad.length) fail('I2 the owner\'s checker: ' + bad.map(v => `[${v.ruleNo} ${v.rule}] ${v.message}`).join(' | '));
      const have = new Map(); for (const p of pg) if (inSet(p)) { if (!have.has(p.setId)) have.set(p.setId, []); have.get(p.setId).push(p.sheetId); }
      const want = I.expected(input.sheets).sets.map(s => s.sheetIds);
      const solo = sets => part(I.check(sets.map((ids, i) => ({ id: 'x' + i, name: 'X' + i, sheetIds: ids })), input.sheets, { skipExempt: true, partialPartners: 'pending', unformed: false }).sets.filter(r => r.units.length === 1).map(r => r.sheets));
      if (!spread && JSON.stringify(solo([...have.values()])) !== JSON.stringify(solo(want))) fail('I1 a set one group makes by itself is not what the rules make of it: have ' + JSON.stringify(solo([...have.values()])) + ' want ' + JSON.stringify(solo(want)));
      for (const p of pg) if (inSet(p) && GOLD_FAMILY.includes(p.metal) && !p.roseCutAt && !p.laserDoneAt) fail(`I3 ${p.sheetId} is in a set without its green line cut`);
      for (const [id, set] of memberOf) { const p = pg.find(x => x.sheetId === id); if (!p || !inSet(p) || p.setId !== set) { const from = (world.sets().find(s => s.setId === set) || {}).name; if (!(from && trims.some(t => t.startsWith(from + ' held more sheets than it needs')))) fail(`I4 ${id} left ${set}`); } }
      for (const p of pg) if (inSet(p)) memberOf.set(p.sheetId, p.setId);
    };
    await judge('start');
    for (const e of day.events) {
      const p = day.pages.get(e.t);
      if (e.k === 'full') Object.assign(p, { releaseFull: true, density: .76 }); else if (e.k === 'arrive') world.addPage(p); else if (e.k === 'cut') p.roseCutAt = Date.now();
      await judge(`${e.t} ${e.k}`);
      if (fails.length) break;
    }
  } finally { SR.PARTIAL_BRIDGE = was; }
  return { seed, bridge, steps, fails, sets: new Set(world.gatePages().filter(p => !p.draft && p.setId).map(p => p.setId)).size, sheets: day.pages.size };
}
module.exports = { incremental, makeDay };

if (require.main === module) (async () => {
  const root = path.resolve(opt('root', Lib.REPO)), seeds = opt('seed') ? String(opt('seed')).split(',').map(Number) : Array.from({ length: +opt('days', 30) }, (_, i) => i + 1);
  let bad = 0, steps = 0, sets = 0, sheets = 0, skipped = 0;
  for (const seed of seeds) for (const bridge of ['hold', 'spread']) {
    const r = await incremental({ root, seed, bridge });
    if (r.skipped) { skipped++; continue; }
    steps += r.steps; sets += r.sets; sheets += r.sheets;
    for (const f of r.fails || []) { bad++; console.log(`seed ${f.seed} (${f.bridge}) step ${f.step} after "${f.what}": ${f.msg}`); }
  }
  if (skipped) console.log(`skipped: ${skipped} (this checkout has no set checker)`);
  if (bad) { console.log('FAIL: setsim-incremental: ' + bad + ' broken invariant reading(s)'); process.exit(1); }
  console.log(`PASS: setsim-incremental: ${seeds.length} made-up days in both settings of PARTIAL_BRIDGE (${sheets} sheets, ${sets} sets formed), ${steps} events, after every event the Gate's sets are the sets the rules make, the owner's checker finds nothing wrong and no gold-family sheet is in a set uncut`);
})().catch(e => { console.error(e); process.exit(1); });
