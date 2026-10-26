// AUDIT (gold family, the pure rules): the remedy of a blocked set (SETFIX A "Cut RG Sheet n" / "Rebuild this set correctly"), the repair plan (SETREPAIR) and a person's moves
// (SetEdit.verifyMoves, the one rule the Library plan and the server's flowApply setMember read), judged by the acceptance checker (charm-nest-set-invariants.js check).
// Fixtures only, made-up ids; no network, no browser. Prints OK / BREAK per finding; exits 1 when a BREAK is reproduced.
'use strict';
const assert = require('node:assert/strict');
const I = require('../../charm-nest-set-invariants.js'), SRp = require('../../charm-nest-set-repair.js'), SE = require('../../charm-nest-set-edit.js'), SR = require('../../charm-nest-set-rules.js');
const breaks = [];
const verdict = (name, ok, detail) => { console.log(`${ok ? 'OK   ' : 'BREAK'} ${name}${ok ? '' : ' :: ' + detail}`); if (!ok) breaks.push(name); };
const RUN = 'run-a';
const sheet = (id, metal, page, o = {}) => Object.assign({ id, metal, runId: RUN, page, sheetIndex: o.setId ? page : null, draft: !o.setId, setId: null, releaseFull: !['rose', 'gold10k', 'gold14k'].includes(metal), density: .7, placedCount: 2, poolIds: [`${id.replace(/\D/g, '') || 1}000${page}_1_1`] }, o);
const link = (a, b, order) => { a.poolIds.push(`${order}_1_1`); b.poolIds.push(`${order}_1_2`); };

/* ── A · "A blocked set is never a dead end" (set-rules.md SETFIX A): the press the remedy names must lead to a set that follows the rules ─────────────────────────────────── */
for (const tied of [true, false]) {
  const gf = sheet('a-gf', 'gold', 1, { setId: 's-2' }), ss = sheet('a-ss', 'silver', 2, { setId: 's-2' }), rg = sheet('a-rg', 'rose', 3, { setId: 's-2', releaseFull: false, density: .4, backsApproved: false });
  link(gf, ss, '7100000001'); if (tied) link(gf, rg, '7100000002');
  const set = { setId: 's-2', seq: 2, runId: RUN, day: '2026-10-25', sheetIds: ['a-gf', 'a-ss', 'a-rg'], status: 'open' };
  const d0 = SRp.diagnose([set], [gf, ss, rg]).bySet['s-2'];
  // (Paul, 26 Oct 2026: "we shouldn't double hold things": a green-line sheet's open back engraving is what its set's Approve waits for, never a rule the set breaks: the remedy is the Cut alone)
  assert(d0, 'the set breaks R12 (uncut)'); assert.equal(d0.remedy.kind, 'cut'); assert.equal(d0.remedy.then, undefined, 'the remedy: Cut (the back engraving is a wait, not a break)');
  assert(!/back engraving/.test(d0.line), 'the red line never names the back engraving: ' + d0.line);
  // the person follows every press the remedy names, one after the other, until the set follows the rules (or the remedy repeats itself: a dead end)
  let cur = rg, d = d0, steps = [], seen = new Set();
  while (d && steps.length < 4) {
    const r = d.remedy, key = r.kind + ':' + (r.then || '') + ':' + JSON.stringify(cur);
    if (seen.has(key)) break; seen.add(key);
    if (r.kind === 'cut') { cur = Object.assign({}, cur, { roseCutAt: Date.now() }); steps.push('cut'); }
    if (r.kind === 'backs' || r.then === 'backs') { cur = Object.assign({}, cur, { backsApproved: true }); steps.push('backs'); }
    if (r.kind === 'rebuild' || r.then === 'rebuild') { const p = SRp.plan(set, [gf, ss, cur], [set], {}); steps.push('rebuild:' + (p.ok ? 'ok' : 'refused')); if (!p.ok) break; }
    d = SRp.diagnose([set], [gf, ss, cur]).bySet['s-2'];
  }
  verdict(`A ${tied ? 'tied' : 'loose'} RG: following the remedy's presses puts the set right (no dead end)`, !d, `still breaks after ${steps.join(' → ')}: "${d && d.line}" remedy ${JSON.stringify(d && d.remedy)}`);
  // and a cut sheet whose only gap is its back engraving is told to approve it, with no press that could only be refused
  const dB = SRp.diagnose([set], [gf, ss, Object.assign({}, rg, { roseCutAt: Date.now() })]).bySet['s-2'];
  // (Paul, 26 Oct 2026: a cut sheet's open back engraving is the set's approval to wait for, not a rule the set breaks: no red line, no Rebuild; Approve for laser cutting waits)
  verdict(`A ${tied ? 'tied' : 'loose'} RG: a cut sheet whose back engraving is open breaks no rule (its set's Approve waits for it)`, !dB, JSON.stringify(dB && dB.remedy));
}

/* ── B · SETREPAIR plan, randomised: when it says ok, the state after is right, the plan is idempotent, it never moves a cut sheet, and it never touches another set ─────────── */
function rng(seed) { let s = seed >>> 0; return () => ((s = (s * 1664525 + 1013904223) >>> 0) / 4294967296); }
function randomWorld(r, k) {
  const pick = l => l[Math.floor(r() * l.length)], sheets = [], sets = [];
  const nSet = 1 + Math.floor(r() * 3);
  let pg = 0, ord = 7200000000 + k * 1000;
  for (let s = 0; s < nSet; s++) {
    const committed = s === 0 && r() < .3, setId = `s-${s + 1}`, ids = [];
    const n = 2 + Math.floor(r() * 5);
    for (let i = 0; i < n; i++) {
      const metal = i === 0 ? 'gold' : i === 1 ? 'silver' : pick(['gold', 'silver', 'gold', 'silver', 'rose', 'gold10k', 'gold14k']);
      const gold = ['rose', 'gold10k', 'gold14k'].includes(metal);
      const o = { setId };
      if (gold) Object.assign(o, { releaseFull: false, density: .4 }, r() < .4 ? { roseCutAt: 1000 + i } : {}, r() < .3 ? { solidIncluded: true } : {}, r() < .5 ? { backsApproved: r() < .6 } : {});
      else if (r() < .15) Object.assign(o, { releaseFull: false, density: .5 });
      if (r() < .05) o.laserDoneAt = 5000;
      const sh = sheet(`w${k}-${pg}`, metal, ++pg, o); sheets.push(sh); ids.push(sh.id);
    }
    sets.push({ setId, seq: s + 1, runId: RUN, day: '2026-10-25', sheetIds: ids, status: committed ? 'complete' : 'open', ...(committed ? { committedAt: 9000 } : {}) });
  }
  const loose = Math.floor(r() * 4);
  for (let i = 0; i < loose; i++) { const metal = pick(['gold', 'silver', 'rose', 'gold10k']), gold = metal !== 'gold' && metal !== 'silver'; sheets.push(sheet(`w${k}-${pg}`, metal, ++pg, gold ? { releaseFull: false, density: .3, ...(r() < .5 ? { roseCutAt: 2000 } : {}) } : r() < .3 ? { releaseFull: false, density: .4 } : {})); }
  const links = Math.floor(r() * 5);
  for (let i = 0; i < links; i++) { const a = pick(sheets), b = pick(sheets); if (a !== b) link(a, b, String(++ord)); }
  return { sheets, sets };
}
const applyPlan = (p, w) => {
  // the state the plan describes: set 0 keeps the set's record; the others are new sets; every other sheet of the formation's pool is a draft
  const rows = w.sheets.map(s => Object.assign({}, s)), byId = new Map(rows.map(s => [s.id, s]));
  const setsAfter = w.sets.filter(s => s.setId !== p.setId).map(s => Object.assign({}, s, { sheetIds: s.sheetIds.slice() }));
  const placed = new Set();
  p.sets.forEach((s, k) => { const id = k === 0 ? p.setId : `new-${k}`; setsAfter.push({ setId: id, seq: s.seq, runId: RUN, day: '2026-10-25', sheetIds: s.sheetIds.slice(), status: 'open' }); s.sheetIds.forEach(i => { placed.add(i); Object.assign(byId.get(i), { setId: id, draft: false, sheetIndex: null }); }); });
  for (const i of [...p.expel, ...w.sets.find(s => s.setId === p.setId).sheetIds]) if (!placed.has(i)) Object.assign(byId.get(i), { setId: null, draft: true, sheetIndex: null });
  for (const r of rows) if (!placed.has(r.id) && !w.sets.some(s => s.setId !== p.setId && s.sheetIds.includes(r.id)) && r.setId === p.setId) Object.assign(r, { setId: null, draft: true });
  return { sheets: rows, sets: setsAfter };
};
{
  let tried = 0, planned = 0; const found = new Map();
  const note = (k, d) => { if (!found.has(k)) found.set(k, d); };
  for (let k = 0; k < 600; k++) {
    const w = randomWorld(rng(9000 + k), k), diag = SRp.diagnose(w.sets, w.sheets);
    for (const setId of Object.keys(diag.bySet)) {
      tried++;
      const set = w.sets.find(s => s.setId === setId), p = SRp.plan(set, w.sheets, w.sets, {});
      if (!p.ok) continue;
      planned++;
      if (SR.exempt(set)) note('committed', `seed ${9000 + k}: a committed set was planned`);
      if (p.expel.some(i => { const s = w.sheets.find(x => x.id === i); return s && (s.roseCutAt || +s.laserDoneAt > 0); })) note('cut', `seed ${9000 + k}: a cut sheet would move`);
      const after = applyPlan(p, w);
      if (process.env.AUDIT_SEED === String(9000 + k)) console.log(JSON.stringify({ setId, plan: { mode: p.mode, keep: p.keep, expel: p.expel, sets: p.sets.map(s => s.sheetIds) }, sheets: w.sheets, sets: w.sets }, null, 1));
      for (const s of w.sets.filter(x => x.setId !== setId)) { const a = after.sets.find(x => x.setId === s.setId); if (JSON.stringify(a.sheetIds) !== JSON.stringify(s.sheetIds)) note('other', `seed ${9000 + k}: ${s.setId} changed`); }
      // (judged the way the Library, the approval gate and the plan itself read records: SetRepair.diagnose, where a gold-family sheet's own tick, solidIncluded true, is R12 (a)'s yes)
      const ours = new Set([setId, ...after.sets.filter(s => /^new-/.test(s.setId)).map(s => s.setId)]);
      const dg = SRp.diagnose(after.sets, after.sheets, { includeStarted: true });
      const bad = [...ours].map(id => dg.bySet[id]).filter(Boolean);
      if (bad.length) note('after:' + bad[0].rules.join('+'), `seed ${9000 + k}, ${setId}: ${bad[0].line}`);
      for (const s of after.sets.filter(x => ours.has(x.setId))) if (!SR.validSet(s.sheetIds.map(i => after.sheets.find(x => x.id === i))).ok) note('R1', `seed ${9000 + k}: ${s.setId} after the plan has no completed GF + SS`);
      if (!p.sets.length) continue;      // (no valid set can be made: the set is emptied; nothing to press again)
      const again = SRp.plan(after.sets.find(s => s.setId === setId), after.sheets, after.sets, {});
      if (!(again.noop || (again.refused && again.refused.key === 'right'))) note('idempotent', `seed ${9000 + k}, ${setId}: pressing again answers ${JSON.stringify(again.refused || again.mode)}`);
    }
  }
  console.log(`      (repair: ${tried} broken sets in 600 random worlds, ${planned} plans that said ok)`);
  for (const k of ['committed', 'cut', 'other', 'R1', 'idempotent']) verdict(`B repair plan never ${({ committed: 'plans a committed set', cut: 'moves a cut sheet', other: 'changes another set', R1: 'leaves a set without a completed GF + SS', idempotent: 'differs when pressed twice' })[k]}`, !found.has(k), found.get(k));
  const afterBad = [...found.keys()].filter(k => k.startsWith('after:'));
  verdict('B repair plan: the state after an ok plan passes check() for the sets it makes', !afterBad.length, afterBad.map(k => found.get(k)).join(' | '));
}

/* ── C · SetEdit.verifyMoves (a person's move into / out of a COMMITTED set; the server's setMember reads the same), randomised: a move it accepts never adds a break of R1, R3, R9; R12 counted apart ── */
{
  const found = new Map(); let accepted = 0, moves = 0;
  const note = (k, d) => { if (!found.has(k)) found.set(k, d); };
  for (let k = 0; k < 800; k++) {
    const r = rng(77000 + k), w = randomWorld(r, k);
    for (const s of w.sets) Object.assign(s, { status: 'complete', committedAt: 9000 });      // (verifyMoves puts a sheet only into a committed set)
    for (const sh of w.sheets) if (sh.setId) sh.solidIncluded = sh.solidIncluded === undefined ? null : sh.solidIncluded;
    const mover = w.sheets[Math.floor(r() * w.sheets.length)], targets = [null, ...w.sets.map(s => s.setId)], to = targets[Math.floor(r() * targets.length)];
    if ((mover.setId || null) === to) continue;
    moves++;
    const recs = Object.fromEntries(w.sheets.map(s => [s.id, s])), sets = Object.fromEntries(w.sets.map(s => [s.setId, { doc: s, members: s.sheetIds.map(i => recs[i]) }]));
    const v = SE.verifyMoves({ moves: [{ id: mover.id, to }], recs: { [mover.id]: mover }, sets, others: w.sheets });
    if (process.env.AUDIT_SEED === String(77000 + k)) console.log(JSON.stringify({ mover: mover.id, to, v, sheets: w.sheets, sets: w.sets }, null, 1));
    if (!v.ok) continue;
    accepted++;
    const before = I.check(w.sets, w.sheets, { membership: false, unformed: false });
    const sheets2 = w.sheets.map(s => s.id === mover.id ? Object.assign({}, s, { setId: to, draft: !to, solidIncluded: to && ['gold10k', 'gold14k'].includes(s.metal) ? true : null }) : s);
    const sets2 = w.sets.map(s => Object.assign({}, s, { sheetIds: s.sheetIds.filter(i => i !== mover.id).concat(s.setId === to ? [mover.id] : []) }));
    const after = I.check(sets2, sheets2, { membership: false, unformed: false });
    const key = x => `${x.rule}:${x.set}:${x.sheets.filter(i => i !== mover.id).sort().join('+')}`, had = new Set(before.violations.map(key));
    // R9 read per ORDER on the moved sheet (rule A): an order on it whose other piece sits on a sheet that ends in a different set (a laser-cut sheet aside). A chain through a THIRD sheet outside
    // every set (a ready gold sheet that should have joined already) is that sheet's own fault, reported before the move as well: not counted here.
    const setAfter = id => { const s = sheets2.find(x => x.id === id); return (s && s.setId && !s.draft) ? s.setId : null; };
    const ordersOf = sh => new Set(sh.poolIds.map(k => k.split('_')[0]).filter(o => o.startsWith('72')));
    const direct = [...ordersOf(mover)].some(o => w.sheets.some(x => x.id !== mover.id && !(+x.laserDoneAt > 0) && ordersOf(x).has(o) && setAfter(x.id) !== setAfter(mover.id) && (x.setId || null) === (mover.setId || null)));
    for (const x of after.violations.filter(x => !had.has(key(x)) && ['R1', 'R3', 'R9', 'R12'].includes(x.ruleNo) && (x.ruleNo !== 'R9' || direct))) note(x.ruleNo, `seed ${77000 + k}: ${mover.id} (${mover.metal}${mover.roseCutAt ? ', cut' : ''}${mover.releaseFull === false ? ', not full' : ''}) ${mover.setId || 'loose'} -> ${to || 'out'}: ${x.message}`);
  }
  console.log(`      (moves: ${moves} random moves, ${accepted} accepted by SetEdit.verifyMoves)`);
  for (const rule of ['R1', 'R3', 'R9']) verdict(`C verifyMoves never accepts a move that adds an ${rule} break`, !found.has(rule), found.get(rule));
  verdict('C verifyMoves never accepts a gold-family sheet into a set without its cut or its yes (R12 a: the server has no green-line rule; the page adds the Cut Sheet press as a later step)', !found.has('R12'), found.get('R12'));
}

console.log(breaks.length ? `\n${breaks.length} BREAK(S): ${breaks.join(' | ')}` : '\nPASS: audit-gold-rules: no break found');
process.exit(breaks.length ? 1 : 0);
