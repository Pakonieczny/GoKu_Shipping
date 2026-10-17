// SETREPAIR (Paul, 17 Oct 2026: "I want you to fix it once and for all."): the explicit one-press repair of an uncommitted set that breaks the owner's rules, and the plain line that says a set is wrong.
// Fixtures only (made-up ids and order numbers; 4191315127 is the owner's Set 1 order); no network, no cloud, no Etsy. Needs `jsdom` (NODE_PATH) for parts 7-8 only.
//   1  the owner's 14-sheet Set 2: diagnose() says it in ONE plain line; plan() equals the correct formation (Set 2 = GF 1 + GF 2 + SS 1, Set 3, Set 4, GF 8 outside, RG 16 / RG 29 partners), checked BEFORE applying
//   2  every refusal: committed / completed / replaced / approved sets, a cut sheet that would move, a busy sheet, a run elsewhere, offline, a later set still forming; nothing else is ever touched
//   3  idempotent: the plan applied, the plan again says "already follows your rules"; a correct set shows no line and no button; Set 1 of the owner is untouched
//   4  the readiness gate: a flagged set is never ready, never in Laser cutting, its Approve button is grey with the rules named; a committed set is never blocked
//   5  property: random pools, the plan is always accepted by check() and its sets are exactly expected()'s
//   6  the words: the red line and the small window
//   7  the page: the real Gate assembly drives the repair (the owner's 14 sheets), numbering through the existing path, nothing deleted, Set 1 untouched, idempotent, no write when refused
//   8  the server: flowApply refuses to approve a set that breaks the rules; a committed set and a right set are not blocked
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const I = require('../../charm-nest-set-invariants.js'), SRp = require('../../charm-nest-set-repair.js'), SR = require('../../charm-nest-set-rules.js'), SN = require('../../charm-nest-sheet-name.js'), RD = require('../../charm-nest-readiness.js');
const clone = x => JSON.parse(JSON.stringify(x));

const piece = (order, n) => `${order}_${order}1_${n}`;
let single = 9000000000;
const own = k => Array.from({ length: k }, () => piece(++single, 1));
const link = (a, b, order) => { a.poolIds.push(piece(order, 1)); b.poolIds.push(piece(order, 2)); };
const sheet = (id, metal, no, o = {}) => ({ id, metal, page: no + 1, sheetIndex: no, releaseFull: true, placedCount: 40, density: .76, draft: false, poolIds: own(3), runId: 'run-1', ...o });
const show = r => I.explain(r).join('\n  ');
const sorted = l => l.slice().sort();

/* the owner's picture (docs/charm-nest/set-rules.md): GF Sheet 1-8, SS Sheet 1-4, RG Sheet 16, RG Sheet 29 in ONE uncommitted Set 2; chains A = SS4-GF7-RG29-GF6-SS3, B = GF5-RG16-GF4-SS2-GF3, C = SS1-GF2-GF1, GF 8 loose;
   `tie` is the unit the partial GF Draft 10 shares an order with. Set 1 (committed) = GF Sheet 1 + SS Sheet 1 of the earlier day, sharing order 4191315127. */
function owner2({ tie = null, setId = 'set-2', withSet1 = true, extra = {} } = {}) {
  const gf = {}, ss = {};
  for (let n = 1; n <= 8; n++) gf[n] = sheet('gf' + n, 'gold', n, { setId, density: .77 });
  for (let n = 1; n <= 4; n++) ss[n] = sheet('ss' + n, 'silver', n, { setId, density: .75 });
  const rg16 = sheet('rg16', 'rose', 16, { page: 16, setId, releaseFull: false, density: .58 }), rg29 = sheet('rg29', 'rose', 29, { page: 29, setId, releaseFull: false, density: .29 });
  const draft = sheet('gf-draft-10', 'gold', 0, { page: 10, draft: true, releaseFull: false, density: .62, sheetIndex: undefined });
  const chain = (order0, ...sheets) => sheets.slice(1).forEach((s, i) => link(sheets[i], s, order0 + i));
  chain(4200000400, ss[4], gf[7], rg29, gf[6], ss[3]);
  chain(4200000500, gf[5], rg16, gf[4], ss[2], gf[3]);
  chain(4200000600, ss[1], gf[2], gf[1]);
  const to = { A: gf[6], B: gf[4], C: gf[2] }[tie]; if (to) link(to, draft, 4200000999);
  const sheets = [...Object.values(gf), ...Object.values(ss), rg16, rg29, draft];
  const members = sheets.filter(s => s.setId === setId);
  const sets = [{ setId, id: setId, name: 'Set-2', seq: 2, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: members.map(s => s.id), sheetNos: { gold: 8, silver: 4, rose: 29 }, ...extra }];
  if (withSet1) {
    const g0 = sheet('s1-gf1', 'gold', 1, { page: 1, setId: 'set-1', runId: 'run-0' }), s0 = sheet('s1-ss1', 'silver', 1, { page: 1, setId: 'set-1', runId: 'run-0', density: .74 });
    link(g0, s0, 4191315127); sheets.push(g0, s0);
    sets.unshift({ setId: 'set-1', id: 'set-1', name: 'Set-1', seq: 1, runId: 'run-0', day: '2026-10-16', group: 'dispatch', status: 'complete', committedAt: 1760600000000, sheetIds: ['s1-gf1', 's1-ss1'] });
  }
  return { sheets, gf, ss, rg16, rg29, draft, sets, set: sets.find(s => s.setId === setId) };
}
const idsIn = (p, k) => sorted(p.sets[k].sheetIds);

/* ── 1 · the owner's Set 2 ────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const f = owner2();
  const d = SRp.diagnose(f.sets, f.sheets), info = d.bySet['set-2'];
  assert(info, 'Set 2 is called wrong');
  assert.equal(d.bySet['set-1'], undefined, 'Set 1 (committed, and right) is never judged');
  assert.equal(info.line, 'Set 2 breaks your rules: it is bigger than the minimum (GF Sheet 3, GF Sheet 4, GF Sheet 5, GF Sheet 6 and 7 more are not needed for it to be valid); Rose Gold sheets RG Sheet 16 and RG Sheet 29 are not cut yet (no green line)',
    'ONE plain line naming the rules and the sheets');
  assert.equal(info.short, 'breaks your rules: bigger than the minimum; RG Sheet 16 and RG Sheet 29 not cut yet');
  assert.deepEqual(sorted(info.rules), ['R12', 'R7']);
  assert.equal(info.line.split('\n').length, 1, 'one line');
  const b = SRp.breaksOf(d, 'set-2'); assert.deepEqual([b.line === info.line, b.items.length > 0, SRp.breaksOf(d, 'set-1')], [true, true, null]);

  const p = SRp.plan(f.set, f.sheets, f.sets, {});
  assert.equal(p.ok, true, JSON.stringify(p.refused));
  assert.equal(p.mode, 'trim', 'the first correct set is made of sheets the set already holds: they stay, nothing of them is touched');
  assert.deepEqual(sorted(p.keep), ['gf1', 'gf2', 'ss1']);
  assert.deepEqual(sorted(p.expel), ['gf3', 'gf4', 'gf5', 'gf6', 'gf7', 'gf8', 'rg16', 'rg29', 'ss2', 'ss3', 'ss4']);
  // the plan equals CharmNestSetInvariants.expected(): Set 2 = C, Set 3 = B, Set 4 = A, GF 8 waits, RG 16 and RG 29 are partners to add
  const pool = f.sheets.filter(s => !['s1-gf1', 's1-ss1'].includes(s.id)), e = I.expected(pool, { firstSeq: 2 });
  assert.deepEqual(p.sets.map(s => sorted(s.sheetIds)), e.sets.map(s => sorted(s.sheetIds)), 'exactly the formation the rules make');
  assert.deepEqual(p.sets.map(s => sorted(s.sheetIds)), [['gf1', 'gf2', 'ss1'], ['gf3', 'gf4', 'gf5', 'ss2'], ['gf6', 'gf7', 'ss3', 'ss4']]);
  assert.deepEqual(p.sets.map(s => s.name), ['Set 2', 'Set 3', 'Set 4'], 'the dissolved set\'s number goes to the first correct set, the next numbers follow it');
  assert.deepEqual(p.sets.map(s => s.setId), ['set-2', null, null], 'only the first keeps the set record; the others are allocated through the existing numbering path when made');
  assert.deepEqual(p.sets.map(s => !!s.predicted), [false, true, true]);
  assert.deepEqual(sorted(p.waiting.map(w => w.id)), ['gf8', 'rg16', 'rg29']);
  assert.deepEqual(p.partners.map(x => [x.set, x.sheet]).sort(), [['Set 3', 'rg16'], ['Set 4', 'rg29']]);
  assert.equal(p.after.ok, true); assert.equal(p.after.violations, 0);
  assert.match(p.signature, /^\["trim","set-2"/);
  // it was verified BEFORE applying: lay the plan on the state and ask the checker, with the rest of the shop around it
  const after = clone(f.sheets).map(s => { const k = p.sets.findIndex(x => x.sheetIds.includes(s.id)); return k >= 0 ? { ...s, setId: 'S' + k, draft: false } : ['s1-gf1', 's1-ss1'].includes(s.id) ? s : { ...s, setId: null, draft: true }; });
  const afterSets = p.sets.map((s, k) => ({ id: 'S' + k, name: s.name, seq: s.seq, sheetIds: s.sheetIds })).concat([f.sets[0]]);
  const verdict = I.check(afterSets, after, { skipExempt: true });
  assert.equal(verdict.ok, true, 'the rebuilt state is accepted by the checker:\n  ' + show(verdict));
  // nothing is deleted: every sheet of the shop is either kept, taken out as a draft, or in a new set; the plan never mentions a deletion
  assert.deepEqual(sorted([...p.sets.flatMap(s => s.sheetIds), ...p.waiting.map(w => w.id)]).filter(i => i !== 'gf-draft-10'), sorted(pool.map(s => s.id).filter(i => i !== 'gf-draft-10')), 'every sheet is in a set or waits: none is lost');
  assert(!/delet|remov/i.test(JSON.stringify(p.words.lines)));
  // Set 1 of the owner is untouched
  assert(!p.expel.includes('s1-gf1') && !p.expel.includes('s1-ss1') && !p.keep.includes('s1-gf1'));
  assert(!p.sets.some(s => s.sheetIds.some(i => i.startsWith('s1-'))), 'Set 1\'s sheets are in no rebuilt set');

  // the partial GF Draft 10 shares an order with one of the units: that unit (with its Rose Gold partner) waits whole outside every set until the draft is full; the others form
  const expectSets = { A: [['gf1', 'gf2', 'ss1'], ['gf3', 'gf4', 'gf5', 'ss2']], B: [['gf1', 'gf2', 'ss1'], ['gf6', 'gf7', 'ss3', 'ss4']], C: [['gf3', 'gf4', 'gf5', 'ss2'], ['gf6', 'gf7', 'ss3', 'ss4']] };
  for (const tie of ['A', 'B', 'C']) {
    const g = owner2({ tie }), q = SRp.plan(g.set, g.sheets, g.sets, {});
    assert.equal(q.ok, true, tie + ' ' + JSON.stringify(q.refused));
    assert.deepEqual(q.sets.map(s => sorted(s.sheetIds)), expectSets[tie], `tie ${tie}`);
    assert(q.held.length === 1 && q.held[0].ids.includes('gf-draft-10'), 'the group of the partial sheet waits whole');
    assert(q.words.lines.some(l => l.kind === 'wait' && /wait with their group until GF Draft 10 is full/.test(l.text)), JSON.stringify(q.words.lines));
  }
  // a Rose Gold sheet with its green line cut and its back engraving approved goes with its unit, and a cut sheet that has to move stops the repair (below)
  const g = owner2(); const rgc = sheet('rgc', 'rose', 3, { page: 3, setId: 'set-2', releaseFull: false, density: .5, roseCutAt: 5, backsApproved: true }); link(g.gf[1], rgc, 4200000700);
  g.sheets.push(rgc); g.set.sheetIds.push('rgc');
  const q = SRp.plan(g.set, g.sheets, g.sets, {}); assert.equal(q.ok, true, JSON.stringify(q.refused));
  assert.deepEqual(sorted(q.keep), ['gf1', 'gf2', 'rgc', 'ss1'], 'a CUT Rose Gold sheet of the unit that stays: it stays (a cut sheet is never moved)'); assert(!q.expel.includes('rgc'));
}

/* ── 2 · the refusals ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const refused = (f, key, o, re) => { const p = SRp.plan(f.set, f.sheets, f.sets, o || {}); assert.equal(p.ok, false, key); assert.equal(p.refused.key, key, JSON.stringify(p.refused)); if (re) assert.match(p.refused.reason, re); assert.deepEqual([p.keep, p.expel], [[], []], 'a refused plan changes nothing'); return p; };
  refused(owner2({ extra: { committedAt: 1760700000000, status: 'complete' } }), 'committed', {}, /Set 2 was sent to the Design Station, completed or replaced: a committed set is never rebuilt/);
  refused(owner2({ extra: { status: 'complete-with-holds' } }), 'committed');
  refused(owner2({ extra: { laserDoneAt: 123 } }), 'committed');
  refused(owner2({ extra: { status: 'superseded' } }), 'committed');
  refused(owner2({ extra: { processReady: true } }), 'approved', {}, /Set 2 is approved for laser cutting already: an approved set is never rebuilt/);
  refused(owner2({ extra: { processReady: true, processSeals: [{ id: 'x', how: 'laserReady', at: 5, by: 'Maria' }] } }), 'approved');
  { // a seal is history, not a state: a set that was ready once and has changed since is not approved; neither is a flag a sheet's own report contradicts
    const a = owner2({ extra: { processSeals: [{ id: 'x', how: 'laserReady', at: 5, by: 'Maria' }] } }); assert.equal(SRp.plan(a.set, a.sheets, a.sets, {}).ok, true, 'seals alone are history');
    const b = owner2({ extra: { processReady: true } }); b.gf[3].laser = { ready: false }; for (const s of b.sheets) if (s.setId === 'set-2' && !s.laser) s.laser = { ready: true };
    assert.equal(SRp.plan(b.set, b.sheets, b.sets, {}).ok, true, 'a stale processReady, when a sheet of the set is not ready now'); }
  // a sheet that was cut and would have to move: the set cannot be rebuilt around it
  { const f = owner2(); f.rg16.roseCutAt = 5; refused(f, 'cut', {}, /RG Sheet 16 was already cut: a cut sheet stays in its set/); }
  { const f = owner2(); f.gf[5].laserDoneAt = 99; refused(f, 'cut', {}, /GF Sheet 5 was already cut/); }
  // a sheet being nested or saved right now: wait
  { const f = owner2(); refused(f, 'busy', { busy: r => r.id === 'ss3' ? 'being nested or saved right now' : '' }, /^SS Sheet 3 is being nested or saved right now: try again when it is done\. Nothing was changed$/); }
  { const f = owner2(); const p = SRp.plan(f.set, f.sheets, f.sets, { busy: r => r.id === 'gf-draft-10' ? 'being nested' : '' }); assert.equal(p.ok, true, 'a loose sheet that is still filling is no concern of the repair'); }
  // the run that makes the set is not open on this screen, the page is offline, a commit is running
  refused(owner2(), 'run', { runHere: false }, /Set 2 is made by a run that is not open on this screen/);
  refused(owner2(), 'offline', { cloud: false }, /offline/);
  refused(owner2(), 'committing', { committing: true }, /committing a set right now/);
  // a later set of the run that is still forming would take the freed sheets before a new set could be numbered: refused with its name
  { const f = owner2(); f.sets.push({ setId: 'set-3', id: 'set-3', name: 'Set-3', seq: 3, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: [] }); refused(f, 'later', {}, /Set 3 comes after Set 2 in this run and is still forming/); }
  // a later set that has formed (a valid set) does not stop it: the new sets are numbered after it
  { const f = owner2(), a = sheet('l-gf', 'gold', 1, { setId: 'set-3', page: 40 }), b = sheet('l-ss', 'silver', 1, { setId: 'set-3', page: 41 }); f.sheets.push(a, b); f.sets.push({ setId: 'set-3', id: 'set-3', name: 'Set-3', seq: 3, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: ['l-gf', 'l-ss'] });
    const p = SRp.plan(f.set, f.sheets, f.sets, {}); assert.equal(p.ok, true, JSON.stringify(p.refused)); assert.deepEqual(p.sets.map(s => s.name), ['Set 2', 'Set 4', 'Set 5'], 'the shop\'s counter is past Set 3: the new sets are numbered after every set it knows'); assert(!p.expel.some(i => i.startsWith('l-')), 'Set 3 is untouched'); }
  // an order-mate in a COMMITTED set is fixed work (the app lets the work go on: bridge Gate 'fixed' groups): the open set is not at fault, so it is neither flagged for it nor refused (review V2, 17 Oct 2026: it was a dead end that also stopped the approval)
  { const f = owner2(); link(f.gf[1], f.sheets.find(s => s.id === 's1-gf1'), 4200000888);
    const p = SRp.plan(f.set, f.sheets, f.sets, {}); assert.equal(p.ok, true, JSON.stringify(p.refused)); assert.equal(I.check(f.sets, f.sheets, { skipExempt: true, unformed: false, membership: false }).violations.some(v => v.rule === 'cardinal'), false, 'no cardinal violation against the open set'); }
  // a rebuild that would still break a rule is refused BEFORE anything is applied (a group spread over TWO OPEN sets, which no formation of these sheets can mend)
  { const f = owner2(), a = sheet('o-gf', 'gold', 1, { setId: 'set-3', page: 50 }), b = sheet('o-ss', 'silver', 1, { setId: 'set-3', page: 51 }); f.sheets.push(a, b);
    f.sets.push({ setId: 'set-3', id: 'set-3', name: 'Set-3', seq: 3, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: ['o-gf', 'o-ss'] }); link(f.gf[1], a, 4200000999);
    const p = SRp.plan(f.set, f.sheets, f.sets, {}); assert.equal(p.ok, false); }
  // no such set / an empty set
  assert.equal(SRp.plan(null, [], []).refused.key, 'noSet');
  { const f = owner2(); const p = SRp.plan({ setId: 'set-77', seq: 77, sheetIds: [] }, f.sheets, f.sets, {}); assert.equal(p.refused.key, 'empty'); }
  // a set that is right shows no line and is not rebuilt
  { const gf = sheet('r-gf', 'gold', 1, { setId: 'set-5', page: 2 }), ss = sheet('r-ss', 'silver', 1, { setId: 'set-5', page: 3 }); link(gf, ss, 4300000001);
    const sets = [{ setId: 'set-5', id: 'set-5', name: 'Set-5', seq: 5, runId: 'run-1', day: '2026-10-17', group: 'dispatch', sheetIds: ['r-gf', 'r-ss'] }];
    const d = SRp.diagnose(sets, [gf, ss]); assert.deepEqual(d.list, [], 'a right set: no line'); assert.equal(SRp.breaksOf(d, 'set-5'), null); assert.equal(SRp.lineHtml({ setId: 'set-5' }), '', 'and no button');
    const p = SRp.plan(sets[0], [gf, ss], sets, {}); assert.deepEqual([p.ok, p.noop, p.refused.key, p.refused.reason], [false, true, 'right', 'Set 5 already follows your rules: nothing to rebuild']); }
  // the owner's own Set 1 (GF Sheet 1 + SS Sheet 1 sharing order 4191315127), uncommitted: right, nothing to do
  { const f = owner2(); f.sets[0].committedAt = null; f.sets[0].status = 'open'; const d = SRp.diagnose(f.sets, f.sheets); assert.equal(d.bySet['set-1'], undefined, 'Set 1 is right'); const p = SRp.plan(f.sets[0], f.sheets, f.sets, {}); assert.equal(p.noop, true); }
  // a set whose records carry no completion mark at all cannot be read: left alone, as SetRules.gate does
  { const f = owner2(); for (const s of f.sheets) delete s.releaseFull; assert.deepEqual(SRp.diagnose(f.sets, f.sheets).list, [], 'nothing to read, nothing said'); }
  // R1 (a set short of a class) is the existing "Waiting for ..." line, not a rebuild
  { const a = sheet('v-gf', 'gold', 1, { setId: 'set-6' }), b = sheet('v-gf2', 'gold', 2, { setId: 'set-6' }); const d = SRp.diagnose([{ setId: 'set-6', id: 'set-6', name: 'Set-6', seq: 6, sheetIds: ['v-gf', 'v-gf2'] }], [a, b]); assert.deepEqual(d.list, []); }
}

/* ── 3 · idempotent, a right set shows nothing, Set 1 untouched ──────────────────────────────────────────────────────────────────────────── */
/** The shop after a plan is applied, as the page leaves it: the first set keeps its record, the later ones are numbered after it, every other sheet of the plan is a draft. */
function applied(f, p) {
  const rows = clone(f.sheets), sets = clone(f.sets), top = Math.max(...sets.map(s => s.seq));
  const byId = new Map(rows.map(r => [r.id, r]));
  p.sets.forEach((s, k) => {
    const id = k === 0 ? p.setId : `set-${top + k}`;
    if (k === 0) sets.find(x => x.setId === id).sheetIds = s.sheetIds.slice(); else sets.push({ setId: id, id, name: `Set-${top + k}`, seq: top + k, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: s.sheetIds.slice() });
    for (const i of s.sheetIds) Object.assign(byId.get(i), { setId: id, draft: false });
  });
  const inNew = new Set(p.sets.flatMap(s => s.sheetIds));
  for (const i of p.expel) if (!inNew.has(i)) Object.assign(byId.get(i), { setId: null, draft: true });
  // the sheets of a new set are numbered from 1 in each metal (a set of its own)
  p.sets.forEach((s, k) => { if (k === 0) return; const no = {}; for (const i of s.sheetIds) { const r = byId.get(i); no[r.metal] = (no[r.metal] || 0) + 1; r.sheetIndex = no[r.metal]; } });
  return { sheets: rows, sets, set: sets.find(x => x.setId === p.setId) };
}
{
  const f = owner2({ tie: 'B' }), p = SRp.plan(f.set, f.sheets, f.sets, {});
  assert.equal(p.ok, true);
  const g = applied(f, p), checkAll = I.check(g.sets, g.sheets, { skipExempt: true, unformed: false, partialPartners: 'pending' });
  assert.equal(checkAll.ok, true, 'the state the plan leaves is accepted by the checker:\n  ' + show(checkAll));
  assert.deepEqual(SRp.diagnose(g.sets, g.sheets).list, [], 'no set says anything any more');
  const again = SRp.plan(g.set, g.sheets, g.sets, {});
  assert.deepEqual([again.ok, again.noop, again.refused.key], [false, true, 'right'], 'pressing it twice is the same as pressing it once: nothing is changed the second time');
  // ... also for the sets it made
  for (const s of g.sets.filter(s => !s.committedAt)) { const q = SRp.plan(s, g.sheets, g.sets, {}); assert.equal(q.noop, true, s.name + ' is right'); }
  assert.deepEqual(sorted(g.sets.find(s => s.setId === 'set-1').sheetIds), ['s1-gf1', 's1-ss1'], 'Set 1 (the committed one) still holds its two sheets');
  assert.deepEqual(g.sheets.filter(s => s.id.startsWith('s1-')).map(s => [s.setId, s.draft]), [['set-1', false], ['set-1', false]], 'and they are still in it');
}

/* ── 4 · the readiness gate: a set that breaks the rules is never approved ────────────────────────────────────────────────────────────────── */
{
  const ready = (id, metal, extra = {}) => ({ id, metal, sheetIndex: 1, page: 1, poolIds: [id + '1'], placedCount: 1, releaseFull: true, verification: { ok: true }, preview: 'p', outputs: { ai: 'f' }, label: { files: [] }, backPool: [], engraving: { [id + '1']: { needed: false, state: 'none', approved: true } }, updatedAt: 1, setId: 'set-1', draft: false, ...extra });
  const gf = ready('a-gf', 'gold'), ss = ready('a-ss', 'silver');
  const set = { setId: 'set-1', seq: 1, name: 'Set 1', sheetIds: ['a-gf', 'a-ss'], status: 'open' };
  const breaks = { items: [{ rule: 'notReady', ruleNo: 'R12', sheets: ['a-rg'], names: ['RG Sheet 1'], message: 'x' }], line: 'Set 1 breaks your rules: Rose Gold sheet RG Sheet 1 is not cut yet (no green line)', short: 'breaks your rules: RG Sheet 1 not cut yet' };
  // without the annotation the gate reads exactly what it always read
  const plain = RD.setGate(set, [gf, ss]); assert.equal(plain.ready, true, JSON.stringify(plain.blockers));
  // with it: not ready, the rules named first, the same words for the button, the board and the plan
  const flagged = { ...set, setBreaks: breaks }, gate = RD.setGate(flagged, [gf, ss]);
  assert.equal(gate.ready, false); assert.equal(gate.blockers[0].rule, 'setBreaks'); assert.equal(gate.reason, 'Set 1 · breaks your rules: RG Sheet 1 not cut yet'); assert.equal(gate.rules.applies && !gate.rules.ok, true);
  assert.equal(RD.laserGroup(flagged, [gf, ss]).ready, false, 'a set that breaks the rules is not in Laser cutting'); assert.equal(RD.set(flagged, [gf, ss]).ready, false);
  assert.equal(RD.laserGroup(set, [gf, ss]).rules.ok, true, 'the same set without the annotation reads as it always did'); assert.equal(RD.laserGroup(flagged, [gf, ss]).rules.ok, false);
  assert.match(RD.explain(flagged, { kind: 'set', sheets: [gf, ss] }).nextText, /^Set 1 breaks your rules/);
  // a committed / completed / replaced set is never blocked
  for (const extra of [{ committedAt: 5 }, { status: 'complete' }, { laserDoneAt: 9 }, { status: 'superseded' }]) { const g2 = RD.setGate({ ...flagged, ...extra }, [gf, ss]); assert.equal(g2.ready, true, JSON.stringify(extra)); }
  // the annotation is data: nothing in the set record is written
  assert.equal(JSON.stringify(set).includes('setBreaks'), false);
}

/* ── 5 · property: random pools ──────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
  let rebuilt = 0, right = 0, cutStops = 0;
  for (let seed = 1; seed <= 300; seed++) {
    const r = rng(seed * 17 + 3), n = 4 + Math.floor(r() * 12), sheets = [];
    for (let i = 0; i < n; i++) {
      const k = r(), no = i + 1;
      sheets.push(k < .42 ? sheet('g' + i, 'gold', no) : k < .78 ? sheet('s' + i, 'silver', no) : k < .86 ? sheet('p' + i, r() < .5 ? 'gold' : 'silver', no, { releaseFull: false, density: .5 })
        : sheet('x' + i, ['rose', 'gold14k', 'gold10k'][i % 3], no, { releaseFull: r() < .3, ...(r() < .5 ? { roseCutAt: 7, backsApproved: true } : {}) }));
    }
    for (let i = 0; i < Math.floor(r() * 5); i++) { const a = sheets[Math.floor(r() * n)], b = sheets[Math.floor(r() * n)]; if (a !== b) link(a, b, 5000000000 + seed * 100 + i); }
    const e = I.expected(sheets, { firstSeq: 1 }), all = e.sets.flatMap(s => s.sheetIds);
    if (!e.sets.length) continue;
    const members = new Set(all), rows = sheets.map(s => members.has(s.id) ? { ...s, setId: 'big', draft: false } : { ...s, draft: true });
    const big = { setId: 'big', id: 'big', name: 'Set-1', seq: 1, runId: 'run-1', day: '2026-10-17', group: 'dispatch', status: 'open', sheetIds: all };
    const d = SRp.diagnose([big], rows), p = SRp.plan(big, rows, [big], {});
    if (!d.bySet.big) { assert.equal(p.noop, true, `seed ${seed}: a right set is not rebuilt`); right++; continue; }
    if (!p.ok) { assert(['cut', 'right'].includes(p.refused.key), `seed ${seed}: refused for a reason a person can read: ${JSON.stringify(p.refused)}`); if (p.refused.key === 'cut') cutStops++; continue; }
    assert.deepEqual(p.sets.map(s => sorted(s.sheetIds)), I.expected(rows, { firstSeq: 1 }).sets.map(s => sorted(s.sheetIds)), `seed ${seed}: the plan is exactly the formation the rules make`);
    const g = applied({ sheets: rows, sets: [big], set: big }, p), c = I.check(g.sets, g.sheets, { skipExempt: true, unformed: false, partialPartners: 'pending' });
    assert.equal(c.ok, true, `seed ${seed}: the rebuilt state is accepted\n  ` + show(c));
    assert.equal(SRp.plan(g.set, g.sheets, g.sets, {}).noop, true, `seed ${seed}: idempotent`);
    rebuilt++;
  }
  assert(rebuilt > 40 && right > 3, `the random pools cover both outcomes (${rebuilt} rebuilt, ${right} right, ${cutStops} stopped by a cut sheet)`);
}
/* ── 7 · the page: the real Gate assembly (charm-nest-bridge.js) drives the repair over the owner's 14 sheets ─────────────────────────────────────────────────────────── */
const O = require('../../charm-nest-orders.js'), SO = require('../../charm-nest-shared-orders.js');
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
assert(gateAt > 0 && GATE.length > 1000, 'the page code to test was found');
const RUN = 'run-synthetic-1';
const X = n => `71000000${String(n).padStart(2, '0')}`;
function pageWorld(layout) {
  // layout: [{ id, metal, page, full, pieces:[pool ids], inSet, sheetIndex, density, over }]; orders shared by giving two sheets pieces of one order (copy 1 and copy 2)
  const writes = [], said = [];
  const c = vm.createContext({ assert, O, SO, SR, SN, console, RD, RUN, writes, allocations: 0 });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{rose:true}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-17', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),save:async()=>{},
  ensure:async(runId,group,o)=>{allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),status:'open',sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
`, c);
  vm.runInContext(GATE, c);
  const pageOf = s => ({ metal: s.metal, page: s.page, runId: RUN, sheetId: s.id, draft: !s.inSet, ...(s.inSet ? { setId: s.inSet, seq: +s.inSet.replace(/\D/g, ''), sheetIndex: s.sheetIndex || 1 } : {}), outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: s.density == null ? .74 : s.density,
    placements: s.pieces.map(id => ({ id })), charms: s.pieces.map(id => ({ id, poolId: id, order: id.split('_')[0] })), verification: { ok: true }, releaseFull: !!s.full, ...(s.over || {}) });
  const pages = layout.map(pageOf);
  const sets = [...new Set(layout.filter(s => s.inSet).map(s => s.inSet))];
  for (const setId of sets) {
    const n = +setId.replace(/\D/g, ''), meta = (layout.find(s => s.setMeta && s.inSet === setId) || {}).setMeta || {};
    c.__rec = { runId: RUN, group: 'dispatch', seq: n, day: '2026-10-17', setId, name: 'Set-' + n, status: 'open', sheetIds: layout.filter(s => s.inSet === setId).map(s => s.id), labelFiles: [], materials: ['gold', 'silver'], orders: {}, sheetNos: meta.sheetNos || {}, ...meta.doc };
    vm.runInContext('B.sets.set(__rec.setId, __rec); allocations=Math.max(allocations,__rec.seq); seq=Math.max(seq,__rec.seq+1);', c);
  }
  c.__p = pages; vm.runInContext('pages.push(...__p)', c);
  const rowOf = p => ({ id: p.sheetId, sheetId: p.sheetId, metal: p.metal, page: p.page, runId: p.runId, setId: p.setId || null, setSeq: p.seq || null, sheetIndex: p.sheetIndex || null, draft: !!p.draft, releaseFull: !!p.releaseFull, placedCount: p.placements.length, density: p.density, poolIds: p.charms.map(x => x.poolId), roseCutAt: p.roseCutAt || undefined, laserDoneAt: p.laserDoneAt || undefined });
  const w = {
    c, pages, writes, said, B: () => c.B, calls: 0, confirmed: [],
    page: id => pages.find(p => p.sheetId === id),
    sets: () => JSON.parse(vm.runInContext('JSON.stringify([...B.sets.values()].map(s=>({name:s.name,seq:s.seq,setId:s.setId,ids:s.sheetIds.slice(),committedAt:s.committedAt||null,group:s.group,runId:s.runId,status:s.status})))', c)),
    ids: set => set.ids.slice().sort(),
    rows: () => pages.map(rowOf),
    puts: () => writes.filter(x => x.n === 'charmNestLibrary' && x.b.op === 'putSheet').map(x => x.b.sheet),
    ops: () => writes.filter(x => x.n === 'charmNestLibrary').map(x => x.b.op),
    poolUpdates: () => writes.filter(x => x.ids),
    env(over = {}) {
      return {
        run: () => vm.runInContext('B.run', c), pages: () => pages, cloud: () => true, committing: () => false, holding: p => { c.__q = p; return vm.runInContext('Gate.holding(__q)', c); }, membershipBusy: () => false,
        setsOfRun: runId => vm.runInContext('[...B.sets.values()]', c).filter(s => s.runId === runId),
        api: async body => {
          if (body.op === 'setGet') { c.__q = body.setId; const s = vm.runInContext('B.sets.get(__q)', c); return { set: s ? JSON.parse(JSON.stringify(s)) : null }; }
          if (body.op === 'listSheets') return { sheets: w.rows() };
          throw new Error('unexpected op ' + body.op);
        },
        assemble: run => { w.calls++; return vm.runInContext('Gate.assemble(B.run)', c); },
        save: async () => { w.saved = (w.saved || 0) + 1; }, refresh: () => {}, say: (t, k) => said.push([t, k]),
        confirm: async p => { w.confirmed.push(p); return true; }, ...over
      };
    },
    group: setId => { c.__q = setId; const s = vm.runInContext('B.sets.get(__q)', c); return { setId, runId: s.runId, seq: s.seq, name: s.name }; }
  };
  return w;
}
const own1 = n => `${X(n)}_1_1`;
/* the owner's picture on the page: Set 1 (committed, earlier) = GF 0 + SS 0 sharing an order; Set 2 = GF 1-8, SS 1-4, RG 16, RG 29 with the chains A, B, C and GF 8 loose */
function ownerPage({ tie = null } = {}) {
  const L = []; let k = 0;
  const add = (id, metal, page, full, extra = {}) => { const s = { id, metal, page, full, pieces: [own1(++k)], inSet: 'set-2', sheetIndex: page, ...extra }; L.push(s); return s; };
  const gf = {}, ss = {}; for (let n = 1; n <= 8; n++) gf[n] = add('gf' + n, 'gold', n, true, { density: .77 }); for (let n = 1; n <= 4; n++) ss[n] = add('ss' + n, 'silver', n, true, { density: .75 });
  const rg16 = add('rg16', 'rose', 16, false, { density: .58, sheetIndex: 16 }), rg29 = add('rg29', 'rose', 29, false, { density: .29, sheetIndex: 29 });
  const draft = add('gf-draft-10', 'gold', 10, false, { density: .62, inSet: null, sheetIndex: undefined }); draft.over = {};
  let o = 20; const lnk = (a, b) => { o++; a.pieces.push(`${X(o)}_1_1`); b.pieces.push(`${X(o)}_1_2`); };
  [[ss[4], gf[7]], [gf[7], rg29], [rg29, gf[6]], [gf[6], ss[3]], [gf[5], rg16], [rg16, gf[4]], [gf[4], ss[2]], [ss[2], gf[3]], [ss[1], gf[2]], [gf[2], gf[1]]].forEach(([a, b]) => lnk(a, b));
  if (tie) lnk({ A: gf[6], B: gf[4], C: gf[2] }[tie], draft);
  const g0 = { id: 's1-gf1', metal: 'gold', page: 1, full: true, pieces: [own1(90), `${X(60)}_1_1`], inSet: 'set-1', sheetIndex: 1, setMeta: { doc: { committedAt: 1760600000000, status: 'complete' } } }, s0 = { id: 's1-ss1', metal: 'silver', page: 1, full: true, pieces: [own1(91), `${X(60)}_1_2`], inSet: 'set-1', sheetIndex: 1, density: .74 };
  L.unshift(g0, s0);
  return L;
}
/* ── review V2 (17 Oct 2026): what the first version got wrong ─────────────────────────────────────────────────────────────────────────── */
{
  // a set approved for laser cutting NOW, or already partly laser-cut, is history: not flagged, not held back from Laser cutting, not rebuilt
  { const f = owner2({ extra: { processReady: true } }); assert.deepEqual(SRp.diagnose(f.sets, f.sheets).list, [], 'an approved set says nothing'); const p = SRp.plan(f.set, f.sheets, f.sets, {}); assert.equal(p.refused.key, 'approved'); }
  { const f = owner2(); f.gf[3].laserDoneAt = 99; assert.deepEqual(SRp.diagnose(f.sets, f.sheets).list, [], 'a set with a laser-cut sheet says nothing'); assert.equal(SRp.plan(f.set, f.sheets, f.sets, {}).refused.key, 'cut', 'and the press still says why it cannot be rebuilt'); }
  // a person's tick is the yes of R12(a): a ticked 14K sheet (saved as solidIncluded: true, no cut recorded) is ready; unticked, it is not
  for (const ticked of [true, false]) {
    const gf = sheet('t-gf', 'gold', 1, { setId: 'set-9' }), ss = sheet('t-ss', 'silver', 1, { setId: 'set-9' }), k = sheet('t-k', 'gold14k', 1, { setId: 'set-9', releaseFull: false, density: .2, backsApproved: true, ...(ticked ? { solidIncluded: true } : {}) });
    link(gf, ss, 4200009001); link(gf, k, 4200009002);
    const d = SRp.diagnose([{ setId: 'set-9', id: 'set-9', name: 'Set-9', seq: 9, runId: 'run-1', status: 'open', sheetIds: ['t-gf', 't-ss', 't-k'] }], [gf, ss, k]);
    assert.equal(d.list.some(i => i.rules.includes('R12')), !ticked, ticked ? 'a ticked 14K sheet is not "not cut yet"' : 'an unticked, uncut 14K sheet in a set still is');
  }
}
(async () => {
  const done = [];
  /* A · the owner's Set 2 */
  {
    const w = pageWorld(ownerPage()), env = w.env();
    const before = JSON.stringify(w.pages.map(p => p.charms.map(x => x.poolId + '@' + p.sheetId)));
    const pre = w.sets(); assert.deepEqual(pre.map(s => [s.name, s.ids.length]), [['Set-1', 2], ['Set-2', 14]], 'the owner\'s picture: Set 2 holds 14 sheets');
    // the card says so: the explicit diagnosis over what the cloud holds
    const world = await env.api({ op: 'listSheets' }), diag0 = SRp.diagnose(w.sets().map(s => ({ ...s, sheetIds: s.ids })), world.sheets);
    assert.deepEqual(Object.keys(diag0.bySet), ['set-2'], 'Set 1 is right and committed: no line; Set 2 breaks the rules');
    // press: the window asks, with the plan
    const r = await SRp.rebuild(w.group('set-2'), { env });
    assert.equal(r.ok, true, JSON.stringify([r.error, r.refused, r.still, w.said]));
    assert.equal(w.confirmed.length, 1, 'the small window asked once, with the plan'); assert.equal(w.confirmed[0].words.title, 'Rebuild Set 2 correctly?');
    assert.equal(w.calls, 2, 'the run\'s own assembly ran twice: sheets leave (once, all at once), then the next sets form');
    const sets = w.sets();
    assert.deepEqual(sets.map(s => [s.name, w.ids(s)]), [['Set-1', ['s1-gf1', 's1-ss1']], ['Set-2', ['gf1', 'gf2', 'ss1']], ['Set-3', ['gf3', 'gf4', 'gf5', 'ss2']], ['Set-4', ['gf6', 'gf7', 'ss3', 'ss4']]],
      'Set 2 = GF 1 + GF 2 + SS 1 under its own number; Set 3 and Set 4 were numbered through the existing path (Sets.ensure) in order; Set 1 is untouched');
    assert.equal(sets[1].setId, 'set-2', 'the dissolved set kept its record and its number');
    // where every sheet is now
    for (const id of ['gf1', 'gf2', 'ss1']) assert.deepEqual([w.page(id).setId, w.page(id).draft, w.page(id).sheetIndex], ['set-2', false, id === 'gf2' ? 2 : 1], id + ' stays where it was, with its number');
    for (const id of ['gf3', 'gf4', 'gf5', 'ss2']) assert.deepEqual([w.page(id).setId, w.page(id).draft], ['set-3', false], id);
    for (const id of ['gf6', 'gf7', 'ss3', 'ss4']) assert.deepEqual([w.page(id).setId, w.page(id).draft], ['set-4', false], id);
    assert.equal(w.page('gf3').sheetIndex, 1); assert.equal(w.page('gf4').sheetIndex, 2); assert.equal(w.page('ss2').sheetIndex, 1, 'a sheet that joins a new set takes the next number of its metal in it (charm-nest-sheet-name.js)');
    assert.deepEqual([w.page('gf8').setId || null, w.page('gf8').draft], [null, true]); assert.match(w.page('gf8').setWait, /needs a completed SS sheet with it$/, 'GF 8 waits outside every set, with the plain line');
    for (const id of ['rg16', 'rg29']) { const p = w.page(id); assert.deepEqual([p.setId || null, p.draft, p.leftSet], [null, true, 'set-2'], id + ' waits for its green line: out of every set until you Include / Cut it'); }
    assert.deepEqual([w.page('s1-gf1').setId, w.page('s1-ss1').setId, w.page('s1-gf1').draft], ['set-1', 'set-1', false], 'Set 1 is untouched');
    // the checker accepts what is there now; no set says anything
    const rows = w.rows(), after = w.sets().map(s => ({ ...s, setId: s.setId, id: s.setId, sheetIds: s.ids }));
    const verdict = I.check(after, rows, { skipExempt: true, unformed: false, partialPartners: 'pending' }); assert.equal(verdict.ok, true, show(verdict));
    assert.deepEqual(SRp.diagnose(after, rows).list, []);
    // nothing deleted, nothing moved between sheets, Set 1 never written
    assert.equal(JSON.stringify(w.pages.map(p => p.charms.map(x => x.poolId + '@' + p.sheetId))), before, 'every piece is on the sheet it was on');
    assert(!w.ops().some(op => /delete|archive|restore|purge/i.test(op)), 'no sheet and no file is deleted: ' + w.ops().join(','));
    assert(w.puts().every(s => !['gf1', 'gf2', 'ss1', 's1-gf1', 's1-ss1'].includes(s.id)), 'the sheets that stay (and Set 1) were never written');
    assert(w.puts().filter(s => s.draft === true).map(s => s.id).sort().join() === 'gf3,gf4,gf5,gf6,gf7,gf8,rg16,rg29,ss2,ss3,ss4', 'the sheets that left were saved as drafts the way the run saves any sheet that leaves a set');
    assert(w.poolUpdates().every(u => !u.ids.some(i => /^7100009[01]_/.test(i)) && !u.ids.some(i => i.startsWith(X(60)))), 'the pieces of Set 1 were not touched');
    assert(w.confirmed[0].words.summary.startsWith('Set 2 becomes Set 2: GF Sheet 1, GF Sheet 2, SS Sheet 1 · Set 3: '));
    // pressed again: nothing to do, nothing written
    const n0 = w.writes.length, again = await SRp.rebuild(w.group('set-2'), { env: w.env() });
    assert.deepEqual([again.ok, again.noop, again.refused.key], [false, true, 'right']); assert.equal(w.writes.length, n0, 'idempotent: not a single write'); assert.equal(w.confirmed.length, 1, 'and no window');
    for (const s of ['set-3', 'set-4']) { const q = await SRp.rebuild(w.group(s), { env: w.env() }); assert.equal(q.noop, true, s + ' is right'); }
    done.push('A');
  }
  /* B · a refusal writes nothing at all */
  {
    const w = pageWorld(ownerPage()); const n0 = () => w.writes.length;
    // the committed Set 1 is never rebuilt
    let r = await SRp.rebuild(w.group('set-1'), { env: w.env() }); assert.equal(r.refused.key, 'committed'); assert.equal(n0(), 0); assert.equal(w.calls, 0);
    // a sheet being nested right now
    w.page('ss3').status = 'nesting'; r = await SRp.rebuild(w.group('set-2'), { env: w.env() }); assert.equal(r.refused.key, 'busy'); assert.match(r.refused.reason, /^SS Sheet 3 is being nested or saved right now: try again when it is done/); assert.equal(n0(), 0); w.page('ss3').status = 'complete';
    // a sheet being changed in its sheet window
    w.page('gf5').keepRelease = { full: true, at: Date.now() }; r = await SRp.rebuild(w.group('set-2'), { env: w.env() }); assert.equal(r.refused.key, 'busy'); assert.match(r.refused.reason, /GF Sheet 5 is being changed in its sheet window/); delete w.page('gf5').keepRelease;
    // the run is open on another screen / this page holds another run
    r = await SRp.rebuild(w.group('set-2'), { env: w.env({ run: () => ({ runId: 'another-run' }) }) }); assert.equal(r.refused.key, 'run'); assert.equal(n0(), 0);
    // offline, committing
    r = await SRp.rebuild(w.group('set-2'), { env: w.env({ cloud: () => false }) }); assert.equal(r.refused.key, 'offline');
    r = await SRp.rebuild(w.group('set-2'), { env: w.env({ committing: () => true }) }); assert.equal(r.refused.key, 'committing'); assert.equal(n0(), 0);
    // Cancel in the window: nothing is changed
    r = await SRp.rebuild(w.group('set-2'), { env: w.env({ confirm: async () => false }) }); assert.deepEqual([r.ok, r.cancelled], [false, true]); assert.equal(n0(), 0); assert.equal(w.calls, 0);
    // the shop changed while the window was open: nothing is changed, and it says so
    let first = true; r = await SRp.rebuild(w.group('set-2'), { env: w.env({ confirm: async () => { w.page('gf8').status = 'nesting'; w.page('gf8').releaseFull = false; w.page('ss4').status = 'nesting'; return true; } }) });
    assert.equal(r.ok, false); assert.equal(n0(), 0, 'a plan that no longer holds writes nothing'); w.page('ss4').status = 'complete'; w.page('gf8').releaseFull = true; w.page('gf8').status = 'complete';
    // the run keeps a sheet (nothing lets it go): the marks are put back, and it says which sheet and why
    const kept = w.env({ assemble: async () => {} }); r = await SRp.rebuild(w.group('set-2'), { env: kept });
    assert.equal(r.ok, false); assert.match(r.error, /could not be taken out of Set 2/);
    for (const p of w.pages) assert.deepEqual([p.leftSet, p.draft && p.sheetId.startsWith('gf-draft') ? 1 : p.draft], [undefined, p.sheetId === 'gf-draft-10' ? 1 : false], p.sheetId + ' is as it was');
    assert(w.said.some(([t]) => /Nothing was deleted/.test(t)), 'and it tells the person nothing was deleted');
    done.push('B');
  }
  /* C · a partial GF Draft 10 shares an order with unit B: that unit waits outside every set whole; the others form */
  {
    const w = pageWorld(ownerPage({ tie: 'B' })), r = await SRp.rebuild(w.group('set-2'), { env: w.env() });
    assert.equal(r.ok, true, JSON.stringify([r.error, r.refused, r.still, w.said]));
    assert.deepEqual(w.sets().map(s => [s.name, w.ids(s)]), [['Set-1', ['s1-gf1', 's1-ss1']], ['Set-2', ['gf1', 'gf2', 'ss1']], ['Set-3', ['gf6', 'gf7', 'ss3', 'ss4']]]);
    for (const id of ['gf3', 'gf4', 'gf5', 'ss2']) assert(w.page(id).draft && !w.page(id).setId, id + ' waits with its group');
    const rows = w.rows(), sets = w.sets().map(s => ({ ...s, id: s.setId, sheetIds: s.ids }));
    assert.equal(I.check(sets, rows, { skipExempt: true, unformed: false, partialPartners: 'pending' }).ok, true);
    done.push('C');
  }
  /* D · a loose completed pair of the run (completed later than every sheet of Set 2) makes its set AFTER the rebuilt ones, in the order the plan said: the sheets that leave are not left to wait for it */
  {
    const layout = ownerPage(); layout.push({ id: 'l-gf', metal: 'gold', page: 20, full: true, pieces: [own1(80)], inSet: null, sheetIndex: undefined }, { id: 'l-ss', metal: 'silver', page: 21, full: true, pieces: [own1(81)], inSet: null, sheetIndex: undefined });
    const w = pageWorld(layout), r = await SRp.rebuild(w.group('set-2'), { env: w.env() });
    assert.equal(r.ok, true, JSON.stringify([r.error, r.refused, r.still, w.said]));
    assert.deepEqual(r.plan.sets.map(s => [s.name, sorted(s.sheetIds)]), [['Set 2', ['gf1', 'gf2', 'ss1']], ['Set 3', ['gf3', 'gf4', 'gf5', 'ss2']], ['Set 4', ['gf6', 'gf7', 'ss3', 'ss4']], ['Set 5', ['gf8', 'l-ss']]]);
    assert.deepEqual(w.sets().map(s => [s.name, w.ids(s)]), [['Set-1', ['s1-gf1', 's1-ss1']], ['Set-2', ['gf1', 'gf2', 'ss1']], ['Set-3', ['gf3', 'gf4', 'gf5', 'ss2']], ['Set-4', ['gf6', 'gf7', 'ss3', 'ss4']], ['Set-5', ['gf8', 'l-ss']]], 'the sets were made in the order the plan said: GF 8 (it was loose after the rebuild) pairs with the loose SS, last');
    assert(w.page('l-gf').draft && !w.page('l-gf').setId, 'the loose GF that has no SS left waits');
    assert.equal(w.page('l-gf').leftSet, undefined, 'the loose sheets are not left marked');
    done.push('D');
  }
  console.log('part 7 ok: ' + done.join(' '));

  /* ── 6 · what the owner sees: the Library groups carry the line, the card draws it with ONE button, the small window says what will happen ─────────────────────────────────── */
  {
    const w = pageWorld(ownerPage()), rows = w.rows(), rawSets = w.sets().map(s => ({ ...s, id: s.setId, sheetIds: s.ids, day: '2026-10-17' }));
    const LG = code.slice(code.indexOf('function libraryGroups(sets, sheets)'), code.indexOf('let _cache = null, libraryRequest'));
    assert(LG.startsWith('function libraryGroups') && /function partnersOf\(list\)/.test(LG), 'the page code to test was found');
    const lc = vm.createContext({ SO, O, console, SetRepair: SRp, _cache: { sheets: rows }, window: { CharmNestSetRules: SR, SetRepair: SRp, SharedOrders: { core: SO.core, sheets: () => rows.map(r => SO.core.sheetOf(r, { label: SN.name(r) })).filter(Boolean) } }, allSheets: () => [] });
    vm.runInContext(LG, lc);
    lc.__s = rawSets.map(s => ({ ...s, sheetIds: s.ids, sheets: undefined })); lc.__r = rows;
    const groups = vm.runInContext('libraryGroups(__s, __r)', lc), g2 = groups.find(g => g.setId === 'set-2'), g1 = groups.find(g => g.setId === 'set-1');
    assert(g2 && g2.setBreaks, 'Set 2 carries the rules it breaks'); assert.equal(g1.setBreaks, undefined, 'Set 1 (committed) carries nothing');
    assert(g2.cardinalLinks && /sheets that share an order stay in one set$/.test(g2.cardinalLinks) && !/no order with the other sheets/.test(g2.cardinalLinks), 'the older "share no order ... valid without them" sentence gives way to the red line (it named whole units as if each sheet stood alone): ' + g2.cardinalLinks);
    assert.equal(g2.setBreaks.line.startsWith('Set 2 breaks your rules: it is bigger than the minimum ('), true);
    const html = SRp.lineHtml(g2);
    assert.match(html, /role="note"/); assert.match(html, /data-set-break="set-2"/); assert.match(html, /<button type="button" class="btn sm" data-rebuild-set="set-2">Rebuild this set correctly<\/button>/);
    // (SETFIX A, Paul 17 Oct 2026) its uncut Rose Gold sheets have their own press, "Cut ...", before the rebuild, and the set says exactly what to press, in that order
    assert.equal((html.match(/<button/g) || []).length, 2, 'the Cut press and the rebuild'); assert.equal((html.match(/data-set-break-line/g) || []).length, 1, 'ONE line');
    assert.match(html, /<button type="button" class="btn sm" data-cut-set="set-2">Cut RG Sheet 16 and RG Sheet 29<\/button><button type="button" class="btn sm" data-rebuild-set="set-2">/);
    assert.match(html, /data-set-break-todo>Press &quot;Cut RG Sheet 16 and RG Sheet 29&quot; first \(the Cut Sheet press: it draws their green lines and records their cuts\), then &quot;Rebuild this set correctly&quot;\.</);
    assert.equal(SRp.lineHtml(g1), '', 'a right or committed set: no line, no button');
    // the card, the gate and the board read the same annotation
    const members = rows.filter(r => r.setId === 'set-2');
    assert.equal(RD.setGate(g2, members).ready, false); assert.match(RD.setGate(g2, members).reason, /^Set-2 · breaks your rules: bigger than the minimum; RG Sheet 16 and RG Sheet 29 not cut yet/, 'the set\'s name as every other reason of the gate says it');
    assert.equal(RD.laserGroup(g2, members).ready, false);
    // the bridge wires it: the groups are annotated where they are made, the card draws the line under the sheets and wires the button
    assert(code.includes('SetRepair.annotate([...groups.values()]') && code.includes('${window.SetRepair ? SetRepair.lineHtml(st) : ""}') && code.includes('SetRepair.wire(card, st)'), 'charm-nest-bridge.js draws and wires it');
    const html1 = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8'), pub = fs.readFileSync(path.join(__dirname, '../../scripts/build-public.cjs'), 'utf8');
    for (const f of ['charm-nest-set-invariants.js', 'charm-nest-set-repair.js']) { assert(html1.includes(`<script src="${f}?v=`), f + ' is loaded by the page'); assert(pub.includes(`"${f}"`), f + ' is published'); assert(fs.existsSync(path.join(__dirname, '../..', f))); }
    assert(html1.indexOf('charm-nest-set-invariants.js') > html1.indexOf('charm-nest-set-rules.js') && html1.indexOf('charm-nest-set-repair.js') > html1.indexOf('charm-nest-set-invariants.js') && html1.indexOf('charm-nest-set-repair.js') < html1.indexOf('<script src="charm-nest-bridge.js'), 'after the rules it reads, before the bridge');
    // the module as a page loads it (window globals, no require); Gate.violations(run) is used when the page has it, and ignored when it answers in a shape this page does not read
    {
      const make = gate => {
        const ctx = vm.createContext({}); ctx.self = ctx; ctx.B = { run: { runId: RUN } }; if (gate) ctx.Gate = gate;
        for (const f of ['charm-nest-sheet-name.js', 'charm-nest-set-rules.js', 'charm-nest-shared-orders.js', 'charm-nest-set-invariants.js', 'charm-nest-set-repair.js']) vm.runInContext(fs.readFileSync(path.join(__dirname, '../..', f), 'utf8'), ctx, { filename: f });
        return ctx;
      };
      const groupsFor = () => rawSets.filter(s => s.setId === 'set-2' || s.setId === 'set-1').map(s => ({ setId: s.setId, name: s.name, seq: s.seq, sheets: [] }));
      const docs = clone(rawSets.map(s => ({ ...s, sheetIds: s.ids }))), rows2 = clone(rows);
      let ctx = make(null), gs = groupsFor(); ctx.SetRepair.annotate(gs, rows2, docs);
      assert.deepEqual(clone(gs.find(g => g.setId === 'set-2').setBreaks.rules).sort(), ['R12', 'R7'], 'no Gate.violations on the page: the checker is called directly');
      assert.equal(gs.find(g => g.setId === 'set-1').setBreaks, undefined);
      // the page's own answer for the run's sets: its words win for the sets it names
      const fake = [{ rule: 'surplus', ruleNo: 'R7', letter: 'b', id: 'x', set: 'set-2', setName: 'Set 2', sheets: ['gf8'], names: ['GF Sheet 8'], message: 'GF Sheet 8 shares no order with the others and Set 2 is valid without it' }];
      for (const shape of [{ ok: false, violations: fake }, fake]) {
        let asked = null; ctx = make({ violations: run => { asked = run; return shape; } }); gs = groupsFor(); ctx.SetRepair.annotate(gs, rows2, docs);
        assert.equal(asked && asked.runId, RUN, 'Gate.violations(run) was asked for the open run');
        assert.deepEqual(clone(gs.find(g => g.setId === 'set-2').setBreaks.rules), ['R7'], 'its answer is the one shown (a checker result or a plain list)');
        assert.match(gs.find(g => g.setId === 'set-2').setBreaks.line, /^Set 2 breaks your rules: it is bigger than the minimum \(GF Sheet 8 is not needed for it to be valid\)$/);
      }
      for (const odd of [() => { throw new Error('boom'); }, () => 'nothing', () => Promise.resolve({ violations: fake }), () => ({ violations: 'no' })]) {
        ctx = make({ violations: odd }); gs = groupsFor(); ctx.SetRepair.annotate(gs, rows2, docs);
        assert.deepEqual(clone(gs.find(g => g.setId === 'set-2').setBreaks.rules).sort(), ['R12', 'R7'], 'an answer in another shape (or a failure) is ignored: the checker is called directly');
      }
    }
    // the small window and the button, in a DOM
    let JSDOM = null; try { JSDOM = require('jsdom').JSDOM; } catch (_) { JSDOM = null; }
    if (JSDOM) {
      const dom = new JSDOM('<body><main id="libBody"><div class="setCard"></div></main></body>', { pretendToBeVisual: true }), doc = dom.window.document;
      globalThis.document = doc;
      try {
        const card = doc.querySelector('.setCard'); card.innerHTML = html; assert.equal(card.querySelectorAll('[data-rebuild-set]').length, 1);
        assert.equal(card.querySelector('[data-set-break-line]').textContent, g2.setBreaks.line, 'the red line reads as the plan says');
        const plan = SRp.plan({ ...rawSets.find(s => s.setId === 'set-2'), setId: 'set-2' }, rows, rawSets, {});
        assert.equal(plan.ok, true);
        const ask = SRp.confirmDialog(plan), dlg = doc.querySelector('dialog.setRepairDlg');
        assert(dlg, 'the window opened'); assert.equal(dlg.querySelector('h3').textContent, 'Rebuild Set 2 correctly?');
        const items = [...dlg.querySelectorAll('li')].map(li => li.textContent);
        assert.equal(items[0], 'Set 2: GF Sheet 1, GF Sheet 2, SS Sheet 1'); assert(items[1].startsWith('Set 3: GF Sheet 1 (was GF Sheet 3), GF Sheet 2 (was GF Sheet 4), GF Sheet 3 (was GF Sheet 5), SS Sheet 1 (was SS Sheet 2)'), items[1]); assert(items[2].startsWith('Set 4: '));
        assert(items.some(t => /^GF Draft 8 \(was GF Sheet 8\) waits for a completed SS sheet$/.test(t)) && items.some(t => /RG Draft 16 \(was RG Sheet 16\) and RG Draft 29 \(was RG Sheet 29\) wait for their green line$/.test(t)), items.join(' | '));
        assert.equal(dlg.querySelector('.nothing').textContent.startsWith('Nothing is deleted'), true);
        assert.deepEqual([...dlg.querySelectorAll('.dlgFoot button')].map(b => b.textContent), ['Cancel', 'Rebuild']);
        dlg.querySelector('[data-k=cancel]').click(); assert.equal(await ask, false); assert.equal(doc.querySelector('dialog.setRepairDlg'), null, 'Cancel closes it and changes nothing');
        const ask2 = SRp.confirmDialog(plan); doc.querySelector('[data-k=go]').click(); assert.equal(await ask2, true);
        // the button: pressed, it rebuilds through the page's own state (here the real Gate over the owner's sheets)
        const w2 = pageWorld(ownerPage()), env = w2.env(); let seen = null;
        card.innerHTML = html; SRp.wire(card, { setId: 'set-2', runId: RUN, seq: 2 }, { env: { ...env, confirm: async p => { seen = p; return true; } } });
        card.querySelector('[data-rebuild-set]').click();
        for (let i = 0; i < 400 && w2.sets().length < 4; i++) await new Promise(r => setTimeout(r, 5));
        assert.equal(w2.sets().length, 4, 'one press: Set 2, Set 3 and Set 4'); assert(seen && seen.ok, 'the window showed the plan first');
        for (let i = 0; i < 100 && card.querySelector('[data-rebuild-set]').disabled; i++) await new Promise(r => setTimeout(r, 5));
      } finally { delete globalThis.document; }
    } else console.log('(jsdom is not installed here: the small window is checked only through its words)');
    assert.equal(plan_words(), true);
    function plan_words() { const p = SRp.plan(rawSets.find(s => s.setId === 'set-2'), rows, rawSets, {}); const wd = p.words; assert.equal(wd.confirm, 'Rebuild'); assert.equal(wd.cancel, 'Cancel'); assert(wd.summary.endsWith('Nothing is deleted.')); assert(wd.summary.startsWith('Set 2 becomes Set 2: GF Sheet 1, GF Sheet 2, SS Sheet 1 · Set 3: ')); return true; }
  }

  /* ── 8 · the server: a set that breaks the rules is never approved; a committed set and a right set are not blocked ──────────────────────────────────────────────────────── */
  {
    const { start } = require('./bridge-server.cjs');
    const SH = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs';
    const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-17';
    const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
    const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
    const lines = {}; let n = 0;
    // a sheet that is ready to be approved (nothing to engrave, label made), one order of its own, optionally sharing `link` with another sheet
    const mk = (id, o = {}) => {
      const order = String(3900000000 + ++n), pool = [order + '_1_1'], { link, metal, ...rest } = o;
      lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: pool.slice(), engraveCandidate: false };
      const orders = [order];
      if (link) { const [lo, copy] = link; pool.push(`${lo}_1_${copy}`); orders.push(lo); lines[lo + '_1'] = lines[lo + '_1'] || { orderId: lo, state: 'written', quantity: 2, poolIds: [], engraveCandidate: false }; lines[lo + '_1'].poolIds.push(`${lo}_1_${copy}`); }
      st.put(SH, id, { id, runId: 'run-s', metal: metal || 'gold', day, status: 'complete', placedCount: pool.length, charmCount: pool.length, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: pool, orders, verification: { ok: true }, releaseFull: true, page: 1,
        outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } }, label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders }], orders }, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
    };
    const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, ''), day, runId: 'run-s', group: 'dispatch', sheetIds, materials: ['gold', 'silver'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts, ...extra });
    // Set 10: GF + SS + a second GF that no order ties to the others (bigger than the minimum)
    mkSet('set-10', ['b-gf1', 'b-ss1', 'b-gf2']); mk('b-gf1', { setId: 'set-10', setSeq: 10, page: 1, sheetIndex: 1 }); mk('b-ss1', { setId: 'set-10', setSeq: 10, metal: 'silver', page: 1, sheetIndex: 1 }); mk('b-gf2', { setId: 'set-10', setSeq: 10, page: 2, sheetIndex: 2 });
    // Set 11: GF + SS sharing an order: exactly the minimum
    mkSet('set-11', ['r-gf', 'r-ss']); mk('r-gf', { setId: 'set-11', setSeq: 11, link: ['3999000001', 1] }); mk('r-ss', { setId: 'set-11', setSeq: 11, metal: 'silver', link: ['3999000001', 2] });
    // Set 12: the same shape as Set 10, but committed to the Design Station: never judged
    mkSet('set-12', ['c-gf1', 'c-ss1', 'c-gf2'], { committedAt: now - 5000, status: 'complete' }); mk('c-gf1', { setId: 'set-12', setSeq: 12, page: 1, sheetIndex: 1 }); mk('c-ss1', { setId: 'set-12', setSeq: 12, metal: 'silver', page: 1, sheetIndex: 1 }); mk('c-gf2', { setId: 'set-12', setSeq: 12, page: 2, sheetIndex: 2 });
    // Set 13: GF + SS and a Rose Gold sheet that has no green line cut
    mkSet('set-13', ['g-gf', 'g-ss', 'g-rg']); mk('g-gf', { setId: 'set-13', setSeq: 13, link: ['3999000002', 1] }); mk('g-ss', { setId: 'set-13', setSeq: 13, metal: 'silver', link: ['3999000003', 1] }); mk('g-rg', { setId: 'set-13', setSeq: 13, metal: 'rose', releaseFull: false, density: .5, link: ['3999000002', 2] });
    st.put(RUNS, 'run-s', { runId: 'run-s', status: 'running', lines });
    try {
      // the page's own gate is the same function: the plain reasons
      let r = await post({ op: 'flowState', sheetIds: ['b-gf1'], setIds: ['set-10'] }); assert.equal(r.status, 200); assert.equal(r.gates['set-10'].ready, true, 'by readiness alone the set could be approved: the rules are what stop it: ' + JSON.stringify(r.gates['set-10']));
      const sealed = id => !!st.doc(SET, id).processReady;
      const writes0 = st.calls.filter(c => c.op === 'flowApply').length;
      // Set 10: bigger than the minimum -> refused, nothing written, whichever way the stale page asks
      for (const steps of [[{ type: 'seal', kind: 'set', id: 'set-10' }], [{ type: 'seal', kind: 'sheet', id: 'b-gf1' }], [{ type: 'release', sheetIds: ['b-gf2'] }]]) {
        r = await post({ op: 'flowApply', by: 'Paul', steps }); assert.equal(r.status, 409, JSON.stringify(steps));
        assert.match(r.error, /^Set 10 breaks your rules: it is bigger than the minimum \(GF Sheet 2 is not needed for it to be valid\), so it cannot be approved for laser cutting\. Press "Rebuild this set correctly" on its card in the Library$/); assert.equal(r.setId, 'set-10'); assert(r.breaks.length >= 1 && r.breaks[0].rule === 'surplus');
      }
      assert(!sealed('set-10') && (st.doc(SH, 'b-gf1').processSeals || []).length === 0, 'nothing was sealed'); assert.equal((st.doc(SH, 'b-gf2').processSeals || []).length, 0);
      // Set 13: a Rose Gold sheet that is not cut (its unit is the GF + SS + RG chain): refused for the green line
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-13' }] }); assert.equal(r.status, 409); assert.match(r.error, /^Set 13 breaks your rules: /); assert.match(r.error, /Rose Gold sheet RG Sheet 1 is not cut yet \(no green line\)/);
      // Set 11 (right) is approved as before; Set 12 (committed) is not blocked by the rules
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-11' }] }); assert.equal(r.status, 200, JSON.stringify(r)); assert(sealed('set-11'), 'a right set is approved');
      r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-12' }] }); assert.notEqual(r.status === 409 && /breaks your rules/.test(r.error || ''), true, 'a committed set is never judged: ' + JSON.stringify(r));
      assert.equal(st.calls.filter(c => c.op === 'flowApply').length - writes0, 6);
    } finally { srv.close(); }
    console.log('part 8 ok: the server refuses to approve a set that breaks the rules');
  }
  console.log('PASS: set-repair: the owner\'s 14-sheet Set 2 is called wrong in ONE plain line and its plan is exactly the correct formation (Set 2 = GF 1 + GF 2 + SS 1, Set 3, Set 4, GF 8 and the Rose Gold sheets waiting), checked before applying; committed / completed / replaced / approved sets, cut and busy sheets, a run elsewhere and an offline page are refused; the repair is idempotent and leaves Set 1 alone; the real Gate assembly carries it out with nothing deleted; a set that breaks the rules cannot be approved (page and server)');
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
