// Sets form ONE AT A TIME, each as small as the minimum allows (Paul, 15 Oct 2026, SETRULE).
// "a set of sheets requires at least one GF and one SS sheet at the minimum and when this threshold is met then that becomes an individual Set of sheets ... how is it that this one set
//  has 4 GF and 2 SS sheets. This should be literally impossible, since I've also asked that orders with multiple pieces or earrings with left and right charms should stay on a given sheet"
// Offline: the pure rules (charm-nest-set-rules.js formAll / form / surplus) and the page's own assembly (Gate.assemble, charm-nest-bridge.js) over made-up sheets. No cloud, no browser.
//   1 a batch of 4 GF + 2 SS completed together: Set 1 = the first GF + the first SS, Set 2 = the next pair, the two GF left wait with "needs a completed SS sheet", a later SS makes Set 3
//   2 streaming the same sheets one by one (in any order they could finish) makes the same sets as the batch; the order is the time a sheet was seen completed, then its number, GF before SS
//   3 sheets that share a multi-piece order are ONE unit (never split, however many GF or SS that is); a chain of units is one set (the cardinal rule outranks the closing)
//   4 partial sheets never join; 10K, 14K and Rose Gold never start or complete a set; an open set short of a class takes only the units that complete it; a set that exists is never touched
//   5 property: random batches, every shuffle: no sheet lost or in two sets, every set valid and minimal, units whole, the same input gives the same sets
//   6 the page's assembly: the batch of image 1 (4 GF + 2 SS) numbered Set-1, Set-2 in order, the rest waits with its line, a later SS makes Set-3; streaming gives the same; a Rose Gold partner goes with its unit
//   7 the Library says which orders link which sheets, and which sheets no order ties to the rest
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const O = require('../../charm-nest-orders.js'), SO = require('../../charm-nest-shared-orders.js'), SR = require('../../charm-nest-set-rules.js'), SN = require('../../charm-nest-sheet-name.js');

/* ── 1-5 the pure rules ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const done = (id, metal, page, at, o = {}) => ({ id, metal, page, releaseFull: true, placedCount: 5, density: .75, ...(at ? { releasedAt: at } : {}), ...o });
const part = (id, metal, page) => ({ id, metal, page, releaseFull: false, placedCount: 5, density: .5 });
const ids = l => l.map(s => s.id);
const GF = n => done('g' + n, 'gold', n), SS = n => done('s' + n, 'silver', n);
const names = f => f.sets.map(l => ids(l).sort());   // (the sheets of a set, by id: their order inside it is not the subject)

/* 1 · the batch of image 1: GF Sheet 1-4 and SS Sheet 1-2 completed together */
let batch = [GF(1), GF(2), GF(3), GF(4), SS(1), SS(2)];
let f = SR.formAll([], batch, false, '', []);
assert.deepEqual(names(f), [['g1', 's1'], ['g2', 's2']], 'Set 1 = the first GF + the first SS, Set 2 = the next GF + the next SS');
assert.deepEqual(f.wait.map(w => w.sheet.id), ['g3', 'g4'], 'the two GF left wait');
for (const w of f.wait) assert.equal(w.why, 'The set has formed, so this sheet waits for the next set: it needs a completed SS sheet with it');
assert.deepEqual([f.last.length, f.valid.ok], [0, true]);
// form() is its first step: ONE set now, the sets after it are `later`
let one = SR.form([], batch, false, '', []);
assert.deepEqual([one.where, ids(one.join).sort(), one.later.map(l => ids(l).sort()), one.wait.length], ['new', ['g1', 's1'], [['g2', 's2']], 2], 'form() gives one minimal set and says what follows');
// the next completed SS makes Set 3 with the first waiting GF; the other GF still waits for the next SS
let stage = SR.form(['g2', 's2'].map(i => batch.find(s => s.id === i)), [GF(3), GF(4), SS(3)], true, 'Set-2', []);
assert.deepEqual([stage.where, ids(stage.join).sort()], ['new', ['g3', 's3']], 'a later SS completes Set 3 with GF Sheet 3');
assert.equal(stage.wait.length, 1); assert.equal(stage.wait[0].sheet.id, 'g4'); assert.equal(stage.wait[0].why, 'The set has formed, so this sheet waits for the next set: it needs a completed SS sheet with it', 'a set formed in this very step is not named yet; the page asks again and names it');
stage = SR.form(['g3', 's3'].map(i => [GF(3), SS(3)].find(s => s.id === i)), [GF(4)], true, 'Set-3', []); assert.deepEqual([stage.where, stage.wait[0].why], [null, 'Set-3 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it']);
// the ONE old reading would have put all six into one set: that is what it must never be again
assert.equal(f.sets.length > 1, true);
for (const s of f.sets) assert.equal(SR.validSet(s).ok, true);
assert.equal(f.sets.every(s => s.filter(x => x.metal === 'gold').length === 1 && s.filter(x => x.metal === 'silver').length === 1), true, 'each set is exactly 1 GF + 1 SS here');
// nothing waits when everything pairs; a lone class waits with the plain line when nothing has formed before
f = SR.formAll([], [GF(1), GF(2)], false, '', []); assert.deepEqual([f.sets.length, f.wait.length], [0, 2]); assert.match(f.wait[0].why, /^Waits for a completed SS sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet/);
f = SR.formAll([], [], false, '', []); assert.deepEqual([f.sets.length, f.last.length, f.wait.length], [0, 0, 0]);

/* 2 · streaming one by one = the batch (the completion order is the sheets' own time) */
const play = (order, times) => {      // sheets complete one at a time in `order`; after each, the sets form from what is free
  const sets = [], free = []; let waits = [];
  for (const [k, sh] of order.entries()) {
    free.push({ ...sh, releasedAt: times ? times[sh.id] : k + 1 });
    for (;;) {
      const last = sets[sets.length - 1] || [], r = SR.form(last, free, !!sets.length, sets.length ? 'Set-' + sets.length : '', []);
      if (!r.join.length) { waits = r.wait; break; }
      if (r.where === 'last') last.push(...r.join); else sets.push(r.join.slice());
      for (const j of r.join) free.splice(free.findIndex(x => x.id === j.id), 1);
    }
  }
  return { sets: sets.map(s => ids(s).sort()), waiting: ids(free).sort(), waits };
};
const G = [1, 2, 3, 4].map(GF), S = [1, 2].map(SS);
const asBatch = t => names(SR.formAll([], [...G, ...S].map(x => ({ ...x, releasedAt: t[x.id] })), false, '', [])).map(l => l.slice().sort());
// GF sheets first (SS orders are rarer): the first SS completes Set 1 with GF 1, the second SS Set 2 with GF 2
let t1 = { g1: 1, g2: 2, g3: 3, g4: 4, s1: 5, s2: 6 }; let sA = play([...G, ...S], t1);
assert.deepEqual(sA.sets, [['g1', 's1'], ['g2', 's2']]); assert.deepEqual(sA.waiting, ['g3', 'g4']); assert.deepEqual(sA.sets, asBatch(t1), 'streaming = the batch');
// interleaved: a set forms the moment its minimum is met, the next GF waits for the next SS
let t2 = { g1: 1, s1: 2, g2: 3, g3: 4, s2: 5, g4: 6 }; let sB = play([G[0], S[0], G[1], G[2], S[1], G[3]], t2);
assert.deepEqual(sB.sets, [['g1', 's1'], ['g2', 's2']]); assert.deepEqual(sB.waiting, ['g3', 'g4']); assert.deepEqual(sB.sets, asBatch(t2), 'streaming = the batch');
// SS first
let t3 = { s1: 1, s2: 2, g1: 3, g2: 4, g3: 5, g4: 6 }; let sC = play([...S, ...G], t3);
assert.deepEqual(sC.sets, [['g1', 's1'], ['g2', 's2']], 'the SS that finished first is in Set 1'); assert.deepEqual(sC.sets, asBatch(t3));
// no time known (a reloaded page): the sheet number decides, GF before SS, and a batch is the same every time
assert.deepEqual(names(SR.formAll([], [S[1], G[3], S[0], G[1], G[0], G[2]], false, '', [])).map(l => l.slice().sort()), [['g1', 's1'], ['g2', 's2']], 'without times: by sheet number');

/* 3 · units: sheets that share a multi-piece order never separate */
const grp = (...l) => l;
// same-colour chain: an order whose pieces are on GF 1 and GF 2 (the thing Paul says must not happen, but if it did): ONE unit, so Set 1 holds both GF and the first SS
f = SR.formAll([], [GF(1), GF(2), GF(3), SS(1), SS(2)], false, '', [grp('g1', 'g2')]);
assert.deepEqual(names(f), [['g1', 'g2', 's1'], ['g3', 's2']], 'GF 1 + GF 2 are one unit: they go into the same set; the rest forms the next');
// a chain through an SS sheet: GF1-SS1, SS1-GF2 (mixed GF+SS orders): one unit of three
f = SR.formAll([], [GF(1), GF(2), SS(1), SS(2), GF(3)], false, '', [grp('g1', 's1'), grp('s1', 'g2')]);
assert.deepEqual(names(f), [['g1', 'g2', 's1'], ['g3', 's2']], 'a GF-SS-GF chain is one unit');
// a day that is ONE component stays one set: the cardinal rule outranks the closing rule
f = SR.formAll([], [GF(1), GF(2), GF(3), GF(4), SS(1), SS(2)], false, '', [grp('g1', 's1'), grp('s1', 'g2'), grp('g2', 's2'), grp('s2', 'g3', 'g4')]);
assert.deepEqual(names(f), [['g1', 'g2', 'g3', 'g4', 's1', 's2']], 'six sheets tied by shared orders are one indivisible unit: one set (never split)'); assert.equal(f.wait.length, 0);
// groups may be given as { ids } (SharedOrders.core.groups) and with the Sheet objects themselves
f = SR.formAll([], [GF(1), GF(2), SS(1), SS(2)], false, '', [{ ids: ['g1', 's2'], orders: ['1'] }]);
assert.deepEqual(names(f), [['g1', 's2'], ['g2', 's1']], 'the unit {GF 1, SS 2} is the earliest and valid alone; GF 2 + SS 1 make the next');
const g1 = GF(1), s2 = SS(2); f = SR.formAll([], [g1, GF(2), SS(1), s2], false, '', [[g1, s2]]); assert.deepEqual(names(f), [['g1', 's2'], ['g2', 's1']]);
// a group's sheets that are not free (a Rose Gold partner, a sheet in a set) are ignored for the unit; their other free mates still make one
f = SR.formAll([], [GF(1), GF(2), SS(1)], false, '', [grp('g1', 'rg-outside', 'g2')]); assert.deepEqual(names(f), [['g1', 'g2', 's1']], 'GF 1 and GF 2 are tied through a sheet that is not free');

/* 4 · who may join */
// a partial sheet never joins (SETPART) and never makes a set; its unit mates are settled without it
f = SR.formAll([], [GF(1), SS(1), part('gp', 'gold', 2), part('sp', 'silver', 2)], false, '', [grp('g1', 'gp')]);
assert.deepEqual(names(f), [['g1', 's1']]); assert.deepEqual(f.wait.map(w => w.sheet.id).sort(), ['gp', 'sp']); assert(f.wait.every(w => /not full yet.*a partial sheet never joins a set$/.test(w.why)));
f = SR.formAll([], [part('gp', 'gold', 1), part('sp', 'silver', 1)], false, '', []); assert.deepEqual([f.sets.length, f.last.length], [0, 0]);
// 10K, 14K and Rose Gold never start or complete a set (a person adds them): a completed one among the free sheets waits
for (const metal of ['rose', 'gold14k', 'gold10k']) {
  f = SR.formAll([], [done('x', metal, 1), GF(1)], false, '', []); assert.deepEqual([f.sets.length, f.wait.length], [0, 2], metal + ' + GF is no set');
  f = SR.formAll([], [done('x', metal, 1), GF(1), SS(1)], false, '', []); assert.deepEqual(names(f), [['g1', 's1']], metal + ' stays out of the set'); assert.deepEqual(f.wait.map(w => w.sheet.id), ['x']);
}
// an open set that is short of a class (an empty or half-made one) is completed first, with the fewest units; the rest goes on to the NEXT set
const half = [GF(1)];
f = SR.formAll(half, [GF(2), SS(1), SS(2), GF(3)], true, 'Set-1', []);
assert.deepEqual([ids(f.last), names(f)], [['s1'], [['g2', 's2']]], 'Set-1 needed an SS: the earliest SS completes it; GF 2 + SS 2 make the next set');
assert.deepEqual(f.wait.map(w => w.sheet.id), ['g3']);
f = SR.formAll([], [SS(1), GF(1)], true, 'Set-1', []); assert.deepEqual([ids(f.last).sort(), f.sets.length], [['g1', 's1'], 0], 'an empty open set (made by the older rule) is the set that forms: it takes the first set and no second number is used');
f = SR.formAll(half, [GF(2), GF(3)], true, 'Set-1', []); assert.deepEqual([f.last.length, f.sets.length, f.wait.length], [0, 0, 2]); assert.match(f.wait[0].why, /^Waits for a completed SS sheet: a set needs/, 'the open set still lacks an SS: the plain line, no next set is made');
// a set that has formed is closed: it takes nothing on its own and is never touched; the next sheets form the next set
f = SR.formAll([GF(1), SS(1)], [GF(2), SS(2), GF(3)], true, 'Set-1', []); assert.deepEqual([f.last.length, names(f)], [0, [['g2', 's2']]]); assert.equal(f.wait[0].why, 'The set has formed, so this sheet waits for the next set: it needs a completed SS sheet with it', 'a set formed in this step is named by the page when it asks again');
f = SR.formAll([GF(1), SS(1)], [GF(3)], true, 'Set-1', []); assert.equal(f.wait[0].why, 'Set-1 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it');

/* 5 · property: random batches */
const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
const shuffle = (l, r) => { l = l.slice(); for (let i = l.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [l[i], l[j]] = [l[j], l[i]]; } return l; };
let sizes = new Set();
for (let seed = 1; seed <= 300; seed++) {
  const r = rng(seed * 7 + 1), n = 2 + Math.floor(r() * 12), sheets = [];
  for (let i = 0; i < n; i++) { const k = r(); sheets.push(k < .45 ? done('g' + i, 'gold', 1 + i, r() < .5 ? 100 + Math.floor(r() * 5) : 0) : k < .85 ? done('s' + i, 'silver', 1 + i, r() < .5 ? 100 + Math.floor(r() * 5) : 0) : k < .93 ? part('p' + i, r() < .5 ? 'gold' : 'silver', 1 + i) : done('x' + i, ['rose', 'gold14k', 'gold10k'][i % 3], 1 + i)); }
  const links = []; for (let i = 0; i < Math.floor(r() * 4); i++) { const a = sheets[Math.floor(r() * n)].id, b = sheets[Math.floor(r() * n)].id; if (a !== b) links.push([a, b]); }
  const first = SR.formAll([], sheets, false, '', links), again = SR.formAll([], sheets, false, '', links), mixed = SR.formAll([], shuffle(sheets, r), false, '', links.slice().reverse());
  assert.deepEqual(names(first), names(again), 'same input twice = same sets'); assert.deepEqual(first.wait.map(w => w.sheet.id), again.wait.map(w => w.sheet.id));
  assert.deepEqual(names(first), names(mixed), 'the sets do not depend on the order the sheets are listed in'); assert.deepEqual(first.wait.map(w => w.sheet.id).sort(), mixed.wait.map(w => w.sheet.id).sort());
  const seen = [...first.sets.flat(), ...first.wait.map(w => w.sheet)].map(x => x.id).sort();
  assert.deepEqual(seen, sheets.map(x => x.id).sort(), 'no sheet is lost and none is in two sets (seed ' + seed + ')');
  const setOf = new Map(); first.sets.forEach((s, k) => s.forEach(x => setOf.set(x.id, k)));
  for (const s of first.sets) {
    assert.equal(SR.validSet(s).ok, true, 'every set holds a completed GF and a completed SS');
    for (const c of s) if (c.releaseFull !== true) assert.fail('a partial sheet joined');
    // minimal: no unit of GF/SS can leave and keep it valid
    assert.deepEqual(SR.surplus(s, links), [], 'a set formed here is the smallest the units allow (seed ' + seed + ')');
  }
  for (const [a, b] of links) if (setOf.has(a) && setOf.has(b)) assert.equal(setOf.get(a), setOf.get(b), 'sheets that share an order are in one set (seed ' + seed + ')');
  for (const [a, b] of links) { const wa = setOf.has(a), wb = setOf.has(b); assert(wa === wb || [a, b].some(id => !/^(g|s)/.test(id) || sheets.find(x => x.id === id).releaseFull !== true || /^x/.test(id)), 'a unit goes in whole: ' + a + ' ' + b); }
  sizes.add(first.sets.length);
  // what waits cannot make a set: no GF-and-SS pair of units is left, and a set is never skipped for one that follows
  const rest = first.wait.map(w => w.sheet).filter(x => x.releaseFull === true && ['GF', 'SS'].includes(SR.metalClass(x)));
  const restUnits = SR.unitsOf(rest, links); assert.equal(SR.formAll([], rest, false, '', links).sets.length, 0, 'nothing left could still form a set (seed ' + seed + ')'); void restUnits;
}
assert(sizes.size > 2, 'the random batches cover several numbers of sets');

/* ── 6 the page's own assembly ──────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
assert(gateAt > 0, 'the page code to test was found');
const RUN = 'run-2026-10-15-batch';
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold14k: '14K' };
const pageOf = (id, metal, page, pool, o = {}) => ({ metal, page, runId: RUN, sheetId: id, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .74,
  placements: pool.map(x => ({ id: x })), charms: pool.map(x => ({ id: x, poolId: x, order: x.split('_')[0] })), verification: { ok: true }, releaseFull: true, ...o });
let counter = 0;
const sheet = (metal, page, o = {}) => { const tag = (metal === 'gold' ? 'G' : metal === 'silver' ? 'S' : metal === 'gold14k' ? 'K' : 'R') + page; return pageOf(tag.toLowerCase() + 'x', metal, page, o.pool || [`93${++counter}00_1_1`, `93${++counter}00_1_1`], o); };
function world(sheets, opts = {}) {
  const c = vm.createContext({ assert, O, SO, SR, SN, console, RUN, allocations: 0, writes: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-15', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const order=[];
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),
  save:async(set)=>{ saved.push({name:set.name,n:set.sheetIds.length}); },
  ensure:async(runId,group,o)=>{ if(o&&o.follow){ const f=o.follow; if(!f.sheetIds.length) throw new Error('the set this one follows holds no sheet yet: the server refuses to number a set after it'); } allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
const saved=[];
`, c);
  vm.runInContext(GATE, c);
  const w = {
    c, pages: [], add(...list) { c.__p = list; vm.runInContext('pages.push(...__p)', c); w.pages.push(...list); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => JSON.parse(vm.runInContext('JSON.stringify([...B.sets.values()].map(s=>({name:s.name,seq:s.seq,ids:s.sheetIds.slice()})))', c)),
    by: id => w.pages.find(p => p.sheetId === id), allocations: () => c.allocations, saved: () => JSON.parse(vm.runInContext('JSON.stringify(saved)', c)),
    label: p => `${CODE[p.metal]} Draft ${p.page}`,
  };
  w.add(...sheets);
  return w;
}

(async () => {
  /* 6a · image 1 (Paul, 15 Oct): GF Sheet 1-4 and SS Sheet 1-2 all completed at the same look (a stream's end, a reload, a slow cloud) */
  const six = () => [sheet('gold', 1), sheet('gold', 2), sheet('gold', 3), sheet('gold', 4), sheet('silver', 1), sheet('silver', 2)];
  let w = world(six());
  await w.assemble();
  let sets = w.sets();
  assert.equal(sets.length, 2, 'two sets, not one'); assert.deepEqual(sets.map(s => s.name), ['Set-1', 'Set-2'], 'numbered in order, no gap');
  assert.deepEqual(sets[0].ids.slice().sort(), ['g1x', 's1x'], 'Set 1 = GF 1 + SS 1'); assert.deepEqual(sets[1].ids.slice().sort(), ['g2x', 's2x'], 'Set 2 = GF 2 + SS 2');
  for (const id of ['g3x', 'g4x']) { const p = w.by(id); assert.equal(p.draft, true); assert(!p.setId); assert.equal(p.setWait, 'Set-2 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it', id + ' says why it waits'); }
  assert.equal(w.allocations(), 2); assert.deepEqual(w.pages.filter(p => p.setId).map(p => [p.sheetId, p.sheetIndex]).sort().join('|'), 'g1x,1|g2x,1|s1x,1|s2x,1', 'each set numbers its own sheets from 1');
  assert.deepEqual(w.saved().map(s => s.name + ':' + s.n), ['Set-1:2', 'Set-2:2'], 'each set was saved holding its sheets, and Set-1 held them before Set-2 was numbered after it (the stub refuses a follow-on number otherwise, as the server does)');
  const writes = w.c.writes.length; await w.assemble(); assert.equal(w.c.writes.length, writes, 'looked at again: nothing moves'); assert.equal(w.allocations(), 2);
  // a later SS completes: Set 3 forms with the first waiting GF, the last GF still waits
  w.add(sheet('silver', 3)); await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 3); assert.deepEqual(sets[2].ids.slice().sort(), ['g3x', 's3x']); assert.equal(sets[2].name, 'Set-3'); assert.equal(w.by('g4x').setWait, 'Set-3 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it');
  assert.deepEqual(sets.slice(0, 2).map(s => s.ids.slice().sort()), [['g1x', 's1x'], ['g2x', 's2x']], 'the sets that formed are not touched');
  // streaming: the same sheets one at a time (every kind of order) make the same sets
  for (const order of [['g1x', 'g2x', 'g3x', 'g4x', 's1x', 's2x'], ['s1x', 's2x', 'g1x', 'g2x', 'g3x', 'g4x'], ['g1x', 's1x', 'g2x', 'g3x', 's2x', 'g4x']]) {
    const all = six(), by = Object.fromEntries(all.map(p => [p.sheetId, p])), stream = world([]);
    for (const id of order) { stream.add(by[id]); await stream.assemble(); }
    const got = stream.sets().map(s => s.ids.slice().sort());
    assert.deepEqual(got, [['g1x', 's1x'], ['g2x', 's2x']], 'streaming ' + order.join(',') + ' = the batch');
  }

  /* 6b · sets that exist are never touched: an older set holding 3 GF + 1 SS (what the batch rule made) stays as it is; the next sheets form the next set */
  w = world([]);
  const legacy = [sheet('gold', 1), sheet('gold', 2), sheet('gold', 3), sheet('silver', 1)].map((p, k) => Object.assign(p, { draft: false, setId: 'set-old', sheetIndex: [1, 2, 3, 1][k], seq: 1 }));
  w.add(...legacy); vm.runInContext(`B.sets.set('set-old',{runId:RUN,group:'dispatch',seq:1,day:today(),setId:'set-old',name:'Set-1',sheetIds:${JSON.stringify(legacy.map(p => p.sheetId))},labelFiles:[],materials:['gold','silver'],orders:{},sheetNos:{}}); seq=2;`, w.c);
  w.add(sheet('gold', 4), sheet('silver', 2), sheet('gold', 5)); await w.assemble(); sets = w.sets();
  assert.deepEqual(sets[0].ids.slice().sort(), legacy.map(p => p.sheetId).sort(), 'the older set is exactly as it was'); assert.deepEqual(sets[1].ids.slice().sort(), ['g4x', 's2x'], 'Set 2 forms from the new pair'); assert.equal(sets.length, 2);
  assert.equal(w.by('g5x').draft, true); assert.match(w.by('g5x').setWait, /^Set-2 has formed, so this sheet waits for the next set/);

  /* 6c · the cardinal rule: sheets that share an order are one unit; it outranks the closing */
  w = world([sheet('gold', 1, { pool: ['9400001_1_1', '9400002_1_1'] }), sheet('gold', 2, { pool: ['9400001_1_2', '9400003_1_1'] }), sheet('gold', 3), sheet('silver', 1), sheet('silver', 2)]);
  await w.assemble(); sets = w.sets();                     // order 9400001 has its two pieces on GF 1 and GF 2 (a same-colour spread): they are one unit
  assert.deepEqual(sets.map(s => s.ids.slice().sort()), [['g1x', 'g2x', 's1x'], ['g3x', 's2x']], 'GF 1 and GF 2 share an order: they go into one set together, the rest forms the next');
  // a chain GF1-SS1-GF2-SS2 through mixed-metal orders is one set of four
  w = world([sheet('gold', 1, { pool: ['9410001_1_1', '9410002_1_1'] }), sheet('silver', 1, { pool: ['9410001_2_1', '9410003_1_1'] }), sheet('gold', 2, { pool: ['9410003_2_1', '9410004_1_1'] }), sheet('silver', 2, { pool: ['9410004_2_1', '9410005_1_1'] }), sheet('gold', 3)]);
  await w.assemble(); sets = w.sets();
  assert.deepEqual(sets.map(s => s.ids.slice().sort()), [['g1x', 'g2x', 's1x', 's2x']], 'a chain of sheets tied by shared orders is one indivisible set (the cardinal rule outranks the closing)'); assert.match(w.by('g3x').setWait, /^Waits for a completed SS sheet|has formed, so this sheet waits/);
  // a Rose Gold or 14K sheet that shares an order with the SECOND unit goes with that unit's set (Rose Gold by its even-numbered rule), never into the first set the same look makes
  w = world([sheet('gold', 1), sheet('silver', 1), sheet('gold', 2, { pool: ['9420001_1_1', '9420002_1_1'] }), sheet('silver', 2), sheet('rose', 1, { pool: ['9420001_2_1'], releaseFull: false, density: .2 })]);
  await w.assemble(); sets = w.sets();
  assert.deepEqual(sets.map(s => s.ids.slice().sort()), [['g1x', 's1x'], ['g2x', 'r1x', 's2x']], 'Rose Gold sheet 1 shares an order with GF 2: it goes into Set 2 (even), not Set 1');
  w = world([sheet('gold', 1), sheet('silver', 1), sheet('gold', 2, { pool: ['9430001_1_1', '9430002_1_1'] }), sheet('silver', 2), sheet('gold14k', 1, { pool: ['9430001_2_1'], releaseFull: false, density: .2, solidPick: true })]);
  await w.assemble(); sets = w.sets();
  assert.deepEqual(sets.map(s => s.ids.slice().sort()), [['g1x', 's1x'], ['g2x', 'k1x', 's2x']], 'a ticked 14K sheet that shares an order with GF 2 goes with GF 2 (Set 2), not into the first set of the look');

  /* 6d · partial sheets and the metals added by hand */
  w = world([sheet('gold', 1), sheet('silver', 1, { releaseFull: false, density: .4 }), sheet('gold', 2), sheet('silver', 2)]);
  await w.assemble(); sets = w.sets();
  assert.deepEqual(sets.map(s => s.ids.slice().sort()), [['g1x', 's2x']], 'the partial SS never joins; the completed GF 1 + SS 2 form the set'); assert.equal(w.by('s1x').draft, true); assert.equal(w.by('g2x').draft, true);
  assert.match(w.by('g2x').setWait, /has formed, so this sheet waits for the next set: it needs a completed SS sheet with it/);

  /* ── 7 the Library says how the sheets of a set are tied ─────────────────────────────────────────────────────────────────────────────── */
  const li = code.indexOf('function linkLine('), lj = code.indexOf('let _cache = null, libraryRequest');
  assert(li > 0 && lj > li, 'linkLine found');
  const row = (id, metal, page, pool, o = {}) => ({ id, metal, page, sheetIndex: o.sheetIndex || page, releaseFull: true, placedCount: pool.length, poolIds: pool, runId: 'r', setId: 'set-x', draft: false, ...o });
  const lib = rows => { const norm = rows.map(r => SO.core.sheetOf(r, { label: `${CODE[r.metal]} Sheet ${r.sheetIndex}` })); const pc = vm.createContext({ SO, SR, window: { SharedOrders: { core: SO.core, sheets: () => norm }, CharmNestSetRules: SR } }); vm.runInContext(code.slice(li, lj).replace(/function partnersOf[\s\S]*$/, '') + ';this.__f = linkLine;', pc); return (g) => pc.__f(g, norm, new Map(norm.map(x => [x.id, x]))); };
  // a set tied by shared orders says which orders tie which sheets
  let rows = [row('a', 'gold', 1, ['9500001_1_1', '9500002_1_1']), row('b', 'silver', 1, ['9500001_2_1', '9500003_1_1']), row('c', 'gold', 2, ['9500003_2_1', '9500004_1_1'])];
  let line = lib(rows)({ setId: 'set-x', status: 'awaiting review', sheets: rows });
  assert.equal(line, 'orders 9500001, 9500003 link GF Sheet 1, SS Sheet 1 and GF Sheet 2: sheets that share an order stay in one set');
  // a minimal pair that shares nothing needs no line
  rows = [row('a', 'gold', 1, ['9500001_1_1']), row('b', 'silver', 1, ['9500002_1_1'])]; assert.equal(lib(rows)({ setId: 'set-x', status: 'awaiting review', sheets: rows }), '');
  // image 1 as the older rule made it: 4 GF + 2 SS, none tied by an order: the set is valid with GF 1 + SS 1, the rest is named
  rows = [['gold', 1], ['gold', 2], ['gold', 3], ['gold', 4], ['silver', 1], ['silver', 2]].map(([m, p], k) => row('r' + k, m, p, [`96000${k}1_1_1`])); line = lib(rows)({ setId: 'set-x', status: 'awaiting review', sheets: rows });
  assert.equal(line, 'GF Sheet 2, SS Sheet 2, GF Sheet 3 and GF Sheet 4 share no order with the other sheets, and the set is valid without them');
  // the same six tied into one chain: the orders are named, nothing is spare
  rows = [['gold', 1], ['silver', 1], ['gold', 2], ['silver', 2], ['gold', 3], ['gold', 4]].map(([m, p], k, all) => row('r' + k, m, p, [`97000${k}1_1_1`, `97000${(k + 1) % 6}1_2_1`]));
  line = lib(rows)({ setId: 'set-x', status: 'awaiting review', sheets: rows }); assert.match(line, /^orders 9700001, 9700011, 9700021 and more link GF Sheet 1, SS Sheet 1, GF Sheet 2, SS Sheet 2, GF Sheet 3 and GF Sheet 4: sheets that share an order stay in one set$/);
  // a set already committed is told how it is tied but never that it could be smaller
  rows = [['gold', 1], ['gold', 2], ['silver', 1]].map(([m, p], k) => row('r' + k, m, p, [`98000${k}1_1_1`])); assert.equal(lib(rows)({ setId: 'set-x', status: 'complete', committedAt: 5, sheets: rows }), '');

  console.log('PASS: sets-batch: completed sheets that arrive together form sets one at a time (4 GF + 2 SS: Set 1 = GF 1 + SS 1, Set 2 = GF 2 + SS 2, the two GF wait, a later SS makes Set 3), streaming gives the same sets, shared-order units are never split, partial sheets and 10K/14K/Rose Gold never start a set, existing sets are never touched, 300 random batches keep every sheet in one place and every set minimal and valid, and the Library names how the sheets of a set are tied');
})().catch(e => { console.error(e); process.exit(1); });
