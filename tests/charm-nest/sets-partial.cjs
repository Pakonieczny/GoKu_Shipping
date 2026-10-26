// A PARTIAL GF or SS sheet never joins a set (Paul, 13 Oct 2026, SETPART).
// "A partial GF and SS sheet should never be part of a Set of sheets. That sheet should have been held back because it was not fully nested. A set of sheets must have at least 1 GF and 1 SS sheet.
//  Once that minimum requirement is satisfied then a set is formed, and all other sheets must be formed into subsequent sets and also meet the same criteria."
// One offline suite, synthetic sheets and order numbers only (no real set, no real order, no buyer text, no cloud, no browser):
//   A  the page's own assembly (Gate.assemble in charm-nest-bridge.js, run in a vm with the real set rules, shared-orders rule and sheet names):
//      1 free partial sheets never join and never start a set; the completed pair forms Set 1; the partial sheets that fill later wait for the NEXT set, and form Set 2 with each other
//      2 the group rule: sheets that share an order are one group; a partial GF/SS sheet in it keeps the whole group out of any set (nothing is split), each held sheet says why in one line;
//        an independent completed pair forms Set 1 meanwhile; when the partial sheet is full the group forms the next set
//      3 a group with a sheet in a set already: the completed sheets of the group still join that set (cardinal rule), the partial one waits outside, is reserved for that set, joins by itself when full,
//        and the set's Approve stays grey with the partner line until then
//      4 a set made BEFORE the rule that holds a partial sheet is not touched by an assembly (no write), and is flagged; a person can take the partial sheet out (the cardinal rule no longer keeps it in)
//      5 a person's drop (the one-assembly hand hint) never takes a partial sheet into a set; 10K/14K/Rose Gold are never "partial" and keep their own conditions
//   B  the Library plan (LibraryFlow.core.planMove): a partial sheet dropped on an open set, a committed set, New set, or a sheet dropped on is refused with ONE plain line and writes nothing; a completed
//      sheet is planned as before; a partial partner keeps the group out; a partial sheet can leave a set that the cardinal rule would otherwise keep it in; the dock zones say the same line
//   C  SetEdit.verifyMoves (the server's setMember step): the same refusals, the same leave, a record with no completion mark is never read as partial
//   D  the why-stuck panel's line for a draft (CharmNestReadiness.explain), and the sheet window's joinInfo / joinSet
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const O = require('../../charm-nest-orders.js'), SO = require('../../charm-nest-shared-orders.js'), SR = require('../../charm-nest-set-rules.js'), SN = require('../../charm-nest-sheet-name.js'), RD = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js'), SE = require('../../charm-nest-set-edit.js');
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
const PARTNERS = code.slice(code.indexOf('function partnersOf(list)'), code.indexOf('let _cache = null, libraryRequest'));
assert(gateAt > 0 && PARTNERS.startsWith('function partnersOf'), 'the page code to test was found');
const sheetwin = fs.readFileSync(path.join(__dirname, '../../charm-nest-sheetwin.js'), 'utf8');
const clone = x => JSON.parse(JSON.stringify(x));

/* ── A · the page's assembly ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const RUN = 'run-synthetic-1';
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
// a sheet: metal, page, completed?, the pieces on it ("order_line_n"; the same order on two sheets = a shared order), optionally already in a set
const spec = (id, metal, page, full, pool, o = {}) => ({ id, metal, page, full, pool, density: o.density == null ? .74 : o.density, ...o });
const pageOf = s => ({ metal: s.metal, page: s.page, runId: RUN, sheetId: s.id, draft: !s.inSet, ...(s.inSet ? { setId: s.inSet, seq: 1, sheetIndex: s.sheetIndex || 1 } : {}), outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: s.density,
  placements: s.pool.map(id => ({ id })), charms: s.pool.map(id => ({ id, poolId: id, order: id.split('_')[0] })), verification: { ok: true }, releaseFull: !!s.full, ...(s.pageOver || {}) });
function world(sheets, opts = {}) {
  const c = vm.createContext({ assert, O, SO, SR, SN, console, RD, RUN, allocations: 0, writes: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:${JSON.stringify(opts.solidIncluded || {})}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-13', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),save:async()=>{},
  ensure:async(runId,group,o)=>{allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
`, c);
  vm.runInContext(GATE, c);
  const pages = sheets.map(pageOf);
  // a set that exists already (made before this rule, or by an earlier assembly): its record lists the sheets that are in it
  const inSet = [...new Set(sheets.filter(s => s.inSet).map(s => s.inSet))];
  for (const setId of inSet) {
    const n = +setId.replace(/\D/g, '');
    vm.runInContext(`{const s={runId:RUN,group:'dispatch',seq:${n},day:today(),setId:${JSON.stringify(setId)},name:'Set-${n}',sheetIds:${JSON.stringify(sheets.filter(s => s.inSet === setId).map(s => s.id))},labelFiles:[],materials:['gold','silver'],orders:{},sheetNos:{}};B.sets.set(s.setId,s);allocations=Math.max(allocations,${n});seq=Math.max(seq,${n + 1});}`, c);
  }
  c.__p = pages; vm.runInContext('pages.push(...__p)', c);
  const w = {
    c, pages, add(...list) { const ps = list.map(pageOf); c.__p = ps; vm.runInContext('pages.push(...__p)', c); pages.push(...ps); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => JSON.parse(vm.runInContext('JSON.stringify([...B.sets.values()].map(s=>({name:s.name,seq:s.seq,setId:s.setId,ids:s.sheetIds.slice()})))', c)),
    page: id => pages.find(p => p.sheetId === id),
    ids: set => set.ids.slice().sort(),
    policy: id => { c.__k = pages.find(p => p.sheetId === id); return JSON.parse(vm.runInContext('JSON.stringify(Gate.policy(__k, 0))', c)); },
    allocations: () => c.allocations, writes: () => c.writes.length, puts: () => c.writes.filter(x => x.n === 'charmNestLibrary' && x.b.op === 'putSheet').map(x => x.b.sheet)
  };
  return w;
}
const X = n => `71000000${String(n).padStart(2, '0')}`;      // synthetic order numbers
const own = (n, k = 1) => `${X(n)}_1_${k}`;                   // a piece of a single order on one sheet

(async () => {
  /* A1 · free sheets: a partial sheet never joins and never starts a set; the completed pair forms Set 1, the partial sheets that fill later wait for the NEXT set */
  let w = world([
    spec('gf-a', 'gold', 1, true, [own(1), own(2)]), spec('ss-a', 'silver', 1, true, [own(3), own(4)]),
    spec('gf-p', 'gold', 2, false, [own(5)], { density: .36 }), spec('ss-p', 'silver', 2, false, [own(6)], { density: .52 })]);
  await w.assemble();
  assert.equal(w.allocations(), 1, 'one set was numbered'); let sets = w.sets(); assert.equal(sets.length, 1);
  assert.deepEqual(w.ids(sets[0]), ['gf-a', 'ss-a'], 'Set 1 = the two completed sheets; the partial GF and SS are not in it');
  for (const id of ['gf-p', 'ss-p']) { const p = w.page(id); assert.equal(p.draft, true, id + ' stays a loose draft'); assert(!p.setId && !p.cardinalPull && !p.cardinalHome, id + ' is in no set'); assert.equal(w.policy(id).include, false); assert.equal(w.policy(id).reason, 'Partial · held for a later set'); }
  assert(w.puts().every(s => ['gf-a', 'ss-a'].includes(s.id)), 'nothing is written for a partial sheet: it is not touched, not deleted');
  let before = w.writes(); await w.assemble(); assert.equal(w.writes(), before, 'looked at again: nothing moves');
  // the partial sheets reach their fill: Set 1 is closed (it formed), so they wait for the next set and form Set 2 together
  w.page('gf-p').releaseFull = true; await w.assemble();
  assert.equal(w.sets().length, 1, 'a lone completed GF after Set 1 formed makes no set'); assert.equal(w.page('gf-p').draft, true); assert.match(w.page('gf-p').setWait, /^Set-1 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it$/);
  w.page('ss-p').releaseFull = true; await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 2); assert.deepEqual(w.ids(sets[1]), ['gf-p', 'ss-p'], 'the later completed sheets form the NEXT set under the same minimum'); assert.deepEqual(w.ids(sets[0]), ['gf-a', 'ss-a'], 'Set 1 is as it was');
  // a partial sheet alone, or a completed GF with only a partial SS: no set at all
  w = world([spec('gf-a', 'gold', 1, true, [own(1)]), spec('ss-p', 'silver', 1, false, [own(2)]), spec('gf-p', 'gold', 2, false, [own(3)])]);
  await w.assemble(); assert.equal(w.allocations(), 0, 'no completed SS sheet: no set'); assert.match(w.page('gf-a').setWait, /^Waits for a completed SS sheet/);

  /* A2 · the group rule. GF-A and SS-A share an order (X10), SS-A and GF-P share another (X11): one group; GF-P is partial -> the whole group waits outside any set, with one plain line each.
         An independent completed pair (GF-B, SS-B) forms Set 1 meanwhile. When GF-P is full the group forms the next set, together. */
  w = world([
    spec('gf-a', 'gold', 1, true, [own(1), `${X(10)}_1_1`]), spec('ss-a', 'silver', 1, true, [own(2), `${X(10)}_2_1`, `${X(11)}_1_1`]),
    spec('gf-p', 'gold', 2, false, [own(3), `${X(11)}_2_1`], { density: .65 }),
    spec('gf-b', 'gold', 3, true, [own(4)]), spec('ss-b', 'silver', 3, true, [own(5)])]);
  await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 1, 'the independent pair forms Set 1'); assert.deepEqual(w.ids(sets[0]), ['gf-b', 'ss-b']);
  for (const id of ['gf-a', 'ss-a']) {
    const p = w.page(id); assert.equal(p.draft, true, id + ' is held out of any set'); assert(!p.setId);
    assert.match(p.cardinalHold, /^Waits: orders? [\d, ]+(and more)? shared with GF Draft 2, which is not full yet, so they stay out of a set together$/, p.cardinalHold);
    assert.equal(w.policy(id).include, false); assert.equal(w.policy(id).reason, p.cardinalHold, 'the sheet\'s card says it in that one line');
  }
  assert(!w.page('gf-p').setId && !w.page('gf-p').cardinalPull, 'the partial sheet is not pulled in by the cardinal rule');
  assert.equal(w.puts().filter(s => ['gf-a', 'ss-a', 'gf-p'].includes(s.id)).length, 0, 'nothing is written for the waiting group');
  before = w.writes(); await w.assemble(); assert.equal(w.writes(), before);
  w.page('gf-p').releaseFull = true; await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 2, 'the group is eligible now: it forms the NEXT set'); assert.deepEqual(w.ids(sets[1]), ['gf-a', 'gf-p', 'ss-a'], 'all three together, nothing split'); assert.deepEqual(w.ids(sets[0]), ['gf-b', 'ss-b'], 'Set 1 is as it was');
  for (const id of ['gf-a', 'ss-a', 'gf-p']) assert(!w.page(id).cardinalHold, id + ' is no longer held');

  /* A3 · a group with a sheet in a set already (Set 1 = GF-B + SS-B + GF-C). SS-D shares an order with GF-C; GF-Q (partial) shares one with SS-D. The completed SS-D joins Set 1 (the cardinal rule
         outranks the closing); GF-Q waits outside, reserved for Set 1, and says so; Set 1's Approve stays grey with the partner line; when GF-Q is full it joins Set 1 by itself. */
  w = world([
    spec('gf-b', 'gold', 1, true, [own(1)], { inSet: 'set-1', sheetIndex: 1 }), spec('ss-b', 'silver', 1, true, [own(2)], { inSet: 'set-1', sheetIndex: 1 }),
    spec('gf-c', 'gold', 2, true, [own(3), `${X(20)}_1_1`], { inSet: 'set-1', sheetIndex: 2 }),
    spec('ss-d', 'silver', 2, true, [own(4), `${X(20)}_2_1`, `${X(21)}_1_1`]), spec('gf-q', 'gold', 3, false, [own(5), `${X(21)}_2_1`], { density: .4 })]);
  const writes0 = w.writes(); await w.assemble(); sets = w.sets();
  assert.equal(w.allocations(), 1, 'no new set'); assert.deepEqual(w.ids(sets[0]), ['gf-b', 'gf-c', 'ss-b', 'ss-d'], 'the completed SS-D joined the set that holds its order mate; the partial GF-Q did not');
  assert.equal(w.page('gf-q').draft, true); assert(!w.page('gf-q').setId && !w.page('gf-q').cardinalPull, 'GF-Q is not pulled in'); assert.equal(w.page('gf-q').cardinalHome, 'set-1', 'it is reserved for the set its group is in: no other set takes it');
  assert.match(w.page('gf-c').cardinalNote, /^Orders? [\d, ]+(and more)? shared with GF Draft 3: GF Draft 3 joins Set-1 once it is full \(it is not full yet \(40% full\)\)$/, w.page('gf-c').cardinalNote);
  assert(w.puts().every(s => s.id === 'ss-d'), 'only SS-D was given its set');
  // the Library derives the partner outside the set, and Approve for laser cutting stays grey with the one line
  const pc = vm.createContext({ SO, window: { CharmNestSetRules: SR, SharedOrders: { core: SO.core, sheets: () => w.pages.map(p => SO.fromPage(p)).filter(Boolean) } }, allSheets: () => w.pages });
  vm.runInContext(PARTNERS, pc);
  const group = { setId: 'set-1', seq: 1, name: 'Set-1', status: 'awaiting review', sheets: w.pages.filter(p => p.setId === 'set-1').map(p => ({ id: p.sheetId })) };
  pc.__g = [group]; vm.runInContext('partnersOf(__g)', pc);
  assert.deepEqual(clone(group.cardinalWait), [{ label: 'GF Draft 3', id: 'gf-q', partial: true }]);
  const rec = (id, metal, o = {}) => ({ id, metal, setId: 'set-1', draft: false, releaseFull: true, placedCount: 5, ...o });
  const rows = [rec('gf-b', 'gold'), rec('ss-b', 'silver'), rec('gf-c', 'gold'), rec('ss-d', 'silver')];
  const g3 = RD.setRules(group, rows); assert.deepEqual([g3.applies, g3.ok], [true, false]); assert.equal(g3.short, 'orders shared with GF Draft 3, which is not full yet: it joins this set once it is full');
  assert.equal(RD.setGate({ ...group, sheetIds: rows.map(r => r.id) }, rows).ready, false, 'grey');
  w.page('gf-q').releaseFull = true; await w.assemble();
  assert.equal(w.page('gf-q').setId, 'set-1', 'full now: it joins the set its orders are in'); assert.equal(w.page('gf-q').draft, false); assert.equal(w.allocations(), 1); assert.deepEqual(w.ids(w.sets()[0]), ['gf-b', 'gf-c', 'gf-q', 'ss-b', 'ss-d']);

  /* A4 · a set made BEFORE this rule that already holds a partial sheet (the 12 Oct picture: Set-1 with a 65 % GF pulled in by the cardinal rule): an assembly changes nothing in it; it is flagged */
  const legacy = () => [
    spec('gf-1', 'gold', 1, true, [own(1), `${X(30)}_1_1`], { inSet: 'set-1', sheetIndex: 1 }), spec('gf-2', 'gold', 2, true, [own(2)], { inSet: 'set-1', sheetIndex: 2 }),
    spec('gf-3', 'gold', 3, false, [own(3), `${X(30)}_2_1`], { inSet: 'set-1', sheetIndex: 3, density: .65 }), spec('ss-1', 'silver', 1, true, [own(4), `${X(30)}_3_1`], { inSet: 'set-1', sheetIndex: 1 })];
  w = world(legacy());
  await w.assemble();
  assert.equal(w.writes(), 0, 'nothing is written: the set is not touched, rebuilt, renamed or dissolved'); assert.equal(w.allocations(), 1); assert.deepEqual(w.ids(w.sets()[0]), ['gf-1', 'gf-2', 'gf-3', 'ss-1']);
  assert(w.pages.every(p => p.setId === 'set-1' && !p.draft), 'every sheet is where it was, the partial GF Sheet 3 too');
  assert.match(w.page('gf-3').cardinalPull, /^Shares order 71000000\d\d with /, 'the old pull still keeps it (that is "not touched"); a person may still take it out (A5)');
  const members = w.pages.map(p => ({ id: p.sheetId, metal: p.metal, setId: 'set-1', draft: false, sheetIndex: p.sheetIndex, releaseFull: p.releaseFull, density: p.density, placedCount: 5 }));
  const flag = SR.gate({ setId: 'set-1', status: 'awaiting review' }, members);
  assert.deepEqual([flag.applies, flag.ok, flag.partial, flag.holding], [true, false, ['GF Sheet 3'], true], 'the set holds its place: it waits for the sheet by name (Decision 3, 26 Oct 2026), never rebuilt');
  assert.deepEqual(RD.setRules({ setId: 'set-1', status: 'awaiting review' }, members).partial, ['GF Sheet 3'], 'the readiness read carries the flag too');

  /* A5 · a person takes the partial sheet out of that set (the Library drop on In progress -> the run's leaveSet marks it taken out and a draft): the cardinal rule no longer keeps it in */
  const gf3 = w.page('gf-3'); gf3.leftSet = 'set-1'; gf3.draft = true;
  await w.assemble();
  assert.equal(gf3.setId, null, 'it left'); assert.equal(gf3.draft, true); assert(!gf3.cardinalPull, 'not pulled back'); assert.deepEqual(w.ids(w.sets()[0]), ['gf-1', 'gf-2', 'ss-1'], 'its mates stay; the set is not emptied or dissolved');
  assert.equal(gf3.cardinalHome, 'set-1', 'reserved for the set its orders are in');
  assert.match(w.page('gf-1').cardinalNote, /GF Draft 3 joins Set-1 once it is full/);
  const putGf3 = w.puts().find(s => s.id === 'gf-3'); assert(putGf3 && putGf3.draft === true && putGf3.setId === null, 'the existing leave path wrote the membership only');

  /* A6 · a person's drop (the one-assembly hand hint) never takes a partial sheet into a set; a completed sheet dropped on a closed set joins it as before */
  w = world([spec('gf-a', 'gold', 1, true, [own(1)]), spec('ss-a', 'silver', 1, true, [own(2)]), spec('gf-p', 'gold', 2, false, [own(3)], { density: .4 }), spec('gf-f', 'gold', 3, true, [own(4)])]);
  await w.assemble(); w.page('gf-p').handSet = 'set-1'; w.page('gf-f').handSet = 'set-1'; await w.assemble();
  assert.equal(w.page('gf-p').draft, true, 'the dropped partial sheet stays a draft'); assert(!w.page('gf-p').setId);
  assert.equal(w.page('gf-f').setId, 'set-1', 'the dropped completed sheet joins the closed set, as before'); assert(!('handSet' in w.page('gf-p')), 'a drop is read by one assembly');

  /* A7 · 10K, 14K and Rose Gold are never "partial": the group rule reads GF and SS only. A 14K sheet ticked "Include" whose order mate is a partial GF outside every set waits with the group
         (it would split the order); a group of completed GF/SS with a Rose Gold partner is exactly what it was (the partner is named, the others go in) */
  assert.deepEqual(['rose', 'gold10k', 'gold14k'].map(m => SR.isPartial({ metal: m, releaseFull: false })), [false, false, false]);
  w = world([spec('gf-a', 'gold', 1, true, [own(1)]), spec('ss-a', 'silver', 1, true, [own(2)]), spec('k-1', 'gold14k', 1, false, [own(3), `${X(40)}_1_1`], { pageOver: { solidPick: true } }), spec('gf-p', 'gold', 2, false, [own(4), `${X(40)}_2_1`], { density: .4 })], { solidIncluded: { gold14k: true } });
  await w.assemble(); sets = w.sets();
  assert.deepEqual(w.ids(sets[0]), ['gf-a', 'ss-a'], 'the independent completed pair forms the set');
  assert.equal(w.page('k-1').draft, true, 'the 14K sheet waits with its partial order mate'); assert.match(w.page('k-1').cardinalHold, /shared with GF Draft 2, which is not full yet/);
  w.page('gf-p').releaseFull = true; await w.assemble();
  // (Paul, 26 Oct 2026: a 14K sheet belongs to the ONE set of the GF / SS sheet it shares orders with: it waits for that sheet's set, never joins another)
  assert.equal(w.page('gf-p').draft, true, 'a lone completed GF waits for the next set (closed Set 1)'); assert.match(w.page('k-1').cardinalHold || '', /shared with GF Draft 2, which is in no set yet: it joins their set once it forms/, 'the 14K sheet waits for its partner\'s set, not for a partial sheet any more');
  assert.equal(w.page('k-1').draft, true, 'and it is not put into Set 1, which holds none of its partners');

  /* B · the Library plan (pure: LibraryFlow.core.planMove over the records) ─────────────────────────────────────────────────────────────────────── */
  const rc = (id, metal, o = {}) => ({ id, runId: 'run-1', metal, metalLabel: CODE[metal], day: '2026-10-13', status: 'complete', draft: true, releaseFull: false, setId: null, sheetIndex: null, page: 3, poolIds: [`${id}_o_1`], placedCount: 5, charmCount: 5, density: .36,
    verification: { ok: true }, orders: [X(90)], ...o });
  const openSet = { setId: 'set-1', seq: 1, name: 'Set-1', status: 'open', runId: 'run-1', sheetIds: [] }, cmtSet = { setId: 'set-2', seq: 2, name: 'Set-2', status: 'complete', committedAt: 5, runId: 'run-1', sheetIds: ['gf-m', 'ss-m'] };
  const state = { sheets: { 'gf-p': rc('gf-p', 'gold'), 'gf-f': rc('gf-f', 'gold', { releaseFull: true, density: .75 }), 'ss-p': rc('ss-p', 'silver', { page: 2 }), 'gf-m': rc('gf-m', 'gold', { releaseFull: true, draft: false, setId: 'set-2', setSeq: 2, sheetIndex: 1 }), 'ss-m': rc('ss-m', 'silver', { releaseFull: true, draft: false, setId: 'set-2', setSeq: 2, sheetIndex: 1 }),
      'gf-x': rc('gf-x', 'gold', { releaseFull: false, draft: false, setId: 'set-2', setSeq: 2, sheetIndex: 2, poolIds: ['gf-x_o_1', `${X(50)}_1_1`], orders: [X(50)] }), 'ss-x': rc('ss-x', 'silver', { releaseFull: true, draft: false, setId: 'set-2', setSeq: 2, sheetIndex: 2, poolIds: ['ss-x_o_1', `${X(50)}_2_1`], orders: [X(50)] }) },
    sets: { 'set-1': openSet, 'set-2': { ...cmtSet, sheetIds: ['gf-m', 'ss-m', 'gf-x', 'ss-x'] } }, runs: { 'run-1': { open: true } }, live: {} };
  const liveOf = o => ({ runHere: true, draft: true, dispatchSetId: 'set-1', openSetIds: ['set-1'], can: { ok: true, byHand: true }, split: [], ...o });
  for (const id of ['gf-p', 'gf-f', 'ss-p']) state.live[id] = liveOf();
  const env = { by: 'Paul', canJoin: true, canRelabel: true };
  const keys = p => p.needs.map(n => n.key);
  let p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-p', to: { set: 'set-1' } }, env);
  assert.equal(p.ok, false); assert.deepEqual(keys(p), ['sheetPartial']); assert.equal(p.needs[0].label, 'GF Draft 3 is not full yet (36% full)'); assert.equal(p.needs[0].detail, 'A partial sheet never joins a set.'); assert.deepEqual(p.steps, [], 'no step: nothing is written');
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-p', to: { set: 'set-2' } }, env); assert.deepEqual(keys(p), ['sheetPartial'], 'a committed set does not take it either');
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-p', to: { newSet: true } }, env); assert.deepEqual(keys(p), ['sheetPartial'], 'New set: the partial sheet is refused first, with its own line');
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-f', to: { set: 'set-1' } }, env); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert(p.steps.some(x => x.type === 'include'), 'a completed sheet is planned as before');
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-f', to: { set: 'set-2' } }, env); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert(p.steps.some(x => x.type === 'setMember'));
  // a completed sheet dropped on a partial one: neither joins (the sheet dropped on is checked as the sheet moved is)
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-f', to: { set: 'set-1' } }, { ...env, also: ['gf-p'] }); assert.equal(p.ok, false); assert(keys(p).includes('sheetPartial'), JSON.stringify(keys(p))); assert(!p.steps.length);
  // a completed sheet that shares an order with a partial one: the partner cannot come, the plan says so in one line and the group stays out together (committed set)
  state.sheets['gf-f'].poolIds.push(`${X(60)}_1_1`); state.sheets['gf-f'].orders.push(X(60)); state.sheets['gf-p'].poolIds.push(`${X(60)}_2_1`); state.sheets['gf-p'].orders.push(X(60));
  const items = [{ orderId: X(60), here: 'GF Draft 4', there: ['GF Draft 3'], hereIds: ['gf-f'], thereIds: ['gf-p'], pieces: [], total: 2 }], grp = { members: [{ id: 'gf-f', label: 'GF Draft 4', setId: null }, { id: 'gf-p', label: 'GF Draft 3', setId: null }] };
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-f', to: { set: 'set-2' } }, { ...env, sharedItems: items, group: grp });
  assert.equal(p.ok, false); const pn = p.needs.find(n => n.key === 'sheetPartial'); assert(pn, JSON.stringify(p.needs)); assert.equal(pn.label, 'GF Draft 3 is not full yet (36% full)'); assert.match(pn.detail, /shares an order with it, so neither of them joins Set 2 yet\.$/);
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-f', to: { set: 'set-1' } }, { ...env, sharedItems: items, group: grp });
  assert.equal(p.ok, false, 'an open set: the partner is not ready to come (joinInfo says not full), so the one yes is not offered'); assert.deepEqual(keys(p), ['sharedOrders']);
  // out of a set: a completed sheet that shares an order is kept by the cardinal rule as ever; a PARTIAL one is not
  const sh2 = [{ orderId: X(50), here: 'GF Sheet 2', there: ['SS Sheet 2'], hereIds: ['gf-x'], thereIds: ['ss-x'], pieces: [], total: 2 }];
  p = LF.core.planMove(state, { kind: 'sheet', id: 'ss-x', to: { area: 'progress' } }, { ...env, sharedItems: [{ ...sh2[0], here: 'SS Sheet 2', there: ['GF Sheet 2'], hereIds: ['ss-x'], thereIds: ['gf-x'] }] });
  assert(keys(p).includes('sharedOrders'), 'a completed sheet of a set stays with the sheets that share its order: ' + JSON.stringify(keys(p)));
  p = LF.core.planMove(state, { kind: 'sheet', id: 'gf-x', to: { area: 'progress' } }, { ...env, sharedItems: sh2 });
  assert.equal(p.ok, true, 'a partial sheet may be taken out of a set whatever it shares: ' + JSON.stringify(p.needs)); assert(p.steps.some(x => x.type === 'setMember' && x.moves[0].to === null), JSON.stringify(p.steps));
  // the dock: every place a set can be made or joined says the same line for a partial sheet
  const page = { area: () => 'progress', setOf: () => null, sets: () => [openSet, cmtSet], live: id => state.live[id] || null, rec: id => state.sheets[id] || null, members: () => null, notReady: () => '' };
  const z = LF.core.zones({ kind: 'sheet', id: 'ss-p' }, page);
  for (const t of z.filter(x => x.set || x.newSet)) { assert.equal(t.ok, false); assert.equal(t.reason, 'SS Draft 2 is not full yet (36% full): a partial sheet never joins a set', JSON.stringify(t)); }
  const zf = LF.core.zones({ kind: 'sheet', id: 'gf-f' }, page); assert(zf.filter(x => x.set).every(x => x.ok), 'a completed sheet is offered every set as before: ' + JSON.stringify(zf));
  // combining two draft sheets: a partial one joins nothing
  const sz = LF.core.sheetZone({ kind: 'sheet', id: 'gf-f' }, 'gf-p', page, { self: 'GF Draft 4', name: 'GF Draft 3' }); assert.equal(sz.ok, false); assert.equal(sz.reason, 'GF Draft 3 is not full yet (36% full): a partial sheet never joins a set.');
  // the older records that carry no completion mark are never judged
  const bare = clone(state); delete bare.sheets['gf-p'].releaseFull; p = LF.core.planMove(bare, { kind: 'sheet', id: 'gf-p', to: { set: 'set-1' } }, env); assert.equal(p.ok, true, 'a record without the completion mark is unknown, not partial');

  /* C · SetEdit.verifyMoves: the server's twin ───────────────────────────────────────────────────────────────────────────────────────────────── */
  const mem = (id, metal, o = {}) => ({ id, metal, page: 1, setId: 'set-9', draft: false, releaseFull: true, placedCount: 5, density: .75, poolIds: [], orders: [], ...o });
  const doc = { setId: 'set-9', seq: 9, committedAt: 5, status: 'complete', sheetIds: ['a', 'b', 'c', 'x'] };
  const recs = { a: mem('a', 'gold', { poolIds: [`${X(70)}_1_1`], orders: [X(70)] }), b: mem('b', 'silver'), c: mem('c', 'gold'), x: mem('x', 'gold', { releaseFull: false, density: .62, poolIds: [`${X(70)}_2_1`], orders: [X(70)], sheetIndex: 4 }),
    d: mem('d', 'gold', { setId: null, draft: true, releaseFull: false, page: 5, density: .4 }), e: mem('e', 'silver', { setId: null, draft: true, page: 6 }), f: mem('f', 'gold', { setId: null, draft: true, releaseFull: false, page: 7, density: .5, laserDoneAt: 123 }),
    g: mem('g', 'gold', { setId: null, draft: true, page: 8, releaseFull: undefined }), k: mem('k', 'gold14k', { setId: null, draft: true, releaseFull: false }) };
  delete recs.g.releaseFull;
  const sets9 = { 'set-9': { doc, members: [recs.a, recs.b, recs.c, recs.x] } };
  let v = SE.verifyMoves({ moves: [{ id: 'd', to: 'set-9' }], recs, sets: sets9, others: [] });
  assert.equal(v.ok, false); assert.equal(v.reasons[0].key, 'sheetPartial'); assert.equal(v.reasons[0].label, 'GF Draft 5 is not full yet (40% full)'); assert.equal(SE.sayWhy(v), 'GF Draft 5 is not full yet (40% full). A partial sheet never joins a set.');
  v = SE.verifyMoves({ moves: [{ id: 'e', to: 'set-9' }], recs, sets: sets9, others: [] }); assert.equal(v.ok, true, JSON.stringify(v.reasons));
  v = SE.verifyMoves({ moves: [{ id: 'g', to: 'set-9' }], recs, sets: sets9, others: [] }); assert.equal(v.ok, true, 'a record with no completion mark is never read as partial');
  v = SE.verifyMoves({ moves: [{ id: 'k', to: 'set-9' }], recs, sets: sets9, others: [] }); assert(!v.reasons.some(r => r.key === 'sheetPartial'), 'a 14K sheet is never "partial": its own conditions apply');
  v = SE.verifyMoves({ moves: [{ id: 'x', to: 'set-8' }], recs, sets: { ...sets9, 'set-8': { doc: { ...doc, setId: 'set-8', seq: 8 }, members: [] } }, others: [] }); assert(v.reasons.some(r => r.key === 'sheetPartial'), 'moving a partial sheet from one set to another is a join');
  v = SE.verifyMoves({ moves: [{ id: 'a', to: null }], recs, sets: sets9, others: [] }); assert.equal(v.reasons[0].key, 'sharedOrders', 'a completed sheet is kept by the cardinal rule as ever');
  v = SE.verifyMoves({ moves: [{ id: 'x', to: null }], recs, sets: sets9, others: [] }); assert.equal(v.ok, true, 'a partial sheet may leave a set although it shares an order: ' + JSON.stringify(v.reasons)); assert.deepEqual(v.shared, []);

  /* D · the why-stuck panel and the sheet window ───────────────────────────────────────────────────────────────────────────────────────────── */
  const dr = (o = {}) => ({ id: 'd1', metal: 'gold', page: 3, draft: true, releaseFull: false, density: .65, placedCount: 5, ...o });
  const nesting = r => RD.explain(r, { kind: 'sheet', rows: [] }).steps.find(s => s.key === 'nesting').detail;
  assert.match(nesting(dr()), /^Not in a set yet: it is not full \(65% full\), and a partial sheet never joins a set;/, 'a held-back partial sheet says why in one plain line');
  assert.match(nesting(dr({ releaseFull: true })), /^Not in a set yet: it joins a set when it is full or released;/, 'any other draft keeps the line it always had');
  const held = dr({ releaseFull: true }); Object.defineProperty(held, 'setWait', { value: 'Waits: orders 7100000010 shared with GF Draft 2, which is not full yet, so they stay out of a set together', enumerable: false });
  assert.match(nesting(held), /^Waits: orders 7100000010 shared with GF Draft 2, which is not full yet, so they stay out of a set together;/, 'a sheet the page holds out says what the page says');
  assert(!('setWait' in JSON.parse(JSON.stringify(held))), 'the page\'s line is derived: a copy of the row does not carry it, nothing saves it');
  assert.match(RD.explain(dr(), { kind: 'sheet', rows: [] }).nextText, /^Not in a set yet: it is not full \(65% full\)/);
  // the sheet window's joinInfo / joinSet (the Library's drop path): the real functions over stand-ins, like library-flow-sheetwin.cjs
  const part = (a, b) => { const i = sheetwin.indexOf(a); assert(i >= 0, 'not found: ' + a); const j = sheetwin.indexOf(b, i); assert(j > i, 'not found: ' + b); return sheetwin.slice(i, j); };
  const sheets = [], calls = [];
  const sc = { console, Set, Map, Math, JSON, Object, Array, String, Number, Promise, Error, Date, calls, getSheets: () => sheets, SR,
    api: async (fn, b) => { calls.push(b.op); return { ok: true }; }, Gate: { modern: () => true, splitWith: () => [], policy: () => ({ reason: 'x' }), assemble: async () => { calls.push('assemble'); for (const x of sheets) if (x.releaseFull && x.draft) { x.draft = false; x.setId = 'set-9'; x.label = { files: [1] }; } }, changeMembership: async () => {} },
    Sets: { ofRun: () => [{ group: 'dispatch', setId: 'set-9', seq: 9, committedAt: null }], renderLabelPng: async () => ({ blob: 'png', ecc: 'M' }) }, CN: { METAL_TAG: { gold: 'GF' }, uploadBytes: async p => ({ path: p, url: 'https://x/' + p }) }, O: { CARD_TO_METAL: {}, safeChunks: ids => [ids], encodeOrderList: ids => 'code:' + ids.join(',') } };
  vm.createContext(sc);
  vm.runInContext(`
    const CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
    const W = { rec: null, id: null, flow: null, el: {}, dlg: { open: false } };
    const S = { mode: 'nest' }, fmt = { pct: v => Math.round(v * 100) + '%' }, nowT = () => 1;
    const allSheets = () => getSheets();
    const BUSY = ['nesting', 'finishing', 'queued'], busy = sh => BUSY.includes(sh.status) || !!(sh.persisted && !sh.persistedDone && !sh.problem);
    const sentToStation = sh => false;
    const sheetName = sh => sh.metal + ' · sheet ' + sh.page, ridOf = c => c.order, sheetNoOf = r => r.sheetIndex || 1, whoAmI = () => 'Paul', nameOf = r => ((CODE[r.metal] || '') + ' Sheet ' + sheetNoOf(r)).trim();
    const agent = () => {}, toast = () => {}, open2 = () => {}, askSplit = async () => null;
    const window = { B: { run: { runId: 'run-1', status: 'processed' } }, Gate, Sets, CN, CharmNestOrders: O, CharmNestSetRules: SR, Session: { schedule() {} }, SheetEvents: { qrLabel() {} } };
    const B = window.B, Session = window.Session, CharmNestSetRules = SR;
  `, sc);
  vm.runInContext(part('function labelPlan()', 'function renderMenu()') + '\n' + part('const savedSheet = async', 'window.SheetWin = { remakeLabel') + '\nthis.X = { joinInfo, joinSet };', sc);
  const sheet = over => Object.assign({ metal: 'gold', page: 3, sheetId: 'gold-p', runId: 'run-1', status: 'complete', dirty: false, draft: true, persistedDone: true, verification: { ok: true }, density: .4, releaseFull: false,
    charms: [{ id: 'a', order: '7100' }], placements: [{ id: 'a' }], rejects: [] }, over);
  sheets.push(sheet(), sheet({ sheetId: 'gold-f', releaseFull: true, density: .75 }), sheet({ sheetId: 'k14', metal: 'gold14k' }));
  let info = sc.X.joinInfo('gold-p'); assert.equal(info.can.ok, false); assert.equal(info.can.byHand, false); assert.equal(info.can.reason, 'Not full yet (40% full): a partial sheet never joins a set.');
  info = sc.X.joinInfo('gold-f'); assert.equal(info.can.ok, true, 'a completed sheet is as it was'); assert.equal(info.can.byHand, true);
  info = sc.X.joinInfo('k14'); assert.equal(info.can.ok, true, '14K is never partial');
  calls.length = 0; await assert.rejects(sc.X.joinSet('gold-p'), /GF Draft 3 is not full yet \(40% full\): a partial sheet never joins a set/); assert.deepEqual(calls, [], 'a drop never releases a partial sheet: nothing is written');
  await sc.X.joinSet('gold-f'); assert(calls.includes('putSheet'), 'the completed sheet joins as before');

  console.log('PASS: sets-partial: a partial GF/SS sheet joins no set by any route (minimum rule, cardinal rule, auto assembly, a drop): free partials stay drafts, a group with a partial sheet waits together and forms the next set when it is full, a group with a sheet in a set keeps the partial one outside with its line and Approve grey, an older set that holds a partial sheet is untouched and flagged (a person can take the sheet out), the Library plan, SetEdit and the sheet window refuse with one plain line, gold sheets keep their own conditions');
})().catch(e => { console.error(e); process.exit(1); });
