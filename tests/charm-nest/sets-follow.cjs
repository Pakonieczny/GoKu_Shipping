// A set FORMS as soon as it can, then CLOSES; the following completed sheets form the NEXT set (Paul, 12 Oct 2026).
// "I already gave you the minimum criteria for a set, and so as soon as the minimum criteria is met, and none of the other stipulations are conflicting than the set
//  should be formed and as a result all following sheets should go into a following new set etc... Back engraving completed is not one of the criteria for a set to exist."
// The page's own assembly (Gate.assemble, charm-nest-bridge.js) over the SIX sheets of Paul's picture (GF 76/73/65 %, RG 23 %, SS 75/48 %, backs 0 / N, all drafts, one shared-order
// chain that ties five of them to the Rose Gold sheet), with the page's own set rules (charm-nest-set-rules.js). No cloud, no browser: the harness answers the writes.
//   1 the picture forms Set 1 at once with the completed sheets (never an empty Set 1, never a hold for the Rose Gold sheet), back engraving is never read
//   2 the Rose Gold sheet is named on every sheet it shares orders with ("orders shared with RG Draft 1: add that sheet to this set"), joins by no other route, and is not taken to Set 2
//   3 Set 1 is then closed: a later completed pair forms Set 2, a lone completed sheet waits, the next completed SS forms Set 3; a sheet that shares an order with Set 1 still joins Set 1
//   4 the sandbox's empty Set-1 (made by the old logic) is the set that forms; partial sheets and a lone completed GF never make a set
//   5 Approve for laser cutting: the set is grey with that one line until the partner is in it (the Library derives set.cardinalWait; charm-nest-readiness.js reads it)
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const O = require('../../charm-nest-orders.js'), SO = require('../../charm-nest-shared-orders.js'), SR = require('../../charm-nest-set-rules.js'), SN = require('../../charm-nest-sheet-name.js'), RD = require('../../charm-nest-readiness.js');
const code = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
const PARTNERS = code.slice(code.indexOf('function partnersOf(list)'), code.indexOf('let _cache = null, libraryRequest'));
assert(gateAt > 0 && PARTNERS.startsWith('function partnersOf'), 'the page code to test was found');

/* the six sheets of the picture (sandbox run run-2026-10-12-yf2iy6lzcd, read only): id, metal, page, density, completed (releaseFull), the pieces on it (order_line_n).
   Shared orders: 4196364003 = SS1+GF1, 4196987621 = SS1+GF2, 4198710558 = SS1+RG1, 4200242132 = RG1+GF3. SS Draft 2 shares nothing. */
const PICTURE = [
  { id: 'gold-mv5cm5zt', metal: 'gold', page: 1, density: .757, full: true, pool: ['4196364003_1_1', '4190000001_1_1', '4190000002_1_1'] },
  { id: 'gold-s2-mv5d5ei5', metal: 'gold', page: 2, density: .731, full: true, pool: ['4196987621_1_1', '4190000003_1_1'] },
  { id: 'gold-s3-mv5do6pe', metal: 'gold', page: 3, density: .651, full: false, pool: ['4200242132_1_1', '4190000004_1_1'] },
  { id: 'rose-mv5cnguj-j4lm0fnguj', metal: 'rose', page: 1, density: .234, full: false, pool: ['4198710558_1_2', '4200242132_1_2', '4190000005_1_1'] },
  { id: 'silver-mv5cmca2', metal: 'silver', page: 1, density: .749, full: true, pool: ['4196364003_1_2', '4196987621_1_2', '4198710558_1_1', '4190000006_1_1'] },
  { id: 'silver-s2-mv5deyoz', metal: 'silver', page: 2, density: .478, full: false, pool: ['4190000007_1_1'] }
];
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG' };
const RUN = 'run-2026-10-12-yf2iy6lzcd';
const pageOf = s => ({ metal: s.metal, page: s.page, runId: RUN, sheetId: s.id, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: s.density,
  placements: s.pool.map(id => ({ id })), charms: s.pool.map(id => ({ id, poolId: id, order: id.split('_')[0] })), verification: { ok: true }, releaseFull: !!s.full });
const extra = (metal, page, id, full, pool) => pageOf({ id, metal, page, density: .75, full, pool: pool || [`9${page}0${id.length}00_1_1`, `9${page}1${id.length}00_1_1`] });

/* the world: the page's Gate with its own cardinal rule and set rules; every cloud write is answered and counted. `engraving`: reading the back engravings is an error (they are not part of forming a set) */
function world(opts = {}) {
  const c = vm.createContext({ assert, O, SO, SR, SN, console, RD, SHEETS: PICTURE, RUN, allocations: 0, writes: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=${opts.firstSeq || 1};
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-12', refreshAllCards=()=>{};
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){}};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),save:async()=>{},
  ensure:async(runId,group,o)=>{allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
`, c);
  vm.runInContext(GATE, c);
  if (opts.emptySet) vm.runInContext(`{const s={runId:RUN,group:'dispatch',seq:1,day:today(),setId:'set-old',name:'Set-1',sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);seq=2;}`, c);
  const pages = (opts.sheets || PICTURE).map(pageOf);
  c.__p = pages; vm.runInContext('pages.push(...__p)', c);
  const w = {
    c, pages, add(...list) { c.__p = list; vm.runInContext('pages.push(...__p)', c); pages.push(...list); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => JSON.parse(vm.runInContext('JSON.stringify([...B.sets.values()].map(s=>({name:s.name,seq:s.seq,ids:s.sheetIds.slice()})))', c)),   // (plain arrays: the vm's own would not compare equal)
    names: id => pages.find(p => p.sheetId === id),
    by: label => pages.find(p => `${CODE[p.metal]} Draft ${p.page}` === label),
    ids: set => set.ids.slice().sort(),
    allocations: () => c.allocations, writes: () => c.writes.length
  };
  return w;
}
const ID = { GF1: 'gold-mv5cm5zt', GF2: 'gold-s2-mv5d5ei5', GF3: 'gold-s3-mv5do6pe', RG1: 'rose-mv5cnguj-j4lm0fnguj', SS1: 'silver-mv5cmca2', SS2: 'silver-s2-mv5deyoz' };
const idsOf = (...k) => k.map(x => ID[x]).sort();
const page = (w, k) => w.pages.find(p => p.sheetId === ID[k]);

(async () => {
  /* 1 · the picture: Set 1 forms at once with the completed sheets, the Rose Gold sheet and the partial sheet SS2 stay outside, nothing is held */
  let w = world();
  await w.assemble();
  assert.equal(w.allocations(), 1, 'one set was numbered, and it is not empty');
  let sets = w.sets(); assert.equal(sets.length, 1);
  assert.equal(sets[0].name, 'Set-1');
  assert.deepEqual(w.ids(sets[0]), idsOf('GF1', 'GF2', 'GF3', 'SS1'), 'Set 1 = the three completed sheets, with the partial GF Draft 3 that shares an order with them (the cardinal rule pulls a partial partner along)');
  for (const k of ['GF1', 'GF2', 'SS1', 'GF3']) { const p = page(w, k); assert.equal(p.draft, false, k + ' left Draft'); assert.equal(p.setId, 'set-1'); assert(p.sheetIndex >= 1, k + ' has its number inside the set: GF Sheet n / SS Sheet n'); assert(!p.cardinalHold, k + ' is not held'); assert(!p.setWait); }
  assert.deepEqual([page(w, 'GF1').sheetIndex, page(w, 'GF2').sheetIndex, page(w, 'SS1').sheetIndex], [1, 2, 1], 'loose GF Draft 1 and 2 became GF Sheet 1 and 2, SS Draft 1 became SS Sheet 1');
  for (const k of ['RG1', 'SS2']) { assert.equal(page(w, k).draft, true, k + ' stays a draft'); assert(!page(w, k).setId); }
  assert(page(w, 'SS2').density < .5 && !page(w, 'SS2').releaseFull, 'SS Draft 2 is a partial sheet: it never starts a set and waits');
  assert(w.pages.every(p => !p.cardinalHold), 'no sheet is held for the Rose Gold sheet');
  const saved = w.c.writes.filter(x => x.n === 'charmNestLibrary' && x.b.op === 'putSheet').map(x => x.b.sheet);
  assert.equal(saved.length, 4, 'four sheets were given their set, nothing else was written'); assert(saved.every(s => s.draft === false && s.setId === 'set-1' && !('releaseFull' in s) && !('placements' in s)), 'only the membership, never the artwork or the completion mark');
  const before = w.writes(); await w.assemble(); assert.equal(w.writes(), before, 'looked at again: nothing moves, nothing is written'); assert.equal(w.allocations(), 1);
  // the partner is named on every sheet that shares orders with it, in Paul's words, and the work is not held for it
  for (const k of ['GF1', 'GF2', 'GF3', 'SS1']) assert(/^Orders? [\d, ]+(and more)? shared with RG Draft 1: add that sheet to (this set|Set-1) \(a Rose Gold sheet joins a set only by its own Cut Sheet press\)$/.test(page(w, k).cardinalNote), k + ': ' + page(w, k).cardinalNote);
  assert(!page(w, 'RG1').cardinalNote && !page(w, 'RG1').cardinalPull, 'the Rose Gold sheet is never pulled in by itself (its own Cut Sheet press does that)');

  /* 2 · Set 1 is closed. A completed GF and a completed SS that come later form Set 2; the Rose Gold sheet is not taken to Set 2 by the even-numbered rule (its orders are in Set 1) */
  w.add(extra('gold', 4, 'gf4x', true), extra('silver', 3, 'ss3xx', true));
  await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 2, 'Set 2 formed'); assert.deepEqual(w.ids(sets[1]), ['gf4x', 'ss3xx'].sort(), 'from the completed pair alone');
  assert.deepEqual(w.ids(sets[0]), idsOf('GF1', 'GF2', 'GF3', 'SS1'), 'Set 1 is as it was: not emptied, not renamed, nothing added or taken out');
  assert.equal(page(w, 'RG1').draft, true, 'the Rose Gold sheet is not put into Set 2 by parity: it belongs with its orders in Set 1'); assert.equal(page(w, 'RG1').cardinalHome, 'set-1');
  assert.equal(page(w, 'GF3').setId, 'set-1', 'GF Draft 3 stays in Set 1 (it was not thrown out when Set 2 formed)');
  assert(/shared with RG Draft 1: add that sheet to Set-1/.test(page(w, 'GF1').cardinalNote), page(w, 'GF1').cardinalNote);
  // a lone completed GF after that waits outside every set, with one plain line; it makes no Set 3
  w.add(extra('gold', 5, 'gf5xx', true)); await w.assemble();
  assert.equal(w.sets().length, 2, 'no Set 3 for a lone completed GF'); assert.equal(w.by('GF Draft 5').draft, true);
  assert.equal(w.by('GF Draft 5').setWait, 'Set-2 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it');
  // the next completed SS: GF Draft 5 and it form Set 3
  w.add(extra('silver', 4, 'ss4xx', true)); await w.assemble(); sets = w.sets();
  assert.equal(sets.length, 3); assert.deepEqual(w.ids(sets[2]), ['gf5xx', 'ss4xx'].sort(), 'Set 3'); const gf5 = w.pages.find(p => p.sheetId === 'gf5xx'); assert(gf5.setId === 'set-3' && !gf5.draft && !gf5.setWait);
  assert.deepEqual(w.ids(sets[1]), ['gf4x', 'ss3xx'].sort(), 'Set 2 is not touched'); assert.equal(w.allocations(), 3);
  // the cardinal rule outranks the closing: a new completed sheet that shares an order with a sheet of closed Set 1 goes into Set 1
  w.add(extra('gold', 6, 'gf6xx', true, ['4190000001_1_2', '4190000099_1_1'])); await w.assemble(); sets = w.sets();
  assert.equal(w.pages.find(p => p.sheetId === 'gf6xx').setId, 'set-1', 'it shares order 4190000001 with GF Sheet 1 of Set 1'); assert.equal(sets.length, 3); assert(w.ids(sets[0]).includes('gf6xx'));

  // a person can still put any sheet into any open set: a completed GF dropped on closed Set 1 (the Library sets handSet for that one assembly) joins Set 1; the same sheet not dropped waits
  w.add(extra('gold', 7, 'gf7xx', true), extra('gold', 8, 'gf8xx', true)); const gf7 = w.pages.find(p => p.sheetId === 'gf7xx'), gf8 = w.pages.find(p => p.sheetId === 'gf8xx'); gf7.handSet = 'set-1';
  await w.assemble(); assert.equal(gf7.setId, 'set-1', 'the dropped sheet joined the set it was dropped on, closed or not'); assert(!('handSet' in gf7), 'a drop is read by one assembly');
  assert.equal(gf8.draft, true, 'the sheet nobody dropped waits for the next set'); assert.equal(w.sets().length, 3); gf7.handSet = 'set-nope'; gf8.handSet = 'set-nope';
  await w.assemble(); assert.equal(gf8.draft, true, 'a drop on a set the run does not hold is no drop');

  /* 3 · the one switch: read the other way (a set keeps taking every following completed sheet until a person approves it) and the same pair joins Set 1 */
  SR.CLOSING.afterFormation = false;
  try {
    w = world(); await w.assemble(); w.add(extra('gold', 4, 'gf4x', true), extra('silver', 3, 'ss3xx', true)); await w.assemble();
    assert.equal(w.sets().length, 1); assert(w.ids(w.sets()[0]).includes('gf4x') && w.ids(w.sets()[0]).includes('ss3xx'));
  } finally { SR.CLOSING.afterFormation = true; }

  /* 4 · the sandbox state of the picture: Set-1 already exists, EMPTY (the old logic numbered it and then held the group). It is the set that forms: no second number is taken */
  w = world({ emptySet: true, firstSeq: 1 });
  await w.assemble(); sets = w.sets();
  assert.equal(w.allocations(), 0, 'no new number: the empty Set-1 is the set that forms'); assert.equal(sets.length, 1);
  assert.deepEqual(w.ids(sets[0]), idsOf('GF1', 'GF2', 'GF3', 'SS1'));
  assert.equal(page(w, 'GF1').setId, 'set-old');

  /* 5 · what must NOT make a set: partial sheets only, a lone completed GF, a completed GF whose only SS is partial; a completed sheet sharing orders with the Rose Gold sheet alone */
  w = world({ sheets: PICTURE.filter(s => s.id === ID.GF3 || s.id === ID.SS2 || s.id === ID.RG1) });                        // the three sheets that are not completed
  await w.assemble(); assert.equal(w.allocations(), 0, 'partial sheets never start a set'); assert.equal(w.sets().length, 0);
  w = world({ sheets: PICTURE.filter(s => s.id === ID.GF1 || s.id === ID.SS2 || s.id === ID.GF2) });                         // two completed GF and a partial SS
  await w.assemble(); assert.equal(w.allocations(), 0, 'no completed SS sheet: no set'); assert.equal(w.sets().length, 0);
  assert.match(page(w, 'GF1').setWait, /^Waits for a completed SS sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet/); assert.equal(page(w, 'GF1').draft, true);
  w = world({ sheets: PICTURE.filter(s => s.id !== ID.SS1 && s.id !== ID.SS2) });                                                 // completed GF 1 and 2, partial GF 3, the Rose Gold sheet: still no SS
  await w.assemble(); assert.equal(w.allocations(), 0);
  w = world({ sheets: PICTURE.filter(s => s.id !== ID.RG1) });                                                                     // without the Rose Gold sheet the same set forms (as ever)
  await w.assemble(); assert.deepEqual(w.ids(w.sets()[0]), idsOf('GF1', 'GF2', 'SS1'), 'GF Draft 3 shares an order only with the Rose Gold sheet, so without it nothing pulls GF Draft 3 in');
  assert(w.pages.every(p => !p.cardinalNote), 'nothing is waiting for a partner when there is none');

  /* 6 · Approve for laser cutting stays grey, with that one line, until the partner sheet is in the set; back engraving still gates the approval (the readiness suites prove the rest) */
  w = world(); await w.assemble();
  const pc = vm.createContext({ SO, window: { SharedOrders: { core: SO.core, sheets: () => w.pages.map(p => SO.fromPage(p)).filter(Boolean) } } });
  vm.runInContext(PARTNERS, pc);
  const group = () => { const set = w.sets()[0], g = { setId: 'set-1', seq: 1, name: 'Set-1', status: 'awaiting review', sheets: w.pages.filter(p => p.setId === 'set-1').map(p => ({ id: p.sheetId })) }; return { set, g }; };
  let { g } = group(); pc.__list = [g]; vm.runInContext('partnersOf(__list)', pc);
  assert.deepEqual(Array.from(g.cardinalWait, x => x.label), ['RG Draft 1'], 'the Library derives the partner outside the set from the sheets (never saved)');
  const full = (id, metal, o = {}) => ({ id, metal, setId: 'set-1', draft: false, releaseFull: true, placedCount: 5, ...o });
  const rows = [full(ID.GF1, 'gold'), full(ID.GF2, 'gold'), full(ID.SS1, 'silver')];
  const rules = RD.setRules(g, rows);
  assert.deepEqual([rules.applies, rules.ok], [true, false]); assert.equal(rules.short, 'orders shared with RG Draft 1: add that sheet to this set');
  assert.equal(RD.setGate({ ...g, sheetIds: rows.map(r => r.id) }, rows).ready, false, 'grey');
  const gateReason = RD.setGate({ ...g, sheetIds: rows.map(r => r.id) }, rows);
  assert(gateReason.blockers.some(b => b.rule === 'setPrinciple' && /orders shared with RG Draft 1: add that sheet to this set/.test(b.text)), JSON.stringify(gateReason.blockers.map(b => b.text)));
  // the partner is in the set: the line is gone (the rule has nothing more to say), and Approve then waits only for what it always waited for
  const joined = { ...g }; delete joined.cardinalWait; const ok = RD.setRules(joined, rows.concat([full(ID.RG1, 'rose', { releaseFull: false })]));
  assert.deepEqual([ok.applies, ok.ok], [true, true]);
  // a set that was committed before is never blocked afterwards
  assert.equal(RD.setRules({ ...g, committedAt: 5, status: 'complete' }, rows).applies, false);

  console.log('PASS: sets-follow: the six sheets of the 12 Oct picture form Set 1 at once (completed sheets, never an empty set, never a hold for the Rose Gold sheet, back engraving never read), the Rose Gold partner is named and keeps Approve grey, Set 1 is then closed: a later completed pair forms Set 2, a lone sheet waits, the next SS forms Set 3, an order shared with Set 1 still goes to Set 1, partial sheets and a lone GF never make a set');
})().catch(e => { console.error(e); process.exit(1); });
