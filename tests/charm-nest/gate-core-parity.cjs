// GATECORE PARITY (Paul, 20 Nov 2026: "the browser page and the cloud must decide sets with ONE copy of the code, so every computer and the cloud agree").
// The same sheets and sets are fed to
//   PAGE   the page's own Gate (charm-nest-bridge.js, run in a vm with the page's io faked), which now asks charm-nest-assembly-core.js for every decision, and
//   CLOUD  charm-nest-assembly-core.js over the same sheets as RECORDS (asPage / asSet, the view and the io shape of netlify/functions/_charmNestAssembly.js),
// and every decision is compared: which sheets each set holds after the look, and every mark the look leaves on a sheet (setWait, cardinalHold, cardinalNote, cardinalPull, cardinalHome, greenAway).
// Each scene also checks the owner's rule it is about, whatever the copy: SETSHUT, KEEPINSET, OWNEROVERRIDE, MINFLOOR, PAIR2, greenFits (R12), SPLITWAIT, and NOPARTIALSET / PERSONOUT.
// GREEN scenes (GREENCORE, Paul 20 Nov 2026: the Rose Gold cut is one copy too): the page's Gate.autoCut (with its RoseStock, SheetWin and HoldUI faked: every cut, take-off, move and release it asks
// is written down) against charm-nest-assembly-core.js autoCutPlan over the records, carried out the way netlify/functions/_charmNestGreenLine.js autoUnit does: RGREST (a finished Rose Gold sheet cut
// into the newest open set), RGONE (one Rose Gold sheet per set: the smaller one's orders move onto the fullest; a set holding a cut one takes no second), RGCLEAR (an order of another set comes off
// the sheet; an order held for a cut sheet is released), a 10K / 14K sheet (joins by its own press, never cut by itself) and a refused cut tried again only once something on the sheet changed.
// --old <file>: the same scenes through an OLDER charm-nest-bridge.js (its own copy of the rules, before GATECORE): the page's decisions must be the same as before (OWNEROVERRIDE excepted: page sheets
// never carry ownerSet, the server keeps the owner's set for them; the scene feeds it to show both copies now read it the same way).
// Offline: no browser, no network, no Firestore, nothing of Etsy.  Usage: node tests/charm-nest/gate-core-parity.cjs [--old <path to an older charm-nest-bridge.js>]
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..');
const O = require(path.join(ROOT, 'charm-nest-orders.js')), SO = require(path.join(ROOT, 'charm-nest-shared-orders.js')), SR = require(path.join(ROOT, 'charm-nest-set-rules.js'));
const SN = require(path.join(ROOT, 'charm-nest-sheet-name.js')), I = require(path.join(ROOT, 'charm-nest-set-invariants.js')), Place = require(path.join(ROOT, 'charm-nest-place.js'));
const Core = require(path.join(ROOT, 'charm-nest-assembly-core.js'));
const oldAt = process.argv.indexOf('--old'), OLD = oldAt > 0 ? fs.readFileSync(path.resolve(process.argv[oldAt + 1]), 'utf8') : null;
const gateOf = code => { const a = code.indexOf('const Gate = window.Gate ='); assert(a > 0, 'the Gate was found'); return code.slice(a, code.indexOf('/* ═══ 21', a)); };
const GATE = gateOf(fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8')), GATE_OLD = OLD ? gateOf(OLD) : null;
const RUN = 'run-parity', DAY = '2026-11-20', MARKS = ['setWait', 'cardinalHold', 'cardinalNote', 'cardinalPull', 'cardinalHome', 'greenAway'];
let fails = 0; const check = (id, ok, ev) => { if (!ok) fails++; console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ev ? '  ' + ev : ''}`); };

/* ── a scene's sheet, three ways: a page (the page's memory), a record (the cloud's) ── */
// s: { id, metal, page, pool: ['<order>_<n>'], full, setId?, draft?, leftSet?, keepInSet?, ownerSet?, solidPick?, sheetIndex? }
const charmsOf = s => s.pool.map(x => ({ id: 'c-' + x, poolId: x, order: x.split('_')[0] }));
function pageOf(s, sets) {
  const set = s.setId ? sets.find(t => t.setId === s.setId) : null, inSet = !!set && !s.draft;
  const p = { metal: s.metal, page: s.page, runId: RUN, sheetId: s.id, draft: !inSet, setId: s.setId && (inSet || s.leftSet) ? s.setId : null, seq: inSet ? set.seq : null, setDay: DAY, group: 'dispatch',
    sheetIndex: inSet ? s.sheetIndex || s.page : null, fileBase: inSet ? `${s.metal}-Set-${set.seq}-${s.sheetIndex || s.page}` : `${s.metal}_working_${s.id}`,
    outputs: { ai: { path: `x/${s.id}.ai` } }, persistedDone: true, persisted: true, status: 'complete', density: s.full ? .8 : .3,
    placements: s.pool.map(x => ({ id: 'c-' + x })), charms: charmsOf(s), verification: { ok: true }, releaseFull: !!s.full };
  if (s.leftSet) p.leftSet = s.leftSet;
  if (typeof s.solidPick === 'boolean') p.solidPick = s.solidPick;
  if (s.keepInSet) p.keepInSet = s.keepInSet;
  if (s.ownerSet) p.ownerSet = s.ownerSet;
  if (s.roseCutAt) p.roseCutAt = s.roseCutAt;
  return p;
}
function recordOf(s, sets, i) {
  const set = s.setId ? sets.find(t => t.setId === s.setId) : null, inSet = !!set && !s.draft;
  return { id: s.id, runId: RUN, metal: s.metal, page: s.page, day: DAY, draft: !inSet, setId: s.setId || null, setSeq: set ? set.seq : null, sheetIndex: inSet ? s.sheetIndex || s.page : null,
    fileBase: inSet ? `${s.metal}-Set-${set.seq}-${s.sheetIndex || s.page}` : `${s.metal}_working_${s.id}`, outputs: { ai: { path: `x/${s.id}.ai` } }, status: 'complete', density: s.full ? .8 : .3,
    placements: s.pool.map(x => ({ id: 'c-' + x })), charms: charmsOf(s), poolIds: s.pool.slice(), verification: { ok: true }, releaseFull: !!s.full,
    ...(s.leftSet ? { leftSet: s.leftSet } : {}), ...(typeof s.solidPick === 'boolean' && ['rose', 'gold10k', 'gold14k'].includes(s.metal) ? { solidIncluded: s.solidPick } : {}),
    ...(s.keepInSet ? { keepInSet: s.keepInSet } : {}), ...(s.ownerSet ? { ownerSet: s.ownerSet } : {}), ...(s.roseCutAt ? { roseCutAt: s.roseCutAt } : {}), cardStartedAt: 1000 + (i || 0) };
}
const setOf = t => ({ setId: t.setId, seq: t.seq, day: DAY, runId: RUN, group: 'dispatch', name: 'Set-' + t.seq, folder: `charmnest/sets/${DAY}/Set-${t.seq}`, sheetIds: (t.sheetIds || []).slice(), labelFiles: [], materials: [],
  orders: {}, sheetNos: {}, status: t.status || 'nesting', ...(t.formedAt ? { formedAt: t.formedAt } : {}), ...(t.committedAt ? { committedAt: t.committedAt } : {}) });
const lineRows = lines => (lines || []).map(l => ({ order: { receiptId: l.orderId }, spec: { material: l.material }, state: l.state, poolIds: l.poolIds }));

/* ── PAGE: the bridge's Gate in a vm, its io faked as tests/charm-nest/sets-batch.cjs does ── */
async function pageLook(scene, gate) {
  const c = vm.createContext({ O, SO, SR, SN, I, console, RUN, DAY, writes: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN,CharmNestSetInvariants:I}, S={cloud:{ok:true},settings:{},mode:'nest'}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN, TOPUP={orders:35,target:.75};
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{},status:'running'},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=>DAY, refreshAllCards=()=>{}, agent=()=>{}, toast=()=>{}, liveRefresh=()=>{};
const api=async(n,b)=>{writes.push(JSON.parse(JSON.stringify(b)));return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={saveSheetBacks:async()=>{}}, Pool={update:async()=>{}}, RunCtl={onSheetDone(){},save:async()=>{}};
const CN={sheetFileBase:sh=>sh.draft?sh.metal+'_working_'+sh.sheetId:sh.metal+'-Set-'+sh.seq+'-'+sh.sheetIndex,loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);if(!(o&&o.labelsOnly)&&!set.committedAt)set.status='nesting';},
  labelsReady:()=>true, ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq), save:async()=>{},
  ensure:async(runId,group,o)=>{const s={runId,group,seq:seq,day:DAY,setId:'set-'+seq,name:'Set-'+seq,folder:'f',sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{},status:'open'};seq++;B.sets.set(s.setId,s);return s}};
`, c);
  require('./_gateCore.cjs')(c);
  vm.runInContext(gate, c);
  c.__in = { sets: scene.sets.map(setOf), pages: scene.sheets.map(s => pageOf(s, scene.sets)), rows: lineRows(scene.lines) };
  vm.runInContext(`for (const s of __in.sets) B.sets.set(s.setId, s); seq = 1 + Math.max(0, ...__in.sets.map(s => s.seq)); pages.push(...__in.pages); B.orders.rows.push(...__in.rows);`, c);
  // (the real page's SharedOrders finds the sets through window.Sets; a module required here cannot, so it is told: the older Gate reads the sets through it)
  SO.configure({ sets: id => vm.runInContext('Sets.ofRun(RUN)', c).find(s => s.setId === id) || null });
  const before = JSON.stringify(c.__in.pages.map(p => [p.charms, p.placements]));
  await vm.runInContext('Gate.assemble(B.run)', c);
  SO.configure({ sets: null });
  const pages = c.__in.pages, sets = vm.runInContext('Sets.ofRun(RUN)', c);
  return { pages, sets, untouched: before === JSON.stringify(pages.map(p => [p.charms, p.placements])), writes: c.writes, ctx: c };
}

/* ── CLOUD: the core over the same sheets as records, the view and io of _charmNestAssembly.js (its writes made in memory) ── */
async function cloudLook(scene) {
  const run = { runId: RUN, releasePolicy: 2, solidIncluded: {}, status: 'running', lines: Object.fromEntries((scene.lines || []).map((l, i) => ['l' + i, l])) };
  const sets = scene.sets.map(t => Core.asSet(setOf(t))), setsById = new Map(sets.map(s => [s.setId, s]));
  const recs = scene.sheets.map((s, i) => recordOf(s, scene.sets, i)), now = Date.now();
  const pages = recs.map(r => Core.asPage(r, { run, setsById, now }));
  const V = { run, pages, sets, now, today: DAY, journal: {}, joinRefused: new Map(), clock: { completion: 0, join: 0 }, fileBase: p => (p.draft ? `${p.metal}_working_${p.sheetId}` : `${p.metal}-Set-${p.seq}-${p.sheetIndex}`) };
  const G = Core.gate(V), writes = [];
  const io = {
    say: () => {}, pace: async () => {},
    async ensureSet() { const seq = 1 + Math.max(0, ...V.sets.map(s => s.seq)), set = Core.asSet({ setId: 'set-' + seq, seq, day: DAY, runId: RUN, group: 'dispatch', name: 'Set-' + seq, sheetIds: [], status: 'open' }); V.sets.push(set); return set; },
    async stampReleased(p, at) { writes.push({ op: 'released', id: p.sheetId, at: at ? 1 : 0 }); },
    async join(p, set, m) { writes.push({ op: 'join', id: p.sheetId, set: set.setId, m }); return { ok: true }; },
    async joined() { return false; },
    async leave(p, set) { writes.push({ op: 'leave', id: p.sheetId, set: set.setId }); },
    async saveSet(set) { writes.push({ op: 'setUpdate', set: set.setId, sheetIds: set.sheetIds.slice() }); }
  };
  const before = JSON.stringify(pages.map(p => [p.charms, p.placements]));
  await G.assemble(io);
  return { pages, sets: V.sets, untouched: before === JSON.stringify(pages.map(p => [p.charms, p.placements])), writes, G, V };
}

/* ── the decisions, side by side ── */
const nameOf = (sets, sid) => { const s = sets.find(x => x.setId === sid); return s ? s.name : sid || null; };
function decisions(r) {
  const out = { sets: {}, marks: {} };
  for (const s of r.sets.slice().sort((a, b) => a.seq - b.seq)) { const ids = r.pages.filter(p => p.setId === s.setId && !p.draft).map(p => p.sheetId).sort(); if (ids.length) out.sets[s.name] = ids; }
  for (const p of r.pages) { const m = {}; for (const k of MARKS) if (p[k]) m[k] = k === 'cardinalHome' ? nameOf(r.sets, p[k]) : String(p[k]); if (Object.keys(m).length) out.marks[p.sheetId] = m; }
  return out;
}
const S = (id, metal, page, pool, o = {}) => Object.assign({ id, metal, page, pool, full: true }, o);
const scenes = [
  { rule: 'FORM', why: 'a completed GF and a completed SS sheet form Set-1; a partial GF sheet waits outside (NOPARTIALSET)', sets: [],
    sheets: [S('GF1', 'gold', 1, ['101_1', '102_1']), S('SS1', 'silver', 1, ['103_1']), S('GF2', 'gold', 2, ['104_1'], { full: false })],
    expect: d => JSON.stringify(d.sets) === JSON.stringify({ 'Set-1': ['GF1', 'SS1'] }) },
  { rule: 'PAIR2', why: 'one SS sheet sharing orders with TWO GF sheets is one unit of 2 + 1: the three form ONE set, never split', sets: [],
    sheets: [S('GF1', 'gold', 1, ['201_1', '202_1']), S('GF2', 'gold', 2, ['203_1', '204_1']), S('SS1', 'silver', 1, ['201_2', '203_2'])],
    expect: d => JSON.stringify(d.sets) === JSON.stringify({ 'Set-1': ['GF1', 'GF2', 'SS1'] }) },
  { rule: 'MINFLOOR', why: 'a formed set bigger than the minimum keeps every sheet (the minimum is a floor; surplus() is [])', sets: [{ setId: 'set-1', seq: 1, formedAt: 1, sheetIds: ['GF1', 'SS1', 'GF2'] }],
    sheets: [S('GF1', 'gold', 1, ['301_1'], { setId: 'set-1' }), S('SS1', 'silver', 1, ['302_1'], { setId: 'set-1' }), S('GF2', 'gold', 2, ['303_1'], { setId: 'set-1', sheetIndex: 2 })],
    expect: d => JSON.stringify(d.sets['Set-1']) === JSON.stringify(['GF1', 'GF2', 'SS1']) && SR.surplus([{ metal: 'gold', releaseFull: true, placedCount: 1 }, { metal: 'silver', releaseFull: true, placedCount: 1 }, { metal: 'gold', releaseFull: true, placedCount: 1 }]).length === 0 },
  { rule: 'SPLITWAIT', why: 'no set forms while an order of its sheets has a Sterling Silver piece still waiting to be placed', sets: [],
    sheets: [S('GF1', 'gold', 1, ['401_1', '402_1']), S('SS1', 'silver', 1, ['403_1'])],
    lines: [{ orderId: '401', material: 'silver', state: 'pooled', poolIds: ['401_9'] }, { orderId: '401', material: 'gold', state: 'pooled', poolIds: ['401_1'] }],
    expect: (d, r) => !Object.keys(d.sets).length && /^Waits for its set: order 401 still has Sterling Silver pieces waiting to be placed, and no set forms while an order is split$/.test((d.marks.GF1 || {}).setWait || '') },
  { rule: 'KEEPINSET', why: 'a partial SS sheet the owner put back in its set stays in it and is not partial there (isPartial false)', sets: [{ setId: 'set-1', seq: 1, formedAt: 1, sheetIds: ['GF1', 'SS1'] }],
    sheets: [S('GF1', 'gold', 1, ['501_1'], { setId: 'set-1' }), S('SS1', 'silver', 1, ['502_1'], { setId: 'set-1', full: false, keepInSet: { setId: 'set-1', by: 'Paul' } })],
    expect: (d, r) => JSON.stringify(d.sets['Set-1']) === JSON.stringify(['GF1', 'SS1']) && SR.isPartial(r.pages.find(p => p.sheetId === 'SS1')) === false },
  { rule: 'OWNEROVERRIDE', why: 'a sheet the owner took out of every set (ownerSet { setId: null }) is never put in one by the assembly, even when the cardinal rule would pull it', sets: [], skipOld: true,
    sheets: [S('GF1', 'gold', 1, ['601_1']), S('SS1', 'silver', 1, ['602_1', '603_2']), S('GF2', 'gold', 2, ['603_1'], { ownerSet: { setId: null, by: 'Paul' } })],
    expect: d => !Object.values(d.sets).some(ids => ids.includes('GF2')) },
  { rule: 'greenFits', why: 'a Rose Gold sheet sharing orders with a set\'s GF and SS sheets is reserved for that ONE set (cardinalHome) and waits for its own cut; a ticked 14K sheet joins the set', sets: [{ setId: 'set-1', seq: 1, formedAt: 1, sheetIds: ['GF1', 'SS1'] }],
    sheets: [S('GF1', 'gold', 1, ['701_1'], { setId: 'set-1' }), S('SS1', 'silver', 1, ['702_1'], { setId: 'set-1' }), S('RG1', 'rose', 1, ['701_2', '702_2'], { full: false }), S('K1', 'gold14k', 1, ['702_3'], { solidPick: true })],
    expect: d => (d.marks.RG1 || {}).cardinalHome === 'Set-1' && !Object.values(d.sets).some(ids => ids.includes('RG1')) && (d.sets['Set-1'] || []).includes('K1') },
  { rule: 'GOLDKARAT join', why: 'a 14K sheet with its own Include joins its set; one without waits for a person\'s Approve & cut green line in its ONE set (cardinalHome), never cut or joined by itself', sets: [{ setId: 'set-1', seq: 1, formedAt: 1, sheetIds: ['GF1', 'SS1'] }],
    sheets: [S('GF1', 'gold', 1, ['751_1'], { setId: 'set-1' }), S('SS1', 'silver', 1, ['752_1'], { setId: 'set-1' }), S('K1', 'gold14k', 1, ['751_2'], { solidPick: true }), S('K2', 'gold14k', 2, ['752_2'], { full: false })],
    expect: d => (d.sets['Set-1'] || []).includes('K1') && !(d.sets['Set-1'] || []).includes('K2') && (d.marks.K2 || {}).cardinalHome === 'Set-1' && /^Waits in Set-1 for your approval and its green cut line/.test((d.marks.K2 || {}).cardinalNote || '') },
  { rule: 'PERSONOUT', why: 'a sheet a PERSON marked to leave its set (leftSet, draft) leaves it; nothing else leaves (NOSETPULL)', sets: [{ setId: 'set-1', seq: 1, formedAt: 1, sheetIds: ['GF1', 'SS1', 'GF2'] }],
    sheets: [S('GF1', 'gold', 1, ['801_1'], { setId: 'set-1' }), S('SS1', 'silver', 1, ['802_1'], { setId: 'set-1' }), S('GF2', 'gold', 2, ['803_1'], { setId: 'set-1', draft: true, leftSet: 'set-1', sheetIndex: 2 })],
    expect: d => JSON.stringify(d.sets['Set-1']) === JSON.stringify(['GF1', 'SS1']) }
];


/* ── GREEN: the automatic cut, page (Gate.autoCut) and cloud (autoCutPlan + autoUnit's way of carrying it out), pass by pass ── */
const tick = () => new Promise(r => setImmediate(r));
async function pageGreen(scene, gate) {
  const c = vm.createContext({ O, SO, SR, SN, I, console, RUN, DAY, acts: [], fails: { cut: false }, setTimeout: (f, ms) => { const t = setTimeout(f, ms); if (t && t.unref) t.unref(); return t; } });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSetRules:SR,CharmNestSheetName:SN,CharmNestSetInvariants:I}, S={cloud:{ok:true},settings:{},mode:'nest',sheets:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN, TOPUP={orders:35,target:.75};
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{},status:'running'},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, pagesOf=m=>(S.sheets[m]&&S.sheets[m].pages)||[], labelOf=m=>m, today=()=>DAY, refreshAllCards=()=>{}, agent=()=>{}, toast=()=>{};
const api=async()=>({});
const Orders={rows:()=>B.orders.rows}, Engrave={saveSheetBacks:async()=>{}}, Pool={update:async()=>{}}, RunCtl={onSheetDone(){},save:async()=>{},poke(){}};
const CN={sheetFileBase:sh=>sh.draft?sh.metal+'_working_'+sh.sheetId:sh.metal+'-Set-'+sh.seq+'-'+sh.sheetIndex,loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),save:async()=>{},
  ensure:async(runId,group)=>{const s={runId,group,seq:seq,day:DAY,setId:'set-'+seq,name:'Set-'+seq,folder:'f',sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{},status:'open'};seq++;B.sets.set(s.setId,s);return s}};
window.RoseStock={ record: async (p, o) => { acts.push({ cut: p.sheetId, set: p.autoMatched || null }); if (fails.cut) throw new Error('Not cut: the layout changed under the press'); p.roseCutAt = 1; }, full: p => !!p.releaseFull, reopen: async () => {} };
window.SheetWin={ takeOffOrder: async o => { acts.push({ clear: o.sheetId, order: o.orderId }); return {}; }, holdKit: { busy: () => false, move: async (x, to) => { acts.push({ move: x.rid, from: x.pieces[0].src.sheetId, to: to.sheetId }); return true; } } };
window.HoldUI={ release: async o => { acts.push({ release: o }); return { ok: false }; } };
`, c);
  require('./_gateCore.cjs')(c);
  vm.runInContext(gate, c);
  c.__in = { sets: scene.sets.map(setOf), pages: scene.sheets.map(s => pageOf(s, scene.sets)), held: scene.held || null };
  vm.runInContext(`for (const s of __in.sets) B.sets.set(s.setId, s); seq = 1 + Math.max(0, ...__in.sets.map(s => s.seq)); pages.push(...__in.pages); for (const m of ['gold','silver','rose','gold10k','gold14k']) S.sheets[m] = { pages: pages.filter(p => p.metal === m) }; if (__in.held) B.run.rgClearHeld = JSON.parse(JSON.stringify(__in.held));`, c);
  SO.configure({ sets: id => vm.runInContext('Sets.ofRun(RUN)', c).find(s => s.setId === id) || null });
  const out = [];
  for (const pass of scene.passes || [{}]) {
    if (pass.change) for (const p of c.__in.pages) pass.change(p);
    c.fails.cut = !!pass.cutFails; c.acts.length = 0;
    vm.runInContext('Gate.autoCut(B.run)', c);
    for (let i = 0; i < 12; i++) await tick();
    const said = {}; for (const p of c.__in.pages) if (p.autoCutWait && !c.acts.some(a => a.cut === p.sheetId || a.clear === p.sheetId)) said[p.sheetId] = p.autoCutWait;
    out.push({ acts: JSON.parse(JSON.stringify(c.acts)), said });
  }
  SO.configure({ sets: null });
  return out;
}
async function cloudGreen(scene) {
  const run = { runId: RUN, releasePolicy: 2, solidIncluded: {}, status: 'running', lines: {} }, state = { tried: {}, oneTried: {}, clearHeld: JSON.parse(JSON.stringify(scene.held || {})), said: {} };
  const recs = scene.sheets.map((s, i) => recordOf(s, scene.sets, i)), out = [];
  for (const pass of scene.passes || [{}]) {
    if (pass.change) for (const r of recs) pass.change(r);
    const sets = scene.sets.map(t => Core.asSet(setOf(t))), setsById = new Map(sets.map(s => [s.setId, s])), now = Date.now();
    const V = { run, pages: recs.map(r => Core.asPage(r, { run, setsById, now })), sets, now, today: DAY, journal: {}, joinRefused: new Map(), clock: { completion: 0, join: 0 }, fileBase: p => p.fileBase };
    const G = Core.gate(V), plan = G.autoCutPlan(state), acts = [];
    // (carried out as autoUnit does: A1 releases, A2 / A10 take-offs, A7 RGONE's moves (every one that is due: the page moves them all in one go), A12 the cuts)
    for (const o of plan.release) { acts.push({ release: o }); state.clearHeld[o] = Object.assign({}, state.clearHeld[o], { releasing: true }); }
    for (const x of plan.clear) for (const o of x.foreign) acts.push({ clear: x.sheet.sheetId, order: o });
    for (const m of plan.merge) { const target = m.list[0]; if (G.rgOneBusy(target)) continue; for (const src of m.list.slice(1)) { if (G.rgOneBusy(src)) continue; for (const o of G.rgOneOrders(src)) { const key = G.rgOneKey(m.home, src, o); if (!G.rgOneDue(state.oneTried, key) || !G.rgOnePieces(src, o).length) continue; state.oneTried[key] = { at: now }; acts.push({ move: o, from: src.sheetId, to: target.sheetId }); } } }
    for (const x of plan.cut) {
      acts.push({ cut: x.sheet.sheetId, set: x.set.setId }); state.tried[x.sheet.sheetId] = Object.assign({ key: x.key, at: now }, pass.cutFails ? { refused: true } : {});
      const r = recs.find(q => q.id === x.sheet.sheetId); r.autoMatched = x.set.setId;
      // (autoUnit's words for a refused cut of a sheet outside its set, kept in Charm_Nest_Green until the cut is made; the page keeps the same words on the sheet, autoCutWait)
      if (pass.cutFails) state.said[x.sheet.sheetId] = `Its green line is cut automatically, and it joins ${x.set.name || "its set"}, once it is ready: the layout changed under the press`; else { r.roseCutAt = 1; delete state.said[x.sheet.sheetId]; }
    }
    for (const [id, w] of Object.entries(plan.said)) state.said[id] = w;
    const said = {}; for (const [id, w] of Object.entries(state.said)) if (!acts.some(a => a.cut === id || a.clear === id)) said[id] = w;
    out.push({ acts, said });
  }
  return out;
}
const sortActs = a => a.map(x => JSON.stringify(x)).sort();
const G_ = (id, metal, page, pool, o = {}) => Object.assign({ id, metal, page, pool, full: true }, o);
const formed = (seq, ids) => ({ setId: 'set-' + seq, seq, formedAt: seq, sheetIds: ids });
const greenScenes = [
  { rule: 'RGREST', why: 'a FINISHED Rose Gold sheet (full) that no order ties to another sheet is cut automatically into the newest open set', sets: [formed(1, ['GF1', 'SS1'])],
    sheets: [G_('GF1', 'gold', 1, ['9001_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9002_1'], { setId: 'set-1' }), G_('RG1', 'rose', 1, ['9003_1', '9004_1'])],
    expect: r => JSON.stringify(r[0].acts) === JSON.stringify([{ cut: 'RG1', set: 'set-1' }]) },
  { rule: 'RGREST line', why: 'a Rose Gold sheet is finished when a LATER Rose Gold sheet of the run in the line has taken pieces (the page\'s line; the cloud reads the same line from its records)', sets: [formed(1, ['GF1', 'SS1'])],
    sheets: [G_('GF1', 'gold', 1, ['9011_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9012_1'], { setId: 'set-1' }), G_('RG1', 'rose', 2, ['9013_1'], { full: false }), G_('RG2', 'rose', 1, ['9014_1'], { full: false })],
    expect: r => JSON.stringify(r[0].acts) === JSON.stringify([{ cut: 'RG1', set: 'set-1' }]) },
  { rule: 'RGONE', why: 'two uncut Rose Gold sheets of ONE set: the smaller one\'s orders move onto the fullest, neither is cut meanwhile', sets: [formed(1, ['GF1', 'SS1'])],
    sheets: [G_('GF1', 'gold', 1, ['9021_1', '9024_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9022_1'], { setId: 'set-1' }), G_('RG1', 'rose', 1, ['9021_2', '9022_2', '9023_1'], { full: false }), G_('RG2', 'rose', 2, ['9024_2'], { full: false })],
    expect: r => JSON.stringify(r[0].acts) === JSON.stringify([{ move: '9024', from: 'RG2', to: 'RG1' }]) && /has 2 Rose Gold sheets/.test(r[0].said.RG1 || '') },
  { rule: 'RGONE cut', why: 'a set that already holds a CUT Rose Gold sheet takes no second one by itself: it waits and says so', sets: [formed(1, ['GF1', 'SS1', 'RG3'])],
    sheets: [G_('GF1', 'gold', 1, ['9031_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9032_1'], { setId: 'set-1' }), G_('RG3', 'rose', 3, ['9033_1'], { setId: 'set-1', roseCutAt: 5 }), G_('RG1', 'rose', 1, ['9031_2', '9032_2'], { full: false })],
    expect: r => !r[0].acts.length && /already has .* cut: a set never takes two partial Rose Gold sheets by itself/.test(r[0].said.RG1 || '') },
  { rule: 'RGCLEAR', why: 'an order of another set comes off the Rose Gold sheet before its cut; an order held for a cut sheet is released', sets: [formed(1, ['GF1', 'SS1']), formed(2, ['GF2', 'SS2', 'RG9'])],
    sheets: [G_('GF1', 'gold', 1, ['9041_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9042_1'], { setId: 'set-1' }), G_('GF2', 'gold', 2, ['9043_1'], { setId: 'set-2' }), G_('SS2', 'silver', 2, ['9045_1'], { setId: 'set-2' }),
      G_('RG9', 'rose', 9, ['9046_1'], { setId: 'set-2', roseCutAt: 5 }), G_('RG1', 'rose', 1, ['9041_2', '9042_2', '9043_2'], { full: false })],
    held: { '9099': { sheetId: 'RG9', at: 1 } },
    expect: r => JSON.stringify(sortActs(r[0].acts)) === JSON.stringify(sortActs([{ release: '9099' }, { clear: 'RG1', order: '9043' }])) },
  { rule: 'GOLDKARAT', why: 'a 10K / 14K sheet is never cut by itself (it waits for a person\'s press); only Rose Gold is', sets: [formed(1, ['GF1', 'SS1'])],
    sheets: [G_('GF1', 'gold', 1, ['9051_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9052_1'], { setId: 'set-1' }), G_('K1', 'gold14k', 1, ['9051_2']), G_('T1', 'gold10k', 1, ['9052_2'])],
    expect: r => !r[0].acts.length },
  { rule: 'RETRY', why: 'a refused cut is not asked again while nothing on the sheet changed, and is asked again at once when its files change', sets: [formed(1, ['GF1', 'SS1'])],
    sheets: [G_('GF1', 'gold', 1, ['9061_1'], { setId: 'set-1' }), G_('SS1', 'silver', 1, ['9062_1'], { setId: 'set-1' }), G_('RG1', 'rose', 1, ['9063_1'])],
    passes: [{ cutFails: true }, { cutFails: true }, { change: x => { if ((x.sheetId || x.id) === 'RG1') x.outputs = Object.assign({}, x.outputs, { png: 'x' }); } }],
    expect: r => r[0].acts.length === 1 && !r[1].acts.length && JSON.stringify(r[2].acts) === JSON.stringify([{ cut: 'RG1', set: 'set-1' }]) }
];

(async () => {
  for (const sc of scenes) {
    const page = await pageLook(sc, GATE), cloud = await cloudLook(sc), dp = decisions(page), dc = decisions(cloud);
    check(`${sc.rule}: page and cloud decide the same`, JSON.stringify(dp) === JSON.stringify(dc), JSON.stringify(dp) === JSON.stringify(dc) ? JSON.stringify(dp.sets) : `\n   page  ${JSON.stringify(dp)}\n   cloud ${JSON.stringify(dc)}`);
    check(`${sc.rule}: ${sc.why}`, !!sc.expect(dp, page) && !!sc.expect(dc, cloud));
    // (SETSHUT: the assembly never puts a piece on a sheet, in either copy; and a sheet in a set takes no new order, charm-nest-place.js closedRec)
    check(`${sc.rule}: SETSHUT, no piece added or moved by the look`, page.untouched && cloud.untouched);
    if (GATE_OLD && !sc.skipOld) { const old = decisions(await pageLook(sc, GATE_OLD)); check(`${sc.rule}: the same decisions as the page's own copy before GATECORE`, JSON.stringify(old) === JSON.stringify(dp), JSON.stringify(old) === JSON.stringify(dp) ? '' : `\n   before ${JSON.stringify(old)}\n   now    ${JSON.stringify(dp)}`); }
  }
  for (const sc of greenScenes) {
    const page = await pageGreen(sc, GATE), cloud = await cloudGreen(sc);
    const same = page.length === cloud.length && page.every((x, i) => JSON.stringify(sortActs(x.acts)) === JSON.stringify(sortActs(cloud[i].acts)) && JSON.stringify(x.said) === JSON.stringify(cloud[i].said));
    check(`GREEN ${sc.rule}: page and cloud do the same`, same, same ? JSON.stringify(page.map(x => x.acts)) : `\n   page  ${JSON.stringify(page)}\n   cloud ${JSON.stringify(cloud)}`);
    check(`GREEN ${sc.rule}: ${sc.why}`, !!sc.expect(page) && !!sc.expect(cloud));
    if (GATE_OLD) { const old = await pageGreen(sc, GATE_OLD), eq = JSON.stringify(old) === JSON.stringify(page); check(`GREEN ${sc.rule}: the same as the page's own copy before GREENCORE`, eq, eq ? '' : `\n   before ${JSON.stringify(old)}\n   now    ${JSON.stringify(page)}`); }
  }
  check('SETSHUT: a sheet in a set takes no new order (charm-nest-place.js closedRec)', Place.closedRec({ id: 'x', metal: 'silver', setId: 'set-1', draft: false }, [], {}, Date.now()) === true && Place.closedRec({ id: 'y', metal: 'silver', draft: true }, [{ sheetIds: ['y'] }], {}, Date.now()) === true);
  check('MINFLOOR: the server\'s JOIN_RULES_AT_WRITE is ["partial"] (only a partial sheet\'s join is refused at the write; charmNestLibrary.js)', /\nconst JOIN_RULES_AT_WRITE = \["partial"\];/.test(fs.readFileSync(path.join(ROOT, 'netlify/functions/charmNestLibrary.js'), 'utf8')));
  check('the page loads the core before the bridge', (() => { const h = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8'), a = h.indexOf('<script src="charm-nest-assembly-core.js'), b = h.indexOf('<script src="charm-nest-bridge.js'), p = h.indexOf('<script src="charm-nest-place.js'); return a > 0 && b > a && p > 0 && p < a; })());
  check('the core ships with the page (scripts/build-public.cjs)', fs.readFileSync(path.join(ROOT, 'scripts/build-public.cjs'), 'utf8').includes('"charm-nest-assembly-core.js"'));
  console.log(fails ? `FAIL gate-core-parity: ${fails}` : 'PASS gate-core-parity');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
