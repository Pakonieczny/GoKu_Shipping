// AUDIT (cardinal rule / shared orders / partial sheets): the page's REAL set assembly (Gate.assemble, charm-nest-bridge.js) and REAL numbering (Sets.ensure / ofRun),
// with the real set rules, shared-orders rule and checker, in a vm. The world is the one tests/charm-nest/sets-scale.cjs builds (copied, not imported: that file runs on require).
// Used by tests/charm-nest/audit-cardinal-*.cjs. Run directly it only checks that the page code is found.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const req = f => require(path.join(ROOT, f));
const O = req('charm-nest-orders.js'), SO = req('charm-nest-shared-orders.js'), SR = req('charm-nest-set-rules.js'), SN = req('charm-nest-sheet-name.js'), I = req('charm-nest-set-invariants.js');
const code = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
const setsAt = code.indexOf('  let groups = null, groupsOf = null'), SETS = code.slice(setsAt, code.indexOf('  const setOfSheet =', setsAt));
assert(gateAt > 0 && setsAt > 0 && /function allocate/.test(SETS), 'the page code to test was found');
const RUN = 'run-2026-10-25-audit';
const TAG = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };

function world() {
  const c = vm.createContext({ O, SO, SR, SN, I, console, RUN, server: { sets: new Map(), keys: new Map(), seq: {} }, clock: { day: '2026-10-25' }, log: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSheetName:SN,CharmNestSetInvariants:I,CharmNestSetRules:SR};
const S={cloud:{ok:true},settings:{}}, CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[], allSheets=()=>pages, labelOf=m=>m, today=()=>clock.day, refreshAllCards=()=>{};
const agent=(scope,kind,text)=>log.push({kind,text});
const api=async(n,b)=>{
  if(b.op==='setAllocate'){
    if(b.after){const pr=server.sets.get(b.after); if(!pr||!(pr.committedAt||(pr.sheetIds||[]).length)) throw new Error('the set this one follows is not a committed set of the run');}
    const key=b.runId+'|'+(b.group||'')+(b.after?'|after:'+b.after:''); if(server.keys.has(key)) return Object.assign({ok:true,existing:true},server.keys.get(key));
    const seq=server.seq[b.day]=(server.seq[b.day]||0)+1, r={setId:'set-'+b.day+'-'+seq,seq,day:b.day}; server.keys.set(key,r); server.sets.set(r.setId,{sheetIds:[]}); return Object.assign({ok:true},r); }
  if(b.op==='setUpdate'){ const s=server.sets.get(b.setId)||{}; Object.assign(s,b.patch); server.sets.set(b.setId,s); }
  return {}; };
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>new Map(),writeBacks:async()=>{},saveSheetBacks:async()=>{}}, Pool={update:async()=>{}};
const RunCtl={onSheetDone(){},save:async()=>{},renderBanner(){}}, toast=()=>{}, document={getElementById:()=>null};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,loadLibrary:async()=>{}};
const Sets=(()=>{ const O={setLabel:s=>'Set-'+s,setFolder:(d,s)=>'charmnest/sets/'+d+'/Set-'+s};
${SETS}
  async function save(set){ await api('charmNestLibrary',{op:'setUpdate',setId:set.setId,patch:{sheetIds:set.sheetIds.slice(),committedAt:set.committedAt||null,status:set.status}}); if(set.committedAt)set.savedCommit=set.committedAt; }
  const onSheetSaved=async(sh,_c,_u,o)=>{const set=o&&o.setOverride; if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId); if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);};
  return {ensure,ofRun,keyOf,byRun,save,onSheetSaved,labelsReady:()=>true};
})();`, c);
  vm.runInContext(GATE, c);
  const count = {};
  const w = {
    c, pages: [],
    add(p) { c.__p = p; vm.runInContext('pages.push(__p)', c); w.pages.push(p); return p; },
    /** a new page of a metal; o: overrides (releaseFull false = still filling) */
    page(metal, o = {}) { count[metal] = (count[metal] || 0) + 1; return w.add(Object.assign({ metal, page: count[metal], runId: RUN, sheetId: `${TAG[metal]}${count[metal]}`, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .5, placements: [], charms: [], verification: { ok: true }, releaseFull: false }, o)); },
    /** put pool ids on a page */
    put(p, ...ids) { for (const id of ids) { p.charms.push({ id, poolId: id, order: id.split('_')[0] }); p.placements.push({ id }); } return p; },
    /** take a pool id off a page (a person's Take off the sheet) */
    off(p, id) { p.charms = p.charms.filter(x => x.poolId !== id); p.placements = p.placements.filter(x => x.id !== id); },
    full(p) { p.releaseFull = true; p.density = .8; return p; },
    by: id => w.pages.find(p => p.sheetId === id),
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => vm.runInContext('Sets.ofRun(RUN)', c).filter(s => !/^superseded/.test(s.status || '')),
    members: set => w.pages.filter(p => p.setId === set.setId && !p.draft),
    inSet: p => !!p && !!p.setId && !p.draft,
    setOf: p => (w.inSet(p) ? (w.sets().find(s => s.setId === p.setId) || {}).seq : null),
    selfCheck: () => JSON.parse(vm.runInContext('JSON.stringify(Gate.selfCheck(B.run))', c)),
    input: () => JSON.parse(vm.runInContext('JSON.stringify(Gate.invariantInput(B.run))', c)),
    /** the checker WITHOUT the live app's partialPartners:'pending' leniency (R11 read strictly) */
    strict: () => { const x = w.input(); return I.check(x.sets, x.sheets, { skipExempt: true, waiting: x.opts.waiting }); },
    commit(set) { set.committedAt = w.tick = Math.max(Date.now(), (w.tick || 0) + 1); set.status = 'complete'; },      // (one ms apart, as real commits are: the next set follows the newest committed one)
    word: () => w.sets().map(s => `${s.name || 'Set ' + s.seq}${s.committedAt ? '(C)' : ''}[${w.members(s).map(p => p.sheetId).join(' ')}]`).join(' ') + ' | out: ' + w.pages.filter(p => !w.inSet(p)).map(p => p.sheetId + (p.releaseFull || p.roseCutAt ? '' : '~')).join(' '),
    marks: () => w.pages.map(p => `${p.sheetId}${w.inSet(p) ? '@' + w.setOf(p) : ''} ${JSON.stringify({ hold: p.cardinalHold, home: p.cardinalHome, pull: p.cardinalPull, note: p.cardinalNote, wait: p.setWait })}`).join('\n'),
  };
  return w;
}

/* ── an independent oracle (written here, not the helpers under test) ─────────────────────────────────────────────────────────────────────────── */
const GOLD = new Set(['rose', 'gold10k', 'gold14k']);
const completed = p => !!(+p.laserDoneAt > 0 || p.roseCutAt || (p.releaseFull && (p.placements || []).length > 0));
const partial = p => (p.metal === 'gold' || p.metal === 'silver') && !completed(p);
/** orderId -> Map(sheet -> Set(poolId)) over what is PLACED */
function orderIndex(pages) {
  const by = new Map();
  for (const p of pages) { const placed = new Set(p.placements.map(x => x.id)); for (const ch of p.charms) if (placed.has(ch.id)) { const o = ch.poolId.split('_')[0]; if (!by.has(o)) by.set(o, new Map()); const m = by.get(o); if (!m.has(p)) m.set(p, new Set()); m.get(p).add(ch.poolId); } }
  return by;
}
const sharedOrders = pages => [...orderIndex(pages)].filter(([, m]) => m.size >= 2 && new Set([...m.values()].flatMap(s => [...s])).size > 1);
/** Faults any state of the real assembly must never show (the uncommitted sets only):
 *   R3   a GF/SS sheet that is not completed is in a set
 *   R1   a set with members holds no completed GF and SS sheet
 *   R12  a gold-family sheet in a set with no cut and no own tick
 *   R9x  an order whose pieces sit on sheets of two different UNCOMMITTED sets */
function invariantFaults(w) {
  const out = [], sets = w.sets();
  for (const s of sets) {
    if (s.committedAt) continue;
    const m = w.members(s); if (!m.length) continue;
    for (const p of m) if (partial(p)) out.push(`R3: ${p.sheetId} is not full and is in Set ${s.seq}`);
    if (!(m.some(p => p.metal === 'gold' && completed(p)) && m.some(p => p.metal === 'silver' && completed(p)))) out.push(`R1: Set ${s.seq} [${m.map(p => p.sheetId)}] holds no completed GF + SS`);
    for (const p of m) if (GOLD.has(p.metal) && !p.roseCutAt && !(+p.laserDoneAt > 0) && p.solidPick !== true) out.push(`R12: ${p.sheetId} is in Set ${s.seq} uncut, nobody ticked it`);
  }
  const open = new Map(sets.filter(s => !s.committedAt).map(s => [s.setId, s.seq]));
  for (const [oid, m] of sharedOrders(w.pages)) {
    const ss = [...new Set([...m.keys()].filter(p => w.inSet(p) && open.has(p.setId)).map(p => open.get(p.setId)))];
    if (ss.length > 1) out.push(`R9x: order ${oid} has pieces on sheets of Set ${ss.join(' and Set ')} (${[...m.keys()].map(p => p.sheetId).join(', ')})`);
  }
  return out;
}

module.exports = { world, invariantFaults, orderIndex, sharedOrders, completed, partial, O, SO, SR, SN, I, RUN, TAG };
if (require.main === module) { world(); console.log('audit-cardinal-lib: the page code was found (this file is a helper of tests/charm-nest/audit-cardinal-*.cjs)'); }
