// SETS AT SCALE (SETFIX, Paul 19 Oct 2026): "Every time I run a large amount of orders, the Sets of Sheets are not following the set rules. Seems like the first set is always correct, but all
// subsequent sets are all messed up. For some reason, the system is choosing to add over 10 sheets to set of sheets #2 ... jamming all of the available sheets into a single set".
//
// A day of several hundred orders streamed through the page's REAL set assembly (Gate.assemble, charm-nest-bridge.js) and the page's REAL numbering (Sets.ensure / allocate / ofRun, the same file),
// with the real set rules (charm-nest-set-rules.js), the real shared-orders rule (charm-nest-shared-orders.js) and the real checker (charm-nest-set-invariants.js). Sheets fill one after the other
// per metal (an order's pieces of one colour stay on one sheet, NOSPREAD); the assembly runs after every intake batch, as LiveNest.finish does. The cloud is a fake that answers setAllocate the way
// charmNestLibrary.js does (a per-DAY counter, idempotent per run / group / "after" set) and records setUpdate.
// The scenes are the orderings of real use:
//   auto      Auto commits every set that is ready (no partner to add) while the run keeps nesting; some lines of an order come in later (a Review hold, an Etsy change, a remake) and land on a
//             later sheet than their order mates
//   rosehub   a Rose Gold sheet keeps filling all day (no green line), and one order in thirty has a piece on it and a piece on the current GF sheet
//   midnight  the run goes on past midnight (or the sandbox stream's clock does), its formed sets waiting for review (not committed): the day's set counter starts at 1 again
//   batch     a small first batch makes Set 1 (committed), then one large pull: every sheet of it completes in the same look (the checkpoint after a big nest), with the Rose Gold hub
//   shuffle   sheets are saved out of order (a sheet's files land several batches after it filled), the page is reloaded mid-run (the sets are read back the way restoreRunSheets keys them)
// After EVERY assembly every set is judged twice:
//   1 by an oracle written HERE (not the helpers under test): a set's units are its sheets tied by orders they share directly, through sheets that could still join it (in this set, or outside every
//     set and not partial, not cut by the laser, not an uncut Rose Gold); a set that stays valid (1 completed GF + 1 completed SS) without one of its units is bigger than the minimum (R7)
//   2 by the app's own checker through Gate.selfCheck (charm-nest-set-invariants.js)
// Usage: node tests/charm-nest/sets-scale.cjs [--root <checkout>] [--seeds 1,2,3] [--orders 700] [--verbose]       (--root: run the same scenes against another checkout, e.g. the commit before the fix)
//        SETS_TRACE=batch:1 prints every set after every look of that scene and seed; SETS_DEBUG="batch:1:batch 7" prints every sheet's marks at that look.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const arg = (k, d) => { const i = process.argv.indexOf('--' + k); return i > 0 ? (process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : true) : d; };
const ROOT = path.resolve(arg('root', path.join(__dirname, '../..'))), VERBOSE = !!arg('verbose', false);
const SEEDS = String(arg('seeds', '1,2,3')).split(',').map(Number), ORDERS = +arg('orders', 700);
const req = f => require(path.join(ROOT, f));
const O = req('charm-nest-orders.js'), SO = req('charm-nest-shared-orders.js'), SR = req('charm-nest-set-rules.js'), SN = req('charm-nest-sheet-name.js'), I = req('charm-nest-set-invariants.js');
const code = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
const setsAt = code.indexOf('  let groups = null, groupsOf = null'), SETS = code.slice(setsAt, code.indexOf('  const setOfSheet =', setsAt));
assert(gateAt > 0 && setsAt > 0 && /function allocate/.test(SETS), 'the page code to test was found');
const RUN = 'run-2026-10-19-scale', CAP = { gold: 40, silver: 40, rose: 40 };
const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;

/* ── the page, as far as the set assembly and the numbering read it; the cloud answers like charmNestLibrary.js ─────────────────────────────────────── */
function world() {
  const c = vm.createContext({ O, SO, SR, SN, I, console, RUN, server: { sets: new Map(), keys: new Map(), seq: {} }, clock: { day: '2026-10-19' }, log: [] });
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
  const w = {
    c, pages: [],
    add(p) { c.__p = p; vm.runInContext('pages.push(__p)', c); w.pages.push(p); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => vm.runInContext('Sets.ofRun(RUN)', c),
    members: set => w.pages.filter(p => p.setId === set.setId && !p.draft),
    selfCheck: () => JSON.parse(vm.runInContext('JSON.stringify(Gate.selfCheck(B.run))', c)),
    commit(set) { set.committedAt = Date.now(); set.status = 'complete'; },
    /** a reload: the page reads the run's sets back from the cloud and keys them the way restoreRunSheets does (the newest open dispatch set at the run's key, every other one at its own) */
    reload() {
      vm.runInContext(`{ const sets=[...B.sets.values()]; B.sets.clear(); const order=(a,b)=>String(a.day||'').localeCompare(String(b.day||''))||(a.seq||0)-(b.seq||0);
        const newestOpen=sets.filter(x=>!x.committedAt&&x.group==='dispatch').sort(order).pop()||null;
        for (const set of sets) Sets.byRun().set(Sets.keyOf(RUN, set.committedAt ? 'committed:'+set.setId : set.group==='dispatch'&&set!==newestOpen ? 'closed:'+set.setId : set.group), set); }`, c);
      for (const p of w.pages) { delete p.releasedAt; delete p.cardinalPull; delete p.cardinalHome; delete p.cardinalHold; delete p.cardinalNote; delete p.setWait; }
    },
  };
  return w;
}

/* ── the oracle (written here): R1, R3, R7 and "no set holds two units it could do without", over the orders the sheets share directly ─────────────────── */
const GOLD = new Set(['rose', 'gold10k', 'gold14k']);
const completed = p => !!(p.releaseFull || p.laserDoneAt || p.roseCutAt) && (p.placements || []).length > 0;
const valid = ps => ps.some(p => p.metal === 'gold' && completed(p)) && ps.some(p => p.metal === 'silver' && completed(p));
function ordersOf(p) { const placed = new Set(p.placements.map(x => x.id)), m = new Map(); for (const ch of p.charms) if (placed.has(ch.id)) { const o = ch.poolId.split('_')[0]; if (!m.has(o)) m.set(o, new Set()); m.get(o).add(ch.poolId); } return m; }
function oracle(w) {
  const faults = [], inSet = p => !!p.setId && !p.draft, sets = w.sets().filter(s => !/^superseded/.test(s.status || ''));
  const ord = new Map(w.pages.map(p => [p, ordersOf(p)]));
  const shares = (a, b) => { const A = ord.get(a), Bm = ord.get(b); for (const [o, keys] of A) { const k2 = Bm.get(o); if (k2 && new Set([...keys, ...k2]).size > 1) return true; } return false; };
  for (const set of sets) {
    if (set.committedAt) continue;
    const mine = w.members(set); if (!mine.length) continue;
    for (const p of mine) if ((p.metal === 'gold' || p.metal === 'silver') && !completed(p)) faults.push(`${set.name}: a partial sheet ${p.sheetId} is in it (R3)`);
    if (!valid(mine)) { faults.push(`${set.name} holds no completed GF and SS sheet (R1): ${mine.map(p => p.sheetId).join(', ')}`); continue; }
    // the sheets that can carry a tie for this set: its own, and the sheets outside every set that could still join one
    const carrier = p => (inSet(p) ? p.setId === set.setId : !(GOLD.has(p.metal) && !p.roseCutAt && !p.solidPick) && !(+p.laserDoneAt > 0) && completed(p));
    const nodes = w.pages.filter(carrier), parent = new Map(nodes.map(p => [p, p])), find = p => (parent.get(p) === p ? p : find(parent.get(p)));
    for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) if (shares(nodes[i], nodes[j])) parent.set(find(nodes[i]), find(nodes[j]));
    const units = new Map(); for (const p of mine) { const r = find(p); if (!units.has(r)) units.set(r, []); units.get(r).push(p); }
    const list = [...units.values()];
    for (const u of list) if (list.length > 1 && valid(list.filter(x => x !== u).flat())) { faults.push(`${set.name} (${mine.length} sheets) is bigger than the minimum (R7): ${u.map(p => p.sheetId).join(', ')} share no order with the rest and it is valid without them; its units: ${list.map(x => '[' + x.map(p => p.sheetId).join(' ') + ']').join(' ')}`); break; }
  }
  return faults;
}

/* ── one day ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
async function day(scene, seed) {
  const r = rng(seed * 7919 + scene.length * 31), w = world(), cur = {}, count = { gold: 0, silver: 0, rose: 0 };
  let oid = 4300000000 + seed * 100000, batchNo = 0, worst = 0, looks = 0;
  const fails = [], sizesSeen = [];
  const tag = { gold: 'GF', silver: 'SS', rose: 'RG' };
  const newPage = m => { count[m]++; const p = { metal: m, page: count[m], runId: RUN, sheetId: `${tag[m]}${count[m]}`, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .3, placements: [], charms: [], verification: { ok: true }, releaseFull: false }; w.add(p); return p; };
  const due = [];      // sheets whose files land later (shuffle): [batch, page]
  const full = p => { p.releaseFull = true; p.density = .78; if (scene === 'shuffle' && r() < .5) { p.persistedDone = false; due.push([batchNo + 1 + Math.floor(r() * 3), p]); } };
  const put = (id, tx, m, n) => {
    let p = cur[m] || (cur[m] = newPage(m));
    if (p.placements.length + n > CAP[m]) { full(p); p = cur[m] = newPage(m); }
    for (let k = 1; k <= n; k++) { const pid = `${id}_${tx}_${k}`; p.charms.push({ id: pid, poolId: pid, order: id }); p.placements.push({ id: pid }); }
  };
  const later = [];
  const order = (o, big) => {
    const id = String(++oid), m1 = r() < .58 ? 'gold' : 'silver'; let tx = 1;
    put(id, tx, m1, r() < .25 ? 2 : 1);                                                                      // one line, a single charm or a pair of earrings (one sheet)
    if (r() < .04) put(id, ++tx, m1 === 'gold' ? 'silver' : 'gold', 1);                                       // a GF + SS order: its two sheets are one unit (the cardinal rule)
    if ((scene === 'rosehub' || scene === 'batch') && r() < 1 / 30) put(id, ++tx, 'rose', 1);                 // the Rose Gold hub: an order with a piece on the Rose Gold sheet that keeps filling
    if ((scene === 'auto' || scene === 'shuffle') && r() < .06) later.push([o + 40 + Math.floor(r() * 120), id, ++tx, r() < .6 ? 'gold' : 'silver']);   // a line that comes in later
    for (let i = later.length - 1; i >= 0; i--) if (later[i][0] <= o) { const [, a, t, m] = later[i]; later.splice(i, 1); put(a, t, m, 1); }
  };
  const look = async what => {
    await w.assemble(); looks++;
    const sets = w.sets(), sizes = sets.map(s => w.members(s).length);
    worst = Math.max(worst, ...sizes.slice(1), 0); sizesSeen.push(sizes);
    for (const f of oracle(w)) fails.push(`${scene} seed ${seed} after ${what}: ${f}`);
    const res = w.selfCheck();
    if (process.env.SETS_TRACE === `${scene}:${seed}`) console.log('   trace', what, sets.map(s => `${s.name}@${s.day}[${w.members(s).map(p => p.sheetId).join(' ')}]`).join(' '));
    if (process.env.SETS_DEBUG === `${scene}:${seed}:${what}`) for (const p of w.pages) console.log('   ', p.sheetId, p.releaseFull ? 'full' : 'filling', p.persistedDone ? '' : 'UNSAVED', p.setId && !p.draft ? 'in ' + p.setId : '', JSON.stringify({ hold: p.cardinalHold, home: p.cardinalHome, note: p.cardinalNote, wait: p.setWait, pull: p.cardinalPull }).slice(0, 300));
    // (not counted against the assembly: a sheet whose order mates are in two different sets already, which a late line made, waits with the plain line "put them in one set first": a person settles it)
    const page = id => w.pages.find(p => p.sheetId === id);
    const counted = v => !(v.rule === 'cardinal' && v.outside && /already in .+ and .+: put them in one set first/.test((page(v.outside) || {}).cardinalHold || ''));
    if (res && !res.ok) for (const v of res.violations.filter(counted)) fails.push(`${scene} seed ${seed} after ${what}: the app's checker: [${v.ruleNo} ${v.rule}] ${v.message}`);
    if (scene === 'auto' || scene === 'shuffle') for (const set of sets) if (!set.committedAt && w.members(set).length && valid(w.members(set)) && !(res && res.partners.some(p => p.set === set.setId))) w.commit(set);   // Auto: a ready set is committed
    return sizes;
  };
  const N = ORDERS;
  if (scene === 'batch') {
    for (let o = 0; o < 40; o++) order(o); for (const m of Object.keys(cur)) if (m !== 'rose' && cur[m].placements.length) { full(cur[m]); cur[m] = null; }
    await look('the first small batch'); for (const s of w.sets()) if (valid(w.members(s))) w.commit(s);
    for (let o = 40; o < N; o++) order(o);
    for (const m of ['gold', 'silver']) if (cur[m] && !cur[m].releaseFull) full(cur[m]);
    await look('the large pull, every sheet complete in one look');
  } else {
    for (let o = 0; o < N; o++) {
      order(o);
      if (o % 25 === 24) {
        batchNo++;
        for (let i = due.length - 1; i >= 0; i--) if (due[i][0] <= batchNo) { due[i][1].persistedDone = true; due.splice(i, 1); }
        if (scene === 'midnight' && o >= N / 2 && w.c.clock.day === '2026-10-19') w.c.clock.day = '2026-10-20';
        if (scene === 'shuffle' && batchNo === 12) w.reload();
        await look('batch ' + batchNo);
      }
    }
    for (const [, a, t, m] of later) put(a, t, m, 1);
    for (const [, p] of due) p.persistedDone = true;
    for (const m of ['gold', 'silver']) if (cur[m] && !cur[m].releaseFull) full(cur[m]);
    await look('the stream end');
  }
  const final = w.sets().map(s => w.members(s).length);
  return { scene, seed, fails, sheets: w.pages.length, sets: final, worst, looks, warnings: w.c.log.filter(l => l.kind === 'warn' && /handed back as the next set/.test(l.text)).length };
}

(async () => {
  const scenes = ['auto', 'rosehub', 'midnight', 'batch', 'shuffle'], out = [];
  for (const scene of scenes) for (const seed of SEEDS) out.push(await day(scene, seed));
  let bad = 0;
  for (const x of out) {
    console.log(`${x.fails.length ? 'FAIL' : 'ok  '} ${x.scene.padEnd(8)} seed ${x.seed}: ${x.sheets} sheets, ${x.looks} looks, sheets per set at the end [${x.sets.join(', ')}], the most sheets any set after Set 1 held: ${x.worst}${x.fails.length ? `, ${x.fails.length} broken rule reading(s)` : ''}`);
    for (const f of x.fails.slice(0, VERBOSE ? 50 : 3)) console.log('     ' + f.slice(0, 600));
    bad += x.fails.length;
  }
  if (bad) { console.log(`FAIL: sets-scale: ${bad} broken rule reading(s) over ${out.length} days`); process.exit(1); }
  console.log(`PASS: sets-scale: ${out.length} days of ${ORDERS} orders (${out.reduce((a, x) => a + x.sheets, 0)} sheets) through the real assembly and numbering: after every look every set is the minimum its units allow, no partial sheet and no set short of a class, and the app's own checker agrees`);
})().catch(e => { console.error(e); process.exit(1); });
