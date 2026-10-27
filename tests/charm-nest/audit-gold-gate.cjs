// AUDIT (gold family, the page's own assembly): does the REAL Gate (charm-nest-bridge.js Gate.assemble) ever put a Rose Gold / 10K / 14K sheet into a set without BOTH of R12's
// conditions, (a) its green line cut or the person's yes (its own tick) AND (b) its back engraving approved (docs/charm-nest/set-rules.md R12, C3)? And does the live self-check see it?
// The bridge's own comment (R12, Gate.backsOf): "(a) its green line is cut (or the person says yes at the Cut Sheet press / Include, which is the sheet's own tick, solidPick) and (b) its back
// engraving is approved". The tick answers (a) only.
// Offline: the real Gate in a vm over made-up pages (the world of tests/charm-nest/sets-owner-day.cjs), with the real SetEdit and a readiness stand-in whose report says the back engraving of a
// gold-family sheet is NOT approved (0 of 1), the same report shape SetEdit.engravingGap reads everywhere. No cloud, no browser.
// Prints OK / BREAK per scenario; exits 1 when a BREAK is reproduced.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), SO = require(path.join(root, 'charm-nest-shared-orders.js')), SR = require(path.join(root, 'charm-nest-set-rules.js')), SN = require(path.join(root, 'charm-nest-sheet-name.js')), I = require(path.join(root, 'charm-nest-set-invariants.js')), SE = require(path.join(root, 'charm-nest-set-edit.js'));
const code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
assert(gateAt > 0, 'the page code to test was found');
const RUN = 'run-2026-10-25-audit';
const breaks = [];
const verdict = (name, ok, detail) => { console.log(`${ok ? 'OK   ' : 'BREAK'} ${name}${ok ? '' : ' :: ' + detail}`); if (!ok) breaks.push(name); };

const page = (tag, metal, pg, pool, o = {}) => ({ metal, page: pg, runId: RUN, sheetId: tag, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .74,
  placements: pool.map(x => ({ id: x })), charms: pool.map(x => ({ id: x, poolId: x, order: x.split('_')[0] })), verification: { ok: true }, releaseFull: true, ...o });
// a readiness stand-in: a gold-family sheet's back engraving is not approved (0 of 1); every other sheet has nothing to approve
const RD = {
  decisions: () => ({}), orderReports: () => ({}),
  approved: false,
  sheet: row => (['rose', 'gold10k', 'gold14k'].includes(row.metal) && !RD.approved ? { ready: false, stages: { approval: false }, faces: { approved: 0, required: 1 } } : { ready: true, stages: { approval: true } }),
};
function world() {
  const c = vm.createContext({ assert, O, SO, SR, SN, I, SE, RD, console, RUN, allocations: 0, writes: [] });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSheetName:SN,CharmNestSetInvariants:I,CharmNestSetRules:SR,SetEdit:SE,CharmNestReadiness:RD}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-25', refreshAllCards=()=>{};
const agentLog=[]; const agent=(scope,kind,text)=>agentLog.push({scope,kind,text});
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>new Map(),writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){},save:async()=>{},poke(){},membershipUpdated(){}}, Session={schedule(){}}, toast=()=>{}, document={getElementById:()=>null};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),
  save:async(set)=>{},
  ensure:async(runId,group,o)=>{ allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
`, c);
  vm.runInContext(GATE, c);
  const json = expr => JSON.parse(vm.runInContext(`JSON.stringify(${expr})`, c));
  const w = {
    pages: [],
    add(...list) { c.__p = list; vm.runInContext('pages.push(...__p)', c); w.pages.push(...list); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    include: (metal, id, on) => { c.__id = id; c.__m = metal; c.__on = on; return vm.runInContext('Gate.changeMembership(__m, __on, pages.find(p => p.sheetId === __id))', c); },
    by: id => w.pages.find(p => p.sheetId === id),
    inSet: p => !!p && !p.draft && !!p.setId,
    violations: () => json('Gate.violations(B.run)'),
    input: () => json('Gate.invariantInput(B.run)'),
    // the run's sets as the Library and the server read them: the sheets' records, with the readiness report of each gold-family sheet (SetRepair.diagnose / the approval gate do exactly this)
    libraryCheck: () => { const inp = w.input(); const sheets = inp.sheets.map(s => (['rose', 'gold10k', 'gold14k'].includes(s.metal) ? Object.assign({}, s, { laser: RD.sheet(s) }) : s)); return I.check(inp.sets, sheets, inp.opts); },
  };
  return w;
}

(async () => {
  for (const metal of ['rose', 'gold10k', 'gold14k']) {
    const tag = { rose: 'RG', gold10k: '10K', gold14k: '14K' }[metal];
    // ── G1 · Include (the sheet's own tick) on an uncut gold-family sheet whose back engraving is NOT approved, next to a formed set it shares an order with ──
    {
      const w = world();
      const gf = page('gf1', 'gold', 1, ['5301_1_1', '5302_1_1']), ss = page('ss1', 'silver', 2, ['5301_1_2', '5303_1_1']);
      const au = page('au1', metal, 3, ['5302_1_2', '5304_1_1'], { releaseFull: false, density: .5, solidPick: true });      // ticked: its own yes (R12 a); backs not approved (R12 b)
      w.add(gf, ss, au); await w.assemble();
      assert(w.inSet(gf) && w.inSet(ss) && gf.setId === ss.setId, 'Set 1 formed from GF 1 + SS 1');
      // (Paul, 27 Oct 2026: "10 K and 14 K gold sheets should be approved to be part of a set just like any other sheet and then hold a given set of sheets until the engraving is done": its
      //  own yes puts it in its partner's set; its open backs are the set's to wait for, never a rule broken)
      verdict(`G1 ${tag}: a ticked ${tag} sheet whose back engraving is not approved joins its partner's set (Paul, 27 Oct 2026)`, w.inSet(au) && au.setId === gf.setId, `${tag} au1 is in ${au.setId || 'no set'}`);
      const v = w.violations(), lib = w.libraryCheck();
      verdict(`G1b ${tag}: no rule is broken by it (the set waits for its backs)`, !v.some(x => x.ruleNo === 'R12') && !lib.violations.some(x => x.ruleNo === 'R12'),
        `Gate.violations: [${v.map(x => x.ruleNo).join(', ')}]; Library: [${lib.violations.map(x => x.ruleNo).join(', ')}]`);
    }
    // ── G2 · NOT ticked, green line cut, back engraving not approved: the cardinal rule pulls it into its partner's set (its backs are no reason it stays out) ──
    {
      const w = world();
      const gf = page('gf1', 'gold', 1, ['5401_1_1', '5402_1_1']), ss = page('ss1', 'silver', 2, ['5401_1_2', '5403_1_1']);
      const au = page('au1', metal, 3, ['5402_1_2', '5404_1_1'], { releaseFull: false, density: .5, roseCutAt: Date.now() });
      w.add(gf, ss, au); await w.assemble();
      verdict(`G2 ${tag}: a cut, unticked ${tag} sheet with unapproved backs is pulled into its partner's set`, w.inSet(au) && au.setId === gf.setId, `${tag} is in ${au.setId || 'no set'}`);
    }
    // ── G2b (sanity of the stand-in) · the same sheet once its back engraving IS approved: now the cardinal rule pulls it into its partner's set ──
    {
      RD.approved = true;
      const w = world();
      const gf = page('gf1', 'gold', 1, ['5601_1_1', '5602_1_1']), ss = page('ss1', 'silver', 2, ['5601_1_2', '5603_1_1']);
      const au = page('au1', metal, 3, ['5602_1_2', '5604_1_1'], { releaseFull: false, density: .5, roseCutAt: Date.now() });
      w.add(gf, ss, au); await w.assemble(); RD.approved = false;
      assert(w.inSet(au) && au.setId === gf.setId, `sanity: a cut ${tag} sheet with approved backs joins its partner's set (${au.setId})`);
    }
    // ── G3 · R4: a ticked gold-family sheet never STARTS a set (only a completed GF, no SS: nothing may form) ──
    {
      const w = world();
      const gf = page('gf1', 'gold', 1, ['5501_1_1']);
      const au = page('au1', metal, 2, ['5502_1_1'], { releaseFull: true, solidPick: true, roseCutAt: Date.now() });
      w.add(gf, au); await w.assemble();
      verdict(`G3 ${tag}: a ticked, cut ${tag} sheet never starts or completes a set (R4)`, !w.inSet(au) && !w.inSet(gf), `gf1 in ${gf.setId}, ${tag} in ${au.setId}`);
    }
  }
  // ── G4 · "a recorded cut keeps a sheet in the set it is in" (R12, SetEdit sheetCut, Gate.staysCut): a 10K / 14K sheet the cardinal rule pulled into a set (no tick of its own) and that was then
  //    cut, after (a) the shared order is taken off its partner (Paul's own way out of a split, R9) and (b) a person ticks ANOTHER sheet of the same metal (Include). changeMembership then writes
  //    solidPick=false on every sheet of the metal that had no tick of its own, staysCut() reads solidPick !== false, and the cut sheet no longer qualifies: does it leave its set? ──
  for (const metal of ['gold10k', 'gold14k']) {
    const tag = metal === 'gold10k' ? '10K' : '14K';
    RD.approved = true;
    const w = world();
    const gf = page('gf1', 'gold', 1, ['5701_1_1', '5702_1_1']), ss = page('ss1', 'silver', 2, ['5701_1_2', '5703_1_1']);
    const au = page('au1', metal, 3, ['5702_1_2', '5704_1_1'], { releaseFull: false, density: .5, roseCutAt: Date.now() });      // cut, backs approved, no tick: the cardinal rule pulls it
    const other = page('au2', metal, 4, ['5705_1_1'], { releaseFull: false, density: .3 });
    w.add(gf, ss, au, other); await w.assemble();
    assert(w.inSet(au) && au.setId === gf.setId, `${tag} was pulled into its partner's set`);
    const setWas = au.setId;
    // (a) the order shared with GF 1 is taken off GF 1 (its piece goes to another sheet: here simply off this one)
    gf.placements = gf.placements.filter(x => x.id !== '5702_1_1'); gf.charms = gf.charms.filter(x => x.id !== '5702_1_1');
    await w.assemble();
    const stillA = w.inSet(au) && au.setId === setWas;
    // (b) a person ticks the other sheet of the metal
    let err = '';
    try { await w.include(metal, 'au2', true); } catch (e) { err = e.message; }
    await w.assemble();
    RD.approved = false;
    verdict(`G4 ${tag}: a cut ${tag} sheet in a set stays in it when another ${tag} sheet is ticked (R12: a recorded cut keeps a sheet in its set)`, w.inSet(au) && au.setId === setWas,
      `after (a) order taken off its partner: in set=${stillA}; after (b) Include on ${tag} au2${err ? ' (error: ' + err + ')' : ''}: au1 solidPick=${au.solidPick}, roseCutAt set, now setId=${au.setId} draft=${au.draft}`);
  }
  // ── G4c · the same without taking any order off: the cut sheet is still tied to GF 1, but its back engraving is no longer approved (a back re-edited after the cut) when the other sheet is ticked ──
  for (const metal of ['gold10k']) {
    RD.approved = true;
    const w = world();
    const gf = page('gf1', 'gold', 1, ['5801_1_1', '5802_1_1']), ss = page('ss1', 'silver', 2, ['5801_1_2', '5803_1_1']);
    const au = page('au1', metal, 3, ['5802_1_2', '5804_1_1'], { releaseFull: false, density: .5, roseCutAt: Date.now() }), other = page('au2', metal, 4, ['5805_1_1'], { releaseFull: false, density: .3 });
    w.add(gf, ss, au, other); await w.assemble();
    const setWas = au.setId; assert(setWas && setWas === gf.setId);
    RD.approved = false;                      // the back engraving of au1 is no longer approved
    try { await w.include(metal, 'au2', true); } catch (_) { /* reported below */ }
    await w.assemble();
    verdict('G4c 10K: a cut 10K sheet still tied to its set by an order stays in it when another 10K sheet is ticked while its back engraving is re-opened', w.inSet(au) && au.setId === setWas,
      `au1 (cut, tied to GF 1) solidPick=${au.solidPick}, now setId=${au.setId} draft=${au.draft}; Set 1 = [${w.pages.filter(p => p.setId === setWas && !p.draft).map(p => p.sheetId).join(', ')}]`);
  }
  console.log(breaks.length ? `\n${breaks.length} BREAK(S): ${breaks.join(' | ')}` : '\nPASS: audit-gold-gate: no break found');
  process.exit(breaks.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
