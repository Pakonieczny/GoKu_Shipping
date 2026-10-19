// SETFIX (Paul, 19 Oct 2026): "Seems like the first set is always correct, but all subsequent sets are all messed up ... the system is choosing to add over 10 sheets to set of sheets #2".
// The rule functions at their boundaries, offline (made-up sheets and order numbers; no page, no cloud):
//   1  charm-nest-shared-orders.js formation `anchors`: a sheet that cannot go into the set being formed (in a set, cut by the laser, an uncut Rose Gold / 10K / 14K sheet outside every set) is
//      attached to the groups it shares orders with but ties none of them together; the partial `ends` keep their rule ('hold' / 'spread', a touch through an anchor counted)
//   2  the checker (charm-nest-set-invariants.js) at the boundary of R7: exactly the minimum (1 completed GF + 1 completed SS) is right; a second unit tied to the first only through a sheet of a
//      committed set, or through an uncut Rose Gold sheet, is SPARE (the app's Set 2 of 10+ sheets was read as one unit and passed); tied directly, or through a cut Rose Gold / a ticked 14K, it is one unit
//   3  expected() / SetRules.formAll over those units: one minimal set per unit, in the order the sheets were seen completed (out-of-order completion), several metals
//   4  the page's real assembly (Gate, charm-nest-bridge.js, in a vm): units that share an order with a sheet of the COMMITTED Set 1 form Set 2, Set 3 ... one each; units tied to an uncut Rose Gold
//      sheet that keeps filling form their own sets and name it as the partner to add; the numbering orders a run's sets by day, then number (past midnight); a "new" set handed back that already holds
//      sheets takes none
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const SO = require(path.join(root, 'charm-nest-shared-orders.js')), SR = require(path.join(root, 'charm-nest-set-rules.js')), I = require(path.join(root, 'charm-nest-set-invariants.js'));
const O = require(path.join(root, 'charm-nest-orders.js')), SN = require(path.join(root, 'charm-nest-sheet-name.js'));
const sorted = l => l.slice().sort(), ofRule = (r, rule) => r.violations.filter(v => v.rule === rule), show = r => I.explain(r).join('\n  ');

/* ── 1 · formation with anchors ──────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const n = (id, keys, setId = null) => ({ id, label: id, metal: 'gold', setId, fixed: '', pieces: keys.map(k => ({ key: k, orderId: k.split('_')[0] })) });
  // A and B each share ONE order with C (a sheet of the committed Set 1); A and B share nothing with each other
  const A = n('A', ['100_1_1', '900_1_1']), B = n('B', ['200_1_1', '901_1_1']), C = n('C', ['100_1_2', '200_1_2'], 'set-1');
  assert.deepEqual(SO.core.formation([A, B, C], [], 'hold').groups.map(g => sorted(g.ids)), [['A', 'B', 'C']], 'without anchors the chain runs through C: A and B are one group (what built the 10+ sheet Set 2)');
  const F = SO.core.formation([A, B, C], [], 'hold', ['C']);
  assert.deepEqual(F.groups.map(g => sorted(g.ids)), [['A', 'C'], ['B', 'C']], 'C is an anchor: attached to each group it shares an order with, tying none of them');
  assert.deepEqual(F.groups.map(g => g.anchors), [['C'], ['C']]); assert.deepEqual(F.groups.map(g => g.orders), [['100'], ['200']]);
  // an order ON the anchor and a sheet (A–C) still makes A's group; an order between two anchors makes none; a direct order still ties
  const D = n('D', ['300_1_1']), E = n('E', ['300_1_2', '400_1_1']), C2 = n('C2', ['400_1_2'], 'set-1');
  assert.deepEqual(SO.core.formation([D, E, C2], [], 'hold', ['C2']).groups.map(g => sorted(g.ids)), [['C2', 'D', 'E']], 'D and E share order 300 directly: one group, with the anchor E shares an order with');
  assert.deepEqual(SO.core.formation([n('X', ['500_1_1'], 's'), n('Y', ['500_1_2'], 's')], [], 'hold', ['X', 'Y']).groups, [], 'two anchors alone make no group');
  // a partial END that touches a group through an anchor only: 'hold' attaches it to the groups it touches through a sheet that can join; 'spread' counts the anchor's set among the groups it touches
  const P = n('P', ['600_1_1', '700_1_1']), Q = n('Q', ['600_1_2']), S1 = n('S1', ['700_1_2'], 'set-2');
  assert.deepEqual(SO.core.formation([P, Q, S1], ['P'], 'hold', ['S1']).groups.map(g => [sorted(g.ids), g.ends]), [[['P', 'Q'], ['P']]], 'hold: Q waits with the partial P');
  const sp = SO.core.formation([P, Q, S1], ['P'], 'spread', ['S1']);
  assert.deepEqual(sp.bridges.map(b => [b.id, b.groups.map(sorted)]), [['P', [['Q'], ['S1']]]], 'spread: P touches Q\'s group and Set 2: a bridge, it holds neither');
}

/* ── 2 · the checker at the boundary of R7 ───────────────────────────────────────────────────────────────────────────────────────────────── */
let single = 4600000000; const own = k => Array.from({ length: k }, () => `${++single}_1_1`);
const sheet = (id, metal, no, o = {}) => ({ id, metal, sheetIndex: no, page: no, releaseFull: true, density: .75, placedCount: 4, poolIds: own(2), ...o });
const link = (a, b, ord) => { a.poolIds.push(`${ord}_1_1`); b.poolIds.push(`${ord}_1_2`); };
const S = (id, list, o = {}) => ({ id, name: id, seq: +id.replace(/\D/g, '') || 1, sheetIds: list.map(x => x.id), ...o });
{
  const g1 = sheet('g1', 'gold', 1, { setId: 'set-2' }), s1 = sheet('s1', 'silver', 1, { setId: 'set-2' });
  let r = I.check([S('set-2', [g1, s1])], [g1, s1]); assert.equal(r.ok, true, 'exactly the minimum: 1 completed GF + 1 completed SS\n  ' + show(r));
  // max + 1: a second unit (GF 2 + SS 2) in the same set
  const g2 = sheet('g2', 'gold', 2, { setId: 'set-2' }), s2 = sheet('s2', 'silver', 2, { setId: 'set-2' });
  r = I.check([S('set-2', [g1, s1, g2, s2])], [g1, s1, g2, s2]); assert.deepEqual(ofRule(r, 'surplus').map(v => sorted(v.sheets)), [['g2'], ['s2']], 'two loose sheets more than the minimum are spare');
  // ... tied to the first unit ONLY through a sheet of the committed Set 1 (each shares one order with it): still spare (the app read it as one unit and passed it)
  const c1 = sheet('c1', 'gold', 1, { setId: 'set-1' }), c2 = sheet('c2', 'silver', 1, { setId: 'set-1' }); link(g1, c1, 4610000001); link(g2, c1, 4610000002); link(g2, s2, 4610000003); link(s1, g1, 4610000004);
  const sets = () => [S('set-1', [c1, c2], { committedAt: 5, status: 'complete' }), S('set-2', [g1, s1, g2, s2])];
  r = I.check(sets(), [c1, c2, g1, s1, g2, s2], { skipExempt: true });
  assert.deepEqual(ofRule(r, 'surplus').map(v => sorted(v.sheets)), [['g2', 's2']], 'GF 2 + SS 2 share no order with GF 1 + SS 1 (their only tie is GF Sheet 1 of the committed Set 1): spare\n  ' + show(r));
  assert.equal(ofRule(r, 'cardinal').length, 0, 'the committed set is not the open set\'s fault');
  // ... tied directly (an order on GF 1 and GF 2): one unit, right
  link(g1, g2, 4610000005); r = I.check(sets(), [c1, c2, g1, s1, g2, s2], { skipExempt: true }); assert.equal(r.ok, true, 'tied directly: one unit\n  ' + show(r));
  // through an uncut Rose Gold sheet outside every set: spare; once it is cut (and its backs approved) it can join, so it ties them: one unit (R9), and it is named as the partner to add
  const a1 = sheet('a1', 'gold', 1), b1 = sheet('b1', 'silver', 1), a2 = sheet('a2', 'gold', 2), b2 = sheet('b2', 'silver', 2), rg = sheet('rg', 'rose', 9, { releaseFull: false, density: .2, draft: true });
  link(a1, b1, 4620000001); link(a2, b2, 4620000002); link(a1, rg, 4620000003); link(a2, rg, 4620000004);
  const big = [S('set-2', [a1, b1, a2, b2])], all = [a1, b1, a2, b2, rg];
  r = I.check(big, all); assert.deepEqual(ofRule(r, 'surplus').map(v => sorted(v.sheets)), [['a2', 'b2']], 'tied only through the uncut Rose Gold sheet: spare\n  ' + show(r));
  assert.deepEqual(r.partners.map(p => [p.sheet, p.kind]), [['rg', 'notReady']], 'the Rose Gold sheet is the partner to add');
  rg.roseCutAt = 7; rg.backsApproved = true; r = I.check(big, all);
  assert.equal(ofRule(r, 'surplus').length, 0, 'a cut Rose Gold sheet ties the sheets it shares orders with: one unit'); assert(ofRule(r, 'cardinal').length >= 1, 'and it belongs in the set (R9)');
  // 10K uncut outside: ties nothing; 14K ticked (the person's yes at Include / Cut Sheet): ties
  for (const [metal, o, tied] of [['gold10k', {}, false], ['gold14k', { cutApproved: true, backsApproved: true }, true]]) {
    const x1 = sheet('x1', 'gold', 1), y1 = sheet('y1', 'silver', 1), x2 = sheet('x2', 'gold', 2), y2 = sheet('y2', 'silver', 2), k = sheet('k', metal, 3, { releaseFull: false, draft: true, ...o });
    link(x1, y1, 4630000001); link(x2, y2, 4630000002); link(x1, k, 4630000003); link(x2, k, 4630000004);
    const rr = I.check([S('set-2', [x1, y1, x2, y2])], [x1, y1, x2, y2, k]); assert.equal(ofRule(rr, 'surplus').length === 0, tied, metal + (tied ? ' ticked ties' : ' uncut ties nothing'));
  }
  // R9 per order: two sets tied only through a sheet outside every set have no order "on sheets of different sets": the outside sheet is named instead
  const m1 = sheet('m1', 'gold', 1, { setId: 'sa' }), n1 = sheet('n1', 'silver', 1, { setId: 'sa' }), m2 = sheet('m2', 'gold', 2, { setId: 'sb' }), n2 = sheet('n2', 'silver', 2, { setId: 'sb' }), f = sheet('f', 'gold', 3, { draft: true });
  link(m1, f, 4640000001); link(f, m2, 4640000002);
  r = I.check([S('sa', [m1, n1]), S('sb', [m2, n2])], [m1, n1, m2, n2, f]);
  assert(!r.violations.some(v => v.rule === 'cardinal' && v.sets), 'no order is on sheets of two sets'); assert(ofRule(r, 'cardinal').every(v => v.outside === 'f') && ofRule(r, 'cardinal').length === 2, 'GF Draft 3 is the one named, in each set\n  ' + show(r));
  link(m1, m2, 4640000003); r = I.check([S('sa', [m1, n1]), S('sb', [m2, n2])], [m1, n1, m2, n2, f]);
  assert.deepEqual(r.violations.filter(v => v.rule === 'cardinal' && v.sets).map(v => v.orders), [['4640000003']], 'an order on sheets of two sets is named, and only that order');
}

/* ── 3 · expected(): one minimal set per unit, in the order the sheets were seen completed ────────────────────────────────────────────────── */
{
  const c = sheet('c', 'gold', 1, { setId: 'set-1' }), u = [];
  for (let k = 1; k <= 4; k++) { const g = sheet('g' + k, 'gold', k + 1, { releasedAt: 100 - k }), s = sheet('s' + k, 'silver', k + 1, { releasedAt: 100 - k }); link(g, s, 4650000000 + k); link(g, c, 4651000000 + k); u.push(g, s); }   // seen completed last-to-first
  // the committed Set 1 is not part of the formation (expected() takes the sheets it is given as free): the four units alone
  const e = I.expected(u, { firstSeq: 2 });
  assert.deepEqual(e.sets.map(s => sorted(s.sheetIds)), [['g4', 's4'], ['g3', 's3'], ['g2', 's2'], ['g1', 's1']], 'four units, four sets of two, the one seen completed first first (out-of-order completion)');
  // the formation beside the committed Set 1 (each unit shares an order with its sheet): accepted, nothing spare, the committed set is not the open sets' fault
  const one = S('set-1', [c], { committedAt: 5, status: 'complete' }), mine = e.sets.map(x => ({ ...x }));
  for (const x of mine) for (const id of x.sheetIds) Object.assign(u.find(p => p.id === id), { setId: x.id, draft: false });
  const r = I.check([one].concat(mine), u.concat([c]), { skipExempt: true }); assert.equal(r.ok, true, show(r));
  // and one set of all four units (what the app built) is refused: three units are spare
  const glued = [one, S('set-2', u)]; for (const p of u) p.setId = 'set-2';
  assert.equal(ofRule(I.check(glued, u.concat([c]), { skipExempt: true }), 'surplus').length, 3, 'three of the four units are spare');
}

/* ── 4 · the page's real assembly ────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const GATE = code.slice(code.indexOf('const Gate = window.Gate ='), code.indexOf('/* ═══ 21', code.indexOf('const Gate = window.Gate ='))), setsAt = code.indexOf('  let groups = null, groupsOf = null'), SETS = code.slice(setsAt, code.indexOf('  const setOfSheet =', setsAt));
assert(GATE.length > 1000 && /function allocate/.test(SETS), 'the page code to test was found');
const RUN = 'run-anchors';
function world({ ensure } = {}) {
  const c = vm.createContext({ O, SO, SR, SN, I, RUN, server: { sets: new Map(), keys: new Map(), seq: {} }, clock: { day: '2026-10-19' }, log: [], fakeEnsure: ensure || null });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSheetName:SN,CharmNestSetInvariants:I,CharmNestSetRules:SR};
const S={cloud:{ok:true},settings:{}}, CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[], allSheets=()=>pages, labelOf=m=>m, today=()=>clock.day, refreshAllCards=()=>{}, agent=(s,kind,text)=>log.push({kind,text});
const api=async(n,b)=>{ if(b.op==='setAllocate'){ if(b.after){const pr=server.sets.get(b.after); if(!pr||!(pr.committedAt||(pr.sheetIds||[]).length)) throw new Error('not a committed set of the run');}
    const key=b.runId+'|'+(b.group||'')+(b.after?'|after:'+b.after:''); if(server.keys.has(key)) return Object.assign({ok:true},server.keys.get(key));
    const seq=server.seq[b.day]=(server.seq[b.day]||0)+1, r={setId:'set-'+b.day+'-'+seq,seq,day:b.day}; server.keys.set(key,r); server.sets.set(r.setId,{sheetIds:[]}); return Object.assign({ok:true},r); }
  if(b.op==='setUpdate'){ const s=server.sets.get(b.setId)||{}; Object.assign(s,b.patch); server.sets.set(b.setId,s); } return {}; };
const Orders={rows:()=>[]}, Engrave={items:()=>new Map(),writeBacks:async()=>{},saveSheetBacks:async()=>{}}, Pool={update:async()=>{}}, RunCtl={onSheetDone(){},save:async()=>{},renderBanner(){}}, toast=()=>{};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,loadLibrary:async()=>{}};
const Sets=(()=>{ const O={setLabel:s=>'Set-'+s,setFolder:(d,s)=>'f/'+d+'/'+s};
${SETS}
  async function save(set){ await api('charmNestLibrary',{op:'setUpdate',setId:set.setId,patch:{sheetIds:set.sheetIds.slice(),committedAt:set.committedAt||null}}); }
  const onSheetSaved=async(sh,_c,_u,o)=>{const set=o.setOverride; if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId); if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);};
  return {ensure:fakeEnsure?(...a)=>fakeEnsure(B,...a):ensure,ofRun,keyOf,byRun,save,onSheetSaved,labelsReady:()=>true};
})();`, c);
  vm.runInContext(GATE, c);
  const pg = [];
  const w = {
    c, pg,
    add(...l) { for (const p of l) { c.__p = p; vm.runInContext('pages.push(__p)', c); pg.push(p); } },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    sets: () => Array.from(vm.runInContext('Sets.ofRun(RUN)', c), s => ({ name: s.name, day: s.day, ids: sorted(pg.filter(p => p.setId === s.setId && !p.draft).map(p => p.sheetId)), set: s })),
    check: () => JSON.parse(vm.runInContext('JSON.stringify(Gate.selfCheck(B.run))', c)),
  };
  return w;
}
let ordNo = 4700000000;
const page = (id, metal, no, o = {}) => ({ metal, page: no, runId: RUN, sheetId: id, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .75, placements: [], charms: [], verification: { ok: true }, releaseFull: true, ...o });
const put = (p, ord, k) => { const pid = `${ord}_1_${k}`; p.charms.push({ id: pid, poolId: pid, order: ord }); p.placements.push({ id: pid }); };
const lone = p => { put(p, String(++ordNo), 1); put(p, String(++ordNo), 1); return p; };
const tie = (a, b) => { const o = String(++ordNo); put(a, o, 1); put(b, o, 2); };
(async () => {
  // 4a · Set 1 committed; every later unit shares one order with a sheet of Set 1: each is a set of its own (the app put them all into Set 2)
  {
    const w = world(), g1 = lone(page('GF1', 'gold', 1)), s1 = lone(page('SS1', 'silver', 1)); tie(g1, s1); w.add(g1, s1); await w.assemble();
    const one = w.sets()[0]; assert.deepEqual(one.ids, ['GF1', 'SS1']); one.set.committedAt = 5; one.set.status = 'complete';
    for (let k = 2; k <= 6; k++) { const g = lone(page('GF' + k, 'gold', k)), s = lone(page('SS' + k, 'silver', k)); tie(g, s); tie(k % 2 ? g1 : s1, g); w.add(g, s); }
    await w.assemble();
    assert.deepEqual(w.sets().map(s => s.ids.length), [2, 2, 2, 2, 2, 2], 'Set 1 and five sets of two, never one set of ten: ' + JSON.stringify(w.sets().map(s => s.ids)));
    const r = w.check(); assert.equal(r.ok, true, r.lines.join('\n'));
  }
  // 4b · a Rose Gold sheet that keeps filling (no green line) takes one piece of an order of each new unit: each unit is a set of its own; the Rose Gold sheet is the partner to add of each
  {
    const w = world(), g1 = lone(page('GF1', 'gold', 1)), s1 = lone(page('SS1', 'silver', 1)); tie(g1, s1); w.add(g1, s1); await w.assemble();
    const rg = lone(page('RG9', 'rose', 9, { releaseFull: false, density: .2 })); w.add(rg);
    for (let k = 2; k <= 7; k++) { const g = lone(page('GF' + k, 'gold', k)), s = lone(page('SS' + k, 'silver', k)); tie(g, s); tie(k % 2 ? s : g, rg); w.add(g, s); await w.assemble(); }
    assert.deepEqual(w.sets().map(s => s.ids.length), [2, 2, 2, 2, 2, 2, 2], 'seven sets of two (the app made Set 2 of twelve): ' + JSON.stringify(w.sets().map(s => s.ids)));
    assert(!rg.setId, 'the uncut Rose Gold sheet is in no set'); const r = w.check(); assert.equal(r.ok, true, r.lines.join('\n'));
    assert.equal(r.partners.filter(p => p.sheet === 'RG9').length, 6, 'it is named as the partner to add of each set it shares an order with');
  }
  // 4c · past midnight: the day's counter starts again at 1; the run's sets are read by day, then number, so the next sets follow the newest one (the app put every later unit into the new day's Set 1)
  {
    const w = world(); w.c.server.seq['2026-10-19'] = 6;
    const unit = k => { const g = lone(page('GF' + k, 'gold', k)), s = lone(page('SS' + k, 'silver', k)); w.add(g, s); };
    unit(1); await w.assemble(); w.c.clock.day = '2026-10-20';
    for (let k = 2; k <= 6; k++) { unit(k); await w.assemble(); }
    assert.deepEqual(w.sets().map(s => [s.name, s.day, s.ids.length]), [['Set-7', '2026-10-19', 2], ['Set-1', '2026-10-20', 2], ['Set-2', '2026-10-20', 2], ['Set-3', '2026-10-20', 2], ['Set-4', '2026-10-20', 2], ['Set-5', '2026-10-20', 2]], 'one set per unit, in day order');
  }
  // 4d · the numbering hands back a set that already holds sheets as the "new" set: nothing joins it, the sheets wait with the reason, the agent log says it once
  {
    const w = world({ ensure: async (B, runId) => [...B.sets.values()][0] });
    w.c.B = undefined;
    vm.runInContext(`B.sets.set('x',{setId:'set-x',seq:1,day:'2026-10-19',runId:RUN,group:'dispatch',name:'Set-1',sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}})`, w.c);
    const g1 = lone(page('GF1', 'gold', 1)), s1 = lone(page('SS1', 'silver', 1)); w.add(g1, s1); await w.assemble();
    assert.deepEqual(w.sets()[0].ids, ['GF1', 'SS1'], 'the run\'s open set took its first unit (it was empty)');
    const g2 = lone(page('GF2', 'gold', 2)), s2 = lone(page('SS2', 'silver', 2)); w.add(g2, s2); await w.assemble();
    assert.deepEqual(w.sets()[0].ids, ['GF1', 'SS1'], 'the formed set handed back as the next set took nothing');
    assert(g2.draft && s2.draft && /handed back as the next set/.test(g2.setWait), 'they wait, with the reason: ' + g2.setWait);
    assert.equal(w.c.log.filter(l => l.kind === 'warn' && /handed back as the next set/.test(l.text)).length, 1, 'the agent log says it once');
  }
  console.log('PASS: sets-anchors: a sheet in a set, a cut sheet and an uncut Rose Gold / 10K / 14K sheet tie no units together (formation, checker, expected, the page\'s assembly); exactly the minimum is right and one unit more is spare; one set per unit after the committed Set 1, around a filling Rose Gold sheet and past midnight; a "new" set that holds sheets takes none');
})().catch(e => { console.error(e); process.exit(1); });
