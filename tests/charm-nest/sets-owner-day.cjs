// THE OWNER'S DAY, replayed through the REAL set assembly (Gate.assemble, charm-nest-bridge.js) and judged by the owner's own rules (charm-nest-set-invariants.js, docs/charm-nest/set-rules.md).
//
// Paul, 17 Oct 2026: "Set 2 was formed incorrectly ... it's a very simple set of requirements ... I want you to fix it once and for all." "There are many discrepancies with set number two such as partial
// sheets being added without the green line and way too many sheets in total ended up in set 2, completely breaking my rules."
// His Set 2 held 14 sheets: GF 1-8, SS 1-4, RG 16 and RG 29 (the Rose Gold sheets uncut, no green line; RG 29 only 29% full). They are three independent groups of sheets tied by shared orders
//   C = SS1 - GF2 - GF1,  B = GF5 - RG16 - GF4 - SS2 - GF3,  A = SS4 - GF7 - RG29 - GF6 - SS3   and one loose GF 8 (no order ties it to anything), with GF Draft 10 (partial) sharing an order with one of them.
// Set 1 (GF Sheet 1 + SS Sheet 1 sharing order 4191315127) was right. The right result for the rest is what the rules make of those sheets: Set 2 = C, Set 3 = B (RG 16 the partner to add), Set 4 = A
// (RG 29 the partner to add), GF 8 waits outside every set ("it needs a completed SS sheet with it"), and the group that shares an order with the partial GF Draft 10 waits whole until it is full.
//
// What this test does (offline: the real Gate in a vm over made-up pages, no cloud, no browser):
//   1  the day arrives step by step in every order that matters (Rose Gold first / last / in between, the three groups in all six orders, sheets finishing one at a time in a seeded shuffle, everything at once);
//      AFTER EVERY STEP: the sets the Gate made are exactly the sets the rules make of the sheets as they are (CharmNestSetInvariants.expected), the checker finds nothing wrong (check ok), no Rose Gold sheet
//      without its green line is in a set, GF 8 is outside, the partner of a set is named and keeps it from being approved
//   2  GF Draft 10 shares an order with each group in turn: that group waits WHOLE (no sheet of it in any set) until Draft 10 is full, the others form their sets
//   3  RG 16 is cut later: it joins ITS group's set (and only that one); RG 29 stays out until it is cut too; an uncut Rose Gold sheet that a person includes is the person's own yes, this sheet only
//   4  the leaks the investigation found are closed: the run-wide Rose Gold tick, the old even-numbered rule and a Rose Gold sheet in a closed set cannot pull a whole group into it
//   5  a partial sheet that shares orders with SEVERAL groups: SetRules.PARTIAL_BRIDGE 'hold' (default: every group it touches waits whole until it is full, then they are one group and one set) and 'spread'
//      (each complete group forms its own minimal set now; the sheet and its mixed orders are flagged for a person to take off)
//   6  the live self-check: the owner's 14-sheet Set 2 is reported rule by rule (Gate.violations), one agent-log warning, the stamps joinedAt / formedAt make the closed-set rule work in the live app;
//      without the set rules the assembly forms nothing and says so
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), SO = require(path.join(root, 'charm-nest-shared-orders.js')), SR = require(path.join(root, 'charm-nest-set-rules.js')), SN = require(path.join(root, 'charm-nest-sheet-name.js')), I = require(path.join(root, 'charm-nest-set-invariants.js'));
const code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), gateAt = code.indexOf('const Gate = window.Gate ='), GATE = code.slice(gateAt, code.indexOf('/* ═══ 21', gateAt));
assert(gateAt > 0, 'the page code to test was found');
const RUN = 'run-2026-10-17-owner';
const NAME = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };

/* ── the page, as far as the Gate reads it ───────────────────────────────────────────────────────────────────────────────────────────────────────── */
const page = (tag, metal, pg, pool, o = {}) => ({ metal, page: pg, runId: RUN, sheetId: tag, draft: true, outputs: { ai: 'a' }, persistedDone: true, status: 'complete', density: .74,
  placements: pool.map(x => ({ id: x })), charms: pool.map(x => ({ id: x, poolId: x, order: x.split('_')[0] })), verification: { ok: true }, releaseFull: true, ...o });
function world(opts = {}) {
  const c = vm.createContext({ assert, O, SO, SR, SN, I, console, RUN, allocations: 0, writes: [], withRules: opts.rules !== false });
  vm.runInContext(`
const window={CharmNestOrders:O,SharedOrders:SO,CharmNestSheetName:SN,CharmNestSetInvariants:I}, S={cloud:{ok:true},settings:{}}, METALS=['gold','silver','rose','gold10k','gold14k'].map(key=>({key}));
if (withRules) window.CharmNestSetRules = SR;
const CharmNestSheetName=SN;
const B={run:{runId:RUN,releasePolicy:2,solidIncluded:{}},sets:new Map(),engrave:{items:new Map()},pool:{rows:new Map()},orders:{rows:[],byKey:new Map()}};
const pages=[]; let seq=1;
const allSheets=()=>pages, labelOf=m=>m, today=()=> '2026-10-17', refreshAllCards=()=>{};
const agentLog=[]; const agent=(scope,kind,text)=>agentLog.push({scope,kind,text});
const api=async(n,b)=>{writes.push({n,b});return {}};
const Orders={rows:()=>B.orders.rows}, Engrave={items:()=>{throw new Error('forming a set must not read the back engravings')},writeBacks:async()=>{},saveSheetBacks:async()=>{}};
const Pool={update:async(ids,p)=>writes.push({ids,p})};
const RunCtl={onSheetDone(){},save:async()=>{},poke(){},membershipUpdated(){}}, Session={schedule(){}}, toast=()=>{}, document={getElementById:()=>null};
const CN={sheetFileBase:sh=>'Set-'+sh.seq+'-'+sh.metal+'-'+sh.sheetIndex,persistSheet:()=>{throw new Error('must not re-render the artwork')},loadLibrary:async()=>{}};
const Sets={onSheetSaved:async(sh,_c,_u,o)=>{const set=o&&o.setOverride;if(!set.sheetIds.includes(sh.sheetId))set.sheetIds.push(sh.sheetId);if(!set.materials.includes(sh.metal))set.materials.push(sh.metal);},labelsReady:()=>true,ofRun:()=>[...B.sets.values()].sort((a,b)=>a.seq-b.seq),
  save:async(set)=>{},
  ensure:async(runId,group,o)=>{ if(o&&o.follow&&!o.follow.sheetIds.length) throw new Error('the set this one follows holds no sheet yet: the server refuses to number a set after it'); allocations++;const s={runId,group,seq:seq++,day:today(),setId:'set-'+allocations,name:'Set-'+(seq-1),sheetIds:[],labelFiles:[],materials:[],orders:{},sheetNos:{}};B.sets.set(s.setId,s);return s}};
`, c);
  vm.runInContext(GATE, c);
  const json = expr => JSON.parse(vm.runInContext(`JSON.stringify(${expr})`, c));
  const w = {
    c, pages: [],
    add(...list) { c.__p = list; vm.runInContext('pages.push(...__p)', c); w.pages.push(...list); },
    assemble: () => vm.runInContext('Gate.assemble(B.run)', c),
    by: id => w.pages.find(p => p.sheetId === id),
    inSet: p => !!p && !p.draft && !!p.setId,
    /** the sets as the pages say, in set order: [[sheetId...]] */
    sets: () => { const by = new Map(); for (const p of w.pages) if (w.inSet(p)) { if (!by.has(p.setId)) by.set(p.setId, { seq: p.seq, ids: [] }); by.get(p.setId).ids.push(p.sheetId); } return [...by.values()].sort((a, b) => a.seq - b.seq).map(s => s.ids.sort()); },
    setOf: id => { const p = w.by(id); return w.inSet(p) ? p.seq : null; },
    input: () => json('Gate.invariantInput(B.run)'),
    check: () => json('Gate.selfCheck(B.run)'),
    violations: () => json('Gate.violations(B.run)'),
    log: () => json('agentLog'), run: () => c.B ? null : vm.runInContext('B.run', c),
    state: () => vm.runInContext('B.run', c),
    word: () => w.sets().map((ids, k) => `Set ${k + 1}: [${ids.join(', ')}]`).join(' | '),
  };
  return w;
}

/* ── the owner's sheets ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
function scene() {
  let n = 0; const uid = () => `97${String(++n).padStart(5, '0')}`;
  const sh = {};
  const add = (tag, metal, pg, o) => { sh[tag] = page(tag, metal, pg, [`${uid()}_1_1`, `${uid()}_1_1`], o); return sh[tag]; };
  const link = (a, b, ord = uid()) => { for (const [t, k] of [[a, 1], [b, 2]]) { sh[t].placements.push({ id: `${ord}_1_${k}` }); sh[t].charms.push({ id: `${ord}_1_${k}`, poolId: `${ord}_1_${k}`, order: ord }); } };
  const part = { releaseFull: false, density: .5 };
  // Set 1: GF Sheet 1 + SS Sheet 1 share order 4191315127
  add('g0', 'gold', 1); add('s0', 'silver', 1); link('g0', 's0', '4191315127');
  // C: SS1 - GF2 - GF1
  add('sC1', 'silver', 2); add('gC1', 'gold', 2); add('gC2', 'gold', 3); link('sC1', 'gC1', '4194075473'); link('gC1', 'gC2', '4195836918');
  // B: GF3 - SS2 - GF4 - GF5 - RG16 (RG 16 not cut: no green line)
  add('gB3', 'gold', 4); add('gB4', 'gold', 5); add('gB5', 'gold', 6); add('sB2', 'silver', 3); add('rB16', 'rose', 16, { releaseFull: false, density: .75 });
  link('gB3', 'sB2', '4196364003'); link('sB2', 'gB4', '4196987621'); link('gB4', 'gB5', '4198617495'); link('gB5', 'rB16', '4198617496');
  // A: GF6 - SS3 - GF7 - SS4 - RG29 (RG 29 only 29% full, not cut)
  add('gA6', 'gold', 7); add('gA7', 'gold', 8); add('sA3', 'silver', 4); add('sA4', 'silver', 5); add('rA29', 'rose', 29, { releaseFull: false, density: .29 });
  link('gA6', 'sA3', '4200871209'); link('sA3', 'gA7', '4200987085'); link('gA7', 'sA4', '4202194362'); link('sA4', 'rA29', '4202194363');
  // the loose GF 8
  add('gL8', 'gold', 9);
  // GF Draft 10 (partial) and other drafts that are not full
  add('d10', 'gold', 10, part);
  for (let i = 11; i < 20; i++) add('d' + i, i % 2 ? 'silver' : 'gold', i, { releaseFull: false, density: .3 + (i - 10) * .02 });
  const groups = { P1: ['g0', 's0'], C: ['sC1', 'gC1', 'gC2'], B: ['gB3', 'gB4', 'gB5', 'sB2'], A: ['gA6', 'gA7', 'sA3', 'sA4'], L: ['gL8'] };
  const rg = { B: 'rB16', A: 'rA29' };
  const full = id => /^[gs]/.test(id);
  return { sh, add, link, uid, groups, rg, full, tags: Object.keys(sh) };
}

/* ── what is judged after EVERY step ────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const part = sets => sets.map(s => s.slice().sort()).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
async function look(w, what, extra = {}) {
  await w.assemble();
  const input = w.input(), res = w.check(), exp = I.expected(input.sheets);
  assert(res, what + ': the self-check ran');
  assert.equal(res.ok, true, what + ': the checker (the owner\'s rules) finds a set wrong:\n  ' + res.lines.join('\n  ') + '\n  ' + w.word());
  assert.deepEqual(part(w.sets()), part(exp.sets.map(s => s.sheetIds)), what + ': the sets are what the rules make of these sheets\n  have ' + w.word() + '\n  want ' + exp.sets.map(s => '[' + s.sheetIds.join(', ') + ']').join(' | '));
  assert.deepEqual(w.violations(), [], what + ': Gate.violations() is empty');
  for (const p of w.pages) {
    if (['rose', 'gold10k', 'gold14k'].includes(p.metal) && w.inSet(p) && !p.solidPick) assert(p.roseCutAt, what + ': ' + p.sheetId + ' is a gold-family sheet in a set without its green line cut, nobody asked for it');
    if (['gold', 'silver'].includes(p.metal) && w.inSet(p)) assert(SR.isCompleted(p), what + ': ' + p.sheetId + ' is in a set but is not full');
  }
  if (w.by('gL8')) assert(!w.inSet(w.by('gL8')) || extra.l8ok, what + ': GF 8 shares no order with anything: it waits outside every set (a set needs a completed SS sheet with it)');
  return { input, res, exp };
}

(async () => {
  /* ═══ 1 · THE DAY, as the sheets arrive ═══════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  /* 1a · the canonical order: Set 1, the Rose Gold sheets exist (filling, uncut) BEFORE their partners are full, then C, B, A, then GF 8; the owner's numbers come out */
  {
    const S = scene(), { sh, groups } = S, w = world();
    const full = tags => { for (const t of tags) { sh[t].releaseFull = true; sh[t].density = .75; } };
    // everything exists; only Set 1's pair is full. B and A are still filling, C too: nothing but Set 1 may form
    const filling = ['sC1', 'gC1', 'gC2', 'gB3', 'gB4', 'gB5', 'sB2', 'gA6', 'gA7', 'sA3', 'sA4', 'gL8']; for (const t of filling) Object.assign(sh[t], { releaseFull: false, density: .5 });
    w.add(...Object.values(sh));
    full(groups.P1); await look(w, 'Set 1 pair'); assert.deepEqual(w.sets(), [['g0', 's0']], 'Set 1 = GF Sheet 1 + SS Sheet 1, as it was');
    // C is full: Set 2 = C
    full(groups.C); await look(w, 'C full'); assert.deepEqual(w.sets(), [['g0', 's0'], ['gC1', 'gC2', 'sC1']], 'Set 2 = unit C (GF 1, GF 2, SS 1) and nothing else');
    // B is full (RG 16 is still uncut, filling at 75%): Set 3 = B, RG 16 is the partner to add
    full(groups.B); await look(w, 'B full'); assert.deepEqual(w.sets()[2], ['gB3', 'gB4', 'gB5', 'sB2'], 'Set 3 = GF 3, GF 4, GF 5, SS 2 (RG 16 is not in it)');
    assert.equal(w.inSet(sh.rB16), false); assert(/shared with RG Draft 16: add that sheet to Set-3 \(a Rose Gold sheet joins a set only by its own Cut Sheet press\)$/.test(sh.gB5.cardinalNote), sh.gB5.cardinalNote);
    let res = w.check(); assert.deepEqual(res.partners.map(p => [p.setName, p.name, p.kind]), [['Set-3', 'RG Draft 16', 'notReady']], 'RG 16 is named as the partner to add to Set 3 and keeps it from being approved'); assert.equal(res.sets.find(s => s.name === 'Set-3').canApprove, false);
    // A is full (RG 29, 29% full and uncut, is still outside): Set 4 = A
    full(groups.A); await look(w, 'A full'); assert.deepEqual(w.sets()[3], ['gA6', 'gA7', 'sA3', 'sA4'], 'Set 4 = GF 6, GF 7, SS 3, SS 4 (RG 29 is not in it)'); assert.equal(w.inSet(sh.rA29), false);
    res = w.check(); assert.deepEqual(res.partners.map(p => p.name).sort(), ['RG Draft 16', 'RG Draft 29'], 'both Rose Gold sheets are partners to add, each to its own set');
    // GF 8 is full: it needs a completed SS sheet with it
    full(groups.L); await look(w, 'GF 8 full'); assert.equal(w.sets().length, 4, 'four sets, GF 8 in none'); assert(!w.inSet(sh.gL8)); assert(/needs a completed SS sheet with it/.test(sh.gL8.setWait), sh.gL8.setWait);
    // 14 sheets, 4 sets, never one set of 14
    assert.deepEqual(w.sets().map(s => s.length), [2, 3, 4, 4], 'the sheet counts: 2, 3, 4, 4, not 14');
    // a second look changes nothing
    const was = JSON.stringify(w.sets()), writes = w.c.writes.length; await w.assemble(); assert.equal(JSON.stringify(w.sets()), was); assert.equal(w.c.writes.length, writes, 'looked at again: nothing is written');
    // RG 16 is cut later (its back engraving is not waiting): it joins ITS group's set, Set 3, and no other; RG 29 stays out
    sh.rB16.roseCutAt = Date.now(); await look(w, 'RG 16 cut');
    assert.equal(w.setOf('rB16'), w.setOf('gB3'), 'RG 16 joined the set of the sheet it shares an order with (Set 3)'); assert.equal(w.setOf('rB16'), 3); assert.equal(w.inSet(sh.rA29), false, 'RG 29 is not cut: it stays outside');
    assert.equal(w.sets()[3].includes('rB16'), false, 'and not in Set 4'); assert(!sh.gB5.cardinalNote, 'its partner is in: nothing is named for Set 3 any more');
    assert.deepEqual(w.check().partners.map(p => p.name), ['RG Draft 29'], 'only RG 29 is still to be added (to Set 4)'); assert.equal(w.check().sets.find(s => s.name === 'Set-3').canApprove, true, 'Set 3 can be approved now');
    sh.rA29.roseCutAt = Date.now(); await look(w, 'RG 29 cut'); assert.equal(w.setOf('rA29'), 4, 'RG 29 joined Set 4, its group\'s set'); assert.deepEqual(w.sets().map(s => s.length), [2, 3, 5, 5]);
    // the stamps the closed-set rule needs
    for (const p of w.pages.filter(p => w.inSet(p))) assert(p.joinedAt > 0, p.sheetId + ' carries the time it joined its set');
    for (const s of vm.runInContext('[...B.sets.values()]', w.c)) assert(s.formedAt > 0, s.name + ' carries the time it formed');
  }

  /* 1b · every order of the three groups × Rose Gold first / in between / last. Sheets are full group by group; after EVERY step the Gate agrees with the rules */
  {
    const perms = [['C', 'B', 'A'], ['C', 'A', 'B'], ['B', 'C', 'A'], ['B', 'A', 'C'], ['A', 'C', 'B'], ['A', 'B', 'C']];
    let steps = 0;
    for (const order of perms) for (const when of ['first', 'between', 'last']) {
      const S = scene(), { sh, groups } = S, w = world(), tag = `${order.join('')}/RG ${when}`;
      const filling = ['sC1', 'gC1', 'gC2', 'gB3', 'gB4', 'gB5', 'sB2', 'gA6', 'gA7', 'sA3', 'sA4', 'gL8']; for (const t of filling) Object.assign(sh[t], { releaseFull: false, density: .5 });
      const roseTags = ['rB16', 'rA29'], roses = roseTags.map(t => sh[t]), rest = Object.values(sh).filter(p => !roseTags.includes(p.sheetId));
      w.add(...rest); if (when === 'first') w.add(...roses);
      Object.assign(sh.g0, { releaseFull: true }); Object.assign(sh.s0, { releaseFull: true }); await look(w, tag + ' · Set 1'); steps++;
      for (const [k, g] of order.entries()) {
        if (when === 'between' && k === 1) { w.add(...roses); await look(w, tag + ' · the Rose Gold sheets arrive'); steps++; }
        for (const t of groups[g]) { Object.assign(sh[t], { releaseFull: true, density: .75 }); await look(w, `${tag} · ${t} full`); steps++; }   // sheet by sheet inside the group: the group forms when its LAST sheet is full
        const sets = w.sets(); assert.equal(sets.length, 2 + k, `${tag} · after ${g}: ${sets.length} sets`);
        assert.deepEqual(sets[1 + k], groups[g].slice().sort(), `${tag} · set ${2 + k} is exactly group ${g}`);
      }
      if (when === 'last') { w.add(...roses); await look(w, tag + ' · the Rose Gold sheets arrive last'); steps++; }
      Object.assign(sh.gL8, { releaseFull: true }); await look(w, tag + ' · GF 8 full'); steps++;
      assert.deepEqual(w.sets().map(s => s.length).sort(), [2, 3, 4, 4], tag + ': 2, 3, 4, 4 sheets'); assert(!w.inSet(sh.gL8) && !w.inSet(sh.rB16) && !w.inSet(sh.rA29), tag + ': GF 8 and the uncut Rose Gold sheets are outside every set');
      // both Rose Gold sheets are cut at once, in either order of looking: each one goes to its own group's set
      sh.rA29.roseCutAt = sh.rB16.roseCutAt = Date.now(); await look(w, tag + ' · both cut'); steps++;
      assert.equal(w.setOf('rB16'), w.setOf('gB3'), tag + ': RG 16 with group B ' + w.word()); assert.equal(w.setOf('rA29'), w.setOf('gA6'), tag + ': RG 29 with group A'); assert.notEqual(w.setOf('rB16'), w.setOf('rA29'), tag + ': two groups, two sets');
    }
    console.log(`   1b: ${perms.length * 3} orders of the day, ${steps} steps judged against the rules`);
  }

  /* 1c · sheets finish ONE AT A TIME in a seeded shuffle (the groups interleave: a group forms the moment its last sheet is full), the Rose Gold sheets come and are cut at random moments */
  {
    const rng = seed => () => (seed = (seed * 1664525 + 1013904223) >>> 0) / 4294967296;
    let steps = 0;
    for (let seed = 1; seed <= 24; seed++) {
      const r = rng(seed * 13 + 7), S = scene(), { sh } = S, w = world(), tag = `shuffle ${seed}`;
      const filling = ['sC1', 'gC1', 'gC2', 'gB3', 'gB4', 'gB5', 'sB2', 'gA6', 'gA7', 'sA3', 'sA4', 'gL8', 'g0', 's0']; for (const t of filling) Object.assign(sh[t], { releaseFull: false, density: .5 });
      const plan = [...filling.map(t => ({ t, k: 'full' })), { t: 'rB16', k: 'add' }, { t: 'rA29', k: 'add' }, { t: 'rB16', k: 'cut' }, { t: 'rA29', k: 'cut' }];
      // random order, but a Rose Gold sheet is cut after it arrived
      const order = plan.slice(); for (let i = order.length - 1; i > 0; i--) { const j = Math.floor(r() * (i + 1)); [order[i], order[j]] = [order[j], order[i]]; }
      const seen = new Set(); const fixed = []; const later = [];
      for (const e of order) { if (e.k === 'cut' && !seen.has(e.t + 'add')) later.push(e); else { fixed.push(e); seen.add(e.t + e.k); } }
      const seq = []; for (const e of fixed) { seq.push(e); for (let i = later.length - 1; i >= 0; i--) if (seen.has(later[i].t + 'add')) { seq.push(later[i]); later.splice(i, 1); } }
      seq.push(...later);
      w.add(...Object.values(sh).filter(p => !['rB16', 'rA29'].includes(p.sheetId)));
      for (const e of seq) {
        if (e.k === 'full') Object.assign(sh[e.t], { releaseFull: true, density: .75 });
        else if (e.k === 'add') w.add(sh[e.t]);
        else sh[e.t].roseCutAt = Date.now();
        if (e.k === 'cut' && !w.pages.includes(sh[e.t])) continue;
        await look(w, `${tag} · ${e.t} ${e.k}`); steps++;
      }
      const sets = w.sets(); assert.equal(sets.length, 4, tag + ': four sets in the end'); assert(!w.inSet(sh.gL8), tag + ': GF 8 outside');
      assert.deepEqual(sets.map(s => s.length).sort(), [2, 3, 5, 5], tag + ': 2, 3, 5, 5 sheets (the Rose Gold sheets are in once cut)');
    }
    console.log(`   1c: 24 shuffled days, ${steps} steps judged against the rules`);
  }

  /* 1d · everything is full at the same look (a stream's end, a reload): the same sets */
  {
    const S = scene(), { sh } = S, w = world(); for (const t of S.tags) if (!['rB16', 'rA29', 'd10'].includes(t) && !/^d\d+/.test(t)) Object.assign(sh[t], { releaseFull: true });
    w.add(...Object.values(sh)); await look(w, 'all at once'); assert.deepEqual(w.sets().map(s => s.length).sort(), [2, 3, 4, 4], 'one look: four sets of 2, 3, 4, 4 sheets, never one set of 14'); assert(!w.inSet(sh.gL8) && !w.inSet(sh.rB16) && !w.inSet(sh.rA29));
  }

  /* ═══ 2 · GF Draft 10 (partial) shares an order with a group: that group waits WHOLE until it is full; the others form ═══════════════════════════════ */
  for (const target of ['C', 'B', 'A']) {
    const S = scene(), { sh, groups } = S, w = world(), tag = `Draft 10 with ${target}`;
    S.link(groups[target][0], 'd10');   // GF Draft 10 shares an order with one sheet of the group
    for (const t of S.tags) if (!['rB16', 'rA29'].includes(t) && !/^d\d+/.test(t)) Object.assign(sh[t], { releaseFull: true, density: .75 });
    w.add(...Object.values(sh)); await look(w, tag);
    const others = ['C', 'B', 'A'].filter(g => g !== target), sets = w.sets();
    assert.equal(sets.length, 3, `${tag}: Set 1 and the two other groups form`); for (const g of others) assert(sets.some(s => JSON.stringify(s) === JSON.stringify(groups[g].slice().sort())), `${tag}: group ${g} formed its own set`);
    for (const t of groups[target]) { assert(!w.inSet(sh[t]), `${tag}: ${t} waits outside every set, whole`); assert(/not full yet/.test(sh[t].cardinalHold || ''), `${tag}: ${t} says why: ${sh[t].cardinalHold}`); }
    // Draft 10 fills up: it is a completed GF sheet of the group now, the group forms as ONE set with it
    Object.assign(sh.d10, { releaseFull: true, density: .75 }); await look(w, tag + ' · Draft 10 full');
    const joined = w.sets().find(s => s.includes('d10')); assert(joined, `${tag}: Draft 10 is in a set`); assert.deepEqual(joined, [...groups[target], 'd10'].sort(), `${tag}: the group and Draft 10 are ONE set`); assert.equal(w.sets().length, 4);
  }

  /* 2b · the late link (the leak that glued the groups: investigation h2-absorb). Set 2 formed from group B. THEN pieces of orders of C, A and GF 8 land on GF Draft 10 (a partial sheet outside every set):
         the Draft is not full, so it is an END of those groups. It must not chain C, A and GF 8 to B and drag them into B's closed set; they wait, whole, until it is full (the rule as written: then ONE group) */
  {
    const S = scene(), { sh, groups } = S, w = world();
    for (const t of [...groups.P1, ...groups.B]) Object.assign(sh[t], { releaseFull: true });
    for (const t of [...groups.C, ...groups.A, ...groups.L]) Object.assign(sh[t], { releaseFull: false, density: .5 });
    w.add(...Object.values(sh).filter(p => !['rB16', 'rA29'].includes(p.sheetId))); await look(w, 'late link · B formed'); assert.deepEqual(w.sets(), [['g0', 's0'], ['gB3', 'gB4', 'gB5', 'sB2']]);
    for (const t of ['gC1', 'gA6', 'gL8', 'gB3']) S.link(t, 'd10');         // the pieces move onto Draft 10 (one of them from group B, the group that already has its set)
    for (const t of [...groups.C, ...groups.A, ...groups.L]) Object.assign(sh[t], { releaseFull: true, density: .76 });
    await w.assemble(); const res = w.check();
    assert.deepEqual(w.sets(), [['g0', 's0'], ['gB3', 'gB4', 'gB5', 'sB2']], 'C, A and GF 8 are complete but Draft 10 (not full) is on an order of each: they wait, whole; Set 2 does not take them (it used to take all of them)');
    for (const t of [...groups.C, ...groups.A, ...groups.L]) assert(!w.inSet(sh[t]) && /not full yet/.test(sh[t].cardinalHold || ''), t + ' waits: ' + sh[t].cardinalHold);
    assert.equal(res.ok, true, 'a partial sheet that shares an order with a formed set is named, not a violation:\n' + res.lines.join('\n')); assert(res.partners.some(x => x.name === 'GF Draft 10' && x.kind === 'partial'), 'Draft 10 is named as the partner that joins once it is full: ' + JSON.stringify(res.partners.map(x => [x.name, x.kind])));
    Object.assign(sh.d10, { releaseFull: true, density: .76 }); await look(w, 'late link · Draft 10 full', { l8ok: true });
    assert.deepEqual(w.sets()[1], [...groups.B, ...groups.C, ...groups.A, ...groups.L, 'd10'].sort(), 'Draft 10 is full: it is a sheet like any other, the groups it ties are ONE group in Set 2 (the written rule; the owner prevents it at the nesting)');
  }

  /* ═══ 3 · a person's own yes on a Rose Gold sheet is that sheet's, never the metal's ═════════════════════════════════════════════════════════════ */
  {
    const S = scene(), { sh, groups } = S, w = world();
    for (const t of S.tags) if (!/^d\d+/.test(t) && !['rB16', 'rA29'].includes(t)) Object.assign(sh[t], { releaseFull: true, density: .75 });
    w.add(...Object.values(sh)); await look(w, 'a person includes RG 16 · before');
    const Gate = vm.runInContext('Gate', w.c);
    // RG 16 uncut: the Include switch (its own tick) puts THIS sheet in the set of its group; RG 29 is not brought along, and the run-wide Rose Gold tick brings nobody
    await Gate.changeMembership('rose', true, sh.rB16);
    assert.equal(w.setOf('rB16'), 3, 'RG 16 is in Set 3 by its own tick'); assert.equal(w.inSet(sh.rA29), false, 'RG 29 is not brought in with it'); assert.equal(sh.rB16.solidPick, true);
    const res = w.check(); assert.equal(res.ok, true, 'a person\'s own yes is not a violation:\n' + res.lines.join('\n')); assert.equal(w.state().solidIncluded.rose, false, 'there is no run-wide Rose Gold tick');
    w.state().solidIncluded.rose = true; await w.assemble(); assert.equal(w.inSet(sh.rA29), false, 'the old run-wide Rose Gold tick (run.solidIncluded.rose) is not read any more: it brought every uncut sheet in'); w.state().solidIncluded.rose = false;
    // and out again by its own switch; RG 29 and RG 16 were never touched otherwise
    await Gate.changeMembership('rose', false, sh.rB16); assert.equal(w.inSet(sh.rB16), false); assert.equal(w.sets().length, 4);
    // the closed Set 3 does not take an unrelated Rose Gold sheet that is cut but shares no order with it: it only comes in by a person's own tick
    const lone = page('rLone', 'rose', 40, ['97900001_1_1'], { releaseFull: false, density: .2, roseCutAt: Date.now() }); w.add(lone); await look(w, 'a cut Rose Gold sheet that shares nothing');
    assert.equal(w.inSet(lone), false, 'a cut Rose Gold sheet that shares no order with any set is not taken in by itself');
    await Gate.changeMembership('rose', true, lone); assert.equal(w.inSet(lone), true, 'its own tick puts it in the newest set'); assert.equal(w.setOf('rLone'), 4);
    assert.equal(w.check().ok, true);
  }

  /* ═══ 4 · the leaks ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  {
    // LEAK 1: the old even-numbered rule. Set 1 (odd) formed; Set 2 (even) forms; a lone uncut partial Rose Gold sheet with no order in common with anything must not join Set 2 (it used to, at any parity,
    // after any Rose Gold press)
    const S = scene(), { sh, groups } = S, w = world();
    for (const t of [...groups.P1, ...groups.C]) Object.assign(sh[t], { releaseFull: true });
    const lone = page('rLone', 'rose', 40, ['97900001_1_1'], { releaseFull: false, density: .22 }), lone2 = page('rLone2', 'rose', 41, ['97900002_1_1'], { releaseFull: false, density: .1 });
    w.add(...groups.P1.map(t => sh[t]), ...groups.C.map(t => sh[t]), lone); await look(w, 'a lone uncut Rose Gold sheet next to Set 2'); assert.equal(w.sets().length, 2); assert.equal(w.inSet(lone), false, 'Set 2 (even) does not take it');
    w.add(lone2); w.state().solidIncluded.rose = true; await look(w, 'the run-wide tick and a second sheet'); assert.equal(w.inSet(lone) || w.inSet(lone2), false, 'nor does the old run-wide tick');
    // LEAK 2: an uncut Rose Gold sheet is no home. Set 2 = C is closed; B and A are complete; the Rose Gold sheets that tie B and A to nothing in Set 2 must not pull them into it
    const S2 = scene(), w2 = world(); for (const t of S2.tags) if (!/^d\d+/.test(t) && !['rB16', 'rA29'].includes(t)) Object.assign(S2.sh[t], { releaseFull: true });
    w2.add(...Object.values(S2.sh)); await look(w2, 'B and A next to a closed Set 2'); assert.equal(w2.sets().length, 4);
    for (const g of ['B', 'A']) for (const t of S2.groups[g]) assert.notEqual(w2.setOf(t), 2, t + ' is not in Set 2');
    // the fallback (a page without the set rules) made ONE set of everything that wanted in: it fails loudly now and forms nothing
    const S3 = scene(), w3 = world({ rules: false }); for (const t of S3.tags) if (!/^d\d+/.test(t)) Object.assign(S3.sh[t], { releaseFull: true });
    w3.add(...Object.values(S3.sh)); await assert.rejects(w3.assemble(), /set rules did not load/, 'without charm-nest-set-rules.js no set is formed, and it says why');
    assert.deepEqual(w3.sets(), [], 'nothing joined any set'); assert.equal(vm.runInContext('B.sets.size', w3.c), 0, 'no set was even numbered'); assert(w3.log().some(e => e.kind === 'warn' && /set rules did not load/.test(e.text)), 'the agent log says so');
  }

  /* ═══ 5 · a partial sheet that shares orders with several groups: SetRules.PARTIAL_BRIDGE ═════════════════════════════════════════════════════════ */
  assert.equal(SR.PARTIAL_BRIDGE, 'hold', 'the default is the rule as written');
  const bridged = () => { const S = scene(), { sh, groups } = S, w = world(); S.link('gC1', 'd10'); S.link('gB3', 'd10'); S.link('gA6', 'd10');   // GF Draft 10 has an order on each of the three groups
    for (const t of S.tags) if (!/^d\d+/.test(t) && !['rB16', 'rA29', 'gL8'].includes(t)) Object.assign(sh[t], { releaseFull: true, density: .75 }); w.add(...Object.values(sh)); return { S, sh, groups, w }; };
  try {
    // 'hold': all three groups wait whole until Draft 10 is full, then it is one group and one set
    { const { sh, groups, w } = bridged(); await look(w, 'bridge · hold · Draft 10 filling'); assert.deepEqual(w.sets(), [['g0', 's0']], 'only Set 1 forms: every group Draft 10 touches waits whole');
      for (const g of ['C', 'B', 'A']) for (const t of groups[g]) assert(!w.inSet(sh[t]) && /not full yet/.test(sh[t].cardinalHold), t + ' waits: ' + sh[t].cardinalHold);
      Object.assign(sh.d10, { releaseFull: true, density: .75 }); await look(w, 'bridge · hold · Draft 10 full'); assert.equal(w.sets().length, 2, 'then they are one group and one set');
      assert.deepEqual(w.sets()[1], [...groups.C, ...groups.B, ...groups.A, 'd10'].sort(), 'Set 2 = C + B + A + Draft 10'); }
    // 'spread': each complete group forms its own minimal set now; Draft 10 and its mixed orders are flagged
    SR.PARTIAL_BRIDGE = 'spread';
    { const { sh, groups, w } = bridged(); await look(w, 'bridge · spread · Draft 10 filling'); assert.equal(w.sets().length, 4, 'Set 1 and one set for each complete group');
      for (const g of ['C', 'B', 'A']) assert(w.sets().some(s => JSON.stringify(s) === JSON.stringify(groups[g].slice().sort())), 'group ' + g + ' has its own set');
      assert(!w.inSet(sh.d10), 'Draft 10 is in none'); assert(/take those orders off a sheet so that no order is spread over two sets/.test(sh.d10.cardinalNote), sh.d10.cardinalNote);
      const res = w.check(); assert.equal(res.ok, true, res.lines.join('\n')); assert(res.partners.some(p => p.kind === 'bridge' && p.name === 'GF Draft 10'), 'the checker lists Draft 10 as a partner that bridges, not as a violation');
      // Draft 10 is full: now it is a sheet of three sets: the cardinal rule is broken, and it is said (a person takes the orders off)
      Object.assign(sh.d10, { releaseFull: true, density: .75 }); await w.assemble(); const res2 = w.check(); assert(res2.violations.some(v => v.rule === 'cardinal'), 'Draft 10 full: its orders are spread over several sets, flagged: ' + res2.lines.join(' | ')); assert(!w.inSet(sh.d10), 'it joins none of them'); }
  } finally { SR.PARTIAL_BRIDGE = 'hold'; }

  /* ═══ 6 · the live self-check ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  {
    // the owner's 14-sheet Set 2, as the old assembly built it: GF 1-8, SS 1-4, RG 16 and RG 29 in one closed set, joined after it had formed
    const S = scene(), { sh, groups } = S, w = world();
    for (const t of S.tags) if (!/^d\d+/.test(t) && !['rB16', 'rA29'].includes(t)) Object.assign(sh[t], { releaseFull: true, density: .75 });
    w.add(...Object.values(sh)); await w.assemble(); assert.deepEqual(w.violations(), [], 'the new assembly built nothing wrong');
    const T0 = 1760000000000; vm.runInContext(`B.sets.clear(); for (const p of pages) { p.setId = null; p.draft = true; delete p.joinedAt; delete p.seq; delete p.sheetIndex; } seq = 1;`, w.c);
    const bad = vm.runInContext(`(() => { const s = { runId: RUN, group: 'dispatch', seq: 2, day: today(), setId: 'set-bad', name: 'Set-2', sheetIds: [], labelFiles: [], materials: [], orders: {}, sheetNos: {}, formedAt: ${T0} }; B.sets.set(s.setId, s); return s; })()`, w.c);
    const members = [...groups.C, ...groups.B, ...groups.A, ...groups.L, 'rB16', 'rA29'], at = { rB16: T0 + 5, rA29: T0 + 5 };
    members.forEach((id, k) => { const p = w.by(id); Object.assign(p, { draft: false, setId: 'set-bad', seq: 2, sheetIndex: 1 + k, joinedAt: at[id] || (groups.C.includes(id) ? T0 : T0 + 3) }); bad.sheetIds.push(id); });
    const set1 = vm.runInContext(`(() => { const s = { runId: RUN, group: 'dispatch', seq: 1, day: today(), setId: 'set-one', name: 'Set-1', sheetIds: ['g0','s0'], labelFiles: [], materials: [], orders: {}, sheetNos: {}, formedAt: ${T0 - 10} }; B.sets.set(s.setId, s); return s; })()`, w.c);
    for (const id of ['g0', 's0']) Object.assign(w.by(id), { draft: false, setId: 'set-one', seq: 1, sheetIndex: 1, joinedAt: T0 - 10 });
    const vs = w.violations(), rules = vs.map(v => v.rule);
    assert.equal(rules.filter(r => r === 'surplus').length, 3, 'three groups the set does not need: ' + rules.join()); assert.equal(rules.filter(r => r === 'notReady').length, 2, 'the two uncut Rose Gold sheets');
    assert(rules.includes('closedJoin'), 'sheets that joined after it had formed (the stamps joinedAt / formedAt make the rule work in the live app)'); assert(!rules.includes('invalid') && !rules.includes('partial'));
    assert.deepEqual(vs.filter(v => v.rule === 'notReady').map(v => v.sheets[0]).sort(), ['rA29', 'rB16']);
    // one agent-log warning for this list (and again only when the list changes)
    vm.runInContext('Gate.selfCheck(B.run)', w.c); const before = w.log().length; await w.assemble(); const warns = w.log().filter(e => e.kind === 'warn' && /Set check:/.test(e.text));
    // (Paul, 26 Oct 2026, Decision 1: "Remove automatically": the next look takes the spare groups out of the uncommitted Set 2 by itself; what is left to say is said once)
    const after = w.violations();
    assert(!after.some(v => v.rule === 'surplus'), 'the spare groups were taken out automatically: ' + after.map(v => v.rule).join());
    const stayed = ['C', 'B', 'A', 'L'].filter(g => groups[g].some(id => w.by(id).setId === 'set-bad' && !w.by(id).draft));
    assert.equal(stayed.length, 1, 'Set 2 keeps exactly the one group it needs, the others left it: ' + stayed.join());
    assert.equal(warns.length >= 1, true, 'the agent log carries a warning: ' + JSON.stringify(w.log()));
    const n1 = w.log().filter(e => /Set check:/.test(e.text)).length; await w.assemble(); assert.equal(w.log().filter(e => /Set check:/.test(e.text)).length, n1, 'the same list is not written twice');
    assert.equal(set1.formedAt, T0 - 10);
  }
  console.log('PASS: sets-owner-day: the owner\'s day replayed through the real set assembly: Set 1 = GF1+SS1, Set 2 = unit C, Set 3 = unit B (RG 16 the partner to add), Set 4 = unit A (RG 29 the partner), GF 8 outside every set, the group that shares an order with GF Draft 10 waits whole until it is full, a cut Rose Gold sheet joins its own group\'s set only, in every order the sheets can arrive (judged after every step by the rules\' own checker), a bridging partial sheet by hold or spread, the old 14-sheet Set 2 reported rule by rule');
})().catch(e => { console.error(e); process.exit(1); });
