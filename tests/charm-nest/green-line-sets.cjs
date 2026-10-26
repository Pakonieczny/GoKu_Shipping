// Decision 2 (Paul, 26 Oct 2026): a Rose Gold / 10K / 14K sheet shares multi-piece orders with ONE Gold Filled sheet and/or ONE Sterling Silver sheet, of ONE set (the intake keeps it so:
// greenFits); once that set is complete its green line is cut automatically and it joins the set (Gate autoCut). Decision 1: a set made bigger than it needs is trimmed by itself.
//   node tests/charm-nest/green-line-sets.cjs
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const { world } = require('./audit-cardinal-lib.cjs');
const root = path.join(__dirname, '../..'), code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const ok = (name, cond, detail) => { assert(cond, name + (detail ? ' :: ' + detail : '')); console.log('  ok  ' + name); };

(async () => {
  /* ── 1 · the intake rule (greenFits, the page's own code) ── */
  {
    const a = code.indexOf('  const lineCuts = m =>'), b = code.indexOf('    return true;\n  }', code.indexOf('  function greenFits(', a)) + '    return true;\n  }'.length;
    assert(a > 0 && b > a, 'greenFits found');
    const c = vm.createContext({ pages: [] }); vm.runInContext('const allSheets = () => pages;\n' + code.slice(a, b) + '\nthis.greenFits = greenFits;', c);
    const pg = (metal, setId, orders) => ({ metal, runId: 'r', setId: setId || null, draft: !setId, charms: orders.map(o => ({ order: o })) });
    const gf1 = pg('gold', 'set-1', ['1', '2']), ss1 = pg('silver', 'set-1', ['3']), gf2 = pg('gold', null, ['4']), ss2 = pg('silver', 'set-2', ['5']), rg = pg('rose', null, ['1']);
    c.pages.push(gf1, ss1, gf2, ss2, rg);
    const fits = o => c.greenFits(rg, o, { runId: 'r' });
    ok('an order on the same GF sheet joins the Rose Gold sheet', fits('2'));
    ok('an order on a second GF sheet does not', !fits('4'));
    ok('an order on the SS sheet of the same set does', fits('3'));
    ok('an order on an SS sheet of another set does not', !fits('5'));
    ok('a Rose Gold-only order goes anywhere', fits('99'));
  }

  /* ── 2 · the automatic Cut Sheet once the partner set is complete ── */
  {
    const w = world(), calls = [];
    // a stand-in for RoseStock.record: the cut is recorded and the sheet's set follows from its partners (the real press joins it, then records the cut)
    w.c.__rec = async sh => { calls.push(sh.sheetId); sh.roseCutAt = Date.now(); await vm.runInContext('Gate.assemble(B.run)', w.c); };
    vm.runInContext('window.RoseStock = { record: (sh) => __rec(sh) }; var stampWho = () => ({ by: "Test" });', w.c);
    const base = { verification: { ok: true }, persistedDone: true, status: 'complete', dirty: false };
    const gf = w.full(w.page('gold', base)), ss = w.full(w.page('silver', base)), rg = w.page('rose', Object.assign({ releaseFull: false, density: .3 }, base));
    w.put(gf, '7001_1_1', '7002_1_1'); w.put(ss, '7001_2_1'); w.put(rg, '7002_2_1');
    const gf2 = w.page('gold', Object.assign({ releaseFull: false, density: .2 }, base)), rg2 = w.page('rose', Object.assign({ releaseFull: false, density: .2 }, base));
    w.put(gf2, '7003_1_1'); w.put(rg2, '7003_2_1');
    await w.assemble(); for (let i = 0; i < 20 && calls.length < 1; i++) await new Promise(r => setTimeout(r, 10)); await new Promise(r => setTimeout(r, 30));
    ok('the Rose Gold sheet whose partners make a complete set is cut automatically, once', calls.filter(x => x === rg.sheetId).length === 1, JSON.stringify(calls));
    ok('and it is in that set with them', w.inSet(rg) && rg.setId === gf.setId && gf.setId === ss.setId, w.word());
    ok('a Rose Gold sheet whose partner is not in a complete set yet is not cut', !calls.includes(rg2.sheetId) && !rg2.roseCutAt, w.word());
  }

  /* ── 3 · Decision 1: a set made bigger than it needs is trimmed by itself ── */
  {
    const w = world(), base = { verification: { ok: true }, persistedDone: true, status: 'complete' };
    const gf1 = w.full(w.page('gold', base)), ss1 = w.full(w.page('silver', base)); w.put(gf1, '8001_1_1'); w.put(ss1, '8002_1_1');
    await w.assemble(); assert(w.inSet(gf1) && w.inSet(ss1), 'Set 1 formed');
    // a late line of SS 1's order lands on GF 2, which also shares an order with SS 2: the cardinal rule pulls GF 2 + SS 2 into Set 1, and GF 1 is spare
    const gf2 = w.full(w.page('gold', base)), ss2 = w.full(w.page('silver', base)); w.put(gf2, '8002_2_1', '8003_1_1'); w.put(ss2, '8003_2_1');
    await w.assemble();
    const set1 = w.sets().find(s => s.setId === ss1.setId);
    ok('the spare unit left Set 1 by itself', !(w.inSet(gf1) && gf1.setId === set1.setId), w.word());
    ok('Set 1 keeps the sheets tied by orders', [ss1, gf2, ss2].every(p => p.setId === set1.setId && !p.draft), w.word());
    ok('nothing left to flag', !w.selfCheck().violations.some(v => v.rule === 'surplus'));
  }
  console.log('PASS: green-line sets: one GF / SS partner of one set, the automatic Cut Sheet once that set is complete, a set made too big trimmed by itself');
})().catch(e => { console.error(e); process.exit(1); });
