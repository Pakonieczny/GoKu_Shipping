// RGONE (Paul, 9 Nov 2026): "There should never be just one piece on a rose gold sheet to be cut that's ridiculous and the system should never automatically do this and there should never be
// two partial rose gold sheets in one set." The live Sandbox Set-1 of 8 Nov 2026: GF Sheet + SS Sheet + RG Sheet (11 pieces, cut) + RG Sheet (1 piece, cut).
//   1  the intake (greenFits): the RG sheet that shares an order with GF 1 takes an order of SS 1 while both are unpaired drafts, the first of their metal outside every set (they form the same set),
//      so no new Rose Gold sheet is opened for one piece; a GF draft already paired with another SS sheet is still refused (the rule kept)
//   2  the automatic cut (Gate.autoCut): two uncut RG sheets whose one set is the same set are never cut as two: the pieces of the smaller move onto the fuller one first (SheetWin.holdKit.move),
//      then ONE sheet is cut; a set that already holds a cut RG sheet gets no second one by itself
//   node tests/charm-nest/rg-one-per-set.cjs      Offline: the page's own code in a vm, no browser, no network.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm'), path = require('node:path');
const { world } = require('./audit-cardinal-lib.cjs');
const root = path.join(__dirname, '../..'), code = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const ok = (name, cond, detail) => { assert(cond, name + (detail ? ' :: ' + detail : '')); console.log('  ok  ' + name); };
const settle = async (n = 60) => { for (let i = 0; i < n; i++) await new Promise(r => setTimeout(r, 5)); };

(async () => {
  /* ── 1 · the intake ── */
  {
    const a = code.indexOf('  const lineCuts = m =>'), b = code.indexOf('    return true;\n  }', code.indexOf('  function greenFits(', a)) + '    return true;\n  }'.length;
    assert(a > 0 && b > a, 'greenFits found');
    const c = vm.createContext({ pages: [] }); vm.runInContext('const allSheets = () => pages;\n' + code.slice(a, b) + '\nthis.greenFits = greenFits;', c);
    const pg = (metal, page, orders) => ({ metal, page, runId: 'r', setId: null, draft: true, charms: orders.map(o => ({ order: o })) });
    const gf1 = pg('gold', 1, ['100', '4216261140']), ss1 = pg('silver', 1, ['200', '4217138816']), rg1 = pg('rose', 1, ['4216261140']);
    c.pages.push(gf1, ss1, rg1);
    ok('the RG sheet of GF 1 takes the order of SS 1: both unpaired, the first GF and SS drafts outside every set (one set), so no new RG sheet for one piece', c.greenFits(rg1, '4217138816', { runId: 'r' }));
    const gf2 = pg('gold', 2, ['300', '400']), ss2 = pg('silver', 2, ['500']), ss3 = pg('silver', 3, ['400']), rg2 = pg('rose', 2, ['300']);
    c.pages.push(gf2, ss2, ss3, rg2);
    ok('a GF draft paired with another SS sheet is still refused (it could end in another set)', !c.greenFits(rg2, '500', { runId: 'r' }));
    const c2 = vm.createContext({ pages: [] }); vm.runInContext('const allSheets = () => pages;\n' + code.slice(a, b) + '\nthis.greenFits = greenFits;', c2);
    const g = pg('gold', 1, ['10']), s1 = pg('silver', 1, ['20']), s2 = pg('silver', 2, ['30']), r = pg('rose', 1, ['10']);
    c2.pages.push(g, s1, s2, r);
    ok('GF 1 with SS 2 (SS 1 is ahead of it): refused', !c2.greenFits(r, '30', { runId: 'r' }));
    ok('GF 1 with SS 1: taken', c2.greenFits(r, '20', { runId: 'r' }));
  }

  /* ── 2 · the automatic cut: one RG sheet per set ── */
  const base = { verification: { ok: true }, persistedDone: true, status: 'complete', dirty: false };
  {
    const w = world(), calls = [], moves = [];
    w.c.__rec = async sh => { calls.push(sh.sheetId); sh.roseCutAt = Date.now(); await vm.runInContext('Gate.assemble(B.run)', w.c); };
    w.c.__move = async (k, target) => { moves.push({ rid: k.rid, from: k.pieces[0].src.sheetId, to: target.sheetId });
      for (const z of k.pieces) { z.src.charms = z.src.charms.filter(x => x !== z.c); z.src.placements = z.src.placements.filter(p => p.id !== z.c.id); target.charms.push(z.c); target.placements.push({ id: z.c.id }); }
      await vm.runInContext('Gate.assemble(B.run)', w.c); return true; };
    vm.runInContext('window.RoseStock = { record: (sh) => __rec(sh) }; var stampWho = () => ({ by: "Test" }); window.SheetWin = { holdKit: { busy: () => false, move: (k, t, who, o) => __move(k, t, who, o) } };', w.c);
    const gf = w.full(w.page('gold', base)), ss = w.full(w.page('silver', base));
    const rgBig = w.page('rose', Object.assign({ density: .08 }, base)), rgOne = w.page('rose', Object.assign({ density: .02 }, base));
    w.put(gf, '9001_1_1', '9002_1_1'); w.put(ss, '9003_1_1', '9004_1_1');
    w.put(rgBig, '9001_2_1', '9101_1_1', '9102_1_1'); w.put(rgOne, '9004_2_1');
    await w.assemble(); for (let i = 0; i < 40 && calls.length < 1; i++) await settle(5); await settle();
    ok('the one-piece RG sheet is never cut', !calls.includes(rgOne.sheetId) && !rgOne.roseCutAt, JSON.stringify({ calls, moves }));
    ok('its piece moved onto the fuller RG sheet first', moves.some(m => m.rid === '9004' && m.from === rgOne.sheetId && m.to === rgBig.sheetId) && rgOne.charms.length === 0, JSON.stringify(moves));
    vm.runInContext('Gate.autoCut(B.run)', w.c); for (let i = 0; i < 40 && !calls.length; i++) await settle(5); await settle();
    ok('then ONE RG sheet is cut into the set', calls.filter(x => x === rgBig.sheetId).length === 1 && calls.length === 1, JSON.stringify(calls));
    const rgIn = w.pages.filter(p => p.metal === 'rose' && w.inSet(p) && p.setId === gf.setId);
    ok('the set holds exactly one RG sheet', rgIn.length === 1 && rgIn[0] === rgBig, w.word());
  }
  {
    const w = world(), calls = [];
    w.c.__rec = async sh => { calls.push(sh.sheetId); sh.roseCutAt = Date.now(); await vm.runInContext('Gate.assemble(B.run)', w.c); };
    vm.runInContext('window.RoseStock = { record: (sh) => __rec(sh) }; var stampWho = () => ({ by: "Test" });', w.c);
    const gf = w.full(w.page('gold', base)), ss = w.full(w.page('silver', base)), rg1 = w.page('rose', Object.assign({ density: .1 }, base));
    w.put(gf, '7001_1_1'); w.put(ss, '7002_1_1'); w.put(rg1, '7001_2_1', '7101_1_1');
    await w.assemble(); for (let i = 0; i < 40 && !calls.length; i++) await settle(5); await settle();
    ok('the first RG sheet is cut into the set', !!rg1.roseCutAt && w.inSet(rg1), w.word());
    const rg2 = w.page('rose', Object.assign({ density: .02 }, base)); w.put(rg2, '7002_2_1');
    await w.assemble(); vm.runInContext('Gate.autoCut(B.run)', w.c); await settle(80);
    ok('a second RG sheet of the same set is not cut by itself', !rg2.roseCutAt && !calls.includes(rg2.sheetId), JSON.stringify(calls));
    ok('and its card says why', /never takes two partial Rose Gold sheets/.test(rg2.autoCutWait || ''), rg2.autoCutWait);
  }
  console.log('PASS: rg-one-per-set: the intake keeps one RG sheet for the first GF + SS drafts, an RG sheet is never opened or cut for one piece of a set that has one, and a set gets one RG sheet');
})().catch(e => { console.error(e); process.exit(1); });
