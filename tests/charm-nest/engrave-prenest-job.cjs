// ENGPRE (engrave-prenest-1013, Paul 13 Oct 2026): "it won't let me go into the engraving detail screen because it says it's not yet placed/nested on a sheet. I don't ever
// remember such a stipulation, a charm does not have to be placed on that sheet for it to be approved/updated by the user for the correct back engraving."
//
// A line that carries an engraving gets its Engrave job BEFORE and independent of nesting and Hold (Engrave.early, charm-nest-bridge.js): on the charm the piece WILL have (Pool.preview), under the
// same job key and the pool ids the piece will get, flagged job.pre. This test, in the REAL-DRAWING world (_engraveRealWorld.cjs: real drawings, real intake, pool, classifier and fit, offline over the
// fake site, made-up names, nothing written anywhere real, no Etsy, no paid call), checks for a back-only necklace on HOLD that was never pooled (the shape of the live order, with an unanswered option
// question on it), a held three-disc necklace, a held line whose words need a person, an earring pair and a line with nothing to engrave:
//   1  the job exists with a real view, mask and fit, under the line's own key, on the pool ids the piece will get; the line is still held, pulled, in no pool, on no sheet, in no run;
//   2  making it costs the fake site nothing that writes or pays (no pool, back, run, sheet, agent or timeline call), and it is not made twice;
//   3  the order window's Fix in Engraving button is enabled and the press OPENS the Engrave detail on that job: no refusal, nothing closed first, and the work area draws (no "Processing stopped");
//   4  Approve records without a sheet (a seal is kept, no back file and no pool write: the file waits for the sheet);
//   5  later released, pooled and nested: it is the SAME job object (key, words, fit, approval, seal), nothing re-read; its back file is written once, on the sheet;
//   6  discs: one job per disc under the disc keys, the words of each from the note; words that need a person stay a proposal for a person ("Words to confirm"), and a person's words fit;
//   7  an earring pair and a line with nothing to engrave get no job; a press that can open nothing leaves the Engrave tab exactly as it was (the empty-tab protection).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules CHROMIUM=/opt/pw-browsers/chromium node tests/charm-nest/engrave-prenest-job.cjs
// What a pass does NOT prove: the live site, a live order, a live reading of the words (the paid reader is never started here), the real Netlify functions.
const R = require('./_engraveRealWorld.cjs'), Base = require('./_orderFactsWorld.cjs');
let pass = 0, fail = 0;
const check = (ok, msg, extra) => { if (ok) { pass++; console.log('  ✓ ' + msg); } else { fail++; console.log('  ✗ ' + msg + (extra !== undefined ? ' :: ' + JSON.stringify(extra) : '')); } };
const V = (name, value) => ({ name, value });
// made-up held orders (Name One / Date One)
const HELD = {
  // (the live order was held by an unanswered option question: "Engraving Side: Back" is not mapped for this made-up listing, so the line carries a needsMapping problem and makePool would refuse it)
  back: { rid: '4290000201', sku: 'COMET_9427', listing: '1890000201', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Nine', placed: { day: 0, h: 9, m: 10 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Engraving Side', 'Back')], personalization: ['Typewriter font\n\nName One\nDate One'] },
  discs: { rid: '4290000202', sku: 'COMET_9427', listing: '1890000105', title: 'Initial Disc Necklace, Personalized Letter Discs', buyer: 'Name Ten', placed: { day: 0, h: 9, m: 50 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Number of Discs', '3 Discs')], personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'] },
  earrings: { rid: '4290000203', sku: 'COMET_9427', listing: '1890000106', title: 'Comet Stud Earrings, Celestial Earrings in 14k Gold Fill', buyer: 'Name Eleven', placed: { day: 0, h: 10, m: 0 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] },
  // (a buyer message beside the personalisation: the words are for a person to confirm, never settled by the free reading)
  noted: { rid: '4290000204', sku: 'COMET_9427', listing: '1890000204', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Twelve', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'], buyerMessage: 'Please put it on the back, thank you.' },
  carry: { rid: '4290000206', sku: 'COMET_9427', listing: '1890000206', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Fourteen', placed: { day: 0, h: 9, m: 18 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Font Choice', 'Stylish')], personalization: ['Name One'] },
  plain: { rid: '4290000205', sku: 'COMET_9427', listing: '1890000205', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Thirteen', placed: { day: 0, h: 9, m: 16 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] }
};
// every call that is not a plain read (the fake site's own names: *Get, *List, getOrderPieces, backPreview, listingPhotos ...)
const isRead = c => /^(get|list)|(Get|List|Preview|Photos|Pieces|Many)$/.test(String(c.op || ''));
// (the intake's own first-sight events, written a moment after the lines arrive, are not the engraving's: only an engraving event would be)
const intakeOnly = c => c.op === 'timelineAdd' && (c.body.events || []).every(e => /^(interpreted|needsDecision|held)$/.test(e.type));
const writes = from => from.filter(c => (!isRead(c) && !intakeOnly(c)) || /agent|chat|anthropic|claude/i.test(c.name));

(async () => {
  const w = await R.world({ viewport: { width: 1440, height: 900 }, shots: process.env.SHOTS || '/tmp' });
  if (!w) { console.log('skipped: no playwright-core'); return; }
  const page = w.page, calls = w.srv.st.calls;
  try {
    await R.build(w, ['back', 'discs']);
    check(w.made.back.jobs.length === 1 && w.made.discs.jobs.length === 3, 'the real world has its usual jobs (back 1, discs 3)');

    // ── the held orders: pulled, a person's hold on them, never pooled (the same shape as the live order) ──
    const keys = await page.evaluate(async ({ fxs, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), out = {};
      await Orders.loadMaps(true);
      for (const [n, fx] of Object.entries(fxs)) {
        const order = orderOf(fx, Date.now()), line = order.lines[0], key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); out[n] = key;
      }
      Orders.interpretAll();
      for (const k of Object.values(out)) { const r = B.orders.byKey.get(k); r.state = 'pulled'; r.hold = 'held by Test'; r.reason = null; }
      return out;
    }, { fxs: HELD, orderSrc: Base.orderOf.toString() });
    const state = k => page.evaluate(k => { const r = B.orders.byKey.get(k); return { state: r.state, hold: r.hold || null, poolIds: (r.poolIds || []).slice(), inPool: (r.poolIds || []).some(id => B.pool.rows.has(id)), jobs: Engrave.jobsOf(r).length, cand: !!(r.spec && r.spec.engraveCandidate), place: window.PiecePlacement && PiecePlacement.ofRow(r).state, run: !!B.run, problems: (r.problems || []).map(p => p.kind) }; }, k);
    const jobsOf = k => page.evaluate(k => Engrave.jobsOf(B.orders.byKey.get(k)).map(j => ({ key: j.key, pre: !!j.pre, state: j.state, text: j.text, slot: j.slot || null, face: j.face === 'front' ? 'front' : 'back', copies: j.copies, view: !!j.view, mask: !!j.mask, fitted: !!(j.fit && (j.fit.glyphs && j.fit.glyphs.length || j.fit.cmds)), font: j.fontKey || null, reason: j.reason || null, seals: window.CNEngravingSeals ? CNEngravingSeals.list(j).length : -1, backs: (j.backs || []).length })), k);
    const s0 = await state(keys.back);
    check(s0.jobs === 0 && s0.state === 'pulled' && s0.hold === 'held by Test' && s0.cand, 'the held order has no job yet, is pulled and held, and carries an engraving (' + JSON.stringify(s0) + ')');
    check(s0.problems.includes('needsMapping'), 'it carries the unanswered option question the live order had (so the line could not be pooled as it is)');

    // ── 1 + 2 · the job is made ──
    await page.evaluate(() => { Orders.render(); }); await R.settle(page, 800);   // (the intake's own first-sight events are written before the measure starts)
    let n0 = calls.length;
    await page.evaluate(k => Engrave.early(B.orders.byKey.get(k)), keys.back);
    const made = await jobsOf(keys.back), mine = calls.slice(n0);
    console.log('   job:', JSON.stringify(made));
    check(made.length === 1 && made[0].pre === true && made[0].key === keys.back, 'one back job, flagged pre, under the line\'s own key');
    check(made[0].view && made[0].mask && made[0].fitted, 'it has a real view, mask and fit (the Back box draws the back, the words inside the area)', made[0]);
    check(made[0].copies.length === 1 && made[0].copies[0] === '4290000201_42900002011_1', 'it holds the pool id the piece will get (' + made[0].copies + ')');
    check(made[0].text === 'Name One\nDate One' && made[0].state === 'review', 'the words are the buyer\'s own, the font word taken out, read free and ready to approve (' + JSON.stringify(made[0].text) + ', ' + made[0].state + ')');
    const s1 = await state(keys.back);
    check(s1.state === 'pulled' && s1.hold === 'held by Test' && s1.poolIds.length === 0 && !s1.inPool && !s1.run && s1.place === 'hold', 'the line is NOT pooled, nested, released or un-held (' + JSON.stringify(s1) + ')');
    check(writes(mine).length === 0, 'making it cost the site nothing that writes or pays (' + [...new Set(mine.map(c => c.op))].join(', ') + ')', writes(mine).map(c => c.name + ':' + c.op + ' ' + JSON.stringify(c.body || {}).slice(0, 300)));
    n0 = calls.length;
    await page.evaluate(k => Engrave.early(B.orders.byKey.get(k)), keys.back);
    check(calls.length - n0 <= 6 && (await jobsOf(keys.back)).length === 1, 'asked again it is not made twice and costs no new work');

    // ── 3 · the order window and the press ──
    await page.evaluate(() => { CN.setMode('orders'); });
    await page.evaluate(k => { window.__before = Engrave.view(); OrderWin.open(k); }, keys.back);
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.querySelector('.engCard .egOpen'); }, null, { timeout: 60000 });
    await R.settle(page, 600);
    const btn = await page.evaluate(() => { const b = document.querySelector('#owEng .engCard .egOpen'); const th = document.querySelector('#owEng .engCard .engFace[data-face=back] .engThumb'); return { text: b.textContent.replace(/\s+/g, ' ').trim(), disabled: b.getAttribute('aria-disabled'), title: b.title || '', thumbDisabled: th && th.getAttribute('aria-disabled'), thumbTitle: th && th.title }; });
    check(/Fix in Engraving|View in Engraving/.test(btn.text) && !btn.disabled && !btn.title && !btn.thumbDisabled, 'the order window\'s Engraving button and Back box are enabled (no "on hold / not on a sheet" refusal) (' + JSON.stringify(btn) + ')');
    const t0 = Date.now();
    await page.click('#owEng .engCard .egOpen');
    await page.waitForFunction(k => { const c = document.querySelector('#egQueue .rvItem'); return c && c.dataset.key === k && !document.getElementById('engraveView').classList.contains('hidden'); }, keys.back, { timeout: 60000 });
    await R.settle(page, 800);
    const det = await page.evaluate(k => { const v = Engrave.view(), c = document.querySelector('#egQueue .rvItem'); return { tab: v.tab, focus: v.focus, mode: CN.S.mode, ow: OrderWin.isOpen(), q: v.q || '', stopped: /Processing stopped/.test(c ? c.textContent : ''), canvas: !!(c && c.querySelector('canvas, svg')), toast: [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent).join('|') }; }, keys.back);
    console.log('   detail:', JSON.stringify(det), (Date.now() - t0) + ' ms');
    check(det.tab === 'place' && det.focus === keys.back && det.mode === 'engrave' && !det.ow, 'the press OPENS the Engrave detail on that very job, the order window closed (' + JSON.stringify({ tab: det.tab, mode: det.mode, ow: det.ow }) + ')');
    check(!det.stopped && det.canvas && !/hold|sheet/i.test(det.toast), 'the work area draws the placement, no "Processing stopped", no refusal line (' + det.toast + ')');
    await R.shot(w, 'ENGPRE-held-detail');
    const s2 = await state(keys.back);
    check(s2.state === 'pulled' && s2.hold === 'held by Test' && s2.poolIds.length === 0 && !s2.inPool, 'opening it did not pool, nest or release the line');

    // ── 4 · Approve without a sheet ──
    n0 = calls.length;
    await page.click('#egQueue [data-a=approve]');
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && j.state === 'approved' && !j.approvalPreparing; }, keys.back, { timeout: 90000 });
    await R.settle(page, 600);
    const ap = await jobsOf(keys.back), apCalls = calls.slice(n0).map(c => c.op).filter(o => !/Get$|List$|getOrderPieces/.test(o || ''));
    console.log('   approval calls:', JSON.stringify(apCalls));
    check(ap[0].state === 'approved' && ap[0].seals === 1, 'Approve stands on the held, un-nested piece with one seal (' + ap[0].state + ', seals ' + ap[0].seals + ')');
    check(ap[0].backs === 0 && !apCalls.some(o => /^(poolPut|poolUpdate|backPut|backInvalidate|putSheet|runPut|startAgent)$/.test(o)), 'and wrote no pool row, no back file, no sheet, no run, no paid reading: the file waits for the sheet', apCalls);
    const s3 = await state(keys.back);
    check(s3.state === 'pulled' && s3.hold === 'held by Test' && !s3.inPool, 'the approved line is still held and pulled');

    // ── 6 · discs, and words that need a person ──
    await page.evaluate(k => Engrave.early(B.orders.byKey.get(k)), keys.discs);
    const dj = await jobsOf(keys.discs);
    console.log('   discs:', JSON.stringify(dj.map(j => [j.key, j.state, j.text, j.copies])));
    check(dj.length === 3 && dj.every(j => j.pre && j.view && j.mask && j.fitted), 'a held three-disc order gets three jobs, each with a real view, mask and fit', dj.map(j => [j.slot, j.view, j.mask, j.fitted]));
    check(dj.map(j => j.slot).join() === 'D1,D2,D3' && dj.map(j => j.text).join() === 'A,B,C' && dj.every((j, i) => j.key === keys.discs + '#D' + (i + 1)), 'under the disc keys, each disc with its own letter, read from the note');
    check(dj.every((j, i) => j.copies.length === 1 && j.copies[0] === '4290000202_42900002021_' + (i + 1)), 'each holds the pool id its disc will get (' + dj.map(j => j.copies[0]).join(' ') + ')');
    console.log('   noted before:', JSON.stringify(await jobsOf(keys.noted)));
    await page.evaluate(k => Engrave.early(B.orders.byKey.get(k)), keys.noted);
    let nj = await jobsOf(keys.noted);
    check(nj.length === 1 && nj[0].pre && nj[0].state === 'words' && nj[0].text === 'Name One' && !nj[0].fitted, 'words with a buyer message beside them stay for a person: "Words to confirm", the proposal already in the box, nothing fitted or approved (' + JSON.stringify(nj.map(j => [j.state, j.text])) + ')');
    const decide = await page.evaluate(async k => { const j = Engrave.jobsOf(B.orders.byKey.get(k))[0]; await Engrave.decideWords(j, { text: 'Name Two', by: 'Test Operator' }); return { state: j.state, text: j.text, fitted: !!(j.fit && (j.fit.glyphs && j.fit.glyphs.length || j.fit.cmds)), pre: !!j.pre }; }, keys.noted);
    check(decide.state === 'review' && decide.text === 'Name Two' && decide.fitted && decide.pre, 'a person\'s words on it settle and fit on the charm it will have (' + JSON.stringify(decide) + ')');

    // ── 7 · nothing for earrings or a plain line; the empty-tab protection ──
    const none = await page.evaluate(async ks => { const out = {}; for (const [n, k] of Object.entries(ks)) { const r = B.orders.byKey.get(k); out[n] = { wanted: !!Engrave.earlyWanted(r), jobs: (await Engrave.early(r)).length }; } return out; }, { earrings: keys.earrings, plain: keys.plain });
    check(none.earrings.jobs === 0 && !none.earrings.wanted && none.plain.jobs === 0 && !none.plain.wanted, 'an earring pair and a line with nothing to engrave get no job (' + JSON.stringify(none) + ')');
    await page.evaluate(() => { try { OrderWin.close(); } catch (_) {} Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: '', openDone: null }); CN.setMode('orders'); });
    await page.evaluate(k => OrderWin.open(k), keys.earrings);
    await page.waitForFunction(() => OrderWin.isOpen());
    const t1 = Date.now();
    const opened = await page.evaluate(({ rid, key }) => EngraveLink.open({ rid, key }), { rid: HELD.earrings.rid, key: keys.earrings });
    const v7 = await page.evaluate(() => { const v = Engrave.view(); return { tab: v.tab, list: v.list, q: v.q || '', mode: CN.S.mode, ow: OrderWin.isOpen() }; });
    check(opened === false && Date.now() - t1 < 4000 && v7.ow && v7.mode === 'orders' && v7.q === '' && v7.list === true, 'a press that can open nothing (an earring) answers at once, closes nothing and leaves the Engrave tab as it was (' + JSON.stringify(v7) + ')');

    // ── 5 · later released, pooled and nested: the SAME job (last: pooling takes every pulled line of the world) ──
    await page.evaluate(k => Engrave.early(B.orders.byKey.get(k)), keys.carry);
    const cj = await jobsOf(keys.carry);
    check(cj.length === 1 && cj[0].pre && cj[0].state === 'review' && cj[0].text === 'Name One' && cj[0].fitted, 'a second held line: its job is made, free, fitted (' + JSON.stringify(cj.map(j => [j.state, j.text, j.fitted])) + ')');
    n0 = calls.length;
    await page.evaluate(async k => { const j = Engrave.items().get(k); window.__job = j; await Engrave.approve(j, 'Test Operator'); }, keys.carry);
    const apc = calls.slice(n0).map(c => c.op).filter(o => !/Get$|List$|getOrderPieces/.test(o || ''));
    const before = await page.evaluate(k => { const j = Engrave.items().get(k); return { state: j.state, text: j.text, fit: JSON.stringify(j.fit && { c: j.fit.centre, s: j.fit.size, a: j.fit.angle }), approvedAt: j.approvedAt, by: j.approvedBy, copies: j.copies.slice(), seals: CNEngravingSeals.list(j).length }; }, keys.carry);
    check(before.state === 'approved' && before.seals === 1 && !apc.some(o => /^(poolPut|poolUpdate|backPut|backInvalidate|putSheet|runPut|startAgent)$/.test(o)), 'approved before it is pooled: one seal, no pool, back, sheet, run or paid call (' + before.state + ', ' + apc.join(',') + ')');
    n0 = calls.length;
    await page.evaluate(async k => {
      const row = B.orders.byKey.get(k); row.hold = null;
      await Pool.addAll({ runId: 'run-pn', lines: {} });
      let n = 0; for (const sh of allSheets()) if (sh.charms && sh.charms.length) { n++; sh.sheetId = sh.sheetId || 'real-sheet-' + n; sh.fileBase = sh.fileBase || 'gold-real-' + n; sh.folderPath = sh.folderPath || 'charmnest/pn-sheet-' + n; sh.placements = sh.charms.map((c, i) => ({ id: c.id, cxPt: 40 + 70 * i, cyPt: 60, angle: 0 })); }
      try { await Engrave.classifyAll(); } catch (e) { console.warn('classifyAll', e && e.message); }
    }, keys.carry);
    const after = await page.evaluate(k => { const r = B.orders.byKey.get(k), j = Engrave.items().get(k); return { same: j === window.__job, n: Engrave.jobsOf(r).length, state: j.state, text: j.text, fit: JSON.stringify(j.fit && { c: j.fit.centre, s: j.fit.size, a: j.fit.angle }), approvedAt: j.approvedAt, by: j.approvedBy, copies: j.copies.slice(), pooled: r.state, poolIds: (r.poolIds || []).slice(), onSheet: !!Pool.sheetOf(j.copies[0]), seals: CNEngravingSeals.list(j).length }; }, keys.carry);
    console.log('   after pooling:', JSON.stringify(after));
    check(after.pooled === 'pooled' && after.onSheet, 'released, pooled and on a sheet (' + after.pooled + ')');
    check(after.same && after.n === 1 && after.state === 'approved', 'the SAME job object (no duplicate, nothing re-read), still approved (' + after.state + ')');
    check(after.text === before.text && after.fit === before.fit && after.approvedAt === before.approvedAt && after.by === before.by && after.seals === 1, 'its words, fit, approval time, approver and seal are exactly what they were');
    check(JSON.stringify(after.copies) === JSON.stringify(before.copies) && JSON.stringify(after.poolIds) === JSON.stringify(before.copies), 'the pool ids the piece got are the very ids the job held (' + after.poolIds + ')');
    check(calls.slice(n0).filter(c => c.op === 'startAgent' || /agent|chat|anthropic|claude/i.test(c.name)).length === 0, 'pooling and the pass over it started no paid reading');
    await page.evaluate(() => Engrave.resumeBacks());
    await page.waitForFunction(k => { const j = Engrave.items().get(k); return j && ((j.backs || []).length >= 1 || (j.stagedBacks || []).length >= 1 || j.backPending) && !j.backSaving; }, keys.carry, { timeout: 120000 }).catch(() => {});
    const fin = await page.evaluate(k => { const j = Engrave.items().get(k); return { staged: (j.stagedBacks || []).length, backs: (j.backs || []).length, pending: j.backPending || null, state: j.state }; }, keys.carry);
    const puts = calls.slice(n0).filter(c => c.op === 'backPut').length;
    // (the world's sheets are not saved on the fake site, so its own check "the sheet holds this charm copy" refuses the last step, the record: the file itself is built, verified and uploaded, staged once)
    check(fin.staged === 1 && puts >= 1 && ['approved', 'written'].includes(fin.state), 'once the piece is on a sheet its approved back file is built, checked and uploaded, once, and its record is sent (staged ' + fin.staged + ', ' + puts + ' backPut, ' + fin.state + '; the fake site then refuses the record only because its sheet was never saved: ' + fin.pending + ')');

    check(w.errors.length === 0, 'no page error (' + w.errors.join(' | ') + ')');
  } finally { await w.close(); }
  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
