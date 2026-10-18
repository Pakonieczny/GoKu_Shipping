// BLOCKREL + RELTOQ in the REAL page (Paul, 17 Oct 2026, orders 4193111740 DRAGON11 and 4191826593 FLORALS2: "rejected incorrectly" — "SKU DRAGON11 blocked: The charm outline is front artwork. Reload its CUT
// geometry before fitting engraving."; "I asked you to add a button so that I can release orders that are flagged for review back to the production/nesting queue"; "Please ensure this works perfectly").
// The real sorter in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory store), the master library of tests/charm-nest/_engraveRealWorld.cjs (real drawings
// served like Storage serves them), the real intake, pool, nest workers and the real release engine. Nothing leaves the machine; every name is made up.
//   1  the Orders tab, On hold: a stud earrings order (COMET_9427, a master entry blocked ONLY by the flip check's message) that the page held under the old rule: "Release hold" ends in SUCCESS, with no
//      "was not fully released", the pair is on a gold sheet (placed and saved), first in the queue, with its step on the timeline
//   2  Review: a SKU blocked for a real reason (SCHRODINGER CAT 2, an open outline) has "Release to nesting queue" (primary) and "Unblock this SKU" under its reason; the button is the same on every row:
//      an unknown SKU shows it greyed with the one plain reason (title and aria text), and pressing it changes nothing
//   3  "Release to nesting queue" on the blocked SKU: the block is set aside for that line (blockOverride { sku, by, at }), the master still says blocked, the order goes first in line onto the next sheet and is
//      placed, the card leaves Review, the timeline has the answer ("Decided by …") and the step; the override is in the run's record and comes back from it
//   4  a Send to Review mark: the row has the button, pressing it takes the order out of Review
//   5  "Unblock this SKU" on a blocked SKU's row still works: the master's own unblock, then the order goes back in line
//   RELHOLD (Paul, 18 Oct 2026): the button is the card's "Release Hold" now, in the card's own column where Complete Order stood (no "Release to nesting queue", nothing under the reason), and a press
//      keeps who and when (row.releaseHold) and puts the seal on the release step of the timeline (data.seal "releaseHold"); tests/charm-nest/release-hold.cjs has the seal, the Orders card and the cloud
//   node tests/charm-nest/review-release-queue.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir> saves a PNG of the Review rows)
'use strict';
const path = require('path'), fs = require('fs');
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
const V = (name, value) => ({ name, value });
const EXACT = 'The charm outline is front artwork. Reload its CUT geometry before fitting engraving.';
const CAT2 = { rid: '4290000131', sku: 'SCHRODINGER CAT 2', listing: '1890000131', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Eleven', placed: { day: 0, h: 9, m: 5 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const UNKNOWN = { rid: '4290000132', sku: 'NO_SUCH_DESIGN_77', listing: '1890000132', title: 'Mystery Charm Necklace', buyer: 'Name Twelve', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const CAT1 = { rid: '4290000134', sku: 'SCHRODINGER CAT 1', listing: '1890000134', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Fourteen', placed: { day: 0, h: 9, m: 8 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const MARKED = { rid: '4290000133', sku: 'COMET_9427', listing: '1890000133', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Thirteen', placed: { day: 0, h: 9, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
const EAR = R.ORDERS.earrings;   // 4290000106: a stud earring PAIR of COMET_9427 (never engraved)
const keyOf = fx => `${fx.rid}_${fx.rid}1`;
const TL = 'Order_Timeline';

const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };

(async () => {
  const shots = process.env.SHOTS || '';
  const W = await Base.world({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  if (!W) { console.log('no playwright-core: skipped'); process.exit(0); }
  const { page, srv } = W, st = srv.st;
  const saw = []; page.on('console', m => saw.push(`[${m.type()}] ${m.text()}`.slice(0, 500)));
  const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(300); } };
  const shot = async name => { if (!shots) return; try { fs.mkdirSync(shots, { recursive: true }); await page.screenshot({ path: path.join(shots, name + '.png') }); } catch (_) {} };
  try {
    // the master library (real drawings), then the two blocks: COMET_9427 only by the flip check's message, SCHRODINGER CAT 2 for a real reason (an open outline)
    R.seedMaster(st);
    const put = (sku, patch) => st.put('Charm_Master_Index', sku, Object.assign({}, st.doc('Charm_Master_Index', sku), patch));
    put('COMET_9427', { blocked: EXACT });
    put('SCHRODINGER CAT 2', { blocked: 'open outline', open: true });
    put('SCHRODINGER CAT 1', { blocked: 'open outline', open: true });
    st.put('Charm_Option_Map', '1890000106', { listingId: '1890000106', map: { style: { 'stud earrings': { field: 'form', value: 'earrings', by: 'Test', at: 1 } } } });
    await page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); if (window.Engrave) await Engrave.loadFonts(); });
    check(await page.evaluate(() => { const a = Master.entryFor('COMET_9427'), b = Master.entryFor('SCHRODINGER CAT 2'); return !!a.blocked && !!b.blocked && b.open === true; }), '0 · the master holds both blocks as the owner\'s did');

    await page.evaluate(() => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-rq`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    });
    const seedRows = fixtures => page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures, orderSrc: Base.orderOf.toString() });
    // the genuinely blocked order and the unknown SKU go through the REAL pool: held, with their questions
    await seedRows([CAT2, CAT1, UNKNOWN, MARKED]);
    await page.evaluate(async () => { await Pool.addAll(B.run); });
    // the earrings order as the old rule left it: held for the flip message (state "held", its reason, no hold flag), the page now reads the line with no question
    await seedRows([EAR]);
    await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'held'; r.reason = 'SKU blocked: ' + 'The charm outline is front artwork. Reload its CUT geometry before fitting engraving.'; r.hold = null; Orders.interpretAll(); Review.syncOrderItems(); Orders.render(); }, keyOf(EAR));
    const rowOf = fx => page.evaluate(k => { const r = B.orders.byKey.get(k); return r ? { state: r.state, reason: r.reason, hold: r.hold || null, frontAt: r.frontAt || null, releasing: r.releasing || null, poolIds: r.poolIds, problems: (r.problems || []).map(p => p.kind), override: r.blockOverride || null } : null; }, keyOf(fx));
    const e0 = await rowOf(EAR), c0 = await rowOf(CAT2), u0 = await rowOf(UNKNOWN);
    check(e0.state === 'held' && !e0.problems.length && !e0.hold, `1 · the earrings line is held by the old rule and the page reads it with no question (${JSON.stringify(e0)})`);
    check(c0.state === 'held' && c0.problems.join() === 'blockedSku' && /SKU blocked: open outline/.test(c0.reason), `2 · the cat necklace is held for a real block, with its question (${JSON.stringify(c0)})`);
    check(u0.problems.join() === 'unmatchedSku', `2 · the unknown SKU asks its question (${JSON.stringify(u0)})`);

    // ── 1 · Release hold on the earrings (Orders › On hold) ──
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('hold', ''); Orders.render(); });
    const relBtn = `#ordBody [data-key="${keyOf(EAR)}"] .relHold`;
    await until(() => page.$(relBtn), 20000, 'the Release hold button of the earrings');
    await shot('01-on-hold');
    const t1 = Date.now();
    await page.click(relBtn);
    await until(() => page.evaluate(rid => !OrderHold.releaseStatus(rid).running && !HoldUI.busy(rid), EAR.rid), 360000, 'the release to end');
    await page.waitForTimeout(1500);
    const e1 = await rowOf(EAR);
    const toasts1 = await page.evaluate(() => window.__toasts);
    check(!toasts1.some(t => /was not fully released|needs a look in Review|not on hold/.test(t[1])), `1 · no failure toast (${JSON.stringify(toasts1.map(t => t[1]).slice(-4))})`);
    check(e1.state === 'pooled' && !e1.hold && !e1.releasing && e1.poolIds.length === 2 && e1.frontAt >= t1 - 1000, `1 · the earrings are pooled, free of any hold and first in the queue (${JSON.stringify(e1)})`);
    const placeOf = rid => page.evaluate(rid => { const out = []; for (const sh of CN.allSheets()) { const on = new Set((sh.placements || []).map(p => p.id)); for (const c of sh.charms || []) if (String(c.order || '').split('/')[0] === rid || String(c.poolId || '').startsWith(rid + '_')) out.push({ poolId: c.poolId, sheet: sh.sheetId || null, metal: sh.metal, placed: on.has(c.id), saved: !!sh.persistedDone, label: (CN.S.sheets[sh.metal].pages.indexOf(sh) + 1) }); } return out; }, rid);
    let at = await until(async () => { const p = await placeOf(EAR.rid); return p.length === 2 && p.every(x => x.placed) && p; }, 240000, 'the pair placed on a sheet');
    check(at.length === 2 && at.every(x => x.metal === 'gold' && x.placed) && at[0].sheet === at[1].sheet, `1 · the pair is placed together on one gold sheet (${JSON.stringify(at)})`);
    const tl = rid => st.list(TL).filter(e => String(e._id).startsWith(rid + '~'));
    const ev1 = await until(() => { const l = tl(EAR.rid).filter(e => e.type === 'released'); return l.length && l; }, 60000, 'the released step on the timeline');
    check(ev1.length === 1 && /^Released from hold by .* · placed on /.test(ev1[0].text), `1 · one permanent step on the timeline: "${ev1[0] && ev1[0].text}"`);

    // ── 2 · the Review rows ──
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    const rowSel = fx => `#rvList .reviewListRow[data-rid="${fx.rid}"]`;
    await until(() => page.$(rowSel(CAT2)), 20000, 'the cat necklace row in Review');
    const look = fx => page.evaluate(sel => {
      const n = document.querySelector(sel); if (!n) return null; const b = n.querySelector('[data-rv-release]'), u = n.querySelector('[data-rv-unblock]');
      const box = x => { const r = x.getBoundingClientRect(); return { x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height) }; };
      return b ? { text: b.textContent.trim(), cls: b.className, how: b.dataset.rvRelease, disabled: b.getAttribute('aria-disabled'), title: b.title, aria: b.getAttribute('aria-label'), unblock: u ? { text: u.textContent.trim(), cls: u.className } : null, box: box(b), row: box(n), parent: b.parentElement.className, reason: (n.querySelector('.reviewReason') || {}).textContent || '', rowActions: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()) } : { none: true, rowActions: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()) };
    }, rowSel(fx));
    const cat = await look(CAT2);
    check(cat && cat.text === 'Release Hold' && /\bgold\b/.test(cat.cls) && cat.how === 'override' && cat.disabled === null && /Cut SCHRODINGER CAT 2 although the master blocks it \(open outline\)/.test(cat.title), `2 · the blocked SKU has "Release Hold", primary, enabled, and the title says what it does (${JSON.stringify(cat && [cat.text, cat.cls, cat.disabled, cat.title])})`);
    check(cat && cat.unblock && cat.unblock.text === 'Unblock this SKU' && /\bghost\b/.test(cat.unblock.cls), `2 · "Unblock this SKU" is beside it, secondary (${JSON.stringify(cat && cat.unblock)})`);
    check(cat && cat.parent === 'rowActions' && cat.box.x >= cat.row.x && cat.box.x + cat.box.w <= cat.row.x + cat.row.w && cat.box.y >= cat.row.y && cat.box.y + cat.box.h <= cat.row.y + cat.row.h + 1, `2 · it sits inside the row's box, in the card's own column of buttons (${JSON.stringify(cat && [cat.parent, cat.box, cat.row])})`);
    check(cat && cat.rowActions.filter(t => t === 'Release Hold').length === 1 && !cat.rowActions.includes('Complete Order') && !cat.rowActions.includes('Release to nesting queue'), `2 · ONE Release Hold in that column, where Complete Order stood, and no "Release to nesting queue" (${JSON.stringify(cat && cat.rowActions)})`);
    check(await page.evaluate(() => ![...document.querySelectorAll('button')].some(b => /Release to nesting queue/i.test(b.textContent)) && !document.querySelector('.rvRel')), '2 · no "Release to nesting queue" button anywhere on the page, none under a reason');
    const unk = await look(UNKNOWN);
    check(unk && unk.text === 'Release Hold' && /\bghost\b/.test(unk.cls) && unk.disabled === 'true' && /SKU NO_SUCH_DESIGN_77 has no design in any master file yet: pick its charm/.test(unk.title) && unk.aria.includes(unk.title.slice(0, 40)), `2 · the unknown SKU shows the same button, greyed, with the one plain reason as title and aria text (${JSON.stringify(unk && [unk.disabled, unk.title])})`);
    await shot('02-review-rows');
    const before = await rowOf(UNKNOWN), n0 = (await page.evaluate(() => window.__toasts)).length;
    await page.$eval(rowSel(UNKNOWN) + ' [data-rv-release]', b => b.click());   // (a greyed button is aria-disabled, not disabled: it can be pressed, and says why; Playwright's click() waits for it to be enabled)
    await page.waitForTimeout(500);
    const after = await rowOf(UNKNOWN), toasts2 = await page.evaluate(() => window.__toasts.slice());
    check(JSON.stringify(before) === JSON.stringify(after) && toasts2.length === n0 + 1 && /has no design in any master file yet/.test(toasts2[toasts2.length - 1][1]), `2 · pressing the greyed button says why and changes nothing (${JSON.stringify(toasts2.slice(-1))})`);

    // ── 3 · Release Hold on the blocked SKU ──
    const master0 = await page.evaluate(() => Master.entryFor('SCHRODINGER CAT 2').blocked);
    const t3 = Date.now();
    await page.click(rowSel(CAT2) + ' [data-rv-release]');
    await until(() => page.evaluate(rid => !OrderHold.releaseStatus(rid).running, CAT2.rid), 360000, 'the release to end');
    await page.waitForTimeout(1500);
    const c1 = await rowOf(CAT2), toasts3 = await page.evaluate(() => window.__toasts.slice());
    check(c1.state === 'pooled' && !c1.problems.length && c1.override && c1.override.sku === 'SCHRODINGER CAT 2' && c1.override.by === 'Test Operator' && c1.override.at >= t3 - 1000, `3 · the line carries the release { sku, by, at } and is pooled (${JSON.stringify(c1)})`);
    check(c1.frontAt >= t3 - 1000 && !c1.releasing, `3 · it went to the front of the queue (frontAt ${c1.frontAt})`);
    check(!toasts3.some(t => t[0] === 'bad'), `3 · no failure toast (${JSON.stringify(toasts3.filter(t => t[0] === 'bad'))})`);
    check(toasts3.some(t => t[0] === 'ok' && /Order 4290000131 released to the nesting queue: first in line, placed on /.test(t[1])), `3 · the page says where it went (${JSON.stringify(toasts3.slice(-2))})`);
    at = await until(async () => { const p = await placeOf(CAT2.rid); return p.length >= 1 && p.every(x => x.placed) && p; }, 240000, 'the cat necklace placed on a sheet');
    check(at.length === 1 && at[0].metal === 'gold' && at[0].placed, `3 · the necklace is placed on a gold sheet next to the earrings or on the next one (${JSON.stringify(at)})`);
    check(await page.evaluate(() => !!Master.entryFor('SCHRODINGER CAT 2').blocked) && master0 === 'open outline', '3 · the master still says the SKU is blocked: nothing was changed there');
    const itemGone = await until(() => page.evaluate(rid => !Review.items().some(i => i.kind === 'blockedSku' && (i.rows || [i.row]).some(r => r && String(r.order.receiptId) === rid)), CAT2.rid), 20000, 'the blocked card to leave Review');
    check(itemGone && !(await page.$(rowSel(CAT2) + ' [data-rv-release]')), '3 · its card has left Review');
    const dec = await until(() => { const l = tl(CAT2.rid).filter(e => e.type === 'decided'); return l.length && l; }, 60000, 'the answer on the timeline');
    check(dec.length >= 1 && /^Decided by Test Operator · Blocked SKU: Released to the nesting queue: SCHRODINGER CAT 2 is cut although it is blocked \(open outline\)/.test(dec[0].text), `3 · the timeline records the answer: "${dec[0] && dec[0].text}"`);
    const rel3 = await until(() => { const l = tl(CAT2.rid).filter(e => e.type === 'released'); return l.length && l; }, 60000, 'the released step');
    check(rel3.length === 1 && /^Released from hold by Test Operator · placed on /.test(rel3[0].text), `3 · and the step of the release: "${rel3[0] && rel3[0].text}"`);
    check(rel3[0] && rel3[0].data && rel3[0].data.seal === 'releaseHold' && +rel3[0].data.sealAt >= t3 - 1000, `3 · the step carries the Release Hold press's seal (RELHOLD: ${JSON.stringify(rel3[0] && rel3[0].data)})`);
    const kept3 = await page.evaluate(k => B.orders.byKey.get(k).releaseHold || null, keyOf(CAT2));
    check(kept3 && kept3.by === 'Test Operator' && kept3.at >= t3 - 1000, `3 · who and when are kept on its line (${JSON.stringify(kept3)})`);
    // persistence: in the run's lines, and back from them
    const rec = await page.evaluate(k => { const row = B.orders.byKey.get(k), r = Orders.lineRecord(row)[1], back = Orders.rowFromRecord(k, JSON.parse(JSON.stringify(r))); return { rec: r.blockOverride, back: back.blockOverride }; }, keyOf(CAT2));
    check(rec.rec && rec.rec.sku === 'SCHRODINGER CAT 2' && rec.back && rec.back.sku === 'SCHRODINGER CAT 2' && rec.back.by === 'Test Operator', `3 · the release is in the line's record and comes back from it (${JSON.stringify(rec)})`);
    await until(() => [...st.docs.values()].some(d => JSON.stringify(d).includes('"blockOverride"')), 30000, 'the run record in the cloud to carry the release').then(() => check(true, '3 · and the run record saved in the cloud carries it'), () => check(false, '3 · the run record saved in the cloud carries the release'));
    // read again with the release: no question; without it: the question returns (the release is what sets it aside)
    const reread = await page.evaluate(k => { const row = B.orders.byKey.get(k), keep = row.blockOverride; Orders.interpretAll(); const a = (row.spec.problems || []).map(p => p.kind); delete row.blockOverride; Orders.interpretAll(); const b = (row.spec.problems || []).map(p => p.kind); row.blockOverride = keep; Orders.interpretAll(); return { withIt: a, without: b, after: (row.spec.problems || []).map(p => p.kind) }; }, keyOf(CAT2));
    check(!reread.withIt.length && reread.without.join() === 'blockedSku' && !reread.after.length, `3 · read with the release: no question; without it: blocked again (${JSON.stringify(reread)})`);

    // ── 4 · a Send to Review mark ──
    const marked = await page.evaluate(async rid => { const r = await CharmNestReviewMark.mark(rid, { by: 'Test Operator' }); Review.syncOrderItems(); Review.render(); return r; }, MARKED.rid);
    check(marked && marked.ok, `4 · the order is sent to Review (${JSON.stringify(marked)})`);
    await until(() => page.$(rowSel(MARKED) + ' [data-rv-release]'), 20000, 'the button on the marked row');
    const mk = await look(MARKED);
    check(mk && mk.how === 'mark' && /\bgold\b/.test(mk.cls) && mk.disabled === null && /Take this order out of Review/.test(mk.title), `4 · the marked order has the button, enabled (${JSON.stringify(mk && [mk.how, mk.title])})`);
    await shot('04-review-mark');
    await page.click(rowSel(MARKED) + ' [data-rv-release]');
    await until(() => page.evaluate(rid => !CharmNestReviewMark.isMarked(rid), MARKED.rid), 30000, 'the mark to be resolved');
    const info = await page.evaluate(rid => CharmNestReviewMark.info(rid), MARKED.rid);
    check(info && info.state === 'done' && info.how === 'other' && info.doneBy === 'Test Operator', `4 · the mark is resolved by the button (${JSON.stringify(info)})`);
    check(await until(() => page.evaluate(rid => !Review.items().some(i => i.kind === 'reviewMark' && i.rid === rid), MARKED.rid), 20000, 'the mark to leave Review'), '4 · the row has left Review');
    const noteOk = await until(() => tl(MARKED.rid).some(e => /Review: released to the nesting queue by Test Operator/.test(e.text || '')), 30000, 'the timeline note');
    check(!!noteOk, '4 · the order\'s timeline says "Review: released to the nesting queue by Test Operator"');

    // ── 5 · Unblock this SKU ──
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await until(() => page.$(rowSel(CAT1) + ' [data-rv-unblock]'), 20000, 'the unblock button on the other blocked SKU');
    const c1before = await rowOf(CAT1);
    check(c1before.state === 'held' && c1before.problems.join() === 'blockedSku', `5 · the other blocked SKU waits in Review (${JSON.stringify(c1before)})`);
    await page.click(rowSel(CAT1) + ' [data-rv-unblock]');
    await until(() => page.evaluate(() => !Master.entryFor('SCHRODINGER CAT 1').blocked), 30000, 'the master to be unblocked');
    check(!st.doc('Charm_Master_Index', 'SCHRODINGER CAT 1').blocked, '5 · the master itself is unblocked in the cloud (the Master tab\'s own unblock)');
    const c1after = await until(async () => { const r = await rowOf(CAT1); return r && r.state === 'pooled' && r; }, 60000, 'the unblocked order to be pooled');
    check(c1after.state === 'pooled' && !c1after.problems.length && !c1after.override, `5 · the order is back in line, with no release of its own needed (${JSON.stringify(c1after)})`);
    const dec5 = await until(() => { const l = tl(CAT1.rid).filter(e => e.type === 'decided'); return l.length && l; }, 60000, 'the unblock on the timeline');
    check(/^Decided by Test Operator · Blocked SKU: Unblocked SCHRODINGER CAT 1/.test(dec5[0].text), `5 · the timeline says so: "${dec5[0] && dec5[0].text}"`);

    const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
    console.log(saw.slice(-25).join('\n'));
  } finally { await W.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nreview-release-queue: ${passed} checks passed`);
  process.exit(fails.length ? 1 : 0);
})();
