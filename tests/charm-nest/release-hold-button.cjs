// RELEASE (Paul, 19 Oct 2026): "For some reason, the new 'Release Hold' button is always greyed out and not functional. There should be very little that prevents the user from pressing that button and
// overwriting the hold. Sometimes the user may choose to do nothing, like in the case of the attached listing example where there's an empty chain that added to the order and the user technically doesn't have to do anything to
// release this order." (order 4203691345 TEST_@: option "Style (See photo #2): Style 8 • 9mm Disc" not mapped, no vector; order 4201047416 GOLD BRACELET CHAIN_6190: option "Style: Box" not mapped, no vector.)
// The real sorter in headless Chromium over the repo's fake site (bridge-server.cjs: the real charmNest* handlers over an in-memory store). Every name is made up. Nothing leaves the machine.
//   1  a line that still asks an option question: Release Hold is ENABLED (the seal orange, RHSAME 20 Oct 2026; was gold, no aria-disabled, focusable) on the Review card and on the order window's piece row, the same words, the old blocker sentence a note in its title
//      ("Releasing anyway: the option ... is not mapped"); the empty chain says there is nothing to place
//   2  the press on a line WITH a design: no refusal, the question is set aside for that line (row.releaseAnyway, in the run's line record and back from it), the line is made up and cut as it reads (pooled), the card leaves Review,
//      the RELEASE HOLD seal is stamped, the timeline's "released" step carries the seal and what was set aside; the cloud's Review document keeps who and when
//   3  the press on the EMPTY CHAIN (no design, "No vector available"): released, nothing to place: "noDesign" with no pieces, no error, no question again; the same seal, timeline step ("nothing": true) and cloud record
//   4  an order of a necklace and an empty chain: the chain is released with nothing to place and the necklace is made up and placed as ever
//   5  a HELD order (Orders > On hold) with an unmapped option: the card's Release hold is enabled, says what it sets aside, and releases (OrderHold.release); a held empty chain releases with nothing placed, no error
//   6  what stays blocked, with a clear reason: a cancelled order, an order that is not held (nothing to release) or already released, an order that is not here, a Custom Orders line, a piece that waits only on its engraving
//   7  the Orders card and the order window agree: the same decision (ReviewRelease.of) and the same title on both for one order
//   PW_DIR=<playwright node_modules> node tests/charm-nest/release-hold-button.cjs     (SHOTS=<dir> saves PNGs)
'use strict';
const path = require('path'), fs = require('fs');
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
const V = (name, value) => ({ name, value });
const NECK = { rid: '4290000401', sku: 'COMET_9427', listing: '1890000401', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Forty', placed: { day: 0, h: 9, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Style (See photo #2)', 'Style 8 • 9mm Disc')] };
const CHAIN = { rid: '4290000402', sku: 'GOLD BRACELET CHAIN_6190', listing: '1436596190', title: 'Dainty Gold Bracelet Chain, Layering Chain Bracelet, Gold Chain Bracelet', buyer: 'Name Fortyone', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [V('Style', 'Box')] };
const MIXED = { rid: '4290000403', sku: 'COMET_9427', listing: '1890000403', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Fortytwo', placed: { day: 0, h: 9, m: 5 }, ship: 5, variations: [V('Metal choice', 'Gold')],
  more: [{ sku: 'GOLD BRACELET CHAIN_6190', title: 'Dainty Gold Bracelet Chain, Layering Chain Bracelet', variations: [V('Style', 'Figaro')] }] };
const HELDN = { rid: '4290000404', sku: 'COMET_9427', listing: '1890000404', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Fortythree', placed: { day: 0, h: 8, m: 55 }, ship: 5, variations: [V('Metal choice', 'Gold'), V('Style (See photo #2)', 'Style 3 • 7mm Disc')] };
const HELDC = { rid: '4290000405', sku: 'GOLD BRACELET CHAIN_6190', listing: '1436596190', title: 'Dainty Gold Bracelet Chain, Layering Chain Bracelet', buyer: 'Name Fortyfour', placed: { day: 0, h: 8, m: 45 }, ship: 5, variations: [V('Style', 'Twist')] };
const CANCEL = { rid: '4290000406', sku: 'GOLD BRACELET CHAIN_6190', listing: '1436596190', title: 'Dainty Gold Bracelet Chain, Layering Chain Bracelet', buyer: 'Name Fortyfive', placed: { day: 0, h: 8, m: 35 }, ship: 5, variations: [V('Style', 'Snake')] };
const PLAIN = { rid: '4290000407', sku: 'COMET_9427', listing: '1890000407', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Fortysix', placed: { day: 0, h: 8, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold')] };
const HELDM = { rid: '4290000408', sku: 'COMET_9427', listing: '1890000408', title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer: 'Name Fortyeight', placed: { day: 0, h: 8, m: 15 }, ship: 5, variations: [V('Metal choice', 'Gold')],
  more: [{ sku: 'GOLD BRACELET CHAIN_6190', title: 'Dainty Gold Bracelet Chain, Layering Chain Bracelet', variations: [V('Style', 'Paperclip')] }] };
const ALL = [NECK, CHAIN, MIXED, HELDN, HELDC, CANCEL, PLAIN, HELDM];
const keyOf = (fx, n) => `${fx.rid}_${fx.rid}${n || 1}`;
const TL = 'Order_Timeline';

const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };

(async () => {
  const shots = process.env.SHOTS || '';
  const W = await Base.world({ viewport: { width: 1440, height: 950 }, reducedMotion: 'no-preference' });
  if (!W) { console.log('no playwright-core: skipped'); process.exit(0); }
  const { page, srv } = W, st = srv.st;
  const saw = []; page.on('console', m => saw.push(`[${m.type()}] ${m.text()}`.slice(0, 500)));
  const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(250); } };
  // (the name bar asks "Laser or Design?" once, after the first name is used: answered, as a person would, so it never covers a button)
  const calm = async () => { await page.evaluate(() => { const b = [...document.querySelectorAll('button')].find(x => x.textContent.trim() === 'Laser' && x.getClientRects().length); if (b) b.click(); }); await page.waitForTimeout(300); };
  // (a press as a person makes it: when the list has stopped moving; a click that lands while the page redraws the list under the pointer is lost, as it would be for a person, who presses again: `took` says whether it took)
  const press = async (sel, took) => {
    for (let n = 0; n < 5; n++) {
      await calm(); let last = '';
      for (let i = 0; i < 40; i++) { const r = await page.evaluate(sel => { const b = document.querySelector(sel), r = b && b.getBoundingClientRect(); return r ? [r.x, r.y, r.width, r.height].map(Math.round).join() : ''; }, sel); if (r && r === last) break; last = r; await page.waitForTimeout(250); }
      // (27 Oct 2026: a plain mouse press at the button's centre, as a person's. page.click arms Playwright's own hit-target guard on the window, and a press abandoned while an order card was drawn
      //  again (off-screen cards skip layout since perf-ui b3bda3e) left it armed: every later click, the page's own included, stopped at the window. Brought into view first, as a person scrolls to it)
      await page.evaluate(sel => { const b = document.querySelector(sel); if (b) b.scrollIntoView({ block: 'center' }); }, sel).catch(() => {});
      const box = await page.$(sel).then(h => h && h.boundingBox()).catch(() => null);
      if (n === 0 && box) await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2).catch(() => {});
      // (a press that did not take: the button's own handler, the code a person's press runs. Playwright's guard can be left armed on the window by an earlier page.click and stop every
      //  event there, the page's own included; the handler itself is what this test checks)
      else await page.evaluate(sel => { const b = document.querySelector(sel); if (b && typeof b.onclick === 'function') b.onclick({ stopPropagation() {}, preventDefault() {}, target: b, currentTarget: b }); }, sel).catch(() => {});
      for (let i = 0; i < 24; i++) { if (await took()) return true; await page.waitForTimeout(250); }
    }
    return false;
  };
  const shot = async name => { if (!shots) return; try { fs.mkdirSync(shots, { recursive: true }); await page.screenshot({ path: path.join(shots, name + '.png') }); } catch (_) {} };
  try {
    R.seedMaster(st);
    await page.evaluate(async () => { await Master.load({ force: true, fresh: true }); await Orders.loadMaps(true); });   // (fresh: a load already under way began before the library was seeded)
    await until(() => page.evaluate(() => !!Master.entryFor('COMET_9427')), 20000, 'the seeded library');
    await page.evaluate(() => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      window.__stamps = []; const so = Seal.stampOn; Seal.stampOn = (host, spec) => { try { const s = spec && (spec.stamp || spec); window.__stamps.push({ how: s && s.how, by: s && s.by, at: s && s.at }); } catch (_) {} return so(host, spec); };
      const has = Cancelled.has.bind(Cancelled); window.__cancel = new Set(); Cancelled.has = rid => window.__cancel.has(String(rid)) || has(rid);
      // (a run that is open: its intake makes a line up at once; the lines are put in as pulled, as the page's own pull does)
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-rlbtn`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    });
    const seedRows = fixtures => page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures, orderSrc: Base.orderOf.toString() });
    await seedRows(ALL);
    const rowOf = key => page.evaluate(k => { const r = B.orders.byKey.get(k); return r && { state: r.state, hold: r.hold || null, reason: r.reason || '', problems: r.problems.map(p => p.kind), pool: (r.poolIds || []).length, ra: r.releaseAnyway || null, rh: r.releaseHold || null, frontAt: r.frontAt || 0 }; }, key);
    const taken = key => async () => { const r = await rowOf(key); return !!(r && r.ra); };
    const bad = () => page.evaluate(() => window.__toasts.filter(t => t[0] === 'bad'));
    const rowSel = fx => `#rvList .reviewListRow[data-rid="${fx.rid}"]`;
    const look = sel => page.evaluate(sel => {
      const n = document.querySelector(sel); if (!n) return null; const b = n.querySelector('[data-rv-release]'); if (!b) return { none: true };
      return { text: b.textContent.trim(), cls: b.className, how: b.dataset.rvRelease, disabled: b.getAttribute('aria-disabled'), dis: b.disabled, title: b.title, aria: b.getAttribute('aria-label'), tab: b.tabIndex, cursor: getComputedStyle(b).cursor, pe: getComputedStyle(b).pointerEvents };
    }, sel);

    // ── 1 · the buttons: enabled, with the old blocker as a note ──
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await until(() => page.$(rowSel(NECK) + ' [data-rv-release]'), 20000, 'Release Hold on the necklace row');
    await until(() => page.$(rowSel(CHAIN) + ' [data-rv-release]'), 20000, 'Release Hold on the chain row');
    const n1 = await look(rowSel(NECK)), c1 = await look(rowSel(CHAIN));
    check(n1 && n1.text === 'Release Hold' && n1.how === 'anyway' && n1.disabled === null && !n1.dis && /\brelHoldBtn\b/.test(n1.cls) && !/ghost/.test(n1.cls) && n1.pe !== 'none' && n1.cursor !== 'not-allowed' && n1.tab >= 0, `1 · the option question does not grey Release Hold: enabled, seal orange (RHSAME), focusable (${JSON.stringify(n1)})`);
    check(/Releasing anyway: the option “Style \(See photo #2\): Style 8 • 9mm Disc” is not mapped/.test(n1.title) && !/not answered yet: say what it decides first/.test(n1.title), `1 · the old blocker is a note on its title: ${n1 && n1.title}`);
    check(c1 && c1.how === 'anyway' && c1.disabled === null && /Releasing anyway: the option “Style: Box” is not mapped/.test(c1.title) && /nothing to place/.test(c1.title) && /no design in any master file/.test(c1.title), `1 · the empty chain: enabled, and it says there is nothing to place (${c1 && c1.title}) master=${JSON.stringify(await page.evaluate(() => [B.master.entries.size, B.master.loadedAt > 0, !!B.master.loading, !!Master.entryFor('COMET_9427')]))}`);
    await shot('01-review');
    // the order window's piece row: the same button, the same words
    await page.evaluate(k => OrderWin.open(k), keyOf(CHAIN));
    await page.waitForFunction(k => OrderWin.key() === k && document.querySelector('#owPcSum [data-rel-hold]'), keyOf(CHAIN), { timeout: 20000 });
    const w1 = await page.evaluate(() => { const b = document.querySelector('#owPcSum [data-rel-hold]'); return b && { how: b.dataset.rvRelease, disabled: b.getAttribute('aria-disabled'), cls: b.className, title: b.title, text: b.textContent.trim() }; });
    check(w1 && w1.text === 'Release Hold' && w1.how === 'anyway' && w1.disabled === null && /\brelHoldBtn\b/.test(w1.cls) && w1.title === c1.title, `7 · the order window's piece row agrees with the Review card: same decision, same title (${JSON.stringify(w1)})`);
    await shot('01-window');
    await page.evaluate(() => OrderWin.close());
    await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 10000 });

    // ── 2 · the press on a line WITH a design ──
    const t0 = Date.now();
    check(await press(rowSel(NECK) + ' [data-rv-release]', taken(keyOf(NECK))), '2 · the press took');
    await until(async () => { const r = await rowOf(keyOf(NECK)); return r && r.ra && !r.problems.length && r.pool > 0; }, 45000, 'the necklace line to be made up');
    const r2 = await rowOf(keyOf(NECK));
    check(r2.ra && r2.ra.kinds.includes('needsMapping') && r2.ra.by && r2.state !== 'held' && r2.state !== 'unmatched' && !r2.problems.length, `2 · the question is set aside for that line and it is made up as it reads (${JSON.stringify(r2)})`);
    check((await bad()).length === 0, `2 · no failure toast (${JSON.stringify(await bad())})`);
    check(await page.evaluate(() => !Review.items().some(i => i.kind === 'needsMapping' && i.rows.some(r => r.order.receiptId === '4290000401'))), '2 · its card has left Review');
    const stamped = await until(() => page.evaluate(() => window.__stamps.find(s => s.how === 'releaseHold')), 15000, 'the stamp').catch(() => null);
    check(stamped && stamped.at >= t0 - 1000, `2 · the RELEASE HOLD seal is stamped on the button (${JSON.stringify(stamped)})`);
    const ev2 = await until(() => st.list(TL).find(e => String(e._id).startsWith(NECK.rid + '~') && e.type === 'released' && e.data && e.data.seal === 'releaseHold'), 30000, 'the released step').catch(() => null);
    check(ev2 && Array.isArray(ev2.data.anyway) && /not mapped/.test(ev2.data.anyway[0]) && /released anyway/i.test(ev2.text), `2 · the timeline's "released" step carries the seal and what was set aside (${JSON.stringify(ev2 && { text: ev2.text, data: ev2.data })})`);
    const cloud2 = await until(() => { const d = st.doc('Charm_Custom_Orders', NECK.rid + '_review'); return d && d.release && d.release.at && d; }, 20000, 'the cloud record').catch(() => null);
    check(cloud2 && cloud2.release.at >= t0 - 1000, `2 · the cloud keeps who and when (${JSON.stringify(cloud2 && cloud2.release)})`);
    const rec2 = await page.evaluate(k => { const row = B.orders.byKey.get(k), rec = Orders.lineRecord(row)[1], back = Orders.rowFromRecord(k, JSON.parse(JSON.stringify(rec))); return { rec: rec.releaseAnyway || null, back: back.releaseAnyway || null }; }, keyOf(NECK));
    check(rec2.rec && rec2.back && rec2.back.kinds.includes('needsMapping') && rec2.back.at === rec2.rec.at, `2 · kept in the run's line record and back from it (${JSON.stringify(rec2)})`);
    await page.evaluate(() => { Orders.interpretAll(); Review.syncOrderItems(); });
    check(await page.evaluate(() => !Review.items().some(i => (i.rows || []).some(r => r.order.receiptId === '4290000401'))), '2 · read again, the question does not come back');

    // ── 3 · the empty chain: released, nothing to place ──
    const t3 = Date.now(); await page.evaluate(() => { window.__stamps.length = 0; });
    check(await press(rowSel(CHAIN) + ' [data-rv-release]', taken(keyOf(CHAIN))), '3 · the press took');
    await until(async () => { const r = await rowOf(keyOf(CHAIN)); return r && r.state === 'noDesign' && r.ra && r.rh; }, 30000, 'the chain line to be released');
    const r3 = await rowOf(keyOf(CHAIN));
    check(r3.state === 'noDesign' && r3.pool === 0 && !r3.problems.length && !r3.hold && /^Released anyway by .+: .*no design in any master file.*nothing is placed/.test(r3.reason) && r3.rh && r3.rh.at >= t3 - 1000, `3 · "noDesign", no pieces, no question, who and when on the line (${JSON.stringify(r3)})`);
    check((await bad()).length === 0, `3 · no error toast (${JSON.stringify(await bad())})`);
    const said3 = await page.evaluate(() => window.__toasts.filter(t => /4290000402/.test(t[1])).map(t => t[1]));
    check(said3.some(t => /nothing to place/.test(t) && /Released anyway: the option/.test(t)), `3 · the page says so: ${JSON.stringify(said3)}`);
    const ev3 = await until(() => st.list(TL).find(e => String(e._id).startsWith(CHAIN.rid + '~') && e.type === 'released' && e.data && e.data.seal === 'releaseHold'), 30000, 'the released step').catch(() => null);
    check(ev3 && ev3.data.nothing === true && +ev3.data.sealAt === r3.rh.at && /nothing to place/.test(ev3.text) && /released anyway/.test(ev3.text), `3 · the timeline step carries the seal, "nothing to place" and what was set aside (${JSON.stringify(ev3 && { text: ev3.text, data: ev3.data })})`);
    const cloud3 = await until(() => { const d = st.doc('Charm_Custom_Orders', CHAIN.rid + '_review'); return d && d.release && d.release.at && d; }, 20000, 'the cloud record').catch(() => null);
    check(cloud3 && cloud3.release.at === r3.rh.at, `3 · the cloud keeps the press (${JSON.stringify(cloud3 && cloud3.release)})`);
    check(await page.evaluate(() => { const r = B.orders.byKey.get('4290000402_42900004021'); r.spec = r.spec; Orders.interpretAll(); Review.syncOrderItems(); return !Review.items().some(i => (i.rows || []).some(x => x.order.receiptId === '4290000402')) && B.orders.byKey.get('4290000402_42900004021').state === 'noDesign'; }), '3 · read again: still released, nothing asked');
    await shot('03-chain-released');

    // ── 4 · a necklace and an empty chain in ONE order ──
    //   (a) the Review press on the chain's card: the chain is released with nothing to place; the necklace line (no question of its own) is not touched by it: the run's own intake places it, as ever
    await page.evaluate(() => { Review.syncOrderItems(); Review.render(); });
    await until(() => page.$(rowSel(MIXED) + ' [data-rv-release]'), 20000, 'Release Hold on the mixed order');
    check(!!(await look(rowSel(MIXED))) && (await look(rowSel(MIXED))).how === 'anyway', '4 · the mixed order: Release Hold enabled');
    const before4 = await rowOf(keyOf(MIXED, 1));
    check(await press(rowSel(MIXED) + ' [data-rv-release]', taken(keyOf(MIXED, 2))), '4a · the press took');
    await until(async () => { const b = await rowOf(keyOf(MIXED, 2)); return b && b.state === 'noDesign' && b.ra; }, 45000, 'the chain line of the mixed order');
    const a4 = await rowOf(keyOf(MIXED, 1)), b4 = await rowOf(keyOf(MIXED, 2));
    check(b4.state === 'noDesign' && b4.pool === 0 && !b4.problems.length && a4.state === before4.state && !a4.ra && !a4.problems.length, `4a · the chain has nothing to place; the necklace line is not touched (${JSON.stringify({ a4, b4 })})`);
    check((await bad()).length === 0, `4a · no error toast (${JSON.stringify(await bad())})`);
    //   (b) a HELD order of both lines, a run open: the engine releases it whole: the necklace is made up and placed on a sheet as ever, the chain has nothing to place
    await page.evaluate(() => {
      for (const k of ['4290000408_42900004081', '4290000408_42900004082']) { const r = B.orders.byKey.get(k); r.hold = 'Held by Test Operator'; r.state = 'held'; r.reason = r.hold; }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.showPile('', ''); Orders.render();
    });
    const cardM = `#ordBody [data-rid="${HELDM.rid}"] .relHold:not([data-gate])`;
    await until(() => page.$(cardM), 20000, 'the held mixed card');
    const planM = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => ({ can: p.canRelease, over: p.overrides, nothing: p.nothing, effects: p.effects, targets: p.targets.map(t => t.pieces) })), HELDM.rid);
    check(planM.can && planM.over.length === 1 && /Paperclip/.test(planM.over[0]) && planM.nothing.length === 1 && planM.targets.join() === '1', `4b · the plan: the necklace goes on a sheet, the chain has nothing to place (${JSON.stringify(planM)})`);
    check(await press(cardM, async () => { const a = await rowOf(keyOf(HELDM, 1)); return !!a && (!a.hold || !!a.ra); }), '4b · the press took');
    await until(async () => { const a = await rowOf(keyOf(HELDM, 1)), b = await rowOf(keyOf(HELDM, 2)); return a && b && !a.hold && !b.hold && b.state === 'noDesign' && a.pool > 0; }, 90000, 'the held mixed order to be released');
    const ev4 = await until(() => st.list(TL).find(e => String(e._id).startsWith(HELDM.rid + '~') && e.type === 'released'), 120000, 'the released step').catch(() => null);
    const a4b = await rowOf(keyOf(HELDM, 1)), b4b = await rowOf(keyOf(HELDM, 2));
    check(['pooled', 'nested', 'written', 'labelled'].includes(a4b.state) && a4b.pool > 0 && b4b.state === 'noDesign' && b4b.pool === 0 && !b4b.problems.length, `4b · the necklace is made up and placed, the chain released with nothing to place (${JSON.stringify({ a4b, b4b })})`);
    check(ev4 && /placed on/.test(ev4.text) && /nothing to place for 1 piece/.test(ev4.text) && ev4.data.nothing === 1 && ev4.data.anyway && /Paperclip/.test(ev4.data.anyway[0]), `4b · the timeline step says where the necklace went and that the chain had nothing to place (${JSON.stringify(ev4 && { text: ev4.text, data: ev4.data })})`);
    check((await bad()).length === 0, `4b · no error toast (${JSON.stringify(await bad())})`);

    // ── 5 · a HELD order, in Open Orders (NOHOLDPILE) ──
    await page.evaluate(() => {
      for (const k of ['4290000404_42900004041', '4290000405_42900004051']) { const r = B.orders.byKey.get(k); r.hold = 'Held by Test Operator'; r.state = 'held'; r.reason = r.hold; }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.showPile('', ''); Orders.render();
    });
    const card = rid => `#ordBody [data-rid="${rid}"] .relHold:not([data-gate])`;
    await until(() => page.$(card(HELDN.rid)), 20000, 'the held necklace card');
    await until(() => page.$(card(HELDC.rid)), 20000, 'the held chain card');
    const h5 = await page.evaluate(sel => { const b = document.querySelector(sel); return b && { disabled: b.disabled, aria: b.getAttribute('aria-disabled'), title: b.title, cls: b.className }; }, card(HELDN.rid));
    check(h5 && !h5.disabled && h5.aria === null && /Releasing anyway: the option “Style \(See photo #2\): Style 3 • 7mm Disc” is not mapped/.test(h5.title), `5 · the held card's Release hold is enabled and says what it sets aside (${JSON.stringify(h5)})`);
    const plan5 = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => ({ can: p.canRelease, why: p.blockedWhy, over: p.overrides, effects: p.effects, nothing: p.nothing })), HELDN.rid);
    check(plan5.can === true && plan5.over.length === 1 && /not mapped/.test(plan5.over[0]) && plan5.effects.some(t => /^Releasing anyway: the option/.test(t)), `5 · the plan says it first (${JSON.stringify(plan5)})`);
    const planC = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => ({ can: p.canRelease, nothing: p.nothing, effects: p.effects })), HELDC.rid);
    check(planC.can === true && planC.nothing.length === 1 && planC.effects.some(t => /nothing to place/.test(t)), `5 · the held chain's plan: nothing to place (${JSON.stringify(planC)})`);
    // the necklace: no run is open here, so it waits at the front of the queue (the placement itself is the engine's own, order-release-engine.cjs)
    await page.evaluate(() => { window.__run = B.run; B.run = null; });
    check(await press(card(HELDN.rid), async () => { const a = await rowOf(keyOf(HELDN)); return !!a && (!a.hold || !!a.ra); }), '5 · the press took');
    await until(async () => { const r = await rowOf(keyOf(HELDN)); return r && !r.hold && r.ra && r.frontAt > 0 && !r.problems.length; }, 45000, 'the held necklace to be released');
    const r5 = await rowOf(keyOf(HELDN));
    check(r5.ra && r5.ra.kinds.includes('needsMapping') && !r5.hold && r5.frontAt > 0, `5 · the held necklace is released at the front of the queue with the question set aside (${JSON.stringify(r5)})`);
    const ev5 = await until(() => st.list(TL).find(e => String(e._id).startsWith(HELDN.rid + '~') && e.type === 'released'), 30000, 'the released step').catch(() => null);
    check(ev5 && Array.isArray(ev5.data.anyway) && /not mapped/.test(ev5.data.anyway[0]) && /released anyway/.test(ev5.text), `5 · its timeline step says what was set aside (${JSON.stringify(ev5 && { text: ev5.text, data: ev5.data })})`);
    // (every release a press allows records the release seal: who and when on the lines, in the cloud's Review record, on the timeline step)
    const rh5 = await until(async () => { const r = await rowOf(keyOf(HELDN)); return r && r.rh && r; }, 30000, 'the seal on the held necklace line').catch(() => null);
    const cloud5 = await until(() => { const d = st.doc('Charm_Custom_Orders', HELDN.rid + '_review'); return d && d.release && d.release.at && d; }, 20000, 'the cloud record').catch(() => null);
    check(rh5 && rh5.rh.by === 'Test Operator' && rh5.rh.at > 0 && /^releaseHold:/.test(rh5.rh.id) && cloud5 && cloud5.release.at === rh5.rh.at && cloud5.release.by === 'Test Operator' && ev5 && ev5.data.seal === 'releaseHold' && +ev5.data.sealAt === rh5.rh.at, `5 · the release seal is recorded for the Orders-card release of an unmapped option: line, cloud, timeline (${JSON.stringify({ rh: rh5 && rh5.rh, cloud: cloud5 && cloud5.release, seal: ev5 && ev5.data.seal })})`);
    check((await bad()).length === 0, `5 · no error toast (${JSON.stringify(await bad())})`);
    // the held empty chain, with a run open: released, nothing placed, no error
    await page.evaluate(() => { B.run = window.__run; CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });
    await until(() => page.$(card(HELDC.rid)), 20000, 'the held chain card');
    check(await press(card(HELDC.rid), async () => { const a = await rowOf(keyOf(HELDC)); return !!a && (!a.hold || !!a.ra); }), '5 · the press took (the chain)');
    await until(async () => { const r = await rowOf(keyOf(HELDC)); return r && !r.hold && r.state === 'noDesign' && r.ra; }, 45000, 'the held chain to be released');
    const r5c = await rowOf(keyOf(HELDC));
    check(r5c.state === 'noDesign' && r5c.pool === 0 && !r5c.problems.length && !r5c.hold, `5 · the held empty chain: released, nothing to place (${JSON.stringify(r5c)})`);
    const ev5c = await until(() => st.list(TL).find(e => String(e._id).startsWith(HELDC.rid + '~') && e.type === 'released'), 30000, 'the released step').catch(() => null);
    check(ev5c && ev5c.data.nothing === 1 && /nothing to place/.test(ev5c.text), `5 · its timeline step says nothing was placed (${JSON.stringify(ev5c && { text: ev5c.text, data: ev5c.data })})`);
    const rh5c = await until(async () => { const r = await rowOf(keyOf(HELDC)); return r && r.rh && r; }, 30000, 'the seal on the held chain line').catch(() => null);
    const cloud5c = await until(() => { const d = st.doc('Charm_Custom_Orders', HELDC.rid + '_review'); return d && d.release && d.release.at && d; }, 20000, 'the cloud record').catch(() => null);
    check(rh5c && rh5c.rh.by === 'Test Operator' && cloud5c && cloud5c.release.at === rh5c.rh.at && ev5c && ev5c.data.seal === 'releaseHold' && +ev5c.data.sealAt === rh5c.rh.at, `5 · and for the empty chain released with nothing to place: line, cloud, timeline (${JSON.stringify({ rh: rh5c && rh5c.rh, cloud: cloud5c && cloud5c.release, seal: ev5c && ev5c.data.seal })})`);
    check(await page.evaluate(rid => !!Orders.releaseStampOf(Orders.rows().filter(r => String(r.order.receiptId) === rid)), HELDC.rid), '5 · the Orders card reads the seal back (Orders.releaseStampOf)');
    const bad5 = (await bad()).filter(t => /HELDC|4290000405|4290000404/.test(t[1]));
    check(!bad5.length, `5 · no error toast for the held orders (${JSON.stringify(bad5)})`);
    await shot('05-held-released');

    // ── 6 · what stays blocked, with a clear reason ──
    const six = await page.evaluate(async ({ cancel, plain, here, chainDone, neck }) => {
      window.__cancel.add(cancel);
      const rowsOf = rid => Orders.rows().filter(r => String(r.order.receiptId) === rid);
      const item = (kind, rid, extra) => Object.assign({ kind, key: 'x:' + kind + rid, rows: rowsOf(rid), row: rowsOf(rid)[0], problem: {} }, extra || {});
      // a cancelled order: the line still asks its option question
      const cr = rowsOf(cancel)[0]; const cItem = item('needsMapping', cancel, { problem: cr.problems[0] });
      const out = { cancelled: ReviewRelease.of(cItem), customOrder: ReviewRelease.of(item('customOrder', plain)), plainEng: ReviewRelease.of(item('engraveWords', plain)), gone: ReviewRelease.of({ kind: 'needsMapping', key: 'x', rows: [], problem: {} }) };
      out.planCancelled = await OrderHold.releasePlan(cancel).then(p => ({ can: p.canRelease, why: p.blockedWhy }));
      out.planPlain = await OrderHold.releasePlan(plain).then(p => ({ can: p.canRelease, why: p.blockedWhy }));
      out.planDone = await OrderHold.releasePlan(chainDone).then(p => ({ can: p.canRelease, why: p.blockedWhy }));
      out.planNeckDone = await OrderHold.releasePlan(neck).then(p => ({ can: p.canRelease, why: p.blockedWhy }));
      out.planNone = await OrderHold.releasePlan(here).then(p => ({ can: p.canRelease, why: p.blockedWhy }));
      out.pressedPlain = await OrderHold.release(plain, { name: 'Test Operator' }).then(r => ({ ok: r.ok, error: r.error }));
      out.pressedNone = await OrderHold.release(here, { name: 'Test Operator' }).then(r => ({ ok: r.ok, error: r.error }));
      out.pressedCancelled = await OrderHold.release(cancel, { name: 'Test Operator' }).then(r => ({ ok: r.ok, error: r.error }));
      out.cancelRow = (({ ra, hold }) => ({ ra, hold }))({ ra: cr.releaseAnyway || null, hold: cr.hold || null });
      return out;
    }, { cancel: CANCEL.rid, plain: PLAIN.rid, here: '4299999999', chainDone: HELDC.rid, neck: HELDN.rid });
    check(six.cancelled.how === '' && /was cancelled: there is nothing to release/.test(six.cancelled.why), `6 · a cancelled order: blocked, with its reason (${JSON.stringify(six.cancelled)})`);
    check(six.customOrder.how === '' && /made by hand/.test(six.customOrder.why), `6 · a Custom Orders line: blocked (made by hand, never nested): ${six.customOrder.why}`);
    check(six.plainEng.how === '' && /Nothing holds this piece out of the nesting queue|waits only on its engraving/.test(six.plainEng.why), `6 · a piece that nothing holds back: blocked, nothing to release (${six.plainEng.why})`);
    check(six.gone.how === '' && /no longer in this run/.test(six.gone.why), `6 · an order that is not here: ${six.gone.why}`);
    check(!six.planCancelled.can && /was cancelled/.test(six.planCancelled.why), `6 · the engine's plan for a cancelled order: ${six.planCancelled.why}`);
    check(!six.planPlain.can && /not on hold/.test(six.planPlain.why), `6 · an order that is not held: ${six.planPlain.why}`);
    check(!six.planDone.can && /not on hold/.test(six.planDone.why) && !six.planNeckDone.can, `6 · an order already released is not on hold: ${six.planDone.why}`);
    check(!six.planNone.can && /not in this sorter/.test(six.planNone.why), `6 · an order that is not here: ${six.planNone.why}`);
    check(!six.pressedPlain.ok && /not on hold/.test(six.pressedPlain.error) && !six.pressedNone.ok && /not in this sorter/.test(six.pressedNone.error) && !six.pressedCancelled.ok && /was cancelled/.test(six.pressedCancelled.error), `6 · pressing them is refused with the same reasons (${JSON.stringify([six.pressedPlain, six.pressedNone, six.pressedCancelled])})`);
    check(six.cancelRow.ra === null && !six.cancelRow.hold, `6 · and nothing was written on the cancelled order's line (${JSON.stringify(six.cancelRow)})`);

    const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
    console.log(saw.slice(-25).join('\n'));
    try { console.log('toasts:', JSON.stringify(await page.evaluate(() => window.__toasts.slice(-8))), 'rows:', JSON.stringify(await page.evaluate(() => Orders.rows().filter(r => /^42900004(03|08)/.test(r.order.receiptId)).map(r => [r.key, r.state, r.reason, r.problems.map(p => p.kind), !!r.releaseAnyway])))); } catch (_) {}
    await shot('stopped');
  } finally { await W.close(); }
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nrelease-hold-button: ${passed} checks passed`);
  process.exit(fails.length ? 1 : 0);
})();
