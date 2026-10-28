// RELWIN2 (Paul, 28 Oct 2026, order 4202871419: "When an order is held for review, the users should be able to release it from the detailed order Modal. Some orders I see the release button but most
// other ones I don't see it so there must be some sort of disconnect here."). His window showed a held order's pieces with "On hold", "In Review" and the ORDER HELD seal, and no Release anywhere:
// a piece row drew Release Hold only when the piece had a Review card of its own. Every way an order is held, each in the REAL sorter page (bridge-server.cjs fake site, _reviewSendWorld.cjs), the
// order window's piece rows now offer Release Hold (the Orders card's own button and press: ReviewRelease, OrderHold.release, the ORDER RELEASED seal), and pressing it there releases the order:
//   a  Hold Order on a piece on a sheet (marked in Review, taken off its sheet)
//   b  Hold Order on an order with nothing on a sheet (the Review mark alone, "Order held by")
//   c  the sheet window's "Take off and hold" / the Library's Hold (the line held, its pool row held, no Review mark)
//   d  a hold made on ANOTHER computer (only the cloud's pool row says so: this page reads it through the placement feed)
//   e  an order of two pieces held by Hold Order: Release Hold on each piece's row; one press releases the order
//   f  an order of two pieces with ONE piece held (the sheet window's "Only this charm"): Release Hold on that piece's row only
// For each: shown (enabled, one per held row), pressed in the window: a "released" step with the Release Hold seal on the timeline, no line held or marked after, the row offers Hold Order again, and
// the order's ORDER RELEASED seal is known to the window (OrderWin.seals()).
//   PW_DIR=<playwright node_modules> node tests/charm-nest/order-window-release.cjs   (through scratchpad/int/slot.sh)
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
check(/charm-nest-bridge\.js\?v=[^"]*-rw2/.test(fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8')), 'source: the bridge\'s cache token carries -rw2');

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  try { require(path.join(pwDir, 'playwright-core')); } catch (_) { console.log('order-window-release: no playwright-core, the page part is skipped'); return done(); }
  const W = require('./_reviewSendWorld.cjs'), V = (name, value) => ({ name, value });
  W.ORDERS.two = { rid: '4291000021', listing: '1891000021', sku: 'COMET_9427', title: 'Comet Necklace', buyer: 'Name Twenty-One', placed: { day: 0, h: 9, m: 45 }, ship: 5, variations: [V('Metal choice', 'Gold')], more: [{ sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace', variations: [V('Metal choice', 'Gold')] }] };
  W.ORDERS.half = { rid: '4291000022', listing: '1891000022', sku: 'COMET_9427', title: 'Comet Necklace', buyer: 'Name Twenty-Two', placed: { day: 0, h: 9, m: 50 }, ship: 5, variations: [V('Metal choice', 'Gold')], more: [{ sku: 'SCHRODINGER CAT 1', title: 'Cat Necklace', variations: [V('Metal choice', 'Gold')] }] };
  const w = await W.open('production', { reducedMotion: 'reduce' });
  if (!w) { console.log('order-window-release: no browser, the page part is skipped'); return done(); }
  const { page } = w, st = w.srv.st;
  const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(250); } };
  const keysOf = rid => page.evaluate(r => Orders.rows().filter(x => String(x.order.receiptId) === r).map(x => x.key), rid);
  const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await W.settle(page, 400); };
  const openWin = async key => { await closeWin(); await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(() => OrderWin.isOpen() && !document.getElementById('owPcSum').hidden, null, { timeout: 20000 }); await W.settle(page, 1500); };
  /** each piece row of the window: its key, its Release Hold buttons (words, enabled), its other buttons */
  const rows = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ key: r.dataset.piece || r.dataset.item || '', rel: [...r.querySelectorAll('[data-rel-hold],[data-cu-release]')].map(b => ({ t: b.textContent.trim(), on: b.getAttribute('aria-disabled') !== 'true' && !b.disabled })), btns: [...r.querySelectorAll('.pcAct button')].map(b => b.textContent.trim()).filter(Boolean) })));
  // the Hold Order engine's run, stood in for (this world keeps its sheets in the cloud only): what the take-off leaves (the lines held, SheetEvents "held"), as hold-btn-review.cjs does
  const standIn = () => page.evaluate(() => {
    const real = window.__real = window.__real || window.OrderHold;
    window.OrderHold = Object.assign({}, real, {
      plan: async r => ({ rid: r, canHold: true, pieces: [{ poolId: r + '_p1', state: 'onSheet', sheetLabel: 'GF Sheet 1' }], sheets: [], fills: [], stays: [] }),
      run: async (r, o) => { const who = (o && o.name) || 'Test Operator', text = 'Taken off GF Sheet 1 by ' + who; for (const row of Orders.rows()) if (String(row.order.receiptId) === r) { row.poolIds = []; row.state = 'held'; row.hold = row.reason = text; row.heldAt = Date.now(); } window.SheetEvents.order({ type: 'held', orderId: r, id: 'sw-sh-1-' + Date.now(), by: who, sheetId: 'sh-1', sheet: 'GF Sheet 1', text: text + ', on hold', data: { pieces: 1 } }); return { ok: true, held: true, steps: [] }; }
    });
  });
  const realEngine = () => page.evaluate(() => { if (window.__real) window.OrderHold = window.__real; });
  const holdOrder = async key => { await openWin(key); await page.click('#owPcSum .revSendBtn[data-rev-host]'); const rid = key.split('_')[0]; await page.waitForFunction(r => CharmNestReviewMark.isMarked(r) && !ReviewSend.busy(r) && !HoldUI.busy(r), rid, { timeout: 30000 }); await W.settle(page, 600); };
  /** the line(s) and pool rows as the sheet window's "Take off and hold" leaves them (the line held, its piece's pool row abandoned and held by the person) */
  const sheetHold = async (keys, who) => {
    await page.evaluate(({ keys, who }) => { for (const row of Orders.rows()) if (keys.includes(row.key)) { row.state = 'held'; row.hold = row.reason = `Taken off GF Sheet 2 by ${who}`; row.heldAt = Date.now(); row.holdSeen = false; } Review.syncOrderItems(); Orders.render(); }, { keys, who });
  };
  const cloudHold = async (nm, who) => {
    const pid = W.poolIdOf(nm); st.put('Charm_Pool', pid, Object.assign({}, st.doc('Charm_Pool', pid), { state: 'abandoned', sheetId: null, setId: null, heldBy: who, heldReason: 'on hold', heldAt: Date.now() }));
    await page.evaluate(r => window.OrderPieces && OrderPieces.load([r], { force: true }), W.ORDERS[nm].rid); await W.settle(page, 800);
    await page.evaluate(() => window.PlacementFeed && PlacementFeed.nudge && PlacementFeed.nudge()); await W.settle(page, 1500);
  };
  /** Release Hold pressed on the window's row `idx`; then what the page and the cloud say */
  const press = async (rid, idx) => {
    const t0 = Date.now();
    await page.locator('#owPcSum .owPcRow').nth(idx).locator('[data-rel-hold],[data-cu-release]').first().click();
    const ev = await until(() => st.list('Order_Timeline').find(e => String(e.orderId) === rid && e.type === 'released' && e.data && e.data.seal === 'releaseHold' && +e.data.sealAt >= t0 - 1000), 60000, `the released step of ${rid}`).catch(() => null);
    await until(() => page.evaluate(r => Orders.rows().filter(x => String(x.order.receiptId) === r).every(x => !Orders.heldNow(x)) && !CharmNestReviewMark.isMarked(r), rid), 30000, `${rid} no longer held`).catch(() => null);
    await W.settle(page, 1500);
    const after = await page.evaluate(r => ({ held: Orders.rows().filter(x => String(x.order.receiptId) === r).filter(x => Orders.heldNow(x)).length, marked: CharmNestReviewMark.isMarked(r), seals: (OrderWin.seals() || []).map(s => s.kind) }), rid);
    return { ev, after, rows: await rows(), ms: Date.now() - t0 };
  };
  const shownOk = (rs, n) => rs.length === n && rs.every(r => r.rel.length === 1 && r.rel[0].t === 'Release Hold' && r.rel[0].on);
  const releasedOk = res => !!res.ev && res.ev.by === 'Test Operator' && res.after.held === 0 && !res.after.marked && res.after.seals.includes('release') && res.rows.every(r => !r.rel.length) && res.rows.some(r => r.btns.includes('Hold Order'));
  const say = res => JSON.stringify({ ev: res.ev && { text: res.ev.text, seal: res.ev.data.seal }, after: res.after, rows: res.rows.map(r => r.btns), s: Math.round(res.ms / 100) / 10 });
  try {
    await W.addOrders(w, ['alone', 'matched', 'two', 'half']);
    await W.seedSheet(w, { id: 'sh-ow1', orders: ['sheeted', 'mate'] });

    // a · Hold Order on a piece on a sheet
    await standIn(); await holdOrder(W.keyOf('sheeted')); await realEngine();
    await openWin(W.keyOf('sheeted'));
    let r = await rows(); check(shownOk(r, 1), `a · Hold Order (taken off its sheet): its row offers Release Hold (${JSON.stringify(r)})`);
    let res = await press(W.ORDERS.sheeted.rid, 0); check(releasedOk(res), `a · pressed in the window: released with the Release Hold seal, nothing held or in Review, Hold Order back on the row ${say(res)}`);

    // b · Hold Order with nothing on a sheet: the Review mark alone ("Order held by")
    await holdOrder(W.keyOf('alone'));
    await openWin(W.keyOf('alone'));
    r = await rows(); check(shownOk(r, 1), `b · Hold Order with nothing on a sheet (the Review mark alone): Release Hold (${JSON.stringify(r)})`);
    res = await press(W.ORDERS.alone.rid, 0); check(releasedOk(res), `b · pressed in the window: released ${say(res)}`);

    // c · the sheet window's "Take off and hold" (or the Library's Hold): the line held, no Review card of its own
    await sheetHold([W.keyOf('matched')], 'Test Operator');
    await openWin(W.keyOf('matched'));
    r = await rows(); check(shownOk(r, 1), `c · held from the sheet window / Library (no Review card): Release Hold, where it showed only "In Review" before (${JSON.stringify(r)})`);
    res = await press(W.ORDERS.matched.rid, 0); check(releasedOk(res), `c · pressed in the window: released ${say(res)}`);

    // d · a hold made on another computer: only the cloud's pool row says so
    await cloudHold('mate', 'Other Person');
    await openWin(W.keyOf('mate'));
    r = await rows(); check(shownOk(r, 1), `d · held on another computer (the cloud's record only): Release Hold (${JSON.stringify(r)})`);
    res = await press(W.ORDERS.mate.rid, 0); check(releasedOk(res), `d · pressed in the window: released ${say(res)}`);

    // e · an order of two pieces held by Hold Order: each piece's row; one press releases the order
    const twoKeys = await keysOf(W.ORDERS.two.rid);
    await standIn(); await holdOrder(twoKeys[0]); await realEngine();
    await openWin(twoKeys[0]);
    r = await rows(); check(shownOk(r, 2), `e · two pieces held: Release Hold on each piece's row (${JSON.stringify(r)})`);
    res = await press(W.ORDERS.two.rid, 1); check(releasedOk(res), `e · one press (the second row) releases the whole order ${say(res)}`);

    // f · two pieces, ONE held (the sheet window's "Only this charm"): Release Hold on that row only
    const halfKeys = await keysOf(W.ORDERS.half.rid);
    await sheetHold([halfKeys[1]], 'Test Operator');
    await openWin(halfKeys[0]);
    r = await rows();
    const heldRow = r.findIndex(x => x.key === halfKeys[1]), freeRow = r.findIndex(x => x.key === halfKeys[0]);
    check(r.length === 2 && heldRow >= 0 && freeRow >= 0 && shownOk([r[heldRow]], 1) && r[freeRow].rel.length === 0, `f · one piece of two held: Release Hold on the held piece's row only (${JSON.stringify(r)})`);
    res = await press(W.ORDERS.half.rid, heldRow); check(releasedOk(res), `f · pressed: released ${say(res)}`);

    check(w.errors.length === 0, 'no script error on the page: ' + (w.errors || []).join(' | '));
  } catch (e) { fails.push('page: ' + (e && e.message || e)); console.log('  FAIL page: ' + (e && e.stack || e)); }
  finally { await w.close(); }
  done();
})();
function done() { console.log(`\norder-window-release: ${passed} passed, ${fails.length} failed`); if (fails.length) process.exitCode = 1; }
