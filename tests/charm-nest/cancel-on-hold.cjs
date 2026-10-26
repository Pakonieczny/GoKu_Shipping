// Cancel Order on the Review card of an order on hold (CANCELREVIEW, Paul 20 Oct 2026: "Relocate the 'Cancel Order' button to be only visible as
// part of the orders that are on Hold inside the Review list order view. Every order on Hold in the Review needs a 'Cancel Order' button. When
// pressed that particular order needs to animate and fly towards the Orders tab and then go into the Cancelled orders sub tab, and this needs to be
// easily visible by the user to understand where the order ended up."). Before: Paul, 5 Oct 2026, the On hold card's Cancel Order; NOHOLDPILE put it
// on the held card in Open Orders; it is in Review now, and nowhere in Orders.
// The sorter runs in headless Chromium against the local fake site (bridge-server.cjs), whose charmNestLibrary is the REAL handler over an in-memory
// Firestore: cancelPut, cancelRestore, cancelList and the order timeline are the production code, so "the cancel op was called once with the right
// order", the record kept under Charm_Nest_Cancelled and the timeline are read from the server's own state. Every request that is not to the
// loopback is aborted; nothing here touches a real order.
//   node tests/charm-nest/cancel-on-hold.cjs [playwright-core dir]    (SHOTS=<dir> also saves screenshots)
// Checks: the button is on the Review card of every order on hold, one per card (an order of two lines: one card, one button), right after the
// card's Release Hold with its height and corners, plain (not orange); none on the card of an order that is not held; none anywhere in the Orders
// tab. A failed save keeps the card and says so. A press asks nothing when a name is saved, shows its spinner, cancels once through the existing
// path, and the card flies from Review to the Orders tab in the top bar (a ring on the tab, a "Cancelled N" tag that rolls up as it lands, only
// transform and opacity animating); the card leaves Review and Review's counts drop; the note says "Order N cancelled — it's in Orders › Cancelled"
// with Undo and Show, and Show opens Orders on Cancelled at that order. The timeline keeps its earlier steps and adds the cancel. Undo puts the
// order back on hold, its card back in Review (flying back out of the Orders tab). Reduced motion: no flight, the note still shows. No name saved:
// the inline bar asks for the Employee Number (EMPNUM, Paul 26 Oct 2026: never a typed name), never a pop-up. Nothing is left on screen.
// (SEALCANCEL, Paul 20 Oct 2026) The press stamps its Cancel Order with the ORDER CANCELLED seal (who and when, the Cancelled Orders ink) before the card
// flies; the seal is kept on the order's Review document as cancel { at, by, how: "cancel", id }, as Release Hold keeps release; Undo marks it undone (it
// no longer shows) and keeps the order's other seals; a failed cancel leaves no seal.
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, pinOf } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const order = (rid, buyer, lines, extra) => Object.assign({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }, extra || {});
const line = (tid, sku, title, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }, extra || {});
const A = '4174601819', B2 = '4174601820', C = '4174601821', D = '4174601822', E = '4174601823', F = '4174601824', G = '4174601825';
const HOLD = 'Taken off SS Sheet 1, GF Sheet 2 by Paul: Add to next sheet';
const ORDERS = [
  order(A, 'Dana Duck', [line(41746018191, 'HUGGIE_DUCK', 'HUGGIE HOOPS- RUBBER DUCK(SHAPE)')], { createTs: SHIP - 2 * DAY }),
  order(B2, 'Bea Two', [line(41746018201, 'STAR_3', 'Tiny Star Charm'), line(41746018202, 'HEART_9', 'Heart Charm')], { createTs: SHIP - 3 * DAY }),
  order(C, 'Cy Open', [line(41746018211, 'MOON_12', 'Crescent Moon Charm')], { createTs: SHIP - 4 * DAY }),
  order(D, 'Di Reduced', [line(41746018221, 'LEAF_2', 'Leaf Charm')], { createTs: SHIP - 5 * DAY }),
  order(E, 'Ed Name', [line(41746018231, 'SEA_TURTLE', 'Sea Turtle Charm')], { createTs: SHIP - 6 * DAY }),
  order(F, 'Fay Fail', [line(41746018241, 'BUNNY5', 'Bunny Charm')], { createTs: SHIP - 7 * DAY }),
  order(G, 'Gus OnlyHeld', [line(41746018251, 'ROSE_4', 'Rose Charm')], { createTs: SHIP - 8 * DAY })
];
const HELD = [A, B2, D, E, F, G];
// (REVIEWHELD, Paul 20 Oct 2026: G is only on hold: nobody sent it to Review and it asks nothing, so its one card in Review is its own "Held order")
const ONLYHELD = [G];
const results = [];
const ONLY = (process.env.ONLY || '').split(',').filter(Boolean);
const check = async (name, fn) => { if (ONLY.length && !ONLY.includes(name.split(' ')[0])) return; try { await fn(); results.push([name, true]); console.log('  ✓ ' + name); } catch (e) { results.push([name, false]); console.log('  ✗ ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n      ')); } };
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  if (SHOTS) fs.mkdirSync(SHOTS, { recursive: true });
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const LIB = `${sorterOrigin}/.netlify/functions/charmNestLibrary`;
  const post = async body => (await fetch(LIB, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })).json();
  const t0 = Date.now() - 3 * 3600e3;
  for (const rid of HELD) await post({ op: 'timelineAdd', events: [
    { orderId: rid, type: 'arrived', by: 'Etsy', at: t0, id: 'seed-arrived-' + rid },
    { orderId: rid, type: 'placed', by: 'Paul', at: t0 + 60e3, sheet: 'SS Sheet 1', id: 'seed-placed-' + rid, text: 'Placed on SS Sheet 1' },
    { orderId: rid, type: 'removed', by: 'Paul', at: t0 + 120e3, sheet: 'SS Sheet 1', id: 'seed-removed1-' + rid, text: 'Taken off SS Sheet 1' },
    { orderId: rid, type: 'held', by: 'Paul', at: t0 + 122e3, id: 'seed-held-' + rid, text: HOLD + ', on hold' }] });
  const timeline = async rid => (await post({ op: 'timelineGet', orderId: rid })).events || [];
  const calls = (op, rid) => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === op && (!rid || String(c.body.orderId) === String(rid)));

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const pages = [];
  /** A page of the sorter on the fake site with these orders, the held ones on hold, every one of them sent to Review (so each has its Review card), on Review. */
  async function open(o) {
    o = o || {};
    const ctx = await browser.newContext({ viewport: o.viewport || { width: 1440, height: 950 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference' });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(({ who, sandbox }) => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(Object.assign({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' }, sandbox ? { sandbox: 'on' } : {}))); if (who) localStorage.setItem('cn.employee', who); else localStorage.removeItem('cn.employee'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.__prompted = 0; window.__confirmed = 0; window.prompt = () => { window.__prompted++; return 'Popup Person'; }; window.confirm = () => { window.__confirmed++; return true; };
    }, { who: o.who === undefined ? 'Tester' : o.who, sandbox: !!o.sandbox });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')); });
    await page.goto(`${sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Cancelled && window.SheetWin && window.CancelUI && window.Motion && window.CharmNestReviewMark && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, held, hold, onlyHeld }) => {
      await Orders.loadMaps(true); await Cancelled.load(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      for (const row of B.orders.rows) if (held.includes(row.order.receiptId)) { row.hold = hold; row.reason = row.hold; row.state = 'held'; row.heldAt = Date.now() - 3600e3; }
      // (Send to Review: each order has its "Marked" card in Review, held or not; the cancelled ones lose theirs with their lines)
      for (const order of orders) if (!onlyHeld.includes(order.receiptId)) await CharmNestReviewMark.mark(order.receiptId, { by: 'Paul' });
      Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: o.orders || ORDERS, held: o.held || HELD, hold: HOLD, onlyHeld: ONLYHELD });
    await page.waitForFunction(n => document.querySelectorAll('#rvList > [data-rid] .cnCancelBtn').length >= n, (o.held || HELD).length);
    page.errors = errors; page.ctx = ctx; pages.push(page);
    return page;
  }
  const LIBRX = /\/\.netlify\/functions\/charmNestLibrary/;
  const routeLib = async (page, fn) => { const h = r => { let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {} return fn(r, b); }; await page.route(LIBRX, h); return () => page.unroute(LIBRX, h); };
  const shot = async (page, name) => { if (SHOTS) { await page.waitForTimeout(350); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
  const rvSel = rid => `#rvList > [data-rid="${rid}"]`;
  const rvCards = (page, rid) => page.evaluate(r => document.querySelectorAll(`#rvList > [data-rid="${r}"]`).length, rid);
  const press = (p, rid) => p.click(rvSel(rid) + ' .cnCancelBtn');
  const gone = (p, rid, ms) => p.waitForFunction(r => !document.querySelector(`#rvList > [data-rid="${r}"]`), rid, { timeout: ms || 15000 });
  const sampler = page => page.evaluate(() => {
    const S = window.__cx = { t0: performance.now(), s: [], props: new Set(), anims: 0, run: true };
    const OK = new Set(['transform', 'opacity']);
    const tick = () => {
      const tab = document.querySelector('#modeSeg [data-mode="orders"]'), tr = tab.getBoundingClientRect(), tag = document.querySelector('.cnTabTag');
      const ghosts = [...document.querySelectorAll('#motionLayer .mGhost')].map(g => { const r = g.getBoundingClientRect(); return { key: (g.querySelector('.mCopy') || {}).dataset?.mkey || '', x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, h: r.height }; });
      S.s.push({ t: Math.round(performance.now() - S.t0), ghosts, beacons: document.querySelectorAll('.cnBeacon').length, tag: tag ? tag.textContent.replace(/\s+/g, ' ').trim() : null,
        notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent.replace(/×$/, '').trim()), tab: { x: tr.left + tr.width / 2, y: tr.top + tr.height / 2 } });
      for (const a of document.getAnimations()) {
        const t = a.effect && a.effect.target; if (!t || !t.closest) continue;
        if (!t.closest('#motionLayer, .cnBeacon, .cnTick, .cnTabTag, .mNote')) continue;
        if (t.closest('.seal, .sealTool, .sealRing')) continue;   // (SEALCANCEL: the shared stamp's own ink, wet then dry, as every seal is pressed: charm-nest-motion.js)
        S.anims++;
        for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) S.props.add(OK.has(p) ? p : p + '@' + (t.className || t.tagName));
      }
      if (S.run) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
  const stopSampler = page => page.evaluate(() => { const S = window.__cx; S.run = false; return { s: S.s, props: [...S.props], anims: S.anims }; });
  const settle = page => page.waitForFunction(() => !document.querySelector('.cnBeacon, .cnTick, .cnTabTag') && !(document.getElementById('motionLayer') || { children: [] }).children.length, null, { timeout: 12000 });
  // (SEALCANCEL) the order's Review document, where its order-level seals are kept; the stamp watcher (every frame: a cancel seal on a card copy, its button tinted)
  const rvDoc = rid => st.doc('Charm_Custom_Orders', rid + '_review') || null;
  const until = async (fn, ms) => { const t = Date.now(); while (Date.now() - t < (ms || 8000)) { const v = fn(); if (v) return v; await sleep(100); } return fn(); };
  const sealWatch = page => page.evaluate(() => { const W = window.__sw = { seen: null, run: true }; const tick = () => { const s = document.querySelector('#motionLayer .seal.seal-cancel'); if (s) { const g = s.closest('.mGhost') || s.parentElement, b = g && g.querySelector('.cnCancelBtn'); W.seen = { model: (() => { try { return JSON.parse(s.querySelector('svg').dataset.sealModel); } catch (_) { return null; } })(), title: (() => { try { return Seal.titleOf({ how: 'cancel', by: JSON.parse(s.querySelector('svg').dataset.sealModel).by, at: +s.dataset.at }); } catch (_) { return ''; } })(), fam: (s.querySelector('svg') || { dataset: {} }).dataset.sealFamily, words: s.textContent, btn: !!(b && b.classList.contains('sealedCancel')) || !!(W.seen && W.seen.btn) }; } if (W.run) requestAnimationFrame(tick); }; requestAnimationFrame(tick); });
  const sealSeen = page => page.evaluate(() => { const W = window.__sw; W.run = false; return W.seen; });
  const counts = page => page.evaluate(() => ({ open: Review.openCount(), seg: (document.querySelector('#reviewView .rvSeg [data-cseg="open"] b') || {}).textContent, tab: document.getElementById('tabReviewN').textContent }));

  const page = await open({});

  await check('1 · Review: every held order\'s card has ONE Cancel Order right after its Release Hold, its height and corners, plain; not on an order that is not held; none in Orders', async () => {
    const got = await page.evaluate(({ HELD, C }) => {
      const info = rid => [...document.querySelectorAll(`#rvList > [data-rid="${rid}"]`)].map(n => {
        const c = n.querySelectorAll('.cnCancelBtn'), b = c[0], rel = n.querySelector('[data-rv-release]');
        if (!b) return { n: 0 };
        const rb = rel.getBoundingClientRect(), cb = b.getBoundingClientRect(), cs = getComputedStyle(b), rs = getComputedStyle(rel);
        return { n: c.length, text: b.textContent.trim(), after: rel.nextElementSibling === b, h: [Math.round(rb.height), Math.round(cb.height)], radius: [rs.borderTopLeftRadius, cs.borderTopLeftRadius], bg: cs.backgroundColor, relBg: rs.backgroundColor, cls: b.className };
      });
      return { held: HELD.map(r => [r, info(r)]), c: info(C), orders: document.querySelectorAll('#ordersView .cnCancelBtn').length };
    }, { HELD, C });
    for (const [rid, cards] of got.held) {
      assert.equal(cards.length, 1, `order ${rid}: one Review card (an order of two lines too): ` + JSON.stringify(cards));
      const k = cards[0];
      assert.equal(k.n, 1, `order ${rid}: one Cancel Order`); assert.equal(k.text, 'Cancel Order'); assert(k.after, `order ${rid}: right after Release Hold`);
      assert(Math.abs(k.h[0] - k.h[1]) <= 1, `the same height as Release Hold: ${k.h}`); assert.equal(k.radius[0], k.radius[1], 'the same corners');
      assert.notEqual(k.bg, k.relBg, 'plain, not the orange of Release Hold'); assert(/\bbtn\b/.test(k.cls) && /\bsm\b/.test(k.cls) && /\bghost\b/.test(k.cls), k.cls);
    }
    assert(got.c.length >= 1 && got.c.every(k => k.n === 0), 'an order that is not held: no Cancel Order on its card(s): ' + JSON.stringify(got.c));
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.renderNow(); });
    await page.waitForFunction(() => document.querySelectorAll('#ordItems > [data-rid]').length >= 6);
    assert.equal(await page.evaluate(() => document.querySelectorAll('#ordersView .cnCancelBtn').length), 0, 'no Cancel Order anywhere in the Orders tab (its held cards keep Release Hold)');
    assert(await page.evaluate(A => !![...document.querySelectorAll('#ordItems > [data-rid]')].find(n => n.dataset.rid === A).querySelector('.relHold'), A), 'Release Hold stays on the Orders card');
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    await page.waitForSelector(rvSel(A) + ' .cnCancelBtn');
    await shot(page, '1-review-buttons');
  });

  await check('2 · a failed save keeps the card, says so plainly, and it can be tried again', async () => {
    let tries = 0;
    const unroute = await routeLib(page, (r, b) => { if (b.op === 'cancelPut' && String(b.orderId) === F) { tries++; return r.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'the library is down' }) }); } return r.fallback(); });
    await sampler(page); await sealWatch(page);
    await press(page, F);
    await page.waitForFunction(() => document.querySelector('#rvList .cnCancelMsg'));
    const msg = await page.textContent(rvSel(F) + ' .cnCancelMsg');
    const s = await stopSampler(page);
    assert(/Not cancelled: .*still on hold/.test(msg), msg); assert.equal(tries, 1);
    assert.equal(await rvCards(page, F), 1, 'the card stays'); assert(!s.s.some(x => x.ghosts.length), 'no card flew');
    assert.equal(st.doc('Charm_Nest_Cancelled', F), undefined, 'no record');
    // (SEALCANCEL, RELNOSEAL: the cancel failed, so no seal: none stamped, none kept)
    await page.waitForTimeout(400);
    assert.equal(await sealSeen(page), null, 'no ORDER CANCELLED seal was stamped');
    assert(!(rvDoc(F) || {}).cancel, 'no cancel seal kept on the order: ' + JSON.stringify(rvDoc(F)));
    assert.equal(await page.evaluate(F => CharmNestReviewMark.cancelled(F), F), null);
    assert.equal(calls('reviewMarkPut').filter(c => c.body.action === 'cancel').length, 0, 'no cancel seal write');
    await unroute();
  });

  await check('3 · a press: spinner while kept, no pop-up; the card flies from Review to the Orders tab (ring, "Cancelled N" rolling up), only transform and opacity; it leaves Review and the counts drop; the note says where', async () => {
    await page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
    const unroute = await routeLib(page, async (r, b) => { if (b.op === 'cancelPut') await sleep(800); return r.fallback(); });
    const c0 = await counts(page);
    await sampler(page); await sealWatch(page);
    await press(page, A);
    const waiting = await page.evaluate(A => { const n = document.querySelector(`#rvList > [data-rid="${A}"]`); return { busy: CancelUI.busy(A), lifted: n.classList.contains('cnBusy'), spin: !!n.querySelector('.cnCancelBtn .spin'), name: !!document.querySelector('.cnNameBar') }; }, A);
    assert(waiting.busy && waiting.lifted && waiting.spin, 'kept, the card lifts, the spinner: ' + JSON.stringify(waiting)); assert.equal(waiting.name, false);
    await shot(page, '3-pressed');
    await gone(page, A);
    await page.waitForFunction(() => document.querySelector('.mNote'), null, { timeout: 8000 });
    await page.waitForTimeout(900);
    const s = await stopSampler(page), seal = await sealSeen(page);
    await unroute();
    // (SEALCANCEL: the stamp came down on the card's Cancel Order: ORDER CANCELLED, who, in the Cancelled Orders family, the button tinted)
    assert(seal, 'an ORDER CANCELLED seal was stamped on the card');
    assert.equal(seal.fam, 'cancelled', JSON.stringify(seal)); assert(/ORDER\s*CANCELLED/.test(seal.words), JSON.stringify(seal));
    assert(seal.model && seal.model.by === 'Tester' && seal.model.action === 'ORDER CANCELLED' && +seal.model.at > 0 && seal.model.date && seal.model.time, 'who and when: ' + JSON.stringify(seal.model)); assert(/^Order cancelled with Cancel Order by Tester · /.test(seal.title), seal.title); assert(seal.btn, 'the button takes the seal\'s ink');
    const kept = await until(() => { const d = rvDoc(A); return d && d.cancel && !d.pending ? d : null; });
    assert(kept, 'the seal is kept on the order\'s Review document: ' + JSON.stringify(rvDoc(A)));
    assert.equal(kept.cancel.how, 'cancel'); assert.equal(kept.cancel.by, 'Tester'); assert(+kept.cancel.at > 0 && kept.cancel.id === `cancel:${A}:${kept.cancel.at}`, JSON.stringify(kept.cancel));
    assert.deepEqual(await page.evaluate(A => { const c = CharmNestReviewMark.cancelled(A); return c && [c.how, c.by, Seal.labelOf(c.how), Seal.INK[c.how], CharmNestReviewMark.sealsOf(A).map(x => x.how)]; }, A), ['cancel', 'Tester', 'ORDER CANCELLED', '#982f48', ['cancel']]);
    const puts = calls('cancelPut', A); assert.equal(puts.length, 1, 'cancelPut once'); assert.equal(puts[0].body.by, 'Tester');
    assert.equal(await page.evaluate(() => window.__prompted + window.__confirmed), 0, 'no pop-up and no confirm');
    const rec = st.doc('Charm_Nest_Cancelled', A); assert(rec && rec.by === 'Tester' && rec.orderId === A, 'kept under Charm_Nest_Cancelled');
    assert.equal(await page.evaluate(A => B.orders.rows.filter(r => r.order.receiptId === A).length, A), 0, 'off every list');
    // the flight, to the Orders tab
    const air = s.s.filter(x => x.ghosts.some(g => g.key === 'ord:rvm:' + A));
    assert(air.length >= 20, 'the card was seen flying over many frames: ' + air.length);
    const g0 = air[0].ghosts.find(g => g.key === 'ord:rvm:' + A), gN = air[air.length - 1].ghosts.find(g => g.key === 'ord:rvm:' + A), tab = air[air.length - 1].tab;
    const d0 = Math.hypot(g0.x - tab.x, g0.y - tab.y), dN = Math.hypot(gN.x - tab.x, gN.y - tab.y);
    assert(dN < d0 * 0.35, `the card gets to the Orders tab: ${Math.round(d0)} px away at first, ${Math.round(dN)} at the end`); assert(gN.w < g0.w * 0.6, 'and shrinks into it');
    const ring = s.s.findIndex(x => x.beacons > 0), fly0 = s.s.findIndex(x => x.ghosts.some(g => g.key === 'ord:rvm:' + A && Math.hypot(g.x - g0.x, g.y - g0.y) > 2));   // (SEALCANCEL: after its stamp, the card sets off)
    assert(ring >= 0 && ring <= fly0 + 3, 'a ring on the Orders tab as the card sets off');
    const tags = [...new Set(s.s.map(x => x.tag).filter(Boolean))];
    assert.equal(tags[0], 'Cancelled 0', 'the tab\'s tag shows the old count while it flies: ' + JSON.stringify(tags)); assert(tags.includes('Cancelled 1'), 'and rolls to the new one: ' + JSON.stringify(tags));
    assert(s.anims > 5 && s.props.every(p => ['transform', 'opacity'].includes(p)), 'only transform and opacity animate: ' + JSON.stringify(s.props));
    const note = await page.evaluate(() => { const n = document.querySelector('.mNote'); return n ? { text: n.querySelector('.mNoteT').textContent, btns: [...n.querySelectorAll('.mNoteBtn')].map(b => b.textContent) } : null; });
    assert(note && note.text === 'Order 4174601819 cancelled — it\'s in Orders › Cancelled', 'the note says where: ' + JSON.stringify(note)); assert.deepEqual(note.btns, ['Undo', 'Show']);
    await page.waitForFunction(o => Review.openCount() === o - 1 && document.getElementById('tabReviewN').textContent === String(o - 1), c0.open, { timeout: 5000 }).catch(() => {});
    const c1 = await counts(page);
    assert.equal(c1.open, c0.open - 1, `Review's count drops: ${JSON.stringify(c0)} → ${JSON.stringify(c1)}`); assert.equal(c1.tab, String(c0.open - 1), 'the Review tab says so'); assert.equal(c1.seg, String(c0.open - 1), 'and Open says so');
    await shot(page, '3-note');
  });

  await check('4 · Show opens Orders on Cancelled at that order; the timeline keeps every step and adds the cancel', async () => {
    await page.click('.mNote .mNoteBtn:text("Show")');
    await page.waitForSelector(`#ordBody .cxRow[data-rid="${A}"]`);
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'cancelled' });
    assert(/Cancelled by Tester/.test(await page.textContent(`#ordBody .cxRow[data-rid="${A}"]`)));
    const ev = await timeline(A), types = ev.map(e => e.type);
    for (const t of ['arrived', 'placed', 'removed', 'held']) assert(types.includes(t), `the earlier ${t} step is kept: ` + types);
    const cx = ev.find(e => e.type === 'cancelled'); assert(cx && cx.by === 'Tester', 'the cancel, who and when: ' + JSON.stringify(cx));
    await shot(page, '4-cancelled-tab');
    await page.evaluate(() => { Orders.showPile('', ''); CN.setMode('review'); Review.render(); });
    await page.waitForSelector(rvSel(B2) + ' .cnCancelBtn');
  });

  await check('5 · nothing is left on screen once it has played', async () => {
    await settle(page);
    const left = await page.evaluate(() => ({ ghosts: document.querySelectorAll('.mGhost, .mLift').length, beacons: document.querySelectorAll('.cnBeacon').length, tags: document.querySelectorAll('.cnTabTag').length, busy: document.querySelectorAll('.cnBusy').length, layer: (document.getElementById('motionLayer') || { children: [] }).children.length }));
    assert.equal(left.ghosts + left.beacons + left.tags + left.busy + left.layer, 0, JSON.stringify(left));
  });

  await check('6 · an order of two lines: one card flies, one cancel, one note; Undo on that note puts it back on hold and its card flies back into Review', async () => {
    await page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
    const c0 = await counts(page);
    // (SEALCANCEL: the order keeps another seal on the same document, a Release Hold, which the Undo of the cancel must not take)
    await page.evaluate(B2 => CharmNestReviewMark.release(B2, { by: 'Paul', at: Date.now() - 60000 }), B2);
    await sampler(page);
    await press(page, B2);
    await gone(page, B2);
    await page.waitForFunction(() => [...document.querySelectorAll('.mNote')].some(n => /Order 4174601820 cancelled/.test(n.textContent)), null, { timeout: 8000 });
    await page.waitForTimeout(700);
    let s = await stopSampler(page);
    assert.equal(calls('cancelPut', B2).length, 1, 'one cancel for the order');
    assert(await until(() => (rvDoc(B2) || {}).cancel), 'the cancel seal is kept'); assert.equal(st.doc('Charm_Nest_Cancelled', B2).lines.length, 2, 'the record keeps both lines');
    assert.equal(Math.max(0, ...s.s.map(x => x.ghosts.length)), 1, 'ONE card flew');
    assert.equal(new Set(s.s.flatMap(x => x.notes.filter(n => /Order 4174601820 cancelled/.test(n)))).size, 1, 'one note');
    await settle(page);
    await page.evaluate(() => { document.querySelector('#reviewView .egPane.scroll').scrollTop = 0; });   // (an order back from Cancelled is the newest activity: its card comes back at the top of Open, on screen here, so it is seen flying back)
    await sampler(page);
    await page.evaluate(() => { const n = [...document.querySelectorAll('.mNote')].find(x => /Order 4174601820 cancelled/.test(x.textContent)); [...n.querySelectorAll('.mNoteBtn')].find(b => b.textContent === 'Undo').click(); });
    await page.waitForSelector(rvSel(B2) + ' .cnCancelBtn', { timeout: 15000 });
    await page.waitForTimeout(1900);
    s = await stopSampler(page);
    assert.equal(calls('cancelRestore', B2).length, 1, 'cancelRestore once'); assert.equal(st.doc('Charm_Nest_Cancelled', B2), undefined, 'out of Cancelled');
    assert.equal(await page.evaluate(B2 => B.orders.rows.filter(r => r.order.receiptId === B2 && r.state === 'held' && r.hold).length, B2), 2, 'both lines held again');
    assert.equal(await rvCards(page, B2), 1, 'its card is back in Review'); assert.equal((await counts(page)).open, c0.open, 'and Review counts it again');
    const flew = s.s.filter(x => x.ghosts.length); assert(flew.length >= 10, 'the card flew back: ' + flew.length);
    const first = flew[0].ghosts[0], last = flew[flew.length - 1].ghosts[0], tab = flew[0].tab;
    assert(Math.hypot(first.x - tab.x, first.y - tab.y) < Math.hypot(last.x - tab.x, last.y - tab.y), 'out of the Orders tab, back into Review');
    assert(s.props.every(p => ['transform', 'opacity'].includes(p)), 'only transform and opacity: ' + JSON.stringify(s.props));
    assert(s.s.some(x => x.notes.some(n => /Order 4174601820 is back on hold/.test(n))), 'a note says it is back on hold');
    const ev = (await timeline(B2)).map(e => e.type); for (const t of ['held', 'cancelled', 'cancelRestored']) assert(ev.includes(t), `the timeline keeps ${t}: ` + ev);
    // (SEALCANCEL: Undo marks the seal undone, so it no longer shows; the order's Release Hold seal stays)
    const un = await until(() => { const d = rvDoc(B2); return d && d.cancel && +d.cancel.undoneAt > 0 ? d : null; });
    assert(un, 'the cancel seal is marked undone: ' + JSON.stringify(rvDoc(B2)));
    assert.equal(un.cancel.how, 'cancel'); assert(un.release && un.release.by === 'Paul', 'the Release Hold seal stays: ' + JSON.stringify(un));
    assert.deepEqual(await page.evaluate(B2 => [CharmNestReviewMark.cancelled(B2), CharmNestReviewMark.sealsOf(B2).map(x => x.how)], B2), [null, ['releaseHold']]);
    await shot(page, '6-undone');
  });

  await check('6b · REVIEWHELD: an order only on hold has its own Held order card in Open (Release Hold, Cancel Order, counted, its Orders card says In Review); Cancel flies it to Orders › Cancelled', async () => {
    await settle(page); await page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
    const card = await page.evaluate(G => { const n = [...document.querySelectorAll('#rvList > [data-rid]')].filter(x => x.dataset.rid === G); const b = n[0] && n[0].querySelector('[data-rv-release]'); return { n: n.length, mk: n[0] && n[0].dataset.mkey, rel: b && b.textContent.trim(), relCls: b && b.className, bg: b && getComputedStyle(b).backgroundColor, cancel: n[0] ? [...n[0].querySelectorAll('.cnCancelBtn')].map(x => x.textContent.trim()) : [], after: !!(b && b.nextElementSibling && b.nextElementSibling.classList.contains('cnCancelBtn')), open: Review.openOrders().has(G), chip: !!document.querySelector('#reviewView [data-k="heldOrder"]') }; }, G);
    assert.equal(card.n, 1, 'one card: ' + JSON.stringify(card)); assert.equal(card.mk, 'held:' + G, 'its own Held order');
    assert.equal(card.rel, 'Release Hold'); assert(/\brelHoldBtn\b/.test(card.relCls) && card.bg === 'rgb(162, 89, 28)', 'the orange Release Hold: ' + JSON.stringify(card));
    assert.deepEqual(card.cancel, ['Cancel Order']); assert(card.after, 'Cancel Order right after it'); assert(card.open, 'Open has it (openOrders)'); assert(card.chip, 'the Held chip is there');
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.renderNow(); });
    await page.waitForFunction(G => { const n = [...document.querySelectorAll('#ordItems > [data-rid]')].find(x => x.dataset.rid === G); const b = n && n.querySelector('.revSendBtn'); return b && /In Review/.test(b.textContent); }, G, { timeout: 8000 });
    await page.evaluate(G => { CN.setMode('review'); Review.showHeld(G); }, G);
    await page.waitForFunction(G => { const n = document.querySelector(`#rvList > [data-rid="${G}"]`); return n && n.classList.contains('mFound'); }, G, { timeout: 8000 });
    await page.evaluate(() => { Review.view().filter = null; Review.render(); });
    await page.waitForSelector(rvSel(G) + ' .cnCancelBtn');
    const c0 = await counts(page);
    await sampler(page);
    await press(page, G);
    await gone(page, G);
    await page.waitForFunction(G => [...document.querySelectorAll('.mNote')].some(n => n.textContent.includes(`Order ${G} cancelled \u2014 it's in Orders \u203a Cancelled`)), G, { timeout: 8000 });
    await page.waitForTimeout(700);
    const s = await stopSampler(page);
    assert.equal(calls('cancelPut', G).length, 1); assert(st.doc('Charm_Nest_Cancelled', G), 'kept under Cancelled');
    const air = s.s.filter(x => x.ghosts.some(g => g.key === 'held:' + G)); assert(air.length >= 15, 'its card flew: ' + air.length);
    const g0 = air[0].ghosts.find(g => g.key === 'held:' + G), gN = air[air.length - 1].ghosts.find(g => g.key === 'held:' + G), tab = air[air.length - 1].tab;
    assert(Math.hypot(gN.x - tab.x, gN.y - tab.y) < Math.hypot(g0.x - tab.x, g0.y - tab.y) * 0.35, 'to the Orders tab');
    assert(s.props.every(p => ['transform', 'opacity'].includes(p)), JSON.stringify(s.props));
    await page.waitForFunction(o => Review.openCount() === o - 1, c0.open, { timeout: 5000 }).catch(() => {});
    assert.equal((await counts(page)).open, c0.open - 1, 'Open counts one fewer'); assert.equal(await page.evaluate(G => Review.openOrders().has(G), G), false);
    await shot(page, '6b-only-held');
  });

  await check('7 · reduced motion: no flight, no ring, no tag; the note still says where', async () => {
    const r = await open({ reduced: true, who: 'Tester', held: [D] });
    await r.evaluate(() => { window.__fly = 0; new MutationObserver(() => { if (document.querySelector('#motionLayer .mGhost, .cnBeacon, .cnTabTag')) window.__fly++; }).observe(document.body, { childList: true, subtree: true }); });
    await press(r, D);
    await gone(r, D);
    await r.waitForSelector('.mNote');
    const got = await r.evaluate(() => ({ fly: window.__fly, note: document.querySelector('.mNote .mNoteT').textContent, btns: [...document.querySelectorAll('.mNote .mNoteBtn')].map(b => b.textContent) }));
    assert.equal(got.fly, 0, 'nothing flew'); assert(/Order 4174601822 cancelled — it's in Orders › Cancelled/.test(got.note), got.note); assert.deepEqual(got.btns, ['Undo', 'Show']);
    assert.equal(calls('cancelPut', D).length, 1);
    await r.close();
  });

  await check('8 · no name saved: the small inline bar asks, never a pop-up; put away it does nothing; named, it goes on', async () => {
    const n = await open({ who: '', held: [E] });
    await press(n, E);
    await n.waitForSelector('.cnNameBar');
    assert.equal(await n.evaluate(() => window.__prompted), 0, 'no browser pop-up'); assert.equal(calls('cancelPut', E).length, 0);
    await n.click('.cnNameBar .cnNbX');
    await n.waitForFunction(() => !document.querySelector('.cnNameBar'));
    await n.waitForTimeout(300); assert.equal(calls('cancelPut', E).length, 0, 'put away: nothing happened'); assert.equal(await rvCards(n, E), 1);
    await press(n, E);
    // (EMPNUM, Paul 26 Oct 2026: the bar asks for the person's 6-digit Employee Number, masked, and the server names them; a typed name signs nobody in)
    await n.waitForSelector('.cnNameBar input');
    assert.deepEqual(await n.evaluate(() => { const b = document.querySelector('.cnNameBar'), i = b.querySelector('input'); return { num: b.dataset.num, type: i.type, value: i.value, cont: !!(b.querySelector('.cnNbCont') && !b.querySelector('.cnNbCont').hidden) }; }), { num: '1', type: 'password', value: '', cont: false }, 'the bar asks for the Employee Number (masked, empty, no one-tap "Continue as")');
    await n.fill('.cnNameBar input', pinOf(st, 'Pat Smith')); await n.press('.cnNameBar input', 'Enter');
    await gone(n, E);
    const put = calls('cancelPut', E); assert.equal(put.length, 1); assert.equal(put[0].body.by, 'Pat Smith');
    await n.close();
  });

  for (const p of pages) assert.deepEqual(p.errors, [], 'no page errors: ' + p.errors.join(' | '));
  await browser.close(); srv.close();
  const bad = results.filter(r => !r[1]);
  console.log(bad.length ? `\n${bad.length} of ${results.length} checks FAILED` : `\nall ${results.length} checks passed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
