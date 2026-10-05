// Cancel Order on an On hold card (Paul, 5 Oct 2026, Image #3: "Add an extra button here 'Cancel Order' that will cancel the
// order and move it to cancelled orders tab. Also add the same style beautiful animation showing the user where the order
// is moving to."). The sorter runs in headless Chromium against the local fake site (bridge-server.cjs), whose
// charmNestLibrary is the REAL handler over an in-memory Firestore: cancelPut, cancelRestore, cancelList and the order
// timeline are the production code, so "the cancel op was called once with the right order", the record kept under
// Charm_Nest_Cancelled and the timeline are read from the server's own state. Every request that is not to the loopback
// is aborted; nothing here touches a real order.
//   node tests/charm-nest/cancel-on-hold.cjs [playwright-core dir]    (SHOTS=<dir> also saves screenshots and mid-flight frames)
// Checks: the button shows only on On hold cards (not in Open Orders, not on an order that is not held), beside Release hold,
// the same height and look; 900 and 390 px layouts; a press asks no name when one is saved, and no confirm and no pop-up;
// the spinner while it is kept; cancelPut once with the order and the person; the card leaves On hold by a flight to the
// Cancelled chip (a ring on the chip first, the count ticking up when it lands, only transform and opacity animating, the
// note "Order N cancelled" with Undo and Show); the order is under Cancelled, the timeline keeps its earlier steps and
// adds the cancel with who and when; Undo restores it (cancelRestore once), the card flies back into On hold and the
// timeline keeps both; a failed save keeps the card and says so plainly; an order of two lines flies as one with one note;
// reduced motion has no flight and still the note; no name saved asks in the inline bar; nothing is left on screen.
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const order = (rid, buyer, lines, extra) => Object.assign({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }, extra || {});
const line = (tid, sku, title, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }, extra || {});
const A = '4174601819', B2 = '4174601820', C = '4174601821', D = '4174601822', E = '4174601823', F = '4174601824', G = '4174601825';
const HOLD = 'Taken off SS Sheet 1, GF Sheet 2 by Paul: Add to next sheet';
const ORDERS = [
  order(A, 'Dana Duck', [line(41746018191, 'HUGGIE_DUCK', 'HUGGIE HOOPS- RUBBER DUCK(SHAPE)', { personalization: ['I ordered silver duck studs and would like to have that same duck charm in gold on these earrings please. Thank you!'] })], { createTs: SHIP - 2 * DAY }),
  order(B2, 'Bea Two', [line(41746018201, 'STAR_3', 'Tiny Star Charm'), line(41746018202, 'HEART_9', 'Heart Charm')], { createTs: SHIP - 3 * DAY }),
  order(C, 'Cy Open', [line(41746018211, 'MOON_12', 'Crescent Moon Charm')], { createTs: SHIP - 4 * DAY }),
  order(D, 'Di Reduced', [line(41746018221, 'LEAF_2', 'Leaf Charm')], { createTs: SHIP - 5 * DAY }),
  order(E, 'Ed Name', [line(41746018231, 'SEA_TURTLE', 'Sea Turtle Charm')], { createTs: SHIP - 6 * DAY }),
  order(F, 'Fay Fail', [line(41746018241, 'BUNNY5', 'Bunny Charm')], { createTs: SHIP - 7 * DAY }),
  order(G, 'Gus Frames', [line(41746018251, 'ROSE_4', 'Rose Charm')], { createTs: SHIP - 8 * DAY })
];
const HELD = [A, B2, D, E, F, G];
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
  // the earlier steps on the order's timeline, as the sheet window's Hold wrote them (kept for good, whatever happens next)
  const t0 = Date.now() - 3 * 3600e3;
  for (const rid of HELD) await post({ op: 'timelineAdd', events: [
    { orderId: rid, type: 'arrived', by: 'Etsy', at: t0, id: 'seed-arrived-' + rid },
    { orderId: rid, type: 'placed', by: 'Paul', at: t0 + 60e3, sheet: 'SS Sheet 1', id: 'seed-placed-' + rid, text: 'Placed on SS Sheet 1' },
    { orderId: rid, type: 'removed', by: 'Paul', at: t0 + 120e3, sheet: 'SS Sheet 1', id: 'seed-removed1-' + rid, text: 'Taken off SS Sheet 1' },
    { orderId: rid, type: 'removed', by: 'Paul', at: t0 + 121e3, sheet: 'GF Sheet 2', id: 'seed-removed2-' + rid, text: 'Taken off GF Sheet 2' },
    { orderId: rid, type: 'held', by: 'Paul', at: t0 + 122e3, id: 'seed-held-' + rid, text: HOLD + ', on hold' }] });
  const timeline = async rid => (await post({ op: 'timelineGet', orderId: rid })).events || [];
  const calls = (op, rid) => st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === op && (!rid || String(c.body.orderId) === String(rid)));

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const pages = [];
  /** A page of the sorter on the fake site with these orders (the held ones on hold), the person already named or not. */
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
    await page.waitForFunction(() => window.CN && window.Orders && window.Cancelled && window.SheetWin && window.CancelUI && window.Motion && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, held }) => {
      await Orders.loadMaps(true); await Cancelled.load(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: order.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems();
      for (const row of B.orders.rows) if (held.includes(row.order.receiptId)) { row.hold = 'Taken off SS Sheet 1, GF Sheet 2 by Paul: Add to next sheet'; row.reason = row.hold; row.state = 'held'; row.heldAt = Date.now() - 3600e3; }
      CN.setMode('orders'); Orders.showPile('hold', ''); Orders.renderNow();
    }, { orders: o.orders || ORDERS, held: o.held || HELD });
    await page.waitForFunction(() => document.querySelectorAll('#ordItems [data-rid]').length > 0);
    page.errors = errors; page.ctx = ctx; pages.push(page);
    return page;
  }
  const LIBRX = /\/\.netlify\/functions\/charmNestLibrary/;
  /** Answers the page's calls to the library its own way for a while (r.fallback() lets one through to the fake site). */
  const routeLib = async (page, fn) => { const h = r => { let b = {}; try { b = r.request().postDataJSON() || {}; } catch (_) {} return fn(r, b); }; await page.route(LIBRX, h); return () => page.unroute(LIBRX, h); };
  const shot = async (page, name) => { if (SHOTS) { await page.waitForTimeout(350); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
  const cardsOf = (page, rid) => page.evaluate(r => [...document.querySelectorAll('#ordItems [data-rid]')].filter(n => n.dataset.rid === r).length, rid);
  const chipN = (page, pile) => page.evaluate(p => { const b = document.querySelector(`#ordChips [data-pile="${p}"] b`); return b ? b.textContent : null; }, pile);
  const sampler = page => page.evaluate(() => {
    const S = window.__cx = { t0: performance.now(), s: [], props: new Set(), anims: 0, run: true };
    const OK = new Set(['transform', 'opacity']);
    const tick = () => {
      const chip = document.querySelector('#ordChips [data-pile="cancelled"]'), hold = document.querySelector('#ordChips [data-pile="hold"]');
      const ghosts = [...document.querySelectorAll('#motionLayer .mGhost')].map(g => { const r = g.getBoundingClientRect(); return { key: (g.querySelector('.mCopy') || {}).dataset?.key || '', x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width, h: r.height }; });
      const cr = chip ? chip.getBoundingClientRect() : null;
      S.s.push({ t: Math.round(performance.now() - S.t0), ghosts, beacons: document.querySelectorAll('.cnBeacon').length, tick: document.querySelectorAll('.cnTick').length, cx: chip && chip.querySelector('b') ? chip.querySelector('b').textContent : null, hold: hold && hold.querySelector('b') ? hold.querySelector('b').textContent : null,
        notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent.replace(/×$/, '').trim()), chip: cr ? { x: cr.left + cr.width / 2, y: cr.top + cr.height / 2, w: cr.width } : null });
      for (const a of document.getAnimations()) {
        const t = a.effect && a.effect.target; if (!t || !t.closest) continue;
        if (!t.closest('#motionLayer, .cnBeacon, .cnTick, .mNote')) continue;
        S.anims++;
        for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) S.props.add(OK.has(p) ? p : p + '@' + (t.className || t.tagName));
      }
      if (S.run) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
  const stopSampler = page => page.evaluate(() => { const S = window.__cx; S.run = false; return { s: S.s, props: [...S.props], anims: S.anims }; });
  const settle = page => page.waitForFunction(() => !document.querySelector('.cnBeacon, .cnTick') && !(document.getElementById('motionLayer') || { children: [] }).children.length, null, { timeout: 12000 });

  // ───────────────────────── the page, the cards ─────────────────────────
  const page = await open({});
  const sel = rid => `#ordItems [data-rid="${rid}"]`;

  await check('1 · the button is on the held cards of On hold, beside Release hold, and nowhere else', async () => {
    const got = await page.evaluate(({ A, B2, C }) => {
      const info = rid => [...document.querySelectorAll(`#ordItems [data-rid="${rid}"]`)].map(n => ({ cancel: [...n.querySelectorAll('.cnCancelBtn')].map(b => b.textContent.trim()), rel: n.querySelectorAll('.relHold:not([data-gate])').length, together: !!n.querySelector('.cnHoldBtns .cnCancelBtn') && !!n.querySelector('.cnHoldBtns .relHold') }));
      return { pile: Orders.view().pile, a: info(A), b: info(B2), c: info(C) };
    }, { A, B2, C });
    assert.equal(got.pile, 'hold');
    assert.equal(got.a.length, 1); assert.deepEqual(got.a[0].cancel, ['Cancel Order']); assert.equal(got.a[0].rel, 1); assert(got.a[0].together, 'the two buttons share one row');
    assert.equal(got.b.length, 2, 'two lines, two cards'); for (const c of got.b) assert.deepEqual(c.cancel, ['Cancel Order']);
    assert.equal(got.c.length, 0, 'an order that is not on hold is not in On hold');
    await page.click('#ordChips [data-pile=""]');
    await page.waitForFunction(() => Orders.view().pile === null && document.querySelectorAll('#ordItems [data-rid]').length >= 4);
    const open = await page.evaluate(() => ({ cancel: document.querySelectorAll('#ordItems .cnCancelBtn').length, rel: document.querySelectorAll('#ordItems .relHold:not([data-gate])').length, cards: document.querySelectorAll('#ordItems [data-rid]').length }));
    assert.equal(open.cancel, 0, 'Open Orders shows held orders with Release hold only: ' + JSON.stringify(open)); assert(open.rel >= 3);
    await page.click('#ordChips [data-pile="hold"]');
    await page.waitForFunction(() => Orders.view().pile === 'hold' && document.querySelectorAll('#ordItems .cnCancelBtn').length === 7);
  });

  await check('2 · same height and calm look as Release hold (plain, not orange, not red); fits 900 and 390 px', async () => {
    const look = () => page.evaluate(A => {
      const n = document.querySelector(`#ordItems [data-rid="${A}"]`), c = n.querySelector('.cnCancelBtn'), r = n.querySelector('.relHold'), cs = getComputedStyle(c), rs = getComputedStyle(r);
      const cr = c.getBoundingClientRect(), rr = r.getBoundingClientRect(), nr = n.getBoundingClientRect();
      return { ch: Math.round(cr.height * 10) / 10, rh: Math.round(rr.height * 10) / 10, fs: [cs.fontSize, rs.fontSize], radius: [cs.borderRadius, rs.borderRadius], bg: [cs.backgroundColor, rs.backgroundColor], border: [cs.borderTopColor, rs.borderTopColor], color: cs.color,
        inside: cr.left >= nr.left - 1 && cr.right <= nr.right + 1 && rr.left >= nr.left - 1 && rr.right <= nr.right + 1, overflowX: document.documentElement.scrollWidth > document.documentElement.clientWidth + 1 || n.scrollWidth > n.clientWidth + 1, lines: c.getClientRects().length, label: c.textContent.trim(), wide: Math.round(cr.width) };
    }, A);
    for (const [w, h, name] of [[1440, 950, 'wide'], [900, 900, '900'], [390, 844, '390']]) {
      await page.setViewportSize({ width: w, height: h });
      await page.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w);   // (the rail folds on a phone, as a person folds it)
      await page.waitForTimeout(400);
      const L = await look();
      await shot(page, `card-${name}`);
      assert.equal(L.ch, L.rh, `${w}px: the two buttons are the same height ${JSON.stringify(L)}`);
      assert.equal(L.fs[0], L.fs[1], 'same type size'); assert.equal(L.radius[0], L.radius[1], 'same corners'); assert.equal(L.bg[0], L.bg[1], 'same plain background'); assert.equal(L.border[0], L.border[1], 'same border');
      assert(L.inside, `${w}px: both buttons inside the card ${JSON.stringify(L)}`); assert.equal(L.overflowX, false, `${w}px: no sideways scroll`);
      const rgb = /rgba?\((\d+),\s*(\d+),\s*(\d+)/.exec(L.color).slice(1).map(Number), spread = Math.max(...rgb) - Math.min(...rgb);
      assert(spread < 60, 'the label is a calm neutral, not orange or red: ' + L.color);
      assert.equal(L.label, 'Cancel Order');
    }
    await page.setViewportSize({ width: 1440, height: 950 });
    await page.evaluate(() => { if (document.getElementById('app').classList.contains('railOff')) document.getElementById('btnRail').click(); });
    await page.waitForTimeout(400);
  });

  await check('3 · a failed save keeps the card, says so in one plain line, and the button can be pressed again', async () => {
    let tries = 0;
    const unroute = await routeLib(page, (r, b) => { if (b.op === 'cancelPut' && String(b.orderId) === F) { tries++; return tries === 1 ? r.abort('failed') : r.fulfill({ status: 500, contentType: 'application/json', body: JSON.stringify({ error: 'the library is down' }) }); } return r.fallback(); });
    await sampler(page);
    await page.click(`${sel(F)} .cnCancelBtn`);
    await page.waitForSelector(`${sel(F)} .cnCancelMsg`);
    let msg = await page.textContent(`${sel(F)} .cnCancelMsg`);
    assert(/nothing was cancelled/i.test(msg) && /still on hold/i.test(msg), 'plain words, nothing lost: ' + msg);
    assert.equal(await cardsOf(page, F), 1, 'the card stays'); assert.equal(await page.evaluate(() => CancelUI.busy('4174601824')), false);
    assert.equal(await page.locator(`${sel(F)} .cnCancelBtn`).isDisabled(), false, 'the button is back');
    assert.equal(await page.locator(`${sel(F)} .relHold`).isDisabled(), false, 'Release hold is back');
    await page.click(`${sel(F)} .cnCancelBtn`);
    await page.waitForFunction(() => /^Not cancelled:/.test((document.querySelector('#ordItems [data-rid="4174601824"] .cnCancelMsg') || {}).textContent || ''));
    msg = await page.textContent(`${sel(F)} .cnCancelMsg`); assert(/still on hold/i.test(msg), 'again plain, nothing lost: ' + msg);
    const s = await stopSampler(page);
    assert.equal(tries, 2); assert.equal(calls('cancelPut', F).length, 0, 'the server never saw a cancel');
    assert(!s.s.some(x => x.ghosts.length), 'no card flew: it never left'); assert(!s.s.some(x => x.notes.some(n => /cancelled/i.test(n))), 'no "cancelled" note');
    assert.equal(await chipN(page, 'cancelled'), '0'); assert.equal(st.doc('Charm_Nest_Cancelled', F), undefined, 'no record');
    assert.equal(await page.evaluate(() => B.orders.rows.filter(r => r.order.receiptId === '4174601824' && r.state === 'held' && r.hold).length), 1, 'the order is still held');
    assert.equal(await page.evaluate(() => window.__prompted + window.__confirmed), 0);
    await shot(page, 'failed-save');
    await unroute();
  });

  await check('4 · a press cancels once through the existing path: spinner while kept, no confirm, no pop-up, then the flight', async () => {
    // (the server answers the cancel after 800 ms, so the waiting state can be read)
    const unroute = await routeLib(page, async (r, b) => { if (b.op === 'cancelPut') await sleep(800); return r.fallback(); });
    const before = { hold: await chipN(page, 'hold'), cx: await chipN(page, 'cancelled') };
    assert.equal(before.cx, '0');
    await sampler(page);
    await page.click(`${sel(A)} .cnCancelBtn`);
    const waiting = await page.evaluate(A => { const n = document.querySelector(`#ordItems [data-rid="${A}"]`), b = n.querySelector('.cnCancelBtn'); return { disabled: b.disabled, text: b.textContent.trim(), spin: !!b.querySelector('.spin'), rel: n.querySelector('.relHold').disabled, lifted: n.classList.contains('cnBusy'), name: !!document.querySelector('.cnNameBar') }; }, A);
    assert(waiting.disabled && waiting.spin && /Cancelling/.test(waiting.text), 'a small labelled spinner: ' + JSON.stringify(waiting));
    assert(waiting.rel, 'Release hold waits too'); assert(waiting.lifted, 'the card lifts'); assert.equal(waiting.name, false, 'the name is saved: nothing is asked');
    await shot(page, 'pressed-spinner');
    await page.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), A, { timeout: 15000 });
    await page.waitForFunction(() => document.querySelector('.mNote'), null, { timeout: 8000 });
    await page.waitForTimeout(700);
    const s = await stopSampler(page);
    await unroute();
    // the op, once, with the right order and the person
    const puts = calls('cancelPut', A); assert.equal(puts.length, 1, 'cancelPut once: ' + puts.length); assert.equal(puts[0].body.by, 'Tester'); assert.equal(calls('cancelPut').length, 1, 'no other order was cancelled');
    assert.equal(await page.evaluate(() => window.__prompted + window.__confirmed), 0, 'no pop-up and no confirm');
    // the record is kept, and the order left every list
    const rec = st.doc('Charm_Nest_Cancelled', A); assert(rec && rec.by === 'Tester' && rec.orderId === A, 'the record is kept under Charm_Nest_Cancelled: ' + JSON.stringify(rec));
    assert.equal(await page.evaluate(A => B.orders.rows.filter(r => r.order.receiptId === A).length, A), 0, 'off every list');
    assert.equal(await page.evaluate(A => Cancelled.has(A), A), true);
    // the flight: a copy of the card in the air, moving to the chip and shrinking into it
    if (process.env.DEBUG_CX) console.log(JSON.stringify(s.s.filter(x => x.ghosts.length).map(x => [x.t, x.ghosts.map(g => [g.key.slice(0, 12), Math.round(g.x), Math.round(g.y), Math.round(g.w)]), x.chip && [Math.round(x.chip.x), Math.round(x.chip.y)], x.cx, x.beacons, x.tick])));
    const air = s.s.filter(x => x.ghosts.some(g => /^4174601819/.test(g.key)));
    assert(air.length >= 20, 'the card was seen flying over many frames: ' + air.length);
    const g0 = air[0].ghosts.find(g => /^4174601819/.test(g.key)), gN = air[air.length - 1].ghosts.find(g => /^4174601819/.test(g.key)), chip = air[air.length - 1].chip;
    const d0 = Math.hypot(g0.x - chip.x, g0.y - chip.y), dN = Math.hypot(gN.x - chip.x, gN.y - chip.y);
    assert(dN < d0 * 0.35, `the card gets to the chip: ${Math.round(d0)} px away at first, ${Math.round(dN)} at the end`); assert(gN.w < g0.w * 0.6, 'and shrinks into it: ' + Math.round(g0.w) + ' → ' + Math.round(gN.w));
    // the chip answers: a ring first, then the count ticks up (it keeps the old count until the card lands)
    const ring = s.s.findIndex(x => x.beacons > 0), landed = s.s.findIndex(x => x.tick > 0), flightStart = s.s.findIndex(x => x.ghosts.length);
    assert(ring >= 0 && ring <= flightStart + 3, 'the Cancelled chip showed a ring as the card set off'); assert(landed > ring, 'the count rolled after the ring');
    const seen = [...new Set(s.s.map(x => x.cx))]; assert.deepEqual(seen, ['0', '01', '1'], 'the count read 0 while the card flew, rolled (both digits, "01") and ends at 1: ' + JSON.stringify(seen));
    assert(s.s.slice(0, landed).filter(x => x.ghosts.length).every(x => x.cx === '0'), 'the number waits for the card'); assert.equal(await chipN(page, 'cancelled'), '1');
    assert.equal(await chipN(page, 'hold'), String(+before.hold - 1), 'On hold counts one fewer');
    // only transform and opacity animate
    assert(s.anims > 5 && s.props.every(p => ['transform', 'opacity'].includes(p)), 'only transform and opacity animate: ' + JSON.stringify(s.props));
    // the note, with Undo and Show
    const note = await page.evaluate(() => { const n = document.querySelector('.mNote'); return n ? { text: n.querySelector('.mNoteT').textContent, btns: [...n.querySelectorAll('.mNoteBtn')].map(b => b.textContent) } : null; });
    assert(note && /Order 4174601819 cancelled/.test(note.text) && !/lines/i.test(note.text), 'the note says so: ' + JSON.stringify(note)); assert.deepEqual(note.btns, ['Undo', 'Show']);
    await shot(page, 'cancelled-note');
  });

  await check('5 · the timeline keeps every earlier step and adds the cancel with who and when; the order sits under Cancelled', async () => {
    const ev = await timeline(A), types = ev.map(e => e.type);
    for (const t of ['arrived', 'placed', 'removed', 'held']) assert(types.includes(t), `the earlier ${t} step is kept: ` + types);
    assert.equal(types.filter(t => t === 'removed').length, 2, 'both removals');
    const cx = ev.find(e => e.type === 'cancelled'); assert(cx && cx.by === 'Tester' && cx.at > t0 + 3 * 3600e3 - 60e3, 'the cancel, who and when: ' + JSON.stringify(cx));
    await page.click('#ordChips [data-pile="cancelled"]');
    await page.waitForSelector(`#ordBody .cxRow[data-rid="${A}"]`);
    const row = await page.textContent(`#ordBody .cxRow[data-rid="${A}"]`); assert(/Cancelled by Tester/.test(row), 'listed under Cancelled with who: ' + row.slice(0, 120));
    await shot(page, 'cancelled-tab');
    await page.click('#ordChips [data-pile="hold"]');
    await page.waitForFunction(() => Orders.view().pile === 'hold');
  });

  await check('6 · nothing is left on screen once it has played', async () => {
    await settle(page);
    const left = await page.evaluate(() => ({ ghosts: document.querySelectorAll('.mGhost, .mLift').length, beacons: document.querySelectorAll('.cnBeacon').length, tick: document.querySelectorAll('.cnTick').length, layer: (document.getElementById('motionLayer') || { children: [] }).children.length, busy: document.querySelectorAll('.cnBusy').length, chip: document.querySelector('#ordChips [data-pile="cancelled"] b').textContent, truth: Cancelled.count() }));
    assert.equal(left.ghosts + left.beacons + left.tick + left.busy, 0, JSON.stringify(left)); assert.equal(left.layer, 0, 'the motion layer is empty: ' + await page.evaluate(() => [...document.getElementById('motionLayer').children].map(n => n.outerHTML.slice(0, 160)).join(' | '))); assert.equal(left.chip, String(left.truth));
  });

  await check('7 · Undo is the existing restore: cancelRestore once, the order is back on hold, its card flies back out of the chip', async () => {
    // (the note from step 4 is still up for 12 s: pressed here, in On hold)
    await page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
    // a fresh cancel of the same order would be a second test of step 4; this one presses Undo on the note CancelUI left
    assert.equal(await page.evaluate(A => CancelUI.canUndo(A), A), true);
    await sampler(page);
    await page.evaluate(() => { CancelUI.undo('4174601819'); });
    await page.waitForSelector(`${sel(A)} .cnCancelBtn`, { timeout: 15000 });
    await page.waitForTimeout(1900);
    const s = await stopSampler(page);
    assert.equal(calls('cancelRestore', A).length, 1, 'cancelRestore once'); assert.equal(calls('cancelRestore').length, 1);
    assert.equal(st.doc('Charm_Nest_Cancelled', A), undefined, 'the record is out of Cancelled (kept in its history)');
    assert.equal(await page.evaluate(A => B.orders.rows.filter(r => r.order.receiptId === A && r.state === 'held' && /Taken off SS Sheet 1, GF Sheet 2 by Paul/.test(r.hold)).length, A), 1, 'held again, with its own words');
    assert.equal(await cardsOf(page, A), 1); assert.equal(await chipN(page, 'cancelled'), '0');
    const flew = s.s.filter(x => x.ghosts.length); assert(flew.length >= 15, 'the card was seen flying back: ' + flew.length);
    const first = flew[0].ghosts[0], last = flew[flew.length - 1].ghosts[0], chip = flew[0].chip;
    assert(Math.hypot(first.x - chip.x, first.y - chip.y) < Math.hypot(last.x - chip.x, last.y - chip.y), 'it starts at the Cancelled chip and moves away from it');
    assert(s.props.every(p => ['transform', 'opacity'].includes(p)), 'only transform and opacity: ' + JSON.stringify(s.props));
    assert(s.s.some(x => x.notes.some(n => /Order 4174601819 is back on hold/.test(n))), 'a note says it is back on hold');
    const ev = (await timeline(A)).map(e => e.type); for (const t of ['arrived', 'placed', 'removed', 'held', 'cancelled', 'cancelRestored']) assert(ev.includes(t), `the timeline keeps ${t}: ` + ev);
    assert.equal(await page.evaluate(A => (CancelUI.canUndo(A)), A), false);
    await shot(page, 'undone');
    // and it can be cancelled again, one record, as before
    await settle(page);
    await page.click(`${sel(A)} .cnCancelBtn`);
    await page.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), A);
    assert.equal(calls('cancelPut', A).length, 2); assert(st.doc('Charm_Nest_Cancelled', A));
  });

  await check('8 · an order of two lines flies as one: both cards, one cancel, one note', async () => {
    await page.evaluate(() => { for (const n of document.querySelectorAll('.mNote')) n.remove(); });
    await settle(page);
    assert.equal(await cardsOf(page, B2), 2);
    await sampler(page);
    await page.click(`${sel(B2)} .cnCancelBtn >> nth=0`);
    await page.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), B2, { timeout: 15000 });
    await page.waitForTimeout(1900);
    const s = await stopSampler(page);
    assert.equal(calls('cancelPut', B2).length, 1, 'one cancel for the order, not one a line');
    const keys = new Set(s.s.flatMap(x => x.ghosts.map(g => g.key))); assert.equal(keys.size, 2, 'both cards flew: ' + [...keys]);
    const notes = new Set(s.s.flatMap(x => x.notes.filter(n => /Order 4174601820 cancelled/.test(n)))); assert.equal(notes.size, 1, 'one note: ' + [...notes]);
    assert.equal(await chipN(page, 'cancelled'), '2', 'A (cancelled again) and this order'); assert.equal(st.doc('Charm_Nest_Cancelled', B2).lines.length, 2, 'the record keeps both lines');
    await shot(page, 'two-lines');
  });

  await check('9 · reduced motion: no flight, the note still shows, the work is the same', async () => {
    const r = await open({ reduced: true, who: 'Tester', held: [D] });
    await r.evaluate(() => { window.__fly = 0; new MutationObserver(m => { if (document.querySelector('#motionLayer .mGhost, .cnBeacon')) window.__fly++; }).observe(document.body, { childList: true, subtree: true }); });
    const cx0 = +await chipN(r, 'cancelled');
    await r.click(`#ordItems [data-rid="${D}"] .cnCancelBtn`);
    await r.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), D);
    await r.waitForSelector('.mNote');
    const got = await r.evaluate(() => ({ fly: window.__fly, note: document.querySelector('.mNote .mNoteT').textContent, btns: [...document.querySelectorAll('.mNote .mNoteBtn')].map(b => b.textContent), cx: document.querySelector('#ordChips [data-pile="cancelled"] b').textContent, anim: document.getAnimations().filter(a => a.effect && a.effect.target && a.effect.target.closest && a.effect.target.closest('#motionLayer')).length }));
    assert.equal(got.fly, 0, 'nothing flew'); assert.equal(got.anim, 0); assert(/Order 4174601822 cancelled/.test(got.note)); assert.deepEqual(got.btns, ['Undo', 'Show']); assert.equal(got.cx, String(cx0 + 1));
    assert.equal(calls('cancelPut', D).length, 1);
    await r.close();
  });

  await check('10 · no name saved: the small inline bar asks, never a pop-up; put away it does nothing; named, it goes on', async () => {
    const n = await open({ who: '', held: [E] });
    await n.click(`#ordItems [data-rid="${E}"] .cnCancelBtn`);
    await n.waitForSelector('.cnNameBar');
    assert.equal(await n.evaluate(() => window.__prompted), 0, 'no browser pop-up'); assert.equal(calls('cancelPut', E).length, 0, 'nothing is cancelled before the name');
    await n.click('.cnNameBar .cnNbX');
    await n.waitForFunction(() => !document.querySelector('.cnNameBar'));
    await n.waitForTimeout(300); assert.equal(calls('cancelPut', E).length, 0, 'put away: nothing happened'); assert.equal(await cardsOf(n, E), 1);
    assert.equal(await n.locator(`#ordItems [data-rid="${E}"] .cnCancelBtn`).isDisabled(), false);
    await n.click(`#ordItems [data-rid="${E}"] .cnCancelBtn`);
    await n.waitForSelector('.cnNameBar input'); await n.fill('.cnNameBar input', 'Pat Smith'); await n.press('.cnNameBar input', 'Enter');
    await n.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), E, { timeout: 15000 });
    const put = calls('cancelPut', E); assert.equal(put.length, 1); assert.equal(put[0].body.by, 'Pat Smith', 'the record carries the name asked for');
    assert.equal(await n.evaluate(() => window.__prompted + window.__confirmed), 0);
    await n.close();
  });

  await check('11 · sandbox and production stay separate: a sandbox page cancels into the sandbox only, a production page never touches it', async () => {
    assert.equal(st.list('Sandbox_Charm_Nest_Cancelled').length, 0, 'nothing of the sandbox so far');
    assert(calls('cancelPut').every(c => !c.body.sandbox), 'no sandbox flag on a production page');
    const sb = await open({ sandbox: true, held: [F] });
    const before = st.list('Charm_Nest_Cancelled').map(x => x._id).sort();
    await sb.click(`#ordItems [data-rid="${F}"] .cnCancelBtn`);
    await sb.waitForFunction(rid => !document.querySelector(`#ordItems [data-rid="${rid}"]`), F, { timeout: 15000 });
    const put = calls('cancelPut', F); assert.equal(put.length, 1); assert.equal(put[0].body.sandbox, true, 'the cancel carries the sandbox flag');
    assert(st.doc('Sandbox_Charm_Nest_Cancelled', F), 'the record is in the sandbox collection'); assert.equal(st.doc('Charm_Nest_Cancelled', F), undefined, 'and not in the real one');
    assert.deepEqual(st.list('Charm_Nest_Cancelled').map(x => x._id).sort(), before, 'the real cancelled list is as it was');
    await sb.close();
  });

  if (SHOTS) await check('12 · frames of the flight (screenshots only): the card lifts, the ring, the flight, the note and the count', async () => {
    const p = await open({ held: [G], viewport: { width: 1100, height: 700 } });
    await p.screenshot({ path: path.join(SHOTS, 'flight-0-before.png') });
    await p.click(`#ordItems [data-rid="${G}"] .cnCancelBtn`);
    await p.waitForSelector('#motionLayer .mGhost');
    let i = 1;
    for (const f of [0.08, 0.22, 0.45, 0.7, 0.9]) {
      await p.evaluate(f => { for (const a of document.getAnimations()) { const t = a.effect && a.effect.target; if (t && t.closest && t.closest('#motionLayer')) { a.pause(); a.currentTime = f * (a.effect.getComputedTiming().duration || 1000) + (a.effect.getComputedTiming().delay || 0); } } }, f);
      await p.screenshot({ path: path.join(SHOTS, `flight-${i++}-${String(Math.round(f * 100)).padStart(2, '0')}pct.png`) });
    }
    await p.evaluate(() => { for (const a of document.getAnimations()) { const t = a.effect && a.effect.target; if (t && t.closest && t.closest('#motionLayer')) a.play(); } });
    await p.waitForSelector('.mNote'); await p.waitForTimeout(260);
    await p.screenshot({ path: path.join(SHOTS, `flight-${i++}-landed-count-rolling.png`) });
    await p.waitForTimeout(900);
    await p.screenshot({ path: path.join(SHOTS, `flight-${i++}-note-undo-show.png`) });
    await p.close();
  });

  for (const p of pages) assert.deepEqual(p.errors, [], 'no page errors: ' + p.errors.join(' | '));
  await browser.close(); srv.close();
  const bad = results.filter(r => !r[1]);
  console.log(bad.length ? `\n${bad.length} of ${results.length} checks FAILED` : `\nall ${results.length} checks passed`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(2); });
