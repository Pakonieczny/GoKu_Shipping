// Browser test of the shared-orders window (charm-nest-shared-orders-modal.js, Paul 5 Oct 2026, point 6): the one pop-up that
// says which multi-piece orders keep a sheet in its set, opens an order's detail view and comes back to itself, takes an order
// off the sheets by the person's own choice, and follows the page's state in real time.
//
// SharedOrders (charm-nest-shared-orders.js, the engine's) is a FAKE here with the exact shapes agreed with its owner
// (between / removeFromSheet / subscribe over a small in-memory store), unless SO_REAL=1 (the real one is on the page then and
// the store below is not used). The page is the real charm-nest-1.html over the local stand-in for the site (bridge-server.cjs);
// every request that is not to the loopback is aborted. Nothing live is ever called.
//   node tests/charm-nest/shared-orders-modal.cjs [playwright-core dir]    (SHOTS=<dir> also saves the screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); process.exit(0); }
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const SLOW = Math.max(1, +process.env.SO_SLOW || 1);   // (SO_SLOW=4 on a busy machine: every wait below is that many times longer)
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);

/* ── the fixture: orders with pieces on GF Sheet 1 (the sheet in question) and on other sheets of its set ── */
const svg = (seed, color) => 'data:image/svg+xml,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="160" height="160" viewBox="0 0 160 160"><rect width="160" height="160" fill="#fffdf8"/><g fill="none" stroke="${color}" stroke-width="5" stroke-linecap="round" stroke-linejoin="round">${[
  '<circle cx="80" cy="46" r="14"/><path d="M80 60v14"/><circle cx="80" cy="108" r="34"/><path d="M80 94v28M66 108h28"/>',
  '<path d="M80 30l14 30 33 4-24 23 6 33-29-16-29 16 6-33-24-23 33-4z"/>',
  '<path d="M80 128C40 98 36 62 60 52c12-5 20 2 20 12 0-10 8-17 20-12 24 10 20 46-20 76z"/>',
  '<circle cx="80" cy="80" r="40"/><path d="M80 40v80M40 80h80"/><circle cx="80" cy="80" r="14"/>',
  '<path d="M44 112c0-40 20-76 36-76s36 36 36 76"/><path d="M60 112h40"/><circle cx="80" cy="64" r="10"/>',
  '<path d="M52 118c-14-30 4-72 40-76 22-2 30 18 14 34-12 12-30 6-30 22 0 14 20 14 20 14"/>'][seed % 6]}</g></svg>`);
const GOLD = '#8a6a22', SILV = '#59616b', ROSE = '#a8655a';
const SH = { gf: ['GF Sheet 1', 'gf-s1', GOLD], ss: ['SS Sheet 1', 'ss-s1', SILV], rg: ['RG Sheet 1', 'rg-s1', ROSE] };
const NAMES = ['Nathaly Soto', 'Emily Chambers', 'Jechelle Aragones', 'Leslie Suhr', 'Yera Espinosa', 'Calvin Ly', 'Heike Wagener', 'Nicole Offermann', 'Sunny Makowiak', 'Sarah Lohe', 'Chanel Sargeant', 'Tim Wright'];
function mkOrders(n, o = {}) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const id = String(4170252963 + i * 137), kinds = o.kinds || [['gf', 'ss'], ['gf', 'ss'], ['gf', 'ss', 'rg'], ['gf', 'rg'], ['gf', 'ss', 'ss'], ['gf', 'ss']][i % 6];
    const long = o.long && i % 2 === 0;
    out.push({ id, who: long ? 'Maximiliane Wilhelmina Featherstonehaugh-Cholmondeley' : NAMES[i % NAMES.length],
      thumb: o.noThumbs ? null : svg(i, SH[kinds[0]][2]),
      pieces: kinds.map((k, j) => ({ label: `Piece ${j + 1}`, sheetLabel: SH[k][0], sheetId: SH[k][1] + (kinds.indexOf(k) !== j ? '-b' : ''), thumb: o.noThumbs ? null : svg(i + j * 2 + 1, SH[k][2]) })) });
  }
  return out;
}
const rid = o => o.id, tid = o => o.id + '1';
const orderRec = o => ({ receiptId: o.id, orderNumber: o.id, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: o.who }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: o.pieces.map((p, j) => ({ transactionId: tid(o) + j, listingId: '18000' + String(tid(o)).slice(-4) + j, sku: 'PIECE_' + (j + 1), title: 'Piece ' + (j + 1) + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' })) });

/* ── the fake SharedOrders (the shapes agreed with the engine's owner), serialised into the page ── */
function fakeShared(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms)), subs = new Set();
  const store = window.__so = { orders: cfg.orders, held: {}, calls: [], reads: 0, delay: cfg.delay == null ? 450 : cfg.delay, fail: null, stay: null, subs };
  const item = (o, id) => ({ orderId: o.id, label: '#' + o.id, customer: o.who, thumb: o.thumb, here: (o.pieces.find(p => p.sheetId === id) || {}).sheetLabel, there: o.pieces.filter(p => p.sheetId !== id).map(p => p.sheetLabel),
    locked: o.locked || [], pieces: o.pieces.map((p, i) => ({ index: i + 1, label: p.label, sheetId: p.sheetId, sheetLabel: p.sheetLabel, setId: 'set-1', thumb: p.thumb })) });
  window.SharedOrders = {
    between(id, target) {
      store.reads++;
      if (store.readFail) return Promise.reject(new Error('The orders could not be read.'));
      if (store.readDelay) { const d = store.readDelay; return sleep(d).then(() => window.SharedOrders._now(id)); }
      return window.SharedOrders._now(id);
    },
    _now(id) { return store.orders.filter(o => !store.held[o.id] && o.pieces.some(p => p.sheetId === id) && o.pieces.some(p => p.sheetId !== id)).map(o => item(o, id)); },
    async removeFromSheet(a) {
      store.calls.push(JSON.parse(JSON.stringify(a))); await sleep(store.delay);
      if (store.fail) return { ok: false, error: store.fail };
      if (store.stay) return { ok: true, removed: [], stayed: store.stay };
      store.held[a.orderId] = a.mode; subs.forEach(f => { try { f(); } catch (_) {} });
      return { ok: true, scope: 'order', removed: [a.orderId + '_1'], stayed: [] };
    },
    subscribe(fn) { if (store.noSub) return () => {}; subs.add(fn); return () => subs.delete(fn); }
  };
}

const until = async (fn, ms = 8000, what = '') => { ms *= SLOW; const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 40)); } };

(async () => {
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ok = [], shotN = { n: 0 };
  const shot = async (page, name) => { if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };

  /** A page of the real app with the fake SharedOrders and the orders in Orders (so the order window can open them). */
  async function boot(orders, o = {}) {
    const ctx = await browser.newContext({ viewport: o.viewport || { width: 1440, height: 900 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference', hasTouch: !!o.touch, isMobile: !!o.touch });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => { const u = r.request().url(); if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' }); if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope=()=>{throw new Error('firebase stub')};export const initializeApp=nope,getApp=nope,getStorage=nope,ref=nope,uploadBytesResumable=nope,getDownloadURL=nope,getAuth=nope,signInAnonymously=nope,onAuthStateChanged=nope;" }); if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }); return r.abort(); });
    await ctx.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) {} return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.fallback(); });
    if (o.noName) await ctx.addInitScript(() => { window.__noName = true; });
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); if (!window.__noName) localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
    if (!process.env.SO_REAL) await ctx.addInitScript(`(${fakeShared.toString()})(${JSON.stringify({ orders, delay: o.delay })})`);
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(20000 * SLOW);
    page.on('pageerror', e => { errors.push('page: ' + e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SharedOrdersModal && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 * SLOW });
    await page.evaluate(async ({ recs }) => {
      await Orders.loadMaps(true);
      for (const order of recs) order.lines.forEach(line => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
      const wait = window.setTimeout.bind(window), T0 = Date.now() - 36e5 * 90;
      OrderTimeline.get = async id => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify({ id: String(id), events: [{ id: id + '~a', orderId: id, type: 'arrived', at: T0, by: 'Etsy', source: 'etsy', text: 'Order arrived from Etsy' }], cancelled: null, where: null })); };
    }, { recs: orders.map(orderRec) });
    await page.waitForTimeout(300);
    return { ctx, page, errors };
  }
  const openModal = (page, o = {}) => page.evaluate(({ o }) => {
    window.__ev = { change: [], close: [], retry: 0 };
    const orders = o.noOrders ? undefined : SharedOrders.between('gf-s1', null);
    const h = SharedOrdersModal.open({ kind: 'sheet', id: 'gf-s1', orders, sheetLabel: 'GF Sheet 1', setLabel: 'Set 1', targetLabel: o.target === undefined ? 'Set 3' : o.target, targetSetId: 'set-3',
      onChange: (rest, why) => window.__ev.change.push([rest.map(x => x.orderId), why]), onClose: r => window.__ev.close.push(r), onRetry: o.noRetry ? undefined : () => { window.__ev.retry++; } });
    return !!h;
  }, { o });
  const cards = page => page.$$eval('.soDlg .soCard:not(.leaving)', cs => cs.map(c => c.dataset.order));
  const text = (page, sel) => page.$eval(sel, e => e.textContent.replace(/\s+/g, ' ').trim());
  const vis = (page, sel) => page.evaluate(s => { const e = document.querySelector(s); if (!e) return false; const r = e.getBoundingClientRect(), cs = getComputedStyle(e); return r.width > 0 && r.height > 0 && cs.visibility !== 'hidden' && +cs.opacity > .5; }, sel);
  const settled = page => page.waitForTimeout(950);   // (the open animation: parts come in by about 760 ms)

  /* ═════ 1 · what it shows ═════ */
  {
    const orders = mkOrders(3);
    const { ctx, page, errors } = await boot(orders);
    assert(await openModal(page), 'the window opens');
    await settled(page);
    assert.equal(await page.evaluate(() => SharedOrdersModal.isOpen()), true);
    assert.deepEqual(await cards(page), orders.map(o => o.id), 'exactly the shared orders, in order');
    const t = await text(page, '.soTitle'), s = await text(page, '.soSub');
    assert.equal(t, 'These orders keep GF Sheet 1 in Set 1', 'title: ' + t);
    assert(/Their pieces sit on other sheets, so it can't move to Set 3 yet\.$/.test(s), 'sentence: ' + s);
    assert(s.split(/\s+/).length <= 14, 'one short sentence: ' + s);
    assert.equal(await text(page, '.soCount'), '3 orders');
    // per card: order number, customer, a picture per piece, the sheet chip and here/there on every piece, and the card is a link
    for (const o of orders) {
      const c = await page.$eval(`.soCard[data-order="${o.id}"]`, (el, id) => ({ no: el.querySelector('.soOrderNo').textContent, who: el.querySelector('.soWho').textContent, tiles: el.querySelectorAll('.soTile').length, imgs: el.querySelectorAll('.soTile img').length, chips: [...el.querySelectorAll('.soPiece')].map(p => [p.querySelector('.soChip').textContent.trim(), p.querySelector('.soWhere').textContent, p.classList.contains('here')]), open: el.querySelector('[data-open]').getAttribute('aria-label'), off: el.querySelector('[data-off]').textContent.trim() }), o.id);
      assert.equal(c.no, '#' + o.id); assert.equal(c.who, o.who); assert.equal(c.tiles, o.pieces.length, 'a picture tile per piece');
      assert.equal(c.imgs, o.pieces.length, 'every tile has its picture');
      assert.deepEqual(c.chips.map(x => x[0]), o.pieces.map(p => p.sheetLabel.replace(' Sheet ', ' ')).sort((a, b) => (b === 'GF 1') - (a === 'GF 1')), 'sheet chips (this sheet first)');
      assert.equal(c.chips[0][1], 'here'); assert(c.chips[0][2], 'GF Sheet 1 is here');
      assert(c.chips.slice(1).every(x => x[1] === 'there' && !x[2]), 'the others are there');
      assert(/^Open order \d+, .+\. \d+ pieces?: /.test(c.open), 'a card that says what it opens: ' + c.open);
      assert.equal(c.off, 'Take off the sheet…');
    }
    assert(!/\blines?\b/i.test(await page.evaluate(() => document.querySelector('.soDlg').innerText)), 'never "line"');
    // the head names the sheets these orders tie together
    const sheets = await page.$$eval('.soSheets .soChip', cs => cs.map(c => c.textContent.trim()));
    assert.deepEqual(sheets, ['GF Sheet 1', 'SS Sheet 1', 'RG Sheet 1'], 'the sheets tied together: ' + sheets);
    await shot(page, 'S2-three-orders');
    ok.push('1 · opens with exactly the shared orders: title, one short sentence, count, a card per order with a picture per piece, sheet chip and here/there, number and customer');

    // focus is inside the window (a modal <dialog>: the page behind is inert), Tab stays in it
    assert(await page.evaluate(() => document.querySelector('.soDlg').contains(document.activeElement)), 'focus starts inside');
    for (let i = 0; i < 14; i++) await page.keyboard.press('Tab');
    assert(await page.evaluate(() => document.querySelector('.soDlg').contains(document.activeElement)), 'Tab never leaves the window');
    ok.push('1 · the focus starts inside the window and Tab cannot leave it');

    // Esc closes it, onClose says how
    await page.keyboard.press('Escape');
    await until(async () => !(await page.evaluate(() => SharedOrdersModal.isOpen())), 3000, 'Esc closes');
    await until(async () => (await page.evaluate(() => __ev.close.length)) === 1, 2000, 'onClose is told');
    const ev = await page.evaluate(() => window.__ev);
    assert.equal(ev.close.length, 1); assert.equal(ev.close[0].cleared, false); assert.equal(ev.close[0].retried, false);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('1 · Esc closes the window; onClose({cleared:false, retried:false})');
    await ctx.close();
  }

  const owOpen = page => page.evaluate(() => { const o = document.getElementById('orderWin'); return !!(o && o.open); });
  const modalUp = page => page.evaluate(() => { const d = document.querySelector('.soDlg'); if (!d || !d.open) return false; const r = d.getBoundingClientRect(); return r.width > 0 && +getComputedStyle(d).opacity > .9; });
  const choose = async (page, id, mode) => {
    await page.click(`.soCard[data-order="${id}"] [data-off]`);
    await until(() => page.$(`.soCard[data-order="${id}"][data-state=ask] .soAsk`), 2000, 'the choice opens');
    if (mode) await page.check(`.soCard[data-order="${id}"] .soAsk input[value=${mode}]`);
  };

  /* ═════ 2 · a quick link opens the order; Back returns here, as it was ═════ */
  {
    const orders = mkOrders(12);
    const { ctx, page, errors } = await boot(orders, { viewport: { width: 1440, height: 640 } });
    assert(await openModal(page)); await settled(page);
    assert.equal((await cards(page)).length, 12, 'twelve orders listed');
    await page.$eval('.soBody', b => { b.scrollTop = 220; });
    const pick = orders[5].id;
    await page.focus(`.soCard[data-order="${pick}"] [data-open]`);
    await page.click(`.soCard[data-order="${pick}"] [data-open]`);
    await until(() => owOpen(page), 6000, 'the order opens');
    const scrolled = await page.$eval('.soBody', b => b.scrollTop);   // (where the list was left: the card pressed may have been brought into view)
    assert(scrolled > 100, 'the list scrolls (it holds twelve): ' + scrolled);
    await until(() => vis(page, '#orderWin #soBack'), 3000, 'the Back button shows in the order view');
    assert((await text(page, '#orderWin #soBack')).includes('Back to shared orders'), 'Back is clear: ' + await text(page, '#orderWin #soBack'));
    assert(await page.evaluate(id => document.getElementById('orderWin').textContent.includes(id), pick), 'the order window shows order ' + pick);
    assert.equal(await page.evaluate(() => SharedOrdersModal.isOpen()), true, 'the window is still open underneath (kept as it was)');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[data-shared-orders]').length), 1, 'no second copy of the window');
    await shot(page, 'S5-order-view-with-back');
    // another computer takes order 3 off while the person looks at the order: it is not pulled out from under them
    await page.evaluate(id => { __so.held[id] = 'hold'; }, orders[2].id);
    await page.waitForTimeout(2300);
    assert.equal((await page.$$('.soDlg .soCard')).length, 12, 'nothing moves in the window while the person is away');
    await page.click('#orderWin #soBack');
    await until(async () => !(await owOpen(page)), 3000, 'Back closes the order view');
    await until(() => modalUp(page), 3000, 'the window comes back up');
    const back = await page.evaluate(() => ({ top: document.querySelector('.soBody').scrollTop, inside: document.querySelector('.soDlg').contains(document.activeElement), pill: !!document.querySelector('#soBack') }));
    assert(Math.abs(back.top - scrolled) < 30, `the list is where it was (scroll ${back.top} vs ${scrolled})`);
    assert(back.inside, 'the focus is back inside the window'); assert(!back.pill, 'the Back button is gone from the order view');
    await until(async () => (await cards(page)).length === 11, 4000, 'the card that went meanwhile now leaves');
    assert(!(await cards(page)).includes(orders[2].id), 'the right card left');
    ok.push('2 · a card opens its order (window kept underneath, a clear Back in the order view); Back returns to the same scroll and focus, and what changed meanwhile now leaves');

    // Esc in the order view also comes back
    await page.click(`.soCard[data-order="${orders[0].id}"] [data-open]`);
    await until(() => owOpen(page), 6000, 'opens again');
    await page.keyboard.press('Escape');
    await until(async () => !(await owOpen(page)), 3000, 'Esc closes the order view');
    await until(() => modalUp(page), 3000, 'back up after Esc');
    assert.equal((await cards(page)).length, 11);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('2 · Esc in the order view returns here too');
    await ctx.close();
  }

  /* ═════ 3 · taking an order off: an explicit choice, a labelled wait, the card leaves, the count follows, then "Nothing holds" ═════ */
  {
    const orders = mkOrders(3);
    const { ctx, page, errors } = await boot(orders, { delay: 1100 });   // (a slow answer, so the wait can be looked at)
    assert(await openModal(page)); await settled(page);
    await choose(page, orders[1].id, null);
    let st = await page.evaluate(() => ({ yes: document.querySelector('.soAsk [data-yes]').disabled, checked: document.querySelectorAll('.soAsk input:checked').length, calls: __so.calls.length, q: document.querySelector('.soAskQ').textContent, opts: [...document.querySelectorAll('.soAsk .soOpt span')].map(s => s.textContent) }));
    assert.equal(st.checked, 0, 'no choice is made for the person'); assert(st.yes, '"Take off" waits for a choice'); assert.equal(st.calls, 0, 'nothing is sent yet');
    assert.deepEqual(st.opts, ['Put on hold', 'Cancel the order']); assert(/^Take #\d+ off its sheets\?$/.test(st.q), st.q);
    await shot(page, 'S6-take-off-choice');
    await page.check(`.soCard[data-order="${orders[1].id}"] .soAsk input[value=hold]`);
    assert.equal(await page.$eval('.soAsk [data-yes]', b => b.disabled), false);
    assert.equal(await text(page, '.soAsk [data-yes]'), 'Take off and hold');
    await page.click('.soAsk [data-yes]');
    await until(() => vis(page, `.soCard[data-order="${orders[1].id}"] .soBusy`), 2000, 'a spinner shows');
    const busy = await page.$eval(`.soCard[data-order="${orders[1].id}"] .soBusy`, b => ({ t: b.textContent.trim(), spin: !!b.querySelector('.soSpin'), role: b.getAttribute('role') }));
    assert.equal(busy.t, 'Taking it off its sheets…'); assert(busy.spin, 'a small spinner'); assert.equal(busy.role, 'status');
    assert(await page.$eval('.soCard:not([data-state=busy]) .soOff', b => b.getAttribute('aria-disabled') === 'true'), 'the other cards cannot start a second change at once');
    await shot(page, 'S7-taking-off');
    await until(async () => (await cards(page)).length === 2, 4000, 'the card leaves');
    assert.deepEqual(await cards(page), [orders[0].id, orders[2].id]);
    assert.equal(await text(page, '.soCount'), '2 orders');
    const calls = await page.evaluate(() => __so.calls);
    assert.equal(calls.length, 1); assert.equal(calls[0].orderId, orders[1].id); assert.equal(calls[0].sheetId, 'gf-s1'); assert.equal(calls[0].mode, 'hold'); assert.equal(calls[0].by, 'Test Operator');
    let ev = await page.evaluate(() => window.__ev);
    assert(ev.change.length >= 1 && JSON.stringify(ev.change[ev.change.length - 1][0]) === JSON.stringify([orders[0].id, orders[2].id]), 'onChange got the remaining orders: ' + JSON.stringify(ev.change));
    assert(await page.evaluate(() => document.querySelector('.soDlg').contains(document.activeElement)), 'the focus stays in the window after the card went');
    await shot(page, 'S8-after-removing-one');
    ok.push('3 · "Take off the sheet…" opens an inline choice (none chosen for them, nothing sent), a labelled spinner while it works, the card leaves, the count follows, onChange gets the rest');

    // cancel is its own explicit choice, with its own words
    await choose(page, orders[0].id, 'cancel');
    assert.equal(await text(page, '.soAsk [data-yes]'), 'Take off and cancel');
    assert(await page.$eval('.soAsk [data-yes]', b => b.classList.contains('danger')), 'cancel looks different from hold');
    await page.click('.soAsk [data-yes]');
    assert.equal(await text(page, `.soCard[data-order="${orders[0].id}"] .soBusy`), 'Cancelling the order…');
    await until(async () => (await cards(page)).length === 1, 4000, 'the second card leaves');
    assert.equal((await page.evaluate(() => __so.calls))[1].mode, 'cancel');
    assert.equal(await text(page, '.soCount'), '1 order');
    await until(async () => (await text(page, '.soTitle')).startsWith('This order keeps'), 2000, 'the title turns singular: ' + await text(page, '.soTitle'));
    ok.push('3 · Cancel the order is a separate explicit choice; the title and count follow ("This order keeps…", "1 order")');

    // the last one: the empty state, with the move offered again
    await choose(page, orders[2].id, 'hold');
    await page.click('.soAsk [data-yes]');
    await until(() => vis(page, '.soClear'), 5000, 'the empty state shows');
    assert.equal(await text(page, '.soTitle'), 'Nothing holds GF Sheet 1 any more');
    assert.equal(await text(page, '.soClearT'), 'It can move to Set 3 now.');
    assert.equal(await text(page, '.soCount'), 'All clear');
    assert.equal(await text(page, '[data-retry]'), 'Move it now'); assert.equal(await text(page, '.soClear [data-close]'), 'Close');
    assert(await page.evaluate(() => document.querySelector('.soDlg').contains(document.activeElement) && document.activeElement.matches('[data-retry]')), 'the focus is on the primary action');
    await shot(page, 'S9-empty-state');
    await page.click('[data-retry]');
    await until(async () => !(await page.evaluate(() => SharedOrdersModal.isOpen())), 3000, 'the window closes');
    await until(async () => (await page.evaluate(() => __ev.retry)) === 1, 2000, 'the move is tried again');
    await until(async () => (await page.evaluate(() => __ev.close.length)) === 1, 2000, 'onClose is told');
    ev = await page.evaluate(() => window.__ev);
    assert.equal(ev.close.length, 1); assert.equal(ev.close[0].cleared, true); assert.equal(ev.close[0].retried, true);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('3 · when the last order goes: "Nothing holds GF Sheet 1 any more", "Move it now" (the caller\'s move is tried again once) and Close');
    await ctx.close();
  }

  /* ═════ 4 · Esc layering, and the page's own state in real time (another computer) ═════ */
  {
    const orders = mkOrders(4);
    const { ctx, page, errors } = await boot(orders);
    assert(await openModal(page, { noRetry: true })); await settled(page);
    await choose(page, orders[0].id, 'hold');
    await page.keyboard.press('Escape');
    await until(async () => (await page.$$eval('.soAsk', a => a.length)) === 0, 1500, 'Esc puts away the open choice first (it folds away, then is gone)');
    assert.equal(await page.evaluate(() => SharedOrdersModal.isOpen()), true, 'and leaves the window');
    assert.equal(await page.evaluate(() => __so.calls.length), 0, 'nothing was sent');
    ok.push('4 · Esc puts away an open choice first (nothing sent), the window stays');

    // another computer holds order 2: its card leaves by itself, within about three seconds, with no press here
    const t0 = Date.now();
    await page.evaluate(id => { __so.held[id] = 'hold'; }, orders[1].id);
    await until(async () => (await cards(page)).length === 3, 3500, 'the card leaves by itself');
    const took = Date.now() - t0; assert(took < 3200, `within about three seconds (${took} ms)`);
    assert(!(await cards(page)).includes(orders[1].id)); assert.equal(await text(page, '.soCount'), '3 orders');
    // and a new offender arrives in front of them
    const extra = mkOrders(5)[4];
    await page.evaluate(o => { __so.orders.push(o); }, extra);
    await until(async () => (await cards(page)).includes(extra.id), 3500, 'a new card arrives');
    assert.equal(await text(page, '.soCount'), '4 orders');
    ok.push(`4 · another computer's change drops a card (${took} ms) and a new shared order arrives, with the count following, no press needed`);

    // no onRetry: the empty state offers Done
    await page.evaluate(() => { for (const o of __so.orders) __so.held[o.id] = 'hold'; });
    await until(() => vis(page, '.soClear'), 4000, 'all clear');
    assert.equal(await page.$$eval('[data-retry]', a => a.length), 0, 'no retry button without a move to retry');
    assert.equal(await text(page, '.soClear [data-close]'), 'Done');
    await page.click('.soClear [data-close]');
    await until(async () => !(await page.evaluate(() => SharedOrdersModal.isOpen())), 3000, 'Done closes');
    await until(async () => (await page.evaluate(() => __ev.close.length)) >= 1, 2000, 'onClose is told');
    const ev = await page.evaluate(() => window.__ev);
    assert.equal(ev.close[ev.close.length - 1].cleared, true);
    // a subscribed change (the engine's own removal) is picked up at once, not on the next second
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('4 · with nothing left and no move to retry: "Nothing holds…" with Done; onClose says cleared');
    await ctx.close();
  }

  /* ═════ 5 · no name signed in, a piece that must stay, a failed change ═════ */
  {
    const orders = mkOrders(3);
    const { ctx, page, errors } = await boot(orders, { noName: true });
    assert(await openModal(page)); await settled(page);
    await choose(page, orders[0].id, 'hold');
    assert.equal(await page.$$eval('.soName', a => a.length), 1, 'asks for a name when none is signed in');
    assert.equal(await page.$eval('.soAsk [data-yes]', b => b.disabled), true, 'Take off waits for the name');
    await page.fill('.soName', 'Paul K');
    assert.equal(await page.$eval('.soAsk [data-yes]', b => b.disabled), false);
    await page.press('.soName', 'Enter');
    await until(async () => (await cards(page)).length === 2, 4000, 'the card leaves');
    assert(/^Paul K\.?$/.test((await page.evaluate(() => __so.calls))[0].by), 'the change is kept under the name (as the app writes names): ' + (await page.evaluate(() => __so.calls))[0].by);
    ok.push('5 · no one signed in: it asks for a name, Take off waits for it, the change is kept under it');
    await ctx.close();
  }
  {
    const orders = mkOrders(3);
    const { ctx, page, errors } = await boot(orders);
    assert(await openModal(page)); await settled(page);
    // a piece that must stay: said plainly, the card stays
    await page.evaluate(() => { __so.stay = [{ poolId: 'p1', why: 'its sheet is already cut' }]; });
    await choose(page, orders[0].id, 'hold'); await page.click('.soAsk [data-yes]');
    await until(() => vis(page, `.soCard[data-order="${orders[0].id}"] .soNote`), 4000, 'a plain note shows');
    const note = await text(page, `.soCard[data-order="${orders[0].id}"] .soNote p`);
    assert.equal(note, 'One piece stays: its sheet is already cut. That keeps this order here.');
    assert.equal((await cards(page)).length, 3, 'the card stays'); assert.equal(await page.$$eval('.soBusy', a => a.length), 0, 'no spinner left behind');
    await page.click(`.soCard[data-order="${orders[0].id}"] .soNote button`);
    assert.equal(await page.$$eval('.soNote', a => a.length), 0, 'Got it puts the note away');
    ok.push('5 · a piece that must stay is said plainly on its card, and the card stays');
    // a failed change: nothing lost, one short line, Try again and Keep it
    await page.evaluate(() => { __so.stay = null; __so.fail = 'The sheet could not be reached.'; });
    await choose(page, orders[1].id, 'hold'); await page.click('.soAsk [data-yes]');
    await until(() => vis(page, `.soCard[data-order="${orders[1].id}"] .soNote.bad`), 4000, 'the failure shows');
    assert.equal(await text(page, `.soCard[data-order="${orders[1].id}"] .soNote p`), 'Not taken off. The sheet could not be reached.');
    assert.equal((await cards(page)).length, 3, 'the card stays');
    await shot(page, 'S10-failed-change');
    await page.evaluate(() => { __so.fail = null; });
    await page.click(`.soCard[data-order="${orders[1].id}"] .soNote button:first-child`);
    await until(() => page.$(`.soCard[data-order="${orders[1].id}"][data-state=ask]`), 2000, 'Try again asks again');
    await page.check(`.soCard[data-order="${orders[1].id}"] .soAsk input[value=hold]`); await page.click('.soAsk [data-yes]');
    await until(async () => (await cards(page)).length === 2, 4000, 'the second try works');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('5 · a failed change says so in one short line, keeps the card, and Try again works');
    await ctx.close();
  }

  /* ═════ 6 · keyboard ═════ */
  {
    const orders = mkOrders(4);
    const { ctx, page, errors } = await boot(orders);
    assert(await openModal(page)); await settled(page);
    const at = () => page.evaluate(() => { const a = document.activeElement, c = a && a.closest && a.closest('.soCard'); return c ? c.dataset.order : null; });
    await page.focus(`.soCard[data-order="${orders[0].id}"] [data-open]`);
    await page.keyboard.press('ArrowRight'); assert.equal(await at(), orders[1].id, 'right');
    await page.keyboard.press('ArrowDown'); assert.equal(await at(), orders[3].id, 'down');
    await page.keyboard.press('ArrowLeft'); assert.equal(await at(), orders[2].id, 'left');
    await page.keyboard.press('ArrowUp'); assert.equal(await at(), orders[0].id, 'up');
    await page.keyboard.press('End'); assert.equal(await at(), orders[3].id, 'End');
    await page.keyboard.press('Home'); assert.equal(await at(), orders[0].id, 'Home');
    // Tab reaches the card's own control, Enter on it opens the choice, and the choice is operable by keys alone
    await page.keyboard.press('Tab'); assert(await page.evaluate(() => document.activeElement.matches('[data-off]')), 'Tab reaches "Take off the sheet…"');
    await page.keyboard.press('Enter');
    await until(() => page.$(`.soCard[data-order="${orders[0].id}"][data-state=ask]`), 2000, 'Enter opens the choice');
    await until(() => page.evaluate(() => document.activeElement.matches('.soAsk input')), 2000, 'the focus moves into the choice');
    await page.keyboard.press('Escape');
    await until(() => page.evaluate(() => document.activeElement.matches('[data-off]')), 2000, 'putting the choice away returns to its button');
    // Enter on the card opens the order; Esc returns
    await page.focus(`.soCard[data-order="${orders[1].id}"] [data-open]`);
    await page.keyboard.press('Enter');
    await until(() => owOpen(page), 6000, 'Enter opens the order');
    await page.keyboard.press('Escape');
    await until(() => modalUp(page), 3000, 'Esc brings the window back');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('6 · keyboard: arrows/Home/End move between cards, Tab reaches the take-off button, Enter opens the choice or the order, Esc steps back one layer at a time');
    await ctx.close();
  }

  /* ═════ 7 · a phone: one column, nothing sideways, big enough targets ═════ */
  {
    const orders = mkOrders(5, { long: true });
    const { ctx, page, errors } = await boot(orders, { viewport: { width: 390, height: 780 }, touch: true });
    assert(await openModal(page)); await settled(page);
    const m = await page.evaluate(() => {
      const d = document.querySelector('.soDlg'), r = d.getBoundingClientRect(), b = document.querySelector('.soBody'), cs = [...document.querySelectorAll('.soCard')].map(c => c.getBoundingClientRect());
      return { w: r.width, left: r.left, right: r.right, vw: innerWidth, side: b.scrollWidth - b.clientWidth, page: document.documentElement.scrollWidth - innerWidth, lefts: [...new Set(cs.map(c => Math.round(c.left)))], off: document.querySelector('.soOff').getBoundingClientRect().height, x: document.querySelector('.soX').getBoundingClientRect().height,
        clip: [...document.querySelectorAll('.soCard')].filter(c => c.scrollWidth > c.clientWidth + 1).length,
        chipCut: [...document.querySelectorAll('.soPiece .soChip span')].filter(e => e.scrollWidth > e.clientWidth + 1).length, cardH: document.querySelector('.soCard').getBoundingClientRect().height };
    });
    assert(m.w <= m.vw * .97 && m.w >= m.vw * .9, `the window fits the phone (${m.w} of ${m.vw})`);
    assert(m.left >= 0 && m.right <= m.vw, 'inside the screen'); assert.equal(m.lefts.length, 1, 'one column'); assert(m.side <= 1, 'nothing scrolls sideways: ' + m.side);
    assert(m.page <= 0, 'the page does not either'); assert(m.off >= 34 && m.x >= 34, `targets are big enough (${m.off}, ${m.x})`); assert.equal(m.clip, 0, 'no card is cut off');
    assert.equal(m.chipCut, 0, 'no sheet chip is cut short ("GF 1" stays "GF 1")'); assert(m.cardH < 160, 'a compact row, not a tall card (' + m.cardH + ' px)');
    await shot(page, 'S11-mobile-390');
    await choose(page, orders[1].id, 'hold');
    const a = await page.evaluate(() => { const r = document.querySelector('.soAsk').getBoundingClientRect(), b = document.querySelector('.soAsk [data-yes]').getBoundingClientRect(); return { inside: r.right <= innerWidth && r.left >= 0, h: b.height }; });
    assert(a.inside && a.h >= 34, 'the choice fits and is easy to press: ' + JSON.stringify(a));
    await shot(page, 'S12-mobile-choice');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('7 · at 390 px: the window fits, one column, no sideways scroll, targets 34 px and up, long names do not break it');
    await ctx.close();
  }

  /* ═════ 8 · reduced motion: no travelling, the state still changes ═════ */
  {
    const orders = mkOrders(2);
    const { ctx, page, errors } = await boot(orders, { reduced: true, delay: 60 });
    assert(await openModal(page)); await page.waitForTimeout(250);
    await choose(page, orders[0].id, 'hold'); await page.click('.soAsk [data-yes]');
    await until(async () => (await cards(page)).length === 1, 2500, 'the card is gone');
    await choose(page, orders[1].id, 'hold'); await page.click('.soAsk [data-yes]');
    await until(() => vis(page, '.soClear'), 2500, 'clear shows');
    const an = await page.evaluate(() => [...document.querySelectorAll('.soClear,.soRing circle,.soRing path')].map(e => getComputedStyle(e).animationName));
    assert(an.every(x => x === 'none'), 'no animation on the empty state: ' + an);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('8 · reduced motion: cards go and the empty state shows without any animation');
    await ctx.close();
  }

  /* ═════ 9 · the states between: reading, a failed read, many orders, long names, one order ═════ */
  {
    const orders = mkOrders(3);
    const { ctx, page, errors } = await boot(orders);
    await page.evaluate(() => { __so.readDelay = 3500; });
    assert(await openModal(page, { noOrders: true }));
    await page.waitForTimeout(500);
    assert.equal(await page.evaluate(() => document.querySelector('.soDlg').dataset.state), 'loading');
    assert((await text(page, '.soWait')).includes('Finding the orders'), 'a labelled wait'); assert.equal(await page.$$eval('.soWait .soSpin', a => a.length), 1);
    assert((await page.$$('.soSkel')).length >= 1, 'skeleton cards, not a blank');
    await shot(page, 'S13-reading');
    await until(async () => (await cards(page)).length === 3, 9000, "the orders arrive");
    assert.equal(await page.evaluate(() => document.querySelector('.soDlg').dataset.state), 'list');
    assert.equal(await page.$$eval('.soWait', a => a.length), 0, 'the wait is gone');
    ok.push('9 · before the first answer: skeleton cards and a small labelled spinner, then the orders');
    await page.evaluate(() => { SharedOrdersModal.close(); });
    await until(async () => (await page.evaluate(() => !SharedOrdersModal.current())), 6000, 'the first window is gone before the next opens');
    // a failed first read: one short line and a way to try again
    await page.evaluate(() => { __so.readDelay = 0; __so.readFail = true; });
    assert(await openModal(page, { noOrders: true }));
    await until(() => vis(page, '.soErr'), 5000, 'the error line shows');
    assert.equal((await text(page, '.soErr span')), 'The orders could not be read.'); assert.equal(await text(page, '.soErr [data-reload]'), 'Try again');
    await shot(page, 'S14-read-failed');
    await page.evaluate(() => { __so.readFail = false; });
    await page.click('.soErr [data-reload]');
    await until(async () => (await cards(page)).length === 3, 5000, 'Try again reads the orders');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('9 · a failed read: one short line and Try again, then it works');
    await ctx.close();
  }
  {
    // one order, twelve orders, long names, and the sheet's pictures missing: the screenshots Design-critic looks at
    for (const [name, n, o, vp] of [['S1-one-order', 1, {}, null], ['S3-twelve-orders', 12, {}, { width: 1440, height: 900 }], ['S4-long-names', 5, { long: true }, null], ['S15-no-pictures', 3, { noThumbs: true }, null]]) {
      const { ctx, page, errors } = await boot(mkOrders(n, o), vp ? { viewport: vp } : {});
      assert(await openModal(page)); await settled(page);
      assert.equal((await cards(page)).length, n);
      const bad = await page.evaluate(() => [...document.querySelectorAll('.soCard')].filter(c => c.scrollWidth > c.clientWidth + 1).length);
      assert.equal(bad, 0, name + ': no card is cut off');
      if (n === 1) { assert.equal(await text(page, '.soCount'), '1 order'); assert((await text(page, '.soSub')).startsWith('Its pieces')); }
      if (o.noThumbs) assert((await page.$$('.soTile .soPh')).length >= 3 && (await page.$$('.soTile img')).length === 0, 'a calm placeholder where a picture is missing');
      await shot(page, name);
      assert.deepEqual(errors, [], name + ' no page errors: ' + errors.join(' | '));
      await ctx.close();
    }
    ok.push('9 · one order (singular words), twelve orders, long names and missing pictures all lay out without cutting anything off');
  }

  /* ═════ 10 · a sheet that cannot give its piece up is said on the card before anything is pressed ═════ */
  {
    const orders = mkOrders(3);
    orders[1].locked = [{ sheetId: 'ss-s1', sheetLabel: 'SS Sheet 1', why: 'its Rose Gold cut is recorded.' }];
    const { ctx, page, errors } = await boot(orders);
    assert(await openModal(page)); await settled(page);
    assert.equal(await page.$$eval('.soCard .soLocked', a => a.length), 1, 'only the order with a locked sheet says so');
    assert.equal(await text(page, `.soCard[data-order="${orders[1].id}"] .soLocked`), 'Stays on SS Sheet 1: its Rose Gold cut is recorded.');
    assert.equal(await page.$$eval(`.soCard[data-order="${orders[0].id}"] .soLocked`, a => a.length), 0);
    await shot(page, 'S16-locked');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('10 · a sheet that cannot let go (cut, completed, set committed) is said on its card: "Stays on SS Sheet 1: its Rose Gold cut is recorded."');
    await ctx.close();
  }

  await browser.close(); srv.close();
  console.log(ok.map(x => '  ✓ ' + x).join('\n'));
})().catch(e => { console.error(e); process.exit(1); });
