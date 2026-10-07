// The orange Hold button, its consent popup and the glue for Hold and Release hold (Paul, 5 Oct 2026: "Add 1 extra button to both of these places in
// the UI. Orange 'Hold' button ... The user must be clearly informed of the consequences of placing an order on hold with a popup and a final consent
// to continue button in the popup."). charm-nest-hold-ui.js (window.HoldUI), over the real Review cards, order window and Orders > On hold.
// The engine (OrderHold) and the film (OrderHoldFx) are FAKES made here: the real ones are other workers' modules, so this proves only the glue.
// Proves, in headless Chromium against the local fake site (bridge-server.cjs; nothing live, no Etsy, no paid call):
//  - the Hold button is in the Review card (Open and Completed) and on the order window's piece row that carries the Review buttons, once each, the
//    same component (class, orange, words) in both; not on a piece row without buttons; not for a held order, not for a cancelled one, and not at all
//    while the engine is missing
//  - the press shows a small labelled spinner while the plan is read; then ONE popup: the plan's sentences (its own effects, or made from its pieces,
//    sheets and fills), QR labels, sheets already cut, the wait in On hold, nothing deleted; "pieces", never "lines"; Continue and Not now
//  - Not now, Esc and a press outside change nothing (no run, no film, no write); from the order window the window steps aside first (never a pop-up
//    over a pop-up: at most one dialog open at any time) and comes back on the same order and tab
//  - Continue asks the name the way Review does (the inline name bar when none is saved), runs the engine once, feeds every step to the film in order,
//    finishes it and goes home; without a film it still runs and ends in Orders > On hold; a failed run says so plainly and never leaves the Nest tab
//  - a plan that cannot be held shows its reason with Close only
//  - Release hold on an On hold card goes through releasePlan, release and the film, then home; without release it is today's press (Review.repool)
//  - 900 and 390 px: nothing sideways, the button inside its card / row, the popup inside the screen
//   node tests/charm-nest/hold-ui.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const P = { rid: '4170837249', cable: '41708372491', gf: '41708372492', rg: '41708372493' };      // a custom piece in Review and two pieces on sheets
const S = { rid: '4171409071', solo: '41714090711' };                                              // a single custom piece: completed from Review
const H = { rid: '4171409072', solo: '41714090721' };                                              // an order on hold
const X = { rid: '4171409073', solo: '41714090731' };                                              // an order that is cancelled
const GF1 = 'sheet-h3-gf1', RG1 = 'sheet-h3-rg1';
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const chain = (tid, sku) => Object.assign(line(tid, sku, null, ''), { variations: [{ name: 'Charm', value: 'Chain only' }], metalKey: null, metalLabel: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  [order(P.rid, 'Leslie Suhr', [chain(P.cable, 'CABLE CHAIN ONLY'), line(P.gf, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(P.rg, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, pidOf(P, 'gf'), pidOf(P, 'rg')]],
  [order(S.rid, 'Sam Solo', [chain(S.solo, 'CHAIN ONLY 5601')]), [null]],
  [order(H.rid, 'Hana Held', [chain(H.solo, 'HELD CHAIN ONLY 5602')]), [null]],
  [order(X.rid, 'Xia Cancelled', [chain(X.solo, 'CANCELLED CHAIN ONLY 5603')]), [null]]];
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };`;

// the plan the fake engine gives for order P: two pieces on two sheets, two spots filled (one from a waiting order, one from a newer sheet), one sheet already cut
const PLAN = rid => ({ rid, label: 'CABLE CHAIN ONLY', customer: 'Leslie Suhr', shipBy: SHIP, canHold: true, blockedWhy: null,
  pieces: [{ poolId: pidOf(P, 'gf'), lineKey: kOf(P, 'gf'), label: 'MIDDLE 9935', metal: 'gold', state: 'onSheet', sheetId: GF1, sheetLabel: 'GF Sheet 1', setId: 's1', setLabel: 'Set 1' },
    { poolId: pidOf(P, 'rg'), lineKey: kOf(P, 'rg'), label: 'MIDDLE 9935', metal: 'rose', state: 'onSheet', sheetId: RG1, sheetLabel: 'RG Sheet 1', setId: 's1', setLabel: 'Set 1' },
    { poolId: 'cut-1', lineKey: 'cut', label: 'CABLE', metal: 'gold', state: 'onCutSheet', sheetId: 'gf3', sheetLabel: 'GF Sheet 3', setId: 's0', setLabel: 'Set 0' }],
  sheets: [{ sheetId: GF1, label: 'GF Sheet 1', setLabel: 'Set 1', metal: 'gold', removes: 1, qrRemade: true }, { sheetId: RG1, label: 'RG Sheet 1', setLabel: 'Set 1', metal: 'rose', removes: 1, qrRemade: true }],
  fills: [{ sheetId: GF1, sheetLabel: 'GF Sheet 1', spots: 1, source: 'waiting', fromSheetId: null, fromSheetLabel: null, orders: 1 }, { sheetId: RG1, sheetLabel: 'RG Sheet 1', spots: 1, source: 'newerSheet', fromSheetId: 'rg2', fromSheetLabel: 'RG Sheet 2', orders: 1 }],
  stays: [{ poolId: 'cut-1', label: 'CABLE', sheetLabel: 'GF Sheet 3', why: 'already cut' }], effects: [] });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  – no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [P.rid], placements: [box('g1', 60, 60)], charms: [ch('g1', P, 'gf', 'MIDDLE_9935')] });
  srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [P.rid], placements: [box('r1', 60, 60)], charms: [ch('r1', P, 'rg', 'MIDDLE_9935')] });
  const poolRow = (o, t, sku, material, sheetId, metal) => srv.st.put('Charm_Pool', pidOf(o, t), { poolId: pidOf(o, t), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: `2026-10-04_${metal}_Set-1_Sheet-1`, updatedAt: Date.now() });
  poolRow(P, 'gf', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(P, 'rg', 'MIDDLE_9935', 'rose', RG1, 'RG');
  const evAt = Date.now(); let evN = 0;
  const ev = (o, type, ago, extra) => srv.st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  ev(P, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(P, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(P, 'gf'), data: { poolId: pidOf(P, 'gf') } });
  ev(P, 'placed', 2900, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(P, 'rg'), data: { poolId: pidOf(P, 'rg') } });
  for (const o of [S, H, X]) ev(o, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      if (/etsy/i.test(u)) outside.push(u);
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => { window.__prompted = (window.__prompted || 0) + 1; return 'Prompted Name'; }; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && window.HoldUI && CN.S.cloud.ok === true, null, { timeout: 60000 });
    // the dialogs open at once, ever (never a pop-up over a pop-up)
    await page.evaluate(() => { window.__dlg = { max: 0 }; const look = () => { const n = document.querySelectorAll('dialog[open]').length; if (n > window.__dlg.max) window.__dlg.max = n; }; new MutationObserver(look).observe(document.documentElement, { subtree: true, attributes: true, attributeFilter: ['open'], childList: true }); });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      B.master.entries.set('MIDDLE_9935', { sku: 'MIDDLE_9935', updatedAt: 1 });
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: ORDERS });
    await page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow').length >= 4, null, { timeout: 20000 });

    const shot = async name => { if (shots) { await page.waitForTimeout(700); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
    const idle = () => page.evaluate(() => Seal.whenIdle());
    const seg = async which => { await idle(); await page.evaluate(w => { document.querySelector(`#reviewView .rvSeg [data-cseg="${w}"]`).click(); }, which); };
    const holdBtns = rid => page.evaluate(r => [...document.querySelectorAll(`[data-hold-btn][data-rid="${r}"]`)].map(b => ({ text: b.textContent.trim(), cls: b.className, src: b.dataset.src, hidden: b.hidden || b.offsetParent === null, where: b.closest('[data-pc-act], [data-pc-hold]') ? 'row' : b.closest('.rowActions') ? 'card' : 'other' })), rid);
    const cardHold = key => page.evaluate(k => { const n = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); return n ? n.querySelectorAll('[data-hold-btn]').length : -1; }, key);
    const dlgText = () => page.evaluate(() => { const d = document.querySelector('dialog.holdDlg[open], .holdInline'); return d ? d.innerText : null; });
    const writes = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && /^(poolPut|poolUpdate|putSheet|setUpdate|cancelPut|customPut|customReopen|runPut)$/.test(String(c.op))).length;
    const openWin = async key => { await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(k => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length >= 1, key, { timeout: 20000 }); };
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }); };
    const cable = kOf(P, 'cable'), gf = kOf(P, 'gf'), solo = kOf(S, 'solo'), held = kOf(H, 'solo'), gone = kOf(X, 'solo');
    const card = k => `#rvList .reviewListRow[data-row="${k}"]`;

    // ── the fake engine and film (the real ones are other workers' modules: only the glue is proved here) ──
    const install = (opts = {}) => page.evaluate(({ plan, noFx, noRelease }) => {
      const sleep = ms => new Promise(r => setTimeout(r, ms));
      const T = window.__t = { plans: [], runs: [], rels: [], relPlans: [], fx: [], returns: [], gate: null, override: null, runResult: null, relPlanResult: null, effects: null };
      const rowsOf = rid => Orders.rows().filter(r => String(r.order.receiptId) === String(rid));
      const eng = {
        plan: async (rid, o) => { T.plans.push({ rid, src: o && o.source }); if (T.gate) await T.gate.promise; const p = Object.assign(JSON.parse(JSON.stringify(plan)), { rid }, T.override || {}); if (T.effects) p.effects = T.effects; return p; },
        run: async (rid, o) => {
          T.runs.push({ rid, name: o && o.name });
          const steps = [{ type: 'start', rid, sheets: ['a', 'b'] }, { type: 'sheetBegin', sheetId: 'a', label: 'GF Sheet 1' }, { type: 'lift', sheetId: 'a', poolIds: ['p1'], rects: [], label: 'GF Sheet 1' }, { type: 'removed', sheetId: 'a', removed: 1 }, { type: 'sheetDone', sheetId: 'a', charmCount: 3, density: .5 }, { type: 'held', rid }];
          for (const s of steps) { s.at = Date.now(); o.onStep(s); await sleep(4); }
          if (T.runResult && T.runResult.ok === false) return T.runResult;
          for (const r of rowsOf(rid)) { r.hold = 'Taken off GF Sheet 1, RG Sheet 1 by ' + o.name + ': Add to next sheet'; r.state = 'held'; }
          o.onStep({ type: 'done', at: Date.now() });
          return { ok: true, held: true, steps };
        },
        status: () => ({ running: false })
      };
      if (!noRelease) {
        eng.releasePlan = async rid => { T.relPlans.push(rid); return T.relPlanResult || { rid, canRelease: true }; };
        eng.release = async (rid, o) => {
          T.rels.push({ rid, name: o && o.name });
          const steps = [{ type: 'start', rid }, { type: 'queued', rid, front: true }, { type: 'target', sheetId: 'a', label: 'GF Sheet 1', spot: 1, newSheet: false }, { type: 'placed', sheetId: 'a', poolIds: ['p1'] }, { type: 'released', rid }];
          for (const s of steps) { s.at = Date.now(); o.onStep(s); await sleep(4); }
          for (const r of rowsOf(rid)) { r.hold = null; r.state = 'pulled'; }
          o.onStep({ type: 'done', at: Date.now() });
          return { ok: true, released: true };
        };
      }
      window.OrderHold = eng;
      const film = kind => (rid, source) => {
        const rec = { kind, rid, source: { name: source && source.name, hasPlan: !!(source && source.plan) }, pushed: [], finished: false, skipped: false };
        T.fx.push(rec); CN.setMode('nest');   // (the real film takes the person to the Nest tab at once)
        let done; const p = new Promise(r => { done = r; });
        return { push: s => rec.pushed.push(s.type), finish: () => { rec.finished = true; setTimeout(done, 25); }, skip: () => { rec.skipped = true; }, done: p };
      };
      if (!noFx) window.OrderHoldFx = { playHold: film('hold'), playRelease: film('release'), returnToOnHold: rid => { T.returns.push(rid); CN.setMode('orders'); Orders.showPile('hold', ''); } };
      else delete window.OrderHoldFx;
    }, { plan: PLAN('x'), noFx: !!opts.noFx, noRelease: !!opts.noRelease });
    const installFake = install;
    const T = () => page.evaluate(() => JSON.parse(JSON.stringify(Object.assign({}, window.__t, { planFor: undefined, gate: undefined }))));

    // ── 1 · no engine, no button (the page's own engine, when its module is in, is taken away for this check) ──
    if (await page.evaluate(() => !!(window.OrderHold && OrderHold.plan && OrderHold.run))) assert.equal(await page.evaluate(() => HoldUI.available()), true, 'the real engine is on the page: the button is offered');
    await page.evaluate(() => { window.__real = { hold: window.OrderHold || null, fx: window.OrderHoldFx || null }; delete window.OrderHold; delete window.OrderHoldFx; Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(() => !document.querySelector('#rvList [data-hold-btn]'), null, { timeout: 10000 });
    assert.deepEqual(await holdBtns(P.rid), [], 'without the engine there is no Hold button in Review');
    assert.equal(await page.evaluate(() => HoldUI.available()), false);

    // ── 2 · the button: the Review card (open), the order window's piece row, the same component ──
    await installFake();
    await page.evaluate(() => { Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-hold-btn]`), cable, { timeout: 15000 });
    let b = await holdBtns(P.rid);
    assert.equal(b.length, 1, 'one Hold button for the order in Review: ' + JSON.stringify(b));
    assert(b[0].text === 'Hold' && b[0].where === 'card' && b[0].src === 'review' && /\bbtn\b/.test(b[0].cls) && /\bholdBtn\b/.test(b[0].cls), 'the card\'s Hold: ' + JSON.stringify(b[0]));
    const look = sel => page.evaluate(s => { const x = document.querySelector(s), c = getComputedStyle(x); return { bg: c.backgroundColor, color: c.color, radius: c.borderRadius, fs: c.fontSize, h: Math.round(x.getBoundingClientRect().height) }; }, sel);
    const rv = await look(`${card(cable)} [data-hold-btn]`);
    assert.equal(rv.bg, 'rgb(162, 89, 28)', 'the app\'s own warning orange: ' + JSON.stringify(rv)); assert.equal(rv.color, 'rgb(255, 255, 255)');
    const order0 = await page.evaluate(k => [...document.querySelectorAll(`#rvList .reviewListRow[data-row="${k}"] .rowActions button`)].map(x => x.textContent.trim()), cable);
    assert(order0.includes('Print QR label') && order0.includes('Complete Order') && order0[order0.length - 1] === 'Hold', 'with the other action buttons, last: ' + JSON.stringify(order0));
    await shot('review-open-1440');
    await openWin(cable);
    await page.waitForFunction(() => document.querySelector('#owPcSum [data-hold-btn]'), null, { timeout: 15000 });
    b = (await holdBtns(P.rid)).filter(x => x.where === 'row');
    assert.equal(b.length, 3, 'one Hold button on each of the order window\'s three piece rows: ' + JSON.stringify(b));
    assert(b.every(x => x.text === 'Hold' && x.src === 'orderWindow' && /\bholdBtn\b/.test(x.cls)) && b[0].text === 'Hold' && b[0].src === 'orderWindow' && /\bholdBtn\b/.test(b[0].cls), JSON.stringify(b[0]));
    const rw = await look('#owPcSum [data-hold-btn]'); assert.equal(rw.bg, rv.bg, 'the same orange in both places'); assert.equal(rw.color, rv.color);
    const onRow = await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ key: r.dataset.piece, hold: r.querySelectorAll('[data-hold-btn]').length, btns: [...r.querySelectorAll('.pcAct button')].map(x => x.textContent.trim()) })));
    assert.deepEqual(onRow.map(r => r.hold), [1, 1, 1], 'on every piece row, the pieces on sheets (no Review card) too: ' + JSON.stringify(onRow));
    assert.deepEqual(onRow.slice(1).map(r => r.btns), [['Hold'], ['Hold']], 'a piece on a sheet has the Hold alone at the right end of its row: ' + JSON.stringify(onRow));
    assert.deepEqual(onRow[0].btns, ['Print QR label', 'Complete Order', 'Hold'], JSON.stringify(onRow[0]));
    const same = await page.evaluate(() => { const a = document.querySelector('#owPcSum [data-hold-btn]'), c = document.querySelector('#rvList [data-hold-btn]'); const strip = n => n.className.replace(/\b(xs|sm)\b/g, '').replace(/\s+/g, ' ').trim(); return { a: strip(a), c: strip(c), same: a.title === c.title }; });
    assert.equal(same.a, same.c, 'one component, two places: ' + JSON.stringify(same));
    await shot('orderwin-row-1440');

    // ── 3 · the press: a small labelled spinner while the plan is read, then ONE popup with the plan's sentences ──
    await page.evaluate(() => { let r; window.__t.gate = { promise: new Promise(x => { r = x; }), release: r }; });
    const w0 = writes();
    await page.click('#owPcSum [data-hold-btn]');
    await page.waitForFunction(() => { const b = document.querySelector('#owPcSum [data-hold-btn]'); return b && b.querySelector('.spin') && /Checking sheets/.test(b.textContent) && b.disabled && b.getAttribute('aria-busy') === 'true'; }, null, { timeout: 8000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog.holdDlg[open]').length), 0, 'no popup while the plan is read');
    await page.evaluate(() => window.__t.gate.release());
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), false, 'the order window stepped aside first');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one dialog open: never a pop-up over a pop-up');
    let text = await dlgText();
    for (const want of [`Put order ${P.rid} on hold?`, 'CABLE CHAIN ONLY', 'Leslie Suhr', '2 pieces come off GF Sheet 1 and RG Sheet 1.', '1 waiting order and 1 order from RG Sheet 2 fill the 2 empty spots.', 'QR labels are made again on 2 sheets.', 'Sheets already cut keep their pieces: GF Sheet 3.', 'The order waits in On hold until someone presses Release hold.', 'Nothing is deleted.', 'Continue', 'Not now'])
      assert(text.includes(want), `the popup says "${want}": ` + text);
    assert(!/\blines?\b/i.test(text), 'pieces, never lines: ' + text);
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('dialog.holdDlg [data-k]')].map(x => x.textContent.trim())), ['Not now', 'Continue'], 'two buttons');
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.textContent.trim()), 'Not now', 'the safe button has the focus');
    await shot('popup-1440');
    // 3a · Not now: the order window comes back as it was; nothing ran, nothing was written
    await page.click('dialog.holdDlg [data-k=no]');
    await page.waitForFunction(() => !document.querySelector('dialog.holdDlg') && document.getElementById('orderWin').open && OrderWin.isOpen(), null, { timeout: 15000 });
    assert.equal(await page.evaluate(k => OrderWin.key() === k, cable), true, 'the same order is on screen again');
    let t = await T(); assert.equal(t.runs.length, 0, 'Not now: no run'); assert.equal(t.fx.length, 0, 'no film'); assert.equal(writes() - w0, 0, 'nothing written');
    assert.equal(await page.evaluate(() => window.__dlg.max), 1, 'never two dialogs at once');
    await page.waitForFunction(() => { const b = document.querySelector('#owPcSum [data-hold-btn]'); return b && !b.disabled && b.textContent.trim() === 'Hold'; }, null, { timeout: 8000 });
    await closeWin();

    // 3b · Esc and a press outside are Not now too (from the Review card: no window to hand off)
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'), null, { timeout: 8000 });
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    await page.mouse.click(4, 4);
    await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'), null, { timeout: 8000 });
    t = await T(); assert.equal(t.runs.length, 0, 'Esc and outside: no run'); assert.equal(t.fx.length, 0); assert.equal(writes() - w0, 0);
    assert.equal(t.plans.length, 3, 'the plan was read once per press');

    // ── 3c · EVERY piece row has the Hold, the plain rows too (pieces on sheets, in no Review card): the same button, one press, one consent ──
    await openWin(cable);
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow [data-hold-btn]').length === 3, null, { timeout: 15000 });
    const plainRows = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => {
      const h = r.querySelector('[data-hold-btn]'), st = r.querySelector('.st'), rr = r.getBoundingClientRect(), nm = r.querySelector('.nm').getBoundingClientRect(), dots = r.querySelector('.steps').getBoundingClientRect(), hr = h ? h.getBoundingClientRect() : null;
      const wr = st && !st.classList.contains('owPcSr') ? st.getBoundingClientRect() : null;
      return { key: r.dataset.piece, tag: r.tagName, plain: r.classList.contains('hasHold'), solo: r.classList.contains('solo'), holds: r.querySelectorAll('[data-hold-btn]').length, text: h && h.textContent.trim(), src: h && h.dataset.src, inAct: !!(h && h.closest('.pcAct')),
        nested: !!(h && h.closest('button:not([data-hold-btn])')), words: wr ? st.textContent.trim() : null, under: !!wr && wr.top >= nm.bottom - 1, wordsBeforeHold: !!wr && !!hr && wr.right <= hr.left + 1,
        rightOfName: !!hr && hr.left >= nm.right - 1, beforeDots: !!hr && hr.right <= dots.left + 1, inside: !!hr && hr.left >= rr.left - 1 && hr.right <= rr.right + 1 && hr.width > 20, clip: r.scrollWidth > r.clientWidth + 1,
        over: document.documentElement.scrollWidth > innerWidth + 1, h: Math.round(rr.height), sameLine: !!hr && Math.abs((hr.top + hr.bottom) / 2 - (nm.top + nm.bottom) / 2) <= 6 };
    }));
    let pr = await plainRows();
    assert.deepEqual(pr.map(r => r.key), [cable, gf, kOf(P, 'rg')], 'the three pieces');
    assert.deepEqual(pr.map(r => r.holds), [1, 1, 1], 'one Hold on every piece row: ' + JSON.stringify(pr));
    for (const r of pr.slice(1)) assert(r.tag === 'DIV' && r.plain && r.text === 'Hold' && r.src === 'orderWindow' && r.inAct && !r.nested && r.words && r.rightOfName && r.beforeDots && r.inside && !r.clip && !r.over, 'a plain row: its words, its Hold at the right end, no button inside a button: ' + JSON.stringify(r));
    assert(pr.slice(1).every(r => r.under || r.wordsBeforeHold), 'the Hold sits right of the words: ' + JSON.stringify(pr));
    assert.equal(await page.evaluate(() => document.querySelectorAll('#owPcSum button button').length), 0, 'no button inside a button');
    // the press on a plain row's Hold: the plan read ONCE, ONE popup, the order window stepped aside, nothing runs
    const w1 = writes(), p0 = (await T()).plans.length;
    await page.click(`#owPcSum [data-piece="${gf}"] [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 }); await page.waitForTimeout(400);
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog.holdDlg[open]').length), 1, 'ONE consent popup from the plain row');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one dialog open: never a pop-up over a pop-up');
    assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), false, 'the order window stepped aside first');
    t = await T(); assert.equal(t.plans.length - p0, 1, 'the plan was read once'); assert.equal(t.plans[t.plans.length - 1].rid, P.rid, 'for the whole order'); assert.equal(t.runs.length, 0, 'nothing runs before Continue');
    assert((await dlgText()).includes(`Put order ${P.rid} on hold?`), 'it asks about the order');
    await shot('plain-row-popup-1440');
    await page.click('dialog.holdDlg [data-k=no]');
    await page.waitForFunction(() => !document.querySelector('dialog.holdDlg') && OrderWin.isOpen(), null, { timeout: 15000 });
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow [data-hold-btn]').length === 3 && ![...document.querySelectorAll('#owPcSum [data-hold-btn]')].some(b => b.disabled), null, { timeout: 8000 });
    t = await T(); assert.equal(t.runs.length, 0, 'Not now: no run'); assert.equal(writes() - w1, 0, 'nothing written');
    // a press on the row itself (its name, not the Hold or the sheet chip) still picks that piece: every row stays, the piece's is marked and keeps its Hold
    await page.click(`#owPcSum [data-piece="${gf}"] .owPcName`);
    await page.waitForFunction(k => OrderWin.selectedPiece() === k && document.querySelectorAll('#owPcSum .owPcRow').length === 3 && document.querySelector(`#owPcSum .owPcRow.sel[data-piece="${k}"] [data-hold-btn]`), gf, { timeout: 15000 });
    pr = await plainRows();
    assert(pr.length === 3 && pr.every(r => r.holds === 1 && r.inAct && r.rightOfName && r.beforeDots && r.inside && !r.clip) && pr.slice(1).every(r => r.plain), 'one piece picked, in no card: every row stays with its Hold: ' + JSON.stringify(pr));
    await shot('plain-row-picked-1440');
    await page.click('#owPcSum [data-pc-all]');
    await page.waitForFunction(() => OrderWin.selectedPiece() === null && document.querySelectorAll('#owPcSum .owPcRow').length === 3 && !document.querySelector('#owPcSum .owPcRow.solo'), null, { timeout: 15000 });
    await closeWin();

    // ── 4 · the plan's own sentences (effects) are shown as given ──
    await page.evaluate(() => { window.__t.effects = ['4 pieces come off GF Sheet 2 and SS Sheet 1.', '3 waiting orders and 1 order from GF Sheet 3 fill the 4 empty spots.']; });
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    text = await dlgText();
    for (const want of ['4 pieces come off GF Sheet 2 and SS Sheet 1.', '3 waiting orders and 1 order from GF Sheet 3 fill the 4 empty spots.', 'QR labels are made again on 2 sheets.', 'Sheets already cut keep their pieces', 'Nothing is deleted.']) assert(text.includes(want), `the popup says "${want}": ` + text);
    assert(!text.includes('2 pieces come off'), 'the plan\'s sentences stand in place of the made ones');
    await page.click('dialog.holdDlg [data-k=no]'); await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'));
    await page.evaluate(() => { window.__t.effects = null; });

    // ── 4b · the orders that move in are counted ONCE each, by order id: ONE order that fills a spot on each of two sheets reads "1 order", not one for each sheet ──
    const popupSays = async fills => {
      await page.evaluate(f => { window.__t.override = { fills: f }; }, fills);
      await page.click(`${card(cable)} [data-hold-btn]`);
      await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
      const said = await dlgText();
      await page.click('dialog.holdDlg [data-k=no]'); await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'));
      await page.evaluate(() => { window.__t.override = null; });
      return said;
    };
    const fill = (sheetId, sheetLabel, from, label, rids, source = 'newerSheet') => ({ sheetId, sheetLabel, spots: 1, source, fromSheetId: from, fromSheetLabel: label, orders: rids.length, rids });
    text = await popupSays([fill(GF1, 'GF Sheet 1', 'gf3', 'GF Sheet 3', ['4170000777']), fill(RG1, 'RG Sheet 1', 'rg2', 'RG Sheet 2', ['4170000777'])]);
    assert(text.includes('1 order from GF Sheet 3 and RG Sheet 2 fills the 2 empty spots.'), 'one order on two sheets reads "1 order ... fills": ' + text);
    assert(!/\b2 orders\b/.test(text), 'and never "2 orders": ' + text);
    text = await popupSays([fill(GF1, 'GF Sheet 1', 'gf3', 'GF Sheet 3', ['4170000777']), fill(RG1, 'RG Sheet 1', 'rg2', 'RG Sheet 2', ['4170000888'])]);
    assert(text.includes('2 orders from GF Sheet 3 and RG Sheet 2 fill the 2 empty spots.'), 'two different orders read "2 orders ... fill": ' + text);
    text = await popupSays([fill(GF1, 'GF Sheet 1', null, null, ['4170000777'], 'waiting'), fill(RG1, 'RG Sheet 1', null, null, ['4170000777'], 'waiting')]);
    assert(text.includes('1 waiting order fills the 2 empty spots.'), 'one waiting order on two sheets reads "1 waiting order fills": ' + text);

    // ── 5 · a plan that cannot be held: its reason, Close only ──
    await page.evaluate(() => { window.__t.override = { canHold: false, blockedWhy: 'Undo the set first: a piece of this order is inside a committed set.' }; });
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    text = await dlgText();
    assert(text.includes('Undo the set first') && text.includes("can't be put on hold yet"), 'its reason, plainly: ' + text);
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('dialog.holdDlg [data-k]')].map(x => x.textContent.trim())), ['Close'], 'Close only');
    assert(!/Continue|Not now/.test(text), 'no consent to give');
    await shot('popup-blocked-1440');
    await page.click('dialog.holdDlg [data-k=no]'); await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'));
    t = await T(); assert.equal(t.runs.length, 0);
    await page.evaluate(() => { window.__t.override = null; });

    // ── 5b · a window that cannot step aside (the sheet window under an order window, say): the popup is drawn on its surface, never over it ──
    await page.evaluate(() => { document.getElementById('dlgSettings').showModal(); });
    await page.evaluate(plan => { window.__asked = HoldUI.confirm(plan).then(v => { window.__answer = v; return v; }); }, PLAN(P.rid));
    await page.waitForFunction(() => document.querySelector('#dlgSettings .holdInline'), null, { timeout: 8000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog.holdDlg').length), 0, 'no second dialog');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 1, 'one dialog open');
    assert(/Continue/.test(await dlgText()) && /Not now/.test(await dlgText()), 'the same words, inside it');
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => window.__answer === false && !document.querySelector('.holdInline'), null, { timeout: 8000 });
    assert.equal(await page.evaluate(() => document.getElementById('dlgSettings').open), true, 'Esc put the question away, not the window under it');
    await page.evaluate(() => document.getElementById('dlgSettings').close());

    // ── 6 · layout: the card, the row and the popup at 900 and 390 (the dark rail folds below 900 px, as the page's own toggle does) ──
    const fold = w => page.evaluate(w => { const off = document.getElementById('app').classList.contains('railOff'); if ((w < 900) !== off) document.getElementById('btnRail').click(); }, w);
    const fit = () => page.evaluate(k => {
      const out = { over: document.documentElement.scrollWidth > innerWidth + 1 };
      const c = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); c.scrollIntoView({ block: 'center' });
      const cr = c.getBoundingClientRect(), hb = c.querySelector('[data-hold-btn]').getBoundingClientRect(), others = [...c.querySelectorAll('.rowActions button')].filter(x => !x.hasAttribute('data-hold-btn')).map(x => x.getBoundingClientRect());
      out.card = { rects: [cr, hb].map(r => [r.left, r.top, r.right, r.bottom].map(Math.round)), inside: hb.left >= cr.left - 1 && hb.right <= cr.right + 1 && hb.top >= cr.top - 1 && hb.bottom <= cr.bottom + 1 && hb.width > 20, apart: others.every(o => o.right <= hb.left + 1 || o.left >= hb.right - 1 || o.bottom <= hb.top + 1 || o.top >= hb.bottom - 1), h: Math.round(cr.height) };
      return out;
    }, cable);
    for (const [w, h] of [[900, 800], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await fold(w); await page.waitForTimeout(500);
      const f = await fit(); await shot(`review-open-${w}`);
      assert(!f.over, `${w}: no sideways scroll of the page`); assert(f.card.inside && f.card.apart, `${w}: the Hold button is inside its card, clear of the others: ` + JSON.stringify(f.card));
      await openWin(cable); await page.waitForTimeout(500);
      const g = await page.evaluate(() => {
        const sum = document.getElementById('owPcSum'); sum.scrollIntoView({ block: 'center' });
        const row = sum.querySelector('.owPcRow.hasAct'), rr = row.getBoundingClientRect(), hb = row.querySelector('[data-hold-btn]').getBoundingClientRect(), nm = row.querySelector('.nm').getBoundingClientRect();
        return { over: document.documentElement.scrollWidth > innerWidth + 1, inside: hb.left >= rr.left - 1 && hb.right <= rr.right + 1 && hb.width > 20, clip: row.scrollWidth > row.clientWidth + 1, nameW: Math.round(nm.width), rowH: Math.round(rr.height), sameLine: Math.abs((hb.top + hb.bottom) / 2 - (nm.top + nm.bottom) / 2) <= (innerWidth > 700 ? 6 : 90) };
      });
      assert(!g.over && g.inside && !g.clip && g.nameW >= 60, `${w}: the piece row holds the button (${JSON.stringify(g)})`);
      if (w > 700) assert(g.sameLine && g.rowH <= 46, `${w}: on the name's line (${JSON.stringify(g)})`);
      await shot(`orderwin-row-${w}`);
      // 6b · the plain rows (pieces on sheets): their words, their Hold at the right end, wrapping neatly, no sideways scroll
      const pf = await plainRows();
      assert.deepEqual(pf.map(r => r.holds), [1, 1, 1], `${w}: a Hold on every row: ` + JSON.stringify(pf));
      for (const r of pf) assert(r.inside && !r.clip && !r.over && r.rightOfName && r.beforeDots && (r.sameLine || !r.plain), `${w}: the row holds its Hold at the right end of the name's line: ` + JSON.stringify(r));
      for (const r of pf.slice(1)) assert(r.words && r.plain, `${w}: a plain row keeps its words: ` + JSON.stringify(r));
      assert(Math.max(...pf.map(r => r.h)) <= (w > 700 ? 80 : 110), `${w}: the rows do not grow ugly (${pf.map(r => r.h)}px)`);
      const sw = await page.evaluate(() => { const d = document.getElementById('orderWin'), s = document.getElementById('owPcSum'), dr = d.getBoundingClientRect(), sr = s.getBoundingClientRect();
        const out = [...s.querySelectorAll('*')].filter(n => n.offsetParent !== null && n.getBoundingClientRect().width > 0 && (n.getBoundingClientRect().right > sr.right + 1 || n.getBoundingClientRect().left < sr.left - 1)).slice(0, 5).map(n => n.tagName + '.' + n.className + ' ' + Math.round(n.getBoundingClientRect().left) + '-' + Math.round(n.getBoundingClientRect().right));
        return { sum: [s.scrollWidth, s.clientWidth], out, sr: [Math.round(sr.left), Math.round(sr.right)], dr: [Math.round(dr.left), Math.round(dr.right)], page: document.documentElement.scrollWidth > innerWidth + 1 }; });
      assert(sw.sum[0] <= sw.sum[1] + 1 && sw.out.length === 0 && !sw.page && sw.sr[1] <= sw.dr[1] + 1, `${w}: nothing of the piece rows sticks out sideways: ` + JSON.stringify(sw));
      await shot(`orderwin-plain-rows-${w}`);
      await closeWin();
      await page.click(`${card(cable)} [data-hold-btn]`);
      await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 }); await page.waitForTimeout(700);
      const p = await page.evaluate(() => { const d = document.querySelector('dialog.holdDlg'), r = d.getBoundingClientRect(), foot = [...d.querySelectorAll('.dlgFoot button')].map(x => x.getBoundingClientRect()); return { fits: r.left >= 0 && r.right <= innerWidth + 0.5 && r.top >= 0 && r.bottom <= innerHeight + 0.5, noScroll: d.scrollWidth <= d.clientWidth + 1, buttons: foot.length === 2 && foot.every(x => x.width > 30 && x.left >= r.left && x.right <= r.right + 0.5) }; });
      assert(p.fits && p.noScroll && p.buttons, `${w}: the popup is inside the screen: ` + JSON.stringify(p));
      await shot(`popup-${w}`);
      await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'));
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await fold(1440); await page.waitForTimeout(400);

    // ── 7 · Continue: the name bar when none is saved, then one run, every step to the film in order, home ──
    await page.evaluate(() => { B.employee = ''; });
    assert.equal(await page.evaluate(() => CNEmployee.name()), '', 'no name saved');
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    await page.click('dialog.holdDlg [data-k=go]');
    await page.waitForFunction(() => document.querySelector('.cnNameBar input'), null, { timeout: 10000 });
    assert.equal(await page.evaluate(() => window.__prompted || 0), 0, 'the inline name bar, never prompt()');
    assert.equal((await T()).runs.length, 0, 'nothing runs before the name is given');
    await shot('namebar-1440');
    await page.fill('.cnNameBar input', 'Paul'); await page.keyboard.press('Enter');
    await page.waitForFunction(() => window.__t.returns.length === 1, null, { timeout: 20000 });
    t = await T();
    assert.equal(t.runs.length, 1, 'the engine ran once'); assert.equal(t.runs[0].rid, P.rid); assert.equal(t.runs[0].name, 'Paul', 'under the name given');
    assert.equal(t.fx.length, 1, 'one film'); assert.equal(t.fx[0].kind, 'hold'); assert.equal(t.fx[0].rid, P.rid);
    assert.deepEqual(t.fx[0].pushed, ['start', 'sheetBegin', 'lift', 'removed', 'sheetDone', 'held', 'done'], 'every step reached the film, in order');
    assert(t.fx[0].finished, 'the film was told it was finished'); assert.deepEqual(t.returns, [P.rid], 'and the person was taken home once');
    assert.equal(t.fx[0].source.name, 'Paul'); assert.equal(t.fx[0].source.hasPlan, true);
    await page.waitForFunction(() => !HoldUI.busy('4170837249'), null, { timeout: 8000 });
    assert.deepEqual(await holdBtns(P.rid), [], 'a held order has no Hold button any more');
    assert.deepEqual(await page.evaluate(() => ({ mode: CN.S.mode, pile: Orders.view().pile })), { mode: 'orders', pile: 'hold' }, 'back in Orders > On hold');
    assert.equal(await page.evaluate(() => window.__dlg.max), 1, 'never two dialogs at once, all the way through');

    // ── 8 · held and cancelled orders have no button; an order released shows it again ──
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), solo, { timeout: 15000 });
    await page.evaluate(([h]) => { for (const r of Orders.rows()) if (String(r.order.receiptId) === h) { r.hold = 'Taken off SS Sheet 1, GF Sheet 2 by Paul: Add to next sheet'; r.state = 'held'; } Review.syncOrderItems(); Review.render(); }, [H.rid]);
    await page.waitForTimeout(300);
    await page.waitForFunction(h => !document.querySelector(`[data-hold-btn][data-rid="${h}"]`), H.rid, { timeout: 12000 });   // (a card leaving flies out first, as a copy of itself)
    assert.deepEqual(await holdBtns(H.rid), [], 'a held order has no Hold button in Review');
    assert.equal(await page.evaluate(r => HoldUI.shown(r), H.rid), false);
    assert.equal(await page.evaluate(r => HoldUI.shown(r), X.rid), true, 'before it is cancelled the order has one');
    assert((await cardHold(gone)) === 1, 'its card shows it');
    await page.evaluate(([x]) => { Cancelled.absorb([x]); Review.syncOrderItems(); Review.render(); }, [X.rid]);
    await page.waitForFunction(x => !document.querySelector(`[data-hold-btn][data-rid="${x}"]`), X.rid, { timeout: 12000 });
    assert.deepEqual(await holdBtns(X.rid), [], 'a cancelled order has no Hold button');
    assert.equal(await page.evaluate(r => HoldUI.shown(r), X.rid), false);
    assert.equal(await page.evaluate(r => HoldUI.slot({ rid: r, source: 'review' }), X.rid), '', 'and no place for one');
    await page.evaluate(k => OrderWin.open(k), held); await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, held, { timeout: 15000 }); await page.waitForTimeout(900);
    assert.deepEqual((await holdBtns(H.rid)), [], 'a held order has none in the order window either');
    await closeWin();

    // ── 9 · a completed card has the button too ──
    await idle(); await page.click(`${card(solo)} [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], solo, { timeout: 20000 });
    await page.waitForFunction(k => !document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), solo, { timeout: 15000 });
    await seg('done'); await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), solo, { timeout: 15000 });
    await idle(); await page.waitForTimeout(600);
    const done0 = await page.evaluate(k => { const n = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); return { q: n.querySelector('.queueLabel').textContent.trim(), btns: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()), holds: n.querySelectorAll('[data-hold-btn]').length }; }, solo);
    assert(/completed/i.test(done0.q) && done0.holds === 1 && done0.btns.includes('Reopen') && done0.btns.includes('Hold'), 'the completed card: ' + JSON.stringify(done0));
    await page.waitForFunction(r => document.querySelectorAll(`[data-hold-btn][data-rid="${r}"]`).length === 1, S.rid, { timeout: 10000 }).catch(async () => { throw new Error('once: ' + JSON.stringify(await holdBtns(S.rid))); });   // (the card's flight to Completed leaves a copy for a moment)
    await shot('review-completed-1440');
    // (a wide screen, Paul's: the card's buttons stand in a column at the right)
    await page.setViewportSize({ width: 2000, height: 600 }); await fold(2000); await page.waitForTimeout(600);
    const wide = await page.evaluate(k => { const c = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); c.scrollIntoView({ block: 'center' }); const cr = c.getBoundingClientRect(), hb = c.querySelector('[data-hold-btn]').getBoundingClientRect(); return { over: document.documentElement.scrollWidth > innerWidth + 1, inside: hb.left >= cr.left - 1 && hb.right <= cr.right + 1 && hb.bottom <= cr.bottom + 1 && hb.top >= cr.top - 1, h: Math.round(cr.height) }; }, solo);
    await shot('review-completed-2000'); assert(!wide.over && wide.inside, '2000: the completed card holds its button: ' + JSON.stringify(wide));
    await page.setViewportSize({ width: 390, height: 844 }); await fold(390); await page.waitForTimeout(500);
    const f2 = await page.evaluate(k => { const c = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); c.scrollIntoView({ block: 'center' }); const cr = c.getBoundingClientRect(), hb = c.querySelector('[data-hold-btn]').getBoundingClientRect(); return { over: document.documentElement.scrollWidth > innerWidth + 1, inside: hb.left >= cr.left - 1 && hb.right <= cr.right + 1 && hb.bottom <= cr.bottom + 1 }; }, solo);
    assert(!f2.over && f2.inside, '390: the completed card holds its button: ' + JSON.stringify(f2)); await shot('review-completed-390');
    // (the same order's piece row in the order window, completed: Completed, its seal, Print QR label, Hold: it fits at 900 and 390)
    for (const [w, h] of [[900, 800], [390, 844]]) {
      await page.setViewportSize({ width: w, height: h }); await fold(w); await page.waitForTimeout(400);
      await closeWin().catch(() => {}); await openWin(solo); await page.waitForFunction(() => document.querySelector('#owPcSum [data-cu-done]') && document.querySelector('#owPcSum [data-hold-btn]'), null, { timeout: 15000 }); await page.mouse.move(3, 3); await page.waitForTimeout(500);
      const g = await page.evaluate(() => {
        const sum = document.getElementById('owPcSum'); sum.scrollIntoView({ block: 'center' });
        const row = sum.querySelector('.owPcRow'), rr = row.getBoundingClientRect(), kids = [...row.querySelectorAll('.pcAct > *')].map(k => k.getBoundingClientRect()).filter(k => k.width > 0), hb = row.querySelector('[data-hold-btn]').getBoundingClientRect(), nm = row.querySelector('.nm').getBoundingClientRect();
        return { over: document.documentElement.scrollWidth > innerWidth + 1, inside: kids.every(k => k.left >= rr.left - 1 && k.right <= rr.right + 1), clip: row.scrollWidth > row.clientWidth + 1, rightOfName: kids.every(k => k.left >= nm.right - 1), holdLast: hb.right >= Math.max(...kids.map(k => k.right)) - 3, rowH: Math.round(rr.height) };
      });
      await shot(`orderwin-row-completed-${w}`);
      assert(!g.over && g.inside && !g.clip && g.rightOfName, `${w}: the completed row holds all its buttons, Hold with them: ${JSON.stringify(g)}`);
      await closeWin();
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await fold(1440); await page.waitForTimeout(400);
    await seg('open');

    // ── 10 · a failed run says so plainly, keeps what was done and never leaves the person in the Nest tab ──
    await page.evaluate(() => { for (const r of Orders.rows()) if (String(r.order.receiptId) === '4170837249') { r.hold = null; r.state = r.poolIds.length ? 'pooled' : 'pulled'; } window.__t.runs.length = 0; window.__t.returns.length = 0; window.__t.fx.length = 0; window.__t.runResult = { ok: false, error: 'the sheet could not be saved' }; window.OrderHoldFx.returnToOnHold = () => {}; B.employee = 'Test Operator'; CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-hold-btn]`), cable, { timeout: 15000 });
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    await page.click('dialog.holdDlg [data-k=go]');
    await page.waitForFunction(() => /not fully put on hold/.test((document.getElementById('toasts') || {}).textContent || ''), null, { timeout: 20000 });
    const bad = await page.evaluate(() => document.getElementById('toasts').innerText);
    assert(/the sheet could not be saved/.test(bad) && !/\blines?\b/i.test(bad), 'a plain message, the reason in it: ' + bad);
    await page.waitForFunction(() => CN.S.mode === 'review' && !HoldUI.busy('4170837249'), null, { timeout: 15000 });
    t = await T(); assert.equal(t.runs.length, 1); assert(t.fx[0].finished && t.fx[0].pushed.includes('error'), 'the film was told how it ended: ' + JSON.stringify(t.fx[0].pushed));
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-hold-btn]`), cable, { timeout: 15000 });
    await page.waitForFunction(r => document.querySelectorAll(`[data-hold-btn][data-rid="${r}"]`).length === 1, P.rid, { timeout: 10000 }).catch(async () => { throw new Error('the button is back, once: ' + JSON.stringify(await page.evaluate(r => [...document.querySelectorAll(`[data-hold-btn][data-rid="${r}"]`)].map(b => { const chain = []; for (let n = b; n && n !== document.body; n = n.parentElement) chain.push(n.tagName.toLowerCase() + (n.id ? '#' + n.id : '') + (n.className && typeof n.className === 'string' ? '.' + n.className.split(' ').join('.') : '')); return chain.slice(0, 7).join(' < ') + ' | ' + (b.closest('.reviewListRow') ? b.closest('.reviewListRow').dataset.row : ''); }), P.rid))); });   // (a card that flew away leaves its copy for a moment)
    assert.equal(await page.evaluate(() => document.querySelector('#rvList [data-hold-btn]').disabled), false);
    await page.evaluate(() => { document.getElementById('toasts').innerHTML = ''; });

    // ── 11 · no film: the hold still runs and ends in Orders > On hold ──
    await installFake({ noFx: true });
    await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-hold-btn]`), cable, { timeout: 15000 });
    await page.click(`${card(cable)} [data-hold-btn]`);
    await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 15000 });
    await page.click('dialog.holdDlg [data-k=go]');
    await page.waitForFunction(() => window.__t.runs.length === 1 && CN.S.mode === 'orders' && Orders.view().pile === 'hold' && !HoldUI.busy('4170837249'), null, { timeout: 20000 });
    assert.equal((await T()).fx.length, 0);

    // ── 12 · Release hold on an On hold card: releasePlan, release, the film, home; without release it is today's press ──
    await installFake();
    await page.evaluate(() => { window.__rp = 0; const was = Review.repool; Review.repool = r => { window.__rp++; return was(r); }; window.__repool = was; });
    await page.evaluate(() => Orders.showPile('hold', ''));
    await page.waitForFunction(r => document.querySelector(`#ordersView [data-rid="${r}"] .relHold`), H.rid, { timeout: 15000 });
    assert.equal(await page.evaluate(r => document.querySelector(`#ordersView [data-rid="${r}"] .relHold`).textContent.trim(), H.rid), 'Release hold');
    await page.click(`#ordersView [data-rid="${H.rid}"] .relHold`);
    await page.waitForFunction(() => window.__t.returns.length === 1, null, { timeout: 20000 });
    t = await T();
    assert.deepEqual(t.relPlans, [H.rid], 'the release was planned'); assert.equal(t.rels.length, 1, 'released once'); assert.equal(t.rels[0].rid, H.rid); assert.equal(t.rels[0].name, 'Test Operator', 'under the saved name, none asked');
    assert.equal(t.fx.length, 1); assert.equal(t.fx[0].kind, 'release'); assert.deepEqual(t.fx[0].pushed, ['start', 'queued', 'target', 'placed', 'released', 'done'], 'the film followed every step');
    assert(t.fx[0].finished); assert.deepEqual(t.returns, [H.rid]);
    assert.equal(await page.evaluate(() => window.__rp), 0, 'the old press was not used');
    assert.equal(await page.evaluate(() => document.querySelectorAll('.cnNameBar:not([data-kind="role"])').length), 0, 'no name asked for a release');   // (the Laser or Design question of a non-Admin sign-in is the same bar in its other state: not a name asked)
    assert.equal(await page.evaluate(() => window.__dlg.max), 1);
    // blocked: its reason, nothing released
    await page.evaluate(([h]) => { for (const r of Orders.rows()) if (String(r.order.receiptId) === h) { r.hold = 'Taken off GF Sheet 1 by Paul: Add to next sheet'; r.state = 'held'; } window.__t.rels.length = 0; window.__t.returns.length = 0; window.__t.relPlanResult = { canRelease: false, blockedWhy: 'Every sheet of its metal is cut: it needs a new sheet first.' }; Orders.render(); }, [H.rid]);
    await page.waitForFunction(r => document.querySelector(`#ordersView [data-rid="${r}"] .relHold`), H.rid, { timeout: 15000 });
    await page.click(`#ordersView [data-rid="${H.rid}"] .relHold`);
    await page.waitForFunction(() => /Every sheet of its metal is cut/.test((document.getElementById('toasts') || {}).textContent || ''), null, { timeout: 10000 });
    t = await T(); assert.equal(t.rels.length, 0, 'a blocked release releases nothing'); assert.equal(await page.evaluate(() => window.__rp), 0);
    await page.waitForFunction(r => { const b = document.querySelector(`#ordersView [data-rid="${r}"] .relHold`); return b && !b.disabled && b.textContent.trim() === 'Release hold'; }, H.rid, { timeout: 8000 });
    await page.evaluate(() => { document.getElementById('toasts').innerHTML = ''; window.__t.relPlanResult = null; });
    // no release in the engine: today's press (Review.repool), unchanged
    await installFake({ noRelease: true });
    await page.evaluate(() => Orders.render());
    await page.waitForFunction(r => document.querySelector(`#ordersView [data-rid="${r}"] .relHold`), H.rid, { timeout: 15000 });
    assert.equal(await page.evaluate(() => HoldUI.canRelease()), false);
    await page.click(`#ordersView [data-rid="${H.rid}"] .relHold`);
    await page.waitForFunction(() => window.__rp === 1, null, { timeout: 10000 });
    t = await T(); assert.equal(t.rels.length, 0); assert.equal(t.fx.length, 0, 'no film for the old press');
    await page.waitForFunction(h => Orders.rows().filter(r => String(r.order.receiptId) === h).every(r => !r.hold), H.rid, { timeout: 15000 });

    // ── 13 · the page's own engine (when its module is in): its plan is read only, and its sentences are shown as they come ──
    if (await page.evaluate(() => !!(window.__real.hold && window.__real.hold.plan))) {
      await page.evaluate(() => { window.OrderHold = window.__real.hold; window.OrderHoldFx = window.__real.fx || undefined; if (!window.__real.fx) delete window.OrderHoldFx; for (const r of Orders.rows()) if (String(r.order.receiptId) === '4170837249') { r.hold = null; r.state = r.poolIds.length ? 'pooled' : 'pulled'; } CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
      await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-hold-btn]`), cable, { timeout: 15000 });
      const w1 = writes();
      await page.click(`${card(cable)} [data-hold-btn]`);
      await page.waitForFunction(() => document.querySelector('dialog.holdDlg[open]'), null, { timeout: 30000 });
      text = await dlgText();
      assert(!/\blines?\b/i.test(text), 'pieces, never lines: ' + text);
      assert(/Continue/.test(text) ? /Not now/.test(text) && /Nothing is deleted/.test(text) : /Close/.test(text) && /can't be put on hold yet/.test(text), 'the real plan is shown: ' + text);
      await shot('popup-real-plan-1440');
      await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('dialog.holdDlg'));
      assert.equal(writes() - w1, 0, 'reading the real plan writes nothing');
    }
    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Hold button (Review open and completed, order window piece row), consent popup, name bar, run with film, errors, Release hold wiring, 900 and 390 px');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Hold UI OK')).catch(e => { console.error(e); process.exit(1); });
