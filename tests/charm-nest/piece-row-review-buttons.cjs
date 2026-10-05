// A piece that is in the Review tab has its Review card's own buttons on its row in the order window (Paul, 5 Oct 2026, round 6, point 2):
//  "place the available complete button beside the actual piece that has that given problem. In this case, it would be the chain only
//   listing ... Only pieces that are already in the Review tab would have the 'Complete' and 'QR Print' buttons and locate them in place
//   of the 'Completed by Hand' text. Ensure to fit both buttons and any associated seals with full and complete functionality. Ensure
//   that everything is correctly, and in real time reflected in the review tab ..."
// Fixture: Paul's order 4170837249 (Leslie Suhr): CABLE CHAIN ONLY (a custom piece, Unknown SKU, in Review) and two MIDDLE 9935
// pieces on GF Sheet 1 and RG Sheet 1; and 4170837260: two pieces in Review (ALPHA CHAIN, BETA CHAIN) and one on a sheet.
// Proves, in headless Chromium over the real charmNestLibrary ops and an in-memory store (nothing live, no Etsy, no paid call):
//  - the buttons appear on the Review pieces' rows only; the pieces on sheets keep their words ("On GF Sheet 1 · next: ...")
//  - the row's controls are the Custom Orders bar's own markup (one code), and the Review card's buttons for the same piece
//  - Complete Order from the row: one completion, one seal, written to the cloud record (customPut), pressed in "Order window" on the
//    order's timeline; the row, the green bar, the Review tab (Completed) and the header read it at once; a double press is one
//  - Print QR label from the row (the print path), Print again, Reopen (seals kept, back under Open), Complete again, Undo
//  - several Review pieces: each row's buttons act for its own piece only, and the name asked is asked in the row that was pressed
//  - no duplicate seals in the record; widths 1440, 900 and 390 (no overflow, dots column aligned, buttons inside the row)
//   node tests/charm-nest/piece-row-review-buttons.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const P = { rid: '4170837249', cable: '41708372491', gf: '41708372492', rg: '41708372493' };
const Q = { rid: '4170837260', alpha: '41708372601', beta: '41708372602', charm: '41708372603' };
const GF1 = 'sheet-r62-gf1', RG1 = 'sheet-r62-rg1';
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  [order(P.rid, 'Leslie Suhr', [line(P.cable, 'CABLE CHAIN ONLY', 'rose', 'Rose Gold Filled'), line(P.gf, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(P.rg, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, pidOf(P, 'gf'), pidOf(P, 'rg')]],
  [order(Q.rid, 'Quinn Two', [line(Q.alpha, 'ALPHA CHAIN', 'rose', 'Rose Gold Filled'), line(Q.beta, 'BETA CHAIN', 'gold', '14k Gold Filled'), line(Q.charm, 'MIDDLE_9935', 'gold', '14k Gold Filled')]), [null, null, pidOf(Q, 'charm')]]];
const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.parent; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
  return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [P.rid, Q.rid], placements: [box('g1', 60, 60), box('g2', 130, 60)], charms: [ch('g1', P, 'gf', 'MIDDLE_9935'), ch('g2', Q, 'charm', 'MIDDLE_9935')] });
  srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [P.rid], placements: [box('r1', 60, 60)], charms: [ch('r1', P, 'rg', 'MIDDLE_9935')] });
  const poolRow = (o, t, sku, material, sheetId, metal) => srv.st.put('Charm_Pool', pidOf(o, t), { poolId: pidOf(o, t), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: `2026-10-04_${metal}_Set-1_Sheet-1`, updatedAt: Date.now() });
  poolRow(P, 'gf', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(P, 'rg', 'MIDDLE_9935', 'rose', RG1, 'RG'); poolRow(Q, 'charm', 'MIDDLE_9935', 'gold', GF1, 'GF');
  // the orders' timelines: arrived, each piece on a sheet placed (the pieces in Review have no step of their own)
  const evAt = Date.now(); let evN = 0;
  const ev = (o, type, ago, extra) => srv.st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  ev(P, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(P, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(P, 'gf'), data: { poolId: pidOf(P, 'gf') } });
  ev(P, 'placed', 2900, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(P, 'rg'), data: { poolId: pidOf(P, 'rg') } });
  ev(Q, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(Q, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(Q, 'charm'), data: { poolId: pidOf(Q, 'charm') } });
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
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      B.master.entries.set('MIDDLE_9935', { sku: 'MIDDLE_9935', updatedAt: 1 });   // (a design in a master file: its pieces are on sheets, in no Review card)
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: ORDERS });
    // the Review tab stays on screen behind the order window, so what it shows is read as it is drawn
    await page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow').length >= 3, null, { timeout: 20000 });

    const shot = async name => { if (shots) { await page.waitForTimeout(900); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    const rec = key => srv.st.doc('Charm_Custom_Orders', key);
    const puts = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'customPut').length;
    // each row of "Its pieces", as drawn
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => {
      const a = r.querySelector('.pcAct'), st = r.querySelector('.st');
      return { key: r.dataset.piece, tag: r.tagName, act: r.classList.contains('hasAct'), btns: a ? [...a.querySelectorAll('button')].map(b => b.textContent.trim()) : [], seals: a ? a.querySelectorAll('.seal').length : 0, more: a && a.querySelector('[data-cu-more]') ? a.querySelector('[data-cu-more]').textContent.trim() : '',
        st: st && !st.classList.contains('owPcSr') ? st.textContent.trim() : null, hidden: st && st.classList.contains('owPcSr') ? st.textContent.trim() : null, html: a ? a.innerHTML : '', stat: a ? (a.querySelector('.cuStat,.cuUndo,.cuWho') || { className: '' }).className : '' };
    }));
    const idle = () => page.evaluate(() => Seal.whenIdle());
    const core = btns => btns.filter(t => !/^(\+\d+|Undo)$/.test(t));   // ("+N" and the Undo offered for a few seconds are not buttons of the card)
    const rowOf = async key => (await rows()).find(r => r.key === key);
    const barOf = () => page.evaluate(() => { const b = document.getElementById('owCustom'); return b && !b.hidden ? { tag: b.querySelector('.tag').textContent.trim(), btns: [...b.querySelectorAll('button')].map(x => x.textContent.trim()), html: b.innerHTML, done: b.classList.contains('done') } : null; });
    // the Review card of a piece, in the list showing (Open or Completed), by its line
    const card = key => page.evaluate(k => { const n = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); return n && { q: n.querySelector('.queueLabel').textContent.trim(), btns: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()).filter(t => /^(Print QR label|Print again|Complete Order|Reopen)$/.test(t)), seals: n.querySelectorAll('.rowActions .seal').length }; }, key);
    const seg = which => page.evaluate(w => { document.querySelector(`#reviewView .rvSeg [data-cseg="${w}"]`).click(); }, which);
    const recStamps = key => (rec(key) && rec(key).stamps || []);
    const uniqueStamps = key => { const s = recStamps(key).map(x => [x.how, x.at, x.by, x.n || ''].join('|')); return s.length === new Set(s).size; };
    const tlOps = rid => srv.st.list('Order_Timeline').filter(e => e.orderId === rid && (e.type === 'sealCompleted' || e.type === 'sealPrinted' || (e.type === 'note' && e.data && e.data.reopened)));
    const openWin = async key => { await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(k => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length === 3, key, { timeout: 20000 }); };

    // ── 1 · the order opened on the CABLE piece: buttons on its row, words on the others ──
    const cable = kOf(P, 'cable'), gf = kOf(P, 'gf'), rg = kOf(P, 'rg');
    await page.waitForFunction(k => Review.pieceItemFor(B.orders.byKey.get(k)), cable, { timeout: 10000 });
    assert.equal(await page.evaluate(k => Review.pieceItemFor(B.orders.byKey.get(k)) === null, gf), true, 'a piece on a sheet is in no Review card');
    await openWin(cable);
    await page.waitForFunction(() => { const r = [...document.querySelectorAll('#owPcSum .owPcRow .st:not(.owPcSr)')]; return r.length === 2 && r.every(x => /Sheet 1/.test(x.textContent)); }, null, { timeout: 15000 });
    let R = await rows();
    assert.deepEqual(R.map(r => r.key), [cable, gf, rg], 'the three pieces, in order');
    assert(R[0].act && R[0].tag === 'DIV', 'the piece in Review has a row with its card\'s buttons');
    assert.deepEqual(R[0].btns, ['Print QR label', 'Complete Order'], 'Print QR label and Complete Order, as its Review card has them: ' + JSON.stringify(R[0]));
    assert.equal(R[0].st, null, 'its words are not shown beside them (they are in place of the words)');
    for (const r of [R[1], R[2]]) { assert(!r.act && r.tag === 'BUTTON' && r.btns.length === 0, 'a piece on a sheet keeps its row: ' + JSON.stringify(r)); assert.match(r.st, /^On (GF|RG) Sheet 1 · next: Laser cut$/, 'and its words: ' + r.st); }
    // (an Unknown SKU piece not yet completed has no green Custom Orders bar: the bar is for custom pieces, and for those completed by hand)
    let bar = await barOf(); assert.equal(bar, null, 'no bar before it is completed');
    // and the Review tab's card for the same piece has those buttons
    let c0 = await card(cable); assert(c0 && c0.q === 'Review required' && c0.btns.join() === 'Print QR label,Complete Order', 'its Review card: ' + JSON.stringify(c0));
    assert.equal(await card(gf), null, 'the pieces on sheets have no card');

    // ── 2 · layout: the row fits at 1440, 900 and 390 (no overflow, dots column aligned, buttons inside the row) ──
    const fit = () => page.evaluate(() => {
      const out = { over: document.documentElement.scrollWidth > innerWidth + 1, rows: [] };
      const sum = document.getElementById('owPcSum'), sr = sum.getBoundingClientRect();
      for (const r of sum.querySelectorAll('.owPcRow')) {
        const rr = r.getBoundingClientRect(), st = r.querySelector('.steps').getBoundingClientRect(), a = r.querySelector('.pcAct');
        const inside = a ? [...a.querySelectorAll('button')].every(b => { const x = b.getBoundingClientRect(); return x.left >= rr.left - 1 && x.right <= rr.right + 1 && x.width > 0; }) : true;
        out.rows.push({ h: Math.round(rr.height), w: Math.round(rr.width), clip: r.scrollWidth > r.clientWidth + 1, inside, stepsRight: Math.round(st.right), stepsLeft: Math.round(st.left), within: st.right <= rr.right + 1 && rr.right <= sr.right + 1, name: Math.round(r.querySelector('.nm').getBoundingClientRect().left) });
      }
      return out;
    });
    // the same check in each state the row has (open, completed, printed, reopened, a name asked), at the three widths
    const layout = async (tag, sizes = [[1440, 900], [900, 800], [390, 844]]) => {
      for (const [w, h] of sizes) {
        await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(500);
        await page.evaluate(() => document.getElementById('owPcSum').scrollIntoView({ block: 'center' }));
        const f = await fit();
        await shot(`r6-2-${w}-${tag}`);
        assert(!f.over, `${tag} ${w}: no sideways scroll of the page`);
        assert.equal(new Set(f.rows.map(r => r.stepsLeft)).size, 1, `${tag} ${w}: the dots column is aligned: ${JSON.stringify(f.rows)}`);
        assert.equal(new Set(f.rows.map(r => r.name)).size, 1, `${tag} ${w}: the names are aligned: ${JSON.stringify(f.rows)}`);
        for (const r of f.rows) { assert(!r.clip && r.inside && r.within, `${tag} ${w}: nothing overflows its row: ` + JSON.stringify(r)); }
        assert(Math.max(...f.rows.map(r => r.h)) <= (w > 700 ? 46 : 84), `${tag} ${w}: the rows do not grow ugly (${f.rows.map(r => r.h)}px)`);
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(400);
    };
    await layout('open');

    // ── 3 · Complete Order from the row: one completion, one seal, the cloud record, the Review tab, everything at once ──
    const before = puts();
    await page.dblclick(`#owPcSum [data-piece="${cable}"] [data-cu-complete]`);   // (a double press is one press)
    await page.waitForFunction(k => B.maps.customDone[k], cable, { timeout: 20000 });
    await page.waitForFunction(k => { const r = document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`); return r && r.querySelector('[data-cu-reopen]'); }, cable, { timeout: 10000 });
    assert.equal(puts() - before, 1, 'one write to the cloud for two clicks');
    const r1 = rec(cable);
    assert(r1 && r1.state === 'completed' && r1.how === 'button' && r1.completedBy === 'Test Operator', 'the cloud record says completed by hand, by whom: ' + JSON.stringify(r1));
    assert.equal(recStamps(cable).length, 1, 'one seal, not two'); assert(uniqueStamps(cable));
    assert.equal(rec(kOf(P, 'gf')), undefined, 'the other pieces are not touched'); assert.equal(rec(rg), undefined);
    const call = srv.st.calls.filter(c => c.op === 'customPut').pop(); assert.equal(call.body.from, 'Order window', 'pressed in the Order window, as the bar\'s press is'); assert.equal(call.body.how, 'button');
    R = await rows();
    assert(R[0].btns.includes('Reopen') && R[0].btns.includes('Print QR label') && !R[0].btns.includes('Complete Order'), 'the completed piece: ' + JSON.stringify(R[0].btns));
    assert(R[1].btns.length === 0 && R[2].btns.length === 0, 'still nothing on the pieces on sheets');
    await page.waitForFunction(() => { const b = document.getElementById('owCustom'); return b && !b.hidden && b.classList.contains('done'); }, null, { timeout: 10000 });
    bar = await barOf(); assert(bar.done && /COMPLETED/i.test(bar.tag), 'the green bar says completed: ' + bar.tag);
    assert(bar.html.endsWith((await rowOf(cable)).html), 'the bar and the row are the same controls after it');
    // Review: out of Open, under Completed with its buttons and seal
    await page.waitForFunction(k => !document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), cable, { timeout: 15000 });
    await seg('done'); await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), cable, { timeout: 15000 });
    c0 = await card(cable); assert(c0.q === 'Completed by hand' && c0.btns.includes('Reopen') && c0.btns.includes('Print QR label') && c0.seals === 1, 'the Review card under Completed: ' + JSON.stringify(c0));
    // the timeline: one point for the press, pressed in the order window
    await page.waitForTimeout(600);
    const ops1 = tlOps(P.rid).filter(e => e.type === 'sealCompleted'); assert.equal(ops1.length, 1, 'one Complete point on the order\'s timeline: ' + JSON.stringify(ops1.map(e => [e.type, e.by, e.data])));
    assert.equal(ops1[0].data.pressedIn, 'Order window'); assert.equal(ops1[0].by, 'Test Operator');
    // the header's rail and the piece row read it too: the piece is completed by hand (its words are in the Review row's place)
    assert.equal(await page.evaluate(() => /Order completed|COMPLETED/i.test(document.getElementById('owNow').textContent) || !!document.querySelector('#owRail .tlStop.d')), true);
    await layout('completed');

    // ── 4 · Print QR label from the row (the print path), then Print again: seals added, never duplicated ──
    await seg('open');
    const p0 = await page.evaluate(() => window.__printed || 0), pb = puts();
    await idle(); await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-print]`);
    await page.waitForFunction(p0 => (window.__printed || 0) > p0, p0, { timeout: 30000 });
    await page.waitForFunction(k => { const r = B.maps.customDone[k]; return r && r.prints === 1; }, cable, { timeout: 30000 });
    await page.waitForFunction(k => /Print again/.test(document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`).textContent) && !document.querySelector('#owPcSum .cuStat'), cable, { timeout: 15000 });
    assert.equal(puts() - pb, 1, 'one write for the print');
    assert(rec(cable).prints === 1 && rec(cable).printedBy === 'Test Operator', JSON.stringify(rec(cable)));
    assert.deepEqual(recStamps(cable).map(s => s.how), ['button', 'print'], 'the completion\'s seal stays, the print has its own'); assert(uniqueStamps(cable));
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Print again', 'Reopen'], JSON.stringify(R[0].btns));
    assert(R[0].seals + (R[0].more ? +R[0].more.slice(1) : 0) >= 1, 'its seal(s) with the +N for the rest: ' + JSON.stringify(R[0]));
    bar = await barOf(); assert(bar.html.endsWith(R[0].html), 'the bar and the row: one code'); assert(bar.btns.includes('Print again'));
    await seg('done'); await page.waitForFunction(k => /Print again/.test(document.querySelector(`#rvList .reviewListRow[data-row="${k}"] .rowActions`)?.textContent || ''), cable, { timeout: 15000 });
    c0 = await card(cable); assert(c0.btns.includes('Print again') && c0.btns.includes('Reopen') && c0.seals === 2, 'Review shows the same: ' + JSON.stringify(c0));
    await layout('printed');
    // "+N" opens the piece's timeline
    if (R[0].more) { await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-more]`); await page.waitForFunction(() => OrderWin.view() === 'timeline', null, { timeout: 10000 }); await page.evaluate(() => OrderWin.setView('info')); await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow').length === 3, null, { timeout: 10000 }); }
    // the other pieces are as they were
    assert.equal(rec(gf), undefined);
    // Print again (the completed card's print): another print, another seal, nothing taken away
    await page.evaluate(() => OrderWin.setView('info')); await page.evaluate(() => { const b = document.querySelector('#owPcSum .owPcName'); b && b.blur(); });
    const p1 = await page.evaluate(() => window.__printed || 0);
    if (await page.evaluate(k => !document.querySelector(`#owPcSum [data-piece="${k}"]`), cable)) await page.evaluate(k => OrderWin.open(k), cable);
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-print]`), cable, { timeout: 15000 });
    await idle(); await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-print]`);
    await page.waitForFunction(p1 => (window.__printed || 0) > p1, p1, { timeout: 30000 });
    await page.waitForFunction(k => B.maps.customDone[k] && B.maps.customDone[k].prints === 2, cable, { timeout: 30000 });
    assert.deepEqual(recStamps(cable).map(s => s.how), ['button', 'print', 'print'], 'a third seal; none replaced'); assert(uniqueStamps(cable));
    assert.deepEqual(await page.evaluate(k => Seal.list(B.maps.customDone[k]).filter(s => s.how !== 'button').map(s => s.n), cable), [1, 2], 'print 1 and print 2 (as the seals number them)');
    await page.waitForFunction(k => !document.querySelector('#owPcSum .cuStat, #owPcSum .working'), cable, { timeout: 15000 });

    // ── 5 · Reopen from the row: back under Open with its seals; the row has the open buttons again, the seals on them ──
    await idle(); await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-reopen]`);
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], cable, { timeout: 30000 });
    await page.waitForFunction(k => { const r = document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`); return r && r.querySelector('[data-cu-complete]') && !r.querySelector('[data-cu-reopen]'); }, cable, { timeout: 15000 });
    assert.equal(rec(cable).state, 'open', 'the cloud record is open again'); assert.equal(recStamps(cable).length, 3, 'its seals are kept for good'); assert(uniqueStamps(cable));
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Print QR label', 'Complete Order'], JSON.stringify(R[0].btns)); assert(R[0].seals >= 1, 'the kept seals rest on its buttons: ' + JSON.stringify(R[0]));
    bar = await barOf(); assert(bar && !bar.done && bar.html.endsWith(R[0].html), 'the bar says open again, the same controls');
    await seg('open'); await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), cable, { timeout: 15000 });
    c0 = await card(cable); assert(c0.q === 'Review required' && c0.btns.join() === 'Print QR label,Complete Order' && c0.seals === 3, 'back under Open, seals on its buttons: ' + JSON.stringify(c0));
    await page.waitForTimeout(600);
    const ops2 = tlOps(P.rid); assert(ops2.some(e => e.type === 'note' && e.data && e.data.reopened) , 'the reopen is on the timeline: ' + JSON.stringify(ops2.map(e => [e.type, e.data && e.data.pressedIn])));
    await layout('reopened');
    // Complete once more: one more seal (the 4th), not two
    await idle(); await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], cable, { timeout: 20000 });
    assert.equal(recStamps(cable).filter(s => s.how === 'button').length, 2, 'the second completion adds one seal'); assert(uniqueStamps(cable));
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-reopen]`), cable, { timeout: 15000 });
    await closeWin();

    // ── 6 · several pieces in Review: each row's buttons act for its own piece; the name is asked in the row pressed ──
    const alpha = kOf(Q, 'alpha'), beta = kOf(Q, 'beta'), charm = kOf(Q, 'charm');
    await page.evaluate(() => { B.employee = ''; });
    await openWin(charm);                                      // opened on the piece on the sheet: its row has words, the others buttons
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow.hasAct').length === 2, null, { timeout: 15000 });
    R = await rows();
    assert.deepEqual(R.map(r => [r.key, r.act]), [[alpha, true], [beta, true], [charm, false]]);
    for (const r of R.slice(0, 2)) assert.deepEqual(r.btns, ['Print QR label', 'Complete Order'], r.key);
    assert.match(R[2].st, /^On GF Sheet 1/);
    assert.equal(await barOf(), null, 'the piece shown is on a sheet: no Custom Orders bar, yet the Review pieces have their buttons');
    await shot('r6-2-1440-two-pieces');
    // pressing Complete Order on BETA with no name known: the question is asked in BETA's row, and only there
    await idle(); await page.click(`#owPcSum [data-piece="${beta}"] [data-cu-complete]`);
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-name]`), beta, { timeout: 10000 });
    let asked = await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => [r.dataset.piece, !!r.querySelector('[data-cu-name]')]));
    assert.deepEqual(asked.filter(x => x[1]).map(x => x[0]), [beta], 'asked in the row pressed, not in the other: ' + JSON.stringify(asked));
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.hasAttribute('data-cu-name')), true, 'its field has the focus');
    assert.equal(rec(beta), undefined, 'nothing is written before the name');
    await layout('name-asked', [[390, 844]]);
    await page.evaluate(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-name]`).focus(), beta);   // (the field is where it was, and typing goes on)
    // (a redraw meanwhile keeps what is typed)
    await page.keyboard.type('Pat'); await page.evaluate(() => OrderWin.paint()); await page.waitForTimeout(300);
    assert.equal(await page.evaluate(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-name]`).value, beta), 'Pat');
    await page.keyboard.press('Enter');
    await page.waitForFunction(k => B.maps.customDone[k], beta, { timeout: 20000 });
    assert(rec(beta).completedBy === 'Pat' && rec(beta).how === 'button', JSON.stringify(rec(beta))); assert.equal(rec(alpha), undefined, 'ALPHA is untouched'); assert.equal(recStamps(beta).length, 1);
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-reopen]`), beta, { timeout: 15000 });
    R = await rows(); assert(R.find(r => r.key === alpha).btns.join() === 'Print QR label,Complete Order', 'ALPHA still has its own open buttons: ' + JSON.stringify(R[0].btns));
    assert(R.find(r => r.key === beta).btns.includes('Reopen'));
    // Undo, offered for a few seconds beside the completed piece, as in Review: takes BETA's completion back, ALPHA never involved
    const undoBtn = `#owPcSum [data-piece="${beta}"] [data-cu-undo]`;
    if (await page.$(undoBtn)) {
      await idle();   // (while a seal is being stamped the page lets no press through: the stamp is never cut short)
      await page.click(undoBtn);
      await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], beta, { timeout: 30000 });
      assert.equal(rec(beta).state, 'open'); assert.equal(recStamps(beta).length, 1, 'its seal is kept');
      await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-complete]`), beta, { timeout: 15000 });
    }
    // BETA (open again, its first seal kept): Print QR label from its own row, the print path of an open card. The seal is stamped on
    // the button of THIS row while the label is made, the print opens, and the card is completed by it, with a seal of its own.
    await page.evaluate(() => { window.__stamps = []; const tick = () => { for (const n of document.querySelectorAll('.cuSealHost .seal')) { const r = n.getBoundingClientRect(); if (r.width > 4) __stamps.push([Math.round(r.left + r.width / 2), Math.round(r.top + r.height / 2)]); } requestAnimationFrame(tick); }; requestAnimationFrame(tick); });
    const pbRect = await page.evaluate(k => { const r = document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-print]`).getBoundingClientRect(); return [r.left, r.top, r.right, r.bottom]; }, beta);
    const pp0 = await page.evaluate(() => window.__printed || 0), betaStamps = recStamps(beta).length;
    await idle(); await page.click(`#owPcSum [data-piece="${beta}"] [data-cu-print]`);
    await page.waitForFunction(p0 => (window.__printed || 0) > p0, pp0, { timeout: 30000 });
    await page.waitForFunction(k => B.maps.customDone[k] && B.maps.customDone[k].prints === 1, beta, { timeout: 30000 });
    assert.equal(recStamps(beta).length, betaStamps + 1, 'the print adds one seal to the one kept'); assert(uniqueStamps(beta)); assert.equal(rec(beta).state, 'completed');
    assert.equal(rec(alpha), undefined, 'ALPHA is untouched by BETA\'s print');
    const stamped = await page.evaluate(() => __stamps);
    assert(stamped.length >= 1 && stamped.every(([x, y]) => x >= pbRect[0] - 40 && x <= pbRect[2] + 40 && y >= pbRect[1] - 40 && y <= pbRect[3] + 40), 'the seal is stamped on the button of the row pressed: ' + JSON.stringify({ stamped, pbRect }));
    await page.waitForFunction(k => /Print again/.test(document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`)?.textContent || ''), beta, { timeout: 15000 });
    await seg('done'); await page.waitForFunction(k => /Print again/.test(document.querySelector(`#rvList .reviewListRow[data-row="${k}"] .rowActions`)?.textContent || ''), beta, { timeout: 15000 });
    assert.equal((await card(beta)).seals, betaStamps + 1, 'the Review card under Completed shows every seal'); await seg('open');
    // ALPHA completed by a press on its own row (BETA's state unchanged by it)
    await idle(); await page.click(`#owPcSum [data-piece="${alpha}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], alpha, { timeout: 20000 });
    assert(rec(alpha).completedBy === 'Pat' && recStamps(alpha).length === 1, JSON.stringify(rec(alpha)));
    await closeWin();
    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Review pieces\' rows have their card\'s buttons (Complete Order, Print QR label; then Print again, seals, +N, Reopen) in place of their words; the pieces on sheets keep theirs; one code with the bar; a press is a press in Review (cloud record, one seal, timeline, Review tab at once); name asked in the row pressed; fits at 1440, 900 and 390');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Piece row Review buttons OK')).catch(e => { console.error(e); process.exit(1); });
