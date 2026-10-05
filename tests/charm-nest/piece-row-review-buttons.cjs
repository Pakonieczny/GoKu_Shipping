// A piece that is in the Review tab has its Review card's own buttons on its row in the order window (Paul, 5 Oct 2026, round 6, point 2):
//  "place the available complete button beside the actual piece that has that given problem. In this case, it would be the chain only
//   listing ... Only pieces that are already in the Review tab would have the 'Complete' and 'QR Print' buttons and locate them in place
//   of the 'Completed by Hand' text. Ensure to fit both buttons and any associated seals with full and complete functionality. Ensure
//   that everything is correctly, and in real time reflected in the review tab ..."
// Fixture: Paul's order 4170837249 (Leslie Suhr): CABLE CHAIN ONLY (a custom piece, Unknown SKU, in Review) and two MIDDLE 9935
// pieces on GF Sheet 1 and RG Sheet 1; and 4170837260: two pieces in Review (ALPHA CHAIN, BETA CHAIN) and one on a sheet.
// Proves, in headless Chromium over the real charmNestLibrary ops and an in-memory store (nothing live, no Etsy, no paid call):
//  - the buttons appear on the Review pieces' rows only; the pieces on sheets keep their status chip ("GF Sheet 1": no "next: ..." any more, see piece-status-chip.cjs)
//  - the row's controls are the Review card's own buttons (one code), and the Review card's buttons for the same piece
//  - Complete Order from the row: one completion, one seal, written to the cloud record (customPut), pressed in "Order window" on the
//    order's timeline; the row, the Review tab (Completed) and the header read it at once; a double press is one
//  - Print QR label from the row (the print path), Print again, Complete again, Undo; a Reopen made in the Review tab (seals kept, back under Open)
//  - several Review pieces: each row's buttons act for its own piece only, and the name asked is asked in the row that was pressed
//  - no duplicate seals in the record; widths 1440, 900 and 390 (no overflow, dots column aligned, buttons inside the row)
// Round 8 (Paul, 5 Oct 2026): "the QR Print and Complete buttons are not positioned to the right side of the chain only listing and NOT below
// it. Remove the Reopen button from this modal." Proved here, at 1440, 900 and 390 and in every state the row has (open, completed, printed,
// reopened, a name asked): the group's box is to the RIGHT of the name's box on the SAME row (never under the name), before the dots, right
// aligned; a completed piece shows "Completed" (the Complete Order button in its done state) and Print again; and there is NO Reopen button
// anywhere in the order window, while Reopen still works in the Review tab.
// Round 8, second ask (Paul, 5 Oct 2026, 12:31): "Remove the duplicate items and brown overlay at the bottom ... which includes the 'Print QR
// Label', 'Complete Order'" and "These buttons should be on the same line as the 'Longer Chain 5682'." Proved here: the tan Custom Orders
// bar is gone from the order window (no #owCustom, no .owCustom), every piece has its Print QR label / Complete Order (or Completed and
// Print again) exactly once, on its own row, whose vertical centre is the name's, at 1440, 900 and 390: in an order of several, in order
// 4171409060's shape (BLUE 72587 on GF Sheet 1, LONGER CHAIN 5682 on no sheet), in a single-piece chain-only order, and with one piece picked.
//   node tests/charm-nest/piece-row-review-buttons.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 10, 17) / 1000);
const P = { rid: '4170837249', cable: '41708372491', gf: '41708372492', rg: '41708372493' };
const Q = { rid: '4170837260', alpha: '41708372601', beta: '41708372602', charm: '41708372603' };
const T = { rid: '4171409060', blue: '41714090601', longer: '41714090602' };   // order 4171409060's shape (Jennifer Irving): BLUE 72587 on GF Sheet 1, LONGER CHAIN 5682 on no sheet
const S = { rid: '4171409071', solo: '41714090711' };                         // a single-piece chain-only order
const GF1 = 'sheet-r62-gf1', RG1 = 'sheet-r62-rg1';
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '19008' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  [order(P.rid, 'Leslie Suhr', [line(P.cable, 'CABLE CHAIN ONLY', 'rose', 'Rose Gold Filled'), line(P.gf, 'MIDDLE_9935', 'gold', '14k Gold Filled'), line(P.rg, 'MIDDLE_9935', 'rose', 'Rose Gold Filled')]), [null, pidOf(P, 'gf'), pidOf(P, 'rg')]],
  [order(Q.rid, 'Quinn Two', [line(Q.alpha, 'ALPHA CHAIN', 'rose', 'Rose Gold Filled'), line(Q.beta, 'BETA CHAIN', 'gold', '14k Gold Filled'), line(Q.charm, 'MIDDLE_9935', 'gold', '14k Gold Filled')]), [null, null, pidOf(Q, 'charm')]],
  [order(T.rid, 'Jennifer Irving', [line(T.blue, 'BLUE_72587', 'gold', '14k Gold Filled'), Object.assign(line(T.longer, 'LONGER CHAIN 5682', null, ''), { variations: [{ name: 'Charm', value: 'Chain only' }], metalKey: null, metalLabel: '' })]), [pidOf(T, 'blue'), null]],
  [order(S.rid, 'Sam Solo', [Object.assign(line(S.solo, 'CHAIN ONLY 5601', null, ''), { variations: [{ name: 'Charm', value: 'Chain only' }], metalKey: null, metalLabel: '' })]), [null]]];
const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.parent; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd)));
  return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const box = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const ch = (id, o, t, sku) => ({ id, name: `${o.rid} · ${sku}`, poolId: pidOf(o, t), order: o.rid, sku });
  srv.st.put('Charm_Nest_Sheets', GF1, { id: GF1, metal: 'gold', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 300, hPt: 140 }, orders: [P.rid, Q.rid, T.rid], placements: [box('g1', 60, 60), box('g2', 130, 60), box('g3', 200, 60)], charms: [ch('g1', P, 'gf', 'MIDDLE_9935'), ch('g2', Q, 'charm', 'MIDDLE_9935'), ch('g3', T, 'blue', 'BLUE_72587')] });
  srv.st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-04', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [P.rid], placements: [box('r1', 60, 60)], charms: [ch('r1', P, 'rg', 'MIDDLE_9935')] });
  const poolRow = (o, t, sku, material, sheetId, metal) => srv.st.put('Charm_Pool', pidOf(o, t), { poolId: pidOf(o, t), orderId: o.rid, transactionId: o[t], lineKey: kOf(o, t), sku, material, copy: 1, quantity: 1, state: 'written', sheetId, sheetName: `2026-10-04_${metal}_Set-1_Sheet-1`, updatedAt: Date.now() });
  poolRow(P, 'gf', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(P, 'rg', 'MIDDLE_9935', 'rose', RG1, 'RG'); poolRow(Q, 'charm', 'MIDDLE_9935', 'gold', GF1, 'GF'); poolRow(T, 'blue', 'BLUE_72587', 'gold', GF1, 'GF');
  // the orders' timelines: arrived, each piece on a sheet placed (the pieces in Review have no step of their own)
  const evAt = Date.now(); let evN = 0;
  const ev = (o, type, ago, extra) => srv.st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at: evAt - ago * 60000, by: 'Test Operator', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  ev(P, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(P, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(P, 'gf'), data: { poolId: pidOf(P, 'gf') } });
  ev(P, 'placed', 2900, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(P, 'rg'), data: { poolId: pidOf(P, 'rg') } });
  ev(Q, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(Q, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(Q, 'charm'), data: { poolId: pidOf(Q, 'charm') } });
  ev(T, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
  ev(T, 'placed', 3000, { sheet: 'GF Sheet 1', sheetId: GF1, lineKey: kOf(T, 'blue'), data: { poolId: pidOf(T, 'blue') } });
  ev(S, 'arrived', 4000, { source: 'etsy', by: 'Etsy' });
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
      B.master.entries.set('MIDDLE_9935', { sku: 'MIDDLE_9935', updatedAt: 1 }); B.master.entries.set('BLUE_72587', { sku: 'BLUE_72587', updatedAt: 1 });   // (a design in a master file: its pieces are on sheets, in no Review card)
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pools[i] ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pools[i] ? [pools[i]] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: ORDERS });
    // the Review tab stays on screen behind the order window, so what it shows is read as it is drawn
    await page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow').length >= 5, null, { timeout: 20000 });

    const shot = async name => { if (shots) { await page.waitForTimeout(900); await page.screenshot({ path: path.join(shots, name + '.png') }); } };
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    const rec = key => srv.st.doc('Charm_Custom_Orders', key);
    const puts = () => srv.st.calls.filter(c => c.name === 'charmNestLibrary' && c.op === 'customPut').length;
    // each row of "Its pieces", as drawn
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => {
      const a = r.querySelector('.pcAct'), st = r.querySelector('.st');
      return { key: r.dataset.piece || (r.querySelector('[data-pc-act]') || { dataset: {} }).dataset.pcAct, solo: r.classList.contains('solo'), label: (r.closest('#owPcSum').querySelector('.fLabel') || {}).textContent, tag: r.tagName, act: r.classList.contains('hasAct') && !r.classList.contains('hasHold'), plain: r.classList.contains('hasHold'), hold: r.querySelectorAll('.holdBtn').length, btns: a ? [...a.querySelectorAll('button')].map(b => b.textContent.trim()).filter(t => t !== 'Hold') : [], seals: a ? a.querySelectorAll('.seal').length : 0, more: a && a.querySelector('[data-cu-more]') ? a.querySelector('[data-cu-more]').textContent.trim() : '',
        st: st && !st.classList.contains('owPcSr') ? st.textContent.trim() : null, hidden: st && st.classList.contains('owPcSr') ? st.textContent.trim() : null, html: a ? a.innerHTML : '', stat: a ? (a.querySelector('.cuStat,.cuUndo,.cuWho') || { className: '' }).className : '' };
    }));
    const idle = () => page.evaluate(() => Seal.whenIdle());
    const core = btns => btns.filter(t => !/^(\+\d+|Undo)$/.test(t));   // ("+N" and the Undo offered for a few seconds are not buttons of the card)
    const rowOf = async key => (await rows()).find(r => r.key === key);
    // the tan "Custom Orders" bar is gone from the order window, and its duplicate buttons with it: every piece's Print QR label, Complete Order
    // (or Print again) is on its own row, once (count by piece), and nothing of them stands outside a row
    const noBar = () => page.evaluate(() => !document.getElementById('owCustom') && !document.querySelector('.owCustom') && !/Custom Orders ·/i.test(document.getElementById('orderWin').textContent));
    const dupes = () => page.evaluate(() => { const cnt = {}; for (const b of document.getElementById('orderWin').querySelectorAll('button, [data-cu-print], [data-cu-complete]')) { const t = b.textContent.trim(); if (!/^(Print QR label|Complete Order|Print again|Completed)$/.test(t)) continue; const k = (b.closest('[data-pc-act]') || { dataset: { pcAct: 'OUTSIDE A ROW' } }).dataset.pcAct + ' | ' + t; cnt[k] = (cnt[k] || 0) + 1; } return cnt; });
    // the order window holds no Reopen button anywhere (Paul, 5 Oct, round 8); Reopen stays in the Review tab
    const reopenInWin = () => page.evaluate(() => [...document.getElementById('orderWin').querySelectorAll('button, [data-cu-reopen]')].filter(b => b.hasAttribute('data-cu-reopen') || /^\s*Reopen\s*$/i.test(b.textContent)).length);
    // the bar's controls and the row's are one code: the row adds the done state of Complete Order ("Completed"), nothing else
    const sansDone = list => list.filter(t => t !== 'Completed');
    // the Review card of a piece, in the list showing (Open or Completed), by its line
    const card = key => page.evaluate(k => { const n = document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`); return n && { q: n.querySelector('.queueLabel').textContent.trim(), btns: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()).filter(t => /^(Print QR label|Print again|Complete Order|Reopen)$/.test(t)), seals: n.querySelectorAll('.rowActions .seal').length }; }, key);
    const seg = async which => { await idle(); await page.evaluate(w => { document.querySelector(`#reviewView .rvSeg [data-cseg="${w}"]`).click(); }, which); };   // (while a seal is being stamped the page lets no press through)
    const recStamps = key => (rec(key) && rec(key).stamps || []);
    const uniqueStamps = key => { const s = recStamps(key).map(x => [x.how, x.at, x.by, x.n || ''].join('|')); return s.length === new Set(s).size; };
    const tlOps = rid => srv.st.list('Order_Timeline').filter(e => e.orderId === rid && (e.type === 'sealCompleted' || e.type === 'sealPrinted' || (e.type === 'note' && e.data && e.data.reopened)));
    const openWin = async (key, n = 3) => { await page.evaluate(k => OrderWin.open(k), key); await page.waitForFunction(([k, n]) => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length === n, [key, n], { timeout: 20000 }); };

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
    for (const r of [R[1], R[2]]) { assert(!r.act && r.plain && r.tag === 'DIV' && r.hold === 1 && r.btns.length === 0, 'a piece on a sheet keeps its row, its words and, at the right end, one orange Hold: ' + JSON.stringify(r)); assert.match(r.st, /^(GF|RG) Sheet 1$/, 'and its status chip, no "next: ...": ' + r.st); }
    // (no tan Custom Orders bar at the foot: its buttons were a second copy of these)
    assert(await noBar(), 'no Custom Orders bar, open'); const d1 = await dupes(); assert.deepEqual(d1, { [`${cable} | Print QR label`]: 1, [`${cable} | Complete Order`]: 1 }, 'each button exactly once, on its own row: ' + JSON.stringify(d1));
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
        // where the group stands against the name (round 8): every part of it (buttons, seals, "+N") right of the name's box, on the
        // same row (its box overlaps the name's from above to below, and its middle is the row's), before the dots, each of its lines
        // (it wraps among itself when the room is short) flush to its right end
        let at = null;
        if (a) {
          const nm = r.querySelector('.nm').getBoundingClientRect(), wd = r.querySelector('.st:not(.owPcSr)'), under = !!wd && wd.getBoundingClientRect().top >= nm.bottom - 1, ar = a.getBoundingClientRect(), kids = [...a.children].map(k => k.getBoundingClientRect()).filter(k => k.width > 0);
          const far = Math.max(...kids.map(k => k.right)), lines = {}; let ln = null; for (const k of [...kids].sort((a, b) => (a.top + a.bottom) - (b.top + b.bottom))) { const c = (k.top + k.bottom) / 2; if (ln === null || c - ln > 12) ln = c; (lines[Math.round(ln)] = lines[Math.round(ln)] || []).push(k.right); }   // (lines of the group, by the middle of each part)
          at = { rightOfName: kids.every(k => k.left >= nm.right - 1), sameRow: ar.top < nm.bottom - 1 && nm.top < ar.bottom - 1, under, middle: under || Math.abs((ar.top + ar.bottom) / 2 - (rr.top + rr.bottom) / 2) <= 4, centre: Math.round(Math.abs((ar.top + ar.bottom) / 2 - (nm.top + nm.bottom) / 2)), beforeDots: st.left >= far - 1,
            flush: Object.values(lines).every(rs => Math.abs(Math.max(...rs) - far) <= 3), nLines: Object.keys(lines).length, nameW: Math.round(nm.width), groupW: Math.round(ar.width), kids: kids.length };
        }
        out.rows.push({ tall: !!r.querySelector('.cuUndo, .cuStat, .cuWho'), h: Math.round(rr.height), w: Math.round(rr.width), clip: r.scrollWidth > r.clientWidth + 1, inside, at, stepsRight: Math.round(st.right), stepsLeft: Math.round(st.left), within: st.right <= rr.right + 1 && rr.right <= sr.right + 1, name: Math.round(r.querySelector('.nm').getBoundingClientRect().left) });
      }
      return out;
    });
    // the same check in each state the row has (open, completed, printed, reopened, a name asked), at the three widths
    const layout = async (tag, sizes = [[1440, 900], [900, 800], [390, 844]]) => {
      for (const [w, h] of sizes) {
        await page.setViewportSize({ width: w, height: h }); await page.mouse.move(3, 3); await page.waitForTimeout(500);   // (the pointer away from the seals: one resting on a seal grows it)
        await page.evaluate(() => document.getElementById('owPcSum').scrollIntoView({ block: 'center' }));
        const f = await fit();
        await shot(`r8-2-${w}-${tag}`);
        assert(!f.over, `${tag} ${w}: no sideways scroll of the page`);
        assert.equal(new Set(f.rows.map(r => r.stepsLeft)).size, 1, `${tag} ${w}: the dots column is aligned: ${JSON.stringify(f.rows)}`);
        assert.equal(new Set(f.rows.map(r => r.name)).size, 1, `${tag} ${w}: the names are aligned: ${JSON.stringify(f.rows)}`);
        for (const r of f.rows) { assert(!r.clip && r.inside && r.within, `${tag} ${w}: nothing overflows its row: ` + JSON.stringify(r)); }
        // (round 8) the buttons are at the right end of the piece's own row: right of the name, on its line, before the dots, flush right
        const acted = f.rows.filter(r => r.at); assert(acted.length >= 1, `${tag} ${w}: a row with buttons`);
        for (const r of acted) {
          assert(r.at.rightOfName, `${tag} ${w}: the buttons are right of the name, never under it: ${JSON.stringify(r.at)}`);
          assert(r.at.sameRow && r.at.middle, `${tag} ${w}: on the same row as the name: ${JSON.stringify(r.at)}`);
          assert(r.at.centre <= 4, `${tag} ${w}: on the name's own line, the same centre line (${r.at.centre}px apart): ${JSON.stringify(r.at)}`);
          assert(r.at.beforeDots && r.at.flush, `${tag} ${w}: before the dots, every line flush to the right: ${JSON.stringify(r.at)}`);
          assert(r.at.nameW >= 60, `${tag} ${w}: the name keeps room to be read (${r.at.nameW}px)`);
        }
        assert.equal(await reopenInWin(), 0, `${tag} ${w}: no Reopen button in the order window`);
        assert(await noBar(), `${tag} ${w}: no tan Custom Orders bar`);
        const dd = await dupes(); assert(Object.values(dd).every(n => n === 1) && !Object.keys(dd).some(k => /OUTSIDE/.test(k)), `${tag} ${w}: every button once, none outside a row: ${JSON.stringify(dd)}`);
        // (a row showing what is happening, the Undo offered for a few seconds or the name asked is taller at 390: it wraps among itself; the orange Hold button, 5 Oct, is one more button in that wrap: 175)
        assert(Math.max(...f.rows.map(r => r.h)) <= (f.rows.some(r => r.tall) ? 175 : w > 700 && !f.rows.some(r => r.at && r.at.under) ? 46 : 110), `${tag} ${w}: the rows do not grow ugly (${f.rows.map(r => r.h)}px)`);
        const calm = acted.filter(r => !r.tall && !r.at.under); if (w > 700 && calm.length) assert(Math.max(...calm.map(r => r.h)) <= 46 && !calm.some(r => r.at.nLines > 1), `${tag} ${w}: wide, the buttons are one line on the name's line`);
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(400);
    };
    await layout('open');

    // ── 3 · Complete Order from the row: one completion, one seal, the cloud record, the Review tab, everything at once ──
    const before = puts();
    await page.dblclick(`#owPcSum [data-piece="${cable}"] [data-cu-complete]`);   // (a double press is one press)
    await page.waitForFunction(k => B.maps.customDone[k], cable, { timeout: 20000 });
    await page.waitForFunction(k => { const r = document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`); return r && r.querySelector('[data-cu-done]'); }, cable, { timeout: 10000 });
    assert.equal(puts() - before, 1, 'one write to the cloud for two clicks');
    const r1 = rec(cable);
    assert(r1 && r1.state === 'completed' && r1.how === 'button' && r1.completedBy === 'Test Operator', 'the cloud record says completed by hand, by whom: ' + JSON.stringify(r1));
    assert.equal(recStamps(cable).length, 1, 'one seal, not two'); assert(uniqueStamps(cable));
    assert.equal(rec(kOf(P, 'gf')), undefined, 'the other pieces are not touched'); assert.equal(rec(rg), undefined);
    const call = srv.st.calls.filter(c => c.op === 'customPut').pop(); assert.equal(call.body.from, 'Order window', 'pressed in the Order window, as the bar\'s press is'); assert.equal(call.body.how, 'button');
    R = await rows();
    // the Complete Order button in its done state, then Print QR label (no label printed yet); no Reopen, no Complete Order
    assert.deepEqual(core(R[0].btns), ['Completed', 'Print QR label'], 'the completed piece: ' + JSON.stringify(R[0].btns));
    const done0 = await page.evaluate(k => { const b = document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-done]`), c = getComputedStyle(b), go = document.querySelector('#owPcSum [data-piece="' + k + '"] [data-cu-print]'); return { dis: b.disabled, cls: b.className, bg: c.backgroundColor, op: c.opacity, text: b.textContent.trim(), seal: !!b.nextElementSibling && b.nextElementSibling.classList.contains('sealRow'), before: !!(b.compareDocumentPosition(go) & Node.DOCUMENT_POSITION_FOLLOWING) }; }, cable);
    assert(done0.dis && /sealedDone/.test(done0.cls) && done0.bg === 'rgb(226, 238, 230)' && done0.op === '1' && done0.text === 'Completed', 'Completed: the Complete Order button in its done state (the green a Complete seal gives it), full strength, no press: ' + JSON.stringify(done0));
    assert(done0.before, 'Completed first, Print QR label after it');
    assert.equal(await reopenInWin(), 0, 'no Reopen button in the order window once completed');
    assert(R[1].btns.length === 0 && R[2].btns.length === 0, 'still nothing on the pieces on sheets');
    assert(await noBar(), 'no green Custom Orders bar either, completed'); R = await rows();
    assert(!R[0].btns.includes('Reopen'), 'no Reopen on the row');
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
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Completed', 'Print again'], JSON.stringify(R[0].btns));
    assert(R[0].seals + (R[0].more ? +R[0].more.slice(1) : 0) >= 1, 'its seal(s) with the +N for the rest: ' + JSON.stringify(R[0]));
    assert(await noBar(), 'no bar, printed'); assert(R[0].btns.includes('Print again') && !R[0].btns.includes('Reopen'));
    // a print seal rests by Print again, a Complete seal by Completed: each by the button that made it (the "+N" follows its seal)
    const seatOf = await page.evaluate(k => [...document.querySelectorAll(`#owPcSum [data-piece="${k}"] .pcAct > *`)].map(n => n.matches('button') ? n.textContent.trim() : n.matches('.sealRow') ? 'seal:' + [...n.querySelectorAll('.seal')].map(x => x.className.match(/seal-(\w+)/)[1]).join('+') : n.textContent.trim()), cable);
    assert(seatOf.includes('Completed') && seatOf.includes('Print again'), 'Completed and Print again: ' + JSON.stringify(seatOf));
    if (seatOf.includes('seal:print')) assert.equal(seatOf.indexOf('seal:print'), seatOf.indexOf('Print again') + 1, 'the print seal right after Print again: ' + JSON.stringify(seatOf));
    if (seatOf.includes('seal:button')) assert.equal(seatOf.indexOf('seal:button'), seatOf.indexOf('Completed') + 1, 'the Complete seal right after Completed: ' + JSON.stringify(seatOf));
    assert(seatOf.includes('seal:print') || seatOf.includes('seal:button'), 'a seal is drawn: ' + JSON.stringify(seatOf));
    await seg('done'); await page.waitForFunction(k => /Print again/.test(document.querySelector(`#rvList .reviewListRow[data-row="${k}"] .rowActions`)?.textContent || ''), cable, { timeout: 15000 });
    c0 = await card(cable); assert(c0.btns.includes('Print again') && c0.btns.includes('Reopen') && c0.seals === 2, 'Review shows the same (and keeps its Reopen): ' + JSON.stringify(c0));
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

    // ── 5 · Reopen stays in the Review tab (the order window has none): back under Open with its seals; the row has the open buttons again ──
    assert.equal(await reopenInWin(), 0, 'no Reopen in the order window, completed and printed');
    await closeWin(); await seg('done');
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-cu-reopen]`), cable, { timeout: 15000 });
    await idle(); await page.click(`#rvList .reviewListRow[data-row="${cable}"] [data-cu-reopen]`);
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], cable, { timeout: 30000 });
    await openWin(cable);
    await page.waitForFunction(k => { const r = document.querySelector(`#owPcSum [data-piece="${k}"] .pcAct`); return r && r.querySelector('[data-cu-complete]') && !r.querySelector('[data-cu-done]'); }, cable, { timeout: 15000 });
    assert.equal(rec(cable).state, 'open', 'the cloud record is open again'); assert.equal(recStamps(cable).length, 3, 'its seals are kept for good'); assert(uniqueStamps(cable));
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Print QR label', 'Complete Order'], JSON.stringify(R[0].btns)); assert(R[0].seals >= 1, 'the kept seals rest on its buttons: ' + JSON.stringify(R[0]));
    assert(await noBar(), 'no bar, open again');
    assert.equal(await reopenInWin(), 0, 'no Reopen in the order window, open again');
    await seg('open'); await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), cable, { timeout: 15000 });
    c0 = await card(cable); assert(c0.q === 'Review required' && c0.btns.join() === 'Print QR label,Complete Order' && c0.seals === 3, 'back under Open, seals on its buttons: ' + JSON.stringify(c0));
    await page.waitForTimeout(600);
    const ops2 = tlOps(P.rid); assert(ops2.some(e => e.type === 'note' && e.data && e.data.reopened) , 'the reopen is on the timeline: ' + JSON.stringify(ops2.map(e => [e.type, e.data && e.data.pressedIn])));
    await layout('reopened');
    // Complete once more: one more seal (the 4th), not two
    await idle(); await page.click(`#owPcSum [data-piece="${cable}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], cable, { timeout: 20000 });
    assert.equal(recStamps(cable).filter(s => s.how === 'button').length, 2, 'the second completion adds one seal'); assert(uniqueStamps(cable));
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-done]`), cable, { timeout: 15000 });
    await layout('completed-again');
    await closeWin();

    // ── 6 · several pieces in Review: each row's buttons act for its own piece; the name is asked in the row pressed ──
    const alpha = kOf(Q, 'alpha'), beta = kOf(Q, 'beta'), charm = kOf(Q, 'charm');
    await page.evaluate(() => { B.employee = ''; });
    await openWin(charm);                                      // opened on the piece on the sheet: its row has words, the others buttons
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow.hasAct:not(.hasHold)').length === 2, null, { timeout: 15000 });
    R = await rows();
    assert.deepEqual(R.map(r => [r.key, r.act]), [[alpha, true], [beta, true], [charm, false]]);
    for (const r of R.slice(0, 2)) assert.deepEqual(r.btns, ['Print QR label', 'Complete Order'], r.key);
    assert.match(R[2].st, /^GF Sheet 1$/);
    assert(await noBar(), 'the piece shown is on a sheet: no Custom Orders bar, yet the Review pieces have their buttons');
    await shot("r8-2-1440-two-pieces");
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
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-piece="${k}"] [data-cu-done]`), beta, { timeout: 15000 });
    R = await rows(); assert(R.find(r => r.key === alpha).btns.join() === 'Print QR label,Complete Order', 'ALPHA still has its own open buttons: ' + JSON.stringify(R[0].btns));
    assert(R.find(r => r.key === beta).btns.includes('Completed') && !R.find(r => r.key === beta).btns.includes('Reopen'), 'BETA: Completed, and no Reopen in the order window');
    assert.equal(await reopenInWin(), 0, 'no Reopen anywhere in the window: two pieces in Review, one completed');
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

    // ── 7 · order 4171409060's shape (Paul, 5 Oct: "These buttons should be on the same line as the Longer Chain 5682"): BLUE 72587 on GF
    //        Sheet 1, LONGER CHAIN 5682 on no sheet. One row each; the second one's buttons at the right end of its name's line. ──
    const longer = kOf(T, 'longer'), blue = kOf(T, 'blue'), actOf = k => `#owPcSum [data-pc-act="${k}"]`;
    await seg('open'); await openWin(longer, 2);
    await page.waitForFunction(() => /Sheet 1/.test((document.querySelector('#owPcSum .owPcRow .st:not(.owPcSr)') || {}).textContent || ''), null, { timeout: 15000 });
    R = await rows();
    assert.deepEqual(R.map(r => r.key), [blue, longer], 'the two pieces');
    assert(!R[0].act && /^GF Sheet 1$/.test(R[0].st), 'BLUE keeps its status chip: ' + JSON.stringify(R[0]));
    assert(R[1].act && !R[1].solo && R[1].tag === 'DIV', 'LONGER CHAIN has its card\'s buttons on its row'); assert.deepEqual(R[1].btns, ['Print QR label', 'Complete Order']);
    const lname = await page.evaluate(k => document.querySelector(`#owPcSum [data-piece="${k}"] .nm`).textContent.trim(), longer);
    assert(/^LONGER CHAIN 5682/.test(lname), 'its name leads its row: ' + lname);
    assert(await noBar(), 'no tan bar under the pieces (4171409060: its two buttons were there twice)');
    assert.deepEqual(await dupes(), { [`${longer} | Print QR label`]: 1, [`${longer} | Complete Order`]: 1 }, 'each button once');
    await layout('t-open');
    await idle(); await page.click(`${actOf(longer)} [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], longer, { timeout: 20000 });
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-pc-act="${k}"] [data-cu-done]`), longer, { timeout: 15000 });
    R = await rows(); assert.deepEqual(core(R[1].btns), ['Completed', 'Print QR label'], JSON.stringify(R[1].btns)); assert.deepEqual(R[0].btns, [], 'BLUE untouched');
    await layout('t-completed');
    const t0 = await page.evaluate(() => window.__printed || 0);
    await idle(); await page.click(`${actOf(longer)} [data-cu-print]`);
    await page.waitForFunction(p => (window.__printed || 0) > p, t0, { timeout: 30000 });
    await page.waitForFunction(k => { const r = B.maps.customDone[k]; return r && r.prints === 1; }, longer, { timeout: 30000 });
    await page.waitForFunction(k => /Print again/.test((document.querySelector(`#owPcSum [data-pc-act="${k}"]`) || {}).textContent || '') && !document.querySelector('#owPcSum .cuStat'), longer, { timeout: 15000 });
    R = await rows(); assert.deepEqual(core(R[1].btns), ['Completed', 'Print again'], JSON.stringify(R[1].btns));
    await layout('t-printed');
    await closeWin();

    // ── 8 · a single-piece chain-only order: its one piece has its own row (there is no bar to carry the buttons any more) ──
    const solo = kOf(S, 'solo');
    await openWin(solo, 1);
    R = await rows();
    assert(R[0].solo && R[0].act && R[0].label === 'Its piece', 'one row, its own: ' + JSON.stringify([R[0].solo, R[0].act, R[0].label]));
    assert.deepEqual(R[0].btns, ['Print QR label', 'Complete Order']);
    assert(await noBar(), 'no bar in a single-piece order either');
    await layout('solo-open');
    await idle(); await page.click(`${actOf(solo)} [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k], solo, { timeout: 20000 });
    await page.waitForFunction(k => document.querySelector(`#owPcSum [data-pc-act="${k}"] [data-cu-done]`), solo, { timeout: 15000 });
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Completed', 'Print QR label'], JSON.stringify(R[0].btns));
    assert.equal(rec(solo).state, 'completed'); assert.equal(recStamps(solo).length, 1);
    await layout('solo-completed');
    const s0 = await page.evaluate(() => window.__printed || 0);
    await idle(); await page.click(`${actOf(solo)} [data-cu-print]`);
    await page.waitForFunction(p => (window.__printed || 0) > p, s0, { timeout: 30000 });
    await page.waitForFunction(k => { const r = B.maps.customDone[k]; return r && r.prints === 1; }, solo, { timeout: 30000 });
    await page.waitForFunction(k => /Print again/.test((document.querySelector(`#owPcSum [data-pc-act="${k}"]`) || {}).textContent || '') && !document.querySelector('#owPcSum .cuStat'), solo, { timeout: 15000 });
    R = await rows(); assert.deepEqual(core(R[0].btns), ['Completed', 'Print again'], JSON.stringify(R[0].btns));
    await layout('solo-printed');
    await closeWin();

    // ── 9 · one piece picked in an order of several: its row alone, with its buttons; all pieces again brings every row back ──
    await openWin(cable, 3);
    await page.click(`#owPieceSw [data-piece="${cable}"]`);
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow').length === 1 && document.querySelector('#owPcSum .owPcRow.solo'), null, { timeout: 15000 });
    R = await rows(); assert(R[0].key === cable && R[0].label === 'This piece' && R[0].act && !R[0].btns.includes('Reopen'), JSON.stringify(R[0]));
    assert(await noBar(), 'no bar with one piece picked'); assert(R[0].btns.includes('Completed'), JSON.stringify(R[0].btns));
    await layout('picked', [[1440, 900], [390, 844]]);
    await page.click('#owPieceSw [data-piece=""]');
    await page.waitForFunction(() => document.querySelectorAll('#owPcSum .owPcRow').length === 3 && !document.querySelector('#owPcSum .owPcRow.solo'), null, { timeout: 15000 });
    await closeWin();
    assert.deepEqual(outside, [], 'no Etsy call');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Review pieces\' rows have their card\'s buttons (Complete Order, Print QR label; then Completed, its seal and +N, Print again) at the right end of their rows, in place of their words, never under the name; no Reopen button anywhere in the order window (Reopen works in the Review tab); the pieces on sheets keep theirs; no tan Custom Orders bar and every button once, on the same line as the name of its piece (the shape of order 4171409060, a single-piece chain-only order, one piece picked); a press is a press in Review (cloud record, one seal, timeline, Review tab at once); name asked in the row pressed; fits at 1440, 900 and 390');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('Piece row Review buttons OK')).catch(e => { console.error(e); process.exit(1); });
