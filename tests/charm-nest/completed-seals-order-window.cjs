// Every Completed piece, and the order that is complete, shows its seal in the order window (Paul, 5 Oct 2026):
//  "The 'Complete' orders/pieces are missing their associated stamp/seal. Please review all modals and fix."
// Fixture: order 4171711853 (Carolyn Schmidt), as Paul's screenshot had it: SPORTS 10 - FIGURE SKATE and FOOTBALL completed by hand (FOOTBALL by
// Complete Order at 10:57 AM, FIGURE SKATE by its QR label at 12:41 PM, Toronto), SNAKE 5 on RG Sheet 1. The permanent timeline holds both presses
// (and, as the server derives it from a record's completedAt, a sealCompleted with data.how "print" for the label).
// Proves, in headless Chromium over the real charmNestLibrary ops and an in-memory store (nothing live, no Etsy, no paid call):
//  - each Completed piece has its seals on its row (FOOTBALL: ORDER COMPLETE; FIGURE SKATE: QR LABEL PRINTED, which is what completed it), small
//  - both "Completed by hand" boxes have that piece's completion seal; the box of a piece on a sheet has none
//  - the card is the ORDER's: while SNAKE 5 is on a sheet it says where the order is (no "Order completed", no ORDER COMPLETE seal); once every
//    piece is complete it says "Completed by ... . time" and draws ONE ORDER COMPLETE seal of that very press (the line and the seal agree)
//  - a record with no stamps still shows what it names (completedAt, how, completedBy), a record with only a timeline press shows that press,
//    and a record with nothing recorded shows no seal (nothing invented)
//  - the Timeline tab draws every press once; the Sheet tab's "Completed by hand" draws the picked piece's completion seal
//  - no seal appears twice in a surface; no seal has a tooltip or a caption; a single-piece order draws its completion once (the card)
//  - fits at 1440, 900 and 390 (no sideways scroll, every seal inside its row or box, small)
//  - Reopen (Review tab) keeps every seal (record, timeline, row); Complete Order again adds one seal and removes none
//  - a mutant that drops the seals (Seal.ofPiece / Seal.pieceRow return nothing) is caught by the same check
//   node tests/charm-nest/completed-seals-order-window.cjs   (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>, SHOTS=<dir>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 9, 17) / 1000);
const T_FOOT = Date.UTC(2026, 9, 5, 14, 57, 12);   // 10:57 AM Toronto
const T_SNK = Date.UTC(2026, 9, 5, 15, 30, 40);    // 11:30 AM
const T_FIG = Date.UTC(2026, 9, 5, 16, 41, 5);     // 12:41 PM
const OC = t => `ORDER COMPLETE 05 OCT 2026 ${t}`, QR = t => `QR LABEL PRINTED 05 OCT 2026 ${t}`;
const mk = (rid, buyer, skus) => { const o = { rid }; const lines = skus.map(([name, sku], i) => { o[name] = rid + (i + 1); return { transactionId: o[name], listingId: '19008' + o[name].slice(-5), sku, title: sku + ' necklace', quantity: 2, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold Filled' }], metalKey: 'rose', metalLabel: 'Rose Gold Filled', personalization: [], buyerMessage: '' }; });
  o.order = { receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }; return o; };
const A = mk('4171711853', 'Carolyn Schmidt', [['fig', 'SPORTS 10 - FIGURE SKATE'], ['snake', 'SNAKE_5'], ['foot', 'FOOTBALL']]);
const Bo = mk('4171711860', 'Bea Complete', [['fig', 'SPORTS 10 - FIGURE SKATE'], ['rope', 'ROPE 12 - CHAIN'], ['foot', 'FOOTBALL']]);
const C = mk('4171711871', 'Cora Legacy', [['old', 'OLD SKU 1'], ['tl', 'TIMELINE ONLY 2'], ['none', 'NOTHING RECORDED 3']]);
const D = mk('4171711882', 'Dan Solo', [['solo', 'SOLO CHAIN 4']]);
const kOf = (o, t) => `${o.rid}_${o[t]}`, pidOf = (o, t) => `${o.rid}_${o[t]}_1`;
const RG1 = 'sheet-s1-rg1';
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title>'], { type: 'text/html' })); } }; } };`;

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.SHOTS || null; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const st = srv.st;
  // ── the store: the sheet SNAKE 5 is on, the orders' timelines, and each piece's custom record ──
  st.put('Charm_Nest_Sheets', RG1, { id: RG1, metal: 'rose', sheetIndex: 1, day: '2026-10-05', status: 'written', stock: { wPt: 200, hPt: 140 }, orders: [A.rid], placements: [{ id: 'r1', cxPt: 60, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }], charms: [{ id: 'r1', name: 'snake', poolId: pidOf(A, 'snake'), order: A.rid, sku: 'SNAKE_5' }] });
  st.put('Charm_Pool', pidOf(A, 'snake'), { poolId: pidOf(A, 'snake'), orderId: A.rid, transactionId: A.snake, lineKey: kOf(A, 'snake'), sku: 'SNAKE_5', material: 'rose', copy: 1, quantity: 2, state: 'written', sheetId: RG1, sheetName: '2026-10-05_RG_Set-1_Sheet-1', updatedAt: Date.now() });
  let evN = 0;
  const ev = (o, type, at, extra) => st.put('Order_Timeline', `${o.rid}~${type}~e${++evN}`, Object.assign({ orderId: o.rid, type, at, by: 'Paul', source: 'sorter', station: '', text: '', data: {} }, extra || {}));
  const rec = (o, t, o2) => st.put('Charm_Custom_Orders', kOf(o, t), Object.assign({ key: kOf(o, t), receiptId: o.rid, transactionId: o[t], sku: 'x', title: 'x', category: 'Unknown SKU', kind: '', state: 'completed', updatedAtMs: Date.now() }, o2));
  const press = (o, t, how, at, by) => ev(o, how === 'button' ? 'sealCompleted' : 'sealPrinted', at, { id: `${kOf(o, t)}-${how}-${at}`, by: by || 'Paul', lineKey: kOf(o, t), transactionId: o[t], text: 'x', data: { how, prints: how === 'print' ? 1 : 0, completed: true, sku: 'x', pressedIn: 'Review · Custom Orders' } });
  // (what the server derives from a record's completedAt: a sealCompleted whose data.how is "print", for a label that completed the piece)
  const derived = (o, t, at) => ev(o, 'sealCompleted', at, { id: `d-seal-done-${kOf(o, t)}`, derived: true, lineKey: kOf(o, t), transactionId: o[t], text: 'Custom order completed: x', data: { how: 'print' } });
  const byButton = (o, t, at) => rec(o, t, { how: 'button', completedAt: at, completedBy: 'Paul', stamps: [{ how: 'button', at, by: 'Paul' }] });
  const byPrint = (o, t, at) => rec(o, t, { how: 'print', completedAt: at, completedBy: 'Paul', printedAt: at, printedBy: 'Paul', lastPrintedAt: at, lastPrintedBy: 'Paul', prints: 1, hasLabel: true, stamps: [{ how: 'print', at, by: 'Paul' }] });
  for (const o of [A, Bo, C, D]) ev(o, 'arrived', T_FOOT - 5 * 86400000, { source: 'etsy', by: 'Etsy' });
  // A: Paul's order
  ev(A, 'placed', T_FOOT - 3 * 86400000, { sheet: 'RG Sheet 1', sheetId: RG1, lineKey: kOf(A, 'snake'), data: { poolId: pidOf(A, 'snake') } });
  byButton(A, 'foot', T_FOOT); press(A, 'foot', 'button', T_FOOT);
  byPrint(A, 'fig', T_FIG); press(A, 'fig', 'print', T_FIG); derived(A, 'fig', T_FIG);
  // B: every piece completed, the last one (FIGURE SKATE's label) at 12:41 PM
  byButton(Bo, 'foot', T_FOOT); press(Bo, 'foot', 'button', T_FOOT);
  byButton(Bo, 'rope', T_SNK); press(Bo, 'rope', 'button', T_SNK);
  byPrint(Bo, 'fig', T_FIG); press(Bo, 'fig', 'print', T_FIG); derived(Bo, 'fig', T_FIG);
  // C: an older record (no stamps, only what it names), a record the permanent timeline alone remembers, and one with nothing recorded
  rec(C, 'old', { how: 'button', completedAt: T_SNK, completedBy: 'Olga' });
  rec(C, 'tl', {}); press(C, 'tl', 'button', T_FIG, 'Seth');
  rec(C, 'none', {});
  // D: a single piece, completed
  byButton(D, 'solo', T_FOOT); press(D, 'solo', 'button', T_FOOT);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, timezoneId: 'America/Toronto' });
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
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Paul'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Paul'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.Seal && window.OrderWin && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders, snakePool }) => {
      await Orders.loadMaps(true);
      B.master.entries.set('SNAKE_5', { sku: 'SNAKE_5', updatedAt: 1 });   // (a design in a master file: SNAKE 5 is on a sheet; every other piece is a custom piece)
      for (const order of orders) order.lines.forEach(line => { const key = CharmNestOrders.lineKey(order, line); const on = line.sku === 'SNAKE_5'; const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: on ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: on ? [snakePool] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, { orders: [A.order, Bo.order, C.order, D.order], snakePool: pidOf(A, 'snake') });
    await page.waitForFunction(() => Object.keys(B.maps.customDone).length >= 6, null, { timeout: 20000 });   // (the records are read: every piece here is completed, so the Open list is empty)
    await page.waitForTimeout(800);

    const shot = async name => { if (shots) { await page.mouse.move(3, 3); await page.waitForTimeout(900); await page.screenshot({ path: path.join(shots, 's1-' + name + '.png') }); } };
    const idle = () => page.evaluate(() => Seal.whenIdle());
    const closeWin = async () => { await page.evaluate(() => OrderWin.isOpen() && OrderWin.close()); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }); };
    const openWin = async (key, n) => {
      await page.evaluate(k => OrderWin.open(k), key);
      await page.waitForFunction(([k, n]) => OrderWin.key() === k && document.querySelectorAll('#owPcSum .owPcRow').length === n && OrderWin._feed() && OrderWin._feed().answer, [key, n], { timeout: 25000 });
      await page.mouse.move(3, 3); await page.waitForTimeout(1800); await idle();
    };
    const view = async v => { await page.evaluate(x => OrderWin.setView(x), v); await page.mouse.move(3, 3); await page.waitForTimeout(2200); await idle(); };
    // what each place of the Overview draws: the card's seal, each box's seal, each piece row's seals (as they read on their faces), and the card's words
    const snap = () => page.evaluate(() => {
      const t = n => n ? n.textContent.replace(/\s+/g, ' ').trim() : null;
      const models = n => n ? [...n.querySelectorAll('svg[data-seal-model]')].map(s => { const j = JSON.parse(s.getAttribute('data-seal-model')); return `${j.action} ${j.date} ${j.time}`; }) : [];
      const card = document.getElementById('owNowCard'), boxes = {}, rows = {};
      for (const c of card.querySelectorAll('.owShChip[data-pc]')) { const item = c.closest('.owShItem'); boxes[c.dataset.pc] = { text: t(c), seals: models(item), titled: item ? item.querySelectorAll('[title]').length : 0 }; }
      for (const r of document.querySelectorAll('#owPcSum .owPcRow')) rows[r.dataset.piece] = { text: t(r), seals: models(r), btns: [...r.querySelectorAll('.pcAct button')].map(b => b.textContent.trim()).filter(x => x && x !== 'Hold'), titled: r.querySelectorAll('.seal [title], .sealRow[title], .seal[title]').length };
      const now = card.querySelector('.tlNowSeal');
      return { pill: t(document.getElementById('owNow')), head: t(card.querySelector('.k')), line: t(card.querySelector('.t')), card: models(now), nCard: card.querySelectorAll('.tlNowSeal').length, cardText: t(card), boxes, rows,
        titledAnywhere: [...document.querySelectorAll('#orderWin .seal, #orderWin .sealRow, #orderWin .tlNowSeal, #orderWin .tlSeal')].filter(n => n.hasAttribute('title') || n.querySelector('title')).length };
    });
    const dup = list => list.length !== new Set(list).size;
    const same = (got, want, what, out) => { if (JSON.stringify(got) !== JSON.stringify(want)) out.push(`${what}: ${JSON.stringify(got)}, wanted ${JSON.stringify(want)}`); };
    // ── order A, as Paul had it: the whole check is one function, so a mutant can be run through it ──
    const aProblems = async () => {
      const s = await snap(), out = [], fig = kOf(A, 'fig'), snake = kOf(A, 'snake'), foot = kOf(A, 'foot');
      same(s.rows[foot] && s.rows[foot].seals, [OC('10:57 AM')], 'FOOTBALL row (Completed by Complete Order)', out);
      same(s.rows[fig] && s.rows[fig].seals, [QR('12:41 PM')], 'FIGURE SKATE row (Completed by its QR label)', out);
      same(s.rows[snake] && s.rows[snake].seals, [], 'SNAKE 5 row (on a sheet)', out);
      same(s.boxes[foot] && s.boxes[foot].seals, [OC('10:57 AM')], 'FOOTBALL box (Completed by hand)', out);
      same(s.boxes[fig] && s.boxes[fig].seals, [QR('12:41 PM')], 'FIGURE SKATE box (Completed by hand)', out);
      same(s.boxes[snake] && s.boxes[snake].seals, [], 'SNAKE 5 box (on a sheet)', out);
      if (s.nCard !== 1 || s.card.length !== 1) out.push('the card draws one seal: ' + JSON.stringify(s.card));
      if (/Order completed/i.test(s.cardText) || /ORDER COMPLETE/.test(s.card.join())) out.push('the order is not complete while SNAKE 5 is on a sheet: ' + s.cardText);
      for (const [k, r] of Object.entries(s.rows)) if (dup(r.seals)) out.push('a seal twice on the row of ' + k);
      return out;
    };
    // the same pieces on the Timeline (the stamps drawn on its canvas, one for each press) and the Sheet tab
    const tlSeals = () => page.evaluate(() => [...document.querySelectorAll('#orderWin .tlCanvas button.tlSt svg[data-seal-model]')].map(s => { const j = JSON.parse(s.getAttribute('data-seal-model')); return `${j.action} ${j.date} ${j.time}`; }));
    // layout: nothing leaves its row, box or the screen, every row/box seal is small, the page never scrolls sideways
    const fitProblems = () => page.evaluate(() => {
      const out = [];
      if (document.documentElement.scrollWidth > innerWidth + 1) out.push('the page scrolls sideways');
      const inside = (n, box, what) => { const r = n.getBoundingClientRect(), b = box.getBoundingClientRect(); if (r.width && (r.left < b.left - 5 || r.right > b.right + 5)) out.push(`${what} leaves its box (${Math.round(r.left)}-${Math.round(r.right)} in ${Math.round(b.left)}-${Math.round(b.right)})`); if (r.width > 30) out.push(`${what} is ${Math.round(r.width)}px wide (it is a small seal)`); };
      for (const r of document.querySelectorAll('#owPcSum .owPcRow')) for (const s of r.querySelectorAll('.seal')) inside(s, r, 'a seal of the row ' + r.dataset.piece.slice(-2));
      for (const i of document.querySelectorAll('#owNowCard .owShItem')) for (const s of i.querySelectorAll('.seal')) inside(s, i, 'a seal of a box');
      for (const s of document.querySelectorAll('#orderWin .seal')) { const r = s.getBoundingClientRect(); if (r.width && (r.right > innerWidth + 1 || r.left < -1)) out.push('a seal is off the screen'); }
      return out;
    });
    const layout = async (tag, sizes = [[1440, 900], [900, 800], [390, 844]]) => {
      for (const [w, h] of sizes) {
        await page.setViewportSize({ width: w, height: h }); await page.mouse.move(3, 3); await page.waitForTimeout(700); await idle();
        await page.evaluate(() => { const n = document.getElementById('owNowCard'); if (n && !n.hidden) n.scrollIntoView({ block: 'start' }); }); await page.waitForTimeout(300);   // (the card and the rows in front, for the picture)
        await shot(`${tag}-${w}`);
        const f = await fitProblems(); assert.deepEqual(f, [], `${tag} at ${w}: ${JSON.stringify(f)}`);
      }
      await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(400);
    };
    const recOf = key => st.doc('Charm_Custom_Orders', key);

    // ── 1 · order A (Paul's): rows, boxes, card; then the same through the mutant ──
    await openWin(kOf(A, 'foot'), 3);
    let p = await aProblems(); assert.deepEqual(p, [], 'order 4171711853: ' + p.join(' | '));
    let s = await snap();
    assert.equal(s.titledAnywhere, 0, 'no seal has a tooltip or a caption');
    assert(s.rows[kOf(A, 'fig')].btns.includes('Completed') && s.rows[kOf(A, 'foot')].btns.includes('Completed'), 'both completed pieces say Completed: ' + JSON.stringify([s.rows[kOf(A, 'fig')].btns, s.rows[kOf(A, 'foot')].btns]));
    await shot('a-overview');
    await layout('a');
    // a mutant that drops the seals (the helper returns nothing) is caught by the same check
    await page.evaluate(() => { window.__seal0 = { ofPiece: Seal.ofPiece, pieceRow: Seal.pieceRow }; Seal.ofPiece = () => []; Seal.pieceRow = () => ''; });
    await closeWin(); await openWin(kOf(A, 'foot'), 3);
    p = await aProblems(); assert(p.length >= 4 && p.some(x => /FOOTBALL row/.test(x)) && p.some(x => /FIGURE SKATE box/.test(x)), 'the mutant that drops the seals is caught: ' + JSON.stringify(p));
    await page.evaluate(() => { Seal.ofPiece = window.__seal0.ofPiece; Seal.pieceRow = window.__seal0.pieceRow; });
    await closeWin(); await openWin(kOf(A, 'foot'), 3);
    p = await aProblems(); assert.deepEqual(p, [], 'and passes again with the helper back: ' + p.join(' | '));

    // ── 2 · the Timeline draws each press once; the Sheet tab's "Completed by hand" draws the picked piece's completion ──
    await view('timeline');
    let tl = await tlSeals();
    assert.equal(tl.filter(x => x === OC('10:57 AM')).length, 1, 'FOOTBALL\'s ORDER COMPLETE once on the Timeline: ' + JSON.stringify(tl));
    assert.equal(tl.filter(x => x === QR('12:41 PM')).length, 1, 'the QR label once on the Timeline: ' + JSON.stringify(tl));
    assert(!dup(tl), 'no seal twice on the Timeline: ' + JSON.stringify(tl));
    await shot('a-timeline');
    await view('info');
    const sheetSeals = async key => {
      await page.click(`#owPieceSw [data-piece="${key}"]`); await page.waitForTimeout(500); await view('sheet');
      const r = await page.evaluate(() => { const n = document.querySelector('#owPlateWrap .owPlateNone[data-none]'); return n ? { text: n.textContent.replace(/\s+/g, ' ').trim(), seals: [...n.querySelectorAll('svg[data-seal-model]')].map(x => { const j = JSON.parse(x.getAttribute('data-seal-model')); return `${j.action} ${j.date} ${j.time}`; }), titled: n.querySelectorAll('[title]').length } : null; });
      await view('info'); return r;
    };
    let sh = await sheetSeals(kOf(A, 'foot')); assert(sh && /Completed by hand/.test(sh.text), 'the Sheet tab of FOOTBALL says Completed by hand: ' + JSON.stringify(sh)); assert.deepEqual(sh.seals, [OC('10:57 AM')], 'with its ORDER COMPLETE seal, once'); assert.equal(sh.titled, 0);
    await shot('a-sheet-football');
    sh = await sheetSeals(kOf(A, 'fig')); assert(sh && /Completed by hand/.test(sh.text)); assert.deepEqual(sh.seals, [QR('12:41 PM')], 'FIGURE SKATE\'s Sheet tab: the QR label that completed it');
    sh = await sheetSeals(kOf(A, 'snake')); assert(!sh, 'SNAKE 5 is on a sheet: no "none" box and no seal on its Sheet tab');
    await page.click('#owPieceSw [data-piece=""]'); await page.waitForTimeout(800); await idle();
    p = await aProblems(); assert.deepEqual(p, [], 'all pieces again: ' + p.join(' | '));

    // ── 3 · reopen keeps every seal; Complete Order again adds one and takes none away ──
    const foot = kOf(A, 'foot'), before = JSON.stringify(recOf(foot).stamps);
    await closeWin();
    await page.evaluate(() => { document.querySelector('#reviewView .rvSeg [data-cseg="done"]').click(); });
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-cu-reopen]`), foot, { timeout: 15000 });
    await idle(); await page.click(`#rvList .reviewListRow[data-row="${foot}"] [data-cu-reopen]`);
    await page.waitForFunction(k => !B.maps.customDone[k] && B.maps.customKept[k], foot, { timeout: 30000 });
    assert.equal(recOf(foot).state, 'open', 'the record is open again'); assert.equal(JSON.stringify(recOf(foot).stamps), before, 'its stamps are the same, none taken away');
    await openWin(foot, 3);
    s = await snap(); assert.deepEqual(s.rows[foot].seals, [OC('10:57 AM')], 'the reopened piece\'s seal rests on its row: ' + JSON.stringify(s.rows[foot]));
    assert(s.rows[foot].btns.includes('Complete Order'), 'its Complete Order button is back: ' + JSON.stringify(s.rows[foot].btns));
    assert.deepEqual(s.rows[kOf(A, 'fig')].seals, [QR('12:41 PM')], 'the other piece keeps its seal');
    await shot('a-reopened'); await layout('a-reopened', [[390, 844]]);
    await view('timeline'); tl = await tlSeals(); assert(tl.includes(OC('10:57 AM')), 'the Timeline still has the seal after the reopen: ' + JSON.stringify(tl)); await view('info');
    await idle(); await page.click(`#owPcSum [data-piece="${foot}"] [data-cu-complete]`);
    await page.waitForFunction(k => B.maps.customDone[k] && (B.maps.customDone[k].stamps || []).length === 2, foot, { timeout: 30000 });
    await page.waitForFunction(k => document.querySelectorAll(`#owPcSum [data-piece="${k}"] svg[data-seal-model]`).length === 2, foot, { timeout: 15000 });
    await page.mouse.move(3, 3); await page.waitForTimeout(1500); await idle();
    s = await snap(); assert.equal(s.rows[foot].seals.length, 2, 'two ORDER COMPLETE seals now (the first, and the new one): ' + JSON.stringify(s.rows[foot].seals));
    assert(s.rows[foot].seals.every(x => /^ORDER COMPLETE /.test(x)) && s.rows[foot].seals.includes(OC('10:57 AM')) && !dup(s.rows[foot].seals), 'the first seal stays, the new one is another: ' + JSON.stringify(s.rows[foot].seals));
    assert.equal(s.boxes[foot].seals.length, 1, 'its box draws the one that completed it now: ' + JSON.stringify(s.boxes[foot])); assert.notEqual(s.boxes[foot].seals[0], OC('10:57 AM'), 'the newest press');
    assert.equal(recOf(foot).stamps.length, 2); assert.deepEqual(await fitProblems(), []);
    await closeWin();

    // ── 4 · order B, every piece completed: the card says it, with ONE seal of that very press (the line and the seal agree) ──
    await openWin(kOf(Bo, 'foot'), 3);
    s = await snap(); const bad = [];
    if (!/Order completed/i.test(s.head || '') || !/Order completed/i.test(s.pill || '')) bad.push('the card and the header say the order is complete: ' + JSON.stringify([s.head, s.pill]));
    same(s.card, [OC('12:41 PM')], 'the card\'s one seal is ORDER COMPLETE of the last press (12:41 PM)', bad);
    const lineTime = ((s.line || '').match(/(\d{1,2}:\d{2}\s?[AP]M)/i) || [])[1] || '', sealTime = ((s.card[0] || '').match(/(\d{1,2}:\d{2}\s?[AP]M)$/i) || [])[1] || '';
    const norm = x => String(x).replace(/[\s  ]/g, '').toUpperCase();
    if (!lineTime || norm(lineTime) !== norm(sealTime)) bad.push(`the card's line (${s.line}) and its seal (${s.card[0]}) are the same moment`);
    same(s.rows[kOf(Bo, 'foot')].seals, [OC('10:57 AM')], 'FOOTBALL row', bad); same(s.rows[kOf(Bo, 'rope')].seals, [OC('11:30 AM')], 'ROPE row', bad); same(s.rows[kOf(Bo, 'fig')].seals, [QR('12:41 PM')], 'FIGURE SKATE row', bad);
    same(Object.values(s.boxes).map(b => b.seals.length), [1, 1, 1], 'every Completed by hand box has its seal', bad);
    assert.deepEqual(bad, [], 'order 4171711860 (all complete): ' + bad.join(' | '));
    assert.equal(s.nCard, 1, 'one seal on the card'); assert.equal(s.titledAnywhere, 0);
    await shot('b-all-complete'); await layout('b', [[900, 800], [390, 844]]);
    await view('timeline'); tl = await tlSeals(); assert(!dup(tl), 'no seal twice on the Timeline of order B: ' + JSON.stringify(tl)); assert(tl.includes(OC('11:30 AM')) && tl.includes(OC('10:57 AM')) && tl.includes(QR('12:41 PM')), 'the Timeline has all three: ' + JSON.stringify(tl)); await view('info');
    await closeWin();

    // ── 5 · order C: a record with no stamps, a record only the timeline remembers, and one with nothing recorded ──
    await openWin(kOf(C, 'old'), 3);
    s = await snap();
    assert.deepEqual(s.rows[kOf(C, 'old')].seals, [OC('11:30 AM')], 'the older record (no stamps): the completion it names (completedAt, how, completedBy)');
    assert.deepEqual(s.rows[kOf(C, 'tl')].seals, [OC('12:41 PM')], 'a record with no stamps and no dates: the press the permanent timeline remembers');
    assert.deepEqual(s.rows[kOf(C, 'none')].seals, [], 'nothing recorded: no seal (nothing invented)');
    assert.deepEqual(s.boxes[kOf(C, 'none')] && s.boxes[kOf(C, 'none')].seals, [], 'and its box has none');
    assert.deepEqual([s.boxes[kOf(C, 'old')].seals, s.boxes[kOf(C, 'tl')].seals], [[OC('11:30 AM')], [OC('12:41 PM')]], 'the boxes of the other two have theirs');
    assert(s.rows[kOf(C, 'none')].btns.includes('Completed'), 'that piece still says Completed');
    assert.equal(s.titledAnywhere, 0); await shot('c-legacy');
    await closeWin();

    // ── 6 · a single piece: its completion is drawn once (the card), the row and the box do not draw it again ──
    await openWin(kOf(D, 'solo'), 1);
    s = await snap();
    assert.deepEqual(s.card, [OC('10:57 AM')], 'the card draws the completion of the single piece: ' + JSON.stringify(s.card));
    const total = s.card.length + Object.values(s.rows).reduce((n, r) => n + r.seals.length, 0) + Object.values(s.boxes).reduce((n, b) => n + b.seals.length, 0);
    assert.equal(total, 1, 'one seal of that fact on the screen: ' + JSON.stringify(s));
    await shot('d-single'); await layout('d', [[390, 844]]);
    await closeWin();

    assert.deepEqual(outside, [], 'no Etsy call'); assert.deepEqual(errors, [], 'no page error');
    console.log('  ok  every Completed piece and the complete order keep their seals in the order window: rows, both "Completed by hand" boxes, the card (one ORDER COMPLETE of the press its line says; none while a piece is on a sheet), the Timeline once, the Sheet tab; older and timeline-only records read, nothing recorded shows no seal; no tooltips; fits 1440/900/390; reopen keeps every seal; a mutant that drops them is caught');
  } finally { await browser.close(); srv.close(); }
}
main().catch(e => { console.error(e); process.exit(1); });
