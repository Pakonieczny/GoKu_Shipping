// The back engraving card in every detailed order view (Paul, 5 Oct 2026, round 3, point 2: "Make sure this new updated and
// functionality is included in all detailed order popup modals inclusive of all places in the application that actually activate
// this model or a the derivative of it"). plans/library-flow/round3-entry-points.md lists every place that opens the one order
// window (OrderWin.open / openOrder / openOrderFrom) or shows an order's detail. This test mounts the surfaces that carry the card
// and drives the entries that reach it, on a fake site (bridge-server.cjs), offline, with a stand-in for Engrave.approve (the
// real one fits, verifies and saves; here only what the card sees of it: the BACK ENGRAVING seal pressed on the button, the job
// settled) and for EngraveLink.open (recorded, so what each card asks for is read back):
//   · Overview of the order window, opened from an Orders row: the card for a piece with a back (words, preview, Approved, View in
//     Engrave); Approved once adds one seal and presses it once; a second press adds nothing; a piece with no back has no card; the
//     piece switcher swaps the card to the other piece's own words and state; an approved piece shows its seal, disabled button;
//   · the same card, same piece, from the hand-overs that name a piece by its pool id only (the charm inspector, the Library issues
//     rows and the Moving bar all go through openOrderFrom / OrderWin.openOrder with {poolId}): the piece asked for, not the first;
//   · the Sheet tab and the Sheet window's piece view keep their card (Approved once, one seal, View in Engrave asks for the piece);
//     approved in one place it reads approved in the others;
//   · the customer conversation window for an order outside the pull has an Open order button that hands over to the order window;
//   · the global search ("/", an order number, Enter) opens the same window on the same card.
// Headless Chromium; every request that is not to the loopback is aborted.
//   node tests/charm-nest/order-engraving-everywhere.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 7, 17) / 1000), SH = 'sheet-oee';
const A = { rid: '4180000011', tids: ['41800000111'], skus: ['FIREBIRD_2'] };                                                 // one piece, a back to approve
const B = { rid: '4170837249', tids: ['41708372491', '41708372492', '41708372493'], skus: ['OWL_CHARM', 'FOX_TAG', 'MOON_DISC'] };   // three pieces
const C = { rid: '4180000033', tids: ['41800000331'], skus: ['TINY_TAG'] };                                                 // no back at all
const OUT = '4179990001';                                                                                                   // an order outside the pull
const pool = (o, i) => `${o.rid}_${o.tids[i]}_1`, keyOf = (o, i) => `${o.rid}_${o.tids[i]}`;
const line = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: '' });
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.tids.map((t, i) => line(t, o.skus[i])) });

function seed(st) {
  const pl = [], ch = [], all = [[A, 0], [B, 0], [B, 1], [B, 2], [C, 0]];
  for (let i = 0; i < 12; i++) {
    const id = 'b' + i, x = all[i] || null, rid = x ? x[0].rid : String(4179000000 + i);
    pl.push({ id, cxPt: 16 + (i % 6) * 30, cyPt: 16 + Math.floor(i / 6) * 30, angle: 0, wPt: 22, hPt: 22 });
    ch.push({ id, name: `${rid} · SKU${i}`, poolId: x ? pool(x[0], x[1]) : `${rid}_${rid}1_1`, order: rid, sku: x ? x[0].skus[x[1]] : 'SKU' + i });
  }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', folder: 'GF_Oct.5.26_Set-1_Sheet-1', stock: { wPt: 230, hPt: 90 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch, backPool: [] });
  for (const [o, i] of all) st.put('Charm_Pool', pool(o, i), { poolId: pool(o, i), orderId: o.rid, transactionId: o.tids[i], lineKey: keyOf(o, i), sku: o.skus[i], material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: 'GF_Oct.5.26_Set-1_Sheet-1', updatedAt: Date.now() });
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.tour.seen', '1'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.CNEngravingSeals && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const hasModule = await page.evaluate(() => !!(window.OrderEngraving && typeof OrderEngraving.mount === 'function'));
    check(hasModule, 'the page loads charm-nest-order-engraving.js (OrderEngraving.mount)');

    // ── the orders, with a job each ─────────────────────────────────────────────────────────────────────────────────────
    //   A0 review (Sheet window approves it) · B0 review (Overview approves it) · B1 review (Sheet tab approves it)
    //   B2 approved with one earlier seal · C0 no back at all
    const jobs = [[A, 0, 'review', 'GOOD // LUCK'], [B, 0, 'review', 'KMB // SMH'], [B, 1, 'review', 'LOVE // MOM'], [B, 2, 'approved', 'MOON // LIGHT'], [C, 0, 'none', '']];
    await page.evaluate(async ({ orders, jobs, SH }) => {
      await Orders.loadMaps(true);
      const rows = new Map();
      for (const [o, pools] of orders) for (const [i, l] of o.lines.entries()) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'written', reason: null, claimedBy: null, poolIds: [pools[i]], engrave: null, material: 'gold', metal: 'gold' }; B.orders.rows.push(row); B.orders.byKey && B.orders.byKey.set(key, row); rows.set(key, row); }
      Orders.interpretAll();
      // Engrave.renderBack stands in for the fitting worker (a plain canvas); approve is the real card's contract without the fit:
      // the seal is added and pressed on the very button the card hands over, then the job is settled
      window.__approves = []; window.__links = [];
      Engrave.renderBack = () => { const cv = document.createElement('canvas'); cv.width = 300; cv.height = 300; return cv; };
      Engrave.approve = async (job, by, button) => {
        if (job._approvalTask) return; job._approvalTask = true;
        try {
          if (job.state !== 'review') return;
          __approves.push({ key: job.key, by });
          await new Promise(r => setTimeout(r, 120));
          const at = Date.now(), seal = CNEngravingSeals.add(job, 'engraveApproved', by, at);
          job.stamping = true; try { await CNEngravingSeals.press(button, seal); } finally { job.stamping = false; }
          Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: at });
          Object.assign(job.row.engrave || (job.row.engrave = {}), { needed: true, state: 'approved', approved: true, approvedBy: by, approvedAt: at, text: job.text, seals: job.engravingSeals });
        } finally { delete job._approvalTask; }
      };
      if (window.EngraveLink) EngraveLink.open = async a => { __links.push(a); return true; };
      const T = Date.now();
      for (const [k, st, text] of jobs) {
        const row = rows.get(k), job = Engrave.ensureJob(row), done = st === 'approved';
        Object.assign(job, { state: st, text, lines: text ? text.split(' // ') : [], activityAt: T, approvedBy: done ? 'Paul' : null, approvedAt: done ? T - 3600e3 : null, backs: [], fit: st === 'review' || done ? {} : null, view: st === 'review' || done ? {} : null, verify: { geometry: { ok: true } } });
        if (done) job.engravingSeals = [{ id: 'old1', how: 'engraveApproved', at: T - 3600e3, by: 'Paul' }];
        row.engrave = st === 'none' ? null : { needed: true, state: st, approved: done, text, approvedBy: job.approvedBy, approvedAt: job.approvedAt, seals: done ? job.engravingSeals : [] };
      }
      CN.setMode('orders'); Orders.render();
    }, { orders: [[order(A, 'Cari Moll'), [pool(A, 0)]], [order(B, 'Ada Byrne'), [0, 1, 2].map(i => pool(B, i))], [order(C, 'Mia Lund'), [pool(C, 0)]]], jobs: jobs.map(([o, i, st, t]) => [keyOf(o, i), st, t]), SH });
    await page.waitForSelector(`#ordItems [data-key="${keyOf(B, 0)}"]`);

    // what the card in a host shows, as a person reads it
    const card = sel => page.evaluate(sel => {
      const host = document.querySelector(sel); if (!host) return { host: false };
      const eng = host.querySelector('.swEng'); if (!eng || !(eng.offsetParent || eng.getClientRects().length)) return { host: true, card: false };
      const b = eng.querySelector('.egApproveButton'), seals = [...eng.querySelectorAll('.seal')];
      return { host: true, card: true, state: eng.dataset.state, words: (eng.querySelector('.words') || {}).textContent || '', preview: !!eng.querySelector('.pv canvas, .pv img'), button: b ? { text: b.textContent.trim(), disabled: b.disabled } : null,
        seals: seals.length, sealsOnButton: eng.querySelectorAll('.egApproveWrap .seal').length, engrave: !!eng.querySelector('[data-e=engrave]'), engraveText: (eng.querySelector('[data-e=engrave]') || {}).textContent || '' };
    }, sel);
    const OV = '#orderWin .owVInfo .owEng', TAB = '#owSheetPanel [data-engraving-panel]', WIN = '[data-r2=eng]';
    const waitCard = (sel, state, ms = 8000) => page.waitForFunction(({ sel, state }) => { const e = document.querySelector(sel + ' .swEng'); return e && (e.offsetParent || e.getClientRects().length) && (!state || e.dataset.state === state); }, { sel, state }, { timeout: ms }).then(() => true, () => false);
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }).catch(() => {}); await page.waitForTimeout(300); };

    // ── 1 · the Overview, opened from an Orders row: piece 1 of 3 ─────────────────────────────────────────────────────────
    await page.click(`#ordItems [data-key="${keyOf(B, 0)}"]`);
    await page.waitForFunction(() => OrderWin.isOpen());
    check(await waitCard(OV, 'approve'), 'Orders row → order window → Overview shows the back engraving card of piece 1 (To approve)');
    let c = await card(OV);
    check(c.card && /KMB/.test(c.words) && c.preview && c.button && c.button.text === 'Approved' && !c.button.disabled && c.seals === 0 && c.engrave, 'the card has the words, the preview, the green Approved button and View in Engrave: ' + JSON.stringify(c));
    check(/View in Engrave|Adjust in Engrave/.test(c.engraveText), 'the shortcut reads for the state: ' + c.engraveText.trim());
    // the card sits under the pictures, in the left column
    const place = await page.evaluate(() => { const ph = document.getElementById('owPhoto').getBoundingClientRect(), v = document.getElementById('owVector').getBoundingClientRect(), e = document.querySelector('#orderWin .owVInfo .owEng'); if (!e) return null; const r = e.getBoundingClientRect(); return { below: r.top >= Math.max(ph.bottom, v.bottom) - 1, sameColumn: Math.abs(r.left - ph.left) < 40 }; });
    check(place && place.below && place.sameColumn, 'under the Etsy listing and Vector design thumbnails, in their column: ' + JSON.stringify(place));
    // View in Engrave asks for exactly this order and piece
    await page.click(OV + ' [data-e=engrave]');
    await page.waitForFunction(() => window.__links.length >= 1);
    let l = await page.evaluate(() => __links[__links.length - 1]);
    check(l && String(l.rid) === B.rid && (l.key === keyOf(B, 0) || l.piece === keyOf(B, 0)), 'View in Engrave asks EngraveLink for order ' + B.rid + ' piece 1: ' + JSON.stringify(l));
    // Approved: once. The seal is pressed on the button once; pressing again adds nothing
    await page.click(OV + ' [data-e=approve]');
    await page.waitForFunction(() => __approves.length >= 1);
    check(await waitCard(OV, 'approved'), 'after Approved the card reads approved');
    await page.waitForTimeout(600);
    c = await card(OV);
    check(c.seals === 1 && c.sealsOnButton === 1 && c.button && c.button.disabled, 'one BACK ENGRAVING seal, on the button, which is now disabled: ' + JSON.stringify(c));
    await page.evaluate(sel => { const b = document.querySelector(sel + ' .egApproveButton'); b.disabled = false; b.click(); b.click(); }, OV);
    await page.waitForTimeout(500);
    c = await card(OV);
    check((await page.evaluate(() => __approves.length)) === 1 && c.seals === 1, 'pressing it again approves nothing and adds no seal (approvals ' + (await page.evaluate(() => __approves.length)) + ', seals ' + c.seals + ')');
    // the other pieces of the order have their own card
    await page.click(`#owPieceSw [data-piece="${keyOf(B, 1)}"]`);
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(B, 1));
    check(await waitCard(OV, 'approve'), 'piece 2 in the switcher: its own card, still to approve (not piece 1\'s approved one)');
    c = await card(OV);
    check(/LOVE/.test(c.words) && !/KMB/.test(c.words) && c.seals === 0, 'piece 2 shows its own words and no seal: ' + JSON.stringify(c));
    await page.click(`#owPieceSw [data-piece="${keyOf(B, 2)}"]`);
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(B, 2));
    check(await waitCard(OV, 'approved'), 'piece 3: its approved card');
    c = await card(OV);
    check(/MOON/.test(c.words) && c.seals === 1 && c.button && c.button.disabled, 'piece 3 shows its words, its one earlier seal and a disabled Approved: ' + JSON.stringify(c));
    // a piece with no back has no card
    await closeWin();
    await page.evaluate(k => OrderWin.open(k), keyOf(C, 0));
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(C, 0));
    await page.waitForTimeout(500);
    c = await card(OV);
    check(!c.card, 'a piece with no back engraving shows no card: ' + JSON.stringify(c));
    await closeWin();

    // ── 2 · hand-overs that name a piece by its pool id only (the charm inspector, Library issues rows, the Moving bar) ──
    await page.evaluate(() => {
      const d = document.createElement('dialog'); d.id = '__src'; d.innerHTML = '<div style="padding:30px"><button id="__go" type="button">Open order</button></div>'; document.body.appendChild(d); d.showModal();
    });
    await page.evaluate(({ rid, pid }) => { document.getElementById('__go').onclick = e => openOrderFrom(e.currentTarget, rid, { poolId: pid }); }, { rid: B.rid, pid: pool(B, 1) });
    await page.click('#__go');
    await page.waitForFunction(() => OrderWin.isOpen());
    check(await page.evaluate(k => OrderWin.key() === k, keyOf(B, 1)), 'openOrderFrom with only {poolId}: the order window is on the piece named (piece 2), not the order\'s first');
    check(await waitCard(OV, 'approve'), 'and its card is there, from the pop-up that handed over');
    c = await card(OV);
    check(/LOVE/.test(c.words), 'it is piece 2\'s own: ' + c.words.trim());
    await closeWin();
    check(await page.evaluate(() => document.getElementById('__src').open), 'closing the order window comes back to the pop-up it grew from');
    await page.evaluate(() => { document.getElementById('__src').close(); document.getElementById('__src').remove(); });
    await page.evaluate(({ rid, pid }) => OrderWin.openOrder(rid, { poolId: pid }), { rid: B.rid, pid: pool(B, 2) });
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(B, 2));
    check(await waitCard(OV, 'approved'), 'OrderWin.openOrder(rid, {poolId}) (the Issues rows): piece 3\'s card');
    await closeWin();

    // ── 3 · the Sheet tab: its card keeps working; approved here it reads approved in the Overview ──────────────────────
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), keyOf(B, 1));
    await page.waitForFunction(() => OrderWin.view() === 'sheet' && OrderWin._sheet() && document.querySelector('#owSheetPanel .owPieces'), null, { timeout: 20000 });
    check(await waitCard(TAB, 'approve', 12000), 'Sheet tab: the card of piece 2 (still to approve)');
    c = await card(TAB);
    check(c.card && /LOVE/.test(c.words) && c.button && !c.button.disabled && c.engrave, 'Sheet tab card: words, Approved, View in Engrave: ' + JSON.stringify(c));
    await page.click(TAB + ' [data-e=approve]');
    check(await waitCard(TAB, 'approved', 12000), 'Sheet tab: approved');
    await page.waitForTimeout(500);
    c = await card(TAB);
    check(c.seals === 1 && c.sealsOnButton === 1 && (await page.evaluate(() => __approves.length)) === 2, 'Sheet tab: one seal on the button, one approval (' + JSON.stringify(c) + ')');
    await page.click('.owTabsV [data-ow-view="info"]');
    check(await waitCard(OV, 'approved'), 'back on the Overview piece 2 now reads approved too');
    c = await card(OV);
    check(c.seals === 1 && c.sealsOnButton === 1, 'with the same single seal: ' + JSON.stringify(c));
    await closeWin();

    // ── 4 · the Sheet window's piece view ──────────────────────────────────────────────────────────────────────────────
    await page.evaluate(({ sh, ps }) => { const c = document.createElement('div'); c.id = '__card'; Object.assign(c.style, { position: 'fixed', left: '100px', top: '100px', width: '300px', height: '90px' }); document.body.appendChild(c);
      SheetWin.open(sh, { select: ps, origin: { tint: '', stock: { wPt: 230, hPt: 90 }, rects: () => { const r = c.getBoundingClientRect(); return { card: r, plate: r }; } } }); }, { sh: SH, ps: pool(A, 0) }).catch(() => {});
    if (!(await page.evaluate(() => SheetWin.isOpen && SheetWin.isOpen()).catch(() => false))) await page.evaluate(({ sh, ps }) => SheetWin.open(sh, { select: ps }), { sh: SH, ps: pool(A, 0) });
    check(await waitCard(WIN, 'approve', 15000), 'Sheet window: the card of the piece selected (A, to approve)');
    c = await card(WIN);
    check(c.card && /GOOD/.test(c.words) && c.button && !c.button.disabled && c.engrave, 'Sheet window card: words, Approved, View in Engrave: ' + JSON.stringify(c));
    await page.click(WIN + ' [data-e=approve]');
    check(await waitCard(WIN, 'approved', 12000), 'Sheet window: approved');
    await page.waitForTimeout(500);
    c = await card(WIN);
    check(c.seals === 1 && c.sealsOnButton === 1 && (await page.evaluate(() => __approves.length)) === 3, 'Sheet window: one seal on the button, one approval (' + JSON.stringify(c) + ')');
    // Open order from here: the order window on the same piece, with the same approved card and the same single seal
    await page.click('[data-r2=openOrd]');
    await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k, keyOf(A, 0));
    check(await waitCard(OV, 'approved'), 'Sheet window → Open order: the Overview card of the same piece, approved');
    c = await card(OV);
    check(c.seals === 1, 'with the one seal that was pressed there: ' + JSON.stringify(c));
    await closeWin();
    await page.evaluate(() => { try { SheetWin.close && SheetWin.close(); } catch (_) {} const d = document.querySelector('dialog.sheetWin[open]'); if (d) d.close(); });

    // ── 5 · the customer conversation window of an order outside the pull ───────────────────────────────────────────────
    await page.evaluate(rid => { window.__solo = CustomerMail.openConversation({ receiptId: rid, scope: 'order' }, null); }, OUT);
    await page.waitForSelector('dialog.cmDlg[open]');
    const solo = await page.evaluate(() => { const b = document.querySelector('dialog.cmDlg [data-open-order]'); return b && { shown: !b.hidden && !!b.offsetParent, text: b.textContent.trim(), title: document.querySelector('dialog.cmDlg h3').textContent }; });
    check(solo && solo.shown && /^Open order/.test(solo.text) && solo.title === 'Order ' + OUT, 'the solo customer window has an Open order button: ' + JSON.stringify(solo));
    await page.click('dialog.cmDlg [data-open-order]');
    await page.waitForFunction(rid => OrderWin.isOpen() && OrderWin.rid() === rid, OUT, { timeout: 15000 });
    check(true, 'it opens the one order window for order ' + OUT);
    await closeWin();
    check(await page.evaluate(() => document.querySelector('dialog.cmDlg').open), 'closing the order window comes back to the conversation window');
    await page.click('dialog.cmDlg [data-x]');
    await page.evaluate(() => { window.__solo = CustomerMail.openConversation({ receiptId: 'test', scope: 'order' }, null); });
    await page.waitForSelector('dialog.cmDlg[open]');
    check(await page.evaluate(() => { const b = document.querySelector('dialog.cmDlg [data-open-order]'); return !b || b.hidden || !b.offsetParent; }), 'the email link test conversation has no Open order button (it is not an order)');
    await page.click('dialog.cmDlg [data-x]');

    // ── 6 · the global search (/): a result opens the same window, on the same card ──────────────────────────────────────
    await page.keyboard.press('/');
    await page.waitForFunction(() => OrderSearch.isOpen() && document.activeElement && document.activeElement.id === 'cnsQ', null, { timeout: 8000 });
    await page.keyboard.type(B.rid);
    await page.waitForSelector(`#cnsList .cnsCard[data-rid="${B.rid}"]`, { timeout: 10000 });
    await page.keyboard.press('Enter');
    await page.waitForFunction(rid => OrderWin.isOpen() && OrderWin.rid() === rid, B.rid, { timeout: 15000 });
    check(await waitCard(OV), 'search result → order window → the card of the piece it shows');
    c = await card(OV);
    check(c.card && c.state === 'approved' && c.seals === 1 && /KMB/.test(c.words), 'it is piece 1\'s approved card, with its one seal: ' + JSON.stringify(c));
    await closeWin();

    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('order-engraving-everywhere: ok');
}
main().catch(e => { console.error(e); process.exit(1); });
