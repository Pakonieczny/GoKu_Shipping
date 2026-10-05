// "Open it in Engraving" lands on the order's own piece (Paul, 5 Oct 2026: "When I click the 'Open it in Engraving' button
// it does not open the actual back engraving modal with this specific order, it only open the Engraving tab list with no
// specific order selected"). window.EngraveLink.open (charm-nest-engrave-link.js) is the one way in; checked here from
// the three places that used to open the tab by themselves, on a single-piece order and on an order of 3 pieces
// (4170837249's shape), with other orders about that would be in front if the piece were not asked for by name:
//   · the Back engraving card in the order window's Overview (its shortcut into Engraving: Fix in Engraving / View in Engraving; the red
//     "Its engraving is still to be settled" box that was a second door was removed on 5 Oct 2026, and the window never says it again);
//   · the Sheet tab's shortcut (Fix in Engraving / View in Engraving);
//   · the Sheet window's own (the window closes, the way back stays);
//   · a piece whose job is not loaded yet is waited for (small labelled spinner, bounded), a missing job falls back to
//     the Engraving tab filtered to the order with a toast, a Sandbox order is never opened from the production page.
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted.
//   node tests/charm-nest/engrave-link.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 7, 17) / 1000), SH = 'sheet-engrave-link';
const A = { rid: '4180000001', tids: ['41800000011'], skus: ['FIREBIRD_2'] };                                              // one piece
const B = { rid: '4170837249', tids: ['41708372491', '41708372492', '41708372493'], skus: ['OWL_CHARM', 'FOX_TAG', 'MOON_DISC'] };   // three pieces
const C = { rid: '4180000003', tids: ['41800000031'], skus: ['TINY_TAG'] };                                              // in front unless asked past
const pool = (o, i) => `${o.rid}_${o.tids[i]}_1`, keyOf = (o, i) => `${o.rid}_${o.tids[i]}`;
const line = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' });
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.tids.map((t, i) => line(t, o.skus[i])) });

function seed(st) {
  const pl = [], ch = [];
  const all = [[A, 0], [B, 0], [B, 1], [B, 2], [C, 0]];
  for (let i = 0; i < 24; i++) {
    const id = 'b' + i, x = all[i - 5] || null;
    pl.push({ id, cxPt: 14 + (i % 8) * 28, cyPt: 14 + Math.floor(i / 8) * 28, angle: 0, wPt: 20, hPt: 20 });
    const rid = x ? x[0].rid : String(4179000000 + i);
    ch.push({ id, name: `${rid} · SKU${i}`, poolId: x ? pool(x[0], x[1]) : `${rid}_${rid}1_1`, order: rid, sku: x ? x[0].skus[x[1]] : 'SKU' + i });
  }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-10-05', status: 'written', fileBase: 'GF_Oct.5.26_Set-1_Sheet-1', folder: 'GF_Oct.5.26_Set-1_Sheet-1', stock: { wPt: 250, hPt: 100 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch, poolIds: ch.map(c => c.poolId) });
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
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-el-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.EngraveLink && CN.S.cloud.ok === true, null, { timeout: 60000 });
    check(await page.evaluate(() => typeof EngraveLink.open === 'function' && typeof EngraveLink.resolve === 'function'), 'the page loads charm-nest-engrave-link.js (EngraveLink.open and resolve)');

    // the orders, with a job each: B's pieces still to settle (its first the most recent of them), its third approved; C is
    // the most recent of all, so it would be in front of everything if the piece were not named
    const jobs = [[A, 0, 'blocked', 1], [B, 0, 'blocked', 3], [B, 1, 'blocked', 2], [B, 2, 'approved', 0], [C, 0, 'blocked', 4]];
    await page.evaluate(async ({ orders, jobs, keys }) => {
      await Orders.loadMaps(true);
      const rows = new Map();
      for (const [o, pools] of orders) for (const [i, l] of o.lines.entries()) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pools[i]], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); rows.set(key, row); }
      Orders.interpretAll();
      for (const [k, st, rank] of jobs) {
        const row = rows.get(k), job = Engrave.ensureJob(row), done = st === 'approved', t = Date.now();
        Object.assign(job, { state: st, text: done ? 'KMB // SMH' : '', lines: done ? ['KMB // SMH'] : [], activityAt: t + rank * 1e6, approvedBy: done ? 'Paul' : null, approvedAt: done ? t - 3600e3 : null, backs: [] });
        row.engrave = { needed: true, state: st, approved: done, text: job.text, approvedBy: job.approvedBy, approvedAt: job.approvedAt };
      }
      CN.setMode('orders'); Orders.render();
    }, { orders: [[orderOf(A, 'Cari Moll'), [pool(A, 0)]], [orderOf(B, 'Ada Byrne'), [0, 1, 2].map(i => pool(B, i))], [orderOf(C, 'Mia Lund'), [pool(C, 0)]]],
      jobs: jobs.map(([o, i, st, rank]) => [keyOf(o, i), st, rank]) });
    await page.waitForTimeout(500);

    // where the Engraving tab is, as a person sees it: the mode, the view's own state, the card in front or the opened row,
    // what the search box holds, and every order number on a card or row there
    const where = () => page.evaluate(() => {
      const v = Engrave.view(), root = document.getElementById('engraveView'), q = root && root.querySelector('#egQ');
      const cards = [...root.querySelectorAll('#egQueue .rvItem')].map(n => n.dataset.key), rows = [...root.querySelectorAll('.doneRow')].map(n => n.dataset.key), open = [...root.querySelectorAll('.doneRow.open')].map(n => n.dataset.key);
      return { mode: CN.S.mode, shown: !root.classList.contains('hidden'), tab: v.tab, focus: v.focus, list: v.list, q: v.q, box: q ? q.value : null, cards, rows, open, orderWin: OrderWin.isOpen(), sheetWin: SheetWin.isOpen(), pill: !!document.querySelector('.swReturn:not([hidden])') };
    });
    const reset = async () => { await page.evaluate(() => { Engrave.restoreView({ tab: 'place', focus: null, list: true, chosen: true, q: '', openDone: null }); CN.setMode('orders'); Orders.render(); }); await page.waitForTimeout(200); };
    const lands = async (o, i, tab, label) => {
      await page.waitForFunction(() => CN.S.mode === 'engrave', null, { timeout: 15000 });
      await page.waitForFunction(k => { const r = document.getElementById('engraveView'); return r && !r.classList.contains('hidden') && (r.querySelector('#egQueue .rvItem[data-key="' + k + '"]') || r.querySelector('.doneRow.open[data-key="' + k + '"]')); }, keyOf(o, i), { timeout: 15000 }).catch(() => {});
      const w = await where(), k = keyOf(o, i);
      check(w.mode === 'engrave' && w.shown && !w.orderWin && !w.sheetWin, `${label}: the Engraving tab is open and no window is over it`);
      check(w.tab === tab && w.q === o.rid && w.box === o.rid, `${label}: the ${tab === 'place' ? 'Placements' : 'Decided'} tab, the search box on order ${o.rid} (${w.tab} / ${w.q} / ${w.box})`);
      if (tab === 'place') check(w.list === false && w.focus === k && w.cards.length === 1 && w.cards[0] === k, `${label}: the placement card of piece ${i + 1} alone is in front (${w.cards.join(',')}; focus ${w.focus})`);
      else check(w.open.length === 1 && w.open[0] === k && w.rows.every(r => r.startsWith(o.rid)), `${label}: Decided shows only this order's rows, piece ${i + 1} opened (${w.open.join(',')} of ${w.rows.join(',')})`);
      return w;
    };

    // ── 1 · the order window's Back engraving card (the red box that used to be a second door is gone) ──
    const redBox = async (o, i) => {
      await page.evaluate(k => OrderWin.open(k), keyOf(o, i));
      await page.waitForSelector('#owEng [data-e=engrave]', { timeout: 15000 });
      const t = await page.evaluate(() => document.getElementById('orderWin').textContent);
      assert(!/still to be settled|Open it in Engraving/i.test(t), 'the red box is not in the order window: ' + t.slice(0, 200));
      await page.click('#owEng [data-e=engrave]');
    };
    await redBox(A, 0); await lands(A, 0, 'place', 'order window card, one piece');
    await reset();
    await redBox(B, 1); const w1 = await lands(B, 1, 'place', 'order window card, piece 2 of 3');
    check(!w1.cards.includes(keyOf(B, 0)) && !w1.cards.includes(keyOf(C, 0)), 'the first piece of the order, and the order that was in front, are not what opened');
    await reset();
    await redBox(B, 0); await lands(B, 0, 'place', 'order window card, piece 1 of 3');
    await reset();
    // the same order, the piece chosen on the window's Its pieces list (the Overview follows it), then the card's shortcut
    await page.evaluate(k => OrderWin.open(k), keyOf(B, 0));
    await page.waitForSelector('#owPcSum .owPcRow[data-piece]', { timeout: 15000 });
    await page.click(`#owPcSum .owPcRow[data-piece="${keyOf(B, 1)}"] .dot`);
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(B, 1));
    await page.waitForSelector('#owEng [data-e=engrave]');
    await page.click('#owEng [data-e=engrave]');
    await lands(B, 1, 'place', 'order window card after choosing piece 2 on the pieces list');
    await reset();

    // ── 2 · the Sheet tab's shortcut ──
    const sheetTab = async (o, i) => {
      await page.evaluate(({ rid, pid }) => OrderWin.openOrder(rid, { view: 'sheet', poolId: pid }), { rid: o.rid, pid: pool(o, i) });
      await page.waitForFunction(({ sh, pid }) => { const n = OrderWin._sheet(); return !!(n && n.sheet && n.sheet.id === sh && n.pointOf(pid)) && document.getElementById('owSheetCv').style.visibility !== 'hidden'; }, { sh: SH, pid: pool(o, i) }, { timeout: 30000 });
      await page.waitForFunction(() => !!document.querySelector('#owSheetPanel [data-engraving-panel] [data-e=engrave]'));
      const text = await page.evaluate(() => document.querySelector('#owSheetPanel [data-engraving-panel] [data-e=engrave]').textContent.trim());
      await page.click('#owSheetPanel [data-engraving-panel] [data-e=engrave]');
      return text;
    };
    check(/Fix in Engraving/.test(await sheetTab(A, 0)), 'Sheet tab, one piece: its shortcut is Fix in Engraving');
    await lands(A, 0, 'place', 'Sheet tab, one piece');
    await reset();
    check(/Fix in Engraving/.test(await sheetTab(B, 1)), 'Sheet tab, piece 2 of 3: its shortcut is Fix in Engraving');
    const w2 = await lands(B, 1, 'place', 'Sheet tab, piece 2 of 3');
    check(!w2.cards.includes(keyOf(B, 0)) && !w2.cards.includes(keyOf(C, 0)), 'the first piece and the order in front are not what opened');
    await reset();
    check(/View in Engraving/.test(await sheetTab(B, 2)), 'Sheet tab, piece 3 of 3 (approved): its shortcut says View in Engraving');
    await lands(B, 2, 'done', 'Sheet tab, piece 3 of 3 (approved)');
    await reset();

    // ── 3 · the Sheet window's own ──
    const sheetWin = async (o, i) => {
      await page.evaluate(({ sh, pid }) => SheetWin.open(sh, { select: pid }), { sh: SH, pid: pool(o, i) });
      await page.waitForFunction(pid => { const W = SheetWin._W; return W.sel && W.sel.poolId === pid && W.view === 'piece' && !W.flip && !W.flying && W.el.detail.querySelector('[data-r2=eng] [data-e=engrave]'); }, pool(o, i), { timeout: 30000 });
      await page.waitForTimeout(500);
      const text = await page.evaluate(() => SheetWin._W.el.detail.querySelector('[data-r2=eng] [data-e=engrave]').textContent.trim());
      await page.click('[data-r2=eng] [data-e=engrave]');
      return text;
    };
    const dismiss = () => page.evaluate(() => { document.querySelector('.swReturn [data-a=x]')?.click(); });
    check(/Fix in Engraving/.test(await sheetWin(B, 1)), 'Sheet window, piece 2 of 3: its shortcut is Fix in Engraving');
    const w3 = await lands(B, 1, 'place', 'Sheet window, piece 2 of 3');
    check(w3.pill, 'the way back to the sheet is still offered (Back to the sheet)');
    await dismiss(); await reset();
    await sheetWin(B, 2); await lands(B, 2, 'done', 'Sheet window, piece 3 of 3 (approved)');
    await dismiss(); await reset();
    await sheetWin(A, 0); await lands(A, 0, 'place', 'Sheet window, one piece');
    await dismiss(); await reset();

    // ── 4 · the helper itself: a job still to come, a job that is missing, Sandbox against production ──
    // resolve() names the piece asked for, and never another piece of the order
    const res = await page.evaluate(({ rid, pools, keys }) => { const r = t => { const x = EngraveLink.resolve(t); return { key: x.job && x.job.key, tab: x.tab }; };
      return { byPool: r({ rid, poolId: pools[1] }), byKey: r({ key: keys[2] }), byPiece: r({ rid, key: keys[0], piece: keys[1] }), orderOnly: r({ rid }), nothing: r({ rid, key: rid + '_999' }) }; },
      { rid: B.rid, pools: [0, 1, 2].map(i => pool(B, i)), keys: [0, 1, 2].map(i => keyOf(B, i)) });
    check(res.byPool.key === keyOf(B, 1) && res.byPool.tab === 'place' && res.byKey.key === keyOf(B, 2) && res.byKey.tab === 'done' && res.byPiece.key === keyOf(B, 1), 'resolve: a copy, a line key and a chosen piece each name that piece (approved: Decided)');
    check(res.orderOnly.key === null && res.nothing.key === null, 'resolve: an order of three pieces named alone, or a piece with no job, names nothing (no guess)');

    // two copies of one piece share one job: the second copy's own pool id finds it
    const copy2 = await page.evaluate(() => { const row = B.orders.byKey.get('4180000001_41800000011'); row.poolIds = ['4180000001_41800000011_1', '4180000001_41800000011_2']; const job = Engrave.ensureJob(row); return { copies: job.copies.length, found: EngraveLink.resolve({ rid: '4180000001', poolId: '4180000001_41800000011_2' }).job === job }; });
    check(copy2.copies === 2 && copy2.found, 'resolve: the second copy of a piece with two copies finds the piece\'s job');
    await page.evaluate(() => { const row = B.orders.byKey.get('4180000001_41800000011'); row.poolIds = ['4180000001_41800000011_1']; Engrave.ensureJob(row); });

    // an order view opened from the Sheet window (it gave the window away): the card's shortcut closes both, nothing is left over the tab
    await page.evaluate(({ sh, pid }) => SheetWin.open(sh, { select: pid }), { sh: SH, pid: pool(B, 1) });
    await page.waitForFunction(pid => { const W = SheetWin._W; return W.sel && W.sel.poolId === pid && W.view === 'piece' && !W.flip && !W.flying && W.el.detail.querySelector('[data-r2=openOrd]'); }, pool(B, 1), { timeout: 30000 });
    await page.waitForTimeout(500);
    await page.click('[data-r2=openOrd]');
    await page.waitForFunction(() => OrderWin.isOpen() && document.querySelector('#owEng [data-e=engrave]'), null, { timeout: 15000 });
    await page.waitForTimeout(600);
    await page.click('#owEng [data-e=engrave]');
    const w4 = await lands(B, 1, 'place', 'card of an order view opened from the Sheet window');
    check(!w4.sheetWin && !w4.orderWin, 'neither the order view nor the Sheet window it came from is left over the Engraving tab');
    await reset();

    // a date filter on the Placements list would hide the piece: it is set to All, and only then
    await page.evaluate(() => { const j = Engrave.items().get('4180000001_41800000011'), old = Date.now() - 30 * 864e5; j.t = old; j.activityAt = old; CNListActivity.set('engrave-place', { range: 'today' }); });
    const hidden = await page.evaluate(() => CNListActivity.select('engrave-place', [Engrave.items().get('4180000001_41800000011')]).length);
    await page.evaluate(() => EngraveLink.open({ rid: '4180000001', key: '4180000001_41800000011' }));
    await lands(A, 0, 'place', 'with a Today filter on the list (the piece is 30 days old)');
    check(hidden === 0 && (await page.evaluate(() => CNListActivity.state('engrave-place').range)) === 'all', 'the Today filter would have hidden it; the list is on All now');
    await reset();

    // a row of the pull, added by a test: its order, its line, its engraving record
    const addRow = (o, rec, spec) => page.evaluate(({ o, rec, spec }) => { const l = o.lines[0], key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [o.receiptId + '_' + l.transactionId + '_1'], engrave: rec, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); window.__row = row; return key; }, { o: order(o, 'Test Buyer'), rec, spec });
    // a missing job: nothing to engrave for this piece
    const P = { rid: '4180000009', tids: ['41800000091'], skus: ['PLAIN_DISC'] }, L = { rid: '4180000010', tids: ['41800000101'], skus: ['LATE_TAG'] }, N = { rid: '4180000011', tids: ['41800000111'], skus: ['NEVER_TAG'] };
    const keyP = await addRow(P, { needed: false, state: 'none', approved: true }, null);
    const miss = await page.evaluate(k => EngraveLink.open({ rid: '4180000009', key: k }), keyP);
    const wm = await where();
    check(miss === false && wm.mode === 'engrave' && wm.q === P.rid && wm.list === true && wm.cards.length === 0, 'a missing job: the Engraving tab filtered to the order, nothing chosen, open() says false (' + JSON.stringify({ q: wm.q, list: wm.list, cards: wm.cards }) + ')');
    const toasts = () => page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent));
    check((await toasts()).some(t => /4180000009/.test(t) && /not loaded|nothing to engrave/.test(t)), 'and a toast says so: ' + (await toasts()).join(' | '));
    await reset();

    // a job still to come (its words not read yet): a small labelled spinner, then the piece, with the window closed
    const keyL = await addRow(L, { needed: true, state: 'classify', approved: false }, { engraveCandidate: true });
    await page.evaluate(k => { window.__late = window.__row; OrderWin.open(k); }, keyL);
    await page.waitForFunction(() => OrderWin.isOpen());
    const pending = page.evaluate(k => EngraveLink.open({ rid: '4180000010', key: k }), keyL);
    await page.waitForSelector('.elWait', { timeout: 5000 });
    const sp = await page.evaluate(() => { const n = document.querySelector('.elWait'); return { text: n.textContent, role: n.getAttribute('role'), inDialog: !!n.closest('dialog'), win: OrderWin.isOpen() }; });
    check(/Finding the engraving for order 4180000010/.test(sp.text) && sp.role === 'status' && sp.inDialog && sp.win, 'a job not there yet: a small labelled spinner shows in the open window while it is waited for (' + sp.text + ')');
    await page.waitForTimeout(500);
    await page.evaluate(() => { const row = __late, job = Engrave.ensureJob(row); Object.assign(job, { state: 'blocked', text: '', lines: [], activityAt: Date.now() }); row.engrave = { needed: true, state: 'blocked', approved: false }; });
    check(await pending === true, 'it arrived while waiting: open() lands on it and says true');
    await lands(L, 0, 'place', 'a job that arrived while waited for');
    check(!(await page.evaluate(() => !!document.querySelector('.elWait'))), 'the spinner is gone');
    await reset();
    // one that never comes: bounded, then the order's tab and a toast
    const keyN = await addRow(N, { needed: true, state: 'classify', approved: false }, { engraveCandidate: true });
    const t0 = Date.now(), never = await page.evaluate(k => EngraveLink.open({ rid: '4180000011', key: k }), keyN), took = Date.now() - t0;
    const wn = await where();
    check(never === false && took >= 5000 && took < 12000 && wn.q === N.rid && wn.list === true, `a job that never comes: waited ${took} ms (bounded), then the Engraving tab for the order`);
    check((await toasts()).some(t => /did not finish loading/.test(t)), 'and the toast says it did not finish loading');
    await reset();

    // Sandbox against production: a Sandbox order is never opened from this (production) page
    const before = await where();
    const sb = await page.evaluate(() => EngraveLink.open({ rid: '4180000001', key: '4180000001_41800000011', sandbox: true }));
    const after = await where();
    check(sb === false && after.mode === before.mode && after.q === before.q && after.focus === before.focus, 'a Sandbox order from the production page: nothing opens, nothing moves');
    check((await toasts()).some(t => /Sandbox/.test(t) && /production mode/.test(t)), 'and the toast says why');
    check(await page.evaluate(() => EngraveLink.open({ rid: '4180000001', key: '4180000001_41800000011', sandbox: false })) === true, 'the same order marked as a production one opens');
    // an order number never matches another order's piece
    await reset();
    const cross = await page.evaluate(() => EngraveLink.open({ rid: '4180000003', key: '4180000001_41800000011' }));
    const wc = await where();
    check(cross === false && wc.cards.length === 0 && wc.q === '4180000003', 'a key of one order with another order\'s number opens neither: the tab is filtered to the number asked for');

    // ── 5 · and the other way round: the Sandbox page never opens a production order ──
    const sbCtx = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    try {
      await sbCtx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await sbCtx.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
      await sbCtx.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'on', sandboxStream: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} window.prompt = () => 'Test Operator'; });
      const sp = await sbCtx.newPage(); sp.setDefaultTimeout(20000);
      sp.on('pageerror', e => { errors.push(e.message); console.error('page error (sandbox):', String(e.message)); });
      await sp.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await sp.waitForFunction(() => window.CN && window.Engrave && window.EngraveLink && CN.S.cloud.ok === true && CN.S.settings.sandbox === 'on', null, { timeout: 60000 });
      const mode0 = await sp.evaluate(() => CN.S.mode);
      const refused = await sp.evaluate(() => EngraveLink.open({ rid: '4180000001', key: '4180000001_41800000011', sandbox: false }));
      const kept = await sp.evaluate(() => ({ mode: CN.S.mode, q: Engrave.view().q, toasts: [...document.querySelectorAll('#toasts .toast .m')].map(n => n.textContent) }));
      check(refused === false && kept.mode === mode0 && !kept.q && kept.toasts.some(t => /production/.test(t) && /Sandbox mode/.test(t)), 'the Sandbox page given a production order: nothing opens, nothing moves, the toast says why (' + kept.toasts.join(' | ') + ')');
      const mine = await sp.evaluate(() => EngraveLink.open({ rid: '4180000001', key: '4180000001_41800000011', sandbox: true }));
      const there = await sp.evaluate(() => ({ mode: CN.S.mode, q: Engrave.view().q }));
      check(mine === false && there.mode === 'engrave' && there.q === '4180000001', 'a Sandbox order on the Sandbox page is taken (no job here: the Engraving tab filtered to the order)');
    } finally { await sbCtx.close(); }

    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('engrave-link: ok');
}
function orderOf(o, buyer) { return order(o, buyer); }
main().catch(e => { console.error(e); process.exit(1); });
