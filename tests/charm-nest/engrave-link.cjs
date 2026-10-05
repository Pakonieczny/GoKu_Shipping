// "Open it in Engraving" lands on the order's own piece (Paul, 5 Oct 2026: "When I click the 'Open it in Engraving' button
// it does not open the actual back engraving modal with this specific order, it only open the Engraving tab list with no
// specific order selected"). window.EngraveLink.open (charm-nest-engrave-link.js) is the one way in; checked here from
// the three places that used to open the tab by themselves, on a single-piece order and on an order of 3 pieces
// (4170837249's shape), with other orders about that would be in front if the piece were not asked for by name:
//   · the red box in the order window's Overview ("Its engraving is still to be settled" / "Open it in Engraving");
//   · the Sheet tab's shortcut (Confirm the words in Engrave / View in Engrave);
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

    // ── 1 · the red box ──
    const redBox = async (o, i) => {
      await page.evaluate(k => OrderWin.open(k), keyOf(o, i));
      await page.waitForSelector('#owFix .owFix button', { timeout: 15000 });
      const t = await page.evaluate(() => document.querySelector('#owFix .owFix').textContent);
      assert(/still to be settled/.test(t) && /Open it in Engraving/.test(t), 'the red box is there: ' + t);
      await page.click('#owFix .owFix button');
    };
    await redBox(A, 0); await lands(A, 0, 'place', 'red box, one piece');
    await reset();
    await redBox(B, 1); const w1 = await lands(B, 1, 'place', 'red box, piece 2 of 3');
    check(!w1.cards.includes(keyOf(B, 0)) && !w1.cards.includes(keyOf(C, 0)), 'the first piece of the order, and the order that was in front, are not what opened');
    await reset();
    await redBox(B, 0); await lands(B, 0, 'place', 'red box, piece 1 of 3');
    await reset();
    // the same order, the piece chosen in the window's own switcher (the Overview follows it), then the box
    await page.evaluate(k => OrderWin.open(k), keyOf(B, 0));
    await page.waitForSelector('#owPieceSw [data-piece]', { timeout: 15000 });
    await page.click(`#owPieceSw [data-piece="${keyOf(B, 1)}"]`);
    await page.waitForFunction(k => OrderWin.key() === k, keyOf(B, 1));
    await page.waitForSelector('#owFix .owFix button');
    await page.click('#owFix .owFix button');
    await lands(B, 1, 'place', 'red box after choosing piece 2 in the switcher');
    await reset();

    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('engrave-link: ok');
}
function orderOf(o, buyer) { return order(o, buyer); }
main().catch(e => { console.error(e); process.exit(1); });
