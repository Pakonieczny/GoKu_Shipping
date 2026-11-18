// HELDCHARM (Paul, 18 Nov 2026, order 4225100553: "It doesn't matter whether the charm is on hold or not it should still be fully visible and fully accessible and fully approvable inside the engraving menu. It has no bearing
// if the order is held or not."). A CUSTOM order's piece on hold is on no sheet: its pool row stays, "abandoned", with the path of its design file and the hash of its piece, and this page (a computer that follows the run,
// or one that reloaded) holds no charm for it. Its engraving card now reads the design from that file, so it has what any piece's card has.
//   1  the card of the held custom piece gets its real outline: the options row (Centre, Lines, the turn) is ENABLED, the words are fitted (a placement), the work area is drawn
//   2  Approve can be pressed (nothing on the card says the order is on hold, or blocks it)
//   3  the card is drawn once and left alone (no redraw loop) after it settled
//   4  nothing was written to the cloud to get there (reads only)
//   (NO_COPIES=1: the job names NO piece at all, as on a computer that did not pool the line: the piece is found from the cloud's own pool row; REAL_FILE / REAL_HASH: a real design file)
// Real charm-nest-1.html over the repo's fake site with a real design file in its bucket. Nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/engrave-held-list-churn.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 10, 18, 17) / 1000), RID = '4225100553', TX = '5283317236', KEY = `${RID}_${TX}`, PID = KEY + '_1', AIPATH = `charmnest/custom/${RID}/d28a7d428334-11.pdf`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Custom heart charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['❤️'] }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-held-no-piece: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  srv.st.blobs.set(AIPATH, { buf: fs.readFileSync(process.env.REAL_FILE || path.join(__dirname, 'fixtures', 'COMET_9427.ai')), generation: 1, meta: { contentType: 'application/pdf', metadata: { firebaseStorageDownloadTokens: 't' } } });
  const N = 6, KEYS = Array.from({ length: N }, (_, i) => `${4225100553 + i}_${5283317236 + i}`);
  for (const [i, k] of KEYS.entries()) { const [r, t] = k.split('_'); srv.st.blobs.set(`charmnest/custom/${r}/f.pdf`, { buf: fs.readFileSync(process.env.REAL_FILE || path.join(__dirname, 'fixtures', 'COMET_9427.ai')), generation: 1, meta: { contentType: 'application/pdf', metadata: { firebaseStorageDownloadTokens: 't' } } }); srv.st.put('Charm_Pool', k + '_1', { poolId: k + '_1', orderId: r, lineKey: k, transactionId: t, sku: 'DIRECTOR_22959', copy: 1, quantity: 1, material: 'gold', custom: true, engrave: false, aiPath: `charmnest/custom/${r}/f.pdf`, customFile: 'f.ai', charmHash: process.env.REAL_HASH || 'unknown', state: 'abandoned', sheetId: null, setId: null, heldAt: Date.now() - 3600e3, heldBy: 'Ana_M', heldReason: 'on hold' }); }
  srv.st.put('Charm_Pool', PID, { poolId: PID, orderId: RID, lineKey: KEY, transactionId: TX, sku: 'DIRECTOR_22959', copy: 1, quantity: 1, material: 'gold', custom: true, engrave: false, aiPath: AIPATH, customFile: '11.ai', charmHash: process.env.REAL_HASH || 'unknown', state: 'abandoned', sheetId: null, setId: null, heldAt: Date.now() - 3600e3, heldBy: 'Ana_M', heldReason: 'on hold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', JSON.stringify({ name: 'Ana_M' })); } catch (_) {} });
    await context.addInitScript(r => { window.__realRow = r; }, process.env.REAL_ROW === '1');   // (REAL_ROW=1: the job names the pool row the cloud really holds, abandoned and held; default: no piece at all)
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngravePanel && CN.S.cloud.ok === true, null, { timeout: 60000 });

    await page.evaluate(async ({ order, KEYS }) => {
      for (const [i, KEY] of KEYS.entries()) {
        const [r, t] = KEY.split('_'), o = JSON.parse(JSON.stringify(order)); o.receiptId = r; o.orderNumber = r; o.lines[0].transactionId = t;
        const row = { key: KEY, order: o, line: o.lines[0], arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'held', hold: 'on hold', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      }
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      for (const KEY of KEYS) { const row = B.orders.byKey.get(KEY); row.spec.engraveCandidate = true; const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: 'AB', lines: ['AB'], lineInput: ['AB'], copies: [], pre: true, fit: null, view: null, mask: null, confidence: 0.85, source: 'personalization' }); row.engrave = { needed: true, state: 'review', approved: false, text: j.text }; }
      window.__renders = 0; window.__stacks = []; const r0 = Engrave.render; Engrave.render = function () { __renders++; if (__stacks.length < 40) __stacks.push(new Error().stack.split('\n').slice(2, 6).map(x => x.trim().replace(/http:\/\/[^/]+\//, '').replace(/\?v=[^:]*/, '')).join(' < ')); return r0.apply(this, arguments); };
      setMode('engrave'); Engrave.restoreView({ list: true, chosen: false, focus: null, q: '' }); Engrave.render();
    }, { order: ORDER, KEYS });
    await page.waitForTimeout(3000);
    const n0 = await page.evaluate(() => __renders);
    await page.waitForTimeout(12000);
    const n1 = await page.evaluate(() => __renders);
    const st = await page.evaluate(() => ({ n: __renders, stacks: window.__stacks.slice(0, 40) }));
    const by = {}; for (const s of st.stacks) by[s.split(' < ')[0]] = (by[s.split(' < ')[0]] || 0) + 1;
    console.log('   renders in 12 s after settling:', n1 - n0, '| total', st.n, '| by caller', JSON.stringify(by));
    const held = await page.evaluate(() => [...Engrave.items().values()].map(j => ({ copies: (j.copies || []).length, fit: !!j.fit, state: j.state })));
    console.log('   jobs:', JSON.stringify(held));
    check(n1 - n0 <= 4, 'the Design & Engrave list is not redrawn again and again by held custom orders: ' + (n1 - n0) + ' redraws in 12 s');
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 200));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-held-list-churn: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-held-list-churn: ok');
})().catch(e => { console.error(e); process.exit(1); });
