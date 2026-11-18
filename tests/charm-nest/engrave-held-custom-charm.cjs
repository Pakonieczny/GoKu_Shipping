// HELDCHARM (Paul, 18 Nov 2026, order 4225100553: "It doesn't matter whether the charm is on hold or not it should still be fully visible and fully accessible and fully approvable inside the engraving menu. It has no bearing
// if the order is held or not."). A CUSTOM order's piece on hold is on no sheet: its pool row stays, "abandoned", with the path of its design file and the hash of its piece, and this page (a computer that follows the run,
// or one that reloaded) holds no charm for it. Its engraving card now reads the design from that file, so it has what any piece's card has.
//   1  the card of the held custom piece gets its real outline: the options row (Centre, Lines, the turn) is ENABLED, the words are fitted (a placement), the work area is drawn
//   2  Approve can be pressed (nothing on the card says the order is on hold, or blocks it)
//   3  the card is drawn once and left alone (no redraw loop) after it settled
//   4  nothing was written to the cloud to get there (reads only)
// Real charm-nest-1.html over the repo's fake site with a real design file in its bucket. Nothing live.   PW_DIR=... CHROMIUM=... node tests/charm-nest/engrave-held-custom-charm.cjs
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
  srv.st.blobs.set(AIPATH, { buf: fs.readFileSync(path.join(__dirname, 'fixtures', 'COMET_9427.ai')), generation: 1, meta: { contentType: 'application/pdf', metadata: { firebaseStorageDownloadTokens: 't' } } });
  srv.st.put('Charm_Pool', PID, { poolId: PID, orderId: RID, lineKey: KEY, transactionId: TX, sku: 'DIRECTOR_22959', copy: 1, quantity: 1, material: 'gold', custom: true, engrave: false, aiPath: AIPATH, customFile: '11.ai', charmHash: 'unknown', state: 'abandoned', sheetId: null, setId: null, heldAt: Date.now() - 3600e3, heldBy: 'Ana_M', heldReason: 'on hold' });
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

    // the order ON HOLD; its pool row is only in the cloud (this page holds no row and no charm for it); its engraving job in Review with the words and no fit
    await page.evaluate(async ({ order, KEY, PID }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'held', hold: 'on hold: the design is not ready', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row);
      Object.assign(j, { state: 'review', text: 'AB', lines: ['AB'], lineInput: ['AB'], copies: [PID], pre: true, fit: null, view: null, mask: null, confidence: 0.85, source: 'personalization' });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__renders = 0; const r0 = Engrave.render; Engrave.render = function () { __renders++; return r0.apply(this, arguments); };
    }, { order: ORDER, KEY, PID });
    srv.st.calls.length = 0;

    await page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back') || Engrave.jobOf(row);
      setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
    }, KEY);
    const read = () => page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back') || Engrave.jobOf(row), c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), q = s => c ? c.querySelectorAll(s).length : -1;
      const centre = c && c.querySelector('[data-a=centre]');
      return { has: !!c, charm: !!(window.Pool && (Pool.charmOf(j.copies[0]) || Pool.previewCharmOf(j.copies[0]))), fit: !!j.fit, state: j.state, centreOn: !!centre && !centre.disabled, linesOn: !!c && !!c.querySelector('[data-a=linecount]') && !c.querySelector('[data-a=linecount]').disabled,
        approveOn: c ? [...c.querySelectorAll('[data-a=approve]')].filter(b => !b.disabled).length : -1, held: c ? /on hold, so its piece is not on a sheet/.test(c.innerText) : null, canvases: q('canvas'), text: c ? c.innerText.replace(/\s+/g, ' ').slice(0, 160) : '' };
    }, KEY);
    let d = await read();
    for (let i = 0; i < 80 && !(d.fit && d.centreOn); i++) { await page.waitForTimeout(250); d = await read(); }
    console.log('   card:', JSON.stringify(d).slice(0, 500));
    if (shots) await page.screenshot({ path: path.join(shots, 'held-custom.png') });
    check(d.has && d.charm, '1a the piece\'s charm is read from its design file (no row, no sheet, no charm on this page before)');
    check(d.fit && d.state === 'review', '1b its words are fitted: a placement to approve (state ' + d.state + ')');
    check(d.centreOn && d.linesOn, '1c the options row under the work area is enabled: ' + JSON.stringify({ centre: d.centreOn, lines: d.linesOn }));
    check(d.canvases >= 1, '1d the work area is drawn: ' + d.canvases + ' canvas(es)');
    check(d.approveOn >= 1 && d.held === false, '2 Approve can be pressed and the card does not say it is on hold: ' + JSON.stringify({ approveOn: d.approveOn, held: d.held }));
    await page.waitForTimeout(1500);
    const churn = await page.evaluate(() => new Promise(res => { const n0 = __renders; setTimeout(() => res(__renders - n0), 4000); }));
    check(churn <= 1, '3 left alone for 4 s the card is not drawn again and again: ' + churn);
    const ops = [...new Set(srv.st.calls.filter(c => c.body && c.body.op).map(c => c.body.op))];
    console.log('   cloud ops during the card:', JSON.stringify(ops));
    const mine = ops.filter(o => /^(poolUpdate|poolPut|backPut|backInvalidate|engraveSyncPut|engravePut|runLinesPatch|runCmdPut|setUpdate|putSheet|approve)/i.test(o));
    check(mine.length === 0, '4 opening the card changed no piece, back, set, sheet or run record in the cloud (reads of the design file and the order only): ' + JSON.stringify(mine));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-held-custom-charm: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-held-custom-charm: ok');
})().catch(e => { console.error(e); process.exit(1); });
