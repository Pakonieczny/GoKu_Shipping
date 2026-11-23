// BROKENBACK (Paul, 22 Nov 2026, order 4238583182, READER_97110 x4 "Book Lover" on SS Sheet 1, the order window's Sheet tab turned to "Back · engraving": "For some reason some order show a broken
// version of the back engraving. This has resolved it's self previously but I have now seen it too many times to ignore it." The panel beside the plate said "No preview here: see the back in
// Design & Engrave": the four engraving jobs were in Review with the cloud's place for their words (keepPlace) but NO fit of this computer's own (a job taken from the cloud is fitted again
// later: CharmNestEngraveSync take, fitMissing, watchOpen). The plate read a job's words only from that fit (SheetWin backOfJob: j.fit + j.view), so each book got the "words not drawable yet"
// label instead — an italic "Book Lover" box ~80 px wide centred on each charm, wider than the charm, the two top books' boxes drawn over one another. It came right by itself once the job was
// fitted here, and broke again at the next take.
// The fixture is a read-only copy of the live records (the sheet trimmed to the two orders of the screenshots, the four pool rows, the cloud's engraving line); the master drawing is a stand-in
// of the book's own size and centre.
//   P  the viewer state of the screenshot: the line taken from the cloud as at a page start, four jobs in Review, no fit, the cloud's place kept; the card says "No preview here"
//   B1 Back · engraving: every book's words are drawn as the laser burns them (from the cloud's place), none as a label
//   B2 each book's words are on that book, and no label is drawn over the neighbours (no label ink on the plate)
//   B3 the words drawn from the cloud's place are where they are drawn once this computer fits the job at that place (no jump when the fit lands)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/sheet-back-pending-words.cjs
'use strict';
const path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const FX = require('./sheet-back-pending-words.fixture.json');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };

const RID = '4238583182', TID = '5298803874', KEY = `${RID}_${TID}`, SH = FX.sheet.id, BOOKS = [1, 2, 3, 4].map(n => `${KEY}_${n}`);
// a stand-in for the book's drawing (BOOK_23102.ai): its size (35.02 x 30.3 pt, the record's) about the centre the cloud's place was fitted on (view cx, cy)
const BX = [2.502, 2.5026, 37.522, 32.8026];
const BOOK = { kind: 'path', subpaths: [[['m', [BX[0], BX[1]]], ['l', [BX[2], BX[1]]], ['l', [BX[2], BX[3]]], ['l', [BX[0], BX[3]]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: BX, closed: true };
const CHARM = { centerPt: [(BX[0] + BX[2]) / 2, (BX[1] + BX[3]) / 2], widthPt: BX[2] - BX[0], heightPt: BX[3] - BX[1], outline: BOOK, members: [BOOK], bbox: BX, hash: '4e60cb6d' };
const SHIP = Math.floor(Date.UTC(2026, 10, 25, 17) / 1000), DAY = 86400;
const line = { transactionId: TID, listingId: '1800097110', sku: 'READER_97110', title: 'Book lover necklace', quantity: 4, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Sterling Silver' }], metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: ['Book Lover'], buyerMessage: '' };
const order = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Reader' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('SKIP no playwright-core'); return; }
  const srv = await start({ receipts: [] }), st = srv.st;
  st.put('Charm_Nest_Sheets', SH, FX.sheet);
  for (const p of FX.pools) st.put('Charm_Pool', p.poolId, Object.assign({}, p, { updatedAt: Date.now() }));
  // the cloud's engraving line, as another computer wrote it (four jobs in Review, each with its place)
  const put = await (await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ op: 'engraveSyncPut', by: 'Michelle_R', dev: 'dother0001', lines: [{ key: FX.line.key, sig: 'other', rec: FX.line.rec }] }) })).json();
  if (!put || put.error) throw new Error('engraveSyncPut: ' + JSON.stringify(put));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: +process.env.VW || 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && window.Engrave && window.CharmNestEngraveSync && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
    // the master drawing, as this computer keeps it (a stand-in of the book's size): every piece of the saved sheet is drawn from it
    await page.evaluate(CHARM => { Pool.keptMaster = async () => ({ sku: 'READER_97110', charms: [Object.assign({}, CHARM)] }); }, CHARM);
    await page.evaluate(async ({ order, pools }) => {
      await Orders.loadMaps(true);
      const ln = order.lines[0], key = CharmNestOrders.lineKey(order, ln);
      const row = { key, order, line: ln, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: 'silver' };
      B.orders.rows.push(row); B.orders.byKey.set(key, row); Orders.interpretAll(); if (row.spec) row.spec.engraveCandidate = true;
      CN.setMode('orders'); Orders.render();
    }, { order, pools: BOOKS });
    // the line taken from the cloud as a page start takes it (CharmNestEngraveSync take, why "start"): the fields as sent, the cloud's place kept for the refit, no fit of this computer's own
    await page.evaluate(async RID => { const l = (await CN.api('charmNestLibrary', { op: 'engraveSyncGet', orders: [RID] })).lines[0]; await CharmNestEngraveSync._take(l, 'start'); }, RID);
    const jobs = () => page.evaluate(BOOKS => BOOKS.map(p => { const j = [...Engrave.items().values()].find(x => (x.copies || []).includes(p)) || null;
      return j && { key: j.key, state: j.state, fit: !!j.fit, view: !!j.view, keep: j.keepPlace ? j.keepPlace.centre : null, text: j.text }; }), BOOKS);
    let js = await jobs();
    // (a job is made for each copy; its piece is its own pool id)
    if (js.some(j => !j)) await page.evaluate(({ KEY, BOOKS }) => { const row = B.orders.byKey.get(KEY); for (const j of Engrave.jobsOf(row)) { const m = /#C(\d+)$/.exec(j.key); if (m && !(j.copies || []).length) j.copies = [BOOKS[+m[1] - 1]]; } }, { KEY, BOOKS });
    js = await jobs();
    check('P1 the viewer state: the four jobs in Review, the words "Book Lover", the cloud\'s place kept, no fit of this computer\'s own',
      js.every(j => j && j.state === 'review' && j.text === 'Book Lover' && !j.fit && !j.view && j.keep && Math.abs(j.keep[0] - 20.012) < .01), JSON.stringify(js));

    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), KEY);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && i.pieces.every(x => x.c) && document.getElementById('owPlateWait').hidden; }, SH, { timeout: 30000 });
    const card = await page.evaluate(() => (document.querySelector('#owSheetPanel [data-engraving-panel]') || {}).textContent || '');
    check('P2 the card beside the plate says what the screenshot says ("To approve", "No preview here")', /No preview here/.test(card) && /approv/i.test(card), card.slice(0, 300));

    await page.click('#owFace [data-face="back"]');
    await page.waitForFunction(() => { const i = OrderWin._sheet(); return i && i.words && /Back side/.test(document.getElementById('owPlateFoot').textContent) && document.getElementById('owPlateWait').hidden
      && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running') && !document.querySelector('.mLand'); }, null, { timeout: 30000 });
    await page.waitForTimeout(600);
    const plate = () => page.evaluate(BOOKS => {
      const i = OrderWin._sheet(), cv = document.getElementById('owSheetCv'), ctx = cv.getContext('2d'), r = cv.getBoundingClientRect(), onCv = p => ({ x: (p.x - r.left) * cv.width / r.width, y: (p.y - r.top) * cv.height / r.height });
      const all = ctx.getImageData(0, 0, cv.width, cv.height).data; let label = 0;
      // the label's own ink (#6a4f1c, the "words not drawable yet" box's words) anywhere on the plate
      for (let j = 0; j < all.length; j += 4) if (Math.abs(all[j] - 106) < 14 && Math.abs(all[j + 1] - 79) < 14 && Math.abs(all[j + 2] - 28) < 14) label++;
      const centres = Object.fromEntries(i.pieces.map(x => [x.poolId, onCv(i.pointOf(x.poolId))]));
      const words = i.words().filter(w => BOOKS.includes(w.poolId)).map(w => { let best = null, bd = Infinity; for (const [pid, c] of Object.entries(centres)) { const d = Math.hypot(c.x - w.x, c.y - w.y); if (d < bd) { bd = d; best = pid; } }
        return { poolId: w.poolId, text: w.text, drawn: w.drawn, x: w.x, y: w.y, nearest: best }; });
      // the hatching blue of the words inside each book (a box 40 % of its size about the point its words are drawn at)
      const blue = w => { const d = ctx.getImageData(Math.round(w.x) - 25, Math.round(w.y) - 12, 50, 24).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (Math.abs(d[j] - 26) < 30 && Math.abs(d[j + 1] - 95) < 30 && Math.abs(d[j + 2] - 196) < 30) n++; return n; };
      return { words: words.map(w => Object.assign(w, { blue: w.drawn ? blue(w) : 0 })), label, k: cv.width / r.width };
    }, BOOKS);
    const b = await plate();
    if (process.env.SHOT) await page.locator('#owPlateWrap').screenshot({ path: process.env.SHOT });
    check('B1 Back · engraving: every book\'s words are drawn as the laser burns them, from the cloud\'s place (none left as a label)',
      b.words.length === 4 && b.words.every(w => w.text === 'Book Lover' && w.drawn && w.blue >= 20), JSON.stringify(b.words));
    check(`B2 each book's words are on that book, and no "Book Lover" label is drawn over the neighbours (${b.label} label pixels)`,
      b.words.every(w => w.nearest === w.poolId) && b.label === 0, JSON.stringify({ label: b.label, words: b.words.map(w => [w.poolId.slice(-1), w.nearest && w.nearest.slice(-1)]) }));
    // the job fitted on this computer AT the cloud's place (what fitJob keeps, keepPlace): the words are where they were already drawn
    await page.evaluate(BOOKS => { for (const p of BOOKS) { const j = [...Engrave.items().values()].find(x => (x.copies || []).includes(p)), k = j.keepPlace;
      j.fit = { centre: k.centre.slice(), angle: k.angle || 0, size: k.size, weight: k.weight, lines: k.lines.slice(), fittedMax: k.fittedMax, layout: { lineGap: k.lineGap } }; j.view = { upAngle: k.upAngle, cx: k.cx, cy: k.cy, members: [] }; }
      // (the words worked out for each piece are forgotten, as a drawing does when what it draws from changes: dropGeo)
      const i = OrderWin._sheet(); for (const x of i.pieces) { x._engGeo = null; x._engGeoKey = null; } i.redraw(); }, BOOKS);
    await page.waitForTimeout(300);
    const b3 = await page.evaluate(BOOKS => OrderWin._sheet().words().filter(w => BOOKS.includes(w.poolId)).map(w => ({ poolId: w.poolId, x: w.x, y: w.y, drawn: w.drawn })), BOOKS);
    const moved = b.words.map(w => { const f = b3.find(q => q.poolId === w.poolId); return f && w.drawn && f.drawn ? Math.hypot(f.x - w.x, f.y - w.y) : Infinity; });
    check(`B3 the words drawn from the cloud's place are where they are drawn once the job is fitted here at that place (moved ${moved.map(v => v.toFixed(2)).join(', ')} px)`, moved.every(v => v < 0.5), JSON.stringify({ before: b.words, after: b3 }));
    check('B4 no page error', errors.length === 0, JSON.stringify(errors).slice(0, 400));
  } finally { await browser.close(); srv.close(); }
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
