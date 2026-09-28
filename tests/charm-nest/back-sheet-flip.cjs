// A sheet turned to its Back shows every charm's own engraving, for every order on the sheet (Paul, 28 Sep: "make sure
// I can see all of the back engraving on all of the charms … a standard for all sheets"):
//  · the geometry: CharmNestBacks.engraveOn puts a saved back's words in the charm's own frame, and pushing them back
//    through the back file's own mirror + hoop-up turn (charm-nest-pdf buildBackFile §7b) recovers the very points the
//    laser burns — same place, same size, same mirroring; a charm with no engraving gets nothing;
//  · the order view's Sheet tab flipped to Back: three orders on one sheet, each charm carrying its own words, all three
//    drawn in ink on their own charm (pixels read off the plate), and the plain charm left plain.
//   node tests/charm-nest/back-sheet-flip.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Backs = require(path.join(root, 'charm-nest-backs.js'));
const Geom = require(path.join(root, 'charm-nest-geom.js'));
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const SH = 'sheet-backface-gf';
const A = { rid: '4173711092', tid: '41737110921' }, B2 = { rid: '4173711093', tid: '41737110931' }, C = { rid: '4173711094', tid: '41737110941' };
const PA = `${A.rid}_${A.tid}_1`, PB = `${B2.rid}_${B2.tid}_1`, PC = `${C.rid}_${C.tid}_1`, PD = `${C.rid}_${C.tid}_2`;
// a square charm 34 pt across, as the sorter holds one on a page: outline, centre, size
const SQ = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
const CHARM = { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: SQ, members: [SQ] };
// each piece its own words, its own hoop-up angle and its own place on the back
const BACKS = [
  { poolId: PA, order: A.rid, sku: 'TINY_TAG', copy: 1, text: 'For Mia', lines: ['For Mia'], sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [20, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' },
  { poolId: PB, order: B2.rid, sku: 'LEAF', copy: 1, text: 'Love, Dad', lines: ['Love, Dad'], sizePt: 6.5, capMm: 1.6, weight: 'Regular', lineGap: .216, centre: [17, 21], angle: 0, upAngle: 0, approvedAt: Date.now() - 3000, approvedBy: 'Test Operator' },
  { poolId: PC, order: C.rid, sku: 'DISC', copy: 1, text: 'Ada 2026', lines: ['Ada', '2026'], sizePt: 6, capMm: 1.5, weight: 'Regular', lineGap: .3, centre: [13, 17], angle: 15, upAngle: 200, approvedAt: Date.now() - 2000, approvedBy: 'Test Operator' }
];

/* ── 1 · the geometry, against the back file's own matrices ── */
function stubFont() {
  const unitsPerEm = 1000, cap = 700;
  const w = (t, size) => String(t).length * size * .5;
  return { unitsPerEm, ascender: 1000, tables: { os2: { sCapHeight: cap } },
    getAdvanceWidth: (t, size) => w(t, size),
    getPath: (t, x, y, size) => { const a = w(t, size), h = size * cap / unitsPerEm;
      return { commands: [{ type: 'M', x, y }, { type: 'L', x: x + a, y }, { type: 'C', x: x + a, y: y - h, x1: x + a, y1: y - h / 2, x2: x + a, y2: y - h }, { type: 'L', x, y: y - h }, { type: 'Z' }],
        getBoundingBox: () => ({ x1: x, y1: y - h, x2: x + a, y2: y }) }; } };
}
function geometry() {
  const font = stubFont(), cx = 17, cy = 17;
  const mul = (m, k) => [m[0] * k[0] + m[1] * k[2], m[0] * k[1] + m[1] * k[3], m[2] * k[0] + m[3] * k[2], m[2] * k[1] + m[3] * k[3], m[4] * k[0] + m[5] * k[2] + k[4], m[4] * k[1] + m[5] * k[3] + k[5]];
  const ap = (m, x, y) => [m[0] * x + m[2] * y + m[4], m[1] * x + m[3] * y + m[5]];
  for (const b of BACKS) {
    const geo = Backs.engraveOn(b, CHARM, { font, geom: Geom });
    assert(geo && geo.glyphs && geo.glyphs.length, 'words of ' + b.poolId + ' laid out');
    assert.equal(geo.text, b.lines.join('\n'));
    // the words as the laser file has them, in the back frame
    const want = Geom.layoutLines(b.lines, font, b.sizePt, b.lineGap, b.angle, b.centre);
    // the back file: mirror about the outline's centre, then turn hoop-up (buildBackFile MR = mul(M, R))
    const th = (90 - b.upAngle) * Math.PI / 180, c0 = Math.cos(th), s0 = Math.sin(th);
    const M = [-1, 0, 0, 1, 2 * cx, 0], R = [c0, s0, -s0, c0, cx - cx * c0 + cy * s0, cy - cx * s0 - cy * c0], MR = mul(M, R);
    const mine = geo.glyphs.flatMap(g => g.cmds), theirs = want.glyphs.flatMap(g => g.cmds);
    assert.equal(mine.length, theirs.length, 'the same glyph commands');
    let moved = 0;
    for (let i = 0; i < mine.length; i++) {
      assert.equal(mine[i].type, theirs[i].type);
      for (const [kx, ky] of [['x', 'y'], ['x1', 'y1'], ['x2', 'y2']]) {
        if (mine[i][kx] == null) continue;
        const back = ap(MR, mine[i][kx], mine[i][ky]);                    // the charm's frame → the back file's frame
        assert(Math.abs(back[0] - theirs[i][kx]) < 1e-9 && Math.abs(back[1] - theirs[i][ky]) < 1e-9,
          `${b.poolId}: ${kx} lands where the laser burns it (${back} vs ${[theirs[i][kx], theirs[i][ky]]})`);
        if (Math.abs(mine[i][kx] - theirs[i][kx]) > 1e-6) moved++;
      }
    }
    assert(moved > 0, b.poolId + ': the words are mirrored, not copied straight over');
  }
  // the words are on the charm, not off it, and the size is the laser's own
  const one = Backs.engraveOn(BACKS[0], CHARM, { font, geom: Geom });
  assert.equal(one.size, 7); assert(Math.abs(one.capPt - 7 * .7) < 1e-9, 'cap height of the written size');
  assert.deepEqual(one.centre.map(v => +v.toFixed(6)), [14, 17], 'a back 3 pt right of centre is cut 3 pt left of it');
  // a piece with nothing engraved, and one whose approval was taken back: nothing to draw
  assert.equal(Backs.engraveOn(null, CHARM, { font, geom: Geom }), null);
  assert.equal(Backs.engraveOn({ poolId: PD, text: '' }, CHARM, { font, geom: Geom }), null);
  assert.equal(Backs.engraveOn(Object.assign({}, BACKS[0], { invalidated: true }), CHARM, { font, geom: Geom }), null);
  // no font yet: the words and their place are still known, so the view can say what is there
  const noFont = Backs.engraveOn(BACKS[0], CHARM, { geom: Geom });
  assert(noFont && !noFont.glyphs && noFont.text === 'For Mia' && noFont.centre[0] === 14);
  console.log('  ✓ the words sit where the back file burns them, mirrored and hoop-up, for every saved back');
}

/* ── 2 · the order view's Sheet tab, flipped to Back ── */
const lineOf = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' });
const orderOf = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

async function view() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const at = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 });
  const charms = [{ id: 'g1', name: `${A.rid} · TINY_TAG`, poolId: PA, order: A.rid, sku: 'TINY_TAG' }, { id: 'g2', name: `${B2.rid} · LEAF`, poolId: PB, order: B2.rid, sku: 'LEAF' },
    { id: 'g3', name: `${C.rid} · DISC`, poolId: PC, order: C.rid, sku: 'DISC' }, { id: 'g4', name: `${C.rid} · DISC`, poolId: PD, order: C.rid, sku: 'DISC' }];
  srv.st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, day: '2026-09-28', status: 'written', stock: { wPt: 300, hPt: 150 },
    orders: [A.rid, B2.rid, C.rid], placements: [at('g1', 50, 50), at('g2', 130, 50), at('g3', 210, 50), at('g4', 210, 110)], charms, backPool: BACKS });
  for (const [p, rid, tid, sku] of [[PA, A.rid, A.tid, 'TINY_TAG'], [PB, B2.rid, B2.tid, 'LEAF'], [PC, C.rid, C.tid, 'DISC'], [PD, C.rid, C.tid, 'DISC']])
    srv.st.put('Charm_Pool', p, { poolId: p, orderId: rid, transactionId: tid, lineKey: `${rid}_${tid}`, sku, material: 'gold', copy: +p.split('_').pop(), quantity: p === PC || p === PD ? 2 : 1, state: 'written', sheetId: SH, sheetName: '2026-09-28_GF_Set-1_Sheet-1', updatedAt: Date.now() });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach((line, i) => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); void i; });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [[orderOf(A.rid, 'Mia Lund', [lineOf(A.tid, 'TINY_TAG')]), [PA]], [orderOf(B2.rid, 'Ola Berg', [lineOf(B2.tid, 'LEAF')]), [PB]], [orderOf(C.rid, 'Ada Vik', [lineOf(C.tid, 'DISC')]), [PC, PD]]] });
    // the sheet as this sorter holds it on its page, so every charm has its drawing (a saved .ai is not read here)
    await page.evaluate(({ SH, charms }) => {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      window.__page = { sheetId: SH, metal: 'gold', sheetIndex: 1, page: 1, status: 'complete',
        placements: [['g1', 50, 50], ['g2', 130, 50], ['g3', 210, 50], ['g4', 210, 110]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 34, hPt: 34 })),
        charms: charms.map(c => Object.assign({}, c, { centerPt: [17, 17], widthPt: 34, heightPt: 34, outline: sq, members: [sq] })), rejects: [] };
      S.sheets.gold.pages.push(window.__page);
    }, { SH, charms });
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${A.rid}_${A.tid}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && i.pieces.every(x => x.c) && document.getElementById('owPlateWait').hidden; }, SH, { timeout: 30000 });
    // Back · engraving
    await page.click('#owFace [data-face="back"]');
    await page.waitForFunction(() => { const i = OrderWin._sheet(); if (!i || !i.words) return false;
      return i.words().length === 3 && i.words().every(w => w.drawn) && document.getElementById('owPlateWait').hidden
        && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running'); }, null, { timeout: 30000 });
    const got = await page.evaluate(() => {
      const i = OrderWin._sheet(), cv = document.getElementById('owSheetCv'), ctx = cv.getContext('2d');
      const ink = p => { const d = ctx.getImageData(Math.round(p.x) - 9, Math.round(p.y) - 9, 19, 19).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n; };
      const r = cv.getBoundingClientRect(), onCv = p => ({ x: (p.x - r.left) * cv.width / r.width, y: (p.y - r.top) * cv.height / r.height });
      const words = i.words(), plain = i.pieces.find(x => !words.some(w => w.poolId === x.poolId));
      const nearest = w => { let best = null, bd = Infinity;
        for (const x of i.pieces) { const p = onCv(i.pointOf(x.poolId)), d = Math.hypot(p.x - w.x, p.y - w.y); if (d < bd) { bd = d; best = x.poolId; } }
        return best; };
      return { words: words.map(w => ({ poolId: w.poolId, rid: w.rid, text: w.text, drawn: w.drawn, ink: ink(w), nearest: nearest(w) })),
        plain: { poolId: plain.poolId, ink: ink(onCv(i.pointOf(plain.poolId))) }, foot: document.getElementById('owPlateFoot').textContent };
    });
    const byPool = Object.fromEntries(got.words.map(w => [w.poolId, w]));
    assert.deepEqual(Object.keys(byPool).sort(), [PA, PB, PC].sort(), 'every charm on the sheet that carries words, not only the order opened');
    assert.equal(byPool[PA].text, 'For Mia'); assert.equal(byPool[PB].text, 'Love, Dad'); assert.equal(byPool[PC].text, 'Ada\n2026');
    assert.deepEqual(got.words.map(w => w.rid).sort(), [A.rid, B2.rid, C.rid].sort(), 'one charm of each of the three orders');
    for (const w of got.words) {
      assert(w.drawn, w.poolId + ': its words are drawn as the laser cuts them, not as a note');
      assert(w.ink >= 4, `${w.poolId}: ink on the charm from behind (${w.ink} dark pixels)`);
    }
    assert.deepEqual(got.words.map(w => w.nearest), got.words.map(w => w.poolId), 'each charm\'s words are on that charm, not on a neighbour');
    assert.equal(got.plain.ink, 0, 'the charm with no engraving is drawn plainly, with no words: ' + got.plain.ink);
    assert.match(got.foot, /Back side, mirrored as the laser sees it · 3 engraved/);
    // and back to the Front: the flip is still a flip, and the plate comes back
    await page.click('#owFace [data-face="front"]');
    await page.waitForFunction(() => /Hover a charm/.test(document.getElementById('owPlateFoot').textContent), null, { timeout: 15000 });
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ three orders on one sheet: each charm\'s own words in ink on its own charm, the plain charm plain');
  } finally { await browser.close(); srv.close(); }
}

geometry();
view().then(() => console.log('Back side of a sheet OK')).catch(e => { console.error(e); process.exit(1); });
