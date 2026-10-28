// SHEETBACK (Paul, 28 Oct 2026, order 4214126694, REPTILE 5 on "GF Draft 10", the Sheet tab turned to "Back · engraving": "the back engraving text and the image did not make it to the backside
// of this charm on the sheet as you can see in the attached image. If I use her attaches an image to a given charm, that image must be perfectly visible along with all the other charm details in
// all thumbnail previews throughout the entire application for that particular charm and order").
// The back was approved after the Charm Editor: the words "Cym" typed there and a traced image are the charm's artBack (charm-nest-art-edit.js), the back's own fit holds no words (artOnly, its
// record's lines []), so the plate — which drew only a back record's words — showed that charm bare while every other charm showed its words. The fixture is the live record of that piece's back
// edit (read-only copy: the words and two of the image's eight grey levels), put on a stand-in charm.
//   A  (node) the record put over a charm: three back parts, the words kept with their outlines (artText "Cym"), the image's greys kept as greys
//   B  the order window's Sheet tab turned to Back (a sheet this page holds): the REPTILE's back is drawn — the words' and the image's ink on the charm, where they lie, the image's light grey in
//      its own grey; the charm with words keeps them; the plain charm stays plain; the foot counts it engraved; the sheet window's own Back draws it too
//   C  a SAVED sheet this page does not hold (its piece drawn from the master): the piece's own edit is read (artList, once, by the "~ae" its record's hash carries) and its back drawn on the plate
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/sheet-back-editor-art.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const REC = require('./sheet-back-editor-art.fixture.json');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}  ${ok ? '' : evidence || ''}`.trimEnd()); };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 30, 17) / 1000);
const SH = 'sheet-gf-draft-10', SH2 = 'sheet-gf-saved-11';
const A = { rid: '4214126694', tid: '5267506322' }, B2 = { rid: '4214126695', tid: '5267506323' }, C = { rid: '4214126696', tid: '5267506324' }, E = { rid: '4214126697', tid: '5267506325' };
const PA = `${A.rid}_${A.tid}_1`, PB = `${B2.rid}_${B2.tid}_1`, PC = `${C.rid}_${C.tid}_1`, PE = `${E.rid}_${E.tid}_1`;
// a stand-in charm 36 pt across (the REPTILE's own drawing is a master file): the live record's parts lie inside it (x 6.1–29.0, y 13.7–29.0)
const SQ = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [36, 0]], ['l', [36, 36]], ['l', [0, 36]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 36, 36], closed: true };
const CHARM = { centerPt: [18, 18], widthPt: 36, heightPt: 36, outline: SQ, members: [SQ], bbox: [0, 0, 36, 36], hash: 'sq36' };
const artRec = poolId => Object.assign({}, REC, { poolId, _note: undefined });
const NOW = Date.now();
// the approved back of a piece whose engraving is the editor's parts alone (writeBacksOnce's record of an artOnly fit), and a back with words of its own
const artOnlyBack = (poolId, order) => ({ poolId, order, sku: 'REPTILE 5', copy: 1, artOnly: true, text: '', lines: [], sizePt: 0, capMm: 0, weight: 'Regular', centre: [18, 18], angle: 0, upAngle: 90, approvedAt: NOW - 5000, approvedBy: 'Paul_K' });
const wordsBack = { poolId: PB, order: B2.rid, sku: 'DISC', copy: 1, text: 'Denney', lines: ['Denney'], sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [18, 18], angle: 0, upAngle: 90, approvedAt: NOW - 4000, approvedBy: 'Paul_K' };

/* ═══ A · the record over a charm ═══ */
function partA() {
  global.CharmNestGeom = global.CharmNestGeom || require(path.join(root, 'charm-nest-geom.js'));
  const AE = require(path.join(root, 'charm-nest-art-edit.js'));
  const out = AE.apply(CHARM, artRec(PA), 'back'), art = out.artBack || [];
  check('A1 the live back edit put over a charm: three parts on its back (the words and two grey levels of the image), none refused, a hash of its own',
    art.length === 3 && out.artEdit && out.artEdit.back && !out.artEdit.back.refused.length && /~ae[0-9a-z]+$/.test(out.hash), JSON.stringify(out.artEdit));
  check('A2 the words keep their text with their outlines ("Cym", hatched blue); the image keeps its greys (artGrey, 0.875 and 0)',
    art.some(s => s.artText === 'Cym' && s.hatchBlue && s.fill) && art.filter(s => s.artGrey).map(s => s.fillRGB[0]).sort().join() === '0,0.875', JSON.stringify(art.map(s => [s.artText, s.artGrey, s.fillRGB])));
}

/* ═══ B, C · the page ═══ */
const lineOf = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' });
const orderOf = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

async function partBC() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('SKIP no playwright-core'); return; }
  const srv = await start({ receipts: [] }), st = srv.st;
  const at = (id, cx, cy) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 36, hPt: 36 });
  const charms = [{ id: 'g1', name: `${A.rid} · REPTILE 5`, poolId: PA, order: A.rid, sku: 'REPTILE 5' }, { id: 'g2', name: `${B2.rid} · DISC`, poolId: PB, order: B2.rid, sku: 'DISC' }, { id: 'g3', name: `${C.rid} · DISC`, poolId: PC, order: C.rid, sku: 'DISC' }];
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 10, day: '2026-10-28', status: 'complete', draft: true, stock: { wPt: 300, hPt: 150 }, orders: [A.rid, B2.rid, C.rid], placements: [at('g1', 50, 60), at('g2', 130, 60), at('g3', 210, 60)], charms, poolIds: [PA, PB, PC], backPool: [artOnlyBack(PA, A.rid), wordsBack] });
  for (const [p, rid, tid, sku] of [[PA, A.rid, A.tid, 'REPTILE 5'], [PB, B2.rid, B2.tid, 'DISC'], [PC, C.rid, C.tid, 'DISC']])
    st.put('Charm_Pool', p, { poolId: p, orderId: rid, transactionId: tid, lineKey: `${rid}_${tid}`, sku, material: 'gold', copy: 1, quantity: 1, state: 'engraved', sheetId: SH, updatedAt: Date.now(), ...(p === PA ? { artEdit: { back: REC.at } } : {}) });
  // C: a saved sheet this page does not hold; its piece's record names the master it was cut from and carries the edit's "~ae" mark
  st.put('Charm_Nest_Sheets', SH2, { id: SH2, metal: 'gold', sheetIndex: 11, day: '2026-10-28', status: 'complete', stock: { wPt: 200, hPt: 120 }, orders: [E.rid], placements: [at('h1', 60, 60)], poolIds: [PE],
    sources: [{ id: 'src-reptile', pool: true, sku: 'REPTILE 5', path: 'charmnest/master/REPTILE_5-SALAMANDER.ai', hash: 'mh-reptile', name: 'REPTILE 5' }],
    charms: [{ id: 'h1', name: `${E.rid} · REPTILE 5`, poolId: PE, order: E.rid, sku: 'REPTILE 5', sourceId: 'src-reptile', index: 0, hash: 'sq36~ae1x2y3z' }], backPool: [artOnlyBack(PE, E.rid)] });
  st.put('Charm_Pool_Art', PE, artRec(PE));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && window.CharmNestArtEdit && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => { try { if (window.CNRole && CNRole.state() === 'ask') CNRole.choose('design'); } catch (_) { /* no role step */ } });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [order, pools] of orders) order.lines.forEach(line => { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, { orders: [[orderOf(A.rid, 'Jenna Keaton', [lineOf(A.tid, 'REPTILE 5')]), [PA]], [orderOf(B2.rid, 'Ola Berg', [lineOf(B2.tid, 'DISC')]), [PB]], [orderOf(C.rid, 'Ada Vik', [lineOf(C.tid, 'DISC')]), [PC]], [orderOf(E.rid, 'Eva Lind', [lineOf(E.tid, 'REPTILE 5')]), [PE]]] });
    // the draft sheet as this sorter holds it: the REPTILE's charm carries its back edit, as CharmNestArtEdit.save's swap leaves it on the page
    await page.evaluate(({ SH, charms, CHARM, rec }) => {
      const base = c => Object.assign({}, c, CHARM, { id: c.id });
      const list = charms.map(c => c.poolId === rec.poolId ? Object.assign(CharmNestArtEdit.apply(base(c), rec, 'back'), { id: c.id }) : base(c));
      window.__page = { sheetId: SH, metal: 'gold', sheetIndex: 10, page: 10, status: 'complete', draft: true,
        placements: [['g1', 50, 60], ['g2', 130, 60], ['g3', 210, 60]].map(([id, cx, cy]) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: 36, hPt: 36 })), charms: list, rejects: [] };
      S.sheets.gold.pages.push(window.__page);
    }, { SH, charms, CHARM, rec: artRec(PA) });

    /* ── B · the order window's Sheet tab, turned to Back ── */
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), `${A.rid}_${A.tid}`);
    await page.evaluate(() => OrderWin.view() === 'sheet' || document.querySelector('.owTabsV [data-ow-view="sheet"]').click());
    await page.waitForFunction(sh => { const i = OrderWin._sheet(); return i && i.sheet.id === sh && i.pieces.every(x => x.c) && document.getElementById('owPlateWait').hidden; }, SH, { timeout: 30000 });
    await page.click('#owFace [data-face="back"]');
    await page.waitForFunction(pa => { const i = OrderWin._sheet(); if (!i || !i.words) return false; const w = i.words();
      return w.some(x => x.poolId === pa && x.drawn) && w.filter(x => x.drawn).length >= 2 && document.getElementById('owPlateWait').hidden && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running'); }, PA, { timeout: 30000 }).catch(() => {});
    const plateOf = () => page.evaluate(({ PA, PC }) => {
      const i = OrderWin._sheet(), cv = document.getElementById('owSheetCv'), ctx = cv.getContext('2d'), r = cv.getBoundingClientRect(), onCv = p => ({ x: (p.x - r.left) * cv.width / r.width, y: (p.y - r.top) * cv.height / r.height });
      // the pixels inside a piece (a box 60% of its size about its centre): dark ink, and the image's light grey (0.875: 223) apart from the plate's own cream
      const k = Math.abs(onCv(i.pointOf(PC)).x - onCv(i.pointOf(PA)).x) / 160;   // (canvas px per pt: the REPTILE and the plain charm are 160 pt apart)
      const read = poolId => { const c = onCv(i.pointOf(poolId)), s = Math.max(6, Math.round(36 * 0.4 * k));
        const d = ctx.getImageData(Math.round(c.x) - s, Math.round(c.y) - s, 2 * s, 2 * s).data; let ink = 0, grey = 0;
        for (let j = 0; j < d.length; j += 4) { const R = d[j], G = d[j + 1], B = d[j + 2]; if (R + G + B < 330) ink++; else if (Math.abs(R - 223) < 10 && Math.abs(G - 223) < 10 && Math.abs(B - 223) < 10) grey++; }
        return { ink, grey }; };
      const w = i.words(), a = w.find(x => x.poolId === PA) || null;
      return { words: w.map(x => ({ poolId: x.poolId, text: x.text, art: x.art || 0, drawn: x.drawn })), a, A: read(PA), C: read(PC), foot: document.getElementById('owPlateFoot').textContent,
        nearest: a ? i.pieces.map(x => [x.poolId, Math.hypot(onCv(i.pointOf(x.poolId)).x - a.x, onCv(i.pointOf(x.poolId)).y - a.y)]).sort((p, q) => p[1] - q[1])[0][0] : null };
    }, { PA, PC });
    const b1 = await plateOf();
    check('B1 the order window\'s Back · engraving: the REPTILE\'s back made in the Charm Editor is on the plate — its three parts, "Cym" its words, drawn, on that charm (not a neighbour)',
      !!b1.a && b1.a.art === 3 && b1.a.text === 'Cym' && b1.a.drawn && b1.nearest === PA, JSON.stringify(b1.words) + ' nearest ' + b1.nearest);
    check(`B2 the plate's own pixels: the words' and the image's ink on the REPTILE (${b1.A.ink} dark), the image's light grey in its own grey (${b1.A.grey}); the plain charm stays plain (${b1.C.ink})`,
      b1.A.ink >= 40 && b1.A.grey >= 10 && b1.C.ink === 0, JSON.stringify({ A: b1.A, C: b1.C }));
    check('B3 the charm with words of its own keeps them; the foot counts both engraved backs', b1.words.some(x => x.poolId === PB && x.text === 'Denney' && x.drawn) && /2 engraved/.test(b1.foot), JSON.stringify({ words: b1.words, foot: b1.foot }));
    await page.click('#owFace [data-face="front"]');
    await page.waitForFunction(pa => /Hover a charm/.test(document.getElementById('owPlateFoot').textContent) && OrderWin._sheet() && OrderWin._sheet().pointOf(pa) && !document.getElementById('owSheetCv').getAnimations().some(a => a.playState === 'running'), PA, { timeout: 15000 }).catch(() => {});
    const f1 = await plateOf();
    check('B4 turned back to the Front, the back\'s parts are not on the front of the plate', f1.A.grey === 0, JSON.stringify(f1.A));

    // the sheet window's own plate, turned over
    await page.click('#owSheetPanel [data-full]');
    await page.waitForFunction(sh => SheetWin.isOpen() && SheetWin.current() === sh && SheetWin._W.geom && !SheetWin._W.flying && !SheetWin._W.el.plate.getAnimations().some(a => a.playState === 'running'), SH, { timeout: 30000 });
    const readW = () => page.evaluate(() => { const W = SheetWin._W, cv = W.el.base, ctx = cv.getContext('2d');
      return Object.fromEntries(W.pieces.map(x => { const cx = W.R + (W.face === 'back' ? W.st.wPt - x.p.cxPt : x.p.cxPt) * W.k, cy = W.R + x.p.cyPt * W.k, r = Math.round(x.p.wPt * W.k * .3);
        const d = ctx.getImageData(Math.round(cx) - r, Math.round(cy) - r, 2 * r, 2 * r).data; let ink = 0, grey = 0;
        for (let j = 0; j < d.length; j += 4) { const R = d[j], G = d[j + 1], B = d[j + 2]; if (R + G + B < 330) ink++; else if (Math.abs(R - 223) < 10 && Math.abs(G - 223) < 10 && Math.abs(B - 223) < 10) grey++; }
        return [x.poolId, { ink, grey }]; })); });
    await page.click('.swStrip [data-r2=face]');
    await page.waitForFunction(() => SheetWin._W.face === 'back' && !SheetWin._W.el.plate.getAnimations().some(a => a.playState === 'running'), null, { timeout: 20000 });
    await page.waitForTimeout(400);
    const wb = await readW();
    check(`B5 the sheet window turned to its Back draws the REPTILE's editor back too (${wb[PA].ink} dark, ${wb[PA].grey} grey), the words of the other, the plain charm plain`, wb[PA].ink >= 40 && wb[PA].grey >= 10 && wb[PB].ink >= 4 && wb[PC].ink === 0, JSON.stringify(wb));
    await page.click('.swStrip [data-r2=face]');
    await page.waitForFunction(() => SheetWin._W.face === 'front', null, { timeout: 20000 });
    await page.evaluate(() => { try { SheetWin.close(); } catch (_) {} try { OrderWin.close(); } catch (_) {} });

    /* ── C · a saved sheet this page does not hold: the piece drawn from its master, with its own edit read ── */
    const c1 = await page.evaluate(async ({ SH2, PE, CHARM }) => {
      // (the master the record names, as this computer keeps it: a stand-in charm with the hash the record was cut from)
      Pool.keptMaster = async () => ({ sku: 'REPTILE 5', charms: [Object.assign({}, CHARM)] });
      const host = document.createElement('div'); host.style.cssText = 'position:fixed;left:0;top:0;width:600px;height:380px;z-index:99999;background:#fff'; const cv = document.createElement('canvas'); host.appendChild(cv); document.body.appendChild(host);
      const info = await SheetWin.drawOrder(cv, SH2, PE.split('_')[0], { back: true });
      for (let t0 = Date.now(); Date.now() - t0 < 15000; await new Promise(r => setTimeout(r, 150))) { const w = info.words(); if (w.some(x => x.poolId === PE && x.drawn)) break; }
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      const x = info.pieces.find(p => p.poolId === PE), w = info.words().find(q => q.poolId === PE) || null, ctx = cv.getContext('2d');
      const d = w ? ctx.getImageData(Math.round(w.x) - 20, Math.round(w.y) - 20, 40, 40).data : []; let ink = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) ink++;
      host.remove();
      return { art: x && x.c && (x.c.artBack || []).length, hash: x && x.c && x.c.hash, w: w && { text: w.text, art: w.art, drawn: w.drawn }, ink };
    }, { SH2, PE, CHARM });
    const reads = st.calls.filter(x => x.name === 'charmNestLibrary' && x.op === 'artList').length;
    check(`C1 a saved sheet not held here: its piece, drawn from the master, carries its own back edit (read once, by its record's "~ae" mark: ${reads} artList) and its back is drawn on the plate (${c1.ink} dark pixels)`,
      c1.art === 3 && c1.w && c1.w.text === 'Cym' && c1.w.drawn && c1.ink >= 20 && reads === 1, JSON.stringify({ c1, reads }));
    check('B6 no page error', errors.length === 0, JSON.stringify(errors).slice(0, 400));
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  partA();
  await partBC();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\n${RESULTS.length - bad.length}/${RESULTS.length} checks pass`);
  process.exit(bad.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
