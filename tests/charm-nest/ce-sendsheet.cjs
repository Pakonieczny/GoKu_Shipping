// SEND TO SHEET (Design & Engrave, items 17, 22, 24-26, 28, 29; D4, D5, D7. Paul, 28 Oct 2026: "custom orders that need a custom charm design now must all be able
// to be opened in the charm design designer ... save it and then that would get attached to the Custom order and that design would be sent to the next available sheet
// and the colour of the sheet would have to be decided by the user at that same time ... some sort of a sophisticated pop-up modal that presents the user with all of
// these options" · "sometimes the user just needs to add a charm onto a sheet and that custom order can be named by the user" · "who creates the errors and who is
// creating the solutions"). charm-nest-custom-piece.js.
//   A  (node) the pure parts: a design's exact size read through the editor's own transforms (a turned cut line), its exact-size picture, the sheet a piece goes onto
//      (the order's own sheet, the earliest open GF / SS sheet, the newest open RG; none: a new sheet), who made the original mistake from an order's history
//   B  the real sorter page over the fake site (bridge-server.cjs; offline): a Custom Orders card's "Open in Charm Designer" → the Charm Designer opens blank on that
//      order → a shape is drawn and made its cut line → Save → the design is kept as a version attached to the order and the pop-up opens prefilled with it (the design drawn at its exact
//      size and mm) → SS picked (the sheet it goes onto named) → Send: the pool is asked for one handmade custom piece of THAT design version on SS for THAT order, one
//      customPieceSent event, the piece is on an SS sheet (the pop-up names that sheet next time), the custom order is released from Review (its line done) and its order
//      window offers its design to edit. Then an orderless piece: named, "Fix a mistake", the order it fixes found by its number,
//      who made the mistake read from that order's history (suggested, changed by the person), GF × 2 → the pool and the log carry both people, and the fixed order's
//      timeline says who fixed it and whose mistake it was. Then "Send to sheet…" from inside the Charm Editor: the pop-up opens over the editor and keys typed in it never
//      reach the editor (its tool, its parts).
//   The design library, the pool's custom side and the design log are the real ones (charm-nest-designs.js, charm-nest-custom-pool.js, charm-nest-design-log.js) over the fake
//   site's real charmNestLibrary handler, their calls recorded; a page without one of them gets a small stand-in, and the line above the checks says which ran.
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome> node tests/charm-nest/ce-sendsheet.cjs     (through scratchpad/int/slot.sh; SS_ONLY=A or B)
'use strict';
const assert = require('node:assert/strict'), path = require('node:path');
const ROOT = path.join(__dirname, '../..');
const RESULTS = [];
const check = (id, ok, evidence) => { RESULTS.push({ id, ok: !!ok }); console.log(`${ok ? 'PASS' : 'FAIL'}  ${id}${ok && evidence ? '  · ' + evidence : ''}${!ok ? '  ' + (evidence || '') : ''}`); };
const ONLY = process.env.SS_ONLY || '';
const PT = 72 / 25.4, MMPT = 25.4 / 72;

/* ═══ A · the pure parts ═══ */
function partA() {
  global.CharmEditor = require(path.join(ROOT, 'charm-nest-charm-editor.js'));
  delete require.cache[require.resolve(path.join(ROOT, 'charm-nest-custom-piece.js'))];
  const CP = require(path.join(ROOT, 'charm-nest-custom-piece.js')), T = CP._t, Mo = global.CharmEditor.model, geo = Mo.createGeo(null);
  // a cut rectangle 18 × 12 mm turned 90° and moved, a filled word, a hidden engraving: the editor's result as it hands it back
  const items = [
    { id: 'cut', kind: 'path', role: 'cut', subpaths: Mo.rectSP(0, 0, 18 * PT, 12 * PT), tx: 7, ty: -3, sx: 1, sy: 1, rot: 90 },
    { id: 'w', kind: 'path', role: 'hatch', fill: [0.5, 0.5, 0.5], subpaths: Mo.rectSP(20, 0, 30, 10), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 },
    { id: 'off', kind: 'path', role: 'line', hidden: true, stroke: true, subpaths: Mo.lineSP(-200, -200, 400, 400) }];
  const out = Mo.exportItems(items, geo), design = { name: 'Turned tag', faces: { front: { face: 'front', unitPt: true, frame: null, items: out } } };
  const sz = T.sizeOf(design);
  check('A1 the exact size is the cut line\'s, through the editor\'s own transform (a 18 × 12 mm tag turned 90° is 12 × 18 mm); a hidden engraving is not drawn',
    Math.abs(sz.wMm - 12) < 1e-6 && Math.abs(sz.hMm - 18) < 1e-6 && T.leavesOf(design.faces.front).length === 2, `${sz.wMm.toFixed(4)} × ${sz.hMm.toFixed(4)} mm`);
  const pic = T.previewSvg(design, { metal: 'silver', fitMm: [58, 46] }), big = T.previewSvg({ faces: { front: { items: [{ kind: 'path', role: 'cut', subpaths: Mo.rectSP(0, 0, 90 * PT, 30 * PT) }] } } }, { fitMm: [58, 46] });
  const wAttr = +/width="([\d.]+)mm"/.exec(pic.svg)[1], grey = /fill-opacity="0\.5"/.test(pic.svg);
  check('A2 the picture is drawn at its exact size in CSS millimetres (the tag and its 1.2 mm margin), the cut filled in the metal, a traced grey kept as its grey; a 90 mm design is drawn smaller and says how much',
    pic.scale === 1 && Math.abs(wAttr - (12 + 2.4)) < 1e-3 && /url\(#cpG\d+\)/.test(pic.svg) && grey && big.scale < 1 && Math.abs(big.scale - 58 / 92.4) < 1e-6, `width ${wAttr} mm, a 90 mm design at ${Math.round(big.scale * 100)}%`);
  const pg = (n, o) => Object.assign({ page: n, metal: 'silver', charms: [{ order: '1' }], runId: 'r1' }, o);
  const ss = [pg(1, { laserDoneAt: 5 }), pg(2, { charms: [{ order: '4200000001' }] }), pg(3)], rg = [pg(1, { metal: 'rose' }), pg(2, { metal: 'rose' })];
  const picks = [T.pickSheet(ss, { metal: 'silver', orderId: '4200000001' }), T.pickSheet(ss, { metal: 'silver' }), T.pickSheet(ss, { metal: 'silver', room: p => p.page !== 2 }), T.pickSheet(rg, { metal: 'rose' }), T.pickSheet([pg(1, { recalled: true })], { metal: 'silver' }), T.pickSheet([pg(1, { charms: [] })], { metal: 'silver' })].map(p => (p ? p.page : null));
  check('A3 the sheet it goes onto: the order\'s own open sheet; else the earliest open SS sheet with room (a cut one passed over); the newest open RG one; none open or none with charms: a new sheet',
    JSON.stringify(picks) === JSON.stringify([2, 2, 3, 2, null, null]), JSON.stringify(picks));
  const t = Date.UTC(2026, 9, 27, 15);
  const evs = [{ type: 'decided', by: 'Ana M', at: t }, { type: 'engraveApproved', by: 'Bea', at: t + 6e4, lineKey: 'L1' }, { type: 'placed', by: 'Charm Sorter', at: t + 9e4 }, { type: 'engraveApproved', by: 'Marco R.', at: t + 7e4, lineKey: 'L2' },
    { type: 'laserDone', by: '123456', at: t + 1e5 }, { type: 'designSent', by: 'Paul K', at: t + 2e5, data: { fix: true } }];
  const c = T.culprits(evs), c1 = T.culprits(evs, { lineKey: 'L1' });
  check('A4 who made the mistake, from the order\'s history: the person who approved the engraving first (the latest of them), then who approved the order; no machine, no PIN, no earlier fix',
    c.suggested === 'Marco R.' && c1.suggested === 'Bea' && c.candidates.map(x => x.name).join() === 'Marco R.,Bea,Ana M' && /approved the engraving/.test(c.why), `${c.why} · piece L1: ${c1.why}`);
  check('A5 an orderless piece\'s number is read off its pool id (D4: CUSTOM-<yyyymmdd>-<n>)', T.customIdOf('CUSTOM-20261028-3_1') === 'CUSTOM-20261028-3' && T.customIdOf('4174476673_1') === '', '');
  delete global.CharmEditor;
}

/* ═══ B · the page ═══ */
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 30, 17) / 1000);
const order = (rid, buyer, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 3 * DAY, updateTs: SHIP - 3 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
const CUSTOM = '4174476673', FIXED = '4178000001';
const ORDERS = [
  order(CUSTOM, 'Emma Lindqvist', [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])]),
  order(FIXED, 'Noor Haddad', [line(41780000011, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Length', '18 Inches']], { metalKey: 'gold', metalLabel: 'GF 14/20' })])
];
/** The pop-up closed after Send; when it does not, what it says (its why line) is the evidence. */
const gone = page => page.waitForFunction(() => !document.querySelector('dialog.cpDlg'), null, { timeout: 30000 }).catch(async e => { console.log('  (pop-up still open)', await page.evaluate(() => (document.querySelector('dialog.cpDlg [data-cp-why]') || {}).textContent)); throw e; });
const settle = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));

async function partB() {
  const pwDir = process.env.PW_DIR || path.join(ROOT, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    // the history of the order a fix fixes: the order approved by Marco R., its engraving approved by Bea, placed by the run
    const t0 = Date.now() - 86400000, post = body => fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(r => r.json());
    const seeded = await post({ op: 'timelineAdd', events: [{ orderId: FIXED, type: 'decided', by: 'Marco R.', at: t0, id: 'd1', text: 'Approved' }, { orderId: FIXED, type: 'engraveApproved', by: 'Bea', at: t0 + 60000, id: 'e1', text: 'Engraving approved' }, { orderId: FIXED, type: 'placed', by: '', at: t0 + 90000, id: 'p1', text: 'On sheet' }] });
    assert(!seeded.error, 'the history is seeded: ' + JSON.stringify(seeded));
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort()));
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('console', m => { if (['warning', 'error'].includes(m.type()) && process.env.SS_DEBUG) console.log('  [page ' + m.type() + ']', m.text().slice(0, 300)); });
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.CharmNestCustomPiece && window.CharmEditor && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const stand = await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems();
      // stand-ins for the design library and the pool's custom side (the designs and custompool workstreams), and what was logged
      const real = { designs: !!window.CharmNestDesigns, pool: typeof Pool.addCustomPiece === 'function', log: !!window.CNDesignLog };
      window.__puts = []; window.__pool = []; window.__logs = [];
      // (each put recorded with what the library answered: its id, version and hash)
      if (real.designs) { const put0 = CharmNestDesigns.put.bind(CharmNestDesigns); CharmNestDesigns.put = (d, o) => { const rec = JSON.parse(JSON.stringify(d)); __puts.push(rec); return put0(d, o).then(r => { rec.id = r.id; rec.version = r.version; rec.hash = r.hash; return r; }); }; }
      else {
        const store = new Map(), subs = []; let n = 0;
        window.CharmNestDesigns = {
          async put(d) { const id = d.id || 'DSG' + (++n), v = ((store.get(id) || {}).version || 0) + 1, rec = Object.assign({}, d, { id, version: v, hash: 'h-' + id + '-' + v, at: Date.now() }); __puts.push(JSON.parse(JSON.stringify(rec))); store.set(id, rec); subs.forEach(f => f()); return { id, version: v, hash: rec.hash, at: rec.at }; },
          async get(id) { const r = store.get(id); return r ? { design: JSON.parse(JSON.stringify(r)) } : null; },
          search: q => [...store.values()].filter(r => String(r.orderId || '').includes(q) || String(r.name).toLowerCase().includes(String(q).toLowerCase())),
          list: async () => ({ designs: [...store.values()] }), onChange: f => subs.push(f) };
      }
      const orig = typeof Pool.addCustomPiece === 'function' ? Pool.addCustomPiece : null;
      window.__poolOut = [];
      Pool.addCustomPiece = async o => { __pool.push(JSON.parse(JSON.stringify(o))); if (orig) { const ids = await orig(o); __poolOut.push(ids); return ids; } const d = new Date(), ymd = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`; const ids = Array.from({ length: o.quantity }, (_, i) => (o.orderId ? `${o.orderId}_cp${i + 1}` : `CUSTOM-${ymd}-1_${i + 1}`)); __poolOut.push(ids); return ids; };
      if (window.CNDesignLog) CNDesignLog.onEvent(e => __logs.push(e)); else window.CNDesignLog = { event: (k, d) => __logs.push(Object.assign({ kind: k }, JSON.parse(JSON.stringify(d)))) };
      CN.setMode('review'); Review.render();
      return real;
    }, ORDERS);
    console.log(`  (stand-ins: design library ${stand.designs ? 'REAL' : 'stand-in'}, pool's custom side ${stand.pool ? 'REAL (recorded)' : 'stand-in'}, design log ${stand.log ? 'REAL' : 'stand-in'})`);

    /* ── B1 · a custom order in the Charm Designer ── */
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList > .reviewListRow[data-rid="${CUSTOM}"]`;
    await page.waitForSelector(`${card} [data-cp-design]`);
    const btn = await page.evaluate(sel => { const b = document.querySelector(sel); const r = b.getBoundingClientRect(); return { text: b.textContent.trim(), w: r.width, h: r.height, inCol: !!b.closest('.rowActions') }; }, `${card} [data-cp-design]`);
    const regular = await page.evaluate(rid => !!document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"] [data-cp-design]`), FIXED);
    check('B1 the Custom Orders card has "Open in Charm Designer" in its column (a regular order\'s card has none)', btn.text === 'Open in Charm Designer' && btn.inCol && btn.h >= 30 && !regular, `${Math.round(btn.w)}×${Math.round(btn.h)} px`);
    await page.evaluate(() => { window.__clicked = 0; document.addEventListener('click', e => { if (e.target.closest('[data-cp-design]')) __clicked++; }, true); });
    const tClick = Date.now();
    await page.click(`${card} [data-cp-design]`);
    await page.waitForFunction(() => window.CharmEditor.current && document.querySelector('.ce-root')).catch(async e => { console.log('  (diagnosis)', JSON.stringify(await page.evaluate(async sel => ({ btn: (document.querySelector(sel) || {}).outerHTML, toasts: [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent), bar: !!document.querySelector('.cnNameBar'), who: B.employee, clicked: window.__clicked, cur: !!CharmEditor.current, try: await Promise.race([CharmNestCustomPiece.openDesigner(Review.customItemFor('4174476673_41744766731')).then(v => 'done ' + JSON.stringify(v), e => 'err ' + e.message), new Promise(r => setTimeout(() => r('pending; cur=' + !!CharmEditor.current), 3000))]) }), `${card} [data-cp-design]`))); throw e; });
    const opened = await page.evaluate(() => ({ title: document.querySelector('.ce-root .ce-id h2').textContent, sub: (document.querySelector('.ce-root .ce-eyebrow') || {}).textContent || '', items: CharmEditor.current.items().length, frame: CharmEditor.current.frame() }));
    check('B2 the Charm Designer opens on that order, blank (20 × 20 mm), titled with the order', opened.items === 0 && /4174476673/.test(opened.title + opened.sub) && Math.abs(opened.frame.wPt * MMPT - 20) < 1e-6, `"${opened.title}" · ${opened.sub} · ${Date.now() - tClick} ms`);
    // a rectangle drawn with the shapes tool, then Save
    await page.click('.ce-root .ce-tool[data-tool="rect"]');
    const a = await page.evaluate(() => CharmEditor.current.clientOf(8, 12)), b = await page.evaluate(() => CharmEditor.current.clientOf(44, 38));
    await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(b.x, b.y, { steps: 8 }); await page.mouse.up(); await settle(page);
    await page.evaluate(() => document.querySelector('.ce-root .ce-item[data-role="cut"]').click()); await settle(page);   // (the role menu: it is the piece's cut line)
    const drawn = await page.evaluate(() => { const it = CharmEditor.current.items()[0]; return it && it.role === 'cut' ? CharmEditor.current.box(it.id) : null; });
    assert(drawn, 'a rectangle was drawn and made the cut line');
    const tSave = Date.now();
    await page.click('.ce-root [data-act="save"]');
    await page.waitForSelector('dialog.cpDlg[open]');
    const saveMs = Date.now() - tSave;
    await page.waitForFunction(() => document.querySelector('dialog.cpDlg [data-cp-plate] svg'));
    const m1 = await page.evaluate(() => {
      const d = document.querySelector('dialog.cpDlg'), svg = d.querySelector('[data-cp-plate] svg'), on = d.querySelector('[data-cp-attach].on');
      return { attach: on && on.dataset.cpAttach, attachText: on && on.textContent, w: svg.getAttribute('width'), h: svg.getAttribute('height'), dim: d.querySelector('.cpDim').textContent, ver: d.querySelector('.cpV').textContent,
        tag: d.querySelector('.cpTag').textContent, rel: !!(d.querySelector('[data-cp-release]') || {}).checked, why: d.querySelector('[data-cp-why]').textContent, dis: d.querySelector('[data-cp-send]').getAttribute('aria-disabled'), openMs: CharmNestCustomPiece.current().openMs, puts: __puts };
    });
    const put = m1.puts[0] || {}, wMm = (drawn[2] - drawn[0]) * MMPT, hMm = (drawn[3] - drawn[1]) * MMPT;
    check('B3 Save keeps the design as a version attached to the order (its exact size with it) and the pop-up opens prefilled: this custom order (to be released from Review once sent), the design and its version',
      m1.puts.length === 1 && put.orderId === CUSTOM && Math.abs(put.widthMm - wMm) < 0.001 && Math.abs(put.heightMm - hMm) < 0.001 && put.faces && put.faces.front && put.by === 'Test Operator' && m1.attach === 'this' && m1.attachText.includes(CUSTOM) && m1.attachText.includes('Emma Lindqvist') && m1.ver === 'v1' && /handmade custom/i.test(m1.tag) && m1.rel,
      `${put.widthMm && put.widthMm.toFixed(2)} × ${put.heightMm && put.heightMm.toFixed(2)} mm · pop-up ${saveMs} ms after Save (its own work ${m1.openMs.toFixed(1)} ms)`);
    check('B4 the design is drawn at its exact size: the picture\'s width and height are its mm (plus the 1.2 mm margin) at 100%, and the size is said in mm',
      Math.abs(parseFloat(m1.w) - (wMm + 2.4)) < 0.01 && Math.abs(parseFloat(m1.h) - (hMm + 2.4)) < 0.01 && m1.dim.replace(/\s/g, '') === `${wMm.toFixed(2)}×${hMm.toFixed(2)}mm`, `${m1.w} × ${m1.h} · "${m1.dim}"`);
    check('B5 nothing is sent before the metal is picked: Send says why', m1.dis === 'true' && /metal/i.test(m1.why), m1.why);
    const pickMs = await page.evaluate(() => { const t = performance.now(); document.querySelector('dialog.cpDlg [data-cp-metal="silver"]').click(); return performance.now() - t; });
    const goes = await page.evaluate(() => ({ goes: document.querySelector('dialog.cpDlg [data-cp-goes]').textContent, send: document.querySelector('dialog.cpDlg [data-cp-send]').textContent, dis: document.querySelector('dialog.cpDlg [data-cp-send]').getAttribute('aria-disabled'), fill: (document.querySelector('dialog.cpDlg [data-cp-plate] stop[data-t="1"]') || { getAttribute: () => '' }).getAttribute('stop-color') === '#8d95a0' }));
    check('B6 SS picked: the sheet it goes onto is named ("a new SS sheet" while no SS sheet is open), the piece is drawn in silver, Send names it too; the press paints in one frame',
      /Goes onto a new SS sheet/.test(goes.goes) && goes.send === 'Send to a new SS sheet' && goes.dis === 'false' && goes.fill && pickMs < 16, `"${goes.goes}" · ${pickMs.toFixed(1)} ms`);
    await page.click('dialog.cpDlg [data-cp-send]');
    await gone(page);
    await page.waitForFunction(() => __poolOut.length >= 1);
    const s1 = await page.evaluate(() => ({ poolIds: __poolOut[0], pool: __pool, logs: __logs.map(e => ({ kind: e.kind, orderId: e.orderId || null, customId: e.customId || null, designId: e.designId, version: e.version, metal: e.metal, quantity: e.quantity, by: e.by, fix: e.fix || e.fixes || null, customName: e.customName || null })) }));
    const c1 = s1.pool[0] || {}, sent1 = s1.logs.filter(e => e.kind === 'customPieceSent');
    check('B7 Send: the pool is asked for ONE handmade custom piece of that design version on SS, for that order, under the person signed in',
      s1.pool.length === 1 && c1.metal === 'silver' && c1.orderId === CUSTOM && c1.quantity === 1 && c1.handmade === true && c1.designId === put.id && c1.version === 1 && c1.hash === put.hash && c1.design && c1.design.faces && c1.by === 'Test Operator' && !c1.fixes,
      JSON.stringify({ metal: c1.metal, orderId: c1.orderId, designId: c1.designId, version: c1.version, hash: c1.hash }));
    check('B8 one customPieceSent event for the send (the design log writes the order\'s timeline from it), and the designer\'s open and save logged under the person',
      sent1.length === 1 && sent1[0].orderId === CUSTOM && sent1[0].metal === 'silver' && s1.logs.some(e => e.kind === 'editorOpen' && e.orderId === CUSTOM) && s1.logs.some(e => e.kind === 'editorSave' && e.designId === put.id), s1.logs.map(e => e.kind).join(', '));
    // the piece is on an SS sheet, the custom order released from Review (its line done, by the person), the next pop-up names the sheet
    await page.waitForFunction(rid => { const r = B.orders.rows.find(x => String(x.order.receiptId) === rid); return r && r.spec && r.spec.customDone; }, CUSTOM, { timeout: 15000 });
    const placed = await page.evaluate(([rid, ids]) => {
      const r = B.orders.rows.find(x => String(x.order.receiptId) === rid), on = CN.S.sheets.silver.pages.find(p => p.charms.some(c => ids.includes(c.poolId)));
      return { done: r.spec.customDone.completedBy || r.spec.customDone.by || '', sheet: on ? CharmNestSheetName.name(on) : '', goes: CharmNestCustomPiece.goesOnto('silver').name, frozen: !!(on && on.charms.find(c => ids.includes(c.poolId)).frozen),
        open: !!document.querySelector(`#rvList > .reviewListRow[data-rid="${rid}"] [data-cp-design]`) };
    }, [CUSTOM, s1.poolIds || []]);
    check('B9 the piece is on an SS sheet at its frozen size, the next pop-up names that sheet; the custom order is released from Review (its line done, by the person who sent it)',
      !!placed.sheet && placed.goes === placed.sheet && placed.frozen && placed.done === 'Test Operator' && !placed.open, `on ${placed.sheet} · released by ${placed.done}`);
    await page.evaluate(rid => OrderWin.openOrder(rid), CUSTOM);
    await page.waitForFunction(() => { const b = document.getElementById('owCpDesign'); return b && !b.hidden; });
    const ow = await page.evaluate(() => { const b = document.getElementById('owCpDesign'); return { text: b.textContent.trim(), title: b.title }; });
    check('B9b the order window shows the design attached to the order (name, version) and opens it in the Charm Designer', ow.text === 'Edit design' && /v1 is attached to this order/.test(ow.title), ow.title);
    await page.evaluate(() => OrderWin.close()); await page.waitForFunction(() => !OrderWin.isOpen());

    /* ── B10 · an orderless piece that fixes a mistake ── */
    await page.evaluate(([id]) => { window.__p2 = CharmNestCustomPiece.open({ designId: id, version: 1 }); }, [put.id]);
    await page.waitForSelector('dialog.cpDlg[open] [data-cp-plate] svg');
    const m2 = await page.evaluate(() => ({ attach: (document.querySelector('dialog.cpDlg [data-cp-attach].on') || {}).dataset.cpAttach, hasThis: !!document.querySelector('dialog.cpDlg [data-cp-attach="this"]') }));
    await page.click('dialog.cpDlg [data-cp-name]'); await page.keyboard.type('Replacement flower for Noor');
    await page.click('dialog.cpDlg [data-cp-reason="fix"]');
    await page.waitForFunction(() => document.activeElement && document.activeElement.matches('[data-cp-fq]'));
    const tq = Date.now(); await page.keyboard.type(FIXED.slice(0, 7));
    await page.waitForFunction(rid => [...document.querySelectorAll('dialog.cpDlg [data-cp-fhits] [data-cp-hit]')].some(h => h.textContent.includes(rid)), FIXED);
    const typeMs = Date.now() - tq;
    const hit = await page.evaluate(() => document.querySelector('dialog.cpDlg [data-cp-fhits] [data-cp-hit].on').textContent);
    await page.keyboard.press('Enter');
    await page.waitForFunction(() => { const i = document.querySelector('dialog.cpDlg [data-cp-causer]'); return i && i.value; }, null, { timeout: 15000 });
    const cause = await page.evaluate(() => { const d = document.querySelector('dialog.cpDlg'); return { value: d.querySelector('[data-cp-causer]').value, why: (d.querySelector('.cpWhyL') || {}).textContent || '', cands: [...d.querySelectorAll('[data-cp-causer-pick]')].map(b => b.dataset.cpCauserPick), picked: d.querySelector('[data-cp-fixed]').textContent }; });
    check('B10 no order: named, "Fix a mistake", the order it fixes found by its number (buyer shown); who made the mistake is read from that order\'s history and suggested (Bea approved its engraving), the others named there offered',
      m2.attach === 'none' && !m2.hasThis && hit.includes('Noor Haddad') && cause.picked.includes(FIXED) && cause.value === 'Bea' && /approved the engraving/.test(cause.why) && cause.cands.includes('Marco R.'), `${cause.why} · offered ${cause.cands.join(', ')} · search ${typeMs} ms`);
    await page.click('dialog.cpDlg [data-cp-causer-pick="Marco R."]');
    await page.click('dialog.cpDlg [data-cp-metal="gold"]');
    await page.click('dialog.cpDlg [data-cp-qty="1"]');
    const f2 = await page.evaluate(() => ({ causer: document.querySelector('dialog.cpDlg [data-cp-causer]').value, send: document.querySelector('dialog.cpDlg [data-cp-send]').textContent, n: document.querySelector('dialog.cpDlg [data-cp-n]').textContent }));
    await page.click('dialog.cpDlg [data-cp-send]');
    await gone(page);
    const out2 = await page.evaluate(() => window.__p2);
    const s2 = await page.evaluate(() => ({ pool: __pool[1], log: __logs.filter(e => e.kind === 'customPieceSent').slice(-1)[0] }));
    const p2 = s2.pool || {}, l2 = s2.log || {}, fx = l2.fix || l2.fixes || {};
    check('B11 the person changed who it was to Marco R.; Send: the pool gets an orderless named piece × 2 on GF, the reason, and the fix naming both people (the suggestion kept beside the choice)',
      f2.causer === 'Marco R.' && f2.n === '2' && /GF/.test(f2.send) && p2.orderId === null && p2.customName === 'Replacement flower for Noor' && p2.quantity === 2 && p2.metal === 'gold' && p2.reason === 'fix' && p2.handmade === true
      && p2.fixes && p2.fixes.orderId === FIXED && p2.fixes.causedBy === 'Marco R.' && p2.fixes.suggested === 'Bea' && p2.by === 'Test Operator' && /^CUSTOM-\d{8}-\d+$/.test(out2.orderId || ''),
      `"${f2.send}" → ${out2.orderId} ${JSON.stringify(out2.poolIds)}`);
    check('B12 its customPieceSent event carries the piece\'s own number and name, who fixed it and whose mistake it was', l2.customId === out2.orderId && l2.customName === 'Replacement flower for Noor' && fx.causedBy === 'Marco R.' && fx.suggested === 'Bea' && fx.orderId === FIXED && l2.by === 'Test Operator', JSON.stringify(fx));
    // the fixed order's timeline says it (charm-nest-design-log.js, through the order timeline's outbox)
    let tl = null;
    if (stand.log) {
      await page.evaluate(() => CNDesignLog.flush());
      for (let i = 0; i < 40 && !tl; i++) { const j = await post({ op: 'timelineGet', orderId: FIXED }); tl = (j.events || []).find(e => e.type === 'designSent' && /mistake by Marco R./.test(e.text || '')) || null; if (!tl) await new Promise(r => setTimeout(r, 250)); }
      check('B13 the order it fixes has it on its timeline: who sent the fix and whose mistake it was', !!tl && tl.by === 'Test Operator', tl ? tl.text : 'no event');
    }

    /* ── B14 · "Send to sheet…" from inside the Charm Editor: the pop-up over the editor, the keys its own ── */
    await page.evaluate(() => { const PT = 72 / 25.4; window.__ce = CharmEditor.open({ title: 'HEART_1203', faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 15 * PT, hPt: 15 * PT }, items: [{ id: 'h1', kind: 'path', role: 'cut', subpaths: CharmEditor.model.ellipseSP(7.5 * PT, 7.5 * PT, 6 * PT, 6 * PT) }] } }, lockRoles: [] }); });
    await page.waitForFunction(() => CharmEditor.current);
    const at = await page.evaluate(() => CharmEditor.current.clientOf(7.5 * 72 / 25.4, 1.5 * 72 / 25.4));
    await page.mouse.click(at.x, at.y); await settle(page);
    const sel0 = await page.evaluate(() => ({ sel: CharmEditor.current.selection(), tool: CharmEditor.current.tool(), cmd: !!(CharmEditor.commands && CharmEditor.commands.sendToSheet) }));
    await page.evaluate(() => { window.__p3 = CharmEditor.commands.sendToSheet.run({}); });
    await page.waitForSelector('dialog.cpDlg[open] [data-cp-name]');
    await page.waitForFunction(() => document.activeElement && document.activeElement.matches('dialog.cpDlg [data-cp-name]'));
    await page.keyboard.type('vr test'); await page.keyboard.press('Backspace'); await page.keyboard.press('Delete');
    const k = await page.evaluate(() => ({ name: document.querySelector('dialog.cpDlg [data-cp-name]').value, tool: CharmEditor.current.tool(), n: CharmEditor.current.items().length, sel: CharmEditor.current.selection(), top: document.querySelector('dialog.cpDlg').matches(':modal'), puts: __puts.length, last: __puts[__puts.length - 1] }));
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.querySelector('dialog.cpDlg'));
    const after = await page.evaluate(async () => ({ editor: !!CharmEditor.current, p3: await window.__p3 }));
    check('B14 "Send to sheet…" in the Charm Editor keeps the drawing as a version and opens the pop-up over the editor; letters, Backspace and Delete typed in it stay in it (the editor\'s tool and its selected part untouched); Esc puts the pop-up away, the editor stays',
      sel0.cmd && sel0.sel.length === 1 && k.top && k.name === 'vr tes' && k.tool === sel0.tool && k.n === 1 && k.sel.length === 1 && k.last && k.last.name === 'HEART_1203' && !k.last.orderId && after.editor && after.p3 === null,
      `typed "${k.name}", tool ${k.tool}, ${k.n} part, ${Math.abs(k.last.widthMm - 12).toFixed(4)} mm off a 12 mm heart`);
    await page.evaluate(() => document.querySelector('.ce-root [data-act="cancel"]').click());
    await page.waitForFunction(() => !CharmEditor.current);
    check('B15 no page errors', errors.length === 0, errors.slice(0, 3).join(' | '));
  } finally { await browser.close(); await srv.close(); }
}

(async () => {
  if (!ONLY || ONLY === 'A') partA();
  if (!ONLY || ONLY === 'B') await partB();
  const bad = RESULTS.filter(r => !r.ok);
  console.log(`\nce-sendsheet: ${RESULTS.length - bad.length}/${RESULTS.length} passed`);
  if (bad.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
