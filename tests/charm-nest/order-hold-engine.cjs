// Hold, the whole order (charm-nest-order-hold.js, Paul 5 Oct 2026): the engine on the real sorter page over the local fake backend
// (bridge-server.cjs: the real charmNestLibrary handler on an in-memory Firestore). Nothing leaves the machine.
//   node tests/charm-nest/order-hold-engine.cjs [playwright-core dir]
// Checks: the plan from fixtures (pure) and from the live page; a 4-piece order across GF Sheet 2 and SS Sheet 1 comes off both and
// the freed spots are filled from the orders waiting (oldest first) and, when none waits, from a newer incomplete sheet of the same
// metal; nothing fits: the spots stay free and the hold stands; a cut sheet keeps its piece (the others come off); a committed set
// blocks; a Rose Gold sheet gives its pieces up and is not filled; a run a reload cut short carries on; no order is ever lost;
// the steps arrive in the order of the contract, one change at a time.
const fs = require('fs'), path = require('path'), assert = require('assert'), vm = require('vm');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const ok = [];

/* ── 1 · the plan from fixtures: pure, no page ── */
function pureTests() {
  const src = fs.readFileSync(path.join(root, 'charm-nest-order-hold.js'), 'utf8');
  const win = {}; vm.runInNewContext(src, { window: win, console, setTimeout, Date, localStorage: undefined });
  const J = x => JSON.parse(JSON.stringify(x)), OH = { plan: win.OrderHold.plan, run: win.OrderHold.run, status: win.OrderHold.status, planFrom: snap => J(win.OrderHold.planFrom(snap)) };   // (plain copies: the page code ran in another realm)
  assert(OH && typeof OH.plan === 'function' && typeof OH.run === 'function' && typeof OH.status === 'function', 'the three calls of the contract exist');
  const piece = (id, status, sheet, extra) => Object.assign({ poolId: id, lineKey: 'k:' + id, label: 'CHARM · ' + id, metal: sheet.startsWith('GF') ? 'gold' : 'silver', status, why: '', sheetId: sheet.replace(/ /g, '-').toLowerCase(), sheetLabel: sheet, setId: null, setLabel: '', placed: true }, extra || {});
  const base = pieces => ({ rid: '4170000001', label: '4170000001', customer: 'Jo Buyer', shipBy: 1790500000, cancelled: false, held: false, rowsLeft: 2, pieces,
    sheets: { 'gf-sheet-2': { label: 'GF Sheet 2', setLabel: 'Set 1', metal: 'gold', removes: 2, spots: 2, fillable: true }, 'ss-sheet-1': { label: 'SS Sheet 1', setLabel: '', metal: 'silver', removes: 2, spots: 2, fillable: true } } });
  const four = [piece('a1', 'off', 'GF Sheet 2', { setLabel: 'Set 1' }), piece('a2', 'off', 'GF Sheet 2', { setLabel: 'Set 1' }), piece('b1', 'off', 'SS Sheet 1'), piece('b2', 'off', 'SS Sheet 1')];
  let P = OH.planFrom(Object.assign(base(four), { candidates: [{ rid: '901', source: 'waiting', metal: 'gold', spots: 1 }, { rid: '902', source: 'waiting', metal: 'gold', spots: 1 }, { rid: '903', source: 'newerSheet', metal: 'silver', fromSheetId: 'ss-sheet-2', fromSheetLabel: 'SS Sheet 2', spots: 2 }] }));
  assert(P.canHold && P.blockedWhy === null, 'a plain 4-piece order can be held');
  assert.deepStrictEqual(Object.keys(P).sort(), ['blockedWhy', 'canHold', 'customer', 'effects', 'estimate', 'fills', 'label', 'pieces', 'rid', 'shipBy', 'sheets', 'stays'].sort(), 'the Plan has the fields of the contract');
  assert.deepStrictEqual(P.pieces.map(p => p.state), ['onSheet', 'onSheet', 'onSheet', 'onSheet'], 'four pieces on sheets');
  assert.deepStrictEqual(P.sheets.map(s => [s.label, s.removes, s.qrRemade]), [['GF Sheet 2', 2, true], ['SS Sheet 1', 2, true]], 'two sheets, two pieces each, a new QR label on both');
  assert(P.shipBy === 1790500000000, 'ship-by in ms');
  const fg = P.fills.filter(f => f.sheetId === 'gf-sheet-2'), fs1 = P.fills.filter(f => f.sheetId === 'ss-sheet-1');
  assert(fg.length === 1 && fg[0].source === 'waiting' && fg[0].orders === 2 && fg[0].spots === 2, 'GF Sheet 2: two waiting orders fill its two spots: ' + JSON.stringify(fg));
  assert(fs1.length === 1 && fs1[0].source === 'newerSheet' && fs1[0].fromSheetLabel === 'SS Sheet 2' && fs1[0].orders === 1, 'SS Sheet 1: one order from the newer SS Sheet 2: ' + JSON.stringify(fs1));
  assert(P.effects.some(t => /4 pieces of this order come off GF Sheet 2 \(Set 1\) and SS Sheet 1/.test(t)), 'the consent text names the pieces and sheets: ' + P.effects.join(' | '));
  assert(P.effects.some(t => /2 waiting orders and 1 order from SS Sheet 2 fill the 4 empty spots/i.test(t)), 'it says what moves in: ' + P.effects.join(' | '));
  assert(P.effects.some(t => /QR labels are made on 2 sheets/.test(t)) && P.effects.some(t => /Release hold/.test(t)) && P.effects.some(t => /Nothing is deleted/.test(t)), 'QR, release and nothing deleted are said');
  assert(!P.effects.some(t => /\blines?\b/i.test(t)), 'never "lines" in a sentence a person reads');
  // nothing fits
  P = OH.planFrom(base(four));
  assert(P.canHold && P.fills.every(f => f.source === 'none') && P.fills.reduce((n, f) => n + f.spots, 0) === 4, 'no candidate: every spot stays free');
  assert(P.effects.some(t => /4 empty spots stay free/.test(t)), 'and it says so: ' + P.effects.join(' | '));
  // a cut sheet keeps its piece
  const cut = four.slice(); cut[0] = piece('a1', 'cut', 'GF Sheet 1', { why: 'that sheet was already cut' });
  P = OH.planFrom(base(cut));
  assert(P.canHold && P.stays.length === 1 && P.stays[0].poolId === 'a1' && P.pieces[0].state === 'onCutSheet' && /already cut/.test(P.stays[0].why), 'a piece on a cut sheet stays and is listed');
  assert(P.effects.some(t => /1 piece stays: GF Sheet 1 \(that sheet was already cut\)/.test(t)), 'the consent text says it stays: ' + P.effects.join(' | '));
  // a committed set blocks
  const sent = four.slice(); sent[1] = piece('a2', 'sent', 'GF Sheet 2', { why: 'its set was already sent to the station' });
  P = OH.planFrom(base(sent));
  assert(!P.canHold && /^Undo the set first/.test(P.blockedWhy) && P.pieces[1].state === 'inCommittedSet' && !P.effects.length, 'a committed set blocks: ' + P.blockedWhy);
  // already held / cancelled / everything cut / alone on a sheet / unloaded
  assert(!OH.planFrom(Object.assign(base(four), { held: true })).canHold, 'an order already on hold cannot be held again');
  assert(/cancelled/.test(OH.planFrom(Object.assign(base(four), { cancelled: true })).blockedWhy), 'a cancelled order cannot be held');
  assert(/already cut/.test(OH.planFrom(base([piece('a1', 'cut', 'GF Sheet 1')])).blockedWhy), 'nothing to take off: every piece is cut');
  assert(/only one on GF Sheet 2/.test(OH.planFrom(base([piece('a1', 'last', 'GF Sheet 2')])).blockedWhy), 'alone on a sheet: blocked, plainly');
  assert(/not open in this sorter/.test(OH.planFrom(base([piece('a1', 'unloaded', 'GF Sheet 4')])).blockedWhy), 'a sheet this page has not loaded blocks');
  // Rose Gold: its pieces come off, no fill
  const rose = base([piece('r1', 'off', 'RG Sheet 1', { metal: 'rose' })]); rose.sheets = { 'rg-sheet-1': { label: 'RG Sheet 1', setLabel: '', metal: 'rose', rose: true, removes: 1, spots: 1, fillable: false } };
  P = OH.planFrom(rose);
  assert(P.canHold && P.fills.length === 1 && P.fills[0].source === 'none' && /Rose Gold/.test(P.fills[0].why) && P.effects.some(t => /Rose Gold sheets are never re-arranged/.test(t)), 'Rose Gold: off the sheet, never filled: ' + P.effects.join(' | '));
  // a piece not on a sheet yet just waits
  P = OH.planFrom(Object.assign(base([piece('z1', 'none', '')]), { sheets: {} }));
  assert(P.canHold && P.pieces[0].state === 'notOnSheet' && P.sheets.length === 0 && P.effects.some(t => /not on a sheet yet/.test(t)), 'a piece not on a sheet just waits');
  ok.push('plan from fixtures: the contract shape, 4 pieces on 2 sheets with fills, nothing fits, cut stays, committed blocks, Rose Gold never filled, already held / cancelled / alone on a sheet blocked, plain sentences');
}

/* ── 2 · the live page ── */
const RUN = 'run-test-1', BASE_TS = 1790000000;
// one fixture: sheets (items placed or waiting), the orders' lines, flags
const fixture = spec => {
  const docs = { sheets: [], pool: [] };
  for (const sh of spec.sheets) {
    const charms = [], placements = [], poolIds = [];
    sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
      const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
      charms.push({ id, poolId, order: rid }); poolIds.push(poolId);
      if (placed !== false) placements.push({ id, cxPt: 30 + col * 40, cyPt: 30 + row * 40, angle: 0 });
      docs.pool.push({ poolId, orderId: rid, sheetId: sh.id, state: 'placed', material: sh.metal });
    });
    docs.sheets.push({ id: sh.id, metal: sh.metal, charms, placements, poolIds, sheetIndex: sh.page, fileBase: sh.id, runId: RUN });
  }
  return docs;
};

(async () => {
  pureTests();
  console.log('order-hold-engine (pure part): all passed\n  ' + ok.join('\n  '));
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  // nothing leaves the machine
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester');
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      // a stand-in painter for the test's round charms
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
      P.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
      P.cutLinesOf = () => [];
      // the nest of this test: what is pinned is placed where it was pinned, the rest waits; the sheet is saved as the real nest saves it
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, sheetId: sh.sheetId, charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        if (localStorage.getItem('__nestMode') === 'hang') return;
        setTimeout(async () => {
          try {
            for (const c of sh.charms) if (c.pinned && !sh.placements.some(p => p.id === c.id)) sh.placements.push({ id: c.id, cxPt: c.pinned.cxPt, cyPt: c.pinned.cyPt, angle: c.pinned.angle || 0, wPt: c.widthPt, hPt: c.heightPt });
            sh.placements = sh.placements.filter(p => sh.charms.some(c => c.id === p.id));
            await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId) } }, { quiet: true });
          } catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh.intakeAppend = false; sh.appendOnly = false; sh.stage = ''; sh.persistedDone = true; sh.density = Math.min(.9, sh.placements.length * .08);
          try { CN.renderCard(sh); } catch (_) {}
        }, 120);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };

  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && Orders.takeOffGone && window.SheetWin && SheetWin.holdKit && window.OrderHold && window.CharmNestSolver && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
    ok.push('the page loads OrderHold and the sheet window kit');

    // the fixture goes into the page and the fake backend
    async function setup(spec) {
      for (const k of [SHEETS, POOL, TL]) for (const [key] of [...st.docs]) if (key.startsWith(k + '/')) st.docs.delete(key);
      const docs = fixture(spec);
      for (const d of docs.sheets) st.put(SHEETS, d.id, d);
      for (const d of docs.pool) st.put(POOL, d.poolId, d);
      await page.evaluate(({ spec, RUN, BASE_TS }) => {
        localStorage.removeItem('__nestMode'); localStorage.removeItem('cn.orderhold.run'); localStorage.removeItem('cn.sheetwin.freed'); window.__nests = [];
        const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
        const mkBits = () => { const b = new Uint8Array(D * D); for (let y = 0; y < D; y++) for (let x = 0; x < D; x++) if (Math.hypot(x + .5 - D / 2, y + .5 - D / 2) <= D / 2 - 1) b[y * D + x] = 1; return b; };
        for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
        window.B.pool.rows.clear();
        for (const sh of spec.sheets) {
          const pr = CN.S.sheets[sh.metal]; let page = null;
          if (sh.page === 1) page = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); page = pr.pages[sh.page - 1]; }
          page.charms = []; page.placements = [];
          sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
            const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
            page.charms.push({ id, name: `${rid} · TEST-${tx}`, poolId, order: rid, lineKey: `${rid}:${5000000000 + tx}`, sku: 'TEST-' + tx, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: D, h: D, scale: 1, bits: mkBits(), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, orderDate: BASE_TS * 1000 - (+rid.slice(-3)) * 60000, orderInfo: { receiptId: rid } });
            if (placed !== false) page.placements.push({ id, cxPt: 30 + col * 40, cyPt: 30 + row * 40, angle: 0, wPt: 2 * R * MM, hPt: 2 * R * MM });
            window.B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.id, state: 'placed', material: sh.metal });
          });
          Object.assign(page, { status: 'complete', sheetId: sh.id, fileBase: sh.id, sheetIndex: sh.page, page: sh.page, runId: RUN, dirty: false, persistedDone: true, persisted: Promise.resolve(), problem: null, density: .3, verification: { ok: true }, intakeAppend: false, appendOnly: false });
          for (const k of ['laserDoneAt', 'roseCutAt', 'recalled']) delete page[k];
          Object.assign(page, sh.flags || {});
          pr.active = Math.max(0, sh.page - 1); CN.renderCard(page);
        }
        const rows = [];
        for (const [rid, o] of Object.entries(spec.orders)) for (const ln of o.lines) {
          const poolIds = Array.from({ length: ln.n }, (_, i) => pid(rid, ln.tx, i + 1));
          rows.push({ key: `${rid}:${5000000000 + ln.tx}`, order: { receiptId: rid, orderNumber: rid, createTs: BASE_TS - (+rid.slice(-3)) * 60, updateTs: BASE_TS, shipBy: BASE_TS + 500000, buyer: { name: 'Buyer ' + rid.slice(-3) }, lines: [], messages: [] },
            line: { transactionId: String(5000000000 + ln.tx), listingId: '', sku: 'TEST-' + ln.tx, title: 'Test charm ' + ln.tx, quantity: ln.n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + ln.tx, quantity: ln.n, material: ln.metal, problems: [] },
            problems: [], state: 'pooled', reason: null, poolIds, engrave: null, material: ln.metal, arrivedAt: Date.now() - 7200000 });
        }
        window.B.orders.rows = rows; window.B.orders.byKey = new Map(rows.map(r => [r.key, r]));
        window.B.sets.clear();
        if (spec.sets) for (const s of spec.sets) window.B.sets.set(`${RUN}|${s.group || 'all'}`, Object.assign({ runId: RUN, group: 'all', orders: {}, name: 'Set 1' }, s));
      }, { spec, RUN, BASE_TS });
      await page.waitForTimeout(200);
    }
    const onSheets = () => page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).flatMap(m => CN.S.sheets[m].pages.filter(p => p.sheetId).map(p => [p.sheetId, p.charms.map(c => c.poolId)]))));
    const runHold = (rid, extra) => page.evaluate(async ({ rid, extra }) => { window.__steps = []; const r = await OrderHold.run(rid, Object.assign({ name: 'Paul', onStep: s => window.__steps.push(s) }, extra || {})); return { r: JSON.parse(JSON.stringify(r)), steps: window.__steps }; }, { rid, extra });
    const types = steps => steps.map(s => s.type);

    // scenario 1 (exploratory)
    const H = '4170000100';
    await setup({
      sheets: [
        { id: 'gold-2', metal: 'gold', page: 2, items: [[H, 1, 1, 0, 0], [H, 1, 2, 1, 0], ['4170000201', 7, 1, 2, 0], ['4170000202', 8, 1, 3, 0]] },
        { id: 'silver-1', metal: 'silver', page: 1, items: [[H, 2, 1, 0, 0], [H, 2, 2, 1, 0], ['4170000203', 9, 1, 2, 0]] },
        { id: 'gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false], ['4170000302', 6, 1, 1, 0, false]] },
      ],
      orders: { [H]: { lines: [{ tx: 1, n: 2, metal: 'gold' }, { tx: 2, n: 2, metal: 'silver' }] }, '4170000201': { lines: [{ tx: 7, n: 1, metal: 'gold' }] }, '4170000202': { lines: [{ tx: 8, n: 1, metal: 'gold' }] }, '4170000203': { lines: [{ tx: 9, n: 1, metal: 'silver' }] }, '4170000301': { lines: [{ tx: 5, n: 1, metal: 'gold' }] }, '4170000302': { lines: [{ tx: 6, n: 1, metal: 'gold' }] } },
    });
    console.log('stock', await page.evaluate(() => JSON.stringify(stockFor('gold'))));
    const plan = await page.evaluate(rid => OrderHold.plan(rid).then(p => JSON.parse(JSON.stringify(p))), H);
    console.log(JSON.stringify(plan, null, 1).slice(0, 3000));
    const res = await runHold(H);
    console.log(JSON.stringify(res.r.error), types(res.steps).join(' '));
    console.log(JSON.stringify(await onSheets()));
    assert.deepStrictEqual(errors, [], 'no page errors');
  } catch (e) {
    console.error('errors:', errors); throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
