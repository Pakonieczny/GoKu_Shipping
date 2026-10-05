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

  // sandbox and production keep separate journals: a run left unfinished in one is never seen from the other
  const store = new Map(), ls = { getItem: k => store.has(k) ? store.get(k) : null, setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) };
  const ctx = vm.createContext({ window: {}, console, setTimeout, Date, localStorage: ls, WORKSPACE_SANDBOX: false });
  vm.runInContext(src, ctx);
  const O2 = ctx.window.OrderHold, rec = { rid: '4170009999', who: 'Paul', at: 1790000000000, lifted: ['gold-2'], done: [], names: ['GF Sheet 2'], sheetId: 'gold-2', n: 3, step: { type: 'removed' } };
  ctx.WORKSPACE_SANDBOX = true; store.set('cn.orderhold.run:sandbox', JSON.stringify({ [rec.rid]: rec }));
  assert(O2.status(rec.rid).resumable === true && O2.pending().length === 1, 'sandbox: sees its own unfinished run');
  ctx.WORKSPACE_SANDBOX = false;
  assert(O2.status(rec.rid).resumable === false && O2.pending().length === 0, 'production: never sees the sandbox run');
  store.set('cn.orderhold.run', JSON.stringify({ '4170008888': Object.assign({}, rec, { rid: '4170008888' }) }));
  assert(O2.pending().length === 1 && O2.pending()[0].rid === '4170008888' && O2.status(rec.rid).resumable === false, 'production: only its own');
  ctx.WORKSPACE_SANDBOX = true;
  assert(O2.pending().length === 1 && O2.pending()[0].rid === rec.rid && O2.status('4170008888').resumable === false, 'sandbox: only its own, too');
  ok.push('sandbox and production keep separate journals (each page sees only its own unfinished run)');
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
        const MM = 72 / 25.4, R = 5, D = Math.ceil(2 * R * MM) + 2, pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`, ageOf = rid => ((spec.orders[rid] || {}).age || 600) ;   // (seconds before BASE_TS: a bigger age is an older order)
        const mkBits = () => { const b = new Uint8Array(D * D); for (let y = 0; y < D; y++) for (let x = 0; x < D; x++) if (Math.hypot(x + .5 - D / 2, y + .5 - D / 2) <= D / 2 - 1) b[y * D + x] = 1; return b; };
        for (const m of Object.keys(CN.S.sheets)) { const pr = CN.S.sheets[m]; pr.pages.length = 1; const p0 = pr.pages[0]; p0.charms = []; p0.placements = []; p0.sheetId = null; p0.fileBase = null; for (const k of ['laserDoneAt', 'roseCutAt', 'recalled', 'setId']) delete p0[k]; p0.status = 'idle'; p0.page = 1; pr.active = 0; }
        window.B.pool.rows.clear();
        for (const sh of spec.sheets) {
          const pr = CN.S.sheets[sh.metal]; let page = null;
          if (sh.page === 1) page = pr.pages[0]; else { while (pr.pages.length < sh.page) addPage(sh.metal); page = pr.pages[sh.page - 1]; }
          page.charms = []; page.placements = [];
          sh.items.forEach(([rid, tx, copy, col, row, placed], i) => {
            const poolId = pid(rid, tx, copy), id = `${sh.id}-c${i}`;
            page.charms.push({ id, name: `${rid} · TEST-${tx}`, poolId, order: rid, lineKey: `${rid}:${5000000000 + tx}`, sku: 'TEST-' + tx, sourceId: 's', ringGeometryVersion: 3, centerPt: [D / 2, D / 2], bbox: [0, 0, D, D], outline: { circle: 1, cx: 0, cy: 0, r: R * MM }, members: [], rMm: R, w: D, h: D, scale: 1, bits: mkBits(), widthPt: 2 * R * MM, heightPt: 2 * R * MM, areaPt2: Math.PI * (R * MM) ** 2, orderDate: (BASE_TS - ageOf(rid)) * 1000, orderInfo: { receiptId: rid } });
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
          rows.push({ key: `${rid}:${5000000000 + ln.tx}`, order: { receiptId: rid, orderNumber: rid, createTs: BASE_TS - ageOf(rid), updateTs: BASE_TS, shipBy: BASE_TS + 500000, buyer: { name: 'Buyer ' + rid.slice(-3) }, lines: [], messages: [] },
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

    const O = (age, ...lines) => ({ age, lines: lines.map(([tx, n, metal]) => ({ tx, n, metal })) });
    const poolDoc = id => st.doc(POOL, id) || {}, sheetDoc = id => st.doc(SHEETS, id) || {};
    const tlOf = rid => st.list(TL).filter(x => x._id.startsWith(rid + '~'));
    const rowsOf = rid => page.evaluate(rid => Orders.rows().filter(r => String(r.order.receiptId) === rid).map(r => ({ state: r.state, hold: r.hold, pieces: r.poolIds.length })), rid);
    const journalNow = () => page.evaluate(() => JSON.parse(localStorage.getItem('cn.orderhold.run') || '{}'));
    const planOf = rid => page.evaluate(rid => OrderHold.plan(rid).then(p => JSON.parse(JSON.stringify(p))), rid);
    const sheetsOf = steps => [...new Set(steps.filter(s => s.sheetId).map(s => s.sheetId))];
    const story = (steps, id) => steps.filter(s => s.sheetId === id || s.toSheetId === id).map(s => s.type).join(' ');
    // no order is ever lost: each piece is on exactly one sheet, or on hold with its record saying who held it; the saved sheets say what the page holds
    async function noLoss(all, what) {
      const sheets = await onSheets(), flat = Object.values(sheets).flat();
      assert.strictEqual(new Set(flat).size, flat.length, `${what}: no piece is on two sheets`);
      for (const id of all) {
        const where = Object.entries(sheets).filter(([, l]) => l.includes(id)).map(([k]) => k), p = poolDoc(id), held = p.state === 'abandoned' && !!p.heldBy;
        assert(where.length === 1 ? !held && p.sheetId === where[0] : where.length === 0 && held && !p.sheetId, `${what}: ${id} is on one sheet or on hold, never lost: sheets ${JSON.stringify(where)}, record ${JSON.stringify(p)}`);
      }
      // (a sheet with nothing placed is not written at all, the window says "nothing left to write": only sheets that carry pieces have a record to compare)
      const placedOn = await page.evaluate(() => Object.fromEntries(Object.keys(CN.S.sheets).flatMap(m => CN.S.sheets[m].pages.filter(p => p.sheetId).map(p => [p.sheetId, p.placements.length]))));
      for (const [sid, list] of Object.entries(sheets)) if (placedOn[sid] > 0) assert.deepStrictEqual([...(sheetDoc(sid).poolIds || [])].sort(), [...list].sort(), `${what}: the saved ${sid} lists what the page holds`);
    }
    const allIds = spec => spec.sheets.flatMap(sh => sh.items.map(([rid, tx, copy]) => pid(rid, tx, copy)));
    const stepsInOrder = steps => { for (let i = 1; i < steps.length; i++) assert(steps[i].at >= steps[i - 1].at && steps[i].t >= steps[i - 1].t, 'every step has `at` and `t`, and they only grow'); assert(steps.every(s => s.at > 1e12 && s.t >= 0), 'at is epoch ms'); };

    /* scenario 1 · a 4-piece order on GF Sheet 2 and SS Sheet 1: its two gold pieces' spots are filled by the orders waiting (oldest first),
       its two silver spots (nothing silver waits) by orders from the newer, incomplete SS Sheet 2 */
    const H = '4170000100';
    const S1 = {
      sheets: [
        { id: 'gold-2', metal: 'gold', page: 2, items: [[H, 1, 1, 0, 0], [H, 1, 2, 1, 0], ['4170000201', 7, 1, 2, 0], ['4170000202', 8, 1, 3, 0]] },
        { id: 'silver-1', metal: 'silver', page: 1, items: [[H, 2, 1, 0, 0], [H, 2, 2, 1, 0], ['4170000203', 9, 1, 2, 0]] },
        { id: 'gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false], ['4170000302', 6, 1, 1, 0, false], ['4170000303', 4, 1, 2, 0, false]] },
        { id: 'silver-2', metal: 'silver', page: 2, items: [['4170000401', 11, 1, 0, 0], ['4170000402', 12, 1, 1, 0], ['4170000403', 13, 1, 2, 0]] },
      ],
      orders: { [H]: O(9000, [1, 2, 'gold'], [2, 2, 'silver']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000203': O(8200, [9, 1, 'silver']),
        '4170000301': O(7000, [5, 1, 'gold']), '4170000302': O(7500, [6, 1, 'gold']), '4170000303': O(6000, [4, 1, 'gold']),
        '4170000401': O(5000, [11, 1, 'silver']), '4170000402': O(5500, [12, 1, 'silver']), '4170000403': O(4000, [13, 1, 'silver']) },
    };
    await setup(S1);
    const idsH = [...[1, 2].map(c => pid(H, 1, c)), ...[1, 2].map(c => pid(H, 2, c))];
    let plan = await planOf(H);
    assert(plan.canHold && plan.blockedWhy === null && plan.label === H && plan.customer === 'Buyer 100' && plan.shipBy === (BASE_TS + 500000) * 1000, 'the plan reads the order: ' + JSON.stringify([plan.label, plan.customer, plan.shipBy]));
    assert.deepStrictEqual(plan.pieces.map(p => p.poolId).sort(), idsH.slice().sort(), 'the plan lists the four pieces of the order');
    assert(plan.pieces.every(p => p.state === 'onSheet' && p.sheetId && p.sheetLabel && p.lineKey), 'each piece: on a sheet, with its sheet, label and line');
    assert.deepStrictEqual(plan.sheets.map(s => [s.sheetId, s.label, s.removes, s.qrRemade, s.metal]), [['gold-2', 'GF Sheet 2', 2, true, 'gold'], ['silver-1', 'SS Sheet 1', 2, true, 'silver']], 'two sheets come off');
    assert.strictEqual(plan.estimate, false, 'the room was really searched, not estimated');
    const pg = plan.fills.filter(f => f.sheetId === 'gold-2'), ps = plan.fills.filter(f => f.sheetId === 'silver-1');
    assert(pg.length === 1 && pg[0].source === 'waiting' && pg[0].orders === 2 && pg[0].spots === 2, 'GF Sheet 2: two waiting orders: ' + JSON.stringify(pg));
    assert(ps.length === 1 && ps[0].source === 'newerSheet' && ps[0].fromSheetId === 'silver-2' && ps[0].fromSheetLabel === 'SS Sheet 2' && ps[0].orders === 2 && ps[0].spots === 2, 'SS Sheet 1: two orders from the newer SS Sheet 2: ' + JSON.stringify(ps));
    assert(plan.effects.some(t => /4 pieces of this order come off GF Sheet 2 and SS Sheet 1/.test(t)) && plan.effects.some(t => /2 waiting orders and 2 orders from SS Sheet 2 fill the 4 empty spots/.test(t)), 'the consent sentences: ' + plan.effects.join(' | '));
    assert.deepStrictEqual(await rowsOf(H), [{ state: 'pooled', hold: undefined, pieces: 2 }, { state: 'pooled', hold: undefined, pieces: 2 }].map(r => Object.assign(r, { hold: r.hold })), 'planning changes nothing');
    assert.strictEqual(st.list(TL).length, 0, 'planning writes nothing');
    assert(!(await page.evaluate(() => (window.__nests || []).length)), 'planning nests nothing');

    let res = await runHold(H);
    assert(res.r.ok && res.r.held === true && !res.r.error, 'the hold goes through: ' + JSON.stringify(res.r.error));
    const T = types(res.steps);
    assert.strictEqual(T[0], 'start'); assert.deepStrictEqual(T.slice(-3), ['sheetDone', 'held', 'done'], 'the story ends: sheetDone, held, done');
    assert.deepStrictEqual(res.steps[0].sheets.sort(), ['gold-2', 'silver-1'], 'start names the sheets');
    assert(res.steps.find(s => s.type === 'held').rid === H && T[T.length - 1] === 'done' && res.r.steps.length === res.steps.length, 'held names the order; the result carries every step');
    stepsInOrder(res.steps);
    for (const id of ['gold-2', 'silver-1']) assert(/^sheetBegin lift removed fillBegin (fillFrom fillPlaced )+qr sheetDone$/.test(story(res.steps, id)), `${id}: its steps come in the contract's order: ` + story(res.steps, id));
    const lift = res.steps.find(s => s.type === 'lift' && s.sheetId === 'gold-2');
    assert.deepStrictEqual(lift.poolIds.sort(), [pid(H, 1, 1), pid(H, 1, 2)].sort(), 'lift names the pieces') ; assert(lift.rects.length === 2 && lift.rects.every(r => r.w > 0 && r.h > 0 && typeof r.x === 'number' && typeof r.y === 'number') && lift.sheet.wPt > 100 && lift.label === 'GF Sheet 2', 'lift carries each piece\'s rectangle in sheet points and the sheet\'s size: ' + JSON.stringify(lift));
    assert(res.steps.filter(s => s.type === 'removed').every(s => s.removed === 2), 'removed says how many');
    const fromGold = res.steps.filter(s => s.type === 'fillFrom' && s.toSheetId === 'gold-2'), fromSilver = res.steps.filter(s => s.type === 'fillFrom' && s.toSheetId === 'silver-1');
    assert.deepStrictEqual(fromGold.map(s => [s.rid, s.source, s.fromSheetId]), [['4170000302', 'waiting', 'gold-3'], ['4170000301', 'waiting', 'gold-3']], 'the oldest waiting orders fill first: ' + JSON.stringify(fromGold));
    assert.deepStrictEqual(fromSilver.map(s => [s.rid, s.source, s.fromSheetId]), [['4170000402', 'newerSheet', 'silver-2'], ['4170000401', 'newerSheet', 'silver-2']], 'with none waiting, the oldest orders of the newer SS Sheet 2 fill: ' + JSON.stringify(fromSilver));
    assert(fromGold.every(s => s.poolIds.length === 1), 'fillFrom names the incoming pieces');
    assert.deepStrictEqual(res.steps.filter(s => s.type === 'fillPlaced' && s.sheetId === 'gold-2').map(s => s.rid), ['4170000302', '4170000301'], 'fillPlaced follows each');
    const sd = res.steps.filter(s => s.type === 'sheetDone');
    assert(sd.length === 2 && sd.every(s => s.charmCount === 4 || s.charmCount === 3) && sd.every(s => typeof s.density === 'number'), 'sheetDone says the charm count and density: ' + JSON.stringify(sd));
    let sh = await onSheets();
    const sorted = a => a.slice().sort();
    assert.deepStrictEqual(sorted(sh['gold-2']), sorted(['4170000201_5000000007_1', '4170000202_5000000008_1', '4170000302_5000000006_1', '4170000301_5000000005_1']), 'GF Sheet 2: its other orders and the two that moved in; none of the held order');
    assert.deepStrictEqual(sh['gold-3'], ['4170000303_5000000004_1'], 'the newest waiting order stays waiting on GF Sheet 3');
    assert.deepStrictEqual(sorted(sh['silver-1']), sorted(['4170000203_5000000009_1', '4170000402_5000000012_1', '4170000401_5000000011_1']), 'SS Sheet 1 likewise');
    assert.deepStrictEqual(sh['silver-2'], ['4170000403_5000000013_1'], 'SS Sheet 2 keeps its newest order');
    for (const id of idsH) { const p = poolDoc(id); assert(p.state === 'abandoned' && !p.sheetId && p.heldBy === 'Paul' && p.heldAt && !p.removedAt, 'a held piece: abandoned, off its sheet, held by Paul, no removal: ' + JSON.stringify(p)); }
    const moved = poolDoc('4170000302_5000000006_1'); assert(moved.sheetId === 'gold-2' && moved.movedFrom === 'gold-3' && moved.movedBy === 'Paul' && moved.moveVerifiedAt, 'a moved order\'s piece says where it came from, who moved it and that it was verified: ' + JSON.stringify(moved));
    const rows1 = await rowsOf(H);
    assert(rows1.every(r => r.state === 'held' && r.hold === 'Taken off GF Sheet 2, SS Sheet 1 by Paul' && r.pieces === 0), 'On hold, with one plain reason: ' + JSON.stringify(rows1));
    const ev = tlOf(H).filter(x => ['removed', 'held', 'cancelled'].includes(x.type));
    assert(ev.length === 2 && ev.every(x => x.type === 'held' && x.by === 'Paul') && ev.some(x => x.sheetId === 'gold-2' && /GF Sheet 2/.test(x.text)) && ev.some(x => x.sheetId === 'silver-1' && /SS Sheet 1/.test(x.text)), 'each sheet is its own timeline step, with the sheet, the person and the time: ' + JSON.stringify(ev.map(x => [x.type, x.sheetId, x.text, x.by])));
    assert.deepStrictEqual(await journalNow(), {}, 'a finished hold leaves no journal'); const stt = await page.evaluate(rid => OrderHold.status(rid), H); assert(stt.running === false && stt.resumable === false, 'status: not running');
    assert(!(await page.evaluate(() => !!SheetWin._W.flow)), 'no change is left running');
    await noLoss(allIds(S1), 'scenario 1');
    // the page's own list: the order is under On hold
    assert(await page.evaluate(rid => Orders.rows().some(r => String(r.order.receiptId) === rid && r.hold && r.state === 'held'), H), 'the order is under On hold');
    ok.push('4 pieces on GF Sheet 2 and SS Sheet 1: all off, GF Sheet 2 refilled by the two oldest waiting orders, SS Sheet 1 (none waiting) by two orders of the newer SS Sheet 2, steps in the contract\'s order, held with one reason, one timeline step per sheet, nothing lost');

    // the order is on hold now: Hold is not offered again
    plan = await planOf(H);
    assert(!plan.canHold && /already on hold/.test(plan.blockedWhy), 'an order on hold cannot be held again: ' + plan.blockedWhy);
    res = await runHold(H);
    assert(!res.r.ok && res.r.held === false && /already on hold/.test(res.r.error) && types(res.steps).join() === 'error' && res.steps[0].message === res.r.error && res.steps[0].rid === H, 'a second Hold is refused with an error step');
    assert.deepStrictEqual(await journalNow(), {}, 'a refused hold leaves no journal');
    ok.push('the same order cannot be held twice (plan and run both say so, nothing is written)');

    /* scenario 2 · nothing fits: the only orders that could fill the spot are too big for it (one piece's room), so the spot stays
       free, the hold stands, and the freed room is kept for the orders that arrive */
    const H2 = '4170000500';
    const S2 = {
      sheets: [
        { id: 's2-gold-2', metal: 'gold', page: 2, items: [[H2, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]] },
        // (a waiting order of three pieces and a placed one of three: each needs three spots, the freed room is one)
        { id: 's2-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false], ['4170000301', 5, 2, 1, 0, false], ['4170000301', 5, 3, 2, 0, false], ['4170000302', 6, 1, 0, 1], ['4170000302', 6, 2, 1, 1], ['4170000302', 6, 3, 2, 1]] },
      ],
      orders: { [H2]: O(9000, [1, 1, 'gold']), '4170000201': O(8000, [7, 1, 'gold']), '4170000301': O(7000, [5, 3, 'gold']), '4170000302': O(6000, [6, 3, 'gold']) },
    };
    await setup(S2);
    plan = await planOf(H2);
    assert(plan.canHold && plan.fills.length === 1 && plan.fills[0].source === 'none' && plan.fills[0].spots === 1 && plan.fills[0].orders === 0, 'the plan says nothing fits: ' + JSON.stringify(plan.fills));
    assert(plan.effects.some(t => /1 empty spot stays free/.test(t)), 'and says so: ' + plan.effects.join(' | '));
    res = await runHold(H2);
    assert(res.r.ok && res.r.held, 'the hold stands: ' + JSON.stringify(res.r.error));
    assert.strictEqual(story(res.steps, 's2-gold-2'), 'sheetBegin lift removed fillBegin fillSkipped qr sheetDone', 'the spot is left free: ' + story(res.steps, 's2-gold-2') + ' ' + JSON.stringify(res.steps.filter(x => /fill/.test(x.type))) + ' est=' + plan.estimate);
    assert(!types(res.steps).includes('fillFrom') && types(res.steps).slice(-2).join() === 'held,done', 'nothing moved in');
    sh = await onSheets();
    assert.deepStrictEqual(sh['s2-gold-2'], ['4170000201_5000000007_1'], 'GF Sheet 2 keeps its other order; the spot is free');
    assert.strictEqual(sh['s2-gold-3'].length, 6, 'the big orders stay where they were');
    assert.strictEqual(await page.evaluate(() => SheetWin.holdKit.freed('s2-gold-2')), 1, 'the freed room is kept for the orders that arrive');
    assert(poolDoc(pid(H2, 1, 1)).state === 'abandoned' && poolDoc(pid(H2, 1, 1)).heldBy === 'Paul', 'held');
    await noLoss(allIds(S2), 'scenario 2');
    ok.push('nothing fits: the spot stays free (kept for new arrivals), the hold stands, the big orders stay where they were, nothing lost');

    /* scenario 3 · a cut sheet keeps its piece: the order's other pieces come off, the piece on the cut sheet is listed as staying */
    const H3 = '4170000600';
    const S3 = {
      sheets: [
        { id: 's3-gold-1', metal: 'gold', page: 1, items: [[H3, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]], flags: { laserDoneAt: 1790000000000 } },
        { id: 's3-gold-2', metal: 'gold', page: 2, items: [[H3, 2, 1, 0, 0], ['4170000202', 8, 1, 1, 0], ['4170000203', 9, 1, 2, 0]] },
        { id: 's3-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false]] },
      ],
      orders: { [H3]: O(9000, [1, 1, 'gold'], [2, 1, 'gold']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000203': O(8200, [9, 1, 'gold']), '4170000301': O(7000, [5, 1, 'gold']) },
    };
    await setup(S3);
    plan = await planOf(H3);
    assert(plan.canHold && plan.stays.length === 1 && plan.stays[0].poolId === pid(H3, 1, 1) && plan.stays[0].sheetLabel === 'GF Sheet 1' && /marked completed/.test(plan.stays[0].why), 'the piece on the cut sheet is listed as staying: ' + JSON.stringify(plan.stays));
    assert.deepStrictEqual(plan.pieces.map(p => [p.poolId, p.state]).sort(), [[pid(H3, 1, 1), 'onCutSheet'], [pid(H3, 2, 1), 'onSheet']].sort(), 'its state is onCutSheet');
    assert.deepStrictEqual(plan.sheets.map(s => s.sheetId), ['s3-gold-2'], 'only the sheet that can give it up comes off');
    assert(plan.effects.some(t => /1 piece stays: GF Sheet 1 \(that sheet was marked completed\)/.test(t)), 'the consent text says it stays: ' + plan.effects.join(' | '));
    res = await runHold(H3);
    assert(res.r.ok && res.r.held, 'the hold goes through for what can come off: ' + JSON.stringify(res.r.error));
    assert.deepStrictEqual(sheetsOf(res.steps), ['s3-gold-2'], 'only GF Sheet 2 is touched');
    sh = await onSheets();
    assert.deepStrictEqual(sorted(sh['s3-gold-1']), sorted([pid(H3, 1, 1), '4170000201_5000000007_1']), 'the cut sheet is exactly as it was, with the piece on it');
    assert(!(await page.evaluate(() => (window.__nests || []).some(n => n.sheetId === 's3-gold-1'))), 'the cut sheet is never nested, filled or taken from');
    assert(poolDoc(pid(H3, 1, 1)).state === 'placed' && poolDoc(pid(H3, 1, 1)).sheetId === 's3-gold-1', 'its record is untouched');
    assert(sh['s3-gold-2'].includes('4170000301_5000000005_1') && !sh['s3-gold-2'].includes(pid(H3, 2, 1)), 'the freed spot on GF Sheet 2 was filled with the waiting order');
    const rows3 = await rowsOf(H3);
    assert(rows3.some(r => r.state === 'held' && /Taken off GF Sheet 2 by Paul/.test(r.hold) && r.pieces === 0) && rows3.some(r => r.state !== 'held' && !r.hold && r.pieces === 1), 'the line on the cut sheet stays as it is: ' + JSON.stringify(rows3));
    ok.push('a piece on a cut sheet stays (listed in stays and in the consent text), the cut sheet is never touched, the rest comes off and the spot is refilled');

    /* scenario 4 · a piece inside a committed set blocks the hold: nothing changes at all */
    const H4 = '4170000700';
    const S4 = {
      sheets: [
        { id: 's4-gold-1', metal: 'gold', page: 1, items: [[H4, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]], flags: { setId: 'set-sent-1' } },
        { id: 's4-gold-2', metal: 'gold', page: 2, items: [[H4, 2, 1, 0, 0], ['4170000202', 8, 1, 1, 0]] },
      ],
      orders: { [H4]: O(9000, [1, 1, 'gold'], [2, 1, 'gold']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']) },
      sets: [{ setId: 'set-sent-1', name: 'Set 1', committedAt: 1790000000000, sheetIds: ['s4-gold-1'] }],
    };
    await setup(S4);
    plan = await planOf(H4);
    assert(!plan.canHold && /^Undo the set first/.test(plan.blockedWhy) && plan.pieces.some(p => p.state === 'inCommittedSet' && p.sheetId === 's4-gold-1') && plan.effects.length === 0, 'a committed set blocks: ' + plan.blockedWhy);
    res = await runHold(H4);
    assert(!res.r.ok && res.r.held === false && /^Undo the set first/.test(res.r.error) && types(res.steps).join() === 'error', 'run refuses with an error step: ' + JSON.stringify(res.r));
    sh = await onSheets();
    assert.deepStrictEqual(sorted(sh['s4-gold-2']), sorted([pid(H4, 2, 1), '4170000202_5000000008_1']), 'nothing came off the other sheet either: a half hold would lose work');
    assert(!(await page.evaluate(() => (window.__nests || []).length)) && st.list(TL).length === 0 && !poolDoc(pid(H4, 2, 1)).heldBy, 'nothing was nested, saved or recorded');
    assert.deepStrictEqual(await journalNow(), {}, 'and no journal');
    ok.push('a piece inside a committed set blocks the hold ("Undo the set first"): plan says so, run refuses, nothing at all changes');

    /* scenario 5 · Rose Gold: its pieces come off its sheet, the sheet is never filled or re-arranged by the hold */
    const H5 = '4170000800';
    const S5 = {
      sheets: [
        { id: 's5-rose-1', metal: 'rose', page: 1, items: [[H5, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0], ['4170000202', 8, 1, 2, 0]] },
        { id: 's5-rose-2', metal: 'rose', page: 2, items: [['4170000301', 5, 1, 0, 0, false]] },
      ],
      orders: { [H5]: O(9000, [1, 1, 'rose']), '4170000201': O(8000, [7, 1, 'rose']), '4170000202': O(8100, [8, 1, 'rose']), '4170000301': O(7000, [5, 1, 'rose']) },
    };
    await setup(S5);
    plan = await planOf(H5);
    assert(plan.canHold && plan.fills.length === 1 && plan.fills[0].source === 'none' && /Rose Gold/.test(plan.fills[0].why) && plan.effects.some(t => /Rose Gold sheets are never re-arranged/.test(t)), 'the plan: never filled: ' + JSON.stringify(plan.fills));
    res = await runHold(H5);
    assert(res.r.ok && res.r.held, 'a Rose Gold piece comes off: ' + JSON.stringify(res.r.error));
    assert.strictEqual(story(res.steps, 's5-rose-1'), 'sheetBegin lift removed fillBegin fillSkipped qr sheetDone', 'its steps, with no fill: ' + story(res.steps, 's5-rose-1'));
    assert(res.steps.find(s => s.type === 'fillSkipped').why === 'Rose Gold sheets are never re-arranged', 'and the reason');
    sh = await onSheets();
    assert.deepStrictEqual(sorted(sh['s5-rose-1']), sorted(['4170000201_5000000007_1', '4170000202_5000000008_1']), 'the others stay exactly where they were');
    assert.deepStrictEqual(sh['s5-rose-2'], ['4170000301_5000000005_1'], 'the waiting Rose Gold order did not move in');
    await noLoss(allIds(S5), 'scenario 5');
    ok.push('Rose Gold: the piece comes off, no order moves into the sheet (never re-arranged), the waiting order stays waiting');

    /* scenario 6 · a hold a reload cut short carries on: GF Sheet 2's piece is off already (its spot waits to be filled), the journal
       says so; the next run resumes with the name in the journal, fills the spot, takes SS Sheet 1's piece off and finishes */
    const H6 = '4170000900';
    const S6 = {
      sheets: [
        { id: 's6-gold-2', metal: 'gold', page: 2, items: [[H6, 1, 1, 0, 0], ['4170000201', 7, 1, 1, 0]] },
        { id: 's6-silver-1', metal: 'silver', page: 1, items: [[H6, 2, 1, 0, 0], ['4170000203', 9, 1, 1, 0]] },
        { id: 's6-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false]] },
      ],
      orders: { [H6]: O(9000, [1, 1, 'gold'], [2, 1, 'silver']), '4170000201': O(8000, [7, 1, 'gold']), '4170000203': O(8200, [9, 1, 'silver']), '4170000301': O(7000, [5, 1, 'gold']) },
    };
    await setup(S6);
    // (the first half, as the engine does it, then the page "goes away": what is left is the journal, the freed spot and the saved records)
    await page.evaluate(async rid => {
      const K = SheetWin.holdKit, off = K.offPlan(rid), mine = off.ok.filter(o => o.sh && o.sh.sheetId === 's6-gold-2');
      await K.takeOff({ rid, whole: true, ids: new Set(mine.map(o => o.id)), ok: mine, stay: [] }, 'Paul', '');
      localStorage.setItem('cn.orderhold.run', JSON.stringify({ [rid]: { rid, who: 'Paul', note: '', at: Date.now() - 5000, lifted: ['s6-gold-2'], done: [], names: ['GF Sheet 2'], sheetId: 's6-gold-2', n: 4, step: { type: 'removed', sheetId: 's6-gold-2', removed: 1, at: Date.now(), t: 100 } } }));
    }, H6);
    let stt6 = await page.evaluate(rid => ({ s: OrderHold.status(rid), p: OrderHold.pending() }), H6);
    assert(stt6.s.running === false && stt6.s.resumable === true && stt6.s.name === 'Paul' && stt6.s.step.type === 'removed' && stt6.s.sheetId === 's6-gold-2', 'status tells a reloaded page where the run stopped: ' + JSON.stringify(stt6.s));
    assert(stt6.p.length === 1 && stt6.p[0].rid === H6 && stt6.p[0].name === 'Paul', 'pending lists it');
    sh = await onSheets();
    assert.deepStrictEqual(sh['s6-gold-2'], ['4170000201_5000000007_1'], 'its piece is off GF Sheet 2 already');
    assert(poolDoc(pid(H6, 1, 1)).state === 'abandoned' && poolDoc(pid(H6, 2, 1)).state === 'placed', 'the other piece is still on SS Sheet 1');
    res = await runHold(H6, { name: '' });
    assert(res.r.ok && res.r.held, 'the resumed hold goes through: ' + JSON.stringify(res.r.error));
    assert(res.steps[0].type === 'start' && res.steps[0].resumed === true && res.steps[0].sheets.includes('s6-gold-2') && res.steps[0].sheets.includes('s6-silver-1'), 'start says it resumed, and names both sheets');
    assert.strictEqual(story(res.steps, 's6-gold-2'), 'sheetBegin fillBegin fillFrom fillPlaced qr sheetDone', 'GF Sheet 2: no lift again, the waiting spot is filled: ' + story(res.steps, 's6-gold-2'));
    assert.strictEqual(story(res.steps, 's6-silver-1'), 'sheetBegin lift removed fillBegin fillSkipped qr sheetDone', 'SS Sheet 1: lifted now');
    sh = await onSheets();
    assert(sh['s6-gold-2'].includes('4170000301_5000000005_1') && !sh['s6-silver-1'].includes(pid(H6, 2, 1)), 'filled; the second piece is off too');
    assert(res.steps.filter(s => s.type === 'lift').length === 1 && res.steps.filter(s => s.type === 'sheetDone').length === 2, 'each sheet done once');
    assert((await rowsOf(H6)).every(r => r.state === 'held' && r.hold === 'Taken off GF Sheet 2, SS Sheet 1 by Paul'), 'one reason for both sheets: ' + JSON.stringify(await rowsOf(H6)));
    assert.deepStrictEqual(await journalNow(), {}, 'the journal is gone'); stt6 = await page.evaluate(rid => OrderHold.status(rid), H6); assert(!stt6.running && !stt6.resumable, 'status: finished');
    await noLoss(allIds(S6), 'scenario 6');
    ok.push('a hold cut short carries on: status/pending tell where it stopped, the next run resumes with the saved name, fills the waiting spot, takes the rest off, one reason, journal gone, nothing lost');

    /* scenario 7 · one change at a time; status while running */
    const H7 = '4170001000', H7b = '4170001001';
    const S7 = {
      sheets: [
        { id: 's7-gold-2', metal: 'gold', page: 2, items: [[H7, 1, 1, 0, 0], [H7b, 3, 1, 1, 0], ['4170000201', 7, 1, 2, 0]] },
        { id: 's7-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false]] },
      ],
      orders: { [H7]: O(9000, [1, 1, 'gold']), [H7b]: O(9100, [3, 1, 'gold']), '4170000201': O(8000, [7, 1, 'gold']), '4170000301': O(7000, [5, 1, 'gold']) },
    };
    await setup(S7);
    const both = await page.evaluate(async ([a, b]) => {
      const seen = [];
      const p1 = OrderHold.run(a, { name: 'Paul', onStep: s => seen.push(s.type) }), p2 = OrderHold.run(b, { name: 'Paul' }), p3 = OrderHold.run(a, { name: 'Paul' });
      const mid = OrderHold.status(a);   // (running as soon as it is called)
      const [r1, r2, r3] = await Promise.all([p1, p2, p3]);
      return { r1: { ok: r1.ok, error: r1.error }, r2: { ok: r2.ok, error: r2.error }, r3: { ok: r3.ok, error: r3.error }, mid: { running: mid.running, who: mid.name }, after: OrderHold.status(a).running };
    }, [H7, H7b]);
    assert(both.r1.ok && !both.r2.ok && !both.r3.ok && /One change at a time/.test(both.r2.error) && /One change at a time/.test(both.r3.error), 'a second hold while one runs is refused, plainly: ' + JSON.stringify(both));
    assert(both.mid.running === true && both.mid.who === 'Paul' && both.after === false, 'status: running while it runs, not after');
    await noLoss(allIds(S7).filter(id => id.startsWith(H7 + '_') || !id.startsWith(H7b)), 'scenario 7');
    assert.strictEqual(poolDoc(pid(H7b, 3, 1)).state, 'placed', 'the refused order is untouched');
    ok.push('one change at a time: a second Hold while one runs is refused with a plain message (status says running), the refused order is untouched');

    /* scenario 8 · an order alone on a sheet cannot leave it empty; a sheet this page has not loaded blocks: both say why, nothing changes */
    const H8 = '4170001100', H8b = '4170001101';
    const S8 = {
      sheets: [
        { id: 's8-gold-2', metal: 'gold', page: 2, items: [[H8, 1, 1, 0, 0], [H8, 1, 2, 1, 0]] },
        { id: 's8-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0, false]] },
        { id: 's8-silver-1', metal: 'silver', page: 1, items: [[H8b, 2, 1, 0, 0], ['4170000203', 9, 1, 1, 0]] },
      ],
      orders: { [H8]: O(9000, [1, 2, 'gold']), [H8b]: O(9100, [2, 2, 'silver']), '4170000301': O(7000, [5, 1, 'gold']), '4170000203': O(8200, [9, 1, 'silver']) },
    };
    await setup(S8);
    // (the second copy of the silver line is recorded on a sheet this page never loaded)
    await page.evaluate(rid => { const p = pid2(rid); window.B.pool.rows.set(p, { poolId: p, orderId: rid, sheetId: 'far-away-sheet', state: 'placed', material: 'silver' }); Orders.rows().find(r => String(r.order.receiptId) === rid).poolIds.push(p); function pid2(r) { return `${r}_5000000002_2`; } }, H8b);
    plan = await planOf(H8);
    assert(!plan.canHold && /only one on GF Sheet 2/.test(plan.blockedWhy) && /never left empty/.test(plan.blockedWhy), 'alone on a sheet: ' + plan.blockedWhy);
    res = await runHold(H8); assert(!res.r.ok && /only one on GF Sheet 2/.test(res.r.error) && types(res.steps).join() === 'error', 'run refuses');
    plan = await planOf(H8b);
    assert(!plan.canHold && /not open in this sorter/.test(plan.blockedWhy), 'a sheet not loaded: ' + plan.blockedWhy);
    res = await runHold(H8b); assert(!res.r.ok && types(res.steps).join() === 'error');
    sh = await onSheets();
    assert.deepStrictEqual(sh['s8-gold-2'].length, 2); assert.deepStrictEqual(sorted(sh['s8-silver-1']), sorted([pid(H8b, 2, 1), '4170000203_5000000009_1']), 'nothing came off');
    assert(!(await page.evaluate(() => (window.__nests || []).length)) && st.list(TL).length === 0 && (await journalNow()) && Object.keys(await journalNow()).length === 0, 'nothing nested, recorded or journalled');
    ok.push('an order alone on a sheet, or with a piece on a sheet this page has not loaded: blocked with a plain reason, nothing changes');

    /* scenario 9 · only a NEWER sheet gives an order away (an older sheet's order stays put); a sheet released into its set keeps its set */
    const H9 = '4170001200';
    const S9 = {
      sheets: [
        { id: 's9-gold-1', metal: 'gold', page: 1, items: [['4170000201', 7, 1, 0, 0], ['4170000202', 8, 1, 1, 0]] },
        { id: 's9-gold-2', metal: 'gold', page: 2, items: [[H9, 1, 1, 0, 0], ['4170000203', 9, 1, 1, 0]], flags: { setId: 'set-9', draft: false } },
        { id: 's9-gold-3', metal: 'gold', page: 3, items: [['4170000301', 5, 1, 0, 0], ['4170000302', 6, 1, 1, 0]] },
      ],
      orders: { [H9]: O(9000, [1, 1, 'gold']), '4170000201': O(8000, [7, 1, 'gold']), '4170000202': O(8100, [8, 1, 'gold']), '4170000203': O(8200, [9, 1, 'gold']), '4170000301': O(7000, [5, 1, 'gold']), '4170000302': O(7100, [6, 1, 'gold']) },
      sets: [{ setId: 'set-9', name: 'Set 9', sheetIds: ['s9-gold-2'] }],
    };
    await setup(S9);
    plan = await planOf(H9);
    assert(plan.canHold && plan.sheets[0].setLabel === 'Set 9' && plan.effects.some(t => /GF Sheet 2 \(Set 9\)/.test(t)) && plan.fills.length === 1 && plan.fills[0].source === 'newerSheet' && plan.fills[0].fromSheetLabel === 'GF Sheet 3', 'the older sheet\'s orders are not offered; the newer sheet is: ' + JSON.stringify(plan.fills));
    res = await runHold(H9);
    assert(res.r.ok, 'ok: ' + res.r.error);
    sh = await onSheets();
    assert(sh['s9-gold-2'].includes('4170000302_5000000006_1') && !sh['s9-gold-2'].includes(pid(H9, 1, 1)), 'the oldest order of the newer sheet moved over: ' + JSON.stringify(sh['s9-gold-2']));
    assert.deepStrictEqual(sorted(sh['s9-gold-1']), sorted(['4170000201_5000000007_1', '4170000202_5000000008_1']), 'the older sheet gave nothing');
    assert(await page.evaluate(() => { const p = SheetWin._W && CN.S.sheets.gold.pages[1]; return p.setId === 'set-9' && !p.keepRelease; }), 'the sheet stays in its set (its hold-over mark is gone)');
    await noLoss(allIds(S9), 'scenario 9');
    ok.push('only a newer sheet gives an order away; a sheet in a set stays in it with its set named in the plan');
    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('order-hold-engine: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors); throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
