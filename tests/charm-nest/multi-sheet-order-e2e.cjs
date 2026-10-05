// ONE offline end-to-end harness for Paul's round-2 point 1: "when an order has 2+ charms in two different Sheets ... the Silver
// Sheet says 'Charm not on sheet yet' which is incorrect ... There is a breakdown in how sheets retain and associate orders
// which have multiple charms spread across several sheets. This must be fixed and thoroughly tested from all side and all
// possible screens, modals, tabs."
//
// It seeds ONE set of multi-piece orders into a fake backend (bridge-server.cjs: the real charmNestLibrary handler over an
// in-memory Firestore) and into the page, then opens EVERY surface that shows or computes "which sheet holds which piece of
// an order" (the inventory: /mnt/project-files/plans/library-flow/surfaces-inventory.md, ids A1..F4 below) from every side
// (opened from each piece, from each sheet) and compares what each one says with the ground truth computed from the fixture.
//
//   node tests/charm-nest/multi-sheet-order-e2e.cjs            all surfaces, exit 1 on any wrong surface
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --list     print the failing surfaces and exit 0 (to record a baseline)
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --only A3,B1   only those surface ids
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --dump     print what each surface says (debugging a fixture)
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules  (CHROMIUM=<chrome> to override)
//
// Truth is computed from the FIXTURE below, never from OrderPieces: a separate check asserts window.OrderPieces (when the
// page has it) says the same, so a wrong OrderPieces cannot hide a wrong screen.
// Offline only: no cloud, no Etsy, no paid AI, nothing written anywhere outside the in-memory fake.
const path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

/* ═══════════════════════════ fixture: the shop ═══════════════════════════ */
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000), NOW = Date.now(), DAYSTR = '2026-10-05';
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
const METAL_LABEL = { gold: '14k Gold Filled', silver: 'Sterling Silver', rose: 'Rose Gold Filled' };
// the sheets: Set-1 holds one GF and one SS sheet (an order spread over both), Set-2 a second GF sheet. `n` = the sheet's number.
const SHEETS = [
  { id: 'sh-gf1', metal: 'gold', n: 1, set: 'set-1' },
  { id: 'sh-ss1', metal: 'silver', n: 1, set: 'set-1' },
  { id: 'sh-gf2', metal: 'gold', n: 2, set: 'set-2' },
];
const SETS = [{ id: 'set-1', seq: 1 }, { id: 'set-2', seq: 2 }];
const sheetLabel = s => `${CODE[s.metal]} Sheet ${s.n}`;
const SHEET = Object.fromEntries(SHEETS.map(s => [s.id, s]));
/* The orders. A line (piece) = { sku, metal, qty, copies: [sheetId|null per copy], state }.
   sku '' = a piece with no SKU (state 'unmatched', never pooled, on no sheet); copies [null] = not nested yet;
   state: 'written' (nested, the default when it has a sheet) | 'pooled' (waiting for a sheet) | 'unmatched' | 'gone' (cancelled piece). */
const ORDERS = [
  // Paul's image 1/2: one SKU in two metals, one piece on the GF sheet and one on the SS sheet of the same set
  { rid: '4170000001', buyer: 'Nathaly Soto', tag: 'two sheets (GF + SS)', lines: [{ sku: 'FEMALE_SYMBOL', metal: 'gold', copies: ['sh-gf1'] }, { sku: 'FEMALE_SYMBOL', metal: 'silver', copies: ['sh-ss1'] }] },
  // three pieces on three sheets: both GF sheets (two different sets) and the SS sheet
  { rid: '4170000002', buyer: 'Mia Lund', tag: 'three sheets (GF, SS, GF)', lines: [{ sku: 'TINY_TAG', metal: 'gold', copies: ['sh-gf1'] }, { sku: 'LEAF_CHARM', metal: 'silver', copies: ['sh-ss1'] }, { sku: 'MOON_STAR', metal: 'gold', copies: ['sh-gf2'] }] },
  // Paul's image 5: a piece with no SKU (on no sheet), the other on GF Sheet 1
  { rid: '4170000003', buyer: 'Emily Chambers', tag: 'one piece has no SKU', lines: [{ sku: '', title: 'CUTE TRICERATOPS W/ HEARTS', metal: 'gold', copies: [null], state: 'unmatched' }, { sku: 'HEALTH1', metal: 'gold', copies: ['sh-gf1'] }] },
  // both pieces on one sheet
  { rid: '4170000004', buyer: 'Ola Berg', tag: 'both pieces on one sheet', lines: [{ sku: 'PAW_PRINT', metal: 'silver', copies: ['sh-ss1'] }, { sku: 'BONE_TAG', metal: 'silver', copies: ['sh-ss1'] }] },
  // quantity 2: the two copies of one piece sit on two GF sheets, a second piece on the SS sheet
  { rid: '4170000005', buyer: 'Sam Ortiz', tag: 'quantity 2 across two sheets', lines: [{ sku: 'HEART_STUD', metal: 'gold', qty: 2, copies: ['sh-gf1', 'sh-gf2'] }, { sku: 'KEY_CHARM', metal: 'silver', copies: ['sh-ss1'] }] },
  // one piece nested, the other has a SKU and is only pooled (waiting for its sheet)
  { rid: '4170000006', buyer: 'Rita Vale', tag: 'one piece still waiting (pooled)', lines: [{ sku: 'STAR_DISC', metal: 'gold', copies: ['sh-gf2'] }, { sku: 'WAVE_CHARM', metal: 'silver', copies: [null], state: 'pooled' }] },
  // a cancelled piece: one piece cancelled (off its sheet), the other nested
  { rid: '4170000007', buyer: 'Tom Hale', tag: 'one piece cancelled', lines: [{ sku: 'ANCHOR', metal: 'gold', copies: ['sh-gf2'] }, { sku: 'SHELL', metal: 'silver', copies: [null], state: 'gone', reason: 'cancelled by Test Operator' }] },
  // controls: a single-piece order on a sheet, and a two-piece order with nothing nested yet
  { rid: '4170000008', buyer: 'Una Park', tag: 'single piece', lines: [{ sku: 'SUN_CHARM', metal: 'gold', copies: ['sh-gf1'] }] },
  { rid: '4170000009', buyer: 'Vik Shah', tag: 'nothing nested yet', lines: [{ sku: 'FERN_LEAF', metal: 'gold', copies: [null], state: 'pooled' }, { sku: 'OAK_LEAF', metal: 'gold', copies: [null], state: 'pooled' }] },
];

/** Every piece (one per copy) with where it is. truth.pieces(order).sheetId null = on no sheet. */
function build() {
  const orders = ORDERS.map(o => {
    const lines = o.lines.map((l, i) => {
      const tid = '41771' + o.rid.slice(-3) + (i + 1);   // unique per line
      const qty = l.qty || 1, state = l.state || (l.copies.some(Boolean) ? 'written' : 'pooled');
      const key = `${o.rid}_${tid}`, copies = Array.from({ length: qty }, (_, c) => {
        const sheet = (l.copies[c] !== undefined ? l.copies[c] : l.copies[0]) || null, s = sheet ? SHEET[sheet] : null;
        return { copy: c + 1, poolId: l.sku && state !== 'unmatched' ? `${key}_${c + 1}` : null, sheetId: state === 'gone' ? null : sheet, sheetLabel: state === 'gone' || !s ? null : sheetLabel(s) };
      });
      return Object.assign({}, l, { tid, qty, state, key, index: i + 1, copies, title: l.title || (l.sku ? l.sku.replace(/_/g, ' ') + ' charm' : ''), label: l.sku ? l.sku.replace(/_/g, ' ') : '' });
    });
    return Object.assign({}, o, { lines });
  });
  return orders;
}
const FIX = build(), byRid = Object.fromEntries(FIX.map(o => [o.rid, o]));
/** The oracle: what is true about an order's pieces. */
const truth = {
  order: rid => byRid[rid],
  pieces: rid => byRid[rid].lines.flatMap(l => l.copies.map(c => ({ key: l.key, index: l.index, label: l.label, sku: l.sku, metal: l.metal, copy: c.copy, qty: l.qty, state: l.state, poolId: c.poolId, sheetId: c.sheetId, sheetLabel: c.sheetLabel }))),
  /** The sheets that hold any live piece of the order, as labels. */
  sheets: rid => [...new Set(truth.pieces(rid).filter(p => p.sheetId).map(p => p.sheetId))].map(id => sheetLabel(SHEET[id])).sort(),
  /** The sheets of one piece (one line: all its copies). */
  sheetsOfLine: (rid, key) => [...new Set(truth.pieces(rid).filter(p => p.key === key && p.sheetId).map(p => p.sheetId))].map(id => sheetLabel(SHEET[id])).sort(),
  /** The lines still in the order (a cancelled piece is gone from the order's own views). */
  liveLines: rid => byRid[rid].lines.filter(l => l.state !== 'gone'),
  /** More than one piece still in the order (two lines, or a line of quantity 2). */
  multi: rid => truth.liveLines(rid).length > 1 || truth.liveLines(rid).some(l => l.qty > 1),
  /** Orders with a piece on this sheet. */
  ordersOn: sheetId => FIX.filter(o => truth.pieces(o.rid).some(p => p.sheetId === sheetId)).map(o => o.rid),
  /** Other sheets an order has pieces on, seen from `sheetId`. */
  otherSheets: (rid, sheetId) => [...new Set(truth.pieces(rid).filter(p => p.sheetId && p.sheetId !== sheetId).map(p => p.sheetId))].map(id => sheetLabel(SHEET[id])).sort(),
  /** Pieces of the order that are NOT on any sheet (cancelled ones excluded: they are off by design). */
  unnested: rid => truth.pieces(rid).filter(p => !p.sheetId && p.state !== 'gone'),
};

/* ═══════════════════════════ seeding the fake backend ═══════════════════════════ */
const fileBase = s => `${CODE[s.metal]}_${DAYSTR}_Set-${SETS.find(x => x.id === s.set).seq}_Sheet-${s.n}`;
/** The stores can disagree about where a piece is (a save that lags): the truth never changes, the screens must still say it.
 *   ok    every store agrees
 *   pool  the pool records of the SS sheet's pieces were not updated yet (no sheetId), sheet and set records are right
 *   sheet the SS sheet's saved record lists no poolIds for them, pool and set records are right
 *   set   the set records hold no order lines (written after the sheets), pool and sheet records are right */
const VARIANTS = ['ok', 'pool', 'sheet', 'set'];
const lagged = (variant, sheetId) => (variant === 'pool' || variant === 'sheet') && sheetId === 'sh-ss1';
function seedServer(srv, variant = 'ok') {
  const { st } = srv, ts = { toMillis: () => NOW };
  const box = (id, i, w = 30) => ({ id, cxPt: 24 + (i % 8) * 34, cyPt: 24 + Math.floor(i / 8) * 34, angle: 0, wPt: w, hPt: w });
  const lines = {};   // the run record: one entry per line, as the Library's readiness reads it
  for (const o of FIX) for (const l of o.lines) lines[l.key] = { orderId: o.rid, transactionId: l.tid, sku: l.sku, state: l.state === 'written' ? 'written' : l.state, quantity: l.qty, material: l.metal, poolIds: l.copies.map(c => c.poolId).filter(Boolean), engraveCandidate: false, noDesign: false };
  st.put('Charm_Nest_Runs', 'run-multi', { runId: 'run-multi', lines });
  for (const s of SHEETS) {
    const pieces = FIX.flatMap(o => o.lines.flatMap(l => l.copies.filter(c => c.sheetId === s.id).map(c => ({ o, l, c })))), orderIds = [...new Set(pieces.map(x => x.o.rid))];
    const placements = pieces.map((x, i) => box('c' + i, i)), charms = pieces.map((x, i) => ({ id: 'c' + i, name: `${x.o.rid} · ${x.l.sku}${x.l.qty > 1 ? ` · ${x.c.copy}/${x.l.qty}` : ''}`, poolId: x.c.poolId, order: x.o.rid, sku: x.l.sku }));
    const outUrl = `${srv.sorterOrigin}/${s.id}`;
    st.put('Charm_Nest_Sheets', s.id, { id: s.id, setId: s.set, setSeq: SETS.find(x => x.id === s.set).seq, sheetIndex: s.n, runId: 'run-multi', metal: s.metal, day: DAYSTR, fileBase: fileBase(s), folder: fileBase(s), status: 'complete', placedCount: pieces.length, charmCount: pieces.length,
      density: .6, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, placements, charms, poolIds: variant === 'sheet' && s.id === 'sh-ss1' ? [] : pieces.map(x => x.c.poolId), orders: orderIds, verification: { ok: true }, outputs: { ai: { path: s.id + '.ai', url: outUrl + '.ai' }, preview: { path: s.id + '.png', url: outUrl + '.png' } },
      label: { files: [{ path: s.id + '-qr.png', url: outUrl + '-qr.png', payload: orderIds.join(','), orders: orderIds }], orders: orderIds }, updatedAt: ts, createdAt: ts });
    for (const x of pieces) st.put('Charm_Pool', x.c.poolId, Object.assign({ poolId: x.c.poolId, orderId: x.o.rid, transactionId: x.l.tid, lineKey: x.l.key, sku: x.l.sku, material: x.l.metal, copy: x.c.copy, quantity: x.l.qty, runId: 'run-multi', createdAt: NOW - 3600e3, updatedAt: NOW }, variant === 'pool' && lagged(variant, s.id) ? { state: 'ready', sheetId: null } : { state: 'written', sheetId: s.id, sheetName: fileBase(s), setId: s.set }));
  }
  // pieces with a SKU that wait for a sheet (pooled), and cancelled pieces (taken off their sheet)
  for (const o of FIX) for (const l of o.lines) for (const c of l.copies) if (c.poolId && !c.sheetId) st.put('Charm_Pool', c.poolId, { poolId: c.poolId, orderId: o.rid, transactionId: l.tid, lineKey: l.key, sku: l.sku, material: l.metal, copy: c.copy, quantity: l.qty, state: l.state === 'gone' ? 'abandoned' : 'ready', sheetId: null, runId: 'run-multi', createdAt: NOW - 3600e3, updatedAt: NOW, ...(l.state === 'gone' ? { removedAt: NOW - 600e3, removedBy: 'Test Operator', removedReason: 'cancelled' } : {}) });
  for (const set of SETS) {
    const sheets = SHEETS.filter(s => s.set === set.id), orders = {};
    for (const o of FIX) for (const l of o.lines) for (const c of l.copies) if (c.sheetId && SHEET[c.sheetId].set === set.id) {
      const e = orders[o.rid] = orders[o.rid] || { held: null, lines: [] }; let ln = e.lines.find(x => x.transactionId === l.tid); if (!ln) e.lines.push(ln = { transactionId: l.tid, sku: l.sku, copies: [] });
      ln.copies.push({ copy: c.copy, sheetId: c.sheetId, sheet: fileBase(SHEET[c.sheetId]), poolId: c.poolId, backPoolId: null });
    }
    st.put('Charm_Nest_Sets', set.id, { setId: set.id, seq: set.seq, day: DAYSTR, runId: 'run-multi', sheetIds: sheets.map(s => s.id), materials: [...new Set(sheets.map(s => s.metal))], orders: variant === 'set' ? {} : orders, labelFiles: [], status: 'labelled', updatedAt: ts, createdAt: ts });
  }
  // the cancel record of the cancelled piece's order is NOT written: a piece cancelled on its own is a taken-off piece, the order lives on
}

/* ═══════════════════════════ the page: boot, rows, live pages ═══════════════════════════ */
const lineRow = (o, l) => ({ transactionId: l.tid, listingId: '18000' + l.tid.slice(-5), sku: l.sku, title: l.title, quantity: l.qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: METAL_LABEL[l.metal] }], metalKey: l.metal, metalLabel: METAL_LABEL[l.metal], personalization: [], buyerMessage: '' });
const orderObj = o => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: o.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: o.lines.map(l => lineRow(o, l)) });

/** modes: live (the run's own pages + pool rows + pulled rows) · pool (pool rows + pulled rows) · rec (pulled rows only) · recall (what the app does by
 *  itself on opening: the last run read from its record, its rows built from the record, no pool rows in this browser) · out (the order is not in the
 *  page's rows at all: only the records know it; opened with OrderWin.openOrder, as a search or a charm on a sheet does) */
const MODES = { live: { pull: true, pool: true, live: true }, pool: { pull: true, pool: true, live: false }, rec: { pull: true, pool: false, live: false }, recall: { pull: true, pool: false, live: false, keep: true }, out: { pull: false, pool: false, live: false, keep: true, drop: true } };
async function boot(srv, chromium, { mode, variant = 'ok', browser: shared = null }) {
  const browser = shared || await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1500, height: 960 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  // the order timeline as the cloud answers it: recorded events plus the ones DERIVED from the pool, sheets and sets (the bridge-server's own
  // timelineGet answers recorded events alone), so "placed on GF Sheet 1" reaches the pieces' rail the way it does live
  await context.route(/\/\.netlify\/functions\/charmNestLibrary/, async route => {
    const req = route.request(); let body = {}; try { body = JSON.parse(req.postData() || '{}'); } catch (_) {}
    if (req.method() !== 'POST' || body.op !== 'timelineGet') return route.fallback();
    try {
      const out = await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: req.postData(), queryStringParameters: {} });
      await route.fulfill({ status: out.statusCode || 200, headers: Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*' }, out.headers || {}), body: out.body || '{}' });
    } catch (e) { await route.fulfill({ status: 500, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: String(e.message) }) }); }
  });
  await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push(e.message); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
  // the app opens on the last run by itself, a moment after the cloud answers (Recall.open, then the run's lines as rows): let that finish, or it lands over
  // the rows put in below; the modes that stage their own rows then put the run down, as its Done does
  await page.waitForFunction(() => window.Recall && Recall.on() && Orders.rows().length > 0, null, { timeout: 20000 }).catch(() => {});
  await page.waitForFunction(() => { const n = Orders.rows().length; if (window.__n === n) return (window.__q = (window.__q || 0) + 1) > 5; window.__n = n; window.__q = 0; return false; }, null, { polling: 120, timeout: 8000 }).catch(() => {});
  // the rows of the pull and (mode 'live') the sorter's own pages: the three ways the app can know where a piece is
  await page.evaluate(async ({ orders, mode, sheets, setIdOf, M, variant }) => {
    await Orders.loadMaps(true);
    if (M.drop) { B.orders.rows = []; B.orders.byKey = new Map(); }
    else if (!M.keep) RunCtl.clearRunState();
    // the library knows these SKUs (a SKU not in a master would raise its own decision on every card)
    for (const { lines } of orders) for (const l of lines) if (l.sku) B.master.entries.set(l.sku.toUpperCase(), { sku: l.sku.toUpperCase(), aiPath: 'masters/' + l.sku + '.ai', form: null });
    if (M.pull && !M.keep) for (const { order, lines } of orders) lines.forEach((l, i) => {
      const line = order.lines[i], key = CharmNestOrders.lineKey(order, line);
      const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: l.state === 'written' ? 'written' : l.state, reason: l.reason || null, claimedBy: null, poolIds: l.copies.map(c => c.poolId).filter(Boolean), engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(key, row);
      if (M.pool) for (const c of l.copies) if (c.poolId) B.pool.rows.set(c.poolId, { poolId: c.poolId, orderId: order.receiptId, lineKey: key, sku: l.sku, material: l.metal, copy: c.copy, quantity: l.qty, state: c.sheetId && !(variant === 'pool' && c.sheetId === 'sh-ss1') ? 'written' : l.state === 'gone' ? 'abandoned' : 'ready', sheetId: c.sheetId && !(variant === 'pool' && c.sheetId === 'sh-ss1') ? c.sheetId : null, sheetName: c.sheetId && !(variant === 'pool' && c.sheetId === 'sh-ss1') ? sheets.find(s => s.id === c.sheetId).fileBase : null, setId: c.sheetId && !(variant === 'pool' && c.sheetId === 'sh-ss1') ? setIdOf[c.sheetId] : null });
    });
    Orders.interpretAll();
    if (M.live) for (const s of sheets) {
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [30, 0]], ['l', [30, 30]], ['l', [0, 30]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25 };
      const charms = s.pieces.map((p, i) => ({ id: 'c' + i, name: `${p.rid} · ${p.sku}`, poolId: p.poolId, order: p.rid, centerPt: [15, 15], widthPt: 30, heightPt: 30, outline: sq, members: [] }));
      S.sheets[s.metal].pages.push({ sheetId: s.id, metal: s.metal, sheetIndex: s.n, page: s.n, status: 'complete', runId: 'run-multi', setId: s.set, fileBase: s.fileBase, placements: s.pieces.map((p, i) => ({ id: 'c' + i, cxPt: 24 + (i % 8) * 34, cyPt: 24 + Math.floor(i / 8) * 34, angle: 0, wPt: 30, hPt: 30 })), charms, backPool: [] });
    }
    CN.setMode('orders'); Orders.render();
  }, {
    orders: FIX.map(o => ({ order: orderObj(o), lines: o.lines.map(l => ({ state: l.state, reason: l.reason || null, sku: l.sku, metal: l.metal, qty: l.qty, copies: l.copies })) })), mode,
    sheets: SHEETS.map(s => ({ id: s.id, metal: s.metal, n: s.n, set: s.set, fileBase: fileBase(s), pieces: FIX.flatMap(o => o.lines.flatMap(l => l.copies.filter(c => c.sheetId === s.id).map(c => ({ rid: o.rid, sku: l.sku, poolId: c.poolId })))) })),
    setIdOf: Object.fromEntries(SHEETS.map(s => [s.id, s.set])), M: MODES[mode], variant,
  });
  return { browser, context, page, errors, shared: !!shared };
}


/* ═══════════════════════════ driving the page ═══════════════════════════ */
const sleep = ms => new Promise(r => setTimeout(r, ms));
/** One finding: surface id (the inventory's), where it was seen from, what was expected and what the screen said. */
class Report {
  constructor() { this.rows = []; this.ctx = ''; }
  at(ctx) { this.ctx = ctx; return this; }
  check(surface, side, ok, detail) { this.rows.push({ surface, ctx: this.ctx, side, ok: !!ok, detail: ok ? '' : String(detail) }); return !!ok; }
  eq(surface, side, got, want, what) { const g = JSON.stringify(got), w = JSON.stringify(want); return this.check(surface, side, g === w, `${what}: the screen says ${g}, the truth is ${w}`); }
  summary(only) {
    const by = new Map();
    for (const r of this.rows) { if (only && !only.has(r.surface)) continue; const e = by.get(r.surface) || { pass: 0, fail: [] }; r.ok ? e.pass++ : e.fail.push(r); by.set(r.surface, e); }
    return by;
  }
}
const pieceName = l => `${l.label || l.title || 'no SKU'} (${CODE[l.metal]})`;
/** Reads `fn` until it answers the same twice (and `ready` agrees), so a screen is read once it has stopped moving. */
async function settle(page, fn, arg, { max = 6000, quiet = 220, ready = null } = {}) {
  const t0 = Date.now(); let last = null, since = Date.now(), val = null;
  while (Date.now() - t0 < max) {
    val = await page.evaluate(fn, arg).catch(() => null);
    const j = JSON.stringify(val);
    if (j !== last) { last = j; since = Date.now(); } else if (Date.now() - since >= quiet && (!ready || ready(val))) return val;
    await sleep(60);
  }
  return val;
}
async function closeAll(page) {
  await page.evaluate(async () => { try { if (window.SheetWin && SheetWin.isOpen && SheetWin.isOpen()) await SheetWin.close(); } catch (_) {} try { if (OrderWin.isOpen()) await OrderWin.close(); } catch (_) {} });
  await page.waitForFunction(() => !OrderWin.isOpen() && !(window.SheetWin && SheetWin.isOpen && SheetWin.isOpen()) && !document.querySelector('dialog[open]'), null, { timeout: 6000 }).catch(async () => { await page.keyboard.press('Escape'); await sleep(300); });
}
/** The order window on one piece of an order, as a person gets there: from the Orders list (the piece's own row) when the order is in the pull, else by the
 *  order's number (a search, the Library, a charm on a sheet) and then the piece's button in the switcher. */
async function openPiece(page, mode, o, l) {
  await closeAll(page);
  if (MODES[mode].pull) await page.evaluate(k => OrderWin.open(k), l.key);
  else await page.evaluate(rid => OrderWin.openOrder(rid), o.rid);
  await page.waitForFunction(rid => OrderWin.isOpen() && document.getElementById('owTitle')?.dataset.rid === rid && document.getElementById('owLoading')?.hidden !== false, o.rid, { timeout: 15000 });
  const live = truth.liveLines(o.rid);
  if (live.length > 1 || live.some(x => x.qty > 1)) {
    await page.waitForFunction(n => document.querySelectorAll('#owPieceSw [data-piece]:not([data-piece=""])').length >= n, live.length, { timeout: 8000 }).catch(() => {});
    await page.evaluate(k => { const b = document.querySelector(`#owPieceSw [data-piece="${k}"]`); if (b) b.click(); }, l.key);
  }
  await settle(page, () => ({ n: document.getElementById('owTlCount')?.textContent || '', now: document.getElementById('owNow')?.textContent || '' }), null, { max: 5000, quiet: 250, ready: v => /\d/.test(v && v.n) });
}
const OV = () => ({
  piece: [...document.querySelectorAll('#owPieceSw [data-piece]')].filter(b => b.classList.contains('on')).map(b => b.dataset.piece || '*'),
  pieces: [...document.querySelectorAll('#owPieceSw [data-piece]')].map(b => b.dataset.piece || '*'),
  swHidden: document.getElementById('owPieceSw')?.hidden !== false,
  chips: [...document.querySelectorAll('#owNowCard .owShChip')].map(b => (b.querySelector('b')?.textContent || '').trim()),
  sheetBtn: (b => b ? { disabled: !!b.disabled || b.getAttribute('aria-disabled') === 'true', title: b.title || '' } : null)(document.querySelector('#owNowCard [data-go="sheet"]')),
  sum: [...document.querySelectorAll('#owPcSum .owPcRow')].map(b => ({ key: b.dataset.piece, st: (b.querySelector('.st')?.textContent || '').trim() })),
  now: (document.getElementById('owNow')?.textContent || '').trim(),
  metaSheet: (() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => (x.querySelector('i')?.textContent || '').trim().toLowerCase() === 'sheet'); return m ? (m.querySelector('span')?.textContent || '').trim() : null; })(),
});
const SHEETTAB = () => ({
  chips: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => ({ label: b.textContent.trim(), on: b.classList.contains('on') })),
  rows: [...document.querySelectorAll('#owSheetPanel .owPieces li')].map(li => ({ sku: (li.querySelector('.sku')?.textContent || '').replace(/copy \d+ of \d+/, '').trim(), where: (li.querySelector('em')?.textContent || '').trim() })),
  head: [...document.querySelectorAll('#owSheetPanel .fLabel')].map(x => x.textContent.trim()).find(t => /This order/.test(t)) || '',
  full: (b => b ? { disabled: !!b.disabled } : null)(document.querySelector('#owSheetPanel [data-full]')),
  off: (b => b ? { disabled: !!b.disabled } : null)(document.querySelector('#owSheetPanel [data-off]')),
  none: (document.querySelector('#owPlateWrap .owPlateNone')?.innerText || '').replace(/\s+/g, ' ').trim(),
  sheetId: (() => { const i = OrderWin._sheet(); return i && i.sheet ? i.sheet.id : null; })(),
  waiting: document.getElementById('owPlateWait')?.hidden === false,
});
const sheetTabReady = v => v && !v.waiting && (v.none || (v.rows.length > 0 && v.chips.length > 0 && v.sheetId));
const readSheetTab = (page, max) => settle(page, SHEETTAB, null, { max: max || 9000, quiet: 280, ready: sheetTabReady });
/** Which label a raw "set-1 · GF_2026-10-05_Set-1_Sheet-1" / "GF Sheet 1" names. */
const norm = t => { const m = /(GF|SS|RG|10K|14K)[ _-]*Sheet[ _-]*(\d+)/i.exec(String(t || '')) || /^(GF|SS|RG|10K|14K)_.*_Sheet-(\d+)$/i.exec(String(t || '').replace(/^.*·\s*/, '')); return m ? `${m[1].toUpperCase()} Sheet ${m[2]}` : String(t || '').trim(); };
const chipId = label => SHEETS.find(s => sheetLabel(s) === label)?.id;

/* ═══════════════════════════ the surfaces ═══════════════════════════ */
/** A and E, the order window: every piece of every order as the side the window is opened from. */
async function orderWindowProbes(page, mode, R, orders) {
  for (const o of orders) {
    const live = truth.liveLines(o.rid), multi = truth.multi(o.rid), all = truth.sheets(o.rid);
    for (const l of o.lines) {
      const side = `${o.rid} from ${pieceName(l)}`, mine = truth.sheetsOfLine(o.rid, l.key), here = l.state !== 'gone';
      if (!here) continue;   // a cancelled piece is no longer in the order the window opens (the other probes cover the Cancelled list)
      await openPiece(page, mode, o, l);
      let ov = await settle(page, OV, null, { max: 2500, quiet: 200 });
      if (live.length > 1 && here) R.check('A2', side, ov.piece.length === 1 && ov.piece[0] === l.key, `the switcher shows ${JSON.stringify(ov.piece)} chosen, not ${l.key}`);
      // A8 / E2 "Where it is now": every piece on its own row, saying the sheet it is on
      if (live.length > 1 && here) {
        await page.evaluate(() => document.querySelector('#owPieceSw [data-piece=""]')?.click());
        const sum = await settle(page, OV, null, { max: 2500, quiet: 250, ready: v => v.sum.length > 0 });
        for (const p of live) {
          const row = sum.sum.find(x => x.key === p.key), want = truth.sheetsOfLine(o.rid, p.key), onWord = row && /^On (GF|SS|RG|10K|14K) Sheet \d+/.exec(row.st);
          if (!row) { R.check('A8', `${o.rid} all pieces, ${pieceName(p)}`, false, `no row for ${pieceName(p)} in "Where it is now"`); continue; }
          if (want.length) R.check('A8', `${o.rid} all pieces, ${pieceName(p)} (seen from ${pieceName(l)})`, !!onWord && want.includes(norm(row.st)), `${pieceName(p)} is on ${want.join(' + ')} but "Where it is now" says "${row.st}"`);
          else R.check('A8', `${o.rid} all pieces, ${pieceName(p)} (seen from ${pieceName(l)})`, !onWord, `${pieceName(p)} is on no sheet but "Where it is now" says "${row.st}"`);
        }
        await page.evaluate(k => document.querySelector(`#owPieceSw [data-piece="${k}"]`)?.click(), l.key);
        ov = await settle(page, OV, null, { max: 2500, quiet: 200 });
      }
      // A6: the Sheet cell of the Overview grid
      if (here) {
        if (mine.length) R.check('A6', side, ov.metaSheet != null && mine.includes(norm(ov.metaSheet)), `the piece is on ${mine.join(' + ')} but the Overview's Sheet cell says ${JSON.stringify(ov.metaSheet)}`);
        else R.check('A6', side, ov.metaSheet == null || !/Sheet/i.test(ov.metaSheet), `the piece is on no sheet but the Overview's Sheet cell says ${JSON.stringify(ov.metaSheet)}`);
      }
      // A4: the "Sheet" button opens THIS piece's sheet; a piece on no sheet has no pressable button (greyed out), it never opens the other piece's sheet
      const btn = ov.sheetBtn;
      if (here && !mine.length && multi) R.check('A4', side, !btn || btn.disabled, `the piece is on no sheet yet and its "Sheet" button can be pressed`);
      if (btn && !btn.disabled) await page.evaluate(() => document.querySelector('#owNowCard [data-go="sheet"]').click()); else await page.evaluate(() => OrderWin.setView('sheet'));
      let tab = await readSheetTab(page);
      if (btn && !btn.disabled && here) {
        const on = tab.chips.find(c => c.on);
        if (mine.length) R.check('A4', side, !!on && mine.includes(on.label), `its "Sheet" button opens ${on ? on.label : JSON.stringify(tab.none)} (the chip lit), the piece is on ${mine.join(' + ')}`);
        else R.check('A4', side, !!tab.none && !tab.chips.length, `a piece on no sheet presses "Sheet" and gets ${JSON.stringify(tab.chips.map(c => c.label))}${tab.none ? '' : ' with a sheet drawn'}`);
      }
      // A3: the sheet chips on the Overview, now that the order's sheets are known: this piece's sheets only
      if (here && live.length > 1) {
        await page.evaluate(() => OrderWin.setView('info'));
        const o2 = await settle(page, OV, null, { max: 2500, quiet: 250 });
        R.eq('A3', side, o2.chips.map(norm).sort(), mine, 'the sheet chips on the Overview');
        await page.evaluate(() => OrderWin.setView('sheet')); tab = await readSheetTab(page);
      }
      // ── Sheet tab: every chip in turn, every piece on its row ──
      if (!all.length) {
        if (here) R.check('A13', side, /not on a sheet yet/i.test(tab.none) && !tab.chips.length, `no piece of this order is on a sheet and the Sheet tab says ${JSON.stringify(tab.none || tab.chips.map(c => c.label))}`);
      } else {
        R.eq('A10', side, tab.chips.map(c => c.label).sort(), all, 'the sheet chips');
        for (let i = 0; i < tab.chips.length; i++) {
          if (!tab.chips[i].on) { await page.evaluate(i => document.querySelector(`#owSheetPanel .owShTabs button[data-at="${i}"]`)?.click(), i); tab = await settle(page, SHEETTAB, null, { max: 9000, quiet: 280, ready: v => sheetTabReady(v) && v.chips[i] && v.chips[i].on }); }
          const cur = tab.chips[i].label, curId = chipId(cur);
          const rows = tab.rows.map(r => `${r.sku}|${r.where}`).sort(), pieces = truth.pieces(o.rid).filter(p => p.state !== 'gone');
          const want = pieces.map(p => `${p.sku}|${p.sheetId === curId ? 'this sheet' : p.sheetLabel || 'not on a sheet yet'}`).sort();
          R.check('A11', `${side}, ${cur} selected`, JSON.stringify(rows) === JSON.stringify(want), `the piece list (${tab.head}) says ${JSON.stringify(rows)}, the truth is ${JSON.stringify(want)}`);
          R.check('A12', `${side}, ${cur} selected`, !!tab.full && !tab.full.disabled && !!tab.off, `Open full sheet / Take off the sheet… are not both there: ${JSON.stringify([tab.full, tab.off])}`);
        }
        // A12: Open full sheet opens the sheet that is selected
        const selLabel = tab.chips.find(c => c.on)?.label;
        await page.evaluate(() => document.querySelector('#owSheetPanel [data-full]')?.click());
        const opened = await page.waitForFunction(() => window.SheetWin && SheetWin.isOpen() && SheetWin.current(), null, { timeout: 8000 }).then(h => h.jsonValue(), () => null);
        R.check('A12', `${side}, Open full sheet`, opened === chipId(selLabel), `Open full sheet on ${selLabel} opened ${opened}`);
        if (opened) { await page.evaluate(() => SheetWin.close()); await page.waitForFunction(() => document.getElementById('orderWin').open, null, { timeout: 6000 }).catch(() => {}); await sleep(200); }
      }
      // ── the rail: the Nested step is reached for a piece on a sheet and not for one on none ──
      if (here && live.length > 1) {
        await page.evaluate(() => OrderWin.setView('timeline'));
        const rail = await settle(page, () => ({ text: (document.querySelector('#owRail, #orderWin .tlRail')?.innerText || '').replace(/\s+/g, ' ') }), null, { max: 2500, quiet: 300 });
        const reached = /ON SHEET\s+\d{1,2}\s+[A-Z]{3}/i.test(rail.text);
        R.check('E2', side, reached === (mine.length > 0), `the piece is ${mine.length ? 'on ' + mine.join(' + ') : 'on no sheet'} and the rail ${reached ? 'shows' : 'does not show'} it nested (${JSON.stringify(rail.text.slice(0, 120))})`);
      }
    }
  }
}

/* ═══════════════════════════ runner ═══════════════════════════ */
const SURFACES = {
  A2: 'Order window · piece switcher: the piece chosen is the piece shown',
  A3: 'Order window · Overview "Where it is now" card: sheet chips of the piece chosen',
  A4: 'Order window · Overview "Sheet" button: opens this piece\'s sheet; greyed out for a piece on no sheet',
  A6: 'Order window · Overview grid "Sheet" cell',
  A8: 'Order window · "Where it is now" per piece (all pieces)',
  A10: 'Order window · Sheet tab: the sheet chips',
  A11: 'Order window · Sheet tab: "This order · N pieces" list, for each sheet chip',
  A12: 'Order window · Sheet tab: Open full sheet / Take off the sheet…',
  A13: 'Order window · Sheet tab: state of an order with no piece on a sheet',
  E2: 'Timeline · the rail reaches Nested for a piece on a sheet, and only then',
};
function parseArgs() {
  const a = process.argv.slice(2), o = { list: a.includes('--list'), dump: a.includes('--dump'), quick: a.includes('--quick'), only: null, variants: null, modes: null, orders: null };
  const val = f => { const i = a.indexOf(f); return i >= 0 ? a[i + 1] : null; };
  if (val('--only')) o.only = new Set(val('--only').split(','));
  if (val('--variants')) o.variants = val('--variants').split(',');
  if (val('--modes')) o.modes = val('--modes').split(',');
  if (val('--orders')) o.orders = val('--orders').split(',');
  return o;
}
async function runOne(chromium, variant, mode, R, args, SHARDS) {
  const srv = await start({ receipts: [] }); seedServer(srv, variant);
  const t0 = Date.now(), ctx = `${variant}/${mode}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const orders = FIX.filter(o => !args.orders || args.orders.includes(o.rid)), shards = Array.from({ length: Math.min(SHARDS, orders.length) }, () => []);
    orders.forEach((o, i) => shards[i % shards.length].push(o));
    await Promise.all(shards.map(async (list, i) => {
      const rep = new Report().at(ctx); let session;
      try {
        session = await boot(srv, chromium, { mode, variant, browser });
        await orderWindowProbes(session.page, mode, rep, list);
        if (session.errors.length) rep.check('PAGE', 'page errors', false, session.errors.slice(0, 3).join(' | '));
      } catch (e) { rep.check('RUN', `${ctx} shard ${i}`, false, e.stack || e.message); }
      finally { try { await session?.context.close(); } catch (_) {} }
      R.rows.push(...rep.rows);
    }));
  } finally { await browser.close(); srv.close(); }
  return Date.now() - t0;
}
async function main() {
  const args = parseArgs(), pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core (set PW_DIR): the browser checks were not run'); return 0; }
  const R = new Report();
  // the matrix: every way the app can know where a piece is, and every store lagging behind the others
  const matrix = args.quick ? [['ok', 'live'], ['ok', 'recall'], ['ok', 'out']] : [['ok', 'live'], ['ok', 'pool'], ['ok', 'rec'], ['ok', 'recall'], ['ok', 'out'], ['pool', 'live'], ['pool', 'recall'], ['pool', 'out'], ['sheet', 'live'], ['sheet', 'recall'], ['sheet', 'out'], ['set', 'live'], ['set', 'recall'], ['set', 'out']];
  const jobs = matrix.filter(([v, m]) => (!args.variants || args.variants.includes(v)) && (!args.modes || args.modes.includes(m)));
  const t0 = Date.now(), CONC = +process.env.CONC || 2, SHARDS = +process.env.SHARDS || 2; let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONC, jobs.length) }, async () => { while (next < jobs.length) { const [v, m] = jobs[next++]; const ms = await runOne(chromium, v, m, R, args, SHARDS); console.log(`  · ${v}/${m} ${(ms / 1000).toFixed(0)} s`); } }));
  const by = R.summary(args.only); let bad = 0;
  console.log(`\n${'surface'.padEnd(6)} ${'result'.padEnd(11)} what`);
  for (const [id, e] of [...by].sort((a, b) => a[0].localeCompare(b[0], undefined, { numeric: true }))) {
    bad += e.fail.length ? 1 : 0;
    console.log(`${id.padEnd(6)} ${(e.fail.length ? `FAIL ${e.fail.length}/${e.fail.length + e.pass}` : `ok ${e.pass}`).padEnd(11)} ${SURFACES[id] || ''}`);
    const ctxs = [...new Set(e.fail.map(f => f.ctx))].sort();
    if (e.fail.length) console.log(`         wrong in: ${ctxs.join(', ')}`);
    for (const f of e.fail.slice(0, args.dump ? 999 : 3)) console.log(`         [${f.ctx}] ${f.side}: ${f.detail}`);
    if (e.fail.length > 3 && !args.dump) console.log(`         … ${e.fail.length - 3} more (--dump lists them all)`);
  }
  console.log(`\n${((Date.now() - t0) / 1000).toFixed(0)} s · ${bad ? bad + ' surface(s) wrong' : 'every surface tells the same truth'}`);
  return args.list ? 0 : bad ? 1 : 0;
}
module.exports = { MODES, VARIANTS, FIX, SHEETS, SETS, SHEET, truth, sheetLabel, seedServer, boot, ORDERS, Report, settle, norm };
if (require.main === module) main().then(c => process.exit(c), e => { console.error(e); process.exit(1); });
