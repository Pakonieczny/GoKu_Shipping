// ONE offline end-to-end harness for Paul's round-2 point 1: "when an order has 2+ charms in two different Sheets ... the Silver
// Sheet says 'Charm not on sheet yet' which is incorrect ... There is a breakdown in how sheets retain and associate orders
// which have multiple charms spread across several sheets. This must be fixed and thoroughly tested from all side and all
// possible screens, modals, tabs."
//
// It seeds ONE set of multi-piece orders into a fake backend (bridge-server.cjs: the real charmNestLibrary handler over an
// in-memory Firestore) and into the page, then opens EVERY surface that shows or computes "which sheet holds which piece of
// an order" (the inventory and the final table: /mnt/project-files/plans/library-flow/surfaces-inventory.md and order-assoc-final-surfaces.md;
// ids: A order window, B sheet window, C Library, D lists and search, E timeline, G server, H OrderPieces, K SharedOrders and its window) from every side
// (opened from each piece, from each sheet) and compares what each one says with the ground truth computed from the fixture.
//
//   node tests/charm-nest/multi-sheet-order-e2e.cjs            all surfaces, exit 1 on any wrong surface
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --list     print the failing surfaces and exit 0 (to record a baseline)
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --only A3,B1   only those surface ids
//   node tests/charm-nest/multi-sheet-order-e2e.cjs --dump     print every wrong screen, not the first three (--json <file> saves them all)
//   --modes live,pool,rec,recall,out   --variants ok,pool,sheet,set,state,stale   --orders 4170000003   (narrow the matrix; --quick = a short one)
//   The full matrix is 20 page-state/store combinations and takes 20 to 35 minutes on a loaded machine (CONC=3 SHARDS=1); under heavy load a hover or a click can
//   miss its window: rerun the wrong combination alone before believing a red.
//   Cases a record's own list decides (a sheet record that lists no poolIds, one that still lists a left order) are printed as "known, by design" (KNOWN below), never counted.
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
 *   set   the set records hold no order lines (written after the sheets), pool and sheet records are right
 *   stale the SS sheet's saved record still lists an order (4170000008) in `orders` whose only piece left it (no pool id of it, no charm of it)
 *   state the order lines still say "pooled" (a line stays pooled in the run record until its set is written) although every record of the sheets is right */
const VARIANTS = ['ok', 'pool', 'sheet', 'set', 'state', 'stale'];
const lineState = (variant, l) => variant === 'state' && l.state === 'written' ? 'pooled' : l.state;
const lagged = (variant, sheetId) => (variant === 'pool' || variant === 'sheet') && sheetId === 'sh-ss1';
/** What the Order check (the server's readiness, the Library's '!' panel) can know: it reads the SAVED sheet records, and a record is the file that gets cut,
 *  so a piece its sheet's record does not list is "not on a saved sheet yet" for it, whatever the pool row or the set say. With every record complete this is
 *  truth.unnested; in variant 'sheet' the SS sheet's record lists nothing, and that is by design (see the report: the readiness is deliberately strict). */
const recordUnnested = (variant, rid) => truth.pieces(rid).filter(p => p.state !== 'gone' && (!p.sheetId || (variant === 'sheet' && lagged(variant, p.sheetId))));
function seedServer(srv, variant = 'ok') {
  const { st } = srv, ts = { toMillis: () => NOW };
  const box = (id, i, w = 30) => ({ id, cxPt: 24 + (i % 8) * 34, cyPt: 24 + Math.floor(i / 8) * 34, angle: 0, wPt: w, hPt: w });
  const lines = {};   // the run record: one entry per line, as the Library's readiness reads it
  for (const o of FIX) for (const l of o.lines) lines[l.key] = { orderId: o.rid, transactionId: l.tid, sku: l.sku, state: lineState(variant, l), quantity: l.qty, material: l.metal, poolIds: l.copies.map(c => c.poolId).filter(Boolean), engraveCandidate: false, noDesign: false };
  st.put('Charm_Nest_Runs', 'run-multi', { runId: 'run-multi', lines });
  for (const s of SHEETS) {
    const pieces = FIX.flatMap(o => o.lines.flatMap(l => l.copies.filter(c => c.sheetId === s.id).map(c => ({ o, l, c })))), orderIds = [...new Set(pieces.map(x => x.o.rid))];
    const placements = pieces.map((x, i) => box('c' + i, i)), charms = pieces.map((x, i) => ({ id: 'c' + i, name: `${x.o.rid} · ${x.l.sku}${x.l.qty > 1 ? ` · ${x.c.copy}/${x.l.qty}` : ''}`, poolId: x.c.poolId, order: x.o.rid, sku: x.l.sku }));
    const outUrl = `${srv.sorterOrigin}/${s.id}`;
    st.put('Charm_Nest_Sheets', s.id, { id: s.id, setId: s.set, setSeq: SETS.find(x => x.id === s.set).seq, sheetIndex: s.n, runId: 'run-multi', metal: s.metal, day: DAYSTR, fileBase: fileBase(s), folder: fileBase(s), status: 'complete', placedCount: pieces.length, charmCount: pieces.length,
      density: .6, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, placements, charms, poolIds: variant === 'sheet' && s.id === 'sh-ss1' ? [] : pieces.map(x => x.c.poolId), orders: variant === 'stale' && s.id === 'sh-ss1' ? [...orderIds, '4170000008'] : orderIds, verification: { ok: true }, outputs: { ai: { path: s.id + '.ai', url: outUrl + '.ai' }, preview: { path: s.id + '.png', url: outUrl + '.png' } },
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
    orders: FIX.map(o => ({ order: orderObj(o), lines: o.lines.map(l => ({ state: lineState(variant, l), reason: l.reason || null, sku: l.sku, metal: l.metal, qty: l.qty, copies: l.copies })) })), mode,
    sheets: SHEETS.map(s => ({ id: s.id, metal: s.metal, n: s.n, set: s.set, fileBase: fileBase(s), pieces: FIX.flatMap(o => o.lines.flatMap(l => l.copies.filter(c => c.sheetId === s.id).map(c => ({ rid: o.rid, sku: l.sku, poolId: c.poolId })))) })),
    setIdOf: Object.fromEntries(SHEETS.map(s => [s.id, s.set])), M: MODES[mode], variant,
  });
  return { browser, context, page, errors, shared: !!shared };
}


/* ═══════════════════════════ driving the page ═══════════════════════════ */
const sleep = ms => new Promise(r => setTimeout(r, ms));
/** One finding: surface id (the inventory's), where it was seen from, what was expected and what the screen said. */
/** Cases the harness cannot call wrong, and why: a surface that reads a saved record's own list says what the record says. Listed in the summary, never counted. */
const KNOWN = [
  { surface: 'C1', variant: 'stale', why: 'the Library card counts the orders its sheet record lists; a record that still lists an order whose piece left it is counted as it is written' },
  { surface: 'C3', variant: 'stale', why: 'the Library search finds a sheet by the orders its record lists, so a record that still lists a left order is found by it' },
  { surface: 'D3', variant: 'stale', why: 'the header search names every sheet whose record lists the order (a record written before pieces had ids says nothing else), so a record that still lists a left order is named' },
  { surface: 'G1', variant: 'stale', why: 'the QR label of the sheet does not cover the order its record lists, so the server says its sheet is not ready until the label is made again' },
];
class Report {
  constructor() { this.rows = []; this.ctx = ''; }
  at(ctx) { this.ctx = ctx; return this; }
  check(surface, side, ok, detail) {
    const k = !ok && KNOWN.find(x => x.surface === surface && this.ctx.startsWith(x.variant + '/'));
    this.rows.push({ surface, ctx: this.ctx, side, ok: !!ok, known: k ? k.why : '', detail: ok ? '' : String(detail) }); return !!ok;
  }
  eq(surface, side, got, want, what) { const g = JSON.stringify(got), w = JSON.stringify(want); return this.check(surface, side, g === w, `${what}: the screen says ${g}, the truth is ${w}`); }
  summary(only) {
    const by = new Map();
    for (const r of this.rows) { if (only && !only.has(r.surface)) continue; const e = by.get(r.surface) || { pass: 0, fail: [], known: [] }; if (r.ok) e.pass++; else if (r.known) e.known.push(r); else e.fail.push(r); by.set(r.surface, e); }
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
  chips: [...document.querySelectorAll('#owNowCard .owShChip:not(.off)')].map(b => (b.querySelector('b')?.textContent || '').trim()),
  offChips: document.querySelectorAll('#owNowCard .owShChip.off').length,
  sheetBtn: (b => b ? { disabled: !!b.disabled || b.getAttribute('aria-disabled') === 'true', title: b.title || '' } : null)(document.querySelector('#owNowCard [data-go="sheet"]')),
  sum: [...document.querySelectorAll('#owPcSum .owPcRow')].map(b => ({ key: b.dataset.piece, st: (b.querySelector('.st')?.textContent || '').trim() })),
  now: (document.getElementById('owNow')?.textContent || '').trim(),
  metaSheet: (() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => (x.querySelector('i')?.textContent || '').trim().toLowerCase() === 'sheet'); return m ? (m.querySelector('span')?.textContent || '').trim() : null; })(),
});
const SHEETTAB = () => ({
  chips: [...document.querySelectorAll('#owSheetPanel .owShTabs button')].map(b => ({ label: b.textContent.trim(), on: b.classList.contains('on'), at: b.dataset.at })),
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
      // (the Sheet tab answers for the piece picked in the switcher: that piece's own sheets, none for a piece on no sheet; with one piece in the order, for the order)
      const scoped = multi ? mine : all;
      if (!scoped.length) {
        if (here) R.check('A13', side, /not on a sheet yet/i.test(tab.none) && !tab.chips.length, `${multi && all.length ? 'this piece is' : 'no piece of this order is'} on no sheet and the Sheet tab says ${JSON.stringify(tab.none || tab.chips.map(c => c.label))}`);
      } else {
        R.eq('A10', side, tab.chips.map(c => c.label).sort(), scoped, 'the sheet chips');
        for (let i = 0; i < tab.chips.length; i++) {
          // (a tab's data-at is its place among ALL the order's sheets; the tabs shown are those of the piece)
          if (!tab.chips[i].on) { await page.evaluate(at => document.querySelector(`#owSheetPanel .owShTabs button[data-at="${at}"]`)?.click(), tab.chips[i].at); tab = await settle(page, SHEETTAB, null, { max: 9000, quiet: 280, ready: v => sheetTabReady(v) && v.chips[i] && v.chips[i].on && v.rows.some(r => /this sheet/i.test(r.where)) }); }
          const cur = tab.chips[i].label, curId = chipId(cur);
          const pieces = truth.pieces(o.rid).filter(p => p.state !== 'gone'), miss = matchRows(tab.rows.map(r => ({ sku: r.sku, kind: whereKind(r.where) })), pieces, curId);
          R.check('A11', `${side}, ${cur} selected`, !miss.length && new RegExp(`This order . ${pieces.length} piece`).test(tab.head), `the piece list (${tab.head}) says ${JSON.stringify(tab.rows.map(r => `${r.sku} ${r.where}`))}: ${miss.join('; ') || 'the count in its header is not ' + pieces.length}`);
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
        await page.evaluate(k => document.querySelector(`#owPieceSw [data-piece="${k}"]`)?.click(), l.key);
        const rail = await settle(page, () => ({ text: (document.querySelector('#owRail, #orderWin .tlRail')?.innerText || '').replace(/\s+/g, ' '), on: [...document.querySelectorAll('#owPieceSw [data-piece].on')].map(b => b.dataset.piece) }), null, { max: 6000, quiet: 400, ready: v => v && !/Loading/i.test(v.text) && v.text.length > 20 });
        const reached = /ON SHEET\s+\d{1,2}\s+[A-Z]{3}/i.test(rail.text);
        R.check('E2', side, reached === (mine.length > 0), `the piece is ${mine.length ? 'on ' + mine.join(' + ') : 'on no sheet'} and the rail ${reached ? 'shows' : 'does not show'} it nested (${JSON.stringify(rail.text.slice(0, 120))})`);
        // E3: the step explainer of Nested (hover on the header rail): done, and naming a sheet the piece is on, or not done
        const box = await page.evaluate(() => { const b = [...document.querySelectorAll('#owRail [data-stage]')].find(x => x.dataset.stage === 'sheet' && x.getBoundingClientRect().width > 0); if (!b) return null; const r = b.getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; });
        if (box) {
          let card = null;
          for (let a = 0; a < 3 && !card; a++) {   // (a hover while the rail is still settling shows nothing: move off and on again)
            await page.mouse.move(2, 2); await sleep(250); await page.mouse.move(box.x, box.y);
            card = await page.waitForFunction(() => { const e = document.querySelector('.tlExp.on'); return e && e.innerText.trim() ? e.innerText.replace(/\s+/g, ' ') : null; }, null, { timeout: 4000 }).then(h => h.jsonValue(), () => null);
          }
          const done = !!card && /\bDONE\b/.test(card), names = mine.filter(m => (card || '').includes(m));
          R.check('E3', side, !!card && done === (mine.length > 0) && (!mine.length || names.length > 0), `the piece is ${mine.length ? 'on ' + mine.join(' + ') : 'on no sheet'} and the Nested step says ${JSON.stringify((card || 'nothing').slice(0, 140))}`);
          await page.mouse.move(2, 2);
        }
      }
    }
  }
}

/* ═══════════════════════════ B · the sheet window, opened on each sheet ═══════════════════════════ */
const short = label => String(label).replace(' Sheet ', ' ');
const norm2 = t => String(t || '').replace(/\s+/g, ' ').trim();
const skuNorm = s => String(s || '').replace(/_+/g, ' ').replace(/\s+/g, ' ').trim().toUpperCase();
/** "this sheet" / "this charm" / "on this sheet" -> here, "GF Sheet 1" -> that label, anything else ("not on a sheet yet", a reason) -> none. */
const whereKind = t => { const s = String(t || ''); if (/this sheet|this charm/i.test(s)) return 'here'; const m = /(GF|SS|RG|10K|14K)\s+Sheet\s+(\d+)/i.exec(s); return m ? `${m[1].toUpperCase()} Sheet ${m[2]}` : 'none'; };
/** The rows of a piece list (sku + what it says about where) against the pieces of the order: the problems, [] when it tells the truth. The pieces with no
 *  SKU are matched by where they say they are alone (each screen has its own words for them). */
function matchRows(rows, pieces, sheetId) {
  const left = rows.map(r => ({ sku: skuNorm(r.sku), kind: r.kind })), miss = [], noSku = [];
  for (const p of pieces) {
    const kind = p.sheetId === sheetId ? 'here' : p.sheetLabel || 'none';
    if (!p.sku) { noSku.push(kind); continue; }
    const i = left.findIndex(r => r.sku === skuNorm(p.sku) && r.kind === kind);
    if (i < 0) miss.push(`${p.sku}${p.qty > 1 ? ` copy ${p.copy}` : ''} should read "${kind}"`); else left.splice(i, 1);
  }
  const lk = left.map(r => r.kind).sort(), wk = noSku.sort();
  if (JSON.stringify(lk) !== JSON.stringify(wk)) miss.push(`the rest of the list is ${JSON.stringify(left)}, the pieces with no SKU should read ${JSON.stringify(wk)}`);
  return miss;
}
const SW = () => ({
  cur: window.SheetWin && SheetWin.current ? SheetWin.current() : null,
  filters: Object.fromEntries([...document.querySelectorAll('[data-f]')].map(b => [b.dataset.f, { n: +(b.querySelector('i')?.textContent || 0), on: b.getAttribute('aria-pressed') === 'true' }])),
  rows: [...document.querySelectorAll('.swOrd[data-rid]')].map(l => ({ rid: l.dataset.rid, tags: [...l.querySelectorAll('.swTag')].map(t => t.textContent.trim()) })),
  chipsShown: !!document.querySelector('.swSheets') && !document.querySelector('.swSheets').hidden,
  chips: [...document.querySelectorAll('.swSheets .swChip')].map(b => ({ id: b.dataset.sheet, label: [...b.childNodes].filter(n => n.nodeType === 3).map(n => n.textContent).join('').trim(), cur: b.hasAttribute('aria-current'), lit: b.classList.contains('lit'), n: +(b.querySelector('b')?.textContent || 0) })),
  head: (document.querySelector('[data-r2=trailHead]')?.textContent || '').trim(),
  trail: [...document.querySelectorAll('[data-r2=trail] li[data-i]')].map(li => ({ sku: (li.querySelector('.sku')?.childNodes[0]?.textContent || '').trim(), where: (li.querySelector('.where')?.textContent || '').trim(), cur: li.classList.contains('cur') })),
});
/** Presses until `ready` holds (a press the window ignores while it is still settling is pressed again, as a person would), at most `max` ms. */
async function pressUntil(page, click, ready, arg, { max = 9000, every = 900 } = {}) {
  const t0 = Date.now();
  while (Date.now() - t0 < max) {
    await page.evaluate(click, arg).catch(() => {});
    if (await page.waitForFunction(ready, arg, { timeout: every, polling: 100 }).then(() => true, () => false)) return true;
  }
  return false;
}
const selectedOrder = rid => document.querySelector('[data-r2=openOrd]')?.title.includes(rid) && document.querySelectorAll('[data-r2=trail] li.cur').length === 1;
const swReady = v => v && v.rows.length > 0;
const swSelReady = v => v && v.trail.length > 0 && v.trail.some(r => r.cur);
async function sheetWinProbes(page, mode, R, sheets) {
  const openSheet = async id => {
    await closeAll(page); await sleep(700);   // (a person cannot press again in the tail of the window closing)
    await page.evaluate(id => SheetWin.open(id), id);
    await page.waitForFunction(id => SheetWin.isOpen() && SheetWin.current() === id, id, { timeout: 15000 }).catch(() => {});
    return settle(page, SW, null, { max: 9000, quiet: 450, ready: swReady });
  };
  for (const S of sheets) {
    const rids = truth.ordersOn(S.id), at = sheetLabel(S);
    let sw = await openSheet(S.id);
    // B4: the orders on this sheet, and the count in its header
    R.eq('B4', at, sw.rows.map(r => r.rid).sort(), rids.slice().sort(), 'the orders listed on the sheet');
    R.eq('B4', `${at}, "All" count`, sw.filters.all ? sw.filters.all.n : null, rids.length, 'the All count');
    // B3: the tags of the other sheets of each order, and the Other sheets filter
    for (const row of sw.rows) R.eq('B3', `${at}, order ${row.rid}`, row.tags.slice().sort(), truth.otherSheets(row.rid, S.id).map(short).sort(), 'the tags of the order\'s other sheets');
    const multi = rids.filter(r => truth.otherSheets(r, S.id).length);
    R.eq('B3', `${at}, "Other sheets" count`, sw.filters.multi ? sw.filters.multi.n : null, multi.length, 'the Other sheets count');
    await page.evaluate(() => document.querySelector('[data-f=multi]')?.click());
    const fl = await settle(page, SW, null, { max: 3000, quiet: 300 });
    R.eq('B3', `${at}, "Other sheets" list`, fl.rows.map(r => r.rid).sort(), multi.slice().sort(), 'the orders the Other sheets filter lists');
    await page.evaluate(() => document.querySelector('[data-f=all]')?.click());
    await settle(page, SW, null, { max: 3000, quiet: 300 });
    // B1 / B2 / B6: every order of the sheet, selected
    for (const rid of rids) {
      // (an order outside the pull, read from records alone: a piece with no SKU has no pool record, nothing the app can read names it. That one piece
      //  is not asked of the sheet window there; the order window opened from it reads the order itself and is asked everything)
      const pieces = truth.pieces(rid).filter(p => p.state !== 'gone' && (mode !== 'out' || p.poolId)), side = `${at}, order ${rid}`, here = pieces.filter(p => p.sheetId === S.id);
      await pressUntil(page, rid => document.querySelector(`.swOrd[data-rid="${rid}"]`)?.click(), selectedOrder, rid);
      let v = await settle(page, SW, null, { max: 7000, quiet: 550, ready: swSelReady });
      const check = (v, how) => {
        const rows = v.trail.map(r => ({ sku: r.sku, kind: whereKind(r.where) })), miss = matchRows(rows, pieces, S.id);
        R.check('B1', `${side}${how}`, !miss.length && v.trail.filter(r => r.cur).length === 1, `the trail (${v.head}) reads ${JSON.stringify(v.trail.map(r => `${r.sku} ${r.where}`))}: ${miss.join('; ') || 'not exactly one "this charm"'}`);
        R.eq('B1', `${side}${how}, header`, +((/(\d+) piece/.exec(v.head) || [])[1]), pieces.length, 'the header\'s piece count');
        if (v.chipsShown || v.chips.length > 1) for (const c of v.chips) {
          const n = pieces.filter(p => p.sheetId === c.id).length;
          const want = SHEET[c.id] ? short(sheetLabel(SHEET[c.id])) : null;
          if (want) R.check('B2', `${side}${how}, the name of chip ${c.id}`, norm2(c.label) === want, `the chip of ${c.id} is named "${c.label}", the sheet is ${want}`);
          R.check('B2', `${side}${how}, chip ${c.label}`, c.lit === (n > 0) && c.n === n, `the chip ${c.label} is ${c.lit ? 'lit' : 'dark'} with ${c.n}, the order has ${n} piece(s) there`);
        }
      };
      check(v, '');
      // a second piece on this same sheet: select it from the trail, the whole list must stay true
      if (here.length > 1) {
        await page.evaluate(() => [...document.querySelectorAll('[data-r2=trail] li[data-i]')].find(li => !li.classList.contains('cur') && /on this sheet/i.test(li.querySelector('.where')?.textContent || ''))?.click());
        const v2 = await settle(page, SW, null, { max: 5000, quiet: 550, ready: swSelReady }); check(v2, ' (the second piece on the sheet selected)');
      }
      // a piece on another sheet: its row opens THAT sheet
      const other = pieces.find(p => p.sheetId && p.sheetId !== S.id);
      if (other) {
        const label = other.sheetLabel;
        const opened = await pressUntil(page, a => [...document.querySelectorAll('[data-r2=trail] li[data-i]')].find(li => (li.querySelector('.where')?.textContent || '').includes(a.label))?.click(), a => window.SheetWin && SheetWin.isOpen() && SheetWin.current() === a.id, { label, id: other.sheetId }, { max: 8000 });
        R.check('B6', `${side}, its row "${label}"`, opened, `the trail row of the piece on ${label} did not open that sheet (the window shows ${await page.evaluate(() => SheetWin.current())})`);
        if (opened) { v = await settle(page, SW, null, { max: 7000, quiet: 550, ready: swSelReady }); R.check('B6', `${side}, on ${label} after the jump`, v.trail.some(r => r.cur && v.cur === other.sheetId) || v.rows.some(r => r.rid === rid), `opened ${label} but the order ${rid} is not listed there`); }
        await openSheet(S.id);
        await pressUntil(page, rid => document.querySelector(`.swOrd[data-rid="${rid}"]`)?.click(), selectedOrder, rid);
        await settle(page, SW, null, { max: 7000, quiet: 550, ready: swSelReady });
      }
      // Open order: the order window opens on this order with THIS sheet lit in its Sheet tab, and every sheet of the order as a tab
      const ok = await pressUntil(page, () => document.querySelector('[data-r2=openOrd]')?.click(), rid => OrderWin.isOpen() && OrderWin.rid && OrderWin.rid() === rid, rid, { max: 14000, every: 1500 });
      if (!ok) R.check('B6', `${side}, Open order`, false, 'Open order did not open the order window');
      else {
        await settle(page, () => ({ n: document.getElementById('owTlCount')?.textContent || '' }), null, { max: 4000, quiet: 300 });
        await page.evaluate(() => OrderWin.setView('sheet'));
        const tab = await readSheetTab(page);
        const on = tab.chips.find(c => c.on);
        R.check('B6', `${side}, Open order -> Sheet tab`, !!on && on.label === sheetLabel(S), `opened from ${at} the order window's Sheet tab shows ${on ? on.label : JSON.stringify(tab.none)} selected`);
        R.eq('B6', `${side}, Open order -> sheet chips`, tab.chips.map(c => c.label).sort(), truth.sheets(rid), 'the order window\'s sheet chips');
      }
      await closeAll(page);
      if (rid !== rids[rids.length - 1]) await openSheet(S.id);
    }
  }
}

/* ═══════════════════════════ C · Library cards ═══════════════════════════ */
/** The Library's Sets view: each set card lists its sheets, each with the number of orders on it. */
async function libraryProbes(page, mode, R, variant) {
  await closeAll(page);
  await page.evaluate(() => CN.setMode('library'));
  await page.waitForSelector('#libBody .setCard', { timeout: 30000 }).catch(() => {});
  const cards = await settle(page, () => [...document.querySelectorAll('#libBody .setCard')].map(c => ({ title: (c.querySelector('[data-set-title]')?.textContent || '').trim(), text: c.innerText.replace(/\s+/g, ' ') })), null, { max: 15000, quiet: 900, ready: v => v && v.length >= 2 && v.every(c => /orders?/.test(c.text)) });
  for (const set of SETS) {
    const card = (cards || []).find(c => new RegExp(`Set-${set.seq}\\b`).test(c.title));
    if (!card) { R.check('C1', `Set-${set.seq}`, false, 'the Library has no card for the set'); continue; }
    const at = [], re = /(GF|SS|RG|10K|14K) Sheet \d+/g; let m; while ((m = re.exec(card.text))) at.push({ label: m[0], i: m.index });
    for (const s of SHEETS.filter(x => x.set === set.id)) {
      const k = at.findIndex(x => x.label === sheetLabel(s));
      if (k < 0) { R.check('C1', `${sheetLabel(s)} on the Set-${set.seq} card`, false, `the card of Set-${set.seq} does not list ${sheetLabel(s)}`); continue; }
      const seg = card.text.slice(at[k].i, k + 1 < at.length ? at[k + 1].i : undefined), want = truth.ordersOn(s.id).length, got = +((/(\d+) orders?/.exec(seg) || [])[1]);
      R.check('C1', `${sheetLabel(s)} on the Set-${set.seq} card`, got === want, `the card says ${got} order(s), ${want} order(s) have a piece on it`);
      const qr = +((/QR label (\d+) orders?/.exec(seg) || [])[1]);
      if (!Number.isNaN(qr)) R.check('C1', `${sheetLabel(s)} QR label on the Set-${set.seq} card`, qr === want, `the QR label says ${qr} order(s), ${want} order(s) have a piece on it`);
    }
  }
  await sharedProbes(page, mode, R);
  await sharedModalProbes(page, mode, R);
  await issuesProbes(page, mode, R, variant);
  await librarySearchProbes(page, mode, R);
  await page.evaluate(() => CN.setMode('orders'));
}

/* K1: the cardinal rule. "Every sheet that shares a multi-piece order stays in the same Set": which sheets must stay together, and which orders a move would split.
   The oracle below is worked out from the fixture alone (an order is shared when more than one of its pieces sits on sheets and those are two or more). */
const sharedTruth = () => FIX.map(o => { const ps = truth.pieces(o.rid).filter(p => p.sheetId); return { rid: o.rid, sheets: [...new Set(ps.map(p => p.sheetId))].sort(), n: new Set(ps.map(p => p.poolId || p.key + p.copy)).size }; }).filter(x => x.sheets.length >= 2 && x.n > 1);
function groupTruth(id) {
  const shared = sharedTruth(), seen = new Set([id]); let grew = true;
  while (grew) { grew = false; for (const o of shared) if (o.sheets.some(x => seen.has(x))) for (const x of o.sheets) if (!seen.has(x)) { seen.add(x); grew = true; } }
  return { ids: [...seen].sort(), orders: shared.filter(o => o.sheets.some(x => seen.has(x))).map(o => o.rid).sort() };
}
function splitTruth(moving, dest) {
  const out = [];
  for (const o of sharedTruth()) {
    const here = o.sheets.filter(x => moving.includes(x)), there = o.sheets.filter(x => !moving.includes(x) && (SHEET[x].set || null) !== dest);
    if (here.length && there.length) out.push({ rid: o.rid, there: there.map(x => sheetLabel(SHEET[x])).sort(), thereIds: there });
  }
  return out;
}
async function sharedProbes(page, mode, R) {
  const ask = (fn, arg) => page.evaluate(({ fn, arg }) => { try { const v = window.SharedOrders[fn](...arg); return JSON.parse(JSON.stringify(v)); } catch (e) { return { error: e.message }; } }, { fn, arg });
  if (!(await page.evaluate(() => !!(window.SharedOrders && SharedOrders.between)))) { R.check('K1', 'SharedOrders', false, 'window.SharedOrders is not on the page'); return; }
  await sleep(1200);
  for (const S of SHEETS) {
    const at = sheetLabel(S), g = await ask('groupOf', [S.id]), want = groupTruth(S.id);
    R.eq('K1', `${at}, the sheets that must stay together`, (g.ids || []).slice().sort(), want.ids, 'the sheets it must stay with');
    R.eq('K1', `${at}, the shared orders of that group`, (g.orders || []).slice().sort(), want.orders, 'the orders that tie them');
    for (const dest of [null, ...SETS.map(x => x.id)]) {
      const items = await ask('between', [S.id, dest]), w = splitTruth([S.id], dest);
      R.eq('K1', `${at} moved to ${dest || 'no set'}, the orders it would split`, Array.isArray(items) ? items.map(i => ({ rid: i.orderId, there: (i.there || []).slice().sort() })).sort((a, b) => a.rid < b.rid ? -1 : 1) : items, w.map(x => ({ rid: x.rid, there: x.there })), 'the orders that would be split');
    }
  }
  for (const set of SETS) {
    const items = await ask('between', [set.id, null]), w = splitTruth(SHEETS.filter(x => x.set === set.id).map(x => x.id), set.id);
    R.eq('K1', `Set ${set.seq} kept as it is, the orders already split across sets`, Array.isArray(items) ? items.map(i => ({ rid: i.orderId, there: (i.there || []).slice().sort() })).sort((a, b) => a.rid < b.rid ? -1 : 1) : items, w.map(x => ({ rid: x.rid, there: x.there })), 'the orders split across sets');
  }
}

/* K2: the shared-orders window (SharedOrdersModal), the pop-up that says which orders keep a sheet in its set: one card per order, each piece with the sheet it is on. */
async function sharedModalProbes(page, mode, R) {
  if (!(await page.evaluate(() => !!(window.SharedOrdersModal && SharedOrdersModal.open)))) { R.check('K2', 'SharedOrdersModal', false, 'window.SharedOrdersModal is not on the page'); return; }
  for (const S of SHEETS) for (const dest of [null, ...SETS.map(x => x.id)]) {
    const want = splitTruth([S.id], dest), at = `${sheetLabel(S)} moved to ${dest || 'no set'}`;
    await page.evaluate(([id, dest]) => { try { SharedOrdersModal.close(); } catch (_) {} try { SharedOrdersModal.open({ sheetId: id, targetSetId: dest }); } catch (_) {} }, [S.id, dest]);
    const read = () => page.evaluate(() => [...document.querySelectorAll('.soDlg .soCard')].map(card => ({ rid: card.dataset.order, pieces: [...card.querySelectorAll('.soPiece')].map(p => `${(p.querySelector('.soChip')?.textContent || '').trim()} ${p.classList.contains('here') ? 'here' : 'there'}`).sort() })).sort((a, b) => a.rid < b.rid ? -1 : 1));
    let got = await read(), prev = '';
    for (let i = 0; i < 8 && JSON.stringify(got) !== prev; i++) { prev = JSON.stringify(got); await sleep(450); got = await read(); }
    const exp = want.map(w => ({ rid: w.rid, pieces: truth.pieces(w.rid).filter(p => p.sheetId && (p.sheetId === S.id || w.thereIds.includes(p.sheetId))).map(p => `${short(sheetLabel(SHEET[p.sheetId]))} ${p.sheetId === S.id ? 'here' : 'there'}`).sort() }));
    R.eq('K2', `${at}, the orders on the cards`, got.map(c => c.rid), exp.map(c => c.rid), 'the orders the window lists');
    for (const e of exp) { const c = got.find(x => x.rid === e.rid); if (c) R.eq('K2', `${at}, order ${e.rid}`, c.pieces, e.pieces, 'the pieces on its card with the sheet each is on'); }
    await page.evaluate(() => { try { SharedOrdersModal.close(); } catch (_) {} }); await sleep(450);
  }
}

/* C3: the Library's own search for an order number: "N sheets hold order X · in M sets" (findSheets, the saved sheet records), and the set cards it keeps. */
async function librarySearchProbes(page, mode, R) {
  const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
  for (const o of FIX) {
    const sheets = [...new Set(truth.pieces(o.rid).filter(p => p.sheetId).map(p => p.sheetId))], sets = [...new Set(sheets.map(id => SHEET[id].set))];
    await page.fill('#libSearch', o.rid); await page.press('#libSearch', 'Enter');
    const line = await page.waitForFunction(() => { const t = (document.querySelector('.ldFound') || {}).innerText || ''; return /hold|holds|No current sheet|Could not/.test(t) && !/Looking through/.test(t) ? t.replace(/\s+/g, ' ').trim() : null; }, null, { timeout: 15000 }).then(h => h.jsonValue()).catch(() => null);
    await sleep(500);
    const cards = await page.evaluate(() => [...document.querySelectorAll('#libBody .setCard')].map(c => (c.querySelector('[data-set-title]')?.textContent || '').trim()));
    if (!sheets.length) R.check('C3', `order ${o.rid} (on no sheet)`, !!line && /^No current sheet/.test(line), `the Library search says "${line}", no sheet holds the order`);
    else {
      const want = `${plural(sheets.length, 'sheet')} ${sheets.length === 1 ? 'holds' : 'hold'} order ${o.rid}`;
      R.check('C3', `order ${o.rid}, the sheets it is found on`, !!line && line.startsWith(want), `the Library search says "${line}", the order is on ${plural(sheets.length, 'sheet')}`);
      const inSets = !!line && (/ in (\d+) sets?/.exec(line) || [])[1];
      R.check('C3', `order ${o.rid}, the sets it is found in`, +inSets === sets.length, `the Library search says it is in ${inSets || 'no'} set(s), the sheets are in ${plural(sets.length, 'set')}`);
      for (const set of SETS.filter(x => sets.includes(x.id))) R.check('C3', `order ${o.rid}, the card of Set-${set.seq}`, cards.some(t => new RegExp(`Set-${set.seq}\\b`).test(t)), `the Library keeps no card for Set-${set.seq}, which holds a piece of the order`);
    }
  }
  await page.click('.ldClear').catch(() => {});
  await page.fill('#libSearch', ''); await page.press('#libSearch', 'Enter').catch(() => {});
}

/* C5: what the Library says holds a sheet back about its ORDERS (the '!' panel reads this: LaserReview.issuesOf), against the pieces the fixture says are on no sheet.
   Only the piece reasons are asked here (not on a sheet yet, no SKU); another sheet's physical stage is the readiness engine's own and has its own tests. */
async function issuesProbes(page, mode, R, variant) {
  for (const S of SHEETS) {
    if (variant === 'sheet' && lagged(variant, S.id)) continue;   // (see recordUnnested)
    const at = sheetLabel(S);
    await page.waitForFunction(id => window.LaserReview && LaserReview.issuesOf && LaserReview.issuesOf(id), S.id, { timeout: 20000 }).catch(() => {});
    await sleep(900);
    const got = await page.evaluate(id => { const r = LaserReview.issuesOf(id); return r ? { issues: (r.issues || []).filter(i => i.step === 'orders').map(i => ({ rid: String(i.orderId), key: i.key })) } : null; }, S.id);
    if (!got) { R.check('C5', at, false, 'the Library has no readiness for the sheet'); continue; }
    const want = truth.ordersOn(S.id).filter(rid => recordUnnested(variant, rid).length).map(rid => ({ rid, key: recordUnnested(variant, rid).some(p => !p.sku) ? 'noSku' : 'pooled' })).sort((a, b) => a.rid < b.rid ? -1 : 1);
    const pieceKeys = new Set(['pooled', 'noSku', 'unmatched', 'noDesign']);
    R.eq('C5', `${at}, the orders held back by a piece on no sheet`, got.issues.filter(i => pieceKeys.has(i.key)).sort((a, b) => a.rid < b.rid ? -1 : 1), want, 'the orders the sheet says wait on a piece');
  }
}

/* ═══════════════════════════ D · Orders list rows, global search ═══════════════════════════ */
/** D1: the state word of an order line says "nested" when every piece of it is on a sheet, "pooled" while any is not: even when the run record still says pooled. */
async function ordersListProbes(page, mode, R, variant) {
  if (!MODES[mode].pull) return;
  const rows = await page.evaluate(() => Orders.rows().map(r => ({ key: r.key, pill: String((Orders.statePill(r) || [])[1] || '') })));
  for (const o of FIX) for (const l of o.lines) {
    if (l.state === 'gone' || lineState(variant, l) !== 'pooled') continue;
    const row = rows.find(r => r.key === l.key); if (!row) { R.check('D1', `${o.rid} ${pieceName(l)}`, false, 'the line is not in the Orders list'); continue; }
    const mine = truth.pieces(o.rid).filter(p => p.key === l.key), allOn = mine.length > 0 && mine.every(p => p.sheetId);
    R.check('D1', `${o.rid} ${pieceName(l)} (the record still says pooled)`, allOn ? /nested/i.test(row.pill) : /pooled/i.test(row.pill), `every piece of the line is ${allOn ? 'on a sheet' : 'not on a sheet'} and its state word says "${row.pill}"`);
  }
}
/** D3: the header search's card of an order names every sheet the order is on, once each. */
async function searchProbes(page, mode, R) {
  await closeAll(page);
  for (const o of FIX) {
    await page.evaluate(() => { if (!OrderSearch.isOpen()) OrderSearch.open(); });
    await page.waitForSelector('#cnsQ', { timeout: 8000 });
    await page.fill('#cnsQ', ''); await page.fill('#cnsQ', o.rid);
    const card = await settle(page, rid => { const c = [...document.querySelectorAll('.cnsCard')].find(x => (x.querySelector('.cnsNum')?.textContent || '').replace(/\D/g, '').includes(rid)); return c ? { sheets: (c.querySelector('.cnsSheet')?.textContent || '').trim(), pill: (c.querySelector('.cnsPill')?.textContent || '').trim(), now: (c.querySelector('.cnsNow')?.textContent || '').trim() } : null; }, o.rid, { max: 9000, quiet: 700, ready: v => !!v });
    const want = truth.sheets(o.rid);
    if (!card) { R.check('D3', `${o.rid}`, false, 'the search found no card for the order number'); continue; }
    const labels = (card.sheets.match(/(?:GF|SS|RG|10K|14K) Sheet \d+/g) || []), extra = +((/\+(\d+)\s*$/.exec(card.sheets) || [])[1] || 0);
    const dup = labels.length !== new Set(labels).size, shown = want.slice(0, 2);
    R.check('D3', `${o.rid} (${o.tag})`, !dup && labels.length + extra === want.length && labels.every(x => want.includes(x)), `the card names ${JSON.stringify(card.sheets)} (pill "${card.pill}", "${card.now}"), the order is on ${JSON.stringify(want)}`);
    void shown;
  }
  await page.evaluate(() => { try { OrderSearch.close(); } catch (_) {} });
}

/* ═══════════════════════════ G · the server's own answer (no browser) ═══════════════════════════ */
/** G1: the production readiness the Library cards and the laser gate read (laserStatus, read only): a sheet waits for an order exactly when one of the
 *  order's live pieces is on no sheet, and says which sheets the order is on. */
async function serverProbes(srv, variant, R) {
  R.at(`${variant}/server`);
  const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body || '{}');
  const r = await call({ op: 'laserStatus', sheetIds: SHEETS.map(s => s.id), setIds: SETS.map(s => s.id), recordSeals: false });
  for (const S of SHEETS) {
    if (variant === 'sheet' && lagged(variant, S.id)) continue;   // (a record that lists no piece has nothing to say about its own orders; the other sheets are asked about it)
    const rec = (r.sheets || []).find(x => x.id === S.id), at = sheetLabel(S);
    if (!rec) { R.check('G1', at, false, 'laserStatus does not answer for the sheet'); continue; }
    const got = rec.orderReadiness || {};
    for (const rid of truth.ordersOn(S.id)) {
      const e = got[rid], blocked = recordUnnested(variant, rid).length > 0;
      if (!e) { R.check('G1', `${at}, order ${rid}`, false, 'the sheet\'s readiness does not mention the order'); continue; }
      R.check('G1', `${at}, order ${rid}`, !!e.ready === !blocked, `${blocked ? 'a piece of the order is on no saved sheet, and' : 'every piece of the order is on a saved sheet, and'} the sheet says the order is ${e.ready ? 'ready' : 'not ready'}${e.why ? ` (${e.why})` : ''}`);
      if (blocked && e.ready === false && Array.isArray(e.onSheets)) R.eq('G1', `${at}, order ${rid}, the sheets it names`, e.onSheets.slice().sort(), [...new Set(truth.pieces(rid).filter(p => p.sheetId && !(variant === 'sheet' && lagged(variant, p.sheetId))).map(p => p.sheetId))].sort(), 'the sheets the order is on');
    }
    for (const rid of Object.keys(got)) if (!truth.ordersOn(S.id).includes(rid)) R.check('G1', `${at}, order ${rid}`, false, 'the sheet waits on an order that has no piece on it');
  }
}

/* ═══════════════════════════ H · OrderPieces itself, against the fixture ═══════════════════════════ */
/** H1: window.OrderPieces (the one truth every screen reads) says what the fixture says, once the records are read; so a wrong OrderPieces cannot hide behind
 *  screens that copy it. */
async function piecesProbes(page, mode, R) {
  if (!(await page.evaluate(() => !!(window.OrderPieces && OrderPieces.of)))) return;
  const rids = FIX.map(o => o.rid);
  await page.evaluate(async rids => { await OrderPieces.load(rids, { force: true }); }, rids);
  const got = await page.evaluate(rids => Object.fromEntries(rids.map(r => [r, OrderPieces.of(r).map(p => ({ key: p.key, nested: !!p.nested, sheetId: p.sheetId || null, gone: !!p.gone, loading: !!p.loading }))])), rids);
  for (const o of FIX) {
    const list = got[o.rid] || [];
    for (const p of truth.pieces(o.rid)) {
      if (p.state === 'gone') { const g = list.find(x => x.key === (p.poolId || `${p.key}_${p.copy}`)); R.check('H1', `${o.rid} ${p.label || p.sku || 'no SKU'} (cancelled)`, !g || g.gone || !g.nested, `a cancelled piece is said to be on ${g && g.sheetId}`); continue; }
      const key = p.poolId || `${p.key}_${p.copy}`, g = list.find(x => x.key === key);
      if (!g) { if (p.poolId || MODES[mode].pull) R.check('H1', `${o.rid} ${p.label || p.sku || 'no SKU'}`, false, `OrderPieces does not list the piece ${key} (it lists ${JSON.stringify(list.map(x => x.key))})`); continue; }
      R.check('H1', `${o.rid} ${p.label || p.sku || 'no SKU'} copy ${p.copy}`, g.nested === !!p.sheetId && g.sheetId === p.sheetId && !g.loading, `OrderPieces says ${g.nested ? 'on ' + g.sheetId : 'on no sheet'}${g.loading ? ' (still reading)' : ''}, the piece is ${p.sheetId ? 'on ' + p.sheetId : 'on no sheet'}`);
    }
    for (const g of list) if (g.nested && !truth.pieces(o.rid).some(p => p.sheetId === g.sheetId)) R.check('H1', `${o.rid} ${g.key}`, false, `OrderPieces puts a piece of the order on ${g.sheetId}, which holds none`);
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
  B1: 'Sheet window · "This order" trail of the order selected: every piece, here or on which other sheet',
  B2: 'Sheet window · the set\'s sheet chips lit for the order selected, with the count of its pieces on each',
  B3: 'Sheet window · the tags of the other sheets on each order row, and the Other sheets filter',
  B4: 'Sheet window · the orders listed on the sheet and their count',
  B6: 'Sheet window · a trail row opens that sheet; Open order lands on this sheet with every sheet of the order',
  H1: 'OrderPieces · window.OrderPieces itself says what the fixture says about each piece',
  G1: 'Server · the readiness each sheet reports for its orders (laserStatus, read only)',
  C1: 'Library · the order count on each sheet of a set card (and its QR label)',
  C3: 'Library · the Library\'s own order-number search: how many sheets and sets hold the order, and the set cards it keeps',
  C5: 'Library · the orders a sheet is held back by (a piece on no sheet, no SKU): what the "!" panel reads',
  K2: 'SharedOrders window · the pop-up for a blocked move: the orders on its cards and the sheet each of their pieces is on',
  K1: 'SharedOrders · the sheets that must stay together and the orders a move would split (the cardinal rule)',
  D1: 'Orders list · the state word of a line (nested only when every piece is on a sheet)',
  D3: 'Header search · the sheets named on an order\'s card',
  E2: 'Timeline · the rail reaches Nested for a piece on a sheet, and only then',
  E3: 'Timeline · the Nested step explainer (hover on the rail): done and naming a sheet of the piece, or not done',
};
function parseArgs() {
  const a = process.argv.slice(2), o = { list: a.includes('--list'), dump: a.includes('--dump'), quick: a.includes('--quick'), only: null, variants: null, modes: null, orders: null, json: null };
  const val = f => { const i = a.indexOf(f); return i >= 0 ? a[i + 1] : null; };
  if (val('--only')) o.only = new Set(val('--only').split(','));
  if (val('--variants')) o.variants = val('--variants').split(',');
  if (val('--modes')) o.modes = val('--modes').split(',');
  if (val('--orders')) o.orders = val('--orders').split(',');
  if (val('--json')) o.json = val('--json');
  return o;
}
async function runOne(chromium, variant, mode, R, args, SHARDS) {
  const srv = await start({ receipts: [] }); seedServer(srv, variant);
  const t0 = Date.now(), ctx = `${variant}/${mode}`;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const orders = FIX.filter(o => !args.orders || args.orders.includes(o.rid)), wants = ps => !args.only || ps.some(p => [...args.only].some(x => x.startsWith(p)));
    const jobs = [];
    if (wants(['A', 'E'])) { const shards = Array.from({ length: Math.min(SHARDS, orders.length) }, () => []); orders.forEach((o, i) => shards[i % shards.length].push(o)); for (const list of shards) jobs.push({ what: 'order window', run: (page, rep) => orderWindowProbes(page, mode, rep, list) }); }
    if (wants(['B'])) jobs.push({ what: 'sheet window', run: (page, rep) => sheetWinProbes(page, mode, rep, SHEETS) });
    if (wants(['C', 'D', 'H', 'K'])) jobs.push({ what: 'library, orders list and search', run: async (page, rep) => {
      if (wants(['H'])) await piecesProbes(page, mode, rep);
      if (wants(['D1'])) await ordersListProbes(page, mode, rep, variant);
      if (wants(['D3'])) await searchProbes(page, mode, rep);
      if (wants(['C', 'K'])) await libraryProbes(page, mode, rep, variant);
    } });
    await Promise.all(jobs.map(async (job, i) => {
      const rep = new Report().at(ctx); let session;
      try {
        session = await boot(srv, chromium, { mode, variant, browser });
        await job.run(session.page, rep);
        if (session.errors.length) rep.check('PAGE', 'page errors', false, session.errors.slice(0, 3).join(' | '));
      } catch (e) { rep.check('RUN', `${ctx} ${job.what} ${i}`, false, e.stack || e.message); }
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
  const matrix = args.quick ? [['ok', 'live'], ['ok', 'recall'], ['ok', 'out']] : [['ok', 'live'], ['ok', 'pool'], ['ok', 'rec'], ['ok', 'recall'], ['ok', 'out'], ['pool', 'live'], ['pool', 'recall'], ['pool', 'out'], ['sheet', 'live'], ['sheet', 'recall'], ['sheet', 'out'], ['set', 'live'], ['set', 'recall'], ['set', 'out'], ['state', 'live'], ['state', 'recall'], ['state', 'out'], ['stale', 'live'], ['stale', 'recall'], ['stale', 'out']];
  const jobs = matrix.filter(([v, m]) => (!args.variants || args.variants.includes(v)) && (!args.modes || args.modes.includes(m)));
  // the server's own answer, once per variant (no page)
  if (!args.only || [...args.only].some(x => x.startsWith('G'))) for (const v of [...new Set(jobs.map(j => j[0]))]) {
    const srv = await start({ receipts: [] }); seedServer(srv, v);
    try { const rep = new Report(); await serverProbes(srv, v, rep); R.rows.push(...rep.rows); } catch (e) { R.rows.push({ surface: 'RUN', ctx: `${v}/server`, side: 'server', ok: false, detail: e.stack || e.message }); } finally { srv.close(); }
  }
  const t0 = Date.now(), CONC = +process.env.CONC || 2, SHARDS = +process.env.SHARDS || 2; let next = 0;
  await Promise.all(Array.from({ length: Math.min(CONC, jobs.length) }, async () => { while (next < jobs.length) { const [v, m] = jobs[next++]; const ms = await runOne(chromium, v, m, R, args, SHARDS); console.log(`  · ${v}/${m} ${(ms / 1000).toFixed(0)} s`); } }));
  if (args.json) require('fs').writeFileSync(args.json, JSON.stringify(R.rows.filter(r => !r.ok && !r.known), null, 1));
  const by = R.summary(args.only); let bad = 0;
  console.log(`\n${'surface'.padEnd(6)} ${'result'.padEnd(11)} what`);
  for (const [id, e] of [...by].sort((a, b) => a[0].localeCompare(b[0], undefined, { numeric: true }))) {
    bad += e.fail.length ? 1 : 0;
    console.log(`${id.padEnd(6)} ${(e.fail.length ? `FAIL ${e.fail.length}/${e.fail.length + e.pass}` : `ok ${e.pass}`).padEnd(11)} ${SURFACES[id] || ''}`);
    if (e.known.length) console.log(`         known, by design (${e.known.length} not counted, in ${[...new Set(e.known.map(f => f.ctx))].sort().join(', ')}): ${e.known[0].known}`);
    const ctxs = [...new Set(e.fail.map(f => f.ctx))].sort();
    if (e.fail.length) console.log(`         wrong in: ${ctxs.join(', ')}`);
    for (const f of e.fail.slice(0, args.dump ? 999 : 3)) console.log(`         [${f.ctx}] ${f.side}: ${f.detail}`);
    if (e.fail.length > 3 && !args.dump) console.log(`         … ${e.fail.length - 3} more (--dump lists them all)`);
  }
  console.log(`\n${((Date.now() - t0) / 1000).toFixed(0)} s · ${bad ? bad + ' surface(s) wrong' : 'every surface tells the same truth'}`);
  return args.list ? 0 : bad ? 1 : 0;
}
module.exports = { MODES, VARIANTS, FIX, SHEETS, SETS, SHEET, truth, sheetLabel, seedServer, boot, ORDERS, Report, settle, norm, closeAll };
if (require.main === module) main().then(c => process.exit(c), e => { console.error(e); process.exit(1); });
