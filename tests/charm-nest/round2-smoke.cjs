/* Round 2 full-page smoke (Library / order window / sheet window): the safety net under the ten requests of
   plans/library-flow/round2.md, which eleven workers change at the same time (bridge, readiness, library, flow, charm-nest-1.html,
   the server's charmNestLibrary). It boots the REAL charm-nest-1.html in headless Chromium against the local stand-in for the
   site (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore), every request that is not to the
   loopback aborted. Nothing live is contacted, nothing is paid for.
     A. a 2-sheet set (GF + SS) whose sheets share multi-piece orders (and an order whose other piece is not on a sheet yet, and one
        whose other piece has no SKU): the Library opens, a live read cycle runs, an order window is opened on EACH piece (Sheet tab),
        a sheet is dragged out of its set, the Approve button is pressed.
     B. a 300-sheet Library (100 sets of 3): it opens, and the time it takes is compared with the baseline (+20%).
   Hard checks (any failure is a regression): no console errors, page errors or unhandled promise rejections; no doubled DOM ids (beyond
   the baseline's); no flight copies, docks or hidden originals left behind; no live read with recordSeals:true; no write call while the
   views are only looked at; every sheet card on the page exactly once, before and after a drag; Library open time within +20% of the
   baseline's. "ACCEPT" checks are Round 2's own truths (the right sheet for each piece, no set-level approve, ...): they are expected to be
   red on the old main and to turn green as the workers land; they are reported, and never counted as a regression.
     node tests/charm-nest/round2-smoke.cjs [--out=file.json] [--baseline=file.json] [--timing-only] [--runs=7] [--shots=dir] [--skip-many]
   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; exit code 1 when a hard check fails.) */
const fs = require('fs'), path = require('path'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const arg = n => { const a = process.argv.find(x => x.startsWith('--' + n + '=')); return a ? a.slice(n.length + 3) : ''; };
const flag = n => process.argv.includes('--' + n);
const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = arg('shots'), RUNS = +arg('runs') || 7;
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', POOL = 'Charm_Pool', RUNS_C = 'Charm_Nest_Runs';
const today = new Date().toISOString().slice(0, 10);

/* ── results ── */
const out = { commit: '', when: new Date().toISOString(), checks: [], dupIds: {}, errors: [], calls: {}, timing: {}, info: {} };
const check = (kind, name, ok, detail) => { out.checks.push({ kind, name, ok: !!ok, detail: detail === undefined ? '' : String(detail).slice(0, 600) }); console.log(`${ok ? (kind === 'ACCEPT' ? 'ok   ' : 'ok   ') : (kind === 'ACCEPT' ? 'open ' : 'FAIL ')} [${kind}] ${name}${detail ? ' — ' + String(detail).slice(0, 300) : ''}`); return !!ok; };
const hard = (name, ok, detail) => check('HARD', name, ok, detail);
const accept = (name, ok, detail) => check('ACCEPT', name, ok, detail);
const info = (name, v) => { out.info[name] = v; console.log(`info  ${name}: ${typeof v === 'string' ? v : JSON.stringify(v)}`); };

/* ── pictures ── */
function crc32(buf) { let c, crc = 0xffffffff; for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; } return (crc ^ 0xffffffff) >>> 0; }
function png(w, h, fn) {
  const raw = Buffer.alloc((w * 3 + 1) * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const [r, g, b] = fn(x, y), o = y * (w * 3 + 1) + 1 + x * 3; raw[o] = r; raw[o + 1] = g; raw[o + 2] = b; }
  const chunk = (t, d) => { const len = Buffer.alloc(4); len.writeUInt32BE(d.length); const td = Buffer.concat([Buffer.from(t), d]), crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td)); return Buffer.concat([len, td, crc]); };
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw)), chunk('IEND', Buffer.alloc(0))]);
}
const COLORS = { gold: [201, 166, 92], silver: [160, 166, 175], rose: [205, 140, 130] };
const preview = (metal, seed) => { const [r, g, b] = COLORS[metal]; return png(120, 100, (x, y) => ((x + seed * 13) % 24 - 10) ** 2 + ((y + seed * 7) % 22 - 9) ** 2 < 40 ? [r, g, b] : [252, 250, 246]); };
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG' };

/* ═════ A · the small world ═════ */
const P = n => String(4170250000 + n);
// [orderId, buyer, [[sku, metal, sheetId|null]...]]. The shared orders put piece 1 on the GF sheet and piece 2 on the SS sheet (Paul's order 4170252963).
const ORDERS = [
  ...[1, 2, 3, 4].map(n => [P(n), 'Shared Buyer ' + n, [['FEMALE_SYMBOL', 'gold', 'gfA1'], ['FEMALE_SYMBOL', 'silver', 'ssA1']]]),
  ...[11, 12].map(n => [P(n), 'Pair Buyer ' + n, [['HEART_CHARM', 'gold', 'gfA1'], ['STAR_CHARM', 'gold', 'gfA1']]]),
  ...[21, 22].map(n => [P(n), 'Single Gold ' + n, [['LEAF_CHARM', 'gold', 'gfA1']]]),
  ...[31, 32].map(n => [P(n), 'Single Silver ' + n, [['MOON_DROP', 'silver', 'ssA1']]]),
  [P(51), 'Unnested Other', [['HEALTH1', 'gold', 'gfA1'], ['CUTE_TRICERATOPS', 'gold', null]]],          // piece 2 is not on any sheet yet
  [P(52), 'No Sku Other', [['MOON_DROP', 'silver', 'ssA1'], ['', 'silver', null]]],                        // piece 2 has no SKU
  ...[61, 62, 63].map(n => [P(n), 'Set Two ' + n, [['LEAF_CHARM', 'gold', 'gfB1']]]),
  ...[71, 72].map(n => [P(n), 'Ready ' + n, [['LEAF_CHARM', 'gold', 'gfR1']]])
];
const SHEET_PLAN = [
  { id: 'gfA1', metal: 'gold', setId: 'set-r2-1', seq: 1, idx: 1, ready: false },
  { id: 'ssA1', metal: 'silver', setId: 'set-r2-1', seq: 1, idx: 2, ready: false },
  { id: 'gfB1', metal: 'gold', setId: 'set-r2-2', seq: 2, idx: 1, ready: false },
  { id: 'gfR1', metal: 'gold', setId: 'set-r2-3', seq: 3, idx: 1, ready: true }
];
const tidOf = (rid, i) => rid + (i + 1), poolOf = (rid, i) => `${rid}_${tidOf(rid, i)}_1`;
function seedSmall(st, blobUrl) {
  const now = Date.now(); let n = 0; const runLines = {};
  for (const sp of SHEET_PLAN) {
    n++; const p = `charmnest/sets/${today}/Set-${sp.seq}/${sp.id}/preview.png`;
    st.blobs.set(p, { buf: preview(sp.metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
    const mine = []; for (const [rid, , pcs] of ORDERS) pcs.forEach((pc, i) => { if (pc[2] === sp.id) mine.push({ rid, i, sku: pc[0], pool: poolOf(rid, i), tid: tidOf(rid, i) }); });
    const orders = [...new Set(mine.map(m => m.rid))];
    const cols = 8, placements = mine.map((m, k) => ({ id: 'c' + k, cxPt: 20 + (k % cols) * 24, cyPt: 20 + Math.floor(k / cols) * 24, angle: 0, wPt: 20, hPt: 20 }));
    const charms = mine.map((m, k) => ({ id: 'c' + k, name: `${m.rid} · ${m.sku}`, poolId: m.pool, order: m.rid, sku: m.sku }));
    const rec = { id: sp.id, metal: sp.metal, metalLabel: sp.metal === 'gold' ? 'GF 14/20' : 'SS', day: today, setId: sp.setId, setSeq: sp.seq, sheetIndex: sp.idx, fileBase: `${CODE[sp.metal]}_${today}_Set-${sp.seq}_Sheet-${sp.idx}`, folder: `${sp.metal}_${sp.id}`,
      orders, poolIds: mine.map(m => m.pool), listings: mine.map((m, i) => String(1718000 + (i % 4))), placedCount: mine.length, charmCount: mine.length, density: 0.7, freePt2: 2000, stock: { wIn: 6, hIn: 5, wPt: 432, hPt: 360 },
      outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: `run-${today}`, placements, charms };
    mine.forEach(m => { (runLines[`${m.rid}_${m.tid}`] = { orderId: m.rid, state: 'written', quantity: 1, poolIds: [m.pool], engraveCandidate: false }); });
    if (sp.ready) Object.assign(rec, { processReady: true, processSeals: [{ id: `laserReady-${now}-${n}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }], outputs: Object.assign({}, rec.outputs, { ai: { path: `${sp.id}.ai`, url: blobUrl(`${sp.id}.ai`) } }), label: { files: [{ path: `${sp.id}-qr.png`, url: rec.outputs.preview.url, payload: orders[0], orders }] } });
    else Object.assign(rec, { placedCount: Math.max(1, mine.length - 1) });
    st.put(SHEETS, sp.id, rec);
    for (const m of mine) st.put(POOL, m.pool, { poolId: m.pool, orderId: m.rid, transactionId: m.tid, lineKey: `${m.rid}_${m.tid}`, sku: m.sku, material: sp.metal, copy: 1, quantity: 1, state: 'written', sheetId: sp.id, sheetName: rec.fileBase, updatedAt: now });
  }
  // the pieces that are not on a sheet yet wait in the pool
  for (const [rid, , pcs] of ORDERS) pcs.forEach((pc, i) => { if (!pc[2] && pc[0]) st.put(POOL, poolOf(rid, i), { poolId: poolOf(rid, i), orderId: rid, transactionId: tidOf(rid, i), lineKey: `${rid}_${tidOf(rid, i)}`, sku: pc[0], material: pc[1], copy: 1, quantity: 1, state: 'pooled', updatedAt: now }); });
  st.put(RUNS_C, `run-${today}`, { runId: `run-${today}`, lines: runLines });
  for (const s of [1, 2, 3]) {
    const mine = SHEET_PLAN.filter(x => x.seq === s), ready = mine.every(x => x.ready);
    st.put(SETS, 'set-r2-' + s, { setId: 'set-r2-' + s, seq: s, day: today, runId: `run-${today}`, name: `Set-${s}`, sheetIds: mine.map(x => x.id), materials: mine.map(x => x.metal), orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now - s * 1000), createdAt: Timestamp.fromMillis(now - 86400000),
      ...(ready ? { processReady: true, processSeals: [{ id: `laserReady-${now}-s${s}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }] } : {}) });
  }
}
/** The order objects the page's rows are made of (injected: the fixture has no Etsy receipts). */
function orderObjects() {
  const SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000), DAY = 86400;
  return ORDERS.map(([rid, buyer, pcs]) => ({
    order: { receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
      lines: pcs.map(([sku, metal], i) => ({ transactionId: tidOf(rid, i), listingId: '18000' + tidOf(rid, i).slice(-5), sku, title: (sku || 'Unknown charm').replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metal === 'gold' ? '14k Gold Filled' : 'Sterling Silver' }], metalKey: metal, metalLabel: metal === 'gold' ? 'GF 14/20' : 'SS', personalization: [], buyerMessage: '' })) },
    pieces: pcs.map(([, , sheet], i) => ({ pool: poolOf(rid, i), sheet, tid: tidOf(rid, i) }))
  }));
}

/* ═════ B · the 300-sheet Library ═════ */
function seedMany(st, blobUrl, n = 300) {
  const now = Date.now(), p = `charmnest/sets/${today}/many/preview.png`;
  st.blobs.set(p, { buf: preview('gold', 3), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
  const url = blobUrl(p), lines = {};
  for (let i = 0; i < n; i++) {
    const setNo = (i / 3 | 0) + 1, k = i % 3, metal = k === 1 ? 'silver' : 'gold', id = 'm' + String(i).padStart(3, '0'), setId = 'set-m-' + setNo;
    const orders = Array.from({ length: 6 }, (_, j) => String(4100000000 + i * 10 + j)), pool = orders.map(o => `${o}_${o}1_1`);
    orders.forEach((o, j) => { lines[`${o}_${o}1`] = { orderId: o, state: 'written', quantity: 1, poolIds: [pool[j]], engraveCandidate: false }; });
    st.put(SHEETS, id, { id, metal, metalLabel: metal, day: today, setId, setSeq: setNo, sheetIndex: k + 1, fileBase: `${CODE[metal]}_${today}_Set-${setNo}_Sheet-${k + 1}`, folder: `${metal}_${id}`, orders, poolIds: pool, listings: orders.map((o, j) => String(1718000 + (j % 4))), placedCount: i % 2 ? 5 : 6, charmCount: 6, density: .7, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - i * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + i), archived: false, runId: `run-${today}` });
    if (k === 2 || n - i <= 3) {
      const setIds = [...Array(3)].map((_, q) => 'm' + String(i - k + q).padStart(3, '0')).filter(x => st.doc(SHEETS, x));
      st.put(SETS, setId, { setId, seq: setNo, day: today, runId: `run-${today}`, name: `Set-${setNo}`, sheetIds: setIds, materials: ['gold', 'silver'], orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now - setNo * 1000), createdAt: Timestamp.fromMillis(now - 86400000) });
    }
  }
  st.put(RUNS_C, `run-${today}`, { runId: `run-${today}`, lines });
}

/* ═════ the page, with its instruments ═════ */
const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 60)); } };
const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
const IGNORE = /firebase stub|Failed to load resource|net::ERR_|favicon/i;
const norm = s => String(s).replace(/\d{6,}/g, 'N').replace(/\b[0-9a-f]{8,}\b/g, 'H').replace(/:\d+:\d+/g, ':L').slice(0, 220);

async function openPage(browser, srv, o = {}) {
  const ctx = await browser.newContext({ viewport: o.viewport || { width: 1440, height: 1700 }, reducedMotion: o.reduced ? 'reduce' : 'no-preference' });
  await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
  await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
  await ctx.addInitScript(() => {
    try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Tester'); localStorage.setItem('cn.mail.station', JSON.stringify('k-round2-smoke')); sessionStorage.setItem('__seeded', '1'); } } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    window.__unhandled = []; addEventListener('unhandledrejection', e => { window.__unhandled.push(String((e.reason && (e.reason.message || e.reason)) || 'rejection')); });
  });
  const page = await ctx.newPage(), h = { ctx, page, errors: [], calls: [], marks: {} };
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => h.errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !IGNORE.test(m.text())) h.errors.push('console: ' + m.text().slice(0, 300)); });
  page.on('request', r => {
    if (r.method() !== 'POST' || !/\/\.netlify\/functions\//.test(r.url())) return;
    let b = {}; try { b = r.postDataJSON() || {}; } catch (_) { /* not json */ }
    h.calls.push({ t: Date.now(), fn: r.url().split('/.netlify/functions/')[1].split('?')[0], op: b.op || (Array.isArray(b.timeline) ? 'timeline' : null), recordSeals: b.recordSeals, wantRevs: !!b.wantRevs, ifRevs: !!b.ifRevs, by: b.by, steps: Array.isArray(b.steps) ? b.steps.map(s => s.type).join('+') : undefined });
  });
  h.mark = name => { h.marks[name] = h.calls.length; };
  h.since = name => h.calls.slice(h.marks[name] || 0);
  h.goto = async hash => {
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html${hash || ''}`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.LibraryDone && CN.S.cloud.ok === true, null, { timeout: 60000 });
  };
  h.close = () => ctx.close();
  return h;
}
const READ_OPS = new Set(['ping', 'lookupCharms', 'listCharms', 'listSheets', 'getSheet', 'backPreview', 'sheetPdf', 'getCalibration', 'jobList', 'getJob', 'getAgent', 'masterGet', 'masterGetMany', 'masterList', 'masterListFiles', 'poolList', 'poolGet', 'sandboxStatus', 'backList', 'setGet', 'setList', 'releaseGet', 'runGet', 'runList', 'history', 'laserDoneList', 'findSheets', 'aliasGet', 'noDesignGet', 'optionMapGet', 'customGet', 'customSheetGet', 'customReadGet', 'cancelList', 'cancelFates', 'timelineGet', 'cancelCheck', 'sessionsList', 'flowState', 'getShapeGuidance', 'listingPhotos']);
/** A call that changes cloud state (an op not known to be a read; laserStatus only with recordSeals:true; a station POST). */
// (an op that is not in the list but is named get.../list.../find... is a read: new reading ops of the round, e.g. getOrderPieces, are not writes)
const isRead = op => READ_OPS.has(op) || /^(get|list|find)[A-Z]/.test(String(op || ''));
const isWrite = c => c.fn === 'charmNestLibrary' ? (c.op === 'laserStatus' ? c.recordSeals === true : !isRead(c.op)) : c.fn === 'firebaseOrders';
const sealCheck = c => c.fn === 'charmNestLibrary' && c.op === 'laserStatus' && c.recordSeals === true;

const dupIds = page => page.evaluate(() => { const m = {}; for (const e of document.querySelectorAll('[id]')) m[e.id] = (m[e.id] || 0) + 1; return Object.entries(m).filter(([, n]) => n > 1).map(([k, n]) => k + '×' + n); });
const leftovers = page => page.evaluate(() => {
  const sel = '.mGhost,.mLand,.mPlus,.mLift,.mdGhost,.dndLift,.fxBox,.dndDock,[data-fx-hide],[data-dnd-state],.dndSource,.sealTool,.sealRing,html[data-library-drag]';
  return [...document.querySelectorAll(sel)].map(e => (e.className && e.className.baseVal === undefined ? e.className : e.tagName) || e.tagName).slice(0, 8).concat(window.LibraryFx && LibraryFx.active && LibraryFx.active() ? ['LibraryFx.active=' + LibraryFx.active()] : []);
});
const quiet = async (page, ms = 800) => { await until(() => page.evaluate(() => (!window.LibraryFx || !LibraryFx.active || LibraryFx.active() === 0) && !(window.LibraryDnd && LibraryDnd.busy && LibraryDnd.busy())), 15000, 'flights ended').catch(() => {}); await page.waitForTimeout(ms); };
const cardIds = page => page.evaluate(() => [...document.querySelectorAll('#libBody .libCard[data-id]')].map(c => c.dataset.id));
const frames = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
const box = (page, sel) => page.evaluate(s => { const e = document.querySelector(s); if (!e) return null; const r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, l: r.left, t: r.top, w: r.width, h: r.height }; }, sel);

/** What every phase must leave true: no errors, no doubled ids, nothing left over. */
async function sweep(h, phase) {
  const d = await dupIds(h.page); out.dupIds[phase] = d;
  const lo = await leftovers(h.page); hard(`${phase}: no flight copy, dock or hidden original left behind`, lo.length === 0, lo.join(', '));
  const un = await h.page.evaluate(() => window.__unhandled.slice());
  const errs = h.errors.concat(un.map(x => 'unhandledrejection: ' + x));
  out.errors.push(...errs.map(e => ({ phase, e: norm(e) })));
}
function judge(baseline) {
  const baseErr = new Set(((baseline && baseline.errors) || []).map(x => x.e)), seen = new Set(), fresh = [], old = [];
  for (const x of out.errors) { if (seen.has(x.e)) continue; seen.add(x.e); (baseErr.has(x.e) ? old : fresh).push(`${x.phase}: ${x.e}`); }
  hard('no console errors, page errors or unhandled promise rejections (beyond the baseline\'s)', fresh.length === 0, fresh.join(' || '));
  if (old.length) info('pre-existing errors (also in the baseline)', old);
  const baseDup = (baseline && baseline.dupIds) || {};
  const bad = [];
  for (const [ph, ids] of Object.entries(out.dupIds)) { const b = new Set(baseDup[ph] || (baseline ? [] : ['*'])); for (const id of ids) if (!b.has(id.replace(/×\d+$/, '')) && !b.has(id) && !b.has('*')) bad.push(`${ph}: ${id}`); }
  const all = Object.entries(out.dupIds).filter(([, v]) => v.length).map(([k, v]) => k + ': ' + v.join(','));
  hard('no doubled DOM ids (beyond the baseline\'s)', bad.length === 0 || (!baseline && false), bad.join(' | '));
  if (all.length) info('doubled ids present', all);
}

/* ═════ the run ═════ */
module.exports = { seedSmall, seedMany, orderObjects, ORDERS, SHEET_PLAN };
if (require.main === module) (async () => {
  const baseline = arg('baseline') && fs.existsSync(arg('baseline')) ? JSON.parse(fs.readFileSync(arg('baseline'), 'utf8')) : null;
  try { out.commit = require('child_process').execSync('git rev-parse --short HEAD', { cwd: root }).toString().trim(); } catch (_) { /* no git */ }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const shot = async (page, name) => { if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
  try {
    if (!flag('timing-only')) await smallWorld(browser, shot);
    if (!flag('skip-many')) await manyWorld(browser, baseline, shot);
    judge(baseline);
  } finally { await browser.close(); }
  const hardFails = out.checks.filter(c => c.kind === 'HARD' && !c.ok), open = out.checks.filter(c => c.kind === 'ACCEPT' && !c.ok);
  if (arg('out')) fs.writeFileSync(arg('out'), JSON.stringify(out, null, 1));
  console.log(`\nround2-smoke: ${out.checks.filter(c => c.kind === 'HARD').length - hardFails.length} hard checks ok, ${hardFails.length} FAILED${hardFails.length ? ' (' + hardFails.map(c => c.name).join('; ') + ')' : ''}; ${out.checks.filter(c => c.kind === 'ACCEPT').length - open.length} accept ok, ${open.length} open${open.length ? ' (' + open.map(c => c.name).join('; ') + ')' : ''}`);
  process.exitCode = hardFails.length ? 1 : 0;
})().catch(e => { console.error(e); process.exitCode = 1; });

/* ── A ── */
async function smallWorld(browser, shot) {
  const srv = await start({ receipts: [] }); const { st } = srv;
  try {
    seedSmall(st, srv.blobUrl);
    const h = await openPage(browser, srv); const { page } = h;
    await h.goto('#library');
    await page.waitForSelector('#libBody .setCard', { timeout: 30000 });
    await page.waitForFunction(() => document.querySelectorAll('#libBody .setCard').length >= 3 && document.querySelectorAll('#libBody .libCard[data-id]').length >= 4, null, { timeout: 20000 });
    // the order rows the page would hold (the fixture has no Etsy receipts)
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const { order, pieces } of orders) order.lines.forEach((line, i) => {
        const key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pieces[i].sheet ? 'written' : 'pooled', reason: null, claimedBy: null, poolIds: [pieces[i].pool], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row);
      });
      Orders.interpretAll();
    }, { orders: orderObjects() });
    await page.waitForTimeout(600);
    await shot(page, 'A01-library');
    info('library sheet cards', await cardIds(page));
    const rows = await page.evaluate(() => Orders.rows().map(r => [r.key, r.state, (r.problems || []).map(p => p.kind || p.key || p.code || String(p).slice(0, 20)).join('+')]));
    info('rows after interpretAll', rows.filter(r => /^41702500(51|52|01)/.test(r[0])));

    // 1 · looked at only: a live read cycle goes by, nothing is written
    // (the rows above were handed to the page by the test: what the page does by itself about new rows - arrivals, custom readings - is over before this)
    await page.waitForTimeout(3500);
    h.mark('look');
    await page.waitForTimeout(3500);                                           // (the live loop reads about every 3 s; a busy machine may be late: it is waited for, up to 15 s)
    await until(() => h.since('look').some(c => c.fn === 'charmNestLibrary' && c.op === 'laserStatus' && c.wantRevs), 15000, 'a live read').catch(() => {});
    const live = h.since('look').filter(c => c.fn === 'charmNestLibrary' && c.op === 'laserStatus');
    hard('the Library follows the cloud: a live read was made (the loop is running)', live.some(c => c.wantRevs), `${live.length} laserStatus calls, ${live.filter(c => c.wantRevs).length} live`);
    hard('a live read never sends recordSeals:true', !h.calls.some(c => c.op === 'laserStatus' && (c.wantRevs || c.ifRevs) && c.recordSeals === true), JSON.stringify(h.calls.filter(c => c.op === 'laserStatus' && c.recordSeals === true)));
    const wrote = h.since('look').filter(isWrite);
    hard('no write call while the Library is only looked at', wrote.filter(c => !sealCheck(c)).length === 0, wrote.map(c => c.fn + ':' + c.op).join(','));
    info('slow seal checks (laserStatus recordSeals:true) while looking', h.since('look').filter(sealCheck).length);
    await sweep(h, 'A1 library looked at');

    // 2 · the set card of the two-sheet set
    const setInfo = await page.evaluate(() => {
      const c = document.querySelector('#libBody .setCard:has(.libCard[data-id="gfA1"])'); if (!c) return null;
      return { sheets: [...c.querySelectorAll('.libCard[data-id]')].map(x => x.dataset.id), approveButtons: c.querySelectorAll('[data-approve-btn]').length, setLevelApprove: c.querySelectorAll(':scope > [data-approve-for], :scope > .approveBox, :scope > .sh .approveBox').length,
        rails: c.querySelectorAll('.flowBox').length, setLevelRail: c.querySelectorAll(':scope > .flowBox, :scope > .sh .flowBox, :scope > .sh ~ .flowBox').length, text: c.innerText.replace(/\s+/g, ' ').slice(0, 400) };
    });
    hard('the 2-sheet set is on the page with both sheets', !!setInfo && setInfo.sheets.includes('gfA1') && setInfo.sheets.includes('ssA1'), JSON.stringify(setInfo && setInfo.sheets));
    info('set card text', setInfo && setInfo.text);
    accept('7/3 · a multi-sheet set has no set-level Approve button and no set-level rail (each sheet has its own)', !!setInfo && setInfo.setLevelApprove === 0 && setInfo.setLevelRail === 0, JSON.stringify({ approveButtons: setInfo && setInfo.approveButtons, setLevelApprove: setInfo && setInfo.setLevelApprove, rails: setInfo && setInfo.rails, setLevelRail: setInfo && setInfo.setLevelRail }));
    accept('2 · the word "lines" is not shown for an order\'s pieces in the Library', !(setInfo && /\bother lines?\b|\blines? (is|are) still\b/i.test(setInfo.text)), setInfo && (/\b(other )?lines?\b/i.exec(setInfo.text) || [''])[0]);

    // 3 · an order window on each piece (Sheet tab): the piece's own sheet, never the other piece's
    const WANT = {};                                                           // pool -> sheet the truth says
    for (const [rid, , pcs] of ORDERS) pcs.forEach((pc, i) => { WANT[poolOf(rid, i)] = pc[2]; });
    h.mark('windows');
    const SHARED = [P(1), P(2), P(3), P(4), P(51), P(52), P(11)];
    const seen = [];
    for (const rid of SHARED) {
      const pcs = ORDERS.find(o => o[0] === rid)[2];
      for (let i = 0; i < pcs.length; i++) {
        const key = `${rid}_${tidOf(rid, i)}`, want = pcs[i][2];
        try {
          await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), key);
          await until(() => page.evaluate(() => OrderWin.isOpen()), 8000, 'order window open');
          // (the sheet is found, and drawn, or the plate says there is none: the window then holds its piece list)
          await until(() => page.evaluate(() => { const w = document.getElementById('owPlateWait'); return (!w || !w.offsetParent) && (!!(window.OrderWin._sheet && OrderWin._sheet()) || !!document.querySelector('#orderWin .owPlateNone') || !!document.querySelector('#orderWin .owPieces li')); }), 12000, 'the Sheet tab settled').catch(() => {});
          await page.waitForTimeout(600);
          const got = await page.evaluate(() => {
            const inf = OrderWin._sheet && OrderWin._sheet(); const panel = document.getElementById('owSheetPanel');
            const pieces = [...document.querySelectorAll('#orderWin .owPieces li')].map(li => li.innerText.replace(/\s+/g, ' ').trim());
            const btns = [...document.querySelectorAll('#orderWin [data-sheet-btn], #orderWin .owSheetBtn, #orderWin [data-open-sheet], #orderWin button')].filter(b => /sheet/i.test(b.textContent) && b.offsetParent).map(b => (b.textContent.trim().slice(0, 28)) + (b.disabled || b.getAttribute('aria-disabled') === 'true' ? ' [off]' : ''));
            return { sheet: inf && inf.sheet && inf.sheet.id || null, view: OrderWin.view(), title: (document.getElementById('owTitle') || {}).textContent || '', pieces, btns: btns.slice(0, 8), tab: (document.querySelector('#orderWin .owTabsV [aria-selected=true]') || {}).textContent || '', panel: panel ? panel.innerText.replace(/\s+/g, ' ').slice(0, 260) : '' };
          });
          seen.push({ key, want, got });
          if (!i && rid === P(1)) await shot(page, 'A02-order-window-piece1');
          if (!want) {
            // 5 · the same piece opened the ordinary way: its Sheet button, Sheet tab and sheet chip are greyed with a reason, and a click on them goes nowhere
            await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* closed */ } }); await page.waitForTimeout(450);
            await page.evaluate(k => OrderWin.open(k), key);
            await until(() => page.evaluate(() => OrderWin.isOpen() && !!document.querySelector('#owNowCard [data-go="sheet"]')), 8000, 'order window open (info view)');
            await page.waitForTimeout(2200);
            const ctl = await page.evaluate(() => {
              const q = s => document.querySelector(s), t = q('.owTabsV [data-ow-view="sheet"]'), btn = q('#owNowCard [data-go="sheet"]');
              const off = e => !!e && e.getAttribute('aria-disabled') === 'true';
              return { view: OrderWin.view(), btnOff: off(btn), btnWhy: btn ? btn.dataset.why || '' : '', tabOff: off(t), chips: [...document.querySelectorAll('#owNowCard .owShChip')].map(c => ({ t: c.textContent.replace(/\s+/g, ' ').trim(), off: off(c) })) };
            });
            await page.click('#owNowCard [data-go="sheet"]', { force: true }).catch(() => {}); await page.waitForTimeout(700);
            await page.click('.owTabsV [data-ow-view="sheet"]', { force: true }).catch(() => {}); await page.waitForTimeout(700);
            ctl.afterClicks = await page.evaluate(() => ({ view: OrderWin.view(), sheet: !!(OrderWin._sheet && OrderWin._sheet()) }));
            seen[seen.length - 1].ctl = ctl;
            if (!out.info.ctlShot) { out.info.ctlShot = 1; await shot(page, 'A02b-unnested-piece-sheet-controls'); }
          }
        } catch (e) { seen.push({ key, want, err: e.message }); }
        await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* closed */ } }); await page.waitForTimeout(350);
      }
    }
    out.info.windows = seen;
    hard('an order window opens on every piece of the shared orders (no error, no hang)', seen.every(s => !s.err), seen.filter(s => s.err).map(s => s.key + ': ' + s.err).join(' | '));
    const wrongSheet = seen.filter(s => !s.err && s.want && s.got.sheet && s.got.sheet !== s.want);
    accept('1/5 · each piece\'s Sheet tab shows that piece\'s own sheet (the sheet the pool says)', wrongSheet.length === 0, wrongSheet.map(s => `${s.key}: wants ${s.want}, shows ${s.got.sheet}`).join(' | '));
    // (a piece on no sheet, opened the ordinary way: every Sheet door greyed with its reason, clicks go nowhere; opened straight onto the Sheet view
    //  with all pieces in front, the window may draw the sheets of the pieces that have one, and then its piece list says this piece is not on a sheet yet)
    const noSheet = seen.filter(s => !s.err && !s.want);
    const notGreyed = noSheet.filter(s => !s.ctl || !(s.ctl.btnOff && s.ctl.tabOff && /not on a sheet yet/i.test(s.ctl.btnWhy) && s.ctl.chips.some(c => /not on a sheet yet/i.test(c.t) && c.off)) || s.ctl.afterClicks.view !== 'info' || s.ctl.afterClicks.sheet);
    accept('5 · a piece that is on no sheet (unnested / no SKU): its Sheet button, Sheet tab and sheet chip are greyed with "not on a sheet yet", and clicking them goes nowhere', noSheet.length > 0 && notGreyed.length === 0, noSheet.length ? notGreyed.map(s => `${s.key}: ${JSON.stringify(s.ctl)}`).join(' | ') : 'the fixture has no piece on no sheet');
    const claims = noSheet.filter(s => s.got.sheet && !s.got.pieces.some(t => /not on a sheet yet/i.test(t)));
    accept('5 · the Sheet view of an order never presents the other piece\'s sheet as this piece\'s: its piece list says "not on a sheet yet" for the piece that is on none', claims.length === 0, claims.map(s => `${s.key}: shows ${s.got.sheet}, list: ${s.got.pieces.join(' / ')}`).join(' | '));
    const lies = seen.filter(s => !s.err && s.want && s.got.pieces.some(t => /not on a sheet yet/i.test(t)) && ORDERS.find(o => s.key.startsWith(o[0]))[2].every(pc => pc[2]));
    accept('1 · a piece list never says "not on a sheet yet" for a piece that is on a sheet', lies.length === 0, lies.map(s => s.key + ': ' + s.got.pieces.join(' / ')).join(' | '));
    const wrote2 = h.since('windows').filter(isWrite);
    hard('no write call while order windows are only opened and looked at', wrote2.filter(c => !sealCheck(c)).length === 0, wrote2.map(c => c.fn + ':' + c.op).join(','));
    await sweep(h, 'A3 order windows');

    // 4 · the sheet window of each sheet of the set opens and closes
    h.mark('sheetwin');
    for (const id of ['gfA1', 'ssA1']) {
      const opened = await page.evaluate(async sid => { try { const f = window.openLibrarySheet || window.Sets && Sets.openSheet; if (!f) return 'no opener'; await f(sid); return 'ok'; } catch (e) { return 'error: ' + e.message; } }, id);
      if (opened === 'ok') { await page.waitForTimeout(900); await page.keyboard.press('Escape'); await page.evaluate(() => { document.querySelectorAll('dialog[open]').forEach(d => { try { d.close(); } catch (_) { /* gone */ } }); }); await page.waitForTimeout(400); }
      hard(`the sheet window of ${id} opens`, opened === 'ok', opened);
    }
    const wrote3 = h.since('sheetwin').filter(isWrite);
    hard('no write call while sheet windows are only opened and looked at', wrote3.filter(c => !sealCheck(c)).length === 0, wrote3.map(c => c.fn + ':' + c.op).join(','));
    await sweep(h, 'A4 sheet windows');

    // 5 · a sheet dragged out of its set: to "New set", then onto another set. The two sheets of set 1 share multi-piece orders, so
    // the cardinal rule says they stay together (round2 point 6); on the old main a drop onto another set simply moves the sheet.
    await page.evaluate(() => { window.scrollTo(0, 0); document.getElementById('stage') && (document.getElementById('stage').scrollTop = 0); });
    const IDS = ['gfA1', 'ssA1', 'gfB1', 'gfR1'];
    const exactlyOnce = a => new Set(a).size === a.length && IDS.every(i => a.includes(i));
    const setsOf = () => page.evaluate(() => { const o = {}; for (const id of ['gfA1', 'ssA1']) { const c = document.querySelector(`#libBody .libCard[data-id="${id}"]`), s = c && c.closest('.setCard'); o[id] = s && s._laserSet ? s._laserSet.setId : null; } return o; });
    const before = await cardIds(page);
    hard('every sheet card is on the page exactly once before the drag', exactlyOnce(before), before.join(','));
    async function dragTo(chipKey, label) {
      let note = '', chipState = '', chipWhy = '';
      try {
        const a = await box(page, '#libBody .libCard[data-id="ssA1"]'); const x0 = a.l + 24, y0 = a.t + 14;
        await page.mouse.move(x0, y0); await page.mouse.down(); await page.mouse.move(x0 + 10, y0 + 8, { steps: 3 });
        await page.waitForSelector('.dndDock .dndChip', { timeout: 5000 });
        const chip = await page.evaluate(k => { const c = document.querySelector(`.dndChip[data-key="${k}"]`); return c ? { state: c.dataset.state, why: (c.querySelector('.dndChipSub') || {}).textContent || '' } : null; }, chipKey);
        if (!chip) throw new Error('no chip ' + chipKey);
        chipState = chip.state; chipWhy = chip.why;
        const target = await box(page, `.dndChip[data-key="${chipKey}"]`);
        await page.mouse.move(target.x, target.y, { steps: 12 }); await frames(page); await page.waitForTimeout(150);
        await shot(page, `A03-drag-over-${label}`);
        await page.mouse.up();
        await page.waitForTimeout(500);
        await shot(page, `A04-after-drop-${label}`);
        await quiet(page, 1500);
        note = 'dropped';
      } catch (e) { note = 'drag failed: ' + e.message.split('\n')[0]; try { await page.mouse.up(); } catch (_) { /* up */ } }
      await page.waitForTimeout(2200);
      return { note, chipState, chipWhy };
    }
    const closeOverlays = async () => {
      await page.evaluate(() => { try { if (window.SharedOrdersModal && SharedOrdersModal.isOpen && SharedOrdersModal.isOpen()) SharedOrdersModal.close(); } catch (_) { /* closed */ } });
      await page.waitForTimeout(900);                                         // (its way out is animated)
      await page.keyboard.press('Escape').catch(() => {}); await page.waitForTimeout(500);
      await page.evaluate(() => { document.querySelectorAll('dialog[open]').forEach(d => { try { d.close(); } catch (_) { /* gone */ } }); }); await page.waitForTimeout(500);
    };
    const modalUp = () => page.evaluate(() => !!(document.querySelector('[data-shared-orders-modal], .sharedOrdersModal, #sharedOrdersModal, dialog.sharedOrders[open], .soModal, [data-so-modal]')) || !!(window.SharedOrdersModal && SharedOrdersModal.isOpen && SharedOrdersModal.isOpen()));
    const hasModule = () => page.evaluate(() => !!window.SharedOrdersModal);

    // 5a · "New set"
    h.mark('drag1');
    const d1 = await dragTo('newSet', 'new-set');
    hard('a sheet can be picked up and put down over "New set" (the drag layer works)', d1.note === 'dropped', d1.note);
    info('drag to New set: chip', { state: d1.chipState, why: d1.chipWhy });
    const after1 = await cardIds(page), where1 = await setsOf();
    hard('every sheet card is on the page exactly once after the drag to New set', exactlyOnce(after1), after1.join(','));
    info('after the drag to New set: set of gfA1 / ssA1', where1);
    accept('6 · a sheet that shares multi-piece orders cannot be dragged out into a new set (it stays with its set)', where1.gfA1 && where1.gfA1 === where1.ssA1, JSON.stringify(where1));
    const m1 = await modalUp();
    const writes1 = h.since('drag1').filter(isWrite).filter(c => !sealCheck(c));
    info('writes after the drag to New set', writes1.map(c => c.fn + ':' + c.op + (c.steps ? '[' + c.steps + ']' : '')));
    if (where1.gfA1 === where1.ssA1) hard('a refused drag to New set writes nothing', writes1.length === 0, writes1.map(c => c.fn + ':' + c.op).join(','));
    await closeOverlays();
    await sweep(h, 'A5a drag out to a new set');

    // 5b · another set (an allowed place on the old main)
    const set2 = await setsOf();
    if (set2.ssA1 === 'set-r2-1') {
      h.mark('drag2');
      const d2 = await dragTo('set:set-r2-2', 'other-set');
      hard('a sheet can be picked up and put down over another set (the drag layer works)', d2.note === 'dropped', d2.note);
      info('drag to Set 2: chip', { state: d2.chipState, why: d2.chipWhy });
      const after2 = await cardIds(page), where2 = await setsOf(), m2 = await modalUp();
      hard('every sheet card is on the page exactly once after the drag to another set', exactlyOnce(after2), after2.join(','));
      info('after the drag to Set 2: set of gfA1 / ssA1; modal', { where: where2, modal: m2, sharedOrdersModalModule: await hasModule() });
      accept('6 · the cardinal rule: dragging a sheet that shares multi-piece orders onto another set is refused and the sheets stay together', where2.gfA1 && where2.gfA1 === where2.ssA1, JSON.stringify(where2));
      accept('6 · the refusal is explained in a modal with the shared orders', m2, m2 ? 'modal found' : 'no shared-orders modal on the page (module loaded: ' + await hasModule() + ')');
      const writes2 = h.since('drag2').filter(isWrite).filter(c => !sealCheck(c));
      info('writes after the drag to Set 2', writes2.map(c => c.fn + ':' + c.op + (c.steps ? '[' + c.steps + ']' : '')));
      if (where2.gfA1 === where2.ssA1) hard('a refused drag onto another set writes nothing', writes2.length === 0, writes2.map(c => c.fn + ':' + c.op).join(','));
      if (m2) {
        const listed = () => page.evaluate(() => ({ cards: document.querySelectorAll('.soCard').length, tiles: document.querySelectorAll('.soCard .soTile').length, opens: document.querySelectorAll('.soCard [data-open]').length, text: (document.querySelector('.soHead, .soTitle, dialog.soDlg h2, [data-so-title]') || {}).textContent || '' }));
        await until(async () => (await listed()).cards > 0, 8000, 'the modal lists its orders').catch(() => {});
        const L1 = await listed();
        info('shared-orders modal', L1);
        accept('6 · the modal lists exactly the 4 orders the two sheets share, each with thumbnails and a link to the order', L1.cards === 4 && L1.tiles >= 8 && L1.opens === 4, JSON.stringify(L1));
        await shot(page, 'A06-shared-orders-modal');
        const n0 = L1.cards;
        // an order is opened from it, and the way back brings the modal up again with the same orders
        await page.evaluate(() => document.querySelector('.soCard [data-open]').click());
        const opened = await until(() => page.evaluate(() => !!(window.OrderWin && OrderWin.isOpen())), 8000, 'the order opens from the modal').then(() => true, () => false);
        await page.waitForTimeout(1200);
        const back = await page.evaluate(() => !!document.getElementById('soBack'));
        await shot(page, 'A07-order-opened-from-modal');
        accept('6 · an order opened from the modal shows the order view with a way back to the modal', opened && back, JSON.stringify({ opened, back }));
        if (back) {
          await page.evaluate(() => document.getElementById('soBack').click());
          const again = await until(() => page.evaluate(() => !!(window.SharedOrdersModal && SharedOrdersModal.isOpen && SharedOrdersModal.isOpen())), 8000, 'back to the modal').then(() => true, () => false);
          await page.waitForTimeout(1000);
          const L2 = await listed();
          accept('6 · going back from the order returns to the modal with the remaining shared orders', again && L2.cards === n0, JSON.stringify({ again, before: n0, after: L2.cards }));
        } else await page.evaluate(() => { try { OrderWin.close(); } catch (_) { /* closed */ } });
      }
      await closeOverlays();
      await sweep(h, 'A5b drag onto another set');
    }

    // 6 · the Approve button on a sheet that is not ready
    const approve = await page.evaluate(() => [...document.querySelectorAll('#libBody [data-approve-btn]')].map(b => ({ id: (b.closest('[data-approve-for]') || {}).dataset && b.closest('[data-approve-for]').dataset.approveFor, disabled: b.disabled || b.getAttribute('aria-disabled') === 'true', text: b.textContent.trim() })));   // (a grey button is aria-disabled now: it keeps the keyboard)
    info('approve buttons', approve);
    h.mark('approve');
    const target2 = approve.find(a => /sheet:gfB1/.test(a.id || '') && !a.disabled) || approve.find(a => !a.disabled);
    if (target2) {
      await page.evaluate(id => { const b = document.querySelector(`#libBody [data-approve-for="${id}"] [data-approve-btn]`); b && b.scrollIntoView({ block: 'center' }); }, target2.id);
      await page.waitForTimeout(300);
      await page.click(`#libBody [data-approve-for="${target2.id}"] [data-approve-btn]`, { timeout: 5000 }).catch(e => info('approve click failed', e.message.split('\n')[0]));
      await page.waitForTimeout(1800);
      await shot(page, 'A05-after-approve');
      await quiet(page, 1500);
      const calls = h.since('approve').filter(c => c.fn === 'charmNestLibrary' && !isRead(c.op) && !(c.op === 'laserStatus' && c.recordSeals !== true));
      info('calls after Approve (' + target2.id + ')', calls.map(c => c.op + (c.steps ? '[' + c.steps + ']' : '')));
      hard('Approve for laser cutting: one press, one request (a double press makes no second)', calls.filter(c => c.op === 'flowApply').length <= 1, calls.map(c => c.op).join(','));
    } else hard('an Approve button is on a sheet that is not ready', false, JSON.stringify(approve));
    // nothing else changed shape: the cards are all still there
    const after2 = await cardIds(page);
    hard('every sheet card is still on the page exactly once after Approve', new Set(after2).size === after2.length && ['gfA1', 'ssA1', 'gfB1', 'gfR1'].every(i => after2.includes(i)), after2.join(','));
    await sweep(h, 'A6 approve');
    await h.close();
  } finally { srv.close(); }
}

/* ── B ── */
async function manyWorld(browser, baseline, shot) {
  const srv = await start({ receipts: [] }); const { st } = srv;
  try {
    seedMany(st, srv.blobUrl, 300);
    const runs = []; let h;
    for (let r = 0; r < RUNS; r++) {
      h = await openPage(browser, srv, { viewport: { width: 1440, height: 900 } });
      await h.goto('');                                                         // (the page opens on the nest; the Library is opened as a person would)
      await h.page.waitForTimeout(1500);
      h.mark('open');
      const ms = await h.page.evaluate(async () => {
        const frames2 = () => new Promise(r2 => requestAnimationFrame(() => requestAnimationFrame(r2)));
        const t0 = performance.now(); CN.setMode('library');
        for (let i = 0; i < 600; i++) { if (document.querySelectorAll('#libBody .libCard[data-id]').length >= 300) break; await new Promise(r2 => setTimeout(r2, 10)); }
        await frames2(); const n = document.querySelectorAll('#libBody .libCard[data-id]').length;
        return { ms: Math.round(performance.now() - t0), n };
      });
      runs.push(ms.ms);
      if (r === 0) hard('a 300-sheet Library draws all 300 sheet cards', ms.n === 300, ms.n);
      if (r < RUNS - 1) await h.close();
    }
    const med = a => { const s = a.slice().sort((x, y) => x - y); return s[s.length >> 1]; };
    out.timing.open300 = { runs, median: med(runs), min: Math.min(...runs) };   // (the best run is the fair one: a busy machine only ever adds time)
    info('Library open time, 300 sheets (ms, runs / median)', out.timing.open300);
    const base = baseline && baseline.timing && baseline.timing.open300;
    if (base) { const b = base.min || base.median, mine = base.min ? out.timing.open300.min : out.timing.open300.median, limit = Math.round(b * 1.2); hard(`Library open time within +20% of the baseline (best run ${mine} ms vs ${b} ms, limit ${limit} ms; median ${out.timing.open300.median} vs ${base.median})`, mine <= limit, `runs ${runs.join(',')} vs baseline ${base.runs.join(',')}`); }
    else info('no baseline timing given', 'the open time was measured, not compared');
    // the last page: it stays open for two live cycles, looked at only
    await h.page.waitForTimeout(6500);
    const live = h.since('open').filter(c => c.op === 'laserStatus');
    hard('300 sheets: live reads carry recordSeals:false, never true', !live.some(c => (c.wantRevs || c.ifRevs) && c.recordSeals === true), live.length + ' laserStatus calls');
    hard('300 sheets: no write call (beyond the slow seal check) while the Library is only looked at', h.since('open').filter(isWrite).filter(c => !sealCheck(c)).length === 0, h.since('open').filter(isWrite).map(c => c.fn + ':' + c.op).join(','));
    info('300 sheets: calls while opening and looking (op x count)', h.since('open').reduce((o, c) => { const k = c.fn + ':' + c.op; o[k] = (o[k] || 0) + 1; return o; }, {}));
    await shot(h.page, 'B01-library-300');
    await sweep(h, 'B 300 sheets');
    await h.close();
  } finally { srv.close(); }
}
