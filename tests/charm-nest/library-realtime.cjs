/* The Library follows a Complete Order and a Print QR Label in real time, and costs what CHANGES, not what time passes
   (Paul, 6 Oct 23:58: "It took over 1 min for the Completed/QR Print orders to be reflected as approved in the Library tab
   when looking at the Set of Sheets being released to move onto the Laser Cutting process. Once an order is Completed/QR
   Print then all effects must take place in real-time"; and 7 Oct 00:07: the Google Cloud bill, forecast $519 a month).

   The real Library (charm-nest-1.html with its bridge), the real charmNestLibrary function over the in-memory Firestore,
   in Chromium, with a LARGE shop: a set of four sheets and 96 pieces whose Order check waits on three pieces that are only in
   Review (an SKU no master has), 49 other sets in progress, 25 completed sets (about 300 sheets in all), 359 open orders.
   Two computers (two browser contexts) and a second tab of the first computer have it open. Each press is a real press of
   the button (the order window's Complete Order, its Print QR label with the print dialog stubbed), or the same write made by
   another computer (srv.call: the real handler).

   Timed, from the moment the record was written to the moment the set card (its Order check step, the '!' list of what is
   left, the single Approve for laser cutting gate, the step rail, and the card's area) shows it:
     · the computer that pressed         ≤ SAME_MS   (1 s)       · another computer    ≤ CROSS_MS  (3 s)
     · a second tab of that computer     ≤ TAB_MS    (1 s, through the BroadcastChannel, never the 3 s loop)
     · a Library tab hidden, then shown  ≤ SHOW_MS   (3 s after it is shown)
   Counted, not timed: the Library's idle read costs ONE document (the cloud's revision) and a few bytes, not one per
   document it is made of; a read after somebody else's unrelated write costs the old probe plus one; a Complete Order of a
   piece of this card costs one full answer and nothing else; the slow check that records seals costs one document when
   nothing changed; the sheets are read for the fields a card uses (field masks ENFORCED by the fake, so a field a card
   needs but the mask leaves out fails here); no Etsy call is made.
   Run once:  PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/library-realtime.cjs  */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const root = path.join(__dirname, '../..');
process.env.CHARM_NEST_DELETE_CODE = 'x-' + Math.random().toString(36).slice(2, 8);
const O = require('./placement-oracle.cjs');
const { Timestamp } = require('./bridge-server.cjs');
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const SAME_MS = 1000, CROSS_MS = 3000, TAB_MS = 1000, SHOW_MS = 3000;
const fails = [], say = (...a) => console.log(...a);
const check = (ok, what, detail) => { if (ok) say('  ok  ', what); else { fails.push(what + (detail ? ' — ' + detail : '')); say('  FAIL', what, detail || ''); } };

/* ── the shop ── */
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets', RUNS = 'Charm_Nest_Runs', POOL = 'Charm_Pool', DAY = '2026-10-06', RUN = 'run-big', tx = n => 5000000000 + n, CODE = { gold: 'GF', silver: 'SS' };
function bigShop() {
  const T_SHEETS = [['T-gf1', 'gold'], ['T-gf2', 'gold'], ['T-ss1', 'silver'], ['T-ss2', 'silver']], BLOCK_AT = [1, 5, 9], now = Date.now();
  const shop = { sheets: [], sets: [], pool: [], runLines: {}, orders: new Map(), target: { setId: 'set-T', sheetIds: T_SHEETS.map(s => s[0]), blockers: [] } };
  let n = 0; const order = rid => { if (!shop.orders.has(rid)) shop.orders.set(rid, { rid, lines: [] }); return shop.orders.get(rid); };
  const addPiece = (sheet, rid, metal) => {
    n++; const key = `${rid}_${tx(n)}`, pid = key + '_1';
    shop.runLines[key] = { orderId: rid, transactionId: String(tx(n)), sku: 'TEST-' + (n % 40), state: 'written', quantity: 1, material: metal, poolIds: [pid], engraveCandidate: false };
    order(rid).lines.push({ n, key, sku: 'TEST-' + (n % 40), metal, pid, kind: 'piece' });
    shop.pool.push({ poolId: pid, orderId: rid, transactionId: String(tx(n)), lineKey: key, sku: 'TEST-' + (n % 40), material: metal, copy: 1, quantity: 1, runId: RUN, state: 'written', sheetId: sheet.id, setId: sheet.setId, sheetName: sheet.fileBase, createdAt: now - 3600e3, updatedAt: now - 600e3 });
    sheet.poolIds.push(pid); if (!sheet.orders.includes(rid)) sheet.orders.push(rid);
  };
  const newSheet = (id, metal, set, idx, o = {}) => {
    const fileBase = `${CODE[metal]}_${DAY}_Set-${set.seq}_Sheet-${idx}`;
    const s = { id, metal, metalLabel: metal, day: DAY, setId: set.setId, setSeq: set.seq, sheetIndex: idx, runId: RUN, fileBase, folder: fileBase, status: 'complete', density: .7, stock: { wPt: 300, hPt: 150, wIn: 6, hIn: 4.5 }, poolIds: [], orders: [], verification: { ok: true }, archived: false, ...o };
    shop.sheets.push(s); set.sheetIds.push(id); return s;
  };
  const T = { setId: 'set-T', seq: 9, sheetIds: [], day: DAY, recent: 5000 }, tSheets = T_SHEETS.map(([id, metal], i) => newSheet(id, metal, T, i + 1));
  for (let i = 0; i < 48; i++) { const rid = String(4172000000 + i); addPiece(tSheets[i % 4], rid, tSheets[i % 4].metal); addPiece(tSheets[(i + 1) % 4], rid, tSheets[(i + 1) % 4].metal); }
  for (const at of BLOCK_AT) {   // a third piece in Review (an unknown SKU, no sheet) holds the order, so the set waits on its Order check
    const rid = String(4172000000 + at); n++; const key = `${rid}_${tx(n)}`, sku = 'NOSKU-' + (shop.target.blockers.length + 1);
    shop.runLines[key] = { orderId: rid, transactionId: String(tx(n)), sku, state: 'unmatched', quantity: 1, material: 'gold', poolIds: [], problems: [{ kind: 'unmatchedSku', reason: 'not in any master file', sku }] };
    order(rid).lines.push({ n, key, sku, metal: 'gold', pid: null, kind: 'unmatched' }); shop.target.blockers.push({ rid, key, n, sku });
  }
  shop.sets.push(T);
  let r1 = 4173000000;
  for (let k = 0; k < 49; k++) {   // 49 other sets in progress, a third of them ready for the laser
    const set = { setId: 'set-' + (100 + k), seq: 10 + k, sheetIds: [], day: DAY, recent: 1 + k }, ready = k % 3 === 0;
    const sh = [0, 1, 2, 3].map(i => newSheet(`S${k}-${i}`, i % 2 ? 'silver' : 'gold', set, i + 1, ready ? {} : { noLabel: true }));
    for (let o = 0; o < 6; o++) { const rid = String(r1++); for (let p = 0; p < 4; p++) addPiece(sh[(o + p) % 4], rid, sh[(o + p) % 4].metal); }
    shop.sets.push(set);
  }
  for (let k = 0; k < 25; k++) {   // 25 completed sets (under Completed)
    const set = { setId: 'set-d' + k, seq: 200 + k, sheetIds: [], day: '2026-09-20', done: true, recent: 0 };
    for (let i = 0; i < 4; i++) { const s = newSheet(`D${k}-${i}`, i % 2 ? 'silver' : 'gold', set, i + 1, { day: '2026-09-20', done: true }); const rid = String(4160000000 + k * 10 + i); n++; s.poolIds.push(`${rid}_${tx(n)}_1`); s.orders.push(rid); }
    shop.sets.push(set);
  }
  for (let e = 0; e < 17; e++) {   // open orders that are only in Review, to make 359
    const rid = String(4174000000 + e); n++; const key = `${rid}_${tx(n)}`, sku = 'NOSKU-x' + e;
    shop.runLines[key] = { orderId: rid, transactionId: String(tx(n)), sku, state: 'unmatched', quantity: 1, material: 'gold', poolIds: [], problems: [{ kind: 'unmatchedSku', reason: 'not in any master file', sku }] };
    order(rid).lines.push({ n, key, sku, metal: 'gold', pid: null, kind: 'unmatched' });
  }
  return shop;
}
function seedCloud(srv, shop) {
  const put = srv.raw.set, now = Date.now(), ts = ms => Timestamp.fromMillis(ms), setBy = new Map(shop.sets.map(s => [s.setId, s]));
  for (const s of shop.sheets) {
    const set = setBy.get(s.setId), rec = { ...s }; delete rec.noLabel; delete rec.done;
    rec.placedCount = rec.charmCount = s.poolIds.length; rec.names = s.orders.join(' '); rec.createdAt = ts(now - 86400e3); rec.updatedAt = ts(now - 3600e3 + (set.recent || 0) * 1000);
    rec.outputs = { preview: { path: s.id + '.png', url: srv.blobUrl(s.id + '.png') }, ai: { path: s.id + '.ai', url: srv.blobUrl(s.id + '.ai') } };
    rec.label = s.noLabel ? { files: [], orders: s.orders } : { files: [{ path: s.id + '-qr.png', url: srv.blobUrl(s.id + '-qr.png'), payload: s.orders[0], orders: s.orders }], orders: s.orders };
    if (s.done) { rec.laserDoneAt = now - 7200e3; rec.laserDoneBy = 'Anna'; }
    rec.charms = s.poolIds.map((p, i) => ({ id: s.id + '-c' + i, poolId: p, order: p.split('_')[0], name: p.split('_')[0] + ' · TEST' }));
    rec.placements = rec.charms.map((c, i) => ({ id: c.id, cxPt: 20 + (i % 6) * 40, cyPt: 20 + Math.floor(i / 6) * 40, angle: 0, wPt: 28, hPt: 28 }));
    put(SHEETS + '/' + s.id, rec);
  }
  for (const set of shop.sets) put(SETS + '/' + set.setId, { setId: set.setId, seq: set.seq, day: set.day, runId: RUN, name: 'Set-' + set.seq, sheetIds: set.sheetIds, materials: ['gold', 'silver'], orders: {}, labels: null, labelFiles: [], status: 'labelled', updatedAt: ts(now - 3600e3 + (set.recent || 0) * 1000), createdAt: ts(now - 86400e3), ...(set.done ? { laserDoneAt: now - 7200e3, laserDoneBy: 'Anna' } : {}) });
  for (const p of shop.pool) put(POOL + '/' + p.poolId, p);
  put(RUNS + '/' + RUN, { runId: RUN, status: 'running', step: 'nest', day: DAY, lines: shop.runLines, orders: [...shop.orders.keys()], sheets: {}, holds: {}, errors: [], resumable: true });
}
/** What a computer that pulled the open orders holds: one row per line. */
function pageRows(shop) {
  const rows = [], BASE_TS = 1790000000;
  for (const o of shop.orders.values()) for (const l of o.lines) {
    const order = { receiptId: o.rid, orderNumber: o.rid, createTs: BASE_TS - 9000, updateTs: BASE_TS, shipBy: BASE_TS + 500000, buyer: { name: 'Buyer ' + o.rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [] };
    const ln = { transactionId: String(tx(l.n)), listingId: '1800' + (l.n % 90), sku: l.sku, title: l.kind === 'unmatched' ? 'Hand made piece ' + l.n : 'Test charm ' + l.n, quantity: 1, variations: [{ name: 'Metal', value: l.metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled' }], metalKey: l.metal, metalLabel: l.metal === 'silver' ? 'Sterling Silver' : '14k Gold Filled', personalization: [] };
    order.lines = [ln]; const un = l.kind === 'unmatched';
    rows.push({ key: l.key, order, line: ln, spec: { designSku: l.sku, quantity: 1, material: l.metal, problems: [] }, problems: un ? [{ kind: 'unmatchedSku', reason: 'not in any master file', sku: l.sku, listingId: ln.listingId, title: ln.title }] : [], state: un ? 'unmatched' : 'written', reason: un ? 'not in any master file' : null, poolIds: l.pid ? [l.pid] : [], engrave: null, material: l.metal, arrivedAt: Date.now() - 7200000 });
  }
  return rows;
}

/* ── the meter: documents read and bytes returned, as Firestore bills them (one document each, a missing one too), and the
      field masks ENFORCED: a read with a mask returns only those fields ── */
function meter(st) {
  const db = st.db, m = { docs: 0, bytes: 0 };
  const top = k => String(k).split('.')[0], maskObj = (d, mask) => { if (!d || !mask || !mask.length) return d; const o = {}; for (const k of mask) { const t = top(k); if (d[t] !== undefined) o[t] = d[t]; } return o; };
  const maskSnap = (s, mask) => (mask && mask.length && s) ? Object.assign(Object.create(s), { data: () => (s.exists ? maskObj(s.data(), mask) : undefined), get: k => (s.exists ? maskObj(s.data(), mask)[k] : undefined) }) : s;
  const add = (d, mask) => { m.docs++; try { m.bytes += d ? JSON.stringify(maskObj(d, mask)).length : 0; } catch (_) {} };
  const origCollection = db.collection.bind(db), origTx = db.runTransaction.bind(db), unwrap = r => (r && r.__raw) || r;
  const wrapRef = ref => { const w = Object.create(ref); w.__raw = ref; w.get = async view => { const s = await ref.get(view); add(s.exists ? s.data() : null, null); return s; }; return w; };
  const wrapQuery = (q, mask) => { const w = {}; for (const k of ['where', 'orderBy', 'limit', 'startAfter']) w[k] = (...a) => wrapQuery(q[k](...a), mask); w.select = (...f) => wrapQuery(q.select(...f), f); w.count = q.count && q.count.bind(q); w.doc = q.doc; w.get = async view => { const r = await q.get(view); for (const d of r.docs) add(d.data(), mask); if (mask && mask.length) { const docs = r.docs.map(d => maskSnap(d, mask)); return { ...r, docs, forEach: fn => docs.forEach(fn) }; } return r; }; return w; };
  db.collection = c => { const col = origCollection(c); return { doc: id => wrapRef(col.doc(id)), where: (...a) => wrapQuery(col.where(...a), null), orderBy: (...a) => wrapQuery(col.orderBy(...a), null), limit: (...a) => wrapQuery(col.limit(...a), null), select: (...f) => wrapQuery(col, f), add: col.add && col.add.bind(col), count: col.count && col.count.bind(col), get: wrapQuery(col, null).get }; };
  db.getAll = async (...refs) => {
    let mask = null; if (refs.length && refs[refs.length - 1] && typeof refs[refs.length - 1].get !== 'function') mask = refs.pop().fieldMask || null;
    const out = await Promise.all(refs.map(r => unwrap(r).get())); out.forEach(s => add(s.exists ? s.data() : null, mask)); return out.map(s => maskSnap(s, mask));
  };
  db.runTransaction = (fn, o) => origTx(t => fn({ ...t, get: async x => { const r = await t.get(unwrap(x)); if (r.docs) r.docs.forEach(d => add(d.data(), null)); else add(r.exists ? r.data() : null, null); return r; }, getAll: async (...refs) => { let mask = null; if (refs.length && refs[refs.length - 1] && typeof refs[refs.length - 1].get !== 'function') mask = refs.pop().fieldMask || null; const out = await Promise.all(refs.map(r => t.get(unwrap(r)))); out.forEach(s => add(s.exists ? s.data() : null, mask)); return out; }, set: t.set, update: t.update, delete: t.delete }), o);
  return { snap: () => ({ docs: m.docs, bytes: m.bytes }), since: a => ({ docs: m.docs - a.docs, bytes: m.bytes - a.bytes }) };
}

/* ── the pages ── */
const PDFMAKE = `window.pdfMake = { createPdf(dd) { return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => {};<\\/script>'], { type: 'text/html' })); } }; } };`;
async function libraryPage(browser, srv, shop, name, ctxOf) {
  const pg = ctxOf ? { ctx: ctxOf.ctx, page: await ctxOf.ctx.newPage(), errors: [] } : await O.openPage(browser, srv, { owner: false, name });
  if (ctxOf) { pg.page.setDefaultTimeout(20000); pg.page.on('pageerror', e => pg.errors.push('page: ' + e.message)); await pg.page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { timeout: 90000 }); await pg.page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && window.OrderWin, null, { timeout: 90000 }); }
  else await pg.ctx.route(u => /pdfmake|vfs_fonts/.test(u.href), r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body: /vfs_fonts/.test(r.request().url()) ? '' : PDFMAKE }));
  const rows = pageRows(shop);
  await pg.page.evaluate(({ rows }) => {
    for (let i = 0; i < 40; i++) window.B.master.entries.set('TEST-' + i, { sku: 'TEST-' + i, updatedAt: 1 });
    window.B.orders.rows = rows; window.B.orders.byKey = new Map(rows.map(r => [r.key, r])); window.B.pool.rows.clear(); window.B.sets.clear(); window.B.run = null; Review.syncOrderItems();
    const card = () => [...document.querySelectorAll('#libBody .setCard')].find(c => c._laserSet && c._laserSet.setId === 'set-T');
    window.__lr = {
      // what the set card says now: its area, the step rail, the Approve gate, and the order ids its '!' list of what is left names
      sig() {
        const c = card(); if (!c) return null;
        const box = c.querySelector('.approveBox[data-level="set"]'), p = document.getElementById('libIssuesPanel'), ids = new Set();
        if (p) { p.querySelectorAll('[data-issue-order]').forEach(n => ids.add(n.dataset.issueOrder)); p.querySelectorAll('[data-issue-orders]').forEach(n => String(n.dataset.issueOrders).split(',').forEach(x => x && ids.add(x))); }
        const r = c.getBoundingClientRect();
        return { area: c.closest('[data-laser-area]') && c.closest('[data-laser-area]').dataset.laserArea, states: [...c.querySelectorAll('.flowBox')].map(b => b.dataset.state), approve: box && box.dataset.mode, panel: p ? [...ids].sort() : null, inView: r.bottom > 0 && r.top < innerHeight, approveBoxes: c.querySelectorAll('.approveBox[data-level="set"]').length };
      },
      waitFor(src, timeout) {
        const pred = new Function('s', 'return (' + src + ')');
        return new Promise(res => {
          let done = false; const t0 = Date.now();
          const check = () => { if (done) return; let s = null, ok = false; try { s = window.__lr.sig(); ok = !!s && pred(s); } catch (e) {} if (ok) { done = true; mo.disconnect(); clearInterval(iv); res({ at: Date.now(), sig: s }); } else if (Date.now() - t0 > timeout) { done = true; mo.disconnect(); clearInterval(iv); res({ at: null, sig: s }); } };
          const mo = new MutationObserver(check); mo.observe(document.getElementById('libBody'), { subtree: true, childList: true, attributes: true, characterData: true });
          const iv = setInterval(check, 15); check();
        });
      },
      toCard() { const c = card(); c && c.scrollIntoView({ block: 'center' }); }
    };
  }, { rows });
  return pg;
}
const show = async pg => { await pg.page.evaluate(() => document.querySelector('#modeSeg [data-mode="library"]').click()); await pg.page.waitForSelector('#libBody .setCard', { timeout: 60000 }); await pg.page.evaluate(() => window.__lr.toCard()); await sleep(1200); };
const toCard = async pg => { await pg.page.evaluate(() => window.__lr.toCard()); await sleep(250); };
const openPanel = async pg => { await toCard(pg); await pg.page.evaluate(() => { const b = document.querySelector('.setCard .flowBox[data-flow-for="sheet:T-gf2"] [data-issues-open]'); b && b.click(); }); await sleep(400); };
async function press(pg, shop, i, how) {
  const b = shop.target.blockers[i];
  await pg.page.evaluate(rid => OrderWin.openOrder(rid), b.rid);
  const q = `#owPcSum [data-pc-act="${b.key}"] [data-cu-${how === 'button' ? 'complete' : 'print'}]`;
  await pg.page.waitForFunction(q => !!document.querySelector(q), q, { timeout: 120000, polling: 100 });
  await sleep(600);
  const t = Date.now(); await pg.page.evaluate(q => document.querySelector(q).click(), q); return t;
}
const closeWin = async pg => { await pg.page.evaluate(() => { try { OrderWin.close(); } catch (e) {} }); await sleep(300); };

(async () => {
  const srv = await O.backend(), shop = bigShop();
  seedCloud(srv, shop);
  const st = srv.st, M = meter(st), writes = [], set0 = st.docs.set.bind(st.docs);
  st.docs.set = (k, v) => { writes.push({ k, at: Date.now() }); return set0(k, v); };
  const etsy = []; const calls = srv.st.calls || [];
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const A = await libraryPage(browser, srv, shop, 'A'), B = await libraryPage(browser, srv, shop, 'B');
  const reqs = { A: [], B: [] };
  for (const [k, pg] of [['A', A], ['B', B]]) {
    pg.page.on('request', r => { if (/charmNestLibrary/.test(r.url()) && r.method() === 'POST') { let op = ''; try { op = JSON.parse(r.postData() || '{}').op; } catch (_) {} if (op === 'laserStatus') reqs[k].push({ at: Date.now(), body: r.postData() }); } });
    pg.page.on('response', async r => { if (/charmNestLibrary/.test(r.url()) && r.request().method() === 'POST') { let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {} if (b.op === 'laserStatus') { try { const j = await r.json(); const x = reqs[k].filter(q => q.body === r.request().postData()).pop(); if (x) x.answer = j; } catch (_) {} } } });
    pg.page.on('request', r => { if (/etsy\.com|openapi\.etsy/.test(r.url())) etsy.push(r.url()); });
  }
  await show(A); await show(B); await sleep(2500);
  const info = await A.page.evaluate(() => ({ cards: document.querySelectorAll('#libBody .setCard').length, set: window.__lr.sig(), sheets: LaserReview.liveState() }));
  say(`Library on two computers: ${info.cards} set cards in the Current list, ${shop.sheets.length} sheets in all, ${shop.orders.size} open orders; set-T: ${info.set.area}, rail ${info.set.states.join('/')}, Approve ${info.set.approve}, ${info.set.approveBoxes} Approve gate`);
  check(info.set.area === 'pending' && info.set.approveBoxes === 1 && !!info.sheets.rev, 'the large Library draws, set-T waits on its Order check behind ONE Approve gate, and the page holds the cloud revision', JSON.stringify(info.set) + JSON.stringify(info.sheets));
  const waitPending = async () => { for (const pg of [A, B]) { await toCard(pg); const r = await pg.page.evaluate(() => window.__lr.waitFor("s.area === 'pending' && s.panel === null || s.area === 'pending'", 40000)); if (!r.at) say('   (a page did not return to pending)'); } };
  const reopenAll = async () => { for (const b of shop.target.blockers) await srv.call({ op: 'customReopen', key: b.key, by: 'Paul' }); await waitPending(); await sleep(1500); };
  const blockerPut = async (i, how) => { const b = shop.target.blockers[i]; await srv.call({ op: 'customPut', key: b.key, receiptId: b.rid, transactionId: b.key.split('_')[1], sku: b.sku, how, by: 'Paul' }); return Date.now(); };
  const lastWrite = () => { const w = writes.filter(x => x.k.startsWith('Charm_Custom_Orders/')); return w.length ? w[w.length - 1].at : 0; };

  /* ── the cost, counted first (server side, the real handler; the viewport of one card, then ten) ── */
  say('\n== what a read costs (documents / bytes)');
  const ask = o => srv.call({ op: 'laserStatus', recordSeals: false, wantRevs: true, ...o });
  const T = { sheetIds: shop.target.sheetIds, setIds: ['set-T'] }, ten = { sheetIds: shop.sets.filter(s => !s.done).slice(0, 10).flatMap(s => s.sheetIds), setIds: shop.sets.filter(s => !s.done).slice(0, 10).map(s => s.setId) };
  for (const [name, v] of [['one set card in view', T], ['ten set cards in view', ten]]) {
    let a = M.snap(); const full = await ask(v); const cFull = M.since(a);
    a = M.snap(); const idle = await ask({ ...v, ifRevs: full.revs, ifRev: full.rev }); const cIdle = M.since(a);
    a = M.snap(); const ver = await ask({ ...v, ifRevs: full.revs, ifRev: full.rev, verify: true }); const cVer = M.since(a);
    const oKey = Object.keys(shop.runLines).find(k => k.startsWith('4173000000_'));
    await srv.call({ op: 'customPut', key: oKey, receiptId: '4173000000', transactionId: oKey.split('_')[1], sku: 'X', how: 'button', by: 'Paul' });
    a = M.snap(); const waste = await ask({ ...v, ifRevs: full.revs, ifRev: full.rev }); const cWaste = M.since(a);
    say(`  ${name}: full read ${cFull.docs} docs ${cFull.bytes} B · idle poll ${cIdle.docs} doc ${cIdle.bytes} B · safety probe ${cVer.docs} docs · after someone else's write ${cWaste.docs} docs`);
    check(idle.unchanged === true && cIdle.docs === 1 && cIdle.bytes <= 64, `${name}: an idle poll reads ONE document (the revision) and at most 64 bytes`, JSON.stringify(cIdle));
    check(ver.unchanged === true && cVer.docs === (full.revs ? Object.keys(full.revs).length : 0) + 1, `${name}: the safety probe reads the documents' update times (one each) and the revision, no more`, `${cVer.docs} docs`);
    check(waste.unchanged === true && waste.via === 'docs' && cWaste.docs === cVer.docs, `${name}: a poll after somebody else's write costs the old probe plus the one revision read, and is not a full answer`, `${cWaste.docs} docs`);
    check(cFull.bytes <= 400000, `${name}: the full read takes the sheets by field mask (the placements only for a sheet without a count)`, `${cFull.bytes} B`);
    // the slow check
    a = M.snap(); const s1 = await srv.call({ op: 'laserStatus', sheetIds: v.sheetIds, setIds: v.setIds, recordSeals: true, wantRev: true, by: 'T' }); const s1c = M.since(a);
    let s2 = await srv.call({ op: 'laserStatus', sheetIds: v.sheetIds, setIds: v.setIds, recordSeals: true, wantRev: true, by: 'T' });   // (the first pass's own writes moved the revision: one more pass finds nothing new)
    a = M.snap(); const s3 = await srv.call({ op: 'laserStatus', sheetIds: v.sheetIds, setIds: v.setIds, recordSeals: true, wantRev: true, ifRev: s2.rev, by: 'T' }); const s3c = M.since(a);
    say(`  ${name}: slow check ${s1c.docs} docs ${s1c.bytes} B in full, ${s3c.docs} doc when nothing changed`);
    check(s3.unchanged === true && s3c.docs === 1 && Array.isArray(s3.added) && !s3.added.length, `${name}: the slow check that records seals costs ONE document when nothing changed since its last pass`, JSON.stringify(s3c));
  }
  for (const b of shop.target.blockers) await srv.call({ op: 'customReopen', key: b.key, by: 'x' });
  await srv.call({ op: 'customReopen', key: Object.keys(shop.runLines).find(k => k.startsWith('4173000000_')), by: 'x' });
  await sleep(5000); await waitPending();

  /* ── the Library's own loop, idle: ONE document a poll, measured on the answers the pages got ── */
  say('\n== an idle Library (12 s, two computers)');
  await toCard(A); await toCard(B); await sleep(1500);
  const i0 = { A: reqs.A.length, B: reqs.B.length }; await sleep(12000);
  for (const k of ['A', 'B']) {
    const polls = reqs[k].slice(i0[k]).filter(r => r.answer), cheap = polls.filter(r => r.answer.unchanged && r.answer.probed === 1);
    say(`  ${k}: ${polls.length} reads in 12 s, ${cheap.length} of them one-document`);
    check(polls.length >= 3 && polls.length <= 7, `${k}: the loop reads about every 2.4 s while the cloud answers the revision (3 to 7 reads in 12 s)`, String(polls.length));
    check(polls.length > 0 && cheap.length >= polls.length - 1, `${k}: every idle read but at most one is a one-document read`, `${cheap.length} of ${polls.length}`);
  }

  /* ── same computer, a real press ── */
  say('\n== the computer that pressed (real presses in the order window)');
  const timings = { same: [], cross: [], tab: [], show: [] };
  for (const how of ['button', 'print']) {
    for (const pg of [A, B]) await openPanel(pg);
    const rid = shop.target.blockers[0].rid, w0 = writes.length;
    const watchA = A.page.evaluate(([rid]) => window.__lr.waitFor(`s.panel && !s.panel.includes('${rid}')`, 20000), [rid]);
    const watchB = B.page.evaluate(([rid]) => window.__lr.waitFor(`s.panel && !s.panel.includes('${rid}')`, 20000), [rid]);
    const tPress = await press(A, shop, 0, how);
    const [ra, rb] = await Promise.all([watchA, watchB]);
    const w = writes.slice(w0).find(x => x.k.startsWith('Charm_Custom_Orders/')), tw = w ? w.at : tPress;
    const same = ra.at ? ra.at - tw : null, cross = rb.at ? rb.at - tw : null;
    timings.same.push(same); timings.cross.push(cross);
    say(`  ${how === 'button' ? 'Complete Order' : 'Print QR label'}: record written ${tw - tPress} ms after the press; this computer repainted ${same} ms after the write${how === 'button' ? ` (${ra.at ? ra.at - tPress : '—'} ms after the press)` : ''}; the other computer ${cross} ms after`);
    check(same !== null && same <= SAME_MS, `${how === 'button' ? 'Complete Order' : 'Print QR label'}: this computer's Library shows it within ${SAME_MS} ms of the record`, `${same} ms`);
    check(cross !== null && cross <= CROSS_MS, `${how === 'button' ? 'Complete Order' : 'Print QR label'}: another computer's Library shows it within ${CROSS_MS} ms of the record`, `${cross} ms`);
    await closeWin(A);
    await reopenAll();
  }

  /* ── another computer, the same write made by it: the set advances as one, the Approve gate, the rail ── */
  say('\n== another computer completes the three pieces (the set advances as one)');
  for (const pg of [A, B]) await openPanel(pg);
  for (const how of ['button', 'print', 'button']) {
    const k = shop.target.blockers.findIndex(b => true);   // (the first still open, in order)
    const i = timings.cross.length - 2 + (how === 'print' ? 1 : 0);
  }
  let lastTw = 0; const crossMs = [];
  for (let i = 0; i < 3; i++) {
    const rid = shop.target.blockers[i].rid, last = i === 2;
    const wa = A.page.evaluate(([w]) => window.__lr.waitFor(w, 20000), [last ? "s.area === 'ready'" : `s.panel && !s.panel.includes('${rid}')`]);
    const wb = B.page.evaluate(([w]) => window.__lr.waitFor(w, 20000), [last ? "s.area === 'ready'" : `s.panel && !s.panel.includes('${rid}')`]);
    const tw = await blockerPut(i, i === 1 ? 'print' : 'button');
    const [ra, rb] = await Promise.all([wa, wb]);
    const a = ra.at ? ra.at - tw : null, b = rb.at ? rb.at - tw : null; crossMs.push(a, b);
    if (!last) { check(ra.sig && ra.sig.area === 'pending' && ra.sig.approveBoxes === 1, `piece ${i + 1} of 3: the set stays ONE card in In progress with one Approve gate (it advances only when all are done)`, JSON.stringify(ra.sig)); }
    else check(ra.sig && ra.sig.area === 'ready' && ra.sig.approveBoxes === 1 && ra.sig.states.every(s => s === 'ready' || s === 'done'), `piece 3 of 3: the set moves to Laser cutting as ONE card (rail ${ra.sig && ra.sig.states.join('/')}, Approve ${ra.sig && ra.sig.approve})`, JSON.stringify(ra.sig));
    say(`  piece ${i + 1} (${i === 1 ? 'print' : 'button'}): computer A ${a} ms, computer B ${b} ms after the record`);
    check(a !== null && a <= CROSS_MS && b !== null && b <= CROSS_MS, `piece ${i + 1} of 3: both computers show it within ${CROSS_MS} ms of the record`, `A ${a} ms, B ${b} ms`);
  }
  await reopenAll();
  // what is left lists only the sheet's own pieces, and no order of another set
  const left = await A.page.evaluate(() => window.__lr.sig().panel);
  check(left && left.length === 3 && left.every(id => ['4172000001', '4172000005', '4172000009'].includes(id)), "the '!' list of what is left names the three orders that wait, and only those", JSON.stringify(left));

  /* ── a second tab of the first computer: a write made there reaches the other tab through the BroadcastChannel, at once ── */
  say('\n== a second tab of the same computer');
  const C = await libraryPage(browser, srv, shop, 'C', A);
  await C.page.evaluate(() => { window.__anyLibraryCall = true; });
  for (let i = 0; i < 3; i++) {
    await toCard(A);
    const rid = shop.target.blockers[i].rid, last = i === 2, b = shop.target.blockers[i];
    const wa = A.page.evaluate(([w]) => window.__lr.waitFor(w, 20000), [last ? "s.area === 'ready'" : `s.panel && !s.panel.includes('${rid}')`]);
    if (i === 0) await openPanel(A);
    const r0 = reqs.A.length, t0 = Date.now();
    await C.page.evaluate(async ([k, rid, tid, sku]) => { await api('charmNestLibrary', { op: 'customPut', key: k, receiptId: rid, transactionId: tid, sku, how: 'button', by: 'Paul' }, { quiet: true }); }, [b.key, rid, b.key.split('_')[1], b.sku]);
    const tw = lastWrite(), ra = await wa, ms = ra.at ? ra.at - tw : null;
    const quick = reqs.A.slice(r0).some(r => r.at - tw >= -50 && r.at - tw <= 450);   // a read within 450 ms of the write: the nudge, not the 2.4 s loop
    timings.tab.push(ms);
    say(`  piece ${i + 1}: written in the other tab, this tab repainted ${ms} ms after; a read started within 450 ms of the write: ${quick}`);
    check(ms !== null && ms <= TAB_MS, `piece ${i + 1}: a Library tab shows a write made in another tab of its computer within ${TAB_MS} ms`, `${ms} ms`);
    check(quick, `piece ${i + 1}: that tab read the cloud at once (the BroadcastChannel nudge), not at its next turn of the loop`, 'no read within 450 ms');
  }
  await C.page.close(); await reopenAll();

  /* ── hidden, then shown ── */
  say('\n== a Library tab hidden, then shown');
  const hide = pg => pg.page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => true }); document.dispatchEvent(new Event('visibilitychange')); });
  const unhide = pg => pg.page.evaluate(() => { Object.defineProperty(document, 'hidden', { configurable: true, get: () => false }); document.dispatchEvent(new Event('visibilitychange')); });
  await toCard(A); await hide(A);
  const h0 = reqs.A.length; for (let i = 0; i < 3; i++) await blockerPut(i, 'button');
  await sleep(3500);
  check(reqs.A.length === h0, 'a hidden Library tab reads nothing', `${reqs.A.length - h0} reads`);
  const ws = A.page.evaluate(() => window.__lr.waitFor("s.area === 'ready'", 20000)); const tShow = Date.now(); await unhide(A); const rs = await ws;
  const sh = rs.at ? rs.at - tShow : null; timings.show.push(sh);
  say(`  shown again: the set is in Laser cutting ${sh} ms later`);
  check(sh !== null && sh <= SHOW_MS, `a Library tab shows what happened while it was hidden within ${SHOW_MS} ms of being shown (the slow check no longer holds the cards' read back)`, `${sh} ms`);
  await reopenAll();

  /* ── what was never done ── */
  check(etsy.length === 0, 'no Etsy call was made by either computer', etsy.slice(0, 2).join(' '));
  check(srv.paid === 0 && !srv.agentAsked, 'no paid model call was made', `paid ${srv.paid}, asked ${srv.agentAsked}`);
  const bad = [...A.errors, ...B.errors].filter(e => !/CORS|Access-Control/.test(e));
  check(!bad.length, 'no page error', bad.slice(0, 2).join(' | '));
  const cfin = await A.page.evaluate(() => LaserReview.liveState());
  say('\nA after the run:', JSON.stringify(cfin), '\ntimings', JSON.stringify(timings));
  await browser.close(); srv.close();
  if (fails.length) { console.error('\nFAILED:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('\nLibrary real time OK: the Library repaints a Complete Order / Print QR Label within ' + SAME_MS + ' ms on the computer that pressed it, ' + TAB_MS + ' ms on another tab of it and ' + CROSS_MS + ' ms on another computer; an idle poll is one document');
})().catch(e => { console.error(e); process.exit(1); });
