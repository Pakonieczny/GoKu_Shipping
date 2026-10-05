// Browser test of the way a blocked drag opens the shared-orders window (charm-nest-library-dnd.js and charm-nest-library-approval-ui.js
// call SharedOrdersModal, charm-nest-shared-orders-modal.js), on the real Library page over the local stand-in for the site
// (bridge-server.cjs: the real charmNestLibrary function over an in-memory Firestore). Paul, 5 Oct 2026, point 6: a sheet that would split a
// multi-piece order between two sets is not moved, and the person is told why and which orders, in the window.
//
// LibraryFlow (the engine's) and SharedOrders (the engine's) are FAKES here, with the shapes agreed with their owner: the plan of a blocked
// move is { ok:false, needs:[{ key:'sharedOrders', label, detail, items:[SharedOrders.between items] }] }. Nothing live is called: every
// request that is not to the loopback is aborted, and no write goes anywhere but the in-memory stand-in.
//   node tests/charm-nest/shared-orders-dnd.cjs [playwright-core dir]      (SHOTS=<dir> also saves screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict'), zlib = require('zlib');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); process.exit(0); }
const { start, Timestamp } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';
const SHEETS = 'Charm_Nest_Sheets', SETS = 'Charm_Nest_Sets';
const SLOW = Math.max(1, +process.env.SO_SLOW || 1);   // (SO_SLOW=4 on a busy machine: every wait below is that many times longer)

/* ── the fixture of library-dnd.cjs: four sets today (1 and 2 ready for the laser, 3 and 4 in progress) over the in-memory site ── */
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

/* Today: set 1 (gold + silver) and set 2 (silver + rose) are ready for the laser; set 3 (gold) and set 4 (gold) are still in progress. */
const PLAN = [
  { setId: 'set-t-1', seq: 1, ready: true, sheets: [['dA01', 'gold'], ['dA02', 'silver']] },
  { setId: 'set-t-2', seq: 2, ready: true, sheets: [['dB01', 'silver'], ['dB02', 'rose']] },
  { setId: 'set-t-3', seq: 3, ready: false, sheets: [['dC01', 'gold']] },
  { setId: 'set-t-4', seq: 4, ready: false, sheets: [['dD01', 'gold']] }
];
function seed(st, blobUrl, roseReady = false) {
  const now = Date.now(), today = new Date(now).toISOString().slice(0, 10);
  let n = 0; const lines = {};
  for (const s of PLAN) {
    s.sheets.forEach(([id, metal], k) => {
      n++; const p = `charmnest/sets/${today}/Set-${s.seq}/${id}/preview.png`;
      st.blobs.set(p, { buf: preview(metal, n), generation: 1, meta: { contentType: 'image/png', metadata: { firebaseStorageDownloadTokens: 't' } } });
      const orders = Array.from({ length: 3 + (n % 3) }, (_, i) => String(3700000000 + n * 10 + i));
      const rec = { id, metal, metalLabel: metal, day: today, setId: s.setId, setSeq: s.seq, sheetIndex: k + 1, fileBase: `${metal.slice(0, 2).toUpperCase()}_${today}_Set-${s.seq}_Sheet-${k + 1}`, folder: `${metal}_${id}`, orders, poolIds: orders.map(o => `${o}_1_1`), listings: orders.map((o, i) => String(1718000 + (i % 4))), placedCount: orders.length, charmCount: orders.length, density: 0.7, freePt2: 2000, stock: { wIn: 6, hIn: 5 }, outputs: { preview: { path: p, url: blobUrl(p) } }, verification: { ok: true }, names: orders.join(' '), status: 'complete', updatedAt: Timestamp.fromMillis(now - n * 1000), createdAt: Timestamp.fromMillis(now - 86400000 + n), archived: false, runId: `run-${today}` };
      if (s.ready && (roseReady || metal !== 'rose')) {
        orders.forEach(o => { (lines[rec.runId] = lines[rec.runId] || {})[`${o}_1`] = { orderId: o, state: 'written', quantity: 1, poolIds: [`${o}_1_1`], engraveCandidate: false }; });
        Object.assign(rec, { processReady: true, processSeals: [{ id: `laserReady-${now}-${n}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }], outputs: Object.assign({}, rec.outputs, { ai: { path: `${id}.ai`, url: blobUrl(`${id}.ai`) } }), label: { files: [{ path: `${id}-qr.png`, url: rec.outputs.preview.url, payload: orders[0], orders }] } });
      } else Object.assign(rec, { placedCount: orders.length - 1 });
      st.put(SHEETS, id, rec);
    });
    st.put(SETS, s.setId, { setId: s.setId, seq: s.seq, day: today, runId: `run-${today}`, name: `Set-${s.seq}`, sheetIds: s.sheets.map(x => x[0]), materials: s.sheets.map(x => x[1]), orders: {}, labels: null, labelFiles: [], status: 'nesting', updatedAt: Timestamp.fromMillis(now - s.seq * 1000), createdAt: Timestamp.fromMillis(now - 86400000), ...(s.ready ? { processReady: true, processSeals: [{ id: `laserReady-${now}-s${s.seq}`, how: 'laserReady', at: now - 3600000, by: 'Anna' }] } : {}) });
  }
  for (const [runId, ls] of Object.entries(lines)) st.put('Charm_Nest_Runs', runId, { runId, lines: ls });
}
/** A sheet's membership changes in the cloud (what the real LibraryFlow.commit would write): its old set loses it, the new one gains it. */
function moveSheet(st, id, toSetId) {
  const rec = st.doc(SHEETS, id), from = st.doc(SETS, rec.setId), to = st.doc(SETS, toSetId);
  st.put(SETS, from.setId, { sheetIds: from.sheetIds.filter(x => x !== id) });
  st.put(SETS, to.setId, { sheetIds: [...to.sheetIds, id] });
  st.put(SHEETS, id, { setId: to.setId, setSeq: to.seq, updatedAt: Timestamp.fromMillis(Date.now()) });
}

/* ── the orders that tie sheet dA01 (GF Sheet 1) to dA02 (SS Sheet 1): pieces on both ── */
const svg = (seed, color) => 'data:image/svg+xml,' + encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="160" height="160" viewBox="0 0 160 160"><rect width="160" height="160" fill="#fffdf8"/><g fill="none" stroke="${color}" stroke-width="5" stroke-linecap="round">${['<circle cx="80" cy="80" r="40"/><path d="M80 40v80M40 80h80"/>', '<path d="M80 30l14 30 33 4-24 23 6 33-29-16-29 16 6-33-24-23 33-4z"/>', '<path d="M80 128C40 98 36 62 60 52c12-5 20 2 20 12 0-10 8-17 20-12 24 10 20 46-20 76z"/>'][seed % 3]}</g></svg>`);
const ORDERS = [['3700000010', 'Nathaly Soto'], ['3700000011', 'Emily Chambers'], ['3700000012', 'Jechelle Aragones']].map(([id, who], i) => ({ id, who, thumb: svg(i, '#8a6a22'),
  pieces: [{ label: 'Piece 1', sheetId: 'dA01', sheetLabel: 'GF Sheet 1', thumb: svg(i, '#8a6a22') }, { label: 'Piece 2', sheetId: 'dA02', sheetLabel: 'SS Sheet 1', thumb: svg(i + 1, '#59616b') }] }));

/* ── the fakes, serialised into the page ── */
function fakeShared(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms)), subs = new Set();
  const store = window.__so = { orders: cfg.orders, held: {}, calls: [] };
  const item = (o, id) => ({ orderId: o.id, label: 'Order ' + o.id, customer: o.who, thumb: o.thumb, here: (o.pieces.find(p => p.sheetId === id) || {}).sheetLabel, there: o.pieces.filter(p => p.sheetId !== id).map(p => p.sheetLabel),
    pieces: o.pieces.map((p, i) => ({ index: i + 1, label: p.label, sheetId: p.sheetId, sheetLabel: p.sheetLabel, setId: 'set-t-1', thumb: p.thumb })) });
  window.SharedOrders = {
    between(id) { return store.orders.filter(o => !store.held[o.id] && o.pieces.some(p => p.sheetId === id) && o.pieces.some(p => p.sheetId !== id)).map(o => item(o, id)); },
    async removeFromSheet(a) { store.calls.push(JSON.parse(JSON.stringify(a))); await sleep(300); store.held[a.orderId] = a.mode; subs.forEach(f => { try { f(); } catch (_) {} }); return { ok: true, scope: 'order', removed: [{ id: a.orderId + '_1' }], stayed: [] }; },
    subscribe(fn) { subs.add(fn); return () => subs.delete(fn); }
  };
}
function fakeFlow(cfg) {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  window.__calls = [];
  window.LibraryFlow = {
    targets(item) {
      const rows = (CN.S.library.rows || []), t = [{ area: 'progress' }, { area: 'laser' }, { area: 'completed' }];
      if (item.kind === 'sheet') { t.push({ newSet: true }); for (const s of new Set(rows.map(r => r.setId).filter(Boolean))) t.push({ set: s }); }
      return t;
    },
    async plan(q) {
      window.__calls.push(['plan', q.kind, q.id, JSON.stringify(q.to)]);
      await sleep(cfg.planMs);
      const base = { kind: q.kind, id: q.id, move: { kind: q.kind, id: q.id, to: q.to }, from: {}, to: Object.assign({ area: null, setId: null }, q.to.set ? { setId: q.to.set } : q.to), auto: [], needs: [], confirm: [], notes: [] };
      const items = q.kind === 'sheet' && q.to.set ? window.SharedOrders.between(q.id, q.to.set) : [];
      if (items.length) {
        const needs = [{ key: 'sharedOrders', label: items.length + ' orders keep this sheet in its set', detail: 'Their pieces are on other sheets.', items }];
        if (cfg.extraNeed) needs.push({ key: 'backs', label: '2 back engravings not approved', detail: 'The back engravings of this set must be approved in the Engrave tab before any of its sheets can be moved or cut.', items: [] });
        return Object.assign(base, { ok: false, needs, shared: items, group: ['SS Sheet 1'] });
      }
      return Object.assign(base, { ok: true, auto: [{ key: 'membership', label: 'Set membership changed' }] });
    },
    async commit(plan, o) {
      window.__calls.push(['commit', plan.id, JSON.stringify(plan.move.to), o && o.by]);
      await sleep(cfg.commitMs);
      if (plan.to.setId) await window.__fixtureMove(plan.id, plan.to.setId);
      return { ok: true, applied: plan.auto.map(a => ({ key: a.key, label: a.label })) };
    },
    async approve() { return { ok: true, auto: [], needs: [], confirm: [], notes: [] }; }
  };
}

const until = async (fn, ms = 20000, what = '') => { ms *= SLOW; const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 50)); } };

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const ok = [];
  const shot = async (page, name) => { if (SHOTS) { fs.mkdirSync(SHOTS, { recursive: true }); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };

  /** The Library page with the fake LibraryFlow and SharedOrders. o.realUi keeps the real LibraryApprovalUI; o.noModal leaves the shared-orders window out. */
  async function open(o = {}) {
    st.docs.clear(); seed(st, srv.blobUrl, false);
    // (o.real: order 3700000010 has a second piece on dA02, so the real engine finds dA01 and dA02 sharing a multi-piece order)
    if (o.real) { const r = st.doc(SHEETS, 'dA02'); st.put(SHEETS, 'dA02', { poolIds: [...r.poolIds, '3700000010_2_1'], orders: [...r.orders, '3700000010'] }); }
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 1500 } });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());   // (registered first: every later, more specific route wins over it)
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/charmNestLibrary/, r => { let b = null; try { b = r.request().postDataJSON(); } catch (_) { /* a get */ } return b && b.op === 'runList' ? r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ runs: [] }) }) : r.continue(); });
    const out = o.real ? [] : [/charm-nest-flow\.js/, /charm-nest-library-fx\.js/, /charm-nest-flow-rose\.js/, /charm-nest-shared-orders\.js/].concat(o.realUi ? [] : [/charm-nest-library-approval-ui\.js/], o.noModal ? [/charm-nest-shared-orders-modal\.js/] : []);
    for (const re of out) await ctx.route(re, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: '/* left out by the test */' }));
    await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* about:blank */ } window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [], writes = [];
    page.setDefaultTimeout(20000 * SLOW);
    page.on('pageerror', e => { errors.push('page: ' + e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    page.on('request', r => { if (r.method() === 'POST' && /charmNest|\.netlify\/functions/.test(r.url())) { try { const b = r.postDataJSON() || {}; if (b.op !== 'laserStatus' && (/apply|commit|hold|cancel(?!List)|rose|move|save|write|delete|purge|reset|remove|put|steps/i.test(b.op || '') || b.steps)) writes.push(b.op || 'post'); } catch (_) { /* not json */ } } });   // (the Library's own once-a-minute laserStatus check at opening is the page's, not the window's)
    await page.exposeFunction('__fixtureMove', (id, to) => moveSheet(st, id, to));
    if (!o.real) {
      await page.addInitScript(`(${fakeShared.toString()})(${JSON.stringify({ orders: ORDERS })})`);
      await page.addInitScript(`(${fakeFlow.toString()})(${JSON.stringify({ planMs: 250, commitMs: 250, extraNeed: !!o.extraNeed })})`);
    }
    await page.goto(`${sorterOrigin}/charm-nest-1.html#library`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.LibraryDone && window.LibraryDnd, null, { timeout: 60000 * SLOW });
    await page.waitForFunction(() => document.querySelectorAll('#libBody .setCard').length >= 4 && CN.S.library.rows.length >= 6 && document.querySelectorAll('#libBody .dndGrip').length > 0, null, { timeout: 30000 * SLOW });
    await page.waitForTimeout(500);
    return { ctx, page, errors, writes };
  }
  const box = (page, sel) => page.evaluate(s => { const e = document.querySelector(s); const r = e.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, l: r.left, t: r.top }; }, sel);
  const sheetSel = id => `#libBody .libCard[data-id="${id}"]`;
  const chipSel = n => `.dndChip[data-key="set:set-t-${n}"]`;
  const frames = page => page.evaluate(() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r))));
  /** The mouse picks a card up and carries it over the chip of a set, and lets go. */
  async function dropOn(page, fromSel, toSel) {
    const a = await box(page, fromSel), x0 = a.l + 24, y0 = a.t + 14;
    await page.mouse.move(x0, y0); await page.mouse.down(); await page.mouse.move(x0 + 10, y0 + 8, { steps: 3 });
    await page.waitForSelector('.dndDock .dndChip', { timeout: 8000 * SLOW });
    let b = await box(page, toSel); await page.mouse.move(b.x, b.y, { steps: 12 }); await frames(page);
    for (let i = 0; i < 4; i++) { const b2 = await box(page, toSel); if (Math.hypot(b2.x - b.x, b2.y - b.y) < 2) break; b = b2; await page.mouse.move(b.x, b.y, { steps: 4 }); await frames(page); }
    await page.mouse.up();
  }
  const modalOpen = page => page.evaluate(() => !!(window.SharedOrdersModal && SharedOrdersModal.isOpen()));
  const cardsIn = page => page.$$eval('.soDlg .soCard:not(.leaving)', cs => cs.map(c => c.dataset.order));
  const idle = page => page.evaluate(() => ({ move: LibraryDnd.state().move, bar: document.querySelectorAll('.dndMovingWrap').length, lifts: document.querySelectorAll('.dndLift').length, docks: document.querySelectorAll('.dndDock').length, src: document.querySelectorAll('.dndSource').length }));
  const quiet = { move: null, bar: 0, lifts: 0, docks: 0, src: 0 };
  const calls = (page, kind) => page.evaluate(k => window.__calls.filter(c => c[0] === k).length, kind);
  const setOf = id => st.doc(SHEETS, id).setId;

  /* ═════ 1 · a blocked drop opens the window: exactly the plan's orders, no red list, nothing written ═════ */
  {
    const { ctx, page, errors, writes } = await open();
    assert.equal(await page.evaluate(() => typeof SharedOrdersModal.open), 'function', 'the window is on the page');
    writes.length = 0;
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => modalOpen(page), 15000, 'the window opens on a blocked drop');
    await page.waitForTimeout(1000);
    const s = await page.evaluate(() => ({ cards: [...document.querySelectorAll('.soDlg .soCard')].map(c => c.dataset.order), red: document.querySelectorAll('.dndLine.bad, .dndHead.bad, .lapNeeds:not([hidden])').length, title: document.querySelector('.soTitle').textContent, sub: document.querySelector('.soSub').textContent, count: document.querySelector('.soCount').textContent, move: LibraryDnd.state().move }));
    assert.deepEqual(s.cards, ORDERS.map(o => o.id), 'lists exactly the orders the plan names');
    assert.equal(s.red, 0, 'no red list behind it'); assert(/^These orders keep .+ in Set 1$/.test(s.title), 'title: ' + s.title); assert(s.sub.includes('Set 3'), 'it says which set it cannot move to: ' + s.sub);
    assert.equal(s.count, '3 orders'); assert.equal(s.move, 'back', 'the card is going home');
    assert.equal(await calls(page, 'commit'), 0, 'no commit'); assert.equal(await calls(page, 'plan'), 1, 'one plan');
    await shot(page, 'D1-blocked-drop-window');
    await page.keyboard.press('Escape');
    await until(async () => !(await modalOpen(page)), 3000, 'Esc closes it');
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 8000, 'nothing is left behind: ' + JSON.stringify(await idle(page)));
    assert.equal(setOf('dA01'), 'set-t-1', 'the sheet was not moved'); assert.deepEqual(writes, [], 'nothing was written: ' + writes);
    ok.push('1 · a blocked drop opens the window with exactly the plan\'s orders (no red list), the card goes home, nothing is committed or written, Esc leaves nothing behind');

    // 2 · take the orders off in the window, then "Move it now" tries the same move again and it goes through
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => modalOpen(page), 15000, 'opens again');
    await page.waitForTimeout(900);
    for (const o of ORDERS) {
      await page.click(`.soCard[data-order="${o.id}"] [data-off]`);
      await page.check(`.soCard[data-order="${o.id}"] .soAsk input[value=hold]`);
      await page.click(`.soCard[data-order="${o.id}"] [data-yes]`);
      await until(async () => !(await cardsIn(page)).includes(o.id), 6000, 'the card for ' + o.id + ' leaves');
    }
    await until(() => page.$('.soClear [data-retry]'), 5000, 'the empty state offers the move');
    assert.equal(await page.evaluate(() => window.__so.calls.map(c => c.mode).join()), 'hold,hold,hold');
    await shot(page, 'D2-nothing-holds');
    await page.click('.soClear [data-retry]');
    await until(async () => (await calls(page, 'commit')) === 1, 15000, 'the move is tried again and committed');
    assert.equal(await calls(page, 'plan'), 3, 'a fresh plan was read for the retry');
    await until(() => setOf('dA01') === 'set-t-3', 8000, 'the sheet is in Set 3 now');
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 15000, 'quiet again: ' + JSON.stringify(await idle(page)));
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('2 · the orders taken off in the window, "Move it now" plans the move again and it is committed (sheet now in Set 3), nothing left behind');
    await ctx.close();
  }

  /* ═════ 3 · other reasons too: the window still opens; the bar keeps only the other reasons, waits while it is open and goes by itself after ═════ */
  {
    const { ctx, page, errors } = await open({ extraNeed: true });
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => modalOpen(page), 15000, 'the window opens although there is another reason too');
    assert.deepEqual(await cardsIn(page), ORDERS.map(o => o.id));
    await page.waitForTimeout(700);
    const bar = await page.evaluate(() => ({ lines: [...document.querySelectorAll('.dndMovingWrap .dndLine.bad')].map(e => e.textContent), btn: document.querySelectorAll('.dndMovingWrap [data-shared]').length }));
    assert.equal(bar.lines.length, 1, 'the bar keeps only the other reason: ' + JSON.stringify(bar.lines));
    assert(/back engravings/.test(bar.lines[0]) && !/orders keep this sheet/.test(bar.lines[0]), 'and it is that one: ' + bar.lines[0]); assert.equal(bar.btn, 0, 'no second way in to the window');
    await page.waitForTimeout(4600);   // (longer than the bar would otherwise stay)
    assert.equal(await page.evaluate(() => document.querySelectorAll('.dndMovingWrap').length), 1, 'the bar waits while the window is open');
    await page.keyboard.press('Escape');
    await until(async () => !(await modalOpen(page)), 3000, 'closed');
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 20000, 'then the bar goes by itself and nothing is left: ' + JSON.stringify(await idle(page)));
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('3 · with another reason too: the window opens, the bar keeps only the other reason (no second way in), waits while the window is open and goes by itself after');
    await ctx.close();
  }

  /* ═════ 4 · the real LibraryApprovalUI: a long explanation is behind "Why"; a sharedOrders need is one "See which orders" button ═════ */
  {
    const { ctx, page, errors } = await open({ extraNeed: true, realUi: true });
    assert.equal(await page.evaluate(() => typeof LibraryApprovalUI.show), 'function');
    // (a) the drop itself: the window opens, and the approval card behind it says only the other reason, its long text behind "Why"
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => modalOpen(page), 15000, 'the window opens');
    await until(() => page.$('.lapNeed[data-need="backs"]'), 8000, 'the approval card says the other reason');
    assert.equal(await page.$$eval('.lapNeed[data-need="sharedOrders"]', a => a.length), 0, 'and not the orders again');
    const why = await page.evaluate(() => { const n = document.querySelector('.lapNeed[data-need="backs"]'); const b = n.querySelector('.lapWhyBtn'), w = n.querySelector('.lapWhy'); const slot = n.querySelector('.lapWhySlot'); return { btn: !!b, closed: !!slot && slot.getBoundingClientRect().height === 0 && getComputedStyle(w).visibility === 'hidden', label: n.querySelector('.lapNeedT b').textContent, head: document.querySelector('.lapNeedsHead span').textContent }; });
    assert(why.btn && why.closed, 'its long explanation is really closed (no height, not visible) behind "Why": ' + JSON.stringify(why));
    assert.equal(why.head, 'Not yet:', 'the red box behind the window says only "Not yet:": ' + why.head);
    await page.keyboard.press('Escape');
    await until(async () => !(await modalOpen(page)), 3000, 'closed');
    await page.click('.lapNeed[data-need="backs"] .lapWhyBtn');
    await until(() => page.$eval('.lapNeed[data-need="backs"] .lapWhySlot', s => s.getBoundingClientRect().height > 8), 4000, 'Why opens it (the slot grows)');
    assert.equal(await page.$eval('.lapNeed[data-need="backs"] .lapWhy', w => getComputedStyle(w).visibility), 'visible', 'and its words show');
    assert.equal(await page.$eval('.lapNeed[data-need="backs"] .lapWhyBtn', b => b.getAttribute('aria-expanded')), 'true');
    await shot(page, 'D3-approval-card-why');
    await page.mouse.move(4, 4);   // (the bar waits while it is pointed at; it goes by itself once the pointer is away)
    await page.evaluate(() => LibraryDnd.cancel());
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 20000, 'quiet again: ' + JSON.stringify(await idle(page)));
    // (b) a plan shown with the card alone (no drop): the shared-orders need is one button, no row of red chips, and it opens the window with those orders
    await page.evaluate(() => {
      const host = document.createElement('div'); host.id = 'tmpHost'; host.style.cssText = 'position:fixed;left:40px;top:40px;width:560px;z-index:5'; document.body.appendChild(host);
      const items = SharedOrders.between('dA01', 'set-t-3');
      window.__uiShared = [];
      LibraryApprovalUI.show(host, { ok: false, kind: 'sheet', id: 'dA01', move: {}, from: {}, to: { area: null, setId: 'set-t-3', label: 'Set 3' }, auto: [], confirm: [], notes: [],
        needs: [{ key: 'sharedOrders', label: items.length + ' orders keep this sheet in its set', detail: 'Their pieces are on other sheets, which stay in Set 1 and so the sheet cannot leave it.', items }] },
      { title: 'Moving GF Sheet 1 to Set 3', kind: 'sheet', onConfirm() {}, onCancel() {}, onShared: (n, b) => { window.__uiShared.push(n.key); return SharedOrdersModal.open({ kind: 'sheet', id: 'dA01', orders: n.items, targetLabel: 'Set 3', from: b }); } });
    });
    await until(() => page.$('#tmpHost .lapNeed[data-need="sharedOrders"] [data-shared]'), 8000, 'the approval card has one button for the orders');
    assert.equal(await page.$$eval('#tmpHost .lapNeed[data-need="sharedOrders"] .lapItem', a => a.length), 0, 'no row of red chips for them');
    assert.equal(await page.$eval('#tmpHost .lapNeed[data-need="sharedOrders"] [data-shared]', b => b.textContent.trim()), 'See which orders');
    assert.equal(await page.$$eval('#tmpHost .lapNeed[data-need="sharedOrders"] .lapWhyBtn', a => a.length), 1, 'its long explanation is behind "Why" too');
    await shot(page, 'D4-approval-card-button');
    await page.click('#tmpHost .lapNeed[data-need="sharedOrders"] [data-shared]');
    await until(() => modalOpen(page), 5000, 'the button opens the window');
    assert.deepEqual(await cardsIn(page), ORDERS.map(o => o.id), 'the same orders');
    assert.deepEqual(await page.evaluate(() => window.__uiShared), ['sharedOrders']);
    await page.keyboard.press('Escape');
    await until(async () => !(await modalOpen(page)), 3000, 'closed');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('4 · LibraryApprovalUI: a long explanation is behind "Why"; a sharedOrders need is one "See which orders" button (no chips) that opens the window with those orders');
    await ctx.close();
  }

  /* ═════ 6 · the REAL engine (SharedOrders, LibraryFlow, the real approval card): a drop that would split an order opens the window with it ═════ */
  {
    const { ctx, page, errors, writes } = await open({ real: true });
    const mods = await page.evaluate(() => ({ flow: !!(window.LibraryFlow && LibraryFlow.plan), shared: !!(window.SharedOrders && SharedOrders.between && SharedOrders.core), ui: !!(window.LibraryApprovalUI && LibraryApprovalUI.show), modal: !!(window.SharedOrdersModal && SharedOrdersModal.open), fake: !!window.__so }));
    assert.deepEqual(mods, { flow: true, shared: true, ui: true, modal: true, fake: false }, 'the real modules are on the page: ' + JSON.stringify(mods));
    const items = await page.evaluate(() => SharedOrders.between('dA01', 'set-t-3'));
    assert.deepEqual(items.map(i => i.orderId), ['3700000010'], 'the engine finds the one order that ties dA01 to dA02: ' + JSON.stringify(items.map(i => i.orderId)));
    writes.length = 0;
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => modalOpen(page), 20000, 'the window opens on the real engine\'s block');
    await page.waitForTimeout(1000);
    const r = await page.evaluate(() => ({ cards: [...document.querySelectorAll('.soDlg .soCard')].map(c => c.dataset.order), tiles: document.querySelectorAll('.soDlg .soCard .soTile').length, no: (document.querySelector('.soDlg .soOrderNo') || {}).textContent, title: document.querySelector('.soTitle').textContent, sub: document.querySelector('.soSub').textContent, off: !!document.querySelector('.soDlg [data-off]') }));
    assert.deepEqual(r.cards, ['3700000010'], 'the order the engine names: ' + JSON.stringify(r)); assert.equal(r.tiles, 2, 'a picture tile per piece'); assert.equal(r.no, '#3700000010');
    assert(/^This order keeps .+ in Set 1$/.test(r.title), 'title: ' + r.title); assert(r.off, 'and a way to take it off');
    await shot(page, 'D5-real-engine-window');
    await page.keyboard.press('Escape');
    await until(async () => !(await modalOpen(page)), 3000, 'closed');
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 20000, 'quiet again: ' + JSON.stringify(await idle(page)));
    assert.equal(setOf('dA01'), 'set-t-1', 'the sheet was not moved'); assert.deepEqual(writes, [], 'nothing was written: ' + writes);
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('6 · the real engine: a drop that would split order 3700000010 opens the window with exactly that order (two piece tiles), the sheet stays, nothing is written');
    await ctx.close();
  }

  /* ═════ 5 · without the window on the page, the old red list still tells it ═════ */
  {
    const { ctx, page, errors } = await open({ noModal: true });
    assert.equal(await page.evaluate(() => typeof window.SharedOrdersModal), 'undefined');
    await dropOn(page, sheetSel('dA01'), chipSel(3));
    await until(() => page.$('.dndLine.bad'), 15000, 'the red list shows');
    assert((await page.$eval('.dndLine.bad', e => e.textContent)).includes('orders keep this sheet in its set'));
    await page.evaluate(() => LibraryDnd.cancel());
    await until(async () => JSON.stringify(await idle(page)) === JSON.stringify(quiet), 20000, 'quiet again: ' + JSON.stringify(await idle(page)));
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    ok.push('5 · with no window on the page the drop still says why, in the plain red list');
    await ctx.close();
  }

  await browser.close(); srv.close();
  console.log(ok.map(x => '  ✓ ' + x).join('\n'));
})().catch(e => { console.error(e); process.exit(1); });
