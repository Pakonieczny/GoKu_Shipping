// The cardinal rule in the REAL page (fake backend only; nothing leaves the machine, nothing live is touched):
//   window.SharedOrders reads the sheets the page holds (a gold page and a silver page of one run that share order A) and answers
//   between / groupOf at once; LibraryFlow.plan for a drag out of / into a set is blocked with exactly those orders;
//   SharedOrders.removeFromSheet (the person's choice: Hold or Cancel, whole order or only this sheet) takes the pieces off through
//   the sheet window's own Take-off flow with no window drawn, then the answer shrinks and LaserReview / the order window / the
//   subscribers are told within the call; a piece on a cut sheet stays with its reason; a bad request writes nothing.
//   node tests/charm-nest/shared-orders-page.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const A = '4400000001', B = '4400000002', D = '4400000004', E = '4400000005', G = '4400000007';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', TL = 'Order_Timeline', CANCELLED = 'Charm_Nest_Cancelled';
// A, E and G each have one gold piece and one silver piece (two pieces, two sheets: shared); B and D have one piece (never shared)
const GOLD = [[A, 1, 1], [B, 2, 1], [E, 4, 1], [G, 6, 1]], SILVER = [[A, 3, 1], [D, 5, 1], [E, 7, 1], [G, 8, 1]];

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const saved = (id, metal, list) => st.put(SHEETS, id, { id, metal, charms: list.map(([r, t, c], i) => ({ id: metal[0] + i, poolId: pid(r, t, c), order: r })), placements: list.map((x, i) => ({ id: metal[0] + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)), orders: [...new Set(list.map(x => x[0]))], sheetIndex: 1, fileBase: id, runId: 'run-test-1', draft: true });
  saved('gold-open-1', 'gold', GOLD); saved('silver-open-1', 'silver', SILVER);
  for (const [r, t, c] of GOLD) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'gold-open-1', state: 'placed', material: 'gold' });
  for (const [r, t, c] of SILVER) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'silver-open-1', state: 'placed', material: 'silver' });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, getFirestore = nope;" });
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
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
      P.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
      P.cutLinesOf = () => [];
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, page: sh.page, sheetId: sh.sheetId, charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        setTimeout(async () => {
          try { await api('charmNestLibrary', { op: 'putSheet', sheet: { id: sh.sheetId, metal: sh.metal, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, order: c.order })), placements: sh.placements.map(p => Object.assign({}, p)), poolIds: sh.charms.map(c => c.poolId) } }, { quiet: true }); }
          catch (e) { sh.problem = e.message; }
          sh.status = 'complete'; sh.dirty = false; sh._byHand = false; sh.stage = ''; sh.persistedDone = true;
          try { CN.renderCard(sh); } catch (_) {}
        }, 300);
      };
      stub.__stub = true; window.startNest = stub; clearInterval(iv);
    }, 0);
  });
  const page = await ctx.newPage(), errors = [], ok = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 15000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  const ids = (rid, metal) => page.evaluate(([m]) => CN.S.sheets[m].pages[0].charms.map(c => c.poolId), [metal]);
  const writes = () => st.calls.filter(c => ['flowApply', 'putSheet', 'poolUpdate', 'setUpdate', 'laserDone'].includes(c.op)).length;
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.Orders && Orders.takeOffGone && window.SharedOrders && window.SheetWin && SheetWin.takeOffOrder && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
    await page.evaluate(({ GOLD, SILVER, A, B, D, E, G }) => {
      const MM = 72 / 25.4, pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const fill = (metal, id, list) => {
        const sh = CN.S.sheets[metal].pages[0]; sh.charms = []; sh.placements = [];
        list.forEach(([r, t, c], i) => {
          const rMm = 5, poolId = pid(r, t, c), cid = metal[0] + i;
          sh.charms.push({ id: cid, name: `${r} · TEST-${t}`, poolId, order: r, lineKey: `${r}:${t}`, sourceId: 's', ringGeometryVersion: 3, centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
          sh.placements.push({ id: cid, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
          window.B.pool.rows.set(poolId, { poolId, orderId: r, sheetId: id, state: 'placed', material: metal });
        });
        Object.assign(sh, { status: 'complete', sheetId: id, fileBase: id, sheetIndex: 1, runId: 'run-test-1', draft: true, dirty: false, persistedDone: true, persisted: Promise.resolve() });
        CN.renderCard(sh);
      };
      fill('gold', 'gold-open-1', GOLD); fill('silver', 'silver-open-1', SILVER);
      const row = (rid, tx, material) => ({ key: `${rid}:${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer ' + rid.slice(-1) }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test charm ' + tx, quantity: 1, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: 1, material, problems: [], engraveCandidate: false },
        problems: [], state: 'pooled', reason: null, poolIds: [pid(rid, tx, 1)], engrave: null, material, arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [...GOLD.map(([r, t]) => row(r, t, 'gold')), ...SILVER.map(([r, t]) => row(r, t, 'silver'))];
      window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
      // the spies: what is told when an order comes off
      window.__told = { changed: 0, nudge: 0, orderWin: 0, subs: 0 };
      if (window.LaserReview) { const c = LaserReview.changed, n = LaserReview.nudge; LaserReview.changed = (...a) => { window.__told.changed++; return c && c.apply(LaserReview, a); }; LaserReview.nudge = (...a) => { window.__told.nudge++; return n && n.apply(LaserReview, a); }; }
      if (window.OrderWin) { const isOpen = OrderWin.isOpen; OrderWin.isOpen = () => true; const nud = OrderWin.nudge; OrderWin.nudge = (...a) => { window.__told.orderWin++; return nud && nud.apply(OrderWin, a); }; OrderWin.__isOpen = isOpen; }
      SharedOrders.subscribe(() => { window.__told.subs++; });
    }, { GOLD, SILVER, A, B, D, E, G });
    await page.waitForTimeout(300);

    // ── 1. what the page says at once: A, E and G are shared between the two sheets; B and D are not ──
    const q = await page.evaluate(({ A, E, G }) => {
      const t0 = performance.now(), items = SharedOrders.between('gold-open-1', 'new'), ms = performance.now() - t0;
      return { ms, orders: items.map(i => i.orderId), a: items.find(i => i.orderId === A), group: SharedOrders.groupOf('gold-open-1'), none: SharedOrders.between('gold-open-1', 'gold-open-1'), silver: SharedOrders.between('silver-open-1', 'new').map(i => i.orderId) };
    }, { A, E, G });
    assert.deepStrictEqual(q.orders, [A, E, G], 'exactly the three orders that have pieces on both sheets: ' + JSON.stringify(q.orders));
    assert.deepStrictEqual(q.silver, [A, E, G], 'the same from the other sheet');
    assert(q.a && q.a.total === 2 && q.a.pieces.length === 2 && q.a.hereIds.length === 1 && q.a.thereIds.length === 1 && q.a.here && q.a.there.length === 1, JSON.stringify(q.a));
    assert.deepStrictEqual(q.group.ids.sort(), ['gold-open-1', 'silver-open-1'], 'the two sheets are one group'); assert(q.ms < 50, 'answered with no network call, ' + q.ms + ' ms');
    ok.push('SharedOrders.between / groupOf from the page: exactly the shared orders, both sheets one group, instant');

    // ── 2. a drag that would separate them is blocked with those orders; nothing is written ──
    const w0 = writes();
    const plan = await page.evaluate(async () => { const p = await LibraryFlow.plan({ kind: 'sheet', id: 'gold-open-1', to: { newSet: true } }); return { ok: p.ok, key: p.needs[0] && p.needs[0].key, items: p.needs[0] ? p.needs[0].items.map(i => i.orderId) : [], shared: (p.shared || []).map(i => i.orderId), group: p.group }; });
    assert.strictEqual(plan.ok, false); assert.strictEqual(plan.key, 'sharedOrders', JSON.stringify(plan)); assert.deepStrictEqual(plan.items, [A, E, G]); assert.deepStrictEqual(plan.shared, [A, E, G]);
    assert.strictEqual(writes(), w0, 'a plan writes nothing');
    ok.push('LibraryFlow.plan in the page: needs.sharedOrders with exactly A, E, G; writes nothing');

    // ── 3. removal: Hold only this sheet's piece of A ──
    const t0 = await page.evaluate(() => Object.assign({}, window.__told));
    const hold = await page.evaluate(({ A }) => SharedOrders.removeFromSheet({ orderId: A, sheetId: 'gold-open-1', mode: 'hold', by: 'Tester', scope: 'sheet', note: 'mixed order' }), { A });
    assert.strictEqual(hold.ok, true, JSON.stringify(hold)); assert.strictEqual(hold.scope, 'sheet'); assert.deepStrictEqual(hold.removed.map(x => x.id), [pid(A, 1, 1)], JSON.stringify(hold)); assert.deepStrictEqual(hold.stayed, []);
    assert.deepStrictEqual(await ids(A, 'gold'), [pid(B, 2, 1), pid(E, 4, 1), pid(G, 6, 1)], 'A\'s gold piece is off the gold sheet'); assert((await ids(A, 'silver')).includes(pid(A, 3, 1)), 'its silver piece stays where it is');
    const pa = st.doc(POOL, pid(A, 1, 1)); assert(pa.state === 'abandoned' && !pa.sheetId && pa.heldBy === 'Tester' && !pa.removedAt, 'held, not removed: ' + JSON.stringify(pa));
    assert.strictEqual(st.doc(POOL, pid(A, 3, 1)).state, 'placed', 'the other piece is untouched on the server too');
    await until(() => st.list(TL).some(x => x._id.startsWith(`${A}~held~`) && x.by === 'Tester'), 10000, 'one held event, by who');
    const after = await page.evaluate(({ A, E, G }) => ({ orders: SharedOrders.between('gold-open-1', 'new').map(i => i.orderId), told: Object.assign({}, window.__told) }), { A, E, G });
    assert.deepStrictEqual(after.orders, [E, G], 'A no longer ties the sheets: ' + JSON.stringify(after.orders));
    assert(after.told.changed > t0.changed && after.told.nudge > t0.nudge && after.told.orderWin > t0.orderWin && after.told.subs > t0.subs, 'LaserReview, the order window and the subscribers were told within the call: ' + JSON.stringify([t0, after.told]));
    ok.push('removeFromSheet(hold, this sheet): piece off, record held (no removal), A stops tying the sheets at once, LaserReview + order window + subscribers nudged');

    // the Library's live read (bridge.js) says SharedOrders.refreshed() when it applied a change from another computer: the subscribers hear it
    const subs0 = await page.evaluate(() => { const n = window.__told.subs; SharedOrders.refreshed(); return [n, window.__told.subs]; }); assert.strictEqual(subs0[1], subs0[0] + 1, 'a live read that changed a card reaches the subscribers');
    // ── 4. Cancel the whole order E: off both sheets, kept under Cancelled ──
    const cancel = await page.evaluate(({ E }) => SharedOrders.removeFromSheet(E, 'gold-open-1', { mode: 'cancel', by: 'Tester', note: 'customer cancelled' }), { E });
    assert.strictEqual(cancel.ok, true, JSON.stringify(cancel)); assert.deepStrictEqual(cancel.removed.map(x => x.id).sort(), [pid(E, 4, 1), pid(E, 7, 1)].sort(), JSON.stringify(cancel));
    assert(!(await ids(E, 'gold')).includes(pid(E, 4, 1)) && !(await ids(E, 'silver')).includes(pid(E, 7, 1)), 'off both sheets');
    await until(() => st.doc(CANCELLED, E), 10000, 'the cancel record'); await until(() => st.list(TL).some(x => x._id.startsWith(`${E}~removed~`)), 10000, 'removal on its timeline');
    assert.deepStrictEqual(await page.evaluate(() => SharedOrders.between('gold-open-1', 'new').map(i => i.orderId)), [G]);
    ok.push('removeFromSheet(cancel): the whole order off every sheet, cancel record kept, the answer shrinks to G');

    // ── 5. a piece on a cut sheet stays, with its reason; the rest comes off ──
    await page.evaluate(() => { CN.S.sheets.silver.pages[0].laserDoneAt = Date.now() - 60000; });
    const cutAns = await page.evaluate(({ G }) => SharedOrders.removeFromSheet({ orderId: G, sheetId: 'gold-open-1', mode: 'hold', by: 'Tester' }), { G });
    assert.strictEqual(cutAns.ok, true, JSON.stringify(cutAns)); assert.deepStrictEqual(cutAns.removed.map(x => x.id), [pid(G, 6, 1)]);
    assert(cutAns.stayed.length === 1 && cutAns.stayed[0].id === pid(G, 8, 1) && /marked completed/.test(cutAns.stayed[0].why), 'the piece on the cut sheet stays and says why: ' + JSON.stringify(cutAns.stayed));
    assert.strictEqual(st.doc(POOL, pid(G, 8, 1)).state, 'placed', 'a cut sheet\'s piece is never touched');
    ok.push('a piece on a cut sheet stays with the plain reason; the other comes off');

    // ── 6. bad requests are refused in plain words and write nothing ──
    const w1 = writes();
    const bad = await page.evaluate(({ B }) => Promise.all([
      SharedOrders.removeFromSheet({ orderId: 'abc', sheetId: 'gold-open-1', mode: 'hold', by: 'Tester' }),
      SharedOrders.removeFromSheet({ orderId: B, sheetId: 'gold-open-1', by: 'Tester' }),
      SharedOrders.removeFromSheet({ orderId: B, sheetId: 'gold-open-1', mode: 'cancel', scope: 'sheet', by: 'Tester' }),
      SharedOrders.removeFromSheet({ orderId: '4400000099', sheetId: 'gold-open-1', mode: 'hold', by: 'Tester', scope: 'sheet' })
    ]), { B });
    for (const r of bad) { assert.strictEqual(r.ok, false); assert(r.error && r.error.length > 10, JSON.stringify(r)); }
    assert.strictEqual(writes(), w1, 'nothing was written for a refused request'); assert((await ids(B, 'gold')).includes(pid(B, 2, 1)), 'B is still on its sheet');
    ok.push('refused in plain words, nothing written: not an order number, no Hold/Cancel choice, a cancel of one sheet only, an order not on the sheet');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('shared-orders-page: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
