// Adversarial: a cancelled order on sheets that are cut, released or labelled (AutoCancel, charm-nest-sheetwin.js).
// A Gold sheet released for cutting (orders A, B) and a Silver sheet already cut (orders D, E), over the local stand-in
// for the site (bridge-server.cjs). Checks:
//   1. A cancelled while its piece sits on the released sheet (released for cutting, not cut: Paul, 29 Sep, a cancelled
//      order's pieces come off every sheet the laser has not cut): the piece comes off, the sheet is nested again and
//      written, the order's line leaves Orders, and nothing is set aside;
//   1b. F cancelled while its piece sits on a sheet that is cut: the piece stays, its line is kept as gone, so the set's
//      readiness does not wait on an engraving decision for it for good (a dropped line left the piece "unidentified":
//      CharmNestReadiness.decisions had no entry, and the set could never be released);
//   2. D cancelled on the cut sheet, then restored here (Orders › Cancelled): the set-aside notice goes with it;
//   3. E cancelled, then restored at another screen: the next check drops its notice;
//   4. the notices survive a reload, and Set aside clears only its own order's.
//   node tests/charm-nest/adv-cancel-cut.cjs
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require('./bridge-server.cjs');

const A = '4200000001', B = '4200000002', D = '4200000004', E = '4200000005', F = '4200000006';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', CANCELLED = 'Charm_Nest_Cancelled';
const GOLD = [[A, 1, 1], [B, 2, 1]], SILVER = [[D, 4, 1], [E, 5, 1], [F, 6, 1]];

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  const saved = (id, metal, list) => st.put(SHEETS, id, { id, metal, charms: list.map(([r, t, c], i) => ({ id: metal[0] + i, poolId: pid(r, t, c), order: r })), placements: list.map((x, i) => ({ id: metal[0] + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)) }, false);
  saved('gold-rel-1', 'gold', GOLD); saved('silver-cut-1', 'silver', SILVER);
  for (const [r, t, c] of GOLD) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'gold-rel-1', state: 'written', material: 'gold' });
  for (const [r, t, c] of SILVER) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'silver-cut-1', state: 'committed', material: 'silver' });
  const cancel = (rid, by, extra) => st.put(CANCELLED, rid, Object.assign({ orderId: rid, by, why: '', at: Date.now(), sheets: [], lines: [], createdAt: Timestamp.now() }, extra), false);

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  // nothing leaves the machine
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  // the operator, no order checks of its own, and a stand-in nest: it keeps every placement (an append-only nest) and
  // saves the sheet as it stands; "hang" is a nest a reload cuts short
  await ctx.addInitScript(() => {
    try {
      if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester');
      const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s));
    } catch (_) { /* about:blank */ }
    window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    const iv = setInterval(() => {
      if (typeof window.startNest !== 'function' || window.startNest.__stub) return;
      // a stand-in painter for the test's round charms (before and after a reload)
      const MM = 72 / 25.4, P = window.CharmNestPDF, realPath = P.pathToCanvas;
      P.drawCharm = (ctx, c, tx, k) => { const [x, y] = tx(c.centerPt[0], c.centerPt[1]); ctx.beginPath(); ctx.arc(x, y, c.rMm * MM * k, 0, 7); ctx.lineWidth = Math.max(1, .35 * k); ctx.strokeStyle = '#d0312d'; ctx.stroke(); };
      P.pathToCanvas = (ctx, p, tx) => { if (p && p.circle) { const [x, y] = tx(p.cx, p.cy), [x1] = tx(p.cx + p.r, p.cy), r = Math.abs(x1 - x); ctx.moveTo(x + r, y); ctx.arc(x, y, r, 0, Math.PI * 2); return; } return realPath(ctx, p, tx); };
      P.cutLinesOf = () => [];
      const stub = sh => {
        window.__nests = (window.__nests || []).concat([{ metal: sh.metal, page: sh.page, sheetId: sh.sheetId, byHand: !!sh._byHand, charms: sh.charms.map(c => c.poolId) }]);
        sh.status = 'nesting'; sh.persistedDone = false; sh.persisted = Promise.resolve(); sh.jobId = 'job-' + Math.random().toString(36).slice(2, 8); sh.stage = 'test nest';
        if (localStorage.getItem('__nestMode') === 'hang') return;
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
  const until = async (fn, ms = 15000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + (what || fn)); await new Promise(r => setTimeout(r, 100)); } };
  const boot = async () => {
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.AutoCancel && AutoCancel.started() && window.startNest && window.startNest.__stub, null, { timeout: 60000 });
  };

  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await boot();
    await page.evaluate(({ GOLD, SILVER, A, B: OB, D, E, F }) => {
      const MM = 72 / 25.4;
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const fill = (metal, id, list, extra, state) => {
        const sh = CN.S.sheets[metal].pages[0]; sh.charms = []; sh.placements = [];
        list.forEach(([r, t, c], i) => {
          const rMm = 5, poolId = pid(r, t, c), cid = metal[0] + i;
          sh.charms.push({ id: cid, name: `${r} · TEST-${t}`, poolId, order: r, lineKey: `${r}:${t}`, sourceId: 's', ringGeometryVersion: 3, centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
          sh.placements.push({ id: cid, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
          window.B.pool.rows.set(poolId, { poolId, orderId: r, sheetId: id, state, material: metal });
        });
        Object.assign(sh, { status: 'complete', sheetId: id, fileBase: id, sheetIndex: 1, runId: 'run-test-1', dirty: false, persistedDone: true, persisted: Promise.resolve() }, extra || {});
        CN.renderCard(sh);
      };
      fill('gold', 'gold-rel-1', GOLD, { releaseFull: true }, 'written');
      fill('silver', 'silver-cut-1', SILVER, { laserDoneAt: Date.now() - 3600000 }, 'committed');
      const row = (rid, tx, n, material, state) => ({ key: `${rid}:${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer ' + rid.slice(-1) }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test charm ' + tx, quantity: n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: n, material, problems: [], engraveCandidate: false },
        problems: [], state, reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material, arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(A, 1, 1, 'gold', 'written'), row(OB, 2, 1, 'gold', 'written'), row(D, 4, 1, 'silver', 'committed'), row(E, 5, 1, 'silver', 'committed'), row(F, 6, 1, 'silver', 'written')];
      window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
      setMode('nest'); window.scrollTo(0, 0);
    }, { GOLD, SILVER, A, B, D, E, F });
    await page.waitForTimeout(300);
    const settle = () => page.evaluate(async () => { const due = await AutoCancel.poll(); await AutoCancel.idle(); return due; });
    const view = () => page.evaluate(F => ({ gold: CN.S.sheets.gold.pages[0].charms.map(c => c.poolId), nests: (window.__nests || []).length, notices: AutoCancel.notices().map(n => n.rid),
      decision: CharmNestReadiness.decisions(Orders.rows())[`${F}_5000000006_1`] || null, visible: Orders.visibleRows().map(r => r.order.receiptId) }), F);

    /* 1 · cancelled on a released sheet: the piece comes off, the sheet is written again, nothing is set aside */
    cancel(A, 'Etsy', { source: 'etsy', at: Date.now() });
    assert.deepStrictEqual(await settle(), [A], 'the order is looked at');
    let v = await view();
    assert.deepStrictEqual(v.gold, [pid(B, 2, 1)], 'the released sheet gives the piece up: ' + JSON.stringify(v.gold));
    assert.strictEqual(v.nests, 1, 'the sheet is nested again, once');
    const pa = st.doc(POOL, pid(A, 1, 1));
    assert(pa.state === 'abandoned' && pa.removedBy === 'Etsy' && pa.removedReason === 'cancelled' && pa.removedAt > 0, 'its piece record says who, why and when: ' + JSON.stringify(pa));
    assert(!v.notices.includes(A), 'nothing is set aside: ' + JSON.stringify(v.notices));
    assert(!v.visible.includes(A), 'its line leaves the Orders list: ' + JSON.stringify(v.visible));
    assert.deepStrictEqual(await settle(), [], 'checked again: nothing to do');
    ok.push('released sheet: the piece comes off, the sheet is nested again and saved, its line leaves Orders, no notice, no repeat job');

    /* 1b · cancelled on a sheet already cut: the piece stays, and the set does not wait on it for good */
    cancel(F, 'Etsy', { source: 'etsy', at: Date.now() });
    assert.deepStrictEqual(await settle(), [F], 'the cut order is looked at');
    v = await view();
    assert.strictEqual(v.nests, 1, 'the cut sheet is never nested');
    assert.strictEqual(st.doc(POOL, pid(F, 6, 1)).state, 'committed', 'its piece record is left as it is');
    assert(v.notices.includes(F), 'the pill says to set it aside: ' + JSON.stringify(v.notices));
    assert(!v.visible.includes(F), 'its line leaves the Orders list: ' + JSON.stringify(v.visible));
    assert(v.decision && v.decision.approved === true && v.decision.needed === false, 'the set does not wait on an engraving decision for the piece left on the cut sheet: ' + JSON.stringify(v.decision));
    assert.deepStrictEqual(await settle(), [], 'checked again: nothing to do (the kept line is not taken up again)');
    ok.push('cut sheet: the piece stays, its line is kept as gone so the set can still be released; no repeat job');

    /* 2 · the cut sheet, then the order restored here: the notice goes */
    cancel(D, 'Anna', { at: Date.now() });
    assert.deepStrictEqual(await settle(), [D], 'the cut order is looked at');
    v = await view();
    assert(v.notices.includes(D), 'a set-aside notice for D: ' + JSON.stringify(v.notices));
    assert.strictEqual(v.nests, 1, 'the cut sheet is never nested (the one nest is the released sheet, step 1)');
    await page.evaluate(D => Cancelled.restore(D), D);
    v = await view();
    assert(!v.notices.includes(D) && v.notices.includes(F), 'restored: its notice goes, the others stay: ' + JSON.stringify(v.notices));
    ok.push('restored here: the set-aside notice of that order goes (and only it)');

    /* 3 · restored at another screen: the next check drops the notice */
    cancel(E, 'Sam', { at: Date.now() });
    await settle();
    assert((await view()).notices.includes(E), 'a notice for E');
    st.docs.delete(`${CANCELLED}/${E}`);
    // (FC3b: the sorter learns of a restore from the cancel counter, which cancelRestore raises in the same commit as its delete; a bare delete is found only at the page's next deep read, within a minute)
    st.put('Charm_Nest_Rev', 'cancel', { n: ((st.doc('Charm_Nest_Rev', 'cancel') || {}).n || 0) + 1 });
    await settle();
    v = await view();
    assert(!v.notices.includes(E) && v.notices.includes(F), 'restored elsewhere: its notice goes at the next check: ' + JSON.stringify(v.notices));
    ok.push('restored at another screen: the notice goes at the next check');

    /* 4 · a reload keeps the notices; Set aside clears only its own */
    cancel(E, 'Sam', { at: Date.now() + 5 });
    await settle();
    assert.deepStrictEqual((await view()).notices.sort(), [F, E].sort(), 'two notices');
    await page.evaluate(() => Session.flush(true));
    await page.reload(); await boot();
    await page.waitForFunction(() => document.querySelectorAll('#runBanner .rbCxItem').length === 2, null, { timeout: 8000 });
    await page.evaluate(F => AutoCancel.ack(F), F);
    v = await view();
    assert.deepStrictEqual(v.notices, [E], 'Set aside clears only its own order: ' + JSON.stringify(v.notices));
    assert.deepStrictEqual(await settle(), [], 'and it does not come back');
    assert.deepStrictEqual((await view()).notices, [E], 'still only E');
    ok.push('the notices survive a reload; Set aside clears only its own; an acknowledged notice does not come back');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('adv-cancel-cut: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
