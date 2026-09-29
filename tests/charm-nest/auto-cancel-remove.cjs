// A cancelled order comes off the sorter's sheets by itself (Paul, 28 Sep, A2-A3: AutoCancel in charm-nest-sheetwin.js).
// Opens the sorter over the local stand-in for the site (bridge-server.cjs: the real charmNestLibrary over an in-memory
// Firestore) with a Gold sheet still filling (orders A, B, C) and a Silver sheet already cut (orders D, E), and a stand-in
// nest that saves the sheet as it stands. Cancel records are written into the cloud as the Etsy mirror or another
// station writes them, and the sorter's cancel check is asked to look (it does so every 2 minutes and at each orders check):
//   1. cancelled on Etsy, on a sheet still filling: its pieces come off, the other charms stay where they were, the sheet
//      is nested again (by hand) and saved without them, its piece records say abandoned by Etsy (cancelled), its lines
//      leave Orders, the timeline has "removed" with the sheet, a note says so and the charm flies off the card;
//   2. cancelled on a cut sheet: nothing moves; the run pill shows "Order D was cancelled. Its pieces are already cut on
//      SS Sheet 1: set them aside." until Set aside is pressed, and the timeline has the note (and who set them aside);
//   3. a record taken back before the sorter acts (the order is not cancelled): nothing moves;
//   4. a reload in the middle (the nest cut short after the pieces came off): the job is taken up after the reload and
//      finished (nested, saved, read back), never done twice.
//   node tests/charm-nest/auto-cancel-remove.cjs [playwright-core dir]
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const A = '4100000001', B = '4100000002', C = '4100000003', D = '4100000004', E = '4100000005';
const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', CANCELLED = 'Charm_Nest_Cancelled', TL = 'Order_Timeline';
// the pieces: Gold (still filling) holds A ×2, B, C; Silver (cut) holds D, E
const GOLD = [[A, 1, 1], [A, 1, 2], [B, 2, 1], [C, 3, 1]], SILVER = [[D, 4, 1], [E, 5, 1]];

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin } = srv;
  // the cloud's side: the two sheets as saved, and the piece records
  const saved = (id, metal, list) => st.put(SHEETS, id, { id, metal, charms: list.map(([r, t, c], i) => ({ id: metal[0] + i, poolId: pid(r, t, c), order: r })), placements: list.map((x, i) => ({ id: metal[0] + i, cxPt: 30 + i * 40, cyPt: 40, angle: 0 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)) }, false);
  saved('gold-open-1', 'gold', GOLD); saved('silver-cut-1', 'silver', SILVER);
  for (const [r, t, c] of GOLD) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'gold-open-1', state: 'placed', material: 'gold' });
  for (const [r, t, c] of SILVER) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: 'silver-cut-1', state: 'committed', material: 'silver' });
  const cancel = (rid, by, extra) => st.put(CANCELLED, rid, Object.assign({ orderId: rid, by, why: '', at: Date.now(), sheets: [], lines: [] }, extra), false);

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
    // the sorter's side: the two pages (a stand-in painter draws their charms), the lines, the piece records
    await page.evaluate(({ GOLD, SILVER, A, B: OB, C, D, E }) => {
      const MM = 72 / 25.4;
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const fill = (metal, id, list, extra) => {
        const sh = CN.S.sheets[metal].pages[0]; sh.charms = []; sh.placements = [];
        list.forEach(([r, t, c], i) => {
          const rMm = 5, poolId = pid(r, t, c), cid = metal[0] + i;
          sh.charms.push({ id: cid, name: `${r} · TEST-${t}`, poolId, order: r, lineKey: `${r}:${t}`, sourceId: 's', ringGeometryVersion: 3, centerPt: [0, 0], bbox: [-rMm * MM, -rMm * MM, rMm * MM, rMm * MM], outline: { circle: 1, cx: 0, cy: 0, r: rMm * MM }, members: [], rMm, widthPt: 2 * rMm * MM, heightPt: 2 * rMm * MM, areaPt2: Math.PI * (rMm * MM) ** 2 });
          sh.placements.push({ id: cid, cxPt: 30 + i * 40, cyPt: 40, angle: 0, wPt: 2 * rMm * MM, hPt: 2 * rMm * MM });
          window.B.pool.rows.set(poolId, { poolId, orderId: r, sheetId: id, state: metal === 'silver' ? 'committed' : 'placed', material: metal });
        });
        Object.assign(sh, { status: 'complete', sheetId: id, fileBase: id, sheetIndex: 1, runId: 'run-test-1', dirty: false, persistedDone: true, persisted: Promise.resolve() }, extra || {});
        CN.renderCard(sh);
      };
      fill('gold', 'gold-open-1', GOLD);
      fill('silver', 'silver-cut-1', SILVER, { laserDoneAt: Date.now() - 3600000 });
      const row = (rid, tx, n, material, state) => ({ key: `${rid}:${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer ' + rid.slice(-1) }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test charm ' + tx, quantity: n, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: n, material, problems: [] },
        problems: [], state, reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material, arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(A, 1, 2, "gold", "pooled"), row(OB, 2, 1, 'gold', 'pooled'), row(C, 3, 1, 'gold', 'pooled'), row(D, 4, 1, 'silver', 'committed'), row(E, 5, 1, 'silver', 'committed')];
      window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
      // (the flights: every Motion.fly is counted)
      window.__flies = 0; const fly = Motion.fly; Motion.fly = (g, to, o) => { window.__flies++; return fly(g, to, o); };
      setMode('nest'); window.scrollTo(0, 0);
    }, { GOLD, SILVER, A, B, C, D, E });
    await page.waitForTimeout(300);
    const where = () => page.evaluate(() => ({ gold: CN.S.sheets.gold.pages[0].charms.map(c => c.poolId), goldAt: Object.fromEntries(CN.S.sheets.gold.pages[0].placements.map(p => [CN.S.sheets.gold.pages[0].charms.find(c => c.id === p.id).poolId, [p.cxPt, p.cyPt]])), silver: CN.S.sheets.silver.pages[0].charms.map(c => c.poolId), rows: Orders.rows().map(r => r.order.receiptId), nests: (window.__nests || []).map(n => n.sheetId + (n.byHand ? ':hand' : '')) }));
    const before = await where();
    const settle = () => page.evaluate(async () => { const due = await AutoCancel.poll(); await AutoCancel.idle(); return due; });

    /* 1 · cancelled on Etsy, on the sheet still filling */
    const atA = Date.now(); cancel(A, 'Etsy', { source: 'etsy', at: atA });
    const due1 = await settle();
    assert.deepStrictEqual(due1, [A], 'the sorter finds the cancelled order it holds: ' + JSON.stringify(due1));
    let w = await where();
    assert.deepStrictEqual(w.gold, [pid(B, 2, 1), pid(C, 3, 1)], 'its two pieces come off the Gold sheet: ' + JSON.stringify(w.gold));
    for (const id of w.gold) assert.deepStrictEqual(w.goldAt[id], before.goldAt[id], 'the other charms stay exactly where they were');
    assert.deepStrictEqual(w.nests, ['gold-open-1:hand'], 'the sheet is nested again as a removal does (by hand): ' + JSON.stringify(w.nests));
    assert(!w.rows.includes(A) && w.rows.includes(B), 'its lines leave Orders: ' + w.rows);
    const recA = st.doc(SHEETS, 'gold-open-1');
    assert.deepStrictEqual(recA.charms.map(c => c.poolId).sort(), [pid(B, 2, 1), pid(C, 3, 1)], 'the saved sheet no longer lists it');
    for (const c of [1, 2]) { const p = st.doc(POOL, pid(A, 1, c)); assert(p.state === 'abandoned' && p.removedBy === 'Etsy' && p.removedReason === 'cancelled' && p.removedVerifiedAt > 0, 'its piece records: abandoned, removed by Etsy (cancelled), verified: ' + JSON.stringify(p)); }
    // one "removed" per sheet: the server stamps it from the piece records, the sorter's own says it in full under the same id
    const remA = st.doc(POOL, pid(A, 1, 1)).removedAt;
    await until(() => { const e = st.doc(TL, `${A}~removed~${remA}-gold-open-1`); return e && /^Removed from GF Sheet 1/.test(e.text || ''); }, 8000, 'the removed event');
    const evsA = st.list(TL).filter(x => x._id.startsWith(`${A}~removed~`)), evA = evsA[0];
    assert(evsA.length === 1 && evA.sheetId === 'gold-open-1' && evA.sheet === 'GF Sheet 1' && evA.data && evA.data.reason === 'cancelled on Etsy' && evA.by === 'Etsy', 'the timeline has one "removed" with its sheet and reason: ' + JSON.stringify(evsA));
    assert.deepStrictEqual(st.doc(CANCELLED, A).fates, [{ sheet: 'GF Sheet 1', fate: 'removed', text: 'taken off GF Sheet 1' }], 'its cancel record says what became of it (Orders › Cancelled reads it)');
    const s1 = await page.evaluate(A => ({ notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent), flies: window.__flies, has: Cancelled.has(A), acState: AutoCancel.state(), popups: document.querySelectorAll('dialog[open]').length }), A);
    assert(s1.notes.some(t => t.includes(`Order ${A} cancelled on Etsy · taken off GF Sheet 1`)), 'a note says so: ' + JSON.stringify(s1.notes));
    assert(s1.flies >= 2, 'its charms fly off the card: ' + s1.flies);
    assert(s1.has && s1.acState.done[A] && +s1.acState.done[A].at === atA && !Object.keys(s1.acState.jobs).length, 'it is in the Cancelled cache, and its job is done: ' + JSON.stringify(s1.acState));
    assert.strictEqual(s1.popups, 0, 'never a pop-up');
    ok.push('cancelled on Etsy, on a sheet still filling: taken off, the rest in place, nested again, saved and read back, records abandoned by Etsy, timeline "removed", a note and a flight');
    const again = await settle();
    assert.deepStrictEqual(again, [], 'checked again: nothing to do (never twice)');
    assert.deepStrictEqual((await where()).nests, ['gold-open-1:hand'], 'and no second nest');

    /* 2 · cancelled on the cut sheet: notice only */
    const atD = Date.now(); cancel(D, 'Anna', { at: atD });
    const due2 = await settle();
    assert.deepStrictEqual(due2, [D], 'the cut order is looked at: ' + JSON.stringify(due2));
    w = await where();
    assert.deepStrictEqual(w.silver, [pid(D, 4, 1), pid(E, 5, 1)], 'its piece stays on the cut sheet');
    assert.deepStrictEqual(w.nests, ['gold-open-1:hand'], 'nothing is nested');
    assert(w.rows.includes(D), 'its committed line stays with its run');
    assert.strictEqual(st.doc(POOL, pid(D, 4, 1)).state, 'committed', 'its piece record is left as it is');
    const TEXT = `Order ${D} was cancelled. Its pieces are already cut on SS Sheet 1: set them aside.`;
    await page.waitForFunction(() => !document.getElementById('runBanner').classList.contains('hidden') && document.querySelector('#runBanner .rbCxItem'), null, { timeout: 5000 });
    await page.waitForFunction(D => [...document.querySelectorAll('.mNote')].some(n => n.textContent.includes(`Order ${D} was cancelled`)), D, { timeout: 3000 }).catch(() => {});
    const pill = await page.evaluate(() => { const h = document.getElementById('runBanner'); return { text: h.querySelector('.rbText').textContent, item: h.querySelector('.rbCxItem span').textContent, n: h.querySelector('.rbCxN').textContent, notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent) }; });
    assert.strictEqual(pill.item, TEXT, 'the pill menu says what to do: ' + pill.item);
    assert(/cancelled · set aside/.test(pill.text) && pill.n === '1', 'the pill itself says so: ' + JSON.stringify(pill));
    assert(pill.notes.some(t => t.includes(TEXT)), 'and a note: ' + JSON.stringify(pill.notes));
    await until(() => st.doc(TL, `${D}~note~autocancel-aside-${atD}`), 8000, 'the timeline note');   // (the page's timeline queue sends it in a second or so)
    const noteD = st.doc(TL, `${D}~note~autocancel-aside-${atD}`);
    assert(noteD && noteD.text === TEXT, 'the timeline has the note: ' + JSON.stringify(noteD));
    assert.deepStrictEqual(st.doc(CANCELLED, D).fates, [{ sheet: 'SS Sheet 1', fate: 'cut', text: 'already cut on SS Sheet 1: set aside' }], 'its cancel record says it was already cut');
    assert(!st.list(TL).some(x => x._id.startsWith(`${D}~removed~`)), 'and nothing says it was taken off');
    // it stays: a reload keeps it, until someone presses Set aside
    assert.deepStrictEqual(await settle(), [], 'looked at once');
    await page.click('#runMenuToggle');
    if (process.env.SHOTS) { await page.waitForTimeout(500); await page.screenshot({ path: path.join(process.env.SHOTS, 'auto-cancel-pill.png'), clip: { x: 700, y: 0, width: 800, height: 420 } }); }
    await page.click(`#runBanner [data-cxack="${D}"]`);
    await page.waitForFunction(() => document.getElementById('runBanner').classList.contains('hidden'), null, { timeout: 5000 });
    // (the cut sheet's step on its timeline now says it was done, and by whom: the same step, never a second)
    await until(() => st.list(TL).some(x => x._id.startsWith(`${D}~cancelStep~cx-`) && /set aside by Tester/.test(x.text || '')), 5000, 'the set-aside step');
    const acks = st.list(TL).filter(x => x._id.startsWith(`${D}~cancelStep~cx-`)), ack = acks[0];
    assert(acks.length === 1 && /^On a cut sheet: set aside by Tester \(SS Sheet 1\)/.test(ack.text), 'who set them aside is on the timeline: ' + ack.text);
    ok.push('cancelled on a cut sheet: nothing moves; the run pill and a note say to set the pieces aside, until Set aside; the timeline has both notes');

    /* 3 · a record taken back before the sorter acts: nothing moves */
    cancel(C, 'Sam');
    await page.evaluate(() => { CN.S.sheets.gold.pages[0].status = 'nesting'; });   // (a page busy: the job waits for it)
    const due3 = await page.evaluate(() => AutoCancel.poll());
    assert.deepStrictEqual(due3, [C], 'queued: ' + JSON.stringify(due3));
    await page.waitForTimeout(700);
    st.docs.delete(`${CANCELLED}/${C}`);                                                // restored meanwhile
    await page.evaluate(async () => { CN.S.sheets.gold.pages[0].status = 'complete'; await AutoCancel.idle(); });
    w = await where();
    assert(w.gold.includes(pid(C, 3, 1)) && w.rows.includes(C), 'an order that is not cancelled is never touched: ' + JSON.stringify(w));
    assert.strictEqual(st.doc(POOL, pid(C, 3, 1)).state, 'placed', 'its piece record neither');
    const s3 = await page.evaluate(C => AutoCancel.state(), C);
    assert(!s3.jobs[C] && !s3.done[C], 'and no job is left for it: ' + JSON.stringify(s3));
    ok.push('a cancel record taken back while the sorter waited for its sheet: the record is read again just before acting, nothing moves');

    /* 4 · a reload in the middle: the nest after the take-off cut short */
    await page.evaluate(() => localStorage.setItem('__nestMode', 'hang'));
    const atB = Date.now(); cancel(B, 'Tom', { at: atB });
    await page.evaluate(() => AutoCancel.poll());
    await until(() => page.evaluate(B => !CN.S.sheets.gold.pages[0].charms.some(c => c.order === B) && CN.S.sheets.gold.pages[0].status === 'nesting' && !!AutoCancel.state().jobs[B], B), 10000, 'the take-off');
    const mid = await page.evaluate(B => AutoCancel.state().jobs[B], B);
    assert(mid && mid.sheets.length === 1 && mid.sheets[0].id === 'gold-open-1' && mid.runSaved, 'the job is written down, with its sheet: ' + JSON.stringify(mid));
    assert(st.doc(SHEETS, 'gold-open-1').charms.some(c => c.order === B), 'the saved sheet still lists it (its nest was cut short)');
    await page.evaluate(() => Session.flush(true));
    await page.evaluate(() => localStorage.setItem('__nestMode', 'ok'));
    await page.reload();
    await boot();
    await page.waitForFunction(B => !AutoCancel.state().jobs[B], B, { timeout: 30000 });
    w = await where();
    assert(!w.gold.includes(pid(B, 2, 1)) && w.gold.includes(pid(C, 3, 1)), 'after the reload the page is as it was left: ' + JSON.stringify(w.gold));
    assert(w.nests.includes('gold-open-1:hand'), 'the job nests the sheet again after the reload: ' + JSON.stringify(w.nests));
    const nests4 = w.nests.length;
    assert(!st.doc(SHEETS, 'gold-open-1').charms.some(c => c.order === B), 'and saves it without the order');
    const pB = st.doc(POOL, pid(B, 2, 1));
    assert(pB.state === 'abandoned' && pB.removedBy === 'Tom' && pB.removedReason === 'cancelled' && pB.removedVerifiedAt > 0, 'its piece record: removed by Tom, verified: ' + JSON.stringify(pB));
    await until(() => { const e = st.doc(TL, `${B}~removed~${pB.removedAt}-gold-open-1`); return e && /^Removed from GF Sheet 1/.test(e.text || ''); }, 8000, 'the removed event');
    const evB = st.list(TL).filter(x => x._id.startsWith(`${B}~removed~`));
    assert(evB.length === 1 && evB[0].data.reason === 'cancelled by Tom' && evB[0].by === 'Tom', 'one "removed" on its timeline: ' + JSON.stringify(evB));
    assert.deepStrictEqual(st.doc(CANCELLED, B).fates, [{ sheet: 'GF Sheet 1', fate: 'removed', text: 'taken off GF Sheet 1' }], 'and its record what became of it');
    const s4 = await page.evaluate(B => AutoCancel.state(), B);
    assert(s4.done[B] && +s4.done[B].at === atB, 'done: ' + JSON.stringify(s4.done[B]));
    assert.deepStrictEqual(await settle(), [], 'looked at again: nothing left to do');
    assert.strictEqual((await where()).nests.length, nests4, 'never twice');
    ok.push('a reload in the middle (the nest cut short): the job is taken up after the reload, nested, saved and read back, once');

    assert.deepStrictEqual(errors, [], 'no page errors');
    console.log('auto-cancel-remove: all passed\n  ' + ok.join('\n  '));
  } catch (e) {
    console.error('errors:', errors);
    throw e;
  } finally {
    await browser.close(); srv.close();
  }
})().catch(e => { console.error(e); process.exit(1); });
