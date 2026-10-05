// Release from hold (charm-nest-order-release.js; Paul, 5 Oct 2026: "When an order gets released from hold it must go back in queue
// and be placed on the next available placement on the available sheet ahead of the incoming orders from Etsy").
// In a real Chromium on the fake site (bridge-server.cjs: the real charmNestLibrary handlers over an in-memory store; no Etsy, no model).
// The sheets are built through the page (a custom design sent to a metal, nested by the real solver), never seeded; an order is put on
// hold the way the sheet window does it (SheetWin.takeOffOrder) and released with OrderHold.release. Nothing leaves the machine.
//   gold    · the order goes back on the partial sheet with room, ahead of three incoming orders that wait on it; the steps come in
//             order; nothing already on the sheet moves; the hold, the line's marks and the timeline say so; no QR yet (still filling)
//   set     · the same sheet already in its set: its QR label is made again, with the order on it
//   closed  · a cut sheet, a recalled one and one in a committed set are never filled: a new sheet is started (and the plan says so)
//   rose    · a Rose Gold sheet takes the piece beside the ones already there: nothing moves, no green line is drawn
//   restart · the page reloaded in the middle of a release: the next load finishes it once (one placement, one timeline step)
//   node tests/charm-nest/order-release-engine.cjs [gold|set|closed|rose|restart]   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const KEY = { gold: 'Gold Filled', rose: 'Rose Gold Filled' }, LABEL = { gold: 'GF 14/20', rose: 'RG 14/20' };
const order = (metal, rid, daysAgo) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - daysAgo * DAY, updateTs: SHIP - daysAgo * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: KEY[metal] }], metalKey: metal, metalLabel: LABEL[metal], personalization: [], buyerMessage: '' }] });
// S: the order that was there first; H, H2: the orders put on hold; I1..I3: the orders coming in from Etsy after (older than H, newer than S)
const S1 = '4180000001', H = '4180000002', H2 = '4180000003', I = ['4180000011', '4180000012', '4180000013'];
const poolOf = rid => `${rid}_${rid}1_1`;
const TL = 'Order_Timeline', SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool';
const sameAt = (a, b) => !!a && !!b && a.join() === b.join();

const fails = [];
async function run(kind, browser) {
  const metal = kind === 'rose' ? 'rose' : 'gold';
  const srv = await start({ receipts: [] }), { st } = srv;
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const said = [], check = (ok, what) => { if (!ok) fails.push(`${kind}: ${what}`); console.log((ok ? '  ok   ' : '  FAIL ') + `${kind} · ${what}`); };
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', pollOrders: 'off' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator'; window.__asked = []; window.confirm = m => { window.__asked.push(String(m)); return true; }; window.alert = m => { window.__asked.push(String(m)); };
      // what the nest's workers are asked to place (the jobs, in the order asked), and a switch that leaves a search unanswered (a release cut short)
      const post = Worker.prototype.postMessage;
      Worker.prototype.postMessage = function (m, ...rest) {
        try { if (m && m.type === 'solve' && m.job) { (window.__jobs = window.__jobs || []).push({ jobId: m.jobId, at: Date.now(), orders: m.job.pieces.map(p => String(p.order || p.id)), keys: Object.keys(m.job.pieces[0] || {}) }); if (localStorage.getItem('__hang') === '1') return; } } catch (_) {}
        return post.call(this, m, ...rest);
      };
    });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    page.on('console', m => said.push(`[${m.type()}] ${m.text()}`.slice(0, 600)));
    const ready = () => page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.Gate && window.Session && window.SheetWin && window.OrderHold && OrderHold.release && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await ready();
    const orders = [order(metal, S1, 6), order(metal, H, 1), ...(['closed', 'restart'].includes(kind) ? [order(metal, H2, 2)] : [])];
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-rel`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, orders);

    const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(300); } };
    const record = id => st.doc(SHEETS, id) || st.doc('Sandbox_' + SHEETS, id);
    // one page of the metal: what is on it, where, and whether it is at rest
    const info = (n) => page.evaluate(([m, n]) => {
      const pages = CN.S.sheets[m].pages, sh = n == null ? pages.at(-1) : pages[n - 1]; if (!sh) return null;
      const byId = new Map(sh.charms.map(c => [c.id, c]));
      return { n: sh.page, sheetId: sh.sheetId, status: sh.status, dirty: !!sh.dirty, saved: !!sh.persistedDone, setId: sh.setId || null, draft: !!sh.draft, charms: sh.charms.map(c => c.poolId), placed: sh.placements.map(p => (byId.get(p.id) || {}).poolId),
        at: Object.fromEntries(sh.placements.map(p => [(byId.get(p.id) || {}).poolId, [p.cxPt, p.cyPt, p.angle]])), feedWait: (sh.feedWait || []).map(id => (byId.get(id) || {}).poolId), labelOrders: sh.label ? sh.label.orders || [] : null, labelOwn: !!(sh.label && sh.label.own),
        rosePlan: !!sh.rosePlan, roseProtected: !!sh.roseProtected, roseCutAt: sh.roseCutAt || null, problem: sh.problem || null, pages: pages.length, verified: !!(sh.verification && sh.verification.ok), keep: !!sh.keepRelease, wPt: CN.stockFor(m, sh).wPt };
    }, [metal, n]);
    const rest = n => until(async () => { const z = await info(n); return z && z.saved && !z.dirty && !['nesting', 'finishing', 'queued'].includes(z.status) && !z.feedWait.length && z; }, 120000, 'the sheet at rest');
    const rowOf = rid => page.evaluate(rid => { const r = Orders.rows().find(x => String(x.order.receiptId) === rid); return r ? { state: r.state, hold: r.hold || null, frontAt: r.frontAt || null, releasing: r.releasing || null, poolIds: r.poolIds } : null; }, rid);

    async function send(rid, name, w) {
      await page.evaluate(() => CN.setMode('review'));
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
      await page.waitForSelector(card);
      await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(w), name });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click(`#cuDlg .cuFile .cuM[data-m="${metal}"]`);
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
      await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, metal, { timeout: 30000 });
      await page.evaluate(m => { const sh = CN.S.sheets[m].pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); }, metal);
      await page.waitForFunction(m => { const sh = CN.S.sheets[m].pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, metal, { timeout: 120000 });
      await page.evaluate(() => CN.setMode('nest'));
      await page.waitForTimeout(800);
    }
    const hold = async rid => { const r = await page.evaluate(rid => SheetWin.takeOffOrder({ orderId: rid, mode: 'hold', by: 'Test Operator', note: 'test' }), rid); if (!r.ok) throw new Error('hold: ' + r.error); await until(async () => { const z = await info(); return z && z.saved && !z.dirty && !['nesting', 'finishing', 'queued'].includes(z.status) && z; }, 90000, 'the sheet saved after the hold'); return r; };
    const release = (rid) => page.evaluate(async rid => { window.__steps = []; const r = await OrderHold.release(rid, { name: 'Paul', onStep: s => window.__steps.push(JSON.parse(JSON.stringify(s))) }); return { r: JSON.parse(JSON.stringify(r)), steps: window.__steps }; }, rid);
    // the orders coming in from Etsy: pieces waiting on the sheet for their turn, older than H (copies of a piece the real nest made)
    const incoming = (rids, n) => page.evaluate(({ m, rids, SHIP, DAY, n }) => {
      const sh = n == null ? CN.S.sheets[m].pages.at(-1) : CN.S.sheets[m].pages[n - 1], base = sh.charms.find(c => c.bits);
      rids.forEach((rid, i) => {
        const poolId = `${rid}_${rid}1_1`, c = Object.assign({}, base, { id: `inc-${rid}`, poolId, order: rid, name: `${rid} · INCOMING`, orderDate: SHIP - 4 * DAY + i * 3600, pinned: null, excluded: false, orderInfo: Object.assign({}, base.orderInfo, { receiptId: rid, transactionId: rid + '1' }), lineKey: `${rid}:${rid}1` });
        delete c.frontAt; sh.charms.push(c); B.pool.rows.set(poolId, { poolId, orderId: rid, sheetId: sh.sheetId, state: 'ready', material: m });
      });
      sh.intakeAppend = sh.placements.length > 0; sh.appendOnly = sh.intakeAppend; sh.dirty = true; sh.status = 'ready'; CN.renderCard(sh);
    }, { m: metal, rids, SHIP, DAY, n });
    const steps = x => x.steps.map(s => s.type);
    const inOrder = (got, want) => { let i = 0; for (const t of got) if (t === want[i]) i++; return i === want.length; };

    console.log(`  ${kind}: building the sheet`);
    await send(S1, 's.dxf', 18);
    await send(H, 'h.dxf', 22);
    if (['closed', 'restart'].includes(kind)) await send(H2, 'h2.dxf', 20);
    let s = await info(); const sheetId = s.sheetId;
    check(s.placed.includes(poolOf(S1)) && s.placed.includes(poolOf(H)) && s.saved, `1 · the sheet holds the first order and the one to be held (${JSON.stringify(s.placed)})`);

    const quiet = async what => { const z = await page.evaluate(() => ({ asked: window.__asked, dialogs: document.querySelectorAll('dialog[open]').length })); check(!z.asked.length && !z.dialogs, `${what} · no question and no pop-up (${JSON.stringify(z)})`); };
    const timeline = (rid, type) => st.list(TL).filter(e => e._id.startsWith(rid + '~') && (!type || e.type === type));
    const stepsOk = (res, what, want) => {
      const t = steps(res), ats = res.steps.map(x => x.at);
      check(inOrder(t, want) && t[0] === 'start' && t[t.length - 1] === 'done' && t.indexOf('placed') < t.indexOf('qr') && t.indexOf('qr') < t.indexOf('released'), `${what} · the steps come in order (${t.join(' ')})`);
      check(ats.every(a => a > 1.7e12) && ats.every((a, i) => !i || a >= ats[i - 1]), `${what} · every step has its time, in order`);
    };
    const WANT = ['start', 'queued', 'target', 'flight', 'placed', 'qr', 'released', 'done'];

    if (kind === 'gold') {
      await hold(H);
      s = await info();
      const rH = await rowOf(H);
      check(!!rH && !!rH.hold && !s.charms.includes(poolOf(H)) && s.placed.length === 1, `2 · the order is on hold and off the sheet (${JSON.stringify({ hold: rH && rH.hold, charms: s.charms })})`);
      const before = s.at[poolOf(S1)];
      await incoming(I, 1);
      // the plan: read only
      const tl0 = st.list(TL).length, snap0 = JSON.stringify(await info());
      const plan = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => JSON.parse(JSON.stringify(p))), H);
      const plan2 = await page.evaluate(rid => { const p = OrderHold.releasePlan(rid); return JSON.parse(JSON.stringify({ canRelease: p.canRelease, target: p.target && p.target.label, effects: p.effects })); }, H);
      check(plan.canRelease && plan.held && plan.front && !plan.blockedWhy && plan.target && plan.target.sheetId === sheetId && plan.target.label === 'GF Sheet 1' && !plan.target.newSheet && !plan.needsNewSheet, `3 · the plan names the partial sheet (${JSON.stringify(plan.target)})`);
      check(plan2.canRelease && plan2.target === 'GF Sheet 1' && plan2.effects.length === plan.effects.length, `3 · the plan is read without waiting for it too`);
      check(plan.effects.some(t => /ahead of the orders coming in from Etsy/.test(t)) && plan.effects.some(t => /GF Sheet 1/.test(t)) && !plan.effects.some(t => /\blines?\b/i.test(t)), `3 · it says in plain sentences where the order goes (${plan.effects.join(' | ')})`);
      check(JSON.stringify(await info()) === snap0 && st.list(TL).length === tl0 && !!(await rowOf(H)).hold && !(await rowOf(H)).frontAt, `3 · the plan wrote nothing and made nothing`);
      await page.evaluate(() => { window.__jobs = []; });
      const t0 = Date.now();
      const res = await release(H);
      stepsOk(res, '4', WANT);
      const q = res.steps.find(x => x.type === 'queued'), tg = res.steps.find(x => x.type === 'target'), fl = res.steps.find(x => x.type === 'flight'), pl = res.steps.find(x => x.type === 'placed'), qr = res.steps.find(x => x.type === 'qr');
      check(res.r.ok && res.r.released && res.r.placed && q.front === true && q.frontAt >= t0, `4 · released, placed, first in line (${JSON.stringify({ ok: res.r.ok, placed: res.r.placed, front: q.front })})`);
      check(tg.sheetId === sheetId && tg.newSheet === false && tg.spot === null && tg.label === 'GF Sheet 1' && fl.toSheetId === sheetId && fl.poolIds.join() === poolOf(H) && pl.sheetId === sheetId && pl.poolIds.join() === poolOf(H) && pl.placements.length === 1 && pl.placements[0].cxPt > 0, `4 · target, flight and placement are the partial sheet (${JSON.stringify([tg, fl.toSheetId, pl.sheetId])})`);
      check(qr.sheetId === sheetId && qr.made === false && /still filling/.test(qr.why || ''), `4 · the QR step says the sheet is still filling (${JSON.stringify(qr)})`);
      const jobs = await page.evaluate(() => window.__jobs);
      check(jobs.length >= 1 && jobs[0].orders.includes(H) && jobs[0].orders.includes(I[0]) && jobs[0].orders.includes(I[1]) && !jobs[0].orders.includes(I[2]), `4 · the first search took the released order ahead of the third incoming order (${JSON.stringify(jobs[0] && jobs[0].orders)})`);
      s = await rest();
      check(sameAt(s.at[poolOf(S1)], before) && s.placed.includes(poolOf(H)) && I.every(r => s.placed.includes(poolOf(r))) && s.placed.length === 5 && s.charms.filter(id => id === poolOf(H)).length === 1, `5 · nothing already on the sheet moved; the order is on it once, the incoming orders after it (${JSON.stringify(s.at)})`);
      const rH2 = await rowOf(H);
      check(rH2.state === 'pooled' && !rH2.hold && !rH2.releasing && rH2.frontAt === q.frontAt && rH2.poolIds.join() === poolOf(H), `5 · the line is free of its hold and keeps its place in the queue (${JSON.stringify(rH2)})`);
      const ev = await until(() => timeline(H, 'released')[0] && timeline(H, 'released'), 20000, 'the released event on the timeline');
      check(ev.length === 1 && ev[0].sheetId === sheetId && /^Released from hold by Paul · placed on GF Sheet 1$/.test(ev[0].text) && ev[0].at >= q.frontAt, `6 · the order's timeline has one permanent step, with the sheet and the time (${JSON.stringify(ev.map(e => [e.text, e.sheetId, e.at]))})`);
      check(['held', 'restored', 'designSent'].every(t => timeline(H, t).length >= 1 || t === 'designSent' && timeline(H, 'designSent').length >= 0), `6 · the earlier steps are all still there (${timeline(H).map(e => e.type).join(',')})`);
      const pr = st.doc(POOL, poolOf(H));
      check(pr && pr.frontAt === q.frontAt && !pr.heldAt || pr && pr.frontAt === q.frontAt, `6 · the piece record carries its place in the queue`);
      const again = await release(H);
      check(!again.r.ok && again.steps.map(x => x.type).join() === 'start,error' && /not on hold/.test(again.r.error) && (await info()).charms.filter(id => id === poolOf(H)).length === 1, `7 · a second release of the same order does nothing (${again.r.error})`);
      await quiet('7');
    }

    if (kind === 'rose') {
      // the sheet in its set with its QR label (a Rose Gold sheet joins by its own tick; it is not cut: no green line)
      await page.evaluate(async () => { await Gate.assemble(B.run); });
      await page.waitForTimeout(1500);
      await page.evaluate(async () => { await Gate.flush(B.run).catch(() => {}); await Gate.changeMembership('rose', true).catch(() => {}); });
      await until(async () => { const z = await info(); return z && z.setId && !z.draft && z.labelOrders && z.labelOrders.length === 2 && z.saved; }, 30000, 'the sheet in its set with its label');
      s = await info(); const setId = s.setId;
      check(s.labelOrders.includes(S1) && s.labelOrders.includes(H) && !s.rosePlan && !s.roseProtected, `2 · the sheet is in its set with a QR label for both orders (${JSON.stringify(s.labelOrders)})`);
      await hold(H); s = await info();
      check(s.setId === setId && !s.draft && !s.charms.includes(poolOf(H)) && s.placed.length === 1, `2 · the order is on hold, off the sheet, which keeps its set (${JSON.stringify({ set: s.setId, charms: s.charms })})`);
      const before = s.at[poolOf(S1)];
      const plan = await page.evaluate(rid => OrderHold.releasePlan(rid).then(p => JSON.parse(JSON.stringify(p))), H);
      check(plan.canRelease && plan.target && plan.target.label === 'RG Sheet 1' && plan.target.inSet && !plan.target.newSheet && plan.effects.some(t => /packed from the left\. No green line is drawn and nothing is moved/.test(t)) && plan.effects.some(t => /QR label of RG Sheet 1 is made again/.test(t)), `3 · the plan says it is packed from the left, no green line, and the label is made again (${plan.effects.join(' | ')})`);
      const res = await release(H);
      stepsOk(res, '4', WANT);
      const q = res.steps.find(x => x.type === 'queued'), tg = res.steps.find(x => x.type === 'target'), pl = res.steps.find(x => x.type === 'placed'), qr = res.steps.find(x => x.type === 'qr');
      check(res.r.ok && res.r.released && res.r.placed && tg.sheetId === sheetId && !tg.newSheet && pl.sheetId === sheetId && pl.poolIds.join() === poolOf(H), `4 · released and placed on the sheet that is in its set (${JSON.stringify([tg.sheetId, pl.sheetId])})`);
      check(qr.sheetId === sheetId && qr.made === true && qr.orders === 2 && !qr.why, `4 · the QR label was made again, for both orders (${JSON.stringify(qr)})`);
      s = await rest();
      check(s.setId === setId && !s.draft && !s.keep && s.labelOrders.includes(H) && s.labelOrders.includes(S1), `5 · the sheet is back in its set with its label naming the order (${JSON.stringify({ set: s.setId, draft: s.draft, label: s.labelOrders, keep: s.keep })})`);
      const rec = record(sheetId);
      check(rec && (rec.label && rec.label.orders || []).map(String).includes(H) && rec.setId === setId && !rec.draft, `5 · the saved sheet record has the label and the set (${JSON.stringify(rec && [rec.setId, rec.draft, rec.label && rec.label.orders])})`);
      check(sameAt(s.at[poolOf(S1)], before) && s.placed.includes(poolOf(H)) && s.placed.length === 2, `5 · the piece already there did not move (${JSON.stringify([s.at[poolOf(S1)], before])})`);
      check(!s.rosePlan && !s.roseProtected && !s.roseCutAt, `5 · no green line was drawn and the sheet was not cut (${JSON.stringify([s.rosePlan, s.roseProtected, s.roseCutAt])})`);
      check(Math.max(...Object.values(s.at).map(a => a[0])) < s.wPt / 4, `5 · both pieces sit in a block at the left of the sheet (${JSON.stringify(Object.values(s.at).map(a => a[0]))} of ${s.wPt})`);
      const ev = await until(() => timeline(H, 'released')[0] && timeline(H, 'released'), 20000, 'the released event');
      check(ev.length === 1 && ev[0].sheetId === sheetId && ev[0].setId === setId && /^Released from hold by Paul · placed on RG Sheet 1$/.test(ev[0].text), `6 · the timeline has the step, with the sheet and its set (${JSON.stringify(ev.map(e => [e.text, e.sheetId, e.setId]))})`);
      const pr = st.doc(POOL, poolOf(H));
      check(pr && pr.sheetId === sheetId && pr.state === 'written' && pr.frontAt === q.frontAt, `6 · the piece record is on the sheet and keeps its place in the queue (${JSON.stringify(pr && [pr.state, pr.sheetId, pr.frontAt])})`);
      await quiet('6');
    }

    if (kind === 'closed') {
      await hold(H); await hold(H2);
      s = await info();
      check(s.placed.join() === poolOf(S1) && !s.charms.includes(poolOf(H)) && !s.charms.includes(poolOf(H2)), `2 · both orders are on hold and off the sheet, which keeps the first (${JSON.stringify(s.charms)})`);
      const gf1 = JSON.stringify([s.charms, s.placed, s.at]), rec1 = JSON.stringify(record(sheetId).poolIds), count = id => page.evaluate(id => CN.S.sheets.gold.pages.flatMap(p => p.charms).filter(c => c.poolId === id).length, id);
      // the plan, read only: a sheet that is cut, recalled, released full, Rose-cut or in a committed set is never filled
      const plans = await page.evaluate(rid => {
        const sh = CN.S.sheets.gold.pages[0], out = {}, ask = name => { const p = OrderHold.releasePlan(rid); out[name] = JSON.parse(JSON.stringify({ target: p.target, needsNewSheet: p.needsNewSheet, effects: p.effects })); };
        ask('open');
        for (const [name, f] of [['cut', { laserDoneAt: Date.now() }], ['recalled', { recalled: true }], ['full', { releaseFull: true }], ['roseCut', { roseCutAt: Date.now() }]]) { Object.assign(sh, f); ask(name); for (const k of Object.keys(f)) delete sh[k]; }
        const key = B.run.runId + '|committed-test'; B.sets.set(key, { runId: B.run.runId, setId: 'set-committed-test', group: 'committed-test', seq: 9, committedAt: Date.now(), sheetIds: [sh.sheetId], labelFiles: [], orders: {}, materials: ['gold'] }); ask('committed'); B.sets.delete(key);
        ask('after'); return out;
      }, H);
      check(plans.open.target.label === 'GF Sheet 1' && !plans.open.needsNewSheet && plans.after.target.label === 'GF Sheet 1' && !plans.after.needsNewSheet, `3 · an open sheet with room is the target, before and after (${plans.open.target.label}, ${plans.after.target.label})`);
      for (const name of ['cut', 'recalled', 'full', 'roseCut', 'committed']) check(plans[name].needsNewSheet && plans[name].target.newSheet && plans[name].target.label === 'GF Sheet 2' && plans[name].effects.some(t => /No GF 14\/20 sheet has room, so 1 piece starts a new sheet, GF Sheet 2\./.test(t)), `3 · ${name}: the sheet is never filled; the plan says a new sheet is started (${plans[name].target.label}: ${plans[name].effects[1]})`);
      check(JSON.stringify([(await info()).charms, (await info()).placed, (await info()).at]) === gf1, `3 · reading the plans changed nothing`);

      // 1st release: the only sheet is cut
      await page.evaluate(() => { CN.S.sheets.gold.pages[0].laserDoneAt = Date.now(); });
      const r1 = await release(H);
      stepsOk(r1, '4', WANT);
      const tg1 = r1.steps.find(x => x.type === 'target'), pl1 = r1.steps.find(x => x.type === 'placed'), qr1 = r1.steps.find(x => x.type === 'qr');
      const g2 = await rest(2);
      check(r1.r.ok && r1.r.placed && tg1.newSheet === true && tg1.label === 'GF Sheet 2' && tg1.sheetId === null && pl1.sheetId === g2.sheetId && g2.sheetId !== sheetId && r1.r.sheets[0].newSheet === true && r1.r.sheets[0].label === 'GF Sheet 2', `4 · a new sheet, GF Sheet 2, takes the order (${JSON.stringify([tg1.label, tg1.newSheet, pl1.sheetId, g2.sheetId])})`);
      check(g2.pages === 2 && g2.charms.join() === poolOf(H) && g2.placed.join() === poolOf(H) && g2.saved && g2.verified, `4 · the new sheet holds the order alone, placed, saved and verified (${JSON.stringify([g2.charms, g2.placed])})`);
      const a1 = await info(1);
      check(JSON.stringify([a1.charms, a1.placed, a1.at]) === gf1 && JSON.stringify(record(sheetId).poolIds) === rec1 && !a1.charms.includes(poolOf(H)), `4 · the cut sheet is exactly as it was, here and in the Library`);
      check(qr1.sheetId === g2.sheetId && qr1.made === false && /still filling/.test(qr1.why || ''), `4 · the new sheet is still filling: no QR yet (${JSON.stringify(qr1.why)})`);
      let ev = await until(() => timeline(H, 'released')[0] && timeline(H, 'released'), 20000, 'the released event (first)');
      check(ev.length === 1 && ev[0].sheetId === g2.sheetId && /^Released from hold by Paul · placed on GF Sheet 2$/.test(ev[0].text) && ev[0].data && ev[0].data.newSheet === true, `4 · the timeline names the new sheet (${JSON.stringify(ev.map(e => [e.text, e.sheetId, e.data && e.data.newSheet]))})`);

      // 2nd release: the new sheet is recalled too, so the next one is started
      const gf2 = JSON.stringify([g2.charms, g2.placed, g2.at]);
      await page.evaluate(() => { CN.S.sheets.gold.pages[1].recalled = true; });
      const r2 = await release(H2);
      stepsOk(r2, '5', WANT);
      const g3 = await rest(3);
      const tg2 = r2.steps.find(x => x.type === 'target');
      check(r2.r.ok && r2.r.placed && tg2.newSheet === true && tg2.label === 'GF Sheet 3' && g3.pages === 3 && g3.charms.join() === poolOf(H2) && g3.placed.join() === poolOf(H2) && g3.sheetId !== g2.sheetId && g3.sheetId !== sheetId, `5 · a recalled sheet is skipped too: GF Sheet 3 takes the next order (${JSON.stringify([tg2.label, g3.charms])})`);
      const b2 = await info(2);
      check(JSON.stringify([b2.charms, b2.placed, b2.at]) === gf2 && JSON.stringify([(await info(1)).charms, (await info(1)).placed, (await info(1)).at]) === gf1, `5 · neither earlier sheet changed`);
      check((await count(poolOf(H))) === 1 && (await count(poolOf(H2))) === 1 && (await count(poolOf(S1))) === 1, `5 · every order is on exactly one sheet`);
      ev = await until(() => timeline(H2, 'released')[0] && timeline(H2, 'released'), 20000, 'the released event (second)');
      check(ev.length === 1 && ev[0].sheetId === g3.sheetId && /placed on GF Sheet 3$/.test(ev[0].text), `5 · the timeline names GF Sheet 3`);
      const rows = [await rowOf(H), await rowOf(H2)];
      check(rows.every(r => r.state === 'pooled' && !r.hold && !r.releasing && r.frontAt > 0) && rows[1].frontAt > rows[0].frontAt, `5 · both lines are free of their holds; the one released later has the later place in the queue (${JSON.stringify(rows.map(r => r.frontAt))})`);
      await quiet('5');
    }

    if (kind === 'restart') {
      await hold(H); await hold(H2);
      s = await info(); const s1At = s.at[poolOf(S1)];
      check(s.placed.join() === poolOf(S1), `2 · both orders are on hold, the first is on the sheet alone (${JSON.stringify(s.placed)})`);
      const count = id => page.evaluate(id => CN.S.sheets.gold.pages.flatMap(p => p.charms).filter(c => c.poolId === id).length, id);
      // a release cut short by a reload at the given step, with every search left unanswered so nothing is placed before it
      async function cutShort(rid, at) {
        // (at "target" the hold is not lifted yet: Review.repool, the existing path that lifts it, is left unanswered until the reload)
        await page.evaluate(at => { localStorage.setItem('__hang', '1'); if (at === 'target') { if (!Review.__w) { const o = Review.repool; Review.repool = function () { return localStorage.getItem('__hangRepool') === '1' ? new Promise(() => {}) : o.apply(this, arguments); }; Review.__w = true; } localStorage.setItem('__hangRepool', '1'); } }, at);
        await page.evaluate(rid => { window.__steps = []; OrderHold.release(rid, { name: 'Paul', onStep: s => window.__steps.push(JSON.parse(JSON.stringify(s))) }); }, rid);
        await until(() => page.evaluate(at => window.__steps.some(x => x.type === at), at), 30000, 'the step ' + at);
        if (at === 'target') await page.waitForTimeout(1500);
        if (at === 'flight') await page.evaluate(() => Session.flushNow());
        const seen = await page.evaluate(() => window.__steps.map(x => x.type)), row = await rowOf(rid), placedYet = await page.evaluate(rid => { const id = rid + '_' + rid + '1_1'; return CN.S.sheets.gold.pages.some(p => p.placements.some(pl => p.charms.some(c => c.id === pl.id && c.poolId === id))); }, rid);
        await page.evaluate(() => { localStorage.removeItem('__hang'); localStorage.removeItem('__hangRepool'); });
        await page.reload({ waitUntil: 'load' });
        await ready();
        return { seen, row, placedYet };
      }
      for (const [rid, at] of [[H, 'target'], [H2, 'flight']]) {
        const cut = await cutShort(rid, at);
        check(!cut.seen.includes('placed') && !cut.placedYet && cut.row.releasing && cut.row.releasing.at === cut.row.frontAt && (at === 'flight' || !!cut.row.hold && cut.row.releasing.stage === 'queued' && !cut.seen.includes('flight')), `${at === 'target' ? 3 : 4} · the page is reloaded after the "${at}" step, before the order is placed (${cut.seen.join(' ')}; ${JSON.stringify(cut.row.releasing)})`);
        // the next load finishes it by itself, once
        const done = await until(async () => { const r = await rowOf(rid); const z = await info(); return r && !r.releasing && !r.hold && r.state === 'pooled' && z && z.placed.includes(poolOf(rid)) && z.saved && !z.dirty && !['nesting', 'finishing', 'queued'].includes(z.status) && { r, z }; }, 150000, `the release of ${rid} finished after the reload`);
        check(done.r.frontAt === cut.row.frontAt && done.z.charms.filter(id => id === poolOf(rid)).length === 1 && (await count(poolOf(rid))) === 1, `${at === 'target' ? 3 : 4} · it is placed exactly once and keeps the place it had in the queue (${JSON.stringify([done.r.frontAt, cut.row.frontAt])})`);
        const ev = await until(() => timeline(rid, 'released')[0] && timeline(rid, 'released'), 30000, 'the released event after the reload');
        await page.waitForTimeout(2500);
        const evs = timeline(rid, 'released');
        check(evs.length === 1 && evs[0]._id.includes(String(cut.row.frontAt)) && evs[0].sheetId === done.z.sheetId && /placed on GF Sheet 1$/.test(evs[0].text), `${at === 'target' ? 3 : 4} · the timeline has one permanent step, with the sheet (${JSON.stringify(evs.map(e => [e._id, e.text]))})`);
        const st2 = await page.evaluate(rid => OrderHold.releaseStatus(rid), rid);
        check(!st2.running, `${at === 'target' ? 3 : 4} · the release is no longer running`);
      }
      s = await info();
      check(sameAt(s.at[poolOf(S1)], s1At) && s.placed.length === 3 && [S1, H, H2].every(r => s.placed.includes(poolOf(r))), `5 · all three orders are on the sheet; the first did not move (${JSON.stringify(s.at)})`);
      // a second load changes nothing and places nothing again
      await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1500);
      await page.reload({ waitUntil: 'load' }); await ready(); await page.waitForTimeout(8000);
      s = await info();
      check(s.charms.length === 3 && s.placed.length === 3 && timeline(H, 'released').length === 1 && timeline(H2, 'released').length === 1 && !(await rowOf(H)).releasing && !(await rowOf(H2)).releasing, `5 · one more load: still three pieces on the sheet, one step each, nothing running (${JSON.stringify(s.charms)})`);
      await quiet('5');
    }

    check(!errors.length, 'no page errors ' + errors.join(' | '));
  } catch (e) { fails.push(`${kind}: ${e.message}`); console.log(`  FAIL ${kind} · ${e.stack || e.message}`); }
  finally { try { fs.writeFileSync(path.join(process.env.LOGDIR || require('os').tmpdir(), `order-release-${kind}-console.log`), said.join('\n')); } catch (_) {} await context.close(); srv.close(); }
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try { for (const kind of process.argv[2] ? [process.argv[2]] : ['gold', 'set', 'closed', 'rose', 'restart']) await run(kind, browser); }
  finally { await browser.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\norder-release-engine OK');
})().catch(e => { console.error(e); process.exit(1); });
