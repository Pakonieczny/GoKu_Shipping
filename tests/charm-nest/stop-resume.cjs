// Stop and Resume on the run banner, against the real sorter and the framed Design Station on the repo's fake site
// (bridge-server.cjs, through day-sim/harness.cjs):
//  1 · Stop with one sheet nesting and another queued behind it: the nesting one finishes and saves as before, the queued
//      one keeps its place, its card says it waits for Resume, it does not start when the first finishes, and it starts
//      on Resume.
//  2 · Stop while the banner says "Adding new orders": the charm searches end at once, every charm placed before stays
//      exactly where it was, the new orders wait on their sheets (not moved to another sheet, not "did not fit", not
//      counted among a sheet's gap-fill tries), the run is stopped with its own plain reason and no alarm, nothing starts
//      until Resume, and Resume places them around the charms already there. A double click on Stop lands on Resume,
//      which waits until the new orders' step has wound down before the run carries on.
//  3 · A reload while an approved engraving's back file is being written: once the workspace is restored the write runs
//      again, and the approval ends written, with a back file for every copy.
//   node tests/charm-nest/stop-resume.cjs [playwright-core dir]
const assert = require('assert');
if (process.argv[2]) process.env.PW_DIR = process.argv[2];
const { open } = require('./day-sim/harness.cjs');

const day = Math.floor(Date.now() / 1000);
const GF = '14k Gold Filled', SS = 'Sterling Silver';
const receipt = (rid, sku, metal, words) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: '', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false,
  transactions: [{ transaction_id: Number(`${rid}1`), listing_id: 1718000 + +sku.slice(-2), receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 3 * 86400, is_personalized: !!words,
    variations: [{ formatted_name: 'Metal', formatted_value: metal }].concat(words ? [{ formatted_name: 'Personalization', formatted_value: words }] : []) }] });
const receipts = [
  receipt(3700000001, 'BR-TST-01', GF, 'ANNA'), receipt(3700000002, 'BR-TST-02', GF), receipt(3700000003, 'BR-TST-03', GF),
  receipt(3700000004, 'BR-TST-04', SS), receipt(3700000005, 'BR-TST-05', SS), receipt(3700000006, 'BR-TST-06', SS)
];
const arrivals = [4, 5, 6, 7, 8].map((n, i) => receipt(3700000011 + i, `BR-TST-0${n}`, GF)).concat([1, 2, 3, 7, 8].map((n, i) => receipt(3700000021 + i, `BR-TST-0${n}`, SS)));
const arrivals2 = [1, 2, 3].map((n, i) => receipt(3700000031 + i, `BR-TST-0${n}`, GF)).concat([4, 5, 6].map((n, i) => receipt(3700000041 + i, `BR-TST-0${n}`, SS)));
const RESUME = 'Waits for Resume';

(async () => {
  const H = await open({ receipts, masters: [{ count: 8, scale: 0.9 }], settings: { budgetS: 30, runMode: 'manual', pullMode: 'all', pollOrders: 'off', heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, ctx, st } = H;
  page.on('dialog', d => d.accept().catch(() => {}));
  const log = (...a) => console.log(...a);
  // every toast, kept after it fades
  const recordToasts = () => page.evaluate(() => { window.__toasts = window.__toasts || []; const host = document.getElementById('toasts'); if (!host || host.__rec) return; host.__rec = true; new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1) window.__toasts.push((n.dataset.kind || 'plain') + ': ' + (n.dataset.msg || n.textContent)); }).observe(host, { childList: true }); });
  const toasts = () => page.evaluate(() => (window.__toasts || []).slice());
  const run = () => page.evaluate(() => B.run && { status: B.run.status, step: B.run.step, stoppedBy: B.run.stoppedBy || null, busy: !!B.run.arrivalBusy, errors: (B.run.errors || []).map(e => e.why), holds: Object.assign({}, B.run.sheetHolds || {}) });
  const sheets = () => page.evaluate(() => allSheets().filter(p => B.run && p.runId === B.run.runId).map(p => {
    const pool = id => { const c = p.charms.find(x => x.id === id); return c ? c.poolId || c.id : id; };
    const stage = p.el && p.el.querySelector('[data-r=stage]');
    return { key: p.metal + ':' + p.page, metal: p.metal, status: p.status, stage: p.stage || '', dirty: !!p.dirty, runHold: p.runHold || null, persistedDone: !!p.persistedDone, pages: CN.pagesOf(p.metal).length,
      charms: CN.activeCharms(p).map(c => c.poolId || c.id), placed: p.placements.map(q => ({ id: pool(q.id), x: +(+q.cxPt).toFixed(4), y: +(+q.cyPt).toFixed(4), a: q.angle })),
      rejects: (p.rejects || []).length, endedBy: p.endedBy || null, topup: p.topup ? JSON.stringify(p.topup) : null, card: stage ? { text: stage.textContent, spinner: !!stage.querySelector('.spin') } : null };
  }));
  const banner = () => page.evaluate(() => ({ text: (document.getElementById('runBanner') || {}).textContent.replace(/\s+/g, ' ').trim(), stop: !!document.getElementById('rbStop'), resume: !!document.getElementById('rbResume') }));
  // the run at rest: nothing nesting, queued, finishing or unsaved, no intake going on
  const settle = (timeout = 300000) => page.waitForFunction(() => {
    const r = B.run; if (!r || r.arrivalBusy || !['processed', 'review', 'paused', 'complete'].includes(r.status)) return false;
    return !allSheets().filter(p => p.runId === r.runId).some(p => ['nesting', 'finishing', 'queued'].includes(p.status) || p._operationStarting || p.dirty || p.persisted && !p.persistedDone);
  }, null, { timeout, polling: 250 });
  const samePlace = (a, b) => a && b && Math.abs(a.x - b.x) < 1e-3 && Math.abs(a.y - b.y) < 1e-3 && a.a === b.a;
  /** every charm placed in `before` is still placed, exactly where it was */
  function unmoved(before, after, when) {
    for (const s of before) for (const p of s.placed) {
      const now = after.flatMap(x => x.placed).find(q => q.id === p.id);
      assert(now, `${when}: ${p.id} (placed on ${s.key}) is no longer placed`);
      assert(samePlace(p, now), `${when}: ${p.id} moved: ${JSON.stringify(p)} → ${JSON.stringify(now)}`);
    }
  }
  /** the stream's own check, asked as a Manual stream would ask it: a sheet waiting for Resume holds the next step */
  const streamHeld = () => page.evaluate(() => { const mode = CN.S.settings.runMode, was = Sandbox.streaming; CN.S.settings.runMode = 'manual'; Sandbox.streaming = () => true; try { return Arrivals.held(); } finally { CN.S.settings.runMode = mode; Sandbox.streaming = was; } });
  try {
    await recordToasts();

    // ── 1 · Stop with a sheet nesting and another queued behind it ──
    await page.evaluate(() => { CN.setMode('nest'); RunCtl.setMode('auto'); });
    await page.waitForFunction(() => { const r = B.run; if (!r || r.status !== 'running') return false; const s = allSheets().filter(p => p.runId === r.runId); return s.some(p => p.status === 'nesting') && s.some(p => p.status === 'queued'); }, null, { timeout: 240000, polling: 5 });
    const s1 = await page.evaluate(() => { const r = B.run, mine = allSheets().filter(p => p.runId === r.runId), was = mine.map(p => ({ key: p.metal + ':' + p.page, status: p.status }));
      document.getElementById('rbStop').click();
      return { was, run: { status: r.status, stoppedBy: r.stoppedBy, busy: !!r.arrivalBusy }, now: mine.map(p => ({ key: p.metal + ':' + p.page, status: p.status, stage: p.stage })) }; });
    log('1 · stopped with', JSON.stringify(s1.was), '→', JSON.stringify(s1.now));
    assert.equal(s1.run.status, 'stopped'); assert.equal(s1.run.stoppedBy, 'stopped by the operator'); assert.equal(s1.run.busy, false);
    const nestingKey = s1.was.find(x => x.status === 'nesting').key, queuedKeys = s1.was.filter(x => x.status === 'queued').map(x => x.key);
    for (const k of queuedKeys) assert.equal(s1.now.find(x => x.key === k).stage, RESUME, `${k}: a queued sheet says at once that it waits for Resume`);
    assert.equal(s1.now.find(x => x.key === nestingKey).status, 'nesting', 'the sheet already nesting carries on');
    // the sheet already nesting finishes and saves as before
    await page.waitForFunction(k => { const p = allSheets().find(x => x.metal + ':' + x.page === k); return p && ['complete', 'partial'].includes(p.status) && p.persistedDone; }, nestingKey, { timeout: 240000, polling: 100 });
    await page.waitForTimeout(3000);
    let sh = await sheets(), b = await banner();
    log('1 · after the nesting sheet finished:', sh.map(s => `${s.key} ${s.status} "${s.stage}" ${s.placed.length}/${s.charms.length}`).join(' · '), '| banner:', b.text.slice(0, 120));
    assert(['complete', 'partial'].includes(sh.find(s => s.key === nestingKey).status));
    assert(!sh.find(s => s.key === nestingKey).runHold, 'a sheet that finished on its own is not held');
    for (const k of queuedKeys) {
      const s = sh.find(x => x.key === k);
      assert.equal(s.status, 'queued', `${k} did not start while the run is stopped`); assert.equal(s.stage, RESUME);
      if (s.card) { assert(s.card.text.includes(RESUME), `${k}: the card says it waits for Resume (${s.card.text})`); assert(!s.card.spinner, 'a sheet waiting for Resume shows no spinner'); }
    }
    assert(!sh.some(s => ['nesting', 'finishing'].includes(s.status)), 'nothing nests while the run is stopped');
    assert(/Stopped:\s*stopped by the operator/.test(b.text), b.text); assert(!/still finishing/.test(b.text), 'a sheet waiting for Resume is not "still finishing": ' + b.text); assert(b.resume && !b.stop);
    assert.equal(await streamHeld(), 'the run is stopped', 'the sandbox stream waits while a sheet waits for Resume');
    assert(!(await toasts()).some(t => /^bad:/.test(t)), 'the Stop a person pressed raises no alarm: ' + (await toasts()).join(' | '));
    // Resume starts the queued sheet
    await page.evaluate(() => document.getElementById('rbResume').click());
    await page.waitForFunction(keys => keys.every(k => { const p = allSheets().find(x => x.metal + ':' + x.page === k); return p && p.status !== 'queued'; }), queuedKeys, { timeout: 60000, polling: 50 });
    await settle();
    sh = await sheets();
    log('1 · after Resume:', (await run()).status, sh.map(s => `${s.key} ${s.status} ${s.placed.length}/${s.charms.length}`).join(' · '));
    for (const s of sh) { assert(['complete', 'partial'].includes(s.status), `${s.key} ${s.status}`); assert.equal(s.placed.length, s.charms.length, `${s.key}: every charm placed after Resume`); assert(!s.runHold, `${s.key} held: ${s.runHold}`); }

    // ── 2 · Stop while new orders are going on ──
    const before = await sheets();
    const beforeToasts = (await toasts()).length;
    receipts.push(...arrivals);
    // the station sweeps Etsy again only ten minutes after its last sweep
    await H.frame().evaluate(() => { window.__realNow = window.__realNow || Date.now.bind(Date); window.__off = (window.__off || 0) + 11 * 60000; Date.now = () => window.__realNow() + window.__off; });
    await page.evaluate(async () => { CN.S.settings.pollOrders = 'on'; await Arrivals.check(); });
    // pressed while a new charm is being turned and tried in its spots (while the rotations are still being prepared, the
    // search answers once they are ready, about half a second later, as it does to the card's own Stop)
    await page.waitForFunction(() => { const r = B.run; return r && r.arrivalBusy && allSheets().some(p => p.runId === r.runId && p.status === 'nesting' && /^(Turning|Trying|Nudging|Placed) /.test(p.stage || '')); }, null, { timeout: 180000, polling: 5 });
    const s2 = await page.evaluate(() => new Promise(resolve => {
      const r = B.run, mine = () => allSheets().filter(p => p.runId === r.runId);
      const text = document.getElementById('runBanner').textContent.replace(/\s+/g, ' '), btn = document.getElementById('rbStop');
      const was = mine().map(p => ({ key: p.metal + ':' + p.page, status: p.status, stage: p.stage })), status = r.status;
      if (!btn) return resolve({ text, was, status, noButton: true });
      const title = btn.title, t0 = performance.now(); btn.click();
      const poll = () => {
        if (!mine().some(p => p.status === 'nesting')) return resolve({ text, title, was, status, ms: Math.round(performance.now() - t0) });
        if (performance.now() - t0 > 20000) return resolve({ text, title, was, status, timeout: true });
        setTimeout(poll, 2);
      };
      poll();
    }));
    log('2 · Stop pressed during', s2.status, JSON.stringify(s2.was), '| banner:', s2.text.slice(0, 100), '| searches ended after', s2.ms, 'ms');
    assert(!s2.noButton, 'Stop is offered while new orders are going on: ' + s2.text);
    if (s2.status === 'processed') assert(/Adding new orders/.test(s2.text), s2.text);
    assert(/charms already placed stay/.test(s2.title), s2.title);
    assert(!s2.timeout, 'the charm searches end when Stop is pressed');
    assert(s2.ms < 1000, `the charm searches end at once (${s2.ms} ms)`);
    await page.waitForFunction(() => { const r = B.run; return r && !r.arrivalBusy && !allSheets().some(p => p.runId === r.runId && (p.status === 'finishing' || p.persisted && !p.persistedDone)); }, null, { timeout: 120000, polling: 50 });
    let r = await run(); sh = await sheets();
    const fresh = await page.evaluate(ids => Orders.rows().filter(x => ids.includes(String(x.order.receiptId))).map(x => ({ rid: String(x.order.receiptId), state: x.state, material: x.spec && x.spec.material, poolIds: x.poolIds || [] })), arrivals.map(x => String(x.receipt_id)));
    log('2 · after Stop:', r.status, r.stoppedBy, '|', sh.map(s => `${s.key} ${s.status} "${s.stage}" ${s.placed.length}/${s.charms.length}${s.endedBy ? ' ' + s.endedBy : ''}`).join(' · '));
    assert.equal(r.status, 'stopped'); assert.equal(r.stoppedBy, 'stopped by the operator', 'the run keeps its own plain reason');
    assert(!r.errors.some(w => /could not be added/.test(w)), 'no second stop: ' + r.errors.join(' | '));
    const after = (await toasts()).slice(beforeToasts);
    assert(!after.some(t => /^bad:|could not be added|did not fit|still waiting for sheet/.test(t)), 'no alarm after the Stop a person pressed: ' + after.join(' | '));
    unmoved(before, sh, 'after Stop');
    assert.equal(fresh.length, arrivals.length, 'every new order came in');
    const newIds = fresh.flatMap(x => x.poolIds); assert.equal(newIds.length, arrivals.length, 'every new order is on a sheet: ' + JSON.stringify(fresh));
    for (const s of sh) assert.equal(s.pages, before.find(x => x.metal === s.metal).pages, `${s.metal}: no sheet was opened for the orders that wait`);
    for (const id of newIds) assert(sh.some(s => s.charms.includes(id)), `${id} waits on a sheet of the run`);
    for (const s of sh) {
      const was = before.find(x => x.key === s.key);
      assert.equal(s.topup, was ? was.topup : null, `${s.key}: the orders that wait are not counted among its gap-fill tries`);
      assert(!s.runHold, `${s.key} was not put on hold: ${s.runHold}`);
      if (s.placed.length < s.charms.length) { assert.equal(s.status, 'queued', `${s.key} waits`); assert.equal(s.stage, RESUME, `${s.key}: ${s.stage}`); if (s.card) assert(s.card.text.includes(RESUME) && !s.card.spinner, `${s.key} card: ${JSON.stringify(s.card)}`); }
    }
    for (const x of fresh) if (!sh.some(s => x.poolIds.every(id => s.placed.some(p => p.id === id)))) assert.equal(x.state, 'pooled', `${x.rid} waits as pooled, not ${x.state}`);
    assert(!Object.values(r.holds).some(w => /stopped/.test(w)), 'no "This sheet was stopped" hold: ' + JSON.stringify(r.holds));
    await page.waitForTimeout(4000);
    const later = await sheets(); b = await banner();
    assert(!later.some(s => ['nesting', 'finishing'].includes(s.status)), 'nothing starts while the run is stopped: ' + later.map(s => s.key + ' ' + s.status).join(', '));
    assert.equal((await run()).status, 'stopped');
    assert(/Stopped:\s*stopped by the operator/.test(b.text) && !/still finishing/.test(b.text) && b.resume, b.text);
    assert.equal(await streamHeld(), 'the run is stopped');
    // Resume places the orders that waited, around the charms already there
    await page.evaluate(() => document.getElementById('rbResume').click());
    await page.waitForFunction(() => B.run.status !== 'stopped', null, { timeout: 30000 });
    await settle();
    r = await run(); sh = await sheets();
    log('2 · after Resume:', r.status, r.step, '|', sh.map(s => `${s.key} ${s.status} ${s.placed.length}/${s.charms.length}`).join(' · '));
    unmoved(before, sh, 'after Resume');
    for (const id of newIds) assert(sh.some(s => s.placed.some(p => p.id === id)), `${id} placed after Resume`);
    for (const s of sh) assert(!s.runHold, `${s.key} held: ${s.runHold}`);
    assert(!r.errors.some(w => /could not be added/.test(w)), r.errors.join(' | '));

    // ── 2b · a double click on Stop while new orders go on: the second click lands on Resume, which takes Stop's place.
    //         The run carries on only once the orders' step has wound down, never beside it ──
    const before2 = await sheets();
    receipts.push(...arrivals2);
    await H.frame().evaluate(() => { window.__off = (window.__off || 0) + 11 * 60000; });
    await page.evaluate(async () => { await Arrivals.check(); });
    await page.waitForFunction(() => { const r = B.run; return r && r.arrivalBusy && allSheets().some(p => p.runId === r.runId && p.status === 'nesting' && /^(Turning|Trying|Nudging|Placed) /.test(p.stage || '')); }, null, { timeout: 180000, polling: 5 });
    const s3 = await page.evaluate(() => new Promise(resolve => {
      const r = B.run; document.getElementById('rbStop').click();
      const button = document.getElementById('rbResume'); if (!button) return resolve({ noResume: true });
      button.click();
      const now = document.getElementById('rbResume'), shown = now ? { text: now.textContent, disabled: now.disabled } : null, t0 = performance.now();
      const poll = () => {
        if (r.status === 'running') return resolve({ shown, busyWhenRunning: !!r.arrivalBusy, ms: Math.round(performance.now() - t0) });
        if (performance.now() - t0 > 60000) return resolve({ shown, timeout: true, status: r.status });
        setTimeout(poll, 1);
      };
      poll();
    }));
    log('2b · double click:', JSON.stringify(s3));
    assert(!s3.noResume && !s3.timeout, JSON.stringify(s3));
    assert(s3.shown && s3.shown.disabled && /Resuming/.test(s3.shown.text), 'the Resume press shows it was taken: ' + JSON.stringify(s3.shown));
    assert.equal(s3.busyWhenRunning, false, 'the run carries on only after the new orders\' step wound down');
    await settle();
    r = await run(); sh = await sheets();
    const fresh2 = await page.evaluate(ids => Orders.rows().filter(x => ids.includes(String(x.order.receiptId))).flatMap(x => x.poolIds || []), arrivals2.map(x => String(x.receipt_id)));
    log('2b · after:', r.status, r.step, '|', sh.map(s => `${s.key} ${s.status} ${s.placed.length}/${s.charms.length}`).join(' · '));
    assert.equal(fresh2.length, arrivals2.length);
    unmoved(before2, sh, 'after the double click');
    for (const id of fresh2) assert(sh.some(s => s.placed.some(p => p.id === id)), `${id} placed after the double click`);
    assert(!r.errors.some(w => /could not be added/.test(w)), r.errors.join(' | '));
    for (const s of sh) assert(!s.runHold, `${s.key} held: ${s.runHold}`);

    // ── 3 · a reload while an approved engraving's back file is being written ──
    await page.waitForFunction(() => [...Engrave.items().values()].some(j => String(j.row.order.receiptId) === '3700000001' && j.state === 'review'), null, { timeout: 180000, polling: 250 });
    let hold = true, held = null, sawPut;
    const putSeen = new Promise(res => { sawPut = res; });
    await ctx.route(/\/__put\//, async route => {
      if (hold && /\/back\//.test(decodeURIComponent(route.request().url()))) { hold = false; held = route; sawPut(); return; }   // left unanswered: the reload cuts it short
      return route.continue().catch(() => {});
    });
    const approvedAt = await page.evaluate(() => { const j = [...Engrave.items().values()].find(j => String(j.row.order.receiptId) === '3700000001'); Engrave.approve(j, 'Tester'); return new Promise(res => setTimeout(() => res(j.approvedAt), 50)); });
    await Promise.race([putSeen, new Promise((_, rej) => setTimeout(() => rej(new Error('the back file upload never started')), 60000))]);
    const mid = await page.evaluate(() => { const j = [...Engrave.items().values()].find(j => String(j.row.order.receiptId) === '3700000001'); return { state: j.state, backs: (j.backs || []).length, copies: j.copies.length }; });
    log('3 · reloading while the back file is being written:', JSON.stringify(mid));
    assert.equal(mid.state, 'approved'); assert.equal(mid.backs, 0);
    await page.reload();
    await held.abort().catch(() => {});
    await page.waitForFunction(() => window.Engrave && window.B && B.run && Engrave.items().size > 0, null, { timeout: 120000 });
    await recordToasts();
    await page.waitForFunction(at => { const j = [...Engrave.items().values()].find(j => String(j.row.order.receiptId) === '3700000001'); return j && j.state === 'written' && j.approvedAt === at && j.copies.every(id => (j.backs || []).some(b => b.poolId === id && b.approvedAt === at)); }, approvedAt, { timeout: 120000, polling: 250 });
    const job = await page.evaluate(() => { const j = [...Engrave.items().values()].find(j => String(j.row.order.receiptId) === '3700000001'); return { state: j.state, approvedAt: j.approvedAt, copies: j.copies.slice(), review: Review.items().filter(i => /back file failed/.test(i.why || '')).length }; });
    const files = [...st.blobs.keys()].filter(k => /\/back\//.test(k) && k.includes(String(approvedAt)));
    log('3 · after the reload:', JSON.stringify(job), files.map(f => f.split('/').pop()).join(', '));
    for (const id of job.copies) assert(files.some(f => f.endsWith(`_back_${id}_${approvedAt}.ai`)), `the back file of ${id} is in storage`);
    assert.equal(job.review, 0, 'no "back file failed" in Review');

    const pageErrors = H.errors.filter(e => /pageerror/.test(e));
    assert.deepEqual(pageErrors, [], 'no page errors');
    console.log(`Stop and Resume OK: a queued sheet waits for Resume and starts on it; Stop during "Adding new orders" ends the searches in ${s2.ms} ms, keeps every placed charm where it was, leaves the new orders waiting on their sheets and places them on Resume (after a double click as well); an approval's back file cut short by a reload is written after the restore`);
  } catch (e) {
    console.error(e);
    try {
      console.error('run:', JSON.stringify(await run()));
      console.error('sheets:', JSON.stringify((await sheets()).map(s => ({ ...s, placed: s.placed.length, charms: s.charms.length }))));
      console.error('agent:', (await page.evaluate(() => CN.AG.events.slice(-25).map(e => e.kind + ' | ' + String(e.text).slice(0, 200)))).join('\n  '));
      console.error('toasts:', (await toasts()).join(' | '));
    } catch (_) {}
    console.error('errors:', H.errors.slice(0, 20).join('\n  '));
    process.exitCode = 1;
  } finally { await H.close(); }
})();
