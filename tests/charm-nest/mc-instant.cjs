// MCINSTANT (Paul, 28 Oct 2026, "14K Draft 2", 17 charms, Manual Control: "every interaction with a given charm on this sheet manual mode is very slow. Everything is delayed ... pressing
// a key several times ... in quick succession results in multi-wait times ... It takes a long time to load a sheet when moving between sheets ... a delay when dragging a charm across the
// sheet with the mouse ... I need this editor to be as responsive as Photoshop ... Every drag every key stroke there could be no delay. Every sheet that opens must be instantaneous ...
// I absolutely wanna keep the API calls to the bare minimum.")
// In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary handlers on an in-memory store; nothing live), a 14K run of two sheets made by the page and its real nest
// (17 charms of real outlines with holes on A, 7 on B), the cloud answering every function call 400 ms late. Budgets with margin for a quiet machine (the measured values are printed):
//   1  switching the window to the other sheet and back: the plate shows its charms within 150 ms (it no longer waits for the cloud's read of the record)
//   2  Manual Control on: no main-thread block over 150 ms (it was one of 1.1 s) (its colours were worked out while the window sat idle)
//   3  a drag of 60 pointer moves: each move's work (its handlers and the frame that draws it) p95 under 16 ms; no long task over 50 ms
//   4  30 arrow presses: under 50 ms of key handling in all, the charm 30 nudges further; the check after the run of keys under 50 ms
//   5  the round handle turned and a corner dragged (30 moves each): p95 under 16 ms a move
//   6  no write to the cloud from the first press to the last key (no putSheet, no file, no claim beyond the heartbeat); the moves kept on this computer (localStorage)
//   7  Manual Control off: ONE save (one record write with the new layout, one set of files), the moves where they were put, the local copy gone
//   8  a reload never loses the moves: what this tab kept for the sheet is back when Manual Control is turned on again ("· 1 unsaved" on the switch), and saved when it is turned off
//   node tests/charm-nest/mc-instant.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>; MI_LAX=1: print the numbers, do not fail on them)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules', '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const exe = process.env.CHROMIUM || ['/opt/pw-browsers/chromium-1194/chrome-linux/chrome', '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome'].find(f => fs.existsSync(f));
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const RUN = 'run-mi-1', NA = 17, NB = 7, N = NA + NB, LAX = !!process.env.MI_LAX, LAT = 400;
  const shapes = ['house', 'notch', 'blob', 'bar'];
  const pdf = await scene({ page: [640, 160], bodies: Array.from({ length: N }, (_, i) => ({ shape: shapes[i % 4], x: 20 + (i % 12) * 50, y: 20 + Math.floor(i / 12) * 60, w: 26, h: 30 })) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: 'reduce' });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => {
    try { localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {}
    window.confirm = () => true; window.alert = () => {}; window.prompt = () => 'Tester';
    // the probe: every pointer / key listener and every frame callback timed, by the phase the test names; long tasks
    const P = window.__perf = { phase: '', ev: [], raf: [], lt: [] }, now = () => performance.now();
    const TYPES = new Set(['pointermove', 'pointerdown', 'pointerup', 'keydown']), wrapped = new WeakMap();
    const add = EventTarget.prototype.addEventListener, rem = EventTarget.prototype.removeEventListener;
    EventTarget.prototype.addEventListener = function (type, fn, opt) {
      if (TYPES.has(type) && typeof fn === 'function') {
        let m = wrapped.get(fn); if (!m) wrapped.set(fn, m = {});
        if (!m[type]) m[type] = function () { const t0 = now(); try { return fn.apply(this, arguments); } finally { if (P.phase) P.ev.push([P.phase, type, t0, now() - t0]); } };
        return add.call(this, type, m[type], opt);
      }
      return add.call(this, type, fn, opt);
    };
    EventTarget.prototype.removeEventListener = function (type, fn, opt) { const m = fn && wrapped.get(fn); return rem.call(this, type, (m && m[type]) || fn, opt); };
    const raf = window.requestAnimationFrame.bind(window);
    window.requestAnimationFrame = fn => raf(ts => { const t0 = now(); try { fn(ts); } finally { if (P.phase) P.raf.push([P.phase, t0, now() - t0]); } });
    try { new PerformanceObserver(l => { for (const e of l.getEntries()) P.lt.push([P.phase, e.startTime, e.duration]); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  });
  const page = await ctx.newPage(), errors = [];
  let lat = 0, phase = '';
  await page.route(/\/\.netlify\/functions\//, async r => { if (lat) await new Promise(z => setTimeout(z, lat)); return r.continue(); });
  const net = [];
  page.on('request', r => { const u = r.url(); if (!/\/\.netlify\/functions\//.test(u)) return; let b = {}; try { b = JSON.parse(r.postData() || '{}'); } catch (_) {} net.push({ phase, fn: u.split('/.netlify/functions/')[1].split('?')[0], op: b.op || '', id: (b.sheet && b.sheet.id) || b.id || '', placements: !!(b.sheet && b.sheet.placements), mark: !!(b.sheet && b.sheet.manual && !b.sheet.placements), bytes: (r.postData() || '').length }); });
  page.on('pageerror', e => errors.push('page: ' + String(e.message).slice(0, 200)));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  page.setDefaultTimeout(60000);
  const setPhase = async p => { phase = p; await page.evaluate(p => { window.__perf.phase = p; }, p); };
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 60)); } };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const pct = (a, p) => { if (!a.length) return 0; const s = a.slice().sort((x, y) => x - y); return +s[Math.min(s.length - 1, Math.floor(p * s.length))].toFixed(2); };
  const take = ph => page.evaluate(ph => { const P = window.__perf, f = a => a.filter(x => x[0] === ph); return { ev: f(P.ev), raf: f(P.raf), lt: f(P.lt) }; }, ph);
  // the work of each frame that had pointer moves: every handler run since the frame before, and the frame's own callbacks (the drawing)
  const perMove = d => {
    const fr = []; for (const r of d.raf.slice().sort((a, b) => a[1] - b[1])) { const f = fr[fr.length - 1]; if (f && r[1] - f.end < 4) { f.ms += r[2]; f.end = r[1] + r[2]; } else fr.push({ t: r[1], end: r[1] + r[2], ms: r[2] }); }
    const out = []; let prev = -Infinity;
    for (const f of fr) { const ev = d.ev.filter(x => x[2] >= prev && x[2] < f.t), mv = ev.some(x => x[1] === 'pointermove'); if (mv) out.push(ev.reduce((s, x) => s + x[3], 0) + f.ms); prev = f.end; }
    return out;
  };
  const results = [], failures = [], load = () => require('os').loadavg()[0].toFixed(2);
  const check = (ok, msg, num) => { if (ok) { results.push(msg); console.log('  ok   ' + msg); } else if (LAX && num) console.log('  SLOW ' + msg); else { failures.push(msg); console.log('  FAIL ' + msg); } };
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && SheetWin.hopKit && window.CNManual && window.Pool && window.RoseStock, null, { timeout: 60000 });
  try {
    /* ── the world: 24 charms on 14K, nested and saved by the page; 17 on A, 7 on B, one run, each laid out by the nest ── */
    await page.evaluate(async b64 => { CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold14k = [10, 2.5]; await CN.intake([new File([Uint8Array.from(atob(b64), c => c.charCodeAt(0))], 'shapes.pdf', { type: 'application/pdf' })], 'gold14k'); }, pdf.toString('base64'));
    await until(() => page.evaluate(N => { const sh = CN.S.sheets.gold14k.pages[0]; if (sh.charms.length === N && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === N && sh.status === 'complete' && sh.persistedDone && sh.sheetId; }, N), 240000, 'the 24 charms nested and saved');
    const RID = n => '43100000' + String(n).padStart(2, '0'), TX = n => String(7100000000 + n);
    await page.evaluate(({ RUN, rids, txs, NA }) => {
      CN.S.settings.stock.gold14k = [100 / 25.4, 50 / 25.4];
      const pr = CN.S.sheets.gold14k, A = pr.pages[0]; addPage('gold14k'); const B = pr.pages[pr.pages.length - 1], all = A.charms.slice();
      all.forEach((c, i) => { c.poolId = `${rids[i]}_${txs[i]}_1`; c.order = rids[i]; c.lineKey = `${rids[i]}_${txs[i]}`; c.orderInfo = { receiptId: rids[i], transactionId: txs[i], copy: 1, quantity: 1 }; });
      const cell = i => [22 + (i % 7) * 39, 24 + Math.floor(i / 7) * 46];   // (a 7 x 3 grid: about 56 % full, as Paul's sheet)
      A.charms = all.slice(0, NA); B.charms = all.slice(NA);
      for (const sh of [A, B]) { sh.charms.forEach((c, i) => { const [x, y] = cell(i); c.pinned = { cxPt: x, cyPt: y, angle: 0 }; c.arrivalPin = true; }); sh.placements = []; sh.dirty = true; sh.status = 'ready'; sh.runId = RUN; sh.draft = true; }
      B.page = 2;
      window.B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
      CN.startNest(A);
    }, { RUN, rids: Array.from({ length: N }, (_, i) => RID(i + 1)), txs: Array.from({ length: N }, (_, i) => TX(i + 1)), NA });
    await until(() => page.evaluate(() => { const ps = CN.S.sheets.gold14k.pages.filter(p => p.charms.length); for (const sh of ps) if (sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return ps.length === 2 && ps.every(sh => sh.status === 'complete' && sh.persistedDone && !sh.dirty && sh.sheetId); }), 240000, 'both sheets saved');
    const [SA, SB] = await page.evaluate(({ RUN }) => {
      const ps = CN.S.sheets.gold14k.pages.filter(p => p.charms.length);
      for (const sh of ps) for (const c of sh.charms) window.B.pool.rows.set(c.poolId, { poolId: c.poolId, orderId: c.order, transactionId: c.orderInfo.transactionId, lineKey: c.lineKey, sheetId: sh.sheetId, state: 'placed', material: 'gold14k', runId: RUN });
      return ps.map(sh => ({ id: sh.sheetId, n: sh.placements.length, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, rid: c.order })) }));
    }, { RUN });
    for (const sh of [SA, SB]) for (const c of sh.charms) st.put('Charm_Pool', c.poolId, { poolId: c.poolId, orderId: c.rid, transactionId: TX(+c.rid.slice(-2)), lineKey: c.poolId.replace(/_1$/, ''), sheetId: sh.id, state: 'placed', material: 'gold14k', runId: RUN });
    console.log(`  ..   world: 14K ${SA.id} (${SA.n} charms) and ${SB.id} (${SB.n} charms), one run, nested and saved by the real nest; every cloud call ${LAT} ms late from here; load ${load()}`);
    lat = LAT;

    /* ── 1 · the window, and the other sheet ── */
    const drawn = id => page.evaluate(id => { const W = SheetWin.hopKit.W; return W.id === id && !!W.rec && W.geom && W.pieces.length > 0 && W.pieces.every(x => x.c); }, id);
    await page.evaluate(id => SheetWin.open(id), SA.id);
    await until(() => drawn(SA.id), 30000, 'A drawn'); await sleep(1500);
    const switchTo = id => page.evaluate(id => new Promise(res => {
      const t0 = performance.now(), W = SheetWin.hopKit.W; SheetWin.hopKit.switchSheet(id);
      const tick = () => { if (W.id === id && W.geom && W.pieces.length && W.pieces.every(x => x.c)) return res(+(performance.now() - t0).toFixed(1)); if (performance.now() - t0 > 20000) return res(-1); requestAnimationFrame(tick); };
      requestAnimationFrame(tick);
    }), id);
    const tB = await switchTo(SB.id); await sleep(LAT + 600);
    const tA = await switchTo(SA.id); await sleep(LAT + 900);
    check(tB > 0 && tB < 150 && tA > 0 && tA < 150, `1 · another sheet shows its charms ${tB} ms after the switch, and back ${tA} ms (< 150 ms; the cloud answers in ${LAT} ms) · load ${load()}`, true);
    await until(() => drawn(SA.id), 20000, 'A drawn again');

    /* ── 2 · Manual Control on ── */
    await until(() => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b && !b.hidden && b.getAttribute('aria-disabled') === 'false'; }), 20000, 'Manual Control can start');
    await sleep(800);   // (the window has sat idle: its checks are made)
    await setPhase('on');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 20000, 'Manual Control on'); await sleep(300);
    { const d = await take('on'), worst = Math.max(0, ...d.lt.map(x => x[2])); check(worst < 150, `2 · Manual Control on: longest main-thread block ${worst.toFixed(0)} ms (< 150 ms) · load ${load()}`, true); }

    /* ── 3 · a drag ── */
    const onPlate = poolId => page.evaluate(id => { const W = SheetWin.hopKit.W, x = W.byPool.get(id); if (!x) return null; const r = W.el.fx.getBoundingClientRect(), kc = W.k / W.dpr, R0 = W.R / W.dpr; return { x: r.left + R0 + x.p.cxPt * kc, y: r.top + R0 + x.p.cyPt * kc }; }, poolId);
    const cid = SA.charms[8].id, a = await onPlate(SA.charms[8].poolId);
    await setPhase('press'); await page.mouse.move(a.x, a.y); await page.mouse.down(); await sleep(400);
    await setPhase('drag');
    for (let i = 1; i <= 60; i++) await page.mouse.move(a.x + i * 1.2, a.y + i * 0.4);
    await sleep(200);
    { const d = await take('drag'), pm = perMove(d), worst = Math.max(0, ...d.lt.map(x => x[2]));
      check(pm.length >= 30 && pct(pm, .95) < 16 && worst < 50, `3 · drag: ${pm.length} moves, work a move p50 ${pct(pm, .5)} ms, p95 ${pct(pm, .95)} ms (< 16 ms), max ${pct(pm, 1)} ms; longest block ${worst.toFixed(0)} ms (< 50 ms) · load ${load()}`, true); }
    await setPhase('drop'); await page.mouse.up(); await sleep(400);

    /* ── 4 · 30 arrows ── */
    const x0 = await page.evaluate(id => CNManual._MC.edits.get(id).cxPt, cid);
    await setPhase('keys');
    for (let i = 0; i < 30; i++) await page.keyboard.press('ArrowRight');
    await sleep(600);
    { const d = await take('keys'), k = d.ev.filter(x => x[1] === 'keydown').reduce((s, x) => s + x[3], 0), worst = Math.max(0, ...d.lt.map(x => x[2]));
      const x1 = await page.evaluate(id => CNManual._MC.edits.get(id).cxPt, cid), want = 30 * 0.05 * 72 / 25.4;
      check(Math.abs(x1 - x0 - want) < 0.01, `4 · 30 arrows moved the charm ${(x1 - x0).toFixed(3)} pt (30 x 0.05 mm = ${want.toFixed(3)} pt)`);
      check(k < 50 && worst < 50, `4 · 30 arrows: ${k.toFixed(1)} ms of key handling in all (< 50 ms); longest block, the check after the run included, ${worst.toFixed(0)} ms (< 50 ms) · load ${load()}`, true); }

    /* ── 5 · the round handle, a corner ── */
    for (const [ph, pick, dx, dy] of [['turn', t => t.knob, 2, 1.5], ['size', t => t.corners[2], 0.6, 0.6]]) {
      const t = await page.evaluate(() => CNManual._tool()); const p0 = pick(t);
      await setPhase(ph); await page.mouse.move(p0[0], p0[1]); await page.mouse.down();
      for (let i = 1; i <= 30; i++) await page.mouse.move(p0[0] + i * dx, p0[1] + i * dy);
      await page.mouse.up(); await sleep(400);
      const d = await take(ph), pm = perMove(d), worst = Math.max(0, ...d.lt.map(x => x[2]));
      if (process.env.MI_DEBUG) console.log(ph, JSON.stringify(d.lt.map(x => [Math.round(x[1]), Math.round(x[2])])), JSON.stringify(d.ev.filter(x => x[3] > 5).map(x => [x[1], Math.round(x[2]), Math.round(x[3])])), JSON.stringify(d.raf.filter(x => x[2] > 5).map(x => [Math.round(x[1]), Math.round(x[2])])));
      check(pm.length >= 15 && pct(pm, .95) < 16 && worst < 50, `5 · ${ph === 'turn' ? 'the round handle' : 'a corner'}: ${pm.length} moves, p95 ${pct(pm, .95)} ms (< 16 ms), max ${pct(pm, 1)} ms; longest block ${worst.toFixed(0)} ms · load ${load()}`, true);
    }

    /* ── 6 · nothing written while working; the moves kept on this computer ── */
    const during = net.filter(n => ['press', 'drag', 'drop', 'keys', 'turn', 'size'].includes(n.phase));
    const writes = during.filter(n => n.fn === 'charmNestOutput' || ['putSheet', 'runPut', 'getSheet', 'poolUpdate', 'timelineAdd'].includes(n.op));
    check(!writes.length, `6 · no cloud write or read from the first press to the last key (${during.length} calls in that time, all the page's own polls: ${[...new Set(during.map(n => n.op || n.fn))].join(', ') || 'none'})`);
    const draft = await page.evaluate(id => { const k = Object.keys(localStorage).find(k => k.startsWith('cn.mc.draft.' + id + '.')); return k ? JSON.parse(localStorage.getItem(k)) : null; }, SA.id);
    check(!!draft && draft.edits.some(([id]) => id === cid), `6 · the moves kept on this computer (localStorage, ${draft ? draft.edits.length : 0} charm(s))`);

    /* ── 7 · off: one save ── */
    const want = await page.evaluate(id => CNManual._MC.edits.get(id), cid);
    await setPhase('save');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(id => { const sh = CN.allSheets().find(p => p.sheetId === id); return !CNManual._MC.on && !SheetWin.hopKit.W.flow && sh && !sh._manualHold && sh.persistedDone && !sh.dirty; }, SA.id), 120000, 'saved');
    await sleep(LAT * 3 + 1500);
    const saved = net.filter(n => n.phase === 'save'), layout = saved.filter(n => n.op === 'putSheet' && n.id === SA.id && n.placements), files = saved.filter(n => n.fn === 'charmNestOutput' && n.op === 'finalize'), marks = saved.filter(n => n.op === 'putSheet' && n.id === SA.id && n.mark);
    const rec = st.doc('Charm_Nest_Sheets', SA.id) || {}, p = (rec.placements || []).find(z => z.id === cid);
    const left = await page.evaluate(id => Object.keys(localStorage).filter(k => k.startsWith('cn.mc.draft.' + id + '.')).length, SA.id);
    // (one save: Manual Control's mark written once; the sheet's record written by that one write of the sheet (the layout, then the links to its files), its files made once)
    check(marks.length === 1 && layout.length >= 1 && layout.length <= 2 && files.length >= 1 && files.length <= 4, `7 · off: ONE save: ${marks.length} save of the moves, ${layout.length} record writes of the layout (the layout, then its files' links), ${files.length} files made once; ${saved.length} calls in all (${[...new Set(saved.map(n => n.op || n.fn))].join(', ')})`);
    check(!!p && Math.abs(p.cxPt - want.cxPt) < 0.01 && Math.abs(p.cyPt - want.cyPt) < 0.01, `7 · the record holds the charm where it was put (${p && p.cxPt}, ${p && p.cyPt})`);
    check(left === 0, `7 · the local copy is gone once saved`);

    /* ── 8 · a reload never loses the moves: what this tab kept for the sheet comes back when Manual Control is turned on again ── */
    const c8 = SA.charms[3].id, q8 = await page.evaluate(([id, cid]) => { const sh = CN.allSheets().find(p => p.sheetId === id), p = sh.placements.find(z => z.id === cid), tab = sessionStorage.getItem('cn.sheetwin.tab'), q = { cxPt: +p.cxPt + 1.5, cyPt: +p.cyPt, angle: +p.angle || 0, size: 1 };
      localStorage.setItem(`cn.mc.draft.${id}.${tab}`, JSON.stringify({ at: Date.now(), by: 'Tester', edits: [[cid, q]] })); return q; }, [SA.id, c8]);
    await until(() => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b && !b.hidden && b.getAttribute('aria-disabled') === 'false'; }), 30000, 'Manual Control can start again');
    await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => CNManual._MC.on), 20000, 'Manual Control on again');
    const back = await page.evaluate(id => { const q = CNManual._MC.edits.get(id), b = document.querySelector('.sheetWin [data-r=manual]'); return { q, unsaved: b.getAttribute('data-unsaved') || '' }; }, c8);
    check(!!back.q && Math.abs(back.q.cxPt - q8.cxPt) < 1e-3 && Math.abs(back.q.cyPt - q8.cyPt) < 1e-3, `8 · the move kept on this computer is back in Manual Control (${back.q && back.q.cxPt}, ${back.q && back.q.cyPt})`);
    await until(() => page.evaluate(() => /unsaved/.test(document.querySelector('.sheetWin [data-r=manual]').getAttribute('data-unsaved') || '')), 3000, 'the unsaved word').catch(() => {});
    const word = await page.evaluate(() => document.querySelector('.sheetWin [data-r=manual]').getAttribute('data-unsaved') || '');
    check(/1 unsaved/.test(word), `8 · the switch says so quietly ("${word}")`);
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(id => { const sh = CN.allSheets().find(p => p.sheetId === id); return !CNManual._MC.on && !SheetWin.hopKit.W.flow && sh && !sh._manualHold && sh.persistedDone && !sh.dirty; }, SA.id), 120000, 'saved again');
    await sleep(LAT * 2 + 800);
    const p8 = ((st.doc('Charm_Nest_Sheets', SA.id) || {}).placements || []).find(z => z.id === c8);
    check(!!p8 && Math.abs(p8.cxPt - q8.cxPt) < 0.01, `8 · saved where it was put (${p8 && p8.cxPt})`);
    check(!errors.length, `no page errors${errors.length ? ': ' + errors.slice(0, 3).join(' | ') : ''}`);
  } catch (e) { failures.push(String(e.stack || e)); console.log('  FAIL ' + (e.stack || e)); }
  await browser.close(); srv.close && srv.close();
  console.log(failures.length ? `\n${failures.length} failed, ${results.length} passed` : `\nall ${results.length} passed`);
  process.exit(failures.length ? 1 : 0);
})();
