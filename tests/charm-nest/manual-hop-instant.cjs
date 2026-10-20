// MANUALHOP (Paul, 19 Oct 2026: "When I moved the charm from one sheet to another [in Manual mode], it took a very long time, and it froze ... There was
// no feedback to the user ... and then it took me out of the manual mode ... This needs to be a near instantaneous process and cannot wait for 10 to 20
// seconds for it to occur. It must be seamless and part of the whole manual operations process flow.")
//
// Everything is REAL here, the nest included (no stub): the sorter page over the fake site (bridge-server.cjs: the real charmNestLibrary handler on an in-memory
// store), two 10K sheets made by the page itself (a PDF of squares dropped on 10K Gold, nested, written, verified and saved by the real nest, then split onto two
// sheets of one run), the sheet window's real Manual Control and the real sheet hop (charm-nest-sheet-hop.js). A charm is dragged with the mouse from sheet A onto
// the dock of sheet B, as Paul does it, and timed:
//   1  from letting go to the charm being on B (and off A) and the hand free again (no hop busy, no copy in flight): under 300 ms
//   2  the longest main-thread block from letting go until the background save has finished (PerformanceObserver "longtask"): under 100 ms
//   3  Manual Control is still on, on sheet A, right after the drop and after the save, and the window still shows A
//   4  a small status near the plate says it is saving, then saved (never a blocking panel); no automatic re-nest of either sheet was started by the move
//   5  the save: A's record without the piece, B's record with it (pinned where it landed), the piece record on B; after a reload, the piece is on B only,
//      nothing duplicated or lost
//   6  a second charm moved right after the first (before the first save ends): both land, both saved, Manual Control still on
//   7  MOVEINSTANT (Paul 20 Oct 2026: "this particular process still took about 10 seconds and the charm was not visible on the sheet where I dropped it until it fully
//      completed ... every time I move charms between sheets, I still get removed from the manual control"): every sheet write of the cloud held 10 s (a slow network):
//        - the charm is on B (its dock's drawing included) and off A within 150 ms of letting go; no steps panel at any moment of the save
//        - a second charm moved while the first is still saving lands as fast; the saves queue in order
//        - B opened from its dock while both still save (Paul's way of looking): the window shows both charms on B at once, Manual Control carries on there, no panel
//        - after the saves: both records right, Manual Control still on, on the sheet the window shows
//        - a save the cloud refuses puts the charm back where it was, on the screen and in the records, says so in a toast, and Manual Control stays on
//      and Manual Control turned off: both sheets written as they stand in the background (the chip, never the panel)
//   8  MOVEINSTANT 2 (Paul 20 Oct 2026: he is never dropped out of Manual Control by moving charms or switching sheets; only his own toggle ends it): a charm moved by
//      hand WITHIN B, not saved, then A opened from its dock (sheet writes held 3 s): Manual Control stays on, now on A, no panel; B's move is saved in the background
//      (the chip); B opened again while that save still runs shows the charm where it was put and takes a hand once saved; B's record holds the move, pinned
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome> node tests/charm-nest/manual-hop-instant.cjs        (MH_LAX=1: print the numbers, do not fail on them)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-mh-1', LAX = !!process.env.MH_LAX;
  // eight 20 pt squares, far apart on the page so each is its own charm
  const pdf = await scene({ page: [560, 200], bodies: [], extraOps: [20, 80, 140, 200, 260, 320, 380, 440].map(x => `q 0 0 0 RG 0.25 w ${x} 20 20 20 re S Q`) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 }, reducedMotion: 'no-preference' });
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
    // every nest started, by whom (the move must start none)
    window.__nests = []; const iv = setInterval(() => { if (typeof window.startNest !== 'function' || window.startNest.__spy) return; const real = window.startNest; const spy = function (sh) { window.__nests.push({ sheetId: sh && sh.sheetId, at: performance.now(), held: !!(sh && sh._manualHold) }); return real.apply(this, arguments); }; spy.__spy = true; window.startNest = spy; if (window.CN) CN.startNest = spy; clearInterval(iv); }, 0);
  });
  const page = await ctx.newPage(), errors = [];
  // (MOVEINSTANT: a slow cloud, and a cloud that refuses a write, for section 7: each sheet write held SLOW ms; FAIL answers a sheet write with an error)
  let SLOW = 0, FAIL = false;
  await page.route(/\/\.netlify\/functions\/charmNestLibrary/, async r => {
    let op = ''; try { op = JSON.parse(r.request().postData() || '{}').op || ''; } catch (_) {}
    if (op === 'putSheet' && FAIL) return r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ error: 'the cloud refused the write (test)' }) });
    if (op === 'putSheet' && SLOW) await new Promise(z => setTimeout(z, SLOW));
    return r.continue();
  });
  page.on('pageerror', e => errors.push('page: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  page.setDefaultTimeout(30000);
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { let v; try { v = await fn(); } catch (_) { v = false; } if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 120)); } };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const ready = () => page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && SheetWin.hopKit && window.SheetHop && window.CNManual && window.Pool && window.RoseStock, null, { timeout: 60000 });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`); await ready();
  const results = [], failures = [];
  const check = (ok, msg, num) => { if (ok) { results.push(msg); console.log('  ok   ' + msg); } else if (LAX && num) { console.log('  SLOW ' + msg); } else { failures.push(msg); console.log('  FAIL ' + msg); } };
  try {
    /* ── the world: eight squares on 10K Gold, nested and saved by the page; then five on sheet A and three on sheet B, one run, each laid out by the nest ── */
    await page.evaluate(async b64 => {
      CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold10k = [4, 2.5];
      const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'gold10k');
    }, pdf.toString('base64'));
    const settledAll = n => page.evaluate(n => { const ps = CN.S.sheets.gold10k.pages.filter(p => p.charms.length); for (const sh of ps) if (sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return ps.length === n && ps.every(sh => sh.status === 'complete' && sh.persistedDone && sh.verification && sh.verification.ok && !sh.dirty && sh.sheetId); }, n);
    await until(() => page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages[0]; if (sh.charms.length === 8 && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === 8 && sh.status === 'complete' && sh.persistedDone && sh.sheetId; }), 120000, 'the 8 squares nested and saved');
    const RID = n => '42100000' + String(n).padStart(2, '0'), TX = n => String(7000000000 + n);
    await page.evaluate(({ RUN, rids, txs }) => {
      const pr = CN.S.sheets.gold10k, A = pr.pages[0]; addPage('gold10k'); const B = pr.pages[pr.pages.length - 1];
      const all = A.charms.slice();
      all.forEach((c, i) => { c.poolId = `${rids[i]}_${txs[i]}_1`; c.order = rids[i]; c.lineKey = `${rids[i]}_${txs[i]}`; c.orderInfo = { receiptId: rids[i], transactionId: txs[i], copy: 1, quantity: 1 }; });
      const LA = [[40, 40], [90, 40], [140, 40], [40, 95], [90, 95]], LB = [[40, 40], [90, 40], [40, 95]];
      A.charms = all.slice(0, 5); B.charms = all.slice(5);
      A.charms.forEach((c, i) => { c.pinned = { cxPt: LA[i][0], cyPt: LA[i][1], angle: 0 }; c.arrivalPin = true; });
      B.charms.forEach((c, i) => { c.pinned = { cxPt: LB[i][0], cyPt: LB[i][1], angle: 0 }; c.arrivalPin = true; });
      for (const sh of [A, B]) { sh.placements = []; sh.dirty = true; sh.status = 'ready'; sh.runId = RUN; sh.draft = true; }
      B.page = 2;
      window.B.run = { runId: RUN, day: new Date().toISOString().slice(0, 10), setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
      CN.startNest(A);
    }, { RUN, rids: [1, 2, 3, 4, 5, 6, 7, 8].map(RID), txs: [1, 2, 3, 4, 5, 6, 7, 8].map(TX) }).catch(e => { throw new Error('split: ' + e.message); });
    await until(() => settledAll(2), 180000, 'both sheets nested at their known spots and saved');
    const W0 = await page.evaluate(({ RUN }) => {
      const ps = CN.S.sheets.gold10k.pages.filter(p => p.charms.length);
      for (const sh of ps) for (const c of sh.charms) window.B.pool.rows.set(c.poolId, { poolId: c.poolId, orderId: c.order, transactionId: c.orderInfo.transactionId, lineKey: c.lineKey, sheetId: sh.sheetId, state: 'placed', material: 'gold10k', runId: RUN });
      return ps.map(sh => ({ id: sh.sheetId, page: sh.page, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, rid: c.order })), at: Object.fromEntries(sh.placements.map(p => [p.id, [p.cxPt, p.cyPt]])) }));
    }, { RUN });
    for (const sh of W0) for (const c of sh.charms) st.put(POOL, c.poolId, { poolId: c.poolId, orderId: c.rid, transactionId: TX(+c.rid.slice(-2)), lineKey: c.poolId.replace(/_1$/, ''), sheetId: sh.id, state: 'placed', material: 'gold10k', runId: RUN });
    const [SA, SB] = W0; console.log(`  ..   world: ${SA.id} (page ${SA.page}, ${SA.charms.length} charms) and ${SB.id} (page ${SB.page}, ${SB.charms.length} charms), one run, nested and saved by the real nest`);

    /* ── the window on A, Manual Control on ── */
    await page.evaluate(id => SheetWin.open(id), SA.id);
    await until(() => page.evaluate(id => { const W = SheetWin.hopKit.W; return W.id === id && W.rec && W.geom && !W.flying && W.pieces.length; }, SA.id), 30000, 'the window on A'); await sleep(500);
    await until(() => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b && !b.hidden && b.getAttribute('aria-disabled') === 'false'; }), 15000, 'Manual Control can start');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 10000, 'Manual Control on');
    await until(() => page.evaluate(id => { const s = SheetHop.state(); return s.docks.next && s.docks.next.sheetId === id; }, SB.id), 6000, 'the next dock is B');

    const onPlate = poolId => page.evaluate(id => { const K = SheetWin.hopKit, W = K.W, x = W.byPool.get(id); if (!x) return null; const r = W.el.fx.getBoundingClientRect(), kc = W.k / W.dpr, R0 = W.R / W.dpr; return { x: r.left + R0 + x.p.cxPt * kc, y: r.top + R0 + x.p.cyPt * kc }; }, poolId);
    const tabOf = side => page.evaluate(s => { const t = document.querySelector(`.shpDock[data-side=${s}] .shpTab`); const r = t.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, side);
    const inPeek = (side, pt) => page.evaluate(([s, pt]) => { const dk = SheetHop._H.docks[s], r = dk.cv.getBoundingClientRect(); return { x: r.left + pt.x / dk.st.wPt * r.width, y: r.top + pt.y / dk.st.hPt * r.height, peek: dk.el.classList.contains('peek'), armed: dk.el.dataset.state === 'armed' }; }, [side, pt]);
    /** the watcher: from the moment the mouse lets go, when the charm is on B and off A, when the hand is free, every long task, Manual Control's state */
    const arm = (poolId, srcId, dstId) => page.evaluate(([poolId, srcId, dstId]) => {
      const M = window.__mh = { t0: 0, on: 0, free: 0, lt: [], mcOffAt: 0, winLeft: 0, poolId };
      if (!window.__mhObs) { window.__mhObs = new PerformanceObserver(l => { for (const e of l.getEntries()) (window.__mh ? window.__mh.lt : []).push([Math.round(e.startTime), Math.round(e.duration)]); }); window.__mhObs.observe({ entryTypes: ['longtask'] }); }
      // (t0 is stamped by the test right before the mouse lets go: the hop's own capture listener stops the pointerup for everything after it)
      const sheets = () => CN.allSheets();
      const tick = () => {
        if (window.__mh !== M) return;
        const now = performance.now();
        if (M.t0) {
          const src = sheets().find(p => p.sheetId === srcId), dst = sheets().find(p => p.sheetId === dstId), c = dst && dst.charms.find(z => z.poolId === poolId);
          const onB = !!c && dst.placements.some(p => p.id === c.id) && !(src.charms || []).some(z => z.poolId === poolId) && !SheetWin.hopKit.W.byPool.has(poolId);
          if (onB && !M.on) M.on = now - M.t0;
          const s = SheetHop.state();
          if (onB && !s.busy && !s.dragging && !M.free) M.free = now - M.t0;
          if (!CNManual._MC.on && !M.mcOffAt) M.mcOffAt = now - M.t0;
          if (SheetWin.hopKit.W.id !== srcId && !M.winLeft) M.winLeft = now - M.t0;
        }
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }, [poolId, srcId, dstId]);
    /** pick a charm of A up with the mouse, carry it over B's dock, aim at pt (B's points), let go */
    async function carry(poolId, pt, side = 'next') {
      const a = await until(() => onPlate(poolId), 10000, poolId + ' drawn on its sheet');
      const dx = side === 'next' ? -40 : 40;
      await page.mouse.move(a.x, a.y); await page.mouse.down(); await page.mouse.move(a.x + 12, a.y + 6, { steps: 4 });
      const t = await tabOf(side);
      await page.mouse.move(t.x + dx, t.y, { steps: 12 }); await page.mouse.move(t.x, t.y, { steps: 4 });
      await until(() => page.evaluate(() => !!document.querySelector('.shpLift') && !!document.querySelector('.shpDock.open')), 5000, 'the charm lifts and the docks open');
      const t2 = await tabOf(side); await page.mouse.move(t2.x + dx, t2.y, { steps: 6 });
      await until(async () => (await inPeek(side, pt)).peek, 4000, 'the dock opens into a view of the other sheet'); await sleep(450);
      const b = await inPeek(side, pt); await page.mouse.move(b.x, b.y, { steps: 8 }); await sleep(250);
      await page.evaluate(() => { window.__mh.t0 = performance.now(); }); await page.mouse.up();
    }
    const saveDone = () => page.evaluate(() => { const K = SheetWin.hopKit; return K.hopState ? !K.hopState().pending : !K.W.flow && !SheetHop.state().busy; });
    const recOf = id => st.doc(SHEETS, id) || {};
    const recHas = (id, poolId) => (recOf(id).poolIds || []).includes(poolId);
    const nestsBy = async () => page.evaluate(() => window.__nests.length);

    /* ── 1-4 · one charm from A to B ── */
    const p1 = SA.charms[4].poolId;   // (the square at (90, 95) on A)
    const n0 = await nestsBy();
    await arm(p1, SA.id, SB.id);
    const tStart = Date.now();
    await carry(p1, { x: 190, y: 95 });
    // right after it lands on B: the status chip, Manual Control
    await until(() => page.evaluate(() => window.__mh.on > 0), 20000, 'the charm lands on B').catch(() => {});
    const early = await page.evaluate(() => ({ mc: CNManual._MC.on, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '', work: !!(SheetWin.hopKit.W.work && SheetWin.hopKit.W.work.state === 'working'), panel: !!document.querySelector('.sheetWin .swWork') && !document.querySelector('.sheetWin [data-r=work]')?.hidden }));
    await until(saveDone, 120000, 'the background save ends');
    const tSaved = Date.now() - tStart;
    await sleep(600);
    const M1 = await page.evaluate(() => Object.assign({}, window.__mh, { tEnd: performance.now() - window.__mh.t0, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '', mc: CNManual._MC.on, win: SheetHop.state() }));
    const sinceDrop = M => M.lt.filter(([s, d]) => s + d >= M.t0), worst1 = Math.max(0, ...sinceDrop(M1).map(([, d]) => d));
    console.log(`  ..   move 1: on B at ${Math.round(M1.on)} ms, hand free at ${Math.round(M1.free)} ms, save ended ${tSaved} ms after the drag began; long tasks since the drop: ${sinceDrop(M1).length} (worst ${worst1} ms: ${JSON.stringify(sinceDrop(M1).map(([s, d]) => [Math.round(s - M1.t0), d]).slice(0, 12))}); Manual Control off at ${M1.mcOffAt ? Math.round(M1.mcOffAt) + ' ms' : 'never'}; early: ${JSON.stringify(early)}; chip at the end: "${M1.chip}"`);
    check(M1.on > 0 && M1.on < 300, `1 · the charm is on B and off A ${Math.round(M1.on)} ms after letting go (< 300 ms)`, true);
    check(M1.free > 0 && M1.free < 300, `1 · the hand is free again ${Math.round(M1.free)} ms after letting go (< 300 ms)`, true);
    check(worst1 < 100, `2 · the longest main-thread block from the drop to the end of the save: ${worst1} ms (< 100 ms)`, true);
    check(early.mc && M1.mc && !M1.mcOffAt && !M1.winLeft, `3 · Manual Control stays on (right after: ${early.mc}, after the save: ${M1.mc}) and the window stays on A`);
    check(!early.panel, `4 · no blocking steps panel during the move (a small status instead: "${early.chip}" → "${M1.chip}")`);
    check(/sav/i.test(early.chip) && /saved/i.test(M1.chip), `4 · the status says saving, then saved ("${early.chip}" → "${M1.chip}")`);
    const nests1 = await page.evaluate(n0 => window.__nests.slice(n0), n0);
    check(nests1.length === 0, `4 · the move started no nest of either sheet (${JSON.stringify(nests1)})`);
    // the records
    check(!recHas(SA.id, p1) && recHas(SB.id, p1), `5 · A's record without the piece, B's record with it (A: ${recHas(SA.id, p1)}, B: ${recHas(SB.id, p1)})`);
    const rp = (() => { const r = recOf(SB.id), c = (r.charms || []).find(z => z.poolId === p1), p = c && (r.placements || []).find(z => z.id === c.id); return p ? { x: +p.cxPt, y: +p.cyPt } : null; })();
    const lv = await page.evaluate(p1 => { const sh = CN.allSheets().find(s => s.charms.some(c => c.poolId === p1)), c = sh.charms.find(z => z.poolId === p1), p = sh.placements.find(z => z.id === c.id); return { sheet: sh.sheetId, x: p.cxPt, y: p.cyPt, pinned: c.pinned && !c.arrivalPin ? c.pinned : null, dirty: !!sh.dirty, src: (() => { const a = CN.allSheets().find(s => s.page === 1 && s.metal === 'gold10k'); return { dirty: !!a.dirty, hold: !!a._manualHold }; })() }; }, p1);
    check(lv.sheet === SB.id && rp && Math.abs(rp.x - lv.x) < .01 && Math.abs(rp.y - lv.y) < .01 && lv.pinned && Math.abs(lv.pinned.cxPt - lv.x) < .01, `5 · on B where it landed (${lv.x.toFixed(1)}, ${lv.y.toFixed(1)}), pinned there, the record says the same (${rp && rp.x.toFixed(1)}, ${rp && rp.y.toFixed(1)})`);
    const pr1 = st.doc(POOL, p1) || {};
    check(pr1.sheetId === SB.id && pr1.movedBy === 'Tester', `5 · the piece record is on B, moved by Tester (${pr1.sheetId}, ${pr1.movedBy})`);

    /* ── 6 · a second charm right after the first, before its save ends ── */
    const p2 = SA.charms[3].poolId;   // (40, 95) on A
    const n1 = await nestsBy();
    await arm(p2, SA.id, SB.id);
    await carry(p2, { x: 240, y: 40 });
    const mid = await page.evaluate(() => ({ mc: CNManual._MC.on }));
    // and a third, at once (it stays on A if the room is gone; A keeps at least one charm)
    await until(saveDone, 120000, 'the second save ends'); await sleep(500);
    const M2 = await page.evaluate(() => Object.assign({}, window.__mh, { mc: CNManual._MC.on, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '' }));
    const worst2 = Math.max(0, ...sinceDrop(M2).map(([, d]) => d));
    console.log(`  ..   move 2: on B at ${Math.round(M2.on)} ms, hand free at ${Math.round(M2.free)} ms; long tasks since the drop ${sinceDrop(M2).length} (worst ${worst2} ms); chip: "${M2.chip}"`);
    check(M2.on > 0 && M2.on < 300 && M2.free > 0 && M2.free < 300, `6 · a second charm: on B at ${Math.round(M2.on)} ms, hand free at ${Math.round(M2.free)} ms`, true);
    check(mid.mc && M2.mc && !M2.mcOffAt, '6 · Manual Control still on after the second move');
    check(!recHas(SA.id, p2) && recHas(SB.id, p2) && recHas(SB.id, p1), '6 · both pieces saved on B and off A');
    const nests2 = await page.evaluate(n1 => window.__nests.slice(n1), n1);
    check(nests2.length === 0, `6 · no nest started by the second move (${JSON.stringify(nests2)})`);


    /* ── 7 · MOVEINSTANT: a slow cloud (every sheet write held 10 s): instant on B, no panel, a second move meanwhile, B opened meanwhile, the saves right, a refused save put back ── */
    SLOW = 10000;
    const p3 = SA.charms[2].poolId, p4 = SA.charms[1].poolId;   // ((140, 40) and (90, 40) on A)
    // a watcher for the whole of section 7: any steps panel, Manual Control's state and sheet against the window's, sampled every 40 ms
    await page.evaluate(() => {
      const L = window.__mi = { panel: [], mcOff: [], mcAway: [] }; const t0 = performance.now();
      window.__miT = setInterval(() => {
        const W = SheetWin.hopKit.W, M = CNManual._MC, t = Math.round(performance.now() - t0);
        const panel = !!W.flow || !!(W.el.work && !W.el.work.hidden && W.el.work.querySelector('.swWork'));
        if (panel) L.panel.push([t, W.flow ? W.flow.title : W.work && W.work.title]);
        if (!M.on) L.mcOff.push(t); else if (M.id !== W.id) L.mcAway.push([t, M.id, W.id]);
      }, 40);
    });
    const dockCount = side => page.evaluate(s => { const dk = SheetHop._H.docks[s]; return dk ? dk.sh.placements.length : -1; }, 'next');
    const bCount0 = await dockCount('next');
    await arm(p3, SA.id, SB.id);
    await carry(p3, { x: 140, y: 95 });
    await until(() => page.evaluate(() => window.__mh.on > 0), 5000, 'the charm lands on B (slow cloud)').catch(() => {});
    const S1 = await page.evaluate(() => ({ on: window.__mh.on, free: window.__mh.free, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '', pending: SheetWin.hopKit.hopState().pending }));
    const bCount1 = await dockCount('next');
    console.log(`  ..   slow move 1: on B at ${Math.round(S1.on)} ms, hand free at ${Math.round(S1.free)} ms, chip "${S1.chip}", saves pending ${S1.pending}; B's dock draws ${bCount0} -> ${bCount1} charms`);
    check(S1.on > 0 && S1.on < 150, `7 · slow cloud: the charm is on B and off A ${Math.round(S1.on)} ms after letting go (< 150 ms)`, true);
    check(bCount1 === bCount0 + 1 && S1.pending === 1 && /sav/i.test(S1.chip), `7 · B's dock draws it at once (${bCount0} -> ${bCount1} charms) while its save runs in the background ("${S1.chip}")`);
    // a second charm while the first is still saving
    await arm(p4, SA.id, SB.id);
    await carry(p4, { x: 190, y: 40 });
    await until(() => page.evaluate(() => window.__mh.on > 0), 5000, 'the second charm lands on B (slow cloud)').catch(() => {});
    const S2 = await page.evaluate(() => ({ on: window.__mh.on, pending: SheetWin.hopKit.hopState().pending, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '' }));
    console.log(`  ..   slow move 2 (the first still saving): on B at ${Math.round(S2.on)} ms, saves pending ${S2.pending}, chip "${S2.chip}"`);
    check(S2.on > 0 && S2.on < 150, `7 · a second move while the first saves: on B ${Math.round(S2.on)} ms after letting go (< 150 ms)`, true);
    check(S2.pending === 2 && !recHas(SB.id, p3) && !recHas(SB.id, p4), `7 · the two saves queue in order (pending ${S2.pending}; neither written yet)`);
    // Paul's look at B: its dock pressed while both still save
    await sleep(500);
    const tb = await tabOf('next'); const tOpen = Date.now();
    await page.mouse.click(tb.x, tb.y);
    const seenB = await until(() => page.evaluate(([id, a, b]) => { const W = SheetWin.hopKit.W; return W.id === id && W.rec && W.byPool.has(a) && W.byPool.has(b) ? { mc: CNManual._MC.on, mcId: CNManual._MC.id, pending: SheetWin.hopKit.hopState().pending } : null; }, [SB.id, p3, p4]), 8000, 'B shows both moved charms').catch(() => null);
    const tSeen = Date.now() - tOpen;
    console.log(`  ..   B opened from its dock while saving: both charms drawn after ${tSeen} ms; ${JSON.stringify(seenB)}`);
    check(!!seenB && seenB.pending > 0 && tSeen < 3000, `7 · B opened while both still save shows both charms at once (${tSeen} ms; saves pending ${seenB && seenB.pending})`);
    check(!!seenB && seenB.mc && seenB.mcId === SB.id, `7 · opening B keeps Manual Control on, now on B (${seenB && seenB.mc}, ${seenB && seenB.mcId === SB.id ? 'B' : seenB && seenB.mcId})`);
    await until(saveDone, 120000, 'the slow saves end');
    await sleep(800);
    const S3 = await page.evaluate(() => ({ mc: CNManual._MC.on, mcId: CNManual._MC.id, win: SheetWin.hopKit.W.id, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '', shown: [...SheetWin.hopKit.W.byPool.keys()] }));
    check(!recHas(SA.id, p3) && !recHas(SA.id, p4) && recHas(SB.id, p3) && recHas(SB.id, p4) && (st.doc(POOL, p3) || {}).sheetId === SB.id && (st.doc(POOL, p4) || {}).sheetId === SB.id, '7 · after the slow saves: both on B\'s record and off A\'s, the piece records on B');
    check(S3.mc && S3.mcId === S3.win && S3.win === SB.id && S3.shown.includes(p3) && S3.shown.includes(p4), `7 · after the saves Manual Control is still on, on the sheet the window shows (${S3.mc}, ${S3.mcId === S3.win}), both charms drawn; chip "${S3.chip}"`);
    // a save the cloud refuses: a charm of B carried to A (the previous dock) comes back
    const pf = SB.charms[0].poolId;   // ((40, 40) on B)
    SLOW = 1500; FAIL = true;
    await until(() => page.evaluate(id => { const s = SheetHop.state(); return s.docks.prev && s.docks.prev.sheetId === id; }, SA.id), 6000, 'the previous dock is A');
    const recB0 = JSON.stringify(recOf(SB.id).poolIds), recA0 = JSON.stringify(recOf(SA.id).poolIds);
    await arm(pf, SB.id, SA.id);
    await carry(pf, { x: 90, y: 95 }, 'prev');
    await until(() => page.evaluate(() => window.__mh.on > 0), 5000, 'the charm lands on A before its save').catch(() => {});
    const F1 = await page.evaluate(() => ({ on: window.__mh.on }));
    const back = await until(() => page.evaluate(([b, id]) => { const sh = CN.allSheets().find(s => s.sheetId === b); return !SheetWin.hopKit.hopState().pending && sh.charms.some(c => c.poolId === id) && SheetWin.hopKit.W.byPool.has(id) ? { toast: [...document.querySelectorAll('#toasts .toast')].map(t => t.textContent).join(' | '), mc: CNManual._MC.on, mcId: CNManual._MC.id, onA: CN.allSheets().find(s => s.charms.some(c => c.poolId === id)).sheetId } : null; }, [SB.id, pf]), 60000, 'the refused move put back').catch(() => null);
    FAIL = false; SLOW = 0;
    console.log(`  ..   refused save: on A at ${Math.round(F1.on)} ms, then ${JSON.stringify(back)}`);
    check(F1.on > 0 && F1.on < 150, `7 · the move that will be refused still lands at once (${Math.round(F1.on)} ms)`, true);
    check(!!back && back.onA === SB.id && JSON.stringify(recOf(SB.id).poolIds) === recB0 && JSON.stringify(recOf(SA.id).poolIds) === recA0, '7 · a refused save puts the charm back on B, on the screen and in the live sheets; neither record changed');
    check(!!back && /not moved/i.test(back.toast) && /back on/i.test(back.toast), `7 · and says so in a toast ("${back && back.toast.slice(0, 160)}")`);
    check(!!back && back.mc && back.mcId === SB.id, '7 · Manual Control stays on after a refused save');
    const L7 = await page.evaluate(() => { clearInterval(window.__miT); return window.__mi; });
    check(!L7.panel.length, `7 · no steps panel at any moment of section 7 (${JSON.stringify(L7.panel.slice(0, 3))})`);
    check(!L7.mcOff.length && !L7.mcAway.length, `7 · Manual Control never went off or left the sheet the window shows (${JSON.stringify([L7.mcOff.slice(0, 3), L7.mcAway.slice(0, 3)])})`);

    /* ── 8 · MOVEINSTANT 2: a move within B not saved yet, then A opened: Manual Control carries on to A, B saved in the background; back on B before that save lands ── */
    SLOW = 3000;
    const pm = p3, to8 = { x: 140, y: 150 };   // ((140, 95) on B since section 7, moved within B: away from the docks at the plate's sides, which would take it to the next sheet)
    await page.evaluate(() => {
      const L = window.__mi8 = { panel: [], mcOff: [], mcAway: [] }; const t0 = performance.now();
      window.__mi8T = setInterval(() => {
        const W = SheetWin.hopKit.W, M = CNManual._MC, t = Math.round(performance.now() - t0);
        if (W.flow || (W.el.work && !W.el.work.hidden && W.el.work.querySelector('.swWork'))) L.panel.push([t, W.flow ? W.flow.title : W.work && W.work.title]);
        if (!M.on) L.mcOff.push(t); else if (M.id !== W.id && !W.flying) L.mcAway.push([t, M.id, W.id]);
      }, 40);
    });
    await until(() => page.evaluate(() => !document.querySelector('.shpDock.open') && !SheetHop.state().busy), 5000, 'the docks folded away').catch(() => {});
    const a8 = await until(() => onPlate(pm), 10000, pm + ' drawn on B'), b8 = await page.evaluate(([x, y]) => CNManual._client(x, y), [to8.x, to8.y]);
    await page.mouse.move(a8.x, a8.y); await page.mouse.down();
    for (let i = 1; i <= 10; i++) await page.mouse.move(a8.x + (b8[0] - a8.x) * i / 10, a8.y + (b8[1] - a8.y) * i / 10);
    await sleep(80);
    await page.mouse.up(); await sleep(400);
    const e8 = await page.evaluate(id => { const M = CNManual._MC, x = SheetWin.hopKit.W.byPool.get(id); return x && M.edits.get(x.id) ? Object.assign({}, M.edits.get(x.id)) : null; }, pm);
    const where8 = e8 ? '' : await page.evaluate(id => JSON.stringify({ on: (CN.allSheets().find(s => s.charms.some(c => c.poolId === id)) || {}).sheetId, sel: CNManual._MC.sel && CNManual._MC.sel.poolId, edits: CNManual._MC.edits.size, hop: SheetHop.state() }), pm);
    check(!!e8 && Math.hypot(e8.cxPt - to8.x, e8.cyPt - to8.y) < 3, `8 · a charm moved by hand within B, not saved yet (${e8 ? [e8.cxPt.toFixed(1), e8.cyPt.toFixed(1)] : where8})`);
    const recPm0 = (() => { const r = recOf(SB.id), c = (r.charms || []).find(z => z.poolId === pm), q = c && (r.placements || []).find(z => z.id === c.id); return q ? [+q.cxPt, +q.cyPt] : null; })();
    await until(() => page.evaluate(id => { const s = SheetHop.state(); return s.docks.prev && s.docks.prev.sheetId === id; }, SA.id), 6000, 'the previous dock is A');
    const ta = await tabOf('prev'); await page.mouse.click(ta.x, ta.y);
    const onA = await until(() => page.evaluate(id => { const W = SheetWin.hopKit.W, M = CNManual._MC; return W.id === id && W.rec && W.geom ? { mc: M.on, mcId: M.id, saving: (document.querySelector('[data-mh=chip]') || {}).textContent || '' } : null; }, SA.id), 8000, 'A opens').catch(() => null);
    console.log(`  ..   B's move not saved, A opened: ${JSON.stringify(onA)}; B's record still has the charm at ${JSON.stringify(recPm0)}`);
    check(!!onA && onA.mc && onA.mcId === SA.id, `8 · opening A with B's move unsaved keeps Manual Control on, now on A (${onA && onA.mc}, ${onA && onA.mcId === SA.id ? 'A' : onA && onA.mcId})`);
    check(!!onA && /sav/i.test(onA.saving), `8 · B's move is being saved in the background ("${onA && onA.saving}")`);
    // back on B before that save lands: B shows the charm where it was put, and takes a hand once it is saved
    await sleep(300);
    await until(() => page.evaluate(id => { const s = SheetHop.state(); return s.docks.next && s.docks.next.sheetId === id; }, SB.id), 6000, 'the next dock is B');
    const tbb = await tabOf('next'); await page.mouse.click(tbb.x, tbb.y);
    const backB = await until(() => page.evaluate(([id, pm]) => { const W = SheetWin.hopKit.W, M = CNManual._MC, x = W.byPool.get(pm); return W.id === id && W.rec && W.geom && x ? { mc: M.on, mcId: M.id, wait: !!M.wait, at: [x.p.cxPt, x.p.cyPt], pending: !!SheetWin.hopKit.hopState().writing } : null; }, [SB.id, pm]), 8000, 'B opens again').catch(() => null);
    console.log(`  ..   back on B while its save runs: ${JSON.stringify(backB)}`);
    check(!!backB && backB.mc && backB.mcId === SB.id && Math.hypot(backB.at[0] - e8.cxPt, backB.at[1] - e8.cyPt) < .01, `8 · B opened again while its save runs: Manual Control on B, the charm drawn where it was put (${backB && backB.at.map(v => v.toFixed(1))}; waiting for its save: ${backB && backB.wait})`);
    await until(() => page.evaluate(() => { const M = CNManual._MC, K = SheetWin.hopKit; return M.on && !M.wait && !M.claiming && !K.hopState().writing && !K.hopState().pending; }), 120000, 'B saved, and taken by Manual Control again');
    await sleep(600);
    const s8 = await page.evaluate(([id, pm]) => { const M = CNManual._MC, W = SheetWin.hopKit.W, sh = CN.allSheets().find(p => p.sheetId === id), c = sh.charms.find(z => z.poolId === pm), q = sh.placements.find(z => z.id === c.id), x = W.byPool.get(pm); return { mc: M.on, mcId: M.id, win: W.id, hold: !!sh._manualHold, pinned: c.pinned && !c.arrivalPin ? [c.pinned.cxPt, c.pinned.cyPt] : null, manual: c.manual || null, live: [q.cxPt, q.cyPt], shown: x ? [x.p.cxPt, x.p.cyPt] : null, chip: (document.querySelector('[data-mh=chip]') || {}).textContent || '' }; }, [SB.id, pm]);
    const recPm = (() => { const r = recOf(SB.id), c = (r.charms || []).find(z => z.poolId === pm), q = c && (r.placements || []).find(z => z.id === c.id); return q ? [+q.cxPt, +q.cyPt] : null; })();
    console.log(`  ..   after B's background save: ${JSON.stringify(s8)}; record ${JSON.stringify(recPm)}; claim on B ${JSON.stringify(recOf(SB.id).manualEdit || null)}`);
    const near8 = v => v && Math.hypot(v[0] - e8.cxPt, v[1] - e8.cyPt) < .01;
    check(near8(recPm) && near8(s8.live) && near8(s8.pinned) && s8.manual && s8.manual.by === 'Tester', `8 · B's move saved in the background: on the record, live and pinned where it was put, marked by Tester (record ${JSON.stringify(recPm)})`);
    check(s8.mc && s8.mcId === SB.id && s8.win === SB.id && s8.hold && !!recOf(SB.id).manualEdit && near8(s8.shown), `8 · after the save Manual Control holds B again (on: ${s8.mc}, held: ${s8.hold}, claimed: ${!!recOf(SB.id).manualEdit}), the charm drawn where it was put`);
    check(/saved/i.test(s8.chip), `8 · the chip says saved ("${s8.chip}")`);
    const L8 = await page.evaluate(() => { clearInterval(window.__mi8T); return window.__mi8; });
    check(!L8.panel.length, `8 · no steps panel at any moment (${JSON.stringify(L8.panel.slice(0, 3))})`);
    check(!L8.mcOff.length && !L8.mcAway.length, `8 · Manual Control never went off or away from the sheet the window shows (${JSON.stringify([L8.mcOff.slice(0, 3), L8.mcAway.slice(0, 3)])})`);
    SLOW = 0;

    /* ── Manual Control off: A's own save, then a reload ── */
    await page.evaluate(() => { const L = window.__miOff = []; window.__miOffT = setInterval(() => { const W = SheetWin.hopKit.W; if (W.flow || (W.el.work && !W.el.work.hidden && W.el.work.querySelector('.swWork'))) L.push(W.flow ? W.flow.title : 'panel'); }, 40); });
    await page.click('.sheetWin [data-r=manual]');
    const tOff = Date.now();
    await sleep(400);
    await until(() => page.evaluate(() => !CNManual._MC.on && !SheetWin.hopKit.W.flow && CN.S.sheets.gold10k.pages.filter(p => p.charms.length).every(sh => sh.status === 'complete' && sh.persistedDone && !sh.dirty && !sh._manualHold)), 240000, 'Manual Control off and its save done');
    await sleep(1500);
    // written as they stand: nothing moved on either sheet when their files were made again
    const after = await page.evaluate(() => Object.fromEntries(CN.S.sheets.gold10k.pages.filter(p => p.charms.length).map(sh => [sh.sheetId, { ok: !!(sh.verification && sh.verification.ok), at: Object.fromEntries(sh.placements.map(p => [sh.charms.find(c => c.id === p.id).poolId, [p.cxPt, p.cyPt]])) }])));
    const near = (a, b) => a && b && Math.abs(a[0] - b[0]) < .01 && Math.abs(a[1] - b[1]) < .01;
    const stayedA = SA.charms.filter(c => ![p1, p2, p3, p4].includes(c.poolId)).every(c => near(after[SA.id].at[c.poolId], SA.at[c.id]));
    const stayedB = SB.charms.filter(c => c.poolId !== pm).every(c => near(after[SB.id].at[c.poolId], SB.at[c.id])) && near(after[SB.id].at[pm], [e8.cxPt, e8.cyPt]) && near(after[SB.id].at[p1], [lv.x, lv.y]) && [p2, p3, p4].every(id => after[SB.id].at[id]);
    const offPanels = await page.evaluate(() => { clearInterval(window.__miOffT); return window.__miOff; });
    console.log(`  ..   Manual Control off: both sheets written as they stand in ${Date.now() - tOff} ms, in the background`);
    check(!offPanels.length, `7 · Manual Control off: the sheets are written as they stand in the background, no steps panel (${JSON.stringify(offPanels.slice(0, 3))})`);
    check(stayedA && stayedB && after[SA.id].ok && after[SB.id].ok, `5 · Manual Control off: both sheets written and verified with every charm where it was (A kept its charms: ${stayedA}; B kept its own and the moved one where it landed: ${stayedB})`);
    await until(() => st.doc(SHEETS, SA.id).manualEdit == null && st.doc(SHEETS, SB.id).manualEdit == null, 15000, 'the claims given back').catch(() => {});
    check(st.doc(SHEETS, SA.id).manualEdit == null && st.doc(SHEETS, SB.id).manualEdit == null, `5 · the claims on A and B were given back (${JSON.stringify([st.doc(SHEETS, SA.id).manualEdit, st.doc(SHEETS, SB.id).manualEdit])})`);
    const allIds = W0.flatMap(s => s.charms.map(c => c.poolId));
    const oneEach = async what => {
      const docs = st.list(SHEETS);
      for (const id of allIds) { const on = docs.filter(d => (d.poolIds || []).includes(id)).map(d => d._id || d.id); assert.equal(on.length, 1, `${what}: ${id} on exactly one saved sheet: ${JSON.stringify(on)}`); }
      const lv2 = await page.evaluate(ids => Object.fromEntries(ids.map(id => [id, CN.allSheets().filter(s => s.charms.some(c => c.poolId === id)).map(s => s.sheetId)])), allIds);
      for (const id of allIds) { const on = docs.filter(d => (d.poolIds || []).includes(id)).map(d => d._id || d.id); assert.deepEqual(lv2[id], on, `${what}: the sorter holds ${id} where the cloud does`); }
    };
    try { await oneEach('before the reload'); check(true, '5 · before the reload: every piece on exactly one saved sheet, the sorter agrees'); } catch (e) { check(false, '5 · before the reload: ' + e.message); }
    await page.reload({ waitUntil: 'load' }); await ready(); await sleep(1500);
    await until(() => page.evaluate(n => CN.S.sheets.gold10k.pages.filter(p => p.charms.length).length >= n, 2), 30000, 'the sheets restored').catch(() => {});
    try { await oneEach('after the reload'); check(true, '5 · after a reload: the moved pieces are on B only, nothing duplicated or lost'); } catch (e) { check(false, '5 · after the reload: ' + e.message); }
    check(errors.length === 0, 'no page errors' + (errors.length ? ': ' + errors.join(' | ') : ''));
  } catch (e) { failures.push('crashed: ' + e.message); console.log('  FAIL crashed: ' + (e.stack || e.message)); }
  finally { await browser.close(); srv.close(); }
  console.log(`\n${results.length} passed, ${failures.length} failed`);
  if (failures.length) { for (const f of failures) console.log('  - ' + f); process.exit(1); }
})().catch(e => { console.error(e); process.exit(1); });
