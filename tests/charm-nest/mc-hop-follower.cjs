// HOPLIVE (Paul, 25 Nov 2026: "When I'm in manual mode inside the 10k 14K or RG sheets I can't seem to move charms between sheets. I used to have an option to drag a charm onto a new sheet,
// but now that option doesn't seem to work because the available sheets are not showing"). Since FASTOPEN / NODISK (23 Nov) a computer holds the run's sheets only as their saved records (the
// cards are pictures, no charms) except the one it looks at in the Nest tab, so a 10K sheet opened from the Library had no sheets of its run to drop onto. In a real Chromium over the fake site:
//   1  two 10K sheets of one run, nested and saved by the real nest; then this computer holds them only as records (charms and placements gone, recalled), as every computer does after a reload
//   2  Manual Control on sheet A asks the cloud to build it here (CharmNestCloudSheets.ensureLive); a sheet that cannot be built WHOLE (here: squares with no SKU drawing) is put back as its
//      record and Manual Control starts on the record, as before, its record untouched (liveWhole: a sheet held short would drop the charms it lacks when written)
//   3  the run's other sheet, not buildable whole either, is never a dock; turned off, both records are as they were
//   (the whole-build path is CharmNestCloudSheets.reload, the very build every computer ran for every followed sheet before 23 Nov (makeLive); it needs SKU masters, which this world has not)
//   node tests/charm-nest/mc-hop-follower.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules', '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', RUN = 'run-hf-1';
  const pdf = await scene({ page: [560, 200], bodies: [], extraOps: [20, 80, 140, 200, 260, 320, 380, 440].map(x => `q 0 0 0 RG 0.25 w ${x} 20 20 20 re S Q`) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const exe = process.env.CHROMIUM || ['/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'].find(f => fs.existsSync(f));
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = () => ({}); export const getAuth = () => ({}); export default {};" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(who => { try { localStorage.setItem('cn.employee', who); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => who; }, 'Tester');
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  page.setDefaultTimeout(30000);
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 150)); } };
  let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('ok   ' + m); };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNManual && window.SheetHop && window.CharmNestCloudSheets, null, { timeout: 60000 });
    /* ── 1 · the world: eight squares on 10K, five on sheet A and three on sheet B, one run, each laid out by the nest and saved ── */
    await page.evaluate(async b64 => {
      CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold10k = [4, 2.5];
      const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'gold10k');
    }, pdf.toString('base64'));
    await until(() => page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages[0]; if (sh.charms.length === 8 && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === 8 && sh.status === 'complete' && sh.persistedDone && sh.sheetId; }), 120000, 'the 8 squares nested and saved');
    const RID = k => '42200000' + String(k).padStart(2, '0'), TX = k => String(7100000000 + k);
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
    }, { RUN, rids: [1, 2, 3, 4, 5, 6, 7, 8].map(RID), txs: [1, 2, 3, 4, 5, 6, 7, 8].map(TX) });
    const settledAll = k => page.evaluate(k => { const ps = CN.S.sheets.gold10k.pages.filter(p => p.charms.length); for (const sh of ps) if (sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return ps.length === k && ps.every(sh => sh.status === 'complete' && sh.persistedDone && sh.verification && sh.verification.ok && !sh.dirty && sh.sheetId); }, k);
    await until(() => settledAll(2), 180000, 'both sheets nested at their known spots and saved');
    const W0 = await page.evaluate(() => CN.S.sheets.gold10k.pages.filter(p => p.charms.length).map(sh => ({ id: sh.sheetId, charms: sh.charms.map(c => ({ id: c.id, poolId: c.poolId, rid: c.order })) })));
    for (const sh of W0) for (const c of sh.charms) st.put(POOL, c.poolId, { poolId: c.poolId, orderId: c.rid, transactionId: TX(+c.rid.slice(-2)), lineKey: c.poolId.replace(/_1$/, ''), sheetId: sh.id, state: 'placed', material: 'gold10k', runId: RUN });
    const [SA, SB] = W0;
    // (as every computer holds them since FASTOPEN / NODISK after a reload: the run's cards from their records, no charms, not carrying the run here)
    await page.evaluate(({ RUN }) => { for (const sh of CN.S.sheets.gold10k.pages.filter(p => p.sheetId)) { sh.recalled = { id: sh.sheetId, metal: 'gold10k', runId: RUN }; sh.followed = true; sh.charms = []; sh.placements = []; } window.B.run = null; }, { RUN });
    ok(await page.evaluate(() => CN.S.sheets.gold10k.pages.filter(p => p.sheetId).every(p => p.recalled && !p.charms.length)), `1 two 10K sheets of one run (${SA.id}: ${SA.charms.length} charms, ${SB.id}: ${SB.charms.length}), held here only as their records`);
    /* ── 2 · this world's squares have no SKU drawing, so the cloud cannot build their sheet here whole: the guard (liveWhole) puts it back and Manual Control starts on its record, as before ── */
    const recBefore = JSON.stringify(st.doc(SHEETS, SA.id).placements);
    await page.evaluate(id => SheetWin.open(id), SA.id);
    await until(() => page.evaluate(id => { const W = SheetWin._W; return W.id === id && !!W.rec && W.geom && !W.flying; }, SA.id), 30000, 'the window on A');
    await until(() => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b && !b.hidden && b.getAttribute('aria-disabled') === 'false'; }), 20000, 'Manual Control can start');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 120000, 'Manual Control on').catch(async e => { throw new Error(e.message + ' · ' + JSON.stringify(await page.evaluate(id => { const W = SheetWin._W, p = CN.allSheets().find(q => q.sheetId === id), b = document.querySelector('.sheetWin [data-r=manual]'); return { on: CNManual._MC.on, claiming: CNManual._MC.claiming, recalled: !!(p && p.recalled), charms: p && p.charms.length, after: W.mcAfterOpen || null, why: b && b.dataset.why }; }, SA.id))); });
    const st2 = await page.evaluate(id => { const p = CN.allSheets().find(q => q.sheetId === id), MC = CNManual._MC; return { recalled: !!(p && p.recalled), charms: p ? p.charms.length : -1, placements: p ? p.placements.length : -1, standalone: !!(MC.sh && MC.sh._standalone), n: MC.sh ? MC.sh.placements.length : 0 }; }, SA.id);
    ok(st2.recalled && st2.charms === 0 && st2.placements === 0, `2 a sheet that could not be built whole here is put back as its record (no sheet with ${SA.charms.length - 1} charms missing is ever held, edited or written)`);
    ok(st2.standalone && st2.n === SA.charms.length, `2 and Manual Control still starts on it, from its record, with all ${st2.n} charms (one press)`);
    ok(JSON.stringify(st.doc(SHEETS, SA.id).placements) === recBefore, '2 its record is untouched');
    /* ── 3 · no sheet built short is a sheet to drop onto ── */
    await page.waitForTimeout(1500);
    ok(await page.evaluate(id => { const s = SheetHop.state(), p = CN.allSheets().find(q => q.sheetId === id); return !(s.docks.next && s.docks.next.sheetId === id) && !!(p && p.recalled && !p.charms.length); }, SB.id), '3 sheet B, not buildable whole here either, is no dock and stays as its record');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => !CNManual._MC.on), 20000, 'Manual Control off');
    ok(JSON.stringify(st.doc(SHEETS, SA.id).placements) === recBefore && JSON.stringify(st.doc(SHEETS, SB.id).placements.map(q => q.id).sort()) === JSON.stringify(SB.charms.map(c => c.id).sort()), '3 turned off with nothing moved: both records exactly as they were');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | ')); n++;
    console.log(`\n${n}/${n} checks pass`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
