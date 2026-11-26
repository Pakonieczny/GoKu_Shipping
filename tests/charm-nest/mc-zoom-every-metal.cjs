// MCEVERY + MCZOOM (Paul, 25 Nov 2026): "When clicking into a sheet inside the library, you need to give users an option to click manual control kind of like we already have in 14K sheets
// ... click on any given charm size it up and down, rotated it and position it anywhere on a sheet. We DO NOT need the ability to move orders between sheets in this case." "Add the ability to
// zoom in into any charm on a sheet while in manual mode, and ensure that the charms are showing as vectors so that their quality does not diminish when zooming in ... the exact positional
// accuracy ... perfectly shown", the pan that goes with it, and "shift + keyboard arrows, and just keyboard arrows movement for very fine adjustment of positioning especially when zoomed in".
// In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary handlers over an in-memory store; nothing live). A GOLD FILLED sheet is made by the page itself (five 20 pt
// squares nested, written, verified and saved by the real nest), then, in its sheet window:
//   1  Manual Control is offered on a GF sheet (it was 10K / 14K only) and starts
//   2  the plate's zoom is there while it is on; the wheel zooms at the pointer: the sheet point under the pointer stays under it, the scale is the plate's times the zoom
//   3  the charm in hand, zoomed: an arrow moves it one screen pixel's worth (0.005 to 0.05 mm), Shift + arrow ten of them; exact
//   4  a drag on the empty sheet pans; Fit is the whole sheet again; a double-click on a charm zooms to it, its centre in the middle of the view
//   5  off: saved through the server (sheetLayoutPatch), the record holds exactly the pose the arrows made; the view is the whole sheet again and the zoom buttons go
//   6  no docks to move a GF charm onto another sheet (the hop stays 10K / 14K)
//   node tests/charm-nest/mc-zoom-every-metal.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules', '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const SHEETS = 'Charm_Nest_Sheets';
  const pdf = await scene({ page: [420, 200], bodies: [], extraOps: [[20, 20], [80, 20], [140, 20], [200, 20], [260, 20]].map(([x, y]) => `q 0 0 0 RG 0.25 w ${x} ${y} 20 20 re S Q`) });
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
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 120)); } };
  let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; console.log('ok   ' + m); };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNManual && window.SheetEvents && window.RoseStock, null, { timeout: 60000 });
    /* ── the sheet, made by the page: five squares on Gold Filled, nested and saved, laid out at known spots ── */
    await page.evaluate(async b64 => {
      CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold = [4, 2.5];
      const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'gold');
    }, pdf.toString('base64'));
    const settled = () => page.evaluate(() => { const sh = CN.S.sheets.gold.pages.at(-1); if (sh.charms.length === 5 && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === 5 && sh.status === 'complete' && sh.persistedDone && !!sh.verification && sh.verification.ok && !sh.dirty && !!sh.sheetId; });
    await until(settled, 120000, 'the GF sheet nested, verified and saved');
    const L = [[40, 40], [90, 40], [40, 95], [170, 100], [230, 45]];
    await page.evaluate(L => { const sh = CN.S.sheets.gold.pages.at(-1); sh.charms.forEach((c, i) => { c.pinned = { cxPt: L[i][0], cyPt: L[i][1], angle: 0 }; c.arrivalPin = true; }); sh.placements = []; sh.dirty = true; sh.status = 'ready'; CN.startNest(sh); }, L);
    await until(settled, 120000, 'the sheet laid out at the known spots');
    const S0 = await page.evaluate(() => { const sh = CN.S.sheets.gold.pages.at(-1); return { id: sh.sheetId, charms: sh.charms.map(c => c.id), metal: sh.metal }; });
    const cB = S0.charms[1];
    ok(S0.metal === 'gold', `a Gold Filled sheet made by the page (${S0.id}): five squares nested, verified and saved`);

    const sw = () => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b ? { hidden: b.hidden, pressed: b.getAttribute('aria-pressed'), disabled: b.getAttribute('aria-disabled'), why: b.dataset.why || '' } : null; });
    await page.evaluate(id => SheetWin.open(id), S0.id);
    await until(() => page.evaluate(id => { const W = SheetWin._W; return W.id === id && !!W.rec && W.geom && !W.flying; }, S0.id), 30000, 'the sheet window');
    await page.waitForTimeout(300);
    /* ── 1 ── */
    const s1 = await until(async () => { const v = await sw(); return v && !v.hidden && v.disabled === 'false' && !v.why ? v : null; }, 20000, 'Manual Control free to start').catch(async e => { throw new Error(e.message + ' · ' + JSON.stringify(await sw())); });
    ok(!s1.hidden, '1 Manual Control is offered on a Gold Filled sheet (it was 10K / 14K only)');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 30000, 'Manual Control on');
    ok(true, '1 and it starts');
    /* ── 2 ── */
    ok(await page.evaluate(() => { const z = document.querySelector('.sheetWin .swZoom'); return !!z && z.classList.contains('on') && getComputedStyle(z).display !== 'none'; }), '2 the plate\'s zoom buttons are there while Manual Control is on');
    const at = (x, y) => page.evaluate(([x, y]) => CNManual._client(x, y), [x, y]);
    const b0 = st.doc(SHEETS, S0.id).placements.find(z => z.id === cB);
    const bc = await at(b0.cxPt, b0.cyPt), P = [Math.round(bc[0]), Math.round(bc[1])];   // (a whole pixel: the wheel's own pointer position)
    const sheetAt = q => page.evaluate(([x, y]) => { const W = SheetWin._W, cv = W.el.fx, r = cv.getBoundingClientRect(), s = cv.width / r.width; return [((x - r.left) * s - (W.ox != null ? W.ox : W.R)) / W.k, ((y - r.top) * s - (W.oy != null ? W.oy : W.R)) / W.k]; }, q);
    const S = await sheetAt(P), before = P;
    await page.mouse.move(P[0], P[1]);
    for (let i = 0; i < 6; i++) { await page.mouse.wheel(0, -240); await page.waitForTimeout(40); }
    await page.waitForTimeout(150);
    const v2 = await page.evaluate(() => { const W = SheetWin._W; return { z: W.zv && W.zv.z, k: W.k, k0: W.k0, ox: W.ox, oy: W.oy }; });
    ok(v2.z > 4, `2 the wheel zooms (${(v2.z * 100).toFixed(0)} %)`);
    ok(Math.abs(v2.k - v2.k0 * v2.z) < 1e-9, '2 the sheet is drawn at the plate\'s scale times the zoom (its vectors drawn again, not a picture enlarged)');
    const anchor = await at(S[0], S[1]), after = await at(b0.cxPt, b0.cyPt);
    ok(Math.hypot(anchor[0] - before[0], anchor[1] - before[1]) < 1, `2 the point under the pointer stays under it (${Math.hypot(anchor[0] - before[0], anchor[1] - before[1]).toFixed(3)} px)`);
    ok(await page.evaluate(() => getComputedStyle(SheetWin._W.el.pv).visibility === 'hidden'), '2 the saved picture of the sheet is not shown over the zoomed vectors');
    /* ── 3 ── */
    await page.mouse.click(after[0], after[1]);
    await until(() => page.evaluate(id => CNManual._MC.sel && CNManual._MC.sel.id === id, cB), 5000, 'the charm in hand');
    const step = await page.evaluate(() => { const W = SheetWin._W, MM = 72 / 25.4; return Math.max(0.005 * MM, Math.min(0.05 * MM, (W.dpr || 1) / W.k)); });
    ok(step < 0.05 * 72 / 25.4 && step >= 0.005 * 72 / 25.4, `3 zoomed, one arrow is ${(step * 25.4 / 72).toFixed(4)} mm (finer than the 0.05 mm of the whole sheet)`);
    const pose = () => page.evaluate(id => { const MC = CNManual._MC, e = MC.edits && MC.edits.get(id); if (e) return { cxPt: e.cxPt, cyPt: e.cyPt }; const p = MC.sh.placements.find(q => q.id === id); return { cxPt: +p.cxPt, cyPt: +p.cyPt }; }, cB);
    const p0 = await pose();
    await page.keyboard.press('ArrowRight'); await page.waitForTimeout(60);
    const p1 = await pose();
    ok(Math.abs((p1.cxPt - p0.cxPt) - step) < 1e-9 && Math.abs(p1.cyPt - p0.cyPt) < 1e-12, `3 an arrow moves it exactly that much (${((p1.cxPt - p0.cxPt) * 25.4 / 72).toFixed(4)} mm)`);
    await page.keyboard.press('Shift+ArrowRight'); await page.waitForTimeout(60);
    const p2 = await pose();
    ok(Math.abs((p2.cxPt - p1.cxPt) - 10 * step) < 1e-9, `3 Shift + arrow moves it ten of them (${((p2.cxPt - p1.cxPt) * 25.4 / 72).toFixed(4)} mm)`);
    await page.keyboard.press('ArrowDown'); await page.waitForTimeout(400);
    const p3 = await pose();
    ok(Math.abs((p3.cyPt - p2.cyPt) - step) < 1e-9, '3 and down by one');
    /* ── 4 ── */
    for (let i = 0; i < 8 && await page.evaluate(() => SheetWin._W.zv.z > 4); i++) { await page.click('.sheetWin .swZoom [data-z=out]'); await page.waitForTimeout(80); }   // (zoomed out to a few charms in view: the buttons)
    ok(await page.evaluate(() => SheetWin._W.zv.z > 1 && SheetWin._W.zv.z <= 4), `4 the − button zooms out (${await page.evaluate(() => Math.round(SheetWin._W.zv.z * 100))} %)`);
    const vx0 = await page.evaluate(() => ({ x: SheetWin._W.zv.x, y: SheetWin._W.zv.y }));
    const empty = await page.evaluate(() => { const W = SheetWin._W, cv = W.el.fx, r = cv.getBoundingClientRect(), s = cv.width / r.width; for (let gy = 0.15; gy < 0.95; gy += 0.07) for (let gx = 0.15; gx < 0.95; gx += 0.07) { const cx = r.left + r.width * gx, cy = r.top + r.height * gy, px = ((cx - r.left) * s - (W.ox != null ? W.ox : W.R)) / W.k, py = ((cy - r.top) * s - (W.oy != null ? W.oy : W.R)) / W.k; if (px < 0 || py < 0 || px > W.st.wPt || py > W.st.hPt) continue; const hit = W.pieces.some(x => Math.abs(x.p.cxPt - px) < 14 && Math.abs(x.p.cyPt - py) < 14); if (!hit) return [cx, cy]; } return null; });
    ok(!!empty, '4 an empty spot of the zoomed sheet found to drag');
    await page.mouse.move(empty[0], empty[1]); await page.mouse.down();
    for (let i = 1; i <= 6; i++) await page.mouse.move(empty[0] + 10 * i, empty[1] + 6 * i);
    await page.mouse.up(); await page.waitForTimeout(150);
    const vx1 = await page.evaluate(() => ({ x: SheetWin._W.zv.x, y: SheetWin._W.zv.y, sel: CNManual._MC.sel && CNManual._MC.sel.id }));
    ok(Math.abs(vx1.x - vx0.x) > 20 || Math.abs(vx1.y - vx0.y) > 10, `4 a drag on the empty sheet pans the view (${(vx1.x - vx0.x).toFixed(0)}, ${(vx1.y - vx0.y).toFixed(0)} px)`);
    ok(vx1.sel === cB, '4 and the charm stays in hand (a pan is no press on the sheet)');
    await page.click('.sheetWin .swZoom [data-z=fit]'); await page.waitForTimeout(150);
    ok(await page.evaluate(() => SheetWin._W.zv.z === 1 && SheetWin._W.ox == null && Math.abs(SheetWin._W.k - SheetWin._W.k0) < 1e-12), '4 Fit is the whole sheet again');
    const cC = S0.charms[3], c0 = st.doc(SHEETS, S0.id).placements.find(z => z.id === cC), cAt = await at(c0.cxPt, c0.cyPt);
    await page.mouse.dblclick(cAt[0], cAt[1]); await page.waitForTimeout(200);
    const dz = await page.evaluate(([x, y]) => { const W = SheetWin._W, cv = W.el.base, mid = [W.R + (cv.width - W.R) / 2, W.R + (cv.height - W.R) / 2], X = (W.ox != null ? W.ox : W.R) + x * W.k, Y = (W.oy != null ? W.oy : W.R) + y * W.k; return { z: W.zv.z, off: Math.hypot(X - mid[0], Y - mid[1]) / W.dpr }; }, [c0.cxPt, c0.cyPt]);
    ok(dz.z > 1.5 && dz.off < 30, `4 a double-click on a charm zooms to it (${(dz.z * 100).toFixed(0)} %, its centre ${dz.off.toFixed(1)} px from the middle)`);
    /* ── 6 ── */
    ok(await page.evaluate(() => !document.querySelector('.shDock, .hopDock, [data-hop-dock]') || [...document.querySelectorAll('.shDock, .hopDock, [data-hop-dock]')].every(d => d.hidden || getComputedStyle(d).display === 'none')), '6 no docks to move a GF charm onto another sheet (the hop stays 10K / 14K)');
    /* ── 5 ── */
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => !CNManual._MC.on), 20000, 'Manual Control off');
    await until(() => { const r = st.doc(SHEETS, S0.id), p = r && r.placements.find(z => z.id === cB); return p && Math.abs(+p.cxPt - p3.cxPt) < 1e-6; }, 60000, 'the save of the moved charm');
    const r1 = st.doc(SHEETS, S0.id), rp = r1.placements.find(z => z.id === cB), rc = r1.charms.find(z => z.id === cB);
    ok(Math.abs(rp.cxPt - p3.cxPt) < 1e-6 && Math.abs(rp.cyPt - p3.cyPt) < 1e-6, `5 saved through the server: the record holds exactly the pose the arrows made (${(rp.cxPt - b0.cxPt).toFixed(4)} pt right, ${(rp.cyPt - b0.cyPt).toFixed(4)} pt down)`);
    ok(!!(rc && rc.manual && rc.pinned), '5 with its hand mark and pin');
    ok(await page.evaluate(() => SheetWin._W.zv.z === 1 && !document.querySelector('.sheetWin .swZoom.on')), '5 the view is the whole sheet again and the zoom buttons go');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | ')); n++;
    console.log(`\n${n}/${n} checks pass`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
