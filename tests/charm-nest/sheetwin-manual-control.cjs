// MANUAL CONTROL (Paul, 18 Oct 2026: "Implement the below only for 14k and 10k solid gold sheets ... the ability to manually move and rotate any of the
// existing charms on a given sheet ... a new button called 'Manual Control' ... click on a charm, move it, rotate it and resize it — all with the same tool ...
// prevent placing a charm over another charm, but be precise enough to allow placing a charm right next to another charm as long as the lines of the two
// adjacent charms do not overlap ... Once the user is done ... they click off the 'Manual Control' button").
// In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary handlers over an in-memory store; nothing live). The 10K sheet is made
// through the page itself (a PDF of five 20 pt squares dropped on 10K Gold, nested, written, verified and saved by the real nest), then laid out at known spots by
// the nest (pins), so every pose below is exact:
//   1  the switch: shown on a 10K / 14K sheet only (not on a Gold-filled sheet), and says why it cannot start (cut, lined, approved, being written, run not here)
//   2  on: the sheet is claimed (manualEdit on its record) and held; press-drag moves a charm, the round handle turns it in 15° steps (Shift: by the degree),
//      a corner resizes it within 80-120 %
//   2b MCFREE (Paul, 20 Oct 2026: "Make it so these control vertices are always visible, even if they have to stick out beyond the bounds of the charm sheet"): a charm
//      at the sheet's top-right corner has its 4 corner squares and its knob drawn (on the stage's own layer) and inside what can be seen, where the plate alone would
//      have cut at least one off; the knob, pressed where it is drawn, turns it; the same at another window size; Ctrl+Z puts it back
//   3  MCFREE (Paul, 20 Oct 2026: "You should NOT prevent the user from placing a charm anywhere on a sheet ... Green when clear, red when conflicting/overlapping"):
//      dragged onto another charm: clay while it is there, and let go it STAYS there, it and the charm it meets red, the stage saying "2 charms overlap"; moved clear,
//      green again and the note gone
//   4  edge to edge with another charm (the outlines touch, never cross): allowed; 3 pt into it: refused (CNManual.fits says the same)
//   5  off: saved through the real nest: the moved charms pinned where they were put (pinned, no arrivalPin, manual {by}), the resized one written at its size,
//      the sheet verified, its record carrying the new layout and the manual mark, the claim given back, a "moved" step on the timeline
//   6  a second computer: it cannot start while the first holds the sheet ("... on another screen"), and the server refuses its claim
//   8  MCFREE: a charm left where it overlaps is saved there (written as it stands, no search), the sheet flagged: "2 charms overlap" on its card and on the stage, the
//      Sets gate holds any set with it; Manual Control again shows both red from the start, and moved clear and saved, the sheet verifies again
//   7  the moved charms survive a later nest from scratch (every other charm free to move): they stay exactly where they were put
//   node tests/charm-nest/sheetwin-manual-control.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>; SHOT_DIR=<dir> saves pictures)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const SHEETS = 'Charm_Nest_Sheets';
  // five 20 pt squares, far apart on the page so each is its own charm
  const pdf = await scene({ page: [420, 200], bodies: [], extraOps: [[20, 20], [80, 20], [140, 20], [200, 20], [260, 20]].map(([x, y]) => `q 0 0 0 RG 0.25 w ${x} ${y} 20 20 re S Q`) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const mkCtx = async name => {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
    await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
      const u = r.request().url();
      if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
      if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
      if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(who => { try { localStorage.setItem('cn.employee', who); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => who;
      window.__tl = []; const iv = setInterval(() => { const T = window.OrderTimeline; if (!T || T.__spy) return; T.__spy = true; const rec = T.record; T.record = e => { try { window.__tl.push(JSON.parse(JSON.stringify(e))); } catch (_) {} return rec ? rec.call(T, e) : e; }; clearInterval(iv); }, 20); }, name);
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push('page: ' + e.message));
    page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
    page.setDefaultTimeout(30000);
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNManual && window.SheetEvents && window.RoseStock, null, { timeout: 60000 });
    return { ctx, page, errors };
  };
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 150)); } };
  const A = await mkCtx('Tester'), page = A.page;
  try {
    /* ── the sheet, made by the page: five squares on 10K Gold, nested and saved, then laid out where the test wants them ── */
    await page.evaluate(async b64 => {
      CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold10k = [4, 2.5];
      const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'gold10k');
    }, pdf.toString('base64'));
    const settled = () => page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1); if (sh.charms.length === 5 && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === 5 && sh.status === 'complete' && sh.persistedDone && !!sh.verification && sh.verification.ok && !sh.dirty && !!sh.sheetId; });
    await until(settled, 120000, 'the 10K sheet nested, verified and saved');
    // the known layout: A B on the top row, C under A, D and E further on (all square to the sheet)
    const L = [[40, 40], [90, 40], [40, 95], [170, 100], [230, 45]];
    await page.evaluate(L => { const sh = CN.S.sheets.gold10k.pages.at(-1); sh.charms.forEach((c, i) => { c.pinned = { cxPt: L[i][0], cyPt: L[i][1], angle: 0 }; c.arrivalPin = true; }); sh.placements = []; sh.dirty = true; sh.status = 'ready'; CN.startNest(sh); }, L);
    await until(settled, 120000, 'the sheet laid out at the known spots');
    const S0 = await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1); return { id: sh.sheetId, charms: sh.charms.map(c => c.id), at: Object.fromEntries(sh.placements.map(p => [p.id, [p.cxPt, p.cyPt, p.angle, p.scale]])) }; });
    const [cA, cB, cC, cD] = S0.charms;
    for (let i = 0; i < 5; i++) { const p = S0.at[S0.charms[i]]; assert(Math.abs(p[0] - L[i][0]) < .6 && Math.abs(p[1] - L[i][1]) < .6 && p[2] === 0, `charm ${i} at its spot: ${p}`); }
    console.log(`ok   a 10K sheet made by the page (${S0.id}): five squares nested, verified and saved, laid out at known spots`);
    // three more records: a Gold-filled sheet, a 10K sheet held by no run here, a 14K sheet with its green line (records only: the Library's view of them)
    const rec = (id, metal, extra) => Object.assign({ id, metal, sheetIndex: 1, day: '2026-10-18', status: 'complete', placements: [{ id: 'z1', cxPt: 40, cyPt: 40, angle: 0, wPt: 20, hPt: 20 }], charms: [{ id: 'z1', name: 'Z' }], poolIds: [], orders: [], stock: { wPt: 288, hPt: 180 }, verification: { ok: true } }, extra || {});
    st.put(SHEETS, 'gold-mc-gf', rec('gold-mc-gf', 'gold')); st.put(SHEETS, 'gold10k-mc-away', rec('gold10k-mc-away', 'gold10k')); st.put(SHEETS, 'gold14k-mc-lined', rec('gold14k-mc-lined', 'gold14k', { rosePlanHash: 'h1' }));

    /* ── 1 · the switch ── */
    const sw = () => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b ? { hidden: b.hidden, pressed: b.getAttribute('aria-pressed'), disabled: b.getAttribute('aria-disabled'), why: b.dataset.why || '', text: b.innerText.trim() } : null; });
    const openSheet = async id => { await page.evaluate(id => SheetWin.open(id), id); await until(() => page.evaluate(id => { const W = SheetWin._W; return W.id === id && !!W.rec && W.geom && !W.flying; }, id), 30000, 'the sheet window on ' + id); await page.waitForTimeout(300); };
    await openSheet('gold-mc-gf');
    assert.equal((await sw()).hidden, true, 'no Manual Control on a Gold-filled sheet');
    await openSheet('gold10k-mc-away');
    let s = await sw(); assert.equal(s.hidden, false); assert.equal(s.disabled, 'true'); assert.match(s.why, /run is not open in this sorter/);
    await openSheet('gold14k-mc-lined');
    s = await sw(); assert.equal(s.hidden, false); assert.equal(s.disabled, 'true');
    await openSheet(S0.id);
    s = await sw(); assert.equal(s.hidden, false, 'shown on the 10K sheet'); assert.equal(s.text, 'Manual Control'); assert.equal(s.disabled, 'false', 'it can start: ' + s.why); assert.equal(s.pressed, 'false');
    // the guards, each as the sheet would stand (the switch follows within a beat)
    const guard = async (set, re, what) => {
      await page.evaluate(set => { const sh = CN.S.sheets.gold10k.pages.at(-1); window.__was = {}; for (const [k, v] of Object.entries(set)) { window.__was[k] = sh[k]; sh[k] = v; } }, set);
      const g = await until(async () => { const v = await sw(); return v.disabled === 'true' && re.test(v.why) ? v : null; }, 5000, 'the switch says ' + what);
      await page.click('.sheetWin [data-r=manual]', { force: true }); await page.waitForTimeout(150);
      assert.equal((await page.evaluate(() => CNManual._MC.on)), false, 'it does not start when ' + what);
      assert.match(await page.evaluate(() => [...document.querySelectorAll('.sheetWin .mNote')].map(n => n.textContent).join(' | ')), re, 'a press says why in the window: ' + what);
      await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1); for (const [k, v] of Object.entries(window.__was)) { if (v === undefined) delete sh[k]; else sh[k] = v; } document.querySelectorAll('.sheetWin .mNote').forEach(n => n.remove()); });
      await until(async () => (await sw()).disabled === 'false', 5000, 'the switch free again after ' + what);
      return g.why;
    };
    const why = [];
    why.push(await guard({ rosePlanHash: 'plan-1' }, /already has its green cut line/, 'the sheet has its green line'));
    why.push(await guard({ laserDoneAt: Date.now() }, /cut on the laser/, 'the sheet was cut'));
    why.push(await guard({ roseCutAt: Date.now() }, /green line has been cut/, 'its line was cut'));
    why.push(await guard({ processReady: true }, /approved for laser cutting/, 'its set is approved'));
    why.push(await guard({ status: 'nesting' }, /being nested or saved/, 'it is being nested'));
    why.push(await guard({ status: 'queued' }, /being nested or saved/, 'it is queued to nest'));
    why.push(await guard({ status: 'finishing' }, /being nested or saved/, 'it is finishing'));
    assert.deepEqual(await page.evaluate(() => [CNManual.available('gold14k-nope').ok, CNManual.available(CN.S.sheets.gold10k.pages.at(-1)).ok]), [false, true]);
    console.log('ok   1  the switch: hidden on GF, shown on 10K/14K; refuses (and says why) when ' + why.map(w => `"${w.slice(0, 48)}…"`).join(', '));

    /* ── 2 · on: select, move, turn, resize ── */
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 10000, 'Manual Control on');
    s = await sw(); assert.equal(s.pressed, 'true');
    const claim = st.doc(SHEETS, S0.id).manualEdit; assert(claim && claim.by === 'Tester' && claim.tab, 'the sheet is claimed on its record: ' + JSON.stringify(claim));
    assert.equal(await page.evaluate(() => CN.S.sheets.gold10k.pages.at(-1)._manualHold), true, 'the run holds it meanwhile');
    assert.equal(await page.evaluate(() => !!(window.LiveNest && LiveNest.closed && LiveNest.closed(CN.S.sheets.gold10k.pages.at(-1)))), true, 'no arrival goes onto it meanwhile (LiveNest.closed)');
    assert.equal(await page.evaluate(id => CNManual.isOn(id) && CNManual.isOn(CN.S.sheets.gold10k.pages.at(-1)) && !CNManual.isOn('other'), S0.id), true, 'CNManual.isOn');
    const at = (x, y) => page.evaluate(([x, y]) => CNManual._client(x, y), [x, y]);
    const drag = async (from, to, o = {}) => { await page.mouse.move(from[0], from[1]); await page.mouse.down(); if (o.shift) await page.keyboard.down('Shift'); const n = o.steps || 8; for (let i = 1; i <= n; i++) await page.mouse.move(from[0] + (to[0] - from[0]) * i / n, from[1] + (to[1] - from[1]) * i / n); await page.waitForTimeout(80); if (o.before) await o.before(); await page.mouse.up(); if (o.shift) await page.keyboard.up('Shift'); await page.waitForTimeout(150); };
    const pose = id => page.evaluate(id => { const M = CNManual._MC; return M.edits.get(id) || M.from.get(id); }, id);
    // (NOMCBAR, 19 Oct 2026: the bar under the sheet is gone and stays hidden; what it said is a toast, the last one Manual Control said is MC.msg)
    const bar = () => page.evaluate(() => [CNManual._MC.msg || '', ...[...document.querySelectorAll('#toasts > *')].map(t => t.innerText)].join('\n'));
    const barHidden = () => page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=mcBar]'); return !b || b.hidden || !b.offsetParent; });
    // move B from (90, 40) to (120, 70)
    await drag(await at(90, 40), await at(120, 70));
    let q = await pose(cB); assert(Math.abs(q.cxPt - 120) < .8 && Math.abs(q.cyPt - 70) < .8 && q.angle === 0, 'B moved: ' + JSON.stringify(q));
    assert.equal(await page.evaluate(() => CNManual._MC.sel && CNManual._MC.sel.id), cB, 'B is in hand');
    assert.match(await bar(), /Placed/);
    assert.equal(await barHidden(), true, 'no Manual Control bar under the sheet (NOMCBAR)');
    // turn it with the knob: a quarter turn snaps to 90°; with Shift, 37° stays 37° (by the degree)
    const turn = async (deg, o) => { const t = await page.evaluate(() => CNManual._tool()), c = t.centre, k = t.knob, a0 = Math.atan2(k[1] - c[1], k[0] - c[0]), r = Math.hypot(k[0] - c[0], k[1] - c[1]), a1 = a0 + deg * Math.PI / 180;
      await drag(k, [c[0] + r * Math.cos(a1), c[1] + r * Math.sin(a1)], Object.assign({ steps: 12 }, o)); };
    await turn(88);
    q = await pose(cB); assert.equal(q.angle, 90, 'a quarter turn snaps to 90°: ' + q.angle);
    await turn(-90 + 37 - 1.6, { shift: true });
    q = await pose(cB); assert(Math.abs(q.angle - 35) <= 1, 'with Shift it turns by the degree: ' + q.angle); assert.notEqual(q.angle % 15, 0, 'not a 15° step: ' + q.angle);
    const turned = q.angle;
    // resize from a corner: 10 % bigger, then as far as it goes (120 %), then back down to 105 %
    const corner = async f => { const t = await page.evaluate(() => CNManual._tool()), c = t.centre, k = t.corners[2]; await drag(k, [c[0] + (k[0] - c[0]) * f, c[1] + (k[1] - c[1]) * f], { steps: 10 }); };
    await corner(1.1); q = await pose(cB); assert(Math.abs(q.size - 1.1) <= .011, 'a corner resizes it: ' + q.size); assert.equal(q.angle, turned, 'the turn kept');
    await corner(2); q = await pose(cB); assert.equal(q.size, 1.2, 'at most 120 %: ' + q.size);
    await corner(1.05 / 1.2); q = await pose(cB); assert(Math.abs(q.size - 1.05) <= .011, 'down to 105 %: ' + q.size);
    const sizeB = q.size, poseB = q;
    assert.equal(Math.round((await pose(cB)).size * 100), Math.round(sizeB * 100), 'its size is kept (the bar that read it is gone)');
    console.log(`ok   2  on: claimed and held; B moved to (${poseB.cxPt}, ${poseB.cyPt}), a quarter turn snapped to 90°, Shift turned it to ${turned}°, a corner sized it 110 % → 120 % (the most) → ${Math.round(sizeB * 100)} %`);
    if (process.env.SHOT_DIR) await page.screenshot({ path: path.join(process.env.SHOT_DIR, 'manual-on.png') });

    /* ── 2c · SIZECHIP and NUDGE (Paul, 27 Oct 2026): the size against the original beside the charm in hand, ↺ back to 100 %; the arrows 0.05 mm, Shift 1 mm ── */
    {
      const chip = () => page.evaluate(() => { const n = document.querySelector('.sheetWin [data-r=mcSize]'); if (!n || n.hidden || !n.offsetParent) return null; const r = n.getBoundingClientRect(), t = CNManual._tool(), fr = t.raw.corners, xs = fr.map(p => p[0]), ys = fr.map(p => p[1]);
        return { text: n.querySelector('[data-mcsz=v]').textContent, reset: !n.querySelector('[data-mcsz=reset]').hidden, title: n.title, r: [r.left, r.top, r.right, r.bottom], frame: [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)] }; });
      let c = await chip();
      assert(c && c.text === '+5%' && c.reset && /105% of its original size/.test(c.title), 'B in hand at 105 %: the chip says +5% with its ↺: ' + JSON.stringify(c));
      assert(c.r[0] >= c.frame[2] + 4 || c.r[2] <= c.frame[0] - 4, 'beside the frame, never over the charm: ' + JSON.stringify(c));
      // another charm taken (at 100 %: no chip), then B again: the chip is back
      const aP = S0.at[cA]; await page.mouse.click(...await at(aP[0], aP[1])); await page.waitForTimeout(150);
      assert.equal(await page.evaluate(() => CNManual._MC.sel && CNManual._MC.sel.id), cA, 'A in hand'); assert.equal(await chip(), null, 'A is at its original size: no chip');
      const bP = await pose(cB); await page.mouse.click(...await at(bP.cxPt, bP.cyPt)); await page.waitForTimeout(150);
      assert.equal(await page.evaluate(() => CNManual._MC.sel && CNManual._MC.sel.id), cB, 'B in hand again'); c = await chip(); assert(c && c.text === '+5%', 'the chip is there again for B: ' + JSON.stringify(c));
      // ↺: back to 100 %, the chip goes; + five times: 105 % again (with the arrows below, the keys reach the charm after a press on it: the plate has the focus)
      await page.click('.sheetWin [data-r=mcSize] [data-mcsz=reset]'); await page.waitForTimeout(150);
      q = await pose(cB); assert.equal(q.size, 1, '↺ puts B back to its original size: ' + q.size); assert.equal(await chip(), null, 'and the chip goes');
      for (let i = 0; i < 5; i++) await page.keyboard.press('Equal');
      q = await pose(cB); assert(Math.abs(q.size - 1.05) < 1e-9, '+ five times: 105 % again: ' + q.size); c = await chip(); assert(c && c.text === '+5%', 'and the chip: ' + JSON.stringify(c));
      // the arrows: 0.05 mm, Shift 1 mm, exactly (never moved to a grid cell)
      const MM = 72 / 25.4, p0 = await pose(cB);
      await page.keyboard.press('ArrowRight'); q = await pose(cB); assert(Math.abs(q.cxPt - (p0.cxPt + 0.05 * MM)) < 1e-6 && q.cyPt === p0.cyPt, '→ moves it 0.05 mm: ' + (q.cxPt - p0.cxPt));
      await page.keyboard.press('Shift+ArrowDown'); q = await pose(cB); assert(Math.abs(q.cyPt - (p0.cyPt + MM)) < 1e-6, 'Shift + ↓ moves it 1 mm: ' + (q.cyPt - p0.cyPt));
      await page.keyboard.press('ArrowLeft'); await page.keyboard.press('Shift+ArrowUp'); q = await pose(cB);
      assert(Math.abs(q.cxPt - p0.cxPt) < 1e-6 && Math.abs(q.cyPt - p0.cyPt) < 1e-6 && q.size === p0.size && q.angle === p0.angle, 'and back exactly: ' + JSON.stringify([q, p0]));
      poseB.cxPt = q.cxPt; poseB.cyPt = q.cyPt;
      console.log(`ok   2c the size chip beside B ("${c.text}", outside its frame), gone for a charm at 100 % and back for B, ↺ to 100 % and + to 105 % again; → 0.05 mm, Shift 1 mm, exact`);
    }

    /* ── 2b · MCFREE: the handles at the sheet's top-right corner, always there to take ── */
    const cE = S0.charms[4], e0 = await pose(cE);
    const toolNow = () => page.evaluate(() => CNManual._tool());
    const allIn = async (T, what) => {
      const vp = page.viewportSize(), pts = [...T.corners, T.knob];
      for (const [i, p] of pts.entries()) {
        assert(p[0] >= T.vis.left + 4 && p[0] <= T.vis.right - 4 && p[1] >= T.vis.top + 4 && p[1] <= T.vis.bottom - 4, `${what}: handle ${i} inside the stage: ${p} in ${JSON.stringify(T.vis)}`);
        assert(p[0] >= 0 && p[1] >= 0 && p[0] <= vp.width && p[1] <= vp.height, `${what}: handle ${i} on the screen: ${p}`);
        // drawn there: the stage's tool layer has ink under it, and a press there takes that handle
        const ink = await page.evaluate(([x, y]) => { const cv = document.querySelector('.sheetWin .swMcTool'), r = cv.getBoundingClientRect(), k = cv.width / r.width; return cv.getContext('2d').getImageData(Math.round((x - r.left) * k), Math.round((y - r.top) * k), 1, 1).data[3]; }, p);
        assert(ink > 0, `${what}: handle ${i} is drawn at ${p}`);
        assert.equal(await page.evaluate(([x, y]) => CNManual.handleAt(x, y), p), i < 4 ? 'size' : 'turn', `${what}: handle ${i} is taken where it is drawn`);
      }
    };
    await drag(await at(230, 45), await at(276, 12), { steps: 14 });
    q = await pose(cE); assert(Math.abs(q.cxPt - 276) < .8 && Math.abs(q.cyPt - 12) < .8, 'E at the top-right corner: ' + JSON.stringify(q));
    let T = await toolNow(); assert(T, 'E is in hand');
    await allIn(T, 'top-right corner');
    const pl = T.plate, cutOff = [...T.raw.corners, T.raw.knob].filter(p => p[0] + 4 > pl.right || p[0] - 4 < pl.left || p[1] - 4 < pl.top || p[1] + 4 > pl.bottom).length;
    assert(cutOff >= 1, 'at least one handle would have been cut off by the plate: ' + JSON.stringify({ raw: T.raw, pl }));
    if (process.env.SHOT_DIR) await page.screenshot({ path: path.join(process.env.SHOT_DIR, 'manual-corner.png') });
    await turn(88);
    q = await pose(cE); assert.equal(q.angle, 90, 'the knob, taken where it is drawn, turns E: ' + JSON.stringify(q));
    assert(Math.abs(q.cxPt - 276) < .8 && Math.abs(q.cyPt - 12) < .8, 'turned where it stands');
    // another window size (the plate drawn at another scale): every handle still inside what can be seen
    const sizes = [];
    for (const vp of [{ width: 1100, height: 720 }, { width: 1680, height: 1050 }]) {
      await page.setViewportSize(vp); await page.waitForTimeout(500);
      T = await toolNow(); await allIn(T, `${vp.width}×${vp.height}`); sizes.push(`${vp.width}×${vp.height}`);
    }
    await page.setViewportSize({ width: 1440, height: 950 }); await page.waitForTimeout(500);
    // Ctrl+Z: back where it stood when Manual Control was turned on (nothing kept for E)
    await page.keyboard.press('Control+z'); await page.waitForTimeout(150);
    q = await pose(cE); assert.deepEqual([q.cxPt, q.cyPt, q.angle], [e0.cxPt, e0.cyPt, e0.angle], 'E put back');
    assert.equal(await page.evaluate(id => CNManual._MC.edits.has(id), cE), false);
    console.log(`ok   2b MCFREE: at the top-right corner every handle (4 corners + the knob) is drawn and taken inside the stage (${cutOff} would have been cut off by the plate), the knob turned E to 90°; the same at ${sizes.join(' and ')}; Ctrl+Z put it back`);

    /* ── 3 · MCFREE: onto another charm: clay while there, and it stays there, red; moved clear, green ── */
    const c0 = await pose(cC);
    const warn = () => page.evaluate(() => { const n = document.querySelector('.sheetWin [data-r=mcWarn]'); return n && !n.hidden && n.offsetParent ? n.innerText.replace(/\s+/g, ' ').trim() : ''; });
    assert.equal(await page.evaluate(() => CNManual._MC.bad.size), 0, 'the nested sheet is all clear (green) as Manual Control starts');
    assert.equal(await warn(), '', 'no overlap note while all is clear');
    let during = null;
    await drag(await at(40, 95), await at(46, 48), { before: async () => { during = await page.evaluate(() => { const d = CNManual._MC.drag; return d && d.res ? { ok: d.res.ok, why: d.res.why } : null; }); } });
    assert(during && during.ok === false && during.why === 'overlap', 'while it lies over A it is clay: ' + JSON.stringify(during));
    q = await pose(cC); assert(Math.abs(q.cxPt - 46) < .8 && Math.abs(q.cyPt - 48) < .8, 'let go there, C STAYS where it was put: ' + JSON.stringify(q));
    assert.equal(await page.evaluate(id => CNManual._MC.edits.has(id), cC), true, 'kept as a move of C');
    assert.deepEqual(await page.evaluate(([a, c]) => [CNManual._MC.bad.has(c), CNManual._MC.bad.has(a)], [cA, cC]), [true, true], 'C and A are red');
    assert.equal(await page.evaluate(id => CNManual.conflicts(id).n, S0.id), 2, 'CNManual.conflicts says 2');
    let w = await warn(); assert.match(w, /2 charms overlap/, 'the stage says so: ' + w);
    assert.match(await bar(), /Placed where you put it, but it overlaps order|Placed where you put it, but it overlaps another charm/, 'the toast says it overlaps: ' + await bar());
    if (process.env.SHOT_DIR) await page.screenshot({ path: path.join(process.env.SHOT_DIR, 'manual-overlap.png') });
    const warnSeen = w;
    // moved clear (pressed where only C lies): green again, the note gone
    await drag(await at(46, 54), await at(40, 101), { steps: 10 });
    q = await pose(cC); assert(Math.abs(q.cxPt - c0.cxPt) < .8 && Math.abs(q.cyPt - c0.cyPt) < .8, 'C moved clear: ' + JSON.stringify(q));
    assert.equal(await page.evaluate(() => CNManual._MC.bad.size), 0, 'nothing red once C is clear');
    assert.equal(await warn(), '', 'the overlap note is gone');
    assert.match(await bar(), /Back where it was|Placed: it fits there/);
    console.log(`ok   3  MCFREE: dragged onto A, clay while there; let go, C stays there, C and A red, the stage says "${warnSeen}"; moved clear, green again and the note gone`);

    /* ── 4 · edge to edge: touching is allowed, 3 pt into it is not ── */
    const fit = (id, x, y, a) => page.evaluate(([sid, id, x, y, a]) => CNManual.fits(sid, id, { cxPt: x, cyPt: y, angle: a }), [S0.id, id, x, y, a]);
    const aAt = S0.at[cA];
    let f = await fit(cC, aAt[0] + 20, aAt[1], 0); assert.equal(f.ok, true, 'C right against A (outlines touching): ' + f.reason); assert(Math.abs(f.pose.cxPt - (aAt[0] + 20)) <= .5, 'at the cell asked for: ' + f.pose.cxPt);
    f = await fit(cC, aAt[0] + 17, aAt[1], 0); assert.equal(f.ok, false); assert.equal(f.why, 'overlap'); assert.match(f.reason, /overlap/);
    f = await fit(cC, aAt[0], aAt[1] + 20, 0); assert.equal(f.ok, true, 'C right under A: ' + f.reason);
    f = await fit(cC, 2, 40, 0); assert.equal(f.ok, false); assert.equal(f.why, 'edge', 'off the sheet: ' + f.why);
    // (ZEROGAP, Paul 27 Oct 2026: no margin from the sheet's edge: right against it fits, a hair past it does not)
    f = await fit(cC, 10.6, aAt[1], 0); assert.equal(f.ok, true, 'C right against the sheet\'s left edge (no margin): ' + f.reason); assert(Math.abs(f.pose.cxPt - 10.6) < 1e-9, 'exactly where asked: ' + f.pose.cxPt);
    f = await fit(cC, 9.5, aAt[1], 0); assert.equal(f.ok, false, 'a hair past the edge'); assert.equal(f.why, 'edge', 'past the edge: ' + f.why);
    f = await page.evaluate(([sid, id]) => CNManual.fits(sid, id, { cxPt: 150, cyPt: 150, angle: 0, size: 1.3 }), [S0.id, cC]); assert.equal(f.why, 'size');
    // the same through the tool: C dragged to touch A's right edge stays there
    await drag(await at(40, 95), await at(aAt[0] + 20, aAt[1]), { steps: 14 });
    q = await pose(cC); assert(Math.abs(q.cxPt - (aAt[0] + 20)) <= .8 && Math.abs(q.cyPt - aAt[1]) <= .8, 'C placed edge to edge with A: ' + JSON.stringify(q));
    const poseC = q;
    // the sheet hop's way in (charm-nest-sheet-hop.js): live page and charm objects, a pose in sheet points; and its cancelDrag()
    const objFit = await page.evaluate(([cid, x, y]) => { const sh = CN.S.sheets.gold10k.pages.at(-1), c = sh.charms.find(z => z.id === cid); return [CNManual.isOn(sh), CNManual.fits(sh, c, { cxPt: x, cyPt: y, angle: 0 }), CNManual.fits(sh, c, { cxPt: x - 3, cyPt: y, angle: 0 })].map(r => typeof r === 'object' ? { ok: r.ok, reason: r.reason } : r); }, [cD, 230, 100]);
    assert.equal(objFit[0], true); assert.equal(objFit[1].ok, true, 'fits(page, charm, pose) with live objects: ' + objFit[1].reason); assert.equal(objFit[2].ok, true);
    const d0 = await pose(cD), dAt = await at(170, 100), dTo = await at(110, 100);
    await page.mouse.move(dAt[0], dAt[1]); await page.mouse.down(); for (let i = 1; i <= 6; i++) await page.mouse.move(dAt[0] + (dTo[0] - dAt[0]) * i / 6, dAt[1]);
    const cancelled = await page.evaluate(() => [CNManual.cancelDrag(), !!CNManual._MC.drag, CNManual.cancelDrag()]);
    await page.mouse.up(); await page.waitForTimeout(150);
    assert.deepEqual(cancelled, [true, false, false], 'cancelDrag stops the drag once: ' + cancelled);
    q = await pose(cD); assert.deepEqual([q.cxPt, q.cyPt, q.angle], [d0.cxPt, d0.cyPt, d0.angle], 'after cancelDrag D is back where it stood, nothing kept');
    assert.equal(await page.evaluate(id => CNManual._MC.edits.has(id), cD), false);
    // the run's own nest waits while Manual Control holds the sheet (it is asked for again once it is off)
    assert.deepEqual(await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1), st0 = sh.status; CN.startNest(sh); const r = [st0, sh.status, !!sh._manualAsked]; delete sh._manualAsked; return r; }), ['complete', 'complete', true], 'startNest waits under the hold');
    console.log(`ok   4  fits(page, charm, pose) with live objects, cancelDrag() springs D back; edge to edge with A allowed (C at ${poseC.cxPt}, ${poseC.cyPt}; fits right of A and under A), 3 pt into A refused ("overlap"), off the sheet refused ("edge"), 130 % refused ("size")`);

    /* ── 6 · a second computer, while this one holds the sheet ── */
    const B = await mkCtx('Other Person');
    await B.page.evaluate(id => SheetWin.open(id), S0.id);
    await until(() => B.page.evaluate(id => { const W = SheetWin._W; return W.id === id && !!W.rec; }, S0.id), 30000, 'the second computer shows the sheet');
    const bsw = await until(() => B.page.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b && !b.hidden && b.dataset.why ? b.dataset.why : null; }), 8000, 'the second computer\'s switch');
    assert.match(bsw, /Tester is moving this sheet's charms by hand on another screen/, 'the second computer is told who holds it: ' + bsw);
    const refused = await B.page.evaluate(id => CN.api('charmNestLibrary', { op: 'manualClaim', id, tab: 'other-tab-1', by: 'Other Person' }, { quiet: true }), S0.id);
    assert.equal(refused.ok, false); assert.equal(refused.held.by, 'Tester', 'the server refuses a second claim');
    console.log('ok   6  a second computer: "' + bsw.slice(0, 70) + '…", and the server refuses its claim');

    /* ── 5 · off: saved through the real nest ── */
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'Manual Control off');
    const done = await until(() => page.evaluate(() => { const w = SheetWin._W.work; return w && w.state !== 'working' && !SheetWin._W.flow ? { state: w.state, title: w.title, note: w.note, steps: w.steps.map(s => [s.text, s.state, s.detail]) } : null; }), 180000, 'the save to end');
    assert.equal(done.state, 'done', 'saved: ' + JSON.stringify(done));
    const live = await page.evaluate(([b, c]) => { const sh = CN.S.sheets.gold10k.pages.at(-1), P = id => sh.placements.find(p => p.id === id), C = id => sh.charms.find(x => x.id === id);
      return { b: P(b), c: P(c), pinB: C(b).pinned, pinC: C(c).pinned, arrB: !!C(b).arrivalPin, manB: C(b).manual, manC: C(c).manual, arrA: !!sh.charms[0].arrivalPin, manA: sh.charms[0].manual || null, ok: sh.verification && sh.verification.ok, hold: !!sh._manualHold, status: sh.status, saved: sh.persistedDone }; }, [cB, cC]);
    const near = (p, q) => Math.abs(p.cxPt - q.cxPt) < 1e-6 && Math.abs(p.cyPt - q.cyPt) < 1e-6 && Math.abs(p.angle - q.angle) < 1e-6;
    assert(near(live.b, poseB), `B saved exactly where it was put: ${JSON.stringify(live.b)} vs ${JSON.stringify(poseB)}`);
    assert(near(live.c, poseC), 'C saved exactly where it was put');
    assert.equal(+live.b.scale.toFixed(5), +(0.971 * sizeB).toFixed(5), 'B written at its own size (the shrink of 105 %): ' + live.b.scale);
    assert.equal(live.c.scale, 0.971, 'C at its design size (the 2.9 % shrink)');
    assert(live.pinB && !live.arrB && live.pinB.size === sizeB, 'B pinned by a person, with its size: ' + JSON.stringify(live.pinB));
    assert.equal(live.manB.by, 'Tester'); assert.equal(live.manB.size, sizeB); assert.equal(live.manC.by, 'Tester'); assert.equal(live.manA, null, 'A was not touched');
    assert.equal(live.ok, true, 'the sheet verified'); assert.equal(live.hold, false, 'let go');
    assert.equal(await page.evaluate(() => !!(window.LiveNest && LiveNest.closed && LiveNest.closed(CN.S.sheets.gold10k.pages.at(-1)))), false, 'open to arrivals again once let go (LiveNest.closed keeps its whole rule)'); assert.equal(live.status, 'complete'); assert.equal(live.saved, true);
    const r1 = st.doc(SHEETS, S0.id), rp = id => r1.placements.find(p => p.id === id), rc = id => r1.charms.find(c => c.id === id);
    assert(near(rp(cB), poseB) && near(rp(cC), poseC), 'the record holds the new layout');
    assert.equal(rc(cB).manual.by, 'Tester'); assert.equal(rc(cB).manual.size, sizeB, 'the record\'s charm keeps its size');
    assert(r1.manual && r1.manual.by === 'Tester' && r1.manual.n === 2, 'the record names who moved what: ' + JSON.stringify(r1.manual));
    assert.equal(r1.manualEdit, null, 'the claim was given back');
    assert(r1.verification && r1.verification.ok, 'the record says it verified');
    const tl = await page.evaluate(() => window.__tl.filter(e => e.type === 'moved' && e.data && e.data.kind === 'manual'));
    const log = await page.evaluate(() => CN.S.sheets.gold10k.pages.at(-1).log.filter(l => /moved by hand/.test(l)));
    assert(log.length && /Tester/.test(log[0]), 'the sheet log says who: ' + log);
    console.log(`ok   5  off: "${done.title}" — B and C pinned where they were put (B at ${Math.round(sizeB * 100)} %, scale ${live.b.scale}), verified, the record holds it with manual {by: Tester, n: 2}, the claim given back; sheet log: "${log[0].replace(/^\S+ /, '')}"${tl.length ? `; ${tl.length} timeline step(s)` : ''}`);
    // the second computer reads the new layout, and may start now
    await B.page.evaluate(id => SheetWin.open(id), S0.id);
    const bview = await until(() => B.page.evaluate(([id, b]) => { const W = SheetWin._W, x = W.rec && W.id === id && W.pieces.find(p => p.id === b); const s = document.querySelector('.sheetWin [data-r=manual]'); return x && s && !/another screen/.test(s.dataset.why || '') ? { at: [x.p.cxPt, x.p.cyPt, x.p.angle, x.p.scale], why: s.dataset.why } : null; }, [S0.id, cB]), 15000, 'the second computer\'s view');
    assert(Math.abs(bview.at[0] - poseB.cxPt) < 1e-6 && Math.abs(bview.at[2] - poseB.angle) < 1e-6, 'the second computer draws B where it was put: ' + bview.at);
    assert.match(bview.why, /run is not open in this sorter/, 'and is no longer told another screen holds it');
    await B.ctx.close();

    // the orders' timelines: one "moved" step per order whose pieces changed, under the person signed in (SheetEvents.adjusted, as the save calls it)
    const steps = await page.evaluate(() => { window.__tl = []; const sh = { sheetId: 'gold10k-tl', metal: 'gold10k', page: 1, sheetIndex: 2, setId: null, draft: true, charms: [{ id: 'a', poolId: '4205028216_51_1', order: '4205028216', lineKey: '4205028216_51' }, { id: 'b', poolId: '4205028216_51_2', order: '4205028216', lineKey: '4205028216_51' }, { id: 'c', poolId: '4205028217_52_1', order: '4205028217', lineKey: '4205028217_52' }] };
      SheetEvents.adjusted(sh, ['a', 'b'], 'Tester'); return new Promise(r => setTimeout(() => r(window.__tl.filter(e => e.type === 'moved')), 2500)); });
    assert.equal(steps.length, 1, 'one step, for the one order that moved: ' + JSON.stringify(steps));
    assert.equal(steps[0].orderId, '4205028216'); assert.equal(steps[0].by, 'Tester'); assert.equal(steps[0].data.kind, 'manual'); assert.match(steps[0].text, /2 of its charms were moved by hand \(Manual Control\)/);
    console.log(`ok   5b the order's timeline: "${steps[0].text}" by ${steps[0].by}`);

    /* ── 8 · MCFREE: left overlapping, saved there; the sheet flagged until it is moved clear ── */
    await until(settled, 60000, 'the sheet at rest before Manual Control again'); await page.waitForTimeout(1500);
    const mcOn = async () => { await openSheet(S0.id); await until(async () => (await sw()).disabled === 'false', 10000, 'the switch free'); await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => CNManual._MC.on), 10000, 'Manual Control on'); };
    const mcOffSaved = async what => { await page.evaluate(() => { window.__w0 = SheetWin._W.work; }); await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'Manual Control off');
      return until(() => page.evaluate(() => { const w = SheetWin._W.work; return w && w !== window.__w0 && /moved by hand/.test(w.title) && w.state !== 'working' && !SheetWin._W.flow ? { state: w.state, title: w.title, note: w.note } : null; }), 180000, what); };
    await mcOn();
    assert.equal(await page.evaluate(() => CNManual._MC.bad.size), 0, 'clear as it starts');
    await drag(await at(170, 100), await at(233, 52), { steps: 14 });
    const poseD = await pose(cD); assert(Math.abs(poseD.cxPt - 233) < .8 && Math.abs(poseD.cyPt - 52) < .8, 'D stays on E: ' + JSON.stringify(poseD));
    assert.deepEqual(await page.evaluate(([d, e]) => [CNManual._MC.bad.has(d), CNManual._MC.bad.has(e)], [cD, cE]), [true, true], 'D and E red');
    const saved8 = await mcOffSaved('the overlapping save to end');
    assert.equal(saved8.state, 'done', 'saved, overlapping: ' + JSON.stringify(saved8)); assert.match(saved8.note, /2 charms overlap/, 'the save says so: ' + saved8.note);
    const f8 = await page.evaluate(([d, sid]) => { const sh = CN.S.sheets.gold10k.pages.at(-1), p = sh.placements.find(z => z.id === d), c = sh.charms.find(z => z.id === d);
      const set = { setId: 'mcfree-probe', runId: sh.runId, sheetIds: [sh.sheetId], name: 'Probe set', group: 'dispatch' }, was = sh.setId; sh.setId = set.setId; let gate = null; try { gate = Sets.releaseIssue(set); } catch (e) { gate = 'threw: ' + e.message; } finally { if (was === undefined) delete sh.setId; else sh.setId = was; }
      return { p, pinned: c.pinned, by: c.manual && c.manual.by, n: CNManual.conflicts(sid).n, ok: sh.verification && sh.verification.ok, card: sh.el ? sh.el.innerText.replace(/\s+/g, ' ') : '', gate, n5: sh.placements.length }; }, [cD, S0.id]);
    assert(Math.abs(f8.p.cxPt - poseD.cxPt) < 1e-6 && Math.abs(f8.p.cyPt - poseD.cyPt) < 1e-6, 'D written exactly where it was put, over E: ' + JSON.stringify(f8.p));
    assert.equal(f8.n5, 5, 'no charm lifted off the sheet'); assert.equal(f8.by, 'Tester');
    assert.equal(f8.n, 2, 'CNManual.conflicts: 2 charms overlap'); assert.equal(f8.ok, false, "the sheet's own verifier flags it");
    assert.match(f8.card, /2 charms overlap/, "the sheet's card in the Nest tab says so: " + f8.card.slice(0, 300));
    assert.match(String(f8.gate), /2 charms overlap/, 'the Sets gate holds a set with it: ' + f8.gate);
    const r8 = st.doc(SHEETS, S0.id).placements.find(z => z.id === cD); assert(Math.abs(r8.cxPt - poseD.cxPt) < .01 && Math.abs(r8.cyPt - poseD.cyPt) < .01, 'the record holds D over E');
    await openSheet(S0.id);
    w = await until(async () => { const v = await warn(); return /2 charms overlap/.test(v) ? v : null; }, 8000, 'the stage note on the saved sheet');
    // Manual Control again: both red from the start; D moved clear: green, saved, verified
    await mcOn();
    assert.deepEqual(await page.evaluate(([d, e]) => [CNManual._MC.bad.has(d), CNManual._MC.bad.has(e)], [cD, cE]), [true, true], 'D and E red as it starts again');
    await drag(await at(233, 59), await at(170, 107), { steps: 14 });
    q = await pose(cD); assert(Math.abs(q.cxPt - 170) < .8 && Math.abs(q.cyPt - 100) < .8, 'D clear again: ' + JSON.stringify(q));
    assert.equal(await page.evaluate(() => CNManual._MC.bad.size), 0, 'all green');
    const clear8 = await mcOffSaved('the clear save to end');
    assert.equal(clear8.state, 'done', JSON.stringify(clear8));
    await until(settled, 120000, 'the sheet verified again once D is clear');
    assert.equal(await page.evaluate(id => CNManual.conflicts(id).n, S0.id), 0, 'no conflicts left');
    console.log(`ok   8  MCFREE: D left over E saved there (written as it stands, the sheet's verifier flagged it), card and stage: "${w}", the Sets gate: "${String(f8.gate).slice(0, 90)}…"; again on, both red from the start; moved clear, saved and verified`);

    /* ── 7 · a later nest from scratch: the moved charms stay ── */
    await until(settled, 60000, 'the sheet at rest before it is nested again'); await page.waitForTimeout(1500);   // (the save's last writes: its QR label, its backs)
    await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1); for (const c of sh.charms) if (!c.manual) { c.pinned = null; delete c.arrivalPin; } sh.placements = []; sh.dirty = true; sh.status = 'ready'; CN.startNest(sh); });
    await until(settled, 120000, 'the sheet nested again from scratch').catch(async e => { console.log('STATE', JSON.stringify(await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1); return { n: sh.charms.length, st: sh.status, stage: sh.stage, p: sh.problem, saved: sh.persistedDone, v: sh.verification && sh.verification.ok, dirty: sh.dirty, op: sh._operationStarting, hold: sh._manualHold, placed: sh.placements.length, rejects: sh.rejects, log: sh.log.slice(-8), id: sh.sheetId }; }))); throw e; });
    const again = await page.evaluate(([b, c]) => { const sh = CN.S.sheets.gold10k.pages.at(-1); return { b: sh.placements.find(p => p.id === b), c: sh.placements.find(p => p.id === c), n: sh.placements.length, ok: sh.verification.ok }; }, [cB, cC]);
    assert.equal(again.n, 5, 'every charm placed again'); assert.equal(again.ok, true);
    assert(near(again.b, poseB) && near(again.c, poseC), 'a nest from scratch keeps B and C exactly where the person put them: ' + JSON.stringify(again));
    assert.equal(+again.b.scale.toFixed(5), +(0.971 * sizeB).toFixed(5), 'and B at its size');
    const r2 = st.doc(SHEETS, S0.id); assert(near(r2.placements.find(p => p.id === cB), poseB), 'and so does its record');
    console.log('ok   7  a later nest from scratch (every other charm free): B and C stay exactly where they were put, B at its size, the sheet verified');

    assert.deepEqual(A.errors.concat(B.errors), [], 'no page errors: ' + A.errors.concat(B.errors).join(' | '));
    console.log('\nbrowser checks passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
