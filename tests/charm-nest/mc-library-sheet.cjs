// MCLIBRARY (Paul, 9 Nov 2026: he is not allowed to use "Manual Control" on 10K and 14K solid gold sheets. "Fix the below, I don't care if it starts cutting in sandbox ... do what you
// have to do to actually fix this. I can't have this problem."). Manual Control looked for the sheet only among the sheets of the run open on THIS screen: opened from the Library, on a
// station without that run, after the run was closed, or recalled onto the cards, it said "This sheet's run is not open in this sorter" and never started.
// In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary and charmNestOutput handlers over an in-memory store; nothing live). A 14K sheet is made by the page
// itself (five 20 pt squares nested, written, verified and saved by the real nest), then:
//   1  a problem left on the sheet (and a write owed) is settled by the press itself: written again as it stands, then Manual Control starts (one press, never a dead grey button)
//   2  a SECOND computer with no run open (the Library's view): Manual Control starts on the sheet loaded from its record, the claim taken on its record; a charm moved there
//   3  this computer, which holds the run, is told another screen holds it (one person at a time), and the server refuses its claim
//   4  off: saved through the server (sheetLayoutPatch), the record holds the new pose, pin and hand mark, the claim given back, no file owed, its .ai, proof, report rewritten at the
//      same paths and checked; the run's page here takes the new layout (mcAdopt)
//   5  a recalled card (its pieces not on the page, "rebuild it first" before): Manual Control starts from its record
//   6  a sheet cut on the laser: still refused, in those words, from the Library and by the server
//   node tests/charm-nest/mc-library-sheet.cjs     (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>)
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
  const mkCtx = async name => {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
    await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
      const u = r.request().url();
      if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
      if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
      if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await ctx.addInitScript(who => { try { localStorage.setItem('cn.employee', who); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => who; }, name);
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
  let B = null;
  try {
    /* ── the sheet, made by the page: five squares on 14K Gold, nested and saved ── */
    await page.evaluate(async b64 => {
      CN.S.settings.stock = CN.S.settings.stock || {}; CN.S.settings.stock.gold14k = [4, 2.5];
      const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'gold14k');
    }, pdf.toString('base64'));
    const settled = () => page.evaluate(() => { const sh = CN.S.sheets.gold14k.pages.at(-1); if (sh.charms.length === 5 && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === 5 && sh.status === 'complete' && sh.persistedDone && !!sh.verification && sh.verification.ok && !sh.dirty && !!sh.sheetId; });
    await until(settled, 120000, 'the 14K sheet nested, verified and saved');
    const L = [[40, 40], [90, 40], [40, 95], [170, 100], [230, 45]];
    await page.evaluate(L => { const sh = CN.S.sheets.gold14k.pages.at(-1); sh.charms.forEach((c, i) => { c.pinned = { cxPt: L[i][0], cyPt: L[i][1], angle: 0 }; c.arrivalPin = true; }); sh.placements = []; sh.dirty = true; sh.status = 'ready'; CN.startNest(sh); }, L);
    await until(settled, 120000, 'the sheet laid out at the known spots');
    const S0 = await page.evaluate(() => { const sh = CN.S.sheets.gold14k.pages.at(-1); return { id: sh.sheetId, charms: sh.charms.map(c => c.id), ai: null }; });
    const [, cB] = S0.charms;
    console.log(`ok   a 14K sheet made by the page (${S0.id}): five squares nested, verified and saved`);

    const sw = p => p.evaluate(() => { const b = document.querySelector('.sheetWin [data-r=manual]'); return b ? { hidden: b.hidden, pressed: b.getAttribute('aria-pressed'), disabled: b.getAttribute('aria-disabled'), why: b.dataset.why || '' } : null; });
    const openSheet = async (p, id) => { await p.evaluate(id => SheetWin.open(id), id); await until(() => p.evaluate(id => { const W = SheetWin._W; return W.id === id && !!W.rec && W.geom && !W.flying; }, id), 30000, 'the sheet window on ' + id); await p.waitForTimeout(300); };
    const free = p => until(async () => { const v = await sw(p); return v && !v.hidden && v.disabled === 'false' && !v.why ? v : null; }, 20000, 'Manual Control free to start').catch(async e => { throw new Error(e.message + ' · ' + JSON.stringify(await sw(p))); });
    const saveDone = p => until(() => p.evaluate(() => { const w = SheetWin._W.work; return w && w.state !== 'working' && !SheetWin._W.flow ? { state: w.state, title: w.title, note: w.note, steps: w.steps.map(s => [s.text, s.state, s.detail]) } : null; }), 180000, 'the save to end');

    /* ── 1 · a problem left on the sheet: the press writes it again as it stands, then Manual Control starts ── */
    await openSheet(page, S0.id);
    await free(page);
    await page.evaluate(() => { const sh = CN.S.sheets.gold14k.pages.at(-1); sh.problem = 'not saved to the cloud — the network went away'; sh._manualHold = true; });
    let s = await until(async () => { const v = await sw(page); return /has a problem/.test(v.why) ? v : null; }, 6000, 'the switch says the problem');
    assert.equal(s.disabled, 'false', 'a problem the press settles leaves the switch pressable: ' + JSON.stringify(s));
    assert.match(s.why, /Press Manual Control to write it again as it stands/);
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 60000, 'Manual Control on after the problem was settled');
    assert.equal(await page.evaluate(() => CN.S.sheets.gold14k.pages.at(-1).problem), null, 'the problem is gone');
    await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'off');
    // a write owed (dirty): the same
    await until(settled, 60000, 'at rest');
    await page.evaluate(() => { const sh = CN.S.sheets.gold14k.pages.at(-1); sh.dirty = true; });
    s = await until(async () => { const v = await sw(page); return /not written after its last change/.test(v.why) ? v : null; }, 6000, 'the switch says the write owed');
    assert.equal(s.disabled, 'false');
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 60000, 'Manual Control on after the owed write');
    await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'off');
    await until(settled, 60000, 'at rest again'); await page.waitForTimeout(800);
    console.log('ok   1  a problem left on the sheet, and a write owed: the switch stays pressable and says so in one line; one press writes it as it stands and Manual Control starts');

    /* ── 2 · a second computer with no run: the Library's view of the sheet ── */
    B = await mkCtx('Other Person');
    assert.equal(await B.page.evaluate(id => CN.allSheets().some(p => p.sheetId === id), S0.id), false, 'the second computer holds no run with this sheet');
    await openSheet(B.page, S0.id);
    s = await free(B.page);
    assert.equal(await B.page.evaluate(() => !!(CNManual._live() && CNManual._live()._standalone)), true, 'the sheet is loaded from its record');
    await B.page.click('.sheetWin [data-r=manual]');
    await until(() => B.page.evaluate(() => CNManual._MC.on), 10000, 'Manual Control on (from the Library)');
    await until(() => (st.doc(SHEETS, S0.id).manualEdit || {}).by === 'Other Person', 10000, 'the claim on the record');
    const at = (p, x, y) => p.evaluate(([x, y]) => CNManual._client(x, y), [x, y]);
    const drag = async (p, from, to) => { await p.mouse.move(from[0], from[1]); await p.mouse.down(); for (let i = 1; i <= 8; i++) await p.mouse.move(from[0] + (to[0] - from[0]) * i / 8, from[1] + (to[1] - from[1]) * i / 8); await p.waitForTimeout(80); await p.mouse.up(); await p.waitForTimeout(200); };
    const b0 = st.doc(SHEETS, S0.id).placements.find(z => z.id === cB);
    await drag(B.page, await at(B.page, b0.cxPt, b0.cyPt), await at(B.page, b0.cxPt + 30, b0.cyPt + 30));
    const poseB = await B.page.evaluate(id => CNManual._MC.edits.get(id), cB);
    assert(poseB && Math.abs(poseB.cxPt - (b0.cxPt + 30)) < 1 && Math.abs(poseB.cyPt - (b0.cyPt + 30)) < 1, 'B moved from the Library: ' + JSON.stringify(poseB));
    console.log(`ok   2  a computer with no run open: Manual Control starts on the sheet loaded from its record, the claim taken; B moved to (${poseB.cxPt.toFixed(2)}, ${poseB.cyPt.toFixed(2)})`);

    /* ── 3 · one person at a time ── */
    await openSheet(page, S0.id);
    const held = await until(async () => { const v = await sw(page); return /Other Person is moving this sheet's charms by hand on another screen/.test(v.why) ? v : null; }, 10000, 'this computer told who holds it');
    assert.equal(held.disabled, 'true');
    const refused = await page.evaluate(id => CN.api('charmNestLibrary', { op: 'manualClaim', id, tab: 'other-tab-1', by: 'Tester' }, { quiet: true }), S0.id);
    assert.equal(refused.ok, false); assert.equal(refused.held.by, 'Other Person');
    await page.evaluate(() => SheetWin.close && SheetWin.close());
    console.log('ok   3  the computer with the run is told "Other Person is moving this sheet\'s charms by hand on another screen", and the server refuses its claim');

    /* ── 4 · off: saved through the server, its files written again from its record ── */
    const filesBefore = st.calls.filter(c => c.name === 'charmNestOutput').length;
    await B.page.click('.sheetWin [data-r=manual]');
    await until(() => B.page.evaluate(() => !CNManual._MC.on), 5000, 'Manual Control off');
    const done = await saveDone(B.page);
    assert.equal(done.state, 'done', 'saved: ' + JSON.stringify(done));
    const r1 = st.doc(SHEETS, S0.id), rp = r1.placements.find(z => z.id === cB), rc = r1.charms.find(z => z.id === cB);
    assert(Math.abs(rp.cxPt - poseB.cxPt) < 1e-6 && Math.abs(rp.cyPt - poseB.cyPt) < 1e-6, 'the record holds B where it was put: ' + JSON.stringify(rp));
    assert.equal(rc.manual.by, 'Other Person'); assert(rc.pinned && Math.abs(rc.pinned.cxPt - poseB.cxPt) < 1e-6, 'pinned there');
    assert.equal(r1.manualEdit, null, 'the claim given back');
    await until(() => st.doc(SHEETS, S0.id).filesOwed == null, 60000, 'no file owed');
    const ups = st.calls.slice(filesBefore).filter(c => c.name === 'charmNestOutput').map(c => c.body && c.body.path).filter(Boolean);
    const aiPath = r1.outputs && r1.outputs.ai && r1.outputs.ai.path;
    assert(aiPath && ups.includes(aiPath), 'its .ai written again at the same path: ' + JSON.stringify({ aiPath, ups }));
    assert(ups.some(p => /_labelled\.pdf$/.test(p)) && ups.some(p => /_nest-report\.json$/.test(p)), 'its proof and report too: ' + ups.join(', '));
    const r2 = st.doc(SHEETS, S0.id);
    assert(r2.verification && r2.verification.ok, 'checked: the file re-rendered and every charm clear: ' + JSON.stringify(r2.verification));
    assert(done.steps.some(([t, stt, d]) => /files again/.test(t) && stt === 'ok' && /written and checked/.test(d || '')), 'the files step says it: ' + JSON.stringify(done.steps));
    // the run's page on the other computer takes the new layout (the feed names the sheet; here asked directly)
    const liveB = () => page.evaluate(id => { const sh = CN.S.sheets.gold14k.pages.at(-1), p = sh.placements.find(q => q.id === id), c = sh.charms.find(q => q.id === id); return { p, by: c.manual && c.manual.by, pin: c.pinned, arr: !!c.arrivalPin }; }, cB);
    const live = await until(async () => { await page.evaluate(id => CNManual._adopt(id), S0.id); const v = await liveB(); return v.by === 'Other Person' ? v : null; }, 20000, 'the run here takes the layout saved on the other computer');
    assert(Math.abs(live.p.cxPt - poseB.cxPt) < 1e-6 && Math.abs(live.p.cyPt - poseB.cyPt) < 1e-6 && live.pin && !live.arr, 'the run here holds B where it was put by hand elsewhere: ' + JSON.stringify(live));
    assert.equal(await page.evaluate(id => CNManual._adopt(id), S0.id), 0, 'taken once: nothing newer the second time');
    console.log(`ok   4  off: "${done.title}" — the record holds B (pinned, manual by Other Person), the claim given back, no file owed; ${new Set(ups).size} files written again at their paths (${aiPath.split('/').pop()}), checked; the run's page took the layout`);

    /* ── 5 · a recalled card: its pieces are not on the page, its record is the sheet ── */
    await page.evaluate(id => { const sh = CN.S.sheets.gold14k.pages.at(-1); sh.recalled = { id, metal: 'gold14k' }; sh.charms = []; sh.placements = []; }, S0.id);
    await openSheet(page, S0.id);
    await free(page);
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on && CNManual._MC.sh._standalone), 10000, 'Manual Control on a recalled card');
    await page.click('.sheetWin [data-r=manual]'); await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'off');
    await until(() => st.doc(SHEETS, S0.id).manualEdit == null, 10000, 'given back');
    console.log('ok   5  a recalled card (no pieces on the page): Manual Control starts from its record, and nothing moved gives the sheet back as it was');

    /* ── 6 · cut on the laser: still the one lock ── */
    const d = st.doc(SHEETS, S0.id); st.put(SHEETS, S0.id, Object.assign({}, d, { laserDoneAt: Date.now() - 60000 }));
    await B.page.evaluate(id => window.CharmNestStore && CharmNestStore.drop && CharmNestStore.drop('sheetFull', id), S0.id);
    await B.page.evaluate(() => SheetWin.close && SheetWin.close()); await B.page.waitForTimeout(400);
    await B.page.evaluate(id => SheetWin.open(id), S0.id);
    const cut = await until(async () => { const v = await sw(B.page); return v && /has been cut on the laser/.test(v.why) ? v : null; }, 20000, 'the laser lock');
    assert.equal(cut.disabled, 'true');
    await B.page.click('.sheetWin [data-r=manual]', { force: true }); await B.page.waitForTimeout(300);
    assert.equal(await B.page.evaluate(() => CNManual._MC.on), false, 'it does not start');
    const srvCut = await B.page.evaluate(id => CN.api('charmNestLibrary', { op: 'manualClaim', id, tab: 'tab-cut-1', by: 'Other Person' }, { quiet: true }), S0.id);
    assert.equal(srvCut.ok, false); assert.equal(srvCut.refused, 'cut');
    console.log('ok   6  cut on the laser: "' + cut.why + '", and the server refuses the claim');

    assert.deepEqual(A.errors.concat(B.errors), [], 'no page errors: ' + A.errors.concat(B.errors).join(' | '));
    console.log('\nbrowser checks passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
