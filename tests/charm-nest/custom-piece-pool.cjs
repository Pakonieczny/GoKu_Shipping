// CUSTOMPOOL (Paul, 28 Oct 2026, items 27 and 28; the lead's D4, D5, D6): a design sent to a sheet becomes a real piece.
// In a real Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary handlers, designPut and customPiecePut included, over an in-memory store; nothing live):
//   1  a design saved to the design library (CharmNestDesigns.put / get: its hash) is sent to SS with no order, named "Spare heart": Pool.addCustomPiece gives
//      CUSTOM-<yyyymmdd>-1_<yyyymmdd>0001_1, its pool row on the server (orderless, its design id, version, hashes, who), and the piece lands on the SS sheet that is open
//      (beside the pieces already on it), nested, verified and saved AT ITS EXACT SIZE: written at 100 % (the other charms 97.1 %), its file read back = its hash
//   2  an order's piece (order 4170000555, two copies) on the same SS sheet: named by the order, its pool ids the order's
//   3  the SS sheet nested again from scratch: the custom pieces still at 100 %, the file still its hash
//   4  sanctity: a piece whose parts in the file are not its hash stops the save, loudly (the sheet's problem says why); put right, it saves again
//   5  10K, Manual Control: the API refuses a resize ("keeps the exact size"), the corner and the + key refuse it in the window and say why, an arrow moves it;
//      saved: still 100 %, its file still its hash, no size on its manual mark
//   6  the QR label: the orderless piece is not in the label's order list (what the stations ship), its name is on the label ("Custom · no order: Spare heart"), the
//      order's piece is; a sheet with only an orderless piece still gets its label (and is ready); the set's orders leave it out
//   node tests/charm-nest/custom-piece-pool.cjs     (through scratchpad/int/slot.sh: PW_DIR, CHROMIUM)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), { scene } = require('./_masterScene.cjs');
  const pdf = await scene({ page: [420, 200], bodies: [], extraOps: [[20, 20], [80, 20], [140, 20]].map(([x, y]) => `q 0 0 0 RG 0.25 w ${x} ${y} 20 20 re S Q`) });
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    if (/fonts\.g/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'off', sandboxStream: 'off', pollOrders: 'off', runMode: 'manual', sound: 'off', notify: 'off', dsOrigin: 'http://127.0.0.1:9' }); localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; window.prompt = () => 'Tester'; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::|status of 4\d\d/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  page.setDefaultTimeout(30000);
  const until = async (fn, ms = 30000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 150)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.CNManual && window.CharmNestCustomPool && window.CharmNestDesigns && window.Pool && typeof Pool.addCustomPiece === 'function', null, { timeout: 60000 });
    // the SS sheet a design goes to: three squares dropped on Sterling Silver, nested and saved (an open sheet with pieces on it)
    await page.evaluate(async b64 => { const u = Uint8Array.from(atob(b64), c => c.charCodeAt(0)); await CN.intake([new File([u], 'squares.pdf', { type: 'application/pdf' })], 'silver'); }, pdf.toString('base64'));
    const settled = (metal, n) => page.evaluate(([metal, n]) => { const sh = CN.S.sheets[metal].pages.at(-1); if (sh.charms.length === n && sh.status === 'ready' && !sh._operationStarting) CN.startNest(sh); return sh.charms.length === n && sh.placements.length === n && ['complete', 'partial'].includes(sh.status) && sh.persistedDone && !!sh.verification && sh.verification.ok && !sh.dirty && !!sh.sheetId && !sh.problem; }, [metal, n]);
    await until(() => settled('silver', 3), 120000, 'the SS sheet of three squares nested and saved');
    const ss0 = await page.evaluate(() => { const sh = CN.S.sheets.silver.pages.at(-1); return { id: sh.sheetId, pages: CN.S.sheets.silver.pages.length }; });

    /* ── 1 · a design of the library, sent to SS with no order ── */
    // a 20 × 16 mm piece (its corners rounded with curves), a red engraved zig-zag, a hatched diamond, and words already outlined in a group the editor moved and turned
    const MMp = 72 / 25.4, W = 20 * MMp, H = 16 * MMp, r = 3 * MMp, k = 0.5523 * r;
    const outline = [[['m', [r, 0]], ['l', [W - r, 0]], ['c', [W - r + k, 0], [W, r - k], [W, r]], ['l', [W, H - r]], ['c', [W, H - r + k], [W - r + k, H], [W - r, H]], ['l', [r, H]], ['c', [r - k, H], [0, H - r + k], [0, H - r]], ['l', [0, r]], ['c', [0, r - k], [r - k, 0], [r, 0]], ['h']]];
    const zig = [[['m', [8, 10]], ['l', [14, 16]], ['l', [20, 10]], ['l', [26, 16]], ['l', [32, 10]]]];
    const diamond = [[['m', [40, 24]], ['l', [46, 30]], ['l', [40, 36]], ['l', [34, 30]], ['h']]];
    // (the matrix the Charm Editor hands back with a group: T(c + t) · R(rot) · T(−c), c the centre of its parts' box)
    const turnAbout = (deg, cx, cy, tx, ty) => { const a = deg * Math.PI / 180, cs = Math.cos(a), sn = Math.sin(a); return [cs, sn, -sn, cs, cx + tx - (cs * cx - sn * cy), cy + ty - (sn * cx + cs * cy)]; };
    const letters = [[['m', [12, 30]], ['l', [16, 30]], ['l', [16, 38]], ['l', [12, 38]], ['h']], [['m', [18, 30]], ['l', [24, 30]], ['l', [24, 33]], ['l', [18, 33]], ['h']]];
    const design0 = { name: 'Spare heart test', source: { kind: 'new' }, widthMm: 20, heightMm: 16, faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: W, hPt: H }, items: [
      { id: 'out', kind: 'path', role: 'cut', subpaths: outline, stroke: [0, 0, 0] },
      { id: 'zig', kind: 'path', role: 'line', subpaths: zig, stroke: [1, 0, 0] },
      { id: 'grp', kind: 'group', tx: 2, ty: -1, rot: 10, matrix: turnAbout(10, 40, 30, 2, -1), children: [{ id: 'dia', kind: 'path', role: 'hatch', subpaths: diamond, fill: [0, 0, 1] }] },
      { id: 'hidden', kind: 'path', role: 'line', hidden: true, subpaths: [[['m', [5, 5]], ['l', [9, 9]]]], stroke: [1, 0, 0] },
      { id: 'txt', kind: 'text', role: 'hatch', text: 'AB', fontKey: 'ace', sizePt: 8, subpaths: letters, fill: [0, 0, 1] }] } } };
    const saved = await page.evaluate(async d => { const s = await CharmNestDesigns.put(d); const got = await CharmNestDesigns.get(s.id, s.version); return { s: { id: s.id, version: s.version, hash: s.hash }, design: got }; }, design0);
    assert(saved.s.id && saved.s.version === 1 && /^[0-9a-f]{64}$/.test(saved.s.hash), 'the design is in the library: ' + JSON.stringify(saved.s));
    const send = (metal, extra) => page.evaluate(([d, s, metal, extra]) => Pool.addCustomPiece(Object.assign({ design: d, version: s.version, hash: s.hash, metal, orderId: null, customName: 'Spare heart', quantity: 1, by: 'Tester' }, extra || {})), [saved.design, saved.s, metal, extra || null]);
    const t0 = Date.now();
    const ids1 = await send('silver');
    const sendMs = Date.now() - t0;
    assert.equal(ids1.length, 1); assert.match(ids1[0], /^CUSTOM-(\d{8})-1_\1\d{4}_1$/, 'an orderless piece is numbered CUSTOM-<yyyymmdd>-<n>: ' + ids1[0]);
    const row = st.doc('Charm_Pool', ids1[0]);
    assert(row && row.orderless === true && row.custom === true && row.customId === ids1[0].split('_')[0] && row.orderId === row.customId && row.material === 'silver' && row.by === 'Tester', 'its pool row on the server: ' + JSON.stringify(row).slice(0, 300));
    assert(row.customPiece && row.customPiece.designId === saved.s.id && row.customPiece.version === 1 && row.customPiece.designHash === saved.s.hash && /^g1-/.test(row.customPiece.geomHash) && row.customPiece.name === 'Spare heart', 'it names its design, version and hashes: ' + JSON.stringify(row.customPiece));
    assert.equal(Math.round(row.customPiece.widthMm * 1000), 20000); assert.equal(Math.round(row.customPiece.heightMm * 1000), 16000);
    // a retry of the same request is the same number (the server's word; the page sends each press once)
    await until(() => settled('silver', 4), 180000, 'the custom piece nested on the SS sheet and saved');
    const live1 = await page.evaluate(id => {
      const pages = CN.S.sheets.silver.pages, sh = pages.find(p => p.charms.some(c => c.poolId === id)), c = sh.charms.find(x => x.poolId === id), p = sh.placements.find(x => x.id === c.id);
      return { pages: pages.length, same: sh === pages.at(-1), sheetId: sh.sheetId, name: c.name, frozen: c.frozen, scale: p.scale, others: sh.placements.filter(x => x.id !== c.id).map(x => x.scale), aiUrl: sh.cloud && sh.cloud.ai, index: sh.placements.indexOf(p) };
    }, ids1[0]);
    assert.equal(live1.pages, ss0.pages, 'no new SS sheet: the open one took it'); assert.equal(live1.sheetId, ss0.id, 'on the SS sheet that was open');
    assert.equal(live1.name, 'Spare heart · Custom · no order');
    assert.equal(live1.scale, 1, 'written at 100 %'); assert(live1.others.every(s => s === 0.971), 'the other charms at 97.1 %: ' + live1.others);
    assert.equal(live1.frozen.designHash, saved.s.hash); assert.equal(live1.frozen.orderless, true); assert.equal(live1.frozen.geomHash, row.customPiece.geomHash);
    // (the sheet's .ai as it was saved: the file the record names, from the fake storage, read back by the page's own reader)
    const fileCheck = async (metal, poolId) => {
      const info = await page.evaluate(([metal, poolId]) => { const sh = CN.S.sheets[metal].pages.find(p => p.charms.some(c => c.poolId === poolId)), c = sh.charms.find(x => x.poolId === poolId); return { sheetId: sh.sheetId, i: sh.placements.findIndex(x => x.id === c.id), want: c.frozen.geomHash }; }, [metal, poolId]);
      const rec = st.doc('Charm_Nest_Sheets', info.sheetId), blob = rec && rec.outputs && rec.outputs.ai && st.blobs.get(rec.outputs.ai.path);
      assert(blob, 'the saved .ai of ' + info.sheetId);
      return page.evaluate(async ([b64, i, want]) => {
        const bytes = Uint8Array.from(atob(b64), ch => ch.charCodeAt(0)), got = (await CharmNestCustomPool.writtenOf(bytes, new Set([i]))).get(i);
        return { written: got ? CharmNestCustomPool.hashOf(got.keys) : null, parts: got ? got.keys.length : 0, scale: got ? Math.sqrt(got.det) : 0, want };
      }, [blob.buf.toString('base64'), info.i, info.want]);
    };
    let fc = await fileCheck('silver', ids1[0]);
    assert.equal(fc.written, fc.want, 'the saved .ai holds exactly its parts: ' + JSON.stringify(fc)); assert.equal(fc.parts, 4, 'cut, line, hatch and the words (the hidden line is not engraved)'); assert(Math.abs(fc.scale - 1) < 1e-6, 'at 100 %: ' + fc.scale);
    const rec1 = st.doc('Charm_Nest_Sheets', live1.sheetId), rc = rec1.charms.find(c => c.poolId === ids1[0]);
    assert(rc && rc.frozen && rc.frozen.designId === saved.s.id && rc.frozen.geomHash === fc.want && rec1.poolIds.includes(ids1[0]), 'the sheet record names the piece, its design and its hash');
    console.log(`ok   1  "Spare heart" (no order) → ${ids1[0]} on the open SS sheet (${live1.sheetId}) in ${sendMs} ms: written at 100 % (the others at 97.1 %), its .ai read back = its hash ${fc.want} (${fc.parts} parts, the hidden one left out)`);

    /* ── 2 · an order's piece, two copies ── */
    const ids2 = await send('silver', { orderId: '4170000555', customName: 'Replacement charm', quantity: 2 });
    assert.equal(ids2.length, 2); assert(ids2.every((id, i) => new RegExp(`^4170000555_\\d{12}_${i + 1}$`).test(id)), 'the order\'s pool ids: ' + ids2);
    const row2 = st.doc('Charm_Pool', ids2[0]); assert(row2.orderless === false && row2.orderId === '4170000555' && /^CUSTOM-\d{8}-2$/.test(row2.customId), 'its row: ' + JSON.stringify(row2).slice(0, 200));
    await until(() => settled('silver', 6), 180000, 'the order\'s pieces nested');
    const names2 = await page.evaluate(ids => { const sh = CN.S.sheets.silver.pages.at(-1); return ids.map(id => { const c = sh.charms.find(x => x.poolId === id); return [c.name, sh.placements.find(p => p.id === c.id).scale]; }); }, ids2);
    assert.deepEqual(names2, [['4170000555 · Custom · Replacement charm · 1/2', 1], ['4170000555 · Custom · Replacement charm · 2/2', 1]]);
    console.log('ok   2  an order\'s piece: ' + ids2.join(', ') + ' named by its order, both at 100 %');

    /* ── 3 · the SS sheet nested again from scratch ── */
    await page.evaluate(() => { const sh = CN.S.sheets.silver.pages.at(-1); for (const c of sh.charms) { c.pinned = null; delete c.arrivalPin; } sh.placements = []; delete sh.appendOnly; delete sh.intakeAppend; sh.nestInitial = null; sh.dirty = true; sh.status = 'ready'; CN.startNest(sh); });
    await until(() => settled('silver', 6), 180000, 'the SS sheet nested from scratch');
    const again = await page.evaluate(() => { const sh = CN.S.sheets.silver.pages.at(-1); return sh.placements.map(p => [sh.charms.find(c => c.id === p.id).frozen ? 'custom' : 'plain', p.scale]); });
    assert(again.every(([k, s]) => (k === 'custom' ? s === 1 : s === 0.971)), 'after a nest from scratch: custom 100 %, the others 97.1 %: ' + JSON.stringify(again));
    fc = await fileCheck('silver', ids1[0]); assert.equal(fc.written, fc.want, 'its file still its hash after the nest');
    console.log('ok   3  nested again from scratch: ' + again.filter(x => x[0] === 'custom').length + ' custom pieces still at 100 %, the file still its hash');

    /* ── 4 · sanctity: a file that is not the hash is not saved ── */
    const bad = await page.evaluate(async id => {
      const sh = CN.S.sheets.silver.pages.at(-1), c = sh.charms.find(x => x.poolId === id), was = c.frozen.geomHash;
      c.frozen = Object.assign({}, c.frozen, { geomHash: 'g1-0000000000000000000000000000-4' });
      // (a Gold or Silver sheet whose layout is as last written keeps its files and writes nothing: finishNest's own rule. Its files are written again here)
      sh._written = null; const ok = await window.writeAsPlaced(sh), problem = sh.problem, status = sh.status;
      c.frozen = Object.assign({}, c.frozen, { geomHash: was });
      sh._written = null; const ok2 = await window.writeAsPlaced(sh);
      return { ok, problem, status, ok2, problem2: sh.problem };
    }, ids1[0]);
    assert.equal(bad.ok, false, 'not saved'); assert.match(bad.problem || '', /^Not saved: a custom piece is not written exactly as it was designed — "Spare heart"'s parts in the file are not its design's/, 'the sheet says why: ' + bad.problem);
    assert.equal(bad.ok2, true, 'put right, it saves'); assert(!bad.problem2, 'and the problem is gone: ' + bad.problem2);
    console.log('ok   4  a file whose parts are not the piece\'s hash is refused: "' + bad.problem.slice(0, 110) + '…"; put right, saved');

    /* ── 5 · 10K: Manual Control moves and turns it, never resizes it ── */
    const ids3 = await send('gold10k', { customName: 'Ten K spare' });
    await until(() => settled('gold10k', 1), 180000, 'the 10K piece nested on a new 10K sheet');
    const tk = await page.evaluate(id => { const sh = CN.S.sheets.gold10k.pages.at(-1), c = sh.charms[0], p = sh.placements[0]; return { id: sh.sheetId, cid: c.id, p: { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle, scale: p.scale } }; }, ids3[0]);
    assert.equal(tk.p.scale, 1);
    const api = await page.evaluate(([id, p]) => [CNManual.fits(CN.S.sheets.gold10k.pages.at(-1), id, { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle, size: 1.1 }), CNManual.fits(CN.S.sheets.gold10k.pages.at(-1), id, { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle })], [ids3[0], tk.p]);
    assert.equal(api[0].ok, false); assert.equal(api[0].why, 'size'); assert.match(api[0].reason, /"Ten K spare" keeps the exact size it was designed at \(20\.00 × 16\.00 mm, design .+ v1\): it can be moved and turned, not resized\./);
    assert.notEqual(api[1].why, 'size', 'at its own size the check is the usual one: ' + JSON.stringify(api[1]));
    await page.evaluate(id => SheetWin.open(id), tk.id);
    await until(() => page.evaluate(id => { const w = SheetWin._W; return w.id === id && !!w.rec && w.geom && !w.flying; }, tk.id), 30000, 'the sheet window');
    await page.waitForTimeout(300);
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => CNManual._MC.on), 10000, 'Manual Control on');
    const at = (x, y) => page.evaluate(([x, y]) => CNManual._client(x, y), [x, y]);
    await page.mouse.click(...await at(tk.p.cxPt, tk.p.cyPt)); await page.waitForTimeout(150);
    assert.equal(await page.evaluate(() => CNManual._MC.sel && CNManual._MC.sel.id), tk.cid, 'the piece in hand');
    const pose = () => page.evaluate(id => { const M = CNManual._MC; return M.edits.get(id) || M.from.get(id); }, tk.cid);
    for (let i = 0; i < 3; i++) await page.keyboard.press('Equal');
    let q = await pose(); assert.equal(q.size, 1, '+ does not size it: ' + q.size);
    assert.match(await page.evaluate(() => CNManual._MC.msg || ''), /keeps the exact size it was designed at/, 'and says why');
    const T = await page.evaluate(() => CNManual._tool()), c0 = T.centre, k0 = T.corners[2];
    await page.mouse.move(k0[0], k0[1]); await page.mouse.down(); for (let i = 1; i <= 8; i++) await page.mouse.move(k0[0] + (k0[0] - c0[0]) * 0.2 * i / 8, k0[1] + (k0[1] - c0[1]) * 0.2 * i / 8); await page.mouse.up(); await page.waitForTimeout(150);
    q = await pose(); assert.equal(q.size, 1, 'a corner does not size it: ' + q.size);
    await page.keyboard.press('Shift+ArrowRight'); await page.waitForTimeout(400);
    q = await pose(); assert(Math.abs(q.cxPt - (tk.p.cxPt + MMp)) < 1e-6 && q.size === 1, 'an arrow moves it 1 mm: ' + JSON.stringify(q));
    await page.click('.sheetWin [data-r=manual]');
    await until(() => page.evaluate(() => !CNManual._MC.on), 5000, 'Manual Control off');
    const done = await until(() => page.evaluate(() => { const w = SheetWin._W.work; return w && w.state !== 'working' && !SheetWin._W.flow ? { state: w.state, title: w.title } : null; }), 180000, 'the save to end');
    assert.equal(done.state, 'done', 'saved: ' + JSON.stringify(done));
    const after = await page.evaluate(() => { const sh = CN.S.sheets.gold10k.pages.at(-1), c = sh.charms[0], p = sh.placements[0]; return { scale: p.scale, cxPt: p.cxPt, manual: c.manual || null, ok: sh.verification && sh.verification.ok, ver: JSON.stringify(sh.verification || null).slice(0, 600), log: (sh.log || []).slice(-6), problem: sh.problem || null }; });
    assert.equal(after.scale, 1, 'saved at 100 %'); assert(Math.abs(after.cxPt - (tk.p.cxPt + MMp)) < 1e-6, 'where it was moved'); assert(after.manual && after.manual.by === 'Tester' && after.manual.size == null, 'its manual mark has no size: ' + JSON.stringify(after.manual)); assert.equal(after.ok, true, 'verified: ' + after.ver + ' ' + JSON.stringify(after.log)); assert.equal(after.problem, null);
    fc = await fileCheck('gold10k', ids3[0]); assert.equal(fc.written, fc.want, 'its file still its hash after Manual Control'); assert(Math.abs(fc.scale - 1) < 1e-6);
    console.log('ok   5  10K: Manual Control refused the resize (API, + key, corner) and said why, moved it 1 mm; saved at 100 %, the file still its hash');
    await page.evaluate(() => SheetWin.close && SheetWin.close());

    /* ── 6 · the QR label ── */
    const lab = await page.evaluate(async ([orderless, ordered]) => {
      const r = await CN.api('charmNestLibrary', { op: 'setAllocate', day: CN.today(), runId: 'run-custom-pool-test' }), out = {};
      const set = { setId: r.setId, seq: r.seq, day: r.day, runId: 'run-custom-pool-test', group: null, name: 'Set-' + r.seq, folder: '', orders: {}, sheetIds: [], materials: [], labelFiles: [] };
      for (const metal of ['silver', 'gold10k']) {
        // (a 10K sheet is a draft until it is chosen into its set: chosen here, as the Sets gate does)
        const sh = CN.S.sheets[metal].pages.at(-1), draft = sh.draft; sh.runId = 'run-custom-pool-test'; sh.draft = false;
        await Sets.onSheetSaved(sh, sh.charms, sh.cloud, { labelsOnly: true, setOverride: set });
        out[metal] = { files: sh.label.files.map(f => ({ label: f.label, notes: f.notes || [], orders: f.orders, payload: f.payload })), ready: Sets.labelsReady(sh, set), why: { draft, setId: sh.setId === set.setId, fileBase: sh.fileBase, paths: sh.label.files.map(f => !!f.path && !!f.url), indexed: (set.labelFiles || []).filter(f => f.sheetId === sh.sheetId).length } };
        sh.draft = draft;
      }
      out.setOrders = Object.keys(set.orders); out.orderless = orderless; out.ordered = ordered;
      return out;
    }, [ids1[0].split('_')[0], '4170000555']);
    const ssL = lab.silver.files[0], tkL = lab.gold10k.files[0];
    assert(!ssL.orders.includes(lab.orderless) && ssL.orders.includes(lab.ordered), 'the SS label lists the order, not the orderless piece: ' + JSON.stringify(ssL.orders));
    assert(!ssL.payload.includes(Number(lab.orderless.replace(/\D/g, '')).toString(36)), 'nor does its QR');
    assert.deepEqual(ssL.notes, ['Custom · no order: Spare heart']); assert.match(ssL.label, /\+ custom$/);
    assert.equal(lab.gold10k.files.length, 1, 'a sheet with only an orderless piece still has its label'); assert.deepEqual(tkL.orders, []); assert.deepEqual(tkL.notes, ['Custom · no order: Ten K spare']);
    assert.equal(lab.silver.ready, true); assert.equal(lab.gold10k.ready, true, 'and it is ready (a set waits for no label): ' + JSON.stringify(lab.gold10k.why));
    assert(!lab.setOrders.includes(lab.orderless) && lab.setOrders.includes(lab.ordered), 'the set\'s orders: ' + lab.setOrders);
    console.log(`ok   6  labels: SS "${ssL.label}" lists ${ssL.orders.length} orders (not ${lab.orderless}) and says "${ssL.notes[0]}"; the 10K sheet's label says "${tkL.notes[0]}" with no order on it`);
    assert.deepEqual(errors, [], 'no page errors');
    console.log('custom-piece-pool: all checks passed');
  } catch (e) { console.error(e); if (errors.length) console.error(errors.join('\n')); process.exitCode = 1; }
  finally { await browser.close().catch(() => {}); try { await srv.close(); } catch (_) {} }
})();
