/* ART EDIT (Paul, 27 Oct 2026): ONE piece's own artwork edit on the real sorter page (charm-nest-1.html with charm-nest-art-edit.js and the bridge), in headless Chromium against the
   in-memory stand-in of tests/charm-nest/bridge-server.cjs (the real charmNestLibrary handler). No live service, no credential.
     A  the page loads the module before the bridge and the bridge lends it the page's ways (setHost); nothing on the page errs
     B  a save on a piece of a written sheet: the cloud's record and the piece's pool row stamp; the sheet holds the piece's NEW charm (same id, its own parts, a hash of its own) while the
        shared master's members are untouched; the front approved on it is withdrawn in the plain words ("This charm's front changed since the words were approved — fit the words again"),
        the cloud's front record invalidated, the back untouched; the sheet writes its files again as it stands (writeAsPlaced) and its record no longer owes them
     C  a cut line of that placed piece is refused on the page, before anything is sent
   PW_DIR=<dir with playwright-core> CHROMIUM=<chrome> node tests/charm-nest/art-edit-page.cjs        (skipped without a browser) */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ROOT = path.join(__dirname, '..', '..');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; };

(async () => {
  const pwDir = process.env.PW_DIR || '/opt/node-tools/node_modules';
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('art-edit-page: skipped (no playwright-core)'); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
  if (!fs.existsSync(chromePath)) { console.log('art-edit-page: skipped (no chrome)'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st, browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  try {
    const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
    ok(/<script src="charm-nest-art-edit\.js\?v=[^"]+"><\/script>/.test(html) && html.indexOf('charm-nest-art-edit.js') < html.indexOf('charm-nest-bridge.js?v='), 'A the page loads the module, with a cache token, before the bridge');
    const page = await browser.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Engrave && window.Pool && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const host = await page.evaluate(() => Object.keys(CharmNestArtEdit.host()).sort().join());
    ok(host === 'api,changed,charmOf,flag,liveCharms,note,parsedOf,placed,rowOf,swap,who' && await page.evaluate(() => typeof Engrave.artChanged === 'function'), 'A the bridge lends the module the page\'s ways: ' + host);

    const PID = '4170000001_10010_1', SHEET = 'gold-art-page-1', b64 = fs.readFileSync(path.join(__dirname, 'fixtures', 'MASTER_5X8_5.ai')).toString('base64');
    st.put('Charm_Pool', PID, { poolId: PID, orderId: '4170000001', sku: 'MASTER_5X8_5', state: 'written', sheetId: SHEET });
    st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', fileBase: 'GF_Oct.27.26_Set-1_Sheet-1', poolIds: [PID] });
    st.put('Charm_Pool_Back', PID + '~front', { poolId: PID, face: 'front', sheetId: SHEET, approvedAt: 5, approvedBy: 'Bea', invalidated: false });
    st.put('Charm_Pool_Back', PID, { poolId: PID, sheetId: SHEET, approvedAt: 6, approvedBy: 'Bea', invalidated: false });
    const out = await page.evaluate(async ([b64, PID, SHEET]) => {
      const bytes = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      const parsed = await CharmNestPDF.parseSource(bytes, 'MASTER_5X8_5.ai'), master = CharmNestPDF.groupCharms(parsed).charms[0];
      Object.assign(master, { id: 'src:0', sourceId: 'src', centerPt: [10, 15], strokePt: 0.5, hash: 'm5x8' });
      const piece = Object.assign({}, master, { id: 'src:' + PID, poolId: PID, pinned: null });   // (Pool.cloneCharm: the master's members shared)
      const sh = allSheets()[0]; Object.assign(sh, { sheetId: SHEET, fileBase: 'GF_Oct.27.26_Set-1_Sheet-1', outputs: { ai: { url: 'x' } }, status: 'complete' });
      sh.charms = sh.charms.concat([piece]); sh.placements = sh.placements.concat([{ id: piece.id, cxPt: 60, cyPt: 60, angle: 0, scale: 0.971 }]);
      B.pool.rows.set(PID, { poolId: PID, state: 'written', sheetId: SHEET });
      const writes = []; window.writeAsPlaced = async s => { writes.push(s.sheetId); return true; };   // (the write as it stands is the nest's own finish: here only that it is asked for)
      const job = { key: 'line-1~front', face: 'front', copies: [PID], state: 'written', approvedAt: 5, approvedBy: 'Bea', view: {}, mask: {}, fit: {}, backs: [{ poolId: PID, face: 'front', approvedAt: 5 }], row: { key: 'line-1', order: { receiptId: '4170000001' }, engrave: { state: 'written', approved: true } } };
      const back = { key: 'line-1', copies: [PID], state: 'written', approvedAt: 6, approvedBy: 'Bea', backs: [], row: job.row };
      Engrave.items().set(job.key, job); Engrave.items().set(back.key, back);
      const ids = CharmNestArtEdit.memberIds(piece), line = piece.members.findIndex(m => CharmNestArtEdit.roleOf(m, piece) === 'line'), view = { M: [1, 0, 0, 1, 0, 0], R: [1, 0, 0, 1, 0, 0] };
      const res = await CharmNestArtEdit.save(PID, 'front', { removed: [ids[line]], added: [{ id: 'dot', kind: 'text', role: 'hatch', subpaths: [[['m', [9, -16]], ['l', [11, -16]], ['l', [11, -14]], ['l', [9, -14]], ['h']]] }] }, { view, by: 'Paul K' });
      for (let i = 0; i < 50 && !writes.length; i++) await new Promise(r => setTimeout(r, 20));
      const live = Pool.charmOf(PID);
      let cut = null; const before = performance.now();
      try { await CharmNestArtEdit.save(PID, 'front', { added: [{ id: 'hole', role: 'cut', subpaths: [[['m', [9, -16]], ['l', [10, -16]], ['l', [10, -15]], ['h']]] }] }, { view, by: 'Paul K' }); } catch (e) { cut = e.refused && e.refused[0] && e.refused[0].why; }
      return { swapped: live === res.charm, sameId: live.id === piece.id, members: live.members.length, was: piece.members.length, masterClean: master.members.every(m => !m.artRedraw && !m.synthetic), sharedArray: piece.members === master.members,
        hash: live.hash, flag: B.pool.rows.get(PID).artEdit, job: [job.state, job.reason, job.approvedAt, !!job.fit], back: [back.state, back.approvedAt], writes, cut, ms: performance.now() - before };
    }, [b64, PID, SHEET]);
    for (let i = 0; i < 50 && !(st.doc('Charm_Pool_Back', PID + '~front') || {}).invalidated; i++) await new Promise(r => setTimeout(r, 50));
    const rec = st.doc('Charm_Pool_Art', PID + '~front');
    ok(rec && rec.by === 'Paul K' && rec.removed.length === 1 && rec.added.length === 1 && rec.sheetId === SHEET, 'B the cloud keeps the edit, who made it, on which sheet');
    ok(out.flag && out.flag.front === rec.at && st.doc('Charm_Pool', PID).artEdit.front === rec.at, 'B the pool row says so, on the page and in the cloud');
    ok(out.swapped && out.sameId && out.members === out.was && /~ae[0-9a-z]+$/.test(out.hash), 'B the sheet holds the piece\'s new charm: same id, one part removed and one added, a hash of its own (' + out.hash + ')');
    ok(out.masterClean && out.sharedArray, 'B the shared master is untouched');
    ok(out.job[0] === 'ready' && out.job[1] === 'This charm\'s front changed since the words were approved — fit the words again' && out.job[2] === null && !out.job[3], 'B the front approved on it goes back to be fitted, in the plain words: ' + JSON.stringify(out.job));
    ok(out.back[0] === 'written' && out.back[1] === 6, 'B the back is untouched');
    ok(st.doc('Charm_Pool_Back', PID + '~front').invalidated === true && st.doc('Charm_Pool_Back', PID).invalidated === false, 'B the cloud\'s front record is withdrawn, the back\'s kept');
    ok(out.writes.join() === SHEET, 'B the sheet writes its files again, as it stands');
    for (let i = 0; i < 50 && st.doc('Charm_Nest_Sheets', SHEET).filesOwed; i++) await new Promise(r => setTimeout(r, 50));
    ok(!st.doc('Charm_Nest_Sheets', SHEET).filesOwed, 'B and its record no longer owes them (filesOwed set by the save, cleared after the write)');
    ok(/nested with/.test(out.cut || ''), 'C a cut line of the placed piece is refused on the page: ' + out.cut);
    ok(!errors.length, 'A nothing on the page erred: ' + errors.join(' | '));
    console.log(`PASS ${n} checks: ART EDIT on the page — the module and its host, a save on a written sheet (the new charm, the master untouched, the front approval withdrawn, the files written again), a cut line refused`);
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
