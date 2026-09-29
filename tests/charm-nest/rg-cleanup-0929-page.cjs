// The page's side of a cleanup made on a saved sheet's record (Cleanups, charm-nest-bridge.js; Paul, 29 Sep: RG 14/20
// Sheet 1 held a custom lion twice and a green line 2 no Cut Sheet press made; the page kept its own older copy of the
// sheet, its saves were refused and a reload did not help). In a real Chromium on the fake site (bridge-server.cjs, the
// real charmNestLibrary and Rose stock handlers over an in-memory store; no Etsy, no model), once for each mode, "one"
// (keep one lion) and "both" (keep both lions):
//   1 · a Rose Gold sheet as Paul's was: a custom design with line 1 around it, then a custom lion of Etsy quantity 2
//       (two copies) and a line 2 around both lions; the workspace kept on this browser (the copy that goes stale);
//   2 · the cleanup runs on the cloud's records (rgCleanupOnce, the code of the one-off op rgCleanup0929, with this
//       sheet's own ids; once the op is removed, the records it leaves are written here instead): the page still open
//       shows line 2 and cannot record a cut of it; in mode "one" its own save of the sheet is refused;
//   3 · the page reloads from its stale copy: the cleanup goes on by itself, with no question or pop-up: mode "one" takes
//       copy 2 off the sheet, the set, the pool, the order line and the custom design and writes the sheet again; mode
//       "both" keeps both lions; line 2 is gone and line 1 is as saved;
//   4 · a save works (mode "one": that rewrite; mode "both": Nest All); no line comes by itself;
//   5 · Cut Sheet: exactly one new line, around the lion(s); line 1 byte for byte;
//   6 · a second reload applies nothing again; a record without a cleanup, or one applied already, changes nothing.
// Screenshots to SHOTS (default /mnt/project-files/plans/rg-cleanup).
//   node tests/charm-nest/rg-cleanup-0929-page.cjs [one|both]   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
//   RG_PAGE_EMULATE=1: write the records the op leaves instead of running it (as after the op's removal)
const path = require('path'), fs = require('fs');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start, Timestamp } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/rg-cleanup';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n, qty = 1) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: qty, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold Filled' }], metalKey: 'rose', metalLabel: 'RG 14/20', personalization: [], buyerMessage: '' }] });
const A = '4175423829', L = '4176537942', LINE = `${L}_${L}1`, CID = 'rg-cleanup-2026-09-29';
const NOTES = { one: 'Extra copy removed from RG 14/20 Sheet 1 (it was placed twice by mistake); line 2 removed (it was added without Cut Sheet)', both: 'Line 2 removed from RG 14/20 Sheet 1 (it was added without Cut Sheet)' };
const sorted = a => JSON.stringify([...(a || [])].sort());
const stagesOf = json => (json ? (JSON.parse(json).stages || []).map(s => s.ids.length) : null);

/** The records rgCleanupOnce leaves (its `cleanup`, line 2's fields, and in mode "one" copy 2 off the sheet, its pool row
    and the set): written here once the op is removed, so this page test keeps running. */
function leave(st, ws, spec, mode) {
  const at = Date.now(), one = mode === 'one', sheet = st.doc(ws + 'Charm_Nest_Sheets', spec.sheetId);
  const cleanup = { id: spec.id, mode, at, by: 'test', removedPoolIds: one ? [spec.dropPool] : [], removedCharmIds: one ? [spec.dropCharm] : [], keptPoolIds: one ? [spec.keepPool] : [spec.keepPool, spec.dropPool], lineRemoved: 2, lineKept: 1 };
  Object.assign(sheet, { rosePlanJson: null, rosePlanHash: null, roseFingerprint: null, cleanup, updatedAt: Timestamp.fromMillis(at) });
  if (!one) return;
  sheet.charms = sheet.charms.filter(c => c.id !== spec.dropCharm); sheet.placements = sheet.placements.filter(p => p.id !== spec.dropCharm);
  Object.assign(sheet, { poolIds: sheet.poolIds.filter(id => id !== spec.dropPool), names: sheet.charms.map(c => c.name).filter(Boolean).join(' '), charmCount: sheet.charms.length, placedCount: sheet.placements.length, dirty: true });
  Object.assign(st.doc(ws + 'Charm_Pool', spec.dropPool), { state: 'abandoned', sheetId: null, setId: null, cleanup: { id: spec.id, at, by: 'test', fromSheetId: spec.sheetId } });
  const set = st.doc(ws + 'Charm_Nest_Sets', spec.setId), o = set.orders[spec.orderId];
  o.lines = o.lines.map(l => (String(l.transactionId) === spec.transactionId ? Object.assign({}, l, { copies: l.copies.filter(c => c.poolId !== spec.dropPool) }) : l));
  set.cleanup = { id: spec.id, at, by: 'test', removedPoolIds: [spec.dropPool], sheetId: spec.sheetId };
}

const fails = [];
async function run(mode, browser) {
  const srv = await start({ receipts: [] }), { st } = srv, lib = st.handlers.charmNestLibrary;
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const check = (ok, what) => { if (!ok) fails.push(`${mode}: ${what}`); console.log((ok ? '  ok   ' : '  FAIL ') + `${mode} · ${what}`); };
  const said = [];   // the page's console, kept beside the screenshots
  try {
    fs.mkdirSync(SHOTS, { recursive: true });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // (a question or a pop-up would be recorded here: the cleanup asks none)
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; window.__asked = []; window.confirm = m => { window.__asked.push(String(m)); return true; }; window.alert = m => { window.__asked.push(String(m)); }; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    page.on('console', m => said.push(`[${m.type()}] ${m.text()}`.slice(0, 600)));
    const ready = () => page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.RoseStock && window.Gate && window.Cleanups && window.Session && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await ready();
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-rg`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, [order(A, 5), order(L, 4, 2)]);
    let sheetId = null;
    const rg = () => page.evaluate(id => {
      const sh = id ? CN.S.sheets.rose.pages.find(p => p.sheetId === id) : CN.S.sheets.rose.pages.at(-1); if (!sh) return null;
      return { sheetId: sh.sheetId, status: sh.status, dirty: !!sh.dirty, saved: !!sh.persistedDone, placed: sh.placements.map(p => p.id), charms: sh.charms.map(c => c.id), plan: (sh.rosePlan?.stages || []).map(s => ({ n: s.n, at: s.at, ids: s.ids })), guard: (sh.roseProtected?.stages || []).map(s => ({ n: s.n, ids: s.ids })), guardJson: sh.roseProtected ? JSON.stringify(sh.roseProtected) : null, cutButton: !!sh.el?.querySelector('.roseCut:not([hidden]) [data-rose="cut"]'), error: sh._roseError || null, problem: sh.problem || null, cleanups: sh.cleanups || null,
        why: { mode: CN.S.mode, el: !!sh.el, slot: sh.el ? (sh.el.querySelector('.roseCut')?.outerHTML || 'none').slice(0, 200) : null, cutAt: sh.roseCutAt || null, recalled: !!sh.recalled, saved: !!sh.persistedDone, ver: !!sh.verification?.ok, dirty: !!sh.dirty, status: sh.status, stock: sh.roseStock ? { id: sh.roseStock.id, rev: sh.roseStock.revision } : null, loaded: !!sh._roseLoaded } };
    }, sheetId);
    const planCalls = () => st.calls.filter(c => c.op === 'rosePlan').map(c => (c.body.cut === true ? 'cut' : 'plain'));
    const record = id => st.doc('Charm_Nest_Sheets', id) || st.doc('Sandbox_Charm_Nest_Sheets', id);
    const until = async (f, ms, what) => { const t = Date.now() + ms; for (;;) { const v = await f(); if (v) return v; if (Date.now() > t) throw new Error('timed out: ' + what); await page.waitForTimeout(400); } };
    async function send(rid, name, w) {
      await page.evaluate(() => CN.setMode('review'));
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
      await page.waitForSelector(card);
      await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(w), name });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click('#cuDlg .cuFile .cuM[data-m="rose"]');
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, null, { timeout: 30000 });
      await page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); });
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, null, { timeout: 120000 });
      await page.evaluate(() => CN.setMode('nest'));
      await page.waitForTimeout(1500);
    }
    // what the card's Nest button runs; then the sheet waited for until its nest (and save) settle
    async function nestByHand() {
      const n0 = await page.evaluate(id => CN.S.sheets.rose.pages.find(p => p.sheetId === id).log.length, sheetId);
      await page.evaluate(id => { const sh = CN.S.sheets.rose.pages.find(p => p.sheetId === id); sh._byHand = true; CN.startNest(sh); }, sheetId);
      await until(() => page.evaluate(([id, n]) => { const sh = CN.S.sheets.rose.pages.find(p => p.sheetId === id); return (sh.log.length > n || sh.status === 'error') && !['nesting', 'finishing', 'queued'].includes(sh.status) && !sh._operationStarting; }, [sheetId, n0]), 120000, 'a nest by hand');
      await page.waitForTimeout(3000);
    }
    const cutSheet = async () => {
      await page.click('.sheetCard[data-m="rose"] [data-rose="cut"]');
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return !sh._roseAction && !sh._rosePlanning && !sh._roseStep; }, null, { timeout: 60000 });
      await page.waitForTimeout(800);
    };

    // 1 · the sheet as Paul's was: line 1 around the first design, the lion twice, line 2 around both lions
    await send(A, 'a.dxf', 18);
    await page.evaluate(() => Gate.changeMembership('rose', true));
    await page.waitForTimeout(2000);
    await cutSheet();
    let s = await rg(); sheetId = s.sheetId;
    const line1 = s.plan[0];
    check(s.plan.length === 1 && s.placed.length === 1, `1 · line 1 around the first design (${JSON.stringify(s.plan.map(p => p.ids.length))})`);
    await send(L, 'lion.dxf', 24);
    await page.evaluate(async () => { const sh = CN.S.sheets.rose.pages.at(-1); sh._roseError = null; await Gate.assemble(B.run); CN.renderCard(sh); });
    await page.waitForTimeout(1500);
    await page.evaluate(async () => { await Gate.flush(B.run).catch(() => {}); await Gate.changeMembership('rose', true).catch(() => {}); });
    await page.waitForTimeout(2500);
    s = await rg();
    const lions = s.placed.filter(id => !line1.ids.includes(id));
    check(s.placed.length === 3 && lions.length === 2 && lions.every(id => id.includes(LINE)) && s.guard.length === 1 && !s.plan.length, `1 · the lion twice, uncut (${JSON.stringify({ placed: s.placed, guard: s.guard.length, plan: s.plan.length })})`);
    await cutSheet(); s = await rg();
    const rec1 = record(sheetId);
    check(s.plan.length === 2 && sorted(s.plan[1].ids) === sorted(lions) && JSON.stringify(stagesOf(rec1.rosePlanJson)) === '[1,2]' && JSON.stringify(stagesOf(rec1.roseProtectedJson)) === '[1]',
      `1 · line 2 around both lions, line 1 alone in roseProtectedJson (${JSON.stringify({ plan: stagesOf(rec1.rosePlanJson), guard: stagesOf(rec1.roseProtectedJson), error: s.error })})`);
    // the workspace on this browser, as it stands: the copy the page opens from after the cleanup
    await page.evaluate(() => Session.flush(true));
    await page.waitForTimeout(1500);

    // 2 · the cleanup on the cloud's records (the op's own code, with this sheet's ids)
    const ws = st.doc('Charm_Nest_Sheets', sheetId) ? '' : 'Sandbox_', D = n => ws + n;
    const keepCharm = lions.find(id => id.endsWith('_1')), dropCharm = lions.find(id => id.endsWith('_2'));
    const spec = { id: CID, workspace: ws, backedUp: 'the test sheet as built', sheetId, sheetLabel: 'RG Sheet 1', orderId: L, lineKey: LINE, transactionId: L + '1',
      keepPool: LINE + '_1', dropPool: LINE + '_2', keepCharm, dropCharm, setId: rec1.setId, stockId: rec1.roseStockId, backups: 'Charm_Nest_Cleanup_Backups', notes: NOTES };
    const guardJson = rec1.roseProtectedJson, cutsBefore = planCalls().filter(c => c === 'cut').length;
    const emulate = process.env.RG_PAGE_EMULATE === '1' || typeof lib.rgCleanupOnce !== 'function';
    if (emulate) { leave(st, ws, spec, mode); check(true, '2 · the records the op leaves, written here (RG_PAGE_EMULATE or no op)'); }
    else {
      spec.expect = lib.rgCleanupDigest(spec, { sheet: record(sheetId), pools: { [spec.keepPool]: st.doc(D('Charm_Pool'), spec.keepPool), [spec.dropPool]: st.doc(D('Charm_Pool'), spec.dropPool) }, set: st.doc(D('Charm_Nest_Sets'), spec.setId), stock: st.doc(D('Charm_Nest_Rose_Stock'), spec.stockId) });
      lib.ops.rgCleanupPageTest = b => lib.rgCleanupOnce(spec, b);
      const call = body => fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(Object.assign(ws ? { sandbox: true } : {}, body)) }).then(async r => ({ status: r.status, body: await r.json() }));
      let r = await call({ op: 'rgCleanupPageTest', mode });
      check(r.status === 200 && r.body.dryRun === true && r.body.matchesBackup === true, `2 · the dry run (${r.status} ${JSON.stringify(r.body.error || r.body.summary || '').slice(0, 300)})`);
      r = await call({ op: 'rgCleanupPageTest', mode, dryRun: false, confirm: CID, by: 'test' });
      check(r.status === 200 && r.body.done === true && r.body.mode === mode, `2 · the cleanup runs, mode "${mode}" (${r.status} ${JSON.stringify(r.body.error || '').slice(0, 300)})`);
    }
    const rec2 = record(sheetId);
    check(!rec2.rosePlanJson && rec2.roseProtectedJson === guardJson && rec2.cleanup?.id === CID && rec2.placements.length === (mode === 'one' ? 2 : 3), `2 · the cloud's sheet: no line 2, line 1 byte for byte, ${mode === 'one' ? 'one lion' : 'both lions'}`);

    // 2 · the page still open has its older copy: line 2 shown, and no cut of it can be recorded
    s = await rg();
    check(s.placed.length === 3 && s.plan.length === 2 && !s.cleanups, `2 · the page still open shows line 2 and ${s.placed.length} pieces (its copy is older)`);
    let snap = JSON.stringify(record(sheetId));
    const staleCut = await page.evaluate(async () => { const sh = CN.S.sheets.rose.pages.at(-1); try { await CN.api('charmNestLibrary', { op: 'roseRecordCut', sheetId: sh.sheetId, stockId: sh.roseStock.id, revision: sh.roseRevision, planHash: sh.rosePlanHash, by: 'test', device: 'charm-nest-1' }, { quiet: true }); return 'accepted'; } catch (e) { return e.message; } });
    check(staleCut !== 'accepted' && JSON.stringify(record(sheetId)) === snap, `2 · a cut of the old line 2 is refused and writes nothing (${staleCut})`);
    if (mode === 'one') {
      // its own save of the sheet (Nest: nested again, then saved) is refused, as Paul's were: copy 2 stays off, in the
      // cloud (this also stops the run, as a failed save does; the reload below writes the sheet again all the same)
      const puts = st.calls.filter(c => c.op === 'putSheet').length;
      await nestByHand();
      const rec = record(sheetId), pool2 = st.doc(D('Charm_Pool'), spec.dropPool), set = st.doc(D('Charm_Nest_Sets'), spec.setId), was = await rg();
      const copies = ((set.orders[L] || {}).lines || []).flatMap(l => l.copies || []).map(c => c.poolId), tried = st.calls.filter(c => c.op === 'putSheet').length - puts;
      check(tried > 0 && /taken off this sheet on purpose/.test(was.problem || '') && rec.placements.length === 2 && !rec.placements.some(p => p.id === dropCharm) && pool2.state === 'abandoned' && !copies.includes(spec.dropPool) && rec.roseProtectedJson === guardJson && !rec.rosePlanJson,
        `2 · the stale page's own save is refused and brings neither copy 2 nor line 2 back (${JSON.stringify({ tried, placed: rec.placements.length, pool2: pool2.state, copies, page: was.problem })})`);
      await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1000);
    }
    await page.locator('.sheetCard[data-m="rose"]').screenshot({ path: path.join(SHOTS, `rg-cleanup-${mode}-stale.png`) });

    // 3 · the reload: from the stale copy, the cleanup goes on by itself
    const gets = () => st.calls.filter(c => c.op === 'getSheet' && c.body.id === sheetId).length;
    const saves = () => st.calls.filter(c => c.op === 'putSheet' && c.body.sheet && c.body.sheet.id === sheetId && Array.isArray(c.body.sheet.placements)).length, saves0 = saves();
    await page.reload({ waitUntil: 'load' });
    await ready();
    await page.waitForFunction(([id, cid]) => { const sh = CN.S.sheets.rose.pages.find(p => p.sheetId === id); return !!(sh && sh.cleanups && sh.cleanups[cid]); }, [sheetId, CID], { timeout: 60000 });
    s = await rg();
    const want = mode === 'one' ? [line1.ids[0], keepCharm] : [line1.ids[0], keepCharm, dropCharm];
    check(sorted(s.placed) === sorted(want) && sorted(s.charms) === sorted(want) && !s.plan.length && s.guard.length === 1 && s.guardJson === JSON.stringify(JSON.parse(guardJson)) && s.cleanups[CID].mode === mode,
      `3 · the reload shows the cleaned sheet: ${want.length} pieces, no line 2, line 1 as saved (${JSON.stringify({ placed: s.placed.length, plan: s.plan.length, guard: s.guard.length, applied: s.cleanups[CID] })})`);
    const local = await page.evaluate(([drop, line]) => { const row = B.orders.byKey.get(line); const copies = [...B.sets.values()].flatMap(set => Object.values(set.orders || {}).flatMap(o => Object.values(o.lines || {}).flatMap(l => (l.copies || []).map(c => c.poolId)))); return { cleared: (B.cleared || {})[drop] || null, pooled: B.pool.rows.has(drop), rowPools: row ? row.poolIds : null, pieces: row ? CustomSheet.piecesOf ? CustomSheet.piecesOf(row) : null : null, sent: row ? (Object.values(B.customDesigns).find(e => e.sent && e.sent.lines[line])?.sent.lines[line] || []).map(pc => !!pc.removed) : null, copies, runLine: B.run?.lines?.[line]?.poolIds || null, asked: window.__asked, dialogs: document.querySelectorAll('dialog[open]').length }; }, [spec.dropPool, LINE]);
    if (mode === 'one') check(local.cleared === CID && !local.pooled && JSON.stringify(local.rowPools) === JSON.stringify([spec.keepPool]) && JSON.stringify(local.sent) === '[false,true]' && !local.copies.includes(spec.dropPool) && !(local.runLine || []).includes(spec.dropPool),
      `3 · copy 2 left the pool, the order line, the set and the custom design here (${JSON.stringify(local)})`);
    else check(!local.cleared && local.pooled && JSON.stringify(local.rowPools) === JSON.stringify([spec.keepPool, spec.dropPool]) && JSON.stringify(local.sent) === '[false,false]', `3 · both copies kept here (${JSON.stringify(local)})`);
    check(!local.asked.length && !local.dialogs, `3 · no question and no pop-up (${JSON.stringify(local.asked)})`);

    // 4 · a save works; no line comes by itself. Mode "both" keeps its files (nothing was taken off): Cut Sheet is offered
    //     at once, and a Nest by hand saves the sheet (its card is drawn again at the next redraw, as after any Nest)
    if (mode === 'both') {
      s = await rg();
      check(s.cutButton && !s.plan.length && !s.dirty && s.saved, `4 · with its files as they were, Cut Sheet is offered at once (${JSON.stringify({ cut: s.cutButton, why: s.cutButton ? undefined : s.why })})`);
      await nestByHand();
    }
    await page.waitForFunction(id => { const sh = CN.S.sheets.rose.pages.find(p => p.sheetId === id); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, sheetId, { timeout: 120000 });
    await until(() => { const r = record(sheetId); return r && r.dirty === false && !r.saving && r.placements.length === want.length; }, 30000, 'the saved sheet');
    await page.waitForTimeout(1500);
    const rec4 = record(sheetId); s = await rg();
    if (mode === 'both') { await page.evaluate(id => CN.renderCard(CN.S.sheets.rose.pages.find(p => p.sheetId === id)), sheetId); await page.waitForTimeout(500); s = await rg(); }
    check(saves() > saves0 && rec4.placements.length === want.length && sorted(rec4.placements.map(p => p.id)) === sorted(want) && rec4.roseProtectedJson === guardJson && !rec4.rosePlanJson && !s.error && !s.problem,
      `4 · the sheet saves: ${want.length} pieces in the cloud, line 1 byte for byte, no line 2 (${JSON.stringify({ saves: saves() - saves0, placed: rec4.placements.length, plan: stagesOf(rec4.rosePlanJson), error: s.error, problem: s.problem })})`);
    check(planCalls().filter(c => c === 'cut').length === cutsBefore && !s.plan.length && s.cutButton, `4 · no line comes by itself; Cut Sheet is offered${mode === 'one' ? ' once the sheet is written again' : ''} (${JSON.stringify({ plan: s.plan.length, cut: s.cutButton, why: s.cutButton ? undefined : s.why })})`);

    // 5 · Cut Sheet: exactly one new line, around the lion(s); line 1 as it was
    const t1 = Date.now(), asked = planCalls().length; await cutSheet(); s = await rg();
    const rec5 = record(sheetId), plan5 = rec5.rosePlanJson ? JSON.parse(rec5.rosePlanJson) : null, guard = JSON.parse(guardJson);
    const lionsNow = mode === 'one' ? [keepCharm] : [keepCharm, dropCharm];
    check(plan5 && plan5.stages.length === 2 && JSON.stringify(plan5.stages[0]) === JSON.stringify(guard.stages[0]) && JSON.stringify(plan5.lines.slice(0, guard.lines.length)) === JSON.stringify(guard.lines) && sorted(plan5.stages[1].ids) === sorted(lionsNow) && plan5.stages[1].at >= t1 && planCalls().slice(asked).join() === 'cut' && rec5.roseProtectedJson === guardJson && s.plan.length === 2,
      `5 · Cut Sheet adds exactly one line, around ${lionsNow.length === 1 ? 'the lion' : 'both lions'}; line 1 byte for byte (${JSON.stringify({ stages: plan5 && plan5.stages.map(x => x.ids.length), calls: planCalls().slice(asked), error: s.error })})`);
    {
      const set = st.doc(D('Charm_Nest_Sets'), spec.setId), copies = ((set.orders[L] || {}).lines || []).flatMap(l => l.copies || []).map(c => c.poolId), pool2 = st.doc(D('Charm_Pool'), spec.dropPool);
      const inSet = rec5.setId === spec.setId && !rec5.draft && (set.sheetIds || []).includes(sheetId);
      if (mode === 'one') check(inSet && JSON.stringify(copies) === JSON.stringify([spec.keepPool]) && pool2.state === 'abandoned' && !pool2.sheetId, `5 · the sheet is in its set with the one lion; copy 2 stays off the set and the pool (${JSON.stringify({ inSet, copies, pool2: [pool2.state, pool2.sheetId] })})`);
      else check(inSet && sorted(copies) === sorted([spec.keepPool, spec.dropPool]), `5 · the sheet is in its set with both lions (${JSON.stringify({ inSet, copies })})`);
    }
    await page.locator('.sheetCard[data-m="rose"]').screenshot({ path: path.join(SHOTS, `rg-cleanup-${mode}-cut-sheet.png`) });

    // 6 · a second reload applies nothing again; records with no cleanup, or one applied already, change nothing
    await page.evaluate(() => Session.flush(true)); await page.waitForTimeout(1000);
    const applied = s.cleanups[CID].at, g0 = gets();
    await page.reload({ waitUntil: 'load' });
    await ready(); await page.waitForTimeout(4000);
    s = await rg();
    const same = await page.evaluate(async id => {
      const sh = CN.S.sheets.rose.pages.find(p => p.sheetId === id), look = () => JSON.stringify([sh.placements.map(p => p.id), sh.rosePlanHash || null, sh.cleanups, sh.dirty]);
      const was = look(); await Cleanups.seen([{ id }, { id, cleanup: null }, { id: 'rose-none', cleanup: { id: 'another' } }, { id, cleanup: { id: 'rg-cleanup-2026-09-29' } }]); await Cleanups.check();
      return was === look();
    }, sheetId);
    check(s.cleanups[CID].at === applied && gets() === g0 && s.plan.length === 2 && same, `6 · a second reload and more reads apply nothing again (${JSON.stringify({ gets: gets() - g0, plan: s.plan.length })})`);
    check(!errors.length, 'no page errors ' + errors.join(' | '));
  } finally { try { fs.writeFileSync(path.join(SHOTS, `rg-cleanup-${mode}-console.log`), said.join('\n')); } catch (_) {} await context.close(); srv.close(); }
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try { for (const mode of process.argv[2] ? [process.argv[2]] : ['one', 'both']) await run(mode, browser); }
  finally { await browser.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nrg-cleanup-0929-page OK: a stale page takes the cloud\'s cleanup on reload, saves, and Cut Sheet adds exactly one line');
})().catch(e => { console.error(e); process.exit(1); });
