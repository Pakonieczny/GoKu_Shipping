// Only Cut Sheet adds a Rose Gold green line (Paul, 29 Sep 01:26 UTC: "approved and sent to sheet ... it also incorrectly
// auto activated a second green line ... that has to be activated by the user"). In a real Chromium on the fake site
// (bridge-server.cjs, the real charmNestLibrary and Rose stock handlers over an in-memory store; no Etsy, no model):
//   1 · a custom design sent to Rose Gold (Custom Orders → Send to Sheet, the tour plays) is nested; the sheet joins the
//       set (Options Include): no line;
//   2 · Cut Sheet: exactly one dated line; the cut itself is refused (the set has no QR labels yet), as on Paul's sheet;
//   3 · a second design sent to the same sheet is nested past line 1; the set is assembled again, the labels/commit
//       step's contour check runs and the allowance is changed: still line 1 only, the new charm shown uncut;
//   4 · a page opened before this rule asks the server for a contour on its own: refused, nothing written;
//   5 · Cut Sheet again: exactly one more dated line, around the second charm only; the sheet stays one block from
//       the left.
// Screenshots to SHOTS (default /mnt/project-files/plans/rg-green-line).
//   node tests/charm-nest/rose-line-send.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const Rose = require(path.join(here, 'netlify/functions/_charmNestRoseStock.js'));
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/rg-green-line';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold Filled' }], metalKey: 'rose', metalLabel: 'RG 14/20', personalization: [], buyerMessage: '' }] });
const A = '4175423829', Bo = '4175423830';

(async () => {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  const fails = [], check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  try {
    fs.mkdirSync(SHOTS, { recursive: true });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(30000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.RoseStock && window.Gate && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      // a run under way, past its pool step: a design sent now goes onto its sheet at once
      const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-rg`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, [order(A, 5), order(Bo, 4)]);
    const rg = () => page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); return { sheetId: sh.sheetId, placed: sh.placements.map(p => ({ id: p.id, x: p.cxPt })), plan: (sh.rosePlan?.stages || []).map(s => ({ n: s.n, at: s.at, ids: s.ids })), guard: (sh.roseProtected?.stages || []).map(s => ({ n: s.n, ids: s.ids })), marks: [...(sh.el?.querySelectorAll('.roseLineTimeline .roseMark') || [])].map(m => m.querySelector('time')?.getAttribute('datetime') || m.textContent.trim()), cutButton: !!sh.el?.querySelector('.roseCut:not([hidden]) [data-rose="cut"]'), inSet: !!sh.setId && !sh.draft, error: sh._roseError || null, cutAt: sh.roseCutAt || null }; });
    const planCalls = () => srv.st.calls.filter(c => c.op === 'rosePlan').map(c => c.body.cut === true ? 'cut' : 'plain');
    const record = id => srv.st.doc('Charm_Nest_Sheets', id) || srv.st.doc('Sandbox_Charm_Nest_Sheets', id);
    const stagesOf = json => json ? (JSON.parse(json).stages || []).map(s => s.ids.length) : null;
    async function send(rid, name) {
      await page.evaluate(() => CN.setMode('review'));
      await page.click('#reviewView .egTab[data-k="customOrder"]');
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`;
      await page.waitForSelector(card);
      await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(18), name });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click('#cuDlg .cuFile .cuM[data-m="rose"]');
      await page.click('#cuDlg [data-send]');
      await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
      // the run is in Manual: the sheet waits for Nest
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, null, { timeout: 30000 });
      await page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); });
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, null, { timeout: 120000 });
      await page.evaluate(() => CN.setMode('nest'));
      await page.waitForTimeout(1500);
    }
    const cutSheet = async () => {
      await page.click('.sheetCard[data-m="rose"] [data-rose="cut"]');
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return !sh._roseAction && !sh._rosePlanning && !sh._roseStep; }, null, { timeout: 60000 });
      await page.waitForTimeout(800);
    };

    // 1 · sent, nested, into the set: no line
    await send(A, 'a.dxf');
    await page.evaluate(() => Gate.changeMembership('rose', true));
    await page.waitForTimeout(2000);
    let s = await rg();
    check(s.placed.length === 1 && s.inSet && !s.plan.length && !s.marks.length && s.cutButton && !planCalls().length, `1 · sent and in the set: no green line until Cut Sheet (${JSON.stringify({ inSet: s.inSet, plan: s.plan, marks: s.marks, calls: planCalls() })})`);

    // 2 · Cut Sheet: one dated line (the cut itself waits for the set's labels)
    const t0 = Date.now(); await cutSheet(); s = await rg();
    const line1 = s.plan[0];
    check(s.plan.length === 1 && line1.at >= t0 && s.marks.length === 1 && planCalls().join() === 'cut' && !s.cutAt, `2 · Cut Sheet adds exactly one dated line (${JSON.stringify({ plan: s.plan.map(p => p.n), marks: s.marks, calls: planCalls(), error: s.error })})`);

    // 3 · a second design sent to the same sheet: nested past line 1, the set assembled again, the labels/commit step's
    //     contour check and a new allowance: line 1 only, the new charm uncut
    await send(Bo, 'b.dxf');
    await page.evaluate(async () => { const sh = CN.S.sheets.rose.pages.at(-1); sh._roseError = null; await Gate.assemble(B.run); CN.renderCard(sh); });
    await page.waitForTimeout(1500);
    await page.evaluate(async () => { await Gate.flush(B.run).catch(() => {}); await Gate.changeMembership('rose', true).catch(() => {}); });
    await page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); const box = sh.el.querySelector('[data-rose-allowance]'); if (box) { box.value = '0.3'; box.dispatchEvent(new Event('change')); } });
    await page.waitForTimeout(2500);
    s = await rg();
    const newId = s.placed.map(p => p.id).find(id => !line1.ids.includes(id)), rec3 = record(s.sheetId);
    check(s.placed.length === 2 && s.inSet && s.guard.length === 1 && !s.plan.length && s.marks.length === 1 && s.marks[0] === new Date(line1.at).toISOString() && planCalls().join() === 'cut' && s.cutButton,
      `3 · second send, set assembled, commit check, new allowance: still line 1 only, the new charm uncut, Cut Sheet offered (${JSON.stringify({ plan: s.plan.map(p => p.n), guard: s.guard.map(p => p.n), marks: s.marks, calls: planCalls() })})`);
    check(rec3 && !rec3.rosePlanJson && JSON.stringify(stagesOf(rec3.roseProtectedJson)) === '[1]', `3 · the saved sheet keeps line 1 and no plan for the new charm (${rec3 && JSON.stringify({ plan: stagesOf(rec3.rosePlanJson), guard: stagesOf(rec3.roseProtectedJson) })})`);
    await page.locator('.sheetCard[data-m="rose"]').screenshot({ path: path.join(SHOTS, 'rg-after-send.png') });

    // 3 · the set waits for that press and says so by name (Paul could not tell what "…layout checks…" wanted): its
    //     release reason and the run's pill, whose words open the sheet at its Cut Sheet button
    const wait = await page.evaluate(() => {
      const sh = CN.S.sheets.rose.pages.at(-1), r = B.run, was = r.status, issue = Sets.releaseIssue(Sets.setOfSheet(sh));
      CN.setMode('review'); r.status = 'processed'; RunCtl.renderBanner();   // (the run rests there once its set is deferred)
      const pill = document.querySelector('#runBanner .rbText')?.textContent, why = document.querySelector('#runBanner .rbWhy')?.textContent;
      document.querySelector('#runBanner [data-rbcut]')?.click(); r.status = was; RunCtl.renderBanner();
      return new Promise(done => requestAnimationFrame(() => requestAnimationFrame(() => done({ issue, pill, why, mode: CN.S.mode, focus: document.activeElement?.matches('.sheetCard[data-m="rose"] [data-rose="cut"]') }))));
    });
    const words = 'Rose Gold Sheet 1 has 1 charm not cut yet: press Cut Sheet';
    check(wait.issue === words && wait.pill === 'Waiting on you · Cut Sheet' && wait.why.endsWith(words) && wait.mode === 'nest' && wait.focus,
      `3 · the set's wait names the sheet and Cut Sheet, and its words open the sheet at the button (${JSON.stringify(wait)})`);

    // 4 · a page opened before this rule asks for the contour on its own: refused, nothing written
    const before = JSON.stringify(record(s.sheetId));
    const old = await page.evaluate(async fp => { const sh = CN.S.sheets.rose.pages.at(-1); try { await CN.api('charmNestLibrary', { op: 'rosePlan', sheetId: sh.sheetId, stockId: sh.roseStock.id, revision: sh.roseStock.revision, fingerprint: fp, shapesJson: JSON.stringify(CharmNestRose.shapes(sh.charms, sh.placements)), allowanceMm: 0.2 }); return 'accepted'; } catch (e) { return e.message; } }, Rose.fingerprint(record(s.sheetId)));
    check(/Only Cut Sheet adds a green line/.test(old) && JSON.stringify(record(s.sheetId)) === before, `4 · an old page's own contour request is refused and writes nothing (${old})`);

    // 5 · Cut Sheet again: exactly one more dated line, around the new charm only
    const t1 = Date.now(), asked = planCalls().length; await cutSheet(); s = await rg();
    const rec5 = record(s.sheetId);
    check(s.plan.length === 2 && s.plan[0].at === line1.at && s.plan[1].at >= t1 && JSON.stringify(s.plan[1].ids) === JSON.stringify([newId]) && s.marks.length === 2 && planCalls().slice(asked).join() === 'cut' && JSON.stringify(stagesOf(rec5.rosePlanJson)) === '[1,1]',
      `5 · the next Cut Sheet adds exactly one dated line, around the new charm (${JSON.stringify({ plan: s.plan.map(p => [p.n, p.ids.length]), marks: s.marks, calls: planCalls().slice(asked), error: s.error })})`);
    const wPt = await page.evaluate(() => CN.stockFor('rose', CN.S.sheets.rose.pages.at(-1)).wPt), xs = s.placed.map(p => p.x);
    check(Math.max(...xs) < wPt * 0.5, `5 · the partial sheet stays one block from the left (charm centres at ${xs.map(x => Math.round(x)).join(', ')} pt of ${Math.round(wPt)})`);
    await page.locator('.sheetCard[data-m="rose"]').screenshot({ path: path.join(SHOTS, 'rg-after-second-cut-sheet.png') });
    check(!errors.length, 'no page errors ' + errors.join(' | '));
  } finally { await context.close(); await browser.close(); srv.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nRose line send OK: only Cut Sheet adds a green line; sending, placing, set assembly, the commit check, a new allowance and old pages add none');
})().catch(e => { console.error(e); process.exit(1); });
