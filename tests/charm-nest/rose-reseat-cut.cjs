// Cut Sheet after "Use this one" (Paul, 7 Oct 2026: "I could not cut the sheet after placing new pieces on the partial sheet"; the card read "Filling" with
// no Set number and a red "Not cut: Included by you"). In a real Chromium on the fake site (bridge-server.cjs: the real charmNestLibrary, Rose stock and
// remnant handlers over an in-memory store that, like Firestore, refuses an array inside an array):
//   1 · a custom design is sent to Rose Gold, nested, included and CUT (cut 1: a stock revision, a cuts document, a leftover record);
//   2 · the set is committed (scenario "committed"), or stays the run's open set (scenario "current");
//   3 · the sheet is moved onto the leftover its own cut made (PartialNest.seat, the code behind Use this one) and nested again;
//   4 · the sheet is still in its set (committed) with its Set number and file name, and Cut Sheet records cut 2: its own cuts document and its own
//       leftover, cut 1 untouched, both cuts on the card with their dates, no error;
//   5 · scenario "page copy lost its place": the page copy as Paul's was (drafted out of the set by the old nextSheetSeq) takes its place back and cuts.
// Nothing here may be silent: a refusal leaves one plain line under the button (the jsdom suite rose-ui.cjs checks the words).
//   node tests/charm-nest/rose-reseat-cut.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const Readiness = require(path.join(here, 'charm-nest-readiness'));
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: 'Rose Gold Filled' }], metalKey: 'rose', metalLabel: 'RG 14/20', personalization: [], buyerMessage: '' }] });
const A = '4175423829';
// the readiness stages (QR labels, production checks) are not what is tested here; the first cut works live
const readinessReal = Readiness.sheet; Readiness.sheet = s => ({ ...readinessReal(s), ready: true });
const fails = [], check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };

async function scenario(name, { commit, lost }) {
  console.log('\n' + name);
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  // the store refuses what Firestore refuses
  const put = srv.st.docs.set.bind(srv.st.docs); srv.st.docs.set = (k, v) => put(k, refuseNestedArrays(v, k));
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  try {
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
      const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-rg`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, [order(A, 5)]);
    // a custom design sent to Rose Gold (Custom Orders → Send to Sheet), nested
    await page.evaluate(() => CN.setMode('review'));
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList .reviewListRow[data-rid="${A}"]`;
    await page.waitForSelector(card);
    await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(18), name: 'a.dxf' });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="rose"]');
    await page.click('#cuDlg [data-send]');
    await page.waitForFunction(() => !document.querySelector('#cuDlg').open && !document.querySelector('#tourLayer > *'), null, { timeout: 30000 });
    await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.charms.length && !['nesting', 'finishing', 'queued'].includes(sh.status); }, null, { timeout: 30000 });
    const nested = () => page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return sh.status === 'complete' && sh.persistedDone && sh.verification?.ok && !sh.dirty; }, null, { timeout: 120000 });
    await page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); if (sh.status !== 'complete' || sh.dirty) CN.startNest(sh); });
    await nested();
    await page.evaluate(() => CN.setMode('nest'));
    await page.waitForTimeout(1500);
    await page.evaluate(() => Gate.changeMembership('rose', true));
    await page.waitForTimeout(2000);
    const sid = await page.evaluate(() => CN.S.sheets.rose.pages.at(-1).sheetId);
    const rec = () => srv.st.doc('Charm_Nest_Sheets', sid) || srv.st.doc('Sandbox_Charm_Nest_Sheets', sid);
    const snap = () => page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); return { err: sh._roseError || null, cutAt: sh.roseCutAt || null, draft: !!sh.draft, setId: sh.setId || null, seq: sh.seq || null, fileBase: sh.fileBase, stockId: sh.roseStock && sh.roseStock.id, rev: sh.roseStock && sh.roseStock.revision, hist: (sh.roseHistory || []).map(c => c.revision).sort(), dates: sh.el ? [...sh.el.querySelectorAll('.roseLineTimeline .roseMark.cut .roseWhen')].map(n => n.textContent.trim()) : [], alert: sh.el && sh.el.querySelector('.roseError') ? sh.el.querySelector('.roseError').textContent : null }; });
    const cutSheet = async () => {
      await page.evaluate(() => CN.renderCard(CN.S.sheets.rose.pages.at(-1)));
      await page.click('.sheetCard[data-m="rose"] [data-rose="cut"]');
      await page.waitForFunction(() => { const sh = CN.S.sheets.rose.pages.at(-1); return !sh._roseAction && !sh._rosePlanning && !sh._roseStep; }, null, { timeout: 60000 });
      await page.waitForTimeout(800);
    };
    const keys = re => [...srv.st.docs.keys()].filter(k => re.test(k)).sort();

    // 1 · cut 1
    await cutSheet();
    const one = await snap(); check(!one.err && one.cutAt && one.hist.join() === '1', `cut 1 recorded (history ${one.hist}, error ${one.err})`);
    const cutsRe = /Rose_Stock\/[^/]+\/cuts\/.+$/, remRe = /Remnants\/.+$/;
    const cut1Key = keys(cutsRe)[0], cut1 = JSON.stringify(srv.st.docs.get(cut1Key)), rem1 = keys(remRe);
    check(keys(cutsRe).length === 1 && rem1.length === 1, `one cuts document and one leftover record (${keys(cutsRe).length}, ${rem1.length})`);

    // 2 · the set
    if (commit) await page.evaluate(async () => { const set = Sets.ofRun(B.run.runId)[0]; set.committedAt = Date.now(); set.status = 'committed'; await Sets.save(set); });
    const before = await snap();

    // 3 · Use this one, on the leftover its own cut made
    const seat = await page.evaluate(async () => {
      const sh = CN.S.sheets.rose.pages.at(-1), list = await PartialSheets.list('rose', { force: true }), item = (list.items || list.cards || []).find(c => c.status === 'available');
      if (!item) return { err: 'no available leftover' };
      const r = await PartialNest.seat(sh, [item.id], {}); return { ok: r.ok, code: r.code, why: r.why };
    });
    check(seat.ok === true, `Use this one seats the sheet on its own leftover (${JSON.stringify(seat)})`);
    await nested(); await page.waitForTimeout(1500);
    const mid = await snap();
    check(!mid.cutAt, 'the re-seat set the old cut aside (no cut on the card yet)');
    if (commit) {
      check(!mid.draft && mid.setId === before.setId && mid.seq === before.seq && !!mid.seq, `the committed set's sheet keeps its place in Set ${before.seq} after it is nested again`);
      check(!/working/.test(mid.fileBase || '') && mid.fileBase === before.fileBase, `and its file name (${mid.fileBase})`);
    } else {
      check(mid.draft || mid.setId, 'the open set\'s sheet is held again by the nest (as before): Cut Sheet puts it back');
    }
    if (lost) {
      await page.evaluate(() => { const sh = CN.S.sheets.rose.pages.at(-1); Object.assign(sh, { draft: true, setId: null, seq: null, sheetIndex: null, fileBase: 'RG_working_' + sh.sheetId }); });
    }
    check(rec().setId === before.setId && !rec().draft || !commit, 'the saved record is still in its set');

    // 4 · cut 2
    await cutSheet();
    const two = await snap();
    check(!two.err && !two.alert, `Cut Sheet records the cut with no refusal (${two.err || two.alert || 'none'})`);
    check(two.cutAt && two.cutAt > one.cutAt, 'the card shows the new green line');
    check(two.hist.join() === '1,2', `the card keeps both cuts (history ${two.hist})`);
    check(two.dates.length === 2 && two.dates.every(d => /^Cut .*\d/.test(d) && !/not recorded/i.test(d)), `each cut shows its number and date (${two.dates.join(' | ')})`);
    check(!two.draft && !!two.setId, 'the sheet is in its set');
    const cuts = keys(cutsRe), rems = keys(remRe);
    check(cuts.length === 2 && cuts.includes(cut1Key), `its own cuts document beside the first (${cuts.map(k => k.split('/').pop())})`);
    check(JSON.stringify(srv.st.docs.get(cut1Key)) === cut1, 'cut 1\'s document is untouched');
    check(rems.length === 2 && rems.includes(rem1[0]), `its own leftover record beside the first (${rems.map(k => k.split('/').pop())})`);
    check(JSON.stringify(rec().roseReseated || []).length > 2, 'the re-seat record is kept on the sheet');
    check(errors.length === 0, 'no page errors' + (errors.length ? ': ' + errors[0] : ''));
  } finally { await context.close(); await browser.close(); srv.close(); }
}

(async () => {
  await scenario('committed set: Use this one on its own leftover, then Cut Sheet', { commit: true });
  await scenario('open (current) set: the same', { commit: false });
  await scenario('committed set, the page copy already lost its place (Paul\'s page): Cut Sheet takes it back', { commit: true, lost: true });
  console.log(fails.length ? `\n${fails.length} FAILED:\n - ${fails.join('\n - ')}` : '\nrose-reseat-cut: ok');
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
