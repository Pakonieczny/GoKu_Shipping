// ENGWHY (engrave-ui-1012 round 2, Paul point 7, first half): "If a user tries to override the auto selection engraving options then there should be a quick prompt asking the user for a reason. There should be
// 3-4 quick clickable options for the user to choose from and a small input text box if the 3-4 shortcut buttons aren't sufficient. This needs to be recorded in firebase, be mindful of how much API usage we
// are using for firebase." The REAL page (charm-nest-1.html, headless Chromium, offline fake site over the real charmNestLibrary handlers and an in-memory Firestore that refuses nested arrays; no Etsy, no paid
// call, nothing live). The run is a finished one, so RunCtl.poke does not start more run steps (their saves are the same with or without a reason; they would only blur the count). Order: Mini Tag "Tag1 (front+back)", note "front - W / back - 1. 27. 24", Ace: the system chooses Front + back, W, 1. 27. 24.
//   1  changing what the system chose and pressing Save shows the reason prompt IN the panel: 3-4 quick buttons, one small text box, Save, Cancel; nothing is written yet
//   2  Cancel and Esc go back to the panel with the edit still staged
//   3  a quick button saves at once with its code, by the signed-in login, with what the system had chosen (`was`) and what was changed (`of`), in the same override record, all flat (no nested arrays)
//   4  the text box saves custom text (code "other"); `was` is the SYSTEM's choice, not the earlier override
//   5  a save equal to the system's choice asks nothing and drops the reason
//   6  WRITE COUNT: the same save through the old path (no reason) and through the prompt: the same Firestore document writes and deletes, the same function calls, the same reads and queries
//   7  no listener, no poll: nothing at all is sent while the prompt is open
//   8  a record without a reason keeps working (read, panel, quiet line)
//   9  a position move that is an override asks too (the one case that has no write today: one run save, counted and printed)
//   10 a wait shows the small labelled "Saving…", a failure is one plain line and the staged edit stays (Save again does not ask again)
//   11 the reason is in the order timeline's existing "edited by" line (no extra timeline write) and read-only afterwards (one quiet line)
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-why.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const Why = require(path.join(root, 'charm-nest-engrave-why.js'));
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - W\nback - 1. 27. 24'] }] };

(async () => {
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

  // ── the pure half, in node ──
  check(Why.REASONS.length >= 3 && Why.REASONS.length <= 4 && Why.REASONS.every(r => r.code && r.label.length <= 24), 'the quick buttons are 3-4 short plain labels: ' + Why.REASONS.map(r => r.label).join(' | '));
  { const out = { by: 'Name One', at: 5 }, prev = { reason: { code: 'typo', text: 'Spelling fix', by: 'Old', at: 1 }, was: { mode: 'back' } };
    Why.fold(out, prev, { reason: { code: 'asked', text: 'Customer asked', of: 'back' }, was: { mode: 'both', front: { text: 'W', font: 'a' }, back: { text: '1', font: 'a' } }, slot: 'D2' });
    check(out.reason.code === 'asked' && out.reason.by === 'Name One' && out.reason.at === 5 && out.reason.slot === 'D2' && out.was.mode === 'both' && out.was.back.font === 'a', 'fold: a patch with a reason sets reason (by, at of the save) and was: ' + JSON.stringify(out));
    const keep = Why.fold({ by: 'x', at: 6 }, prev, { mode: 'front' }); check(keep.reason === prev.reason && keep.was === prev.was, 'fold: a patch that says nothing about it (any other caller) keeps what the line had');
    const gone = Why.fold({ by: 'x', at: 6 }, prev, { mode: 'front', reason: null, was: null }); check(!('reason' in gone) && !('was' in gone), 'fold: reason: null drops both (the save is equal to the system\'s choice)');
    const long = Why.fold({ by: 'x', at: 6 }, null, { reason: { code: 'other', text: 'y'.repeat(500) } }); check(long.reason.text.length === Why.MAX_TEXT, 'a custom reason is cut at ' + Why.MAX_TEXT + ' characters');
    const bad = Why.fold({ by: 'x', at: 6 }, null, { reason: { code: 'nonsense', text: 'z' } }); check(!bad.reason, 'an unknown code is not stored'); }
  check(Why.reasonOf({ engravingOverride: { mode: 'back' } }) === null && Why.reasonOf({}) === null, 'no reason in the record: reasonOf is null (old records keep working)');
  { const r = Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'Customer asked', by: 'Name One', at: 1791847578705 }, was: { mode: 'both' } } });
    check(r && r.line === 'Changed: Customer asked · Name One' && /Customer asked/.test(r.title) && r.was.mode === 'both', 'reasonOf gives the quiet line and a title: ' + JSON.stringify({ line: r && r.line, title: r && r.title })); }
  check(Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'x', by: 'a', at: 1, slot: 'D1' } } }, 'D2') === null && !!Why.reasonOf({ engravingOverride: { reason: { code: 'asked', text: 'x', by: 'a', at: 1, slot: 'D1' } } }, 'D1'), 'a reason written for one piece is not another piece\'s');

  // ── the page ──
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-why: playwright-core not found, the page checks were skipped'); process.exit(fails.length ? 1 : 0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [], strictArrays: true });   // (a document with an array inside an array is refused, as Firestore refuses it)
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-why-1', { id: 'gold-why-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-why-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-why-1', state: 'placed', material: 'gold' });
  // every Firestore document write and delete of the in-memory database, by key
  const W = { sets: [], dels: [] }, dset = srv.st.docs.set.bind(srv.st.docs), ddel = srv.st.docs.delete.bind(srv.st.docs);
  srv.st.docs.set = (k, v) => { W.sets.push(k); return dset(k, v); }; srv.st.docs.delete = k => { W.dels.push(k); return ddel(k); };
  const snap = () => ({ sets: W.sets.length, dels: W.dels.length, reads: srv.st.reads || 0, queries: srv.st.queries || 0, calls: srv.st.calls.length });
  const since = a => { const calls = {}; for (const c of srv.st.calls.slice(a.calls)) { const k = c.name + ':' + (c.op || (c.body && c.body.timeline ? 'timeline' : c.q && c.q.cancelCheck ? 'cancelCheck' : '-')); calls[k] = (calls[k] || 0) + 1; } return calls; };
  const delta = a => { const b = snap(); const kinds = ks => ks.reduce((m, k) => { const c = k.split('/')[0]; m[c] = (m[c] || 0) + 1; return m; }, {}); return { sets: b.sets - a.sets, dels: b.dels - a.dels, reads: b.reads - a.reads, queries: b.queries - a.queries, calls: since(a), setKinds: kinds(W.sets.slice(a.sets)), delKinds: kinds(W.dels.slice(a.dels)) }; };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    // the page's own time-driven background calls (the cancel feed, the order window's piece look-up, the stations' GET) are answered empty here and never reach the fake: what is left in the counts is what a SAVE does
    await page.route(/\/\.netlify\/functions\/(charmNestLibrary|firebaseOrders)/, r => {
      const q = r.request(), body = q.postData() || '', bg = (q.method() === 'GET' && /firebaseOrders/.test(q.url())) || /"op"\s*:\s*"(cancelList|getOrderPieces)"/.test(body);
      return bg ? r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify({ success: true, ok: true, rows: [], pieces: [], cancels: [], orders: [] }) }) : r.continue();
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngraveSetup && window.CharmNestEngravePanel && window.CharmNestEngraveWhy && CharmNestEngraveSetup.autoChoice && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the order (read by the real intake), its back job, and an open run (so a save reaches the one run save)
    await page.evaluate(async ({ order, KEY, RID }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      window.__job0 = { state: 'review', text: '1. 27. 24', lines: ['1. 27. 24'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.9, decision: null, faceFontWord: null };
      const j = Engrave.ensureJob(row); Object.assign(j, JSON.parse(JSON.stringify(__job0)));
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-why`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'engrave', status: 'complete', processingComplete: true, finishedAt: Date.now(), mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [RID] };
      B.run.lines[KEY] = Orders.lineRecord(row)[1];
      window.__calls = []; const call = DesignLink.call; DesignLink.call = function (name) { __calls.push(name); return call.apply(this, arguments); };
      window.__tl = []; if (window.OrderTimeline && OrderTimeline.record) { const rec = OrderTimeline.record; OrderTimeline.record = function (e) { __tl.push(JSON.parse(JSON.stringify(e))); return rec.apply(this, arguments); }; }
    }, { order: ORDER, KEY, RID });
    // put the line back as it was, with a saved run and the one live part it holds
    const reset = async () => {
      await page.evaluate(async ({ KEY }) => {
        const row = B.orders.byKey.get(KEY); delete row.engravingOverride; CharmNestEngravePanel.DRAFTS.clear();
        const j = Engrave.jobOf(row, null, 'back'); Object.assign(j, JSON.parse(JSON.stringify(__job0)));
        B.run.lines[KEY] = Orders.lineRecord(row)[1]; await RunCtl.save(B.run); __tl.length = 0;
      }, { KEY });
      await page.waitForTimeout(2200);   // (the order timeline's idle send)
    };
    const settle = async (a, wantRunPut = true) => {
      const t0 = Date.now(); while (Date.now() - t0 < 8000) { if (!wantRunPut || srv.st.calls.slice(a.calls).some(c => c.op === 'runPut')) break; await page.waitForTimeout(50); }
      await page.waitForTimeout(3000);   // (the timeline's idle send, the redraws)
    };
    const open = async () => {
      await page.evaluate(() => { CharmNestEngravePanel.DRAFTS.clear(); setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: null }); Engrave.render(); });
      await page.waitForSelector('.pvSetup .engp .engpWords', { timeout: 20000 });
    };
    const P = '.pvSetup';
    const ovOf = () => page.evaluate(KEY => { const o = B.orders.byKey.get(KEY).engravingOverride; return o ? JSON.parse(JSON.stringify(o)) : null; }, KEY);
    const prompt = () => page.evaluate(() => { const w = document.querySelector('.pvSetup .engpWhy'); if (!w) return null; const r = e => e.getBoundingClientRect(); const chips = [...w.querySelectorAll('.engpChip')], inp = w.querySelector('.engpWhyText'), bs = [...w.querySelectorAll('.engpWhyRow button')]; const p = document.querySelector('.pvSetup .engp');
      return { chips: chips.map(c => c.textContent.trim()), codes: chips.map(c => c.dataset.code), input: !!inp, ph: inp && inp.placeholder, btns: bs.map(b => b.textContent.trim()), mainActs: !!p.querySelector('.engpActs [data-engp=save]'), text: w.textContent.replace(/\s+/g, ' '), w: r(w).width, over: chips.concat(inp ? [inp] : [], bs).some(e => r(e).right > r(p).right + 0.5), inView: r(w).bottom <= innerHeight, bottom: Math.round(r(w).bottom), winH: innerHeight }; });

    // ═══ 6a · the OLD way (what the panel did before this change: the patch alone, no reason): the write count to match ═══
    await reset();
    const patchBack = { back: { text: '1. 28. 24', fontWord: null } };
    let a = snap();
    await page.evaluate(({ KEY, p }) => Engrave.saveSetup(B.orders.byKey.get(KEY), p), { KEY, p: patchBack });
    await settle(a);
    const oldWay = delta(a), oldOv = await ovOf();
    check(oldOv && oldOv.back && oldOv.back.text === '1. 28. 24' && !oldOv.reason && !oldOv.was, 'the old way saves the words with no reason in the record: ' + JSON.stringify(oldOv && { back: oldOv.back.text, reason: oldOv.reason }));
    check(oldWay.calls['charmNestLibrary:runPut'] === 1, 'the old way is ONE run save (runPut): ' + JSON.stringify(oldWay.calls));
    console.log('    old way: ' + JSON.stringify(oldWay));

    // ═══ 1 · the prompt ═══
    await reset(); await open();
    await page.fill(P + ' .engpWords', '1. 28. 24');
    a = snap();
    await page.click(P + ' [data-engp=save]');
    await page.waitForSelector(P + ' .engpWhy');
    let pr = await prompt();
    check(pr && pr.chips.length >= 3 && pr.chips.length <= 4, 'changing the system\'s words and pressing Save asks "Why change it?" with 3-4 quick buttons, in the panel: ' + JSON.stringify(pr && pr.chips));
    check(pr.input && /else/i.test(pr.ph) && pr.btns.join('|') === 'Save|Cancel', 'one small text box ("' + pr.ph + '") with its own Save and a Cancel: ' + pr.btns.join('|'));
    check(!pr.mainActs && /^Why change it\?/.test(pr.text), 'the prompt replaces the Save / Cancel row (no pop-up): "' + pr.text + '"');
    check(!pr.over && pr.inView, 'it fits the panel\'s width: nothing sticks out (over ' + pr.over + ', in view ' + pr.inView + ', bottom ' + pr.bottom + ' / window ' + pr.winH + ')');
    check((await ovOf()) === null && srv.st.calls.slice(a.calls).filter(c => c.op === 'runPut').length === 0, 'nothing is saved while the prompt is open');
    if (shots) await page.locator(P + ' .engp').screenshot({ path: path.join(shots, 'why-prompt.png') });

    // ═══ 7 · nothing is sent while the prompt waits (no listener, no poll): a window with the prompt open sends exactly what a window without it sends (the page's own background calls) and writes nothing ═══
    const quiet = snap(); await page.waitForTimeout(2500);
    const q = delta(quiet);
    await page.click(P + ' [data-engp=whyCancel]'); const idle0 = snap(); await page.waitForTimeout(2500); const q0 = delta(idle0);
    await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    check(q.sets === 0 && q.dels === 0 && JSON.stringify(q.calls) === JSON.stringify(q0.calls) && q.reads === q0.reads && q.queries === 0, 'while the prompt waits: no write, delete, read, query or call, the same as a window without the prompt (open: ' + JSON.stringify({ sets: q.sets, dels: q.dels, reads: q.reads, calls: q.calls }) + ' / closed: ' + JSON.stringify({ reads: q0.reads, calls: q0.calls }) + ')');

    // ═══ 2 · Cancel and Esc go back with the edit staged ═══
    await page.click(P + ' [data-engp=whyCancel]');
    let back = await page.evaluate(() => { const p = document.querySelector('.pvSetup .engp'); return { why: !!p.querySelector('.engpWhy'), text: p.querySelector('.engpWords').value, save: p.querySelector('[data-engp=save]') && !p.querySelector('[data-engp=save]').disabled, focus: document.activeElement && document.activeElement.dataset && document.activeElement.dataset.engp }; });
    check(!back.why && back.text === '1. 28. 24' && back.save, 'Cancel returns to the panel with the edit still staged (words kept, Save on): ' + JSON.stringify(back));
    await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    await page.keyboard.press('Escape');
    back = await page.evaluate(() => { const p = document.querySelector('.pvSetup .engp'); return { why: !!p.querySelector('.engpWhy'), text: p.querySelector('.engpWords').value, save: p.querySelector('[data-engp=save]') && !p.querySelector('[data-engp=save]').disabled }; });
    check(!back.why && back.text === '1. 28. 24' && back.save, 'Esc does the same: ' + JSON.stringify(back));
    check((await ovOf()) === null, 'still nothing saved');

    // ═══ 10 · the wait is labelled, a failure is one plain line, the edit stays and Save again does not ask again ═══
    await page.evaluate(() => { const real = Engrave.saveSetup; window.__real = real; window.__fail = 1; Engrave.saveSetup = async (row, patch) => { await new Promise(r => setTimeout(r, 700)); if (__fail-- > 0) return { ok: false, message: 'Not saved: the test said so' }; return real(row, patch); }; });
    await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    await page.click(P + ' [data-engp=why][data-code=asked]');
    const busy = await page.evaluate(() => { const p = document.querySelector('.pvSetup .engp'), s = p.querySelector('.engpStat'); return { stat: s && s.textContent.trim(), spin: !!(s && s.querySelector('.owSpin')), saveOff: p.querySelector('[data-engp=save]').disabled, why: !!p.querySelector('.engpWhy') }; });
    check(busy.stat === 'Saving…' && busy.spin && busy.saveOff && !busy.why, 'a chip saves at once: the small labelled spinner "Saving…" shows while it waits: ' + JSON.stringify(busy));
    await page.waitForSelector(P + ' .engpErr');
    const fail = await page.evaluate(() => { const p = document.querySelector('.pvSetup .engp'); return { err: [...p.querySelectorAll('.engpErr')].map(e => e.textContent.trim()), text: p.querySelector('.engpWords').value, save: !p.querySelector('[data-engp=save]').disabled, why: !!p.querySelector('.engpWhy') }; });
    check(fail.err.length === 1 && /Not saved/.test(fail.err[0]) && fail.text === '1. 28. 24' && fail.save && !fail.why, 'a failure is one plain line and the staged edit stays: ' + JSON.stringify(fail));
    a = snap();
    await page.click(P + ' [data-engp=save]');
    const again = await page.evaluate(() => !!document.querySelector('.pvSetup .engpWhy'));
    check(!again, 'Save again retries with the same answer: it does not ask again');
    await page.waitForFunction(KEY => !!B.orders.byKey.get(KEY).engravingOverride, KEY, { timeout: 20000 });
    await settle(a);
    const newWay = delta(a), ov1 = await ovOf();

    // ═══ 3 · the record ═══
    check(ov1 && ov1.reason && ov1.reason.code === 'asked' && ov1.reason.text === 'Customer asked' && ov1.reason.by === 'Test Operator' && ov1.reason.at > 1e12 && ov1.reason.at === ov1.at && ov1.reason.of === 'back', 'a quick button saved with its code, by the signed-in login (never typed), at the save\'s instant, saying what was changed: ' + JSON.stringify(ov1 && ov1.reason));
    check(ov1.was && ov1.was.mode === 'both' && ov1.was.front && ov1.was.front.text === 'W' && ov1.was.back && ov1.was.back.text === '1. 27. 24' && ov1.was.back.font === 'jost-ace', 'with what the SYSTEM had chosen, from autoChoice: ' + JSON.stringify(ov1 && ov1.was));
    check(ov1.back && ov1.back.text === '1. 28. 24' && !ov1.mode || (ov1.back && ov1.back.text === '1. 28. 24'), 'in the same override record as the person\'s words: ' + JSON.stringify({ back: ov1.back && ov1.back.text, mode: ov1.mode }));
    let flat = true; try { refuseNestedArrays(ov1, 'engravingOverride'); } catch (_) { flat = false; }
    check(flat && !srv.st.docs.get('x'), 'flat: no array inside an array in the record');
    check(!(await page.evaluate(() => __calls.slice())).length, 'no staff note, no paid reading');

    // ═══ 6b · THE WRITE COUNT: the same save with a reason writes exactly what the old way wrote ═══
    const sorted = o => JSON.stringify(Object.keys(o).sort().map(k => [k, o[k]]));
    const same = (x, y) => sorted(x) === sorted(y);
    check(oldWay.sets === newWay.sets && oldWay.dels === newWay.dels, 'Firestore document writes and deletes: old way ' + oldWay.sets + ' written / ' + oldWay.dels + ' deleted, with a reason ' + newWay.sets + ' / ' + newWay.dels + ' (the same documents, by collection: ' + JSON.stringify(newWay.setKinds) + ')');
    check(same(oldWay.setKinds, newWay.setKinds) && same(oldWay.delKinds, newWay.delKinds), 'the same collections are written: old ' + JSON.stringify(oldWay.setKinds) + ' new ' + JSON.stringify(newWay.setKinds));
    check(same(oldWay.calls, newWay.calls), 'the same function calls: old ' + JSON.stringify(oldWay.calls) + ' new ' + JSON.stringify(newWay.calls));
    check(oldWay.reads === newWay.reads && oldWay.queries === newWay.queries, 'no read added: reads old ' + oldWay.reads + ' / new ' + newWay.reads + ', queries old ' + oldWay.queries + ' / new ' + newWay.queries);
    console.log('    new way: ' + JSON.stringify(newWay));

    // ═══ 11 · the order timeline's existing line carries the reason; the panel shows one quiet line ═══
    const tl = await page.evaluate(() => __tl.filter(e => e.type === 'engraveChanged').map(e => ({ text: e.text, why: e.data && e.data.why })));
    check(tl.length >= 1 && tl.every(e => /\(Customer asked\)/.test(e.text) && e.why === 'Customer asked'), 'the order timeline\'s own "edited by" line says why (no extra line): ' + JSON.stringify(tl));
    await open();
    const quietLine = await page.evaluate(() => { const l = document.querySelector('.pvSetup .engpWhyLine'); return l && { text: l.textContent.trim(), title: l.title }; });
    check(quietLine && quietLine.text === 'Changed: Customer asked · Test Operator' && /Customer asked/.test(quietLine.title), 'afterwards the panel shows the reason read-only, one quiet line: ' + JSON.stringify(quietLine));
    if (shots) await page.locator(P + ' .engp').screenshot({ path: path.join(shots, 'why-saved.png') });

    // ═══ 4 · the text box: custom text; `was` is the SYSTEM's choice, not the earlier override ═══
    await page.selectOption(P + ' .engpFont', { label: 'Stylish' });
    await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    check(await page.evaluate(() => document.querySelector('.pvSetup [data-engp=whySave]').disabled), 'the text box\'s Save is off until something is typed');
    await page.fill(P + ' .engpWhyText', '  The buyer phoned   about the font  ');
    a = snap();
    await page.press(P + ' .engpWhyText', 'Enter');
    await page.waitForFunction(KEY => { const o = B.orders.byKey.get(KEY).engravingOverride; return o && o.reason && o.reason.code === 'other'; }, KEY, { timeout: 20000 });
    await settle(a);
    const ov2 = await ovOf();
    check(ov2.reason.code === 'other' && ov2.reason.text === 'The buyer phoned about the font' && ov2.reason.by === 'Test Operator' && /font/.test(ov2.reason.of), 'the text box saves custom text (code "other"), tidied, with what changed: ' + JSON.stringify(ov2.reason));
    check(ov2.was.back.text === '1. 27. 24' && ov2.was.mode === 'both', '`was` is still the system\'s choice, not the earlier override: ' + JSON.stringify(ov2.was));
    check(ov2.back && ov2.back.fontWord === 'Stylish', 'the font was saved with the words: ' + JSON.stringify(ov2.back));

    // ═══ 5 · a save equal to the system's choice asks nothing and drops the reason ═══
    await open();
    await page.fill(P + ' .engpWords', '1. 27. 24'); await page.selectOption(P + ' .engpFont', { index: 0 });
    a = snap();
    await page.click(P + ' [data-engp=save]');
    const asked = await page.evaluate(() => !!document.querySelector('.pvSetup .engpWhy'));
    check(!asked, 'putting the system\'s own words and font back asks nothing');
    await page.waitForFunction(KEY => { const o = B.orders.byKey.get(KEY).engravingOverride; return o && !o.reason; }, KEY, { timeout: 20000 });
    await settle(a);
    const ov3 = await ovOf();
    check(ov3 && !('reason' in ov3) && !('was' in ov3), 'and the old reason and the system\'s choice are dropped from the record (nothing is overridden now): ' + JSON.stringify(ov3));
    check(!(await page.evaluate(() => !!document.querySelector('.pvSetup .engpWhyLine'))), 'no quiet line is left');

    // ═══ the mode counts too ═══
    await open();
    await page.click(P + ' [data-engp=mode][data-mode=back]');
    await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    await page.click(P + ' [data-engp=why][data-code=listing]');
    await page.waitForFunction(KEY => { const o = B.orders.byKey.get(KEY).engravingOverride; return o && o.reason && o.reason.code === 'listing'; }, KEY, { timeout: 20000 });
    const ov4 = await ovOf();
    check(ov4.mode === 'back' && ov4.reason.of === 'mode' && ov4.was.mode === 'both', 'changing the mode asks and records it: ' + JSON.stringify({ mode: ov4.mode, of: ov4.reason.of, was: ov4.was.mode }));
    await settle(snap(), false);

    // ═══ 8 · a record without a reason keeps working ═══
    await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY); row.engravingOverride = { mode: 'back', back: { text: 'Old words', lines: ['Old words'], fontWord: null }, by: 'Old Name', at: 1790000000000 }; CharmNestEngravePanel.DRAFTS.clear(); }, KEY);
    await open();
    const old = await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), vm = CharmNestEngraveSetup.read(row); return { mode: vm.mode, text: vm.setup.back && vm.setup.back.text, why: CharmNestEngraveWhy.reasonOf(row), line: !!document.querySelector('.pvSetup .engpWhyLine'), words: document.querySelector('.pvSetup .engpWords').value, ask: !!document.querySelector('.pvSetup .engpWhy') }; }, KEY);
    check(old.mode === 'back' && old.text === 'Old words' && old.why === null && !old.line && !old.ask, 'an old record (no reason) reads, draws and shows no reason line: ' + JSON.stringify(old));
    await page.fill(P + ' .engpWords', 'Older words'); await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
    check(true, 'and a further change to it asks as usual');
    await page.click(P + ' [data-engp=whyCancel]');

    // ═══ 9 · a position move that is an override asks too ═══
    await page.evaluate(() => { const P = window.CharmNestEngravePosition; window.__posWas = { canMove: P.canMove, nudge: P.nudge }; });   // (stubs: the fixture has no outline to move the words in; the panel's own flow is what is tested)
    await reset();
    await page.evaluate(() => { const P = CharmNestEngravePosition; P.canMove = () => true; P.nudge = (job, dx, dy) => { job.nudged = true; return { ok: true, clamped: false, moved: { dx, dy } }; }; });
    await open();
    const posOn = await page.evaluate(() => !!document.querySelector('.pvSetup .engpPb:not([disabled])'));
    if (posOn) {
      await page.click(P + ' [data-act=right]');
      a = snap();
      await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy');
      await page.click(P + ' [data-engp=why][data-code=asked]');
      await page.waitForFunction(KEY => { const o = B.orders.byKey.get(KEY).engravingOverride; return o && o.reason; }, KEY, { timeout: 20000 });
      await settle(a);
      const pos = delta(a), ov5 = await ovOf();
      check(ov5.reason.of === 'position' && ov5.was.mode === 'both' && !ov5.mode && !ov5.back, 'moving the words is an override: it asks and records "position" (no words or mode in the record): ' + JSON.stringify(ov5));
      check(pos.calls['charmNestLibrary:runPut'] === 1, 'the one case with no save today: one run save (runPut), nothing else: ' + JSON.stringify(pos.calls) + ' writes ' + pos.sets + ' deletes ' + pos.dels);
      console.log('    position-only: ' + JSON.stringify(pos));
      const rf = await page.evaluate(KEY => { const row = B.orders.byKey.get(KEY), E = CharmNestEngraveSetup; return { setup: E.setupFor(row).mode, edited: E.setupFor(row).edited, back: E.setupFor(row).back && E.setupFor(row).back.text }; }, KEY);
      check(rf.setup === 'both' && rf.edited === false && rf.back === '1. 27. 24', 'a reason-only record changes nothing about what the line engraves: ' + JSON.stringify(rf));
    } else console.log('  (the position buttons are not on in this fixture: the position check was skipped)');
    await page.evaluate(() => { const P = CharmNestEngravePosition, w = window.__posWas; if (w) { P.canMove = w.canMove; P.nudge = w.nudge; } });

    check(errors.length === 0, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }

  // ═══ the REAL-DRAWING world (ENGLIVE): real charm drawings, real jobs, the engraver's own layout and capitals. The panel opens on the jobs' words ("NAME\nONE", Ace), the system says "Name One": an untouched face
  //     is never a words override, so a font-only or a position-only Save is asked as exactly that ═══
  let R = null; try { R = require('./_engraveRealWorld.cjs'); } catch (_) { /* this build has no real world */ }
  if (R) {
    const w = await R.world({ viewport: { width: 1440, height: 900 } });
    if (!w) console.log('  (no playwright-core: the real-world checks were skipped)');
    else try {
      const made = await R.build(w, ['both']); const key = made.both.key, page = w.page;
      await R.openEngraveDetail(w, 'both', { face: 'back' });
      const P = '.pvSetup', ovOf = () => page.evaluate(k => { const o = B.orders.byKey.get(k).engravingOverride; return o ? JSON.parse(JSON.stringify(o)) : null; }, key);
      const info = await page.evaluate(k => { const row = B.orders.byKey.get(k), vm = CharmNestEngraveSetup.read(row), a = CharmNestEngraveSetup.autoChoice(row, null); return { panel: document.querySelector('.pvSetup .engpWords').value, auto: a.back.text, mode: a.mode, nudgeable: [...document.querySelectorAll('.pvSetup .engpPb')].some(b => !b.disabled), jobText: vm.rows.find(r => r.face === 'back').text }; }, key);
      console.log('    real world, untouched Back: panel ' + JSON.stringify(info.panel) + ' / system ' + JSON.stringify(info.auto) + ' / position buttons on: ' + info.nudgeable);
      const ask = async (what) => { await page.click(P + ' [data-engp=save]'); await page.waitForSelector(P + ' .engpWhy', { timeout: 10000 }); };
      const answer = async () => { await page.click(P + ' [data-engp=why][data-code=asked]'); await page.waitForFunction(k => { const o = B.orders.byKey.get(k).engravingOverride; return o && o.reason; }, key, { timeout: 60000 }); await page.waitForTimeout(500); };
      const reopen = async () => { await page.evaluate(k => { delete B.orders.byKey.get(k).engravingOverride; CharmNestEngravePanel.DRAFTS.clear(); }, key); await R.openEngraveDetail(w, 'both', { face: 'back' }); };
      // a position move with the real position control
      let ov; const pos = await page.evaluate(() => !!document.querySelector('.pvSetup .engpPb[data-act=right]:not([disabled])'));
      if (pos) {
        await page.click(P + ' [data-act=right]'); await ask(); await answer();
        ov = await ovOf();
        check(ov.reason.of === 'position' && !ov.back && !ov.mode, 'moving the words with the real position buttons is asked as the POSITION only: ' + JSON.stringify(ov.reason));
      } else console.log('  (the real job cannot move yet in this run: the position check was skipped)');
      await reopen();
      // a font pick alone
      const fonts = await page.evaluate(() => [...document.querySelectorAll('.pvSetup .engpFont option')].map(o => o.value).filter(Boolean));
      await page.selectOption(P + ' .engpFont', fonts.find(f => /stylish/i.test(f)) || fonts[0]);
      await ask(); await answer();
      ov = await ovOf();
      check(ov.reason.of === 'font', 'a font pick alone is asked as the FONT only (the untouched words, capitals and line split of the engraver, are not a words override): ' + JSON.stringify(ov.reason));
      check(ov.was.back.text === info.auto && ov.was.mode === 'both', 'and the record keeps the system\'s words and mode: ' + JSON.stringify(ov.was));
      // typed words
      await reopen();
      await page.fill(P + ' .engpWords', 'Name Two'); await ask(); await answer();
      ov = await ovOf();
      check(ov.reason.of === 'back' && ov.back.text === 'Name Two', 'new words are asked as the BACK words: ' + JSON.stringify({ of: ov.reason.of, back: ov.back && ov.back.text }));
      // the same words typed again in other capitals: nothing to ask in a capitals font
      await reopen();
      await page.fill(P + ' .engpWords', 'NAME ONE');
      const same = await page.evaluate(() => { const b = document.querySelector('.pvSetup [data-engp=save]'); return { saveOn: !!b && !b.disabled }; });
      if (same.saveOn) { await page.click(P + ' [data-engp=save]'); await page.waitForTimeout(400); }
      check(!(await page.evaluate(() => !!document.querySelector('.pvSetup .engpWhy'))), 'the system\'s own name retyped in capitals (Ace engraves capitals) asks nothing');
      check(w.errors.length === 0, 'no page errors in the real world: ' + w.errors.join(' | '));
    } finally { await w.close(); }
  }
  if (fails.length) { console.log('\nengrave-why: ' + fails.length + ' failed'); fails.forEach(f => console.log('  - ' + f)); process.exit(1); }
  console.log('\nengrave-why: ok');
})().catch(e => { console.error(e); process.exit(1); });
