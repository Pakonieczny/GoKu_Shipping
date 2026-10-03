// Reset the sandbox (Settings) leaves nothing of the rehearsal in the BROWSER (Paul, 3 Oct 2026: "I press reset and the same
// records just keep on coming back": completed custom orders, "Sent to Sheet" cards, the "On the sheets" strip).
// The cloud's records went; this browser kept its own copy and the reload restored it: the saved workspace (IndexedDB
// "charm-nest-workspace", key "sandbox": the answered cards, the designs sent to the sheets, the sets), queued messages,
// drafts, journals — and the designs sent were written to the cloud again from that copy.
// The real page, against the repo's fake server (bridge-server.cjs: the real charmNestLibrary handler over an in-memory
// Firestore). Seeds the sandbox side of the browser and the production side, presses the page's reset and checks that:
//   · the sandbox side is empty after the reload (IndexedDB, localStorage, shared lists without the sandbox's items),
//   · the queued message, the timeline event and the custom send are never sent after the reset,
//   · Review › Completed shows none of the old records (and an old line key has no "sent" decision: a replayed order is new),
//   · another tab of the browser stops saving and reloads, and its last write does not bring the records back,
//   · the production side's keys (localStorage, IndexedDB) and records are unchanged,
//   · the button shows a labelled spinner while it works, and the result is said in words.
//   node tests/charm-nest/sandbox-reset-browser.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');

const RID_DONE = '4173162973', RID_SENT = '4170408845', OLD = 'OLDREC';

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  // cloud records the reset has to delete (sandbox) and must not touch (production)
  st.put('Sandbox_Charm_Custom_Orders', `${RID_DONE}_9001`, { key: `${RID_DONE}_9001`, receiptId: RID_DONE, sku: 'CHAIN_8941', state: 'completed', lastPrintedBy: 'paul', lastPrintedAt: Date.now() - 86400000, updatedAtMs: Date.now() });
  st.put('Sandbox_Charm_Custom_Sheet', 'card-old', { ck: `custom:${RID_SENT}:X`, rid: RID_SENT, phase: 'sent' });
  st.put('Sandbox_Charm_Pool', 'pool-old', { poolId: 'pool-old', state: 'ready' });
  st.put('Charm_Custom_Orders', 'prod-1', { key: 'prod-1', receiptId: '4100000001', state: 'completed' });
  const prodDocsBefore = JSON.stringify(st.list('Charm_Custom_Orders'));

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; }, { sorter: sorterOrigin });
  // every message post is held back (the record cannot be reached), so what waits in the outbox stays waiting until the reset
  const posts = []; await ctx.route(/firebaseOrders/, r => { if (r.request().method() === 'POST') { posts.push(r.request().postData() || ''); return r.abort(); } return r.continue(); });
  const errors = [];
  const open = async () => { const p = await ctx.newPage(); p.on('pageerror', e => errors.push(e.message)); await p.goto(`${sorterOrigin}/charm-nest-1.html`); return p; };
  const boot = p => p.waitForFunction(() => window.CN && window.TeamMail && window.Sandbox && window.Session && Session.ready(), null, { timeout: 60000 });
  const page = await open();
  await page.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 });
  await page.evaluate(station => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'on', dsOrigin: station, pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s)); }, stationOrigin);
  await page.reload(); await boot(page);
  assert(await page.evaluate(() => WORKSPACE_SANDBOX), 'the page is in the sandbox');

  // ── seed: the sandbox side, and the production side beside it ──
  const sandboxKeys = {
    'cn.team.outbox:sandbox': [{ id: 'q-old', rid: RID_DONE, text: OLD + ' message', who: 'paul', at: Date.now(), tries: 0 }],
    'cn.team.drafts:sandbox': { [RID_DONE]: { t: OLD + ' draft', at: Date.now() } },
    'cn.mail.drafts:sandbox': { [RID_DONE]: { t: OLD + ' mail draft', at: Date.now() } },
    'cn.autoCancel.v1:sandbox': { known: [], done: { [RID_DONE]: { at: 1, t: Date.now(), mine: OLD } }, jobs: {}, notices: [], pend: {} },
    'cn.tl.sheets:sandbox': { [OLD]: Date.now() },
    'cn.sheetwin.moves:sandbox': { [RID_DONE]: { rid: RID_DONE, by: OLD } },
    'cn.sheetwin.freed:sandbox': { [OLD]: [] },
    'cn.roseRehearsal.sandbox.v1': { [OLD]: 1 },
    'cn.arrivals.sandbox': { seen: { [RID_DONE]: 1 }, recorded: { [RID_DONE]: true }, lastCheck: 1, nextCheck: 1, lastAdded: 0, error: null, mark: OLD }
  };
  const prodKeys = {
    'cn.team.outbox': [{ id: 'q-prod', rid: '4100000001', text: 'production message', who: 'paul', at: Date.now(), tries: 0, error: 'held' }],
    'cn.team.drafts': { 4100000001: { t: 'production draft', at: Date.now() } },
    'cn.mail.drafts': { 4100000001: { t: 'production mail draft', at: Date.now() } },
    'cn.autoCancel.v1:production': { known: [], done: { 4100000001: { at: 1, t: Date.now() } }, jobs: {}, notices: [], pend: {} },
    'cn.tl.sheets': { prod: Date.now() }, 'cn.sheetwin.moves': { 4100000001: { rid: '4100000001' } }, 'cn.sheetwin.freed': { p: [] },
    'cn.arrivals.production': { seen: { 4100000001: 1 }, lastCheck: 1, nextCheck: 1, lastAdded: 0, error: null },
    'cn.customRead.production': { '4100000001_1': { hash: 'h', at: Date.now() } }
  };
  const keptKeys = { 'cn.customRead.sandbox': { kept_1: { hash: 'paid', at: Date.now() } }, 'cn.listImageFraming.sandbox': [['1', { s: 2, x: 0, y: 0 }]] };
  const sharedLists = {
    'orderTimeline.outbox.v1': [{ orderId: RID_DONE, type: 'qrLabel', id: OLD + '-ev', sandbox: true, mode: 'sorter', at: Date.now() }, { orderId: '4100000001', type: 'scan', id: 'prod-ev', sandbox: false, mode: 'station', at: Date.now() }],
    'cn.mail.outbox': [{ id: 'm-sb', body: { sandbox: true, receiptId: RID_DONE, clientId: 'm-sb', text: OLD }, at: Date.now(), tries: 0 }, { id: 'm-prod', body: { sandbox: false, receiptId: '4100000001', clientId: 'm-prod', text: 'prod' }, at: Date.now(), tries: 0 }],
    'station_activity_q.dev1': [{ id: 'a-sb', sandbox: true, action: 'print', at: Date.now() }, { id: 'a-prod', action: 'scan', at: Date.now() }]
  };
  await page.evaluate(({ sandboxKeys, prodKeys, keptKeys, sharedLists, RID_DONE, RID_SENT }) => {
    for (const o of [sandboxKeys, prodKeys, keptKeys, sharedLists]) for (const [k, v] of Object.entries(o)) localStorage.setItem(k, JSON.stringify(v));
    // what the page kept in memory and saves in its workspace: a card answered (Completed › "QR label printed by paul") and
    // custom designs sent to the sheets, whose record was never confirmed in the cloud (sendCloudPending: re-sent by recover())
    Review.settled().unshift({ key: `ord:custom:${RID_DONE}:CHAIN_8941`, kind: 'customOrder', why: 'QR label printed by paul · Sep 30 11:48 PM', lines: 1, orders: [RID_DONE], by: 'paul', t: Date.now() - 3600000 });
    B.customDesigns[`custom:${RID_SENT}:X`] = { ck: `custom:${RID_SENT}:X`, rid: RID_SENT, at: Date.now() - 7200000, files: [], sent: { id: `custom-sheet:custom:${RID_SENT}:X:1`, at: Date.now() - 7200000, by: 'paul', lines: { [`${RID_SENT}_1`]: [] } }, sendCloudPending: true };
    Session.schedule();
  }, { sandboxKeys, prodKeys, keptKeys, sharedLists, RID_DONE, RID_SENT });
  // the production workspace checkpoint (IndexedDB), written as the page does: a marker only, to be found unchanged
  const idb = (p, fn, arg) => p.evaluate(async ([src, arg]) => { const f = eval(src); return f(arg); }, [fn.toString(), arg]);
  const idbPutRaw = (p, key, value) => idb(p, ([key, value]) => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readwrite'); tx.objectStore('workspaces').put(value, key); tx.oncomplete = () => res(true); tx.onerror = () => rej(tx.error); }; r.onerror = () => rej(r.error); }), [key, value]);
  const idbDump = p => idb(p, () => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readonly'), s = tx.objectStore('workspaces'), keys = s.getAllKeys(), out = {}; keys.onsuccess = () => { let n = keys.result.length; if (!n) return res(out); for (const k of keys.result) { const g = s.get(k); g.onsuccess = () => { out[k] = g.result; if (!--n) res(out); }; } }; }; r.onerror = () => rej(r.error); }));
  const PROD_CHECKPOINT = { v: 1, at: 1, epoch: '', marker: 'production-untouched', settled: [{ key: 'prod' }] };
  await idbPutRaw(page, 'production', PROD_CHECKPOINT);
  await idbPutRaw(page, 'sandbox:best:sheet-old', { jobId: OLD });
  assert.strictEqual(await page.evaluate(() => Session.flushNow()), true, 'the workspace was saved');
  let dump = await idbDump(page);
  assert(dump.sandbox && dump.sandbox.settled.length === 1 && dump.sandbox.customDesigns, 'the sandbox checkpoint holds the answered card and the sent designs');

  // ── control: a plain reload restores all of it (this is what kept coming back) ──
  const reviewText = p => p.evaluate(() => { CN.setMode('review'); const v = Review.view(); v.cseg = 'done'; v.filter = null; Review.render(); return document.getElementById('reviewView').innerText; });
  await page.reload(); await boot(page);
  const control = await page.evaluate(([a, b]) => ({ settled: Review.settled().length, designs: Object.keys(B.customDesigns), sent: !!CustomSheet.decisionOf({ key: `${b}_1` }) }), [RID_DONE, RID_SENT]);
  assert(control.settled === 1 && control.designs.length === 1 && control.sent, 'control: a reload alone brings the old records back: ' + JSON.stringify(control));
  assert(/4173162973/.test(await reviewText(page)), 'control: Review › Completed lists the old record after a plain reload');

  // a second tab of the same browser, holding the same old state
  const tab2 = await open(); await boot(tab2); await tab2.evaluate(() => { window.__stale = 1; });
  assert.strictEqual((await tab2.evaluate(() => Review.settled().length)), 1, 'the second tab restored the old records too');
  await page.waitForTimeout(2600);   // both tabs' outboxes flush 1.5 s after a load: the old event has been sent, once, before the press

  // ── the press ──
  const mark = st.calls.length, postsBefore = posts.length;
  await page.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
  const spin = await page.evaluate(() => { const b = document.getElementById('stSandboxReset'); return { disabled: b.disabled, busy: b.getAttribute('aria-busy'), spinner: !!b.querySelector('.spin'), text: b.textContent.trim() }; });
  assert(spin.disabled && spin.busy === 'true' && spin.spinner && /Resetting/.test(spin.text), 'the button shows a spinner and says what it is doing: ' + JSON.stringify(spin));
  await page.waitForNavigation({ waitUntil: 'load', timeout: 60000 }); await boot(page);
  await page.waitForFunction(() => /Sandbox cleaned/.test(document.getElementById('toasts').innerText), null, { timeout: 8000 }).catch(() => {});
  const note = await page.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent));
  assert(note.some(t => /Sandbox cleaned — \d+ record\(s\) and \d+ file\(s\) removed; nothing is left/.test(t)), 'the clean page says what was removed and that nothing is left: ' + JSON.stringify(note));
  await tab2.waitForFunction(() => !window.__stale, null, { timeout: 15000 });   // the other tab saw the reset, stopped saving and reloaded
  await boot(tab2);
  await page.waitForTimeout(3500);   // the outbox flushes 1.5 s after a load; the timeline's too

  // ── the cloud: the sandbox's records are gone, production's are not, nothing was written back ──
  for (const c of ['Sandbox_Charm_Custom_Orders', 'Sandbox_Charm_Custom_Sheet', 'Sandbox_Charm_Pool']) assert.strictEqual(st.list(c).length, 0, 'the cloud holds no ' + c);
  assert.strictEqual(JSON.stringify(st.list('Charm_Custom_Orders')), prodDocsBefore, 'production records are untouched');
  // (before the wipe the page may well re-send a record it holds: it is what the wipe then deletes; after it, nothing may)
  let wipeAt = -1; st.calls.forEach((c, i) => { if (i >= mark && c.op === 'sandboxReset') wipeAt = i; });
  assert(wipeAt >= 0, 'the page asked the cloud to reset the sandbox');
  const since = st.calls.slice(wipeAt + 1);
  assert(!since.some(c => c.op === 'customSheetPut'), 'the old send was not written to the cloud again');
  assert(!since.some(c => c.op === 'timelineAdd' && JSON.stringify(c.body).includes(OLD)), 'the old timeline event was not sent: ' + JSON.stringify(since.map((c, i) => [i, c.op, JSON.stringify(c.body).includes(OLD)]).filter(x => x[2])) + ' of ' + since.length + ' ' + JSON.stringify(since.filter(c => c.op === 'timelineAdd' && JSON.stringify(c.body).includes(OLD)).map(c => c.body)).slice(0, 600) + ' ops: ' + since.map(c => c.op).join(','));
  assert(!posts.slice(postsBefore).some(t => t.includes(OLD)), 'the old queued message was not sent');

  // ── the browser: the sandbox side is empty, the production side is as it was ──
  const ls = await page.evaluate(() => Object.fromEntries(Object.keys(localStorage).map(k => [k, localStorage.getItem(k)])));
  for (const k of Object.keys(sandboxKeys)) assert(ls[k] == null || !ls[k].includes(OLD) && !ls[k].includes(RID_DONE), `the sandbox key ${k} still holds the old records: ${ls[k]}`);
  assert(!/q-old|m-sb|a-sb/.test(JSON.stringify(ls)) && !ls['orderTimeline.outbox.v1']?.includes(OLD), 'no queued sandbox item is left in any list');
  for (const [k, v] of Object.entries(prodKeys)) assert.strictEqual(ls[k], JSON.stringify(v), `the production key ${k} is unchanged`);
  assert.strictEqual(ls['cn.customRead.sandbox'], JSON.stringify(keptKeys['cn.customRead.sandbox']), 'Claude\'s paid readings are kept');
  assert(ls['cn.listImageFraming.sandbox'] != null, 'the photo framing (a preference, not a record) is kept (the page itself rewrites it on leaving)');
  assert.deepStrictEqual(JSON.parse(ls['orderTimeline.outbox.v1']).map(e => e.id), ['prod-ev'], 'the shared timeline outbox keeps production\'s event only');
  assert.deepStrictEqual(JSON.parse(ls['cn.mail.outbox']).map(e => e.id), ['m-prod'], 'the shared mail outbox keeps production\'s message only');
  assert.deepStrictEqual(JSON.parse(ls['station_activity_q.dev1']).map(e => e.id), ['a-prod'], 'the shared activity queue keeps production\'s event only');
  dump = await idbDump(page);
  assert.deepStrictEqual(dump.production, PROD_CHECKPOINT, 'the production workspace checkpoint is unchanged');
  assert(!('sandbox:best:sheet-old' in dump), 'the sandbox\'s best-layout records are gone');
  assert(!dump.sandbox || (dump.sandbox.settled.length === 0 && Object.keys(dump.sandbox.customDesigns || {}).length === 0 && dump.sandbox.epoch), 'the sandbox checkpoint, if one was written since, holds none of the old records');

  // ── Review: no old record, no stamp ──
  const after = await page.evaluate(([a, b]) => ({ settled: Review.settled().length, designs: Object.keys(B.customDesigns).length, done: Object.keys(B.maps.customDone || {}).length, sent: !!CustomSheet.decisionOf({ key: `${b}_1` }) }), [RID_DONE, RID_SENT]);
  assert.deepStrictEqual(after, { settled: 0, designs: 0, done: 0, sent: false }, 'the reloaded page holds none of the old records: ' + JSON.stringify(after));
  const text = await reviewText(page);
  assert(!/4173162973|4170408845|QR label printed|Sent to Sheet|On the sheets/i.test(text), 'Review › Completed shows none of the old records: ' + text.slice(0, 300));
  const t2 = await tab2.evaluate(() => ({ settled: Review.settled().length, designs: Object.keys(B.customDesigns).length }));
  assert.deepStrictEqual(t2, { settled: 0, designs: 0 }, 'the other tab came back clean too (its last write was not restored)');

  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox reset browser OK: the saved workspace, queues, drafts and journals of the sandbox side are gone after the reset, nothing was written back, Review shows none of the old records, another tab reloaded clean, production keys and records unchanged');
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
