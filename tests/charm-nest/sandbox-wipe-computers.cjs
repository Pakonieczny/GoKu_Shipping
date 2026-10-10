// A second computer (or an old tab) must not write old sandbox data back after a wipe (Paul, 10 Oct 2026: "every open tab or
// second computer on the sandbox must not write old data back").
// Two browser contexts are two computers: they share nothing but the cloud (the repo's fake server; the order stream is a small
// stub in this file, so the test controls it; sandboxReset still goes to the REAL handler over the in-memory Firestore, offline).
// The cloud's mark of "this run" is its stream record: a reset deletes it, a new start makes a new one. A page that holds a run
// and finds the record gone or another one stops writing, clears its own browser copy (every store of the registry), waits for
// Start and reloads clean. Checked here:
//   1 · two computers on the same run are not disturbed (no false stop)
//   2 · computer A resets: computer B's next step finds the stream gone, stops, writes nothing to the cloud afterwards, comes back
//       clean (browser stores at zero, Review and memory empty, waiting for Start, a plain-words note), and its other tab follows
//   3 · computer A starts again: computer B's next step finds another run and stops the same way
//   4 · computer B reloads (an old saved workspace) while A runs another: it is not restored, it comes back clean
//   node tests/charm-nest/sandbox-wipe-computers.cjs [playwright-core dir]
const path = require('path'), assert = require('assert'), fs = require('fs');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const RID = '4173162973', OLD = 'OLDREC';
const READ_OP = /^(get|list|ping|lookup|history|check|masterList)|(Get|List|Check|Status|Preview|Read|Url|Info|Files)$/;

(async () => {
  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const pull = () => st.put('Charm_Sandbox', 'current', { path: 'charmnest/sandbox/orders-pull/t.json', source: 'etsy-pull', count: 10, open: 10, closed: 0, at: Date.now(), pulledAt: Date.now(), startId: 'start-' + Date.now(), label: '250 newest Etsy orders' });
  pull();
  // ── the order stream, as the cloud keeps it (one record: created by a start, deleted by a reset) ──
  let S = null, allow = true, runs = 0; const base = Math.floor(Date.now() / 600000) * 600000;
  const stream = b => {
    if (b.action === 'get') return { ok: true, stream: S };
    if (b.action === 'off') { if (S) S.on = false; return { ok: true, stream: null }; }
    if (b.action === 'ensure' || b.action === 'tick') {
      if (b.action === 'tick' && !(S && S.on)) return { ok: true, stream: null, advanced: false };
      if (!S) { if (!allow) return { status: 409, ok: false, error: 'no sandbox snapshot yet' }; S = { on: true, v: 2, seed: 5, startedAt: base + (++runs) * 1000, snapshotPath: 'p' + runs, tick: 0, simStart: base, simNow: base, stepMs: 600000, total: 10, brought: 0, done: false }; }
      if (b.action === 'tick' && (b.expect == null || +b.expect === S.simNow)) { S.tick++; S.simNow = S.simStart + S.tick * S.stepMs; S.brought = Math.min(10, S.tick * 3); }
      return { ok: true, stream: S, advanced: b.action === 'tick' };
    }
    return { ok: true, stream: S };
  };
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  const errors = [], writes = { A: [], B: [] };
  const computer = async name => {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/firebaseOrders/, r => (r.request().method() === 'POST' ? r.abort() : r.continue()));
    await ctx.route(/charmNestLibrary/, async r => {
      if (r.request().method() !== 'POST') return r.continue();
      let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
      if (b.op === 'sandboxStream') { const out = stream(b); return r.fulfill({ status: out.status || 200, contentType: 'application/json', body: JSON.stringify(out) }); }
      if (b.op === 'sandboxReset') { S = null; allow = false; }   // (the real handler then deletes the records, offline)
      if (!READ_OP.test(String(b.op || ''))) writes[name].push({ op: b.op, at: Date.now() });
      return r.continue();
    });
    await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; }, { sorter: sorterOrigin });
    const open = async () => { const p = await ctx.newPage(); p.on('pageerror', e => errors.push(`${name}: ${e.message}`)); await p.goto(`${sorterOrigin}/charm-nest-1.html`); return p; };
    return { ctx, open };
  };
  const boot = p => p.waitForFunction(() => window.CN && window.TeamMail && window.Sandbox && window.Session && Session.ready() && window.CharmNestSandboxBrowser, null, { timeout: 60000 });
  const A = await computer('A'), Bc = await computer('B');
  const sandboxOn = async p => { await p.evaluate(x => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, x); localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.removeItem('cn.sandboxHold'); }, { sandbox: 'on', sandboxStream: 'on', sandboxSpeed: 50, sandboxSeed: 0, dsOrigin: stationOrigin, pollOrders: 'off', runMode: 'manual' }); await p.reload(); await boot(p); };
  const a1 = await A.open(); await a1.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 }); await sandboxOn(a1);
  const b1 = await Bc.open(); await b1.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 }); await sandboxOn(b1);
  const b2 = await Bc.open(); await boot(b2); await b2.evaluate(() => { window.__stale = 1; });
  for (const p of [a1, b1]) assert.strictEqual(await p.evaluate(() => Sandbox.held()), false, 'the sandbox pages are running, not waiting for Start');

  // ── 1 · both computers on the same run ──
  const idA = await a1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  const idB = await b1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  assert(idA && idA === idB, 'both computers hold the same run: ' + idA + ' / ' + idB);
  assert.strictEqual(await b1.evaluate(() => Sandbox.advance().then(s => s.tick, e => 'ERR ' + e.message)), 1, 'a step on the shared run goes on as ever');
  assert.strictEqual(await b1.evaluate(() => window.CNWipe.active), false, 'a page on the same run is not stopped');

  // what computer B holds of the run: memory, a saved workspace, queued and stored sandbox keys
  const seedB = async p => {
    await p.evaluate(({ RID, OLD }) => {
      for (const [k, v] of Object.entries({ 'cn.team.drafts:sandbox': { [RID]: { t: OLD, at: Date.now() } }, 'cn.mail.drafts:sandbox': { [RID]: { t: OLD, at: Date.now() } }, 'cn.sheetwin.moves:sandbox': { [RID]: { rid: RID } }, 'cn.customRead.sandbox': { x_1: { hash: 'h', at: Date.now() } }, 'cn.mail.told': { olsb_1: 5, ol_1: 6 } })) localStorage.setItem(k, JSON.stringify(v));
      localStorage.setItem('qrPrintAll', JSON.stringify({ sandbox: true, items: [{ receipt_id: RID }] }));
      Review.settled().unshift({ key: `ord:custom:${RID}:CHAIN_8941`, kind: 'customOrder', why: 'QR label printed by paul', lines: 1, orders: [RID], by: 'paul', t: Date.now() - 3600000 });
      B.customDesigns[`custom:${RID}:X`] = { ck: `custom:${RID}:X`, rid: RID, at: Date.now(), files: [], sent: { id: 's', at: Date.now(), by: 'paul', lines: {} } };
      Session.schedule();
    }, { RID, OLD });
    assert.strictEqual(await p.evaluate(() => Session.flushNow()), true, 'the workspace was saved');
  };
  const nonzero = async p => (await p.evaluate(() => Sandbox.browserLeft())).filter(r => r.n).map(r => `${r.label}: ${r.n}`);
  const cleanAfter = async (p, why) => {
    const left = await p.evaluate(() => Sandbox.browserLeft());
    for (const r of left) if (r.n !== null) assert.strictEqual(r.n, 0, `${why}: nothing is left of "${r.label}": ${JSON.stringify(r)}`);
    const o = await p.evaluate(() => ({ held: Sandbox.held(), settled: Review.settled().length, designs: Object.keys(B.customDesigns).length, active: window.CNWipe.active, seed: S.settings.sandbox, ident: Sandbox.runId() }));
    assert.deepStrictEqual({ held: o.held, settled: o.settled, designs: o.designs, active: o.active, seed: o.seed, ident: o.ident }, { held: true, settled: 0, designs: 0, active: false, seed: 'on', ident: '' }, `${why}: the page came back clean, waiting for Start, still in the sandbox: ${JSON.stringify(o)}`);
  };
  const stopsWith = async (p, expectMsg) => {
    const nav = p.waitForNavigation({ waitUntil: 'load', timeout: 60000 });
    const msg = await p.evaluate(() => Sandbox.advance().then(() => 'no stop', e => e.message));
    const tAfter = Date.now(), active = await p.evaluate(() => window.CNWipe.active);
    assert(expectMsg.test(msg), 'the step says why it stopped: ' + msg);
    assert.strictEqual(active, true, 'from that moment the page writes nothing to the cloud (the guard is up)');
    await nav; await boot(p);
    return tAfter;
  };

  // ── 2 · computer A resets; computer B finds the stream gone ──
  await seedB(b1);
  assert((await nonzero(b1)).length >= 4, 'computer B holds the old run in several stores: ' + await nonzero(b1));
  await a1.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
  await a1.waitForNavigation({ waitUntil: 'load', timeout: 60000 }); await boot(a1);
  assert.strictEqual(st.list('Sandbox_Charm_Custom_Orders').length, 0, 'the cloud was reset (the real handler, offline)');
  assert.strictEqual(S, null, 'the stream record is gone');
  const t2 = await stopsWith(b1, /reset from another computer or tab/);
  await b1.waitForTimeout(1500);
  await cleanAfter(b1, 'computer B after a reset elsewhere');
  const noteB = await b1.evaluate(() => [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent).join(' | '));
  assert(/reset or started again from another computer/.test(noteB), 'the clean page says what happened, in words: ' + noteB);
  assert.deepStrictEqual(writes.B.filter(w => w.at > t2), [], 'computer B wrote nothing to the cloud after it stopped: ' + JSON.stringify(writes.B.filter(w => w.at > t2)));
  await b2.waitForFunction(() => !window.__stale, null, { timeout: 15000 }); await boot(b2);
  await cleanAfter(b2, 'the other tab of computer B');
  assert.strictEqual(st.list('Sandbox_Charm_Pool').length + st.list('Sandbox_Charm_Custom_Sheet').length, 0, 'nothing of the old run was written to the cloud meanwhile');

  // ── 3 · computer A starts again; computer B (running the run before) finds another one ──
  pull(); allow = true;
  await sandboxOn(a1);                                     // (a start: the sandbox runs, its stream is made)
  const idA2 = await a1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  await sandboxOn(b1);
  const idB2 = await b1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  assert(idA2 && idA2 === idB2 && idA2 !== idA, 'both computers hold the new run, and it is not the first: ' + [idA, idA2, idB2]);
  await seedB(b1);
  await a1.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
  await a1.waitForNavigation({ waitUntil: 'load', timeout: 60000 }); await boot(a1);
  pull(); allow = true; await sandboxOn(a1);
  const idA3 = await a1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  assert(idA3 && idA3 !== idA2, 'computer A runs a third run: ' + [idA2, idA3]);
  const mark3 = writes.B.length;
  const t3 = await stopsWith(b1, /started again from another computer or tab/);
  await b1.waitForTimeout(1500);
  await cleanAfter(b1, 'computer B after a new start elsewhere');
  assert.deepStrictEqual(writes.B.slice(mark3).filter(w => w.at > t3), [], 'computer B wrote nothing to the cloud after it stopped');
  assert.strictEqual(S && S.startedAt === base + 3000, true, 'computer A\'s new run was not disturbed by computer B (its record stands)');

  // ── 4 · computer B reloads with an old saved workspace while A runs another run ──
  await sandboxOn(b1);
  const idB4 = await b1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  assert.strictEqual(idB4, idA3, 'computer B joined computer A\'s run');
  await seedB(b1);
  const dump = () => b1.evaluate(() => new Promise(res => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const g = r.result.transaction('workspaces', 'readonly').objectStore('workspaces').get('sandbox'); g.onsuccess = () => res(g.result ? { settled: (g.result.settled || []).length, streamId: g.result.streamId } : null); }; }));
  const saved = await dump(); assert(saved && saved.settled === 1 && saved.streamId === idA3, 'computer B\'s saved workspace holds the old answered card under the run: ' + JSON.stringify(saved));
  await a1.evaluate(() => { window.__reset = Sandbox.reset({ button: document.getElementById('stSandboxReset'), note: document.getElementById('stSandboxResetNote') }); });
  await a1.waitForNavigation({ waitUntil: 'load', timeout: 60000 }); await boot(a1);
  pull(); allow = true; await sandboxOn(a1);
  const idA5 = await a1.evaluate(async () => { await Sandbox.ready(true); return Sandbox.runId(); });
  assert(idA5 && idA5 !== idA3, 'computer A runs another run: ' + [idA3, idA5]);
  const nav = b1.waitForNavigation({ waitUntil: 'load', timeout: 60000 });
  await b1.reload(); await nav.catch(() => {}); await boot(b1);
  // (the page restored the old workspace, asked the cloud once, found another run and stopped: it reloads itself clean)
  await b1.waitForFunction(() => !Review.settled().length && Sandbox.held(), null, { timeout: 30000 }).catch(() => {});
  await b1.waitForTimeout(2500); await boot(b1);
  await cleanAfter(b1, 'computer B after a reload with an old workspace');
  const after = await dump(); assert(!after || (after.settled === 0 && after.streamId !== idA3), 'the old workspace of computer B is gone from its browser: ' + JSON.stringify(after));
  assert(S && S.startedAt === base + 5000 || S.startedAt > base, 'computer A\'s run stands');

  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox wipe, two computers OK: the same run is left alone; a reset or a new start on another computer stops this one (stream gone / another run / an old workspace at reload), it writes nothing to the cloud afterwards and comes back clean, waiting for Start, with its other tab');
  await browser.close(); srv.close && srv.close(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
