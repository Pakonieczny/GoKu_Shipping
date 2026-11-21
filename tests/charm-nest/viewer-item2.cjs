// ITEM 2 (Paul, 21 Nov 2026: no new orders in Orders / Nest / Library on the staff computers since the cloud took every run, though the cloud's run held 31 new lines in state
// "pulled" and the "Today" count moved).
//   Why: a viewer (mode "on", charm-nest-viewer.js) never calls Session.listen(), the only thing that makes Session.ready() true. The cloud world's applyToRows
//   (charm-nest-cloud-world.js) waited for Session.ready() before putting the cloud's lines on the page: on a viewer it asked again every 800 ms for ever, and put none on.
//   The run follower (charm-nest-run-follow.js) hands every line to applyToRows once the cloud world drives the page, so a line the cloud's run holds and the world does
//   not hold yet (or holds with no receipt read) was never added either.
//   P  the two modules in a bare context: a viewer (CNViewer on, Session frozen, never ready) takes the world's lines at once and asks no more; a page in mode off whose
//      Session is not ready still waits as before; a line the run holds and the world lacks is added on a viewer and is not called "not in the cloud"; off: not added
//   E  the REAL page over the fake site, booted GENUINELY as a viewer (Charm_Nest_Open/drive mode "on" seeded before it loads): every line of the cloud's run, the pulled ones
//      too, is in its Orders list; a new order the cloud then pulls comes in at the world's feed bump with nothing pressed; the same page in mode off boots as before
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item2.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const R = 'run-2026-11-21-item2';

/* ── P: the modules in a bare context ── */
function bare(o) {
  const ev = new Map(), timers = [];
  const w = { console: { log() {}, warn() {}, error() {} }, Promise, Date, JSON, Map, Set, Math, Object, Array, String, Number };
  w.setTimeout = (fn, ms) => { timers.push({ fn, ms }); return timers.length; }; w.clearTimeout = () => {}; w.setInterval = () => 0; w.clearInterval = () => {};
  w.window = w; w.document = { readyState: 'complete', hidden: false, addEventListener() {} };
  w.addEventListener = (n, fn) => ev.set(n, fn); w.dispatchEvent = () => true; w.CustomEvent = function (n, d) { this.type = n; this.detail = d && d.detail; };
  w.localStorage = { getItem: () => null, setItem() {}, removeItem() {}, key: () => null, length: 0 };
  const rows = [];
  w.B = { run: null, orders: { rows, byKey: new Map() } };
  w.Orders = { rows: () => rows, interpretAll() { w.__interp = (w.__interp || 0) + 1; for (const r of rows) if (!r.spec) r.spec = { read: 'here' }; }, render() { w.__render = (w.__render || 0) + 1; },
    rowFromRecord: (k, l) => ({ key: k, order: { receiptId: String(l.orderId) }, line: { transactionId: String(l.transactionId) }, spec: null, problems: [], state: l.state || 'pulled', poolIds: [], fromRecord: true }) };
  w.Session = { ready: () => !!o.ready, frozen: () => !!o.frozen };
  if (o.viewer !== undefined) w.CNViewer = { on: () => !!o.viewer };
  const world = o.world || [];
  w.CN = { S: { cloud: { ok: true }, settings: {} }, api: async (fn, body) => {
    if (body.op === 'worldGet') return { summary: { at: 1000, live: world.length }, review: null, lease: null, lines: world, next: null };
    if (body.op === 'runList') return { runs: [{ runId: R, status: 'running', lines: Object.keys(o.run || {}).length }] };
    if (body.op === 'runGet') return { run: { runId: R, status: 'running', lines: o.run || {}, orders: [], sheets: {} } };
    return { ok: true };
  } };
  const ctx = vm.createContext(w);
  vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-cloud-world.js'), 'utf8'), ctx);
  vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-run-follow.js'), 'utf8'), ctx);
  return { w, rows, timers, ev };
}
const wl = (k, state) => ({ key: k, sig: 's-' + k, at: 1000, order: { receiptId: k.split('_')[0] }, line: { transactionId: k.split('_')[1] }, spec: { designSku: 'LEAF_CHARM', material: 'gold' }, problems: [], state: state || 'pulled', run: {} });
async function partP() {
  const K1 = '4296000001_42960000011', K2 = '4296000002_42960000021', K3 = '4296000003_42960000031';
  { const { w, rows, timers } = bare({ viewer: true, frozen: true, ready: false, world: [wl(K1), wl(K2, 'pooled')] });
    await w.CloudWorld.refresh('whole');
    const a = w.CloudWorld.applyToRows('world');
    const again = timers.filter(t => t.ms === 800).length;
    ok('P1 a viewer (mode on, Session frozen, never ready): the cloud world\'s lines are put on its rows at once, the pulled one too, and it asks no more', !!a && a.added === 2 && rows.length === 2 && rows.some(r => r.key === K1 && r.state === 'pulled') && again === 0, { a, rows: rows.map(r => [r.key, r.state]), again }); }
  { const { w, rows, timers } = bare({ viewer: false, frozen: false, ready: false, world: [wl(K1)] });
    await w.CloudWorld.refresh('whole');
    const a = w.CloudWorld.applyToRows('world');
    ok('P2 mode off, the page\'s own start not done (Session not ready): it waits for it as before (nothing put on, asked again in 800 ms)', a === null && rows.length === 0 && timers.some(t => t.ms === 800), { a, n: rows.length }); }
  { const { w, rows, timers } = bare({ viewer: undefined, ready: false, world: [wl(K1)] });
    await w.CloudWorld.refresh('whole');
    ok('P3 a page with no viewer module: waits as before', w.CloudWorld.applyToRows('world') === null && timers.some(t => t.ms === 800)); }
  { const run = { [K1]: { orderId: '4296000001', transactionId: '42960000011', state: 'pulled' }, [K3]: { orderId: '4296000003', transactionId: '42960000031', state: 'pulled', sku: 'LEAF_CHARM' } };
    const { w, rows } = bare({ viewer: true, frozen: true, ready: false, world: [wl(K1)], run });
    await w.CloudWorld.refresh('whole');
    await w.CharmNestRunFollow.now('feed');
    const r3 = rows.find(r => r.key === K3);
    ok('P4 a viewer: a line the cloud\'s run holds (pulled) and the world does not hold yet is added from the run, read, and listed', rows.length === 2 && !!r3 && r3.state === 'pulled' && !!r3.spec && w.__render > 0, rows.map(r => [r.key, r.state, !!r.spec]));
    w.CloudWorld.applyToRows('world-again');
    ok('P5 ... and the world\'s next pass does not call it gone ("not in the cloud") while the run holds it', rows.find(r => r.key === K3).state === 'pulled', rows.map(r => [r.key, r.state, r.reason]));
    // the run lets the line go: it is gone here too
    delete run[K3];
    await w.CharmNestRunFollow.now('feed');
    ok('P6 ... once the run no longer holds it (and the world does not), it is gone here too', rows.find(r => r.key === K3).state === 'gone', rows.map(r => [r.key, r.state, r.reason])); }
  { const run = { [K3]: { orderId: '4296000003', transactionId: '42960000031', state: 'pulled' } };
    const { w, rows } = bare({ viewer: false, frozen: false, ready: true, world: [wl(K1)], run });
    await w.CloudWorld.refresh('whole');
    await w.CharmNestRunFollow.now('feed');
    ok('P7 mode off (no viewer): the run follower does as before (the world\'s lines only; nothing added from the run here)', rows.length === 1 && rows[0].key === K1, rows.map(r => r.key)); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P8 charm-nest-1.html loads the changed files under new tokens', /charm-nest-cloud-world\.js\?v=[^"]*-it2(?:-\w+)*"/.test(html) && /charm-nest-run-follow\.js\?v=[^"]*-it2(?:-\w+)*"/.test(html));
}

/* ── E: the REAL page, booted as a viewer ── */
async function partE() {
  let Base; try { Base = require('./_orderFactsWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const w = await Base.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { log0('E: no playwright-core: the real page was not run'); return; }
  const page = w.page, st = w.srv.st, lib = st.handlers.charmNestLibrary;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 250)); } };
  try {
    // 1 · mode off (no switch on the fake site): the page boots as before, its workspace kept; the fixtures read by the page's own intake give the cloud's lines their exact shape
    await until(() => page.evaluate(() => !!(window.CNViewer && CNViewer.known() && Session.ready())), 30000);
    const off = await page.evaluate(() => ({ viewer: !!(window.CNViewer && CNViewer.on()), ready: Session.ready(), frozen: Session.frozen() }));
    ok('E0 mode off: the page boots as before (no viewer, its Session listening and saved)', off.viewer === false && off.ready === true && off.frozen === false, off);
    const OLD = ['single', 'multi'], NEW = ['qty3', 'late', 'message'], LATER = ['pairMatching'];
    await Base.seed(page, OLD.concat(NEW, LATER));
    const made = await page.evaluate(() => Orders.rows().filter(r => r && !r.parentRow).map(r => ({ key: r.key, order: r.order, line: r.line, spec: r.spec, problems: r.problems || [], rec: CharmNestPoolMake.lineRecord(r, B)[1] })));
    const byRid = rid => made.filter(m => String(m.order.receiptId) === String(rid));
    const keysOf = names => names.flatMap(n => byRid(Base.FIXTURES[n].rid).map(m => m.key));
    // 2 · the fake site as the cloud has it since 20 Nov 23:41: the switch on, the cloud carrying a running run whose lines are the old ones (pooled) and the new pulled ones,
    //     and the cloud world holding every one of them; plus one pulled line the run holds and the world has not read yet
    const T = Date.now();
    let at = T - 60000;
    const putWorld = (m, state) => { at += 1; const doc = { v: 1, key: m.key, receiptId: String(m.order.receiptId), order: m.order, line: m.line, spec: m.spec, problems: m.problems, state, reason: null, run: { poolIds: [] }, etsyOpen: true, inRun: true, fromRecord: false, runId: R };
      st.put('Charm_World_Lines', m.key, { v: 1, key: m.key, receiptId: doc.receiptId, state, runId: R, sig: 'sig-' + m.key + '-' + at, at, gone: false, data: JSON.stringify(doc) }, false); };
    const runLines = {};
    for (const m of made) {
      const name = Object.keys(Base.FIXTURES).find(n => Base.FIXTURES[n].rid === String(m.order.receiptId));
      if (LATER.includes(name)) continue;
      const state = OLD.includes(name) ? 'pooled' : 'pulled';
      runLines[m.key] = Object.assign({}, m.rec, { state });
      if (name !== 'message') putWorld(m, state);   // ('message': the run holds it, the world has not read it yet)
    }
    const summary = () => ({ at, live: Object.keys(runLines).length, counts: { today: Object.keys(runLines).length } });
    st.put('Charm_World', 'summary', summary(), false);
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: T - 3600e3 });
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'pool', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [...new Set(Object.values(runLines).map(l => String(l.orderId)))], lines: runLines, createdAt: T - 7200e3, updatedAt: T });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T + 3600e3, since: T - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    // 3 · the same computer loads the page again: it boots as a viewer, by the real boot path (CNViewer.boot reads the switch, Session.freeze, no restore)
    await page.reload({ waitUntil: 'load' });
    const booted = await until(() => page.evaluate(() => !!(window.CNViewer && CNViewer.known() && CNViewer.on() && window.Session && Session.frozen() && window.CloudWorld && CloudWorld.state().loadedAt)), 60000);
    const v = await page.evaluate(() => ({ viewer: CNViewer.on(), frozen: Session.frozen(), ready: Session.ready(), run: B.run ? B.run.runId : null, world: CloudWorld.state() }));
    ok('E1 the page booted as a viewer by its own boot (mode on read from the fake cloud): frozen, no run held, the cloud world read', !!booted && v.viewer && v.frozen && !v.run && v.world.size === Object.keys(runLines).length - 1, v);
    const want = Object.keys(runLines), pulled = keysOf(NEW);
    const have = async () => page.evaluate(keys => { const by = new Map(Orders.rows().map(r => [r.key, r])); return keys.map(k => { const r = by.get(k); return { k, state: r ? r.state : null, spec: !!(r && r.spec) }; }); }, want);
    const all = await until(async () => { const h = await have(); return h.every(x => x.state && x.state !== 'gone' && x.spec) ? h : null; }, 30000);
    const got = all || await have();
    const facts = await page.evaluate(() => ({ applied: CloudWorld.state().applied, follow: (({ runId, last }) => ({ runId, last }))(CharmNestRunFollow.state()), rows: Orders.rows().length }));
    ok('E2 the viewer\'s Orders hold EVERY line of the cloud\'s run, the pulled ones too (and the one the world has not read yet), each read', !!all && pulled.every(k => got.find(x => x.k === k && x.state === 'pulled')),
      { got, facts });
    // what a person sees: the Orders tab lists them
    const shown = await page.evaluate(rids => { CN.setMode('orders'); Orders.render(); const t = (document.getElementById('ordersView') || document.querySelector('[data-view=orders]') || document.body).innerText; return rids.filter(r => t.includes(r)); },
      NEW.concat(OLD).map(n => Base.FIXTURES[n].rid));
    const why3 = shown.length === NEW.length + OLD.length ? null : await page.evaluate(rid => { const r = Orders.rows().find(x => String(x.order && x.order.receiptId) === rid); let view = null; try { view = Orders.view(); } catch (_) {}
      return { row: r && { key: r.key, state: r.state, fromRecord: r.fromRecord, runOnly: r.runOnly, order: r.order, spec: r.spec && { designSku: r.spec.designSku, material: r.spec.material } }, view, html: (document.getElementById('ordersView') ? 'ordersView' : 'body'), len: document.body.innerText.length }; }, Base.FIXTURES.message.rid);
    ok('E3 ... and the Orders tab shows every one of those orders', shown.length === NEW.length + OLD.length, { shown, why3 });
    // 4 · the cloud pulls a new order: the world takes it and its feed is bumped (as the world build does). The viewer shows it with nothing pressed
    const m2 = made.filter(m => String(m.order.receiptId) === Base.FIXTURES.pairMatching.rid);
    for (const m of m2) { runLines[m.key] = Object.assign({}, m.rec, { state: 'pulled' }); putWorld(m, 'pulled'); }
    st.put('Charm_World', 'summary', summary(), false);
    st.put('Charm_Nest_Runs', R, { lines: runLines, updatedAt: Date.now() });
    await lib.worldBump();
    const later = await until(() => page.evaluate(keys => keys.every(k => { const r = Orders.rows().find(x => x.key === k); return r && r.state === 'pulled' && r.spec; }), m2.map(m => m.key)), 90000);
    ok('E4 a new order the cloud pulls afterwards comes onto the viewer by itself (the world\'s feed), pulled', !!later && m2.length > 0, await page.evaluate(() => CloudWorld.state()));
    // (before the fix the world's pass answered null here, waiting for a Session.ready() a viewer never reaches, and set itself again every 800 ms)
    const pass = await page.evaluate(() => CloudWorld.applyToRows('probe'));
    ok('E5 the world\'s pass on the viewer goes through (it does not wait for a start a viewer never makes)', !!pass && pass.why === 'probe' && pass.size >= 6, pass);
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  if (process.env.IT2_PART === 'offline') await partP();
  else {
    if (!process.env.SKIP_PAGE) await partE();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { IT2_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) failed++;
  }
  log0(failed ? `FAIL viewer-item2 (${failed})` : 'PASS viewer-item2');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item2: ' + (e && e.stack || e)); process.exit(1); });
