// ITEM 8 (Paul, 21 Nov 2026): Release Hold with things set aside ("release anyway", a blocked SKU cut anyway), pressed on a staff computer (a viewer) while the cloud carries every run (mode "on").
//   What was wrong: the viewer's release sent only { orderId, by, dev, runId }: the cloud lifted the hold, and the line kept its questions or its blocked SKU while the page said
//   "Released in the cloud". An order NOT on hold went down the page's own way (made up here: nothing is; saveLinesNow: writes nothing) while the toast said "first in line".
//   S  the library in mode "on" (offline, _cloudFakes.cjs): releaseAsk from a viewer with the page's own body (keys, lines with releaseAnyway / blockOverride, seal, note) releases the held
//      line WITH what was set aside; queue: true takes a line that is not held but has something set aside (a blocked SKU, an unmapped / unknown design): first in line, "pulled" again,
//      the seal on it, no timeline step of the cloud's (the page writes its own); a line with nothing set aside is left alone; without queue (every page before, mode off) nothing changes.
//      Then the cloud's own code applies it: the intake takes the line only once the world has read it with the override (Intake.rowOf), the piece maker's block gate lets it through
//      (O.blockFor), the world's reading sets the questions aside (World.afterRead), and the queue puts it first (O.byQueue).
//   P  charm-nest-viewer.js in a bare context: in mode "on" OrderHold.release of an order whose held lines are on the page is the page's own release (its cloud path builds the body);
//      an order the page does not hold goes as before, now with the press's seal; in mode "off" OrderHold.release is the page's, untouched
//   E  the REAL page over the fake site (_orderFactsWorld.cjs), made a viewer after its orders are in (Session frozen, no run held, the run followed, mode "on" on the fake site): Release Hold
//      on a blocked SKU and on an unknown SKU (released anyway) reaches the cloud's run line with blockOverride / releaseAnyway, first in line, the seal; the person is told what the cloud did,
//      never "placed when the next run starts" or "first in line" over a press that reached nowhere; one "released" step on each timeline
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item8.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1400))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const O = require(path.join(ROOT, 'charm-nest-orders.js'));
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js'), Intake = F.load('_charmNestIntake.js'), World = F.load('_charmNestWorld.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const R = 'run-2026-11-21-item8';
    const A = '4290000801_42900008011', B = '4290000802_42900008021', C = '4290000803_42900008031', D = '4290000804_42900008041', rid = k => k.split('_')[0];
    const seed = mode => {
      F.reset(); F.clock.set(T0);
      F.seed('Charm_Nest_Open/drive', { mode, runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'takeoff', 'cmd:release', 'cmd:placewaiting'], flippedAt: T0 - 3600e3 }); lib._driveForget();
      F.seed('Charm_Nest_Open/run', { runId: R, at: T0 - 3600e3 });
      F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 - 3600e3 } });
      F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [rid(A), rid(B), rid(C), rid(D)],
        lines: {
          [A]: { orderId: rid(A), state: 'held', hold: 'On hold by Test Operator: ask the buyer', heldAt: T0 - 7200e3, reason: 'On hold by Test Operator: ask the buyer', poolIds: [] },   // a person's hold, with an unmapped option
          [B]: { orderId: rid(B), state: 'held', reason: 'SKU blocked: open outline', poolIds: [], sku: 'SCHRODINGER CAT 2' },   // the piece maker's hold: a blocked SKU (no person's hold)
          [C]: { orderId: rid(C), state: 'unmatched', reason: 'not in any master file', poolIds: [], sku: 'NO_SUCH_DESIGN_77' },   // an unknown SKU
          [D]: { orderId: rid(D), state: 'pulled', reason: null, poolIds: [] } } });
      F.seed('Charm_Pool/' + A + '_1', { poolId: A + '_1', orderId: rid(A), runId: R, state: 'abandoned', sheetId: null, heldAt: T0 - 7200e3, heldBy: 'Test Operator' });
    };
    const lines = async () => { const g = await P({ op: 'runGet', runId: R }); return (g.run && g.run.lines) || {}; };
    const tl = async r => ((await P({ op: 'timelineGet', orderId: r })).events || []).filter(e => e.type === 'released');
    const who = 'Michelle', at = T0 - 1000, seal = r => ({ how: 'releaseHold', at, by: who, id: `releaseHold:${r}:${at}` });
    const anyway = kinds => ({ at, by: who, kinds });

    seed('on');
    // S1 the held order, as the page's own release builds it (charm-nest-order-release.js askCloud): keys, what was set aside, the seal, the note
    let r = await P({ op: 'releaseAsk', orderId: rid(A), by: who, dev: PAGE, cv: CV, runId: R, keys: [A], lines: { [A]: { releaseAnyway: anyway(['needsMapping']) } }, seal: seal(rid(A)), note: 'released anyway: the option “Style: Box” is not mapped' });
    let L = await lines(), a = L[A] || {}, ev = await tl(rid(A));
    ok('S1 mode on, a viewer\'s Release Hold with the page\'s own body: the hold is lifted AND the question set aside stays on the line (releaseAnyway), first in line, the seal kept',
      r.http === 200 && r.ok && !a.hold && a.state === 'pulled' && a.reason == null && a.releaseAnyway && a.releaseAnyway.kinds.join() === 'needsMapping' && a.frontAt === r.frontAt && a.releaseHold && a.releaseHold.by === who && a._lp && a._lp.dev === 'cloud' && +F.doc('Charm_Pool/' + A + '_1').repooledAt > 0, { r, a });
    ok('S1b ... one "released" step on its timeline, with the seal and what was set aside', ev.length === 1 && ev[0].data && ev[0].data.seal === 'releaseHold' && /released anyway: the option/.test(ev[0].text), ev);
    // S2 a blocked SKU cut anyway, no person's hold (Review's Release Hold, queue)
    const bo = { sku: 'SCHRODINGER CAT 2', by: who, at };
    r = await P({ op: 'releaseAsk', orderId: rid(B), by: who, dev: PAGE, cv: CV, runId: R, keys: [B], lines: { [B]: { blockOverride: bo, releaseAnyway: anyway(['blockedSku']) } }, queue: true, seal: seal(rid(B)) });
    L = await lines(); const b = L[B] || {};
    ok('S2 queue: the blocked SKU\'s line carries the block set aside (blockOverride) and is "pulled" again, first in line, with the seal',
      r.http === 200 && r.ok && !r.already && b.state === 'pulled' && b.reason == null && b.blockOverride && b.blockOverride.sku === 'SCHRODINGER CAT 2' && b.blockOverride.by === who && b.frontAt === r.frontAt && b.releaseHold && b.releaseHold.id === seal(rid(B)).id && r.lines[B] && r.lines[B].releasedAt === r.frontAt, { r, b });
    ok('S2b ... no "released" step of the cloud\'s (no hold was lifted: the page that pressed writes its own, as it always did)', (await tl(rid(B))).length === 0);
    // S3 an unknown SKU released anyway (queue): unmatched → pulled with its question set aside
    r = await P({ op: 'releaseAsk', orderId: rid(C), by: who, dev: PAGE, cv: CV, runId: R, keys: [C], lines: { [C]: { releaseAnyway: anyway(['unmatchedSku']) } }, queue: true, seal: seal(rid(C)), note: 'released anyway: SKU NO_SUCH_DESIGN_77 has no design in any master file yet' });
    L = await lines(); const c = L[C] || {};
    ok('S3 queue: the unknown SKU\'s line ("unmatched") is "pulled" again with its question set aside, first in line', r.ok && c.state === 'pulled' && c.releaseAnyway && c.releaseAnyway.kinds.join() === 'unmatchedSku' && c.frontAt === r.frontAt, { r, c });
    // S4 a line with nothing set aside is not taken by queue
    r = await P({ op: 'releaseAsk', orderId: rid(D), by: who, dev: PAGE, cv: CV, runId: R, keys: [D], lines: {}, queue: true });
    L = await lines();
    ok('S4 queue: a line with nothing set aside and no hold is left as it is (the answer says so: already)', r.ok && r.already === true && L[D].state === 'pulled' && !L[D].frontAt && !L[D].releaseHold, { r, d: L[D] });
    // S5 the cloud applies what was set aside: its own intake, piece maker gate, world reading and queue order
    const wl = (k, facts) => ({ key: k, order: { receiptId: rid(k), createTs: 1763700000 }, line: { transactionId: k.split('_')[1], sku: 'SCHRODINGER CAT 2' }, spec: { designSku: 'SCHRODINGER CAT 2', material: 'gold', problems: [] }, problems: [], runId: R, run: World.runFacts(facts) });
    const before = { orderId: rid(B), state: 'held', reason: 'SKU blocked: open outline', poolIds: [] };
    const rowB = Intake.rowOf(B, b, wl(B, b), R), stale = Intake.rowOf(B, b, wl(B, before), R);
    const entry = { blocked: 'open outline', open: true };
    ok('S5 the cloud\'s intake takes the released line once the world has read it with the override (state pulled: due; blockOverride and frontAt carried), and waits while the world read it without',
      !!rowB && rowB.state === 'pulled' && rowB.blockOverride && rowB.blockOverride.sku === 'SCHRODINGER CAT 2' && rowB.frontAt === b.frontAt && stale === null, { rowB: rowB && { state: rowB.state, blockOverride: rowB.blockOverride, frontAt: rowB.frontAt }, stale });
    ok('S5b ... the piece maker\'s block gate (O.blockFor, makePool) cuts the SKU with the override and still refuses it without', !O.blockFor(entry, 'SCHRODINGER CAT 2', rowB && rowB.blockOverride) && !!O.blockFor(entry, 'SCHRODINGER CAT 2', null));
    const rc = World.afterRead({ spec: { problems: [{ kind: 'unmatchedSku', reason: 'not in any master file' }], material: 'gold' }, state: 'pulled', releaseAnyway: c.releaseAnyway }, null, null);
    ok('S5c ... the world\'s reading sets the unknown SKU\'s question aside (released anyway)', rc.problems.length === 0, rc.problems);
    const q = [{ key: D, frontAt: 0, order: { createTs: 1 } }, { key: B, frontAt: b.frontAt, order: { createTs: 9e9 } }].sort(O.byQueue(x => x.order.createTs));
    ok('S5d ... and the queue puts the released order first, ahead of an older order', q[0].key === B, q.map(x => x.key));
    // S6 drive off / a page before this build (no queue): nothing changes for a line that is not held
    seed('off');
    r = await P({ op: 'releaseAsk', orderId: rid(C), by: who, dev: PAGE, runId: R, keys: [C], lines: { [C]: { releaseAnyway: anyway(['unmatchedSku']) } } });
    L = await lines();
    ok('S6 mode off, no queue (every page as before): a line that is not held is not taken, as before', r.ok && r.already === true && L[C].state === 'unmatched' && !L[C].releaseAnyway, { r, c: L[C] });
    r = await P({ op: 'releaseAsk', orderId: rid(B), by: who, dev: PAGE, runId: R, keys: [B], lines: { [B]: { blockOverride: bo } }, seal: seal(rid(B)) });
    L = await lines();
    ok('S6b ... and a line the piece maker held ("held") is released with its block set aside, and gets the cloud\'s step, as before', r.ok && L[B].state === 'pulled' && L[B].blockOverride && (await tl(rid(B))).length === 1, { r, b: L[B] });
  } finally { console.log = quiet; console.warn = qw; }
}

async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8');
  async function page(o) {
    const calls = [], own = [];
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => { const t = setTimeout(f, ms); if (t && t.unref) t.unref(); return t; }, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, Date, JSON, performance: { now: () => 0 } };
    w.window = w;
    w.B = { run: null };
    w.Orders = { rows: () => o.rows || [], heldNow: r => !!r.hold };
    w.employeeName = () => 'Michelle';
    w.CharmNestRunFollow = { state: () => ({ runId: 'run-2026-11-21-item8' }), dev: () => PAGE, now: () => {} };
    const mine = (rid, opts) => { own.push({ rid, opts }); return Promise.resolve({ ok: true, released: true, via: 'cloud' }); };
    w.OrderHold = { release: mine };
    w.CN = { api: async (fn, body) => {
      calls.push(body);
      if (body.op === 'driveGet') return { ok: true, mode: o.mode || 'on', drive: { mode: o.mode || 'on', cloudSteps: ['takeoff', 'cmd:release'] } };
      if (body.op === 'releaseAsk') return { ok: true, frontAt: 123 };
      return { ok: true };
    } };
    vm.runInContext(src, vm.createContext(w));
    await w.CNViewer.ready();
    return { w, calls, own, mine };
  }
  const sealP = { how: 'releaseHold', at: 1763737200000, by: 'Michelle', id: 'releaseHold:4290000801:1763737200000' };
  { const { w, calls, own } = await page({ rows: [{ key: '4290000801_42900008011', order: { receiptId: '4290000801' }, state: 'held', hold: 'On hold' }] });
    const res = await w.OrderHold.release('4290000801', { name: 'Michelle', seal: sealP });
    ok('P1 mode on: an order whose held line is on the page is released by the page\'s own release (its cloud path sends the keys, what was set aside, the seal, the note), not the bare ask',
      w.OrderHold.release._viewer === true && own.length === 1 && own[0].opts.seal === sealP && !calls.some(c => c.op === 'releaseAsk') && res.ok, { own, calls: calls.map(c => c.op) }); }
  { const { w, calls, own } = await page({ rows: [] });
    const res = await w.OrderHold.release('4290000809', { name: 'Michelle', seal: sealP });
    const ask = calls.find(c => c.op === 'releaseAsk');
    ok('P2 ... an order the page does not hold is asked of the cloud as before, now with the press\'s seal', own.length === 0 && ask && ask.orderId === '4290000809' && ask.by === 'Michelle' && ask.seal && ask.seal.id === sealP.id && ask.runId === 'run-2026-11-21-item8' && res.ok && res.via === 'cloud', { ask, res }); }
  { const { w, mine } = await page({ mode: 'off', rows: [{ key: 'x_y', order: { receiptId: '4290000801' }, hold: 'On hold' }] });
    ok('P3 mode off: OrderHold.release is the page\'s own, untouched', w.OrderHold.release === mine); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P4 charm-nest-1.html loads the changed viewer and bridge under new tokens (-it8)', /charm-nest-viewer\.js\?v=1120-v14-on1[-\w]*-it8(?:-\w+)*"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-it8(?:-\w+)*"/.test(html));
}

async function partE() {
  let Base, Rw; try { Base = require('./_orderFactsWorld.cjs'); Rw = require('./_engraveRealWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const W = await Base.world({ viewport: { width: 1440, height: 950 }, reducedMotion: 'reduce' });
  if (!W) { log0('E: no playwright-core: the real page was not run'); return; }
  const { page, srv } = W, st = srv.st;
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 200)); } };
  const V = (name, value) => ({ name, value });
  const CAT2 = { rid: '4290000831', sku: 'SCHRODINGER CAT 2', listing: '1890000831', title: 'Cat Necklace, Whimsical Cat Charm Gift', buyer: 'Name Eighty', placed: { day: 0, h: 9, m: 5 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
  const UNKNOWN = { rid: '4290000832', sku: 'NO_SUCH_DESIGN_77', listing: '1890000832', title: 'Mystery Charm Necklace', buyer: 'Name Eighty One', placed: { day: 0, h: 9, m: 15 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] };
  const keyOf = fx => `${fx.rid}_${fx.rid}1`;
  const RUN = 'run-2026-11-21-it8page';
  try {
    Rw.seedMaster(st);
    st.put('Charm_Master_Index', 'SCHRODINGER CAT 2', Object.assign({}, st.doc('Charm_Master_Index', 'SCHRODINGER CAT 2'), { blocked: 'open outline', open: true }));
    await page.evaluate(async () => { await Master.load(true); await Orders.loadMaps(true); });
    await page.evaluate(RUN => {
      window.__toasts = []; const was = window.toast; window.toast = (m, k, ms) => { window.__toasts.push([k || '', String(m)]); return was && was(m, k, ms); };
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: RUN, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, RUN);
    await page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const fx of fixtures) { const order = orderOf(fx, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: [CAT2, UNKNOWN], orderSrc: Base.orderOf.toString() });
    await page.evaluate(async () => { await Pool.addAll(B.run); });
    const rowOf = fx => page.evaluate(k => { const r = B.orders.byKey.get(k); return r ? { state: r.state, hold: r.hold || null, frontAt: r.frontAt || null, problems: (r.problems || []).map(p => p.kind), override: r.blockOverride || null, anyway: r.releaseAnyway || null, held: Orders.heldNow(r) } : null; }, keyOf(fx));
    const c0 = await rowOf(CAT2), u0 = await rowOf(UNKNOWN);
    ok('E0 the blocked SKU and the unknown SKU wait with their questions (the page made them up as the run\'s computer)', c0 && c0.problems.join() === 'blockedSku' && u0 && u0.problems.join() === 'unmatchedSku', { c0, u0 });
    // the cloud's run, as the run's computer left it (its lines), then the page becomes a viewer: no run held, the run followed, its workspace frozen, mode "on" on the fake site
    const recs = await page.evaluate(() => Object.fromEntries(Orders.rows().map(Orders.lineRecord)));
    await page.evaluate(RUN => {
      B.run = null;
      const F = window.CharmNestRunFollow, st0 = F.state; F.state = () => Object.assign(st0(), { runId: RUN });
      window.__viewerOn = CNViewer.on; CNViewer.on = () => true; CNViewer.asViewer();
      Session.freeze();
    }, RUN);
    st.put('Charm_Nest_Runs', RUN, { runId: RUN, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', releasePolicy: 2, sheets: {}, holds: {}, errors: [], orders: [CAT2.rid, UNKNOWN.rid], lines: recs }, false);
    st.put('Charm_Nest_Open', 'run', { runId: RUN, at: Date.now() });
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'auto', cloudSteps: ['pull', 'pool', 'nest', 'takeoff', 'cmd:release', 'cmd:placewaiting'], flippedAt: Date.now() - 3600e3 });
    st.put('Charm_Nest_Run_Carrier', RUN, { runId: RUN, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now() + 3600e3, since: Date.now() - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    const viewer = await page.evaluate(() => ({ frozen: Session.frozen(), run: !!B.run, own: !!OrderHold.release._viewer }));
    ok('E1 the page is a viewer: workspace frozen, no run held, OrderHold.release the viewer\'s', viewer.frozen && !viewer.run && viewer.own, viewer);
    const cloudLine = k => { let lines = Object.assign({}, (st.doc('Charm_Nest_Runs', RUN) || {}).lines || {}); for (const p of st.list('Charm_Nest_Run_Live').filter(x => x.runId === RUN)) { try { Object.assign(lines, JSON.parse(p.json || '{}')); } catch (_) {} } return lines[k] || null; };
    const tl = rid => st.list('Order_Timeline').filter(e => String(e._id).startsWith(rid + '~') && e.type === 'released');
    const press = async fx => {
      await page.evaluate(() => { CN.setMode('review'); Review.syncOrderItems(); Review.render(); });
      const sel = `#rvList .reviewListRow[data-rid="${fx.rid}"] [data-rv-release]`;
      if (!(await until(() => page.$(sel), 20000))) return 'no button';
      const n0 = await page.evaluate(() => window.__toasts.length);
      await page.click(sel);
      await until(() => page.evaluate(({ rid, n0 }) => window.__toasts.length > n0 && !OrderHold.releaseStatus(rid).running && !(window.HoldUI && HoldUI.busy && HoldUI.busy(rid)), { rid: fx.rid, n0 }), 60000);
      await page.waitForTimeout(1200);
      return page.evaluate(n0 => window.__toasts.slice(n0), n0);
    };
    // ── the blocked SKU (its line is held by the piece maker: OrderHold.release on the viewer → the page's own release, the cloud's path)
    const t1 = Date.now(), said1 = await press(CAT2);
    const l1 = await until(() => { const l = cloudLine(keyOf(CAT2)); return l && l.blockOverride ? l : null; }, 20000) || cloudLine(keyOf(CAT2));
    ok('E2 Release Hold on the blocked SKU from the viewer reaches the cloud\'s run line WITH the block set aside: blockOverride, "pulled", first in line, the seal',
      !!l1 && l1.state === 'pulled' && !l1.hold && l1.blockOverride && l1.blockOverride.sku === 'SCHRODINGER CAT 2' && l1.blockOverride.by === 'Test Operator' && +l1.frontAt >= t1 - 1000 && l1.releaseHold && l1.releaseHold.by === 'Test Operator' && l1._lp && l1._lp.dev === 'cloud', { said1, l1 });
    ok('E3 ... the person is told the cloud released it and places it; no failure, never "placed when the next run starts"',
      Array.isArray(said1) && said1.some(t => t[0] === 'ok' && /Order 4290000831 released in the cloud/.test(t[1])) && !said1.some(t => t[0] === 'bad' || /placed when the next run starts|first in line, placed on/.test(t[1])), said1);
    const ev1 = await until(() => { const l = tl(CAT2.rid); return l.length ? l : null; }, 20000) || [];
    const c1 = await rowOf(CAT2);
    ok('E4 ... one "released" step on its timeline, with the seal; the page\'s row carries the override and asks nothing', ev1.length === 1 && ev1[0].data && ev1[0].data.seal === 'releaseHold' && c1.override && c1.override.sku === 'SCHRODINGER CAT 2' && !c1.problems.length, { ev1: ev1.map(e => [e.text, e.data && e.data.seal]), c1 });
    // ── the unknown SKU, released anyway (not on hold: liftOrder's own way on a viewer → releaseAsk queue)
    const t2 = Date.now(), said2 = await press(UNKNOWN);
    const l2 = await until(() => { const l = cloudLine(keyOf(UNKNOWN)); return l && l.releaseAnyway ? l : null; }, 20000) || cloudLine(keyOf(UNKNOWN));
    ok('E5 Release Hold on the unknown SKU (released anyway) from the viewer reaches the cloud\'s run line: releaseAnyway with its question, "pulled" again, first in line, the seal',
      !!l2 && l2.state === 'pulled' && l2.releaseAnyway && (l2.releaseAnyway.kinds || []).includes('unmatchedSku') && +l2.frontAt >= t2 - 1000 && l2.releaseHold && l2.releaseHold.by === 'Test Operator', { u0, said2, l2 });
    ok('E6 ... the person is told the cloud makes its pieces up and places them, with what was set aside; never the page\'s "first in line" over nothing',
      Array.isArray(said2) && said2.some(t => t[0] === 'ok' && /Order 4290000832 released in the cloud: first in line; the cloud makes its pieces up and places them\. Released anyway: SKU NO_SUCH_DESIGN_77/.test(t[1])) && !said2.some(t => t[0] === 'bad' || /released to the nesting queue: first in line/.test(t[1])), said2);
    const ev2 = await until(() => { const l = tl(UNKNOWN.rid); return l.length ? l : null; }, 20000) || [];
    ok('E7 ... one "released" step on its timeline (the page\'s own, with the seal and what was set aside; none of the cloud\'s "from hold")',
      ev2.length === 1 && /Released to the nesting queue by Test Operator · Release Hold \(Review\)/.test(ev2[0].text) && ev2[0].data && ev2[0].data.seal === 'releaseHold' && (ev2[0].data.anyway || []).length > 0, ev2.map(e => [e.text, e.data]));
    await page.evaluate(() => { CNViewer.on = window.__viewerOn; });
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 4).join(' | ')); }
  const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
  ok('E no page error', errs.length === 0, errs.slice(0, 2).join(' | '));
  await W.close();
}

(async () => {
  // (parts S and P in a process of their own: the fake site of part E and the offline cloud of part S each load the library with their own database, and one process keeps one copy of it)
  if (process.env.IT8_PART === 'offline') { await partS(); await partP(); }
  else {
    if (process.env.IT8_PAGE === '1') await partE();   // (the real page, part E: IT8_PAGE=1; not yet run green)
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { IT8_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0(out.slice(-2000)); }
  }
  log0(failed ? `FAIL viewer-item8 (${failed})` : 'PASS viewer-item8');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item8: ' + (e && e.stack || e)); process.exit(1); });
