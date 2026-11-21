// S2 (Paul, 21 Nov 2026): Review on the staff computers (viewers) in cloud mode "on".
//   1 · Resolved (the decisions a person answered, Review.settled) came back from the browser's workspace, which a viewer does not keep: lost at every reload, different on each
//       computer. Now each answer given on a viewer is kept in the cloud (reviewSettledPut, Charm_Nest_Review_Settled/list: the newest 500 of 14 days), read back at the start and
//       at the feed (g:review-settled), and the cloud world reads it into the Review core (mode "on"), so Resolved is the same list on every computer.
//   2 · "Accept the new order" (Order changed, chg:<line>) cards were made only by the page's own Etsy intake, which never runs on a viewer. A viewer now draws the cloud's change cards,
//       and Accept is a press the cloud acts on: takeOffAsk { kind: changed, lineKeys } (the take-off lane makes the line up again; a second press moves nothing again; a line whose
//       change does not touch what it is made of keeps its pieces, as the page's acceptChange).
//   P  charm-nest-viewer.js in a bare context (mode off: nothing sent; mode on: the press, the list merged), and the bridge's changed places (read from the source)
//   S  the server, offline (_cloudFakes.cjs, the real library handler): reviewSettledPut / reviewSettledGet, the cap, the feed, mode on taken; the op classes
//   T  the cloud's take-off lane, offline (the real driver pass): an accepted change is made up again once, per line; an unchanged-shape line keeps its pieces
//   C  the Review core with the cloud's settled list: Resolved lists it (the key every page draws), Open lists the cloud's change card
//   E  the REAL page over the fake site, booted as a viewer: the cloud's change card is drawn, Accept sends the cloud's press, the answer is kept in the cloud, and after a reload
//      (nothing kept on the computer) Resolved lists it again with its words
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-s2.cjs        (SKIP_PAGE=1: without part E)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120';

/* ═══ P · the viewer module in a bare context; the bridge's source ═══ */
async function partP() {
  function bare(o) {
    const calls = [], toasts = [], ev = new Map();
    const w = { console: { log() {}, warn() {}, error() {} }, Promise, Date, JSON, Map, Set, Math, Object, Array, String, Number };
    w.setTimeout = (fn, ms) => { if (!ms || ms < 1000) Promise.resolve().then(fn); return 1; }; w.clearTimeout = () => {}; w.setInterval = () => 0; w.clearInterval = () => {};
    w.window = w; w.document = { hidden: false, addEventListener() {}, getElementById: () => null };
    w.addEventListener = (n, fn) => ev.set(n, fn); w.localStorage = { getItem: () => null, setItem() {}, removeItem() {} };
    w.toast = (t, tone) => toasts.push([tone, t]); w.employeeName = () => o.name == null ? 'Michelle' : o.name;
    w.CharmNestRunFollow = { dev: () => 'd0michelle1', state: () => ({ runId: 'run-s2' }), now() {} };
    const settled = [], rows = new Map();
    w.Review = { settled: () => settled, render() { w.__render = (w.__render || 0) + 1; } };
    w.B = { run: null, orders: { byKey: rows } };
    w.CN = { api: async (fn, body) => {
      calls.push(body);
      if (body.op === 'driveGet') return { ok: true, mode: o.mode, drive: { mode: o.mode, cloudSteps: o.steps || ['takeoff', 'files'] } };
      if (body.op === 'takeOffAsk') return o.refuse ? { error: o.refuse, status: 409 } : { ok: true, orderId: body.orderId, askId: 'a1', kind: body.kind };
      if (body.op === 'reviewSettledGet') return { ok: true, settled: o.cloudList || [] };
      if (body.op === 'reviewSettledPut') return o.putFails ? { error: 'down' } : { ok: true, wrote: 1 };
      return { ok: true };
    } };
    vm.runInContext(fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8'), vm.createContext(w));
    return { w, calls, toasts, settled, rows };
  }
  const flush = async () => { for (let i = 0; i < 12; i++) await new Promise(r => setImmediate(r)); };
  const K = '4300000001_43000000011', RID = '4300000001';
  { const { w, calls, settled } = bare({ mode: 'off' });
    await w.CNViewer.ready(); await flush();
    const put = w.CNViewer.settledPut({ key: 'chg:' + K, kind: 'orderChanged', t: 5, orders: [RID] }), pulled = await w.CNViewer.settledPull('x');
    ok('P1 mode off: an answer is not sent to the cloud, no list is read, Resolved is left to the page as before', put === false && pulled === null && !calls.some(c => /reviewSettled|takeOffAsk/.test(c.op)) && settled.length === 0, calls.map(c => c.op)); }
  { const { w, calls, toasts } = bare({ mode: 'on' });
    await w.CNViewer.ready(); await flush();
    const r = await w.CNViewer.acceptChange({ rid: RID, lineKeys: [K, '4399999999_1'], name: 'Michelle' });
    const ask = calls.find(c => c.op === 'takeOffAsk');
    ok('P2 mode on: Accept the new order is ONE press the cloud acts on: takeOffAsk { kind changed, the line\'s key only, the person\'s name }, said in words', r.ok && !!ask && ask.kind === 'changed' && ask.orderId === RID && JSON.stringify(ask.lineKeys) === JSON.stringify([K]) && ask.by === 'Michelle' && ask.dev === 'd0michelle1' && toasts.some(([t, s]) => t === 'ok' && /accepted/.test(s)), { r, ask, toasts }); }
  { const { w, calls, toasts } = bare({ mode: 'on', steps: ['files'] });
    await w.CNViewer.ready(); await flush();
    const r = await w.CNViewer.acceptChange({ rid: RID, lineKeys: [K], name: 'Michelle' });
    ok('P3 mode on, the cloud\'s take-off lane not on: refused in words, nothing sent', !r.ok && !calls.some(c => c.op === 'takeOffAsk') && toasts.some(([t, s]) => t === 'bad' && /Nothing was changed/.test(s)), { r, toasts }); }
  { const { w, toasts } = bare({ mode: 'on', refuse: 'Order 4300000001 has 20 requests waiting' });
    await w.CNViewer.ready(); await flush();
    const r = await w.CNViewer.acceptChange({ rid: RID, lineKeys: [K], name: 'Michelle' });
    ok('P4 the cloud refuses: the person reads why, and the answer is not taken', !r.ok && toasts.some(([t, s]) => t === 'bad' && /20 requests waiting/.test(s)), toasts); }
  { const { w, toasts } = bare({ mode: 'on', name: '' });
    await w.CNViewer.ready(); await flush();
    const r = await w.CNViewer.acceptChange({ rid: RID, lineKeys: [K], name: '' });
    ok('P5 no name: refused, "sign in first"', !r.ok && toasts.some(([t, s]) => t === 'bad' && /Sign in first/.test(s)), toasts); }
  { const cloudList = [{ key: 'ord:sku:X', kind: 'unmatchedSku', why: 'SKU X unknown', answer: 'Picked LEAF', by: 'Ann', t: 2000, orders: ['4300000002'], lines: 1, pieces: 1, rowKey: '4300000002_43000000021' }];
    const { w, calls, settled, rows } = bare({ mode: 'on', cloudList });
    rows.set('4300000002_43000000021', { key: '4300000002_43000000021', order: { receiptId: '4300000002' } });
    await w.CNViewer.ready(); await flush();
    ok('P6 mode on, at the start: the cloud\'s Resolved list is read and becomes Review.settled (its line found on the page)', calls.some(c => c.op === 'reviewSettledGet') && settled.length === 1 && settled[0].key === 'ord:sku:X' && settled[0].answer === 'Picked LEAF' && !!settled[0].row && settled[0].cloud === true, settled);
    const first = settled[0];
    // an answer given here: unshifted by Review.remove, then sent
    const mine = { key: 'chg:' + K, row: { key: K }, kind: 'orderChanged', why: 'Etsy updated order', answer: 'Accept the change', lines: 1, pieces: 1, orders: [RID], by: 'Michelle', t: 3000, rowKey: K };
    settled.unshift(mine); w.CNViewer.settledPut(mine); await flush();
    const put = calls.find(c => c.op === 'reviewSettledPut');
    ok('P7 an answer given here is sent to the cloud with its words, its line, who and when (reviewSettledPut)', !!put && put.rec.key === 'chg:' + K && put.rec.answer === 'Accept the change' && put.rec.rowKey === K && put.rec.t === 3000 && put.rec.by === 'Michelle' && JSON.stringify(put.rec.orders) === JSON.stringify([RID]), put);
    // the cloud's list read again (another computer's answer added): the same objects are kept, newest first, and nothing given here is lost
    w.CN.api = (orig => async (fn, body) => body.op === 'reviewSettledGet' ? { ok: true, settled: [Object.assign({}, cloudList[0]), Object.assign({}, put.rec), { key: 'ord:opt:A', kind: 'needsMapping', why: 'option', answer: '', by: 'Bo', t: 2500, orders: ['4300000003'], lines: 1 }] } : orig(fn, body))(w.CN.api);
    await w.CNViewer.settledPull('feed');
    ok('P8 the list read again: one record each, newest first, the objects drawn already kept (nothing drawn again for nothing)', settled.length === 3 && settled.map(d => d.t).join() === '3000,2500,2000' && settled[2] === first && settled[0] === mine, settled.map(d => [d.key, d.t])); }
  { const { w, settled } = bare({ mode: 'on', putFails: true });
    await w.CNViewer.ready(); await flush();
    const mine = { key: 'chg:' + K, kind: 'orderChanged', t: 4000, orders: [RID], lines: 1 };
    settled.unshift(mine); w.CNViewer.settledPut(mine); await flush();
    await w.CNViewer.settledPull('feed');
    ok('P9 an answer the cloud has not taken yet stays in Resolved here (not dropped by a read of the cloud\'s list)', settled.length === 1 && settled[0] === mine && mine.pendingCloud === true && w.CNViewer.settledState().pending === 1, w.CNViewer.settledState()); }
  // the bridge, read from its source (it runs whole in part E)
  const src = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
  const acc = src.slice(src.indexOf('async function acceptChange(it)'), src.indexOf('/* ═══ LISTPICK'));
  ok('P10 bridge: acceptChange on a viewer sends the cloud\'s press first and repools nothing here; only a taken press records the answer', /if \(rvViewer\(\) && typeof CNViewer\.acceptChange === "function"\) \{[\s\S]{0,200}await CNViewer\.acceptChange\(\{ rid: String\(r\.order\.receiptId\), lineKeys: \[r\.key\]/.test(acc) && /if \(!ans \|\| !ans\.ok\) return[\s\S]{0,120}changeAsked\.set\(r\.key[\s\S]{0,120}remove\(it\.key\);\s*\n\s*return ans;/.test(acc));
  const rem = src.slice(src.indexOf('  function remove(key, how) {'), src.indexOf('  function problemText(p) {'));
  ok('P11 bridge: Review.remove sends the answer (its words, its line) to the cloud only on a viewer', /if \(viewerNow\) \{[\s\S]{0,200}d\.answer = \(said && said\.say\) \|\| ""; d\.rowKey = [\s\S]{0,80}CNViewer\.settledPut\(d\)/.test(rem) && /const viewerNow = rvViewer\(\)/.test(rem));
  const sync = src.slice(src.indexOf('  function syncOrderItems() {'), src.indexOf('  function syncOrderItems() {') + 6000);
  ok('P12 bridge: on a viewer, syncOrderItems makes one Order changed card per line the cloud says waits on a change, with the cloud\'s words; mode off unchanged', /if \(rvViewer\(\)\) \{[\s\S]{0,1600}items\(\)\.push\(\{ kind: "orderChanged", key, row: r, rows: \[r\], old: null, why/.test(sync) && /const rvViewer = \(\) => !B\.run && !!\(window\.CNViewer/.test(src));
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P13 charm-nest-1.html loads the changed bridge and viewer under new tokens (-s2)', /charm-nest-bridge\.js\?v=[^"]*-s2(?:-\w+)*"/.test(html) && /charm-nest-viewer\.js\?v=[^"]*-s2(?:-\w+)*"/.test(html));
  const entries = fs.readFileSync(path.join(ROOT, 'scripts/netlify-function-entries.json'), 'utf8');
  ok('P14 the new server helper is classified as a helper (next to _charmNestCustomMake.js)', /"_charmNestCustomMake\.js", "_charmNestReviewSettled\.js"/.test(entries));
}

/* ═══ S · T · C: the cloud, offline (one child process: the fakes' clock is its own) ═══ */
async function partCloud() {
  process.env.EDIT_PASSCODE = 'test-pass';
  const T0 = Date.UTC(2026, 10, 21, 16, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  console.log = () => {}; console.warn = () => {};
  const lib = F.load('charmNestLibrary.js'), spy = F.spy(lib), Drive = F.load('_charmNestDrive.js'), Lease = F.load('_charmNestLease.js'), C = F.load('_charmNestOpClass.js');
  const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode, feed: (r.headers || {})['X-CN-Feed'] || '' }); };
  const STEPS = Drive.RUN_STEPS;
  function setDrive(mode, steps) { F.seed('Charm_Nest_Open/drive', { mode, runMode: 'auto', cloudSteps: steps || STEPS.concat(['takeoff']) }); lib._driveForget(); }

  /* S · the store */
  {
    F.reset(); F.clock.set(T0); setDrive('on');
    ok('S0 the op classes: reviewSettledPut is a person\'s press, reviewSettledGet a read', C.classOf('reviewSettledPut') === 'PRESS' && C.classOf('reviewSettledGet') === 'READ');
    const rec = { key: 'chg:4300000001_43000000011', kind: 'orderChanged', why: 'Etsy updated order 4300000001 after the pull · SKU changed', answer: 'Accept the change', by: 'Michelle', t: T0 - 1000, orders: ['4300000001'], lines: 1, pieces: 1, rowKey: '4300000001_43000000011' };
    const a = await P({ op: 'reviewSettledPut', rec, by: 'Michelle', dev: 'd0michelle1', cv: CV });
    ok('S1 mode on: a viewer\'s answer is taken (a person\'s press, never refused as production) and named in the feed (g:review-settled)', a.http === 200 && a.ok && a.wrote === 1 && /g:review-settled/.test(a.feed), a);
    const g = await P({ op: 'reviewSettledGet', cv: CV });
    ok('S2 the list read back: the record whole (key, kind, the answer in words, who, when, its order, its line)', g.http === 200 && g.settled.length === 1 && ['key', 'kind', 'why', 'answer', 'by', 't', 'rowKey'].every(f => g.settled[0][f] === rec[f]) && JSON.stringify(g.settled[0].orders) === JSON.stringify(rec.orders), g.settled);
    const again = await P({ op: 'reviewSettledPut', rec, by: 'Michelle', dev: 'd0michelle2', cv: CV });
    ok('S3 the same answer sent again (a retry, another tab) is kept once', again.ok && again.wrote === 0 && (await P({ op: 'reviewSettledGet' })).settled.length === 1, again);
    const bad = await P({ op: 'reviewSettledPut', rec: { key: 'eng:x', t: T0 }, by: 'Ann', dev: 'd1aaaa', cv: CV });
    ok('S4 an engraving or held notice is not a decision: refused, nothing written', bad.http === 400 && (await P({ op: 'reviewSettledGet' })).settled.length === 1, bad);
    // the cap: the newest 500 of 14 days
    F.seed('Charm_Nest_Review_Settled/list', { j: JSON.stringify([Object.assign({}, rec, { key: 'ord:old', t: T0 - 15 * 86400000 })]), at: T0 - 15 * 86400000, n: 1 });
    for (let i = 0; i < 26; i++) { const recs = []; for (let k = 0; k < 20; k++) { const n = i * 20 + k; recs.push({ key: 'ord:sku:S' + n, kind: 'unmatchedSku', why: 'SKU ' + n, answer: 'Picked', by: 'Ann', t: T0 - 1000 - n * 1000, orders: [String(4300001000 + n)], lines: 1 }); } await P({ op: 'reviewSettledPut', recs, by: 'Ann', dev: 'd1aaaa', cv: CV }); }
    const big = await P({ op: 'reviewSettledGet' });
    ok('S5 kept: the newest 500, nothing older than 14 days, newest first', big.settled.length === 500 && !big.settled.some(r => r.key === 'ord:old') && big.settled[0].key === 'ord:sku:S0' && big.settled.every((r, i, l) => !i || l[i - 1].t >= r.t), { n: big.settled.length, first: big.settled[0] && big.settled[0].key });
    const doc = F.doc('Charm_Nest_Review_Settled/list');
    ok('S6 one document (JSON text: no array in an array), well under Firestore\'s 1 MB', !!doc && typeof doc.j === 'string' && Buffer.byteLength(doc.j) < 400000, doc && Buffer.byteLength(doc.j || ''));
    const src = fs.readFileSync(path.join(ROOT, 'netlify/functions/_charmNestWorld.js'), 'utf8');
    ok('S7 the cloud world reads the list into the Review core in mode "on" only (any other mode: as before, the carrying page\'s own)', /if \(ops\.reviewSettledGet && await driveMode\(db\) === "on"\)[\s\S]{0,200}ops\.reviewSettledGet\(\{\}\)/.test(src) && /ReviewCore\.build\(\{[^}]*cancelled: \[\.\.\.cancelled\], settled, engraving \}\)/.test(src));
  }

  /* T · the take-off lane: an accepted change */
  {
    F.reset(); F.clock.set(T0); setDrive('standby');
    const R = 'run-2026-11-21-s2', X = '4310000001', Y = '4310000002', A = '4310000003';
    // (a piece's id is <order>_<transaction>_<n>, the transaction 5 to 20 digits, as the library's isPoolId reads it)
    const tx = (o, t) => o.slice(-5) + t, lk = (o, t) => `${o}_${tx(o, t)}`, id = (o, t, n) => `${lk(o, t)}_${n}`;
    const run = () => F.doc('Charm_Nest_Runs/' + R) || {};
    const lineOf = k => { const r = run(); if (r.lines && r.lines[k]) return r.lines[k]; for (const p of F.list('Charm_Nest_Run_Live').filter(p => p.runId === R)) { const j = JSON.parse(p.json || '{}'); if (j[k]) return j[k]; } return null; };
    async function pass() {
      // (a press kicked the driver at once: driveSoon dispatched its lease and sent its nonce; else the cron's tick, else a heartbeat dispatch, as cloud-takeoff.cjs)
      const kick = F.net.kicks.filter(k => k.name === 'charmNestDrive-background').pop(); F.net.kicks.length = 0;
      let nonce = kick ? (() => { try { return JSON.parse(kick.body).nonce; } catch (_) { return null; } })() : null;
      const nonces = []; if (!nonce) { await Drive.tick({ db: F.db, kick: n => nonces.push(n) }); nonce = nonces[0]; }
      if (!nonce) { const g = await Lease.dispatch(F.db, F.db.collection('Charm_Nest_Drive').doc('lease'), 'test', { heartbeat: true }); nonce = g && g.nonce; }
      return nonce ? Drive.pass({ db: F.db, Lib: lib, nonce, sleep: async () => {}, renewMs: 0, budgetMs: 3600e3 }) : null;
    }
    F.seed('Charm_Nest_Open/run', { runId: R, at: T0 });
    F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T0, since: T0 } });
    const L = (o, t, ids, extra) => [lk(o, t), Object.assign({ orderId: o, state: 'written', poolIds: ids }, extra || {})];
    F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'processed', step: 'complete', mode: 'auto', day: '2026-11-21', errors: [], orders: [X, Y, A],
      lines: Object.fromEntries([L(X, 11, [id(X, 11, 1)], { changePending: true, repoolChanged: true, changedAt: T0 - 9000, changedWhy: 'SKU changed' }), L(X, 12, [id(X, 12, 1)], { changePending: true, repoolChanged: true, changedAt: T0 - 9000 }),
        L(Y, 21, [id(Y, 21, 1)], { changePending: true, repoolChanged: false, changedAt: T0 - 8000, changedWhy: 'note changed' }), L(A, 31, [id(A, 31, 1)])]) });
    const row = (pid, o) => F.seed('Charm_Pool/' + pid, { poolId: pid, orderId: o, lineKey: pid.replace(/_\d+$/, ''), state: 'written', sheetId: 'gf-1', material: 'gold', runId: R, quantity: 1 });
    for (const [pid, o] of [[id(X, 11, 1), X], [id(X, 12, 1), X], [id(Y, 21, 1), Y], [id(A, 31, 1), A]]) row(pid, o);
    const charm = (cid, pid, order) => ({ id: cid, poolId: pid, order, name: cid, hash: 'h-' + cid }), place = (cid, x, y) => ({ id: cid, cxPt: x, cyPt: y, angle: 0, scale: 1 });
    F.seed('Charm_Nest_Sheets/gf-1', { id: 'gf-1', runId: R, metal: 'gold', fileBase: 'GF_working_gf-1', draft: true, archived: false, layoutRev: 4, contentRev: 2,
      poolIds: [id(X, 11, 1), id(X, 12, 1), id(Y, 21, 1), id(A, 31, 1)], orders: [X, Y, A],
      charms: [charm('c-x1', id(X, 11, 1), X), charm('c-x2', id(X, 12, 1), X), charm('c-y', id(Y, 21, 1), Y), charm('c-a', id(A, 31, 1), A)],
      placements: [place('c-x1', 10, 20), place('c-x2', 50, 20), place('c-y', 90, 20), place('c-a', 130, 20)], outputs: { ai: { path: 'charmnest/x/GF.ai', url: 'u' } } });
    const before = F.doc('Charm_Nest_Sheets/gf-1');
    const a1 = await P({ op: 'takeOffAsk', orderId: X, kind: 'changed', lineKeys: [lk(X, 11)], note: 'Accept the new order', by: 'Michelle', dev: 'd0michelle1', cv: CV });
    ok('T1 a viewer\'s Accept the new order is taken by the cloud (takeOffAsk changed, its line named)', a1.http === 200 && a1.ok && !!a1.askId, a1);
    await pass(); await pass();
    const sh = F.doc('Charm_Nest_Sheets/gf-1'), x11 = lineOf(lk(X, 11)), x12 = lineOf(lk(X, 12)), p11 = F.doc('Charm_Pool/' + id(X, 11, 1));
    ok('T2 the accepted line\'s old piece came off its sheet (superseded, no sheet), the others stay where they were, byte for byte', p11.state === 'superseded' && p11.sheetId === null && !sh.poolIds.includes(id(X, 11, 1)) && sh.poolIds.includes(id(X, 12, 1)) && sh.poolIds.includes(id(A, 31, 1))
      && JSON.stringify(sh.placements) === JSON.stringify(before.placements.filter(p => p.id !== 'c-x1')), { p11, poolIds: sh.poolIds });
    ok('T3 the accepted line is made up again (pulled, no piece, no longer waiting on a change)', x11 && x11.state === 'pulled' && (x11.poolIds || []).length === 0 && !x11.changePending && !x11.repoolChanged, x11);
    ok('T4 the other changed line of the same order, not accepted, still waits with its piece (one card, one line)', x12 && x12.changePending === true && JSON.stringify(x12.poolIds) === JSON.stringify([id(X, 12, 1)]) && F.doc('Charm_Pool/' + id(X, 12, 1)).state === 'written', x12);
    const j1 = Object.values((F.doc('Charm_Nest_TakeOff/' + X) || {}).jobs || {}).find(j => j.kind === 'changed');
    ok('T5 its journal says done', !!j1 && j1.status === 'done', j1);
    // the same press from a second computer (its card was still drawn there): nothing moves a second time
    F.clock.advance(60000);
    const mark = spy.ops.length, rowBefore = JSON.stringify(F.doc('Charm_Pool/' + id(X, 11, 1))), sheetBefore = JSON.stringify(F.doc('Charm_Nest_Sheets/gf-1')), lineBefore = JSON.stringify(lineOf(lk(X, 11)));
    const a2 = await P({ op: 'takeOffAsk', orderId: X, kind: 'changed', lineKeys: [lk(X, 11)], note: 'Accept the new order', by: 'Paul', dev: 'd0paulpc01', cv: CV });
    await pass(); await pass();
    const offAgain = spy.ops.slice(mark).filter(o => o.op === 'cloudTakeOff'), j2 = Object.entries((F.doc('Charm_Nest_TakeOff/' + X) || {}).jobs || {}).filter(([, j]) => j.kind === 'changed');
    ok('T6 the same Accept pressed again on another computer moves nothing again (its own job, done; no take-off; the sheet, the piece and the line as they were)', a2.ok && j2.length === 2 && j2.every(([, j]) => j.status === 'done') && !offAgain.length && JSON.stringify(F.doc('Charm_Pool/' + id(X, 11, 1))) === rowBefore && JSON.stringify(F.doc('Charm_Nest_Sheets/gf-1')) === sheetBefore && JSON.stringify(lineOf(lk(X, 11))) === lineBefore, { ops: offAgain.map(o => o.op), jobs: j2 });
    F.clock.advance(60000);
    // a change that does not touch what the piece is made of (no repoolChanged): the page kept its pieces; so does the cloud
    const a3 = await P({ op: 'takeOffAsk', orderId: Y, kind: 'changed', lineKeys: [lk(Y, 21)], note: 'Accept the new order', by: 'Michelle', dev: 'd0michelle1', cv: CV });
    await pass(); await pass();
    const y21 = lineOf(lk(Y, 21)), py = F.doc('Charm_Pool/' + id(Y, 21, 1));
    ok('T7 a line whose change does not touch its piece keeps it on its sheet, and only stops waiting (as the page\'s acceptChange)', a3.ok && y21 && !y21.changePending && y21.state === 'written' && JSON.stringify(y21.poolIds) === JSON.stringify([id(Y, 21, 1)]) && py.state === 'written' && F.doc('Charm_Nest_Sheets/gf-1').poolIds.includes(id(Y, 21, 1)), { y21, py: py.state });
    ok('T8 no Etsy call, no address outside the named functions', F.etsy.calls === 0 && !F.net.tripwire.length, { etsy: F.etsy.calls, trip: F.net.tripwire });
  }

  /* C · the Review core with the cloud's settled list */
  {
    const Core = require(path.join(ROOT, 'charm-nest-review-core.js'));
    const K = '4320000001_43200000011', now = T0;
    const line = (key, extra) => Object.assign({ key, state: 'pooled', poolIds: [key + '_1'], order: { receiptId: key.split('_')[0], createTs: Math.floor(now / 1000) - 7200, updateTs: Math.floor(now / 1000) - 60, shipBy: Math.floor(now / 1000) + 86400 }, line: { transactionId: key.split('_')[1], listingId: '1800000001', sku: 'LEAF_CHARM', title: 'Leaf' }, spec: { designSku: 'LEAF_CHARM', material: 'gold' }, problems: [] }, extra || {});
    const rv1 = Core.build({ now, lines: [line(K, { changePending: true, changedAt: now - 5000, changedWhy: 'Etsy updated order 4320000001 after the pull · SKU changed' })], settled: [] });
    const card = rv1.cards.find(c => c.key === 'chg:' + K);
    ok('C1 the cloud\'s Review holds the line\'s Order changed card in Open, with the cloud\'s words and time', rv1.openKeys.includes('chg:' + K) && !!card && card.state === 'Open' && /SKU changed/.test(card.why) && card.t === now - 5000, card);
    const rec = { key: 'chg:' + K, kind: 'orderChanged', why: card.why, answer: 'Accept the change', by: 'Michelle', t: now - 1000, orders: ['4320000001'], lines: 1, rowKey: K };
    const rv2 = Core.build({ now, lines: [line(K, { state: 'pulled', poolIds: [] })], settled: [rec] });
    ok('C2 once accepted, the cloud\'s Resolved lists the answer under the key every page draws (settled:<key>:<t>), Open no longer holds the card', rv2.resolvedKeys.includes(`settled:chg:${K}:${now - 1000}`) && !rv2.openKeys.includes('chg:' + K) && rv2.counts.resolved >= 1, { open: rv2.openKeys, resolved: rv2.resolvedKeys });
  }
  ok('the owner\'s rules: no Etsy call, the Etsy switch never written', F.etsy.calls === 0 && !F.stopWrites.length);
}

/* ═══ E · the REAL page, booted as a viewer ═══ */
async function partE() {
  let Base; try { Base = require('./_orderFactsWorld.cjs'); } catch (e) { ok('E world', false, String(e)); return; }
  const Core = require(path.join(ROOT, 'charm-nest-review-core.js'));
  const w = await Base.world({ viewport: { width: 1440, height: 900 } });
  if (!w) { ok('E: playwright-core was found (the real page runs)', false); return; }
  const page = w.page, st = w.srv.st, lib = st.handlers.charmNestLibrary, R = 'run-2026-11-21-s2page';
  const until = async (fn, ms) => { const t0 = Date.now(); for (;;) { let v = null; try { v = await fn(); } catch (_) { v = null; } if (v) return v; if (Date.now() - t0 > ms) return null; await new Promise(r => setTimeout(r, 250)); } };
  try {
    await until(() => page.evaluate(() => !!(window.CNViewer && CNViewer.known() && Session.ready())), 30000);
    const off = await page.evaluate(() => ({ viewer: !!(window.CNViewer && CNViewer.on()), frozen: Session.frozen() }));
    ok('E0 mode off: the page boots as before (no viewer)', off.viewer === false && off.frozen === false, off);
    const NAMES = ['single', 'late'];
    await Base.seed(page, NAMES);
    const made = await page.evaluate(() => Orders.rows().filter(r => r && !r.parentRow).map(r => ({ key: r.key, order: r.order, line: r.line, spec: r.spec, problems: r.problems || [], rec: CharmNestPoolMake.lineRecord(r, B)[1] })));
    const ridOf = n => Base.FIXTURES[n].rid, mOf = n => made.find(m => String(m.order.receiptId) === ridOf(n));
    const CH = mOf('single'), K = CH && CH.key, RID = ridOf('single'), WHY = `Etsy updated order ${RID} after the pull · SKU changed`;
    if (!CH) throw new Error('the fixture order was not made on the page');
    const T = Date.now();
    let at = T - 60000;
    const runOf = (m, extra) => Object.assign({ poolIds: [m.key + '_1'] }, extra || {});
    const putWorld = (m, state, run) => { at += 1; const doc = { v: 1, key: m.key, receiptId: String(m.order.receiptId), order: m.order, line: m.line, spec: m.spec, problems: m.problems, state, reason: null, run, etsyOpen: true, inRun: true, fromRecord: false, runId: R };
      st.put('Charm_World_Lines', m.key, { v: 1, key: m.key, receiptId: doc.receiptId, state, runId: R, sig: 'sig-' + m.key + '-' + at, at, gone: false, data: JSON.stringify(doc) }, false); };
    const coreLine = (m, state, run) => ({ key: m.key, state, poolIds: run.poolIds || [], changePending: !!run.changePending, changedAt: run.changedAt || 0, changedWhy: run.changedWhy || '', arrivedAt: T - 3600e3,
      order: { receiptId: String(m.order.receiptId), createTs: m.order.createTs, updateTs: m.order.updateTs, shipBy: m.order.shipBy }, line: { transactionId: String(m.line.transactionId), listingId: String(m.line.listingId || ''), sku: m.line.sku, title: m.line.title },
      spec: m.spec ? { designSku: m.spec.designSku || null, special: null, material: m.spec.material || null } : null, problems: m.problems || [] });
    const putReview = (lines, settled) => { const rv = Core.build({ now: Date.now(), lines, settled }); st.put('Charm_World', 'review', { v: 1, at: rv.at, counts: rv.counts, data: JSON.stringify({ cards: rv.cards, openOrders: rv.openOrders, openKeys: rv.openKeys, resolvedKeys: rv.resolvedKeys, itemKeys: rv.itemKeys }) }, false); return rv; };
    // the cloud as it stands: mode on, the cloud carries the run; the order's line changed after the pull (the take-off lane's revalidate flagged it), the other is as it was
    const chRun = runOf(CH, { changePending: true, repoolChanged: true, changedAt: T - 30000, changedWhy: WHY }), lateM = mOf('late'), lateRun = runOf(lateM);
    putWorld(CH, 'pooled', chRun); putWorld(lateM, 'pooled', lateRun);
    const rv0 = putReview([coreLine(CH, 'pooled', chRun), coreLine(lateM, 'pooled', lateRun)], []);
    const runLines = { [CH.key]: Object.assign({}, CH.rec, { state: 'pooled' }, chRun), [lateM.key]: Object.assign({}, lateM.rec, { state: 'pooled' }, lateRun) };
    st.put('Charm_World', 'summary', { at, live: 2, counts: { today: 2 } }, false);
    st.put('Charm_Nest_Open', 'drive', { mode: 'on', runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: T - 3600e3 });
    st.put('Charm_Nest_Runs', R, { runId: R, status: 'running', step: 'pool', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [RID, ridOf('late')], lines: runLines, createdAt: T - 7200e3, updatedAt: T });
    st.put('Charm_Nest_Run_Carrier', R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: T + 3600e3, since: T - 3600e3 } });
    for (const h of Object.values(st.handlers || {})) try { if (h && typeof h._driveForget === 'function') h._driveForget(); } catch (_) {}
    await page.reload({ waitUntil: 'load' });
    const booted = await until(() => page.evaluate(() => !!(window.CNViewer && CNViewer.known() && CNViewer.on() && Session.frozen() && window.CloudWorld && CloudWorld.state().loadedAt)), 60000);
    ok('E1 the page booted as a viewer (mode on, frozen, no run held, the cloud world read)', !!booted, await page.evaluate(() => ({ on: !!(window.CNViewer && CNViewer.on()), run: !!B.run })));
    // 2 · the cloud's Order changed card is drawn in Open with its Accept button
    const card = await until(() => page.evaluate(k => { const it = Review.items().find(x => x.key === 'chg:' + k); return it ? { kind: it.kind, why: it.why } : null; }, K), 30000);
    ok('E2 the viewer\'s Review holds the cloud\'s Order changed card, with the cloud\'s words', !!card && card.kind === 'orderChanged' && card.why === WHY, { card, rv0: rv0.openKeys });
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    const btn = await until(() => page.evaluate(() => { const b = document.querySelector('[data-rv-accept]'); return b && b.offsetParent !== null ? true : null; }), 20000);
    ok('E3 ... in Open, with its "Accept the new order" button', !!btn, await page.evaluate(() => (document.querySelector('#reviewView') || document.body).innerText.slice(0, 400)));
    // 3 · the person presses Accept
    if (btn) await page.click('[data-rv-accept]');
    const asked = await until(() => { const d = st.doc('Charm_Nest_TakeOff', RID); const a = d && d.asks && Object.values(d.asks).find(x => x && x.kind === 'changed'); return a || null; }, 20000);
    ok('E4 Accept sent ONE press the cloud acts on: takeOffAsk changed, the line named, under the person\'s name (nothing repooled on the page)', !!asked && JSON.stringify(asked.lineKeys) === JSON.stringify([K]) && asked.by === 'Test Operator' && asked.state === 'waiting', asked);
    const kept = await until(() => { const d = st.doc('Charm_Nest_Review_Settled', 'list'); let l = []; try { l = JSON.parse((d && d.j) || '[]'); } catch (_) {} return l.find(r => r.key === 'chg:' + K) || null; }, 20000);
    ok('E5 the answer is kept in the cloud: the decision, its words ("Accept the change"), who, when, its order and its line', !!kept && kept.answer === 'Accept the change' && kept.by === 'Test Operator' && kept.kind === 'orderChanged' && JSON.stringify(kept.orders) === JSON.stringify([RID]) && kept.rowKey === K && kept.t > T, kept);
    const here = await page.evaluate(k => { Review.syncOrderItems(); const d = Review.settled()[0]; return { card: Review.items().some(x => x.key === 'chg:' + k), top: d && { key: d.key, t: d.t, answer: d.answer }, pool: B.orders.byKey.get(k) && B.orders.byKey.get(k).poolIds }; }, K);
    ok('E6 on the page: the card left Open and is not drawn again while the cloud makes the line up; Resolved has the answer on top', !here.card && here.top && here.top.key === 'chg:' + K && here.top.answer === 'Accept the change', here);
    if (kept) {
      const mk = `settled:chg:${K}:${kept.t}`;
      const shown = await until(() => page.evaluate(mk => { Review.showCard(mk, 'done'); const n = [...document.querySelectorAll('[data-mkey]')].find(x => x.dataset.mkey === mk); return n ? n.innerText : null; }, mk), 15000);
      ok('E7 Resolved shows it at once (before the cloud\'s next build): "Accept the change", who', !!shown && /Accept the change/.test(shown) && /Test Operator/.test(shown), shown);
      // 4 · the cloud did it: the line made up again (no longer waiting), the world built again with the cloud's settled list. The computer reloads: it keeps nothing of its own
      const doneRun = runOf(CH, { poolIds: [] });
      putWorld(CH, 'pulled', doneRun);
      runLines[CH.key] = Object.assign({}, CH.rec, { state: 'pulled', poolIds: [] });
      st.put('Charm_Nest_Runs', R, { lines: runLines, updatedAt: Date.now() });
      const cloudList = JSON.parse(st.doc('Charm_Nest_Review_Settled', 'list').j);
      const rv1 = putReview([coreLine(CH, 'pulled', doneRun), coreLine(lateM, 'pooled', lateRun)], cloudList);
      st.put('Charm_World', 'summary', { at, live: 2, counts: { today: 2 } }, false);
      await lib.worldBump();
      await page.reload({ waitUntil: 'load' });
      const back = await until(() => page.evaluate(mk => { if (!(window.CNViewer && CNViewer.on() && CNViewer.settledState().reads > 0)) return null; const d = Review.settled().find(x => 'settled:' + x.key + ':' + x.t === mk); return d ? { answer: d.answer, by: d.by, cloud: d.cloud, row: !!d.row } : null; }, mk), 60000);
      ok('E8 after a reload (nothing kept on this computer) Resolved is read from the cloud: the same decision, its words, who', !!back && back.answer === 'Accept the change' && back.by === 'Test Operator' && back.cloud === true && rv1.resolvedKeys.includes(mk), { back, resolved: rv1.resolvedKeys });
      const shown2 = await until(() => page.evaluate(mk => { CN.setMode('review'); Review.showCard(mk, 'done'); const n = [...document.querySelectorAll('[data-mkey]')].find(x => x.dataset.mkey === mk); return n ? n.innerText : null; }, mk), 20000);
      ok('E9 ... and drawn under Resolved with "Accept the change" (the cloud\'s Review lists it too: every computer the same)', !!shown2 && /Accept the change/.test(shown2), shown2);
      const gone = await page.evaluate(k => { Review.syncOrderItems(); return Review.items().some(x => x.key === 'chg:' + k); }, K);
      ok('E10 the line no longer waits on a change in the cloud: no Order changed card is drawn again', gone === false);
    }
  } catch (e) { ok('E run', false, String(e && e.stack || e).split('\n').slice(0, 3).join(' | ')); }
  ok('E no page error', w.errors.length === 0, w.errors.slice(0, 2).join(' | '));
  await w.close();
}

(async () => {
  if (process.env.S2_PART === 'cloud') { await partCloud(); }
  else {
    await partP();
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { S2_PART: 'cloud' }), encoding: 'utf8', timeout: 300000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) |^ERROR|^  /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0('FAIL cloud part: ' + out.slice(-1500)); }
    if (!process.env.SKIP_PAGE) await partE();
  }
  if (process.env.S2_PART) { log0(failed ? `FAIL part (${failed})` : 'PASS part'); process.exit(failed ? 1 : 0); }
  log0(failed ? `FAIL viewer-s2 (${failed})` : 'PASS viewer-s2');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-s2: ' + (e && e.stack || e)); process.exit(1); });
