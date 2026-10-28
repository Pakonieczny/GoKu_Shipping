// The design log and the Custom pieces view (Design & Engrave items 34 to 37 and D7; Paul, 28 Oct 2026: "all of this must be tracked by employee, and I
// need to know when employees are commissioning custom pieces, especially when they don't belong to an order that way we can correctly track our rates
// and who creates the errors and who is creating the solutions"). Fakes only: nothing leaves the loopback, no real name, PIN, passcode or order.
//   1 · CNDesignLog in node (a stand-in page): a burst is handed over ONCE (its notes, its timeline steps, one designLog request), never later than
//       maxMs after its first event; pagehide hands over at once and sends with keepalive; a refused send is kept on the disk and sent again; the
//       timeline steps (a save, a custom piece, the order a fix fixes); the efficiency phrases and their counters; the sandbox's own outbox; who made
//       the mistake (causeOf); a PIN is never a name; the cost of one call
//   2 · the server, the REAL doors over the fixture Firestore of bridge-server.cjs: op designLog (stored once, both kinds of one send one record, a
//       PIN refused, the sandbox's own collection), the activity door's rollup counters, op custom (pieces, people, rates, days; its reads, cached),
//       the overview's person totals.design, the sandbox registry
//   3 · one browser check (through slot.sh): the real sorter page logs a burst through CNDesignLog (real CNAct, station-activity.js, order timeline,
//       designLog), and the Efficiency console's Custom pieces tab shows it: figures, the flagged no-order pieces, the fix with who made the mistake,
//       the per-person rate, the filter, the 7-day range; the People row's design line
//   node tests/charm-nest/design-log.cjs        (the browser part runs when PW_DIR / CHROMIUM are set: slot.sh sets them)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const wait = ms => new Promise(r => setTimeout(r, ms));
const SRC = fs.readFileSync(path.join(root, 'charm-nest-design-log.js'), 'utf8');

/* ── a stand-in page for charm-nest-design-log.js ── */
function page(o = {}) {
  const disk = o.disk || new Map(), calls = { acts: [], tl: [], posts: [], fetches: [], saFlush: 0, tlFlush: 0 }, listeners = {};
  const win = { console, setTimeout, clearTimeout, Date, JSON, Math, Promise, Intl,
    localStorage: { getItem: k => (disk.has(k) ? disk.get(k) : null), setItem: (k, v) => disk.set(k, String(v)), removeItem: k => disk.delete(k) },
    location: { search: '', protocol: 'http:' }, addEventListener: (t, fn) => (listeners[t] = listeners[t] || []).push(fn),
    document: { addEventListener: (t, fn) => (listeners['doc:' + t] = listeners['doc:' + t] || []).push(fn), visibilityState: 'visible' },
    CNEmployee: { name: () => o.name == null ? 'Ivy' : o.name },
    StationSession: { who: () => ({ person: 'Ivy', role: o.role || 'design', station: o.role || 'design', device: 'charm-nest-1' }) },
    CNAct: (a, x) => { calls.acts.push([a, x, Date.now()]); return true; },
    StationActivity: { flush: f => { calls.saFlush++; return Promise.resolve(f); } },
    OrderTimeline: { record: e => { calls.tl.push(e); return e; }, flush: () => { calls.tlFlush++; }, get: o.timeline || (async () => ({ events: [] })) },
    CN: { api: async (fn, b) => { calls.posts.push({ fn, b, at: Date.now() }); if (o.fail && o.fail()) throw new Error('offline'); return { ok: true, written: b.records.length }; }, S: { settings: { sandbox: o.sandbox ? 'on' : 'off' }, passcode: 'fake-pass' } },
    fetch: async (url, init) => { calls.fetches.push({ url, init }); return { ok: true, status: 200, json: async () => ({ ok: true }) }; } };
  win.window = win; vm.createContext(win); vm.runInContext(SRC, win);
  const L = win.CNDesignLog; Object.assign(L.options, { quietMs: 40, maxMs: 150, retryMs: 30, maxRetryMs: 200 }, o.options || {});
  const fire = t => { for (const fn of listeners[t] || []) fn({}); };
  return { L, calls, disk, fire, win };
}
const K = require(path.join(root, 'netlify/functions/_activityKinds.js'));

(async () => {
  /* ── 1 · CNDesignLog ── */
  {
    // a burst: six events within a few ms are handed over once, after the quiet time
    const P = page(), L = P.L, t0 = Date.now();
    L.event('editorOpen', { orderId: '3521000999', sku: 'HRT-1' });
    L.event('editorSave', { orderId: '3521000999', sku: 'HRT-1', faces: ['front'] });
    L.event('designSave', { orderId: '3521000999', designId: 'des-heart', version: 3, name: 'Heart' });
    L.event('customPieceSent', { orderId: '3521000999', designId: 'des-heart', version: 3, name: 'Heart', metal: 'GF', quantity: 2, sendId: 'send-1' });
    L.event('customPieceSent', { orderId: 'CUSTOM-20261028-3', designId: 'des-star', version: 1, name: 'Star', metal: 'rose', customName: 'Spare star', reason: 'a forgotten piece', sendId: 'send-2' });
    L.event('fixPiece', { customId: 'CUSTOM-20261028-4', designId: 'des-moon', name: 'Moon', metal: 'SS', customName: 'Redo moon', reason: 'misspelt name', sendId: 'send-3', fixes: { orderId: '3521000888', causedBy: 'Ana M', suggested: 'Ivy' } });
    await wait(15);
    assert.equal(P.calls.acts.length, 0, 'nothing is handed over while the burst goes on');
    assert.equal(L.pending().events, 6);
    await wait(120);
    const at = P.calls.acts.map(a => a[2]);
    assert.equal(P.calls.acts.length, 6, 'one efficiency note per event');
    assert(Math.max(...at) - Math.min(...at) < 5, 'handed over at once');
    assert.equal(P.calls.posts.length, 1, 'ONE designLog request for the burst');
    const recs = P.calls.posts[0].b.records;
    assert.deepEqual(JSON.parse(JSON.stringify(recs.map(r => [r.id, r.kind, r.orderId || '', r.customId || '', r.metal]))), [['dl-send-1', 'customPieceSent', '3521000999', '', 'gold'], ['dl-send-2', 'customPieceSent', '', 'CUSTOM-20261028-3', 'rose'], ['dl-send-3', 'fixPiece', '', 'CUSTOM-20261028-4', 'silver']]);
    assert.deepEqual(JSON.parse(JSON.stringify(recs[2].fix)), { orderId: '3521000888', causedBy: 'Ana M', suggested: 'Ivy' });
    assert.equal(P.calls.posts[0].b.op, 'designLog'); assert.equal(P.calls.posts[0].b.sandbox, false);
    // the efficiency notes: fixed phrases, counted by the rollup's own classifier
    const det = P.calls.acts.map(a => a[1].detail);
    assert.deepEqual(det, ['design: editor opened', 'design: editor saved', 'design: design saved', 'design: custom piece sent · gold', 'design: custom piece sent · rose · no order', 'design: fix piece sent · silver · no order']);
    const X = {}; for (const d of det) for (const [k, v] of Object.entries(K.classify({ station: 'design', action: 'note', detail: d }).x)) X[k] = (X[k] || 0) + v;
    assert.deepEqual(X, { x_dopen: 1, x_dsave: 2, x_cust: 3, x_custNo: 2, x_fix: 1 });
    for (const d of det) assert.equal(K.classify({ station: 'design', action: 'note', detail: d }).kind, '', 'a design step is never an issue kind');
    assert.equal(P.calls.acts[0][1].orderId, '3521000999'); assert(!('orderId' in P.calls.acts[4][1]), 'a piece with no order touches no order');
    // the timeline: one "Design added" for the save (the library's version wins over the editor's own), the custom piece on its order, the fix on the order it fixes
    const tl = P.calls.tl.map(e => [e.orderId, e.type, e.by, e.station]);
    assert.deepEqual(tl, [['3521000999', 'designDropped', 'Ivy', 'design'], ['3521000999', 'designSent', 'Ivy', 'design'], ['3521000888', 'designSent', 'Ivy', 'design']]);
    assert.match(P.calls.tl[0].text, /^Design saved in the Charm Editor: Heart v3$/);
    assert.match(P.calls.tl[1].text, /^Custom piece sent to the GF sheet by Ivy: Heart v3 × 2$/);
    assert.match(P.calls.tl[2].text, /^Fix piece sent to the SS sheet by Ivy \(CUSTOM-20261028-4 “Redo moon”\) · mistake by Ana M$/);
    assert.equal(P.calls.tl[2].data.causedBy, 'Ana M'); assert.equal(P.calls.tl[2].data.fixedBy, 'Ivy');
    await wait(60);
    assert.deepEqual(JSON.parse(JSON.stringify(L.pending())), { events: 0, records: 0 }, 'acknowledged records leave the outbox');
    console.log(`  ✓ a burst of 6 events: handed over once ${Math.round(Math.min(...at) - t0)} ms after the first, 6 notes, 3 timeline steps, 1 designLog request with 3 records`);
  }
  {
    // a long burst never waits more than maxMs; pagehide hands over at once and sends with keepalive
    const P = page({ options: { quietMs: 60, maxMs: 150 } }), L = P.L, t0 = Date.now();
    for (let i = 0; i < 10; i++) { L.event('imageTrace', { file: `photo-${i}.png` }); await wait(30); }
    const first = P.calls.acts[0][2] - t0;
    assert(first >= 140 && first < 230, `the first hand-over of a running burst comes by maxMs (${first} ms)`);
    const handOvers = new Set(P.calls.acts.map(a => Math.round(a[2] / 10))).size;
    assert(handOvers >= 2, 'and the burst goes on in more hand-overs');
    L.event('customPieceSent', { orderId: '3521000777', metal: 'gold', sendId: 'send-9' });
    P.fire('pagehide');
    assert.equal(P.calls.acts[P.calls.acts.length - 1][1].detail, 'design: custom piece sent · gold', 'pagehide: handed over at once');
    assert(P.calls.saFlush >= 1 && P.calls.tlFlush >= 1, 'and both doors are asked to send now');
    await wait(5);
    assert.equal(P.calls.fetches.length, 1); assert.equal(P.calls.fetches[0].init.keepalive, true, 'the records go with keepalive');
    assert.equal(P.calls.fetches[0].init.headers['X-Edit-Passcode'], 'fake-pass');
    console.log(`  ✓ a running burst is handed over by maxMs (first after ${first} ms, ${handOvers} hand-overs in 300 ms); pagehide sends at once with keepalive`);
  }
  {
    // a refused send stays on the disk and goes again; another page load sends what an earlier one left
    let down = true; const disk = new Map();
    const P = page({ disk, fail: () => down }), L = P.L;
    L.event('fixPiece', { sendId: 'send-4', fixes: { orderId: '3521000888', causedBy: 'Bea' } });
    await L.flush(); await wait(10);
    assert.equal(JSON.parse(disk.get('cn.designLog.outbox')).length, 1, 'kept on the disk while the cloud says no');
    const tries = P.calls.posts.length; down = false; await wait(260);
    assert(P.calls.posts.length > tries, 'sent again'); assert(!disk.has('cn.designLog.outbox'), 'and gone from the disk once acknowledged');
    down = true; L.event('customPieceSent', { sendId: 'send-5' }); await L.flush(); await wait(5);
    const P2 = page({ disk }); await P2.L.flush(); await wait(5);
    assert.deepEqual(JSON.parse(JSON.stringify(P2.calls.posts.map(x => x.b.records.map(r => r.id)))), [['dl-send-5']], 'a later page sends what the earlier one left');
    // the sandbox: its own outbox and its own store
    const S = page({ sandbox: true }); S.L.event('customPieceSent', { sendId: 'sb-1' }); S.L.options.retryMs = 1e6; S.win.CN.api = async () => { throw new Error('later'); };
    await S.L.flush(); await wait(5);
    assert(S.disk.has('cn.designLog.outbox.sandbox') && !S.disk.has('cn.designLog.outbox'), 'the sandbox keeps its records under the sandbox\'s mark');
    console.log('  ✓ refused: kept on the disk and sent again; an earlier page\'s records go with the next; the sandbox has its own outbox');
  }
  {
    // who made the mistake (D7): from the order's own timeline
    const evs = [{ type: 'decided', by: 'Bea', at: 1 }, { type: 'engraveApproved', by: 'Ivy', at: 5 }, { type: 'placed', by: 'System', at: 6 }, { type: 'designSent', by: 'Zed', at: 7, data: { fix: true } }, { type: 'engraveApproved', by: 'Ana M', at: 3, lineKey: 'L2' }];
    const P = page({ timeline: async id => ({ events: id === '3521000888' ? evs : [] }) });
    const c = JSON.parse(JSON.stringify(await P.L.causeOf('3521000888')));
    assert.equal(c.suggested, 'Ivy'); assert.equal(c.why, 'Ivy approved the engraving');
    assert.deepEqual(c.candidates.map(x => x.name), ['Ivy', 'Ana M', 'Bea'], 'the engraving first, newest first; nobody for the System; never an earlier fix');
    assert.equal(JSON.parse(JSON.stringify(await P.L.causeOf('3521000888', { lineKey: 'L2' }))).suggested, 'Ana M', 'the line\'s own step');
    // a PIN is never a name; a CUSTOM id is no order
    const n = P.L.event('customPieceSent', { by: '482915', orderId: 'CUSTOM-20261028-9' });
    assert.equal(n.by, 'Ivy'); assert.equal(n.orderId, undefined); assert.equal(n.customId, 'CUSTOM-20261028-9');
    assert.equal(P.L.event('Not a kind!', {}), null); assert.equal(P.L.event('', {}), null);
    assert.equal(P.L._t.phraseOf(P.L.event('maskApplied', {})), 'design: mask applied', 'a workstream\'s own kind is a design note of its own words');
    // the cost of one call (the press that makes it paints in the same frame)
    const Q = page({ options: { quietMs: 1e6, maxMs: 1e6 } }); const t = process.hrtime.bigint();
    for (let i = 0; i < 2000; i++) Q.L.event('editorSave', { orderId: '3521000999', sku: 'HRT-1', faces: ['front', 'back'] });
    const per = Number(process.hrtime.bigint() - t) / 1e6 / 2000;
    assert(per < 0.25, `one event() costs ${per.toFixed(4)} ms`);
    console.log(`  ✓ causeOf suggests Ivy (approved the engraving), the line's own person for a line; a PIN is no name; one event() costs ${per.toFixed(4)} ms of main thread`);
  }

  /* ── 2 · the server: the real doors over the fixture Firestore ── */
  // op designLog runs in the REAL charmNestLibrary of bridge-server.cjs (its in-memory Firestore, which also serves the page below). The efficiency
  // documents (activity, the day's rollups, Design_Log) are kept on _sandboxFakes.cjs's in-memory Firestore, which merges nested maps and increments
  // as Firestore does (a day's rollup is made of them); the activity door, op designLog and employeeEfficiency are called on it in-process.
  const SF = require('./_sandboxFakes.cjs');
  const { start, signInAs } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st } = srv, fnDir = path.join(root, 'netlify/functions');
  const PASS = 'synthetic-pass-not-real';
  const call = async (name, body, q = '') => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/${name}${q}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, j: await r.json() }; };
  const DL = require(path.join(fnDir, '_designLog.js')), Activity = require(path.join(fnDir, '_stationActivity.js')), EE = require(path.join(fnDir, 'employeeEfficiency.js'));
  const today = DL.nyDay(Date.now()), FV = SF.admin.firestore.FieldValue;
  const queries = {};      // collection -> queries run (what the console's answers cost)
  const counted = (c, q) => new Proxy(q, { get(t, k) { const v = t[k]; if (k === 'get') return (...a) => { queries[c] = (queries[c] || 0) + 1; return v.apply(t, a); }; return ['where', 'orderBy', 'limit', 'select', 'startAfter'].includes(k) ? (...a) => counted(c, v.apply(t, a)) : v; } });
  const fdb = Object.assign(Object.create(SF.db), { collection: c => counted(c, SF.db.collection(c)) });
  SF.store.set('config/editPasscode', { passcode: PASS }); delete process.env.EDIT_PASSCODE;
  const eff = async body => { const r = await EE._t.handle({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }, fdb); return { status: r.statusCode, j: JSON.parse(r.body) }; };
  const fdoc = (c, id) => SF.store.get(c + '/' + id);
  try {
    {
      const now = Date.now();
      const recs = [
        { id: 'dl-s1', kind: 'customPieceSent', by: 'Ivy', station: 'design', clientAt: now - 60000, orderId: '3521000999', designId: 'des-heart', version: 3, name: 'Heart', metal: 'gold', quantity: 2, poolIds: ['3521000999_1_1'] },
        { id: 'dl-s2', kind: 'customPieceSent', by: 'Ivy', station: 'design', clientAt: now - 50000, customId: 'CUSTOM-20261028-3', customName: 'Spare star', reason: 'a forgotten piece', metal: 'rose' },
        { id: 'dl-s3', kind: 'customPieceSent', by: 'Bea', station: 'sorter', clientAt: now - 40000, customId: 'CUSTOM-20261028-4', customName: 'Redo moon', metal: 'silver' },
        { id: 'dl-pin', kind: 'customPieceSent', by: '123456', clientAt: now }];
      const fix = { id: 'dl-s3', kind: 'fixPiece', by: 'Bea', clientAt: now - 39000, customId: 'CUSTOM-20261028-4', fix: { orderId: '3521000888', causedBy: 'Ana M', suggested: 'Ivy', poolId: '3521000888_1_1' } };
      let r = await call('charmNestLibrary', { op: 'designLog', records: recs });
      assert.equal(r.status, 200, JSON.stringify(r.j)); assert.deepEqual([r.j.written, r.j.refused], [3, 1], 'three stored, the PIN-named one refused');
      r = await call('charmNestLibrary', { op: 'designLog', records: recs.slice(0, 3) });
      assert.equal(st.list('Design_Log').length, 3, 'sent again: stored once');
      // the fix of send 3 arrives as its own event: the same record, now a fix with who made the mistake
      r = await call('charmNestLibrary', { op: 'designLog', records: [fix] });
      const s3 = st.doc('Design_Log', 'dl-s3');
      assert.equal(s3.kind, 'fixPiece'); assert.equal(s3.customName, 'Redo moon', 'the send\'s own fields stay');
      assert.deepEqual([s3.fix.orderId, s3.fix.causedBy, s3.fix.suggested], ['3521000888', 'Ana_M', 'Ivy'], 'stored under her one login name (Paul, 9 Oct 2026)');
      assert.equal(s3.orderless, true); assert.equal(s3.day, today); assert(s3.at <= Date.now() && s3.serverAt >= s3.at);
      assert.equal(st.doc('Design_Log', 'dl-s1').orderless, false);
      // the sandbox writes its own collection, and a record never changes store
      r = await call('charmNestLibrary', { op: 'designLog', sandbox: true, records: [{ id: 'dl-sb1', by: 'Ivy', sandbox: true }, { id: 'dl-sb2', by: 'Ivy', sandbox: false }] });
      assert.deepEqual([r.j.written, r.j.refused], [1, 1]); assert(st.doc('Sandbox_Design_Log', 'dl-sb1') && !st.doc('Design_Log', 'dl-sb1'));
      const F = require(path.join(root, 'charm-nest-sandbox-families.js'));
      assert.equal(F.ownerOf('firestore', 'Sandbox_Design_Log/dl-sb1').kind, 'protected', 'employee efficiency data: kept by a sandbox wipe');
      // the same records on the efficiency store (one write each, merged as Firestore merges)
      await DL.add(fdb, FV, recs, {}); await DL.add(fdb, FV, [fix], {});
      assert.deepEqual([fdoc('Design_Log', 'dl-s3').kind, fdoc('Design_Log', 'dl-s3').customName, fdoc('Design_Log', 'dl-s3').fix.causedBy], ['fixPiece', 'Redo moon', 'Ana_M']);
      console.log('  ✓ op designLog: 3 records stored once (a retry changes nothing), a PIN refused, the fix merged into its send, the sandbox in Sandbox_Design_Log');
    }
    {
      // the stations' activity door: design notes add the design counters to the person's day
      const ev = (id, person, detail, orderId = '') => ({ id: 'dl-test-event-' + id, station: 'design', device: 'charm-nest-1', person, action: 'note', orderId, detail, at: Date.now() - 30000, seq: 1, sincePrevMs: 20000, role: 'design' });
      const r = await Activity.add(fdb, FV, [ev('a', 'Ivy', 'design: editor opened', '3521000999'), ev('b', 'Ivy', 'design: design saved', '3521000999'), ev('c', 'Ivy', 'design: custom piece sent · gold', '3521000999'), ev('d', 'Ivy', 'design: custom piece sent · rose · no order'), ev('e', 'Bea', 'design: fix piece sent · silver · no order')], {});
      assert.equal(r.written, 5, JSON.stringify(r));
      const x = fdoc('Efficiency_Daily', `${today}__Ivy`).stations.design;
      assert.deepEqual([x.x_dopen, x.x_dsave, x.x_cust, x.x_custNo, x.notes], [1, 1, 2, 1, 4]);
      assert.equal(fdoc('Efficiency_Daily', `${today}__Bea`).stations.design.x_fix, 1);
      // Ana M worked four orders today (assembly): the mistake put down to her reads as a rate
      SF.store.set(`Efficiency_Daily/${today}__Ana_M`, { day: today, person: 'Ana_M', v: 1, events: 8, firstAt: Date.now() - 3600e3, lastAt: Date.now() - 60e3, stations: { assembly: { scans: 4, completes: 4, parts: 6, orders: 4, activeMs: 900000 } }, touched: { '3521000101': { assembly: true }, '3521000102': { assembly: true }, '3521000103': { assembly: true }, '3521000104': { assembly: true } } });
      console.log(`  ✓ the activity door counts design steps in the day: Ivy opened 1, saved 1, sent 2 (1 with no order); Bea 1 fix`);
    }
    {
      for (const k of Object.keys(queries)) delete queries[k];
      const { status, j } = await eff({ op: 'custom', key: PASS });
      assert.equal(status, 200, JSON.stringify(j));
      const first = Object.assign({}, queries);
      assert.deepEqual([j.from, j.to, j.days], [today, today, 1]);
      assert.deepEqual(j.totals, { sent: 3, pieces: 4, orderless: 2, fixes: 1, caused: 1, people: 2 });
      assert.deepEqual(j.pieces.map(p => [p.id, p.by, p.orderless]), [['dl-s3', 'Bea', true], ['dl-s2', 'Ivy', true], ['dl-s1', 'Ivy', false]], 'newest first');
      const fx = j.pieces[0].fix; assert.deepEqual([fx.orderId, fx.causedBy, fx.suggested, fx.changed], ['3521000888', 'Ana M.', 'Ivy', true], 'the person changed who the history suggested');
      const P = Object.fromEntries(j.people.map(p => [p.name, p]));
      assert.deepEqual([P.Ivy.sent, P.Ivy.pieces, P.Ivy.orderless, P.Ivy.saved, P.Ivy.opened, P.Ivy.orders], [2, 3, 1, 1, 1, 1]);
      assert.deepEqual([P.Bea.sent, P.Bea.fixes, P.Bea.changed], [1, 1, 1]);
      assert.deepEqual([P['Ana M.'].caused, P['Ana M.'].orders, P['Ana M.'].errorRate, P['Ana M.'].sent], [1, 4, 25, 0], 'one mistake in four orders worked: 25%');
      assert.equal(j.byDay.length, 1); assert.deepEqual(j.byDay[0], { day: today, sent: 3, pieces: 4, orderless: 2, fixes: 1, caused: 1 });
      assert.equal(first.Design_Log, 1, 'one range query of Design_Log'); assert.equal(first.Efficiency_Daily, 1, 'one of the rollups');
      // asked again within seconds: both come from the instance's cache
      for (const k of Object.keys(queries)) delete queries[k];
      await eff({ op: 'custom', key: PASS });
      assert.equal((queries.Design_Log || 0) + (queries.Efficiency_Daily || 0), 0, 'asked again: no query of either');
      const w = await eff({ op: 'custom', key: PASS, days: 7 });
      assert.equal(w.j.byDay.length, 7); assert.equal(w.j.totals.sent, 3);
      assert.equal((await eff({ op: 'custom', key: 'wrong' })).status, 401, 'the gate');
      // the overview folds the design work into the person's totals
      const ov = await eff({ op: 'overview', key: PASS, roster: true });
      assert.equal(ov.status, 200, JSON.stringify(ov.j).slice(0, 300));
      const iv = ov.j.people.find(p => p.name === 'Ivy');
      assert.deepEqual(iv.totals.design, { opened: 1, saved: 1, imported: 0, custom: 2, orderless: 1, fixes: 0 });
      assert(!ov.j.people.find(p => p.name === 'Ana M.').totals.design, 'nobody else\'s answer changes');
      console.log(`  ✓ op custom: 3 pieces (2 no order, 1 fix), Ana M. 1 mistake in 4 orders = 25%; ${JSON.stringify(first)} queries, none again within seconds; totals.design on the overview`);
    }

    /* ── 3 · the browser: the real page logs, the console shows ── */
    // (the browser runs only through slot.sh, which sets PW_DIR: two browser tests at a time on this machine)
    let chromium = null; try { if (process.env.PW_DIR) ({ chromium } = require(path.join(process.env.PW_DIR, 'playwright-core'))); } catch (_) {}
    if (!chromium) console.log('  – the browser check was not run (run it through slot.sh, which sets PW_DIR and CHROMIUM)');
    else await browserCheck(chromium);
  } finally { srv.close(); }
  console.log('design-log: all checks passed');
  process.exit(0);     // (the stand-in pages' retry timers would keep node waiting)

  async function browserCheck(chromium) {
    const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
    try {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
      await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => r.abort());
      // the three doors this test is about answer from the efficiency store (in-process, the REAL code); everything else from bridge-server.cjs
      await ctx.route(u => /\/\.netlify\/functions\/(firebaseOrders|employeeEfficiency|charmNestLibrary)$/.test(u.pathname), async route => {
        const req = route.request(), name = new URL(req.url()).pathname.split('/').pop(); let b = null; try { b = JSON.parse(req.postData() || 'null'); } catch (_) {}
        const reply = (code, body) => route.fulfill({ status: code, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: typeof body === 'string' ? body : JSON.stringify(body) });
        if (name === 'employeeEfficiency') { const r = await EE._t.handle({ httpMethod: req.method(), headers: req.headers(), body: req.postData() || '' }, fdb); return reply(r.statusCode, r.body); }
        if (name === 'firebaseOrders' && b && Array.isArray(b.activity)) return reply(200, Object.assign({ success: true }, await Activity.add(fdb, FV, b.activity, { prefix: /[?&]sandbox=1/.test(req.url()) ? 'Sandbox_' : '' })));
        if (name === 'charmNestLibrary' && b && b.op === 'designLog') return reply(200, await DL.add(fdb, FV, b.records, { prefix: b.sandbox === true ? 'Sandbox_' : '' }));
        return route.fallback();
      });
      await require('./master-admin-door.cjs').admin(ctx);
      await ctx.addInitScript(() => { window.confirm = () => true; window.alert = () => {}; });
      const pg = await ctx.newPage(), errs = [];
      pg.on('pageerror', e => errs.push(e.message));
      await pg.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
      await pg.waitForFunction(() => window.CNDesignLog && window.CN && window.CNAct && window.StationActivity && document.readyState === 'complete', null, { timeout: 60000 });
      st.fn.firebaseOrders = require(path.join(fnDir, 'firebaseOrders.js')).handler;     // (the sign-in session: the real door on the page's store)
      await pg.evaluate(() => { CNRole.setAdminLookup(async () => true); });            // (the Admin: no "Laser or Design?" step, as tests/stations/laser-wired.cjs signs in)
      const me = await signInAs(pg, srv, 'Paul K');     // (the name the sign-in keeps: "Paul K.")
      await pg.waitForFunction(n => { const w = StationActivity.who(); return !!w && w.person === n; }, me, { timeout: 15000 })
        .catch(async e => { throw new Error('not signed in: ' + JSON.stringify(await pg.evaluate(() => ({ sa: StationActivity.who(), ss: StationSession.who(), page: StationSession.page && StationSession.page(), name: CNEmployee.name(), role: CNRole.state() })))); });
      // a burst from the page: the editor opened and saved on an order, a custom piece with no order, a fix of order 3521000888
      const ms = await pg.evaluate(() => {
        const t = performance.now();
        CNDesignLog.event('editorOpen', { orderId: '3521000555', sku: 'CAT-2' });
        CNDesignLog.event('editorSave', { orderId: '3521000555', sku: 'CAT-2', faces: ['back'] });
        CNDesignLog.event('customPieceSent', { orderId: 'CUSTOM-20261028-7', designId: 'des-cat', version: 2, name: 'Cat', metal: 'gold14k', quantity: 1, customName: 'Shop sample', reason: 'for the display', sendId: 'pg-1' });
        CNDesignLog.event('fixPiece', { customId: 'CUSTOM-20261028-8', designId: 'des-cat', version: 2, name: 'Cat', metal: 'gold', customName: 'Redo cat', reason: 'wrong font', sendId: 'pg-2', fixes: { orderId: '3521000888', causedBy: 'Ana M', suggested: 'Ana M' } });
        return performance.now() - t;
      });
      await pg.evaluate(() => CNDesignLog.flush().then(() => StationActivity.flush(true)));
      await pg.waitForFunction(() => CNDesignLog.pending().records === 0, null, { timeout: 15000 });
      for (let i = 0; i < 60 && !((fdoc('Efficiency_Daily', `${today}__${me}`) || {}).stations || {}).sorter; i++) { await pg.evaluate(() => StationActivity.flush(true)); await wait(250); }
      const pk = fdoc('Efficiency_Daily', `${today}__${me}`);
      assert(pk && pk.stations && pk.stations.sorter, 'the real station-activity.js wrote the notes');
      assert.deepEqual([pk.stations.sorter.x_dopen, pk.stations.sorter.x_dsave, pk.stations.sorter.x_cust, pk.stations.sorter.x_custNo, pk.stations.sorter.x_fix], [1, 1, 2, 2, 1]);
      assert(fdoc('Design_Log', 'dl-pg-1') && fdoc('Design_Log', 'dl-pg-2').fix.causedBy === 'Ana_M', 'the records reached Design_Log');
      await wait(5200);   // (an answer of op custom is kept 5 s per instance, as the overview's: what was written since shows after it)
      { const q0 = queries.Design_Log || 0, chk = await eff({ op: 'custom', key: PASS }); assert.equal(chk.j.totals.sent, 5, 'op custom sees them: ' + JSON.stringify({ q: (queries.Design_Log || 0) - q0, today, pg1: fdoc('Design_Log', 'dl-pg-1'), ids: chk.j.pieces.map(p => p.id), now: Date.now() })); }
      await pg.evaluate(() => OrderTimeline.flush()); await wait(400);
      const tl = st.list('Order_Timeline').filter(e => e.orderId === '3521000555' || e.orderId === '3521000888').map(e => [e.orderId, e.type, e.by]);
      assert.deepEqual(tl.sort(), [['3521000555', 'designDropped', me], ['3521000888', 'designSent', me]], 'the order\'s timeline: the save, and the fix on the order it fixes');
      // the console: Workspace > Employee efficiency, the passcode, the Custom pieces tab
      await pg.waitForFunction(() => window.CharmNestMasterAdmin && CharmNestMasterAdmin.is() === true, null, { timeout: 15000 });
      await pg.click('#moreMenu > summary'); await pg.click('#moreMenu .moreList button[data-mode="efficiency"]');
      await pg.waitForSelector('#efficiencyView .efKey input', { state: 'visible', timeout: 20000 });
      await pg.fill('#efficiencyView .efKey input', PASS); await pg.press('#efficiencyView .efKey input', 'Enter');
      await pg.waitForSelector('#efficiencyView .efPeople .efP', { timeout: 20000 });
      const line = await pg.locator('#efficiencyView .efP[data-name="Paul K."] .efPD').innerText();
      assert.match(line, /^2 custom pieces \(2 no order\) · 1 fix · 1 design save$/, 'the People row folds in the design work: ' + line);
      await pg.click('#efficiencyView .efTabBtn[data-tab="custom"]');
      await pg.waitForFunction(() => location.hash === '#efficiency/custom' && document.querySelectorAll('#efficiencyView .efcRow').length === 5, null, { timeout: 15000 })
        .catch(async () => { throw new Error('the Custom pieces tab: ' + JSON.stringify(await pg.evaluate(() => ({ hash: location.hash, mounted: Efficiency.state.mounted, tab: Efficiency.state.tab, rows: document.querySelectorAll('#efficiencyView .efcRow').length, text: (document.querySelector('#efPgCustom') || {}).innerText }))) + ' errors: ' + errs.join(' | ')); });
      const kp = await pg.$$eval('#efficiencyView .efc .efKV', n => n.map(x => x.textContent));
      assert.deepEqual(kp, ['5', '4', '2', '2'], 'custom pieces, no order, fixes, mistakes');
      assert.equal(await pg.locator('#efficiencyView .efcRow.no').count(), 4, 'every piece with no order is flagged');
      const ana = await pg.locator('#efficiencyView .efcPeople tr[data-name="Ana M."]').innerText();
      assert.match(ana.replace(/\s+/g, ' '), /Ana M\. — — — 2 4 50% —/, 'Ana M: 2 mistakes in 4 orders worked = 50%: ' + ana);
      await pg.click('#efficiencyView .efcFilter button[data-f="fix"]');
      const fixes = await pg.$$eval('#efficiencyView .efcRow', n => n.map(x => x.innerText.replace(/\s+/g, ' ')));
      assert.equal(fixes.length, 2);
      assert(fixes.some(t => /Redo cat/.test(t) && /Fixes order 3521000888 · mistake by Ana M\. · fixed by Paul K\./.test(t)), fixes.join(' | '));
      assert(fixes.some(t => /mistake by Ana M\. \(the order's history said Ivy\) · fixed by Bea/.test(t)), 'a changed suggestion says so');
      const busy = await pg.evaluate(() => { const t = performance.now(); document.querySelector('#efficiencyView .efcFilter button[data-f="all"]').click(); return performance.now() - t; });
      // the range: 7 days asks op custom for 7 days and draws the days
      const asked = [];
      pg.on('request', r => { if (/employeeEfficiency/.test(r.url())) { try { const b = JSON.parse(r.postData() || '{}'); if (b.op === 'custom') asked.push(b.days); } catch (_) {} } });
      await pg.click('#efficiencyView .efSeg button[data-days="7"]');
      await pg.waitForFunction(() => { const s = document.querySelector('#efficiencyView .efcDaysSec'); return s && !s.hidden && s.querySelectorAll('.efcDay').length === 7; }, null, { timeout: 15000 });
      assert(asked.includes(7), 'asked for 7 days');
      await pg.screenshot({ path: path.join(process.env.SHOT_DIR || require('os').tmpdir(), 'design-log-custom-1440.png'), fullPage: false }).catch(() => {});
      // phone width: no sideways scroll on the tab
      await pg.setViewportSize({ width: 390, height: 800 }); await wait(200);
      const over = await pg.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
      assert(over <= 1, `no sideways scroll at 390 px (${over})`);
      await pg.screenshot({ path: path.join(process.env.SHOT_DIR || require('os').tmpdir(), 'design-log-custom-390.png') }).catch(() => {});
      assert.deepEqual(errs, [], 'no page errors');
      console.log(`  ✓ browser: 4 events from the page cost ${ms.toFixed(2)} ms; notes counted, records stored, timeline written; the Custom pieces tab: 5 pieces, 4 flagged, Ana M 50%; filter ${busy.toFixed(1)} ms; 7 days; 390 px`);
    } finally { await browser.close(); }
  }
})().catch(e => { console.error(e); process.exit(1); });
