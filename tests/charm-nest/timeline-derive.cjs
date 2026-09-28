// The order timeline read back whole (Paul C1 · C7 · D2): timelineGet answers an order's recorded events plus the events
// read from the records the shop already keeps (arrivals, pool, backs, sheets, sets, custom orders, custom readings,
// cancel record, the Team's thread, the design ledgers and the inbox's Etsy mirror), deduplicated against the recorded
// ones, with `where` (where the order is now). Against an in-memory Firestore that answers only single-field queries,
// hands out only the fields asked for, and waits a little on every read so the number of round trips shows. No network.
//   node tests/charm-nest/timeline-derive.cjs
const path = require('path'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── in-memory Firestore ──────────────────────────────────────────────── */
const store = new Map(), touched = new Set(), LAT = 40;
let queries = 0;
const ts = ms => ({ toMillis: () => ms });
const SENT = { __ts: 1 };
const FieldValue = { serverTimestamp: () => SENT, delete: () => ({ __del: 1 }), increment: n => ({ __inc: n }) };
const wait = () => new Promise(r => setTimeout(r, LAT));
const getPath = (o, f) => f.split('.').reduce((x, k) => (x == null ? undefined : x[k]), o);
const norm = v => (v && v.toMillis ? v.toMillis() : v);
function pick(d, fields) {
  const o = {};
  for (const f of fields) {
    const parts = f.split('.'), v = getPath(d, f); if (v === undefined) continue;
    let t = o; parts.slice(0, -1).forEach(p => { t = t[p] = t[p] || {}; }); t[parts[parts.length - 1]] = v;
  }
  return o;
}
const top = p => p.split('/')[0];
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key,
    async get(mask, batched) { touched.add(top(coll)); if (!batched) await wait(); const d0 = store.get(key), d = d0 && mask ? pick(d0, mask) : d0; return { exists: !!d, id, ref: this, data: () => (d ? JSON.parse(JSON.stringify(d), rev) : undefined) }; },
    collection(sub) { return query(key + '/' + sub); },
    async set(data, opts) { const cur = opts && opts.merge ? store.get(key) || {} : {}; const next = Object.assign({}, cur); for (const [k, v] of Object.entries(data)) if (v === SENT) next[k] = ts(Date.now()); else next[k] = v; store.set(key, next); }
  };
}
// timestamps survive the copy a read hands out
const rev = (k, v) => (v && typeof v === 'object' && v.__ms != null ? ts(v.__ms) : v);
const put = (key, d) => store.set(key, JSON.parse(JSON.stringify(d, (k, v) => (v && typeof v === 'object' && typeof v.toMillis === 'function' ? { __ms: v.toMillis() } : v)), rev));
function query(coll, filters = [], order = null, lim = 0, mask = null) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim, mask); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim, mask); },
    limit(n) { return query(coll, filters, order, n, mask); },
    select(...fields) { return query(coll, filters, order, lim, fields); },
    doc(id) { return docRef(coll, id); },
    async get() {
      touched.add(top(coll)); queries++; await wait();
      // single-field only: one field filtered and ordered at most (no composite index anywhere)
      const fields = new Set(filters.map(([f]) => f).concat(order ? [order[0]] : []));
      assert(fields.size <= 1, `a timeline query uses one field only (${coll}: ${[...fields]})`);
      assert(lim > 0, `every timeline query is capped (${coll})`);
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }));
      for (const [f, op, v0] of filters) rows = rows.filter(({ v: d }) => {
        const x = getPath(d, f);
        if (op === '==') return norm(x) === v0; if (op === 'in') return v0.includes(norm(x));
        if (op === 'array-contains-any') return Array.isArray(x) && x.some(e => v0.includes(e));
        throw new Error('unexpected operator ' + op);
      });
      if (order) { rows = rows.filter(r => getPath(r.v, order[0]) !== undefined); rows.sort((a, b) => { const x = norm(getPath(a.v, order[0])), y = norm(getPath(b.v, order[0])); return (x > y ? 1 : x < y ? -1 : 0) * (order[1] === 'desc' ? -1 : 1); }); }
      rows = rows.slice(0, lim);
      const docs = rows.map(({ id, v }) => { const d = mask ? pick(v, mask) : v; return { id, exists: true, ref: docRef(coll, id), data: () => JSON.parse(JSON.stringify(d, (k, x) => (x && typeof x === 'object' && typeof x.toMillis === 'function' ? { __ms: x.toMillis() } : x)), rev) }; });
      return { size: docs.length, docs, empty: !docs.length };
    }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, d, o) { ops.push(() => ref.set(d, o)); }, async commit() { for (const o of ops) await o(); } }; },
  // one round trip for the whole list, as Firestore's batchGet
  async getAll(...refs) { const o = refs.length && typeof refs[refs.length - 1].get !== 'function' ? refs.pop() : null; await wait(); return Promise.all(refs.map(r => r.get(o && o.fieldMask, true))); }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => '__name__' }, Timestamp: { fromMillis: ts } }), storage: () => ({ bucket: () => ({}) }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => ({ ok: true, status: 202 });
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const Timeline = require(path.join(fnDir, '_orderTimeline.js'));
const post = body => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const ok = async body => { const r = await post(body); assert.strictEqual(r.status, 200, body.op + ': ' + JSON.stringify(r.body)); return r.body; };

/* ── one order, across every record the shop keeps ── */
const RID = '3521000777', T0 = Date.parse('2026-09-20T10:00:00Z'), M = 60000, H = 3600000, D = 86400000;
const sec = ms => Math.floor(ms / 1000);
const big = 'x'.repeat(20000);   // the heavy fields a timeline read must leave out
put(`EtsyMail_Receipts/${RID}`, { receipt_id: RID, created_timestamp: sec(T0 - H), updated_timestamp: sec(T0 + 5 * D), status: 'Completed', is_shipped: true, is_paid: true,
  raw: { shipments: [{ shipment_notification_timestamp: sec(T0 + 5 * D), carrier_name: 'USPS', tracking_code: '9400111' }], transactions: [{ description: big }] } });
put(`Charm_Nest_Arrivals/${RID}`, { id: RID, firstSeenAt: T0, createTs: sec(T0 - H) });
const pool = (id, o) => put(`Charm_Pool/${id}`, Object.assign({ poolId: id, orderId: RID, runId: 'run-1', createdAt: ts(T0 + 10 * M), updatedAt: ts(T0 + 2 * H) }, o));
pool(`${RID}_tx1_1`, { transactionId: 'tx1', lineKey: `${RID}_tx1`, sku: 'BR-1', material: 'gold', copy: 1, state: 'committed', sheetId: 'shG', setId: 'set-1', engraveApprovedBy: 'Ana', committedAt: T0 + 2 * H });
pool(`${RID}_tx1_2`, { transactionId: 'tx1', lineKey: `${RID}_tx1`, sku: 'BR-1', material: 'gold', copy: 2, state: 'committed', sheetId: 'shG', setId: 'set-1', engraveApprovedBy: 'Ana', committedAt: T0 + 2 * H, movedAt: T0 + 40 * M, movedBy: 'Ben', movedFrom: 'shOld', movedTo: 'shG' });
pool(`${RID}_tx2_1`, { orderId: +RID, transactionId: 'tx2', lineKey: `${RID}_tx2`, sku: 'BR-2', material: 'silver', copy: 1, state: 'abandoned', sheetId: null, createdAt: ts(T0 + 11 * M), removedAt: T0 + 50 * M, removedBy: 'Cara', removedReason: 'on hold' });
pool(`${RID}_tx3_1`, { transactionId: 'tx3', lineKey: `${RID}_tx3`, sku: 'RG-1', material: 'rose', copy: 1, state: 'written', sheetId: 'shR', createdAt: ts(T0 + 12 * M) });
pool('3999999999_tx1_1', { orderId: '3999999999', transactionId: 'tx1', sku: 'OTHER' });
put(`Charm_Pool_Back/${RID}_tx1_1`, { poolId: `${RID}_tx1_1`, order: RID, transactionId: 'tx1', sheetId: 'shG', setId: 'set-1', approvedAt: T0 + H, approvedBy: 'Ana', text: 'Love\nMom', outputs: { ai: { path: big } } });
const sheet = (id, o) => put(`Charm_Nest_Sheets/${id}`, Object.assign({ id, archived: false, createdAt: ts(T0 + 5 * M), updatedAt: ts(T0 + 3 * H), charms: [{ id: 'c', outline: big }], placements: [{ id: 'c' }] }, o));
sheet('shG', { metal: 'gold', metalLabel: 'GF 14/20', sheetIndex: 2, setId: 'set-1', setSeq: 1, fileBase: 'GF_Sep.20.26_Set-1_Sheet-2', orders: [RID, '3999999999'], poolIds: [`${RID}_tx1_1`, `${RID}_tx1_2`, '3999999999_tx1_1'], stock: { wPt: 283.46, hPt: 141.73 },
  label: { files: [{ path: 'l.png', orders: [RID, '3999999999'], part: 1 }] }, laserDoneAt: T0 + 3 * H, laserDoneBy: 'Dee' });
sheet('shR', { metal: 'rose', metalLabel: 'RG', sheetIndex: 1, orders: [+RID], poolIds: [`${RID}_tx3_1`], createdAt: ts(T0 + 6 * M), roseStockId: 'stock-1', rosePlanHash: 'h1', roseCutAt: T0 + 150 * M,
  rosePlanJson: JSON.stringify({ stages: [{ n: 1, at: T0 + 130 * M, ids: [`pool:src:${RID}_tx3_1`], lines: [0, 3] }, { n: 2, at: T0 + 140 * M, ids: ['pool:src:3888888888_tx1_1'], lines: [3, 5] }], shapes: [{ paths: big }] }) });
sheet('shArchived', { metal: 'gold', sheetIndex: 1, archived: true, orders: [RID], laserDoneAt: T0 + H, laserDoneBy: 'Nope' });
put('Charm_Nest_Rose_Stock/stock-1/cuts/shR', { at: T0 + 150 * M, by: 'Eve', planJson: big });
put('Charm_Nest_Sets/set-1', { setId: 'set-1', seq: 1, name: 'Set-1', day: '2026-09-20', committedAt: T0 + 2 * H, committed: [RID, '3999999999'], orders: { [RID]: { held: null, lines: [] }, 3999999999: { held: null, lines: [{ big }] } }, labelFiles: [{ big }] });
put(`Charm_Custom_Orders/${RID}_tx9`, { key: `${RID}_tx9`, receiptId: RID, transactionId: 'tx9', sku: 'CUSTOM-1', completedAt: T0 + 30 * M, completedBy: 'Fay', how: 'print', printedAt: T0 + 30 * M, printedBy: 'Fay', prints: 2,
  stamps: [{ how: 'print', at: T0 + 30 * M, by: 'Fay' }, { how: 'print', at: T0 + 31 * M, by: 'Fay' }], label: big });
put(`Charm_Nest_CustomRead/${RID}_tx9`, { order: RID, latest: 'h9', reads: { h9: { kind: 'custom', confidence: 0.93, summary: 'A custom charm from a drawing', at: T0 + 20 * M } }, decided: { kind: 'custom', by: 'Fay', at: T0 + 25 * M }, decidedSandbox: { kind: 'regular', by: 'Sim', at: T0 + 26 * M } });
const msg = (id, text, by, at) => put(`Brites_Orders/${RID}/messages/${id}`, { text, senderName: by, senderRole: 'staff', timestamp: ts(at) });
msg('m1', 'Please engrave Love Mom on the back', 'Ivy', T0 + 15 * M);
msg('designed-set-set-1', 'DESIGNED :)', 'Charm Sorter (Gia)', T0 + 2 * H + 30000);
msg('m3', 'QA1', 'Hal', T0 + 4 * H);
msg('m4', 'PE', 'Hal', T0 + 4 * H + 10 * M);
put(`Design_Completed Orders/${RID}`, { orderId: RID, completed: true, completedAt: ts(T0 + 2 * H + 20000) });
put(`Design_Order_Archive/${RID}`, { receiptId: RID, completedAtMs: T0 + 2 * H + 10000, completedBy: 'Charm Sorter (Gia)', setId: 'set-1', sheetIds: ['shG'], status: { createdTs: sec(T0 - H), isShipped: true }, items: [{ big }], raw: { big } });
// what was recorded as it happened (the timeline's own events)
const rec = (key, e) => put(`Order_Timeline/${RID}~${e.type}~${key}`, Object.assign({ orderId: RID, source: 'sorter', station: 'sorter', device: '', lineKey: '', transactionId: '', sheetId: '', sheet: '', setId: '', text: '', data: null, milestone: true, createdAt: ts(T0) }, e));
rec('p1', { type: 'placed', at: T0 + 20 * M, by: 'Gia', sheetId: 'shG', sheet: 'GF Sheet 2', text: 'Placed' });
rec('l1', { type: 'laserDone', at: T0 + 3 * H + 60000, by: 'Dee', sheetId: 'shG', sheet: 'GF Sheet 2', station: 'laser' });
rec('e1', { type: 'engraveApproved', at: T0 + H + 10 * M, by: 'Ana', sheetId: 'shG' });
rec('w1', { type: 'welded', at: T0 + 4 * H + 5 * M, by: 'Jon', source: 'station', station: 'welding', device: 'weld-1' });
rec('s1', { type: 'shipped', at: T0 + 5 * D - 60000, by: 'Kim', source: 'station', station: 'shipping', device: 'shipping-1' });

/* ── the sandbox's own copy of the same order number: a rehearsal that was cancelled on its sheet ── */
const S0 = T0 + 7 * D;
put(`Sandbox_Charm_Nest_Arrivals/${RID}`, { id: RID, firstSeenAt: S0, createTs: sec(S0 - H) });
put(`Sandbox_Charm_Pool/${RID}_tx1_1`, { poolId: `${RID}_tx1_1`, orderId: RID, transactionId: 'tx1', lineKey: `${RID}_tx1`, sku: 'BR-1', material: 'gold', sheetId: 'sbG', createdAt: ts(S0 + 5 * M) });
put('Sandbox_Charm_Nest_Sheets/sbG', { id: 'sbG', metal: 'gold', sheetIndex: 1, orders: [RID], poolIds: [`${RID}_tx1_1`], createdAt: ts(S0 + 2 * M), archived: false });
put(`Sandbox_Charm_Nest_Cancelled/${RID}`, { orderId: RID, by: 'Sim', why: 'rehearsal', at: S0 + H, sheets: ['sbG'] });
put(`Sandbox_Brites_Orders/${RID}/messages/q2`, { text: 'QA2', senderName: 'Sim', timestamp: ts(S0 + 30 * M) });
put(`Sandbox_Order_Timeline/${RID}~pulled~x`, { orderId: RID, type: 'pulled', at: S0 + M, by: 'Sim', source: 'sorter', station: 'sorter', text: 'Pulled', milestone: false });

(async () => {
  /* 1 · production: the whole history, recorded and derived */
  touched.clear(); queries = 0;
  const t0 = Date.now();
  const a = await ok({ op: 'timelineGet', orderId: RID });
  const took = Date.now() - t0;
  const byId = new Map(a.events.map(e => [e.id, e]));
  const derived = a.events.filter(e => e.derived), recorded = a.events.filter(e => !e.derived);
  const has = (type, pred = () => true) => a.events.filter(e => e.type === type && pred(e));
  assert.strictEqual(recorded.length, 5, 'every recorded event is answered');
  assert(!a.derived.errors, 'no read failed: ' + JSON.stringify(a.derived.errors));
  for (const e of derived) {
    assert(Timeline.TYPES.has(e.type), 'a known type: ' + e.type);
    assert(e.id.startsWith(`${RID}~${e.type}~d-`), 'a derived id of its own: ' + e.id);
    assert(e.at > 1e12 && e.orderId === RID && e.series === undefined, 'a whole event: ' + JSON.stringify(e));
    assert(JSON.stringify(e.data || null).length <= 2048 && e.text.length <= 200, 'small: ' + e.id);
  }
  // oldest first
  assert(a.events.every((e, i) => !i || a.events[i - 1].at <= e.at), 'the timeline is in time order');
  // from Etsy (the mirror): placed, completed; its shipment is the one the station recorded (within 3 minutes)
  const placed = byId.get(`${RID}~arrived~d-etsy-placed`);
  assert(placed && placed.at === T0 - H && placed.by === 'Etsy' && placed.milestone === true, 'placed on Etsy: ' + JSON.stringify(placed));
  assert(byId.get(`${RID}~arrived~d-first-seen`).at === T0 && byId.get(`${RID}~arrived~d-first-seen`).milestone === false, 'first seen by the sorter');
  assert.strictEqual(has('shipped').length, 1, 'shipped once: the recorded one wins over the mirror\'s within 3 minutes');
  assert(!has('shipped')[0].derived && has('shipped')[0].by === 'Kim', 'the recorded shipment stays');
  assert(has('etsyCompleted', e => e.derived && e.at === T0 + 5 * D).length === 1, 'completed on Etsy');
  // the custom reading and the person's decision (production's, not the sandbox's)
  assert(has('customRead', e => e.lineKey === `${RID}_tx9` && /custom/.test(e.text) && e.at === T0 + 20 * M).length === 1, 'custom reading');
  const dec = has('customDecided'); assert(dec.length === 1 && dec[0].by === 'Fay' && dec[0].data.kind === 'custom', 'custom decision: ' + JSON.stringify(dec));
  // the pieces: one pooled event per line (the order number stored as a number found too), moved, taken off
  const pooled = has('pooled'); assert.deepStrictEqual(pooled.map(e => e.lineKey).sort(), [`${RID}_tx1`, `${RID}_tx2`, `${RID}_tx3`], 'one pooled event per line');
  assert(pooled.find(e => e.lineKey === `${RID}_tx1`).data.pieces === 2, 'a line with two pieces');
  const moved = has('moved'); assert(moved.length === 1 && moved[0].by === 'Ben' && moved[0].sheetId === 'shG' && moved[0].sheet === 'GF Sheet 2', 'moved: ' + JSON.stringify(moved));
  const removed = has('removed'); assert(removed.length === 1 && removed[0].by === 'Cara' && removed[0].data.reason === 'on hold' && removed[0].lineKey === `${RID}_tx2`, 'taken off: ' + JSON.stringify(removed));
  // back engraving: the back's approval (10 minutes from the recorded one: both kept); the piece's own mark folded in
  const eng = has('engraveApproved'); assert.strictEqual(eng.length, 2, 'the recorded approval and the back\'s own: ' + JSON.stringify(eng));
  assert(eng.some(e => e.derived && e.by === 'Ana' && e.at === T0 + H && /Love \/ Mom/.test(e.text)), 'the back\'s approval with its words');
  // sheets: placed on the RG sheet (the GF one was recorded, at another time: the recorded placement wins), labels,
  // Rose Gold line and cut, laser (recorded within 3 minutes: one), an archived sheet left out
  const pl = has('placed'); assert.strictEqual(pl.length, 2, 'placed once per sheet: ' + JSON.stringify(pl.map(e => [e.id, e.sheet])));
  assert(pl.some(e => !e.derived && e.sheetId === 'shG') && pl.some(e => e.derived && e.sheetId === 'shR' && e.sheet === 'RG Sheet 1' && e.at === T0 + 12 * M), 'placed on the RG sheet when its piece was ready');
  assert(has('placed', e => e.sheetId === 'shG')[0].by === 'Gia', 'the recorded placement stays');
  const qr = has('qrLabel'); assert(qr.length === 1 && qr[0].sheetId === 'shG' && qr[0].at === T0 + 2 * H && qr[0].data.approx, 'QR label, bounded by the commit: ' + JSON.stringify(qr));
  const rl = has('roseLine'); assert(rl.length === 1 && rl[0].data.n === 1 && rl[0].at === T0 + 130 * M, 'only the green line its piece is on: ' + JSON.stringify(rl));
  const rc = has('roseCut'); assert(rc.length === 1 && rc[0].by === 'Eve' && rc[0].station === 'laser', 'RG cut with who cut it');
  assert.strictEqual(has('laserDone').length, 1, 'the laser cut once (recorded within 3 minutes)');
  assert(!a.events.some(e => e.sheetId === 'shArchived' || e.by === 'Nope'), 'a repacked (archived) sheet is left out');
  // committed: the set's own record; the archive, the design ledger and the pieces' marks say the same and are folded in
  const sc = has('setCommitted'); assert(sc.length === 1 && sc[0].setId === 'set-1' && sc[0].at === T0 + 2 * H && sc[0].by === 'Charm Sorter (Gia)' && /Set-1/.test(sc[0].text), 'set committed once: ' + JSON.stringify(sc));
  assert(!has('note', e => e.data && e.data.stamp === 'designComplete').length, 'the design ledger folds into the commit');
  // custom seals: one per press (two prints a minute apart), and the completion
  assert.strictEqual(has('sealPrinted').length, 2, 'a seal per press');
  assert(has('sealCompleted', e => e.by === 'Fay' && e.at === T0 + 30 * M).length === 1, 'custom completion');
  // the Team's thread: stamps as their own events, the rest as messages
  const notes = has('note').map(e => e.text).sort(); assert.deepStrictEqual(notes, ['DESIGNED :)', 'PE', 'QA1'], 'the Team\'s stamps: ' + JSON.stringify(notes));
  assert(has('note', e => e.text === 'DESIGNED :)')[0].milestone === true, 'DESIGNED :) is a milestone');
  assert(has('teamMessage', e => e.by === 'Ivy' && /Love Mom/.test(e.text)).length === 1, 'a Team message');
  assert(!has('cancelled').length && !has('etsyCancelled').length && a.cancelled === null, 'not cancelled');
  // where it is now: completed on Etsy, last seen at shipping by Kim
  assert.strictEqual(a.where.stage, 'completed', 'where: ' + JSON.stringify(a.where));
  assert(a.where.station === 'shipping' && a.where.by === 'Kim' && a.where.label === 'Completed on Etsy' && a.where.cut === true, 'where: ' + JSON.stringify(a.where));
  assert(a.where.step === 8 && a.where.rail[a.where.step] === 'Completed', 'the rail\'s last step');
  // cheap: two round trips beside the recorded read, one-field queries only (asserted by the fake), capped
  assert(took < LAT * 5, `answered in ${took} ms with ${LAT} ms a round trip: at most about three round trips`);
  assert(!touched.has('Sandbox_Charm_Pool') && !touched.has('Sandbox_Order_Timeline'), 'production reads production');
  // stable: asked again, the same ids
  const again = await ok({ op: 'timelineGet', orderId: RID });
  assert.deepStrictEqual(again.events.map(e => e.id), a.events.map(e => e.id), 'the same ids every time');
  // derive:false answers the recorded events alone
  const only = await ok({ op: 'timelineGet', orderId: RID, derive: false });
  assert(only.events.length === 5 && only.events.every(e => !e.derived) && only.where.stage === 'shipped', 'recorded only: ' + JSON.stringify(only.where));

  /* 2 · the sandbox: its own records (the Sandbox_ prefix exactly where charmNestLibrary puts it), no Etsy mirror */
  touched.clear();
  const b = await ok({ op: 'timelineGet', orderId: RID, sandbox: true });
  for (const name of ['Charm_Nest_Arrivals', 'Charm_Pool', 'Charm_Nest_Sheets', 'Charm_Custom_Orders', 'Charm_Nest_Cancelled', 'Order_Timeline', 'Brites_Orders', 'Design_Completed Orders', 'Design_Order_Archive'])
    assert(touched.has('Sandbox_' + name) && !touched.has(name), `the sandbox reads Sandbox_${name} only: ${[...touched]}`);
  assert(!touched.has('EtsyMail_Receipts'), 'the sandbox does not read production\'s Etsy mirror');
  assert(touched.has('Charm_Nest_CustomRead') && !touched.has('Sandbox_Charm_Nest_CustomRead'), 'custom readings are shared, as _charmNestCustomRead keeps them');
  const bt = t => b.events.filter(e => e.type === t);
  assert(bt('pulled').length === 1 && !bt('pulled')[0].derived, 'the sandbox\'s recorded event');
  // (a custom reading is shared by both workspaces, as _charmNestCustomRead keeps it; a decision is per workspace)
  assert(bt('arrived').some(e => e.at === S0) && !b.events.some(e => e.at < S0 - 2 * H && !/^custom/.test(e.type)), 'only the sandbox\'s history: ' + JSON.stringify(b.events.map(e => [e.type, e.at - S0])));
  assert(bt('customDecided').length === 1 && bt('customDecided')[0].by === 'Sim', 'the sandbox\'s own custom decision');
  assert(bt('placed').length === 1 && bt('placed')[0].sheet === 'GF Sheet 1', 'placed on its sandbox sheet');
  assert(bt('cancelled').length === 1 && bt('cancelled')[0].by === 'Sim' && /rehearsal/.test(bt('cancelled')[0].text), 'its cancel record is an event');
  assert(bt('note').length === 1 && bt('note')[0].text === 'QA2', 'its own Team stamp');
  assert(b.cancelled && b.cancelled.by === 'Sim', 'its cancel record');
  assert(b.where.stage === 'cancelled' && b.where.sheet === 'GF Sheet 1' && /pieces on GF Sheet 1/.test(b.where.text) && b.where.step === 1, 'where: cancelled on the sheet, the rail stopped at On sheet: ' + JSON.stringify(b.where));

  /* 3 · the rules on their own: dedupe and where */
  const ev = (type, at, o) => Object.assign({ orderId: RID, type, at, data: null, text: '' }, o);
  const d = Timeline.dedupe([ev('placed', T0, { sheetId: 'A' })], [ev('placed', T0 + 2 * M, { sheetId: 'A' }), ev('placed', T0 + M, { sheetId: 'B' }), ev('placed', T0 + 10 * M, { sheetId: 'A' }), ev('laserDone', T0 + 4 * M, { sheetId: 'A' }), ev('placed', T0 + 9 * H, { sheetId: 'A', data: { approx: true } })]);
  assert.deepStrictEqual(d.map(e => [e.type, e.sheetId, e.at - T0]), [['placed', 'B', M], ['placed', 'A', 10 * M], ['laserDone', 'A', 4 * M]], 'dedupe: same type and sheet within ±3 min, or any time when the time is a bound');
  const w = (list, c, h) => Timeline.whereOf(list, c, h);
  assert.strictEqual(w([ev('arrived', T0)]).stage, 'waiting');
  assert.strictEqual(w([ev('arrived', T0), ev('needsDecision', T0 + M)]).stage, 'review');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A', sheet: 'SS Sheet 3' })]).label, 'On SS Sheet 3');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A', sheet: 'SS Sheet 3' }), ev('removed', T0 + M, { data: { reason: 'on hold' } })]).stage, 'held');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A' }), ev('laserDone', T0 + M, { sheetId: 'A' }), ev('sorted', T0 + 2 * M, { station: 'sorting', by: 'Lu' })]).stage, 'sorted');
  const wa = w([ev('laserDone', T0, { sheetId: 'A' }), ev('assembled', T0 + M, { station: 'assembly', device: 'assembly-2', by: 'Mo' }), ev('scan', T0 + 2 * M, { station: 'shipping', by: 'Ned' })]);
  assert(wa.stage === 'assembled' && wa.station === 'shipping' && wa.by === 'Ned', 'the last station and person: ' + JSON.stringify(wa));
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A' }), ev('cancelled', T0 + M, { by: 'Oz' })]).stage, 'cancelled');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A' }), ev('cancelled', T0 + M), ev('cancelRestored', T0 + 2 * M)]).stage, 'sheet');
  assert.strictEqual(w([ev('arrived', T0)], null, { sheets: [{ sheetId: 'Q', sheet: 'GF Sheet 4', cut: false }] }).label, 'On GF Sheet 4', 'the sheets that hold it now win over a missing placement');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A' })], null, { sheets: [] }).stage, 'waiting', 'a sheet that no longer holds it');
  assert.strictEqual(w([ev('placed', T0, { sheetId: 'A' })], { by: 'Etsy', at: T0 + M }).stage, 'cancelled', 'a cancel record alone');
  assert.strictEqual(w([ev('arrived', T0), ev('note', T0 + M, { data: { stamp: 'DESIGNED :)' } })]).stage, 'designed', 'designed at the design station');

  /* 4 · a read that fails is named, and the rest still answers */
  const realGetAll = db.getAll; db.getAll = async (...refs) => { if (refs.some(r => r.path && r.path.startsWith('Charm_Nest_Sets/'))) throw new Error('boom'); return realGetAll(...refs); };
  const c = await ok({ op: 'timelineGet', orderId: RID });
  db.getAll = realGetAll;
  assert(c.derived.errors && c.derived.errors.some(x => /^sets: boom/.test(x)) && c.events.some(e => e.type === 'setCommitted'), 'a failed read is named; the archive still says it was committed');

  console.log(`timeline-derive: OK (${derived.length} derived, ${recorded.length} recorded, ${a.derived.dropped} folded in; ${took} ms at ${LAT} ms a read; ${queries} queries)`);
})().catch(e => { console.error(e); process.exit(1); });
