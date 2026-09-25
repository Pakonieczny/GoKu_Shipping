// Uptime: what the server reads and keeps stays bounded however long the shop runs. Runs the real charmNestLibrary,
// firebaseOrders and designArchive handlers against an in-memory Firestore/Storage that counts every document it
// hands out. No network.
//   node tests/charm-nest/server-history-bounds.cjs
const path = require('path'), crypto = require('crypto'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── in-memory Firestore that counts reads ─────────────────────────────── */
const store = new Map();                                   // "coll/id" (or "coll/id/sub/id") → data
const reads = new Map(), jsonReads = { n: 0 };             // documents handed out, per collection
const SENT = { ts: { __ts: 1 }, del: { __del: 1 } };
const ts = ms => ({ toMillis: () => ms });
const FieldValue = { serverTimestamp: () => SENT.ts, delete: () => SENT.del, increment: n => ({ __inc: n }) };
const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Date) && !v.toMillis && !v.__inc && v !== SENT.ts && v !== SENT.del;
const clone = v => Array.isArray(v) ? v.map(clone) : v instanceof Date ? new Date(v) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
const value = (cur, v) => v === SENT.ts ? ts(Date.now()) : v && v.__inc != null ? (cur || 0) + v.__inc : clone(v);
function merge(target, src, deep) {
  for (const [k, v] of Object.entries(src)) {
    if (v === SENT.del) delete target[k];
    else if (deep && plain(v) && plain(target[k])) merge(target[k], v, true);
    else target[k] = plain(v) ? merge({}, v, true) : value(target[k], v);
  }
  return target;
}
const getPath = (o, f) => f.split('.').reduce((x, k) => (x == null ? undefined : x[k]), o);
const norm = v => (v && v.toMillis ? v.toMillis() : v instanceof Date ? v.getTime() : v);
// (sheetCharms counts sheet records handed out with their charms: the lists read only the fields they send)
const sheetCharms = { n: 0 };
const count = (coll, d) => { const c = coll.split('/')[0].includes('Run_Lines') ? 'Charm_Nest_Run_Lines' : coll.split('/').length > 1 ? coll.split('/')[0] + '/' + coll.split('/').slice(-1)[0] : coll; reads.set(c, (reads.get(c) || 0) + 1); if (d && d.json != null && /Run_Lines/.test(coll)) jsonReads.n++; if (d && d.charms && coll === 'Charm_Nest_Sheets') sheetCharms.n++; };
const pick = (d, fields) => Object.fromEntries(fields.filter(f => d[f] !== undefined).map(f => [f, d[f]]));
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key, parent: { id: coll },
    // (getAll passes its fieldMask: only those fields are handed out)
    async get(mask) { const d0 = store.get(key), d = d0 && mask ? pick(d0, mask) : d0; if (d) count(coll, d); return { exists: !!d, id, ref: this, data: () => (d ? clone(d) : undefined) }; },
    collection(sub) { return query(key + '/' + sub); },
    async set(data, opts) { store.set(key, merge(opts && opts.merge ? clone(store.get(key) || {}) : {}, data, !!(opts && opts.merge))); },
    async update(data) {
      const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND: ' + key);
      const next = clone(cur);
      for (const [k, v] of Object.entries(data)) { const parts = k.split('.'), last = parts.pop(); let o = next; for (const p of parts) o = plain(o[p]) ? o[p] : (o[p] = {}); if (v === SENT.del) delete o[last]; else o[last] = value(o[last], v); }
      store.set(key, next);
    },
    async delete() { store.delete(key); }
  };
}
function query(coll, filters = [], order = null, lim = 0, mask = null, after = null) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim, mask, after); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim, mask, after); },
    limit(n) { return query(coll, filters, order, n, mask, after); },
    select(...fields) { return query(coll, filters, order, lim, fields, after); },
    startAfter(v) { return query(coll, filters, order, lim, mask, v); },
    doc(id) { return docRef(coll, id || 'auto' + crypto.randomBytes(6).toString('hex')); },
    async add(data) { const r = q.doc(); await r.set(data); return r; },
    count() { return { get: async () => ({ data: () => ({ count: q.rows().length }) }) }; },   // an aggregate hands out no documents
    async get() {
      const docs = q.rows().map(({ id, v }) => { const d = mask ? Object.fromEntries(mask.filter(f => v[f] !== undefined).map(f => [f, v[f]])) : v; count(coll, d); return { id, exists: true, ref: docRef(coll, id), data: () => clone(d) }; });
      return { size: docs.length, docs, empty: !docs.length };
    },
    rows() {
      let rows =[...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), v }));
      for (const [f, op, v0] of filters) rows = rows.filter(({ v: d }) => {
        const x = norm(getPath(d, f)), v = Array.isArray(v0) ? v0.map(norm) : norm(v0);
        if (op === '==') return x === v; if (op === '!=') return x !== undefined && x !== v;
        if (op === 'in') return v.includes(x);
        if (op === 'array-contains') return Array.isArray(getPath(d, f)) && getPath(d, f).includes(v0);
        if (op === 'array-contains-any') return Array.isArray(getPath(d, f)) && getPath(d, f).some(e => v0.includes(e));
        if (x === undefined || x === null || typeof x !== typeof v) return false;   // a range never matches a missing field
        return op === '<' ? x < v : op === '<=' ? x <= v : op === '>' ? x > v : op === '>=' ? x >= v : false;
      });
      // FieldPath.documentId() orders by the document's id; startAfter starts past the value given
      const key = r => (order[0] === '__name__' ? r.id : getPath(r.v, order[0]));
      if (order) { rows = rows.filter(r => key(r) !== undefined); rows.sort((a, b) => { const x = norm(key(a)), y = norm(key(b)); const c = x > y ? 1 : x < y ? -1 : 0; return order[1] === 'desc' ? -c : c; }); }
      if (order && after != null) rows = rows.filter(r => (order[1] === 'desc' ? norm(key(r)) < norm(after) : norm(key(r)) > norm(after)));
      return lim ? rows.slice(0, lim) : rows;
    }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, d, o) { ops.push(() => ref.set(d, o)); }, update(ref, d) { ops.push(() => ref.update(d)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { for (const o of ops) await o(); } }; },
  async getAll(...refs) { const o = refs.length && typeof refs[refs.length - 1].get !== 'function' ? refs.pop() : null; return Promise.all(refs.map(r => r.get(o && o.fieldMask))); },
  async runTransaction(fn) { return fn({ get: r => r.get(), set: (r, d, o) => r.set(d, o), update: (r, d) => r.update(d), delete: r => r.delete() }); }
};
/* ── in-memory Storage (copy and move as GCS does them: metadata travels with the bytes) ── */
const blobs = new Map(), gcs = { copies: 0, moves: 0 };
const md5 = buf => crypto.createHash('md5').update(buf).digest('base64');
const notFound = p => Object.assign(new Error('No such object: ' + p), { code: 404 });
const bucket = {
  name: 'test-bucket',
  file(p) {
    const f = {
      name: p,
      async save(buf, o = {}) { buf = Buffer.from(buf); blobs.set(p, { buf, contentType: o.contentType || null, md5Hash: md5(buf), metadata: Object.assign({}, o.metadata && o.metadata.metadata) }); },
      async exists() { return [blobs.has(p)]; },
      async download() { if (!blobs.has(p)) throw notFound(p); return [blobs.get(p).buf]; },
      async delete() { if (!blobs.delete(p)) throw notFound(p); },
      async getMetadata() { const b = blobs.get(p); if (!b) throw notFound(p); return [{ contentType: b.contentType, md5Hash: b.md5Hash, size: b.buf.length, metadata: Object.assign({}, b.metadata) }]; },
      async setMetadata(m) { const b = blobs.get(p); if (!b) throw notFound(p); if (m.contentType) b.contentType = m.contentType; Object.assign(b.metadata, m.metadata || {}); },
      async copy(dest) { const b = blobs.get(p); if (!b) throw notFound(p); gcs.copies++; blobs.set(typeof dest === 'string' ? dest : dest.name, { buf: Buffer.from(b.buf), contentType: b.contentType, md5Hash: b.md5Hash, metadata: Object.assign({}, b.metadata) }); },
      async move(dest) { await f.copy(dest); gcs.copies--; gcs.moves++; blobs.delete(p); },
      async makePublic() {},
      publicUrl() { return 'https://storage.example/' + p; },
      async getSignedUrl(o) { return ['https://storage.example/' + p + '?sig=' + o.action]; }
    };
    return f;
  }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => '__name__' }, Timestamp: { fromMillis: ts } }), storage: () => ({ bucket: () => bucket }) };
let kick = { ok: true, status: 202 };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => kick;
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const orders = require(path.join(fnDir, 'firebaseOrders.js'));
const archive = require(path.join(fnDir, 'designArchive.js'));
const call = (h, body, qs) => h.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: qs || {} }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));
const post = body => call(lib, body);
const readsOf = async fn => { reads.clear(); jsonReads.n = 0; const r = await fn(); return { r, reads: new Map(reads), total: [...reads.values()].reduce((a, b) => a + b, 0), json: jsonReads.n }; };
const DAY_MS = 86400000, T0 = Date.parse('2026-09-24T12:00:00Z');
const day = i => new Date(T0 - i * DAY_MS).toISOString().slice(0, 10);
const days = n => Math.round(n / DAY_MS);

/* One set a day: three sheets, a finished run, and two parts of line archive holding its engraved words. */
function seedDay(i, extra = {}) {
  const d = day(i), runId = 'run-' + d, setId = 'set-' + d + '-1', order = String(extra.order || 5000000 + i), at = T0 - i * DAY_MS;
  store.set('Charm_Nest_Sets/' + setId, { setId, seq: 1, day: d, runId, status: 'complete', updatedAt: ts(at), materials: ['gold'], orders: { [order]: {} }, sheetIds: [1, 2, 3].map(k => `sh-${d}-${k}`) });
  for (const k of [1, 2, 3]) store.set(`Charm_Nest_Sheets/sh-${d}-${k}`, { id: `sh-${d}-${k}`, setId, setSeq: 1, runId, day: d, metal: 'gold', status: 'complete', orders: k === 1 ? [order] : [], sheetIndex: k, fileBase: `GF_${d}_Set-1_Sheet-${k}`, updatedAt: ts(at), charms: [{ sku: extra.sheetSku || 'BR-' + i }] });
  store.set('Charm_Nest_Runs/' + runId, { runId, setId, seq: 1, day: d, status: 'complete', lines: {}, lineArchive: { lines: 2, parts: 2, sheets: 3 }, orders: [], updatedAt: ts(at), createdAt: ts(at) });
  for (const p of [1, 2]) {
    const lines = { [`${order}_${p}`]: { orderId: order, sku: p === 1 ? (extra.sku || 'SKU-' + i) : 'SKU-OTHER', engrave: { text: 'word' + i } } }, json = JSON.stringify(lines);
    store.set(`Charm_Nest_Run_Lines/${runId}~p${p}`, { runId, lines: 1, bytes: json.length, json, keys: Object.keys(lines), orders: [order], at: at + p, seq: p });
  }
}

(async () => {
  /* ── F2: history reads a page, never the whole history ── */
  for (let i = 0; i < 120; i++) seedDay(i, i === 100 ? { order: 7777001 } : i === 60 || i === 3 ? { sku: 'FIND-ME-77' } : {});
  const first = await readsOf(() => post({ op: 'history', limit: 10 }));
  assert.strictEqual(first.r.status, 200, JSON.stringify(first.r.body));
  assert.strictEqual(first.r.body.sets.length, 10, 'a page of ten groups');
  assert.strictEqual(first.r.body.sets[0].day, day(0), 'newest first');
  assert.strictEqual(first.reads.get('Charm_Nest_Run_Lines') || 0, 0, 'a listing never reads the line archive: ' + JSON.stringify([...first.reads]));
  assert(first.total < 300, 'a listing reads a page worth of records, not the history: ' + first.total);
  assert(first.r.body.sets.every(g => g.sheets.length === 3), 'each set with its whole membership');
  assert.deepStrictEqual(first.r.body.next, { day: day(9), skip: 1 }, 'and says where the next page starts');
  assert(!('nextOffset' in first.r.body));
  const second = await post({ op: 'history', limit: 10, cursor: first.r.body.next });
  assert.strictEqual(second.body.sets[0].day, day(10), 'the next page carries on where the first stopped');
  assert(!second.body.sets.some(g => first.r.body.sets.some(x => x.key === g.key)), 'with no group twice');
  // a history twice as long costs a listing nothing more
  for (let i = 120; i < 300; i++) seedDay(i);
  const again = await readsOf(() => post({ op: 'history', limit: 10 }));
  assert.strictEqual(again.total, first.total, `listing cost is independent of history length: ${first.total} then ${again.total}`);
  // the Sets menu reads twenty groups the same way
  const menu = await readsOf(() => post({ op: 'history', limit: 20 }));
  assert.strictEqual(menu.r.body.sets.length, 20); assert.strictEqual(menu.reads.get('Charm_Nest_Run_Lines') || 0, 0); assert(menu.total < 400, 'menu reads ' + menu.total);
  assert.strictEqual(menu.r.body.setCount, 20, 'the counts describe what is shown');

  // an order number is found however old, from the order lists sheets and archive parts keep
  const old = await readsOf(() => post({ op: 'history', q: '7777001', today: day(0) }));
  const hit = old.r.body.sets.find(g => g.setId === 'set-' + day(100) + '-1');
  assert(hit, 'an order from 100 days ago is found by its number: ' + JSON.stringify(old.r.body.sets.map(g => g.setId)));
  assert.strictEqual(hit.sheets.length, 3, 'with its whole set');
  assert(old.json <= 2 + 2 * 30, 'the archive read is the order\'s own parts and the 30-day window, not all of it: ' + old.json);
  // a word or SKU is searched through 30 days at a time, newest first; older ones are one request further each
  const recent = await readsOf(() => post({ op: 'history', q: 'find-me-77', today: day(0) }));
  assert.deepStrictEqual(recent.r.body.sets.map(g => g.day), [day(3)], 'the recent match only');
  assert.deepStrictEqual(recent.r.body.window, { from: day(29), to: null });
  assert(recent.json <= 2 * 30, 'line archive of the window only: ' + recent.json);
  assert(recent.r.body.next, 'and a way to search older records');
  let cursor = recent.r.body.next, pages = 0, found = null;
  while (cursor && !found && pages < 10) { const r = await post({ op: 'history', q: 'find-me-77', today: day(0), cursor }); pages++; found = r.body.sets.find(g => g.day === day(60)); cursor = r.body.next; }
  assert(found && pages === 2, 'the 60-day-old match is two windows further: ' + pages);
  const wide = await post({ op: 'history', q: 'find-me-77', today: day(0), days: 90 });
  assert.deepStrictEqual(wide.body.sets.map(g => g.day), [day(3), day(60)], 'or one wider window');
  const none = await post({ op: 'history', q: 'nothing-like-this', today: day(0) });
  assert.strictEqual(none.body.sets.length, 0);

  /* ── F8: the Library's set list reads the newest saved sets only ── */
  const sets = await readsOf(() => post({ op: 'setList', limit: 5 }));
  assert.strictEqual(sets.r.body.sets.length, 5); assert(sets.reads.get('Charm_Nest_Sets') <= 15, 'setList reads ' + sets.reads.get('Charm_Nest_Sets'));
  const since = await readsOf(() => post({ op: 'setList', from: day(2), to: day(0) }));
  assert.deepStrictEqual(since.r.body.sets.map(s => s.day).sort(), [day(2), day(1), day(0)].sort()); assert(since.reads.get('Charm_Nest_Sets') <= 10, 'bounded by date too');

  /* ── F11/F16: every append-only row carries its expiry ── */
  const now = Date.now();
  // what has outlived its expiry is also removed by the code itself (no TTL policy needed): seeded here, checked below
  store.set('Design_Bridge/session-old', { sessionId: 'session-old', updatedAt: ts(now - 31 * 864e5) });
  for (const k of ['a', 'b']) store.set('Design_Bridge/session-old/log/' + k, { t: now - 31 * 864e5, dir: 'evt' });
  store.set('Design_Bridge/session-abc/log/expired', { t: now - 40 * 864e5, dir: 'evt', expireAt: new Date(now - 864e5) });
  store.set('Charm_Nest_Arrivals/old-181', { id: 'old-181', firstSeenAt: now - 181 * 864e5 });
  store.set('Charm_Nest_Arrivals/keep-179', { id: 'keep-179', firstSeenAt: now - 179 * 864e5 });
  store.set('Design_RealTime_Selected_Orders/tomb-31', { selected: false, at: ts(now - 31 * 864e5) });
  store.set('Design_RealTime_Selected_Orders/tomb-5', { selected: false, at: ts(now - 5 * 864e5) });
  store.set('Design_RealTime_Selected_Orders/held-31', { selected: true, selectedBy: 'c9', at: ts(now - 31 * 864e5) });
  store.set('Design_RealTime_Selected_Orders/held-61', { selected: true, selectedBy: 'c9', at: ts(now - 61 * 864e5) });
  await post({ op: 'bridgeLog', session: 'session-abc', meta: { page: 'x' }, rows: [{ t: 1, dir: 'cmd', type: 'go' }, { t: 2, dir: 'evt', type: 'ok' }] });
  const logRows = [...store.entries()].filter(([k]) => k.startsWith('Design_Bridge/session-abc/log/'));
  assert.strictEqual(logRows.length, 2, 'the session keeps its new rows, and its expired one went');
  assert(!store.has('Design_Bridge/session-old') && !store.has('Design_Bridge/session-old/log/a') && !store.has('Design_Bridge/session-old/log/b'), 'a session not written for 30 days went with its log');
  for (const d of [store.get('Design_Bridge/session-abc'), ...logRows.map(([, v]) => v)]) assert(d.expireAt instanceof Date && Math.abs(days(d.expireAt - now) - 30) <= 1, 'log rows expire in 30 days');
  await post({ op: 'bridgeLog', sandbox: true, session: 'session-abc', rows: [{ t: 1, dir: 'cmd', type: 'go' }] });
  assert.strictEqual(days(store.get('Sandbox_Design_Bridge/session-abc').expireAt - now), 3, 'sandbox log rows in 3');
  await post({ op: 'arrivalRecord', orders: [{ id: '9001', createTs: 1 }] });
  assert.strictEqual(days(store.get('Charm_Nest_Arrivals/9001').expireAt - now), 180, 'an arrival is kept 180 days');
  assert(!store.has('Charm_Nest_Arrivals/old-181') && store.has('Charm_Nest_Arrivals/keep-179'), 'an arrival first seen over 180 days ago goes');
  await post({ op: 'arrivalRecord', sandbox: true, orders: [{ id: '9001', createTs: 1 }] });
  assert.strictEqual(days(store.get('Sandbox_Charm_Nest_Arrivals/9001').expireAt - now), 3);
  const rt = body => call(orders, body);
  await rt({ rtLockIds: ['4001', '4002'], clientId: 'c1' }); await rt({ rtClaimIds: ['4002'], claimRun: 'run-x' });
  assert(!('expireAt' in store.get('Design_RealTime_Selected_Orders/4001')), 'a live lock never expires');
  await rt({ rtUnlockIds: ['4001', '4002'] });
  assert.strictEqual(days(store.get('Design_RealTime_Selected_Orders/4001').expireAt - now), 30, 'an unlock tombstone expires in 30 days');
  assert(!('expireAt' in store.get('Design_RealTime_Selected_Orders/4002')), 'but not while the order is still claimed');
  assert(!store.has('Design_RealTime_Selected_Orders/tomb-31') && store.has('Design_RealTime_Selected_Orders/tomb-5'), 'a tombstone a month old goes');
  assert(store.has('Design_RealTime_Selected_Orders/held-31') && !store.has('Design_RealTime_Selected_Orders/held-61'), 'a held lock goes only past 60 days');
  await rt({ rtUnclaimIds: ['4002'] });
  assert.strictEqual(days(store.get('Design_RealTime_Selected_Orders/4002').expireAt - now), 30, 'nor after it is released');
  await rt({ rtLockIds: ['4001'], clientId: 'c2' });
  assert(!('expireAt' in store.get('Design_RealTime_Selected_Orders/4001')), 'a lock taken again clears the expiry');
  const both = await rt({ rtLockIds: ['4003'], rtUnlockIds: ['4001'], clientId: 'c2' });
  const bothBody = both.body;
  assert(store.get('Design_RealTime_Selected_Orders/4003').selected === true && store.get('Design_RealTime_Selected_Orders/4001').selected === false, 'locks and unlocks sent together are both written');
  assert(bothBody.locked === 1 && bothBody.unlocked === 1 && store.get('Design_RealTime_Selected_Orders/4001').expireAt instanceof Date, 'the unlock is a tombstone with its expiry');

  /* ── F12/F18: a new approval archives the old one's files; the sheet keeps a short copy ── */
  const pid = '5000001_7000001_1', folder = `charmnest/sheets/${day(0)}/GF_working_sh-back`;
  store.set('Charm_Nest_Sheets/sh-back', { id: 'sh-back', day: day(0), metal: 'gold', poolIds: [pid], backPool: [], draft: true });
  const v1 = { ai: { path: `${folder}/back/b_${pid}_1000.ai`, url: 'u1' }, png: { path: `${folder}/back/b_${pid}_1000.png`, url: 'u2' } };
  store.set('Charm_Pool_Back/' + pid, { poolId: pid, sheetId: 'sh-back', approvedAt: 1000, approvedBy: 'Paul', outputs: v1 });
  for (const f of Object.values(v1)) await bucket.file(f.path).save(Buffer.from('approved ' + f.path));
  const v2 = { ai: { path: `${folder}/back/b_${pid}_2000.ai`, url: 'u3' }, png: { path: `${folder}/back/b_${pid}_2000.png`, url: 'u4' } };
  const big = { poolId: pid, sheetId: 'sh-back', approvedAt: 2000, approvedBy: 'Paul', text: 'MAEVE', sizePt: 7, outputs: v2, verified: { geometry: { ok: true, gapMm: 0.4 }, file: { ok: true, why: null } }, review: { note: 'r'.repeat(4000) }, reference: 'data:image/png;base64,' + 'A'.repeat(30000), flipChecks: [{ ok: true }], metrics: { fill: 0.5 } };
  let r = await post({ op: 'backPut', back: big });
  assert.strictEqual(r.status, 200, JSON.stringify(r.body)); assert.strictEqual(r.body.superseded, 2);
  for (const f of Object.values(v1)) { assert(!blobs.has(f.path), 'the superseded file left its place'); assert(blobs.has(f.path.replace('charmnest/', 'charmnest/superseded/')), 'and is kept in the archive'); }
  assert.deepStrictEqual(store.get('Charm_Pool_Back/' + pid).superseded[0].files.map(f => f.to), Object.values(v1).map(f => f.path.replace('charmnest/', 'charmnest/superseded/')), 'the record says where each went');
  const copy = store.get('Charm_Nest_Sheets/sh-back').backPool[0];
  assert(!copy.review && !copy.reference && !copy.flipChecks && !copy.metrics, 'the sheet keeps a short copy: ' + Object.keys(copy));
  assert(copy.verified.geometry.ok && copy.verified.file.ok && copy.outputs.ai.path === v2.ai.path && copy.approvedBy === 'Paul' && copy.sizePt === 7, 'with what readiness and the manifest read');
  r = await post({ op: 'getSheet', id: 'sh-back' });
  assert.strictEqual(r.body.sheet.backPool[0].review.note.length, 4000, 'getSheet puts the whole record back');
  assert.strictEqual(r.body.sheet.laser.saved, 1, 'and readiness still counts the saved back');
  r = await post({ op: 'backPut', back: big });
  assert.strictEqual(r.body.superseded, 0, 'saving the same approval again moves nothing');
  // a sheet record past 900 KB is refused in words, before Firestore refuses it
  r = await post({ op: 'putSheet', sheet: { id: 'sh-huge', metal: 'gold', charms: Array.from({ length: 3200 }, (_, i) => ({ id: 'c' + i, name: 'n'.repeat(300) })) } });
  assert.strictEqual(r.status, 413); assert(/900 KB/.test(r.body.error), r.body.error); assert(!store.has('Charm_Nest_Sheets/sh-huge'));

  /* ── F12: a repacked sheet's own files go; a released one's stay ── */
  const outs = base => ({ ai: { path: base + '.ai' }, labelled: { path: base + '_labelled.pdf' }, report: { path: base + '_nest-report.json' }, preview: { path: base.replace(/[^/]+$/, 'preview.png') } });
  const files = o => Object.values(o).map(x => x.path).concat([o.ai.path.replace(/\.ai$/, '.pdf')]);
  store.set('Charm_Nest_Runs/run-open', { runId: 'run-open', day: day(0), status: 'running' });
  const repacked = outs(`charmnest/sheets/${day(0)}/GF_working_sh-repack/GF_working_sh-repack`), kept = outs(`charmnest/sets/${day(0)}/Set-2/GF_Set-2_Sheet-1/GF_Set-2_Sheet-1`);
  store.set('Charm_Nest_Sheets/sh-repack', { id: 'sh-repack', runId: 'run-open', day: day(0), outputs: repacked });
  store.set('Charm_Nest_Sets/set-cut', { setId: 'set-cut', committedAt: ts(now), day: day(0) });
  store.set('Charm_Nest_Sheets/sh-released', { id: 'sh-released', runId: 'run-open', setId: 'set-cut', day: day(0), outputs: kept });
  for (const p of files(repacked).concat(files(kept))) await bucket.file(p).save(Buffer.from(p));
  r = await post({ op: 'archiveEmptySheet', id: 'sh-repack', runId: 'run-open' });
  assert(r.body.ok && r.body.deletedFiles === 5, JSON.stringify(r.body)); assert(files(repacked).every(p => !blobs.has(p)), 'its five files and the .pdf beside its .ai are gone');
  assert(store.get('Charm_Nest_Sheets/sh-repack').archived);
  r = await post({ op: 'archiveEmptySheet', id: 'sh-released', runId: 'run-open' });
  assert(r.body.ok && files(kept).every(p => blobs.has(p)), 'a sheet of a committed set keeps its files');
  // a deleted sheet's back files stay while a back record names them; the rest are archived, never deleted
  const pidA = '5000002_7000002_1', pidB = '5000003_7000003_1', dir = `charmnest/sets/${day(0)}/Set-3/S/back`;
  const bA = { ai: { path: dir + '/a.ai' }, png: { path: dir + '/a.png' } }, bB = { ai: { path: dir + '/b_old.ai' }, png: { path: dir + '/b_old.png' } };
  store.set('Charm_Pool_Back/' + pidA, { poolId: pidA, approvedAt: 5, outputs: bA });
  store.set('Charm_Pool_Back/' + pidB, { poolId: pidB, approvedAt: 9, outputs: { ai: { path: dir + '/b_new.ai' }, png: { path: dir + '/b_new.png' } } });
  const gone = outs(`charmnest/sets/${day(0)}/Set-3/S/S`);
  store.set('Charm_Nest_Sheets/sh-gone', { id: 'sh-gone', day: day(0), outputs: gone, backPool: [{ poolId: pidA, approvedAt: 5, outputs: bA }, { poolId: pidB, approvedAt: 7, outputs: bB }] });
  for (const p of files(gone).concat(Object.values(bA).map(x => x.path), Object.values(bB).map(x => x.path))) await bucket.file(p).save(Buffer.from(p));
  r = await post({ op: 'deleteSheet', id: 'sh-gone', code: process.env.CHARM_NEST_DELETE_CODE || '975311' });
  assert(r.body.deleted && files(gone).every(p => !blobs.has(p)), 'the sheet\'s own files are deleted');
  assert(Object.values(bA).every(x => blobs.has(x.path)), 'a back its record still names stays in place');
  assert(Object.values(bB).every(x => !blobs.has(x.path) && blobs.has(x.path.replace('charmnest/', 'charmnest/superseded/'))), 'an older approval of a back is archived');

  /* ── F13: the .pdf is a copy made in the bucket, once per .ai ── */
  const fin = outs(`charmnest/sets/${day(0)}/Set-4/GF_Set-4_Sheet-1/GF_Set-4_Sheet-1`);
  store.set('Charm_Nest_Sheets/sh-final', { id: 'sh-final', day: day(0), outputs: Object.assign({ pdf: null }, fin) });
  await bucket.file(fin.ai.path).save(Buffer.from('%PDF-1.7 illustrator'), { contentType: 'application/illustrator', metadata: { metadata: { firebaseStorageDownloadTokens: 'ai-token' } } });
  gcs.copies = 0;
  r = await post({ op: 'sheetPdf', ids: ['sh-final', 'no-such-sheet'] });
  const pdfPath = fin.ai.path.replace(/\.ai$/, '.pdf'), pdf = blobs.get(pdfPath);
  assert(pdf && pdf.contentType === 'application/pdf' && pdf.buf.equals(blobs.get(fin.ai.path).buf), 'the .pdf is the .ai, typed as a PDF');
  assert(pdf.metadata.firebaseStorageDownloadTokens && pdf.metadata.firebaseStorageDownloadTokens !== 'ai-token', 'with its own download token');
  assert.strictEqual(store.get('Charm_Nest_Sheets/sh-final').outputs.pdf.path, pdfPath, 'and the record says where it is'); assert(store.get('Charm_Nest_Sheets/sh-final').outputs.ai.path === fin.ai.path, 'next to the other outputs');
  assert.strictEqual(r.body.urls['sh-final'], store.get('Charm_Nest_Sheets/sh-final').outputs.pdf.url);
  await post({ op: 'sheetPdf', id: 'sh-final' });
  assert.strictEqual(gcs.copies, 1, 'an unchanged .ai is not copied again');

  /* ── F14: a job that never started leaves no parked payload ── */
  kick = { ok: false, status: 500, statusText: 'down' };
  r = await post({ op: 'startAgent', mode: 'grouping', payload: { big: 'x'.repeat(1000) } });
  assert.strictEqual(r.status, 400); assert(![...blobs.keys()].some(k => k.startsWith('charmnest/agent/')), 'the payload is deleted when the kick fails');
  kick = { ok: true, status: 202 };

  /* ── F9: one mirrored copy per listing photo, not per order line ── */
  const fetched = [];
  global.fetch = async url => { fetched.push(url); return { ok: true, headers: { get: () => 'image/jpeg' }, arrayBuffer: async () => Buffer.from('jpeg ' + url) }; };
  r = await call(archive, { op: 'put', orders: [{ receiptId: '8001', items: [{ transactionId: 't1', imageUrl: 'https://i.etsystatic.com/a.jpg' }] }, { receiptId: '8002', items: [{ transactionId: 't2', imageUrl: 'https://i.etsystatic.com/a.jpg' }] }] });
  assert.strictEqual(r.status, 200, JSON.stringify(r.body));
  const mirrors = [...blobs.keys()].filter(k => k.startsWith('design-archive/'));
  assert.deepStrictEqual(mirrors, ['design-archive/listing/' + crypto.createHash('sha1').update('https://i.etsystatic.com/a.jpg').digest('hex') + '.jpg'], 'one file per picture: ' + mirrors);
  assert.strictEqual(fetched.length, 1, 'fetched once');

  /* ── the SKU index past 3,000 comes back whole, in parts; its signature says when a reload can be skipped ── */
  for (let i = 0; i < 3500; i++) { const sku = 'IX-' + String(i).padStart(5, '0'); store.set('Charm_Master_Index/' + sku, { sku, masterHash: 'm1', masterName: 'M.ai', indexedAt: ts(T0 + i), widthPt: 10, heightPt: 10 }); }
  store.set('Charm_Master_Index/SHELL-ONLY', { updatedAt: ts(1) });   // a patch on a SKU never indexed: not an entry
  let ix = await post({ op: 'masterList', limit: 3000 }), skus = ix.body.entries.map(e => e.sku), ixParts = 1;
  const sig = ix.body.index;
  assert(sig && sig.count === 3501 && sig.indexedAt === T0 + 3499, 'the first part says the index signature: ' + JSON.stringify(sig));
  assert.strictEqual(ix.body.entries.length, 3000); assert(ix.body.next, 'and where the rest starts');
  while (ix.body.next) { ix = await post({ op: 'masterList', limit: 3000, cursor: ix.body.next }); skus.push(...ix.body.entries.map(e => e.sku)); ixParts++; assert(!ix.body.index && !ix.body.truncated); }
  assert.strictEqual(skus.length, 3500, 'all 3,500 entries come back: ' + skus.length); assert.strictEqual(new Set(skus).size, 3500, 'each once'); assert.strictEqual(ixParts, 2);
  assert.deepStrictEqual((await post({ op: 'masterListFiles' })).body.index, sig, 'the files list says the same signature while nothing changed');
  await post({ op: 'masterPatch', sku: 'IX-00007', patch: { engravable: false } });
  assert.notDeepStrictEqual((await post({ op: 'masterListFiles' })).body.index, sig, 'an edit changes it');
  store.delete('Charm_Master_Index/IX-00008');
  assert.strictEqual((await post({ op: 'masterListFiles' })).body.index.count, 3500, 'and so does a removal');

  /* ── a set list with its sheets comes in parts under the answer cap, each sheet read for its list fields only ── */
  const bigBacks = (sid, n) => Array.from({ length: n }, (_, k) => ({ poolId: `${8000000 + k}_${9000000 + k}_1`, sheetId: sid, approvedAt: 1000 + k, approvedBy: 'Paul', text: 'x'.repeat(200), outputs: { ai: { path: 'charmnest/a.ai', url: 'https://u/a.ai' }, png: { path: 'charmnest/a.png', url: 'https://u/a.png' } }, verified: { geometry: { ok: true }, file: { ok: true } } }));
  const bigSheetIds = [], bigDay = day(-1);   // the newest day on record: these forty are the list's newest sets
  for (let s = 0; s < 40; s++) {
    const setId = 'set-big-' + s, ids = [1, 2, 3].map(k => `sh-big-${s}-${k}`);
    store.set('Charm_Nest_Sets/' + setId, { setId, seq: s + 1, day: bigDay, runId: 'run-big', status: 'open', updatedAt: ts(T0 + 1000 + s), sheetIds: ids, orders: {} });
    for (const id of ids) { bigSheetIds.push(id); store.set('Charm_Nest_Sheets/' + id, { id, setId, runId: 'run-big', day: bigDay, metal: 'gold', status: 'complete', placedCount: 150, poolIds: [], backPool: bigBacks(id, 150), charms: [{ outline: 'c'.repeat(50000) }], updatedAt: ts(T0 + 1000 + s) }); }
  }
  // a record from before placedCount: readiness counts its placements, read for it alone
  Object.assign(store.get('Charm_Nest_Sheets/sh-big-0-1'), { placedCount: 0, placements: Array.from({ length: 150 }, (_, k) => ({ id: 'p' + k })) });
  sheetCharms.n = 0;
  let sl = await post({ op: 'setList', includeSheets: true, limit: 40 }), slParts = 1, maxBytes = 0;
  const gotSets = [...sl.body.sets], gotSheets = [...sl.body.sheets];
  for (;;) {
    maxBytes = Math.max(maxBytes, Buffer.byteLength(JSON.stringify(sl.body)));
    if (!sl.body.next) break;
    assert.strictEqual(sl.body.truncated, true, 'a part that is not the last says so');
    sl = await post({ op: 'setList', includeSheets: true, limit: 40, cursor: sl.body.next }); slParts++; gotSets.push(...sl.body.sets); gotSheets.push(...sl.body.sheets);
  }
  assert.deepStrictEqual(gotSets.map(s => s.setId).sort(), Array.from({ length: 40 }, (_, s) => 'set-big-' + s).sort(), 'every set of the list, once');
  assert.deepStrictEqual(gotSheets.map(s => s.id).sort(), bigSheetIds.slice().sort(), 'and every sheet of theirs, once');
  assert(slParts >= 3 && maxBytes < 4.5e6, `the list (about ${Math.round(gotSheets.length * Buffer.byteLength(JSON.stringify(gotSheets[0])) / 1e6)} MB) came in ${slParts} parts of at most ${(maxBytes / 1e6).toFixed(2)} MB`);
  assert(gotSheets.every(s => s.backs.length === 150 && s.laser && s.laser.total === 150), 'each with its backs and its laser readiness');
  assert.strictEqual(sheetCharms.n, 0, 'no sheet was read with its charms');
  // the Library's sheet list of a run the same way
  let ls = await post({ op: 'listSheets', runId: 'run-big', limit: 500 }), lsSheets = [...ls.body.sheets];
  while (ls.body.next) { ls = await post({ op: 'listSheets', cursor: ls.body.next }); lsSheets.push(...ls.body.sheets); }
  assert.deepStrictEqual(lsSheets.map(s => s.id).sort(), bigSheetIds.slice().sort(), 'listSheets comes back whole, in parts');
  assert.strictEqual(sheetCharms.n, 0);
  // the history sends each sheet once, in its group, and a page under the cap
  const hs = await post({ op: 'history', limit: 40 });
  assert(!('sheets' in hs.body), 'no second list of the sheets'); assert(Buffer.byteLength(JSON.stringify(hs.body)) < 4.5e6 && hs.body.truncated.size === true && hs.body.next, 'a page of groups that would pass the cap stops short and says where to go on');

  console.log(`server-history-bounds OK · listing ${first.total} reads (0 line parts) at 120 and 300 days · order found 100 days back · search window 30 days · expiries, archive moves, slim backs, 900 KB guard, .pdf copy, payload cleanup, listing mirrors · 3,500 SKUs in ${ixParts} parts · set list in ${slParts} parts of ≤ ${(maxBytes / 1e6).toFixed(1)} MB`);
})().catch(e => { console.error(e); process.exit(1); });
