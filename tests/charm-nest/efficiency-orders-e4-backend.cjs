// A loopback backend for the order list's contract check (tests/charm-nest/efficiency-orders.cjs, section 6): E4's REAL employeeEfficiency
// handler (the personOrders op, netlify/functions/_employeeProfile.js) over an in-memory Firestore filled with E4's INVENTED shop
// (tests/charm-nest/employee-profile-seed.cjs), a fake clock and a fake passcode. No network, no Firestore, no real names. It runs in its own
// process so that its clock and module hooks never touch the browser test.
//   FAKE_PASS=<passcode> node tests/charm-nest/efficiency-orders-e4-backend.cjs      → prints "PORT <n>", then answers POST / with the real handler's JSON
'use strict';
const path = require('path'), Module = require('module'), http = require('http');
const root = path.join(__dirname, '../..');
const S = require('./employee-profile-seed.cjs');

/* A small in-memory Firestore (where / orderBy / limit, Timestamp), the same idea as tests/stations/employee-profile.cjs. */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } static fromMillis(m) { return new Ts(m); } }
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
function fakeStore() {
  const colls = new Map(), reads = [];
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : v && typeof v === 'object' ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
  function query(name, filters, order, lim) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim),
      limit: n => query(name, filters, order, n),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        reads.push(Math.max(1, docs.length));
        return { docs: docs.map(({ id, d }) => ({ id, data: () => keep(d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const db = { collection: name => Object.assign(query(name, [], null, null), {
    doc: id => ({ id,
      get: async () => { reads.push(1); const d = data(name).get(id); return { exists: !!d, data: () => keep(d) }; },
      set: async () => { throw new Error('this backend never writes'); },
      create: async () => { throw new Error('this backend never writes'); } }) }) };
  return { db, put: (name, id, d) => data(name).set(id, keep(d)), reads };
}

const quiet = () => {};
const realLog = { log: console.log, warn: console.warn, error: console.error, info: console.info };
console.log = console.warn = console.error = console.info = quiet;
process.env.EDIT_PASSCODE = process.env.FAKE_PASS || 'fixture-pass-123';
Date.now = () => S.NOW;

const realLoad = Module._load, fakeAdmin = { firestore: Object.assign(() => ({}), { Timestamp: Ts, FieldValue: { serverTimestamp: () => 'ts' } }) };
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const fn = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
Module._load = realLoad;
require(path.join(root, 'netlify/functions/_editPasscode.js')).resetCache();

const st = fakeStore();
S.seed((c, id, d) => st.put(c, id, d));
let ip = 0;
http.createServer((req, res) => {
  let b = ''; req.on('data', c => { b += c; });
  req.on('end', async () => {
    try {
      const r = await fn._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ip % 250) }, body: b }, st.db);
      res.writeHead(r.statusCode, { 'content-type': 'application/json' }); res.end(r.body || '{}');
    } catch (e) { res.writeHead(500, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: false, error: String(e && e.message || e) })); }
  });
}).listen(0, '127.0.0.1', function () { process.stdout.write('PORT ' + this.address().port + '\n'); });
