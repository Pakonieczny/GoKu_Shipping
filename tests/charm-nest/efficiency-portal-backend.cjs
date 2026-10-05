// The loopback "whole shop" behind the Employee efficiency end-to-end test (tests/charm-nest/efficiency-portal-e2e.cjs): the REAL server code over
// ONE in-memory Firestore, in its own process (so its fake clock never touches the browser test):
//   · netlify/functions/employeeEfficiency.js  (ops overview, live, person, personOrders, orders ...) behind a FAKE manager passcode
//   · netlify/functions/firebaseOrders.js      (the open station door: {session}, {activity}, {live}) that the real station-session.js and
//                                              station-activity.js write to from the fake station pages below
//   · the invented shop of employee-profile-seed.cjs (14 weeks, six people, days off, issues, receipts) and, unless PORTAL_EMPTY=1, nothing else
//   · fake station pages (/weld-1.html, /assembly-2.html ...) that load the real client libraries and nothing of Etsy
// The clock starts at the seed's "now" (Mon 5 Oct 2026, 15:00 New York) and then runs on; POST /ctl/skew moves it. No network, no real
// Firestore, no real name, PIN, passcode or customer. Nothing leaves the loopback.
//   PORTAL_PASS=<fake passcode> [PORTAL_EMPTY=1] node tests/charm-nest/efficiency-portal-backend.cjs  → prints "PORT <n>"
'use strict';
const path = require('path'), Module = require('module'), http = require('http'), fs = require('fs');
const root = path.join(__dirname, '../..');
const S = require('./employee-profile-seed.cjs');

/* ── an in-memory Firestore: where / orderBy / limit / select, doc get/set/update/create/delete, getAll, transactions, nested merges, increments ── */
class Ts { constructor(m) { this.m = m; } toMillis() { return this.m; } toDate() { return new Date(this.m); } static fromMillis(m) { return new Ts(m); } static now() { return new Ts(Date.now()); } }
const INC = n => ({ __inc: n }), SRV = { __srv: 1 }, DEL = { __del: 1 };
const kind = v => v instanceof Ts ? 'ts' : typeof v === 'number' ? 'num' : typeof v === 'string' ? 'str' : 'other';
const val = v => v instanceof Ts ? v.m : v;
const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Ts) && v.__inc == null && !v.__srv && !v.__del;
const keep = v => v instanceof Ts ? v : Array.isArray(v) ? v.map(keep) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, keep(x)])) : v;
function apply(prev, data, merge) {
  const out = merge && prev ? keep(prev) : {};
  for (const [k, v] of Object.entries(data)) {
    if (v === undefined) continue;
    if (v && v.__inc != null) out[k] = (Number(out[k]) || 0) + v.__inc;
    else if (v && v.__srv) out[k] = new Ts(Date.now());
    else if (v && v.__del) delete out[k];
    else if (plain(v)) out[k] = apply(merge && plain(out[k]) ? out[k] : null, v, merge);
    else out[k] = keep(v);
  }
  return out;
}
function fakeStore() {
  const colls = new Map(), st = { reads: 0, queries: 0, writes: 0, by: {} };
  const data = n => { if (!colls.has(n)) colls.set(n, new Map()); return colls.get(n); };
  const countRead = (name, n) => { st.reads += Math.max(1, n); st.by[name] = (st.by[name] || 0) + Math.max(1, n); };
  function query(name, filters, order, lim, sel) {
    return {
      where: (f, op, v) => query(name, filters.concat([[f, op, v]]), order, lim, sel),
      orderBy: (f, d) => query(name, filters, [f, d || 'asc'], lim, sel),
      limit: n => query(name, filters, order, n, sel),
      select: (...f) => query(name, filters, order, lim, f),
      get: async () => {
        let docs = [...data(name)].map(([id, d]) => ({ id, d }));
        for (const [f, op, v] of filters) docs = docs.filter(({ d }) => {
          const x = d[f]; if (x === undefined || kind(x) !== kind(v)) return false;
          const a = val(x), b = val(v);
          return op === '==' ? a === b : op === '>=' ? a >= b : op === '>' ? a > b : op === '<' ? a < b : op === '<=' ? a <= b : false;
        });
        if (order) { const [f, dir] = order; docs = docs.filter(({ d }) => d[f] !== undefined).sort((p, q) => (val(p.d[f]) < val(q.d[f]) ? -1 : val(p.d[f]) > val(q.d[f]) ? 1 : 0) * (dir === 'desc' ? -1 : 1)); }
        if (lim != null) docs = docs.slice(0, lim);
        st.queries++; countRead(name, docs.length);
        return { docs: docs.map(({ id, d }) => ({ id, exists: true, data: () => keep(sel ? Object.fromEntries(Object.entries(d).filter(([k]) => sel.includes(k))) : d) })), size: docs.length, empty: !docs.length };
      }
    };
  }
  const ref = (name, id) => ({ id, path: name + '/' + id, name,
    get: async () => { st.queries++; countRead(name, 1); const d = data(name).get(id); return { exists: !!d, id, data: () => keep(d) }; },
    set: async (v, o) => { st.writes++; data(name).set(id, apply(data(name).get(id), v, !!(o && o.merge))); },
    create: async v => { st.writes++; if (data(name).has(id)) throw Object.assign(new Error('6 ALREADY_EXISTS'), { code: 6 }); data(name).set(id, apply(null, v, false)); },
    update: async v => { st.writes++; if (!data(name).has(id)) throw Object.assign(new Error('5 NOT_FOUND'), { code: 5 }); data(name).set(id, apply(data(name).get(id), v, true)); },
    delete: async () => { st.writes++; data(name).delete(id); } });
  const db = {
    collection: name => Object.assign(query(name, [], null, null, null), { doc: id => ref(name, id) }),
    getAll: async (...a) => Promise.all(a.filter(x => x && x.get).map(r => r.get())),
    runTransaction: async fn => {
      const w = [];
      const tx = { get: r => r.get(), getAll: (...rs) => Promise.all(rs.filter(x => x && x.get).map(r => r.get())), set: (r, d, o) => { w.push(['set', r, d, o]); return tx; }, create: (r, d) => { w.push(['create', r, d]); return tx; }, update: (r, d) => { w.push(['update', r, d]); return tx; }, delete: r => { w.push(['delete', r]); return tx; } };
      const out = await fn(tx);
      for (const [k, r, d, o] of w) await r[k](d, o);
      return out;
    }
  };
  return { db, st, put: (name, id, d) => { if (name === 'Station_Activity' && typeof d.ts === 'number') d = Object.assign({}, d, { ts: new Ts(d.ts) }); data(name).set(id, keep(d)); }, data, get: (name, id) => data(name).get(id), all: name => [...data(name)].map(([id, d]) => Object.assign({ _id: id }, d)) };
}

/* ── the clock: the seed's "now", then real time, plus whatever a test skewed ── */
const realNow = Date.now.bind(Date), T0 = realNow(); let skew = 0;
Date.now = () => S.NOW + (realNow() - T0) + skew;
const realLog = { error: console.error };
console.log = console.warn = console.info = () => {}; console.error = () => {};
process.env.EDIT_PASSCODE = process.env.PORTAL_PASS || 'fixture-pass-123';

const store = fakeStore();
const fakeAdmin = { firestore: Object.assign(() => store.db, { Timestamp: Ts, FieldValue: { serverTimestamp: () => SRV, increment: INC, delete: () => DEL, arrayUnion: (...a) => a, arrayRemove: () => [] }, FieldPath: { documentId: () => '__name__' } }) };
const realLoad = Module._load;
Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin; return realLoad.call(this, req, ...rest); };
const eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js'));
const door = require(path.join(root, 'netlify/functions/firebaseOrders.js'));
Module._load = realLoad;
require(path.join(root, 'netlify/functions/_editPasscode.js')).resetCache();

const EMPTY = process.env.PORTAL_EMPTY === '1';
let PORT = 0;
if (!EMPTY) {
  S.seed((c, id, d) => store.put(c, id, d));
  // what the app already stores for the pictures (the live board reads these and never asks Etsy): the vector design per SKU and the listing photo per
  // listing. The server accepts https addresses only, so they are https; the test's browser answers them itself (they never leave the machine).
  ['CH-MOON-GF', 'CH-STAR-SS', 'ST-PEARL-GF', 'CH-HEART-RG', 'ST-BEE-SS', 'CH-LEAF-GF'].forEach(sku => store.put('Charm_Master_Index', sku, { sku, thumbUrl: `https://thumbs.test/vec/${sku}.svg` }));
  for (let i = 1; i <= 6; i++) store.put('Etsy_Listing_Image_Cache', String(1912340000 + i), { images: [{ rank: 1, url_570xN: `https://i.etsystatic.com/fake/il_570xN.${1912340000 + i}_abc.jpg` }] });
}

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.svg': 'image/svg+xml', '.json': 'application/json' };
const PAGES = {   // the fake station pages: the real libraries, a sign-in done by the test, nothing else
  '/weld-1.html': { station: 'welding', device: 'weld-1' }, '/assembly-2.html': { station: 'assembly', device: 'assembly-2' }, '/shipping-1.html': { station: 'shipping', device: 'shipping-1' },
  '/sorting.html': { station: 'sorting', device: 'sorting-1' }, '/design-message.html': { station: 'design', device: 'design-message' }, '/etsy-mail-1.html': { station: 'inbox', device: 'etsy-mail-1' }
};
const pageHtml = (p, sandbox) => `<!doctype html><html><head><meta charset="utf-8"><title>${p.device}</title></head><body><h1>${p.device}</h1>
<script src="/station-session.js"></script><script src="/station-activity.js"></script><script src="/station-live-order.js"></script>
<script>window.__who = null;
StationSession.init({ station: ${JSON.stringify(p.station)}, device: ${JSON.stringify(p.device)}, sandbox: ${sandbox ? 'true' : 'false'}, person: function () { return window.__who; }, signOut: function () { window.__who = null; } });
window.__signIn = function (name) { window.__who = { name: name, id: '' }; StationSession.signedIn({ name: name, id: '' }); };
window.__signOut = function () { try { StationActivity.idle(); } catch (e) {} StationSession.signedOut('signOut'); window.__who = null; };
</script></body></html>`;

let ip = 0;
const readBody = req => new Promise(r => { const c = []; req.on('data', d => c.push(d)); req.on('end', () => r(Buffer.concat(c).toString('utf8'))); });
const json = (res, code, body, extra) => { res.writeHead(code, Object.assign({ 'content-type': 'application/json', 'access-control-allow-origin': '*', 'cache-control': 'no-store' }, extra || {})); res.end(JSON.stringify(body)); };
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  try {
    if (u.pathname === '/fn/employeeEfficiency' || u.pathname === '/fn/firebaseOrders') {
      const b = req.method === 'POST' ? await readBody(req) : '';
      const q = Object.fromEntries(u.searchParams.entries());
      const headers = Object.assign({ 'x-nf-client-connection-ip': req.headers['x-test-ip'] || '203.0.113.' + (++ip % 250) }, req.headers['x-test-ip'] ? {} : {});
      let out;
      if (u.pathname.endsWith('employeeEfficiency')) out = await eff._t.handle({ httpMethod: req.method, headers, queryStringParameters: q, body: b }, store.db);
      else out = await door.handler({ httpMethod: req.method, headers, queryStringParameters: q, body: b });
      res.writeHead(out.statusCode || 200, Object.assign({ 'content-type': 'application/json', 'access-control-allow-origin': '*', 'access-control-allow-headers': '*' }, out.headers || {})); return res.end(out.body || '');
    }
    if (req.method === 'OPTIONS') { res.writeHead(204, { 'access-control-allow-origin': '*', 'access-control-allow-headers': '*', 'access-control-allow-methods': 'GET,POST,OPTIONS' }); return res.end(); }
    // the test's controls
    if (u.pathname === '/ctl/now') return json(res, 200, { now: Date.now() });
    if (u.pathname === '/ctl/skew') { skew += Number(u.searchParams.get('ms')) || 0; return json(res, 200, { now: Date.now() }); }
    if (u.pathname === '/ctl/reads') { if (u.searchParams.get('reset')) { store.st.reads = 0; store.st.queries = 0; store.st.by = {}; } return json(res, 200, { reads: store.st.reads, queries: store.st.queries, by: store.st.by, writes: store.st.writes }); }
    if (u.pathname === '/ctl/list') return json(res, 200, store.all(u.searchParams.get('c')).map(d => JSON.parse(JSON.stringify(d))));
    if (u.pathname === '/ctl/doc') return json(res, 200, store.get(u.searchParams.get('c'), u.searchParams.get('id')) || null);
    if (u.pathname === '/ctl/put') { const b = JSON.parse(await readBody(req)); store.put(b.c, b.id, b.doc); return json(res, 200, { ok: true }); }
    if (PAGES[u.pathname]) { res.writeHead(200, { 'content-type': MIME['.html'] }); return res.end(pageHtml(PAGES[u.pathname], u.searchParams.get('sandbox') === '1')); }
    const f = { '/station-session.js': 1, '/station-activity.js': 1, '/station-live-order.js': 1 }[u.pathname];
    if (f) { res.writeHead(200, { 'content-type': MIME['.js'], 'cache-control': 'no-store' }); return res.end(fs.readFileSync(path.join(root, u.pathname.slice(1)))); }
    res.writeHead(404); res.end('not here');
  } catch (e) { json(res, 500, { ok: false, error: String(e && e.stack || e) }); }
});
server.listen(0, '127.0.0.1', function () { PORT = this.address().port; process.stdout.write('PORT ' + PORT + '\n'); });
process.on('uncaughtException', e => { realLog.error('portal backend:', e && e.stack || e); });
