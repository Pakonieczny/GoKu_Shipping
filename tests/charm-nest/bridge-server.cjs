// A local stand-in for the deployed site, for the bridge tests: serves the repo root on TWO origins (the sorter and the
// station are different origins in production, so the origin checks are exercised for real) and answers the Netlify
// functions both pages call with an in-memory fake — Etsy included — so a whole run can happen without the internet.
//   const { start } = require('./bridge-server.cjs'); const srv = await start(); … srv.close();
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.mjs': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.ai': 'application/pdf', '.pdf': 'application/pdf', '.otf': 'font/otf', '.ttf': 'font/ttf' };

function makeState(opts = {}) {
  const st = { docs: new Map(), blobs: new Map(), receipts: opts.receipts || [], counters: new Map(), calls: [], agentResults: opts.agentResults || {}, fail: {} };
  st.doc = (coll, id) => st.docs.get(coll + '/' + id);
  st.put = (coll, id, data, merge = true) => { const cur = merge && st.docs.get(coll + '/' + id) || {}; st.docs.set(coll + '/' + id, Object.assign({}, cur, data)); return st.docs.get(coll + '/' + id); };
  st.list = coll => [...st.docs.entries()].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => Object.assign({ _id: k.slice(coll.length + 1) }, v));
  return st;
}
const json = (res, code, body, extra) => { res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS,PUT' }, extra || {})); res.end(JSON.stringify(body)); };
const readBody = req => new Promise(r => { const c = []; req.on('data', d => c.push(d)); req.on('end', () => r(Buffer.concat(c))); });
const tokenUrl = (base, p) => `${base}/__blob/${encodeURIComponent(p)}?alt=media&token=t`;

/** The functions the two pages call, over the in-memory state. */
async function functions(st, name, req, res, base) {
  const q = Object.fromEntries(new URL(req.url, base).searchParams.entries());
  const bodyBuf = req.method === 'POST' ? await readBody(req) : null; let body = {}; try { body = bodyBuf && bodyBuf.length ? JSON.parse(bodyBuf.toString('utf8')) : {}; } catch (_) { body = {}; }
  st.calls.push({ name, op: body.op || q.op || null, body, q });
  if (st.fail[name]) return json(res, 500, { error: 'forced failure' });
  if (name === 'authGate') return json(res, 200, { locked: false });
  // ── Design Station's Etsy ──
  if (name === 'listOpenOrders') { const off = +q.offset || 0; return json(res, 200, { results: st.receipts.slice(off, off + 100).map(r => Object.assign({}, r, { transactions: undefined })) }); }
  if (name === 'etsyOrderProxy') { const r = st.receipts.find(x => String(x.receipt_id) === String(q.orderId)); if (!r) return json(res, 404, { error: 'no such receipt' }); return json(res, 200, { receipt: Object.assign({}, r, { transactions: undefined }), transactions: r.transactions }); }
  // a listing's pictures, and the proxy the station hands the sorter: a 1×1 PNG is enough to prove the card paints it
  if (name === 'etsyImages') return json(res, 200, [{ listing_id: q.listingId, url_570xN: `${base}/__pic/${q.listingId}.jpg` }]);
  if (name === 'imageProxy') { res.writeHead(200, { 'Content-Type': 'image/png', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }); return res.end(Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/fteaysAAAAASUVORK5CYII=', 'base64')); }
  if (name === 'refreshEtsyToken') return json(res, 200, { access_token: 'tok', refresh_token: 'ref', expires_in: 3600 });
  const SB = q.sandbox === '1' ? 'Sandbox_' : '';
  if (name === 'firebaseOrders') {
    if (req.method === 'POST') {
      const RT = SB + 'Design_RealTime_Selected_Orders';
      if (Array.isArray(body.rtClaimIds)) { body.rtClaimIds.forEach(id => st.put(RT, id, { claimed: true, claimedBy: body.claimedBy || 'sorter', claimRun: body.claimRun || null, at: Date.now() })); return json(res, 200, { success: true, count: body.rtClaimIds.length }); }
      if (Array.isArray(body.rtUnclaimIds)) { body.rtUnclaimIds.forEach(id => st.put(RT, id, { claimed: false, claimedBy: null, at: Date.now() })); return json(res, 200, { success: true }); }
      if (Array.isArray(body.rtLockIds)) { body.rtLockIds.forEach(id => st.put(RT, id, { selected: true, selectedBy: body.clientId, page: body.page, at: Date.now() })); return json(res, 200, { success: true }); }
      if (Array.isArray(body.rtUnlockIds)) { body.rtUnlockIds.forEach(id => st.put(RT, id, { selected: false, selectedBy: null, at: Date.now() })); return json(res, 200, { success: true }); }
      if (Array.isArray(body.completedIds)) { body.completedIds.forEach(id => st.put(SB + 'Design_Completed Orders', id, { completed: true, at: Date.now() })); return json(res, 200, { success: true }); }
      if (Array.isArray(body.uncompleteIds)) { body.uncompleteIds.forEach(id => st.docs.delete(SB + 'Design_Completed Orders/' + id)); return json(res, 200, { success: true }); }
      if (typeof body.newMessage === 'string') { st.put(SB + 'Brites_Orders', body.orderNumber, { touched: Date.now() }); st.put(SB + 'Brites_Orders/' + body.orderNumber + '/messages', 'm' + Date.now() + Math.random().toString(36).slice(2, 6), { text: body.newMessage, senderName: body.employeeName || 'Staff', at: Date.now() }); return json(res, 200, { success: true }); }
      if (body.staffNote !== undefined) { st.put(SB + 'Brites_Orders', body.orderNumber, { 'Staff Note': body.staffNote }); return json(res, 200, { success: true }); }
      return json(res, 200, { success: true });
    }
    const RT = SB + 'Design_RealTime_Selected_Orders'; const DC = SB + 'Design_Completed Orders', BO = SB + 'Brites_Orders';
    if (q.dcFor != null) return json(res, 200, { success: true, orderNumbers: q.dcFor.split(',').filter(id => st.doc(DC, id)), now: Date.now() });
    if (q.staffNotesFor != null) return json(res, 200, { success: true, orderNumbers: q.staffNotesFor.split(',').filter(id => (st.doc(BO, id) || {})['Staff Note']), now: Date.now() });
    if (q.rtFor != null) { const locks = {}, claims = {}; q.rtFor.split(',').forEach(id => { const v = st.doc(RT, id); if (v && v.selected) locks[id] = v; if (v && v.claimed) claims[id] = { claimedBy: v.claimedBy, run: v.claimRun }; }); return json(res, 200, { success: true, locks, claims, now: Date.now() }); }
    if (q.rtSince != null) { const since = +q.rtSince; const locks = {}, unlocks = [], claims = {}, unclaims = []; st.list(RT).forEach(v => { if ((v.at || 0) < since) return; if (v.selected === true) locks[v._id] = { selectedBy: v.selectedBy, page: v.page, atMs: v.at }; else if (v.selected === false) unlocks.push(v._id); if (v.claimed === true) claims[v._id] = { claimedBy: v.claimedBy, run: v.claimRun, atMs: v.at }; else if (v.claimed === false) unclaims.push(v._id); }); return json(res, 200, { success: true, locks, unlocks, claims, unclaims, now: Date.now() }); }
    if (q.rt === '1') { const locks = {}, claims = {}; st.list(RT).forEach(v => { if (v.selected) locks[v._id] = v; if (v.claimed) claims[v._id] = { claimedBy: v.claimedBy, run: v.claimRun }; }); return json(res, 200, { success: true, locks, claims }); }
    if (q.dcSince != null) return json(res, 200, { success: true, orderNumbers: st.list(DC).filter(v => (v.at || 0) >= +q.dcSince).map(v => v._id), now: Date.now() });
    if (q.designCompleted === '1') return json(res, 200, { success: true, orderNumbers: st.list(DC).map(v => v._id) });
    if (q.staffNotes === '1') return json(res, 200, { success: true, orderNumbers: st.list(BO).filter(v => v['Staff Note']).map(v => v._id) });
    if (q.orderId) { const d = st.doc(BO, q.orderId); return json(res, 200, d ? { success: true, data: d } : { success: false, notFound: true }); }
    return json(res, 400, { error: 'unhandled' });
  }
  if (name === 'designArchive') { if (req.method === 'POST') { (body.orders || []).forEach(o => st.put(SB + 'Design_Order_Archive', o.receiptId, o)); return json(res, 200, { success: true, saved: (body.orders || []).length }); } if (q.op === 'index') return json(res, 200, { success: true, rows: [], nextCursor: null, now: Date.now() }); if (q.op === 'have') return json(res, 200, { success: true, have: [] }); return json(res, 200, { success: true, rows: [] }); }
  // ── sorter's functions: the real handlers against an in-memory firebase-admin ──
  if (name === 'etsyApiUsage') return json(res, 200, { verified: false, count: 0, error: 'no counters in the test server' });
  if (name === 'etsyApiProbe') return json(res, 200, { ok: true, verified: true, limitPerDay: 10000, remainingToday: 9990, note: 'test server' });
  if (name === 'charmNestLibrary' || name === 'charmNestOutput' || name === 'charmNestCheck' || name === 'etsySandbox' || name === 'charmNestAgent-background' || name === 'charmEngrave-background' || name === 'charmMaster-background') {
    const h = st.handlers[name]; if (!h) return json(res, 404, { error: 'no handler' });
    const out = await h.handler({ httpMethod: req.method, headers: Object.fromEntries(Object.entries(req.headers)), body: bodyBuf ? bodyBuf.toString('utf8') : '', queryStringParameters: q });
    res.writeHead(out.statusCode || 200, Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*' }, out.headers || {})); return res.end(out.body || '');
  }
  return json(res, 404, { error: 'no functions in the test server: ' + name });
}

/** firebase-admin fake shared by the real charmNest* handlers (the same shape tests/charm-nest/functions.cjs injects). */
function fakeAdmin(st, base) {
  const SERVER_TS = { __ts: true };
  const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => ({ __del: true }) };
  const Timestamp = { fromMillis: ms => ({ toMillis: () => ms }) };
  const apply = (target, src) => { for (const [k, v] of Object.entries(src)) { if (v && v.__inc != null) target[k] = (target[k] || 0) + v.__inc; else if (v && v.__del) delete target[k]; else if (v === SERVER_TS) target[k] = { toMillis: () => Date.now() }; else target[k] = v; } return target; };
  const docRef = (coll, id) => ({ id, path: coll + '/' + id, collection: sub => collection(coll + '/' + id + '/' + sub), async get() { const d = st.docs.get(coll + '/' + id); return { exists: !!d, id, data: () => (d ? { ...d } : undefined), get: k => d && d[k] }; }, async set(data, o) { const cur = (o && o.merge && st.docs.get(coll + '/' + id)) || {}; st.docs.set(coll + '/' + id, apply({ ...cur }, data)); }, async delete() { st.docs.delete(coll + '/' + id); } });
  const query = (coll, filters = [], order = null, lim = 0) => ({ where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), order, lim), orderBy: (f, dir) => query(coll, filters, [f, dir || 'asc'], lim), limit: n => query(coll, filters, order, n), select() { return this; }, async get() { let rows = [...st.docs.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), data: () => ({ ...v }), ref: docRef(coll, k.slice(coll.length + 1)), get: f => v[f] })); for (const [f, op, v] of filters) rows = rows.filter(r => { const x = r.data()[f]; return op === '==' ? x === v : op === '>=' ? x >= v : op === '<=' ? x <= v : op === '!=' ? x !== v : true; }); if (order) rows.sort((a, b) => { const x = a.data()[order[0]], y = b.data()[order[0]]; const xv = x && x.toMillis ? x.toMillis() : x, yv = y && y.toMillis ? y.toMillis() : y; const c = xv > yv ? 1 : xv < yv ? -1 : 0; return order[1] === 'desc' ? -c : c; }); if (lim) rows = rows.slice(0, lim); return { size: rows.length, docs: rows, empty: !rows.length, forEach: fn => rows.forEach(fn) }; }, async add(data) { const id = 'auto' + Math.random().toString(36).slice(2, 8); await docRef(coll, id).set(data); return docRef(coll, id); }, doc: id => docRef(coll, id || 'auto' + Math.random().toString(36).slice(2, 10)) });
  const collection = c => query(c);
  const db = { collection, batch() { const ops = []; return { set: (ref, data, o) => ops.push(() => ref.set(data, o)), delete: ref => ops.push(() => ref.delete()), async commit() { for (const o of ops) await o(); } }; }, async getAll(...refs) { return Promise.all(refs.map(r => r.get())); }, async runTransaction(fn) { const t = { get: ref => ref.get(), set: (ref, data, o) => ref.set(data, o) }; return fn(t); } };
  const bucket = { name: 'test-bucket', async getMetadata() { return [{ location: 'TEST', storageClass: 'STANDARD', cors: [{ origin: st.corsOrigins || ['*'], method: ['GET', 'PUT', 'HEAD'], responseHeader: ['Content-Type'], maxAgeSeconds: 3600 }] }]; }, file(p) { return { async save(buf, o) { st.blobs.set(p, { buf: Buffer.from(buf), meta: { contentType: o && o.contentType, metadata: (o && o.metadata && o.metadata.metadata) || {} } }); }, async exists() { return [st.blobs.has(p)]; }, async download() { const b = st.blobs.get(p); if (!b) throw new Error('no blob ' + p); return [b.buf]; }, async delete() { st.blobs.delete(p); }, async getMetadata() { const b = st.blobs.get(p); if (!b) throw new Error('no blob'); return [{ contentType: b.meta.contentType, size: b.buf.length, metadata: Object.assign({ firebaseStorageDownloadTokens: 't' }, b.meta.metadata) }]; }, async setMetadata(m) { const b = st.blobs.get(p) || { buf: Buffer.alloc(0), meta: { metadata: {} } }; if (m.contentType) b.meta.contentType = m.contentType; Object.assign(b.meta.metadata, m.metadata || {}); st.blobs.set(p, b); }, async getSignedUrl() { return [`${base}/__put/${encodeURIComponent(p)}`]; } }; } };
  return { firestore: Object.assign(() => db, { FieldValue, Timestamp }), storage: () => ({ bucket: () => bucket }) };
}

async function start(opts = {}) {
  const st = makeState(opts);
  const servers = [];
  const handler = (base) => async (req, res) => {
    try {
      if (req.method === 'OPTIONS') return json(res, 204, {});
      const u = new URL(req.url, base); const p = decodeURIComponent(u.pathname);
      if (p.startsWith('/.netlify/functions/')) return functions(st, p.slice('/.netlify/functions/'.length), req, res, base);
      if (p.startsWith('/__put/')) { const key = decodeURIComponent(p.slice(7)); const buf = await readBody(req); st.blobs.set(key, { buf, meta: { contentType: req.headers['content-type'], metadata: { firebaseStorageDownloadTokens: 't' } } }); return json(res, 200, { ok: true }); }
      if (p.startsWith('/__blob/')) { const key = decodeURIComponent(p.slice(8)); const b = st.blobs.get(key); if (!b) { res.writeHead(404); return res.end(); } res.writeHead(200, { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }); return res.end(b.buf); }
      const f = path.join(root, p === '/' ? 'charm-nest-1.html' : p);
      if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404, { 'Access-Control-Allow-Origin': '*' }); return res.end(); }
      const headers = { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp', 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' };
      if (p === '/design-1.html') headers['Content-Security-Policy'] = `frame-ancestors 'self' ${opts.sorterOrigin || 'http://127.0.0.1:*'} http://localhost:* http://127.0.0.1:*`;
      res.writeHead(200, headers); fs.createReadStream(f).pipe(res);
    } catch (e) { json(res, 500, { error: e.message }); }
  };
  const listen = host => new Promise(r => { const s = http.createServer((req, res) => handler(`http://${host}:${s.address().port}`)(req, res)); s.listen(0, host, () => r(s)); });
  const sorter = await listen('127.0.0.1'); const station = await listen('localhost');
  servers.push(sorter, station);
  const sorterOrigin = `http://127.0.0.1:${sorter.address().port}`, stationOrigin = `http://localhost:${station.address().port}`;
  // the real charmNest* handlers against the in-memory admin; node-fetch (the background kicks) rewired to call the handlers directly
  const admin = fakeAdmin(st, stationOrigin);
  const fnDir = path.join(root, 'netlify/functions');
  const Module = require('module'), realLoad = Module._load;
  const handlers = {};
  Module._load = function (req, ...rest) {
    if (req === 'node-fetch') return async (url, init) => { const name = String(url).split('/.netlify/functions/')[1]; const h = handlers[name]; if (!h) return { ok: false, status: 404, statusText: 'no handler', text: async () => '' }; setTimeout(() => h.handler({ httpMethod: 'POST', headers: {}, body: init && init.body || '' }).catch(e => console.error('bg', e)), 20); return { ok: true, status: 202, text: async () => '' }; };
    if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return admin;
    if (req === '@resvg/resvg-js') throw new Error('not installed in the test');
    return realLoad.call(this, req, ...rest);
  };
  for (const n of ['charmNestLibrary', 'charmNestOutput', 'charmNestCheck', 'etsySandbox', 'charmNestAgent-background', 'charmEngrave-background', 'charmMaster-background']) { delete require.cache[require.resolve(path.join(fnDir, n + '.js'))]; handlers[n] = require(path.join(fnDir, n + '.js')); }
  // the model is a stub: answers per mode from opts.agentResults or a sensible default
  const anthro = require(path.join(fnDir, '_etsyMailAnthropic.js'));
  anthro.callClaudeRaw = async (o) => { const sys = (o.system && o.system[0] && o.system[0].text) || ''; const mode = /engraver and decide/.test(sys) ? 'engraveIntent' : /rendered BACK/.test(sys) ? 'engraveReview' : /strip directly BELOW/.test(sys) ? 'labelRead' : /quality reviewer/.test(sys) ? 'grouping' : /final inspector/.test(sys) ? 'layout' : 'name'; const fn = st.agentResults[mode]; const text = JSON.stringify(typeof fn === 'function' ? fn(o) : (fn || defaultAgent(mode, o))); return { stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: 1 }, content: [{ type: 'text', text }] }; };
  process.env.ANTHROPIC_API_KEY = 'test';
  st.handlers = handlers;
  return { st, sorterOrigin, stationOrigin, close: () => { servers.forEach(s => s.close()); Module._load = realLoad; }, blobUrl: p => tokenUrl(stationOrigin, p) };
}
function defaultAgent(mode, o) {
  if (mode === 'engraveIntent') { const txt = String(o.messages[0].content[0].text); const m = /Personalisation field: (\[.*?\])/.exec(txt); let pers = []; try { pers = JSON.parse(m ? m[1] : '[]'); } catch (_) {} const engrave = pers.length > 0; return { engrave, text: pers.join('\n'), source: engrave ? 'personalization' : 'none', sourceQuote: pers[0] || '', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: engrave ? 0.97 : 0.95 }; }
  if (mode === 'engraveReview') return { legible: true, notes: 'reads fine', concerns: [] };
  if (mode === 'labelRead') return { reads: [] };
  if (mode === 'grouping') { const n = (o.messages[0].content.filter(c => c.type === 'image').length - 1); return { verdicts: Array.from({ length: n }, (_, i) => ({ index: i, verdict: 'complete', mergeInto: null, note: '' })), realCharmCount: n, summary: 'all complete' }; }
  if (mode === 'layout') return { ok: true, issues: [], summary: 'fine' };
  return { charms: [] };
}
module.exports = { start, makeState };
if (require.main === module) start().then(s => console.log('sorter', s.sorterOrigin, 'station', s.stationOrigin));
