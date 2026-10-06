// The fake shop behind tests/stations/all-stations-e2e.cjs, in its own process (so its fake clock never touches the test's own timing).
//   · tests/charm-nest/bridge-server.cjs  (the repo served on TWO origins, the sorter's charmNest* handlers over one in-memory Firestore)
//   · the REAL netlify/functions/firebaseOrders.js (the open station door: {pinLogin}, {session}, {activity}, {live}, {timeline}, the scanner relay, messages)
//     and the REAL employeeEfficiency.js (the portal's reader, behind a FAKE manager passcode) over that same Firestore
//   · fake etsyOrderProxy / etsyImages / listOpenOrders and the few other functions a station page asks for, answered from fixtures.js (no Etsy, no AI)
//   · /__fs/*  a Firestore client's door (the pages' firebase compat SDK is replaced by fake-firebase.js, which talks to it), so a phone scanner's
//     write really reaches the desktop page's onSnapshot
//   · /__ctl/* the test's controls: the clock (skew / set), documents, the call log, the egress guard, the PIN-leak counter
// The clock is the date the test gives (--now), then real time, plus whatever the test skewed. Date itself is replaced (new Date() too).
// No network leaves this process: every outbound connection to a non-loopback address is recorded and destroyed.
//   node tests/stations/all-stations/backend.cjs --now=2026-10-07T13:00:00Z   → prints "READY {json}"
'use strict';
const path = require('path'), http = require('http'), net = require('net'), fs = require('fs'), { PassThrough } = require('stream');
const root = path.join(__dirname, '../../..');
const arg = k => { const a = process.argv.find(x => x.startsWith('--' + k + '=')); return a ? a.slice(k.length + 3) : ''; };

/* ── the clock ── */
const RealDate = Date, realNow = RealDate.now.bind(RealDate), T0 = realNow();
const BASE = arg('now') ? RealDate.parse(arg('now')) : realNow();
let skew = 0;
const nowMs = () => BASE + (realNow() - T0) + skew;
class FakeDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(nowMs()); else super(...a); }
  static now() { return nowMs(); }
}
global.Date = FakeDate;

/* ── the egress guard: nothing leaves the loopback ── */
const egress = [];
const isLoop = h => { h = String(h || '').replace(/^\[|\]$/g, ''); return !h || h === 'localhost' || h === '::1' || /^127\./.test(h) || h === '0.0.0.0' || h === '::ffff:127.0.0.1'; };
const realConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  let host = '', port = 0;
  const o = args[0];
  if (o && typeof o === 'object' && !Array.isArray(o)) { host = o.host || ''; port = o.port || 0; if (o.path) return realConnect.apply(this, args); }
  else if (typeof o === 'number' || typeof o === 'string') { port = +o; host = typeof args[1] === 'string' ? args[1] : ''; }
  if (!isLoop(host)) { egress.push({ host, port, at: nowMs() }); const e = new Error('egress blocked by the test: ' + host); process.nextTick(() => this.destroy(e)); return this; }
  return realConnect.apply(this, args);
};
if (typeof global.fetch === 'function') {
  const rf = global.fetch;
  global.fetch = function (u, ...r) { try { const h = new URL(typeof u === 'string' ? u : (u && u.url) || String(u)).hostname; if (!isLoop(h)) { egress.push({ host: h, port: 0, via: 'fetch', at: nowMs() }); return Promise.reject(new Error('egress blocked by the test: ' + h)); } } catch (_) {} return rf.call(this, u, ...r); };
}

/* ── logs: the shop speaks only through the control door ── */
const realErr = console.error.bind(console);
console.log = console.warn = console.info = () => {}; console.error = () => {};
const calls = [], stats = { pinLeaks: 0, unfaked: {}, paid: 0, paidTried: 0, paidNames: {}, functions: {} };
const PAID = /anthropic|claude|openai|gemini|charmNestAgent|charmEngrave|charmMaster|etsyMailDraftReply|etsyMailAutoPipeline|etsyMailLearn|etsyMailIntent|geminiImage|openAiCode/i;

const { start } = require(path.join(root, 'tests/charm-nest/bridge-server.cjs'));
const FX = require('./fixtures.cjs');

(async () => {
  // capture the servers the bridge starts, to put the fakes in front of its handlers
  const made = [], realCreate = http.createServer;
  http.createServer = function (...a) { const s = realCreate.apply(this, a); made.push(s); return s; };
  const srv = await start({ receipts: [] });
  http.createServer = realCreate;
  const { st } = srv;
  const fnDir = path.join(root, 'netlify/functions');
  // the real door and reader run over the bridge's documents with Firestore's own merge rules (deep-store.cjs)
  const Module = require('module'), { wrapAdmin } = require('./deep-store.cjs');
  const deep = wrapAdmin(st.admin, st), prevLoad = Module._load;
  Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return deep.admin; return prevLoad.call(this, req, ...rest); };
  const door = require(path.join(fnDir, 'firebaseOrders.js')), eff = require(path.join(fnDir, 'employeeEfficiency.js'));
  Module._load = prevLoad;
  try { require(path.join(fnDir, '_stationPinLogin.js')).deps.sleep = async () => {}; } catch (_) {}
  const PASS = arg('pass') || 'all-stations-fake-pass';
  process.env.EDIT_PASSCODE = PASS;
  try { require(path.join(fnDir, '_editPasscode.js')).resetCache(); } catch (_) {}
  st.put('config', 'editPasscode', { passcode: PASS });
  // a paid call would be the model: count it (the bridge's stub answers, nothing is bought)
  try { const an = require(path.join(fnDir, '_etsyMailAnthropic.js')), was = an.callClaudeRaw; an.callClaudeRaw = async (...a) => { stats.paid++; stats.paidNames.callClaudeRaw = (stats.paidNames.callClaudeRaw || 0) + 1; return was(...a); }; } catch (_) {}

  const admin = deep.admin, db = deep.db, FV = admin.firestore.FieldValue;
  const pins = new Set();
  const secretIn = text => { for (const p of pins) if (new RegExp('(^|\\D)' + p + '(\\D|$)').test(text)) return true; return false; };      // (six digits standing alone; the same digits inside a longer number such as a millisecond time are not a PIN, and a polled response repeats them every second)

  /* the doors: the real station door and the real efficiency reader */
  async function realDoor(name, req, res, bodyBuf, q) {
    const event = { httpMethod: req.method, headers: Object.assign({}, req.headers), queryStringParameters: q, body: bodyBuf ? bodyBuf.toString('utf8') : '' };
    const out = name === 'employeeEfficiency' ? await eff._t.handle(event, db) : await door.handler(event);
    const headers = Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Cache-Control': 'no-store' }, out.headers || {});
    res.writeHead(out.statusCode || 200, headers); res.end(out.body || '');
    return out;
  }
  const send = (res, code, body, extra) => { res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Cache-Control': 'no-store', 'Cross-Origin-Resource-Policy': 'cross-origin' }, extra || {})); res.end(Buffer.isBuffer(body) || typeof body === 'string' ? body : JSON.stringify(body)); };
  const readAll = req => new Promise(r => { const c = []; req.on('data', d => c.push(d)); req.on('end', () => r(Buffer.concat(c))); });

  /* ── the Firestore client's door (wire: values in JSON; {__fv:'delete'|'ts'|'inc'|'union'} sentinels in, {__ts:ms} out) ── */
  const toWire = v => {
    if (v && typeof v.toMillis === 'function' && !Array.isArray(v)) return { __ts: v.toMillis() };
    if (Array.isArray(v)) return v.map(toWire);
    if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toWire(x)]));
    return v;
  };
  const fromWire = v => {
    if (Array.isArray(v)) return v.map(fromWire);
    if (v && typeof v === 'object') {
      if (v.__fv === 'delete') return FV.delete();
      if (v.__fv === 'ts') return FV.serverTimestamp();
      if (v.__fv === 'inc') return FV.increment(v.n);
      if (v.__ts != null) return admin.firestore.Timestamp.fromMillis(v.__ts);
      return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, fromWire(x)]));
    }
    return v;
  };
  const docAt = p => db.doc ? db.doc(p) : (() => { const parts = p.split('/'); let r = db.collection(parts[0]).doc(parts[1]); for (let i = 2; i < parts.length; i += 2) r = r.collection(parts[i]).doc(parts[i + 1]); return r; })();
  const collAt = p => { const parts = p.split('/'); let r = db.collection(parts[0]); for (let i = 1; i < parts.length; i += 2) r = r.doc(parts[i]).collection(parts[i + 1]); return r; };
  async function fsOp(op, b) {
    if (op === 'get') { const s = await docAt(b.path).get(); return { exists: !!s.exists, id: b.path.split('/').pop(), data: s.exists ? toWire(s.data()) : null }; }
    if (op === 'set') { await docAt(b.path).set(fromWire(b.data), b.merge ? { merge: true } : undefined); return { ok: true }; }
    if (op === 'update') { await docAt(b.path).update(fromWire(b.data)); return { ok: true }; }
    if (op === 'delete') { await docAt(b.path).delete(); return { ok: true }; }
    if (op === 'add') { const id = 'auto' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6); await collAt(b.path).doc(id).set(fromWire(b.data)); return { ok: true, id }; }
    if (op === 'query') {
      let q = collAt(b.path);
      for (const [f, o, v] of b.where || []) q = q.where(f, o, fromWire(v));
      for (const [f, d] of b.order || []) q = q.orderBy(f, d);
      if (b.limit) q = q.limit(b.limit);
      const s = await q.get();
      return { docs: s.docs.map(d => ({ id: d.id, data: toWire(d.data()) })) };
    }
    throw new Error('unknown fs op ' + op);
  }

  const controls = async (u, req, res) => {
    const p = u.pathname, q = Object.fromEntries(u.searchParams.entries());
    if (p === '/__ctl/now') return send(res, 200, { now: nowMs() });
    if (p === '/__ctl/skew') { skew += Number(q.ms) || 0; return send(res, 200, { now: nowMs() }); }
    if (p === '/__ctl/set') { skew += Number(q.now) - nowMs(); return send(res, 200, { now: nowMs() }); }
    if (p === '/__ctl/doc') { const s = await docAt(q.path).get(); return send(res, 200, s.exists ? toWire(s.data()) : null); }
    if (p === '/__ctl/list') { const s = await collAt(q.coll).get(); return send(res, 200, s.docs.map(d => Object.assign({ _id: d.id }, toWire(d.data())))); }
    if (p === '/__ctl/calls') { const from = +q.from || 0; return send(res, 200, { total: calls.length, calls: calls.slice(from) }); }
    if (p === '/__ctl/stats') return send(res, 200, { egress, pinLeaks: stats.pinLeaks, unfaked: stats.unfaked, paid: stats.paid, paidTried: stats.paidTried, paidNames: stats.paidNames, functions: stats.functions, now: nowMs() });
    if (p === '/__ctl/eff') { const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); const out = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.7' }, queryStringParameters: {}, body: JSON.stringify(Object.assign({ key: PASS }, b)) }, db); return send(res, 200, out.body); }
    if (p === '/__ctl/put') { const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); await docAt(b.path).set(fromWire(b.data), b.merge ? { merge: true } : undefined); return send(res, 200, { ok: true }); }
    if (p === '/__ctl/roster') {   // { "<number>": "<name>" }: the fake Employee Numbers. The numbers are kept here only to prove none of them leaves in a response
      const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); Object.keys(b).forEach(k => pins.add(k));
      await docAt('Brites_Orders/Employee Numbers').set(b); return send(res, 200, { ok: true, n: Object.keys(b).length });
    }
    if (p === '/__ctl/fixtures') { const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); FX.set(b); return send(res, 200, { ok: true }); }
    if (p === '/__ctl/admins') { const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); await docAt('config/stationAdmins').set({ names: b.names || [] }); return send(res, 200, { ok: true }); }
    return send(res, 404, { error: 'no such control' });
  };

  const front = orig => async (req, res) => {
    let u;
    try {
      u = new URL(req.url, 'http://x');
      if (req.method === 'OPTIONS') return send(res, 204, {});
      if (u.pathname.startsWith('/__ctl/')) return await controls(u, req, res);
      if (u.pathname.startsWith('/__pic/')) { res.writeHead(200, { 'Content-Type': 'image/png', 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }); return res.end(FX.PNG); }
      if (u.pathname.startsWith('/__fs/')) { const b = JSON.parse((await readAll(req)).toString('utf8') || '{}'); return send(res, 200, await fsOp(u.pathname.slice(6), b)); }
      if (u.pathname.startsWith('/.netlify/functions/')) {
        const name = u.pathname.slice('/.netlify/functions/'.length), q = Object.fromEntries(u.searchParams.entries());
        const buf = req.method === 'POST' ? await readAll(req) : null, text = buf ? buf.toString('utf8') : '';
        let b = {}; try { b = text ? JSON.parse(text) : {}; } catch (_) {}
        const op = b.op || q.op || (b.session ? 'session' : Array.isArray(b.activity) ? 'activity' : b.live ? 'live' : b.pinLogin !== undefined ? 'pinLogin' : Array.isArray(b.timeline) ? 'timeline' : b.orderNumField !== undefined ? 'scanRelay' : typeof b.newMessage === 'string' ? 'newMessage' : null);
        const rec = { name, method: req.method, op, at: nowMs(), via: '' };
        // a PIN may be in a { pinLogin } body and nowhere else (not in a URL, not in another body)
        if (secretIn(u.search) || (op !== 'pinLogin' && secretIn(text))) stats.pinLeaks++;
        stats.functions[name] = (stats.functions[name] || 0) + 1;
        // a paid-looking function the page TRIED to call: answered by a fake (never forwarded), counted apart. stats.paid counts only a call that reached real code (a model client, or the bridge answering it).
        if (PAID.test(name)) { const k = name + (op ? ':' + op : ''); stats.paidTried++; stats.paidNames[k] = (stats.paidNames[k] || 0) + 1; rec.paidLooking = true; }
        calls.push(rec);
        if (name === 'employeeEfficiency' || name === 'firebaseOrders') { rec.via = 'real'; const out = await realDoor(name, req, res, buf, q); if (secretIn(out.body || '') && op !== 'pinLogin') stats.pinLeaks++; rec.status = out.statusCode || 200; return; }
        const fake = await FX.answer(name, req.method, q, b, st, { db, admin, now: nowMs }, req.headers);
        if (fake !== undefined) { rec.via = 'fake'; return send(res, fake.status || 200, fake.body, fake.headers); }
        rec.via = 'bridge';
        // not faked here: the bridge's own (charmNest*, designArchive ... ); anything it does not know is logged
        const replay = Object.assign(new PassThrough(), { method: req.method, url: req.url, headers: req.headers });
        replay.end(buf || undefined);
        const wasWrite = res.writeHead.bind(res);
        res.writeHead = (code, ...r) => { rec.status = code; if (code === 404) stats.unfaked[name] = (stats.unfaked[name] || 0) + 1; else if (rec.paidLooking) stats.paid++; return wasWrite(code, ...r); };
        return await orig(replay, res);
      }
      return await orig(req, res);
    } catch (e) { try { send(res, 500, { error: String(e && e.stack || e).slice(0, 400) }); } catch (_) {} }
  };
  for (const s of made) { const orig = s.listeners('request')[0]; s.removeAllListeners('request'); s.on('request', front(orig)); }
  // the control door also on its own port, for the test process (no CORS needed)
  const ctl = await new Promise(r => { const s = realCreate(front(() => {})); s.listen(0, '127.0.0.1', () => r(s)); });
  process.stdout.write('READY ' + JSON.stringify({ sorterOrigin: srv.sorterOrigin, stationOrigin: srv.stationOrigin, ctl: 'http://127.0.0.1:' + ctl.address().port, t0: nowMs(), pass: PASS }) + '\n');
})().catch(e => { realErr('backend failed:', e && e.stack || e); process.exit(1); });
process.on('uncaughtException', e => { realErr('backend:', e && e.stack || e); });
process.on('unhandledRejection', e => { realErr('backend (rejection):', e && e.stack || e); });
