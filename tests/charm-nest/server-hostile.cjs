// The server side of the Charm Sorter against a hostile, clumsy or offline caller (ADVSERVER, 15 Oct 2026: Paul's final test before production).
// Offline, on the in-memory Firestore / Storage of _sandboxFakes.cjs, against the REAL charmNestLibrary / charmNestOutput handlers. It proves:
//   · the door: a name the object itself has ("constructor", "toString", "__proto__") is no op (400, nothing run, nothing written); a wrong method on the POST-only
//     functions, a body that is empty, not JSON, an array, "null" or huge answers a plain JSON 4xx with the CORS headers and no stack;
//   · every op the browser files send to charmNestLibrary is registered (or owned by the password door), every gated op exists, and no registered op answers a
//     plain call with a thrown error, a stack or a non-JSON body;
//   · two benches: two runs that pool the same new lines at the same moment leave each line with ONE owner (the other run is told `contended`), set numbers asked at
//     the same moment are never given twice, a sheet saved twice at once is one record, an order's first arrival is stamped once (Firestore's optimistic
//     transactions are stood in for here: a document read and changed by another commit before this commit runs the callback again);
//   · a connection lost in the middle of a write (Firestore refuses the commit) answers a JSON 500, leaves nothing half-written, and the same request sent again works.
//   node tests/charm-nest/server-hostile.cjs
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const F = require('./_sandboxFakes.cjs'); F.install();
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), out = require(path.join(fnDir, 'charmNestOutput.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
const { store, blobs, writes, canon } = F;
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ok  ' + m); };
const post = async (h, b, ev) => { const r = await h.handler(Object.assign({ httpMethod: 'POST', headers: {}, body: typeof b === 'string' ? b : JSON.stringify(b) }, ev || {})); let j; try { j = JSON.parse(r.body); } catch (_) { j = null; } return { status: r.statusCode, body: j, headers: r.headers || {}, raw: String(r.body) }; };
const L = (b, ev) => post(lib, b, ev), O = (b, ev) => post(out, b, ev);
const plain = r => r.body && typeof r.body === 'object' && !/\n\s+at |node_modules|\.js:\d+/.test(r.raw) && r.headers['Access-Control-Allow-Origin'] === '*';

/* ── Firestore's optimistic transactions: a document this transaction read that another commit changed before this one commits runs the callback again ── */
let retries = 0, arm = 0;
F.db.runTransaction = async function (fn) {
  for (let attempt = 0; attempt < 8; attempt++) {
    const reads = new Map(), ops = [];
    const rec = r => { if (r && r.path && !reads.has(r.path)) reads.set(r.path, canon(store.get(r.path))); };
    const tx = {
      get: async r => { rec(r); const s = await r.get(); if (s && s.docs) s.docs.forEach(d => rec(d.ref)); return s; },
      getAll: (...refs) => { refs.forEach(rec); return F.db.getAll(...refs); },
      set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d))
    };
    const o = await fn(tx);
    if ([...reads].some(([p, c]) => canon(store.get(p)) !== c)) { retries++; continue; }
    if (arm > 0 && --arm === 0) { const e = new Error('14 UNAVAILABLE: No connection established'); e.code = 14; throw e; }
    await Promise.all(ops.map(f => f()));   // (each body runs at once: the commit is one step)
    return o;
  }
  throw new Error('10 ABORTED: too much contention on these documents');
};
const snapshot = () => canon([...store.entries()].filter(([k]) => !/^Charm_Nest_Rev\//.test(k)).sort(([a], [b]) => (a < b ? -1 : 1)));
const DAY = '2026-10-15', RUN1 = 'run-aaaa0001', RUN2 = 'run-bbbb0002';
const pid = (o, k) => `${4200000000 + o}_${9000000000 + o}_${k || 1}`;
const row = (o, run) => ({ poolId: pid(o), runId: run, orderId: String(4200000000 + o), state: 'pooled', quantity: 1, material: 'gold', metal: 'gold' });

(async () => {
  /* ── 1 · the door ── */
  for (const op of ['constructor', 'toString', '__proto__', 'hasOwnProperty', 'valueOf']) {
    const w = writes.length, r = await L({ op }), o = await O({ op });
    ok(r.status === 400 && /unknown op/.test(r.body.error) && o.status === 400 && /unknown op/.test(o.body.error) && writes.length === w, `"${op}" is no op on the library or the output function (400, nothing run)`);
  }
  { const r = await L({ op: ['ping'] }), o = await O({ op: { a: 1 } }); ok(r.status === 400 && o.status === 400, 'an op that is not a string is unknown too'); }
  for (const [label, body] of [['empty', ''], ['not JSON', '{not json'], ['an array', '[1,2]'], ['null', 'null'], ['a number', '7'], ['3 MB of junk', 'x'.repeat(3e6)], ['5,000 brackets deep', '['.repeat(5000) + ']'.repeat(5000)]]) {
    const r = await L(body), o = await O(body);
    ok(r.status === 400 && plain(r) && o.status === 400 && plain(o), `a body that is ${label}: plain JSON 400 with the CORS headers, no stack`);
  }
  for (const m of ['GET', 'PUT', 'DELETE', 'HEAD']) { const r = await out.handler({ httpMethod: m, headers: {}, body: '{"op":"url"}' }); ok(r.statusCode === 405 && JSON.parse(r.body).error && r.headers['Access-Control-Allow-Origin'] === '*', `charmNestOutput ${m}: 405 JSON with CORS`); }
  ok((await out.handler({ httpMethod: 'OPTIONS', headers: {} })).statusCode === 204 && (await lib.handler({ httpMethod: 'OPTIONS', headers: {} })).statusCode === 204, 'OPTIONS: 204 on both, no passcode asked');
  { process.env.EDIT_PASSCODE = 'pw-test-' + Date.now();
    let r = await L({ op: 'ping', calibration: false }); ok(r.status === 401 && r.body.error === 'unauthorized' && plain(r), 'a passcode is set: no header is a plain 401 with CORS');
    r = await L({ op: 'ping', calibration: false }, { headers: { 'X-Edit-Passcode': 'nope' } }); ok(r.status === 401, 'a wrong passcode is 401');
    r = await L({ op: 'ping', calibration: false }, { headers: { 'x-edit-passcode': process.env.EDIT_PASSCODE } }); ok(r.status === 200, 'the right passcode (header in any case) goes through');
    r = await O({ op: 'url', path: 'charmnest/a.png' }); ok(r.status === 401, 'the output function asks the passcode too');
    delete process.env.EDIT_PASSCODE; }
  { const p = 'charmnest/sheets/a.png', r = await O({ op: 'put', path: '../../etc/passwd', contentType: 'image/png', base64: Buffer.from('x').toString('base64') });
    ok(r.status === 200 && [...blobs.keys()].every(k => k.startsWith('charmnest/')) && !r.body.path.includes('..'), 'a storage path with ".." stays under charmnest/ (' + r.body.path + ')'); void p; }
  { const r = await O({ op: 'sign', path: 'charmnest/./master/files/x.ai' }), r2 = await O({ op: 'sign', path: 'charmnest/master/files/x.ai' }), r3 = await O({ op: 'sign', path: 'charmnest/sandbox/master/files/x.ai', sandbox: true });
    ok(r2.status === 401 && r3.status === 401 && r2.body.error === 'repo-locked', 'a master path (also in the sandbox) needs the repository token'); void r; }

  /* ── 2 · the registry ── */
  const names = Object.keys(lib.ops);
  const used = new Map();
  for (const f of fs.readdirSync(root).filter(f => /^charm-nest.*\.(js|html)$/.test(f))) {
    const s = fs.readFileSync(path.join(root, f), 'utf8'), re = /charmNestLibrary['"`]?\s*,\s*/g; let m;
    while ((m = re.exec(s))) { const om = /\bop\s*:\s*["'`]([A-Za-z0-9_]+)["'`]/.exec(s.slice(m.index, m.index + 400)); if (om) { if (!used.has(om[1])) used.set(om[1], f); } }
  }
  const missing = [...used.keys()].filter(k => !names.includes(k) && !RA.owns(k));
  ok(used.size > 60 && !missing.length, `every one of the ${used.size} ops the browser files send to charmNestLibrary is registered or owned by the password door${missing.length ? ' — MISSING: ' + missing.map(k => k + ' (' + used.get(k) + ')').join(', ') : ''}`);
  const outUsed = new Set(); for (const f of fs.readdirSync(root).filter(f => /^charm-nest.*\.(js|html)$/.test(f))) { const s = fs.readFileSync(path.join(root, f), 'utf8'), re = /charmNestOutput['"`]?\s*,\s*(?:Object\.assign\(\s*)?\{\s*op\s*:\s*["'`](\w+)["'`]/g; let m; while ((m = re.exec(s))) outUsed.add(m[1]); }
  ok(outUsed.size >= 3 && [...outUsed].every(k => ['sign', 'put', 'url', 'finalize'].includes(k)), 'every op the browser sends to charmNestOutput exists (' + [...outUsed].join(', ') + ')');
  const gated = Object.keys(RA.LIBRARY).concat(Object.keys(RA.SETTINGS_OPS));
  ok(gated.length >= 15 && gated.every(op => names.includes(op) || RA.owns(op)), `every gated op (${gated.length}) is registered or owned by the door`);
  let bad = [];
  for (const op of names) {
    const r = await L({ op }).catch(e => ({ status: 'THROW ' + e.message, raw: '', headers: {}, body: null }));
    const need = RA.requirement('charmNestLibrary', op, { op });
    if (need ? !(r.status === 401 && r.body.error === 'repo-locked') : !(r.status >= 200 && r.status < 600 && plain(r))) bad.push(op + ':' + r.status);
  }
  ok(!bad.length, `all ${names.length} registered ops answer a plain call with JSON (gated ones refuse without a token) and no stack${bad.length ? ' — ' + bad.join(', ') : ''}`);

  /* ── 3 · two benches ── */
  store.clear(); store.set('Charm_Nest_Runs/' + RUN1, { runId: RUN1, status: 'running', step: 'nest' }); store.set('Charm_Nest_Runs/' + RUN2, { runId: RUN2, status: 'running', step: 'nest' });
  const lines = Array.from({ length: 60 }, (_, i) => i + 1);
  retries = 0;
  const [pa, pb] = await Promise.all([L({ op: 'poolPut', pools: lines.map(i => row(i, RUN1)) }), L({ op: 'poolPut', pools: lines.map(i => row(i, RUN2)) })]);
  const toldA = new Set((pa.body.contended || []).map(c => c.poolId)), toldB = new Set((pb.body.contended || []).map(c => c.poolId));
  const owner = i => store.get('Charm_Pool/' + pid(i)).runId;
  ok(pa.status === 200 && pb.status === 200 && retries > 0, 'two runs pooling the same 60 new lines at the same moment: both answered, one of them had to read again (' + retries + ' retries)');
  ok(lines.every(i => (owner(i) === RUN1 ? toldB.has(pid(i)) && !toldA.has(pid(i)) : toldA.has(pid(i)) && !toldB.has(pid(i)))), 'each line has ONE owner, and the other run is told "contended" for it (a ' + toldA.size + ', b ' + toldB.size + ')');
  ok(pa.body.written + pb.body.written === 60, 'the 60 lines were written once in all (' + pa.body.written + ' + ' + pb.body.written + ')');
  { const again = await L({ op: 'poolPut', pools: lines.map(i => row(i, RUN1)).filter(p => owner(+p.orderId - 4200000000) === RUN1) }); ok(again.status === 200 && again.body.written > 0 && !(again.body.contended || []).length, 'the owner sending its lines again (a retry after a lost answer) writes them again without a clash'); }

  store.clear(); retries = 0;
  let rs = await Promise.all([1, 2, 3].map(() => L({ op: 'setAllocate', day: DAY, runId: RUN1, group: 'GF' })));
  ok(new Set(rs.map(r => r.body.setId)).size === 1 && [...store.keys()].filter(k => k.startsWith('Charm_Nest_Sets/')).length === 1, 'three requests for the same run\'s set at once: one set, one number');
  store.clear();
  rs = await Promise.all([L({ op: 'setAllocate', day: DAY, runId: RUN1, group: 'GF' }), L({ op: 'setAllocate', day: DAY, runId: RUN2, group: 'SS' })]);
  ok(new Set(rs.map(r => r.body.seq)).size === 2 && store.get('Charm_Nest_Counters/' + DAY).seq === 2, 'two runs asking for a set at once get two different numbers');
  store.clear();
  const sheet = { id: 'sheet-test-0001', metal: 'gold', day: DAY, runId: RUN1, poolIds: [pid(1)], charmCount: 1 };
  rs = await Promise.all([L({ op: 'putSheet', sheet }), L({ op: 'putSheet', sheet }), L({ op: 'putSheet', sheet: Object.assign({}, sheet, { charmCount: 2 }) })]);
  ok(rs.every(r => r.status === 200 && r.body.ok) && [...store.keys()].filter(k => k.startsWith('Charm_Nest_Sheets/')).length === 1, 'a sheet saved three times at once is one record');
  store.clear();
  const orders = Array.from({ length: 30 }, (_, i) => ({ id: String(4300000000 + i), createTs: 1 }));
  rs = await Promise.all([1, 2, 3].map(() => L({ op: 'arrivalRecord', orders })));
  ok(rs.every(r => r.status === 200 && r.body.count24 === 30) && [...store.keys()].filter(k => k.startsWith('Charm_Nest_Arrivals/')).length === 30, 'the same 30 orders arriving at three benches at once: 30 arrivals, counted once');

  /* ── 4 · the connection lost in the middle of a write ── */
  const lost = async (name, call, retry, check) => {
    store.clear(); blobs.clear(); const before = snapshot();
    arm = 1; const r = await call(); arm = 0;
    ok(r.status === 500 && typeof r.body.error === 'string' && plain(r), `${name}: the commit is refused -> a JSON 500 with the reason, no stack`);
    ok(snapshot() === before, `${name}: nothing half-written`);
    const again = await retry(); ok(again.status === 200 && (!check || check(again.body)), `${name}: the same request sent again works`);
  };
  await lost('putSheet', () => L({ op: 'putSheet', sheet }), () => L({ op: 'putSheet', sheet }), b => b.ok);
  await lost('setAllocate', () => L({ op: 'setAllocate', day: DAY, runId: RUN1, group: 'GF' }), () => L({ op: 'setAllocate', day: DAY, runId: RUN1, group: 'GF' }), b => b.seq === 1);
  await lost('poolPut', () => L({ op: 'poolPut', pools: lines.map(i => row(i, RUN1)) }), () => L({ op: 'poolPut', pools: lines.map(i => row(i, RUN1)) }), b => b.written === 60);
  { const orig = F.bucket.file; let fail = true; F.bucket.file = p => { const f = orig.call(F.bucket, p); return Object.assign({}, f, { async save() { if (fail) { const e = new Error('14 UNAVAILABLE: No connection established'); e.code = 14; throw e; } return f.save.apply(f, arguments); } }); };
    blobs.clear(); const r = await O({ op: 'put', path: 'charmnest/sheets/a.png', contentType: 'image/png', base64: Buffer.from('hello').toString('base64') });
    ok(r.status === 500 && plain(r) && !blobs.size, 'an upload whose Storage write fails: a JSON 500, no file left');
    fail = false; const r2 = await O({ op: 'put', path: 'charmnest/sheets/a.png', contentType: 'image/png', base64: Buffer.from('hello').toString('base64') }); ok(r2.status === 200 && blobs.has('charmnest/sheets/a.png'), 'and the same upload sent again is stored'); F.bucket.file = orig; }

  console.log('\nserver-hostile: ' + n + ' checks passed');
  process.exit(0);
})().catch(e => { console.error('FAIL ' + (e && e.stack || e)); process.exit(1); });
