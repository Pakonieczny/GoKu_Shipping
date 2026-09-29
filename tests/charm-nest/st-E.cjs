// Station tracking E, Nesting (Paul, 28 Sep): the Nested stamps the server makes from poolUpdate (placed as a sheet is
// saved, setCommitted as its set is committed) name the person on duty at the sorter, as its own sign-in keeps them
// (cn.employee: the sorter has no person login), or nobody: by "" with data.signedIn false, never "System".
//   1. The page: stampWho() and Pool.update(ids, patch, who) send { by | signedIn false, device } beside the patch
//      (never onto the pool rows), and both Nested writes pass it (the functions run in a vm, with fakes).
//   2. The server: the real charmNestLibrary handler against an in-memory Firestore; placed and setCommitted carry the
//      name, or by "" with data.signedIn false; a page that says neither keeps the old default. No network.
//   node tests/charm-nest/st-E.cjs
const path = require('path'), fs = require('fs'), vm = require('vm'), crypto = require('crypto'), assert = require('assert');
const root = path.join(__dirname, '../..');
delete process.env.EDIT_PASSCODE;

/* ── 1. the page ── */
{
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const grab = re => { const m = re.exec(src); assert(m, 'found in charm-nest-bridge.js: ' + re); return m[0]; };
  const code = [
    grab(/^const employeeName = .*$/m),
    grab(/^const stampWho = .*$/m),
    grab(/^ {2}async function update\(poolIds, patch, who\) .*$/m).trim()
  ].join('\n') + '\n;({ employeeName, stampWho, update })';
  const calls = [];
  const ctx = { B: { employee: '', link: null, pool: { rows: new Map([['4170000009_1_1', { poolId: '4170000009_1_1', state: 'pooled' }]]) } }, S: { cloud: { ok: true } }, api: async (fn, body) => { calls.push({ fn, body }); return { ok: true }; } };
  const run = vm.runInNewContext(code, ctx);
  (async () => {
    ctx.B.employee = 'Marco R.';
    assert.deepStrictEqual(JSON.parse(JSON.stringify(run.stampWho())), { by: 'Marco R.', device: 'charm-nest-1' });
    ctx.B.employee = ''; ctx.B.link = { state: () => ({ employee: 'Nia' }) };
    assert.strictEqual(run.stampWho().by, 'Nia', 'the name the Design Station hello gave, when the sorter has none of its own');
    ctx.B.link = null; ctx.B.employee = '   ';
    assert.deepStrictEqual(JSON.parse(JSON.stringify(run.stampWho())), { by: '', signedIn: false, device: 'charm-nest-1' }, 'nobody named: no guess');
    ctx.B.employee = 'Marco R.';
    await run.update(['4170000009_1_1'], { sheetId: 'sh1', state: 'written' }, run.stampWho());
    const b = calls.pop().body;
    assert(b.op === 'poolUpdate' && b.by === 'Marco R.' && b.device === 'charm-nest-1' && b.patch.sheetId === 'sh1' && !('by' in b.patch), JSON.stringify(b));
    assert(!('by' in ctx.B.pool.rows.get('4170000009_1_1')), 'who is not written onto the pool rows');
    await run.update(['4170000009_1_1'], { state: 'abandoned' });
    const b2 = calls.pop().body;
    assert(b2.op === 'poolUpdate' && !('by' in b2) && !('signedIn' in b2), 'a caller that passes no who sends what it did');
    await run.update(['4170000009_1_1'], { op: 'x' }, { op: 'nope', poolIds: [] , by: 'Z' });
    assert.strictEqual(calls.pop().body.op, 'poolUpdate', 'who never replaces the op or the ids');
    // both Nested writes pass the person on duty
    assert(/state: "written", sheetName: sh\.fileBase \}, stampWho\(\)\)/.test(src), 'onSheetSaved (placed) passes stampWho()');
    assert(/\{ state: "committed", committedAt: Date\.now\(\) \}, stampWho\(\)\)/.test(src), 'commit (setCommitted) passes stampWho()');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    assert(/charm-nest-bridge\.js\?v=\d{8}-/.test(html), 'the page loads the new bridge');
    console.log('page: ok');
  })().then(server).catch(e => { console.error(e); process.exit(1); });
}

/* ── 2. the server ── */
async function server() {
  const store = new Map(), SENT = { ts: { __ts: 1 }, del: { __del: 1 } };
  const ts = ms => ({ toMillis: () => ms });
  const FieldValue = { serverTimestamp: () => SENT.ts, delete: () => SENT.del, increment: n => ({ __inc: n }) };
  const plain = v => v && typeof v === 'object' && !Array.isArray(v) && !v.toMillis && !v.__inc && v !== SENT.ts && v !== SENT.del;
  const clone = v => Array.isArray(v) ? v.map(clone) : plain(v) ? Object.fromEntries(Object.entries(v).map(([k, x]) => [k, clone(x)])) : v;
  const value = (cur, v) => v === SENT.ts ? ts(Date.now()) : v && v.__inc != null ? (cur || 0) + v.__inc : clone(v);
  const merge = (t, s) => { for (const [k, v] of Object.entries(s)) { if (v === SENT.del) delete t[k]; else if (plain(v) && plain(t[k])) merge(t[k], v); else t[k] = plain(v) ? merge({}, v) : value(t[k], v); } return t; };
  const docRef = (coll, id) => {
    const key = coll + '/' + id;
    return { id, path: key, parent: { id: coll },
      async get() { const d = store.get(key); return { exists: !!d, id, ref: this, data: () => (d ? clone(d) : undefined) }; },
      async set(data, o) { store.set(key, merge(o && o.merge ? clone(store.get(key) || {}) : {}, data)); },
      async update(data) { store.set(key, merge(clone(store.get(key) || {}), data)); }, async delete() { store.delete(key); } };
  };
  const query = coll => ({ where() { return this; }, orderBy() { return this; }, limit() { return this; }, select() { return this; }, doc: id => docRef(coll, id || 'a' + crypto.randomBytes(6).toString('hex')), async get() { return { size: 0, docs: [], empty: true }; } });
  const db = { collection: c => query(c),
    batch() { const ops = []; return { set(r, d, o) { ops.push(() => r.set(d, o)); }, update(r, d) { ops.push(() => r.update(d)); }, delete(r) { ops.push(() => r.delete()); }, async commit() { for (const o of ops) await o(); } }; },
    async getAll(...refs) { if (refs.length && typeof refs[refs.length - 1].get !== 'function') refs.pop(); return Promise.all(refs.map(r => r.get())); } };
  const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue, FieldPath: { documentId: () => '__name__' }, Timestamp: { fromMillis: ts } }), storage: () => ({ bucket: () => ({ name: 'test', file: () => ({}) }) }) };
  const Module = require('module'), realLoad = Module._load;
  Module._load = function (req, ...rest) {
    if (req === 'node-fetch') return async () => { throw new Error('no network in tests'); };
    if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return fakeAdmin;
    return realLoad.call(this, req, ...rest);
  };
  const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
  const ok = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); assert.strictEqual(r.statusCode, 200, JSON.stringify(r.body)); return JSON.parse(r.body); };
  const ev = (rid, type) => { const l = [...store].filter(([k, v]) => k.startsWith('Order_Timeline/') && v.orderId === rid && v.type === type).map(([, v]) => v); assert.strictEqual(l.length, 1, `${type} of ${rid}: ${l.length}`); return l[0]; };
  const SET = 'set-2026-09-28-1', T = Date.now() - 60000;
  const rows = rid => { const ids = [rid + '_80001_1', rid + '_80002_1']; for (const id of ids) store.set('Charm_Pool/' + id, { poolId: id, orderId: rid, setId: SET, state: 'pooled' }); return ids; };
  const patch = { sheetId: 'sh-14k-1', setId: SET, state: 'written', sheetName: '14K_Sep.28.26_Set-1_Sheet-1' };

  // signed in on the sorter: placed and setCommitted name the person, at the sorter, from charm-nest-1
  const R1 = '4170000301', A = rows(R1);
  await ok({ op: 'poolUpdate', poolIds: A, patch, by: 'Marco R.', device: 'charm-nest-1' });
  const p1 = ev(R1, 'placed');
  assert(p1.by === 'Marco R.' && p1.station === 'sorter' && p1.device === 'charm-nest-1' && p1.milestone === true && p1.sheet === '14K Sheet 1' && p1.data.copies === 2 && !('signedIn' in p1.data), JSON.stringify(p1));
  await ok({ op: 'poolUpdate', poolIds: A, patch, by: 'Someone else', device: 'charm-nest-1' });
  assert.strictEqual(ev(R1, 'placed').by, 'Marco R.', 'the sheet saved again: one placed, its person kept');
  await ok({ op: 'poolUpdate', poolIds: A, patch: { state: 'committed', committedAt: T }, by: 'Ana P.', device: 'charm-nest-1' });
  const c1 = ev(R1, 'setCommitted');
  assert(c1.by === 'Ana P.' && c1.station === 'sorter' && c1.device === 'charm-nest-1' && c1.at === T && c1.text === 'Set 1', JSON.stringify(c1));

  // nobody signed in: by "" and data.signedIn false (the seal says "not signed in"), never "System"
  const R2 = '4170000302', B2 = rows(R2);
  await ok({ op: 'poolUpdate', poolIds: B2, patch, by: '', signedIn: false, device: 'charm-nest-1' });
  const p2 = ev(R2, 'placed');
  assert(p2.by === '' && p2.data.signedIn === false && p2.station === 'sorter', JSON.stringify(p2));
  await ok({ op: 'poolUpdate', poolIds: B2, patch: { state: 'committed', committedAt: T + 1 }, signedIn: false, device: 'charm-nest-1' });
  const c2 = ev(R2, 'setCommitted');
  assert(c2.by === '' && c2.data.signedIn === false, JSON.stringify(c2));

  // an older page that says neither keeps what it had (placed: "System")
  const R3 = '4170000303';
  await ok({ op: 'poolUpdate', poolIds: rows(R3), patch });
  const p3 = ev(R3, 'placed'); assert(p3.by === 'System' && p3.device === '' && !('signedIn' in p3.data), JSON.stringify(p3));
  // and the pool rows keep only their patch
  const row = store.get('Charm_Pool/' + A[0]); assert(!('by' in row) && !('signedIn' in row) && !('device' in row), JSON.stringify(row));
  console.log('server: ok');
  console.log('st-E: all passed');
}
