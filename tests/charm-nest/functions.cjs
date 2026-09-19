// Handler smoke test for the charmNest* Netlify functions against an in-memory
// Firestore/Storage fake injected in place of ./firebaseAdmin. No network.
//   node tests/charm-nest/functions.cjs
const path = require('path'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── in-memory Firestore ───────────────────────────────────────────────── */
const store = new Map();                       // "coll/id" → data
const SERVER_TS = { __ts: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }), delete: () => ({ __del: true }) };
function applyValues(target, src) { for (const [k, v] of Object.entries(src)) { if (v && v.__inc != null) target[k] = (target[k] || 0) + v.__inc; else if (v && v.__del) delete target[k]; else if (v === SERVER_TS) target[k] = { toMillis: () => Date.now() }; else target[k] = v; } return target; }
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key,
    async get() { const d = store.get(key); return { exists: !!d, id, data: () => (d ? { ...d } : undefined) }; },
    collection(sub) { return query(coll + '/' + id + '/' + sub); },
    async set(data, opts) { const cur = (opts && opts.merge && store.get(key)) || {}; store.set(key, applyValues({ ...cur }, data)); },
    async delete() { store.delete(key); }
  };
}
function query(coll, filters = [], order = null, lim = 0) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim); },
    limit(n) { return query(coll, filters, order, n); },
    select() { return q; },
    async get() {
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), data: () => ({ ...v }), ref: docRef(coll, k.slice(coll.length + 1)) }));
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = r.data()[f]; return op === '==' ? x === v : op === '>=' ? x >= v : op === '<=' ? x <= v : true; });
      if (order) rows.sort((a, b) => { const x = a.data()[order[0]], y = b.data()[order[0]]; const c = (x && x.toMillis ? x.toMillis() : x) > (y && y.toMillis ? y.toMillis() : y) ? 1 : -1; return order[1] === 'desc' ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      return { size: rows.length, docs: rows, empty: !rows.length };
    },
    async add(data) { const id = 'auto' + Math.random().toString(36).slice(2, 8); await docRef(coll, id).set(data); return docRef(coll, id); },
    doc(id) { return docRef(coll, id || 'auto' + Math.random().toString(36).slice(2, 10)); }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, data, opts) { ops.push(() => ref.set(data, opts)); }, delete(ref) { ops.push(() => ref.delete()); }, async commit() { for (const o of ops) await o(); } }; },
  async getAll(...refs) { return Promise.all(refs.map(r => r.get())); },
  async runTransaction(fn) { return fn({ get: ref => ref.get(), set: (ref, data, opts) => ref.set(data, opts) }); }
};
/* ── in-memory Storage ─────────────────────────────────────────────────── */
const blobs = new Map();
const bucket = {
  name: 'test-bucket',
  file(p) {
    return {
      async save(buf, opts) { blobs.set(p, { buf, meta: { contentType: opts.contentType, size: buf.length, metadata: (opts.metadata && opts.metadata.metadata) || {} } }); },
      async exists() { return [blobs.has(p)]; },
      async download() { return [blobs.get(p).buf]; },
      async delete() { blobs.delete(p); },
      async getMetadata() { const b = blobs.get(p); return [{ contentType: b.meta.contentType, size: b.meta.size, metadata: b.meta.metadata }]; },
      async setMetadata(m) { const b = blobs.get(p) || { buf: Buffer.alloc(0), meta: { metadata: {} } }; if (m.contentType) b.meta.contentType = m.contentType; Object.assign(b.meta.metadata, m.metadata || {}); blobs.set(p, b); },
      async getSignedUrl(o) { return ['https://storage.example/' + p + '?sig=' + o.action]; }
    };
  }
};
const fakeAdmin = { firestore: Object.assign(() => db, { FieldValue }), storage: () => ({ bucket: () => bucket }) };
require.cache[require.resolve(path.join(fnDir, 'firebaseAdmin.js'))] = { id: 'fake', filename: 'firebaseAdmin.js', loaded: true, exports: fakeAdmin };
// node-fetch is only used to kick the background function; stub it
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => ({ ok: true, status: 202, text: async () => '' });
  if (req === 'firebase-admin') return fakeAdmin;                   // _etsyMailAnthropic requires it for context fetching
  if (/[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fakeAdmin;
  return realLoad.call(this, req, ...rest);
};

const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const out = require(path.join(fnDir, 'charmNestOutput.js'));
const name = require(path.join(fnDir, 'charmNestName.js'));
const solve = require(path.join(fnDir, 'charmNestSolve-background.js'));
const post = (h, body, headers = {}) => h.handler({ httpMethod: 'POST', headers, body: JSON.stringify(body) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body || '{}') }));

(async () => {
  // ── gate ──
  process.env.EDIT_PASSCODE = 'secret';
  let r = await post(lib, { op: 'ping' }); assert.strictEqual(r.status, 401, 'locked without passcode');
  r = await post(lib, { op: 'ping' }, { 'x-edit-passcode': 'secret' }); assert.strictEqual(r.status, 200, 'passcode accepted');
  delete process.env.EDIT_PASSCODE;
  r = await post(lib, { op: 'nope' }); assert.strictEqual(r.status, 400);

  // ── charm library ──
  const H1 = 'abcdef0123456789', H2 = '0123456789abcdef';
  r = await post(lib, { op: 'putCharms', charms: [{ hash: H1, name: 'compass-rose', namedBy: 'claude', widthPt: 40, heightPt: 40, areaPt2: 900 }, { hash: H2, name: 'x-01', namedBy: 'fallback' }] });
  assert.strictEqual(r.body.count, 2);
  r = await post(lib, { op: 'putCharms', charms: [{ hash: H1, name: 'file-03', namedBy: 'fallback' }] }); // a fallback name must not overwrite Claude's
  r = await post(lib, { op: 'lookupCharms', hashes: [H1, H2, 'zz'] });
  assert.strictEqual(r.body.charms[H1].name, 'compass-rose', 'rank protection'); assert.strictEqual(r.body.charms[H1].timesUsed, 2);
  r = await post(lib, { op: 'renameCharm', hash: H1, name: 'Compass Rose' }); assert.strictEqual(r.status, 200);
  r = await post(lib, { op: 'putCharms', charms: [{ hash: H1, name: 'from-model', namedBy: 'claude' }] });
  r = await post(lib, { op: 'lookupCharms', hashes: [H1] }); assert.strictEqual(r.body.charms[H1].name, 'Compass Rose', 'operator name wins');
  r = await post(lib, { op: 'listCharms', q: 'compass' }); assert.strictEqual(r.body.charms.length, 1);

  // ── sheets ──
  r = await post(lib, { op: 'putSheet', sheet: { id: 'gold-abc123', metal: 'gold', metalLabel: 'GF 14/20', day: '2026-09-15', status: 'complete', charmCount: 21, placedCount: 21, density: 0.71, outputs: { preview: { url: 'u' } }, names: 'compass rose axolotl' } });
  assert.strictEqual(r.status, 200);
  await post(lib, { op: 'putSheet', sheet: { id: 'silver-def456', metal: 'silver', metalLabel: 'SS', day: '2026-09-14', status: 'partial', charmCount: 10, placedCount: 8 } });
  r = await post(lib, { op: 'listSheets' }); assert.strictEqual(r.body.sheets.length, 2);
  r = await post(lib, { op: 'listSheets', metal: 'gold' }); assert.strictEqual(r.body.sheets.length, 1); assert.strictEqual(r.body.sheets[0].preview, 'u');
  r = await post(lib, { op: 'listSheets', from: '2026-09-15' }); assert.strictEqual(r.body.sheets.length, 1);
  r = await post(lib, { op: 'getSheet', id: 'gold-abc123' }); assert.strictEqual(r.body.sheet.names, 'compass rose axolotl');
  r = await post(lib, { op: 'deleteSheet', id: 'silver-def456', code: '000000' }); assert.strictEqual(r.status, 403, 'wrong passcode refused'); r = await post(lib, { op: 'deleteSheet', id: 'silver-def456', code: '975311' }); r = await post(lib, { op: 'listSheets' }); assert.strictEqual(r.body.sheets.length, 1, 'deleted sheet gone');
  r = await post(lib, { op: 'putCalibration', row: { sheetId: 'gold-abc123', metal: 'gold', count: 21, cv: 0.4, largestFrac: 0.12, density: 0.713, placedAll: true } }); assert.strictEqual(r.status, 200);
  r = await post(lib, { op: 'ping' }); assert.strictEqual(r.body.sheets, 1); assert.strictEqual(r.body.calibration.length, 1);

  // ── storage ──
  r = await post(out, { op: 'sign', path: 'sheets/2026-09-15/gold-abc123/gold-abc123.ai', contentType: 'application/pdf' });
  assert(r.body.uploadUrl && r.body.path.startsWith('charmnest/') && r.body.token, 'signed');
  r = await post(out, { op: 'put', path: '../etc/passwd', contentType: 'application/json', base64: Buffer.from('{"a":1}').toString('base64') });
  assert.strictEqual(r.status, 200); assert(r.body.path.startsWith('charmnest/') && !r.body.path.includes('..'), 'path confined: ' + r.body.path);
  await bucket.file('charmnest/x.pdf').save(Buffer.from('%PDF'), { contentType: 'application/pdf', metadata: { metadata: {} } });
  r = await post(out, { op: 'finalize', path: 'charmnest/x.pdf', token: 'tok', contentType: 'application/pdf' }); assert(r.body.url.includes('token=tok'));
  r = await post(out, { op: 'url', path: 'charmnest/x.pdf' }); assert(r.body.url.includes('token=tok'));
  r = await post(out, { op: 'url', path: 'charmnest/missing' }); assert.strictEqual(r.status, 400);

  // ── naming guards (no API key → skipped, not failed) ──
  delete process.env.ANTHROPIC_API_KEY;
  r = await post(name, { charms: [{ index: 0, thumb: 'data:image/png;base64,iVBORw0KGgo=' }] }); assert.strictEqual(r.status, 200); assert(r.body.skipped);
  process.env.ANTHROPIC_API_KEY = 'x';
  r = await post(name, { charms: [] }); assert.strictEqual(r.status, 400);
  delete process.env.ANTHROPIC_API_KEY;

  // ── AI review guards ──
  const review = require(path.join(fnDir, 'charmNestReview.js'));
  delete process.env.ANTHROPIC_API_KEY;
  r = await post(review, { mode: 'grouping', overview: 'data:image/jpeg;base64,/9j/', charms: [] }); assert.strictEqual(r.status, 200); assert(r.body.skipped, 'no key → skipped');
  process.env.ANTHROPIC_API_KEY = 'x';
  r = await post(review, { mode: 'grouping', charms: [{ index: 0, thumb: 'data:image/png;base64,iVBORw0KGgo=' }] }); assert.strictEqual(r.status, 400, 'overview required');
  r = await post(review, { mode: 'layout', placements: [] }); assert.strictEqual(r.status, 400, 'preview required');
  const agentMod = require(path.join(fnDir, '_charmNestAgent.js'));
  const pr = agentMod.buildRequest('place', { wIn: 7.125, hIn: 6.03, insetIn: 0.02, clearancePt: -0.5, round: 1, maxRounds: 10, sheet: 'data:image/jpeg;base64,/9j/', pockets: [{ wIn: 7, hIn: 6, xIn: 0, yIn: 0 }], placed: [], remaining: [{ id: 'a:0', name: 'compass', thumb: 'data:image/png;base64,iVBORw0KGgo=', wIn: 2.8, hIn: 2.8, areaIn2: 4.1 }] });
  assert(pr.system && pr.schema && pr.content.length >= 4, 'place request built');
  assert(agentMod.buildRequest('place', { remaining: [] }).error, 'place needs the sheet image');
  delete process.env.ANTHROPIC_API_KEY;

  // ── agent job: startAgent → background (model stubbed) → getAgent ──
  const agentBg = require(path.join(fnDir, 'charmNestAgent-background.js'));
  const anthro = require(path.join(fnDir, '_etsyMailAnthropic.js'));
  const realCall = anthro.callClaudeRaw;
  anthro.callClaudeRaw = async (o) => ({ stop_reason: 'end_turn', usage: { input_tokens: 10, output_tokens: 5 }, content: [{ type: 'text', text: JSON.stringify(o.output_config || o.outputFormat ? { verdicts: [{ index: 0, verdict: 'complete', mergeInto: null, note: '' }], realCharmCount: 1, summary: 'one charm, fine' } : {}) }] });
  process.env.ANTHROPIC_API_KEY = 'x';
  r = await post(lib, { op: 'startAgent', mode: 'grouping', payload: { sourceName: 't', overview: 'data:image/jpeg;base64,/9j/', charms: [{ index: 0, thumb: 'data:image/png;base64,iVBORw0KGgo=' }] } });
  assert(r.body.id, 'agent id: ' + JSON.stringify(r.body));
  const agentId = r.body.id;
  assert(blobs.has('charmnest/agent/' + agentId + '.json'), 'payload parked in storage');
  const bgr = await agentBg.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: agentId, mode: 'grouping' }) });
  assert.strictEqual(bgr.statusCode, 200);
  r = await post(lib, { op: 'getAgent', id: agentId });
  assert.strictEqual(r.body.job.status, 'done'); assert.strictEqual(r.body.job.result.realCharmCount, 1, JSON.stringify(r.body.job));
  assert(!blobs.has('charmnest/agent/' + agentId + '.json'), 'parked payload cleaned up');
  anthro.callClaudeRaw = realCall; delete process.env.ANTHROPIC_API_KEY;

  // ── server solver: startJob → background → done ──
  const S = require(path.join(__dirname, '../../charm-nest-solver.js'));
  const pack = bits => { const o = new Uint8Array(Math.ceil(bits.length / 8)); for (let i = 0; i < bits.length; i++) if (bits[i]) o[i >> 3] |= 1 << (i & 7); return o; };
  const piece = (id, wPt, hPt) => { const s = 6, w = Math.round(wPt * s), h = Math.round(hPt * s), bits = new Uint8Array(w * h).fill(1); return { id, w, h, scale: s, packed: true, bits: Buffer.from(pack(bits)).toString('base64'), areaPt2: wPt * hPt }; };
  const job = { sheet: { wPt: 200, hPt: 150, insetPt: 1.5 }, clearancePt: 0.5, angles: [0, 90], timeBudgetMs: 20000, seed: 2, pieces: [piece('a', 60, 40), piece('b', 50, 50), piece('c', 70, 30), piece('d', 30, 30)] };
  r = await post(lib, { op: 'startJob', sheetId: 'gold-abc123', job }); assert(r.body.id, 'job id');
  const jobId = r.body.id;
  const bg = await solve.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: jobId, job }) });
  assert.strictEqual(bg.statusCode, 200);
  r = await post(lib, { op: 'getJob', id: jobId });
  assert.strictEqual(r.body.job.status, 'done', JSON.stringify(r.body.job).slice(0, 300));
  assert.strictEqual(r.body.job.result.placements.length, 4, 'all four placed on the server');
  assert(r.body.job.result.verification.ok, 'server verification');
  const solved = r.body.job.result;
  void S;

  // ── bridge ops (design §13): master index with duplicate blocking, pool contention, set numbering per date, runs, bridge log, maps ──
  r = await post(lib, { op: 'masterPutIndex', masterHash: 'aaaa1111', masterName: 'A.ai', entries: [{ sku: 'BR-CMP-01', charmHash: 'abcdef01', widthPt: 40, heightPt: 50, areaPt2: 1200, members: 5, holes: 1, engravable: true, upAngle: 88, aiPath: 'charmnest/master/BR-CMP-01.ai' }, { sku: 'BR-SZD-02', size: 'S', charmHash: 'abcdef02', widthPt: 20, heightPt: 20, aiPath: 'charmnest/master/BR-SZD-02__S.ai' }, { sku: 'BR-SZD-02', size: 'M', charmHash: 'abcdef03', widthPt: 30, heightPt: 30, aiPath: 'charmnest/master/BR-SZD-02__M.ai' }] });
  assert.strictEqual(r.body.written, 2, 'two SKUs, one of them sized');
  r = await post(lib, { op: 'masterGet', sku: 'br-szd-02' }); assert.deepStrictEqual(Object.keys(r.body.entry.sizes).sort(), ['M', 'S'], 'sizes recorded'); assert.strictEqual(r.body.entry.upAngle, null);
  r = await post(lib, { op: 'masterPatch', sku: 'BR-CMP-01', patch: { engravable: false } }); r = await post(lib, { op: 'masterGet', sku: 'BR-CMP-01' }); assert.strictEqual(r.body.entry.engravable, false); assert.strictEqual(r.body.entry.engravableBy, 'operator');
  r = await post(lib, { op: 'masterPutIndex', masterHash: 'aaaa1111', masterName: 'A.ai', entries: [{ sku: 'BR-CMP-01', charmHash: 'abcdef01', widthPt: 40, heightPt: 50, engravable: true }] });
  r = await post(lib, { op: 'masterGet', sku: 'BR-CMP-01' }); assert.strictEqual(r.body.entry.engravable, false, 'a re-index keeps the operator override');
  r = await post(lib, { op: 'masterPutIndex', masterHash: 'bbbb2222', masterName: 'B.ai', entries: [{ sku: 'BR-CMP-01', charmHash: 'abcdef09', widthPt: 44, heightPt: 50 }] });
  assert.strictEqual(r.body.blocked.length, 1, 'the same SKU in a second master is blocked'); assert.strictEqual(r.body.sizeMoved.length, 1, 'and its width moved 10%');
  r = await post(lib, { op: 'masterGet', sku: 'BR-CMP-01' }); assert(/two master files/.test(r.body.entry.blocked));
  r = await post(lib, { op: 'masterPatch', sku: 'BR-CMP-01', patch: { blocked: null } }); r = await post(lib, { op: 'masterGet', sku: 'BR-CMP-01' }); assert.strictEqual(r.body.entry.blocked, null, 'operator unblocks');
  r = await post(lib, { op: 'masterPutFile', file: { masterHash: 'aaaa1111', name: 'A.ai', charms: 3, labelled: 2, unlabelled: [2], orphans: [], duplicates: [] } }); assert.strictEqual(r.status, 200);
  r = await post(lib, { op: 'masterListFiles' }); assert.strictEqual(r.body.files.length, 1);
  r = await post(lib, { op: 'poolPut', pools: [{ poolId: '3521337740_4412778001_1', runId: 'run-A', orderId: '3521337740', sku: 'BR-CMP-01', material: 'gold', copy: 1, quantity: 1, state: 'ready' }] }); assert.strictEqual(r.body.written, 1);
  r = await post(lib, { op: 'poolPut', pools: [{ poolId: '3521337740_4412778001_1', runId: 'run-B', orderId: '3521337740', sku: 'BR-CMP-01', material: 'gold', copy: 1, quantity: 1, state: 'ready' }] }); assert.strictEqual(r.body.contended.length, 1, 'a second run contending for the line is refused'); assert.strictEqual(r.body.contended[0].runId, 'run-A');
  // …unless the run holding the line was stopped or given up: then the line is anyone's again
  r = await post(lib, { op: 'runPut', run: { runId: 'run-A', day: '2026-09-16', status: 'stopped', step: 'claim', lines: {} } });
  r = await post(lib, { op: 'poolPut', pools: [{ poolId: '3521337740_4412778001_1', runId: 'run-B', orderId: '3521337740', sku: 'BR-CMP-01', material: 'gold', copy: 1, quantity: 1, state: 'ready' }] }); assert.strictEqual(r.body.contended.length, 0, 'a stopped run does not hold its lines against a new one'); assert.strictEqual(r.body.written, 1);
  r = await post(lib, { op: 'poolUpdate', poolIds: ['3521337740_4412778001_1'], patch: { runId: 'run-A' } });   // back to run-A for the rest of the checks
  r = await post(lib, { op: 'runPut', run: { runId: 'run-A', day: '2026-09-16', status: 'running', step: 'pool', lines: {} } });
  r = await post(lib, { op: 'poolUpdate', poolIds: ['3521337740_4412778001_1'], patch: { state: 'written', sheetId: 'gold-x' } }); r = await post(lib, { op: 'poolList', runId: 'run-A' }); assert.strictEqual(r.body.pools[0].state, 'written');
  r = await post(lib, { op: 'setAllocate', day: '2026-09-16', runId: 'run-A' }); assert.strictEqual(r.body.seq, 1); const setA = r.body.setId;
  r = await post(lib, { op: 'setAllocate', day: '2026-09-16', runId: 'run-B' }); assert.strictEqual(r.body.seq, 2, 'the next set of the same day is Set-2');
  r = await post(lib, { op: 'setAllocate', day: '2026-09-16', runId: 'run-A' }); assert.strictEqual(r.body.seq, 1); assert(r.body.existing, 'idempotent per run');

  r = await post(lib, { op: 'setAllocate', day: '2026-09-17', runId: 'run-C' }); assert.strictEqual(r.body.seq, 1, 'numbering restarts per date');
  // one set per kin group of a run: the SS set and the GF+14K set of one run are two numbers, asked for twice they are the same two,
  // and the next run of the day counts on from there — never renumbering, never reusing
  r = await post(lib, { op: 'setAllocate', day: '2026-09-17', runId: 'run-D', group: 'silver' }); const dSS = r.body; assert.strictEqual(dSS.seq, 2, 'the day counter carries on: ' + JSON.stringify(dSS));
  r = await post(lib, { op: 'setAllocate', day: '2026-09-17', runId: 'run-D', group: 'gold+gold14k' }); const dGF = r.body; assert.strictEqual(dGF.seq, 3, 'a second kin group of the same run is its own set');
  r = await post(lib, { op: 'setAllocate', day: '2026-09-17', runId: 'run-D', group: 'silver' }); assert(r.body.existing && r.body.setId === dSS.setId, 'asked again, the same set');
  r = await post(lib, { op: 'setAllocate', day: '2026-09-17', runId: 'run-E', group: 'silver' }); assert.strictEqual(r.body.seq, 4, 'a later run the same day is the next number, whatever the earlier one made');
  // the release record is shop-wide and validated
  r = await post(lib, { op: 'releaseGet' }); assert.deepStrictEqual(r.body.lastReleased, {}, 'nothing released yet');
  r = await post(lib, { op: 'releasePut', lastReleased: { rose: '2026-09-17' }, released: { gold14k: '2026-09-18' } }); assert.strictEqual(r.body.lastReleased.rose, '2026-09-17'); assert.strictEqual(r.body.released.gold14k, '2026-09-18');
  r = await post(lib, { op: 'releasePut', lastReleased: { rose: 'yesterday' } }); assert.strictEqual(r.status, 400, 'a date that is not a date is refused');
  r = await post(lib, { op: 'setUpdate', setId: setA, patch: { status: 'labelled', sheetIds: ['gold-x', 'silver-y'], materials: ['gold', 'silver'] } }); r = await post(lib, { op: 'setGet', setId: setA }); assert.deepStrictEqual(r.body.set.materials, ['gold', 'silver']);
  r = await post(lib, { op: 'setList', from: '2026-09-16', to: '2026-09-16' }); assert.strictEqual(r.body.sets.length, 2);
  r = await post(lib, { op: 'runPut', run: { runId: 'run-A', step: 'nest', status: 'running', day: '2026-09-16', lines: { a: { state: 'pooled' } } } }); r = await post(lib, { op: 'runGet', runId: 'run-A' }); assert.strictEqual(r.body.run.step, 'nest');
  r = await post(lib, { op: 'runList' }); assert(r.body.runs.some(x => x.runId === 'run-A' && x.lines === 1));
  // a calibration row is a statistic, so a sheet with nothing to teach is skipped, never refused: the refusal used to
  // travel up through the save and stop a run whose sheets were already written, verified and on record
  r = await post(lib, { op: 'putCalibration', row: { sheetId: 'gold-x', metal: 'gold', density: 0, count: 0 } });
  assert.strictEqual(r.status, 200, 'an empty calibration row is not an error'); assert(r.body.ok && r.body.skipped, 'and it says it was skipped');
  r = await post(lib, { op: 'putCalibration', row: { sheetId: 'gold-x', metal: 'gold', density: 0.61, count: 40, cv: 0.4, largestFrac: 0.1 } });
  assert(r.body.ok && !r.body.skipped, 'a real one is kept'); r = await post(lib, { op: 'getCalibration' }); assert(r.body.rows.some(x => x.sheetId === 'gold-x' && x.count === 40), 'the real one is on record'); assert(!r.body.rows.some(x => !(x.count > 0)), 'and no empty row ever was');
  // history: one search over every run and sheet on record, and an honest count of how far it looked
  await post(lib, { op: 'runPut', run: { runId: 'run-H', step: 'commit', status: 'complete', day: '2026-09-15', seq: 4, setId: 'set-2026-09-15-4', lines: { 'k1': { state: 'pooled', orderId: '9911', sku: 'BR-HIS-01', engrave: { text: 'MAEVE' } } } } });
  r = await post(lib, { op: 'history' }); assert(r.body.runs.some(x => x.runId === 'run-H' && x.seq === 4), 'every run is listed');
  assert(r.body.scanned && r.body.scanned.runs >= 2, 'and it says how many it read: ' + JSON.stringify(r.body.scanned));
  r = await post(lib, { op: 'history', q: '9911' }); assert.strictEqual(r.body.runs.length, 1, 'found by order number'); assert.deepStrictEqual(r.body.runs[0].hitOrders, ['9911']);
  r = await post(lib, { op: 'history', q: 'maeve' }); assert.strictEqual(r.body.runs.length, 1, 'found by the words that were engraved');
  r = await post(lib, { op: 'history', q: 'br-his' }); assert.deepStrictEqual(r.body.runs[0].hitSkus, ['BR-HIS-01'], 'found by SKU');
  r = await post(lib, { op: 'history', q: 'nothing-like-this' }); assert.strictEqual(r.body.runs.length, 0, 'and it does not invent matches');
  // Arrival counts use first import, not refreshes, lines, or midnight boundaries.
  let arrivals = await post(lib, { op: 'arrivalRecord', orders: [{ id: '7001', createTs: 123 }, { id: '7001', createTs: 123 }, { id: '7002', createTs: 456 }] });
  assert.equal(arrivals.body.count24, 2); assert.equal(arrivals.body.count1, 2);
  const firstImport = arrivals.body.firstSeen['7001'];
  arrivals = await post(lib, { op: 'arrivalRecord', orders: [{ id: '7001', createTs: 999 }] });
  assert.equal(arrivals.body.firstSeen['7001'], firstImport); assert.equal(arrivals.body.count24, 2);
  assert.equal(store.get('Charm_Nest_Arrivals/7001').createTs, 123, 'Etsy order date is immutable');
  store.set('Charm_Nest_Arrivals/7001', { id:'7001', createTs:123, firstSeenAt:Date.now()-2*3600000 });
  store.set('Charm_Nest_Arrivals/7002', { id:'7002', createTs:456, firstSeenAt:Date.now()-25*3600000 });
  arrivals = await post(lib, { op:'arrivalRecord', orders:[] });
  assert.equal(arrivals.body.count24,1); assert.equal(arrivals.body.count1,0);
  arrivals = await post(lib, { op:'arrivalRecord', sandbox:true, orders:[{id:'7001',createTs:123}] });
  assert.equal(arrivals.body.count24,1); assert.equal(arrivals.body.count1,1,'sandbox has its own ledger');

  // Run creation date stays stable across ordinary checkpoints.
  store.get('Charm_Nest_Runs/run-H').createdAt = 12345;
  await post(lib, { op:'runPut', run:{ runId:'run-H', status:'complete' }, merge:true });
  r = await post(lib, { op:'runGet', runId:'run-H' }); assert.equal(r.body.run.createdAt,12345);
  // Search must include a set's whole membership, beyond the old 200-sheet window.
  for(let i=0;i<205;i++) store.set('Charm_Nest_Sheets/recent-'+i,{id:'recent-'+i,day:'2026-09-19',metal:'gold',status:'complete',updatedAt:99999+i});
  store.set('Charm_Nest_Sets/history-old',{setId:'history-old',seq:9,day:'2026-09-01',runId:'run-old',status:'complete',orders:{'8100':{},'8101':{}}});
  store.set('Charm_Nest_Runs/run-old',{runId:'run-old',day:'2026-09-01',status:'complete',lines:{a:{orderId:'8100',sku:'FIND-OLDER',engrave:{text:'remember me'}},b:{orderId:'8101'}}});
  for(let i=0;i<2;i++) store.set('Charm_Nest_Sheets/old-'+i,{id:'old-'+i,setId:'history-old',setSeq:9,runId:'run-old',day:'2026-09-01',metal:'gold',status:'complete',orders:[String(8100+i)],sheetIndex:i+1,updatedAt:1});
  r=await post(lib,{op:'history',q:'remember me',limit:1});
  assert.equal(r.body.sets.length,1);assert.equal(r.body.sets[0].sheets.length,2,'a match returns the whole set');assert.equal(r.body.sets[0].orders,2);
  r=await post(lib,{op:'listSheets',runId:'run-old',limit:10});assert.equal(r.body.sheets.length,2,'scope before limit');
  r=await post(lib,{op:'history',limit:2});assert.equal(r.body.sets.length,2);assert.equal(r.body.nextOffset,2);assert.equal(r.body.sets[0].day,'2026-09-19');
  r=await post(lib,{op:'history',q:'set 9'});assert(r.body.sets.some(s=>s.setId==='history-old'));
  // Allocation retries reuse the same number, but a different run gets a new number.
  const a1=await post(lib,{op:'setAllocate',day:'2026-09-19',runId:'run-test1',group:'gold'});
  const a2=await post(lib,{op:'setAllocate',day:'2026-09-19',runId:'run-test1',group:'gold'});
  const a3=await post(lib,{op:'setAllocate',day:'2026-09-19',runId:'run-test2',group:'gold'});
  assert.equal(a1.body.setId,a2.body.setId);assert.equal(a3.body.seq,a1.body.seq+1);
  // Intake can retire only an open run's sheet, never another run or finished production.
  r=await post(lib,{op:'archiveEmptySheet',id:'old-0',runId:'run-other'});assert.equal(r.status,400);
  r=await post(lib,{op:'archiveEmptySheet',id:'old-0',runId:'run-old'});assert.equal(r.status,400);
  for(const key of [...store.keys()]) if (/^(Charm_Nest_Sheets\/(recent-|old-)|Charm_Nest_Sets\/history-old|Charm_Nest_Runs\/run-old)/.test(key)) store.delete(key);
  r = await post(lib, { op: 'bridgeLog', session: 'k3f9a2xyz', rows: [{ t: 1, dir: 'cmd', type: 'hello' }, { t: 2, dir: 'reply', type: 'hello', ms: 12 }], meta: { bench: 'design-1' } }); assert.strictEqual(r.body.rows, 2);
  assert.strictEqual([...store.keys()].filter(k => k.startsWith('Design_Bridge/k3f9a2xyz/log/')).length, 2, 'two log rows under the session');
  r = await post(lib, { op: 'aliasPut', listingId: '1718', sku: 'BR-CMP-01', by: 'Ana' }); r = await post(lib, { op: 'aliasGet' }); assert.strictEqual(r.body.aliases['1718'].sku, 'BR-CMP-01');
  r = await post(lib, { op: 'noDesignPut', pattern: '^CHAIN' }); r = await post(lib, { op: 'noDesignPut', sku: 'BOX-01' }); r = await post(lib, { op: 'noDesignPut', pattern: '(' }); assert.strictEqual(r.status, 400, 'a bad pattern is refused');
  r = await post(lib, { op: 'noDesignGet' }); assert.deepStrictEqual(r.body.list.patterns, ['^CHAIN']); assert.deepStrictEqual(r.body.list.skus, ['BOX-01']);
  r = await post(lib, { op: 'optionMapPut', listingId: '1718', optionName: 'Style', optionValue: 'Charm & Chain', map: { field: 'form', value: 'necklace' }, by: 'Ana' }); r = await post(lib, { op: 'optionMapGet' }); assert.strictEqual(r.body.maps['1718'].style['charm & chain'].value, 'necklace');
  r = await post(lib, { op: 'startAgent', mode: 'engraveIntent', payload: { order: '1' } }); assert(r.body.id, 'engrave modes start through the same op');
  const eng = require(path.join(fnDir, 'charmEngrave-background.js'));
  anthro.callClaudeRaw = async () => ({ stop_reason: 'end_turn', usage: {}, content: [{ type: 'text', text: JSON.stringify({ engrave: true, text: 'ANNA\n9.26.25', source: 'personalization', sourceQuote: 'ANNA 9.26.25', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.97 }) }] });
  process.env.ANTHROPIC_API_KEY = 'x';
  await eng.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify({ id: r.body.id, mode: 'engraveIntent' }) });
  r = await post(lib, { op: 'getAgent', id: r.body.id }); assert.strictEqual(r.body.job.result.text, 'ANNA\n9.26.25'); assert.strictEqual(r.body.job.result.confidence, 0.97);
  const req = agentMod.buildRequest('labelRead', { sourceName: 'm', strips: [{ index: 0, image: 'data:image/png;base64,iVBORw0KGgo=' }] }); assert(req.schema && req.content.length === 3);
  assert(agentMod.buildRequest('engraveReview', {}).error, 'engraveReview needs the back image');
  anthro.callClaudeRaw = realCall; delete process.env.ANTHROPIC_API_KEY;
  console.log('bridge ops OK');
  // ── purge: the records of past runs go, in production and sandbox; the master index and maps stay ──
  r = await post(lib, { op: 'purgeHistory', code: '000000' }); assert.strictEqual(r.status, 403, 'a purge needs the passcode');
  r = await post(lib, { op: 'purgeHistory', code: '975311' }); assert.strictEqual(r.status, 409, 'a purge is refused while a run is open: ' + JSON.stringify(r.body));
  r = await post(lib, { op: 'runPut', run: { runId: 'run-A', day: '2026-09-16', status: 'stopped', step: 'pool', lines: {} } });
  r = await post(lib, { op: 'purgeHistory', code: '975311' }); assert(r.body.ok && r.body.docs.Charm_Pool >= 1 && r.body.docs.Charm_Nest_Sets >= 1, 'the run records were wiped: ' + JSON.stringify(r.body.docs));
  r = await post(lib, { op: 'poolList', runId: 'run-A' }); assert.strictEqual(r.body.pools.length, 0, 'no pool rows remain');
  r = await post(lib, { op: 'masterGet', sku: 'BR-CMP-01' }); assert(r.body.entry, 'the master index is not history and stays');
  r = await post(lib, { op: 'setAllocate', day: '2026-09-16', runId: 'run-A' }); assert.strictEqual(r.body.seq, 1, 'set numbering starts over after a purge'); assert(!r.body.existing);
  console.log('functions OK ·', store.size, 'docs ·', blobs.size, 'blobs · server job placed', solved.placements.length, 'in', solved.trials, 'trial(s)');
})().catch(e => { console.error(e); process.exit(1); });
