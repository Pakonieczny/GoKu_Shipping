// Handler smoke test for the charmNest* Netlify functions against an in-memory
// Firestore/Storage fake injected in place of ./firebaseAdmin. No network.
//   node tests/charm-nest/functions.cjs
const path = require('path'), assert = require('assert');
const fnDir = path.join(__dirname, '../../netlify/functions');

/* ── in-memory Firestore ───────────────────────────────────────────────── */
const store = new Map();                       // "coll/id" → data
const SERVER_TS = { __ts: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, increment: n => ({ __inc: n }) };
function applyValues(target, src) { for (const [k, v] of Object.entries(src)) { if (v && v.__inc != null) target[k] = (target[k] || 0) + v.__inc; else if (v === SERVER_TS) target[k] = { toMillis: () => Date.now() }; else target[k] = v; } return target; }
function docRef(coll, id) {
  const key = coll + '/' + id;
  return {
    id, path: key,
    async get() { const d = store.get(key); return { exists: !!d, id, data: () => (d ? { ...d } : undefined) }; },
    async set(data, opts) { const cur = (opts && opts.merge && store.get(key)) || {}; store.set(key, applyValues({ ...cur }, data)); }
  };
}
function query(coll, filters = [], order = null, lim = 0) {
  const q = {
    where(f, op, v) { return query(coll, filters.concat([[f, op, v]]), order, lim); },
    orderBy(f, dir) { return query(coll, filters, [f, dir || 'asc'], lim); },
    limit(n) { return query(coll, filters, order, n); },
    select() { return q; },
    async get() {
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => ({ id: k.slice(coll.length + 1), data: () => ({ ...v }) }));
      for (const [f, op, v] of filters) rows = rows.filter(r => { const x = r.data()[f]; return op === '==' ? x === v : op === '>=' ? x >= v : op === '<=' ? x <= v : true; });
      if (order) rows.sort((a, b) => { const x = a.data()[order[0]], y = b.data()[order[0]]; const c = (x && x.toMillis ? x.toMillis() : x) > (y && y.toMillis ? y.toMillis() : y) ? 1 : -1; return order[1] === 'desc' ? -c : c; });
      if (lim) rows = rows.slice(0, lim);
      return { size: rows.length, docs: rows, empty: !rows.length };
    },
    async add(data) { const id = 'auto' + Math.random().toString(36).slice(2, 8); await docRef(coll, id).set(data); return docRef(coll, id); },
    doc(id) { return docRef(coll, id); }
  };
  return q;
}
const db = {
  collection: c => query(c),
  batch() { const ops = []; return { set(ref, data, opts) { ops.push(() => ref.set(data, opts)); }, async commit() { for (const o of ops) await o(); } }; },
  async getAll(...refs) { return Promise.all(refs.map(r => r.get())); }
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
  r = await post(lib, { op: 'deleteSheet', id: 'silver-def456' }); r = await post(lib, { op: 'listSheets' }); assert.strictEqual(r.body.sheets.length, 1, 'archived sheet hidden');
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
  void S;
  console.log('functions OK ·', store.size, 'docs ·', blobs.size, 'blobs · server job placed', r.body.job.result.placements.length, 'in', r.body.job.result.trials, 'trial(s)');
})().catch(e => { console.error(e); process.exit(1); });
