// Reset the sandbox leaves no RELATIONSHIP of a custom order behind (Paul, 3 Oct 2026, after the reset that cleared every
// record: the Orders card of 4170408845 "CUTE TRICERATOPS W/ HEARTS" still said "2/5 NESTED" with a blue LION as its
// Vector design: the custom lion Paul had dropped on that order in the sandbox on 29 Sep, still tied to the order).
// What tied the order to the lion, and what is checked here (the sandbox plays the real order numbers, so every key below
// exists on BOTH sides: the sandbox's and production's):
//   1 · the learned maps (a listing's answered SKU, an option's answered design, a SKU "has no design") were written to
//       the SHARED collections from the sandbox (Charm_Sku_Aliases, Charm_Option_Map, Charm_Sku_NoDesign): no sandbox
//       copy, so no Reset ever reached them and production could read them. Now a sandbox answer is written to Sandbox_…
//       only, a sandbox read reads its own answer over the shared one, a sandbox "remove" never deletes production's row
//       (a mark in the sandbox's copy takes it off the sandbox's list), and the sandbox's copy is wiped by Reset (and by
//       Purge) and listed by sandboxStatus; renameCharm no longer writes the shared charm library from the sandbox.
//   2 · the designs sent to the sheets (Charm_Custom_Sheet, Charm_Pool, Charm_Custom_Orders: Sandbox_ already) are wiped,
//       and the lion's FILE (charmnest/custom/{order}/… which the file door moves to charmnest/sandbox/custom/…) goes with
//       the sandbox's files; production's custom file, sheet record, pool row and seal of the same order stay.
//   3 · the browser's own saved workspace (IndexedDB, scope "sandbox") held the "sent" lion and a page re-pooled the line
//       from it three seconds after the order came back, if a Reset ran from another tab before the clean-up reached the
//       browser. The page now asks the cloud before it restores: a checkpoint whose recorded sends the cloud no longer
//       holds is not restored (all of them: dropped whole; some: only those).
//   The shared stores written BEFORE this fix cannot be told from production's (no sandbox mark on the document), so
//   Reset deletes none of them: they are listed live read-only and were empty; the test pins that Reset leaves a shared
//   row it cannot prove as it was.
//   node tests/charm-nest/sandbox-reset-relations.cjs [playwright-core dir]    (the page part is skipped without playwright)
'use strict';
const path = require('path'), assert = require('assert'), fs = require('fs');
const fnDir = path.join(__dirname, '../../netlify/functions');

async function serverMain() {
/* ── a clock the test moves: every module reads Date.now at call time ── */
const realNow = Date.now; let skew = 0, slow = 0; Date.now = () => realNow() + skew;

/* ── in-memory Firestore: nested merges, dotted paths, in / range / array queries, field masks, cursors, 500-write batches ── */
class TS { constructor(ms) { this.ms = ms; } toMillis() { return this.ms; } toDate() { return new Date(this.ms); } get seconds() { return Math.floor(this.ms / 1000); } get nanoseconds() { return (this.ms % 1000) * 1e6; } }
const store = new Map();   // "coll/id[/sub/id…]" → data
const SERVER_TS = { __sts: true }, DEL = { __del: true };
const FieldValue = { serverTimestamp: () => SERVER_TS, delete: () => DEL, increment: n => ({ __inc: n }) };
const plain = v => !!v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof TS) && !(v instanceof Date) && v !== SERVER_TS && v !== DEL && v.__inc === undefined;
const clone = v => Array.isArray(v) ? v.map(clone) : v instanceof Date ? new Date(v.getTime()) : v === SERVER_TS ? new TS(Date.now()) : (v && v.__inc !== undefined) ? v.__inc : plain(v) ? Object.fromEntries(Object.entries(v).filter(([, x]) => x !== DEL).map(([k, x]) => [k, clone(x)])) : v;
function merge(into, patch, deep) {
  for (const [k, v] of Object.entries(patch)) {
    if (v === DEL) delete into[k];
    else if (v && v.__inc !== undefined) into[k] = (typeof into[k] === 'number' ? into[k] : 0) + v.__inc;
    else if (deep && plain(v) && plain(into[k])) into[k] = merge(clone(into[k]), v, true);
    else into[k] = clone(v);
  }
  return into;
}
const getPath = (d, f) => String(f).split('.').reduce((x, k) => (x == null ? undefined : x[k]), d);
const val = x => (x instanceof TS ? x.ms : x instanceof Date ? x.getTime() : x);
const canon = v => JSON.stringify(v, function (k, x) { const raw = this[k]; if (raw instanceof TS) return { __ts: raw.ms }; if (raw instanceof Date) return { __date: raw.getTime() }; return x && typeof x === 'object' && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map(key => [key, x[key]])) : x; });
const snapOf = (key, d, mask) => {
  const id = key.slice(key.lastIndexOf('/') + 1), data = d ? (mask ? Object.fromEntries(mask.filter(f => getPath(d, f) !== undefined).map(f => [f, clone(getPath(d, f))])) : clone(d)) : undefined;
  return { id, exists: !!d, ref: docRef(key), data: () => (data ? clone(data) : undefined), get: f => (d ? getPath(d, f) : undefined) };
};
function docRef(key) {
  return { id: key.slice(key.lastIndexOf('/') + 1), path: key, collection: sub => query(key + '/' + sub),
    async get() { return snapOf(key, store.get(key)); },
    async set(data, o) { store.set(key, merge(o && o.merge ? clone(store.get(key) || {}) : {}, data, !!(o && o.merge))); },
    async create(data) { if (store.has(key)) throw new Error('ALREADY_EXISTS ' + key); store.set(key, merge({}, data, false)); },
    async update(data) { const cur = store.get(key); if (!cur) throw new Error('NOT_FOUND ' + key); store.set(key, merge(clone(cur), data, false)); },
    async delete() { store.delete(key); } };
}
function query(coll, filters = [], orders = [], lim = 0, mask = null, after = null) {
  const q = {
    where: (f, op, v) => query(coll, filters.concat([[f, op, v]]), orders, lim, mask, after),
    orderBy: (f, dir) => query(coll, filters, orders.concat([[f, dir || 'asc']]), lim, mask, after),
    limit: n => query(coll, filters, orders, n, mask, after),
    select: (...f) => query(coll, filters, orders, lim, f, after),
    startAfter: (...v) => query(coll, filters, orders, lim, mask, v),
    count: () => ({ get: async () => { const s = await query(coll, filters, orders, 0, null, after).get(); return { data: () => ({ count: s.size }) }; } }),
    async get() {
      // a collection lists the documents written in it, never a parent that only holds a subcollection (as Firestore)
      let rows = [...store.entries()].filter(([k]) => k.startsWith(coll + '/') && !k.slice(coll.length + 1).includes('/')).map(([k, d]) => ({ k, d, id: k.slice(coll.length + 1) }));
      const field = (r, f) => (f === '__name__' ? r.id : val(getPath(r.d, f)));
      for (const [f, op, v] of filters) rows = rows.filter(r => {
        const x = field(r, f), y = Array.isArray(v) ? v.map(val) : val(v), raw = f === '__name__' ? undefined : getPath(r.d, f);
        return op === '==' ? x === y : op === '>=' ? x >= y : op === '<=' ? x <= y : op === '>' ? x > y : op === '<' ? x < y : op === 'in' ? y.includes(x) : op === 'array-contains' ? (raw || []).includes(v) : op === 'array-contains-any' ? (raw || []).some(z => v.includes(z)) : false;
      });
      const ord = orders.length ? orders : [['__name__', 'asc']];
      rows.sort((a, b) => { for (const [f, dir] of ord) { const x = field(a, f), y = field(b, f), c = x > y ? 1 : x < y ? -1 : 0; if (c) return dir === 'desc' ? -c : c; } return 0; });
      if (after) rows = rows.filter(r => { for (const [i, [f, dir]] of ord.entries()) { const x = field(r, f), y = val(after[i]); if (x === y) continue; return dir === 'desc' ? x < y : x > y; } return false; });
      if (lim) rows = rows.slice(0, lim);
      const docs = rows.map(r => snapOf(r.k, r.d, mask));
      return { size: docs.length, docs, empty: !docs.length, forEach: fn => docs.forEach(fn) };
    },
    doc: id => docRef(coll + '/' + (id || 'auto' + Math.random().toString(36).slice(2, 12))),
    // as Firestore: every document that exists, and every one that only holds a subcollection (never written)
    async listDocuments() { return [...new Set([...store.keys()].filter(k => k.startsWith(coll + '/')).map(k => k.slice(coll.length + 1).split('/')[0]))].sort().map(id => docRef(coll + '/' + id)); },
    async add(data) { const r = q.doc(); await r.set(data); return r; }
  };
  return q;
}
const db = {
  collection: c => query(c),
  // every commit takes `slow` ms of the test's clock (where a reset of a big sandbox spends its time)
  batch() { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d)),
    async commit() { assert(ops.length <= 500, 'a write batch holds at most 500 writes (' + ops.length + ')'); skew += slow; for (const o of ops) await o(); } }; },
  async getAll(...refs) { let mask = null; if (refs.length && typeof refs[refs.length - 1].get !== 'function') mask = refs.pop().fieldMask || null; return Promise.all(refs.map(async r => snapOf(r.path, store.get(r.path), mask))); },
  async runTransaction(fn) {
    const ops = [], tx = { get: r => r.get(), getAll: (...refs) => db.getAll(...refs), set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)), delete: r => ops.push(() => r.delete()), create: (r, d) => ops.push(() => r.create(d)) };
    const out = await fn(tx); for (const o of ops) await o(); return out;
  }
};
/* ── in-memory Storage ── */
const blobs = new Map();
const bucket = {
  name: 'test-bucket',
  file(p) {
    return { name: p,
      async save(buf, o) { blobs.set(p, { buf: Buffer.from(buf), contentType: o && o.contentType }); },
      async exists() { return [blobs.has(p)]; },
      async download() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob ' + p), { code: 404 }); return [b.buf]; },
      async delete(o) { if (!blobs.has(p) && !(o && o.ignoreNotFound)) throw Object.assign(new Error('no blob ' + p), { code: 404 }); blobs.delete(p); },
      async makePublic() {}, publicUrl: () => 'https://storage.example/' + p,
      async getMetadata() { const b = blobs.get(p); if (!b) throw Object.assign(new Error('no blob'), { code: 404 }); return [{ contentType: b.contentType, size: b.buf.length, metadata: {} }]; } };
  },
  async getFiles(q = {}) {
    assert(q.autoPaginate === false && q.maxResults > 0, 'files are listed a page at a time');
    const names = [...blobs.keys()].filter(k => k.startsWith(q.prefix || '')).sort().filter(k => !q.pageToken || k > q.pageToken), page = names.slice(0, q.maxResults);
    return [page.map(n => this.file(n)), names.length > page.length ? Object.assign({}, q, { pageToken: page[page.length - 1] }) : null];
  }
};
const admin = { firestore: Object.assign(() => db, { FieldValue, Timestamp: Object.assign(TS, { fromMillis: ms => new TS(ms) }), FieldPath: { documentId: () => '__name__' } }), storage: () => ({ bucket: () => bucket }) };
const Module = require('module'), realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (req === 'node-fetch') return async () => { throw new Error('no network in this test'); };
  if (req === 'firebase-admin' || req === './firebaseAdmin' || /[\/]firebaseAdmin(\.js)?$/.test(req)) return admin;
  return realLoad.call(this, req, ...rest);
};
global.fetch = async () => ({ ok: true, headers: { get: () => 'image/jpeg' }, arrayBuffer: async () => new Uint8Array([255, 216, 255]).buffer });
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(fnDir, 'charmNestLibrary.js'));
const post = b => lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) }).then(r => ({ status: r.statusCode, body: JSON.parse(r.body) }));
const must = async b => { const r = await post(b); assert.strictEqual(r.status, 200, JSON.stringify(b).slice(0, 140) + ' → ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };

/* ── the order of the evidence, and the keys both sides write ── */
const LION = '4170408845', TX = '4170408845101', LINE = `${LION}_${TX}`, POOL1 = `${LINE}_1`, CK = `custom:${LION}:CUTE_TRICERATOPS_4170`;
const LISTING = '1800123456', LISTING2 = '1800777000';
const NOW = realNow(), SENT_AT = NOW - 3 * 86400000;
const LION_FILE = `charmnest/sandbox/custom/${LION}/lion.ai`;           // where the file door puts a sandbox page's custom file
const PROD_FILE = `charmnest/custom/${LION}/prod-design.ai`;            // production's own custom file of the same order
const LEGACY_FILE = `charmnest/custom/${LION}/lion-legacy.ai`;          // a file at the shared path: nothing says who wrote it
const sheet = (file, by, name) => ({ ck: CK, rid: LION, at: SENT_AT - 1000, phase: 'sent',
  files: [{ id: 'file-1', name, kind: 'ai', size: 2400, hash: 'abcde01234567890123456789', cloud: { path: file, url: 'https://saved.example/' + file }, metal: 'gold', qty: 1, pieces: 1, wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
  sent: { id: `custom-sheet:${CK}:${SENT_AT}`, at: SENT_AT, by, lines: { [LINE]: [{ f: 'file-1', i: 0 }] } } });
let prodNoDesignRow = null, sandboxPatternRow = null;

/** What one side wrote about the order: sb true is a sandbox page (every call tagged sandbox), false a production page. */
async function seedRelations(sb) {
  const L = b => must(Object.assign({}, b, sb ? { sandbox: true } : {}));
  const by = sb ? 'paul-sandbox' : 'ann-production', design = sb ? 'LION_SANDBOX_9' : 'TRICERATOPS_PROD_1';
  // the listing's answered design: for the SKU the line came with, and for the listing itself
  await L({ op: 'aliasPut', listingId: LISTING, sku: design, fromSku: 'CUTE_TRICERATOPS_4170', title: 'CUTE TRICERATOPS W/ HEARTS', by });
  await L({ op: 'aliasPut', listingId: LISTING, sku: design, title: 'CUTE TRICERATOPS W/ HEARTS', by });
  if (sb) await L({ op: 'aliasPut', listingId: LISTING2, sku: 'LION_SANDBOX_9', title: 'a listing only the sandbox answered', by });
  // the option that picks the charm
  await L({ op: 'optionMapPut', listingId: LISTING, optionName: 'Size', optionValue: 'Small', map: { field: 'design', value: design }, by });
  if (sb) await L({ op: 'optionMapPut', listingId: LISTING2, optionName: 'Size', optionValue: 'Large', map: { field: 'design', value: 'LION_SANDBOX_9' }, by });
  // "this SKU has no design"
  const row = await L({ op: 'noDesignPut', sku: sb ? 'LION_NODESIGN' : 'PROD_NODESIGN_1', note: 'a SKU with no design', by });
  const pat = await L({ op: 'noDesignPut', pattern: sb ? '^LION_ONLY' : '^PROD_ONLY', note: 'a pattern', by });
  if (sb) sandboxPatternRow = pat.id; else prodNoDesignRow = row.id;
  // the designs sent to the sheets, their line in the pool, the line's seal
  await L({ op: 'customSheetPut', record: sb ? sheet(LION_FILE, by, 'Lion.ai') : sheet(PROD_FILE, by, 'Production design.ai') });
  await L({ op: 'poolPut', pools: [{ poolId: POOL1, orderId: LION, transactionId: TX, lineKey: LINE, runId: 'run-20260929-1', state: 'pooled', material: 'gold', copy: 1, quantity: 1, custom: true, sku: sb ? 'LION_SANDBOX_9' : 'TRICERATOPS_PROD_1' }] });
  await L({ op: 'customPut', key: LINE, by, how: 'button', receiptId: LION, transactionId: TX, sku: design, title: 'CUTE TRICERATOPS W/ HEARTS', kind: 'custom' });
  await L({ op: 'customDecide', key: LINE, kind: 'custom', by });
  blobs.set(sb ? LION_FILE : PROD_FILE, { buf: Buffer.from(sb ? 'LION' : 'PRODUCTION DESIGN') });
}
/** Records a long-running sandbox holds in bulk: pages of learned answers, so the reset needs more than one call. */
function bulkMaps(sb, n) {
  const P = sb ? 'Sandbox_' : '';
  for (let i = 0; i < n; i++) {
    const lid = String(1900000000 + i), id = 'row' + String(i).padStart(5, '0');
    store.set(`${P}Charm_Sku_Aliases/${lid}`, { listingId: lid, sku: 'BULK_' + i, v: 2, by: sb ? 'paul-sandbox' : 'ann-production' });
    store.set(`${P}Charm_Option_Map/${lid}`, { listingId: lid, map: { size: { small: { field: 'design', value: 'BULK_' + i, by: 'x', at: 1 } } } });
    store.set(`${P}Charm_Sku_NoDesign/${id}`, { sku: 'BULKND_' + i, by: 'x' });
  }
}

/* ── what is where ── */
const sandboxKeys = () => [...store.keys()].filter(k => k.startsWith('Sandbox_'));
const families = keys => [...new Set(keys.map(k => k.split('/').filter((_, i) => i % 2 === 0).join('/')))].sort();
const isOwn = k => k.startsWith('Sandbox_') || k === 'Charm_Sandbox/stream';
const shared = k => k.startsWith('Charm_Nest_CustomRead/');   // carries the sandbox's decision beside production's
const productionMap = () => new Map([...store.entries()].filter(([k]) => !isOwn(k)).map(([k, v]) => { const c = clone(v); if (shared(k)) { delete c.decidedSandbox; delete c.updatedAt; } return [k, canon(c)]; }));
const blobMap = () => new Map([...blobs.entries()].filter(([k]) => !k.startsWith('charmnest/sandbox/')).map(([k, b]) => [k, b.buf.toString('hex') + '|' + (b.contentType || '')]));
const resetAll = async () => { slow = 1500; let r = await post({ op: 'sandboxReset', sandbox: true }), calls = 1, deleted = r.body.deleted || 0; assert.strictEqual(r.status, 200, JSON.stringify(r.body)); const first = r.body; while (r.body.more && calls < 600) { r = await post({ op: 'sandboxReset', sandbox: true }); assert.strictEqual(r.status, 200, JSON.stringify(r.body)); calls++; deleted += r.body.deleted || 0; } slow = 0; return { first, last: r.body, calls, deleted }; };
const sbGet = (op, b) => must(Object.assign({ op, sandbox: true }, b));
const prGet = (op, b) => must(Object.assign({ op }, b));

const names = o => JSON.stringify(o);
async function serverSection() {
  store.clear(); blobs.clear(); skew = 0; slow = 0;
  store.set('Charm_Sandbox/current', { path: 'charmnest/sandbox/orders-cur.json', count: 2, at: NOW, takenBy: 'test' });
  store.set('Charm_Sandbox/stream', { on: true, v: 2, seed: 7, speed: 50, stepMs: 600000, tick: 12, snapshotPath: 'charmnest/sandbox/orders-cur.json' });
  for (const f of ['orders-cur.json', 'master/BRITES-master.ai']) blobs.set('charmnest/sandbox/' + f, { buf: Buffer.from('x') });
  store.set(`Charm_Nest_CustomRead/${LINE}`, { reads: { h1: { kind: 'custom' } }, order: LION, updatedAt: new TS(NOW - 9000) });
  // the charm library is one shared collection: a charm named in the sandbox must not rename it for production
  store.set('Charm_Nest_Library/abcdef0123456789', { hash: 'abcdef0123456789', name: 'Production name', namedBy: 'operator' });
  // rows in the shared maps from BEFORE this fix: no mark on them says who wrote them, so Reset never deletes them
  store.set('Charm_Sku_Aliases/1800999999', { listingId: '1800999999', sku: 'OLD_SHARED_ALIAS', v: 2, by: 'paul', title: 'an old answer' });
  store.set('Charm_Option_Map/1800999999', { listingId: '1800999999', map: { size: { small: { field: 'design', value: 'OLD_SHARED_ALIAS', by: 'paul', at: 1 } } } });
  store.set('Charm_Sku_NoDesign/old-shared-row', { sku: 'OLD_SHARED_NODESIGN', by: 'paul' });
  // a file at the shared custom path of the order: nothing marks who wrote it (production's, as far as anything can tell)
  blobs.set(LEGACY_FILE, { buf: Buffer.from('LEGACY LION'), contentType: 'application/pdf' });
  // production wrote first, then the sandbox, on the same keys
  await seedRelations(false); bulkMaps(false, 20);
  const prodWrote = productionMap(), prodBlobs = blobMap();
  await seedRelations(true); bulkMaps(true, 700);
  const renamed = await must({ op: 'renameCharm', hash: 'abcdef0123456789', name: 'LION', sandbox: true });
  assert(renamed.skipped, 'renameCharm from the sandbox says it did not write');
  // the sandbox removes its own pattern row, and production's SKU row from ITS list (never from production's)
  await must({ op: 'noDesignDelete', id: sandboxPatternRow, sandbox: true });
  await must({ op: 'noDesignDelete', id: prodNoDesignRow, sandbox: true });

  /* ── 1 · where every sandbox answer went: its own copy; production's documents are as production left them ── */
  assert.deepStrictEqual([...productionMap().keys()].sort(), [...prodWrote.keys()].sort(), 'the sandbox added no document to a shared collection');
  for (const [k, v] of prodWrote) assert.strictEqual(productionMap().get(k), v, "a sandbox answer changed production's document " + k);
  assert.strictEqual(store.get('Charm_Nest_Library/abcdef0123456789').name, 'Production name', 'the shared charm library was not renamed from the sandbox');
  assert.strictEqual(store.get(`Sandbox_Charm_Sku_Aliases/${LISTING}`).sku, 'LION_SANDBOX_9', "the sandbox's answer is its own copy");
  assert.strictEqual(store.get(`Charm_Sku_Aliases/${LISTING}`).sku, 'TRICERATOPS_PROD_1', "production's answer for the same listing is unchanged");
  assert(store.has(`Sandbox_Charm_Option_Map/${LISTING}`) && store.has(`Sandbox_Charm_Sku_NoDesign/${'del_' + prodNoDesignRow}`), "the sandbox's option answer and its mark on production's no-design row are its own");
  assert(store.has('Charm_Sku_NoDesign/' + prodNoDesignRow) && !store.has('Sandbox_Charm_Sku_NoDesign/' + sandboxPatternRow), "production's no-design row is still there; the sandbox's own row it removed is gone");
  const seededFamilies = families(sandboxKeys());
  for (const f of ['Charm_Sku_Aliases', 'Charm_Option_Map', 'Charm_Sku_NoDesign', 'Charm_Custom_Sheet', 'Charm_Pool', 'Charm_Custom_Orders']) assert(seededFamilies.includes('Sandbox_' + f), 'the sandbox holds a copy of ' + f + ' (' + seededFamilies.join(', ') + ')');

  /* ── 2 · what each side reads: the sandbox its own answer over the shared one's, production never the sandbox's ── */
  const sbAlias = (await sbGet('aliasGet')).aliases, prAlias = (await prGet('aliasGet')).aliases;
  assert(sbAlias[LISTING].sku === 'LION_SANDBOX_9' && sbAlias[LISTING].bySku.CUTE_TRICERATOPS_4170 === 'LION_SANDBOX_9' && sbAlias[LISTING2] && sbAlias['1800999999'].sku === 'OLD_SHARED_ALIAS', "the sandbox reads its own alias over the shared one's, and the shared ones it has no answer for");
  assert(prAlias[LISTING].sku === 'TRICERATOPS_PROD_1' && !prAlias[LISTING2] && !names(prAlias).includes('LION_SANDBOX_9'), "production reads none of the sandbox's aliases");
  const sbOpt = (await sbGet('optionMapGet')).maps, prOpt = (await prGet('optionMapGet')).maps;
  assert(sbOpt[LISTING].size.small.value === 'LION_SANDBOX_9' && sbOpt[LISTING2].size.large.value === 'LION_SANDBOX_9' && sbOpt['1800999999'].size.small.value === 'OLD_SHARED_ALIAS', "the sandbox reads its own option answer over the shared one's");
  assert(prOpt[LISTING].size.small.value === 'TRICERATOPS_PROD_1' && !prOpt[LISTING2] && !names(prOpt).includes('LION_SANDBOX_9'), "production reads none of the sandbox's option answers");
  const sbND = (await sbGet('noDesignGet')).list, prND = (await prGet('noDesignGet')).list;
  assert(sbND.skus.includes('LION_NODESIGN') && !sbND.skus.includes('PROD_NODESIGN_1') && sbND.skus.includes('OLD_SHARED_NODESIGN') && sbND.patterns.includes('^PROD_ONLY') && !sbND.patterns.includes('^LION_ONLY'), "the sandbox's no-design list: its own row, production's but the one it removed, never the pattern it deleted: " + names(sbND.skus.concat(sbND.patterns)));
  assert(prND.skus.includes('PROD_NODESIGN_1') && !prND.skus.some(x => /LION/.test(x)) && !prND.patterns.some(x => /LION/.test(x)), "production's no-design list holds none of the sandbox's rows and still has the row the sandbox removed");
  const sbSheet = (await sbGet('customSheetGet', { keys: [CK], lineKeys: [LINE] })).records[CK], prSheet = (await prGet('customSheetGet', { keys: [CK], lineKeys: [LINE] })).records[CK];
  assert(sbSheet && sbSheet.sent.by === 'paul-sandbox' && sbSheet.files[0].cloud.path === LION_FILE, 'the sandbox reads the lion it sent');
  assert(prSheet && prSheet.sent.by === 'ann-production' && prSheet.files[0].cloud.path === PROD_FILE, "production reads only its own design for the same order");
  assert((await sbGet('poolList', { orderId: LION })).pools.length === 1 && (await prGet('poolList', { orderId: LION })).pools.length === 1, 'each side has its own pool row of the line');
  const status = await sbGet('sandboxStatus');
  for (const n of ['Charm_Sku_Aliases', 'Charm_Option_Map', 'Charm_Sku_NoDesign']) assert(status.records[n] > 0, 'sandboxStatus lists the sandbox copy of ' + n);
  console.log(`seeded: the sandbox holds ${sandboxKeys().length} records in ${seededFamilies.length} families (aliases ${status.records.Charm_Sku_Aliases}, option maps ${status.records.Charm_Option_Map}, no-design ${status.records.Charm_Sku_NoDesign}) beside production's same-key records`);

  /* ── 3 · the reset, a call at a time (a commit takes 1.5 s of the clock): the sandbox keeps no relationship ── */
  const run = await resetAll();
  assert(run.first.more === true && run.calls >= 2, 'a reset that runs out of time says so: ' + names(run.first) + ' · ' + run.calls + ' calls');
  assert(run.last.more === false && !run.last.filesError, 'the calls finish the reset: ' + names(run.last));
  console.log(`reset: ${run.calls} calls removed ${run.deleted} records and ${run.last.files} files`);
  assert.deepStrictEqual(families(sandboxKeys()), [], 'the sandbox holds nothing: left ' + families(sandboxKeys()).join(', '));
  assert(!blobs.has(LION_FILE) && blobs.has('charmnest/sandbox/orders-cur.json') && blobs.has('charmnest/sandbox/master/BRITES-master.ai'), "the lion's file went with the sandbox's files; the snapshot and the master stay");
  assert.deepStrictEqual([...productionMap().keys()].sort(), [...prodWrote.keys()].sort(), 'production lost or gained no document');
  for (const [k, v] of prodWrote) assert.strictEqual(productionMap().get(k), v, 'production is byte-identical after the reset: ' + k);
  assert.deepStrictEqual([...blobMap()].sort(), [...prodBlobs].sort(), "production's custom file and the file at the shared path are byte-identical (every file outside charmnest/sandbox/)");
  assert(store.get(`Charm_Sku_Aliases/${LISTING}`).sku === 'TRICERATOPS_PROD_1' && store.has(`Charm_Option_Map/${LISTING}`) && store.has('Charm_Sku_NoDesign/' + prodNoDesignRow) && blobs.has(PROD_FILE) && blobs.has(LEGACY_FILE), "production's alias, option answer, no-design row, custom file and sheet survive");
  assert(store.has('Charm_Sku_Aliases/1800999999') && store.has('Charm_Option_Map/1800999999') && store.has('Charm_Sku_NoDesign/old-shared-row'), 'a shared row nothing proves the sandbox wrote is never deleted by Reset');

  /* ── 4 · the order arrives again: the sandbox finds the shared (master) answer and no lion ── */
  const after = { alias: (await sbGet('aliasGet')).aliases, opt: (await sbGet('optionMapGet')).maps, nd: (await sbGet('noDesignGet')).list };
  assert.deepStrictEqual(after.alias, (await prGet('aliasGet')).aliases, 'the sandbox reads exactly what production reads: no sandbox alias is left');
  assert.deepStrictEqual(after.opt, (await prGet('optionMapGet')).maps, 'the same for the option maps');
  assert.deepStrictEqual(after.nd, (await prGet('noDesignGet')).list, "and the no-design list: production's row is back on the sandbox's list (the mark that hid it is gone)");
  assert(!names(after).includes('LION'), 'no lion in anything the sandbox reads');
  const sheetAfter = await sbGet('customSheetGet', { keys: [CK], lineKeys: [LINE] });
  assert.deepStrictEqual(Object.keys(sheetAfter.records), [], 'the line has no sent design in the sandbox: it is not routed to a custom file');
  assert.strictEqual((await sbGet('poolList', { orderId: LION })).pools.length, 0, 'no sandbox pool row of the line');
  assert.strictEqual((await prGet('customSheetGet', { keys: [CK] })).records[CK].sent.by, 'ann-production', "production's design of the order is still sent");
  const zero = await sbGet('sandboxStatus');
  assert(Object.values(zero.records).every(n => n === 0), 'sandboxStatus finds nothing left: ' + names(zero.records));
  // a late write of an old page lands in the sandbox's copy only, and the next press clears it
  await must({ op: 'aliasPut', listingId: LISTING, sku: 'LION_SANDBOX_9', title: 'late', by: 'old tab', sandbox: true });
  assert(store.has(`Sandbox_Charm_Sku_Aliases/${LISTING}`) && store.get(`Charm_Sku_Aliases/${LISTING}`).sku === 'TRICERATOPS_PROD_1', "a late sandbox answer is the sandbox's copy only");
  assert((await post({ op: 'sandboxReset', sandbox: true })).body.more === false && !sandboxKeys().length, 'pressed again, the reset clears it');

  /* ── 5 · Purge all run history clears the sandbox's maps too, and leaves production's learned answers alone ── */
  await seedRelations(true); bulkMaps(true, 400);
  const maps = () => new Map([...store.entries()].filter(([k]) => /^Charm_(Sku_Aliases|Option_Map|Sku_NoDesign)\//.test(k)).map(([k, v]) => [k, canon(v)]));
  const mapsBefore = maps();
  slow = 1500; let r = await post({ op: 'purgeHistory', code: '975311', force: true, sandbox: true }), purges = 1;
  while (r.body.more && purges < 200) { r = await post({ op: 'purgeHistory', code: '975311', force: true, sandbox: true }); purges++; }
  slow = 0;
  assert(r.status === 200 && r.body.ok && !r.body.more, 'the purge finishes: ' + names(r.body).slice(0, 200));
  assert.deepStrictEqual(families(sandboxKeys()), [], 'after the purge the sandbox holds nothing: left ' + families(sandboxKeys()).join(', '));
  assert.deepStrictEqual([...maps()].sort(), [...mapsBefore].sort(), "production's learned answers are what they were after the purge");
  console.log(`purge: ${purges} call(s) cleared the sandbox's learned maps; production's answers untouched`);
}
  await serverSection();
}

/* ═══ the real page: a saved sandbox workspace that holds a "sent" lion is not restored when the cloud no longer holds it ═══ */
async function pageMain(pwArg) {
  const root = path.join(__dirname, '../..');
  const pwDir = pwArg || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the page part was not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const LION = '4170408845', LINE = `${LION}_4170408845101`, CK = `custom:${LION}:CUTE_TRICERATOPS_4170`;
  const OTHER = '4173162973', LINE2 = `${OTHER}_4173162973201`, CK2 = `custom:${OTHER}:CUSTOM-N-001`;
  const at = Date.now() - 3 * 86400000;
  /** A custom design sent to the sheets, as the page keeps it (and as the cloud records it): one file, one piece, one line. */
  const design = (ck, rid, line, name, by) => ({ ck, rid, at: at - 1000, phase: 'sent',
    files: [{ id: 'file-1', name, kind: 'ai', size: 2400, hash: 'abcde01234567890123456789', cloud: { path: `charmnest/sandbox/custom/${rid}/${name}`, url: `https://saved.example/${rid}/${name}` }, metal: 'gold', qty: 1, pieces: 1, wMm: 14, hMm: 18, maxPt: 52, minPt: 40, maxAreaPt2: 2000, state: 'ready' }],
    sent: { id: `custom-sheet:${ck}:${at}`, at, by, lines: { [line]: [{ f: 'file-1', i: 0 }] } } });
  const lion = design(CK, LION, LINE, 'Lion.ai', 'Tester'), other = design(CK2, OTHER, LINE2, 'Name necklace.ai', 'Tester');

  const srv = await start({ receipts: [] });
  const { st, sorterOrigin, stationOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
  // tokenised Storage URLs (the real handlers build them for the test bucket) are answered from the in-memory blob store
  await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const m = /\/o\/(.+)$/.exec(new URL(r.request().url()).pathname), b = st.blobs.get(m ? decodeURIComponent(m[1]) : ''); if (!b) return r.fulfill({ status: 404, body: 'no blob' }); return r.fulfill({ status: 200, headers: { 'Content-Type': b.meta.contentType || 'application/octet-stream', 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
  await ctx.addInitScript(({ sorter }) => { if (location.origin === sorter) localStorage.setItem('cn.employee', 'Tester'); window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; }, { sorter: sorterOrigin });
  const errors = [];
  const page = await ctx.newPage(); page.on('pageerror', e => errors.push(e.message));
  const boot = () => page.waitForFunction(() => window.CN && window.TeamMail && window.Sandbox && window.Session && Session.ready(), null, { timeout: 60000 });
  // the cloud can be made not to answer the question of which custom designs were sent (cannotAsk), or not to record one (cannotRecord): all else answers
  let cannotAsk = false, cannotRecord = false;
  await ctx.route(/charmNestLibrary/, r => { const body = r.request().postData() || '', down = r.request().method() === 'POST' && (cannotAsk && /"op":"customSheetGet"/.test(body) || cannotRecord && /"op":"customSheetPut"/.test(body)); return down ? r.fulfill({ status: 503, contentType: 'application/json', body: '{"error":"unavailable"}' }) : r.continue(); });
  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.CN && window.Sandbox, null, { timeout: 60000 });
  await page.evaluate(station => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'on', dsOrigin: station, pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s)); }, stationOrigin);
  await page.reload(); await boot();
  assert(await page.evaluate(() => WORKSPACE_SANDBOX), 'the page is in the sandbox');
  const idbDump = () => page.evaluate(() => new Promise((res, rej) => { const r = indexedDB.open('charm-nest-workspace', 1); r.onsuccess = () => { const tx = r.result.transaction('workspaces', 'readonly'), s = tx.objectStore('workspaces'), keys = s.getAllKeys(), out = {}; keys.onsuccess = () => { let n = keys.result.length; if (!n) return res(out); for (const k of keys.result) { const g = s.get(k); g.onsuccess = () => { out[k] = g.result; if (!--n) res(out); }; } }; }; r.onerror = () => rej(r.error); }));
  const inCloud = ck => st.list('Sandbox_Charm_Custom_Sheet').some(d => d.ck === ck);
  /** Saves a workspace holding these designs as sent, then reloads the page (as a reload does): what the page holds after it. */
  const saveThenReload = async designs => {
    await page.evaluate(ds => { for (const d of ds) B.customDesigns[d.ck] = JSON.parse(JSON.stringify(d)); Session.schedule(); }, designs);
    assert.strictEqual(await page.evaluate(() => Session.flushNow()), true, 'the workspace was saved');
    const saved = (await idbDump()).sandbox;
    assert(saved && designs.every(d => saved.customDesigns && saved.customDesigns[d.ck] && saved.customDesigns[d.ck].sent), 'the saved sandbox workspace holds the designs as sent');
    await page.reload(); await boot();
    return page.evaluate(([l1, l2]) => ({ cks: Object.keys(B.customDesigns || {}).sort(), lion: !!CustomSheet.sentOf({ key: l1 }), other: !!CustomSheet.sentOf({ key: l2 }), toasts: [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent) }), [LINE, LINE2]);
  };
  const refused = t => t.some(x => /sandbox was reset from another page: its old saved workspace was not restored/.test(x));
  const wipeDesigns = async () => { await page.evaluate(() => { for (const k of Object.keys(B.customDesigns)) delete B.customDesigns[k]; }); };

  // ── control · the cloud holds the sent lion (nobody reset): a reload restores it, as before ──
  assert.strictEqual(inCloud(CK), false, 'the cloud holds no lion yet');
  await page.evaluate(d => api('charmNestLibrary', { op: 'customSheetPut', phase: 'sent', record: d }), lion);
  assert.strictEqual(inCloud(CK), true, "the lion is recorded in the sandbox's sheet collection");
  let s = await saveThenReload([lion]);
  assert(s.cks.includes(CK) && s.lion && !refused(s.toasts), 'control: a workspace whose sent design the cloud holds is restored with it: ' + JSON.stringify(s));

  // ── the evidence · the sandbox was reset from an older page: the cloud is clean, this browser kept its workspace ──
  for (const d of st.list('Sandbox_Charm_Custom_Sheet')) st.docs.delete('Sandbox_Charm_Custom_Sheet/' + d._id);
  for (const d of st.list('Sandbox_Charm_Custom_Sheet')) assert.fail('cloud not clean');
  assert.strictEqual(inCloud(CK), false, "an older page's reset cleared the cloud's sheet records");
  s = await page.evaluate(([l]) => ({ cks: Object.keys(B.customDesigns || {}), lion: !!CustomSheet.sentOf({ key: l }) }), [LINE]);
  assert(s.lion, 'the page in memory still holds the lion until it reloads (the old tab)');
  await page.evaluate(() => Session.schedule());
  assert.strictEqual(await page.evaluate(() => Session.flushNow()), true);
  await page.reload(); await boot();
  s = await page.evaluate(([l1]) => ({ cks: Object.keys(B.customDesigns || {}).sort(), lion: !!CustomSheet.sentOf({ key: l1 }), toasts: [...document.querySelectorAll('#toasts .toast .m')].map(x => x.textContent) }), [LINE]);
  assert(s.cks.length === 0 && !s.lion, 'a saved workspace whose sent designs the cloud no longer holds is not restored: the line is tied to no lion: ' + JSON.stringify(s));
  assert(refused(s.toasts), 'the page says so in words: ' + JSON.stringify(s.toasts));
  let dump = await idbDump();
  assert(!dump.sandbox || !Object.keys(dump.sandbox.customDesigns || {}).length, 'the saved workspace went too (or holds none of it)');

  // ── a mix · two designs saved, the cloud holds one of them: the one it does not hold is dropped, the other stays ──
  await page.evaluate(d => api('charmNestLibrary', { op: 'customSheetPut', phase: 'sent', record: d }), other);
  s = await saveThenReload([lion, other]);
  assert(s.cks.includes(CK2) && s.other && !s.cks.includes(CK) && !s.lion && !refused(s.toasts), 'only the design the cloud holds is restored: ' + JSON.stringify(s));

  // ── a design still waiting to be recorded (sendCloudPending) is the page's own to send: never dropped by this check,
  //    and its presence keeps the workspace (only the forgotten design beside it goes) ──
  for (const d of st.list('Sandbox_Charm_Custom_Sheet')) st.docs.delete('Sandbox_Charm_Custom_Sheet/' + d._id);
  await wipeDesigns();
  cannotRecord = true;   // (the page's own retry of the send must not reach the cloud while the workspace is saved and reloaded)
  s = await saveThenReload([Object.assign({}, lion, { sendCloudPending: true }), other]);
  cannotRecord = false;
  assert(s.cks.includes(CK) && s.lion && !s.cks.includes(CK2) && !s.other && !refused(s.toasts), 'a send the cloud has not been told of yet is kept, the forgotten one beside it is dropped: ' + JSON.stringify(s));

  // ── a cloud that cannot be asked loses nobody's workspace ──
  await wipeDesigns();
  cannotAsk = true;
  await page.evaluate(d => { B.customDesigns[d.ck] = JSON.parse(JSON.stringify(d)); Session.schedule(); }, lion);
  await page.evaluate(() => Session.flushNow());
  await page.reload(); await boot();
  cannotAsk = false;
  dump = await idbDump();
  assert(dump.sandbox && dump.sandbox.customDesigns && dump.sandbox.customDesigns[CK], 'a cloud that does not answer (a reload offline) leaves the saved workspace as it was');

  // ── the order arrives again after the reset: its design is the MASTER charm, not the lion, and it is not "nested" ──
  // (the line's SKU is in the master library, as the triceratops is in the shop's; the lion was only ever a custom design
  //  sent for it, which nothing in the sandbox holds any more)
  const tmp = fs.mkdtempSync(path.join(require('os').tmpdir(), 'cn-relations-')), masterPath = path.join(tmp, 'BRITES-master.ai');
  const fx = await require('./fixture-master.cjs').buildMaster(masterPath, { count: 2, edge: false }), SKU = fx.charms[0].sku;
  await page.evaluate(() => CN.setMode('master'));
  await page.waitForSelector('#mFile', { state: 'attached' });
  await page.setInputFiles('#mFile', masterPath);
  await page.waitForFunction(() => { const j = [...B.master.jobs.values()][0]; return j && ['done', 'error'].includes(j.state); }, null, { timeout: 120000 });
  assert.strictEqual(await page.evaluate(() => [...B.master.jobs.values()][0].state), 'done', 'the master library is indexed');
  await wipeDesigns();
  const arrived = await page.evaluate(async ([rid, tid, sku]) => {
    await Orders.loadMaps(true);
    const ship = Math.floor(Date.now() / 1000) + 86400;
    const line = { transactionId: tid, listingId: '1800123456', sku, title: 'CUTE TRICERATOPS W/ HEARTS', quantity: 1, expectedShipDate: ship, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' };
    const order = { receiptId: rid, orderNumber: rid, createTs: ship - 5 * 86400, updateTs: ship - 5 * 86400 + 60, shipBy: ship, buyer: { name: 'Buyer 8845' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line] };
    const key = CharmNestOrders.lineKey(order, line), row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
    B.orders.rows.push(row); B.orders.byKey.set(key, row);
    Orders.interpretAll(); Review.syncOrderItems();
    const before = { key, sent: !!CustomSheet.sentOf(row), decision: !!CustomSheet.decisionOf(row), designSku: row.spec && row.spec.designSku };
    await Pool.addAll({ runId: 'run-relations', lines: {} });
    const charm = (row.poolIds || []).map(id => Pool.charmOf(id)).find(Boolean) || null;
    CN.setMode('orders'); Orders.render();
    const card = [...document.querySelectorAll('#ordersView *')].find(n => n.children.length === 0 && n.textContent.includes('CUTE TRICERATOPS'));
    // the card's Vector design: the list row draws it into its [data-vector] box (the pooled charm if the line has one, else the master's)
    let host = null; for (let i = 0; i < 60 && !host; i++) { await new Promise(r => setTimeout(r, 100)); host = [...document.querySelectorAll('#ordersView [data-vector]')].find(h => h.closest('[data-key]') && h.closest('[data-key]').dataset.key === row.key && h.querySelector('canvas, img, svg')) || null; }
    const vec = host ? host.innerHTML.slice(0, 120) : null;
    const boxes = [...document.querySelectorAll('#ordersView [data-vector]')].map(h => [(h.closest('[data-key]') || {dataset: {}}).dataset.key, h.getAttribute('aria-busy'), h.innerHTML.slice(0, 60)]);
    return Object.assign(before, { state: row.state, reason: row.reason, pooled: (row.poolIds || []).length, pill: Orders.statePill(row)[1], onSheet: (row.poolIds || []).some(id => Pool.sheetOf(id)),
      charm: charm && { custom: !!charm.custom, customCk: charm.customCk || null, sourceId: String(charm.sourceId || ''), sku: charm.orderInfo && charm.orderInfo.sku, name: charm.name }, vectorDrawn: !!vec, boxes, listed: !!card,
      ordersText: document.getElementById('ordersView') ? document.getElementById('ordersView').innerText.replace(/\s+/g, ' ') : '' });
  }, [LION, '4170408845101', SKU]);
  assert.strictEqual(arrived.key, LINE, 'the line has the key of the evidence');
  assert(!arrived.sent && !arrived.decision, 'the line is tied to no custom design: ' + JSON.stringify(arrived));
  assert(arrived.state === 'pooled' && arrived.pooled === 1 && arrived.charm, 'the line is pooled from its master design: ' + JSON.stringify(arrived));
  assert(!arrived.charm.custom && !arrived.charm.customCk && !/^cust:/.test(arrived.charm.sourceId) && arrived.charm.sku === SKU && !/Custom/.test(arrived.charm.name), 'its pooled charm is the master design, not a custom lion: ' + JSON.stringify(arrived.charm));
  assert(arrived.vectorDrawn, 'the card draws a Vector design for it');
  assert(!/nested/i.test(arrived.pill) && !arrived.onSheet && /pooled/.test(arrived.pill), 'its badge is not NESTED: ' + arrived.pill);
  const pooledRow = st.list('Sandbox_Charm_Pool').find(p => p.poolId === `${LINE}_1`);
  assert(pooledRow && pooledRow.sku === SKU && !pooledRow.custom, "the sandbox's pool row of the line is the master design's: " + JSON.stringify(pooledRow));
  assert(!st.list('Charm_Pool').some(p => p.orderId === LION), "and production's pool is untouched by it");

  console.log(`order ${LION} arrives again: pooled from master ${SKU} (${arrived.charm.sourceId.slice(0, 18)}…), badge "${arrived.pill}", ${arrived.boxes.length} Vector design box(es) on the Orders list`);
  assert.deepStrictEqual(errors.filter(e => !/firebase stub/.test(e)), [], 'no page errors');
  console.log('sandbox reset relations: page OK (a saved sandbox workspace holding a lion the cloud no longer holds is not restored; the control, the mix, a pending send and an unreachable cloud behave)');
  await browser.close(); srv.close && srv.close();
}

(async () => {
  if (process.argv[2] === '--page') return pageMain(process.argv[3]);
  await serverMain();
  console.log('sandbox reset relations: server OK');
  // the page part runs in its own process: it brings its own in-memory server (bridge-server.cjs), with the real handlers over it
  const r = require('child_process').spawnSync(process.execPath, [__filename, '--page', process.argv[2] || ''], { stdio: 'inherit' });
  if (r.status !== 0) process.exit(r.status || 1);
})().catch(e => { console.error(e); process.exit(1); });
