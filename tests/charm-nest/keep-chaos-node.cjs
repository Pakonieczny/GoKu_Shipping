// KEEP-CHAOS (Node only): the fast variant of tests/charm-nest/keep-chaos.cjs for the ENGINE (charm-nest-keep.js, real) over the SERVER (the real charmNestLibrary
// handler, the real password door, the real customSaveGet / customSaveCommit / customSkuCheck over the in-memory Firestore + Storage of _sandboxFakes.cjs). No browser:
// the page's Master.saveCustom is stood in by a faithful copy of its steps (build bytes, upload the .ai and the picture once each, commit under the upload token; the
// page's own saveCustom runs in the browser harness). The Etsy op is a fake Etsy with the real contract: nothing is written before the repository is saved, the
// listing's SKU is written once. The same properties P1-P8, the same faults at every step (net error, 500, reply lost, token expiry, slow), the same final-state
// assertions, in about a minute with no CPU slot.
//   node tests/charm-nest/keep-chaos-node.cjs          (ONLY=<regex> · OUT=<file.json> · DRIVER=press|auto)
//   DRIVER=durable (default now that CharmNestKeep.durable exists: the page's own keeper). DRIVER=press: the engine as it was used before the keeper: ONE run() per press; a "reload" is a new engine, nobody presses. Every scenario that needs the save to resume
//   by itself fails here, and says so.  DRIVER=auto: the harness presses retry() the way a durable keeper would (after every failure, after the password, after a
//   reload), up to 6 times: it measures the engine and the server alone. When CharmNestKeep.durable exists the scenarios run through it too (DRIVER=durable).
'use strict';
const path = require('path'), fs = require('fs'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const { store, blobs, writes, fileWrites } = Fk;
const warn = console.warn, err = console.error; console.warn = () => {}; console.error = () => {};
let netCalls = 0; global.fetch = async () => { netCalls++; throw new Error('no network in this test'); };
const origFile = Fk.bucket.file.bind(Fk.bucket);
const ages = new Map();   // path -> ms of the file's last update (default: now), for the age of an abandoned file
Fk.bucket.file = p => { const f = origFile(p), gm = f.getMetadata; f.getMetadata = async () => { const [m] = await gm.call(f); return [Object.assign({}, m, { updated: new Date(ages.has(p) ? ages.get(p) : Date.now()).toISOString(), metadata: { firebaseStorageDownloadTokens: 'tok' + String(p.length) } })]; }; return f; };
const lib = require(path.join(fnDir, 'charmNestLibrary.js')), RA = require(path.join(fnDir, '_charmNestRepoAuth.js'));
const O = require(path.join(root, 'charm-nest-orders.js')), Sku = require(path.join(root, 'charm-nest-sku.js')), NameSku = Object.assign({}, Sku, { etsyKey: () => '' })   /* (KEEPOWNSKU, Paul 20 Oct 2026: these cases run the save's own naming, for a line whose SKU cannot be kept; the order's own SKU: keep-override-server.cjs, keep-relink-live.cjs) */, Keep = require(path.join(root, 'charm-nest-keep.js'));
const ONLY = process.env.ONLY ? new RegExp(process.env.ONLY, 'i') : null, DRIVER = process.env.DRIVER || (typeof Keep.durable === 'function' ? 'durable' : 'press');   // (the page saves through the durable keeper, charm-nest-bridge.js KeepLib().durable: the one-press engine is only what the page used before it, and fails every resume scenario by design)
const sleep = ms => new Promise(r => setTimeout(r, ms));
const GOOD = 'pw-' + crypto.randomBytes(5).toString('hex');
const post = async b => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b), queryStringParameters: {} }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };

const LISTING = '1799994821', RID1 = '4196630529', RID2 = '4196630530', RAW = "ACORN'S LEAF";
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const designs = {   // two real-looking drawings: a file (bytes), its charm (geometry hash)
  A: { name: 'Acorns_Leaf.ai', bytes: Buffer.from('%PDF-1.4\nleaf-A ' + 'a'.repeat(300)), charmHash: 'aaaa1111bbbb2222' },
  B: { name: 'Fox_Head.ai', bytes: Buffer.from('%PDF-1.4\nfox-B ' + 'b'.repeat(300)), charmHash: 'bbbb3333cccc4444' }
};
for (const d of Object.values(designs)) d.hash = sha(d.bytes);

/* ── DRIVER=durable: the keeper (CharmNestKeep.durable) over a fake browser store, a virtual clock (its backoff of 2 s ... 5 min runs in milliseconds) and navigator.locks ── */
function makeClock() {
  const c = { now: 1.8e12, timers: [] };
  c.setTimeout = (fn, ms) => { const t = { fn, at: c.now + Math.max(0, +ms || 0), off: false }; c.timers.push(t); return t; }; c.clearTimeout = t => { if (t) t.off = true; };
  c.flush = async (k = 30) => { for (let i = 0; i < k; i++) await new Promise(r => setImmediate(r)); };
  c.advance = async ms => { const end = c.now + ms; for (let g = 0; g < 3000; g++) { const due = c.timers.filter(t => !t.off && t.at <= end).sort((a, b) => a.at - b.at)[0]; if (!due) break; c.now = Math.max(c.now, due.at); due.off = true; due.fn(); await c.flush(); } c.now = end; await c.flush(); };
  return c;
}
const WAITING = new Set(['saving', 'retry', 'connection', 'data']);
function keeperFor(w, tabName) {
  const chain = w.navChain = w.navChain || new Map();
  const locks = { request: (name, fn) => { const prev = chain.get(name) || Promise.resolve(); const p = prev.catch(() => {}).then(() => fn()); chain.set(name, p); return p; } };
  const k = Keep.durable({ store: { get: key => (w.ls.has(key) ? w.ls.get(key) : null), set: (key, v) => { w.ls.set(key, v); } }, now: () => w.clock.now, setTimeout: w.clock.setTimeout, clearTimeout: w.clock.clearTimeout, tab: tabName || 'tab' + (w.keepers.length + 1),
    online: () => !w.offline, lock: w.lock, locks,
    saveBuilt: async (rec, b) => { w.builtStore.set(rec.key, b); }, loadBuilt: async rec => w.builtStore.get(rec.key) || null, dropBuilt: async rec => { w.builtStore.delete(rec.key); },   // (the built bytes kept in the page's IndexedDB: a reload does not build or upload again)
    resolve: async rec => { const t = w.tabs[rec.rid]; return t ? { state: 'ready', ctx: t.ctx } : { state: 'gone', why: 'no such order' }; }, make: ctx => Keep.make(ctx) });
  k.engineEvents = []; k.on(ev => { if (ev && ev.type === 'engine') k.engineEvents.push(ev.ev); }); w.keepers.push(k); return k;
}
/** the tab's "engine" as the scenarios drive it, in durable mode: run() = record the intent and let the keeper carry it, in virtual time, until it stops or waits for a person. */
function durableTab(w, tab, keeper) {
  let key = null; const K = () => keeper || w.keeper;
  const phase = () => { const rec = key && K().get(key); return rec ? K().describe(rec) : { phase: 'none' }; };
  const settle = async (maxMs = 40 * 60e3) => { for (let t = 0; t < maxMs; t += 1500) { await w.clock.flush(40); await sleep(0); const p = phase().phase; if (!WAITING.has(p)) { await w.clock.flush(40); return; } await w.clock.advance(1500); } };
  const done = () => { const p = phase().phase, rec = store.get('Charm_Custom_Saves/' + tab.rid); if (p === 'linked') return { ok: true, done: 'linked' }; if (p === 'rehearsed') return { ok: true, done: 'rehearsed' }; return { ok: false, done: rec && rec.sku ? 'saved' : 'held', phase: p }; };
  return {
    async open() { await w.ready; const probe = Keep.make(Object.assign({}, tab.ctx, { api: async (fn, b) => { if (b.op !== 'customSaveGet') throw new Error('no'); return (await post(b)).body; } })); return probe.open(); },
    async run() { await w.ready; const info = await this.open(); if (!info.offered) { return { ok: false, done: null, why: info.why }; } const r = K().begin({ sandbox: !!tab.ctx.sandbox, rid: tab.rid, listingId: tab.listingId, hash: tab.ctx.F.hash, name: tab.ctx.F.name, kind: 'ai', by: 'Paul', ck: 'c' + tab.rid, title: tab.ctx.title, line: tab.ctx.line }); key = r.key; await settle(); return done(); },
    async retry() { if (key) { try { K().retry(key); } catch (_) {} K().resume('press'); } await settle(); return done(); },
    async unlock(pw) { const r = await w.lock.submitInline('upload', pw); if (r.ok) { K().resume('lock'); return { ok: true }; } return { ok: false, reason: r.reason }; },
    snapshot() { return { rows: {} }; }, on() {}, phase, settle
  };
}

/* ── a world: a server state shared by every engine/tab of the scenario ── */
function reset() { store.clear(); blobs.clear(); writes.length = 0; fileWrites.length = 0; netCalls = 0; }
function mkWorld(o = {}) {
  reset();
  const w = { o, log: [], faults: [], uploads: [], builds: 0, locked: !!o.locked, tokens: new Set(), etsy: { writes: {}, listingSku: {}, beforeRepo: 0 }, notes: [], dialogs: 0, hooks: {}, killed: null, clock: makeClock(), ls: new Map(), builtStore: new Map(), tabs: {}, keepers: [], listeners: new Set(), offline: false };
  w.lock = { held: !w.locked, tok: null, onChange(fn) { w.listeners.add(fn); return () => w.listeners.delete(fn); },
    async issue() { const t = await RA.issue(Fk.db, { action: 'upload', by: 'Tester' }); w.lock.tok = { action: 'upload', token: t.token, expiresAt: Date.now() + 3600e3 }; return w.lock.tok; },
    find: a => (a === 'upload' && w.lock.held && w.lock.tok ? w.lock.tok : null), forget() { w.lock.held = false; w.lock.tok = null; for (const f of [...w.listeners]) { try { f(); } catch (_) {} } }, isRepoLocked: e => !!(e && e.data && e.data.error === 'repo-locked'),
    ensure() { w.dialogs++; throw new Error('dialog'); }, call() { w.dialogs++; throw new Error('dialog'); }, guard() { w.dialogs++; throw new Error('dialog'); }, gate() { w.dialogs++; throw new Error('dialog'); },
    submitInline: async (action, pw) => { if (pw !== GOOD) return { ok: false, reason: 'wrong' }; w.lock.held = true; await w.lock.issue(); for (const f of [...w.listeners]) { try { f(); } catch (_) {} } return { ok: true, token: w.lock.tok.token }; } };
  w.ready = (async () => { if (!w.locked) await w.lock.issue(); })();
  w.reloadKeeper = () => { for (const k of w.keepers) { try { k.close(); } catch (_) {} } w.navChain = new Map(); w.keeper = keeperFor(w); w.keeper.resume('load'); return w.keeper; };
  if (DRIVER === 'durable') w.keeper = keeperFor(w, 'tab1');
  w.step = async (name, call) => {   // faults of the call itself
    const n = w.log.filter(c => c.step === name).length + 1; w.log.push({ step: name, n });
    const f = w.faults.find(f => !f.done && f.step === name && (!f.nth || f.nth === n)); if (!f) return call();
    if (f.times) { f.hits = (f.hits || 0) + 1; if (f.hits >= f.times) f.done = true; } else f.done = true;
    if (f.on) await f.on(w);
    if (f.act === 'net') throw Object.assign(new Error('Failed to fetch'), { transient: true });
    if (f.act === 'http500') throw Object.assign(new Error('forced failure'), { status: 500, transient: true });
    if (f.act === 'lost') { await call(); throw Object.assign(new Error('Failed to fetch'), { transient: true }); }
    if (f.act === 'locked') { w.lock.forget(); throw Object.assign(new Error('repo-locked'), { status: 401, data: { error: 'repo-locked' } }); }
    if (f.act === 'expire') { RA.__w = w; w.lock.tok = Object.assign({}, w.lock.tok, { token: 'f'.repeat(64) + '.' + '0'.repeat(32) }); return call(); }
    if (f.act === 'slow') { await sleep(f.ms || 80); return call(); }
    if (f.act === 'kill') { w.killed && w.killed(); await new Promise(() => {}); }   // the page is gone: this call never answers
    if (f.act === 'barrier') { await barrier(w, f); return call(); }
    return call();
  };
  const barriers = new Map(); const barrier = (w, f) => new Promise(res => { const b = barriers.get(f.key) || { n: f.n || 2, w: [] }; barriers.set(f.key, b); b.w.push(res); const go = () => { if (barriers.get(f.key) === b) barriers.delete(f.key); b.w.forEach(r => r()); b.w = []; }; if (b.w.length >= b.n) go(); else setTimeout(go, f.maxWait || 1500); });   // (a barrier that nobody else reaches opens after 1.5 s: a keeper that serialises the two saves is not a deadlock)
  // the page's api(): the real handler; a 4xx/5xx or a refusal the way api() throws it
  w.api = async (fn, body) => {
    await w.ready;
    const step = body.op === 'customSaveGet' ? 'open' : body.op === 'customSkuCheck' ? 'sku' : body.op === 'customSaveCommit' ? 'commit' : body.op === 'customEtsySkuWrite' ? 'etsy' : 'other';
    return w.step(step, async () => {
      if (step === 'etsy') return fakeEtsy(w, body);
      const r = await post(body);
      if (r.status === 401 && r.body.error === 'repo-locked') throw Object.assign(new Error('repo-locked'), { status: 401, data: r.body });
      if (r.status >= 500) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { status: r.status, transient: true, data: r.body });
      if (r.status >= 400) throw Object.assign(new Error(r.body.error || 'HTTP ' + r.status), { status: r.status, data: r.body });
      return r.body;
    });
  };
  return w;
}
function fakeEtsy(w, body) {
  const rec = store.get('Charm_Custom_Saves/' + body.rid);
  if (!rec || !rec.sku) { w.etsy.beforeRepo++; return { ok: false, state: 'failed', reason: 'no-record', retry: false }; }
  if (rec.etsy && rec.etsy.state === 'linked') return { ok: true, state: 'linked' };
  const L = rec.listingId; if (w.etsy.listingSku[L] !== rec.sku) { w.etsy.writes[L] = (w.etsy.writes[L] || 0) + 1; w.etsy.listingSku[L] = rec.sku; }
  store.set('Charm_Custom_Saves/' + body.rid, Object.assign({}, rec, { etsy: Object.assign({}, rec.etsy || {}, { state: 'linked' }) }));
  return { ok: true, state: 'linked' };
}
/** the page's Master (the parts the engine uses): entries and aliases read from the server's records, saveCustom as the page does it (steps copied from the bridge). */
function mkMaster(w) {
  const entries = () => new Map([...store.entries()].filter(([k]) => k.startsWith('Charm_Master_Index/')).map(([k, v]) => [k.slice(19), v]));
  const M = {
    entryFor: s => entries().get(String(s || '').toUpperCase()) || null, looseFor: s => { const k = O.looseKey(s); for (const x of entries().keys()) if (O.looseKey(x) === k) return x; return ''; },
    sameDesign: hash => { for (const [k, v] of entries()) if (v.charmHash === hash) return k; return ''; },
    async saveCustom(spec, hooks) {
      const fail = (reason, text, extra) => Object.assign({ ok: false, reason, text }, extra || {});
      const sku = String(spec.reuse || spec.sku || '').toUpperCase(), c = spec.charm, F = spec.F;
      // (KEEPOVERRIDE / KEEPOWNSKU, as the bridge's: the shop's own SKU may have other characters; the files go under Sku.fileKey; the override, etsyOwn and the tag go with the commit)
      const ov = spec.override && spec.override.via ? spec.override : null, shop = !!(spec.etsyOwn || (ov && ov.via === 'own')) && !Sku.ownWhy(sku);
      if (!/^[A-Z0-9][A-Z0-9_-]{1,31}$/.test(sku) && !shop) return fail('bad-sku', 'that SKU cannot be used');
      const fileKey = spec.reuse ? sku : Sku.fileKey(sku, spec.fileTag || '', !!(ov && ov.via === 'own'));
      let built = spec.built || null;
      if (!spec.reuse && !built) { w.builds++; built = { ai: Buffer.concat([Buffer.from('%PDF-1.4\nbuilt ' + sku + ' ' + c.hash + ' '), Buffer.alloc(260, 7)]), png: Buffer.concat([Buffer.from('89504e470d0a1a0a', 'hex'), Buffer.alloc(80, 9)]) }; try { hooks.built(built); } catch (_) {} }
      if (spec.rehearsal) return { ok: true, rehearsed: true, sku, created: !spec.reuse };
      let tok = w.lock.find('upload');
      const need = async () => { let t = null; try { t = await hooks.needPassword(); } catch (_) {} return t && t.token ? t : w.lock.find('upload'); };
      if (!tok) tok = await need(); if (!tok) return fail('locked', 'needs the repository password');
      const up = Object.assign({}, spec.uploaded || {});
      const once = async (which, p, bytes) => {
        if (up[which]) return;
        for (let again = false; ; again = true) {
          try { await w.step('upload', async () => { const r = await post({ op: 'masterVerifyToken', repoToken: tok.token }); /* (the door checks the token on the signed PUT in production; here: the same RA) */ const ok = await RA.verify ? RA.verify(Fk.db, tok.token, 'upload').catch(() => false) : true; if (ok === false) throw Object.assign(new Error('repo-locked'), { status: 401, data: { error: 'repo-locked' } }); w.uploads.push(p); Fk.bucket.file(p).save(bytes, { contentType: which === 'ai' ? 'application/illustrator' : 'image/png' }); }); up[which] = { path: p }; try { hooks.uploaded(which, up[which]); } catch (_) {} return; }
          catch (e) { if (!again && w.lock.isRepoLocked(e)) { w.lock.forget(); tok = await need(); if (!tok) throw Object.assign(new Error('locked'), { keepLocked: true }); continue; } throw e; }
        }
      };
      if (!spec.reuse) { try { await once('ai', `charmnest/master/custom/${fileKey}.ai`, built.ai); await once('png', `charmnest/master/custom/${fileKey}.png`, built.png); } catch (e) { if (e && e.keepLocked) return fail('locked', 'needs the repository password'); throw e; } }
      const body = () => ({ op: 'customSaveCommit', repoToken: tok.token, rid: String(spec.rid), listingId: String(spec.listingId), sku, oldSku: String(spec.oldSku || ''), productId: String(spec.productId || ''), optionPairs: spec.optionPairs || [], group: spec.group, title: spec.title, by: spec.by || '',
        design: { name: F.name, kind: 'ai', hash: F.hash, bytes: F.bytes.length }, reuse: !!spec.reuse, ...(ov ? { override: { via: ov.via, from: ov.from } } : {}), ...(spec.etsyOwn ? { etsyOwn: true } : {}), ...(spec.fileTag ? { fileTag: spec.fileTag } : {}), entry: spec.reuse ? { charmHash: c.hash } : { charmHash: c.hash, widthPt: 60, heightPt: 50, areaPt2: 2400, members: 1, holes: 1, engravable: true, upAngle: 0, upSource: 'drawn' }, file: Object.assign({ masterHash: F.hash, name: F.name }, spec.reuse || !built ? {} : { aiSha256: sha(built.ai), aiBytes: built.ai.length, pngSha256: sha(built.png) }) });   // (the proof of the bytes built, as the page sends it: KEEP B3)
      let r;
      for (let again = false; ; again = true) {
        try { r = await w.api('charmNestLibrary', body()); break; }
        catch (e) { if (!again && w.lock.isRepoLocked(e)) { w.lock.forget(); tok = await need(); if (!tok) return fail('locked', 'needs the repository password'); continue; } throw e; }
      }
      if (!r || r.ok !== true) { const why = (r && r.error) || 'error'; return fail(why, why, { sku: r && r.sku }); }
      return { ok: true, sku: r.sku || sku, created: r.created !== false, entry: r.entry || null, alias: r.alias || '' };
    }
  };
  return M;
}
/** one tab: an engine for (order, listing, design), made the way the bridge makes it. */
function mkTab(w, o = {}) {
  const d = o.design || designs.A, rid = o.rid || RID1, listingId = o.listing || LISTING, sku = o.sku != null ? o.sku : RAW;
  const aliases = () => { const m = {}; for (const [k, v] of store.entries()) if (k.startsWith('Charm_Sku_Aliases/')) m[k.slice(18)] = v; return m; };
  const events = [], Master = mkMaster(w);
  const ctx = { rid, listingId, title: "Acorn&#39;s Leaf Charm", line: { sku, productId: o.productId || '9100000001', variations: o.variations || [{ name: 'Metal', value: 'GF 14/20', propertyId: 513, valueId: 1234 }], listingId }, listingKind: null,
    F: { name: d.name, kind: 'ai', bytes: d.bytes, hash: d.hash, pieces: 1, state: 'ready' }, charm: { hash: d.charmHash, open: false }, parsed: { ok: 1 }, by: 'Paul', sandbox: !!o.sandbox, S: { cloud: { ok: true } }, O, Sku: NameSku, Master, aliases, tableFor: () => o.table || null, api: (fn, b) => w.api(fn, b), lock: w.lock, now: Date.now };
  ctx.setTimeout = w.clock.setTimeout; ctx.clearTimeout = w.clock.clearTimeout;
  const tab = { events, ctx, w, rid, listingId };
  if (DRIVER === 'durable') { w.tabs[rid] = tab; tab.k = durableTab(w, tab, o.tab ? (w['keeper_' + o.tab] = w['keeper_' + o.tab] || keeperFor(w, o.tab)) : null); return tab; }
  const k = Keep.make(ctx); k.on(ev => events.push(ev)); tab.k = k;
  return tab;
}
/** the driver: how the save is carried on after the person pressed the switch. press = once; auto = the harness presses retry until it is done (the least a durable keeper does). */
async function carry(w, tab, o = {}) {
  await w.ready;
  if (DRIVER === 'durable') { const info = await tab.k.open(); if (!info.offered) return { info, res: null, presses: 0 }; let res = await tab.k.run(); if (tab.k.phase().phase === 'password' && o.password !== false) { await tab.k.unlock(GOOD); res = await tab.k.retry(); } return { info, res, presses: 1 }; }
  let info = await tab.k.open(); if (!info.offered) return { info, res: null, presses: 0 };
  let res = await tab.k.run(), presses = 1;
  if (DRIVER === 'press') return { info, res, presses };
  for (let i = 0; i < 6 && !(res && res.done === 'linked' || res && res.done === 'rehearsed'); i++) {
    if (res && res.done === 'held' && tab.events.some(e => e.row === 'repo' && e.state === 'locked') && o.password !== false) { await tab.k.unlock(GOOD); }
    res = await tab.k.retry(res && res.done === 'saved' ? 'etsy' : 'repo'); presses++;
  }
  return { info, res, presses };
}

/* ── assertions on the server's state ── */
const docs = coll => [...store.entries()].filter(([k]) => k.startsWith(coll + '/')).map(([k, v]) => [k.slice(coll.length + 1), v]);
function expectSaved(w, o = {}) {
  const soft = [], f = [], rid = o.rid || RID1, listing = o.listing || LISTING, d = o.design || designs.A;
  const idx = docs('Charm_Master_Index').filter(([, v]) => v.charmHash === d.charmHash), others = docs('Charm_Master_Index').filter(([, v]) => v.charmHash !== d.charmHash && !o.allowOthers);
  if (idx.length !== (o.entries == null ? 1 : o.entries)) f.push(`entries for the dropped file's geometry: ${idx.length} (${idx.map(x => x[0]).join(',') || 'none'}), wanted ${o.entries == null ? 1 : o.entries}`);
  if (others.length) f.push('other entries: ' + others.map(x => x[0]).join(','));
  const e = idx[0] && idx[0][1], sku = idx[0] && idx[0][0];
  if (e) {
    for (const p of [e.aiPath, e.thumbPath]) if (!blobs.has(p)) f.push('file missing: ' + p);
    const ai = blobs.get(e.aiPath); if (ai && ai.buf.slice(0, 4).toString() !== '%PDF') f.push('the saved .ai is not a PDF file');
    const litter = [...blobs.keys()].filter(b => b !== e.aiPath && b !== e.thumbPath && !docs('Charm_Master_Index').some(([, v]) => v.aiPath === b || v.thumbPath === b)); if (litter.length) f.push('files without an entry: ' + litter.join(','));
    if (!store.has('Charm_Master_Files/' + e.masterHash)) f.push('no file record');
    const al = store.get('Charm_Sku_Aliases/' + listing); if (!al) f.push('no alias'); else { const to = Object.entries(al.bySku || {}).filter(([, v]) => v === sku); if (to.length !== 1 && al.sku !== sku) f.push('alias keys to ' + sku + ': ' + to.length + ' ' + JSON.stringify(al.bySku)); if (!o.anyAlias && Object.entries(al.bySku || {}).some(([, v]) => v !== sku)) f.push('alias points at another SKU too: ' + JSON.stringify(al.bySku)); }
    const rec = store.get('Charm_Custom_Saves/' + rid); if (!rec) f.push('no save record'); else { if (rec.sku !== sku) f.push('record names ' + rec.sku); if (o.etsy !== false && !(rec.etsy && rec.etsy.state === 'linked')) f.push('Etsy not linked: ' + JSON.stringify(rec.etsy)); }
    if (o.etsy !== false && (w.etsy.writes[listing] || 0) !== 1) f.push('Etsy written ' + (w.etsy.writes[listing] || 0) + ' times');
    if (o.once !== false) { const dup = w.uploads.filter((p, i) => w.uploads.indexOf(p) !== i); if (dup.length) soft.push('uploaded twice (same path, same bytes): ' + [...new Set(dup)].join(',')); if (w.builds > 1) soft.push('built ' + w.builds + ' times'); }
    f.push(...assoc(w, sku, listing, o));
  }
  if (w.etsy.beforeRepo) f.push('Etsy asked before the repository was saved: ' + w.etsy.beforeRepo);
  if (w.dialogs) f.push('a dialog was opened');
  return { f, w: soft, sku };
}
/** P5: the NEXT order of the listing, and another variation of it, read by the real orders code over the records the server holds. */
function assoc(w, sku, listing, o = {}) {
  const f = [], aliases = Object.fromEntries(docs('Charm_Sku_Aliases').map(([k, v]) => [k, v])), master = new Map(docs('Charm_Master_Index'));
  const ctx = { optionMaps: {}, aliases, noDesign: { patterns: [], skus: [] }, masterEntry: s => master.get(String(s || '').toUpperCase()) || null, masterLoose: s => { const k = O.looseKey(s); for (const x of master.keys()) if (O.looseKey(x) === k) return x; return ''; }, masterShared: () => null, masterLike: () => null, masterSkus: () => [...master.keys()] };
  const kinds = [['next order', [['Metal', 'GF 14/20']], o.rawSku != null ? o.rawSku : RAW], ['other variation', [['Metal', 'Sterling Silver'], ['Length', '18 Inches']], o.rawSku != null ? o.rawSku : RAW]];
  if (o.aliasWorks === false) kinds.splice(0, kinds.length, ['next order (Etsy now carries the new SKU)', [['Metal', 'GF 14/20']], sku]);   // (a SKU the alias cannot link: the Etsy step is what links the next order)
  for (const [label, vars, rawSku] of kinds) {
    const order = { receiptId: '4199990001', orderNumber: '4199990001', createTs: 1, updateTs: 1, shipBy: 2000000000, buyer: { name: 'B' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [{ transactionId: '41999900011', listingId: listing, sku: rawSku, title: "Custom Charm - Acorn's Leaf, Personalized", quantity: 1, expectedShipDate: 2000000000, variations: vars.map(([name, value]) => ({ name, value })), metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] };
    let sp; try { sp = O.interpretLine(order, order.lines[0], ctx); } catch (e) { f.push(label + ': the orders code threw ' + e.message); continue; }
    if (sp.designSku !== sku) f.push(`${label} of listing ${listing} is not associated with ${sku}: designSku=${sp.designSku} special=${sp.special && sp.special.kind} problems=${(sp.problems || []).map(p => p.kind)}`);
    else if (sp.special || (sp.problems || []).length) f.push(`${label}: associated but still ${sp.special ? 'special ' + sp.special.kind : ''} ${(sp.problems || []).map(p => p.kind)}`);
  }
  return f;
}
const nothingWritten = w => { const f = []; const wr = writes.filter(x => !/^CharmRepo_Tokens\//.test(x.path)); if (wr.length) f.push('records written: ' + [...new Set(wr.map(x => x.path.split('/')[0]))].join(',')); if (fileWrites.length) f.push('files written: ' + fileWrites.length); if (Object.keys(w.etsy.writes).length) f.push('Etsy written'); return f; };

/* ── scenarios ── */
const SC = []; const scen = (name, props, fn) => SC.push({ name, props, fn });
const FAULTS = { net: { act: 'net', times: 2 }, http500: { act: 'http500', times: 2 }, lost: { act: 'lost', times: 1 }, locked: { act: 'locked', times: 1 }, slow: { act: 'slow', ms: 120, times: 1 } };
scen('baseline: drop, ON, saved and linked', ['P5'], async () => { const w = mkWorld(); const t = mkTab(w); await carry(w, t); return expectSaved(w); });
for (const step of ['open', 'sku', 'upload', 'commit', 'etsy']) for (const [fname, F] of Object.entries(FAULTS)) {
  if (fname === 'locked' && !['upload', 'commit'].includes(step)) continue;
  scen(`fault ${fname} @ ${step}`, ['P1', 'P3', 'P6'], async () => { const w = mkWorld(); w.faults.push(Object.assign({ step, nth: step === 'upload' ? 2 : 1 }, F)); const t = mkTab(w); await carry(w, t); return expectSaved(w); });
}
for (const step of ['sku', 'upload', 'commit', 'etsy']) scen(`reload (the engine is lost) @ ${step}, a new engine, nobody presses`, ['P1'], async () => {
  const w = mkWorld(); let killed; const k = new Promise(r => { w.killed = r; }); w.faults.push({ step, act: 'kill', nth: step === 'upload' ? 2 : 1 });
  const t1 = mkTab(w); await w.ready; if (DRIVER === 'durable') { t1.k.run(); } else t1.k.open().then(() => t1.k.run()); await Promise.race([k, sleep(3000)]);
  w.faults.length = 0;
  if (DRIVER === 'durable') { w.reloadKeeper(); const t2 = mkTab(w); w.tabs[RID1] = t2; await t2.k.settle ? t2.k.settle() : null; for (let i = 0; i < 100; i++) { await w.clock.advance(5000); } return expectSaved(w); }
  const t2 = mkTab(w);   // the reloaded page: a fresh engine; the intent is not here, so only a press (or a keeper) can go on
  if (DRIVER === 'press') { /* nobody presses: this is what main does after a reload */ } else await carry(w, t2);
  return expectSaved(w);
});
scen('reload after the repository step, before Etsy: Etsy once, nothing saved twice', ['P1', 'P6'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'etsy', act: 'net', times: 1 }); const t1 = mkTab(w); await carry({ ready: w.ready }, t1).catch(() => {}); await t1.k.open(); const r1 = await t1.k.run();
  const t2 = mkTab(w); const r = await carry(w, t2); return expectSaved(w);
});
scen('two tabs, same order, same file, at the same moment', ['P4'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'commit', act: 'barrier', key: 'c', n: 2, times: 2 }); const a = mkTab(w, { tab: 'tabA' }), b = mkTab(w, { tab: 'tabB' }); await w.ready; await Promise.all([a.k.open(), b.k.open()]);
  const [ra, rb] = await Promise.all([a.k.run(), b.k.run()]); if (DRIVER !== 'press') for (const t of [a, b]) await carry(w, t); const s = expectSaved(w, { once: false });
  for (const [n, r, t] of [['A', ra, a], ['B', rb, b]]) { const bad = t.events.filter(e => e.state === 'failed'); if (bad.length && DRIVER === 'press') s.f.push(`tab ${n} ended with a failed row (${bad[0].row}: ${bad[0].text}); the other tab saved: "already linked" is the calm answer`); }
  return s;
});
scen('two orders of the same listing, same file, at the same moment: one entry', ['P4'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'commit', act: 'barrier', key: 'c', n: 2, times: 2 }); const a = mkTab(w, { rid: RID1 }), b = mkTab(w, { rid: RID2 }); await w.ready; await Promise.all([a.k.open(), b.k.open()]);
  await Promise.all([a.k.run(), b.k.run()]); if (DRIVER !== 'press') { await carry(w, a); await carry(w, b); }
  const s = expectSaved(w, { saves: 2, once: false }); const r2 = store.get('Charm_Custom_Saves/' + RID2); if (!r2 || r2.sku !== s.sku) s.f.push('order 2 is not linked to ' + s.sku + ': ' + JSON.stringify(r2 && { sku: r2.sku, etsy: r2.etsy })); return s;
});
// (KEEPOVERRIDE, Paul 20 Oct 2026: "If a user engages the toggle to update the charm repository, then that's exactly what needs to happen": a second order's switch on a listing that already reads the
//  first design is an explicit correction and takes the listing (the first design keeps its SKU, an audit record says so); a second window that looked before the first save landed is refused calmly)
scen('two orders, same listing, DIFFERENT files: one design answers the listing; the second switch, when it saw the first, takes it over (KEEPOVERRIDE)', ['P2', 'P4'], async () => {
  const w = mkWorld(); const a = mkTab(w, { rid: RID1, design: designs.A }), b = mkTab(w, { rid: RID2, design: designs.B }); await w.ready; await b.k.open();   // (both windows are open before either saved)
  await carry(w, a); const sA = expectSaved(w, { allowOthers: true });
  await b.k.run(); if (DRIVER !== 'press') await carry(w, b); const al = store.get('Charm_Sku_Aliases/' + LISTING), targets = Object.values((al && al.bySku) || {}); const f = [...sA.f];
  const second = store.get('Charm_Custom_Saves/' + RID2); const lines = DRIVER === 'durable' ? [b.k.phase().line, (await b.k.open()).why].filter(Boolean) : b.events.filter(e => e.done).map(e => e.text); if (!second && !lines.length) f.push('the second order ended with no line at all');
  if (targets.length !== 1) f.push('the listing answers with more than one design: ' + JSON.stringify(targets));
  else if (second && second.sku !== sA.sku) {
    if (targets[0] !== second.sku) f.push('the second design was saved but the listing does not read it: ' + JSON.stringify(targets));
    if (!docs('Charm_Keep_Overrides').length) f.push('the takeover left no audit record');
    if (!store.get('Charm_Master_Index/' + sA.sku)) f.push('the first design lost its SKU');
  } else if (targets[0] !== sA.sku) f.push('the listing\'s alias was taken over with no save of the second design: ' + JSON.stringify(targets) + ' (first ' + sA.sku + ')');
  return { f };
});
scen('SKU taken by ANOTHER design (made a moment before): next free SKU, the other entry untouched', ['P2'], async () => {
  const w = mkWorld(); const t = mkTab(w); const info = await t.k.open(); const sku = info.sku;
  store.set('Charm_Master_Index/' + sku, { sku, charmHash: 'dead'.repeat(4), aiPath: 'charmnest/master/' + sku + '.ai', thumbPath: 'charmnest/master/' + sku + '.png' }); const other = JSON.stringify(store.get('Charm_Master_Index/' + sku));
  await carry(w, t); const s = expectSaved(w, { allowOthers: true }); if (JSON.stringify(store.get('Charm_Master_Index/' + sku)) !== other) s.f.push('the other design\'s entry was changed'); if (s.sku === sku) s.f.push('saved over the taken SKU'); return s;
});
scen('SKU taken by the SAME file (an order saved it a moment ago): linked to it, no second entry', ['P2', 'P4'], async () => {
  const w = mkWorld(); const a = mkTab(w, { rid: RID1 }), b = mkTab(w, { rid: RID2 }); await w.ready; await b.k.open();   // (b read the library before a saved)
  await carry(w, a); const r = await b.k.run(); if (DRIVER !== 'press') await carry(w, b);
  const s = expectSaved(w, { saves: 2, once: false }); const r2 = store.get('Charm_Custom_Saves/' + RID2); const al2 = store.get('Charm_Sku_Aliases/' + LISTING); if ((!r2 || r2.sku !== s.sku) && !(al2 && Object.values(al2.bySku || {}).includes(s.sku) && !r2)) s.f.push('order 2 is not linked to ' + s.sku + ': ' + JSON.stringify(r2 && { sku: r2.sku }) + ' ended ' + (r && r.done)); return s;
});
scen('the same file in the repository under another listing: reused, no new file', ['P2'], async () => {
  const w = mkWorld(); const a = mkTab(w, { rid: RID1 }); await carry(w, a); const before = [...blobs.keys()].length, idxBefore = docs('Charm_Master_Index').length;
  const b = mkTab(w, { rid: RID2, listing: '1800004999', sku: 'OTHER LEAF' }); w.etsy.listingSku = {}; await carry(w, b);
  const f = []; if (blobs.size !== before) f.push('files were uploaded again'); if (docs('Charm_Master_Index').length !== idxBefore) f.push('a second entry was made'); const rec = store.get('Charm_Custom_Saves/' + RID2); if (!rec || !rec.sku) f.push('order 2 not linked'); if (!store.get('Charm_Sku_Aliases/1800004999')) f.push('no alias for the other listing'); return { f };
});
for (const [label, sku] of [['missing', ''], ['malformed', 'a b/c?? !!'], ['lower case', 'acorn leaf 4821'], ['reserved family (CUSTOM_6673)', 'CUSTOM_6673'], ['already a master design', 'HEART_1111']]) scen(`listing SKU ${label}: a defined calm end`, ['P2'], async () => {
  const w = mkWorld(); if (label === 'already a master design') store.set('Charm_Master_Index/HEART_1111', { sku: 'HEART_1111', charmHash: designs.B.charmHash, aiPath: 'x', thumbPath: 'y' });
  const t = mkTab(w, { sku }); const r = await carry(w, t); const f = []; const info = r.info;
  if (label === 'already a master design' && info.offered) {   // (KEEPOVERRIDE: the line's own SKU read as ANOTHER drawing: the switch moves HEART_1111 to the dropped design, an audit record says so)
    const e = store.get('Charm_Master_Index/HEART_1111');
    if (r.res && r.res.done !== 'linked') f.push('ended ' + (r.res && r.res.done)); else { if (!e || e.charmHash === designs.B.charmHash) f.push('HEART_1111 still draws the old design: ' + JSON.stringify(e)); if (!docs('Charm_Keep_Overrides').length) f.push('no audit record of the override'); }
    return { f };
  }
  if (!info.offered) { if (!info.why) f.push('not offered and no reason'); f.push(...nothingWritten(w)); return { f }; }
  if (r.res && r.res.done !== 'linked') f.push('ended ' + (r.res && r.res.done) + ': ' + (r.res && JSON.stringify(t.events.filter(e => e.state === 'failed').map(e => e.text)))); else f.push(...expectSaved(w, { rawSku: sku, anyAlias: true, aliasWorks: info.aliasWorks }).f); return { f };
});
scen('a listing with several products/variations (a table): saved, linked, the other variation too', ['P2', 'P5'], async () => {
  const w = mkWorld(); const table = { at: 1, n: 3, products: [{ id: '1', sku: RAW, d: 0, pv: ['513:1234'] }, { id: '2', sku: RAW, d: 0, pv: ['513:1235'] }, { id: '3', sku: 'OTHER', d: 0, pv: ['513:1236'] }], nm: [], pn: [] };
  store.set('Charm_Listing_Skus/' + LISTING, table); const t = mkTab(w, { table }); await carry(w, t); return expectSaved(w);
});
scen('sandbox: rehearses, writes nothing (no record, no file, no Etsy)', ['P8'], async () => { const w = mkWorld(); const t = mkTab(w, { sandbox: true }); const r = await carry(w, t); const f = nothingWritten(w); if (!r.res || r.res.done !== 'rehearsed') f.push('ended ' + (r.res && r.res.done)); if (w.uploads.length) f.push('uploaded in the sandbox'); return { f }; });
scen('locked at the press, password given later, then it completes', ['P3'], async () => {
  const w = mkWorld({ locked: true }); const t = mkTab(w); await w.ready; await t.k.open(); const r1 = await t.k.run(); const f = [];
  if (DRIVER === 'durable') { if (t.k.phase().phase !== 'password') f.push('locked: expected the phase "password", got ' + t.k.phase().phase); }
  else if (r1.done !== 'held' || !t.events.some(e => e.row === 'repo' && e.state === 'locked')) f.push('locked: expected "held" with a locked row, got ' + r1.done);
  f.push(...nothingWritten(w).filter(x => !/Etsy/.test(x))); const u = await t.k.unlock(GOOD); if (!u.ok) f.push('unlock failed'); const r2 = await t.k.retry('repo');
  if (DRIVER === 'press' && r2.done !== 'linked') f.push('after the password: ' + r2.done); f.push(...expectSaved(w).f); return { f };
});
scen('wrong password then the right one', ['P3'], async () => { const w = mkWorld({ locked: true }); const t = mkTab(w); await w.ready; await t.k.open(); await t.k.run(); const a = await t.k.unlock('nope'); const f = []; if (a.ok || a.reason !== 'wrong') f.push('a wrong password: ' + JSON.stringify(a)); await t.k.unlock(GOOD); await t.k.retry('repo'); f.push(...expectSaved(w).f); return { f }; });
scen('token expires between the uploads and the commit, password given: nothing built or uploaded twice', ['P1', 'P6'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'commit', act: 'locked', times: 1 }); const t = mkTab(w); await carry(w, t, {}); return expectSaved(w);
});
scen('engine: a second run() while one runs is the same run; retry() when done changes nothing', ['P4'], async () => {
  if (DRIVER === 'durable') return { f: [], skip: 'engine-level (the keeper has one flight per intent: see the two-tab scenarios)' };
  const w = mkWorld(); const t = mkTab(w); await w.ready; await t.k.open(); const [a, b] = [t.k.run(), t.k.run()]; const f = []; if (a !== b) f.push('two runs'); await a; const r = await t.k.retry('repo'); const s = expectSaved(w); return { f: [...f, ...s.f] };
});
scen('two DIFFERENT designs choose the same SKU at the same moment: neither file is overwritten, each entry has its own drawing', ['P4', 'P5', 'P6'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'upload', act: 'barrier', key: 'u', n: 2, times: 2 });
  const twin = Object.assign({}, designs.B, { name: designs.A.name });   // (the same file name, so the same proposed SKU, another drawing)
  const a = mkTab(w, { rid: RID1, design: designs.A }), b = mkTab(w, { rid: RID2, design: twin }); await w.ready; await Promise.all([a.k.open(), b.k.open()]);
  await Promise.all([a.k.run(), b.k.run()]); if (DRIVER !== 'press') { await carry(w, a); await carry(w, b); }
  const f = [];
  for (const [sku, e] of docs('Charm_Master_Index')) { const blob = blobs.get(e.aiPath); const txt = blob ? blob.buf.toString('latin1') : ''; const m = /built (\S+) (\S+) /.exec(txt); if (!m) f.push(sku + ': its file is gone'); else if (m[2] !== e.charmHash) f.push(`entry ${sku} says drawing ${e.charmHash} but its stored file holds drawing ${m[2]} (a file was overwritten by the other design)`); }
  if (docs('Charm_Master_Index').length < 1) f.push('nothing was saved');
  const litter = [...blobs.keys()].filter(b => !docs('Charm_Master_Index').some(([, v]) => v.aiPath === b || v.thumbPath === b)); if (litter.length) f.push('files without an entry: ' + litter.join(','));
  return { f };
});
scen('abandoned upload (the page died after the files, before the commit): the server has a repair path', ['P6'], async () => {
  const w = mkWorld(); w.faults.push({ step: 'commit', act: 'kill', times: 1 }); let killed; const k = new Promise(r => { w.killed = r; }); const t = mkTab(w); await w.ready; if (DRIVER === 'durable') { t.k.run(); } else t.k.open().then(() => t.k.run()); await Promise.race([k, sleep(3000)]); w.keepers.forEach(x => { try { x.close(); } catch (_) {} });
  const f = []; const files = [...blobs.keys()].filter(b => /charmnest\/master\/custom\//.test(b)); if (files.length !== 2) f.push('(setup) expected two uploaded files, found ' + files.length);
  // the repair path: an explicit op, or the server sweeps files that have no entry after a day, on the next successful commit
  const ops = Object.keys(lib.ops || {}).filter(o => /custom/i.test(o) && /(repair|sweep|clean|orphan|litter|upkeep)/i.test(o));
  for (const p of files) ages.set(p, Date.now() - 25 * 3600e3);
  if (ops.length) { const r = await post({ op: ops[0], olderThanMs: 0, repoToken: w.lock.tok && w.lock.tok.token }); }
  else { w.faults.length = 0; const other = mkTab(w, { rid: RID2, listing: '1800004999', sku: 'OTHER LEAF', design: designs.B }); await carry(w, other); }   // (another order saves: the server's chance to sweep)
  const left = [...blobs.keys()].filter(b => files.includes(b)); if (left.length) f.push(ops.length ? 'the repair op ' + ops[0] + ' left ' + left.length + ' file(s)' : 'abandoned files (no entry, older than a day) are still there after another order saved: ' + left.join(','));
  return { f };
});
scen('commit refused for missing files, then the files come: no half state in between', ['P6'], async () => {
  const w = mkWorld(); await w.ready; const tok = w.lock.tok.token; const base = { op: 'customSaveCommit', repoToken: tok, rid: RID1, listingId: LISTING, sku: 'LEAF_4821', oldSku: RAW, productId: '9100000001', optionPairs: [], group: { mode: 'all', productIds: [] }, title: 'T', by: 'P', design: { name: 'a.ai', kind: 'ai', hash: designs.A.hash, bytes: 300 }, reuse: false, entry: { charmHash: designs.A.charmHash, widthPt: 60, heightPt: 50, areaPt2: 2400, members: 1, holes: 1 }, file: { masterHash: designs.A.hash, name: 'a.ai' } };
  const f = []; const r = await post(base); if (r.body.ok !== false || r.body.error !== 'not-uploaded') f.push('without files: ' + JSON.stringify(r.body).slice(0, 80)); if (docs('Charm_Master_Index').length || docs('Charm_Custom_Saves').length || docs('Charm_Sku_Aliases').length) f.push('a record was written for a commit that was refused');
  blobs.set('charmnest/master/custom/LEAF_4821.ai', { buf: Buffer.concat([Buffer.from('%PDF-1.4\n'), Buffer.alloc(300)]) }); blobs.set('charmnest/master/custom/LEAF_4821.png', { buf: Buffer.alloc(100, 1) });
  const r2 = await post(base); if (!r2.body.ok) f.push('with files: ' + JSON.stringify(r2.body).slice(0, 80)); const r3 = await post(base); if (!(r3.body.ok && r3.body.created === false)) f.push('a repeat of the commit: ' + JSON.stringify(r3.body).slice(0, 80)); if (docs('Charm_Master_Index').length !== 1) f.push('entries: ' + docs('Charm_Master_Index').length); return { f };
});

(async () => {
  const list = SC.filter(s => !ONLY || ONLY.test(s.name)); const results = [];
  console.log(`keep-chaos-node: ${list.length} scenarios, driver=${DRIVER}`);
  for (const s of list) {
    let r, crash = null; try { r = await Promise.race([s.fn(), sleep(15000).then(() => { throw new Error('timed out'); })]); } catch (e) { crash = String((e && e.stack) || e).split('\n').slice(0, 2).join(' | '); r = { f: ['scenario crashed: ' + crash.slice(0, 200)] }; }
    const f = (r && r.f) || [], wn = (r && r.w) || []; results.push({ name: s.name, props: s.props, pass: !f.length, f, warn: wn, skip: r && r.skip });
    console.log(`${f.length ? 'FAIL' : r && r.skip ? 'SKIP' : wn.length ? 'WARN' : 'PASS'}  ${s.name}${f.length ? '\n      - ' + f.slice(0, 3).join('\n      - ') : ''}${wn.length ? '\n      ~ ' + wn.slice(0, 2).join('\n      ~ ') : ''}`);
  }
  const pass = results.filter(r => r.pass).length;
  console.log(`\nkeep-chaos-node: ${pass}/${results.length} scenarios green (${results.filter(r => r.pass && r.warn.length).length} with a warning, ${results.filter(r => r.skip).length} skipped), ${results.length - pass} failing (driver=${DRIVER})`);
  if (process.env.OUT) fs.writeFileSync(process.env.OUT, JSON.stringify({ driver: DRIVER, at: new Date().toISOString(), results }, null, 1));
  console.warn = warn; console.error = err; process.exit(pass === results.length ? 0 : 1);
})();
