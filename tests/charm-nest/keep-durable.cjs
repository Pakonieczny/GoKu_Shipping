// KEEPSAVE (Paul, 16 Oct 2026): the save of a dropped vector file under its Etsy listing's SKU is a DURABLE INTENT that runs by itself.
// Node only, all fakes: the REAL engine (charm-nest-keep.js make) and the REAL keeper (CharmNestKeep.durable) over a fake browser store, clock, lock,
// network and a fake cloud that keeps the contract of the real one (create-only commit, "again" for the same order, "sku-taken" for another drawing).
// No Etsy, no Firestore, no network. Each case fails without the keeper (the engine alone writes nothing until run() is called).
//   1 recorded before any step · 2 the happy path · 3 a fault at EVERY step, two kinds (a passing failure; the page killed and reloaded) · 4 waiting: password given later
//   by a person in this or another tab, offline then online, file / SKU data not ready · 5 switch off before / after the commit · 6 two tabs, double press, a second file,
//   two orders of one listing · 7 backoff, needs attention, terminal reasons, Try again · 8 sleep and wake · 9 sandbox · 10 reload after done
//   node tests/charm-nest/keep-durable.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Keep = require(path.join(root, 'charm-nest-keep.js')), O = require(path.join(root, 'charm-nest-orders.js')), Sku = require(path.join(root, 'charm-nest-sku.js')), NameSku = Object.assign({}, Sku, { etsyKey: () => '' })   /* (KEEPOWNSKU, Paul 20 Oct 2026: these cases run the save's own naming, for a line whose SKU cannot be kept; the order's own SKU: keep-override-server.cjs, keep-relink-live.cjs) */;
let n = 0; const ok = (c, m) => { n++; assert(c, m); }; const eq = (a, b, m) => { n++; assert.deepStrictEqual(a, b, m); };
const flush = async (k = 40) => { for (let i = 0; i < k; i++) await new Promise(r => setImmediate(r)); };

const RID = '4196630529', LISTING = '1799994821', HASH = 'ab'.repeat(32), SKU = 'ACORNS_LEAF_4821';
const transient = () => Object.assign(new Error('timed out'), { transient: true });

/** The whole page and cloud, in memory. opt: sandbox, online, locked, ready (the design is read), faults: { point: [kind...] } */
function world(opt = {}) {
  const w = { now: 1.8e12, timers: [], online: opt.online !== false, ready: opt.ready !== false, tableReady: true, srv: { entries: new Map(), records: new Map(), files: new Map(), applied: 0, etsyApplied: 0, uploads: [], calls: [], builds: 0, etsyCalls: 0 },
    faults: opt.faults || {}, ls: new Map(), builtStore: new Map(), storeFails: false, lockHeld: opt.locked === false || opt.locked === undefined ? true : false, lockListeners: new Set(), keepers: [], dialogs: 0 };
  const srv = w.srv;
  w.setTimeout = (fn, ms) => { const t = { fn, at: w.now + Math.max(0, +ms || 0), off: false }; w.timers.push(t); return t; };
  w.clearTimeout = t => { if (t) t.off = true; };
  w.advance = async ms => {
    const end = w.now + ms;
    for (let guard = 0; guard < 500; guard++) {
      const due = w.timers.filter(t => !t.off && t.at <= end).sort((a, b) => a.at - b.at)[0]; if (!due) break;
      w.now = Math.max(w.now, due.at); due.off = true; due.fn(); await flush(12);
    }
    w.now = end; await flush(12);
  };
  w.pump = async (ms = 0) => { await flush(12); await w.advance(ms); };
  // the browser's storage (shared by every "tab" of the world)
  w.store = { get: k => { if (w.storeFails) throw new Error('storage blocked'); return w.ls.has(k) ? w.ls.get(k) : null; }, set: (k, v) => { if (w.storeFails) throw new Error('quota'); w.ls.set(k, v); } };
  // the repository lock (the password gate): found = a token is held
  w.lock = { find: a => (w.lockHeld && a === 'upload' ? { action: 'upload', token: 'T', expiresAt: w.now + 1e6 } : null), forget() { w.lockHeld = false; }, onChange(fn) { w.lockListeners.add(fn); return () => w.lockListeners.delete(fn); },
    ensure() { w.dialogs++; throw new Error('a dialog'); }, call() { w.dialogs++; throw new Error('a dialog'); } };
  w.unlock = () => { w.lockHeld = true; for (const f of [...w.lockListeners]) f(); };
  w.relock = () => { w.lockHeld = false; for (const f of [...w.lockListeners]) f(); };
  // navigator.locks, shared by the tabs: one holder per name at a time
  const chain = new Map();
  w.locks = { request: (name, fn) => { const prev = chain.get(name) || Promise.resolve(); const p = prev.catch(() => {}).then(() => fn()); chain.set(name, p); return p; }, reset: () => chain.clear() };
  // faults
  const take = async point => {
    const q = w.faults[point]; if (!q || !q.length) return null; const f = q.shift();
    if (f === 'hang') await new Promise(() => {});
    if (f === 'transient') throw transient();
    if (f === 'error') throw new Error('the server said something odd');
    return f;
  };
  // the cloud
  const sameHash = (h) => { for (const [s, e] of srv.entries) if (e.hash === h) return s; return ''; };
  const api = async (fn, b) => {
    if (!w.online) throw transient();
    srv.calls.push(b.op);
    if (b.op === 'customSaveGet') { await take('get'); return { ok: true, save: srv.records.get(String(b.rid)) || null }; }
    if (b.op === 'customSkuCheck') { await take('check'); srv.checks = (srv.checks || []).concat([b.candidates.length]); const free = b.candidates.find(c => !srv.entries.has(c)); return free ? { ok: true, free, taken: [] } : { ok: false, error: 'none' }; }
    if (b.op === 'customEtsySkuWrite') {
      srv.etsyCalls++; const f = await take('etsy'); const rec = srv.records.get(String(b.rid));
      if (!rec) return { ok: false, error: 'no-save', reason: 'no-save', retry: false, text: 'no save for this order' };
      if (f && f.etsy) return Object.assign({ ok: false }, f.etsy);
      if (rec.etsy.state !== 'linked') { rec.etsy = { state: 'linked' }; srv.etsyApplied++; }
      if (f === 'lost') throw transient();
      if (f === 'lost-hang') await new Promise(() => {});
      return { ok: true, state: 'linked' };
    }
    throw new Error('unexpected op ' + b.op);
  };
  // Master.saveCustom as the bridge does it (build, token, two uploads, create-only commit) with a hook for the switch (hooks.alive)
  const Master = { entryFor: s => (srv.entries.has(String(s).toUpperCase()) ? { sku: String(s).toUpperCase() } : null), looseFor: () => '', sameDesign: h => sameHash(h),
    async saveCustom(spec, hooks) {
      const fail = (reason, text, extra) => Object.assign({ ok: false, reason, text }, extra || {});
      const alive = () => !hooks.alive || hooks.alive() !== false;
      try {
        if (w.gate) { const g = w.gate; w.gate = null; await g; }   // (a test holds the first save here, as a slow network would)
        let built = spec.built || null;
        if (!spec.reuse && !built) { await take('build'); srv.builds++; built = { ai: new Uint8Array(40), png: new Uint8Array(8) }; hooks.built(built); }
        if (spec.rehearsal) return { ok: true, rehearsed: true };
        if (!w.online) return fail('offline', 'you are offline');
        let tok = w.lock.find('upload'); if (!tok) tok = await hooks.needPassword(); if (!tok) return fail('locked', 'needs the repository password');
        const up = Object.assign({}, spec.uploaded || {});
        if (!spec.reuse) for (const which of ['ai', 'png']) {
          if (up[which]) continue;
          if (!alive()) return fail('cancelled', 'switched off');
          await take('upload-' + which); srv.uploads.push(spec.sku + '.' + which); srv.files.set(spec.sku + '.' + which, 1); up[which] = { path: which }; hooks.uploaded(which, up[which]);
        }
        if (!alive()) return fail('cancelled', 'switched off');
        const f = await take('commit');
        if (f === 'taken') return fail('sku-taken', 'that SKU was just taken');
        if (f && f.fail) return fail(f.fail, f.text || f.fail, f.extra || {});
        const rid = String(spec.rid), have = srv.records.get(rid);
        let sku = spec.sku, created = true;
        if (have) { sku = have.sku; created = false; }
        else {
          const same = sameHash(spec.charm.hash);
          if (same) { sku = same; created = false; }
          else if (srv.entries.has(sku)) return fail('sku-taken', 'that SKU was just taken');
          else { for (const which of ['ai', 'png']) if (!srv.files.has(sku + '.' + which)) return fail('not-uploaded', 'the file did not reach the repository'); srv.entries.set(sku, { hash: spec.charm.hash }); }
          srv.records.set(rid, { rid, sku, phase: 'repo', reused: !created, etsy: { state: 'pending' }, at: w.now }); srv.applied++;
        }
        if (f === 'lost') throw transient();
        if (f === 'lost-hang') await new Promise(() => {});
        return { ok: true, sku, created, entry: { sku }, alias: 'bySku' };
      } catch (e) { const net = !!e && e.transient === true; return fail(net ? 'offline' : 'error', net ? 'you could not reach the cloud' : String(e.message || e)); }
    } };
  w.Master = Master;
  w.design = opt.design || { rid: RID, listingId: LISTING, hash: HASH, charmHash: 'c1c2c3c4c5c6c7c8', name: 'Acorns_Leaf.ai', title: "Acorn&#39;s Leaf Charm", sku: 'ACORN&#39;S LEAF' };
  w.S = { cloud: { get ok() { return w.online; } } };
  // the page hands the design over as it is NOW
  w.resolve = async rec => {
    if (opt.gone && opt.gone()) return { state: 'gone', why: 'the file was removed' };
    if (!w.ready) return { state: 'wait', wait: 'data', why: 'the design is being read' };
    const d = Object.assign({}, w.design, { rid: rec.rid, listingId: rec.listingId });
    return { state: 'ready', ctx: { rid: rec.rid, listingId: rec.listingId, title: d.title, line: { sku: d.sku, productId: '', variations: [{ name: 'Metal', value: 'GF', propertyId: 513, valueId: 1234 }], listingId: rec.listingId }, listingKind: null,
      F: { name: d.name, kind: 'ai', bytes: new Uint8Array(500), hash: rec.hash, pieces: 1, state: 'ready' }, charm: { hash: d.charmHash, open: false }, parsed: { ok: 1 }, by: 'Paul', sandbox: !!rec.sandbox, S: w.S, O, Sku: NameSku, Master,
      aliases: {}, tableFor: () => (w.tableReady ? null : null), api, lock: w.lock, designs: 1, now: () => w.now } };
  };
  w.mk = (tab) => {
    const k = Keep.durable({ store: w.store, now: () => w.now, setTimeout: w.setTimeout, clearTimeout: w.clearTimeout, tab: tab || 'tab' + (w.keepers.length + 1), online: () => w.online, lock: w.lock, locks: opt.noLocks ? null : w.locks,
      saveBuilt: async (rec, b) => { w.builtStore.set(rec.hash, b); }, loadBuilt: async rec => w.builtStore.get(rec.hash) || null, dropBuilt: async rec => { w.builtStore.delete(rec.hash); },
      resolve: w.resolve, make: ctx => Keep.make(ctx), etsy: rec => api('charmNestLibrary', { op: 'customEtsySkuWrite', rid: rec.rid, listingId: rec.listingId }) });
    k.log = []; k.on(ev => { if (ev.type === 'state') k.log.push(ev.view.phase); }); w.keepers.push(k); return k;
  };
  w.keeper = w.mk('tab1');
  w.spec = (over) => Object.assign({ sandbox: !!opt.sandbox, rid: RID, listingId: LISTING, hash: HASH, name: 'Acorns_Leaf.ai', kind: 'ai', by: 'Paul', ck: 'custom:' + RID, title: 'Acorn', line: { sku: 'ACORN&#39;S LEAF' } }, over || {});
  w.state = (k) => (k || w.keeper).list().map(r => r.state + ':' + r.wait)[0];
  w.rec = (k) => (k || w.keeper).list()[0];
  /** the page was killed (the old tab's work stops where it is) and loaded again: a new tab over the same storage */
  w.reload = () => { w.keeper.close(); w.locks.reset(); w.timers.forEach(t => { t.off = true; }); w.keeper = w.mk('tab' + (w.keepers.length + 1)); w.keeper.resume('load'); return w.keeper; };
  return w;
}

(async () => {
  // ─── 1 · recorded before any step ───
  {
    const w = world(); const res = w.keeper.begin(w.spec());
    ok(res.created && res.key === 'live|' + RID + '|' + LISTING + '|' + HASH, 'the intent key is order + listing + file hash');
    const stored = JSON.parse(w.ls.get('cn.keepIntents.v1')).intents[res.key];
    ok(stored && stored.state === 'wanted' && stored.hash === HASH && stored.rid === RID, 'the intent is in browser storage when begin() returns (before any step)');
    eq(w.srv.calls, [], 'and nothing has been asked of the cloud yet'); eq(w.srv.uploads, [], 'nothing uploaded'); eq(w.srv.builds, 0, 'nothing built');
    eq(w.keeper.describe(stored).phase, 'saving', 'the one calm line says saving');
    await w.pump(0); await w.keeper.idle();
    ok(w.rec().state === 'done' && w.rec().outcome === 'linked', 'it then runs by itself, nobody pressed run(): linked');
    eq([w.srv.applied, w.srv.etsyApplied, w.srv.entries.size, w.srv.uploads.length, w.dialogs], [1, 1, 1, 2, 0], 'one commit, one Etsy write, one entry, two uploads, no dialog');
    eq(w.keeper.describe(w.rec()).line, 'Saved and linked · ' + SKU, 'the line says saved and linked with the SKU');
  }

  // ─── 2 · a fault at EVERY step: a passing failure, then the page killed and reloaded there ───
  const POINTS = [['get', 'customSaveGet'], ['check', 'customSkuCheck'], ['build', 'build'], ['upload-ai', 'upload .ai'], ['upload-png', 'upload .png'], ['commit', 'commit (before)'], ['commit-lost', 'commit (reply lost)'], ['etsy', 'Etsy'], ['etsy-lost', 'Etsy (reply lost)']];
  for (const [point, label] of POINTS) {
    const lost = point.endsWith('-lost'), pt = point.replace('-lost', '');
    // (a) a passing failure there: the keeper backs off and finishes by itself
    {
      const w = world({ faults: { [pt]: [lost ? 'lost' : 'transient'] } }); w.keeper.begin(w.spec());
      for (let i = 0; i < 12 && w.rec().state !== 'done'; i++) await w.pump(i === 0 ? 0 : 400000);
      await w.keeper.idle();
      ok(w.rec().state === 'done' && w.rec().outcome === 'linked', label + ': a passing failure is retried by itself and the save ends linked (' + w.rec().state + ')');
      eq([w.srv.applied, w.srv.etsyApplied, w.srv.entries.size], [1, 1, 1], label + ': exactly one commit, one Etsy change, one entry');
      ok(w.srv.uploads.length <= 4 && w.dialogs === 0, label + ': at most one repeated upload, no dialog');
    }
    // (b) the page killed there (the call never answers) and loaded again: the intent is found and continues
    {
      const w = world({ faults: { [pt]: [lost ? 'lost-hang' : 'hang'] } });
      w.keeper.begin(w.spec()); await w.pump(0);
      ok(w.rec().state === 'running', label + ': killed here, the record says running');
      w.reload(); await w.pump(0); for (let i = 0; i < 6 && w.rec().state !== 'done'; i++) await w.pump(400000);
      ok(w.rec().state === 'done' && w.rec().outcome === 'linked', label + ': after a reload the save continues from the record and ends linked (' + w.rec().state + ')');
      eq([w.srv.applied, w.srv.etsyApplied, w.srv.entries.size], [1, 1, 1], label + ': after the reload still exactly one commit, one Etsy change, one entry');
    }
  }

  // ─── 2b · a reload does not build or upload again ───
  {
    const w = world({ faults: { 'upload-png': ['hang'] } }); w.keeper.begin(w.spec()); await w.pump(0);
    eq([w.srv.builds, w.srv.uploads], [1, ['ACORNS_LEAF_4821.ai']], 'killed before the second upload: one build, the first file uploaded');
    ok(w.rec().up && w.rec().up.ai === true && !w.rec().up.png && w.builtStore.size === 1, 'the record keeps the upload mark and the built file is kept in the browser');
    w.reload(); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.srv.builds, w.srv.uploads.length, w.srv.applied, w.srv.etsyApplied, w.builtStore.size], ['done', 1, 2, 1, 1, 0], 'after the reload: NOT built again, the first file NOT uploaded again, the second uploaded, one commit, one Etsy change, the kept copy dropped');
  }

  // ─── 3 · waiting: the password, the connection, data not ready ───
  {
    const w = world({ locked: true }); w.lockHeld = false;
    w.keeper.begin(w.spec()); await w.pump(0);
    eq([w.state(), w.keeper.describe(w.rec()).phase], ['waiting:password', 'password'], 'no password yet: the save waits for the password (state, calm line)');
    ok(/password/i.test(w.keeper.describe(w.rec()).line) && /by itself/.test(w.keeper.describe(w.rec()).line), 'the line says it waits for the password and saves by itself');
    eq([w.srv.calls, w.srv.uploads, w.dialogs], [[], [], 0], 'nothing was asked, uploaded or opened while it waits (the gate is kept, never auto-filled)');
    await w.pump(120000);   // minutes pass, the window was closed, a tab was switched: still waiting
    eq(w.state(), 'waiting:password', 'minutes later it still waits');
    w.unlock(); await w.pump(0); await w.keeper.idle();
    ok(w.rec().state === 'done' && w.rec().outcome === 'linked', 'the moment the lock opens it completes by itself: nobody pressed Save');
    eq([w.srv.applied, w.srv.etsyApplied, w.dialogs], [1, 1, 0], 'one commit, one Etsy change, no dialog');
  }
  {
    // the lock opened in ANOTHER tab (the storage event reaches this tab as external()) and after a reload with the password still given
    const w = world(); w.lockHeld = false; w.keeper.begin(w.spec()); await w.pump(0);
    w.reload(); await w.pump(0); eq(w.state(), 'waiting:password', 'reloaded with no password: still waiting, from the record');
    w.lockHeld = true; w.keeper.external(); await w.pump(0); await w.keeper.idle();
    ok(w.rec().state === 'done', 'unlocked elsewhere: this tab hears it (storage) and completes');
  }
  {
    const w = world({ online: false }); w.keeper.begin(w.spec()); await w.pump(0);
    eq([w.state(), w.keeper.describe(w.rec()).phase], ['waiting:connection', 'connection'], 'offline: waiting for the connection');
    eq(w.srv.calls, [], 'nothing is attempted while offline');
    w.online = true; w.keeper.resume('online'); await w.pump(0); await w.keeper.idle();
    ok(w.rec().state === 'done', 'back online it completes by itself');
    // the connection drops in the middle of the run (a failure reads offline): it waits and goes on
    const v = world({ faults: { 'upload-png': ['transient'] } }); v.keeper.begin(v.spec()); await v.pump(0);
    v.online = false; await v.pump(30000);
    eq(v.rec().wait, 'connection', 'a connection lost mid-run: waiting for the connection');
    v.online = true; v.keeper.resume('online'); await v.pump(0); for (let i = 0; i < 5 && v.rec().state !== 'done'; i++) await v.pump(300000);
    eq([v.rec().state, v.srv.applied, v.srv.etsyApplied], ['done', 1, 1], 'and it ends linked after the connection came back');
  }
  {
    const w = world({ ready: false }); w.keeper.begin(w.spec()); await w.pump(0);
    eq([w.state(), w.srv.calls, w.srv.builds], ['waiting:data', [], 0], 'the file is still being read: it waits, builds nothing, calls nothing (a half-read file is never saved)');
    for (let i = 0; i < 6; i++) await w.pump(20000);
    eq(w.state(), 'waiting:data', 'it polls gently and still waits');
    w.ready = true; await w.pump(20000); await w.keeper.idle();
    ok(w.rec().state === 'done' && w.srv.applied === 1, 'the file is read: the save completes by itself');
  }

  // ─── 4 · the switch off ───
  {
    // off before anything: nothing is saved
    const w = world({ locked: true }); w.lockHeld = false; const r = w.keeper.begin(w.spec()); await w.pump(0);
    eq(w.keeper.cancel(r.key), { ok: true }, 'switch off while waiting: cancelled'); w.unlock(); await w.pump(0); await w.keeper.idle();
    eq([w.state(), w.srv.applied, w.srv.uploads.length, w.keeper.describe(w.rec()).line], ['off:', 0, 0, ''], 'nothing was saved and the line is empty (nothing saved)');
  }
  {
    let release; const w = world(); w.gate = new Promise(r => { release = r; });
    const r = w.keeper.begin(w.spec());
    await w.pump(0);
    ok(w.rec().state === 'running', 'mid-run, before the commit');
    eq(w.keeper.cancel(r.key, 'switched off'), { ok: true }, 'the switch off before the commit is accepted'); release(); await w.pump(0); await w.keeper.idle();
    eq([w.state(), w.srv.applied, w.srv.entries.size, w.srv.etsyApplied], ['off:', 0, 0, 0], 'mid-run off: no commit, no entry, no Etsy change (fully not saved)');
    // and a later switch on starts again cleanly
    const again = w.keeper.begin(w.spec()); ok(again.created, 'switching on again makes a fresh intent'); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.srv.applied, w.srv.entries.size], ['done', 1, 1], 'which then completes: one entry');
  }
  {
    // off AFTER the commit (before Etsy): too late to un-save, so the link is completed, never half
    const w = world({ faults: { etsy: ['transient'] } }); const r = w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    ok(w.rec().repo === true && w.rec().state === 'waiting', 'saved to the repository, Etsy failed once, retrying');
    const c = w.keeper.cancel(r.key); eq([c.ok, c.committed], [false, true], 'the switch off after the commit is refused honestly (committed)');
    await w.pump(300000); await w.keeper.idle();
    eq([w.rec().state, w.rec().outcome, w.srv.etsyApplied], ['done', 'linked', 1], 'and the link is completed: fully linked');
  }

  // ─── 5 · two tabs, double press, a second file, two orders of one listing ───
  {
    const w = world(); const a = w.keeper, b = w.mk('tab2');
    a.begin(w.spec()); const second = b.begin(w.spec());
    ok(!second.created, 'the second tab pressing the same switch finds the same intent (no duplicate)');
    const third = a.begin(w.spec()); ok(!third.created, 'a double press makes no second intent');
    await w.pump(0); b.resume('load'); await w.pump(0); await a.idle(); await b.idle();
    eq([w.keeper.list().length, w.srv.applied, w.srv.etsyApplied, w.srv.entries.size, w.rec().state], [1, 1, 1, 1, 'done'], 'two tabs, one intent, one commit, one Etsy change, linked');
  }
  {
    const w = world({ locked: true }); w.lockHeld = false; const a = w.keeper;
    const first = a.begin(w.spec()); const other = a.begin(w.spec({ hash: 'cd'.repeat(32) }));
    ok(other.conflict && other.key === first.key, 'a second file for the same order and listing does not start a second save: the first design stays (deterministic)');
    eq(a.list().length, 1, 'one intent only');
  }
  {
    // two orders of the same listing, the same file, saving at once: one wins, the other is "already linked", no duplicate entry
    const w = world(); const a = w.keeper;
    a.begin(w.spec({ rid: '4196630001' })); a.begin(w.spec({ rid: '4196630002', ck: 'custom:4196630002' })); await w.pump(0); await a.idle();
    const rs = a.list();
    eq([rs.length, rs.every(r => r.state === 'done'), w.srv.entries.size, w.srv.applied], [2, true, 1, 2], 'two orders, one listing, one file: both end linked, ONE entry in the repository (the second reuses the first)');
    ok(new Set(rs.map(r => r.sku)).size === 1, 'both name the same SKU');
  }
  {
    const w = world(); w.srv.entries.set(SKU, { hash: 'ff'.repeat(8) });   // a SKU taken by ANOTHER design
    w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    ok(w.rec().state === 'done' && w.rec().sku !== SKU && w.srv.entries.size === 2, 'the planned SKU is taken by another design: the next free SKU is used, no question asked (' + w.rec().sku + ')');
  }

  // ─── 6 · backoff, needs attention, terminal reasons, Try again ───
  {
    const w = world({ faults: { commit: Array(20).fill('error') } }); w.keeper.begin(w.spec()); let seen = [];
    for (let i = 0; i < 40 && w.rec().state !== 'attention'; i++) { await w.pump(i === 0 ? 0 : 330000); seen.push(w.rec().fails); }
    eq(w.rec().state, 'attention', 'a failure that never ends becomes ONE needs-attention line after the tries');
    ok(/^Needs attention: /.test(w.keeper.describe(w.rec()).line), 'the line says what needs attention');
    ok(w.rec().fails >= 8 && w.rec().fails <= 9, 'after eight tries (' + w.rec().fails + ')');
    const calls = w.srv.calls.length; await w.pump(900000); eq(w.srv.calls.length, calls, 'and it stops by itself (no endless loop)');
    w.faults.commit = []; ok(w.keeper.retry(w.rec().key), 'Try again is accepted'); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.srv.applied], ['done', 1], 'Try again completes the save');
  }
  {
    // the pauses grow
    const w = world({ faults: { commit: Array(5).fill('error') } }); w.keeper.begin(w.spec()); await w.pump(0);
    const gaps = []; let last = w.now;
    for (let i = 0; i < 4; i++) { const at = w.rec().nextAt; gaps.push(at - w.now); await w.pump(at - w.now + 1); }
    ok(gaps[1] > gaps[0] && gaps[2] > gaps[1], 'the pause before each new try grows (' + gaps.join(', ') + ' ms)');
  }
  {
    // a design that can never be saved (bad geometry): one attention line at once, nothing retried
    const w = world(); const orig = w.Master.saveCustom; w.Master.saveCustom = async () => ({ ok: false, reason: 'geometry', text: 'the design cannot be kept as a charm (open outline)' });
    w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().fails, /open outline/.test(w.keeper.describe(w.rec()).line)], ['attention', 1, true], 'bad geometry: needs attention at once, with the reason, no retry loop');
  }
  {
    // Etsy refuses for good: the repository save stays, one attention line; a later page load tries again once and finishes when it was fixed
    const w = world({ faults: { etsy: [{ etsy: { error: 'scope', reason: 'scope', retry: false, text: 'Etsy refused the change. Paul needs to re-authorise Etsy with listings_w' } }] } });
    w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().repo, w.rec().auto], ['attention', true, 'load'], 'Etsy needs re-authorising: the repository save stands, one attention line');
    ok(/re-authorise/.test(w.keeper.describe(w.rec()).line), 'the line says the one thing to do');
    w.reload(); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().outcome, w.srv.applied, w.srv.etsyApplied], ['done', 'linked', 1, 1], 'a later page load tries Etsy again and links it (nothing is saved twice)');
  }
  {
    const w = world({ faults: { check: [{ x: 1 }] } });   // an odd (object) fault must not wedge the keeper
    w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle(); ok(['done', 'waiting', 'attention'].includes(w.rec().state), 'an odd answer never wedges the keeper (' + w.rec().state + ')');
  }

  // ─── 7 · sleep and wake, the order gone, the file removed ───
  {
    const w = world({ faults: { commit: ['transient'] } }); w.keeper.begin(w.spec()); await w.pump(0);
    eq(w.rec().wait, 'retry', 'a failure schedules a pause');
    w.now += 6 * 3600000; w.timers.forEach(t => { t.off = true; });   // the computer slept: the timers never fired
    w.keeper.resume('wake'); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.srv.applied], ['done', 1], 'after sleep, a wake-up resumes by itself and completes');
  }
  {
    let gone = false; const w = world({ locked: true, gone: () => gone }); w.lockHeld = false; w.keeper.begin(w.spec()); await w.pump(0);
    gone = true; w.unlock(); await w.pump(0); await w.keeper.idle();
    eq([w.state(), w.srv.applied], ['off:', 0], 'the file was removed from the order while it waited: nothing is saved, the line is empty');
    ok(/removed/.test(w.rec().why), 'and the record says why');
  }
  {
    let gone = false; const w = world({ gone: () => gone, faults: { etsy: ['transient'] } }); w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    gone = true; await w.pump(300000); await w.keeper.idle();
    ok(w.rec().repo === true && ['attention', 'done'].includes(w.rec().state), 'the file went after the repository save: the save stands (it is never un-saved)');
  }

  // ─── 7b · the cloud's newer answers and the page-load helpers ───
  {
    // the listing's answer is ANOTHER design's: final and calm, no Etsy step, no retry
    const w = world({ faults: { commit: [{ fail: 'listing-linked', text: 'this listing is already linked to OTHER_0001', extra: { sku: 'OTHER_0001' } }] } }); w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().outcome, w.rec().sku, w.srv.etsyCalls, w.srv.applied], ['done', 'already', 'OTHER_0001', 0, 0], 'listing-linked: done, calm, no Etsy step, nothing written, no retry');
    eq(w.keeper.describe(w.rec()).line, 'Already linked to OTHER_0001', 'and the line says so');
  }
  {
    // Etsy is being updated by another run (a lease): a pause of about 45 s, not a failure
    const w = world({ faults: { etsy: [{ etsy: { error: 'running', reason: 'running', retry: true, text: '' } }] } }); w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().wait, w.rec().fails, w.rec().nextAt - w.now > 40000], ['waiting', 'retry', 0, true], 'Etsy "running": waits about 45 s, no failure counted');
    await w.pump(60000); await w.keeper.idle(); eq([w.rec().state, w.rec().outcome, w.srv.etsyApplied], ['done', 'linked', 1], 'and then links');
  }
  {
    // a save in the repository whose Etsy step is not done (made by another computer): this page does the step, no design needed
    const w = world(); w.srv.records.set('4196630999', { rid: '4196630999', sku: 'LEAF_9999', phase: 'repo', etsy: { state: 'pending' } });
    ok(w.keeper.adoptPending([{ rid: '4196630999', listingId: LISTING, sku: 'LEAF_9999' }], false) === 1, 'a pending Etsy step is adopted');
    ok(w.keeper.adoptPending([{ rid: '4196630999', listingId: LISTING, sku: 'LEAF_9999' }], false) === 0, 'and only once');
    await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().etsyOnly, w.srv.etsyApplied, w.srv.builds, w.srv.uploads.length], ['done', true, 1, 0, 0], 'the Etsy step ran from the record alone: nothing built, nothing uploaded');
  }
  {
    // a design that belongs to another workspace (sandbox page / live page) is left alone; one not found anywhere is reported, not waited for for ever
    const w = world(); const orig = w.resolve; let mode = 'skip'; const k = Keep.durable({ store: w.store, now: () => w.now, setTimeout: w.setTimeout, clearTimeout: w.clearTimeout, online: () => true, lock: w.lock, resolve: async () => ({ state: mode }), make: ctx => Keep.make(ctx) });
    k.begin(w.spec()); await w.pump(0); await k.idle(); ok(k.list()[0].state === 'wanted', 'skip: another workspace\'s intent is left as it is');
    mode = 'lost'; k.resume('x'); await w.pump(0); await k.idle(); ok(k.list()[0].state === 'attention' && /drop the file again/.test(k.describe(k.list()[0]).line), 'lost: one attention line that says what to do');
  }
  {
    // the cloud says the record needs repair: the SAME save runs again under the same SKU; the engine does not read it as linked
    const w = world(); w.srv.records.set(RID, { rid: RID, sku: 'LEAF_5555', phase: 'repo', etsy: { state: 'pending' }, repair: { state: 'files-missing' } });
    const x = await w.resolve({ rid: RID, listingId: LISTING, hash: HASH }); const e = Keep.make(x.ctx); const info = await e.open();
    eq([info.offered, info.saved, info.sku], [true, null, 'LEAF_5555'], 'a record flagged for repair is a normal run again under its SKU, not "already saved"');
  }
  {
    // SKU candidates used up: the engine still asks the cloud with the refused ones (it always finds a free SKU), never ends "No free SKU"
    const w = world({ faults: { commit: ['taken', 'taken', 'taken', 'taken', 'taken', 'taken', 'taken'] } }); w.keeper.begin(w.spec());
    for (let i = 0; i < 20 && w.rec().state !== 'done' && w.rec().state !== 'attention'; i++) await w.pump(i === 0 ? 0 : 330000);
    ok(w.rec().state === 'done' && (w.srv.checks || []).every(c => c >= 1), 'a run of taken SKUs: the cloud is always asked with at least one candidate, and the save ends (' + w.rec().state + ', checks ' + (w.srv.checks || []).join() + ')');
  }

  {
    // two tabs waiting for the password: a write in one wakes the other (the storage event), which must not write back for ever
    const w = world(); w.lockHeld = false; const a = w.keeper, b = w.mk('tab2'); let writes = 0; const set = w.store.set;
    w.store.set = (k, v) => { writes++; set(k, v); setImmediate(() => { for (const x of [a, b]) x.external(); }); };
    a.begin(w.spec()); await w.pump(0); for (let i = 0; i < 6; i++) await w.pump(60000);
    ok(writes > 0 && writes < 12, 'two waiting tabs do not wake each other in a loop (' + writes + ' writes in six minutes)');
    eq(w.state(a), 'waiting:password', 'and both still wait for the password');
    w.unlock(); await w.pump(0); await a.idle(); await b.idle(); await w.pump(0);
    eq([w.rec().state, w.srv.applied, w.srv.etsyApplied], ['done', 1, 1], 'unlocked: one commit and one Etsy change though two tabs were waiting');
  }

  // ─── 8 · the sandbox rehearses, never writes the real repository ───
  {
    const w = world({ sandbox: true }); const r = w.keeper.begin(w.spec({ sandbox: true }));
    ok(/^sbx\|/.test(r.key), 'a sandbox intent has its own key (never mixed with a real one)');
    await w.pump(0); await w.keeper.idle();
    eq([w.rec().state, w.rec().outcome, w.srv.applied, w.srv.uploads.length, w.srv.etsyCalls], ['done', 'rehearsed', 0, 0, 0], 'rehearsed: nothing committed, nothing uploaded, Etsy never called');
    eq(w.keeper.describe(w.rec()).line, 'Rehearsal complete. Nothing was saved.', 'and the line says so');
    const live = w.keeper.begin(w.spec({ sandbox: false })); ok(live.created && live.key !== r.key, 'a real switch-on in the same page is a different intent');
  }

  // ─── 9 · after it is done, a reload only shows it ───
  {
    const w = world(); w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    const calls = w.srv.calls.length; w.reload(); await w.pump(0); await w.reload().idle?.call(w.keeper);
    eq([w.srv.calls.length, w.keeper.describe(w.rec()).line], [calls, 'Saved and linked · ' + SKU], 'a finished save shown after a reload makes no call at all');
    // the intent stays across reloads while it waits, and survives unusable storage as memory for this tab
    const v = world(); v.storeFails = true; v.keeper.begin(v.spec()); await v.pump(0); await v.keeper.idle();
    eq([v.keeper.list().length, v.keeper.list()[0].state, v.srv.applied], [1, 'done', 1], 'storage blocked (private window): the save still runs and ends, the tab remembers it');
  }
  {
    // without navigator.locks the same thing holds in one tab
    const w = world({ noLocks: true }); w.keeper.begin(w.spec()); w.keeper.begin(w.spec()); await w.pump(0); await w.keeper.idle();
    eq([w.srv.applied, w.srv.etsyApplied, w.rec().state], [1, 1, 'done'], 'no Web Locks: one tab, one commit, one Etsy change');
  }
  // ─── 10 · the engine alone still writes nothing until run() ───
  {
    const w = world(); const x = await w.resolve({ rid: RID, listingId: LISTING, hash: HASH }); const k = Keep.make(x.ctx);
    const info = await k.open(); ok(info.offered, 'the engine offers the save'); eq([w.srv.applied, w.srv.uploads.length], [0, 0], 'open() wrote nothing');
    const g = world(); const y = await g.resolve({ rid: RID, listingId: LISTING, hash: HASH }); y.ctx.alive = () => false; const e = Keep.make(y.ctx); await e.open();
    const out = await e.run(); eq([out.done, e.outcome().reason, g.srv.applied], ['held', 'cancelled', 0], 'the engine stops before its first step when the switch is off (ctx.alive)');
  }
  console.log('keep-durable: ' + n + ' checks passed');
})().catch(e => { console.error(e); process.exit(1); });
