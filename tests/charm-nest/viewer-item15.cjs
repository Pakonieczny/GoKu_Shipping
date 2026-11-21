// ITEM 15 (Paul 21 Nov 2026: "messages must tell the truth"): the boot message of a viewer (mode "on") about the design saves begun on this computer, offline.
// The real charm-nest-viewer.js in a vm page with stubs (CN.api answers driveGet and customSaveGet from a fake cloud; localStorage is a Map kept across "reloads").
//   A  the save intents the cloud finished (linked on Etsy, or matched by the listing's own answer) are removed from this browser and not counted; done, switched-off and
//      rehearsal intents are never asked about nor counted; what is really unfinished is said once, in calm words, naming the orders; only READ ops are sent
//   B  a second reload with the same intents says nothing; a change (this page's keeper now resumes them: item 14) is said once, then nothing again; a new intent is said
//   C  item 14 merged: the page's keeper resumes them and says "Finishing N design save(s)" itself (charm-nest-bridge.js keepViewerResume), so the viewer says nothing about the
//      live ones (never twice), whichever name the keeper exposes; one that needs a person is still said; a cloud that does not answer: kept (never guessed finished)
//   D  mode off: nothing is said, nothing asked, nothing removed; the old record shape { v, intents } with no intent says nothing (the old code said "2")
//   NODE_PATH=<deps> node tests/charm-nest/viewer-item15.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
const SRC = fs.readFileSync(path.join(ROOT, 'charm-nest-viewer.js'), 'utf8');
const KEY = 'cn.keepIntents.v1', SAID = 'cn.keepIntentsSaid.v1';
let failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
process.on('unhandledRejection', e => { console.log('FAIL an unhandled rejection: ' + ((e && e.stack) || e)); failed++; });

const T = Date.UTC(2026, 10, 21, 9, 0, 0);
const rec = (key, o) => Object.assign({ v: 1, key, sandbox: false, rid: '', listingId: '', hash: 'h-' + key, name: key + '.ai', kind: 'ai', by: 'Ann', ck: 'ck-' + key, title: '', line: null, at: T - 3600000, upd: T - 60000, state: 'wanted', wait: '', why: '', sku: '', repo: false, etsy: false, fails: 0, nextAt: 0, auto: '', outcome: '', tab: 't1' }, o);
/** The intents of this computer's browser (charm-nest-keep.js's record shape). */
function seedIntents() {
  const list = [
    rec('k1', { rid: '4100000001', listingId: '1700000001', state: 'waiting', wait: 'retry' }),                    // the cloud linked it on Etsy → finished
    rec('k2', { rid: '4100000002', listingId: '1700000002', state: 'running', repo: true, sku: 'CU-AB12' }),       // in the repository, the listing's answer matches → finished
    rec('k3', { rid: '4100000003', listingId: '1700000003', state: 'wanted' }),                                    // nothing in the cloud → unfinished, not in the repository
    rec('k4', { rid: '4100000004', listingId: '1700000004', state: 'waiting', wait: 'retry', repo: true, sku: 'CU-CD34' }),   // in the repository, Etsy still to do → unfinished
    rec('k5', { rid: '4100000005', listingId: '1700000005', state: 'done', outcome: 'linked', repo: true, etsy: true, sku: 'CU-EF56' }),
    rec('k6', { rid: '4100000006', listingId: '1700000006', state: 'off', why: 'switched off' }),
    rec('k7', { rid: '4100000007', listingId: '1700000007', state: 'wanted', sandbox: true }),
    rec('k8', { rid: '4100000008', listingId: '1700000008', state: 'wanted', at: T - 600000 })                      // a correction: the listing's record is older than the switch → unfinished
  ];
  const intents = {}; for (const r of list) intents[r.key] = r;
  return JSON.stringify({ v: 1, intents });
}
/** The cloud's save records (customSaveGet), by order. */
const SAVES = {
  '4100000001': { id: 's1', rid: '4100000001', listingId: '1700000001', sku: 'CU-ZZ01', phase: 'repo', etsy: { state: 'linked' }, at: T - 1800000, alias: 'whole', oldSku: '' },
  '4100000002': { id: 's2', rid: '4100000002', listingId: '1700000002', sku: 'CU-AB12', phase: 'repo', etsy: { state: 'pending' }, at: T - 1800000, alias: 'bySku', oldSku: 'SHOP-OK' },
  '4100000004': { id: 's4', rid: '4100000004', listingId: '1700000004', sku: 'CU-CD34', phase: 'repo', etsy: { state: 'pending' }, at: T - 1800000, alias: 'none', oldSku: 'SHOP-NO' },
  '4100000008': { id: 's8', rid: '4100000008', listingId: '1700000008', sku: 'CU-OLD8', phase: 'repo', etsy: { state: 'linked' }, at: T - 86400000, alias: 'whole', oldSku: '' }
};

/** One page load: the real viewer file in a fresh context; the browser's storage is the Map passed in (it survives the "reload"). */
function page(o) {
  const calls = [], toasts = [], timers = [];
  const store = o.store;
  const w = { console: { log() {}, warn() {}, error() {} }, Promise, Date, JSON, Math, Object, Array, Set, Map, String, Number, Intl,
    setTimeout: (f, ms) => { timers.push({ f, ms }); return timers.length; }, clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    performance: { now: () => 1000 }, location: { reload() { calls.push({ op: 'reload' }); } } };
  w.window = w;
  w.localStorage = { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) };
  w.toast = (t, tone, ms) => { toasts.push({ t, tone, ms }); return {}; };
  w.Session = { freeze() { calls.push({ op: 'freeze' }); } };
  w.CharmNestSku = { aliasWorks: (old, sku) => old === 'SHOP-OK' };
  if (o.keepName === 'lib') w.CharmNestKeep = { resumesOnViewer: true };   // (charm-nest-keep.js's own flag, a boolean)
  if (o.keepName === 'sheet') w.CustomSheet = { keepResumes: () => true };
  w.CN = { api: async (fn, body) => {
    calls.push(body);
    if (body.op === 'driveGet') return { ok: true, mode: o.mode || 'on', drive: { mode: o.mode || 'on', cloudSteps: [] } };
    if (body.op === 'customSaveGet') { if (o.down) throw new Error('Failed to fetch'); const s = SAVES[body.rid]; return s ? { ok: true, save: Object.assign({}, s) } : { ok: true, save: null }; }
    return { ok: true };
  } };
  vm.runInContext(SRC, vm.createContext(w));
  return { w, calls, toasts, timers,
    /** boot(), then the boot's delayed check (the 4 s timer), awaited */
    async load() { const on = await w.CNViewer.boot(); for (const t of timers.filter(t => t.ms === 4000)) await t.f(); return on; } };
}
const intentsIn = store => Object.keys((JSON.parse(store.get(KEY) || '{}').intents) || {}).sort();

(async () => {
  /* ── A ── */
  const store = new Map([[KEY, seedIntents()]]);
  { const p = page({ store });
    const on = await p.load();
    const asked = p.calls.filter(c => c.op === 'customSaveGet').map(c => c.rid).sort();
    ok('A1 mode on: the page boots as a viewer (Session frozen)', on === true && p.calls.some(c => c.op === 'freeze'));
    ok('A2 only the open intents of this workspace are asked of the cloud (customSaveGet): never a done, switched-off or rehearsal one', JSON.stringify(asked) === JSON.stringify(['4100000001', '4100000002', '4100000003', '4100000004', '4100000008']), asked);
    ok('A3 only READ ops are sent (driveGet, customSaveGet): no save, no repair, no intent written to the cloud', p.calls.filter(c => c.op && c.op !== 'freeze').every(c => c.op === 'driveGet' || c.op === 'customSaveGet'), p.calls.map(c => c.op));
    ok('A4 the intents the cloud finished (linked; matched by the listing\'s answer) are removed from this browser; done, off, rehearsal and unfinished ones stay', JSON.stringify(intentsIn(store)) === JSON.stringify(['k3', 'k4', 'k5', 'k6', 'k7', 'k8']), intentsIn(store));
    const msg = p.toasts.map(t => t.t);
    const want = '1 design save started on this computer is in the repository; the cloud\'s repair finishes it within 10 minutes (order 4100000004). 2 design saves started on this computer did not reach the repository: open the design window of orders 4100000003, 4100000008 to save them again';
    ok('A5 the really unfinished ones are said once, in words, naming the orders (3 unfinished, never "2 ... are not finished here any more")', p.toasts.length === 1 && msg[0] === want, msg);
    ok('A6 ... red only because a person must act (two saves never reached the repository and nobody on this page resumes them)', p.toasts.length === 1 && p.toasts[0].tone === 'bad', p.toasts);
    ok('A7 the old words are gone', !msg.some(t => /not finished here any more/.test(t))); }

  /* ── B ── */
  { const p = page({ store }); await p.load();
    ok('B1 a second reload, the same intents: nothing is said again', p.toasts.length === 0, p.toasts);
    ok('B2 ... and nothing more is removed', JSON.stringify(intentsIn(store)) === JSON.stringify(['k3', 'k4', 'k5', 'k6', 'k7', 'k8']), intentsIn(store)); }
  { const p = page({ store, keepName: 'sheet' }); await p.load();
    const mem = JSON.parse(store.get(SAID) || '{}');
    ok('B3 this page\'s keeper now resumes them (item 14, CustomSheet.keepResumes): the viewer says nothing (the keeper says "Finishing 3 design saves" itself), its note follows the change', p.toasts.length === 0 && mem.k3 === 'live|0|1' && mem.k4 === 'live|1|1' && mem.k8 === 'live|0|1', { toasts: p.toasts, mem }); }
  { const p = page({ store, keepName: 'sheet' }); await p.load();
    ok('B4 ... and the reload after it says nothing', p.toasts.length === 0, p.toasts); }
  { const all = JSON.parse(store.get(KEY)); all.intents.k9 = rec('k9', { rid: '4100000009', listingId: '1700000009', state: 'wanted', at: T }); store.set(KEY, JSON.stringify(all));
    const p = page({ store, keepName: 'sheet' }); await p.load();
    ok('B5 a new save begun here while the keeper resumes: still the keeper\'s to say (nothing from the viewer), noted', p.toasts.length === 0 && JSON.parse(store.get(SAID)).k9 === 'live|0|1', p.toasts); }
  { const all = JSON.parse(store.get(KEY)); all.intents.k9 = Object.assign(all.intents.k9, { state: 'attention', why: 'Etsy refused the SKU' }); store.set(KEY, JSON.stringify(all));
    const p = page({ store, keepName: 'sheet' }); await p.load();
    ok('B6 one that now needs a person is said, even while the keeper resumes the rest (red: a person must act)', p.toasts.length === 1 && p.toasts[0].t === '1 design save started on this computer needs a person: open the design window of order 4100000009' && p.toasts[0].tone === 'bad', p.toasts);
    const q = page({ store, keepName: 'sheet' }); await q.load();
    ok('B7 ... once: the next reload says nothing', q.toasts.length === 0, q.toasts); }

  /* ── C ── */
  { const s2 = new Map([[KEY, seedIntents()]]); const p = page({ store: s2, keepName: 'lib' }); await p.load();
    ok('C1 the keeper library\'s flag (CharmNestKeep.resumesOnViewer) is read too: the finished ones are removed, nothing said by the viewer', p.toasts.length === 0 && JSON.stringify(intentsIn(s2)) === JSON.stringify(['k3', 'k4', 'k5', 'k6', 'k7', 'k8']), { toasts: p.toasts, left: intentsIn(s2) });
    ok('C2 CNViewer.keepResumes() answers as the keeper says', p.w.CNViewer.keepResumes() === true && page({ store: new Map() }).w.CNViewer.keepResumes() === false); }
  { const s3 = new Map([[KEY, seedIntents()]]); const p = page({ store: s3, down: true, keepName: 'lib' }); await p.load();
    ok('C3 the cloud does not answer, the keeper resumes: nothing is removed (never guessed finished), the viewer says nothing (the keeper runs all five and says so)', JSON.stringify(intentsIn(s3)) === JSON.stringify(['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8']) && p.toasts.length === 0 && Object.keys(JSON.parse(s3.get(SAID))).sort().join() === 'k1,k2,k3,k4,k8', { left: intentsIn(s3), toasts: p.toasts }); }
  { const s3b = new Map([[KEY, seedIntents()]]); const p = page({ store: s3b, down: true }); await p.load();
    ok('C3b the cloud does not answer, no keeper resumes: all five open ones are kept, counted and said', JSON.stringify(intentsIn(s3b)) === JSON.stringify(['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8']) && p.toasts.length === 1 && /^2 design saves .* in the repository.*\. 3 design saves .* did not reach the repository/.test(p.toasts[0].t), p.toasts); }
  { const all = { v: 1, intents: { a: rec('a', { rid: '4100000001', listingId: '1700000001' }), b: rec('b', { rid: '4100000002', listingId: '1700000002', repo: true, sku: 'CU-AB12', state: 'running' }), c: rec('c', { rid: '4100000005', state: 'done' }) } };
    const s4 = new Map([[KEY, JSON.stringify(all)], [SAID, JSON.stringify({ a: 'live|0|0' })]]); const p = page({ store: s4 }); await p.load();
    ok('C4 everything the cloud finished: nothing is said, the finished ones are removed, the memory of what was said is cleared', p.toasts.length === 0 && JSON.stringify(intentsIn(s4)) === '["c"]' && !s4.has(SAID), { toasts: p.toasts, left: intentsIn(s4), said: s4.get(SAID) }); }

  /* ── D ── */
  { const s5 = new Map([[KEY, seedIntents()]]); const before = s5.get(KEY); const p = page({ store: s5, mode: 'off' });
    const on = await p.load();
    ok('D1 mode off: not a viewer, nothing said, no customSaveGet, the browser\'s intents untouched (the page\'s own keeper runs them as before)', on === false && p.toasts.length === 0 && !p.calls.some(c => c.op === 'customSaveGet' || c.op === 'freeze') && s5.get(KEY) === before && !s5.has(SAID), { toasts: p.toasts, calls: p.calls });
    const r = await p.w.CNViewer.keepSaves();
    ok('D2 ... and the check itself does nothing in mode off', r.said === false && s5.get(KEY) === before); }
  { const s6 = new Map([[KEY, JSON.stringify({ v: 1, intents: {} })]]); const p = page({ store: s6 }); await p.load();
    ok('D3 mode on, the record holds no intent: nothing is said (the old code counted its two fields and said "2 design save(s)")', p.toasts.length === 0, p.toasts); }
  { const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
    ok('D4 charm-nest-1.html loads the changed viewer under a new token (-ds1)', /charm-nest-viewer\.js\?v=1120-v14-on1[-\w]*-ds1[-\w]*"/.test(html)); }

  console.log(failed ? `\n${failed} FAILED` : '\nall item 15 checks passed');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL ' + ((e && e.stack) || e)); process.exit(1); });
