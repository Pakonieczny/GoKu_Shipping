/* SHEETLIVE (Paul, 27 Nov 2026: "I've been having too many problems with the sheets and the library showing wrong old information, not updating, not being able to click the manual button";
   "nothing can be on any computer. Everything must be grabbed from the cloud"). Node only, offline: node tests/charm-nest/sheet-live.cjs
   The page's ONE in-memory copy of every sheet (charm-nest-sheet-live.js), loaded as it is, with the real store's NEWERWINS rule (charm-nest-store.js newerOf), fake timers and a fake cloud.
     A  one object per sheet; NEWERWINS in any order; the whole record supersedes the row of its version (its readiness taken in); a newer row makes a window's whole record read again
     B  watch / unwatch by reference; only watched sheets are read; a name reads once; a burst is one read; a name during a read is exactly one more read; nothing dropped while busy
     C  subscribers told; the person's own write shows at once and an older answer never takes it back; no timer of its own (no polling), retries bounded
     D  names of a watched sheet's set and orders; '*' / reset / verify read every watched sheet in ONE call by revision; a sheet gone
     E  the page's adapter: the real changes feed (charm-nest-sync-worker.js) names a sheet, SHEETLIVE reads it; the store's records reach it; the spinner reads it (charm-nest-1.html SHEETBUSY) */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..'), read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 600))); if (cond) passed++; else failed++; };
const SLmod = require(path.join(ROOT, 'charm-nest-sheet-live.js'));
const Store = require(path.join(ROOT, 'charm-nest-store.js'));
const newerOf = Store.newerOf;
const between = (src, from, to) => { const a = src.indexOf(from); if (a < 0) return null; const b = src.indexOf(to, a + from.length); return b < 0 ? null : src.slice(a, b + to.length); };

/* fake timers and a fake cloud */
function clock() {
  let t = 1764250000000, seq = 0; const q = new Map();
  const set = (f, ms) => { const id = ++seq; q.set(id, { at: t + Math.max(0, +ms || 0), f }); return id; };
  const clear = id => { q.delete(id); };
  const settle = () => new Promise(r => setImmediate(r));   // (every promise waiting on the fake cloud has gone on before a timer is looked for)
  const run = async ms => { const end = t + ms; await settle(); for (;;) { let next = null; for (const [id, x] of q) if (x.at <= end && (!next || x.at < next[1].at)) next = [id, x]; if (!next) break; q.delete(next[0]); t = next[1].at; next[1].f(); await settle(); } t = end; await settle(); };
  return { now: () => t, set, clear, run, pending: () => q.size };
}
function cloud(docs) {
  const calls = []; const gate = { hold: null };
  const api = async (fn, body) => {
    calls.push(JSON.parse(JSON.stringify(body)));
    if (gate.hold) await gate.hold;
    if (gate.fail) throw new Error('offline');
    if (body.op === 'getSheet') { const d = docs.get(body.id); if (!d) return { sheet: null }; if (body.ifRev && body.ifRev === d.rev) return { unchanged: true, rev: d.rev }; return Object.assign({ sheet: JSON.parse(JSON.stringify(d.rec)), rev: d.rev }, body.pieces ? { pools: { [d.rec.id + '_p']: { size: 1 } } } : {}); }
    if (body.op === 'getMany') { const recs = {}, gone = []; for (const k of body.keys) { const d = docs.get(k.slice(2)); if (!d) { gone.push(k); continue; } if (body.ifRevs[k] === d.rev) continue; recs[k] = { rev: d.rev, rec: JSON.parse(JSON.stringify(d.rec)) }; } return { recs, gone }; }
    return { error: 'unknown op' };
  };
  return { api, calls, gate };
}
const whole = (id, n, more) => Object.assign({ id, setId: 'T1', orders: ['4200000001'], updatedAt: 1764250000000 + n * 1000, layoutRev: n, contentRev: n, charms: [{ id: 'c' + n }], placements: [{ id: 'c' + n, cxPt: n, cyPt: n }] }, more || {});
const row = (id, n, more) => Object.assign({ id, setId: 'T1', orders: ['4200000001'], updatedAt: 1764250000000 + n * 1000, layoutRev: n, contentRev: n, engraving: {} }, more || {});
const make = (docs, o) => { const T = clock(), C = cloud(docs || new Map()); const L = SLmod.create(Object.assign({ api: C.api, now: T.now, setTimeout: T.set, clearTimeout: T.clear, newerOf }, o || {})); return { T, C, L }; };

(async () => {
  /* ── A · one record per sheet, newer always wins ── */
  {
    const { L } = make();
    const r1 = row('A', 1), got = L.put('A', r1, { rev: '100.1' });
    ok('A1 the first record of a sheet is held as given, and every caller gets that very object', got === r1 && L.get('A') === r1 && L.get('A') === L.get('A'));
    const f1 = whole('A', 1); const g2 = L.put('A', f1, { rev: '100.1' });
    ok('A2 the whole record of the same version supersedes the row: one record, now whole', g2 === f1 && L.full('A') === f1 && L.state().held === 1);
    const r1b = row('A', 1, { engraving: { 'x_1': { ok: true } } }), g3 = L.put('A', r1b, { rev: '100.1' });
    ok('A3 a row of the same version brings its readiness into the whole record (a new object), its charms kept', g3 !== f1 && Array.isArray(g3.charms) && g3.engraving.x_1.ok === true && L.full('A') === g3);
    const g4 = L.put('A', row('A', 1, { engraving: { 'x_1': { ok: true } } }), { rev: '100.1' });
    ok('A4 the same row again changes nothing (the same object stays)', g4 === g3);
    // any order of three versions: the newest stands
    const versions = [[whole('B', 1), '101.1'], [whole('B', 2), '102.1'], [whole('B', 3), '103.1']];
    const perms = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]; let all = true;
    for (const p of perms) { const { L: L2 } = make(); for (const i of p) L2.put('B', versions[i][0], { rev: versions[i][1] }); if (L2.get('B') !== versions[2][0] || L2.rev('B') !== '103.1') all = false; }
    ok('A5 NEWERWINS: three answers landing in every order, the newest is the one held (by the cloud\'s revision)', all);
    let byCounters = true;
    for (const p of perms) { const { L: L2 } = make(); for (const i of p) L2.put('C', versions[i][0], {}); if (L2.get('C') !== versions[2][0]) byCounters = false; }
    ok('A6 NEWERWINS with no revision said: the record\'s own counters (updatedAt, layoutRev, contentRev) decide', byCounters);
    const { L: L3 } = make(); const held = L3.put('D', whole('D', 5), { rev: '105.1' }), back = L3.put('D', row('D', 4), { rev: '104.1' });
    ok('A7 an older answer returns the newer record held, which every view keeps drawing', back === held && L3.get('D') === held && L3.state().older === 1);
    const rows = L3.putRows([row('D', 4), row('E', 1)], { 's:D': '104.1', E: '106.1' });
    ok('A8 a list\'s rows put in: each answered with the record held (the newer whole one for D), in order', rows[0] === held && rows[1].id === 'E' && L3.rev('E') === '106.1');
    // a newer row overtakes the whole record a window needs: the whole one is read
    const docs = new Map([['F', { rev: '201.1', rec: whole('F', 2) }]]), m = make(docs);
    m.L.put('F', whole('F', 1), { rev: '200.1' }); m.L.watch('F', 'sheetwin', { full: true });
    m.L.put('F', row('F', 2), { rev: '201.1' }); await m.T.run(100);
    ok('A9 a newer list row overtakes a window\'s whole record: it is held at once, and the whole record is read (in full, not by the row\'s revision)', m.C.calls.length === 1 && m.C.calls[0].op === 'getSheet' && !m.C.calls[0].ifRev && m.L.full('F') && m.L.full('F').layoutRev === 2, m.C.calls);
  }

  /* ── B · what is read, and how often ── */
  {
    const docs = new Map([['A', { rev: '300.1', rec: whole('A', 1) }], ['B', { rev: '301.1', rec: whole('B', 1) }]]), { T, C, L } = make(docs);
    L.put('A', whole('A', 1), { rev: '300.1' }); L.put('B', whole('B', 1), { rev: '301.1' });
    L.names(['s:A', 's:B'], 'keys'); await T.run(200);
    ok('B1 nothing watched: a name reads nothing', C.calls.length === 0);
    const offA = L.watch('A', 'sheetwin', { full: true }), offA2 = L.watch('A', 'orderwin', { full: true });
    L.names(['s:A', 's:B'], 'keys'); await T.run(200);
    ok('B2 only the watched sheet is read, by its revision (one field when nothing moved)', C.calls.length === 1 && C.calls[0].op === 'getSheet' && C.calls[0].id === 'A' && C.calls[0].ifRev === '300.1', C.calls);
    offA(); L.names(['s:A'], 'keys'); await T.run(200);
    ok('B3 one of two views lets go: the other still watches it', C.calls.length === 2);
    offA(); L.names(['s:A'], 'keys'); await T.run(200);
    ok('B4 the same unwatch twice counts once', C.calls.length === 3);
    offA2(); L.names(['s:A'], 'keys'); await T.run(200);
    ok('B5 the last view lets go: nothing more is read for it', C.calls.length === 3 && L.state().watched.length === 0);
    // a burst coalesces; a name during a read reads exactly once more
    L.watch('A', 'sheetwin', { full: true }); C.calls.length = 0;
    for (let i = 0; i < 5; i++) L.names(['s:A'], 'keys');
    await T.run(200);
    ok('B6 a burst of five names of one sheet is ONE read', C.calls.length === 1, C.calls.length);
    let release; C.gate.hold = new Promise(r => { release = r; }); C.calls.length = 0;
    L.names(['s:A'], 'keys'); await T.run(100);
    ok('B7 a name: one read on its way', C.calls.length === 1 && L.state().inflight.includes('A'));
    docs.set('A', { rev: '302.1', rec: whole('A', 2) });   // (the cloud changes while the read is out)
    L.names(['s:A'], 'keys'); L.names(['s:A'], 'keys'); L.names(['s:A'], 'keys'); await T.run(100);
    ok('B8 three names while it is out: no second read yet', C.calls.length === 1);
    C.gate.hold = null; release(); await T.run(200);
    ok('B9 once it is back: exactly ONE more read, and it brings the change made meanwhile (nothing dropped)', C.calls.length === 2 && L.get('A').layoutRev === 2 && L.rev('A') === '302.1', C.calls.map(c => c.ifRev));
    await T.run(60000);
    ok('B10 and nothing after it', C.calls.length === 2);
  }

  /* ── C · views told; the person's own write; no timer of its own ── */
  {
    const docs = new Map([['A', { rev: '400.1', rec: whole('A', 1) }]]), { T, C, L } = make(docs);
    const seen = [], all = [];
    const offS = L.subscribe('A', ev => seen.push(ev)), offAll = L.subscribe('*', ev => all.push(ev.id + ':' + ev.why));
    const f1 = whole('A', 1); L.put('A', f1, { rev: '400.1' }); L.put('A', f1, { rev: '400.1' }); L.put('A', whole('A', 0), { rev: '399.1' });
    ok('C1 a view is told each change of the record held (once), never for the same object or an older answer', seen.length === 1 && seen[0].rec === f1 && seen[0].full === true && all.length === 1);
    const mine = L.mine('A', { orders: ['4200000001', '4200000002'] }, { rev: '401.5' });
    ok('C2 a person\'s own write shows at once: a new object, the fields saved, told to the views', mine !== f1 && L.get('A') === mine && mine.orders.length === 2 && seen[1].why === 'mine' && f1.orders.length === 1);
    L.put('A', whole('A', 1), { rev: '400.1' });
    ok('C3 an answer read before the write (its revision earlier) never takes it back', L.get('A') === mine);
    L.put('A', whole('A', 1, { orders: ['4200000001'] }), {});
    ok('C4 nor an answer with no revision that is not newer by its counters', L.get('A') === mine);
    const cloudRec = whole('A', 2, { orders: ['4200000001', '4200000002'] }); L.put('A', cloudRec, { rev: '401.5' });
    ok('C5 the cloud\'s own record at the write\'s revision replaces it: from then on it is the cloud\'s', L.get('A') === cloudRec && L.rev('A') === '401.5');
    const { L: L2 } = make(); L2.put('Z', whole('Z', 9), { rev: '900.1' }); const z = L2.get('Z'); L2.mine('Z', { orders: [] }, { rev: '800.1' });
    ok('C6 a write whose answer is older than the record held changes nothing (the record held already has it, or a later change)', L2.get('Z') === z);
    const n0 = seen.length, a0 = all.length; offS(); offAll(); L.put('A', whole('A', 3), { rev: '402.1' });
    ok('C7 a view that let go is told nothing more', seen.length === n0 && all.length === a0 && n0 === 3);
    // no timers: an hour with sheets watched, nothing named, reads nothing and leaves no timer
    C.calls.length = 0; L.watch('A', 'sheetwin', { full: true }); L.watch('B', 'nest', {});
    await T.run(3600000);
    ok('C8 NO POLLING: an hour of watched sheets with nothing named reads nothing, and no timer is left', C.calls.length === 0 && T.pending() === 0);
    // retries bounded
    C.gate.fail = true; L.names(['s:A'], 'keys'); await T.run(3600000);
    ok('C9 a read that fails is tried again at most three times (2 s, 4 s, 8 s), then left to the next name', C.calls.length === 4 && T.pending() === 0, C.calls.length);
    C.gate.fail = false; L.names(['s:A'], 'keys'); await T.run(1000);
    ok('C10 the next name reads it again', C.calls.length === 5);
    const src = read('charm-nest-sheet-live.js');
    ok('C11 the file keeps no timer of its own: no setInterval, its only setTimeouts are the burst\'s and the bounded retry', !/setInterval\s*\(/.test(src) && (src.match(/C\.setTimeout\(/g) || []).length === 2);
    ok('C12 nothing is kept on the computer: no localStorage, sessionStorage, indexedDB or caches in it', !/localStorage|sessionStorage|indexedDB|caches\./.test(src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '')));
  }

  /* ── D · names of a sheet's set and orders; every watched sheet at once; gone ── */
  {
    const docs = new Map([['A', { rev: '500.1', rec: whole('A', 1, { setId: 'T1', orders: ['4200000001'] }) }], ['B', { rev: '501.1', rec: whole('B', 1, { setId: 'T2', orders: ['4200000009'] }) }], ['C', { rev: '502.1', rec: whole('C', 1, { setId: 'T2', orders: [] }) }]]);
    const { T, C, L } = make(docs);
    for (const id of ['A', 'B', 'C']) L.put(id, docs.get(id).rec, { rev: docs.get(id).rev });
    L.watch('A', 'sheetwin', { full: true, orders: true }); L.watch('B', 'nest', {}); L.watch('C', 'nest', {});
    const told = []; L.subscribe('*', ev => told.push(ev.id + ':' + ev.why));
    L.names(['t:T2'], 'keys'); await T.run(200);
    ok('D1 a set named: its watched sheets are read (one call by revision for both)', C.calls.length === 1 && C.calls[0].op === 'getMany' && C.calls[0].keys.sort().join() === 's:B,s:C' && C.calls[0].ifRevs['s:B'] === '501.1', C.calls);
    C.calls.length = 0; L.names(['o:4200000001'], 'keys'); await T.run(200);
    ok('D2 an order of a sheet a view watches with its orders (the sheet window): that sheet is read, and the view is told the order moved', C.calls.length === 1 && C.calls[0].id === 'A' && told.includes('A:order'));
    C.calls.length = 0; L.names(['p:4200000009', 'x:4200000009'], 'keys'); await T.run(200);
    ok('D3 an order of a sheet watched without its orders (a Nest card): nothing read for it', C.calls.length === 0);
    ok('D4 wants(): the feed hands it only what concerns a watched sheet', L.wants('s:A') && L.wants('t:T2') && L.wants('o:4200000001') && !L.wants('o:4200000009') && !L.wants('s:Q') && !L.wants('t:*') && L.wants('*') && L.wants('s:*'));
    C.calls.length = 0; L.names([], 'verify'); await T.run(200);
    ok('D5 a verify (or a reset, or \'*\'): every watched sheet in ONE call, by revision', C.calls.length === 1 && C.calls[0].op === 'getMany' && C.calls[0].keys.length === 3);
    docs.delete('B'); C.calls.length = 0; L.names(['s:B'], 'keys'); await T.run(200);
    ok('D6 a sheet the cloud no longer has: let go, and its views told (rec null)', !L.has('B') && told.includes('B:gone'));
    const n = L.watchSet('cards', () => ['C']); C.calls.length = 0; L.names(['s:C'], 'keys'); await T.run(200);
    ok('D7 a list of cards watched as one (watchSet): a card named is read', C.calls.length === 1 && C.calls[0].id === 'C'); n();
    const { T: T2, C: C2, L: L2 } = make(new Map([['P', { rev: '600.1', rec: whole('P', 1) }]]));
    const r = await (async () => { const p = L2.read('P', { pieces: true }); await T2.run(10); return p; })();
    ok('D8 read(id, { pieces }) reads now, whole, with the pieces\' rows, kept beside the record', !!r && r.id === 'P' && C2.calls[0].pieces === true && !!L2.pools('P'));
  }

  /* ── E · the page's adapter: the real feed, the store, the spinner ── */
  {
    const T = clock(); const docs = new Map([['A', { rev: '700.1', rec: whole('A', 1) }]]), C = cloud(docs);
    let feedN = 0; const answers = [];
    const api = async (fn, body, opts) => { if (body.op === 'getChanges') { feedN++; const a = answers.shift() || { n: '1.0.0.0', keys: [] }; return a; } return C.api(fn, body, opts); };
    class BC { constructor() { this.onmessage = null; } postMessage() {} close() {} }
    const doc = { visibilityState: 'visible', addEventListener() {}, hidden: false };
    const win = { document: doc, BroadcastChannel: BC, navigator: { onLine: true }, addEventListener() {}, dispatchEvent() {}, CustomEvent: class {}, console, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, Promise, JSON, Math, Date, Map, Set, Object, Array, String, Number, RegExp, Error, Symbol };
    win.self = win; win.window = win;
    const ctx = vm.createContext(win);
    for (const f of ['charm-nest-store.js', 'charm-nest-sync-worker.js', 'charm-nest-sheet-live.js']) vm.runInContext(read(f), ctx, { filename: f });
    const SL = ctx.CNSheetLive, St = ctx.CharmNestStore, Y = ctx.CNSync;
    ok('E1 the page\'s SHEETLIVE loads after the store and the feed, and follows the feed for sheets, sets and an order\'s pieces, cancel and timeline', !!SL && typeof SL.get === 'function' && Y.state().subs === 1);
    SL.config({ api, now: T.now, setTimeout: T.set, clearTimeout: T.clear });
    await St.init({ world: 'production' });
    St.put('sheetFull', 'A', whole('A', 1), '700.1');
    ok('E2 a record the store keeps (a window\'s read, the feed\'s getMany) reaches the one copy, with its revision', !!SL.full('A') && SL.rev('A') === '700.1');
    SL.watch('A', 'sheetwin', { full: true });
    Y.start({ world: 'production', api });
    await new Promise(r => setTimeout(r, 30));
    docs.set('A', { rev: '701.1', rec: whole('A', 2) });
    answers.push({ n: '2.0.0.0', keys: ['s:A', 's:Q'] }); Y.push.nudge() || Y.poke();
    await new Promise(r => setTimeout(r, 900)); await T.run(200); await new Promise(r => setTimeout(r, 10));
    ok('E3 the feed names the watched sheet (and one not watched): SHEETLIVE reads it by revision, and holds the cloud\'s new record', C.calls.length === 1 && C.calls[0].ifRev === '700.1' && SL.get('A').layoutRev === 2 && SL.rev('A') === '701.1', { calls: C.calls, feedN });
    answers.push({ n: '3.0.0.0', keys: ['*'] }); Y.push.nudge() || Y.poke();
    await new Promise(r => setTimeout(r, 900)); await T.run(200); await new Promise(r => setTimeout(r, 10));
    ok('E4 a write that named nothing (\'*\'): every watched sheet is looked at by its revision', C.calls.length === 2 && C.calls[1].ifRev === '701.1', C.calls);
    // the spinner reads the one record
    const html = read('charm-nest-1.html'), sb = between(html, '(function sheetBusy() {', 'window.CNSheetBusy = { see, of, why: busyWhy, decorate, soon };\n})();');
    const d2 = { body: { children: [] }, head: { children: [], appendChild() {} }, createElement: () => ({}), getElementById: () => null, addEventListener() {}, querySelectorAll: () => [], querySelector: () => null };
    const c2 = vm.createContext({ document: d2, MutationObserver: class { observe() {} }, console, setTimeout: () => 0, clearTimeout() {}, Date, JSON, Map, Set, String, Number, Object, Array });
    c2.window = c2; const L4 = SLmod.create({ newerOf }); c2.CNSheetLive = L4; c2.CharmNestStore = { newerOf };
    vm.runInContext(sb, c2);
    const now = Date.now(); L4.put('S', { id: 'S', nestJob: { status: 'running', at: now }, updatedAt: now }, { rev: '800.2' });
    c2.CNSheetBusy.see({ id: 'S', nestJob: { status: 'done', at: now - 5 }, updatedAt: now - 10 }, 'S');
    ok('E5 SHEETBUSY: an older record seen by a view never brings the spinner back: it answers from the newest of the one copy\'s record and what it saw', c2.CNSheetBusy.of('S') === 'nesting' && L4.get('S').nestJob.status === 'running');
    c2.CNSheetBusy.see({ id: 'S', nestJob: { status: 'done', at: now + 5 }, updatedAt: now + 10 }, 'S');
    ok('E6 the finished record seen by any view: every view\'s spinner goes (the spinner only reads the one copy: a record it sees with no revision never takes the copy\'s revision away)', c2.CNSheetBusy.of('S') === '' && L4.get('S').nestJob.status === 'running' && L4.rev('S') === '800.2');
    L4.put('S', { id: 'S', nestJob: { status: 'running', at: now + 20 }, updatedAt: now + 30 }, { rev: '801.1' });
    ok('E6b the one copy reads a newer one (a nest started again): the spinner says so again', c2.CNSheetBusy.of('S') === 'nesting');
    ok('E7 busy(id) is the spinner\'s own rule on the one record', SLmod.create({ busyWhy: r => (r && r.x ? 'nesting' : '') }).busy('none') === '');
    const page = html;
    ok('E8 the page loads it after the store, the feed and the push, before every view that uses it; the build publishes it',
      (at => at('sheet-live') > at('push') && at('push') > at('sync-worker') && at('sync-worker') > at('store') && at('sheet-live') < at('cloud-sheets') && at('sheet-live') < at('bridge') && at('sheet-live') < at('sheetwin') && at('sheet-live') < page.indexOf('(function sheetBusy() {'))(f => page.indexOf('<script src="charm-nest-' + f + '.js'))
      && /"charm-nest-sheet-live\.js"/.test(read('scripts/build-public.cjs')));
  }


  /* ── R · the review's cases (27 Nov 2026) ── */
  {
    const docs = new Map([['G', { rev: '1600.1', rec: whole('G', 3) }]]), { T, C, L } = make(docs), told = [];
    L.put('G', docs.get('G').rec, { rev: '1600.1' }); L.watch('G', 'sheetwin', { full: true }); L.subscribe('G', ev => told.push(ev.why));
    docs.delete('G'); L.names(['s:G'], 'keys'); await T.run(200);
    const gone1 = told.filter(w => w === 'gone').length;
    const back = L.put('G', Object.assign({}, whole('G', 3), { archived: true }), { rev: '', from: 'store' });
    L.gone('G'); L.gone('G');
    ok('R1 a sheet gone is told ONCE: its archived copy put back by another view (the store\'s mark) is not taken, so no view reads it again and again', gone1 === 1 && back === null && !L.has('G') && told.filter(w => w === 'gone').length === 1, told);
    const restored = whole('G', 4); L.put('G', restored, { rev: '1601.1', from: 'list' });
    ok('R2 ... a NEWER record of it (put back in the cloud: restoreSheet) is taken again', L.get('G') === restored);
    const { L: L2 } = make(); L2.put('M', whole('M', 2), {}); const m = L2.mine('M', { orders: ['x'] }, {});
    L2.put('M', whole('M', 2), { rev: '1700.1' });
    ok('R3 mine() with no revision known: an answer read before the write (the same counters) is refused ...', L2.get('M') === m);
    const after = whole('M', 3); L2.put('M', after, { rev: '1700.5' });
    ok('R4 ... and the cloud\'s newer record replaces it (never stuck on the person\'s own copy)', L2.get('M') === after);
  }

  /* ── F · stage 2, the Library (charm-nest-bridge.js LaserReview, the lists, LIVEPV) ── */
  const SW = { on: { library: true, sheetwin: true, orderwin: true, nest: true }, off: { library: false, sheetwin: false, orderwin: false, nest: false } };
  {
    const bsrc = read('charm-nest-bridge.js'), html = read('charm-nest-1.html');
    const rec = between(bsrc, '  const olderRec=(a,b)=>', "  const recOf=id=>records.get(String(id || '')) || null;");
    const app = (() => { const a = bsrc.indexOf('  function applyStatus(response,ids,always){'), b = bsrc.indexOf('\n  const arm=ms=>', a); return a > 0 && b > a ? bsrc.slice(a, b) : ''; })();
    const pr = (() => { const a = bsrc.indexOf('  const ROW_FIELDS=['), b = bsrc.indexOf('\n  }\n', bsrc.indexOf('  function patchRow(rec){')); return a > 0 && b > a ? bsrc.slice(a, b + 4) : ''; })();
    const mk = sw => {
      const L = SLmod.create({ newerOf, switches: () => sw }), St = { newerOf, put() {}, rev: () => '' };
      const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Error });
      ctx.window = ctx; ctx.CNSheetLive = L; ctx.CharmNestStore = St; ctx.records = new Map(); ctx.sets = new Map(); ctx.live = { sigs: new Map(), shape: new Map() };
      ctx.pics = []; ctx.keep = () => {}; ctx.picNow = (s, id) => ctx.pics.push(id); ctx.wantReload = () => {}; ctx.document = { querySelectorAll: () => [] };
      ctx.S = { library: { rows: [] } }; ctx.Gate = { projectLibraryRecords: rows => rows };
      vm.runInContext(rec + '\n' + app + '\n' + pr + '\nthis.LR = { record, applyStatus, patchRow, slLib };', ctx);
      return { L, ctx, LR: ctx.LR };
    };
    {
      const { L, ctx, LR } = mk(SW.on);
      const newer = whole('S1', 5), older = row('S1', 4);
      L.put('S1', newer, { rev: '905.1', from: 'sheetwin' });   // (the sheet window read the whole record)
      ctx.records.set('S1', row('S1', 3));
      const got = LR.record(older, '904.1');
      ok('F1 Library on: an older read hands back the newer record the one copy holds, and the Library draws that one (its record, its picture)', got === newer && ctx.records.get('S1') === newer && ctx.pics.includes('S1'));
      const d = LR.applyStatus({ sheets: [row('S1', 4)], revs: { 's:S1': '904.1' } }, ['S1'], false);
      ok('F2 Library on: a live answer older than the one copy\'s record changes nothing (the newer stays on the card)', ctx.records.get('S1') === newer && d === false);
      const r6 = row('S1', 6), d2 = LR.applyStatus({ sheets: [r6], revs: { 's:S1': '906.1' } }, ['S1'], false);
      ok('F3 Library on: a newer live answer is the one copy\'s record now, and the card draws it', d2 === true && L.get('S1') === r6 && ctx.records.get('S1') === r6 && L.rev('S1') === '906.1');
      const listRow = row('S1', 2); ctx.S.library.rows = [listRow]; LR.patchRow(r6);
      ok('F4 Library on: the list\'s row takes the one copy\'s newest row fields as a NEW row (never changed in place: neither the old row nor the one copy\'s record)', ctx.S.library.rows[0] !== r6 && ctx.S.library.rows[0] !== listRow && ctx.S.library.rows[0].updatedAt === r6.updatedAt && listRow.layoutRev === 2 && r6.layoutRev === 6);
      LR.patchRow({ id: 'S1', archived: true });
      ok('F5 a sheet gone: its row says archived, the one copy\'s record untouched', ctx.S.library.rows[0].archived === true && !r6.archived);
      const wholeA = whole('SA', 9, { outputs: { preview: { url: 'https://x/pa.png' }, ai: { url: 'https://x/a.ai' } }, backPool: [{ poolId: 'p' }] }); L.put('SA', wholeA, { rev: '909.1' });
      const listed = L.listRows([Object.assign(row('SA', 8), { outputs: { ai: 'https://x/a8.ai' }, backs: [], preview: 'https://x/p8.png' })], { SA: '908.1' })[0];
      ok('F5b a list drawn while the one copy holds a newer WHOLE record keeps its rows list-shaped (outputs, backs as a list row has them), with the newer row fields and picture', listed !== wholeA && listed.layoutRev === 9 && listed.outputs.ai === 'https://x/a8.ai' && Array.isArray(listed.backs) && listed.preview === 'https://x/pa.png' && !('charms' in listed));
    }
    {
      const { L, ctx, LR } = mk(SW.off);
      L.put('S2', whole('S2', 5), { rev: '905.1' }); ctx.records.set('S2', row('S2', 3));
      const r4 = row('S2', 4), got = LR.record(r4, '904.1');
      ok('F6 Library off: as before: the Library\'s own record decides, the one copy is not asked', got === r4 && ctx.records.get('S2') === r4 && !LR.slLib());
    }
    ok('F7 the lists\' rows go into the one copy and are drawn from it (loadLibrary, the Sets view, the held list), the switch read at use',
      /rows = slRows\(rows \|\| r\.sheets \|\| \[\], r && r\.revs\);/.test(html) && /const held = slRows\(h\.rows\);/.test(html) && /L\.on\("library"\) && Array\.isArray\(list\) \? L\.listRows\(list, revs\) : list/.test(html)
      && /const records = slRows\(got \? got\.sheets/.test(bsrc) && /const rows = slRows\(h\.sheets\);/.test(bsrc));
    ok('F8 LIVEPV compares a page\'s own drawing with the one copy\'s record first; the cards\' pictures stay under their versioned link (picUrl)',
      /slr = SL && typeof SL\.on === "function" && SL\.on\("library"\) \? SL\.get\(id\) : null;/.test(html) && /const all = \[slr, LR/.test(html) && /window\.picUrl\(\{preview:url,previewAt:at\}\)/.test(bsrc));
    ok('F9 a newer record another view read (the sheet window, the order window, a Nest card, a person\'s own write) is drawn on its Library card at once, with no read of its own',
      /slOff=window\.CNSheetLive\.subscribe\('\*',ev=>/.test(bsrc) && /ev\.from==='library' \|\| ev\.from==='list'/.test(bsrc) && /records\.set\(ev\.id,ev\.rec\);picNow\(ev\.rec,ev\.id\)/.test(bsrc) && /recOf:id=>\(slLib\(\) && slLib\(\)\.get\(String\(id \|\| ''\)\)\) \|\| recOf\(id\)/.test(bsrc));
    ok('F10 the switches are one constant object in the page, every view on, read at use',
      /<script>window\.CN_SHEETLIVE=\{library:true,sheetwin:true,orderwin:true,nest:true\};<\/script>/.test(html) && html.indexOf('window.CN_SHEETLIVE=') < html.indexOf('<script src="charm-nest-sheet-live.js'));
  }

  /* ── G · stage 2, the sheet window (charm-nest-sheetwin.js) ── */
  {
    const src = read('charm-nest-sheetwin.js');
    const block = between(src, '  const backSig = r =>', '  setInterval(follow, 2000);');
    const T = clock(), docs = new Map([['S1', { rev: '1000.1', rec: whole('S1', 1) }]]), C = cloud(docs);
    let liveFeed = true;
    const L = SLmod.create({ api: C.api, now: T.now, setTimeout: T.set, clearTimeout: T.clear, newerOf, switches: () => SW.on, live: () => liveFeed });
    const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise, setTimeout: (f) => { f(); return 0; }, clearTimeout() {}, setInterval: () => 0 });
    ctx.window = ctx; ctx.CNSheetLive = L; ctx.CharmNestStore = { newerOf }; ctx.document = { hidden: false };
    const opened = [];
    const r1 = L.put('S1', whole('S1', 1), { rev: '1000.1' });
    ctx.W = { dlg: { open: true }, id: 'S1', token: 1, rec: r1, recBase: r1, going: new Set(), landing: [] };
    ctx.MC = { on: false, edits: new Map(), from: new Map() }; ctx.HOP = { pending: 0 };
    ctx.FOLLOW = { id: 'S1', revs: null, busy: false, fails: 0, at: Date.now(), moved: Date.now(), touch: Date.now() };
    ctx.followGap = () => (liveFeed ? 600000 : 0); ctx.tryDo = f => { try { return f ? f() : null; } catch (_) { return null; } };
    ctx.followHook = function () {}; ctx.mcChanged = (a, b) => !a || !b || a.cxPt !== b.cxPt; ctx.hopOverlay = r => r; ctx.head = () => {};
    ctx.api = async () => { throw new Error('the window asks the cloud nothing itself'); };
    ctx.open = async (id, o) => { opened.push([id, o && o.held]); ctx.W.recBase = L.full(id); ctx.W.rec = L.full(id); };
    vm.runInContext(block + '\nthis.SWF = { follow };', ctx);
    const F = ctx.SWF;
    const r2 = L.put('S1', whole('S1', 2), { rev: '1001.1', from: 'read' });
    await F.follow(true);
    ok('G1 sheet window on: a newer record in the one copy (read for any view) is drawn in place from it, with no read of the window\'s own', opened.length === 1 && opened[0][1] === true && ctx.W.rec === r2 && C.calls.length === 0, { opened, calls: C.calls.length });
    ctx.MC.on = true; ctx.MC.drag = { moved: true }; L.put('S1', whole('S1', 3), { rev: '1002.1', from: 'read' });
    await F.follow(true);
    ok('G2 mid Manual Control drag: nothing drawn under the hand, the change kept for later', opened.length === 1 && ctx.FOLLOW.owed === 'S1');
    ctx.MC.drag = null; ctx.MC.on = false; await F.follow();
    ok('G3 ... and drawn at the beat after the gesture ends', opened.length === 2 && ctx.W.rec.layoutRev === 3);
    const same = Object.assign({}, ctx.W.rec, { engraving: { x: 1 } }); L.put('S1', same, { rev: '1002.1', from: 'read' });
    await F.follow(true);
    ok('G4 the same layout, another mark (its readiness): the window takes the record with no redraw', opened.length === 2 && ctx.W.recBase === L.full('S1'));
    await F.follow(); await F.follow(true);
    ok('G5 the feed answering: the window\'s beat and a name ask the cloud nothing (the one copy reads by the feed)', C.calls.length === 0);
    liveFeed = false; ctx.FOLLOW.at = 0; await F.follow(); await T.run(200);
    ok('G6 the feed not answering: the window\'s own net asks the one copy to look, by revision (one read)', C.calls.length === 1 && C.calls[0].op === 'getSheet' && C.calls[0].ifRev === '1002.1', C.calls);
    const fg = src.slice(src.indexOf('const followGap = () => {'), src.indexOf('\n  };', src.indexOf('const followGap = () => {')) + 4);
    ok('G7 the window\'s beat slows to its ten-minute net while the one copy follows the feed (followGap)', /CNSheetLive\.on\("sheetwin"\) && CNSheetLive\.live\(\) && FOLLOW\.id === W\.id\) return 600000;/.test(fg) && /typeof window !== "undefined"/.test(fg));
    ok('G8 open() draws the one copy\'s whole record, watches the sheet (its set and orders too) while the window shows it, puts every answer through it; close lets go',
      /const SLw = slOn\("sheetwin"\); if \(SLw\) slWatch\(SLw, id\);/.test(src) && /W\.slOff = L\.watch\(id, "sheetwin", \{ full: true, orders: true \}\)/.test(src) && /SLw\.put\(id, r\.sheet, \{ rev: r\.rev \|\| "", full: true, from: "sheetwin", why: "read" \}\)/.test(src) && /slWatch\(null, null\); if \(W\.mcLive\) W\.mcLive\.clear\(\);/.test(src)
      && /if \(!SLw && r && r\.sheet && !r\.held && St\) \{ if \(typeof St\.older === "function" && St\.older\("sheetFull", id, r\.sheet, r\.rev \|\| ""\)/.test(src));
    ok('G9 a person\'s own Manual Control save shows at once in the one copy (mine), every view with it', /L\.mine\(id, \{ placements: next\.placements, charms: next\.charms, manualEdit: null, filesOwed: next\.filesOwed \}, \{ rev: ans\.rev \|\| "" \}\)/.test(src));
  }

  /* ── H · Manual Control in CLOUD MODE (GF_Nov.24.26_Set-2_Sheet-1, 27 Nov 2026) ── */
  {
    const src = read('charm-nest-sheetwin.js');
    const fnOf = (sig) => { const a = src.indexOf(sig); if (a < 0) return ''; const b = src.indexOf('\n  }\n', a); return src.slice(a, b + 4); };
    const code = ['  const slOn = v =>', '  const cloudPage = p =>'].map(k => { const a = src.indexOf(k); return src.slice(a, src.indexOf('\n', a) + 1); }).join('')
      + fnOf('  function mcRunSheet(id) {') + fnOf('  function mcHeld(id) {') + fnOf('  function mcStandalone() {')
      + (s0 => s0.slice(s0.indexOf('  const mcLive = () =>'), s0.indexOf('\n', s0.indexOf('  const mcLive = () =>')) + 1))(src);
    const mkMC = (sw, viewer, followed) => {
      const L = SLmod.create({ newerOf, switches: () => sw });
      const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math });
      ctx.window = ctx; ctx.CNSheetLive = L; ctx.CNViewer = { on: () => viewer };
      ctx.tryDo = f => { try { return f ? f() : null; } catch (_) { return null; } };
      ctx.MC_METALS = ['gold', 'silver', 'rose', 'gold10k', 'gold14k']; ctx.MC = { on: false, claiming: false, sh: null };
      ctx.STAND = { id: '', key: '', sh: null, ready: false, why: '', tries: 0, at: 0, tok: 0 }; ctx.mcStandPrep = () => {}; ctx.stockOf = () => null; ctx.recSources = () => [];
      ctx.layoutSig = r => JSON.stringify((r.placements || []).map(p => [p.id, p.cxPt, p.cyPt, p.angle]));
      ctx.CNManual = null;
      const charmOf = id => ({ id, bits: new Uint8Array(4), w: 2, h: 2 });
      const rec = { id: 'GF1', metal: 'gold', fileBase: 'GF_Nov.24.26_Set-2_Sheet-1', setId: 'T9', placements: [{ id: 'a', cxPt: 10, cyPt: 20, angle: 90 }, { id: 'b', cxPt: 40, cyPt: 25, angle: 0 }], charms: [{ id: 'a', poolId: '4200000001_1_1' }, { id: 'b', poolId: '4200000002_1_1' }], updatedAt: 1000, layoutRev: 7 };
      L.put('GF1', rec, { rev: '1100.1', full: true });
      ctx.W = { id: 'GF1', rec: L.full('GF1'), geom: true, pieces: [{ id: 'a', c: charmOf('a'), p: rec.placements[0] }, { id: 'b', c: charmOf('b'), p: rec.placements[1] }], mcLive: null };
      // (the Nest card's copy of the same sheet, built again in the background: the same places, OTHER charm objects)
      const page = { sheetId: 'GF1', metal: 'gold', followed, recalled: null, placements: rec.placements.map(p => Object.assign({}, p)), charms: [charmOf('a'), charmOf('b')] };
      ctx.allSheets = () => [page];
      vm.runInContext(code + '\nthis.M = { mcLive, mcHeld };', ctx);
      return { L, ctx, page };
    };
    // the press's own check, as charm-nest-sheetwin.js mcToggle makes it (the window's pieces must be the sheet's charms, else "The window showed an older copy of this sheet")
    const refused = (W, sh) => { const live = new Map(sh.placements.map(p => [p.id, p])), shown = W.pieces.filter(x => !x.gone), byId = new Map(sh.charms.map(c => [c.id, c])); return !W.geom || shown.length !== live.size || shown.some(x => !live.has(x.id) || x.c !== byId.get(x.id)); };
    ok('H0 the press still checks the window\'s pieces against the sheet it works on (MCREBIND kept, harmless)', /if \(W\.geom && mcRebind\(shown, sh\)\)/.test(src) && /if \(!W\.geom \|\| shown\.length !== live\.size \|\| shown\.some\(x => !live\.has\(x\.id\) \|\| x\.c !== byId\.get\(x\.id\)\)\)/.test(src));
    {
      const { L, ctx, page } = mkMC(SW.on, true, true);
      const sh = ctx.M.mcLive();
      ok('H1 cloud mode (CNViewer.on): Manual Control works on the sheet made from the one copy\'s record and the window\'s OWN drawn pieces, never the Nest card\'s copy', !!sh && sh._standalone === true && sh !== page && sh._rec === L.full('GF1') && sh.charms.every((c, i) => c === ctx.W.pieces[i].c));
      ok('H2 ... so the press starts: no "older copy" refusal', !!sh && !refused(ctx.W, sh));
      // a background refresh: the one copy reads the record again (another object, the same layout); the window takes it (G4), the Nest card's copy is rebuilt again with other objects
      const again = Object.assign({}, L.full('GF1'), { updatedAt: 2000, previewAt: 1990 }); L.put('GF1', again, { rev: '1101.1', full: true }); ctx.W.rec = L.full('GF1');
      page.charms = page.charms.map(c => Object.assign({}, c));
      const sh2 = ctx.M.mcLive();
      ok('H3 GF_Nov.24.26_Set-2_Sheet-1: after a background refresh with an identical layout, Manual Control still starts (the window\'s pieces, the one copy\'s record)', !!sh2 && !refused(ctx.W, sh2) && sh2.charms.every((c, i) => c === ctx.W.pieces[i].c));
      ok('H4 cloud mode: no page of this computer is the sheet Manual Control works on', ctx.M.mcHeld('GF1') === null);
      ctx.W.mcLive = new Set(['GF1']);
      ok('H5 ... save one Manual Control built here for itself just now (HOPLIVE, 10K / 14K: the sheets to move charms onto)', ctx.M.mcHeld('GF1') === page);
    }
    {
      const { ctx, page } = mkMC(SW.on, false, true);
      ok('H6 a computer that follows a run another carrier holds (mode off, followed): cloud mode too', ctx.M.mcHeld('GF1') === null && ctx.M.mcLive()._standalone === true);
    }
    {
      const { ctx, page } = mkMC(SW.on, false, false);
      ok('H7 the computer that CARRIES the run (its own page, mode off): its live sheet, exactly as before', ctx.M.mcLive() === page);
    }
    {
      const { ctx, page } = mkMC(SW.off, true, true);
      ok('H8 the sheet window\'s switch off: as before (the page\'s copy first)', ctx.M.mcLive() === page);
    }
  }

  /* ── I · stage 2, the order window's Sheet tab ── */
  {
    const src = read('charm-nest-sheetwin.js'), bsrc = read('charm-nest-bridge.js');
    const part = src.slice(src.indexOf('  const OW_SL = new Map();'), src.indexOf('  function recFor(id, o) {'));
    const T = clock(), docs = new Map([['S7', { rev: '1200.1', rec: whole('S7', 1) }]]), C = cloud(docs);
    const L = SLmod.create({ api: C.api, now: T.now, setTimeout: T.set, clearTimeout: T.clear, newerOf, switches: () => SW.on });
    const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise, Error });
    ctx.window = ctx; ctx.orderRecs = new Map(); ctx.tryDo = f => { try { return f ? f() : null; } catch (_) { return null; } }; ctx.slOn = v => (SW.on[v] ? L : null);
    const remade = []; ctx.OrderWin = { isOpen: () => true, remade: ids => remade.push(...ids) };
    vm.runInContext(part + '\nthis.OW = { recLive, orderLetGo };', ctx);
    const p1 = ctx.OW.recLive(L, 'S7', {}); await T.run(200); const g1 = await p1;
    ok('I1 order window on: a sheet not held is read once (whole), and the tab watches it in the one copy', !!g1 && g1.id === 'S7' && C.calls.length === 1 && !C.calls[0].ifRev && L.state().watched.includes('S7'));
    const p2 = ctx.OW.recLive(L, 'S7', {});
    ok('I2 asked again with nothing moved: the same record, no read; while the order window is open no minute counts (the feed keeps it the cloud\'s)', p2 === p1 && C.calls.length === 1 && /\(watching \|\| Date\.now\(\) - hit\.at < 60000\)/.test(part));
    docs.set('S7', { rev: '1201.1', rec: whole('S7', 2) }); L.names(['s:S7'], 'keys'); await T.run(200);
    ok('I3 the feed names the sheet: the one copy reads it by revision and the order window draws it again (remade)', C.calls.length === 2 && C.calls[1].ifRev === '1200.1' && remade.includes('S7'));
    const g3 = await ctx.OW.recLive(L, 'S7', { fresh: true });
    ok('I4 the window asks for it fresh after that: the newer record the one copy holds, no read more', g3.layoutRev === 2 && C.calls.length === 2);
    ctx.OW.orderLetGo();
    ok('I5 the order window closes: the sheets of its Sheet tab are watched no more, and the records it drew are let go (the next opening asks after each)', !L.state().watched.includes('S7') && !ctx.orderRecs.has('S7'));
    ctx.OrderWin.isOpen = () => false; const n0 = C.calls.length; const g5 = await ctx.OW.recLive(L, 'S7', {}); await T.run(200);
    ok('I5b another view drawing a sheet (the run history, the set picker) with the order window closed: drawn at once, asked after by revision, and NOT watched', !!g5 && C.calls.length === n0 + 1 && C.calls[n0].ifRev && !L.state().watched.includes('S7'));
    ok('I6 wiring: recFor goes to recLive while the switch is on; the old minute-kept path stays for the switch off; the window\'s close lets go',
      /\{ const L = slOn\("orderwin"\); if \(L\) return recLive\(L, id, o\); \}/.test(src) && /hit && Date\.now\(\) - hit\.at < 60000\) return hit\.p;/.test(src) && /SheetWin\.orderLetGo && SheetWin\.orderLetGo\(\)/.test(bsrc)
      && /const orderLiveOf = id => \{ const p = pageLive\(id\); return p && slOn\("orderwin"\) && cloudPage\(p\) \? null : p; \};/.test(src));
  }

  /* ── J · stage 2, the Nest cards in cloud mode (charm-nest-cloud-sheets.js) ── */
  {
    const mkNest = (sw, viewer, followed) => {
      const T = clock(); const docs = new Map([['N1', { rev: '1300.1', rec: whole('N1', 1) }], ['N2', { rev: '1301.1', rec: whole('N2', 1) }], ['N3', { rev: '1302.1', rec: whole('N3', 1) }]]), C = cloud(docs);
      const L = SLmod.create({ api: C.api, now: T.now, setTimeout: T.set, clearTimeout: T.clear, newerOf, switches: () => sw });
      const own = []; const follows = [];
      const pg = (id, metal, built) => Object.assign({ sheetId: id, metal, runId: 'R1', followed, status: 'complete', placements: built ? [{ id: 'c1', cxPt: 1, cyPt: 1, angle: 0 }] : [], charms: built ? [{ id: 'c1', poolId: '4200000001_1_1' }] : [], recalled: built ? null : row(id, 1), layoutRev: built ? 1 : null, contentRev: built ? 1 : null });
      const P1 = pg('N1', 'gold', false), P2 = pg('N2', 'silver', true), P3 = pg('N3', 'rose', true);
      const rebuilt = [];
      const win = { console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise, Error, setTimeout: (f, ms) => (ms > 1000 ? 0 : setTimeout(f, ms)), clearTimeout, setInterval: () => 0,
        document: { readyState: 'complete', hidden: false }, CNSheetLive: L, CNViewer: { on: () => viewer, sawSheet() {} },
        CN: { api: async (fn, body) => { own.push(body); return C.api(fn, body); }, renderCard() {}, agent() {} }, B: { orders: {}, run: null },
        Recall: { rebuild: async (p, o) => { const a = (o && o.answer) || {}; rebuilt.push([p.sheetId, !!(a.pools && +a.layoutRev === 4 && !('_live' in a)), { lr: +a.layoutRev, whole: Array.isArray(a.charms), pools: !!a.pools }]); p.charms = [{ id: 'c2', poolId: '4200000001_1_1' }]; p.placements = [{ id: 'c2', cxPt: 2, cyPt: 2, angle: 0 }]; p.recalled = null; }, piecesOf: async () => ({}), follow: async () => true },
        CNSync: { follow: spec => { follows.push(spec); return { live: () => true }; } }, CNRefit: { see() {} }, OrderWin: null };
      win.self = win; win.window = win;
      const ctx = vm.createContext(win);
      vm.runInContext('var allSheets = () => [P1, P2, P3]; var S = { mode: "nest", sheets: { gold: { pages: [P1], active: 0 }, silver: { pages: [P2], active: 0 }, rose: { pages: [P3], active: 0 } } };', Object.assign(ctx, { P1, P2, P3 }));
      ctx.S.sheets.silver.pages = [{}, P2]; ctx.S.sheets.silver.active = 0;   // (N2's card is not the one shown: another page of its metal is)
      vm.runInContext(read('charm-nest-cloud-sheets.js'), ctx, { filename: 'charm-nest-cloud-sheets.js' });
      for (const id of ['N1', 'N2', 'N3']) L.put(id, docs.get(id).rec, { rev: docs.get(id).rev });
      return { T, C, L, own, follows, P1, P2, P3, rebuilt, ctx, docs };
    };
    const settleAll = async T => { for (let i = 0; i < 4; i++) { await new Promise(r => setTimeout(r, 15)); await T.run(100); } };
    {
      const N = mkNest(SW.on, true, true);
      await settleAll(N.T);
      const spec = N.follows.find(f => f.name === 'cloud-sheets');
      ok('J1 cloud mode: the Nest cards\' sheets are watched in the one copy, and the cards\' own feed takes none of their names', !!spec && !spec.wants('s:N1') && !spec.wants('s:N2') && ['N1', 'N2', 'N3'].every(id => N.L.state().watched.includes(id)));
      N.own.length = 0; N.rebuilt.length = 0;
      for (const id of ['N1', 'N2', 'N3']) N.docs.set(id, { rev: '14' + id.slice(1) + '0.1', rec: whole(id, 4, { contentRev: 4 }) });
      N.L.names(['s:N1', 's:N2', 's:N3'], 'keys'); await settleAll(N.T);
      ok('J2 the feed names them: ONE read by revision for the three, by the one copy (the cards ask nothing themselves)', N.C.calls.filter(c => c.op === 'getMany').length === 1 && N.own.filter(b => b.op !== 'getMany' && !(b.op === 'getSheet' && b.pieces)).length === 0, N.own);
      const own = (p, id) => p._slSig === N.ctx.CharmNestCloudSheets.slSig(N.L.get(id)) && p.recalled !== N.L.get(id) && p.recalled.layoutRev === N.L.get(id).layoutRev && !Object.values(p).includes(N.L.get(id));   // (no page holds the one copy's own object)   // (the card's own copy of the one copy's record: the card may mark it, the one copy's is never changed)
      ok('J3 a card shown from its record takes the one copy\'s newer record at once (its own copy of it)', own(N.P1, 'N1') && N.P1.recalled.layoutRev === 4);
      ok('J4 a page built here that is NOT on screen is not rebuilt in the background: it is its record (its picture) again, built when looked at', own(N.P2, 'N2') && !N.P2.placements.length && !N.rebuilt.some(x => x[0] === 'N2'));
      ok('J5 the page on screen is built on demand from the one copy\'s record, with its pieces\' rows, and nothing read again for it', N.rebuilt.some(x => x[0] === 'N3' && x[1] === true) && N.own.filter(b => b.op === 'getSheet').length === 0, { rebuilt: N.rebuilt, own: N.own });
    }
    {
      const N = mkNest(SW.on, true, true);
      await settleAll(N.T); N.own.length = 0; N.rebuilt.length = 0; N.C.calls.length = 0;
      N.docs.set('N3', { rev: '1700.1', rec: whole('N3', 5) });
      N.L.put('N3', row('N3', 5), { rev: '1700.1', from: 'list' });   // (a newer LIST ROW of the sheet on screen reaches the one copy first: the Library's list, the run's list)
      await settleAll(N.T);
      const b3 = N.rebuilt.filter(x => x[0] === 'N3').map(x => x[2]);
      ok('J8 a page on screen is never built from a list row: the one copy reads the whole record first (ONE read, in full), and the page is built from that', N.C.calls.filter(c => c.id === 'N3').length === 1 && b3.length >= 1 && b3.every(x => x.whole && x.pools && x.lr === 5) && N.C.calls.some(c => c.op === 'getSheet' && c.id === 'N3' && !c.ifRev) && (N.P3.placements || []).length > 0, { rebuilt: N.rebuilt, calls: N.C.calls });
      N.C.calls.length = 0; N.P2.charms = [{ id: 'c1', poolId: '4200000001_1_1' }]; N.P2.placements = [{ id: 'c1', cxPt: 1, cyPt: 1, angle: 0 }]; N.P2.recalled = null; N.P2._manualHold = true;
      N.ctx.CharmNestCloudSheets._changed(['s:N2']); await settleAll(N.T);
      ok('J9 a page in use (Manual Control holds it) reads nothing and is not touched; it waits', N.C.calls.length === 0 && N.P2.placements.length === 1);
    }
    {
      const N = mkNest(SW.on, false, false);
      await settleAll(N.T);
      const spec = N.follows.find(f => f.name === 'cloud-sheets');
      ok('J6 the computer that CARRIES the run: its cards follow the feed themselves, as before; the one copy watches none of them', !!spec && spec.wants('s:N2') && !N.L.state().watched.includes('N2'));
    }
    {
      const N = mkNest(SW.off, true, true);
      await settleAll(N.T);
      const spec = N.follows.find(f => f.name === 'cloud-sheets');
      ok('J7 the Nest cards\' switch off: as before (the cards\' own feed and reads)', !!spec && spec.wants('s:N1') && !N.L.state().watched.includes('N1'));
    }
  }


  /* ── K · stage 3, nothing kept on the computer (NOTHINGKEPT) ── */
  const WORK = /^cn\.(outbox\.|sync\.cursor\.|sheetwin\.(freed|moves)|mc\.draft\.|orderhold\.run|laserSheetLast\.)/;
  class FakeStorage { constructor(init) { this._m = new Map(Object.entries(init || {})); this.writes = []; } get length() { return this._m.size; } key(i) { return [...this._m.keys()][i] ?? null; } getItem(k) { k = String(k); return this._m.has(k) ? this._m.get(k) : null; } setItem(k, v) { k = String(k); this.writes.push(k); this._m.set(k, String(v)); } removeItem(k) { this._m.delete(String(k)); } keys() { return [...this._m.keys()]; } }
  {
    // the outbox: a press with no disk is kept in memory and never written; an older page's journal is read once and deleted
    const ls = new FakeStorage({ 'cn.outbox.production': JSON.stringify([{ k: '0000000a1-old-1', mid: 'mp-old', world: 'production', op: 'putSheet', args: {}, state: 'pending', tab: 'x' }]) });
    const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise, setTimeout, clearTimeout, localStorage: ls, queueMicrotask });
    ctx.self = ctx; vm.runInContext(read('charm-nest-store.js'), ctx);
    const St = ctx.CharmNestStore; await St.init({ world: 'production' });
    const leftover = St.pending().map(e => e.mid);
    St.mutate({ entity: 's:A', op: 'putSheet', args: { sheet: { id: 'A' } }, label: 'x' }); await new Promise(r => setTimeout(r, 20));
    ok('K1 the outbox (cn.outbox.*): a press is kept in this page\'s memory only, never written to the computer; an older page\'s unsent presses are read once (sent) and deleted',
      leftover.includes('mp-old') && St.pending().length === 2 && !ls.writes.some(k => /^cn\.outbox\./.test(k)) && ls.getItem('cn.outbox.production') === null, { writes: ls.writes, leftover });
  }
  {
    // the feed's cursor
    const ls = new FakeStorage({ 'cn.sync.cursor.production': JSON.stringify({ n: '9.9.9.9', at: Date.now(), cg: 1 }) });
    class BC { postMessage() {} close() {} }
    const doc = { visibilityState: 'visible', addEventListener() {} };
    const win = { document: doc, BroadcastChannel: BC, navigator: { onLine: true }, addEventListener() {}, dispatchEvent() {}, CustomEvent: class {}, console, setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {}, localStorage: ls, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise };
    win.self = win; const ctx = vm.createContext(win); vm.runInContext(read('charm-nest-sync-worker.js'), ctx);
    const asked = []; const api = async (fn, body) => { asked.push(body.since); return { n: '1.0.0.0', keys: [] }; };
    ctx.CNSync.start({ world: 'production', api }); await new Promise(r => setTimeout(r, 30));
    ok('K2 the feed\'s cursor (cn.sync.cursor.*): kept in memory only: a new leader asks from the count it heard, never from a cursor on the computer, and none is written', asked[0] == null && !ls.writes.some(k => /^cn\.sync\.cursor\./.test(k)) && ls.getItem('cn.sync.cursor.production') === null, { asked, writes: ls.writes });
  }
  {
    // the last laser completion
    const ls = new FakeStorage({ 'cn.laserSheetLast.v1': JSON.stringify({ ana: { local: 5 } }) });
    const ctx = vm.createContext({ console, JSON, Map, Set, Date, Object, Array, String, Number, Math, Promise, setTimeout: () => 0, clearTimeout() {}, setInterval: () => 0, localStorage: ls });
    ctx.self = ctx; ctx.window = ctx; vm.runInContext(read('charm-nest-laser-time.js'), Object.assign(ctx, { root: ctx }));
    const LT = ctx.LaserSheetTime;
    if (LT) { LT.configure({ call: async () => ({}), who: () => ({ name: 'Ana', startAt: Date.now() - 60000 }), role: () => 'laser', now: () => Date.now() }); try { LT.noted({ ok: true }, { now: Date.now() }, 'Ana'); LT.forget('Ana'); } catch (_) {} }
    ok('K3 the last laser completion (cn.laserSheetLast.*): in memory only (the cloud keeps every completion: prepare reads it), the old one deleted, none written', !!LT && !ls.writes.some(k => /^cn\.laserSheetLast/.test(k)) && ls.getItem('cn.laserSheetLast.v1') === null, ls.writes);
  }
  {
    const oh = read('charm-nest-order-hold.js'), sw = read('charm-nest-sheetwin.js'), lt = read('charm-nest-laser-time.js'), st = read('charm-nest-store.js'), sy = read('charm-nest-sync-worker.js');
    const part = oh.slice(oh.indexOf('  const JRN = new Map();'), oh.indexOf('  const active = new Map();'));
    const ls = new FakeStorage({ 'cn.orderhold.run': JSON.stringify({ '4200000001': { step: 'moving' } }) });
    const ctx = vm.createContext({ JSON, Object, Map, Array, localStorage: ls, KEY: () => 'cn.orderhold.run' });
    vm.runInContext(part + '\nthis.J = journal;', ctx);
    const was = ctx.J.all(); ctx.J.set('4200000002', { step: 'start' }); ctx.J.drop('4200000001');
    ok('K4 a Hold\'s steps (cn.orderhold.run): in memory only; an older page\'s are read once (the step a reload interrupted) and deleted', was['4200000001'] && ctx.J.all()['4200000002'] && !ctx.J.all()['4200000001'] && !ls.writes.length && ls.getItem('cn.orderhold.run') === null);
    const code = t => t.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
    ok('K5 the sheet window writes none of its work to the computer: freed spots, moves on their way, Manual Control\'s moves not saved (memory: FREED, MOVES, MC_DRAFTS)',
      !/localStorage\.setItem\((FREED_KEY|JOURNAL|mcDraftKey|k)\(?/.test(code(sw)) && /MC_DRAFTS\.set\(k, JSON\.stringify/.test(sw) && /MC_DRAFTS\.set\(mcDraftKey\(id\)/.test(sw) && /function journal\(rid, entry\) \{ if \(entry\) MOVES\[rid\] = entry; else delete MOVES\[rid\]; \}/.test(sw));
    ok('K6 nor do the store, the feed, the Hold and the laser time: no localStorage.setItem in any of them', ![st, sy, oh, lt].some(t => /localStorage\.setItem\(|\.setItem\(cursorKey/.test(code(t))));
  }
  {
    // the page's NODISK + NOTHINGKEPT scripts: old work keys moved to memory and deleted, allowed keys kept on the computer
    const html = read('charm-nest-1.html');
    const scripts = []; const re = /<script>([\s\S]*?)<\/script>/g; let m; while ((m = re.exec(html)) && scripts.length < 2) { if (/NODISK|NOTHINGKEPT/.test(m[1])) scripts.push(m[1]); }
    const run = settings => {
      const init = { 'cn.outbox.production': '[{"k":"1"}]', 'cn.sync.cursor.production': '{"n":"1.1.1.1"}', 'cn.sheetwin.freed': '{}', 'cn.sheetwin.moves': '{}', 'cn.mc.draft.S1.tab1': '{"edits":[]}', 'cn.orderhold.run': '{}', 'cn.orderhold.run:sandbox': '{}', 'cn.laserSheetLast.v1': '{}',
        'cn.employee': 'Ana', 'cn.dev': 'dev-1', 'cn.push': 'on', 'cn.sw': 'off', 'cn.card.tab': 'team' };
      if (settings) init['cn.settings'] = settings;
      const disk = new FakeStorage(init);
      function Storage() {} Storage.prototype.getItem = function (k) { return disk.getItem(k); }; Storage.prototype.setItem = function (k, v) { return disk.setItem(k, v); }; Storage.prototype.removeItem = function (k) { return disk.removeItem(k); };
      Object.defineProperty(Storage.prototype, 'length', { get: () => disk.length }); Storage.prototype.key = function (i) { return disk.key(i); };
      const ls = new Storage();
      const ctx = vm.createContext({ Storage, localStorage: ls, IDBFactory: function () {}, indexedDB: { deleteDatabase() {} }, Map, String, RegExp });
      ctx.IDBFactory.prototype = { open() {} };
      for (const sc of scripts) vm.runInContext(sc, ctx);
      return { disk, ls };
    };
    for (const [label, settings] of [['production', null], ['the sandbox', '{"sandbox":"on"}']]) {
      const { disk, ls } = run(settings);
      const left = disk.keys().filter(k => WORK.test(k));
      ok(`K7 ${label}: every work key an older page left is deleted from the computer at load`, left.length === 0, left);
      const once = ls.getItem('cn.outbox.production');
      ls.setItem('cn.mc.draft.S2.tab1', '{"edits":[1]}');
      ok(`K8 ${label}: ... and moved into the page's memory (the code that kept it reads it once), later writes of those names stay in memory`, once === '[{"k":"1"}]' && ls.getItem('cn.mc.draft.S2.tab1') === '{"edits":[1]}' && disk.getItem('cn.mc.draft.S2.tab1') === null);
      ls.setItem('cn.employee', 'Michelle'); ls.setItem('cn.dev', 'dev-2');
      ok(`K9 ${label}: the allowed keys stay on the computer and still work (cn.employee, cn.dev, cn.settings, cn.push, cn.sw, the screen's own tabs)`, disk.getItem('cn.employee') === 'Michelle' && disk.getItem('cn.dev') === 'dev-2' && disk.getItem('cn.push') === 'on' && disk.getItem('cn.sw') === 'off' && disk.getItem('cn.card.tab') === 'team' && (settings ? disk.getItem('cn.settings') === settings : true));
    }
    ok('K10 NOTHINGKEPT runs right after NODISK, before every other script, in both worlds', scripts.length === 2 && html.indexOf('NOTHINGKEPT (SHEETLIVE stage 3') < html.indexOf('FASTOPEN (Paul, 23 Nov 2026') && !/sandbox/.test(scripts[1].replace(/\/\*[\s\S]*?\*\//, '')));
  }


  /* ── L · stage 4, every name kind reaches the sheet a view shows ── */
  {
    const kinds = [['s:A', 'its sheet (any write of the record: the writer, Manual Control, a hop, a take-off, a Change SKU, a green line, a nest\'s end)'], ['t:T1', 'its set (labels, membership, a set rewritten by a swap, a take-off, custom done)'],
      ['p:4200000001', 'an order\'s pieces (an engraving approved on either face, a piece moved, held or taken off)'], ['x:4200000001', 'an order cancelled'], ['o:4200000001', 'an order\'s timeline (every op that stamps an event)'],
      ['*', 'a write that named nothing (backPut on either face, a cancel, a restore)']];
    for (const [k, what] of kinds) {
      const docs = new Map([['A', { rev: '1500.1', rec: whole('A', 1, { setId: 'T1', orders: ['4200000001'] }) }]]), { T, C, L } = make(docs);
      L.put('A', docs.get('A').rec, { rev: '1500.1' }); L.watch('A', 'sheetwin', { full: true, orders: true });
      L.names([k], 'keys'); await T.run(200);
      ok(`L1 the sheet window (its orders watched) reads its sheet once by revision for ${k}: ${what}`, C.calls.length === 1 && (C.calls[0].ifRev === '1500.1' || (C.calls[0].ifRevs || {})['s:A'] === '1500.1'), C.calls);
    }
    for (const why of ['reset', 'verify', 'lost']) {
      const docs = new Map([['A', { rev: '1500.1', rec: whole('A', 1) }]]), { T, C, L } = make(docs);
      L.put('A', docs.get('A').rec, { rev: '1500.1' }); L.watch('A', 'sheetwin', { full: true }); L.names([], why); await T.run(200);
      ok(`L2 a ${why} of the feed reads it once by revision`, C.calls.length === 1);
    }
    for (const [k, reads] of [['s:A', 1], ['t:T1', 1], ['*', 1], ['p:4200000001', 0], ['x:4200000001', 0], ['o:4200000001', 0], ['s:B', 0], ['t:T9', 0]]) {
      const docs = new Map([['A', { rev: '1500.1', rec: whole('A', 1, { setId: 'T1', orders: ['4200000001'] }) }]]), { T, C, L } = make(docs);
      L.put('A', docs.get('A').rec, { rev: '1500.1' }); L.watchSet('nest', () => ['A']); L.names([k], 'keys'); await T.run(200);
      ok(`L3 a Nest card (no orders watched): ${k} reads ${reads ? 'it once' : 'nothing'}`, C.calls.length === reads, C.calls);
    }
    {
      const docs = new Map([['A', { rev: '1500.1', rec: whole('A', 1, { setId: 'T1', orders: ['4200000001'] }) }]]), { T, C, L } = make(docs);
      L.put('A', docs.get('A').rec, { rev: '1500.1' }); L.watch('A', 'sheetwin', { full: true, orders: true });
      L.names(['s:B', 't:T9', 'p:4200000009', 'x:4200000009', 'o:4200000009', 'p:*', 'x:*', 'o:*', 't:*'], 'keys'); await T.run(200);
      ok('L4 names of other sheets, sets and orders, and the kinds\' "some of it" wildcards, read nothing for it (their own views follow them: the placement feed, the engrave sync)', C.calls.length === 0);
      L.names(['s:A', 't:T1', 'p:4200000001', 'x:4200000001', 'o:4200000001'], 'keys'); await T.run(200);
      ok('L5 one answer naming it five ways: ONE read', C.calls.length === 1);
    }
  }

  console.log(`\nsheet-live: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
