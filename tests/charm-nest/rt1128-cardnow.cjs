/* CARDNOW (Paul, 28 Nov 2026: Library thumbnails do not update at once and look different from the detailed view; "it can not take more than 30s at worst case scenario to visually updated the system
   everywhere"; of how: "Why aren't you just updating the changes? Not the entire goddamn fucking shit"). Node only, offline, no jsdom: node tests/charm-nest/rt1128-cardnow.cjs
   The page's own code is run (vm) over a small stand-in DOM, as rt1128-noscrolljump.cjs runs it: charm-nest-sheet-live.js as it is, the Library's LaserReview, the one-piece path (Recall's charmOfRecord)
   and CardNow sliced out of charm-nest-bridge.js, and LIVEPV sliced out of charm-nest-1.html. Stand-ins: the cloud (every op counted), the design files (page memory → this computer's store → a download,
   as Pool.masterCharm reads them; every download counted), paintPreview (what it was asked to draw), the sheet window (its drawn pieces).
     A · the 28 Nov case (GF draft d-34970fa918a0793f, the sheet window open on it): layout 59 → 60 with 3 pieces added while its saved picture is of layout 58: the card shows the 3 new pieces at once,
         drawn by the page; only those 3 are made (their rows, their designs), the 67 already drawn are reused; the other cards are not touched; nothing is written; the view is not scrolled
     B · the cloud's picture catches up (previewRev 60): the card keeps the page's drawing (no flip back), LIVEPV and PICNOW leave it; a move of one piece and a saved front engraving (backPool: saved, its words changed, withdrawn) each draw
         the card again with no read; a saved picture of a later layout than the drawing takes its place
     M · what it costs (the Library alone, only list rows held): the first drawing of a 70-piece sheet with its designs not kept / in page memory / in this computer's store; a change that adds 3 pieces;
         a move of 1 piece; and the same with the sheet window open */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..'), read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 700))); if (cond) passed++; else failed++; };
const SLmod = require(path.join(ROOT, 'charm-nest-sheet-live.js'));
const { newerOf } = require(path.join(ROOT, 'charm-nest-store.js'));
const copy = x => JSON.parse(JSON.stringify(x));
const T0 = 1764360000000, GF = 'd-34970fa918a0793f';
const bridge = read('charm-nest-bridge.js'), page = read('charm-nest-1.html');
const between = (src, a, b) => { const i = src.indexOf(a); if (i < 0) return null; const j = src.indexOf(b, i); return j < 0 ? null : src.slice(i, j); };
const LR_SRC = between(bridge, 'const LaserReview =', 'const Sets =');
const CHARM_OF = between(bridge, '  async function charmOfRecord(d, rc, row) {', '  async function rebuildOnce(pg, opts) {');
const CARDNOW = between(bridge, 'const CardNow = window.CardNow = (() => {', '// (CARDNOW end)');
const LIVEPV = between(page, '(function livePreviews() {', '/* ═══ SHEETBUSY');
ok('0 the page\'s code is there: LaserReview, the one-piece path, LIVEPV, and CardNow (charm-nest-bridge.js)', !!LR_SRC && !!CHARM_OF && !!LIVEPV && !!CARDNOW, { LR: !!LR_SRC, charmOf: !!CHARM_OF, livepv: !!LIVEPV, cardNow: !!CARDNOW });

/* ── a sheet record of n pieces (each its own design), a list row of it ── */
const piece = (i, at) => { const pid = `42${String(1000000 + i)}_1_1`, sku = 'SKU' + i, ai = `masters/${sku}.ai`; return { c: { id: `pool:${ai}:${pid}`, poolId: pid, sku, hash: 'h' + i, aiPath: ai, sourceId: 'pool:' + ai, order: pid.split('_')[0] }, p: { id: `pool:${ai}:${pid}`, cxPt: 10 + (i % 10) * 60 + (at || 0), cyPt: 10 + Math.floor(i / 10) * 40, angle: 0, wPt: 40, hPt: 30, scale: 0.971 } }; };
function sheet(id, lr, n, more) {
  const ps = Array.from({ length: n }, (_, i) => piece(i));
  return Object.assign({ id, metal: 'gold', draft: true, runId: 'R1', setId: null, orders: ps.map(x => x.c.order), poolIds: ps.map(x => x.c.poolId), placedCount: n, charmCount: n, density: 0.4, freePt2: 100,
    updatedAt: T0 + lr * 1000, layoutRev: lr, contentRev: lr, previewRev: 58, previewAt: T0, preview: `https://x.test/${id}/preview.png`, stock: { wPt: 720, hPt: 360 }, backPool: [], engraving: {},
    charms: ps.map(x => x.c), placements: ps.map(x => x.p) }, more || {});
}
const rowOf = rec => { const r = Object.assign({}, rec); delete r.charms; delete r.placements; delete r.backPool; return r; };   // (a list row: no layout)
const moved = (rec, k, dx, more) => Object.assign(copy(rec), more || {}, { placements: rec.placements.map((p, i) => (i === k ? Object.assign({}, p, { cxPt: p.cxPt + dx }) : p)) });

/* ── the stand-in DOM (as rt1128-noscrolljump.cjs) ── */
const calls = [];
class El {
  constructor(tag) { this.tagName = String(tag).toUpperCase(); this.children = []; this.parentElement = null; this.dataset = {}; this.style = {}; this.attrs = {}; this._html = ''; this.textContent = ''; this.h = 0; const c = this._cls = new Set();
    this.classList = { add: (...x) => x.forEach(k => c.add(k)), remove: (...x) => x.forEach(k => c.delete(k)), contains: k => c.has(k), toggle: (k, on) => { if (on === undefined ? !c.has(k) : on) c.add(k); else c.delete(k); } }; }
  get className() { return [...this._cls].join(' '); } set className(v) { this._cls.clear(); String(v).split(/\s+/).filter(Boolean).forEach(k => this._cls.add(k)); }
  get isConnected() { return true; } get offsetParent() { return this.hidden ? null : {}; }
  get innerHTML() { return this._html; } set innerHTML(v) { this._html = String(v); this.htmlWrites = (this.htmlWrites || 0) + 1; }
  get src() { return this.attrs.src || ''; } set src(v) { this.attrs.src = String(v); this.srcWrites = (this.srcWrites || 0) + 1; }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; } setAttribute(k, v) { this.attrs[k] = String(v); } removeAttribute(k) { delete this.attrs[k]; } hasAttribute(k) { return k in this.attrs; }
  appendChild(n) { if (n.parentElement) n.parentElement.children.splice(n.parentElement.children.indexOf(n), 1); this.children.push(n); n.parentElement = this; return n; }
  insertBefore(n, ref) { if (!ref) return this.appendChild(n); this.children.splice(this.children.indexOf(ref), 0, n); n.parentElement = this; return n; }
  remove() { if (this.parentElement) { this.parentElement.children.splice(this.parentElement.children.indexOf(this), 1); this.parentElement = null; } this.removed = true; }
  replaceWith(n) { const p = this.parentElement; if (!p) return; p.children.splice(p.children.indexOf(this), 1, n); n.parentElement = p; this.parentElement = null; }
  querySelector() { return null; } querySelectorAll() { return []; } closest() { return null; }
  addEventListener() {} removeEventListener() {} dispatchEvent() { return true; }
  scrollIntoView(o) { calls.push(['scrollIntoView', o]); } scrollTo(...a) { calls.push(['scrollTo', a]); } scrollBy(...a) { calls.push(['scrollBy', a]); } focus(o) { calls.push(['focus', o]); }
}

/* ── one page: the Library's cards, the one copy, the cloud, the designs ── */
function world(o) {
  o = o || {}; calls.length = 0;
  const docs = new Map(), ops = [];
  const pools = id => ({ poolId: id, orderId: id.split('_')[0], size: null, state: 'placed', sheetId: GF });
  const api = async (fn, body) => {
    ops.push(Object.assign({ fn }, copy(body)));
    if (body.op === 'getSheet') { const d = docs.get(body.id); if (!d) return { sheet: null }; if (body.ifRev && body.ifRev === d.rev) return { unchanged: true, rev: d.rev }; return Object.assign({ sheet: copy(d.rec), rev: d.rev }, body.pieces ? { pools: Object.fromEntries((d.rec.poolIds || []).map(id => [id, pools(id)])) } : {}); }
    if (body.op === 'getMany') { const recs = {}; for (const k of body.keys) { const d = docs.get(k.slice(2)); if (d && body.ifRevs[k] !== d.rev) recs[k] = { rev: d.rev, rec: copy(d.rec) }; } return { recs, gone: [] }; }
    if (body.op === 'poolGet') return { pools: Object.fromEntries(body.poolIds.map(id => [id, pools(id)])) };
    return { ok: true };
  };
  const cloud = (rec, rev) => docs.set(rec.id, { rec: copy(rec), rev });
  // the design files: page memory (B.pool.sources), this computer's store (Pool.keptMaster), else a download (Pool.masterCharm's order)
  const D = { memory: new Map(), store: new Set(o.store || []), downloads: [], storeReads: 0 };
  for (const k of o.memory || []) D.memory.set(k, null);
  const srcOf = key => ({ id: 'pool:' + key, charms: [{ id: 'pool:' + key + ':0', name: key, members: [], outline: {}, centerPt: [0, 0], hash: 'hh' }] });
  const Pool = {
    sizeEntry: (e, size) => (e && e.sizes && size && e.sizes[size]) || e,
    masterCharm: async (e, size) => { const key = Pool.sizeEntry(e, size).aiPath; let s = D.memory.get(key); if (s) return s; if (D.store.has(key)) D.storeReads++; else D.downloads.push(key); s = srcOf(key); D.memory.set(key, s); return s; },
    ensureBase: async src => src.charms[0], cloneCharm: (b, id) => Object.assign({}, b, { id }) };
  for (const k of D.memory.keys()) D.memory.set(k, srcOf(k));
  const stage = new El('div'), body = new El('main'); stage.id = 'stage'; body.id = 'libBody'; stage.appendChild(body);
  let top = 300; const writes = []; Object.defineProperty(stage, 'scrollTop', { get: () => top, set: v => { writes.push(+v); top = +v; } });
  stage.getBoundingClientRect = () => ({ top: 0, bottom: 800, left: 0, right: 1200, width: 1200, height: 800 });
  const cards = [], imgs = [], paints = [], frames = [], timers = [];
  let n = 0;
  const card = id => { const c = new El('div'); c.className = 'setCard'; c.dataset.laserCard = 'set'; c._laserSheets = [id]; c._laserSet = { setId: 'T-' + id, seq: 1, day: '2026-11-28', sheetIds: [id] }; c.h = 400;
    c.getBoundingClientRect = () => ({ top: 0, bottom: 400, left: 0, right: 300, width: 300, height: 400 }); body.appendChild(c); cards.push(c);
    const img = new El('img'); img.dataset.sheetPreview = id; img.attrs.src = `https://x.test/${id}/preview.png?v=${T0}`; img.srcWrites = 0; imgs.push(img); c.img = img; return c; };
  const doc = { hidden: false, readyState: 'complete', activeElement: null, head: new El('head'), body: new El('body'), documentElement: new El('html'), hasFocus: () => true, addEventListener() {}, removeEventListener() {},
    createElement: t => { const e = new El(t); if (String(t).toLowerCase() === 'canvas') e.toDataURL = () => 'data:image/png;base64,PAGE' + (++n); return e; },
    getElementById: id => (id === 'stage' ? stage : id === 'libBody' ? body : null), querySelector: () => null,
    querySelectorAll: sel => (/img\[data-sheet-preview\]/.test(sel) ? imgs.slice() : /\[data-laser-card\]/.test(sel) && !/data-laser-area/.test(sel) ? cards.filter(c => c.parentElement) : []) };
  const win = { document: doc, console: { log() {}, warn() {}, error() {}, info() {} }, innerHeight: 800, innerWidth: 1200, performance: { now: () => Date.now() }, JSON, Math, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, Boolean, RegExp, Error, Symbol, Intl,
    requestAnimationFrame: f => (frames.push(f), frames.length), cancelAnimationFrame() {}, setTimeout: (f, ms) => (timers.push({ f, ms: +ms || 0 }), timers.length), clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    addEventListener() {}, removeEventListener() {}, CustomEvent: class { constructor(t, x) { this.type = t; Object.assign(this, x); } }, MutationObserver: class { observe() {} },
    esc: x => String(x == null ? '' : x), cors: x => x, S: { mode: 'library', cloud: { ok: false }, library: { rows: [] }, settings: {} }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() },
    O: { setLabel: k => 'Set-' + k }, CNListActivity: { compareDays: () => 0, state: () => ({ direction: 'desc' }), isSet: () => true, setDay: () => '2026-11-28' }, api,
    picUrl: r => r.preview + '?v=' + (+r.previewAt || 'r' + r.previewRev), CN_SHEETLIVE: { library: true, sheetwin: true, orderwin: true, nest: true }, CharmNestStore: { newerOf, get: () => null },
    LibraryDone: { isDone: r => !!(r && r.laserDoneAt), refreshCards() {}, addedSeals() {}, snapshot: () => null, glideFrom() {} },
    // (the one-piece path's own world: the master library, the pieces' fields, the artwork edits; SKUNOW off)
    Master: { entryFor: sku => ({ sku, aiPath: `masters/${sku}.ai`, sizes: {} }) }, Pool, Review: { skuNowOf: () => null },
    CharmNestPoolPieces: { sourceOf: () => null, customIndexOf: () => null, cleanFields: () => ({}), fieldsOf: () => ({}), placedCharms: d => (d.placements || []).map(p => ({ p, c: (d.charms || []).find(c => c.id === p.id) || null })) },
    CharmNestArtEdit: { piece: async c => c },
    stockFor: (m, sh) => (sh && sh.keptStock) || { wPt: 720, hPt: 360 }, shippedOf: () => null,
    paintPreview: (cv, sh, clean) => { paints.push({ id: sh.sheetId, clean, n: sh.placements.length, ids: sh.placements.map(p => p.id), at: Object.fromEntries(sh.placements.map(p => [p.id, p.cxPt])), drawn: sh.placements.every(p => sh.charms.some(c => c.id === p.id && c.members)), fronts: (sh.backPool || []).filter(b => b.face === 'front').length, words: (sh.backPool || []).filter(b => b.face === 'front' && !b.invalidated).map(b => b.text), inPage: false }); } };
  win.window = win; win.self = win;
  const ctx = vm.createContext(win);
  vm.runInContext('var placedOfRecord = d => CharmNestPoolPieces.placedCharms(d);', ctx);
  vm.runInContext(read('charm-nest-readiness.js'), ctx);
  win.CNSheetLive = SLmod.create({ api, newerOf, switches: () => win.CN_SHEETLIVE });
  vm.runInContext(LR_SRC, ctx);
  vm.runInContext('window.Recall = { charmOf: (() => { ' + CHARM_OF + ' return charmOfRecord; })() };', ctx);
  vm.runInContext(LIVEPV, ctx);
  if (CARDNOW) vm.runInContext(CARDNOW + '\n;', ctx);
  // (time passes: the one copy's reads, CardNow's look (a zero timer); the Library's own later timers are not run, as in rt1128-noscrolljump.cjs)
  const settle = async () => { for (let i = 0; i < 40; i++) { await new Promise(r => setTimeout(r, 5)); const now = timers.filter(t => t.ms <= 0); for (const t of now) timers.splice(timers.indexOf(t), 1); now.forEach(t => t.f()); for (const f of frames.splice(0)) f(); } };
  const none = { state: () => ({}), _entry: () => null, urlOf: () => '' }, last = () => paints[paints.length - 1] || { id: '', n: 0, ids: [], at: {}, fronts: 0, drawn: false };
  return { win, ctx, LR: win.LaserReview, SL: win.CNSheetLive, CN: () => win.CardNow || none, last, cards, imgs, card, paints, ops, D, cloud, docs, settle, writes, top: () => top };
}
const reads = ops => ops.every(x => ['getSheet', 'getMany', 'poolGet'].includes(x.op));
const tally = ops => ops.reduce((m, x) => (m[x.op] = (m[x.op] || 0) + 1, m), {});

(async () => {
  /* ── A · the 28 Nov case, the sheet window open on the draft ── */
  {
    const P = world();
    const A = P.card('A'), B = P.card(GF), C = P.card('C');
    const recA = sheet('A', 5, 4, { previewRev: 5 }), recC = sheet('C', 7, 4, { previewRev: 7 }), r59 = sheet(GF, 59, 67);
    for (const r of [recA, recC]) { P.cloud(r, '900.' + r.id.length); P.LR.record(rowOf(r), '900.' + r.id.length); }
    // the sheet window holds the draft's whole record and has drawn its 67 pieces (charm-nest-sheetwin.js W.pieces)
    P.cloud(r59, '901.1'); P.SL.watch(GF, 'sheetwin', { full: true });
    const winPieces = r59.charms.map(c => ({ id: c.id, poolId: c.poolId, c: Object.assign({}, c, { members: [], outline: {}, centerPt: [0, 0], fromWindow: true }) }));
    P.win.SheetWin = { _W: { id: GF, rec: P.SL.put(GF, copy(r59), { rev: '901.1', from: 'sheetwin' }), pieces: winPieces } };
    P.LR.record(rowOf(r59), '901.1');
    await P.settle();
    const first = P.paints.filter(x => x.id === GF);
    ok('A1 the draft\'s card (picture of layout 58, layout 59) is drawn by the page from the window\'s own 67 pieces: no read, no design read', first.length === 1 && first[0].n === 67 && first[0].clean === true && P.ops.length === 0 && P.D.downloads.length === 0 && /^data:image\/png;base64,PAGE/.test(B.img.src), { paints: first.length, ops: P.ops, dl: P.D.downloads.length, src: B.img.src.slice(0, 40) });
    ok('A2 ... the cards whose saved picture is current are left as they are (nothing drawn, nothing read)', A.img.srcWrites === 0 && C.img.srcWrites === 0 && !P.paints.some(x => x.id !== GF));
    // 20:36 UTC: the cloud puts 3 pieces on it: layout 60, its saved picture still of layout 58; the window's follow reads the whole record (the window has not drawn them yet)
    const r60 = sheet(GF, 60, 70); P.cloud(r60, '902.1'); P.ops.length = 0; P.D.downloads.length = 0; const before = B.img.src, aw = A.img.srcWrites, cw = C.img.srcWrites;
    P.SL.put(GF, copy(r60), { rev: '902.1', from: 'sheetwin' });
    await P.settle();
    const last = P.last(), added = r60.placements.slice(67).map(p => p.id);
    ok('A3 the card shows the 3 new pieces at once: drawn again by the page with all 70 pieces', last.id === GF && last.n === 70 && added.every(id => last.ids.includes(id)) && last.drawn && B.img.src !== before && /PAGE/.test(B.img.src), { n: last.n, src: B.img.src.slice(0, 40) });
    ok('A4 only the 3 new pieces are made: their 3 rows in ONE poolGet and their 3 designs; the 67 drawn pieces are reused', tally(P.ops).poolGet === 1 && P.ops.length === 1 && P.ops[0].poolIds.length === 3 && P.D.downloads.length === 3 && P.CN().state().made === 3 && P.CN().state().reused === 67, { ops: P.ops, dl: P.D.downloads, st: P.CN().state() });
    ok('A5 no getSheet: the record came with the window\'s own read (SHEETLIVE)', !P.ops.some(x => x.op === 'getSheet'));
    ok('A6 nothing is written to the cloud (reads only)', reads(P.ops), P.ops);
    ok('A7 the other cards are not touched (their pictures never set, their cards never written)', A.img.srcWrites === aw && C.img.srcWrites === cw && !(A.htmlWrites || C.htmlWrites), { a: A.img.srcWrites, c: C.img.srcWrites });
    ok('A8 the view is not moved: no scroll / focus call, the scroller never written', calls.length === 0 && P.writes.length === 0 && P.top() === 300, { calls, writes: P.writes });
    ok('A9 the drawing copy is the card\'s own: the page holds no sheet for it (allSheets stays empty, Manual Control never sees it)', P.win.allSheets().length === 0 && !!P.CN()._entry(GF) && !!P.CN()._entry(GF).copy && P.CN()._entry(GF).copy.placements.length === 70);

    /* ── B · after it ── */
    // the cloud's file job writes the new picture (previewRev 60): the list's row of it
    const r60p = Object.assign(copy(r60), { previewRev: 60, previewAt: T0 + 62000, preview: `https://x.test/${GF}/preview.png`, updatedAt: T0 + 62000 }); P.cloud(r60p, '903.1'); P.ops.length = 0;
    const drawn = B.img.src, nPaint = P.paints.length; P.LR.record(rowOf(r60p), '903.1'); await P.settle();
    P.win.LivePreview.tick(); await P.settle();
    ok('B1 the cloud\'s picture caught up (previewRev 60): the card keeps the page\'s drawing, no flip back (PICNOW and LIVEPV leave it)', B.img.src === drawn && P.paints.length === nPaint && B.img.dataset.cardNow === '1', { src: B.img.src.slice(0, 50) });
    ok('B2 ... its row has no layout: the whole record is read once, through the one copy (shared with the window\'s watch)', tally(P.ops).getSheet === 1 && P.ops.length === 1 && reads(P.ops), P.ops);
    // a person moves one piece by hand (contentRev rises, layoutRev stays): the window's follow
    const r61 = moved(r60p, 5, 30, { contentRev: 61, updatedAt: T0 + 70000 }); P.cloud(r61, '904.1'); P.ops.length = 0; P.D.downloads.length = 0;
    P.SL.put(GF, copy(r61), { rev: '904.1', from: 'sheetwin' }); await P.settle();
    const mv = P.last(), pid = r61.placements[5].id;
    ok('B3 a move of one piece draws the card again with that piece where it now lies: no read, no design read', mv.at[pid] === r60.placements[5].cxPt + 30 && mv.n === 70 && P.ops.length === 0 && P.D.downloads.length === 0 && P.CN().state().made === 3, { at: mv.at[pid], ops: P.ops });
    // its front engraving saved on one piece (backPool: face front, its file): the record brings it
    const r62 = Object.assign(copy(r61), { backPool: [{ face: 'front', poolId: r61.charms[2].poolId, approvedAt: T0 + 80000, text: 'Waldo', lines: ['Waldo'], outputs: { ai: { url: 'https://x.test/f.ai' } } }], updatedAt: T0 + 80000 });
    P.cloud(r62, '905.1'); P.ops.length = 0; const nb = P.paints.length;
    P.SL.put(GF, copy(r62), { rev: '905.1', from: 'sheetwin' }); await P.settle();
    ok('B4 a saved front engraving (backPool) draws the card again with it (paintPreview gets the record\'s backPool), no read', P.paints.length === nb + 1 && P.last().fronts === 1 && P.ops.length === 0, { paints: P.paints.length - nb, ops: P.ops });
    // (FRONTPIC, another branch, draws a piece's saved front from the sheet's backPool in paintPreview: its words changed, or the front withdrawn, is a change of that piece: the card is drawn again)
    const r62b = Object.assign(copy(r62), { backPool: [Object.assign({}, r62.backPool[0], { text: 'Waldo & Co', lines: ['Waldo & Co'], approvedAt: T0 + 85000 })], updatedAt: T0 + 85000 }); P.cloud(r62b, '905.2'); const nc = P.paints.length;
    P.SL.put(GF, copy(r62b), { rev: '905.2', from: 'sheetwin' }); await P.settle();
    ok('B4b its words changed: the card is drawn again with the new words, no read', P.paints.length === nc + 1 && JSON.stringify(P.last().words) === '["Waldo & Co"]' && P.ops.length === 0, { words: P.last().words, ops: P.ops });
    const r62c = Object.assign(copy(r62b), { backPool: [Object.assign({}, r62b.backPool[0], { invalidated: true })], updatedAt: T0 + 88000 }); P.cloud(r62c, '905.3'); const nd = P.paints.length;
    P.SL.put(GF, copy(r62c), { rev: '905.3', from: 'sheetwin' }); await P.settle();
    ok('B4c the front withdrawn: the card is drawn again without it, no read', P.paints.length === nd + 1 && P.last().words.length === 0 && P.ops.length === 0, { words: P.last().words, ops: P.ops });
    // the cloud's saved picture of a LATER layout than the card's drawing: it is the newer one
    const keep = B.img.src; P.SL.put(GF, Object.assign(rowOf(r62), { layoutRev: 64, contentRev: 64, previewRev: 64, previewAt: T0 + 99000, updatedAt: T0 + 99000 }), { rev: '906.1', from: 'list' });
    ok('B5 never a picture older than the cloud\'s: a saved picture of a later layout than the drawing is the one shown (urlOf gives none)', P.CN().urlOf(GF) === '' && keep === B.img.src);
    P.win.LivePreview.tick();
    ok('B6 ... LIVEPV puts that saved picture on the card', /x\.test\/d-34970fa918a0793f\/preview\.png\?v=/.test(B.img.src) && !B.img.dataset.cardNow, { src: B.img.src });
  }

  /* ── M · what it costs: the Library alone (only list rows held), a 70-piece sheet ── */
  const measure = async (label, o) => {
    const P = world(o); const B = P.card(GF); P.card('A');
    const r1 = sheet(GF, 59, 70); P.cloud(r1, '910.1');
    if (o.window) { P.SL.watch(GF, 'sheetwin', { full: true }); P.win.SheetWin = { _W: { id: GF, rec: P.SL.put(GF, copy(r1), { rev: '910.1', from: 'sheetwin' }), pieces: r1.charms.map(c => ({ id: c.id, poolId: c.poolId, c: Object.assign({}, c, { members: [], outline: {}, centerPt: [0, 0] }) })) } }; }
    P.LR.record(rowOf(r1), '910.1'); await P.settle();
    const first = { ops: tally(P.ops), rows: P.ops.filter(x => x.op === 'poolGet').reduce((s, x) => s + x.poolIds.length, 0) + P.ops.filter(x => x.op === 'getSheet' && x.pieces).length * 70, designs: P.D.downloads.length, store: P.D.storeReads, drawn: P.last().n };
    P.ops.length = 0; P.D.downloads.length = 0; P.D.storeReads = 0;
    const r2 = sheet(GF, 60, 73); P.cloud(r2, '911.1');
    if (o.window) P.SL.put(GF, copy(r2), { rev: '911.1', from: 'sheetwin' }); else P.LR.record(rowOf(r2), '911.1');
    await P.settle();
    const add = { ops: tally(P.ops), rows: P.ops.filter(x => x.op === 'poolGet').reduce((s, x) => s + x.poolIds.length, 0), designs: P.D.downloads.length, store: P.D.storeReads, drawn: P.last().n };
    P.ops.length = 0; P.D.downloads.length = 0; P.D.storeReads = 0;
    const r3 = moved(r2, 1, 25, { contentRev: 61, updatedAt: T0 + 61500 }); P.cloud(r3, '912.1');
    if (o.window) P.SL.put(GF, copy(r3), { rev: '912.1', from: 'sheetwin' }); else P.LR.record(rowOf(r3), '912.1');
    await P.settle();
    const mv = { ops: tally(P.ops), rows: P.ops.filter(x => x.op === 'poolGet').reduce((s, x) => s + x.poolIds.length, 0), designs: P.D.downloads.length, store: P.D.storeReads, at: P.last().at[r3.placements[1].id] === r2.placements[1].cxPt + 25 };
    console.log(`     ${label}\n       first drawing: ${JSON.stringify(first)}\n       +3 pieces:     ${JSON.stringify(add)}\n       move 1 piece:  ${JSON.stringify(mv)}`);
    return { first, add, mv, B, P };
  };
  const designs70 = Array.from({ length: 73 }, (_, i) => `masters/SKU${i}.ai`);
  const m1 = await measure('Library alone, designs not kept on this computer', {});
  ok('M1 first drawing of a 70-piece sheet, designs not kept: ONE getSheet (whole, with its pieces\' rows), 70 design reads', JSON.stringify(m1.first.ops) === '{"getSheet":1}' && m1.first.designs === 70 && m1.first.drawn === 70, m1.first);
  ok('M2 a change that adds 3 pieces: ONE getSheet (the new layout) and ONE poolGet of the 3 new rows, 3 design reads', JSON.stringify(m1.add.ops) === '{"getSheet":1,"poolGet":1}' && m1.add.rows === 3 && m1.add.designs === 3 && m1.add.drawn === 73, m1.add);
  ok('M3 a move of 1 piece: ONE getSheet (the new layout), no row, no design', JSON.stringify(m1.mv.ops) === '{"getSheet":1}' && m1.mv.designs === 0 && m1.mv.at, m1.mv);
  const m2 = await measure('Library alone, designs already in page memory', { memory: designs70 });
  ok('M4 designs in page memory: the same reads of the cloud, no design read at all', m2.first.designs === 0 && m2.first.store === 0 && m2.add.designs === 0 && JSON.stringify(m2.first.ops) === '{"getSheet":1}' && JSON.stringify(m2.add.ops) === '{"getSheet":1,"poolGet":1}', { first: m2.first, add: m2.add });
  const m3 = await measure('Library alone, designs in this computer\'s store (IndexedDB)', { store: designs70 });
  ok('M5 designs in this computer\'s store: no download (70, then 3, store reads)', m3.first.designs === 0 && m3.first.store === 70 && m3.add.designs === 0 && m3.add.store === 3, { first: m3.first, add: m3.add });
  const m4 = await measure('the sheet window open on it (its whole record and its drawn pieces held)', { window: true });
  ok('M6 the window open: the first drawing reads nothing (its 70 drawn pieces); +3 pieces: their rows only (one poolGet of 3) and 3 designs; a move: nothing', JSON.stringify(m4.first.ops) === '{}' && m4.first.designs === 0 && JSON.stringify(m4.add.ops) === '{"poolGet":1}' && m4.add.rows === 3 && m4.add.designs === 3 && JSON.stringify(m4.mv.ops) === '{}' && m4.mv.at, { first: m4.first, add: m4.add, mv: m4.mv });
  ok('M7 every measured path only reads', [m1, m2, m3, m4].every(m => Object.keys(Object.assign({}, m.first.ops, m.add.ops, m.mv.ops)).every(k => ['getSheet', 'getMany', 'poolGet'].includes(k))));

  /* ── code text: the wiring that only the whole page shows ── */
  ok('W1 the page loads charm-nest-bridge.js under a new token (-cw1)', /charm-nest-bridge\.js\?v=[^"]*-cw1(?:-\w+)*"/.test(page));
  ok('W2 LIVEPV asks CardNow for a card it does not draw itself, and before putting back an older saved picture', !!LIVEPV && /if \(!sh \|\| !\(sh\.placements \|\| \[\]\)\.length\) \{ cardNow\(img, null, rowsOf\); continue; \}/.test(LIVEPV) && /if \(!localOk\(sh, c\)\) \{ if \(cardNow\(img, c, rowsOf\)\) continue;/.test(LIVEPV));
  ok('W3 CardNow writes nothing and draws with the page\'s own paintPreview (clean)', !!CARDNOW && /paintPreview\(cv, copy, true\)/.test(CARDNOW) && !/op: "(?:putSheet|poolPut|poolUpdate|sheetLayoutPatch)"/.test(CARDNOW) && !/uploadBytes/.test(CARDNOW));

  console.log(`\nrt1128-cardnow: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
})().catch(e => { console.log('FAIL rt1128-cardnow threw: ' + (e && e.stack || e)); process.exit(1); });
