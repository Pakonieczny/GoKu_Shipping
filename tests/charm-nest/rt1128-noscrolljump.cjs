/* NOSCROLLJUMP (Paul, 28 Nov 2026: "Why does the library continuously scroll on my behalf when it's being updated? I told you I don't want any part of the system to get changed when things are being
   updated. You can't scroll the user or scroll down or scroll up."). Node only, offline, no jsdom: node tests/charm-nest/rt1128-noscrolljump.cjs
   The page's own code is run (vm): charm-nest-sheet-live.js as it is, and the Library's LaserReview sliced out of charm-nest-bridge.js over a small stand-in DOM with a stand-in layout (cards stacked
   in the Library's scroller, #stage); what only a browser can show of the Sets drawing is checked in the code's text.
     A · SHEETLIVE: the same record again (the store's echo of a list read, a copy of what is held) is no change: the object held stays and NO view is told; an older one is not told either; a real
         change is told once
     B · the Library: a store echo, and a newer record that changes nothing a card shows (its write time, the nest job's beat), draw NOTHING again
     C · the Library: a real change to ONE sheet draws only its card, in place: the other cards are the very same nodes, never touched; its picture is the same <img> with the new picture; no
         scrollIntoView / scrollTo / scrollBy / focus is called and the scroller is not moved
     D · the Library: a drawing that makes a card ABOVE the view taller (a rail, a "Checking…" line) leaves the card the person looks at exactly where it stood on screen
     E · the Sets view (code text): a list read again keeps every card whose set and drawing did not change, draws only the changed set's card in its place, and holds the view */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '..', '..'), read = f => fs.readFileSync(path.join(ROOT, f), 'utf8');
let passed = 0, failed = 0;
const ok = (name, cond, detail) => { console.log((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + JSON.stringify(detail).slice(0, 600))); if (cond) passed++; else failed++; };
const SLmod = require(path.join(ROOT, 'charm-nest-sheet-live.js'));
const { newerOf } = require(path.join(ROOT, 'charm-nest-store.js'));
const copy = x => JSON.parse(JSON.stringify(x));
const T0 = 1764300000000;
const row = (id, n, more) => Object.assign({ id, setId: 'T-' + id, setSeq: 1, sheetIndex: 1, metal: 'gold', orders: ['42000000' + n], poolIds: [id + '_p1'], placedCount: 3, charmCount: 3, density: 0.4, freePt2: 100,
  updatedAt: T0 + n * 1000, layoutRev: n, contentRev: n, preview: 'https://x.test/' + id + '.png', previewAt: T0 + n, engraving: {} }, more || {});

/* ── A · SHEETLIVE tells only what changed ── */
{
  const L = SLmod.create({ api: null, newerOf, switches: { library: true } });
  const told = []; L.subscribe('*', ev => told.push(ev));
  const a = row('A', 1), held = L.put('A', a, { rev: '500.1', from: 'library' });
  ok('A1 a first record is held and told once', held === a && told.length === 1);
  const echo = L.put('A', copy(a), { rev: '500.1', from: 'store' });
  ok('A2 the same record again (a copy: the store\'s echo of a list read) changes nothing: the held object stays and NO view is told', echo === a && L.get('A') === a && told.length === 1, { told: told.length, same: echo === a });
  const rows = L.putRows([copy(a)], { 's:A': '500.1' });
  ok('A3 a list read of the version already held tells nobody either', rows[0] === a && told.length === 1, { told: told.length });
  const older = L.put('A', row('A', 0), { rev: '499.1', from: 'list' });
  ok('A4 an older record is not taken and not told (NEWERWINS)', older === a && told.length === 1);
  const a2 = row('A', 2, { placedCount: 4 }), got = L.put('A', a2, { rev: '501.1', from: 'sheetwin' });
  ok('A5 a real change is taken and told once', got === a2 && told.length === 2 && told[1].rec === a2);
  L.put('A', copy(a2), { rev: '502.1', from: 'store' });
  ok('A6 the same content under a newer revision: its revision is learned, nobody is told', L.get('A') === a2 && L.rev('A') === '502.1' && told.length === 2, { rev: L.rev('A'), told: told.length });
}

/* ── the Library over a stand-in DOM ── */
const calls = [];   // (every scroll or focus the page asks for)
let hooks = { drawn: null };
class El {
  constructor(tag) { this.tagName = String(tag).toUpperCase(); this.children = []; this.parentElement = null; this.dataset = {}; this.style = {}; this.attrs = {}; this._html = ''; this.textContent = ''; this.h = 0; const c = this._cls = new Set();
    this.classList = { add: (...x) => x.forEach(k => c.add(k)), remove: (...x) => x.forEach(k => c.delete(k)), contains: k => c.has(k), toggle: (k, on) => { if (on === undefined ? !c.has(k) : on) c.add(k); else c.delete(k); } }; }
  get className() { return [...this._cls].join(' '); } set className(v) { this._cls.clear(); String(v).split(/\s+/).filter(Boolean).forEach(k => this._cls.add(k)); }
  get isConnected() { return true; }
  get innerHTML() { return this._html; } set innerHTML(v) { this._html = String(v); this.htmlWrites = (this.htmlWrites || 0) + 1; }
  get src() { return this.attrs.src || ''; } set src(v) { this.attrs.src = String(v); this.srcWrites = (this.srcWrites || 0) + 1; }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; } setAttribute(k, v) { this.attrs[k] = String(v); } removeAttribute(k) { delete this.attrs[k]; } hasAttribute(k) { return k in this.attrs; }
  appendChild(n) { if (n.parentElement) n.parentElement.children.splice(n.parentElement.children.indexOf(n), 1); this.children.push(n); n.parentElement = this; return n; }
  insertBefore(n, ref) { if (!ref) return this.appendChild(n); if (n.parentElement) n.parentElement.children.splice(n.parentElement.children.indexOf(n), 1); this.children.splice(this.children.indexOf(ref), 0, n); n.parentElement = this; return n; }
  remove() { if (this.parentElement) { this.parentElement.children.splice(this.parentElement.children.indexOf(this), 1); this.parentElement = null; } this.removed = true; }
  replaceWith(n) { const p = this.parentElement; if (!p) return; p.children.splice(p.children.indexOf(this), 1, n); n.parentElement = p; this.parentElement = null; }
  querySelector() { return null; } querySelectorAll() { return []; } closest() { return null; }
  addEventListener() {} removeEventListener() {} dispatchEvent() { return true; }
  scrollIntoView(o) { calls.push(['scrollIntoView', o]); } scrollTo(...a) { calls.push(['scrollTo', a]); } scrollBy(...a) { calls.push(['scrollBy', a]); } focus(o) { calls.push(['focus', o]); }
}
function library() {
  calls.length = 0; hooks = { drawn: null };
  const stage = new El('div'), body = new El('main'); stage.id = 'stage'; body.id = 'libBody'; stage.appendChild(body);
  let top = 0; const writes = []; Object.defineProperty(stage, 'scrollTop', { get: () => top, set: v => { writes.push(+v); top = +v; } });
  stage.getBoundingClientRect = () => ({ top: 0, bottom: 800, left: 0, right: 1200, width: 1200, height: 800 });
  const cards = [], imgs = [];
  // (the cards stand one under the other in the scroller: each card's place on screen is where the cards above it end, less what is scrolled)
  const rectOf = card => { let y = 0; for (const c of cards) { if (c === card) break; y += c.h; } const t = y - top; return { top: t, bottom: t + card.h, left: 0, right: 300, width: 300, height: card.h }; };
  const card = (id, h) => {
    const c = new El('div'); c.className = 'setCard'; c.dataset.laserCard = 'set'; c._laserSheets = [id]; c._laserSet = { setId: 'T-' + id, seq: 1, day: '2026-11-28', sheetIds: [id], processReady: false }; c.h = h;
    c.getBoundingClientRect = () => rectOf(c); body.appendChild(c); cards.push(c);
    const img = new El('img'); img.dataset.sheetPreview = id; img.attrs.src = 'https://x.test/' + id + '.png?v=' + (T0 + 1); img.srcWrites = 0; imgs.push(img); c.img = img;
    return c;
  };
  const doc = { hidden: false, readyState: 'complete', activeElement: null, head: new El('head'), body: new El('body'), documentElement: new El('html'),
    hasFocus: () => true, addEventListener() {}, removeEventListener() {}, createElement: t => new El(t),
    getElementById: id => (id === 'stage' ? stage : id === 'libBody' ? body : null),
    querySelector: () => null,
    querySelectorAll: sel => (/img\[data-sheet-preview\]/.test(sel) ? imgs.slice() : /\[data-laser-card\]/.test(sel) && !/data-laser-area/.test(sel) ? cards.filter(c => c.parentElement) : []) };
  const frames = [], timers = [];
  const win = { document: doc, console: { log() {}, warn() {}, error() {} }, innerHeight: 800, innerWidth: 1200, performance: { now: () => Date.now() }, JSON, Math, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, Boolean, RegExp, Error, Symbol, Intl,
    requestAnimationFrame: f => (frames.push(f), frames.length), cancelAnimationFrame() {}, setTimeout: (f, ms) => (timers.push({ f, ms }), timers.length), clearTimeout() {}, setInterval: () => 0, clearInterval() {},
    addEventListener() {}, removeEventListener() {}, CustomEvent: class { constructor(t, o) { this.type = t; Object.assign(this, o); } },
    esc: x => String(x == null ? '' : x), cors: x => x, S: { mode: 'library', cloud: { ok: false }, library: { rows: [] } }, allSheets: () => [], Orders: { rows: () => [] }, Engrave: { items: () => new Map() },
    O: { setLabel: n => 'Set-' + n }, CNListActivity: { compareDays: () => 0, state: () => ({ direction: 'desc' }), isSet: () => true, setDay: () => '2026-11-28' }, api: async () => ({}),
    picUrl: r => r.preview + '?v=' + r.previewAt, CN_SHEETLIVE: { library: true, sheetwin: true, orderwin: true, nest: true },
    LibraryDone: { isDone: r => !!(r && r.laserDoneAt), refreshCards: () => { if (hooks.drawn) hooks.drawn(); }, addedSeals() {}, snapshot: () => null, glideFrom() {} } };
  win.window = win; win.self = win;
  const ctx = vm.createContext(win);
  vm.runInContext(read('charm-nest-readiness.js'), ctx);
  win.CNSheetLive = SLmod.create({ api: null, newerOf, switches: () => win.CN_SHEETLIVE });
  const src = read('charm-nest-bridge.js');
  vm.runInContext(src.slice(src.indexOf('const LaserReview ='), src.indexOf('const Sets =')), ctx);
  const runFrames = () => { for (let i = 0; i < 6 && frames.length; i++) frames.splice(0).forEach(f => f()); };
  return { win, LR: win.LaserReview, SL: win.CNSheetLive, stage, body, cards, imgs, card, frames, runFrames, writes, scrollTop: () => top, setTop: v => { top = v; } };
}

/* ── B, C · the Library draws only what changed, in place ── */
{
  const P = library();
  const A = P.card('A', 400), B = P.card('B', 400), C = P.card('C', 400);
  const recs = { A: row('A', 1), B: row('B', 1), C: row('C', 1) };
  for (const k of 'ABC') P.LR.record(recs[k], '600.' + k.charCodeAt(0));
  P.win.S.library.rows = 'ABC'.split('').map(k => Object.assign({}, recs[k]));
  P.runFrames(); P.frames.length = 0;
  P.setTop(500);   // (the person has scrolled: B's top is 100 px above the view, C is on screen)
  const kids = P.body.children.slice(), htmlBefore = P.cards.map(c => c.htmlWrites || 0);
  const mark = () => P.cards.forEach(c => { c._lrFresh = true; });
  mark();

  // B1 · the store's echo of B (a list read elsewhere brought the very record B already shows)
  P.SL.put('B', copy(P.SL.get('B')), { rev: '600.66', from: 'store' });
  ok('B1 a store echo of a sheet draws nothing again (no drawing is even asked for)', P.frames.length === 0, { frames: P.frames.length, echo: P.SL.state().echo });
  P.runFrames();
  // B2 · a newer record of B that changes nothing its card shows: its write time and the nest job's beat
  const quiet = Object.assign({}, P.SL.get('B'), { updatedAt: T0 + 90000, nestJob: { id: 'j1', status: 'running', at: T0 + 90000 } });
  P.SL.put('B', quiet, { rev: '601.1', from: 'sheetwin' });
  ok('B2 a newer record that changes nothing the card shows draws nothing again', P.frames.length === 0, { frames: P.frames.length });
  P.runFrames();
  ok('B3 ... and the Library still holds that newer record (what it compares with next)', P.LR.recOf('B') === quiet);
  ok('B4 no card was drawn in B1-B3', P.cards.every(c => c._lrFresh === true), P.cards.map(c => c._lrFresh));

  // C · a real change to B: its counts and its picture
  const b2 = Object.assign({}, quiet, { placedCount: 4, charmCount: 4, density: 0.5, updatedAt: T0 + 120000, layoutRev: 2, contentRev: 2, preview: 'https://x.test/B2.png', previewAt: T0 + 120000 });
  P.SL.put('B', b2, { rev: '602.1', from: 'sheetwin' });
  ok('C1 a real change to one sheet asks for a drawing', P.frames.length > 0);
  P.runFrames();
  ok('C2 only that sheet\'s card is drawn: A and C are not drawn again', B._lrFresh === false && A._lrFresh === true && C._lrFresh === true, P.cards.map(c => c._lrFresh));
  ok('C3 every card is the very same node, in the same place (nothing removed, nothing replaced)', P.body.children.length === 3 && P.body.children.every((c, i) => c === kids[i]) && P.cards.every(c => !c.removed));
  ok('C4 the other cards\' nodes were not written at all', (A.htmlWrites || 0) === htmlBefore[0] && (C.htmlWrites || 0) === htmlBefore[2], { A: A.htmlWrites, C: C.htmlWrites });
  ok('C5 the changed card\'s picture is the same <img>, showing the new picture; the others\' pictures were not touched', B.img.src === 'https://x.test/B2.png?v=' + (T0 + 120000) && B.img.srcWrites === 1 && A.img.srcWrites === 0 && C.img.srcWrites === 0, { b: B.img.src, w: P.imgs.map(i => i.srcWrites) });
  ok('C6 no scrollIntoView / scrollTo / scrollBy / focus was called', calls.length === 0, calls);
  ok('C7 the scroller was not moved (scrollTop 500, never written)', P.scrollTop() === 500 && P.writes.length === 0, { top: P.scrollTop(), writes: P.writes });
}

/* ── D · a card above the view grows while the cards are drawn: what the person looks at stays put ── */
{
  const P = library();
  const A = P.card('A', 400), B = P.card('B', 400), C = P.card('C', 400);
  const recs = { A: row('A', 1), B: row('B', 1), C: row('C', 1) };
  for (const k of 'ABC') P.LR.record(recs[k], '700.' + k.charCodeAt(0));
  P.runFrames(); P.frames.length = 0; P.setTop(500);
  const before = B.getBoundingClientRect().top, beforeC = C.getBoundingClientRect().top;
  hooks.drawn = () => { A.h = 520; };   // (the drawing makes A, above the view, 120 px taller: a rail line, "Checking…")
  P.SL.put('C', Object.assign({}, recs.C, { placedCount: 2, updatedAt: T0 + 50000, layoutRev: 2 }), { rev: '701.1', from: 'sheetwin' });
  P.runFrames();
  ok('D1 the card at the top of the view stands exactly where it stood on screen', B.getBoundingClientRect().top === before && C.getBoundingClientRect().top === beforeC, { before, after: B.getBoundingClientRect().top });
  ok('D2 ... held by the Library\'s own scroller by exactly what grew above it, never by a scroll call', P.scrollTop() === 620 && calls.length === 0, { top: P.scrollTop(), calls });
  hooks.drawn = null; P.writes.length = 0;
  P.SL.put('C', Object.assign({}, P.SL.get('C'), { placedCount: 1, updatedAt: T0 + 60000, layoutRev: 3 }), { rev: '702.1', from: 'sheetwin' });
  P.runFrames();
  ok('D3 a drawing that changes no height writes no scroll at all', P.writes.length === 0 && P.scrollTop() === 620, P.writes);
  P.setTop(0); hooks.drawn = () => { A.h = 600; };
  P.SL.put('C', Object.assign({}, P.SL.get('C'), { placedCount: 3, updatedAt: T0 + 70000, layoutRev: 4 }), { rev: '703.1', from: 'sheetwin' });
  P.runFrames();
  ok('D4 at the very top of the list nothing is held or scrolled', P.scrollTop() === 0 && calls.length === 0);
}

/* ── E · the Sets view keeps its cards (code text: the drawing needs the whole page) ── */
{
  const br = read('charm-nest-bridge.js'), a = br.indexOf('  async function renderLibrary(body, opts) {'), b = br.indexOf('  function heldLibrary() {', a), fn = a >= 0 && b > a ? br.slice(a, b) : '';
  ok('E1 the Sets drawing no longer empties the Library on every list read (sections only when there are none)', !!fn && /if \(!inPlace\) LaserReview\.sections\(body\)/.test(fn) && !/\n\s*LaserReview\.sections\(body\);/.test(fn));
  ok('E2 a set whose record and drawing are what its card shows keeps that very card', /had\._cardSig === sig && had\._builtHtml === html/.test(fn) && /placeAt\(had, ready\)/.test(fn));
  ok('E3 a set that changed has only its own card drawn again, where it stood', /had\.replaceWith\(card\)/.test(fn));
  ok('E4 the Sets drawing holds the view (keepView), and paints only the cards drawn anew', /LaserReview\.keepView/.test(fn) && /LaserReview\.changed\(new Set\(fresh\.concat\(back\)/.test(fn));
  const lr = br.slice(br.indexOf('const LaserReview ='), br.indexOf('const Sets ='));
  ok('E5 nothing on the Library\'s drawing path scrolls to anything (its only scrollIntoView is the person\'s own press on a \'!\')', (lr.match(/\.scrollIntoView\b/g) || []).length === 1 && /bang\.scrollIntoView/.test(lr) && !/\.scrollTo\(|\.scrollBy\(/.test(lr));
}

console.log(`\nrt1128-noscrolljump: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
