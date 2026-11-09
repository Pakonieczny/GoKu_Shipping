// CNT1 (Paul, 9 Nov 2026: "Numbers in the left hand pull out menu don't match the totals on the right hand main page."): ONE count per step, in orders.
//   1. Every rail row's count equals the number of orders the Orders list shows under that row's filter (every row pressed, a stale filter and a Time range let go).
//   2. The Review and Engraving chips and the top nav's Review and Design badges say the same orders as their rows; the pieces are only in the hover.
//   3. The rail's Completed equals the Completed tab (CNCompleted, shipped / etsyCompleted); an order whose pieces were all completed by hand is not Completed: it is open, at Assembled.
//   4. Open Orders (the list, the Orders tab's badge) excludes completed and cancelled orders, and the open steps add up to it.
// The real module code (charm-nest-bridge.js's Orders and Ladder, charm-nest-flow-stages.js, charm-nest-completed.js) runs in a vm over a made-up fixture. No network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const slice = (from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
const base = () => ({ JSON, Map, Set, WeakMap, Promise, Math, Object, Array, String, Number, Error, Intl, Date, RegExp, console, setTimeout, clearTimeout });
const store = () => { const m = new Map(); return { getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }; };
const el = () => ({ textContent: '', title: '', value: '', animate() {} });

/* ── the fixture: an order at every step, two shipped, one cancelled, one gone ── */
const line = (rid, key, more) => Object.assign({ key, order: { receiptId: rid }, line: { transactionId: key }, state: 'pooled', spec: {}, material: 'gold' }, more || {});
const on = cut => ({ state: 'sheet', onSheet: true, sheets: [{ id: cut ? 'CUT' : 'OPEN', cut }] });
const places = {
  o1: { state: 'waiting', sheets: [] },                         // ORDERIN: waiting for a sheet
  r1: on(false),                                                // REVIEW1: on a sheet, but the Review tab's Open list has it
  r2: { state: 'hold', sheets: [] },                            // HELD: on hold
  d1: on(false),                                                // ENGRAVE: on a sheet, engraving still to settle (the Design & Engrave tab has it)
  s1: on(false), s2: on(true),                                  // SHEET: one piece cut, one only on a sheet
  c1: on(true),                                                 // CUT
  so: on(true), we: on(true), as: on(true),                     // SORTED, WELDED, ASSEMBLED (learned)
  h1: { state: 'hand' }, h2: { state: 'hand' },                 // HANDONLY: every piece completed by hand, not shipped
  p1: on(true),                                                 // PACKED: its shipping label printed, not marked shipped
  sh: on(true), ec: on(true), x1: { state: 'waiting', sheets: [] }
};
const rows = [
  line('100', 'o1'), line('101', 'r1'), line('102', 'r2', { hold: 'customer asked' }), line('103', 'd1', { engrave: { needed: true, approved: false } }),
  line('104', 's1'), line('104', 's2'), line('105', 'c1'), line('106', 'so'), line('107', 'we'), line('108', 'as'),
  line('109', 'h1'), line('109', 'h2', { material: 'silver' }), line('110', 'p1'),
  line('200', 'sh', { order: { receiptId: '200', isShipped: true, shippedAt: 1762600000 } }),                  // SHIPPED (Etsy says so)
  line('201', 'ec', { order: { receiptId: '201', status: 'Completed', shippedAt: 1762500000 } }),              // COMPLETED on Etsy
  line('300', 'x1'),                                                                                          // CANCELLED
  line('400', 'g1', { state: 'gone' })                                                                        // GONE
];
const learned = { 106: 'sorted', 107: 'welded', 108: 'assembled', 110: 'labelPrinted' };
const expect = { order: ['100'], review: ['101', '102'], design: ['103'], sheet: ['104'], cut: ['105'], sorted: ['106'], welded: ['107'], assembled: ['108', '109'], packed: ['110'] };

function world() {
  const localStorage = store();
  const doc = { getElementById: id => elems[id] || null, addEventListener() {}, dispatchEvent() {}, createElement: () => el() };
  const elems = { tabOrdersN: el(), tabReviewN: el(), tabEngraveN: el() };
  const win = { localStorage, document: doc, PiecePlacement: { ofRow: r => places[r.key] || null },
    Cancelled: { has: rid => String(rid) === '300', count: () => 1 },
    Review: { openOrders: () => new Map([['101', {}], ['102', {}], ['300', {}]]), count: () => 1, openCount: () => 3 },   // (a cancelled order on Review's list is no open order)
    Engrave: { pendingOrderIds: () => new Set(['103', '200']), pendingRows: () => 343, pendingCount: () => 0, reviewedRows: () => 0 } };
  // the flow and the Completed pile: the real files
  const fctx = Object.assign({ self: win }, base()); vm.createContext(fctx);
  vm.runInContext(fs.readFileSync('charm-nest-flow-stages.js', 'utf8'), fctx);
  vm.runInContext(fs.readFileSync('charm-nest-completed.js', 'utf8'), fctx);
  for (const [rid, type] of Object.entries(learned)) win.CNFlow.learnEvents(rid, [{ type, station: 'shipping', at: 5 }]);
  // the Orders tab: the real module
  const OV_ACT = { range: 'week', direction: 'desc' };
  const ctx = { window: win, document: doc, B: { orders: { rows, byKey: new Map() }, maps: {}, review: { items: [] }, pool: { rows: new Map() } }, S: { cloud: { ok: true }, settings: {}, mode: 'orders' }, O,
    WORKSPACE_SANDBOX: false, allSheets: () => [], replaceCutWhy: () => '', agent() {},
    CNListActivity: { state: () => OV_ACT, matches: (r, range) => range === 'all' || String(r.order.receiptId) === '100', compare: () => 0, key: () => '', set: (k, v) => Object.assign(OV_ACT, v) }, ...base() };
  win.Review.syncOrderItems = () => {};
  for (const k of ['CNFlow', 'CNCompleted', 'Cancelled', 'Review', 'Engrave', 'PiecePlacement']) ctx[k] = win[k];   // (on the page, window is the global)
  vm.createContext(ctx);
  vm.runInContext(slice('const Orders = window.Orders = (() => {', 'const PageVersion = (() => {'), ctx);
  const Orders = win.Orders; Orders.render = () => {};
  win.CNCompleted.note(rows);   // (what the Orders head does first: the rows' own shipped facts learned, nothing read)
  return { win, ctx, Orders, elems, OV_ACT };
}

const ridsOf = list => [...new Set(list.map(r => String(r.order.receiptId)))].sort();

/* ── 1 · each row's count = the orders under its filter ── */
function rowsTest() {
  const { win, Orders, OV_ACT } = world();
  const f = Orders.flowNow();
  assert.deepEqual(Object.fromEntries(Object.entries(expect).map(([k, v]) => [k, f.counts[k]])), Object.fromEntries(Object.entries(expect).map(([k, v]) => [k, v.length])));
  for (const s of win.CNFlow.STAGES) {
    if (s.id === 'completed') continue;
    Orders.view().metal = 'silver'; Orders.view().act = 'hold'; Orders.view().q = 'zzz'; OV_ACT.range = 'week';   // (a stale narrowing: the press lets it go)
    Orders.showStage(s.id);
    const shown = ridsOf(Orders.visibleRows());
    assert.equal(shown.length, f.counts[s.id], `${s.label}: the row says ${f.counts[s.id]}, the list under it shows ${shown.length}`);
    assert.deepEqual(shown, (expect[s.id] || []).slice().sort(), s.label);
  }
  // Completed: the Completed pile, its every order
  OV_ACT.range = 'week'; Orders.showStage('completed');
  assert.equal(Orders.view().pile, 'completed'); assert.equal(OV_ACT.range, 'all', 'the Time range lets go');
  const listed = [...win.CNCompleted.select({ range: OV_ACT.range }).map(e => String(e.rid))].sort();
  assert.deepEqual(listed, ['200', '201']);
  assert.equal(f.counts.completed, listed.length, 'the rail\'s Completed = the orders the Completed tab lists');
  assert.equal(f.counts.completed, win.CNCompleted.count(), 'the rail\'s Completed = the Completed tab\'s count');
  assert.equal(f.byRid.get('109'), 'assembled', 'every piece completed by hand, not shipped: open, at Assembled (not Completed)');
}

/* ── 2 · Open Orders ── */
function openTest() {
  const { win, Orders, elems, OV_ACT } = world();
  OV_ACT.range = 'all'; Orders.showPile(null);
  const open = ridsOf(Orders.visibleRows());
  for (const rid of ['200', '201', '300', '400']) assert(!open.includes(rid), `${rid} is not an open order`);
  const f = Orders.flowNow(), steps = win.CNFlow.STAGES.filter(s => s.id !== 'completed').reduce((n, s) => n + f.counts[s.id], 0);
  assert.equal(open.length, 14 - 0 - 3, 'Open Orders: the 11 open orders');
  assert.equal(steps, open.length, 'the open steps add up to Open Orders');
  Orders.tabCount(); assert.equal(elems.tabOrdersN.textContent, String(open.length), 'the Orders tab\'s badge says Open Orders');
  assert(rows.filter(r => !['200', '201', '300', '400'].includes(r.order.receiptId)).every(Orders.openRow) && !rows.filter(r => ['200', '201', '300', '400'].includes(r.order.receiptId)).some(Orders.openRow));
}

/* ── 3 · the rail: its rows, chips and the nav badges ── */
function railTest() {
  const { win, Orders, elems } = world();
  const f = Orders.flowNow();
  const buttons = html => [...html.matchAll(/<button[^>]*data-stage-go="(\w+)"/g)].map(m => ({ dataset: { stageGo: m[1] }, onclick: null }));
  const host = { html: '', go: [], set innerHTML(v) { this.html = v; this.go = buttons(v); }, querySelectorAll(sel) { return sel === '[data-stage-go]' ? this.go : []; }, querySelector: () => null };
  const doc = { getElementById: id => (id === 'ladder' ? host : elems[id] || null), addEventListener() {} };
  const Engrave = win.Engrave, Review = Object.assign({}, win.Review);
  const ctx = { window: Object.assign({}, win, { OrderTimelineUI: { laneIcon: () => '' } }), document: doc, requestAnimationFrame: () => {},
    B: { run: { runId: 'r1', setId: 'set-2026-11-08-1', status: 'processed', step: 'commit', committed: [] }, master: { entries: new Map() }, orders: { rows } }, S: { settings: {}, mode: 'orders' }, O,
    Review, Engrave, Orders, Master: { missingCount: () => 0 }, Sets: {}, allSheets: () => [], setMode() {}, fmtT: String, lineTotal: () => 1,
    esc: v => String(v).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])), ...base() };
  vm.createContext(ctx);
  vm.runInContext(slice('const CNFrame = window.CNFrame', '/* ═══ 17 · DesignLink') + slice('const LiveStrip = window.LiveStrip = (() => {', 'const DesignLink = window.DesignLink'), ctx);
  ctx.window.LiveStrip.now();   // (the nav badges and the rail, as every frame draws them)
  const h = host.html;
  const counts = Object.fromEntries([...h.matchAll(/data-stage="(\w+)"[\s\S]*?<span class="c">([^<]*)<\/span>/g)].map(m => [m[1], m[2]]));
  for (const s of win.CNFlow.STAGES) {
    const want = f.counts[s.id] ? String(f.counts[s.id]) : ['sorted', 'welded', 'assembled', 'packed'].includes(s.id) ? '—' : '0';
    assert.equal(counts[s.id], want, `${s.label} on the rail`);
  }
  const rev = +h.match(/Review<b>(\d+)<\/b>/)[1], eng = +h.match(/Engraving<b>(\d+)<\/b>/)[1];
  assert.equal(rev, f.counts.review, 'the Review chip = In review / On hold'); assert.equal(eng, f.counts.design, 'the Engraving chip = Design & engrave');
  assert.equal(elems.tabReviewN.textContent, String(rev), 'the nav Review badge = the Review chip');
  assert.equal(elems.tabEngraveN.textContent, String(eng), 'the nav Design badge = the Engraving chip');
  assert.match(elems.tabEngraveN.title, /^1 order waiting on Design & engrave · 343 pieces to settle/, 'the pieces are said, as pieces, in the hover');
  assert.match(h, /title="1 order waiting on Design &amp; engrave · 343 pieces[^"]*">Engraving<b>1<\/b>/);
  // the Completed row = the Completed tab
  assert.equal(counts.completed, String(win.CNCompleted.count()));
  // a station step nobody was seen at says "—" and says why
  win.CNFlow.STAGES.forEach(() => {}); assert.match(h, /data-stage="welded"[\s\S]*?title="Welded — [^"]*Learned from the order timelines/);
}

rowsTest(); openTest(); railTest();
console.log('rail-counts: ok');
