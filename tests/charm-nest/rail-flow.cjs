// FLW1 (Paul, 9 Nov 2026: "Also, this list does not match the actual process flow so make sure it matches."): the rail lists the production flow, the order Timeline's
// steps, each with the open orders standing at it (the order where its slowest piece is), a press shows them on Orders, and the run is one status line under it.
//   1. The rows and their order are the Timeline's lanes and words.
//   2. Where an order stands: its slowest piece on a sheet → On a sheet; welded then assembled → Assembled; shipped → Shipped; held → In review / On hold.
//   3. The steps after the laser are learned from timeline reads the page already makes (OrderTimeline.get / onRecord), never read for it.
//   4. The Orders list's stage filter: a press on a row narrows Open Orders to the orders at that step; Completed opens the Completed pile.
// (CNT1, 9 Nov 2026: the Shipping lane's open step is "Packed" (packed / label printed, not marked shipped); Completed is the Completed pile, every order of it; an order
//  whose pieces were all completed by hand is an open order at Assembled, not Completed. The full count rules are tests/charm-nest/rail-counts.cjs.)
//   5. The rail draws the rows (lane icons, counts, Review / Engraving beside their steps) and the run's status line, and a press on a row shows the step.
// The real module code runs in a vm; the page is a small stand-in. No network.
const assert = require('node:assert/strict');
const plain = x => (x === undefined ? x : JSON.parse(JSON.stringify(x)));
const deepEqual = assert.deepEqual; assert.deepEqual = (a, b, m) => deepEqual(plain(a), plain(b), m);   // (values made in the vm are another realm's)
const fs = require('node:fs');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const flowSrc = fs.readFileSync('charm-nest-flow-stages.js', 'utf8');
const tlSrc = fs.readFileSync('charm-nest-timeline-ui.js', 'utf8');
const slice = (from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
const base = () => ({ JSON, Map, Set, WeakMap, Promise, Math, Object, Array, String, Number, Error, Intl, Date, RegExp, console, setTimeout, clearTimeout });
const store = () => { const m = new Map(); return { getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), removeItem: k => m.delete(k) }; };

function loadFlow(extra) {
  const root = Object.assign({ localStorage: store() }, extra || {});
  const ctx = Object.assign({ self: root }, base()); vm.createContext(ctx);
  vm.runInContext(flowSrc, ctx);
  return root.CNFlow;
}

/* ── 1 · the rows are the Timeline's ── */
function rowsTest() {
  const F = loadFlow();
  assert.deepEqual(F.STAGES.map(s => s.label), ['Order in', 'In review / On hold', 'Design & engrave', 'On a sheet', 'Laser cut', 'Sorted', 'Welded', 'Assembled', 'Packed', 'Completed']);
  // the lanes, in the order the Timeline draws them (charm-nest-timeline-ui.js LANES): every row is on one of them and the rows walk them in order (Completed is Etsy's own end)
  const lanes = [...tlSrc.match(/const LANES = \[([\s\S]*?)\];/)[1].matchAll(/k: "(\w+)"/g)].map(m => m[1]);
  assert.deepEqual(lanes, ['etsy', 'office', 'sheet', 'sorting', 'welding', 'assembly', 'shipping']);
  const at = F.STAGES.map(s => lanes.indexOf(s.lane));
  assert(at.every(i => i >= 0), 'every row on a Timeline lane');
  assert.deepEqual(at.slice(0, -1), [...at.slice(0, -1)].sort((a, b) => a - b), 'the rows walk the lanes in the Timeline\'s order');
  assert.deepEqual(F.STAGES.map(s => s.lane), ['etsy', 'office', 'office', 'sheet', 'sheet', 'sorting', 'welding', 'assembly', 'shipping', 'etsy']);
  // the Timeline's own stage words are the rows' (STAGE_LABEL)
  const label = Function('return ' + tlSrc.match(/const STAGE_LABEL = (\{[^}]*\});/)[1])();
  for (const [k, id] of [['review', 'review'], ['sheet', 'sheet'], ['sorted', 'sorted'], ['welded', 'welded'], ['assembled', 'assembled'], ['packed', 'packed']]) assert(F.STAGES.find(s => s.id === id).label.includes(label[k]), `${id}: the Timeline's word "${label[k]}"`);
  // the timeline exports the lane icons the rail draws
  assert.match(tlSrc, /const laneIcon = k => iconSvg\(\(LANE\[k\] \|\| LANE\.office\)\.ic\)/); assert.match(tlSrc, /iconOf, laneIcon, LANES,/);
}

/* ── 2 · where an order stands ── */
const line = (rid, key, more) => Object.assign({ key, order: { receiptId: rid }, line: {}, state: 'pooled', spec: {} }, more || {});
function standTest() {
  const F = loadFlow();
  const places = {}, learned = {};
  const c = { place: r => places[r.key] || null, held: r => !!r.hold, review: r => !!r.inReview, nested: () => false, cut: (r, pl) => !!(pl && pl.sheets.every(s => s.cut)), learned: rid => learned[rid] || 0 };
  const on = (cut) => ({ state: 'sheet', onSheet: true, sheets: [{ id: 'S1', cut }] });
  // an order of two pieces: one cut, one only on a sheet → the slowest piece is on a sheet
  places.a1 = on(true); places.a2 = on(false);
  // welded, then assembled (learned from its timeline) → Assembled; its sheet is cut
  places.b1 = on(true); learned.B = F.AT.welded; learned.B = F.AT.assembled;
  // held → In review / On hold, whatever was learned
  places.c1 = on(true); learned.C = F.AT.sorted;
  // waiting with engraving to settle → Design & engrave; a Review card → In review
  places.d1 = { state: 'waiting', sheets: [] };
  places.e1 = { state: 'waiting', sheets: [] };
  // a piece waiting with nothing to decide → Order in; some copies on a sheet and some not → the slow copy is not on one
  places.f1 = { state: 'waiting', sheets: [] }; places.g1 = { state: 'sheet', onSheet: true, partial: true, sheets: [{ id: 'S2' }] };
  // completed by hand: skipped; an order of only those is made, not shipped: Assembled (CNT1); a cancelled one is not counted
  places.h1 = { state: 'hand' }; places.h2 = on(false); places.i1 = { state: 'hand' }; places.j1 = { state: 'cancelled' };
  const rows = [line('A', 'a1'), line('A', 'a2'), line('B', 'b1'), line('C', 'c1', { hold: true }), line('D', 'd1', { engrave: { needed: true, approved: false } }), line('E', 'e1', { inReview: true }),
    line('F', 'f1'), line('G', 'g1'), line('H', 'h1'), line('H', 'h2'), line('I', 'i1'), line('J', 'j1'), line('K', 'k1', { state: 'gone' })];
  const shipped = [{ rid: 'S', type: 'shipped' }, { rid: 'T', type: 'etsyCompleted' }, { rid: 'U', type: 'shipped' }];
  const { counts, byRid } = F.count(rows, c, shipped);
  const at = Object.fromEntries(byRid);
  assert.deepEqual(at, { A: 'sheet', B: 'assembled', C: 'review', D: 'design', E: 'review', F: 'order', G: 'order', H: 'sheet', I: 'assembled' });
  assert.equal(at.A, 'sheet', 'the slowest piece on a sheet: On a sheet'); assert.equal(at.B, 'assembled', 'welded then assembled: Assembled');
  assert.deepEqual(counts, { order: 2, review: 2, design: 1, sheet: 2, cut: 0, sorted: 0, welded: 0, assembled: 2, packed: 0, completed: 3 });
  // every order of the Completed pile (shipped or Etsy completed) counts under Completed
  assert.equal(F.count([], c, [{ rid: 'Z', type: 'shipped' }]).counts.completed, 1);
  // both pieces cut → Laser cut; nothing on the page says where a piece is → its own nesting marker
  places.a2 = on(true); assert.equal(F.count(rows, c, []).byRid.get('A'), 'cut');
  assert.equal(F.orderRank([line('Q', 'q1')], Object.assign({}, c, { place: () => null, nested: () => true }), 'Q'), F.AT.sheet);
}

/* ── 3 · learned from the reads the page already makes ── */
async function learnTest() {
  let reads = 0; const recorders = [];
  const T = { get: async id => { reads++; return { events: id === '77' ? [{ type: 'laserDone', at: 1 }, { type: 'sorted', at: 2 }, { type: 'welded', at: 3 }, { type: 'assembled', at: 4 }, { type: 'labelPrinted', station: 'sorting', at: 5 }] : [] }; }, onRecord: f => recorders.push(f) };
  const F = loadFlow({ OrderTimeline: T });
  assert.equal(F.learned('77'), 0);
  await T.get('77'); await new Promise(r => setTimeout(r, 0));
  assert.equal(reads, 1, 'nothing more is read'); assert.equal(F.learned('77'), F.AT.assembled, 'welded then assembled: Assembled (a Sorting label moves nothing)');
  for (const f of recorders) f({ orderId: '77', type: 'shipped', at: 9 });
  assert.equal(F.learned('77'), F.AT.packed, 'a shipping scan recorded on this page: no further than Packed for an open order (the Completed pile takes it)');
  for (const f of recorders) f({ orderId: '77', type: 'sorted', at: 10 });
  assert.equal(F.learned('77'), F.AT.packed, 'never goes back');
  assert.equal(F.learnEvents('78', [{ type: 'labelPrinted', station: 'shipping', at: 1 }]), true); assert.equal(F.learned('78'), F.AT.packed, 'a shipping label: the Shipping lane');
}

/* ── 4 · the Orders list's stage filter ── */
function ordersTest() {
  const ordersSrc = slice('const Orders = window.Orders = (() => {', 'const PageVersion = (() => {');
  const rows = [line('100', 'x1', { material: 'gold' }), line('100', 'x2', { material: 'gold' }), line('200', 'y1', { material: 'gold' }), line('300', 'z1', { material: 'gold', hold: true })];
  const places = { x1: { state: 'sheet', onSheet: true, sheets: [{ id: 'S1', cut: true }] }, x2: { state: 'sheet', onSheet: true, sheets: [{ id: 'S2' }] }, y1: { state: 'waiting', sheets: [] }, z1: { state: 'hold', sheets: [] } };
  const win = { PiecePlacement: { ofRow: r => places[r.key] || null } };
  const ctx = { window: win, B: { orders: { rows, byKey: new Map() }, maps: {}, review: { items: [] }, pool: { rows: new Map() } }, S: { cloud: { ok: true }, settings: {}, mode: 'orders' }, O, Review: { syncOrderItems() {} },
    CNListActivity: { state: () => ({ range: 'all', direction: 'desc' }), matches: () => true, compare: () => 0, key: () => '', set() {} }, WORKSPACE_SANDBOX: false,
    allSheets: () => [], replaceCutWhy: () => '', document: { getElementById: () => null }, agent() {}, ...base() };
  vm.createContext(ctx);
  vm.runInContext(ordersSrc, ctx);
  win.CNFlow = ctx.CNFlow = loadFlow(); ctx.PiecePlacement = win.PiecePlacement;   // (on the page, window is the global)
  const Orders = win.Orders;
  Orders.render = () => {};   // (the list is not drawn here: visibleRows is what it would draw)
  const shown = () => [...new Set(Orders.visibleRows().map(r => r.order.receiptId))].sort();
  assert.deepEqual(shown(), ['100', '200', '300'], 'Open Orders, nothing pressed');
  assert.deepEqual(Object.fromEntries(Orders.flowNow().byRid), { 100: 'sheet', 200: 'order', 300: 'review' });
  Orders.view().stage = 'sheet'; assert.deepEqual(shown(), ['100'], 'On a sheet: the order whose slowest piece is on a sheet');
  Orders.view().stage = 'review'; assert.deepEqual(shown(), ['300']);
  Orders.view().stage = 'welded'; assert.deepEqual(shown(), [], 'nobody there');
  // Completed is the Completed pile
  Orders.view().stage = 'order';
  assert.equal(typeof Orders.showStage, 'function');
  Orders.showStage('completed'); assert.equal(Orders.view().pile, 'completed'); assert.equal(Orders.view().stage, null, 'the stage lets go in the Completed pile');
  Orders.showStage('order'); assert.equal(Orders.view().pile, null); assert.equal(Orders.view().stage, 'order'); assert.deepEqual(shown(), ['200'], 'Order in');
  Orders.showPile(null); assert.equal(Orders.view().stage, null, 'a pile pressed lets the stage go'); assert.deepEqual(shown(), ['100', '200', '300']);
  // the source: the stage is a chip under the bar, counted on the Filters button and let go by Clear / Clear all / Show everything
  assert.match(ordersSrc, /if \(OV\.stage\) items\.push\(\["stage", stageWord\(OV\.stage\), "Stage"\]\)/);
  assert.match(ordersSrc, /\[OV\.metal, OV\.form, OV\.eng, actNow\(\), OV\.stage\]\.filter\(Boolean\)\.length/);
  assert.equal((ordersSrc.match(/OV\.act = OV\.stage = null/g) || []).length, 3, 'Clear, Clear all and a pile pressed let the stage go');
  assert.match(ordersSrc, /OV\.act = null; OV\.stage = null; OV\.q = ""/, 'Show everything lets it go');
}

/* ── 5 · the rail ── */
function railTest() {
  const frameSrc = slice('const CNFrame = window.CNFrame', '/* ═══ 17 · DesignLink');
  const frames = [];
  const buttons = html => [...html.matchAll(/<button[^>]*data-stage-go="(\w+)"/g)].map(m => ({ dataset: { stageGo: m[1] }, onclick: null }));
  const host = { html: '', go: [], set innerHTML(v) { this.html = v; this.go = buttons(v); }, querySelectorAll(sel) { return sel === '[data-stage-go]' ? this.go : []; }, querySelector: () => null };
  const F = loadFlow();
  const shown = []; let mode = '';
  const flow = { counts: { order: 12, review: 3, design: 5, sheet: 40, cut: 7, sorted: 0, welded: 0, assembled: 2, packed: 9, completed: 120 }, byRid: new Map(), at: Date.now() };
  const OV = { stage: null };
  const ctx = { window: { CNFlow: F, OrderTimelineUI: { laneIcon: k => `<svg data-l="${k}"></svg>` } }, document: { getElementById: id => (id === 'ladder' ? host : null), addEventListener() {} }, requestAnimationFrame: f => frames.push(f),
    B: { run: { runId: 'r1', setId: 'set-2026-11-08-1', status: 'processed', step: 'commit', committed: [] }, master: { entries: new Map() }, orders: { rows: [] } }, S: { settings: {}, mode: 'nest' }, O,
    Review: { count: () => 19 }, Engrave: { pendingCount: () => 0, reviewedRows: () => 0, pendingRows: () => 343 },
    Orders: { rows: () => [], flowPeek: () => flow, flowNow: () => flow, view: () => OV, showStage: id => { shown.push(id); OV.stage = id; } }, Master: { missingCount: () => 0 }, Sets: {}, allSheets: () => [],
    setMode: m => { mode = m; }, fmtT: t => String(t), lineTotal: () => 1, esc: v => String(v).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])), ...base() };
  ctx.window.Orders = ctx.Orders; ctx.window.S = ctx.S;
  vm.createContext(ctx);
  vm.runInContext(frameSrc + slice('const LiveStrip = window.LiveStrip = (() => {', 'const DesignLink = window.DesignLink'), ctx);
  const { Ladder } = ctx.window;
  Ladder.now();
  const h = host.html;
  // the rows, in the flow's order, each with its lane's icon and its count
  const rows = [...h.matchAll(/data-stage="(\w+)"[\s\S]*?<i class="ic" data-lane="(\w+)"><svg data-l="(\w+)"><\/svg><\/i><span class="n">([^<]+)<\/span><span class="c">([^<]*)<\/span>/g)].map(m => [m[1], m[2], m[4], m[5]]);
  assert.deepEqual(rows.map(r => r[0]), F.STAGES.map(s => s.id));
  assert.deepEqual(rows.map(r => r[2]), ['Order in', 'In review / On hold', 'Design &amp; engrave', 'On a sheet', 'Laser cut', 'Sorted', 'Welded', 'Assembled', 'Packed', 'Completed']);
  assert.deepEqual(rows.map(r => r[3]), ['12', '3', '5', '40', '7', '—', '—', '2', '9', '120'], 'counts; a station step with nothing learned says "—"');
  assert(rows.every(r => r[1] === F.STAGES.find(s => s.id === r[0]).lane));
  // the Review and Engraving chips beside their steps say the step's own orders (CNT1), the pieces in the hover
  assert.match(h, /data-stage="review"[^]*?Review<b>3<\/b>[^]*?data-stage="design"[^]*?title="5 orders waiting on Design &amp; engrave · 343 pieces to settle[^"]*">Engraving<b>5<\/b>[^]*?data-stage="sheet"/);
  // the old run steps are gone from the list: the run is one status line under it, its steps in its hover and its details
  assert.doesNotMatch(h, /Processing complete<\/b>/); assert.doesNotMatch(h, /class="ldBand/);
  assert.match(h, /<details class="ldRun[^"]*"><summary title="Processing complete · set-2026-11-08-1 · run r1[^"]*"><i class="g done"><\/i><span>Last run · set-2026-11-08-1 · complete<\/span><\/summary>/);
  assert(h.indexOf('ldRun') > h.indexOf('data-stage="completed"'), 'the status line is at the bottom');
  for (const st of ['Pull', 'Pool', 'Nest', 'Check', 'Engrave', 'Labels', 'Commit']) assert.match(h.slice(h.indexOf('ldRun')), new RegExp(`<span class="n">${st}</span>`), `${st} is still reachable in the details`);
  assert.match(h.slice(h.indexOf('ldRun')), /data-tab="nest"/, 'its buttons still go to their tabs');
  // a running run says so, with its step
  ctx.B.run = { runId: 'r2', setId: 'set-2026-11-09-1', status: 'running', step: 'nest' }; Ladder.now();
  assert.match(host.html, /<span>Run · set-2026-11-09-1 · running — Nest<\/span>/);
  // a press on a row shows that step on Orders
  const sheet = host.go.find(b => b.dataset.stageGo === 'sheet'); sheet.onclick();
  assert.equal(mode, 'orders'); assert.deepEqual(shown, ['sheet']);
  ctx.S.mode = 'orders'; Ladder.now(); assert.match(host.html, /class="ldRow ldF on" role="listitem" data-stage="sheet"/, 'the step shown is marked');
  // no run: still a status line
  ctx.B.run = null; Ladder.now(); assert.match(host.html, /<span>No run open · 0 SKUs<\/span>/); assert.match(host.html, /data-stage="order"/);
}

(async () => {
  rowsTest(); standTest(); await learnTest(); ordersTest(); railTest();
  console.log('rail-flow: ok');
})().catch(e => { console.error(e); process.exit(1); });
