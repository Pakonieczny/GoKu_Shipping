// An update costs what changed, not what the day has held (Paul, 24 Sep: everything stays on, nothing piles up).
//   1. Pool.charmOf / sheetOf / onSheets / holding answer from one index: nothing is walked again while no page, charm
//      list or placement list changed, and every answer is the old walk's through pieces placed, added, moved, removed.
//   2. Orders.interpretAll reads again only the lines whose inputs changed: an override, the option maps, the library.
//   3. Orders.unclaim sends a hundred orders per message.
//   4. The strip, the ladder and the Orders tab are drawn once a frame, however often they are asked for.
// The real module code runs in a vm; the page and the station are small stand-ins. No network.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const O = require('../../charm-nest-orders.js');
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const slice = (from, to) => { const a = source.indexOf(from), b = source.indexOf(to, a); assert(a >= 0 && b > a, 'slice ' + from); return source.slice(a, b); };
// (a build without the frame queue draws at once, which is what part 4 catches, rather than failing to slice)
const frameSrc = source.includes('const CNFrame = window.CNFrame') ? slice('const CNFrame = window.CNFrame', '/* ═══ 17 · DesignLink') : '';
const ordersSrc = slice('const Orders = window.Orders = (() => {', '/* ═══ 19 · Master');
const base = () => ({ JSON, Map, Set, WeakMap, Promise, Math, Object, Array, String, Number, Error, Intl, Date, console });
const frames = [];
const frame = () => { for (const f of frames.splice(0)) f(16); };

/* ── 1 · the pool's index ── */
function poolTest() {
  let reads = 0;
  // a sheet's lists count every element read, so a lookup that walks the sheets shows
  const counted = list => new Proxy(list, { get(t, k, r) { if (typeof k === 'string' && /^\d+$/.test(k)) reads++; return Reflect.get(t, k, r); } });
  const page = (name, charms, placed) => ({ name, charms: counted(charms.map(([id, poolId]) => ({ id, poolId }))), placements: counted(placed.map(id => ({ id }))) });
  const A = page('A', [['a1', 'p1'], ['a2', 'p2'], ['m1', undefined]], ['a1', 'm1']), Bp = page('B', [['b3', 'p3']], ['b3']);   // m1: a piece of no line
  const pages = [A, Bp];
  const ctx = { window: {}, allSheets: () => pages.slice(), ...base() };
  vm.createContext(ctx);
  vm.runInContext(slice('const Pool = window.Pool = (() => {', '/* Carry-forward'), ctx);
  const Pool = ctx.window.Pool;
  // the walks the index replaced, as they were: every answer must be theirs
  const walk = {
    charmOf: id => pages.flatMap(sh => [...sh.charms]).find(c => c.poolId === id) || null,
    sheetOf: id => pages.find(sh => sh.placements.some(p => { const c = sh.charms.find(x => x.id === p.id); return c && c.poolId === id; })) || null,
    onSheets: ids => { if (!ids.length) return false; const all = new Set(pages.flatMap(sh => sh.charms.map(c => c.poolId)).filter(Boolean)); return ids.every(id => all.has(id)); },
  };
  const same = why => {
    for (const id of ['p1', 'p2', 'p3', 'p4', 'nope', undefined]) { assert.equal(Pool.charmOf(id), walk.charmOf(id), `${why}: charmOf ${id}`); assert.equal(Pool.sheetOf(id), walk.sheetOf(id), `${why}: sheetOf ${id}`); }
    for (const ids of [['p1'], ['p1', 'p3'], ['p2', 'p4'], ['p1', undefined], []]) assert.equal(Pool.onSheets({ poolIds: ids }), walk.onSheets(ids), `${why}: onSheets ${ids}`);
  };
  const names = set => [...set].map(sh => sh.name).sort();
  same('at the start');
  assert.equal(Pool.sheetOf('p1'), A); assert.equal(Pool.sheetOf('p2'), null, 'a piece on the card but not placed is on no sheet yet');

  // nothing changed: asked again and again, no sheet is read again (the walks read every charm of every sheet each time)
  reads = 0;
  for (let i = 0; i < 100; i++) { Pool.charmOf('p' + (i % 5)); Pool.sheetOf('p' + (i % 5)); Pool.onSheets({ poolIds: ['p1', 'p3'] }); }
  assert.equal(reads, 0, 'a lookup with nothing changed reads no sheet again');

  // placed: a piece put on its sheet (the same list, longer)
  A.placements.push({ id: 'a2' }); same('placed'); assert.equal(Pool.sheetOf('p2'), A);
  // added: a page with a piece of its own and a second copy of one already on A
  const Cp = page('C', [['c4', 'p4'], ['c5', 'p2']], ['c4', 'c5']); pages.push(Cp); same('a page added');
  assert.equal(Pool.sheetOf('p4'), Cp); assert.equal(Pool.charmOf('p2').id, 'a2', 'the first page holding a piece answers, as the walk did');
  assert.deepEqual(names(Pool.holding(['p4', 'p2'])), ['A', 'C']);
  // moved: taken off one sheet (a new list) and put on another
  Bp.charms = counted([]); Bp.placements = counted([]); A.charms.push({ id: 'a3', poolId: 'p3' }); A.placements.push({ id: 'a3' }); same('moved');
  assert.equal(Pool.sheetOf('p3'), A); assert.deepEqual(names(Pool.holding(['p3'])), ['A']);
  // removed: taken out of its lists
  A.charms = counted([...A.charms].filter(c => c.id !== 'a1')); A.placements = counted([...A.placements].filter(p => p.id !== 'a1')); same('removed');
  assert.equal(Pool.charmOf('p1'), null); assert.equal(Pool.onSheets({ poolIds: ['p1'] }), false);
  // a placement taken off while the piece stays on the card: the next page with it placed answers
  A.placements = counted([...A.placements].filter(p => p.id !== 'a2')); same('unplaced'); assert.equal(Pool.sheetOf('p2'), Cp);
  // a page taken away with its pieces
  pages.splice(pages.indexOf(Cp), 1); same('a page removed');
  assert.equal(Pool.charmOf('p4'), null); assert.deepEqual(names(Pool.holding(['p4', 'p2'])), ['A'], 'a piece on the card but not placed is still held by its page');
}

/* ── 2 · lines read again only when what they are read from changed ── */
async function interpretTest() {
  let reads = 0;
  const Ocount = Object.assign({}, O, { interpretLine: (...a) => { reads++; return O.interpretLine(...a); } });
  const entries = new Map([['OK', { sku: 'OK' }], ['SIZED', { sku: 'SIZED', sizes: { S: { aiPath: 's' }, M: { aiPath: 'm' } } }], ['DONE', { sku: 'DONE' }]]);
  let served = { maps: {}, aliases: {}, list: { patterns: [], skus: [], rows: [] } };
  const mk = (id, state, sku, metalKey, variations = []) => ({ key: id + '_t' + id, state, order: { receiptId: id, updateTs: 1, staffNote: '' }, line: { transactionId: 't' + id, listingId: 'L' + id, sku, metalKey, title: sku, quantity: 1, variations }, spec: null, problems: [], poolIds: [] });
  const r1 = mk('1', 'pulled', 'OK', 'silver', [{ name: 'Style', value: 'Dainty' }]), r2 = mk('2', 'pooled', 'SIZED', ''), r3 = mk('3', 'committed', 'DONE', 'gold'), r4 = mk('4', 'gone', 'OK', 'gold');
  const ctx = { window: {}, B: { orders: { rows: [r1, r2, r3, r4], byKey: new Map() }, maps: { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [], rows: [] }, loadedAt: 0 } },
    S: { cloud: { ok: true }, settings: {} }, O: Ocount, Master: { entryFor: sku => entries.get(sku) || null }, Review: { syncOrderItems() {} },
    api: async (fn, body) => body.op === 'optionMapGet' ? { maps: served.maps } : body.op === 'aliasGet' ? { aliases: served.aliases } : { list: served.list },
    document: { getElementById: () => null }, agent() {}, ...base() };
  vm.createContext(ctx);
  vm.runInContext(ordersSrc, ctx);
  const Orders = ctx.window.Orders;
  const pass = () => { reads = 0; Orders.interpretAll(); return reads; };
  // what the old pass made of a line, read from scratch: a line not read again must come out the same
  const fresh = row => { const r = { ...row }; r.spec = O.interpretLine(r.order, r.line, Orders.ctx()); r.problems = r.spec.problems.slice(); if (r.spec.noDesign) r.state = r.state === 'pulled' ? 'noDesign' : r.state; if (r.materialOverride) { r.spec.material = r.materialOverride; r.problems = r.problems.filter(p => p.kind !== 'needsMaterial'); } if (r.sizeOverride) { r.spec.size = r.sizeOverride; r.problems = r.problems.filter(p => p.kind !== 'missingSize'); } r.material = r.spec.material; return r; };
  const shape = r => JSON.stringify([r.spec, r.problems, r.material, r.state]);
  const asFresh = why => { for (const r of [r1, r2]) assert.equal(shape(r), shape(fresh(r)), `${why}: line ${r.key} reads as it would from scratch`); };
  const kinds = r => r.problems.map(p => p.kind).sort();

  assert.equal(pass(), 3, 'the first pass reads every line but the gone one');
  assert.deepEqual(kinds(r1), ['needsMapping']); assert.deepEqual(kinds(r2), ['missingSize', 'needsMaterial']); assert.equal(r4.spec, null);
  assert.equal(pass() + pass(), 0, 'an update with nothing changed reads no line again'); asFresh('unchanged');

  // a person's decision: only that line is read again
  r2.materialOverride = 'gold'; assert.equal(pass(), 1); assert.equal(r2.spec.material, 'gold'); assert.deepEqual(kinds(r2), ['missingSize']);
  r2.sizeOverride = 'M'; assert.equal(pass(), 1); assert.deepEqual(kinds(r2), []); asFresh('overrides');
  assert.equal(pass(), 0, 'and once read, it is left alone again');

  // the option maps: read again unchanged, the lines are left alone; a mapping learned reads the open lines again
  await Orders.loadMaps(true); const maps = ctx.B.maps.optionMaps; assert.equal(pass(), 2, 'maps read for the first time (new objects)');
  await Orders.loadMaps(true); assert.equal(ctx.B.maps.optionMaps, maps, 'the same maps are kept as they were'); assert.equal(pass(), 0, 'maps read again unchanged leave the lines alone');
  served = { ...served, maps: { L1: { Style: { Dainty: { field: 'form', value: 'necklace' } } } } };
  await Orders.loadMaps(true); assert.notEqual(ctx.B.maps.optionMaps, maps); assert.equal(pass(), 2, 'a mapping learned reads the open lines again, not the committed one');
  assert.deepEqual(kinds(r1), []); assert.equal(r1.spec.form, 'necklace'); asFresh('maps');

  // the library: a size added to one SKU, another SKU withdrawn: only their lines are read again
  entries.set('SIZED', { sku: 'SIZED', sizes: { S: { aiPath: 's' }, M: { aiPath: 'm' }, L: { aiPath: 'l' } } }); assert.equal(pass(), 1);
  entries.set('OK', { sku: 'OK', blocked: 'withdrawn' }); assert.equal(pass(), 1); assert.deepEqual(kinds(r1), ['blockedSku']); asFresh('library');
  entries.set('DONE', { sku: 'DONE', blocked: 'withdrawn' }); assert.equal(pass(), 0, 'a committed line is not read again');

  // the order: a new copy from Etsy, or a staff note written into it
  r1.order = { ...r1.order, updateTs: 2 }; assert.equal(pass(), 1);
  r2.order.staffNote = 'gift wrap'; assert.equal(pass(), 1); assert.equal(r2.spec.staffNote, 'gift wrap'); asFresh('order');
  // a spec put there by something else (a re-validation reads the line itself) is read again at the next pass, as before
  r2.spec = O.interpretLine(r2.order, r2.line, Orders.ctx()); assert.equal(pass(), 1); asFresh('spec replaced');
  assert.equal(r4.spec, null, 'a gone line is never read');
}

/* ── 3 · unclaim, a hundred at a time ── */
async function unclaimTest() {
  const sent = []; let failAt = 0;
  const rows = [];
  const add = (prefix, n) => { const ids = []; for (let i = 0; i < n; i++) { const id = prefix + i; ids.push(id); rows.push({ key: id, order: { receiptId: id }, line: {}, state: 'committed', claimedBy: 'sorter' }); } return ids; };
  const ctx = { window: {}, B: { orders: { rows, byKey: new Map() }, maps: {} }, S: { cloud: { ok: true }, settings: {} }, O, Review: { syncOrderItems() {} },
    DesignLink: { call: async (type, body) => { sent.push(body.receiptIds.length); if (sent.length === failAt) throw new Error('station busy'); return {}; } },
    document: { getElementById: () => null }, agent() {}, ...base() };
  vm.createContext(ctx);
  vm.runInContext(ordersSrc, ctx);
  const Orders = ctx.window.Orders;
  const day = add('a', 250);
  await Orders.unclaim(day);
  assert.deepEqual(sent, [100, 100, 50], 'a long day goes a hundred per message');
  assert(rows.every(r => r.unclaimed && r.claimedBy === null));
  sent.length = 0; await Orders.unclaim(day); assert.deepEqual(sent, [], 'an order let go is not sent again');
  // a message the station does not answer stops the rest: they are let go at the next pass
  const next = add('b', 250); failAt = 2;
  await Orders.unclaim(next);
  assert.deepEqual(sent, [100, 100], 'nothing is sent after a message that failed');
  const of = ids => rows.filter(r => ids.includes(r.key));
  assert(of(next.slice(0, 100)).every(r => r.unclaimed), 'the answered hundred are let go');
  assert(of(next.slice(100)).every(r => !r.unclaimed && r.claimedBy === null), 'the rest wait for the next pass');
  sent.length = 0; failAt = 0; await Orders.unclaim(next);
  assert.deepEqual(sent, [100, 50]); assert(rows.every(r => r.unclaimed));
}

/* ── 4 · drawn once a frame ── */
function frameTest() {
  const drawn = { strip: 0, ladder: 0, tab: 0 };
  const node = name => ({ parentElement: { open: true }, querySelectorAll: () => [], set innerHTML(v) { drawn[name]++; }, set textContent(v) { if (name === 'tab') drawn.tab++; } });
  const els = { railRecentBody: node('strip'), ladder: node('ladder'), tabReviewN: node('review'), tabEngraveN: node('engrave') };
  const ctx = { window: {}, document: { getElementById: id => els[id] || null }, requestAnimationFrame: f => frames.push(f), setTimeout,
    B: { run: null, master: { entries: new Map() }, orders: { rows: [] } }, S: { settings: {} }, O, Review: { count: () => 0 }, Engrave: { pendingCount: () => 0, reviewedCount: () => 0 },
    Orders: { rows: () => [] }, Master: { missingCount: () => 0 }, Sets: {}, allSheets: () => [], setMode() {}, fmtT: t => String(t), esc: v => String(v), ...base() };
  vm.createContext(ctx);
  vm.runInContext(frameSrc + slice('const LiveStrip = window.LiveStrip = (() => {', 'const DesignLink = window.DesignLink'), ctx);
  const { LiveStrip, Ladder } = ctx.window;
  const count = () => ({ ...drawn });

  // a pool pass, log lines and a run step ask many times in one go: one drawing each, at the frame
  LiveStrip.render(); Ladder.render(); LiveStrip.push({ kind: 'POOL', text: 'a line' }); LiveStrip.render(); Ladder.render(); LiveStrip.push({ kind: 'POOL', text: 'another' });
  assert.deepEqual(count(), { strip: 0, ladder: 0, tab: 0 }, 'nothing is drawn before the frame');
  assert.equal(frames.length, 1, 'one frame asked for');
  frame(); assert.deepEqual(count(), { strip: 1, ladder: 1, tab: 0 }, 'the strip and the ladder with it, once');
  frame(); assert.deepEqual(count(), { strip: 1, ladder: 1, tab: 0 }, 'and not again');
  // whichever was asked for first: the strip draws the ladder with itself
  Ladder.render(); LiveStrip.render(); frame(); assert.deepEqual(count(), { strip: 2, ladder: 2, tab: 0 });
  // the banner draws the strip with itself (RunCtl.renderBanner): asked for after the strip, the strip waits behind it
  let banner = 0; const CNFrame = ctx.window.CNFrame;
  LiveStrip.render(); CNFrame.later('banner', () => { banner++; LiveStrip.now(); }); frame();
  assert.equal(banner, 1); assert.deepEqual(count(), { strip: 3, ladder: 3, tab: 0 }, 'the strip once, by the banner');
  // a banner with nothing to draw it on still leaves the strip to be drawn
  LiveStrip.render(); CNFrame.later('banner', () => { banner++; }); frame(); assert.deepEqual(count(), { strip: 4, ladder: 4, tab: 0 });
  // drawn at once (the banner's own call) takes the waiting drawing with it
  LiveStrip.render(); LiveStrip.now(); frame(); assert.deepEqual(count(), { strip: 5, ladder: 5, tab: 0 });
  // no frames to be had: a timer stands in
  delete ctx.requestAnimationFrame; LiveStrip.render(); LiveStrip.render();
  assert.equal(drawn.strip, 5);
  return new Promise(res => setTimeout(res, 40)).then(() => {
    assert.equal(drawn.strip, 6, 'drawn once by the timer');

    // the Orders tab: asked for at every arrival, run step and pool pass; drawn once, and only its count while hidden
    let hidden = true;
    const view = { classList: { contains: c => c === 'hidden' && hidden }, dataset: {} }, tab = node('tab');
    const octx = { window: {}, document: { getElementById: id => id === 'ordersView' ? view : id === 'tabOrdersN' ? tab : null }, requestAnimationFrame: f => frames.push(f),
      B: { orders: { rows: [{ key: 'k', state: 'pooled', order: { receiptId: '1' }, line: { transactionId: 't' }, poolIds: ['p'] }], byKey: new Map() }, maps: {} }, S: { cloud: { ok: true }, settings: {} }, O, ...base() };
    vm.createContext(octx);
    vm.runInContext(frameSrc + ordersSrc, octx);
    const Orders = octx.window.Orders;
    for (let i = 0; i < 5; i++) Orders.render();
    assert.equal(drawn.tab, 0, 'the Orders tab is not drawn before the frame');
    frame(); assert.equal(drawn.tab, 1, 'the Orders tab is drawn once for five asks');
    Orders.render(); Orders.renderNow(); frame(); assert.equal(drawn.tab, 2, 'drawn at once (the tab shown) takes the waiting drawing with it');
  });
}

(async () => {
  const failed = [];
  for (const [name, test] of [['pool index', poolTest], ['interpretAll', interpretTest], ['unclaim', unclaimTest], ['frames', frameTest]]) {
    try { await test(); } catch (e) { failed.push(name); console.error(`${name}:`, e); }
  }
  if (failed.length) { console.error('FAILED: ' + failed.join(', ')); process.exitCode = 1; return; }
  console.log('Uptime churn OK: pool index, lines read again only when changed, unclaim by the hundred, drawings once a frame');
})();
