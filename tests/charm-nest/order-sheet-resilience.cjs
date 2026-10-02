// Actual SheetWin read/draw functions, with controlled metadata, source and image completion order.
// No browser driver or persisted orders are involved.
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const source = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8');
const between = (start, end) => {
  const a = source.indexOf(start), b = source.indexOf(end, a);
  assert(a >= 0 && b > a, 'production function boundaries exist: ' + start);
  return source.slice(a, b);
};
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const tick = () => new Promise(setImmediate);
const plain = value => JSON.parse(JSON.stringify(value));
const charm = (id, rid = '4176277010') => ({ id, poolId: rid + '_line_1', order: rid, sku: 'KILLER WHALE 2', name: rid + ' · KILLER WHALE 2', centerPt: [10, 10], outline: { subpaths: [] }, members: [], widthPt: 20, heightPt: 20 });
const page = (id, rid = '4176277010') => ({ sheetId: id, metal: 'gold', sheetIndex: 1, placements: [{ id: 'c1', cxPt: 31, cyPt: 29, wPt: 20, hPt: 20 }], charms: [charm('c1', rid)], backPool: [] });
const record = (id, extra = {}) => Object.assign({ id, metal: 'gold', stock: { wPt: 280, hPt: 140 }, placements: [{ id: 'c1', cxPt: 90, cyPt: 95, wPt: 20, hPt: 20 }], charms: [charm('c1')], backPool: [] }, extra);

function harness() {
  const h = { live: new Map(), calls: [], sourceCalls: [], callbacks: [], paints: [], observers: [], images: [], frames: new Map(), frame: 0, recovery: [], warnings: [], handler: async (_name, body) => ({ sheet: record(body.id), backs: [] }), sourceHandler: async () => ({ pool: false, charms: [charm('base')] }) };
  class Observer {
    constructor(fn) { this.fn = fn; this.disconnected = false; h.observers.push(this); }
    observe(target) { this.target = target; }
    disconnect() { this.disconnected = true; }
    notify() { if (!this.disconnected) this.fn(); }
  }
  class Image {
    constructor() { h.images.push(this); }
    set src(value) { this.value = value; }
    get src() { return this.value; }
  }
  const assets = { loadImage: (image, url) => { const task = deferred(); h.recovery.push({ image, url, task }); return task.promise; } };
  const context = vm.createContext({
    console: { warn: (...args) => h.warnings.push(args) }, Map, Set, Date, Promise, Object, Array, String, Number,
    window: { ResizeObserver: Observer, CharmNestAssets: assets, Engrave: { fonts: { ok: true, Regular: {} } } }, ResizeObserver: Observer, CharmNestAssets: assets,
    Engrave: { fonts: { ok: true, Regular: {} } }, Image,
    document: { createElement: () => ({ getContext: () => ({ drawImage() {} }) }) },
    performance: { now: () => 100 },
    api: (...args) => { h.calls.push(args); return h.handler(...args); },
    liveOf: id => h.live.get(id) || null,
    stockFor: () => ({ wPt: 280, hPt: 140 }), stockOf: r => r.stock,
    engOf: (x, rec) => ({ back: (rec.backPool || []).find(b => b.poolId === x.poolId) || null }),
    Pool: { cloneCharm: (c, id) => Object.assign({}, c, { id }) },
    CharmNestOrders: { orderQuery: q => String(q || ''), orderGroups: (pieces, q) => pieces.filter(x => !q || x.rid.includes(q)) },
    CharmNestBacks: { engraveOn: () => null },
    cors: url => url,
    tryDo: fn => { try { return fn(); } catch (e) { h.warnings.push(e); return null; } },
    sourceGeom: s => { h.sourceCalls.push(s.id); return h.sourceHandler(s); },
    requestAnimationFrame: fn => { const id = ++h.frame; h.frames.set(id, fn); return id; },
    cancelAnimationFrame: id => h.frames.delete(id),
    paintOrderBase: G => { if (!G.cv.isConnected || G.cv._order !== G || !G.cv.visible) return false; h.paints.push(G.rec.id); G.base = {}; return true; },
    paintOrder: () => {},
    soonPaint: G => { if (context.paintOrderBase(G)) context.paintOrder(G); },
    hitOrder: () => null, engGeoOf: () => null, engPointOf: () => ({}), sheetNoOf: rec => rec.sheetIndex || 1,
    orderHalos: () => {},
  });
  vm.runInContext(between('  function piecesOf(rec) {', '  const liveOf ='), context);
  vm.runInContext(between('  const orderRecs =', '  /* ── the back of a sheet'), context);
  vm.runInContext(between('  const sheetBacks =', '  /** The words of one piece'), context);
  vm.runInContext(between('  async function drawOrder(cv,', '  /* ── a Nest card turned over'), context);
  h.context = context;
  h.canvas = () => ({ isConnected: true, visible: true, parentElement: {}, width: 400, height: 200, getBoundingClientRect: () => ({ left: 0, top: 0, width: 400, height: 200 }) });
  h.draw = (...args) => context.drawOrder(...args);
  h.rec = id => context.recFor(id);
  return h;
}

async function main() {
  {
    const h = harness(), metadata = deferred(), cv = h.canvas(), p = page('local'); h.live.set('local', p);
    h.handler = (_fn, body) => body.op === 'getSheet' ? metadata.promise : Promise.resolve({ backs: [] });
    const shown = [], task = h.draw(cv, 'local', '4176277010', { onInfo: i => shown.push(i) });
    let settled = false; task.then(() => { settled = true; }); await tick();
    assert(settled && shown.length === 1, 'a fully available local sheet settles before its stalled cloud metadata');
    assert.equal(shown[0].mine[0].c, p.charms[0], 'the real local charm geometry is present on the first onInfo');
    assert(h.paints.includes('local'), 'the local sheet paints without a cloud answer');
    assert.equal(h.calls[0][2].timeoutMs, 12000, 'getSheet view reads have their own short deadline');
    metadata.resolve({ sheet: record('local', { setSeq: 9, folder: 'saved-file', laserDoneAt: 5000, backPool: [{ poolId: p.charms[0].poolId, approvedAt: 4000, approvedBy: 'Paul' }] }) }); await tick();
    assert.equal(shown[0].rec.setSeq, 9, 'saved set metadata enriches the existing live view');
    assert.equal(shown[0].rec.laserDoneAt, 5000);
    assert.equal(shown[0].sheet.name, 'saved-file');
    assert.equal(shown[0].mine[0].p.cxPt, 31, 'late saved placements never roll back the current local placement');
    assert.equal(shown[0].mine[0].eng.back.approvedBy, 'Paul', 'saved engraving approval enriches the piece');
    assert.equal(shown.length, 2, 'the active panel receives its metadata enrichment');
    assert.equal(plain(shown[0].search('4176277')).length, 1, 'incremental order search is preserved');
    shown[0].back(true); assert.equal(cv._order.backSide, true); shown[0].back(false); assert.equal(cv._order.backSide, false, 'front/back remains reversible');
    assert.equal(h.calls.find(call => call[1].op === 'backList')[2].timeoutMs, 12000, 'back-side metadata also has a short read deadline');
  }
  {
    const h = harness(), pending = new Map(), cv = h.canvas(); for (const id of ['A', 'B']) { h.live.set(id, page(id)); pending.set(id, deferred()); }
    h.handler = (_fn, body) => body.op === 'getSheet' ? pending.get(body.id).promise : Promise.resolve({ backs: [] });
    let firstCallbacks = 0, secondCallbacks = 0;
    const first = await h.draw(cv, 'A', '4176277010', { onInfo: () => firstCallbacks++ }); const observer = h.observers[0];
    await h.draw(cv, 'B', '4176277010', { onInfo: () => secondCallbacks++ }); const before = h.paints.length;
    pending.get('A').resolve({ sheet: record('A', { folder: 'obsolete', setSeq: 99 }) }); await tick();
    assert.equal(cv._order.rec.id, 'B'); assert.equal(firstCallbacks, 1, 'superseded live metadata never repaints its old panel');
    assert.equal(h.paints.length, before, 'superseded metadata never paints over the new sheet'); assert(observer.disconnected, 'replacement releases the prior layout observer');
    first.back(true); assert.equal(firstCallbacks, 1); assert.equal(cv._order.backSide, false, 'an old info handle cannot turn the active sheet');
    pending.get('B').resolve({ sheet: record('B') }); await tick(); assert.equal(secondCallbacks, 2);
  }
  {
    const h = harness(), remote = deferred(), cv = h.canvas(); h.live.set('new', page('new'));
    h.handler = (_fn, body) => body.id === 'old' ? remote.promise : Promise.resolve({ sheet: record(body.id), backs: [] });
    let callbacks = 0; const old = h.draw(cv, 'old', '4176277010', { onInfo: () => callbacks++, onProgress: () => callbacks++ });
    await h.draw(cv, 'new', '4176277010');
    remote.resolve({ sheet: record('old', { sources: [{ id: 'old-source' }], charms: [Object.assign(charm('c1'), { sourceId: 'old-source', index: 0 })] }) }); await old;
    assert.equal(callbacks, 0, 'a superseded cold record cannot call onInfo or progress');
    assert.equal(h.sourceCalls.length, 0, 'a superseded cold read never starts obsolete geometry downloads');
    assert.equal(cv._order.rec.id, 'new');
  }
  {
    const h = harness(), cv = h.canvas(), jobs = new Map(), backs = deferred(); let active = true;
    const charms = Array.from({ length: 8 }, (_, i) => Object.assign(charm('c' + i), { sourceId: 's' + i, index: 0 }));
    const sources = charms.map(c => ({ id: c.sourceId, name: c.sourceId }));
    h.handler = async (_fn, body) => body.op === 'backList' ? backs.promise : { sheet: record('closed', { charms, sources, placements: charms.map(c => ({ id: c.id, cxPt: 30, cyPt: 30 })) }) };
    h.sourceHandler = s => { const job = deferred(); jobs.set(s.id, job); return job.promise; };
    let callbacks = 0; const task = h.draw(cv, 'closed', '4176277010', { back: true, isCurrent: () => active, onInfo: () => callbacks++, onProgress: () => callbacks++, onWait: () => callbacks++ }); await tick();
    assert.equal(h.sourceCalls.length, 4, 'source loading retains its existing four-lane limit');
    active = false; const before = callbacks, painted = h.paints.length;
    for (const job of jobs.values()) job.resolve({ pool: false, charms: [charm('base')] });
    backs.resolve({ backs: [{ poolId: '4176277010_line_1', approvedAt: 5000 }] }); await task; await tick();
    assert.equal(callbacks, before, 'closing the owner suppresses late progress, back/font and panel callbacks');
    assert.equal(h.paints.length, painted, 'a closed owner receives no late paint');
    assert.equal(h.sourceCalls.length, 4, 'closing stops the queued next sources while in-flight shared reads finish');
    assert(h.observers[0].disconnected, 'settled stale work releases its layout observer');
  }
  {
    const h = harness(), cv = h.canvas(); cv.visible = false; const p = page('hidden'); p.sheetId = null;
    const info = await h.draw(cv, p, '4176277010'); assert.equal(h.paints.length, 0);
    cv.visible = true; h.observers[0].notify(); assert.equal(h.paints.length, 1, 'a zero-sized hidden view draws when its parent becomes visible');
    assert.equal(info.mine.length, 1, 'layout retry keeps the same sheet and piece selection');
    cv.isConnected = false; h.observers[0].notify(); assert(h.observers[0].disconnected, 'removed views release their observer');
  }
  {
    const h = harness(), cv = h.canvas(); h.handler = async (_fn, body) => ({ sheet: record(body.id, { outputs: { preview: { url: 'https://fixture/preview.png' } } }) });
    await h.draw(cv, 'preview', '4176277010'); const image = h.images[0]; image.onerror();
    assert.equal(h.recovery.length, 1, 'a failed saved picture uses the shared image recovery pipeline');
    h.recovery[0].task.resolve(); await tick(); assert.equal(cv._order.img, image, 'recovered current preview becomes visible');
    const oldPaints = h.paints.length;
    await h.draw(cv, 'other', '4176277010'); const otherImage = h.images[1]; otherImage.onerror();
    h.live.set('replacement', page('replacement')); await h.draw(cv, 'replacement', '4176277010'); const after = h.paints.length;
    h.recovery[1].task.resolve(); await tick();
    assert.equal(cv._order.rec.id, 'replacement'); assert.equal(h.paints.length, after, 'late image recovery cannot paint over another sheet');
    assert.equal(otherImage.onload, null, 'replacement releases the old image event handler'); assert(h.paints.length > oldPaints);
  }
  {
    const h = harness(), first = deferred(); h.handler = () => first.promise;
    const a = h.rec('retry'), b = h.rec('retry'); assert.equal(a, b, 'concurrent metadata reads share one request'); assert.equal(h.calls.length, 1);
    first.reject(Object.assign(new Error('temporarily offline'), { transient: true })); await Promise.allSettled([a, b]); await tick();
    h.handler = async () => ({ sheet: record('retry') }); const restored = await h.rec('retry'); assert.equal(restored.id, 'retry'); assert.equal(h.calls.length, 2, 'failed metadata reads are evicted and reopening can retry');
    await h.rec('retry'); assert.equal(h.calls.length, 2, 'a successful metadata read is reused within its existing minute cache');
    const cv = h.canvas(); h.live.set('retry', page('retry')); const info = await h.draw(cv, 'retry', '4176277010'); assert.equal(info.rec.stock.wPt, 280, 'cached metadata remains available to a local drawing');
  }
  {
    const h = harness(), cv = h.canvas(), metadata = deferred(); h.live.set('offline', page('offline')); h.handler = () => metadata.promise;
    const info = await h.draw(cv, 'offline', '4176277010'); const before = h.paints.length;
    metadata.reject(new Error('metadata timed out')); await tick();
    assert.equal(cv._order.rec.id, 'offline'); assert.equal(info.mine[0].c, h.live.get('offline').charms[0], 'failed background metadata preserves the local sheet and geometry');
    assert.equal(h.paints.length, before, 'a failed metadata enrichment never clears the sheet');
    h.handler = async (_fn, body) => ({ sheet: record(body.id, { setSeq: 4 }) }); const retried = await h.draw(cv, 'offline', '4176277010'); await tick();
    assert.equal(retried.rec.setSeq, 4, 'reopening recovers enrichment after a transient metadata failure');
  }
  {
    const h = harness(), cv = h.canvas(), firstBack = deferred(); h.live.set('back-retry', page('back-retry')); let reads = 0;
    h.handler = async (_fn, body) => body.op === 'backList' ? (++reads === 1 ? firstBack.promise : { backs: [{ poolId: '4176277010_line_1', approvedAt: 9 }] }) : { sheet: record(body.id) };
    const info = await h.draw(cv, 'back-retry', '4176277010', { back: true }); await tick();
    firstBack.reject(new Error('back read timed out')); await tick(); assert.equal(cv._order.backsAsked, false, 'a failed back read remains retryable in the same open view');
    info.back(false); info.back(true); await tick(); assert.equal(reads, 2, 'returning to Back retries the failed shared cache read');
    assert.equal(cv._order.backs.get('4176277010_line_1').approvedAt, 9, 'the recovered back becomes available on its original piece');
  }
  console.log('PASS: order sheet resilience — immediate live geometry, bounded retryable metadata, late enrichment, latest-canvas ownership, close/reopen source isolation, hidden-view layout recovery, preview retry, preserved search and reversible front/back');
}
main().catch(e => { console.error(e); process.exitCode = 1; });
