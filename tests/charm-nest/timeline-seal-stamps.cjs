// The real timeline uses the shared faces, then awaits complete physical presses before repainting or following a new step.
const assert = require('node:assert/strict'), fs = require('node:fs'), { JSDOM } = require('jsdom');
const dom = new JSDOM('<main id="timeline"></main><section id="compact"></section>', { url: 'http://127.0.0.1', runScripts: 'outside-only', pretendToBeVisual: true });
const w = dom.window, d = w.document;
w.matchMedia = () => ({ matches: false });
w.Element.prototype.getAnimations = () => [];
w.Element.prototype.animate = () => ({ finished: Promise.resolve(), cancel() {}, playState: 'finished' });
w.eval(fs.readFileSync('charm-nest-motion.js', 'utf8'));
w.eval(fs.readFileSync('charm-nest-timeline-ui.js', 'utf8'));
const T = w.OrderTimelineUI, at = Date.now(), orderId = '4175254511', compactOrder = '4175254512';
const arrived = { id: 'arrive', type: 'arrived', orderId, at: at - 172800000, by: 'Paul Konieczny', source: 'sorter' };
const laser = { id: 'laser', type: 'laserDone', orderId, at, by: 'Seth Signed', source: 'sorter' };
const welded = { id: 'weld', type: 'welded', orderId, at: at + 1000, by: 'Julie Signed', source: 'sorter' };
const backfill = { id: 'older-restored', type: 'restored', orderId, at: at - 86400000, by: 'Historic Signer', source: 'sorter' };
const answer = [arrived], listeners = new Set(), presses = [], pending = [];
w.localStorage.setItem('cn.employee', 'Current Viewer');
w.OrderTimeline = {
  get: async id => ({ events: id === compactOrder ? [{ ...arrived, orderId: compactOrder }] : answer.slice(), cancelled: null }),
  onRecord: fn => { listeners.add(fn); return () => listeners.delete(fn); }
};
// Control only the press-completion promise. Rendering, event derivation, shared models and repaint logic are real.
w.Seal.press = async element => { presses.push(element); await new Promise(resolve => pending.push(resolve)); };
const flush = async () => { for (let i = 0; i < 18; i++) await Promise.resolve(); };
const record = event => { for (const fn of listeners) fn(event); };
(async () => {
  let timeline, compact;
  try {
    timeline = T.mount(d.querySelector('#timeline'), { orderId, live: true }); await flush();
    assert.equal(presses.length, 0, 'opening saved history does not play a new physical press');
    assert.equal(d.querySelector('.tlSt').querySelector('svg').dataset.sealFamily, 'received');
    const original = d.querySelector('#timeline .tlBig').dataset.key;
    record(laser); await flush();
    assert.equal(presses.length, 1, 'a new recorded action starts one physical press');
    const first = presses[0], face = first.firstChild;
    assert(first.classList.contains('pending'), 'fresh ink waits for contact in the shared press');
    assert.equal(face.dataset.sealFamily, 'laser');
    assert.equal(d.querySelector('#timeline .tlBig').dataset.key, original, 'next detail waits for the full press');
    record(welded); await flush();
    assert.equal(presses.length, 1, 'a second live record cannot interrupt the first press');
    assert.equal(first.firstChild, face, 'a queued redraw cannot replace the actual face being stamped');
    assert(!d.querySelector('.tlSt[data-key="welded~weld"]'), 'a later record waits for the first animation');
    pending.shift()(); await flush();
    assert.equal(presses.length, 2, 'the second physical press starts after the first is complete');
    assert.equal(d.querySelector('#timeline .tlBig').dataset.key, 'laserDone~laser', 'only the completed first press may advance the detail');
    assert(first.isConnected, 'the first historical seal remains');
    assert.equal(first.firstChild, face, 'the completed face remains unchanged when the queue redraws');
    assert(!first.classList.contains('pending'), 'completed ink remains visible');
    pending.shift()(); await flush();
    assert.equal(d.querySelector('#timeline .tlBig').dataset.key, 'welded~weld', 'the second detail waits until its own press is complete');
    assert.equal(d.querySelectorAll('#timeline .tlSt[data-key]').length, 3, 'all historical seals remain');
    const m = JSON.parse(first.querySelector('svg').dataset.sealModel);
    assert.equal(m.by, 'Seth Signed', 'the event retains its actual recorded signer');
    assert.equal(m.at, at, 'the event retains its exact timestamp');
    assert.doesNotMatch(first.querySelector('svg').textContent, /Seth Signed|Current Viewer/, 'the regular face reserves identity for hover');
    assert.match(T.stampSvg(laser, true, { hover: true }), /Seth Signed/, 'hover uses the saved signer');
    assert.doesNotMatch(T.stampSvg(laser, true, { hover: true }), /Current Viewer/, 'hover cannot substitute the current viewer');
    answer.push(laser, welded, backfill); await timeline.refresh(); await flush();
    assert.equal(presses.length, 2, 'discovering an older historical record stays silent');
    assert.equal(d.querySelectorAll('#timeline .tlSt[data-key]').length, 4, 'a discovered old seal is still retained and visible');
    compact = T.mount(d.querySelector('#compact'), { orderId: compactOrder, live: true, compact: true }); await flush();
    assert.equal(presses.length, 2, 'a compact historical rail stays silent');
    record({ ...laser, orderId: compactOrder }); await flush();
    assert.equal(presses.length, 3, 'a compact fresh event uses the same shared physical press');
    assert(presses[2].matches('.tlSeal'), 'the compact press targets the actual rail seal');
    assert.equal(presses[2].querySelector('svg').dataset.sealFamily, 'laser');
    record({ id: 'cancel', type: 'cancelled', orderId: compactOrder, at: at + 2000, by: 'Seth Signed', source: 'sorter' }); await flush();
    assert.equal(presses.length, 3, 'fresh cancellation cannot interrupt the compact laser press');
    pending.shift()(); await flush();
    assert.equal(presses.length, 4, 'fresh cancellation receives its own complete matching stamp');
    assert.equal(presses[3].querySelector('svg').dataset.sealFamily, 'cancelled');
    pending.shift()(); await flush();
    const shippingLabel = T.faceModel({ type: 'labelPrinted', at, station: 'shipping', data: { label: 'shipping' }, by: 'Seth Signed' });
    assert.equal(shippingLabel.family, 'fulfilment'); assert.doesNotMatch(shippingLabel.action, /QR/, 'a shipping label cannot be claimed as a QR label');
    const plain = { type: 'engraveChanged', at: at + 3000, by: 'Historic Signer', source: 'sorter', data: { how: 'skipped', decidedAt: at - 5000 } };
    assert(T.sealed(plain), 'cut plain is an actual historical decision with a seal');
    assert(!T.sealed({ ...plain, data: { how: 'words' } }), 'ordinary text edits do not create achievement seals');
    const plainModel = T.faceModel(plain);
    assert.equal(plainModel.family, 'engraving'); assert.equal(plainModel.action, 'CUT PLAIN'); assert.equal(plainModel.icon, 'plain');
    assert.equal(plainModel.at, at - 5000, 'cut plain retains the actual decision time');
    const unknownDate = T.faceModel({ ...plain, data: { how: 'skipped', decidedAt: 0 } });
    assert.equal(unknownDate.at, 0); assert.equal(unknownDate.date, ''); assert.equal(unknownDate.time, '');
    assert.equal(unknownDate.by, 'Historic Signer', 'an unknown date cannot erase a recorded signer');
    console.log('PASS: real shared faces; silent saved/backfilled history; serial live and compact/cancelled presses; concurrent redraw preserves ink; each detail awaits full completion; immutable identity/time and truthful cut-plain history.');
  } finally { timeline?.destroy(); compact?.destroy(); w.close(); }
})().catch(err => { console.error(err); process.exitCode = 1; });
