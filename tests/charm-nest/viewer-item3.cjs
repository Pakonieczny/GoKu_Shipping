// ITEM 3 (Paul, 21 Nov 2026, the cloud carrying every run since 20 Nov 23:41, every staff computer a viewer): an order's history made on a staff computer (its timeline steps: a Review
// decision, an engraving choice, a Hold Order and its answer, a cancel) must reach the cloud and show in the order window's Timeline. "Staff keep FULL use of their own inputs from any
// computer"; nothing may be refused, silent, or say "done" when nothing happened.
//   S  the library in mode "on" (offline, _cloudFakes.cjs): a current page's timelineAdd is taken (8463bff) and one with no cv still gets the reload words, which is why every page path
//      that sends the timeline must carry the page's cv; a viewer's Cancel Order record (cancelPut) and Hold Order (reviewMarkPut) are taken; cancelFates from a page stays the cloud's
//      (its take-off unit writes the cancel's fates and removals); mode off unchanged
//   P  order-timeline.js in a bare context: its own outbox (used when the computer's outbox is not there) sends the page's cv beside the events, as the page's api() does; a station
//      page (no CNViewer) and the station door (firebaseOrders) send exactly what they sent before
// Offline only: no Etsy, no paid call, no live write. Made-up names.
//   NODE_PATH=<deps> PW_DIR=<playwright> CHROMIUM=<chrome> node tests/charm-nest/viewer-item3.cjs
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let failed = 0;
const log0 = console.log.bind(console);
const ok = (name, cond, detail) => { log0((cond ? 'OK   ' : 'FAIL ') + name + (cond || detail === undefined ? '' : ' :: ' + (typeof detail === 'string' ? detail : JSON.stringify(detail)).slice(0, 1500))); if (!cond) failed++; };
const CV = 'cloud14-20261120', PAGE = 'd0michelle1';

async function partS() {
  process.env.EDIT_PASSCODE = 'test-pass';   // (the offline library's door; never set while the real page of part E talks to its fake site)
  const T0 = Date.UTC(2026, 10, 21, 15, 0, 0);
  const F = require('./_cloudFakes.cjs')({ now: T0 });
  const quiet = console.log, qw = console.warn; console.log = () => {}; console.warn = () => {};
  try {
    const lib = F.load('charmNestLibrary.js');
    const P = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({}, o, { http: r.statusCode }); };
    const RID = '4241500001';
    const seed = mode => { F.reset(); F.clock.set(T0); F.seed('Charm_Nest_Open/drive', { mode, runMode: 'manual', cloudSteps: ['engrave', 'files', 'takeoff'], flippedAt: T0 - 3600e3 }); lib._driveForget(); };
    seed('on');
    const ev = (id, type, text) => ({ id, orderId: RID, type, at: T0, by: 'Michelle', station: 'sorter', text });
    let r = await P({ op: 'timelineAdd', events: [ev('i3-dec', 'decided', 'Decided by Michelle · material gold'), ev('i3-none', 'engraveChanged', 'No engraving — decided by Michelle'), ev('i3-hold', 'note', 'Order held by Michelle')], dev: PAGE, cv: CV });
    let tl = await P({ op: 'timelineGet', orderId: RID });
    const types = (tl.events || []).map(e => e.type);
    ok('S1 mode on: a viewer\'s Review decision, engraving None and Hold Order note (one timelineAdd, with its cv) are taken and kept', r.http === 200 && !r.cloudCarries && ['decided', 'engraveChanged', 'note'].every(t => types.includes(t)), { r, types });
    r = await P({ op: 'timelineAdd', events: [ev('i3-old', 'note', 'from a page that sent no cv')], dev: PAGE });
    ok('S2 ... the same events with no cv get the reload words (so every page path that sends the timeline must send the page\'s cv)', r.http === 409 && r.reload === true, r);
    r = await P({ op: 'timelineAdd', events: [ev('i3-nodev', 'note', 'no cv, no computer name')] });
    ok('S3 ... with no cv and no computer name either (order-timeline.js\'s own outbox, before item 3): refused too', r.http === 409 && r.cloudCarries === true, r);
    r = await P({ op: 'cancelPut', orderId: RID, by: 'Michelle', why: '', at: T0, record: { buyer: 'Name One', lines: [] }, dev: PAGE, cv: CV });
    tl = await P({ op: 'timelineGet', orderId: RID });
    const cx = tl.cancelled && (tl.cancelled[RID] || tl.cancelled);
    ok('S4 mode on: a viewer\'s Cancel Order record (cancelPut) is taken: the order\'s timeline says cancelled, by whom', r.http === 200 && !r.error && !!cx && cx.by === 'Michelle', { r, cancelled: tl.cancelled });
    r = await P({ op: 'reviewMarkPut', action: 'mark', rid: RID, by: 'Michelle', dev: PAGE, cv: CV });
    ok('S5 mode on: a viewer\'s Hold Order (reviewMarkPut) is taken', r.http === 200 && !r.error && r.record && r.record.review && r.record.review.state === 'open', r);
    r = await P({ op: 'cancelFates', orderId: RID, fates: [], by: 'Michelle', removals: [{ id: 'queue~the queue', where: 'the queue', kind: 'queue', outcome: 'removed', by: 'Michelle', text: 'Taken out of the queue' }], dev: PAGE, cv: CV });
    ok('S6 mode on: a cancel\'s fates and removals stay the cloud\'s (its take-off unit writes them): a page\'s cancelFates is refused, so a viewer must not send it', r.http === 409 && r.cloudCarries === true && !r.reload, r);
    seed('off');
    r = await P({ op: 'timelineAdd', events: [ev('i3-off', 'note', 'mode off')] });
    const r2 = await P({ op: 'cancelFates', orderId: RID, fates: [], by: 'Michelle', removals: [{ id: 'queue~the queue', where: 'the queue', kind: 'queue', outcome: 'removed', by: 'Michelle', text: 'Taken out of the queue' }], dev: PAGE, cv: CV });
    ok('S7 drive off: nothing is refused (a timelineAdd with no cv, a page\'s cancelFates): every page works as before the cloud', r.http === 200 && r2.http === 200, { r, r2 });
  } finally { console.log = quiet; console.warn = qw; }
}

async function partP() {
  const src = fs.readFileSync(path.join(ROOT, 'order-timeline.js'), 'utf8');
  function page(o) {
    const sent = [], store = new Map();
    const w = { console: { log() {}, warn() {}, error() {} }, setTimeout: (f, ms) => setTimeout(f, Math.min(ms, 5)), clearTimeout, Promise, Date, JSON, Math, Set, Map, Object, Array, String, Number, Blob: class { constructor(p) { this.size = String(p[0]).length; } } };
    w.window = w; w.location = { protocol: 'http:' }; w.addEventListener = () => {}; w.document = { addEventListener() {}, visibilityState: 'visible' };
    w.localStorage = { getItem: k => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)), removeItem: k => store.delete(k) };
    w.queueMicrotask = f => Promise.resolve().then(f);
    w.fetch = async (url, init) => { sent.push({ url, body: JSON.parse(init.body) }); return { ok: true, status: 200, json: async () => ({ ok: true }) }; };
    if (o.viewer) w.CNViewer = { cv: () => CV, on: () => true };
    vm.runInContext(src, vm.createContext(w));
    if (o.mode) w.OrderTimeline.config({ mode: o.mode, station: o.mode === 'station' ? 'sorting' : '' });
    return { w, sent };
  }
  const flushed = async ({ w, sent }, ev) => { w.OrderTimeline.record(ev); for (let i = 0; i < 50 && !sent.length; i++) await new Promise(r => setTimeout(r, 10)); await w.OrderTimeline.flush(); return sent; };
  { const p = page({ viewer: true }), sent = await flushed(p, { orderId: '4241500001', type: 'note', text: 'a note', id: 'p1', by: 'Michelle' });
    const b = sent[0] && sent[0].body;
    ok('P1 the sorter page with no outbox of its own: order-timeline.js sends its events with the page\'s cv, as api() does', !!b && /charmNestLibrary$/.test(sent[0].url) && b.op === 'timelineAdd' && b.cv === CV && b.events.length === 1 && b.events[0].id === 'p1', sent); }
  { const p = page({}), sent = await flushed(p, { orderId: '4241500001', type: 'note', text: 'a note', id: 'p2' });
    const b = sent[0] && sent[0].body;
    ok('P2 a page with no CNViewer sends exactly what it sent before (no cv)', !!b && b.op === 'timelineAdd' && !('cv' in b), sent); }
  { const p = page({ viewer: true, mode: 'station' }), sent = await flushed(p, { orderId: '4241500001', type: 'scan', text: 'Seen at Sorting', id: 'p3' });
    const b = sent[0] && sent[0].body;
    ok('P3 a station page\'s door (firebaseOrders) is untouched: its events alone, no cv', !!b && /firebaseOrders/.test(sent[0].url) && Array.isArray(b.timeline) && !('cv' in b), sent); }
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  ok('P4 charm-nest-1.html loads the changed order-timeline.js under a new token', /order-timeline\.js\?v=[^"]*-i3(?:-\w+)*"/.test(html), (html.match(/order-timeline\.js\?v=[^"]*"/g) || []).join(' '));
}
(async () => {
  // (parts S and P in a process of their own, with the offline cloud's fakes installed)
  if (process.env.I3_PART === 'offline') { await partS(); await partP(); }
  else {
    const r = require('child_process').spawnSync(process.execPath, [__filename], { env: Object.assign({}, process.env, { I3_PART: 'offline' }), encoding: 'utf8', timeout: 180000 });
    const out = String(r.stdout || '') + String(r.stderr || ''); for (const l of out.split('\n')) if (/^(OK|FAIL) /.test(l)) log0(l);
    if (r.status !== 0) { failed++; if (!/^(OK|FAIL) /m.test(out)) log0(out.slice(0, 2000)); }
  }
  log0(failed ? `FAIL viewer-item3 (${failed})` : 'PASS viewer-item3');
  process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL viewer-item3: ' + (e && e.stack || e)); process.exit(1); });
