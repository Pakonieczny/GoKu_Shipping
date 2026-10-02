// Independent audit of the shared Sent to Sheet achievement face, historical identity,
// and its actual pointer-hover lifecycle. No order data or services are written.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');
const root = path.join(__dirname, '../..');
const dom = new JSDOM('<main id="cards"></main><p id="outside">Work area</p>', {
  url: 'https://test.invalid', runScripts: 'outside-only', pretendToBeVisual: true
});
const w = dom.window, d = w.document, outside = d.getElementById('outside');
let clock = 0, timerId = 0, hit = outside;
const timers = new Map();
w.setTimeout = (fn, delay = 0) => { timers.set(++timerId, { fn, at: clock + delay }); return timerId; };
w.clearTimeout = id => timers.delete(id);
w.setInterval = (fn, delay) => { timers.set(++timerId, { fn, at: clock + delay, interval: delay }); return timerId; };
w.clearInterval = id => timers.delete(id);
w.matchMedia = () => ({ matches: false });
w.Element.prototype.getAnimations = () => [];
w.Element.prototype.animate = () => ({ finished: Promise.resolve(), cancel() {}, playState: 'finished' });
d.elementFromPoint = () => hit;
Object.defineProperty(w.HTMLElement.prototype, 'offsetWidth', { get() { return 84; } });
Object.defineProperty(w.HTMLElement.prototype, 'offsetHeight', { get() { return 84; } });
w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8'));
w.eval(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'));
w.localStorage.setItem('cn.employee', 'Different current viewer');
// Screenshot order numbers, materials, actors and clock times; the fixture calendar day is synthetic.
const records = [
  { rid: '4174476673', metal: 'gold', state: 'written', count: 3, total: 5, at: Date.UTC(2026, 9, 1, 21, 46, 15), by: 'paul' },
  { rid: '4171770802', metal: 'silver', state: 'pooled', count: 2, total: 5, at: Date.UTC(2026, 9, 1, 23, 25), by: 'paul' }
];
const stampOf = rec => ({ id: 'designSent-' + rec.rid + '-' + rec.at, how: 'sheet', at: rec.at, by: rec.by });
const eventOf = rec => ({ id: 'sent-' + rec.rid, type: 'designSent', orderId: rec.rid, at: rec.at, by: rec.by, source: 'sorter', lineKey: rec.rid + '_1', data: { pieces: rec.total } });
const textsOf = svg => Array.from(svg.querySelectorAll('text'), n => n.textContent);
const noop = () => {};
const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
function between(source, start, end) {
  const i = source.indexOf(start), j = source.indexOf(end, i + start.length);
  assert(i >= 0 && j > i, 'production source boundary ' + start); return source.slice(i, j);
}
function lineOf(marker) { const i = bridge.indexOf(marker); assert(i >= 0, marker); return bridge.slice(i, bridge.indexOf('\n', i)); }
function auditReview() {
  // Execute actual classification, record derivation, row markup, controls and input handlers.
  // Cloud/thumbnail/printing services are outside this display test; the production send tests own them.
  const O = require(path.join(root, 'charm-nest-orders.js'));
  const rows = records.map(rec => ({ key: rec.rid + '_1', state: rec.state, poolIds: Array.from({ length: rec.total }, (_, i) => rec.rid + '_1_' + (i + 1)), problems: [],
    order: { receiptId: rec.rid, createTs: Math.floor((rec.at - 86400000) / 1000) }, line: { sku: rec.metal === 'gold' ? 'CUSTOM_6673' : 'CURB', title: 'Custom charm' },
    spec: { material: rec.metal, designSku: rec.metal === 'gold' ? 'CUSTOM_6673' : 'CURB', special: { label: 'Custom charm' } } }));
  const decisions = new Map(rows.map((row, i) => [row.key, { at: records[i].at, by: records[i].by, lines: { [row.key]: row.poolIds.map(() => ({ f: 'design-' + i, c: 0 })) } }]));
  const customItems = rows.map(row => ({ kind: 'customOrder', key: 'ord:custom:' + row.order.receiptId + ':' + row.line.sku, row, rows: [row] }));
  const view = d.createElement('section'); view.id = 'reviewView'; d.body.appendChild(view);
  const opened = [], originalMotion = w.Motion; w.Motion = null;
  Object.assign(w, { O, esc: escapeHtml, fmtT: at => new Date(at).toISOString(), B: { orders: { rows, byKey: new Map(rows.map(row => [row.key, row])) }, maps: { customDone: {}, customKept: {} } },
    items: () => customItems, mine: () => true, isNotice: () => false, rowsOf: it => it.rows || (it.row ? [it.row] : []),
    tabOf: it => it.kind, actOf: () => null, actStamp: () => '', settled: [], sentToSheet: record => record?.how === 'sheet',
    Orders: { rows: () => rows, statePill: row => [row.state, row.state === 'written' ? '3/5 written' : '2/5 nested'] },
    LiveStrip: { render: noop }, employeeName: () => 'Different current viewer', askEmployee: noop,
    CustomSheet: { decisionOf: row => decisions.get(row?.key), prune: noop, stamp: () => '', cardOf: () => ({ sent: true, files: [{ name: 'custom.dxf', state: 'ready' }] }), stripHtml: () => '', wire: noop },
    CustomPrint: { statusHtml: () => '', stamp: () => '', undoing: () => false, wire: noop },
    CustomRead: { count: () => 0, stamp: () => '', chip: () => '', bandOf: () => '' },
    ListMedia: { pair: () => '<div class="comparePair"></div>', mount: noop, more: noop }, purchaseMarkup: () => '',
    OrderWin: { open: (key, opts) => opened.push({ key, view: opts?.view }), openOrder: noop },
    KIND_WORDS: { customOrder: 'Custom Orders' },
    el: (tag, className) => { const node = d.createElement(tag); node.className = className; return node; },
    settledRow: () => { throw new Error('a sent decision must never use a Completed row'); }, acted: {} });
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
  w.eval(lineOf('  const customKey = row =>') + '\n' + lineOf('  const mkeyOf = it =>') + '\n' +
    between(bridge, '  function stampOf(it)', '  /* ── Custom Orders that ask nothing') +
    between(bridge, '  const infoItems = new Map();', '  /** The decision card inside another view') +
    between(bridge, '  function reviewRow(it)', '  /** A decision answered, under Completed') +
    lineOf('  const RV =') + '\nlet reviewFilter=null;const reviewRows=new Map();\n' +
    between(bridge, '  function render(opts)', '  /** What another tab') +
    '\nwindow.auditRV=RV;window.auditRender=render;window.auditLists=customLists;');
  try {
    const lists = w.auditLists(new Set());
    assert.equal(lists.open.length, 0, 'both already sent orders leave Open');
    assert.equal(lists.done.length, 0, 'partial nesting/writing is not a completed order');
    assert.equal(lists.sent.length, 2, 'both screenshot cases are independently classified as Decided');
    for (const it of lists.sent) { assert(it.decided); assert(!it.done); assert.equal(it.record.state, 'decided'); assert.equal(it.record.by, 'paul'); }
    w.auditRV.cseg = 'sent'; w.auditRender({ still: true });
    const shown = () => Array.from(view.querySelectorAll('#rvList > .reviewListRow'), node => node.dataset.rid);
    assert.deepEqual(shown(), ['4171770802', '4174476673'], 'latest actual send is first, even while only some copies are nested/written');
    const field = view.querySelector('#rvOrderFind');
    const input = value => { field.focus(); field.value = value; field.dispatchEvent(new w.Event('input', { bubbles: true })); };
    for (const rec of records) {
      for (let count = 1; count <= rec.rid.length; count++) {
        const query = rec.rid.slice(0, count); input(query);
        const wanted = count <= 3 ? ['4171770802', '4174476673'] : [rec.rid];
        assert.deepEqual(shown(), wanted, 'Decided filters immediately after each digit ' + query);
        assert.equal(view.querySelectorAll('#rvList .orderMatch').length, wanted.length, 'each possible order stays highlighted');
        assert.equal(d.activeElement, field, 'typing retains focus');
      }
    }
    input('4170000000'); assert.deepEqual(shown(), [], 'no impossible result remains');
    input('417'); assert.equal(shown().length, 2, 'backspace restores possible orders');
    input(''); assert.equal(view.querySelectorAll('.orderMatch').length, 0, 'clear removes search highlights');
    w.CNListActivity.set('review-sent', { direction: 'asc' }); w.auditRender({ still: true });
    assert.deepEqual(shown(), ['4174476673', '4171770802'], 'Oldest first reverses actual send activity');
    w.CNListActivity.set('review-sent', { direction: 'desc' }); w.auditRender({ still: true });
    const row = view.querySelector('.reviewListRow[data-rid="4171770802"]');
    assert.equal(row.querySelectorAll('.seal-sheet').length, 1, 'the actual Decided row has one original send seal');
    assert.doesNotMatch(row.querySelector('.seal-sheet svg').textContent, /paul|COMPLETE|QR LABEL/);
    assert.match(row.textContent, /2\/5 nested/, 'live progress remains visible');
    assert.deepEqual(Array.from(row.querySelectorAll('.rowActions button'), button => button.textContent), ['View designs', 'Open sheet', 'History']);
    row.querySelector('[data-cu-sheet]').click(); row.querySelector('[data-cu-history]').click();
    assert.deepEqual(opened, [{ key: '4171770802_1', view: 'sheet' }, { key: '4171770802_1', view: 'timeline' }], 'links reach the actual sheet and history views');
    const first = row.querySelector('.seal-sheet').innerHTML; w.auditRender({ still: true });
    assert.equal(view.querySelector('.reviewListRow[data-rid="4171770802"] .seal-sheet').innerHTML, first, 'redraw keeps exact original face');
    w.auditRV.cseg = 'open'; w.auditRender({ still: true }); assert.deepEqual(shown(), []);
    w.auditRV.cseg = 'done'; w.auditRender({ still: true }); assert.deepEqual(shown(), [], 'neither sent row is stranded in Completed');
  } finally { w.Motion = originalMotion; }
}
function rectFor(el, left = 410, top = 560) {
  const rect = { left, right: left + 84, top, bottom: top + 84, width: 84, height: 84 };
  el.getBoundingClientRect = () => rect; el.getClientRects = () => [rect];
}
function point(type, node, relatedTarget = null) {
  hit = type === 'pointerout' ? relatedTarget : node;
  // (a pointerout carries where the pointer has gone to)
  const rect = (type === 'pointerout' ? relatedTarget && relatedTarget.getBoundingClientRect() : node.getBoundingClientRect()) || { left: -9, width: 0, top: -9, height: 0 };
  node.dispatchEvent(new w.MouseEvent(type, { bubbles: true, relatedTarget, clientX: rect.left + rect.width / 2, clientY: rect.top + rect.height / 2 }));
}
async function advance(ms) {
  const end = clock + ms;
  for (;;) {
    const next = [...timers.entries()].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
    if (!next) break;
    const [id, t] = next; clock = t.at;
    if (t.interval) t.at += t.interval; else timers.delete(id);
    t.fn(); for (let i = 0; i < 8; i++) await Promise.resolve();
  }
  clock = end; for (let i = 0; i < 8; i++) await Promise.resolve();
}
async function audit() {
  auditReview();
  const seals = [];
  for (const rec of records) {
    const stamp = stampOf(rec), host = d.createElement('section');
    host.dataset.rid = rec.rid;
    host.innerHTML = w.Seal.row({ stamps: [stamp] }); d.getElementById('cards').appendChild(host);
    const seal = host.querySelector('.seal'), svg = seal.querySelector('svg'); rectFor(seal, 410 + seals.length * 120);
    const savedModel = JSON.parse(svg.dataset.sealModel), actionText = textsOf(svg)[0];
    assert.equal(savedModel.family, 'prepared', rec.rid + ': sending is a preparation achievement');
    assert.equal(savedModel.action, 'SENT TO SHEET');
    assert.equal(savedModel.by, 'paul'); assert.equal(savedModel.at, rec.at);
    assert.equal(actionText, 'SENT TO SHEET');
    assert.equal(svg.querySelector('g').getAttribute('fill'), w.Seal.FAMILY.prepared.ink);
    assert.doesNotMatch(svg.textContent, /QR LABEL|COMPLETE|LASER CUT|paul|Different current viewer/, 'a send does not claim print/completion/cutting or show identity on its small face');
    assert(textsOf(svg).includes('01 OCT 2026'), 'the send date is legible');
    assert(textsOf(svg).some(t => t === (rec.metal === 'gold' ? '5:46 PM' : '7:25 PM')), 'the original Toronto time is shown');
    assert.equal(seal.style.getPropertyValue('--sz'), '84px', 'send uses the standardized seal size');
    assert.match(w.Seal.titleOf(stamp), /^Sent to sheet by paul/);
    assert.doesNotMatch(w.Seal.titleOf(stamp), /completed|QR label/i);
    assert.equal(w.Seal.hasPrint({ stamps: [stamp] }), false, 'a send must never claim that a QR label was printed');
    const timelineModel = w.OrderTimelineUI.faceModel(eventOf(rec));
    assert.equal(timelineModel.family, savedModel.family); assert.equal(timelineModel.action, savedModel.action);
    assert.equal(timelineModel.at, rec.at); assert.equal(timelineModel.by, 'paul');
    const timelineFace = d.createElement('span'); timelineFace.innerHTML = w.OrderTimelineUI.stampSvg(eventOf(rec), true);
    assert.deepEqual(textsOf(timelineFace.querySelector('svg')), textsOf(svg), 'the chronology and list use the same wording, date and time');
    timelineFace.innerHTML = w.OrderTimelineUI.stampSvg(eventOf(rec), true, { hover: true });
    assert.match(timelineFace.textContent, /Signed bypaul/, 'timeline enlargement uses the recorded actor');
    assert.doesNotMatch(timelineFace.textContent, /Different current viewer/);
    assert(w.OrderTimelineUI.sealed(eventOf(rec)), 'an actual send is a recorded achievement with historical ink');
    const overview = d.createElement('span'); overview.innerHTML = w.OrderTimelineUI.nowStamps([eventOf(rec)], {}).seal;
    assert.equal(overview.querySelector('.tlNowSeal svg').dataset.sealFamily, 'prepared');
    assert.equal(textsOf(overview.querySelector('svg'))[0], 'SENT TO SHEET', 'Overview shows the genuine latest send achievement');
    assert.doesNotMatch(overview.textContent, /COMPLETE|QR LABEL|paul|Different current viewer/);
    const derived = w.OrderTimelineUI.derive([eventOf(rec)]);
    assert.equal(derived.hand, null, 'a send never marks the order as completed by hand');
    assert.equal(derived.W.stage, 'waiting', 'a send alone stays queued until actual nesting records establish a sheet');
    assert.equal(derived.stages[1].first, null, 'the send seal cannot fabricate a Nested milestone');
    const tool = d.createElement('span'); tool.innerHTML = w.Seal.tool('sheet', svg);
    assert.equal(tool.querySelector('svg').dataset.stampFamily, 'prepared', 'the wooden head matches the prepared family');
    assert(tool.querySelector('path[d="' + svg.querySelector('[data-seal-outline]').getAttribute('d') + '"]'), 'the head uses the exact earned seal outline');
    seals.push(seal);
  }
  const first = seals[0], originalFace = first.innerHTML, zoomed = () => d.querySelector('[data-seal-zoom]'), DELAY = w.Seal.zoom.DELAY;
  assert.equal(DELAY, 500, 'one named 500 ms hover rest');
  point('pointerover', first); await advance(300); assert.equal(zoomed(), null, 'fast movement (300 ms) cannot zoom a send-seal');
  point('pointerout', first, outside); await advance(2000); assert.equal(zoomed(), null, 'leaving cancels the delayed zoom');
  point('pointerover', first); await advance(DELAY); assert.equal(zoomed(), first, 'resting opens the send seal itself in place: no second seal');
  assert.match(first.getAttribute('aria-label'), /Sent to sheet by paul/, 'the recorded signer is its accessible name'); assert.doesNotMatch(first.outerHTML, /Different current viewer/);
  assert.equal(d.querySelectorAll('.sealLens,.tlLoupe,[data-seal-caption]').length, 0, 'no lens or caption exists');
  const grown = w.Seal.zoom.rectOf(first); assert(grown && grown.left >= 0 && grown.top >= 0 && grown.right <= w.innerWidth && grown.bottom <= w.innerHeight, 'the grown seal stays in the view');
  point('pointerout', first, outside); await advance(0); assert.equal(zoomed(), null, 'the zoom goes back immediately on pointer exit');
  assert.equal(first.innerHTML, originalFace, 'zoom never edits historical face data');
  const mixed = w.Seal.list({ prints: 1, stamps: [stampOf(records[0]), { how: 'print', at: records[0].at + 60000, by: 'Seth' }] });
  assert.equal(mixed[0].how, 'sheet'); assert(!mixed[0].n, 'send does not consume a print sequence number');
  assert.equal(mixed[1].how, 'print'); assert.equal(mixed[1].n, 1, 'a later genuine QR print starts at one');
  let presses = 0; const originalPress = w.Seal.press;
  w.Seal.press = async () => { presses++; };
  w.OrderTimeline = { get: async () => ({ events: [eventOf(records[0])], cancelled: null }) };
  const historyHost = d.createElement('main'); d.body.appendChild(historyHost);
  const history = w.OrderTimelineUI.mount(historyHost, { orderId: records[0].rid, live: false });
  try {
    await advance(0);
    const actual = historyHost.querySelector('.tlSt[data-key^="designSent~"]');
    assert(actual, 'the actual history chronology retains the send achievement');
    assert.equal(actual.querySelector('svg').dataset.sealFamily, 'prepared');
    assert.equal(JSON.parse(actual.querySelector('svg').dataset.sealModel).at, records[0].at);
    assert.equal(presses, 0, 'historical recovery displays old ink without stamping a new approval');
  } finally { history.destroy(); w.Seal.press = originalPress; }
  for (const [at, date, time] of [[Date.UTC(2026, 9, 2, 3, 59), '01 OCT 2026', '11:59 PM'], [Date.UTC(2026, 9, 2, 4), '02 OCT 2026', '12:00 AM']]) {
    const stamp = { how: 'sheet', at, by: 'paul' }, event = { type: 'designSent', at, by: 'paul' };
    const small = d.createElement('span'), timeline = d.createElement('span');
    small.innerHTML = w.Seal.svg(stamp); timeline.innerHTML = w.OrderTimelineUI.stampSvg(event, true);
    assert.deepEqual(textsOf(timeline.querySelector('svg')), textsOf(small.querySelector('svg')), 'all surfaces agree across the Toronto date rollover');
    assert(textsOf(small.querySelector('svg')).includes(date)); assert(textsOf(small.querySelector('svg')).includes(time));
  }
  console.log('PASS: reported partial GF/SS cases go only to Decided; each digit filters/highlights immediately; chronological sort and sheet/history controls work; original Sent to Sheet seals, matching timeline/Overview/wood outlines, immutable actor/date/time, silent old history, full hover delay/immediate exit and separate QR history');
}
audit().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => w.close());
