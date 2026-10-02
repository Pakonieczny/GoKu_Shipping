// Actual Review list, shared seals and activity controls: completed sheet sends resolve to Decided, retain their
// original decision and usable links, and never silently become completed production or generate new stamp history.
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { JSDOM } = require('jsdom');
const root = path.resolve(__dirname, '../..');
const dom = new JSDOM('<body><div id="modeSeg"><button data-mode="nest"></button><button data-mode="orders"></button></div><div id="reviewView"></div></body>', { runScripts: 'outside-only', pretendToBeVisual: true, url: 'https://example.test/' });
const w = dom.window, d = w.document;
w.matchMedia = () => ({ matches: true }); w.requestAnimationFrame = () => 1; w.cancelAnimationFrame = () => {}; w.setInterval = () => 1;
w.Element.prototype.getAnimations = () => []; w.Element.prototype.scrollIntoView = () => {};
w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8')); w.Motion = null;
w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
const O = require(path.join(root, 'charm-nest-orders.js')), now = Date.now(), day = 86400000;
function row(rid, state, special = true) {
  const key = rid + '_1';
  return { key, state, poolIds: state === 'written' || state === 'pooled' ? [key + '_1'] : [], problems: [],
    spec: { designSku: 'CUSTOM_' + rid, ...(special ? { special: { label: 'Custom charm' } } : {}) },
    order: { receiptId: String(rid) }, line: { sku: 'CUSTOM_' + rid, title: 'Custom charm' } };
}
const gf = row(4174476673, 'written'), ss = row(4171770802, 'pooled'), regular = row(4179999999, 'written', false), waiting = row(4180000000, 'pulled'), hand = row(4190000000, 'noDesign');
hand.spec.customDone = { how: 'button', completedAt: now - 4 * day, completedBy: 'Seth', stamps: [{ how: 'button', at: now - 4 * day, by: 'Seth' }] };
const pendingKeys = new Set();
const decisions = new Map([[gf.key, { id: 'gf-original', at: now - 3 * day, by: 'paul', lines: { [gf.key]: [{}] } }],
  [ss.key, { id: 'ss-original', at: now - 2 * day, by: 'paul', lines: { [ss.key]: [{}] } }],
  [regular.key, { id: 'regular-original', at: now - 60000, by: 'Seth', lines: { [regular.key]: [{}] } }]]);
const rows = [gf, ss, regular, waiting, hand], calls = [], records = new Map();
const addCalls = [];
w.B = { review: { items: [{ kind: 'customOrder', key: 'ord:custom:' + gf.order.receiptId + ':' + gf.spec.designSku, row: gf, rows: [gf] }, { kind: 'unmatchedSku', key: 'ord:sku:' + regular.spec.designSku, row: regular, rows: [regular] }] },
  maps: { customDone: {}, customKept: {} }, orders: { rows, byKey: new Map(rows.map(r => [r.key, r])) } };
w.Orders = { rows: () => rows, statePill: r => ['ok', r.state === 'written' ? 'on a sheet' : r.state === 'pooled' ? 'waiting for a sheet' : 'waiting'] };
w.CustomSheet = {
  decisionOf: r => r && (decisions.get(r.key) || r._customSentDecision || w.B.maps.customSent?.[r.key]) || null,
  sending: r => !!r && pendingKeys.has(r.key),
  sentOf: r => { const sent = r && decisions.get(r.key); return sent ? { sent, files: [] } : null; },
  stamp: it => String(w.CustomSheet.decisionOf(it.row)?.at || ''), prune() {},
  cardOf: it => ({ files: [{ id: 'design' }], sent: !!w.CustomSheet.decisionOf(it.row), busy: '', why: '', e: pendingKeys.has(it.row?.key) ? { sendIntent: {} } : null,
    open: !it.decided && (pendingKeys.has(it.row?.key) || (it.rows || [it.row]).some(r => !r.poolIds?.length)) }),
  buttonsHtml: it => it.decided ? '' : '<button data-cu-send>Send to Sheet</button>', stripHtml: () => '',
  wire: (node, it) => { node.querySelectorAll('[data-cu-designs]').forEach(b => b.onclick = () => calls.push(['designs', it.row.key])); }
};
w.CustomPrint = { statusHtml: () => '', stamp: () => '', undoing: () => false, freshOf: () => 0, failNote: () => '',
  keptButtonHtml: (it, act, cls, label) => `<button data-cu-${act}>${label}</button>`, buttonHtml: (it, act, cls, label) => `<button data-cu-${act}>${label}</button>`,
  wire: () => null, print: it => calls.push(['print', it.row.key]), complete: it => calls.push(['complete', it.row.key]), reopen: it => calls.push(['reopen', it.row.key]) };
w.CustomRead = { stamp: () => '', chip: () => '', bandOf: () => '', count: () => 0 };
w.LiveStrip = { render() {} }; w.ListMedia = { pair: r => `<div class="comparePair">${r.order.receiptId}</div>`, mount() {}, more() {} };
w.employeeName = () => 'Current operator'; w.askEmployee = () => 'Current operator'; w.RunCtl = { renderBanner() {} };
w.OrderWin = { open: (key, opts) => calls.push([opts?.view || 'overview', key]), openOrder: () => {} };
w.O = O; w.purchaseMarkup = r => `<span>${r.line.title}</span>`; w.fmtT = t => String(t); w.setMode = () => {};
w.esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
w.el = (tag, cls, markup) => { const n = d.createElement(tag); if (cls) n.className = cls; if (markup) n.innerHTML = markup; return n; };
// Neither a rerender, a restore or a switch should replay a wooden stamp or create fresh historical records.
w.Seal.press = (...args) => { addCalls.push(args); }; w.Seal.pressPending = (...args) => { addCalls.push(args); };
const source = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), start = source.indexOf('const Review = window.Review ='), end = source.indexOf('/* ═══ 24b · Sandbox', start);
const buttonsStart=source.indexOf('  function buttonsHtml(it, primary, hint = true)'),buttonsEnd=source.indexOf('  const hasFiles =',buttonsStart);
w.eval('window._reviewButtons=(function(){const cardOf=it=>window.CustomSheet.cardOf(it),DROP_IC="";'+source.slice(buttonsStart,buttonsEnd)+'return buttonsHtml;})()');
w.CustomSheet.buttonsHtml=w._reviewButtons;
assert(start >= 0 && end > start); w.eval(source.slice(start, end));
const R = w.Review, cards = () => [...d.querySelectorAll('#rvList .reviewListRow')], ids = () => cards().map(n => n.dataset.rid), get = r => cards().find(n => n.dataset.rid === r.order.receiptId);
let checks = 0;
function check(name, fn) { fn(); checks++; console.log('  ✓ ' + name); }
function mode(seg) { R.view().cseg = seg; R.view().filter = null; R.view().q = ''; R.render({ still: true }); }
try {
  check('successful legacy sends leave Open even while their stale review question remains', () => {
    mode('open'); assert.deepEqual(ids(), [waiting.order.receiptId]); assert.equal(R.count(), 0);
    assert.equal(d.querySelector('[data-cseg="sent"] b').textContent, '3');
    assert.equal(d.querySelector('[data-cseg="open"] b').textContent, '1');
  });
  check('both screenshot orders and ordinary Review sends are in Decided, never Completed', () => {
    mode('sent'); assert.deepEqual(ids(), [regular.order.receiptId, ss.order.receiptId, gf.order.receiptId]);
    mode('done'); assert.deepEqual(ids(), [hand.order.receiptId]); mode('sent');
  });
  check('all decided cards show one actual sent seal with original actor/time, without manufacturing a stamp', () => {
    for (const r of [gf, ss, regular]) {
      const n = get(r), seal = n.querySelector('.seal-sheet'), decision = decisions.get(r.key);
      assert(seal, 'sent-to-sheet seal exists'); assert.equal(n.querySelectorAll('.seal').length, 1);
      assert.equal(seal.dataset.at, String(decision.at)); assert(seal.getAttribute('aria-label').includes(decision.by));
      assert.doesNotMatch(seal.querySelector('svg').textContent, /Current operator|paul|Seth/, 'actor remains in enlargement metadata');
      assert(!n.querySelector('[data-cu-send],[data-cu-print],[data-cu-complete],[data-cu-reopen]'));
    }
    assert.deepEqual(addCalls, []);
  });
  check('Open sheet, History and View designs act on the live original row identity', () => {
    const n = get(gf); n.querySelector('[data-cu-sheet]').click(); n.querySelector('[data-cu-history]').click(); n.querySelector('[data-cu-designs]').click();
    assert.deepEqual(calls, [['sheet', gf.key], ['timeline', gf.key], ['designs', gf.key]]);
  });
  check('the third switch and minimal order search occupy the existing toolbar', () => {
    assert.equal(d.querySelectorAll('#reviewView .ordBar').length, 1); assert.equal(d.querySelectorAll('#reviewView .rvSeg').length, 1);
    assert.equal(d.querySelectorAll('.rvSeg button').length, 3); assert.equal(d.querySelectorAll('#rvOrderFind').length, 1);
    const bar = d.querySelector('.ordBar'); assert(bar.contains(d.querySelector('.cnListTools'))); assert(bar.contains(d.querySelector('.cnOrderFind')));
  });
  check('each typed order digit removes impossible matches immediately and keeps matching cards highlighted', () => {
    for (const [q, expected] of [['4', 3], ['41', 3], ['417', 3], ['4174', 1], ['41744', 1], ['4174476673', 1], ['41744766730', 0]]) {
      const input = d.querySelector('#rvOrderFind'); input.value = q; input.dispatchEvent(new w.Event('input', { bubbles: true }));
      assert.equal(cards().length, expected, q); assert(cards().every(n => n.classList.contains('orderMatch')), q + ' highlighted');
    }
    const input = d.querySelector('#rvOrderFind'); input.value = ''; input.dispatchEvent(new w.Event('input')); assert.equal(cards().length, 3);
  });
  check('date ranges and ascending/descending sorting use recorded activity rather than rerender time', () => {
    const scope = 'review-sent'; w.CNListActivity.set(scope, { range: 'today', direction: 'desc' }); R.render({ still: true }); assert.deepEqual(ids(), [regular.order.receiptId]);
    w.CNListActivity.set(scope, { range: 'all', direction: 'asc' }); R.render({ still: true }); assert.deepEqual(ids(), [gf.order.receiptId, ss.order.receiptId, regular.order.receiptId]);
    w.CNListActivity.set(scope, { range: 'all', direction: 'desc' }); R.render({ still: true }); assert.deepEqual(ids(), [regular.order.receiptId, ss.order.receiptId, gf.order.receiptId]);
    assert.equal(R.customItemFor(gf.key).t, decisions.get(gf.key).at);
  });
  check('repeated restore/rerender/switches neither duplicate nor replace the original send history', () => {
    const before = JSON.stringify([...decisions]); for (let i = 0; i < 5; i++) { mode('open'); mode('done'); mode('sent'); }
    assert.equal(JSON.stringify([...decisions]), before); assert.equal(cards().length, 3); assert.equal(d.querySelectorAll('#rvList .seal-sheet').length, 3); assert.deepEqual(addCalls, []);
  });
  check('past print/hand seals survive on a newly resolved sheet decision without duplicating old ink', () => {
    const sent = decisions.get(gf.key); w.B.maps.customKept[gf.key] = { stamps: [{ how: 'print', at: sent.at - 1, by: 'Alex' }] };
    sent.stamps = [{ how: 'sheet', at: sent.at, by: sent.by }]; R.render({ still: true });
    const n = get(gf); assert.equal(n.querySelectorAll('.seal').length, 2); assert.equal(n.querySelectorAll('.seal-sheet').length, 1);
    assert(n.querySelector('.seal-print').getAttribute('aria-label').includes('Alex')); assert.equal(R.printable(R.customItemFor(gf.key)), false);
  });
  check('pending durable sends remain actionable in Open until actually finalized', () => {
    const pending = row(4160000000, 'pulled'); rows.push(pending); w.B.orders.byKey.set(pending.key, pending);
    mode('open'); assert(ids().includes(pending.order.receiptId)); assert(get(pending).querySelector('[data-cu-send]'));
    mode('sent'); assert(!ids().includes(pending.order.receiptId)); mode('done'); assert(!ids().includes(pending.order.receiptId));
  });
  check('a shared question advances to its next unsent order rather than stranding that order behind a sent first row', () => {
    const unsent = row(4130000000, 'pulled', false); rows.push(unsent); w.B.orders.byKey.set(unsent.key, unsent);
    w.B.review.items.push({ kind: 'unmatchedSku', key: 'ord:sku:shared', row: regular, rows: [regular, unsent] });
    mode('open'); const n = get(unsent); assert(n); assert.equal(n.dataset.row, unsent.key); assert(n.querySelector('[data-cu-send]'));
    assert(!ids().includes(regular.order.receiptId)); assert.equal(R.count(), 1); assert.equal(R.actFor(unsent.key).row, unsent, 'the popup acts on the requested unsent order');
  });
  check('cloud-only sheet decision on a regular row is recovered with signer/time and usable history', () => {
    const cloud = row(4150000000, 'pooled', false); cloud._customSentDecision = { how: 'sheet', at: now - day, by: 'Paul', stamps: [{ how: 'sheet', at: now - day, by: 'Paul' }] };
    rows.push(cloud); w.B.orders.byKey.set(cloud.key, cloud); mode('sent');
    assert(ids().includes(cloud.order.receiptId)); assert.equal(get(cloud).querySelectorAll('.seal-sheet').length, 1);
    assert.equal(R.customItemFor(cloud.key).record.by, 'Paul'); assert.equal(R.isDecided(cloud.key), true);
    mode('open'); assert(!ids().includes(cloud.order.receiptId)); mode('done'); assert(!ids().includes(cloud.order.receiptId));
  });
  check('order lookup outside the pull accepts the authoritative sent-decision fallback row', () => {
    const archive = row(4140000000, 'written', false); archive._customSentDecision = { at: now - 6 * day, by: 'Seth' };
    const it = R.customItemFor(archive.key, archive); assert(it.decided); assert(!it.done); assert.equal(it.row, archive);
    assert.equal(it.record.at, archive._customSentDecision.at); assert.equal(it.record.by, 'Seth'); assert.equal(it.record.stamps[0].how, 'sheet');
  });
  check('hand completion/reopen controls remain available in Completed', () => {
    mode('done'); const n = get(hand); assert(n.querySelector('[data-cu-print]')); assert(n.querySelector('[data-cu-reopen]'));
    n.querySelector('[data-cu-reopen]').click(); assert.deepEqual(calls.at(-1), ['reopen', hand.key]);
  });
  check('the recalled GF 4174476673 partial without any send record stays Open with actual sheet/history access and no invented ink', () => {
    const original = decisions.get(gf.key); decisions.delete(gf.key); delete w.B.maps.customKept[gf.key];
    gf.poolIds = [gf.key + '_1', gf.key + '_2', gf.key + '_3']; gf.spec.quantity = 5; gf.state = 'written';
    w.B.review.items = w.B.review.items.filter(it => !it.rows?.includes(gf));
    const before = JSON.stringify(gf); mode('open'); const n = get(gf); assert(n, 'the unproven partial is still visible in Open');
    assert.equal(n.querySelectorAll('.seal').length, 0); assert(!n.querySelector('[data-cu-print],[data-cu-complete],[data-cu-send]'), 'already pooled copies do not offer another hand completion or send');
    const start = calls.length; n.querySelector('[data-cu-sheet]').click(); n.querySelector('[data-cu-history]').click();
    assert.deepEqual(calls.slice(start), [['sheet', gf.key], ['timeline', gf.key]]);
    assert.equal(R.isDecided(gf), false); assert.equal(R.customItemFor(gf.key).record, null);
    mode('sent'); assert(!ids().includes(gf.order.receiptId)); mode('done'); assert(!ids().includes(gf.order.receiptId));
    assert.equal(JSON.stringify(gf), before); assert.deepEqual(addCalls, []); decisions.set(gf.key, original);
  });
  check('a partial failed multi-line send keeps Retry visible and prevents competing print/completion even after its spinner clears', () => {
    const pooled = row(4120000000, 'pooled'), unpooled = row(4120000000, 'pulled'); unpooled.key += 'b';
    pooled.hold = 'save retry'; unpooled.hold = 'save retry'; pendingKeys.add(pooled.key); pendingKeys.add(unpooled.key);
    rows.push(pooled, unpooled); w.B.orders.byKey.set(pooled.key, pooled); w.B.orders.byKey.set(unpooled.key, unpooled);
    mode('open'); const n = get(pooled); assert(n); assert(n.querySelector('[data-cu-send]'), 'the pending send can retry');
    assert(!n.querySelector('[data-cu-print],[data-cu-complete]'), 'cannot compete with a retained send intent');
    const it = R.customItemFor(pooled.key); assert.equal(R.printable(it), false); assert(!it.decided && !it.done);
    mode('sent'); assert(!ids().includes(pooled.order.receiptId)); mode('done'); assert(!ids().includes(pooled.order.receiptId));
  });
  console.log(`PASS: ${checks} custom-send Review resilience checks.`);
} finally { w.close(); }
