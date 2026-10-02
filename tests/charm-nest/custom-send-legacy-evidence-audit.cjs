// Independent cross-browser regression: no local custom design entry and no modern send receipt.
// Recover only from existing original timeline and custom-pool/source evidence through the real API,
// then execute the actual CustomSheet and Review factories. No shop data or browser automation.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { JSDOM } = require('jsdom');
const { start } = require('./bridge-server.cjs');
const root = path.join(__dirname, '../..'), bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const excerpt = (a, b) => { const i = bridge.indexOf(a), j = bridge.indexOf(b, i); assert(i >= 0 && j > i, a); return bridge.slice(i, j); };
const noop = () => {};
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
async function main() {
  const server = await start({ receipts: [] });
  const dom = new JSDOM('<div id="modeSeg"><button data-mode="nest"></button></div><section id="reviewView"></section>', { url: 'https://test.invalid', runScripts: 'outside-only', pretendToBeVisual: true });
  const w = dom.window, d = w.document, at = Date.now() - 86400000;
  const evidence = [
    { rid: '4174476673', line: '4174476673_1', metal: 'gold', at, by: 'paul', state: 'written', done: 3, path: 'charmnest/custom/4174476673/original.pdf', file: 'legacy-lion.ai' },
    { rid: '4171770802', line: '4171770802_1', metal: 'silver', at: at + 60000, by: 'Historic SS signer', state: 'pooled', done: 2, path: 'charmnest/custom/4171770802/original.pdf', file: 'legacy-silver.ai' }
  ];
  const unknown = { rid: '4170000999', line: '4170000999_1', metal: 'gold', at: at + 120000, by: '', state: 'written', done: 1, path: 'charmnest/custom/4170000999/original.pdf', file: 'unproven.ai' };
  const all = [...evidence, unknown], storedSheets = [];
  for (const rec of all) {
    const sheetId = 'legacy-sheet-' + rec.rid, sourceId = 'cust:legacy-' + rec.rid;
    const poolIds = Array.from({ length: 5 }, (_, i) => rec.line + '_' + (i + 1));
    for (const [i, poolId] of poolIds.entries()) server.st.put('Charm_Pool', poolId, { poolId, lineKey: rec.line, orderId: rec.rid, transactionId: '1', material: rec.metal,
      custom: true, customFile: rec.file, aiPath: rec.path, copy: i + 1, quantity: 5, state: i < rec.done ? 'written' : 'ready', sheetId: i < rec.done ? sheetId : null });
    const sheet = { id: sheetId, sheetId, metal: rec.metal, orders: [rec.rid], poolIds: poolIds.slice(0, rec.done),
      sources: [{ id: sourceId, name: rec.file, path: rec.path, custom: true }],
      charms: poolIds.slice(0, rec.done).map(poolId => ({ id: sourceId + ':' + poolId, poolId, sourceId, index: 0, custom: true, aiPath: rec.path, customCk: 'custom:' + rec.rid + ':original' })) };
    server.st.put('Charm_Nest_Sheets', sheetId, sheet); storedSheets.push(sheet);
    if (rec !== unknown) server.st.put('Order_Timeline', rec.rid + '~designSent~original-send', { orderId: rec.rid, type: 'designSent', at: rec.at, by: rec.by,
      source: 'sorter', station: 'sorter', lineKey: rec.line, transactionId: '1',
      text: 'Original custom designs sent to sheets', data: { files: [{ name: rec.file, qty: 5, metal: rec.metal, pieces: 1 }], pieces: 5, placed: true } });
  }
  const snapshot = () => JSON.stringify([...server.st.docs.entries()].sort(([a], [b]) => a.localeCompare(b)));
  const originalStore = snapshot(), calls = [], mutations = [], opens = [];
  const rows = all.map(rec => ({ key: rec.line, state: rec.state, poolIds: Array.from({ length: 5 }, (_, i) => rec.line + '_' + (i + 1)), problems: [],
    order: { receiptId: rec.rid, createTs: Math.floor((at - 86400000) / 1000) }, line: { sku: rec.rid === '4174476673' ? 'CUSTOM_6673' : 'CURB', title: 'Custom charm', transactionId: '1' },
    spec: { quantity: 5, material: rec.metal, designSku: rec.rid === '4174476673' ? 'CUSTOM_6673' : 'CURB', special: { label: 'Custom charm' } } }));
  w.matchMedia = () => ({ matches: true }); w.requestAnimationFrame = () => 1; w.setInterval = () => 1;
  w.Element.prototype.getAnimations = () => []; w.Element.prototype.scrollIntoView = noop;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-motion.js'), 'utf8')); w.Motion = null;
  w.eval(fs.readFileSync(path.join(root, 'charm-nest-activity.js'), 'utf8'));
  w.O = require(path.join(root, 'charm-nest-orders.js'));
  w.B = { customDesigns: {}, review: { items: rows.map(row => ({ kind: 'customOrder', key: 'ord:custom:' + row.order.receiptId + ':' + row.line.sku, row, rows: [row] })) },
    maps: { customDone: {}, customKept: {} }, orders: { rows, byKey: new Map(rows.map(row => [row.key, row])) }, pool: { rows: new Map(all.flatMap(rec => Array.from({ length: 5 }, (_, i) => { const id = rec.line + '_' + (i + 1); return [id, server.st.doc('Charm_Pool', id)]; }))) } };
  w.S = { settings: {}, cloud: { ok: true }, poolSources: {}, sources: [] };
  w.METALS = [{ key: 'gold', label: 'GF', color: '#a90' }, { key: 'silver', label: 'SS', color: '#999' }];
  w.MM = 25.4 / 72; w.labelOf = metal => metal === 'gold' ? 'GF' : 'SS'; w.stockFor = () => ({ wPt: 500, hPt: 500 });
  w.allSheets = () => storedSheets; w.employeeName = () => 'Different current viewer'; w.askEmployee = w.employeeName;
  w.Orders = { rows: () => rows, statePill: row => ['ok', row.state === 'written' ? '3/5 written' : '2/5 nested'] };
  w.Pool = { charmOf: poolId => storedSheets.flatMap(sheet => sheet.charms).find(charm => charm.poolId === poolId), sheetOf: poolId => storedSheets.find(sheet => sheet.poolIds.includes(poolId)),
    onSheets: () => true, settle: () => { mutations.push('pool-settle'); } };
  w.CustomPrint = { statusHtml: () => '', stamp: () => '', undoing: () => false, freshOf: () => 0, failNote: () => '',
    keptButtonHtml: (it, act, cls, text) => `<button data-cu-${act}>${text}</button>`, buttonHtml: (it, act, cls, text) => `<button data-cu-${act}>${text}</button>`, wire: () => null,
    print: () => mutations.push('print'), complete: () => mutations.push('complete') };
  w.CustomRead = { stamp: () => '', chip: () => '', bandOf: () => '', count: () => 0 };
  w.LiveStrip = { render: noop }; w.ListMedia = { pair: () => '', mount: noop, more: noop }; w.RunCtl = { renderBanner: noop };
  w.OrderWin = { open: (key, opts) => opens.push([key, opts?.view]), isOpen: () => false, openOrder: noop };
  w.Session = { schedule: noop, flushNow: async () => true }; w.TL = { line: () => mutations.push('timeline-write') };
  w.CharmNestAssets = { bytes: () => { throw new Error('metadata recovery must not read or remake artwork'); } };
  w.P = {}; w.esc = esc; w.fmtT = String; w.purchaseMarkup = () => ''; w.agent = noop; w.toast = noop;
  w.el = (tag, className, html) => { const element = d.createElement(tag); element.className = className || ''; element.innerHTML = html || ''; return element; };
  w.api = async (name, body) => {
    calls.push(JSON.parse(JSON.stringify(body))); assert.equal(name, 'charmNestLibrary'); assert.equal(body.op, 'customSheetGet', 'recovering an old send is read-only');
    const out = await server.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} });
    assert.equal(out.statusCode, 200, out.body); return JSON.parse(out.body);
  };
  w.Seal.press = w.Seal.pressPending = w.Seal.stampOn = () => { mutations.push('stamp'); };
  w.eval(excerpt('const CustomSheet = window.CustomSheet = (() => {', '/* ═══ 23b'));
  w.eval(excerpt('const Review = window.Review = (() => {', '/* ═══ 24b · Sandbox'));
  w.Review.repool = () => { mutations.push('repool'); };
  try {
    assert.equal(Object.keys(w.B.customDesigns).length, 0, 'the fresh browser has no local custom entry');
    assert.equal(server.st.list('Charm_Custom_Sheet').length, 0, 'no modern send receipt is seeded');
    await w.CustomSheet.load({ force: true });
    assert(calls.length > 0); assert(calls.some(call => evidence.every(rec => call.legacyLineKeys?.includes(rec.line))), 'the actual client requests bounded legacy evidence for already placed custom rows');
    for (const [i, rec] of evidence.entries()) {
      const decision = w.CustomSheet.decisionOf(rows[i]); assert(decision, 'the original recorded send is recovered for ' + rec.rid);
      assert.equal(decision.by, rec.by); assert.equal(decision.at, rec.at); assert.equal(decision.lines[rec.line].length, 5, 'original copy count survives');
      assert(w.CustomSheet.sentOf(rows[i]).legacy, 'inferred metadata is explicitly legacy');
      assert.equal(await w.CustomSheet.prepare(rows[i], null), null, 'legacy display metadata cannot manufacture new pool copies');
    }
    assert.equal(w.CustomSheet.decisionOf(rows[2]), null, 'custom pool placement alone does not invent a signed send');
    const mode = seg => { w.Review.view().cseg = seg; w.Review.view().q = ''; w.Review.view().filter = null; w.Review.render({ still: true }); };
    const shown = () => Array.from(d.querySelectorAll('#rvList .reviewListRow'), element => element.dataset.rid);
    mode('sent'); assert.deepEqual(shown(), ['4171770802', '4174476673']);
    for (const rec of evidence) {
      const card = d.querySelector('.reviewListRow[data-rid="' + rec.rid + '"]'), seal = card.querySelector('.seal-sheet');
      assert(seal); assert.equal(card.querySelectorAll('.seal').length, 1); assert.equal(seal.dataset.at, String(rec.at));
      const model = JSON.parse(seal.querySelector('svg').dataset.sealModel); assert.equal(model.by, rec.by); assert.equal(model.at, rec.at); assert.equal(model.action, 'SENT TO SHEET');
      assert.doesNotMatch(seal.querySelector('svg').textContent, /paul|Historic SS signer|Different current viewer|COMPLETE|QR LABEL/);
      assert(!card.querySelector('[data-cu-print],[data-cu-complete],[data-cu-send]'), 'recovered decisions cannot ask to approve again or claim completion');
      card.querySelector('[data-cu-sheet]').click(); card.querySelector('[data-cu-history]').click();
    }
    assert.deepEqual(opens, evidence.flatMap(rec => [[rec.line, 'sheet'], [rec.line, 'timeline']]));
    mode('done'); assert.deepEqual(shown(), [], 'no recovered send falsely becomes a completed order');
    mode('open'); assert.deepEqual(shown(), [unknown.rid], 'unproven metadata stays open');
    await w.CustomSheet.load({ force: true }); mode('sent'); assert.equal(d.querySelectorAll('#rvList .seal-sheet').length, 2, 'repeated reads cannot add another stamp');
    assert.deepEqual(mutations, [], 'recovery never stamps, pools, prints, completes or writes timeline history');
    assert.equal(snapshot(), originalStore, 'all original server records remain byte-for-byte unchanged');
    assert.equal(server.st.list('Charm_Custom_Sheet').length, 0, 'a read does not create a new receipt');
    console.log('PASS: fresh client with no local entry/modern receipt reads genuine original timeline+pool/source evidence through real API, places both partial orders only in Decided, preserves historical signer/time/copies and usable sheet/history controls; missing proof stays open; zero remote writes, stamps, repools, prints or false completions');
  } finally { w.close(); server.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
