// Custom Orders (Paul, 25 Sep): every purchase that is not a regular listing — custom charms and pieces, rework, chain
// only, add-ons, private listings — is read as such (CharmNestOrders.specialOf) and grouped under Review → Custom Orders,
// once per line with its category; chain only is never pooled; a QR label printed from there is the sorting station's
// own sticker (QR Printer.html, the same object sorting.html hands it) and marks the line completed in the cloud
// (charmNestLibrary customPut), under Review's Completed switch (one for every filter), where it can be printed again or reopened; a click on
// a card opens the order window with everything about the order and its notes.
// Part 1 runs in node. Part 2 opens the sorter in headless Chromium against the local fake site (bridge-server.cjs):
// every request that is not to the loopback is aborted, the label's PDF library is a stub that records the page it is
// given and whose print() only counts, so nothing is printed and nothing live is written.
//   node tests/charm-nest/custom-orders.cjs [playwright-core dir]   (CN_SHOTS=dir keeps screenshots)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, lines, extra) => Object.assign({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines }, extra || {});
const line = (tid, sku, title, variations, extra) => Object.assign({ transactionId: String(tid), listingId: String(1800000000 + (tid % 100000)), sku, title, quantity: 1, expectedShipDate: SHIP, variations: variations.map(([name, value]) => ({ name, value })), metalKey: '', metalLabel: '', personalization: [], buyerMessage: '' }, extra || {});
// Paul's five examples, as the sorter showed them on 25 Sep
const EXAMPLES = [
  order(4174476673, [line(41744766731, 'CUSTOM_6673', 'CUSTOM CHARM', [['Price', '28']])]),
  order(4176576272, [line(41765762721, 'RE_5460', 'MODIFICATION REWORK FREE SHIPPING', [['Price', '144']])]),
  order(4176744752, [line(41767447521, 'CHAIN_8941', 'CHAIN REPLACEMENT', [['Metal', 'Gold'], ['Length', '17 Inches']])]),
  order(4175423829, [line(41754238291, 'CUSTOM-N-001-665441', 'Custom Name Necklace, Personalized Gold Charm Necklace', [['Metal', '14k Gold Filled']], { metalKey: 'gold', metalLabel: 'GF 14/20' })]),
  order(4177425406, [line(41774254061, 'CUSTOM-H-020-660181', 'Custom Huggie Earrings, Dainty Charm Huggies', [['Metal', 'Sterling Silver']], { metalKey: 'silver', metalLabel: 'Sterling Silver' })])
];
const WANT = { CUSTOM_6673: 'Custom charm', RE_5460: 'Rework', CHAIN_8941: 'Chain only', 'CUSTOM-N-001-665441': 'Custom necklace', 'CUSTOM-H-020-660181': 'Custom huggies' };
// regular listings stay regular, however their titles read
const REGULAR = [
  order(4178000001, [line(41780000011, 'BLOOMING_20239', 'Blooming Flower Charm Necklace', [['Metal', '14k Gold Filled'], ['Length', '18 Inches']], { metalKey: 'gold', metalLabel: 'GF 14/20' })]),
  order(4178000002, [line(41780000021, 'BUNNY5', 'Bunny Charm with Gift Box', [['Metal', 'Sterling Silver']], { metalKey: 'silver', metalLabel: 'Sterling Silver' })]),
  order(4178000003, [line(41780000031, 'SEA_TURTLE', 'Sea Turtle Charm', [['Metal', 'Mystery alloy']])])   // metal not read: a question about its options
];
const master = new Set(['BLOOMING_20239', 'BUNNY5', 'SEA_TURTLE']);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: sku => (master.has(sku) ? { sku } : null) }, extra || {});
const kinds = sp => sp.problems.map(p => p.kind).sort().join(',');

/* ── 1 · node ── */
{
  for (const o of EXAMPLES) {
    const l = o.lines[0], sp = O.interpretLine(o, l, ctx());
    assert(sp.special, `${l.sku} is read as a special purchase`);
    assert.equal(sp.special.label, WANT[l.sku], `${l.sku} → ${WANT[l.sku]}`);
    assert(sp.special.why && sp.special.signals.length, 'it says why');
    assert(!sp.problems.some(p => p.kind === 'needsMapping' && /price/i.test(p.optionName)), `${l.sku}: a "Price" option is not an option to map (it was a second card under Options)`);
  }
  const chain = O.interpretLine(EXAMPLES[2], EXAMPLES[2].lines[0], ctx());
  assert.equal(chain.noDesign, true, 'chain only is never cut'); assert.equal(chain.problems.length, 0, 'and asks nothing'); assert.match(chain.noDesignWhy, /chain only · not laser cut/);
  assert.equal(chain.special.notCut, true);
  for (const o of REGULAR) { const sp = O.interpretLine(o, o.lines[0], ctx()); assert.equal(sp.special, undefined, `${o.lines[0].sku} is a regular listing`); assert.equal(sp.noDesign, false); }
  assert.equal(kinds(O.interpretLine(REGULAR[0], REGULAR[0].lines[0], ctx())), '', 'a regular line reads as before');
  assert.equal(kinds(O.interpretLine(REGULAR[2], REGULAR[2].lines[0], ctx())), 'needsMaterial', 'a regular line whose metal is not read still asks for it');
  // a regular design is never taken for a special one by a loose word in its SKU or title
  const loose = order(1, [line(11, 'CHAIN_HEART', 'Heart charm chain only option', [['Metal', 'Gold']])]);
  master.add('CHAIN_HEART'); assert.equal(O.interpretLine(loose, loose.lines[0], ctx()).special, undefined, 'a SKU in a master file is its design'); master.delete('CHAIN_HEART');
  // the buyer's own choice of no charm, an add-on, and a line with no SKU at all
  const opt = order(2, [line(21, 'BUTTERFLY', 'Butterfly Charm Necklace', [['Style', 'Chain only'], ['Metal', 'Gold']])]); master.add('BUTTERFLY');
  assert.equal(O.interpretLine(opt, opt.lines[0], ctx()).special.kind, 'chainOnly', 'the buyer chose the chain alone');
  const mapped = ctx({ optionMaps: { [opt.lines[0].listingId]: { style: { 'chain only': { field: 'form', value: 'necklace' } } } } });
  assert.equal(O.interpretLine(opt, opt.lines[0], mapped).special, undefined, 'unless a person mapped that value for the listing'); master.delete('BUTTERFLY');
  const box = order(3, [line(31, 'BOX-01', 'Gift box', [])]); assert.equal(O.interpretLine(box, box.lines[0], ctx()).special.label, 'Add-on');
  // a charm whose SKU is not indexed yet is an Unknown SKU question, however its title reads: only a chain product's own
  // SKU or title makes a line chain only (never cut), and words shop titles use for regular listings count only alone
  const special = (sku, title) => { const o = order(5, [line(51, sku, title, [['Metal', 'Gold']], { metalKey: 'gold' })]); const sp = O.interpretLine(o, o.lines[0], ctx()); return sp.special ? sp.special.kind : null; };
  assert.equal(special('CHAIN-HEART', 'Chain Link Heart Charm'), null);
  assert.equal(special('BUTTER_7', 'Butterfly Charm Necklace, Chain Only Option'), null);
  assert.equal(special('', 'Butterfly Charm Necklace, Chain Only Option'), null, 'nor with no SKU at all');
  assert.equal(special('BUNNY9', 'Bunny Charm Necklace with Gift Box'), null);
  assert.equal(special('NEWSKU_1', 'Custom Charm Necklace, Personalized Name Necklace'), null);
  assert.equal(special('NEWSKU_2', 'Made to Order Initial Necklace'), null);
  assert.equal(special('XYZ1', 'Chain Replacement, 18 inch'), 'chainOnly');
  assert.equal(special('CHAIN-17IN', 'Gold chain'), 'chainOnly');
  assert.equal(special('EXT-2IN', 'Extender'), 'chainOnly');
  assert.equal(special('', 'Gift Box'), 'addOn');
  assert.equal(special('', 'Rush Order Fee'), 'addOn');
  assert.equal(special('ABC9', 'Custom Order for Sarah, 3 charms'), 'customCharm');
  assert.equal(special('ABC6', 'Custom Order for Sarah'), 'customOther');
  assert.equal(special('ABC8', 'Add-on: second initial'), null, 'the words "add on" never make a line special: Claude reads it (Paul, 25 Sep 20:27)');
  assert.equal(special('FAIRY 3', 'Fairy Charm Add On Charm Gold Fairy Pendant'), null, 'a regular charm-only listing that says "add on"');
  assert.equal(special('ABC7', 'Re-engrave my charm'), 'rework');
  const ext = order(4, [line(41, '', 'Chain extender 2 inch', [])]); const se = O.interpretLine(ext, ext.lines[0], ctx());
  assert.equal(se.special.kind, 'chainOnly'); assert.equal(se.problems.length, 0, 'no "no SKU" question for a chain');
  // completed by hand (its QR label printed): finished, never pooled
  const done = O.interpretLine(EXAMPLES[0], EXAMPLES[0].lines[0], ctx({ customDone: { [O.lineKey(EXAMPLES[0], EXAMPLES[0].lines[0])]: { state: 'completed' } } }));
  assert.equal(done.noDesign, true); assert.equal(done.problems.length, 0); assert.match(done.noDesignWhy, /completed by hand/);
  // a finished line never holds its order
  const ev = O.evaluateOrder([{ key: 'k', state: 'noDesign', spec: chain, problems: [] }, { key: 'k2', state: 'committed', spec: { noDesign: false }, problems: [] }]);
  assert.equal(ev.committable, true, 'a chain-only line does not hold its order');
  // the sticker: the object sorting.html hands QR Printer.html for the same order
  const lab = O.sortingLabel(EXAMPLES[2], EXAMPLES[2].lines[0]);
  assert.equal(lab.userTypedOrderNum, '4176744752'); assert.equal(lab.dispatchDate, '02 Oct 2026'); assert.equal(lab.primaryItemIndex, 0);
  assert.deepEqual(lab.items[0].variations, [{ formatted_name: 'Metal', formatted_value: 'Gold' }, { formatted_name: 'Length', formatted_value: '17 Inches' }]);
  assert.deepEqual(lab.items[0].keywords, ['chain'], 'a chain is one dot, as the sorting station counts it');
  assert.equal(lab.items[0].receipt_id, 4176744752); assert.equal(lab.items[0].sku, 'CHAIN_8941'); assert.equal(lab.items[0].dispatch_date, '02 Oct 2026');
  assert.deepEqual(lab.notesBlock, { metal: 'Gold Filled', title4: 'CHAIN REPLACEMENT', matrixNums: '', matrixTitle: '' });
  assert.equal(O.sortingLabel(EXAMPLES[0], EXAMPLES[0].lines[0]).notesBlock.metal, 'No Metal');
  assert.equal(O.sortingMetal([{ name: 'Metal Choice', value: '14K Rose Gold Filled + Engraving' }]), 'Rose Gold');
  assert.equal(O.sortingMetal([{ name: 'Colour', value: 'Sterling Silver' }]), 'Silver');
  assert.equal(O.sortingLabel(Object.assign({}, EXAMPLES[1], { lines: [Object.assign({}, EXAMPLES[1].lines[0], { expectedShipDate: 0 })] })).dispatchDate, '02 Oct 2026', 'a restored line takes its order\'s ship-by date');
  assert.deepEqual(O.sortingLabel(EXAMPLES[4], EXAMPLES[4].lines[0]).items[0].keywords, ['earrings', 'huggie', 'huggies', 'huggie earrings'], 'huggies: the sorting station\'s own phrase hits (one dot: three of its kind to one of the other)');
  console.log('  ✓ classifier, completion and sticker');
}

// a custom design as a .dxf (a 22 × 20 mm rounded heart-ish tag drawn as a polyline with bulges, a hole, red engraving)
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DESIGN_DXF = DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, 18, 20, 0, 42, 0.4, 10, 18, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, 9, 20, 16, 40, 1.2, 0, 'LINE', 8, 'ENGRAVE', 62, 1, 10, 4, 20, 8, 11, 14, 21, 8, 0, 'ENDSEC', 0, 'EOF');
let DESIGN_AI = '';
async function designFixtures() {
  global.window = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
  const D = require(path.join(root, 'charm-nest-dxf.js'));
  const tag = DG(0, 'SECTION', 2, 'ENTITIES', 0, 'CIRCLE', 8, '0', 10, 0, 20, 0, 40, 7, 0, 'CIRCLE', 8, '0', 10, 0, 20, 5, 40, 1, 0, 'ENDSEC', 0, 'EOF');
  DESIGN_AI = Buffer.from((await D.toPdf(tag, 'tag.dxf')).bytes).toString('base64');   // an .ai is a PDF
}

async function library() {
  const srv = await start({ receipts: [] });
  try {
    const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body);
    const key = '4176744752_41767447521', label = O.sortingLabel(EXAMPLES[2], EXAMPLES[2].lines[0]);
    assert.equal((await call({ op: 'customGet' })).records[key], undefined);
    const a = await call({ op: 'customPut', key, by: 'Ann', label, receiptId: '4176744752', transactionId: '41767447521', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT', category: 'Chain only', kind: 'chainOnly' });
    assert.equal(a.ok, true); assert.equal(a.record.state, 'completed'); assert.equal(a.record.prints, 1); assert.equal(a.record.printedBy, 'Ann');
    assert.equal(a.record.label, undefined, 'the list every sorter reads carries no stickers'); assert.equal(a.record.hasLabel, true);
    assert.deepEqual(JSON.parse((await call({ op: 'customGet', key })).record.label), label, 'the sticker is kept, read by its line');
    const b = await call({ op: 'customPut', key, by: 'Bob', label, receiptId: '4176744752' });
    assert.equal(b.record.prints, 2, 'printed again'); assert.equal(b.record.printedBy, 'Ann', 'the first print is kept'); assert.equal(b.record.lastPrintedBy, 'Bob');
    const got = await call({ op: 'customGet' }); assert.equal(got.records[key].prints, 2); assert.equal(got.truncated, false); assert.equal(got.records[key].label, undefined); assert.equal(got.records[key].hasLabel, true);
    // the lines a sorter has pulled, read by their keys, without their stickers
    const byKeys = await call({ op: 'customGet', keys: [key, '1_9'] });
    assert.deepEqual(Object.keys(byKeys.records), [key]); assert.equal(byKeys.keys, 2); assert.equal(byKeys.records[key].label, undefined); assert.equal(byKeys.records[key].hasLabel, true);
    assert.match((await call({ op: 'customPut', key: 'a/b', by: 'x' })).error || '', /key/, 'a key that is not a line key is refused');
    // the sandbox keeps its own records
    await call({ op: 'customPut', key: '1_2', by: 'x', sandbox: true });
    assert(srv.st.doc('Sandbox_Charm_Custom_Orders', '1_2') && !srv.st.doc('Charm_Custom_Orders', '1_2'), 'sandboxed');
    // reopened (29 Sep): the record stays, open, with every seal; an older page's customDelete reopens the same way
    const ro = await call({ op: 'customDelete', key, by: 'Eve' }); assert.equal(ro.ok, true); assert.equal(ro.record.state, 'open');
    const kept = (await call({ op: 'customGet' })).records[key]; assert.equal(kept.state, 'open', 'reopened, kept'); assert.equal(kept.stamps.length, 2, 'its seals stay'); assert.deepEqual(kept.history.map(h => [h.how, h.by]), [['reopen', 'Eve']]);
    // Complete Order (27 Sep): completed with no label printed, so no print is counted; a print afterwards keeps who completed it
    const c = await call({ op: 'customPut', how: 'button', key, by: 'Cy', label, receiptId: '4176744752', sku: 'CHAIN_8941' });
    assert.equal(c.record.state, 'completed'); assert.equal(c.record.completedBy, 'Cy'); assert.equal(c.record.how, 'button'); assert(c.record.completedAt > 0);
    // (on the reopened record: no print counted, the two before the reopen kept, the Complete Order seal after them)
    assert.equal(c.record.prints, 2, 'no print counted'); assert.equal(c.record.lastPrintedBy, 'Bob'); assert.equal(c.record.hasLabel, true, 'its sticker is kept to print later');
    assert.equal(c.record.stamps.map(x => x.how).join(), 'print,print,button', 'every seal kept');
    const d = await call({ op: 'customPut', key, by: 'Di', label, receiptId: '4176744752' });
    assert.equal(d.record.prints, 3); assert.equal(d.record.completedBy, 'Cy', 'printed later: still completed by Cy'); assert.equal(d.record.how, 'button');
    assert.equal((await call({ op: 'customGet', keys: [key] })).records[key].completedBy, 'Cy', 'the lists carry who completed it');
    console.log('  ✓ cloud records: put, print again, read, sandbox, reopen, Complete Order');
  } finally { srv.close(); }
}

/* ── 2 · the sorter in Chromium ── */
async function browserChecks() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null;
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    const js = body => ({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin', 'Access-Control-Allow-Origin': '*' }, body });
    // the label's PDF library: records the page it is given; the "PDF" is a page whose print() only counts
    const PDFMAKE = `window.pdfMake = { createPdf(dd) { const top = window.parent; (top.__qrDocs = top.__qrDocs || []).push(JSON.parse(JSON.stringify(dd))); if (top.__failPrint) throw new Error('printer stub failure');
      return { getBlob(cb) { cb(new Blob(['<!doctype html><title>label</title><script>window.print = () => { window.parent.parent.__printed = (window.parent.parent.__printed || 0) + 1; };<\\/script>'], { type: 'text/html' })); } }; } };`;
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      const u = r.request().url();
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/pdfmake/.test(u)) return r.fulfill(js(PDFMAKE));
      if (/cdn\.jsdelivr\.net\/npm\/pdfmake@[^/]+\/build\/vfs_fonts/.test(u)) return r.fulfill(js(''));
      if (/qrcodejs/.test(u)) return r.fulfill(js(fs.readFileSync(path.join(root, 'lib/qrcode.min.js'))));
      if (/fonts\.googleapis|fonts\.gstatic/.test(u)) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    // (a reload is proven by a mark the old page carried and the new one does not)
    const boot = async () => {
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => !window.__oldPage && window.CN && window.Orders && window.Review && window.CustomPrint && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    };
    await boot();
    const orders = EXAMPLES.concat(REGULAR);
    await page.evaluate(async orders => {
      // as an arrival does (Arrivals.merge): the rows are added and read in one go, never left unread across an await
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, orders);
    const bar = () => page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => b.textContent.trim()));
    const chips = await bar();
    assert(chips.some(t => /^Custom Orders5$/.test(t)), 'one Custom Orders chip counting the five lines: ' + chips.join(' | '));
    assert(!chips.some(t => /Material/.test(t)), 'no Material chip any more');
    assert(chips.some(t => /^Options\d/.test(t)), 'a regular line whose metal was not read is under Options');
    assert(!chips.some(t => /^Everything/.test(t)), 'no Everything chip: Open is everything');
    const state = await page.evaluate(() => ({ chain: B.orders.byKey.get('4176744752_41767447521').state, pool: B.orders.byKey.get('4176744752_41767447521').poolIds.length, decisions: Review.count() }));
    assert.equal(state.chain, 'noDesign', 'chain only never goes into the pool'); assert.equal(state.pool, 0);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => ({ rid: n.dataset.rid, cls: n.className, label: n.querySelector('.engravingIdentity .purchaseLabel')?.textContent, why: n.querySelector('.reviewReason')?.textContent, buttons: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()) })));
    let list = await rows();
    assert.equal(list.length, 5, 'each custom line once: ' + JSON.stringify(list));
    for (const o of EXAMPLES) { const r = list.find(x => x.rid === o.receiptId); assert(r, o.receiptId + ' listed'); assert.equal(r.label, WANT[o.lines[0].sku], 'its category shows'); }
    const chainRow = list.find(x => x.rid === '4176744752');
    assert.match(chainRow.cls, /cuInfo/); assert.match(chainRow.why, /not laser cut/, 'the card says chain only is not cut');
    assert.deepEqual(chainRow.buttons, ['Print QR label', 'Complete Order'], 'nothing to decide and nothing to cut: its label, or completed as it is');
    // no Review & resolve on a custom card (Paul, 27 Sep 19:45): Complete Order, and Send to Sheet for its own designs
    for (const r of list.filter(x => x.rid !== '4176744752')) assert.deepEqual(r.buttons, ['Print QR label', 'Complete Order', 'Send to Sheet', 'Drop .ai / .dxf designs here'], r.rid + ': ' + r.buttons.join(', '));
    // one Open / Completed switch at the front of the bar, for every filter (Paul, 27 Sep)
    // no Everything chip (Paul, 27 Sep: it always read the same as Open)
    const seg = await page.evaluate(() => ({ first: document.querySelector('#reviewView .ordBar').firstElementChild.className, b: [...document.querySelectorAll('#reviewView .ordBar .rvSeg button')].map(b => b.textContent.trim()), all: !!document.querySelector('#reviewView .egTab[data-k=""]'), n: Review.count() }));
    assert.equal(seg.first, 'rvSeg', 'the switch comes first in the bar'); assert.equal(seg.all, false, 'no Everything chip');
    const openAll = +seg.b[0].replace('Open', '');
    assert.deepEqual(seg.b, ['Open' + openAll, 'Completed0']); assert(openAll >= 6, 'Open counts everything that waits: ' + JSON.stringify(seg));
    const barH = await page.evaluate(() => document.querySelector('#reviewView .ordBar').getBoundingClientRect().height);
    assert(barH < 40, 'the bar stays one line: ' + barH);
    // a click on the card opens the order window, which asks nothing (Paul, 29 Sep 01:01: no decision box in Review or
    // in any window): the card's buttons deal with it
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] .engravingIdentity');
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 10000 });
    assert.deepEqual(await page.evaluate(() => ({ box: !!document.querySelector('#owFix .rvItem, #owFix .cuStep'), fix: document.getElementById('owFix').innerHTML, words: /waiting on a decision/i.test(document.getElementById('orderWin').textContent), panel: !!document.querySelector('#rvList .reviewDetails') })), { box: false, fix: '', words: false, panel: false }, 'no question box, in the window or on the card');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-orders-open.png') });

    // print the chain-only sticker: the sorting station's label, then completed: the seal is pressed on the button and
    // the card flies to Completed, where a note says what arrived and offers Undo
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4176744752_41767447521'], null, { timeout: 30000 });
    await page.waitForFunction(() => document.querySelector('#motionLayer .mGhost .seal.seal-print'), null, { timeout: 5000 });
    if (shots) { await page.waitForTimeout(420); await page.screenshot({ path: path.join(shots, 'seal-stamp.png') }); await page.waitForTimeout(700); await page.screenshot({ path: path.join(shots, 'seal-stamped.png') }); await page.waitForTimeout(1500); await page.screenshot({ path: path.join(shots, 'seal-flight.png') }); }
    await page.waitForFunction(() => document.querySelector('.mNote') && !document.querySelector('#motionLayer .mGhost'), null, { timeout: 8000 });
    const note1 = await page.evaluate(() => ({ text: document.querySelector('.mNote .mNoteT').textContent, acts: [...document.querySelectorAll('.mNote .mNoteBtn')].map(b => b.textContent), card: !!document.querySelector('#rvList .reviewListRow[data-rid="4176744752"]'), stamped: document.querySelector('#rvList .reviewListRow[data-rid="4176744752"]') }));
    assert.equal(note1.text, 'Order 4176744752 moved to Completed · QR label printed by Test Operator'); assert.deepEqual(note1.acts, ['Undo', 'Show']);
    assert.equal(note1.card, false, 'the card left Open (it flew to Completed)');
    if (shots) await page.screenshot({ path: path.join(shots, 'seal-note.png') });
    const printed = await page.evaluate(() => ({ docs: window.__qrDocs, printed: window.__printed, handed: JSON.parse(localStorage.getItem('qrPrintAll')) }));
    assert.equal(printed.printed, 1, 'the print dialog was opened once (a stub: nothing printed)');
    const dd = printed.docs[0];
    assert.deepEqual(dd.pageSize, { width: 72, height: 72 }, '1 × 1 in'); assert.deepEqual(dd.pageMargins, [0, 0, 0, 0]);
    const at = (x, y) => dd.content.find(c => c.absolutePosition && c.absolutePosition.x === x && c.absolutePosition.y === y);
    assert.equal(at(2, 6).width, 28.9, 'the QR at 2,6, 28.9 pt'); assert.match(at(2, 6).image, /^data:image\/png/);
    assert.deepEqual([at(2, 39).text, at(2, 39).fontSize, at(2, 39).bold], ['02 Oct 2026', 4.7, true], 'dispatch date');
    assert.deepEqual([at(2, 46).text, at(2, 46).fontSize, at(2, 46).bold], ['4176744752', 4.7, true], 'order number');
    assert.equal(at(4, 2.75).canvas.length, 1, 'one dot for a chain');
    assert.match(JSON.stringify(at(33, 0.5)), /Gold Filled/, 'the notes column: metal'); assert.match(JSON.stringify(at(33, 0.5)), /CHAIN REPLACEMENT/);
    assert.equal(printed.handed.userTypedOrderNum, '4176744752'); assert.equal(printed.handed.items[0].sku, 'CHAIN_8941');
    const rec = srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521');
    assert(rec && rec.state === 'completed' && rec.prints === 1 && rec.printedBy === 'Test Operator' && rec.category === 'Chain only', 'marked completed in the cloud: ' + JSON.stringify(rec));
    await page.waitForFunction(() => !CustomPrint.busy('cinfo:custom:4176744752:CHAIN_8941'));
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .rvSeg button')].map(b => b.textContent.trim())), ['Open' + (openAll - 1), 'Completed1']);
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));

    // a decision printed by hand: completed, no longer a decision, never pooled
    const before = await page.evaluate(() => Review.count());
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4174476673_41744766731'] && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    const after = await page.evaluate(() => ({ n: Review.count(), st: B.orders.byKey.get('4174476673_41744766731').state, problems: B.orders.byKey.get('4174476673_41744766731').problems.length }));
    assert.equal(after.n, before - 1, 'its decision is gone'); assert.equal(after.st, 'noDesign'); assert.equal(after.problems, 0);
    // another sorter reopens it (the records come back without it): the line is a line to settle again, then completed again
    const flip = await page.evaluate(() => {
      const key = '4174476673_41744766731', row = B.orders.byKey.get(key), keep = B.maps.customDone, out = [];
      B.maps.customDone = Object.fromEntries(Object.entries(keep).filter(([k]) => k !== key)); Orders.interpretAll(); out.push([row.state, row.problems.length]);
      B.maps.customDone = keep; Orders.interpretAll(); out.push([row.state, row.problems.length]); Review.render(); return out;
    });
    assert.deepEqual(flip, [['pulled', 2], ['noDesign', 0]]);
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    list = await rows();
    // each carries its seal, the print button in the seal's colour
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => [n.dataset.rid, [...n.querySelectorAll('.sealRow .seal')].map(x => x.className.replace(/\s+/g, ' ').trim()), n.querySelector('[data-cu-print]').className.includes('sealedPrint')]).sort()),
      [['4174476673', ['seal seal-print'], true], ['4176744752', ['seal seal-print'], true]], 'one print seal each');
    assert.match(await page.getAttribute('#rvList .reviewListRow[data-rid="4176744752"] .seal', 'title'), /^QR label printed by Test Operator · /);
    assert.deepEqual(list.map(x => x.rid).sort(), ['4174476673', '4176744752'], 'both under Completed');
    // Completed holds every filter: pressing it again shows all it holds (here what Custom Orders does: nothing else was answered)
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    assert.equal(await page.evaluate(() => Review.view().filter), null, 'Completed pressed lets go of the filter');
    assert.deepEqual((await rows()).map(x => x.rid).sort(), ['4174476673', '4176744752'], 'all of Completed');
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => b.textContent.trim())), ['Custom Orders2'], 'only the kinds completed, with their counts');
    // a decision answered under another filter joins the same Completed folder, under its own chip (put back after)
    const opt = await page.evaluate(() => {
      const it = Review.items().find(x => x.kind === 'needsMaterial' || x.kind === 'needsMapping'); Review.remove(it.key, 'Test Operator'); Review.view().filter = null; Review.render();
      const out = { chips: [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(b => b.textContent.trim()), row: document.querySelector('#rvList .rvSettled .rowActions')?.textContent.trim(), seg: document.querySelector('#reviewView .rvSeg [data-cseg="done"]').textContent.trim() };
      document.querySelector('#reviewView .egTab[data-k="needsMapping"]').click(); out.only = [...document.querySelectorAll('#rvList .reviewListRow')].map(n => n.dataset.rid);
      Review.settled().shift(); Review.add(it); return out;
    });
    assert.deepEqual(opt.chips, ['Custom Orders2', 'Options1'], JSON.stringify(opt)); assert.equal(opt.seg, 'Completed3');
    assert.match(opt.row, /^Resolved\s*Test Operator · /, 'who answered it and when'); assert.deepEqual(opt.only, ['4178000003'], 'Options under Completed: its answer only');
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    for (const r of list) { assert.match(r.cls, /cuDone/); assert.deepEqual(r.buttons, ['Print again', 'Reopen']); assert.match(r.why, /QR label printed by Test Operator/); }
    // a completed line whose SKU has since got a design (no longer special) is still listed, to be reopened
    assert(await page.evaluate(() => { const row = B.orders.byKey.get('4174476673_41744766731'), sp = row.spec.special; delete row.spec.special; Review.render(); const has = !!document.querySelector('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-reopen]'); row.spec.special = sp; Review.render(); return has; }), 'listed under Completed without its special reading');
    // the chip pressed again lets go of it, and back: Completed is still the one chosen
    await page.click('#reviewView .egTab[data-k="customOrder"]'); assert.equal(await page.evaluate(() => Review.view().filter), null);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    assert.equal(await page.evaluate(() => Review.view().cseg), 'done', 'the Open / Completed choice is kept');
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-orders-completed.png') });
    // printed again: the same sticker, counted
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(() => (window.__printed || 0) >= 3 && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').prints, 2);
    // a second seal, pressed where it shows, beside the first
    await page.waitForFunction(() => document.querySelectorAll('#rvList .reviewListRow[data-rid="4176744752"] .seal.seal-print').length === 2 && !document.querySelector('.seal.pending') && !document.querySelector('#motionLayer .sealTool'), null, { timeout: 8000 });
    assert.deepEqual(srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').stamps.map(x => x.how), ['print', 'print']);
    if (shots) await page.screenshot({ path: path.join(shots, 'seal-two.png') });
    // a press on the seal over the button prints (the seal passes the press on); one elsewhere only wobbles
    const sealHit = await page.evaluate(() => { const n = document.querySelector('#rvList .reviewListRow[data-rid="4176744752"]'), s = n.querySelectorAll('.seal')[1], r = s.getBoundingClientRect(); const e = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return e && e.closest('.seal') === s; });
    assert(sealHit, 'the seal takes the pointer where it is (and passes a press over the button to it)');
    const pb0 = await page.evaluate(() => window.__printed || 0);
    { const s = await page.$$('#rvList .reviewListRow[data-rid="4176744752"] .seal'); const bb = await s[1].boundingBox(); await page.mouse.click(bb.x + bb.width * .7, bb.y + bb.height / 2); }
    await page.waitForTimeout(300);
    assert.equal(await page.evaluate(() => [window.__printed || 0, !!document.querySelector('.seal.wobble'), OrderWin.isOpen()].join()), [pb0, true, false].join(), 'a press on the seal away from the button prints nothing and opens nothing');
    // a cancelled order is never printed silently (adversarial, 28 Sep): the first press asks in the button itself (clay,
    // no pop-up), a second within 4 s prints, and the order's timeline says it was printed although cancelled
    await page.evaluate(() => Cancelled.absorb(['4176744752']));
    const pc0 = await page.evaluate(() => window.__printed || 0);
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForTimeout(400);
    const asked = await page.evaluate(() => { const b = document.querySelector('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]'); return [b.textContent, b.classList.contains('danger'), window.__printed || 0, document.querySelectorAll('dialog[open]').length].join(); });
    assert.equal(asked, ['Cancelled order: print anyway?', true, pc0, 0].join(), 'the first press prints nothing and asks in the button: ' + asked);
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(pc0 => (window.__printed || 0) > pc0 && !document.querySelector('.cuStat, .btn.working'), pc0, { timeout: 30000 });
    for (const t0 = Date.now(); !srv.st.list('Order_Timeline').some(x => x._id.startsWith('4176744752~note~cu-anyway-print-')); ) { if (Date.now() - t0 > 8000) throw new Error('no timeline note for the print despite the cancel'); await new Promise(r => setTimeout(r, 150)); }
    assert.match(srv.st.list('Order_Timeline').find(x => x._id.startsWith('4176744752~note~cu-anyway-print-')).text, /although the order is cancelled/);
    await page.evaluate(() => Cancelled.load(true));
    // reopened: back to Open, a decision again
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-reopen]');
    await page.waitForFunction(() => !B.maps.customDone['4174476673_41744766731'] && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4174476673_41744766731').state, 'open', 'the record is kept, open');
    assert(srv.st.doc('Charm_Custom_Orders', '4174476673_41744766731').stamps.length >= 1, 'with its seals');
    assert.equal(await page.evaluate(() => Review.count()), before, 'its decision is back');
    // printed from Open with no name saved: the name is asked in the card (never a prompt over the page), then the card
    // says "Marked completed · Undo", and Undo takes the completion back
    await page.click('#reviewView .rvSeg [data-cseg="open"]');
    await page.evaluate(() => { B.employee = ''; localStorage.removeItem('cn.employee'); window.prompt = () => { window.__prompted = true; return 'x'; }; });
    const huggies = '#rvList .reviewListRow[data-rid="4177425406"]', hKey = '4177425406_41774254061';
    await page.click(huggies + ' [data-cu-print]');
    await page.waitForSelector(huggies + ' [data-cu-name]');
    await page.fill(huggies + ' [data-cu-name]', 'Test Operator');
    await page.click(huggies + ' [data-cu-name-ok]');
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    await page.waitForFunction(k => B.maps.customDone[k] && [...document.querySelectorAll('.mNote .mNoteT')].some(n => /^Order 4177425406 moved to Completed/.test(n.textContent)), hKey, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', hKey).printedBy, 'Test Operator', 'the name typed in the card is recorded');
    await page.click('.mNote .mNoteBtn:text("Undo")');
    await page.waitForFunction(k => !B.maps.customDone[k] && !document.querySelector('.cuStat, .btn.working'), hKey, { timeout: 30000 })
      .catch(async e => { throw new Error('undo: ' + await page.evaluate(k => JSON.stringify({ done: !!B.maps.customDone[k], stat: [...document.querySelectorAll('.cuStat, .btn.working')].map(n => n.outerHTML.slice(0, 200)), toasts: [...document.querySelectorAll('#toasts .toast')].map(n => n.textContent), notes: [...document.querySelectorAll('.mNote')].map(n => n.textContent) }), hKey)); });
    // it flies back into Open from the Completed switch
    await page.waitForFunction(() => document.querySelector('#rvList .reviewListRow[data-rid="4177425406"]') && !document.querySelector('#motionLayer .mGhost'), null, { timeout: 8000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', hKey).state, 'open', 'Undo keeps the record, open'); assert.equal(srv.st.doc('Charm_Custom_Orders', hKey).stamps.length, 1, 'and its seal');
    assert.equal(await page.evaluate(() => Review.count()), before, 'and its decision is back');
    assert.equal(await page.evaluate(() => !!window.__prompted), false, 'no prompt was opened');
    // a printer that fails marks nothing, and its card says so
    await page.evaluate(() => { window.__failPrint = true; });
    await page.click('#rvList .reviewListRow[data-rid="4176576272"] [data-cu-print]');
    await page.waitForFunction(() => /^Not printed/.test(document.querySelector('#rvList .reviewListRow[data-rid="4176576272"] .rowActions')?.textContent || '') && !document.querySelector('.cuStat, .btn.working'), null, { timeout: 30000 })
      .catch(async e => { throw new Error('the card says it was not printed: ' + await page.textContent('#rvList .reviewListRow[data-rid="4176576272"] .rowActions')); });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176576272_41765762721'), undefined, 'a label that did not print completes nothing'); assert.equal(await page.evaluate(() => document.querySelectorAll('#rvList .reviewListRow[data-rid="4176576272"] .sealRow .seal.seal-print').length), 1, 'its seal stays on the button');
    await page.evaluate(() => { window.__failPrint = false; });

    // the order window: everything about the order, its conversations and its notes
    await page.click('#rvList .reviewListRow[data-rid="4176576272"] .engravingIdentity');
    await page.waitForFunction(() => OrderWin.isOpen());
    const win = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, custom: document.getElementById('owCustom').hidden ? null : document.getElementById('owCustom').textContent,
      meta: [...document.querySelectorAll('#owMeta .m')].map(m => m.querySelector('i').textContent + ': ' + m.querySelector('span').textContent), fix: !!document.querySelector('#owFix .rvItem[data-kind=customOrder]'),
      note: document.querySelector('label[for=owNote]').textContent, tabs: [...document.querySelectorAll('#orderWin [data-ow-tab]')].map(b => [...b.children].map(c => c.textContent.trim()).filter(Boolean).join(' ')), dialogs: document.querySelectorAll('dialog[open]').length }));
    assert.match(win.title, /4176576272/); assert.match(win.custom || '', /Custom Orders · Rework/); assert.match(win.custom, /Retry print/, "the print that did not open keeps its seal and offers Retry print (28 Sep)");
    for (const want of ['Order: 4176576272', 'Buyer: Buyer 6272', 'Price: 144', 'Custom order: Rework · SKU RE_5460', 'Title: MODIFICATION REWORK FREE SHIPPING']) assert(win.meta.includes(want), want + ' in ' + JSON.stringify(win.meta));
    assert(win.meta.some(m => /^Purchased: Sep 27, 2026/.test(m)), 'when it was bought: ' + JSON.stringify(win.meta));
    assert.equal(win.fix, false, 'no decision box in the window (29 Sep 01:01)'); assert.match(win.note, /^Order notes/); assert.deepEqual(win.tabs, ['Team internal', 'Customer on Etsy'], 'the team\'s thread and the customer\'s, side by side');
    assert.equal(win.dialogs, 1, 'one window, never one on top of another');
    // a note left for the next person is saved to the order
    await page.fill('#owNote', 'Customer wants the old chain back — call before shipping');
    await page.waitForFunction(() => document.getElementById('owNote').classList.contains('has'));
    await page.waitForTimeout(1200);
    assert.equal((srv.st.doc('Brites_Orders', '4176576272') || {})['Staff Note'], 'Customer wants the old chain back — call before shipping', 'saved to the order');
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-orders-window.png') });
    await page.click('#owClose');
    // another station writes a note; the next person to open the order sees it
    srv.st.put('Brites_Orders', '4175423829', { 'Staff Note': 'Engrave on the back only (Ann, station 2)' });
    await page.evaluate(() => OrderWin.open('4175423829_41754238291'));
    await page.waitForFunction(() => document.getElementById('owNote').value === 'Engrave on the back only (Ann, station 2)', null, { timeout: 10000 });
    await page.click('#owClose');

    // Complete Order: completed at once with no label printed, "Completed by" under Completed, and reopened again
    await page.click('#rvList .reviewListRow[data-rid="4177425406"] [data-cu-complete]');
    await page.waitForFunction(k => B.maps.customDone[k] && document.querySelector('#motionLayer .mGhost .seal.seal-button'), hKey, { timeout: 30000 });
    if (shots) { await page.waitForTimeout(1100); await page.screenshot({ path: path.join(shots, 'seal-complete.png') }); }
    await page.waitForFunction(() => /moved to Completed · completed by Test Operator with no label printed$/.test(document.querySelector('.mNote .mNoteT')?.textContent || ''), null, { timeout: 8000 });
    const byButton = srv.st.doc('Charm_Custom_Orders', hKey);
    // (its print before the Undo is kept: that label was printed, and its seal stays with the order)
    assert(byButton && byButton.how === 'button' && byButton.completedBy === 'Test Operator' && byButton.prints === 1 && byButton.stamps.map(x => x.how).join() === 'print,button', 'completed without a new print: ' + JSON.stringify(byButton));
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    assert.match(await page.textContent('#rvList .reviewListRow[data-rid="4177425406"] .reviewReason'), /^Completed by Test Operator · /, 'who completed it and when');
    assert.deepEqual(await page.evaluate(() => { const n = document.querySelector('#rvList .reviewListRow[data-rid="4177425406"]'); return [[...n.querySelectorAll('.seal')].map(x => x.classList.contains('seal-button')), n.querySelector('[data-cu-print]').textContent]; }), [[false, true], 'Print again'], 'the kept print seal, then the Complete Order seal');
    await page.click('#rvList .reviewListRow[data-rid="4177425406"] [data-cu-reopen]');
    await page.waitForFunction(k => !B.maps.customDone[k] && !document.querySelector('.cuStat, .btn.working'), hKey, { timeout: 30000 });
    await page.click('#reviewView .rvSeg [data-cseg="open"]');

    // a custom order's own designs: .ai / .dxf dropped on its card, a metal picked for each in one window, Send to Sheet
    const neck = '#rvList .reviewListRow[data-rid="4175423829"]', nKey = '4175423829_41754238291';
    const drop = (sel, files) => page.evaluate(({ sel, files }) => {
      const dt = new DataTransfer();
      for (const f of files) dt.items.add(new File([f.b64 ? Uint8Array.from(atob(f.b64), c => c.charCodeAt(0)) : f.text], f.name));
      const n = document.querySelector(sel);
      for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt }));
    }, { sel, files });
    await drop(neck, [{ name: 'heart-name.dxf', text: DESIGN_DXF }, { name: 'notes.txt', text: 'not a design' }]);
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
    const d1 = await page.evaluate(() => ({ rows: [...document.querySelectorAll('#cuDlg .cuFile')].map(n => ({ name: n.querySelector('.cuMeta b').textContent, meta: n.querySelector('.cuMeta .dim').textContent, on: [...n.querySelectorAll('.cuM.on')].map(b => b.textContent) })), title: document.getElementById('cuDlgT').textContent, send: document.querySelector('#cuDlg [data-send]').disabled, dialogs: document.querySelectorAll('dialog[open]').length, sheetsBefore: allSheets().reduce((n, p) => n + p.charms.length, 0) }));
    assert.equal(d1.title, 'Designs for order 4175423829'); assert.equal(d1.dialogs, 1, 'one window');
    assert.equal(d1.rows.length, 1, 'the .txt is left out'); assert.equal(d1.rows[0].name, 'heart-name.dxf'); assert.deepEqual(d1.rows[0].on, ['GF 14/20'], 'the order\'s metal first');
    assert.match(d1.rows[0].meta, /^22\.\d × 20\.\d mm · 1 piece · DXF in mm$/, 'its size as traced: ' + d1.rows[0].meta);
    assert.equal(d1.send, false, 'ready to send');
    assert.equal(await page.evaluate(() => document.getElementById('dropAll').classList.contains('on')), false, 'the page\'s own drop veil is gone after the drop');
    // a second design, an .ai, dropped on the window itself, put on Sterling Silver and cut twice
    await drop('#cuDlg', [{ name: 'Tag Back.ai', b64: DESIGN_AI }]);
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg .cuFile .cuThumb img').length === 2, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile:nth-child(2) .cuM[data-m="silver"]');
    await page.click('#cuDlg .cuFile:nth-child(2) [data-q="1"]');
    assert.deepEqual(await page.evaluate(() => ({ sum: document.getElementById('cuSum').textContent, all: !document.querySelector('#cuDlg .cuAll').hidden })), { sum: '2 designs · 3 pieces to cut', all: true });
    if (shots) { await page.waitForTimeout(400); await page.screenshot({ path: path.join(shots, 'custom-designs-window.png') }); }
    await page.click('#cuDlg [data-x]');
    const strip = await page.evaluate(sel => ({ strip: document.querySelector(sel + ' .cuDesigns')?.textContent, gold: document.querySelector(sel + ' [data-cu-send]')?.className }), neck);
    assert.match(strip.strip, /2 designs · 3 pieces/); assert.match(strip.gold, /gold/, 'Send to Sheet is the next step');
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-designs-card.png') });
    await page.click(neck + ' [data-cu-send]');
    await page.waitForFunction(k => CustomSheet.sentOf(B.orders.byKey.get(k)) && !document.querySelector('#rvList .cuStat, #rvList .btn.working'), nKey, { timeout: 30000 });
    // no run here: the line waits for the next one, settled (its SKU asks nothing); a run puts it on as any line
    const w = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { st: r.state, problems: r.problems.length, eng: r.spec.engraveCandidate, custom: Review.count() }; }, nKey);
    assert.deepEqual([w.st, w.problems, w.eng], ['pulled', 0, false]);
    assert(srv.st.blobs && [...srv.st.blobs.keys()].some(k => /charmnest\/(sandbox\/)?custom\/4175423829\//.test(k)), 'each design copied to the cloud');
    const placed = await page.evaluate(async k => {
      const row = B.orders.byKey.get(k); await Pool.poolAdd(row, null); Orders.interpretAll(); Review.render();
      const pieces = allSheets().flatMap(p => p.charms.filter(c => c.custom).map(c => [p.metal, c.name]));
      return { st: row.state, ids: row.poolIds, pieces, pools: row.poolIds.map(id => B.pool.rows.get(id)).map(p => [p.material, p.custom, p.customFile]), tag: [...document.querySelectorAll('.cuTag')].map(t => t.textContent),
        why: document.querySelector('#rvList .reviewListRow[data-rid="4175423829"] .reviewReason')?.textContent, sent: !!document.querySelector('#rvList .reviewListRow[data-rid="4175423829"] .cuDesigns.sent') };
    }, nKey);
    assert.equal(placed.st, 'pooled'); assert.deepEqual(placed.ids, ['4175423829_41754238291_1', '4175423829_41754238291_2', '4175423829_41754238291_3']);
    assert.deepEqual(placed.pieces.map(p => p[0]).sort(), ['gold', 'silver', 'silver'], 'each design on its own metal: ' + JSON.stringify(placed.pieces));
    assert.deepEqual(placed.pools, [['gold', true, 'heart-name.dxf'], ['silver', true, 'Tag Back.ai'], ['silver', true, 'Tag Back.ai']]);
    assert(placed.tag.includes('1 custom') && placed.tag.includes('2 custom'), 'each sheet says how many custom pieces it holds: ' + placed.tag);
    assert.match(placed.why, /its own designs, on their way to the laser/); assert(placed.sent, 'the card shows its designs as sent');
    // a drop anywhere else in Review starts nothing
    await drop('#reviewView .ordBar', [{ name: 'stray.ai', b64: DESIGN_AI }]);
    assert.deepEqual(await page.evaluate(() => [S.sources.length, document.getElementById('dropAll').classList.contains('on')]), [0, false], 'not taken as a manual nest job, and no veil left over');

    // a reload: the workspace comes back (its lines, its decisions, the switch where it was) and what was completed with it
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    // (the Send tour above may still be playing: a press on the Review it comes home to is the person's own, it goes
    // through, and the tour skips after it. Skipped at the press, the list was laid out anew under the finger and the
    // click was lost: the switch stayed on Open, and the reload below came back on Open)
    assert.equal(await page.evaluate(() => Review.view().cseg), 'done', 'the Completed press went through while the tour played');
    await page.evaluate(async () => { await Session.flush(); window.__oldPage = true; });
    await boot();
    await page.waitForFunction(() => B.orders.rows.length === 8 && document.querySelector('#rvList .reviewListRow.cuDone'), null, { timeout: 30000 });
    assert.deepEqual(await page.evaluate(k => ({ sent: !!CustomSheet.sentOf(B.orders.byKey.get(k)), pieces: allSheets().reduce((n, p) => n + p.charms.filter(c => c.custom).length, 0), files: Object.values(B.customDesigns)[0].files.map(F => F.bytes.length > 100) }), nKey), { sent: true, pieces: 3, files: [true, true] }, 'the designs and their pieces survive a reload');
    list = await rows();
    assert.deepEqual(list.map(x => x.rid), ['4176744752'], 'completed survives a reload'); assert.deepEqual(list[0].buttons, ['Print again', 'Reopen']);
    assert.deepEqual(await page.evaluate(() => ({ st: B.orders.byKey.get('4176744752_41767447521').state, v: Review.view().cseg })), { st: 'noDesign', v: 'done' });
    // its order has left the pull (shipped, or another day's pull): its label can still be printed, from the record
    await page.evaluate(() => { B.orders.rows = B.orders.rows.filter(r => r.order.receiptId !== '4176744752'); B.orders.byKey.delete('4176744752_41767447521'); Review.syncOrderItems(); Review.render(); });
    await page.waitForFunction(() => { const n = document.querySelector('#rvList .reviewListRow.cuDone'); return n && !n.querySelector('[data-cu-reopen]'); });
    list = await rows();
    assert.deepEqual(list.map(x => x.rid), ['4176744752']); assert.deepEqual(list[0].buttons, ['Print again'], 'no line to reopen');
    const printedBefore = await page.evaluate(() => window.__printed || 0);
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(n => (window.__printed || 0) > n && !document.querySelector('.cuStat, .btn.working'), printedBefore, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').prints, 4, 'printed from the record (the fourth: one was printed despite a cancel above)');
    assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('qrPrintAll')).userTypedOrderNum), '4176744752', 'the same sticker');
    // an Options card: no "Review & resolve" and no question anywhere (Paul, 29 Sep 00:38 and 01:01); a click opens its
    // order, whose window asks nothing, and the card stays in Review for its buttons
    await page.click('#reviewView .rvSeg [data-cseg="open"]');
    const optCard = '#rvList .reviewListRow[data-rid="4178000003"]';
    const optKey = await page.getAttribute(optCard, 'data-mkey');
    assert.equal(await page.$(optCard + ' [data-review-open]'), null, 'no Review & resolve');
    await page.click(optCard + ' .engravingIdentity');
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 10000 });
    assert.deepEqual(await page.evaluate(() => ({ fix: document.getElementById('owFix').innerHTML, hold: !!document.querySelector('#orderWin [data-a=hold]'), panel: !!document.querySelector('#rvList .reviewDetails') })), { fix: '', hold: false, panel: false }, 'nothing asked');
    await page.click('#owClose'); await page.waitForFunction(() => !OrderWin.isOpen());
    assert.equal(await page.evaluate(k => [...document.querySelectorAll('#rvList .reviewListRow')].some(n => n.dataset.mkey === k), optKey), true, 'it stays in Review');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Review → Custom Orders, QR label, Completed, Reopen, the order window and its notes (Chromium)');
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  await library();
  await designFixtures();
  await browserChecks();
  console.log('Custom Orders OK: five examples classified, chain only never pooled, one card per line, sorting-station QR label → Completed → print again / reopen, order window and notes');
})().catch(e => { console.error(e); process.exit(1); });
