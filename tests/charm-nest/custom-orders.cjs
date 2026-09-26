// Custom Orders (Paul, 25 Sep): every purchase that is not a regular listing — custom charms and pieces, rework, chain
// only, add-ons, private listings — is read as such (CharmNestOrders.specialOf) and grouped under Review → Custom Orders,
// once per line with its category; chain only is never pooled; a QR label printed from there is the sorting station's
// own sticker (QR Printer.html, the same object sorting.html hands it) and marks the line completed in the cloud
// (charmNestLibrary customPut), under the tab's Completed switch, where it can be printed again or reopened; a click on
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
    assert.equal((await call({ op: 'customDelete', key })).ok, true); assert.equal((await call({ op: 'customGet' })).records[key], undefined, 'reopened');
    console.log('  ✓ cloud records: put, print again, read, sandbox, reopen');
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
    assert(chips.some(t => /^Everything\d+/.test(t)));
    const state = await page.evaluate(() => ({ chain: B.orders.byKey.get('4176744752_41767447521').state, pool: B.orders.byKey.get('4176744752_41767447521').poolIds.length, decisions: Review.count() }));
    assert.equal(state.chain, 'noDesign', 'chain only never goes into the pool'); assert.equal(state.pool, 0);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const rows = () => page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(n => ({ rid: n.dataset.rid, cls: n.className, label: n.querySelector('.engravingIdentity .purchaseLabel')?.textContent, why: n.querySelector('.reviewReason')?.textContent, buttons: [...n.querySelectorAll('.rowActions button')].map(b => b.textContent.trim()) })));
    let list = await rows();
    assert.equal(list.length, 5, 'each custom line once: ' + JSON.stringify(list));
    for (const o of EXAMPLES) { const r = list.find(x => x.rid === o.receiptId); assert(r, o.receiptId + ' listed'); assert.equal(r.label, WANT[o.lines[0].sku], 'its category shows'); }
    const chainRow = list.find(x => x.rid === '4176744752');
    assert.match(chainRow.cls, /cuInfo/); assert.match(chainRow.why, /not laser cut/, 'the card says chain only is not cut');
    assert.deepEqual(chainRow.buttons, ['Print QR label'], 'nothing to decide: only its label');
    for (const r of list.filter(x => x.rid !== '4176744752')) assert.deepEqual(r.buttons, ['Print QR label', 'Review & resolve'], r.rid + ' keeps Review & resolve');
    const seg = await page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .rvSeg button')].map(b => b.textContent.trim()));
    assert.deepEqual(seg, ['Open5', 'Completed0'], 'the Open / Completed switch sits in the bar');
    const barH = await page.evaluate(() => document.querySelector('#reviewView .ordBar').getBoundingClientRect().height);
    assert(barH < 40, 'the bar stays one line: ' + barH);
    // Review & resolve still opens the question's own controls
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-review-open]');
    assert(await page.evaluate(() => !!document.querySelector('#rvList .reviewListRow[data-rid="4174476673"] .reviewDetails .cuStep [data-f=mat]')), 'the material question inside the custom card');
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-review-open]');
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-orders-open.png') });

    // print the chain-only sticker: the sorting station's label, then completed
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4176744752_41767447521'], null, { timeout: 30000 });
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
    assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .rvSeg button')].map(b => b.textContent.trim())), ['Open4', 'Completed1']);
    // (a print dialog closed without printing looks the same: the card stays, saying so, with an Undo for a while)
    assert.match(await page.textContent('#rvList .reviewListRow[data-rid="4176744752"] .rowActions'), /^Marked completed · Undo$/, 'the card itself says it was completed, with its Undo');

    // a decision printed by hand: completed, no longer a decision, never pooled
    const before = await page.evaluate(() => Review.count());
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-print]');
    await page.waitForFunction(() => B.maps.customDone['4174476673_41744766731'] && !document.querySelector('.cuStat'), null, { timeout: 30000 });
    const after = await page.evaluate(() => ({ n: Review.count(), st: B.orders.byKey.get('4174476673_41744766731').state, problems: B.orders.byKey.get('4174476673_41744766731').problems.length }));
    assert.equal(after.n, before - 1, 'its decision is gone'); assert.equal(after.st, 'noDesign'); assert.equal(after.problems, 0);
    // another sorter reopens it (the records come back without it): the line is a line to settle again, then completed again
    const flip = await page.evaluate(() => {
      const key = '4174476673_41744766731', row = B.orders.byKey.get(key), keep = B.maps.customDone, out = [];
      B.maps.customDone = Object.fromEntries(Object.entries(keep).filter(([k]) => k !== key)); Orders.interpretAll(); out.push([row.state, row.problems.length]);
      B.maps.customDone = keep; Orders.interpretAll(); out.push([row.state, row.problems.length]); Review.render(); return out;
    });
    assert.deepEqual(flip, [['pulled', 2], ['noDesign', 0]]);
    // the Undo lasts about 12 s, then the cards are Completed's alone
    await page.waitForFunction(() => !document.querySelector('#rvList .cuUndo'), null, { timeout: 20000 });
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    list = await rows();
    assert.deepEqual(list.map(x => x.rid).sort(), ['4174476673', '4176744752'], 'both under Completed');
    for (const r of list) { assert.match(r.cls, /cuDone/); assert.deepEqual(r.buttons, ['Print again', 'Reopen']); assert.match(r.why, /QR label printed by Test Operator/); }
    // a completed line whose SKU has since got a design (no longer special) is still listed, to be reopened
    assert(await page.evaluate(() => { const row = B.orders.byKey.get('4174476673_41744766731'), sp = row.spec.special; delete row.spec.special; Review.render(); const has = !!document.querySelector('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-reopen]'); row.spec.special = sp; Review.render(); return has; }), 'listed under Completed without its special reading');
    // another chip and back: Completed is still the one chosen
    await page.click('#reviewView .egTab[data-k=""]'); await page.click('#reviewView .egTab[data-k="customOrder"]');
    assert.equal(await page.evaluate(() => Review.view().cseg), 'done', 'the Open / Completed choice is kept');
    if (shots) await page.screenshot({ path: path.join(shots, 'custom-orders-completed.png') });
    // printed again: the same sticker, counted
    await page.click('#rvList .reviewListRow[data-rid="4176744752"] [data-cu-print]');
    await page.waitForFunction(() => (window.__printed || 0) >= 3 && !document.querySelector('.cuStat'), null, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').prints, 2);
    // reopened: back to Open, a decision again
    await page.click('#rvList .reviewListRow[data-rid="4174476673"] [data-cu-reopen]');
    await page.waitForFunction(() => !B.maps.customDone['4174476673_41744766731'] && !document.querySelector('.cuStat'), null, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4174476673_41744766731'), undefined, 'the record is gone');
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
    await page.waitForFunction(k => B.maps.customDone[k] && document.querySelector('#rvList .reviewListRow[data-rid="4177425406"] [data-cu-undo]'), hKey, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', hKey).printedBy, 'Test Operator', 'the name typed in the card is recorded');
    await page.click(huggies + ' [data-cu-undo]');
    await page.waitForFunction(k => !B.maps.customDone[k] && !document.querySelector('.cuStat'), hKey, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', hKey), undefined, 'Undo deletes the record');
    assert.equal(await page.evaluate(() => Review.count()), before, 'and its decision is back');
    assert.equal(await page.evaluate(() => !!window.__prompted), false, 'no prompt was opened');
    // a printer that fails marks nothing, and its card says so
    await page.evaluate(() => { window.__failPrint = true; });
    await page.click('#rvList .reviewListRow[data-rid="4176576272"] [data-cu-print]');
    await page.waitForFunction(() => /^Not printed/.test(document.querySelector('#rvList .reviewListRow[data-rid="4176576272"] .rowActions')?.textContent || '') && !document.querySelector('.cuStat'), null, { timeout: 30000 })
      .catch(async e => { throw new Error('the card says it was not printed: ' + await page.textContent('#rvList .reviewListRow[data-rid="4176576272"] .rowActions')); });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176576272_41765762721'), undefined, 'a label that did not print completes nothing');
    await page.evaluate(() => { window.__failPrint = false; });

    // the order window: everything about the order, its conversations and its notes
    await page.click('#rvList .reviewListRow[data-rid="4176576272"] .engravingIdentity');
    await page.waitForFunction(() => OrderWin.isOpen());
    const win = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, custom: document.getElementById('owCustom').hidden ? null : document.getElementById('owCustom').textContent,
      meta: [...document.querySelectorAll('#owMeta .m')].map(m => m.querySelector('i').textContent + ': ' + m.querySelector('span').textContent), fix: !!document.querySelector('#owFix .rvItem[data-kind=customOrder]'),
      note: document.querySelector('label[for=owNote]').textContent, tabs: [...document.querySelectorAll('#orderWin [data-ow-tab]')].map(b => [...b.children].map(c => c.textContent.trim()).filter(Boolean).join(' ')), dialogs: document.querySelectorAll('dialog[open]').length }));
    assert.match(win.title, /4176576272/); assert.match(win.custom || '', /Custom Orders · Rework/); assert.match(win.custom, /Print QR label/);
    for (const want of ['Order: 4176576272', 'Buyer: Buyer 6272', 'Price: 144', 'Custom order: Rework · SKU RE_5460', 'Title: MODIFICATION REWORK FREE SHIPPING']) assert(win.meta.includes(want), want + ' in ' + JSON.stringify(win.meta));
    assert(win.meta.some(m => /^Purchased: Sep 27, 2026/.test(m)), 'when it was bought: ' + JSON.stringify(win.meta));
    assert(win.fix, 'its decision, answered in the window'); assert.match(win.note, /^Order notes/); assert.deepEqual(win.tabs, ['Team internal', 'Customer on Etsy'], 'the team\'s thread and the customer\'s, side by side');
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

    // a reload: the workspace comes back (its lines, its decisions, the switch where it was) and what was completed with it
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    await page.evaluate(async () => { await Session.flush(); window.__oldPage = true; });
    await boot();
    await page.waitForFunction(() => B.orders.rows.length === 8 && document.querySelector('#rvList .reviewListRow.cuDone'), null, { timeout: 30000 });
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
    await page.waitForFunction(n => (window.__printed || 0) > n && !document.querySelector('.cuStat'), printedBefore, { timeout: 30000 });
    assert.equal(srv.st.doc('Charm_Custom_Orders', '4176744752_41767447521').prints, 3, 'printed from the record');
    assert.equal(await page.evaluate(() => JSON.parse(localStorage.getItem('qrPrintAll')).userTypedOrderNum), '4176744752', 'the same sticker');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Review → Custom Orders, QR label, Completed, Reopen, the order window and its notes (Chromium)');
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  await library();
  await browserChecks();
  console.log('Custom Orders OK: five examples classified, chain only never pooled, one card per line, sorting-station QR label → Completed → print again / reopen, order window and notes');
})().catch(e => { console.error(e); process.exit(1); });
