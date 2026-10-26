// A pre-made piece (PREMADE; Paul, 12 Oct 2026, order 4174384672, SKU DAINTY_1450, Etsy listing 1096331450 "Dainty Circle Choker Necklace"):
// "For this listing we do not laser print the ring. This is a pre-made piece and should be flagged as Custom tab in the Review tab. Do not match this SKU with a repo charm."
// The master DOES hold a DAINTY_1450 (the double ring), which every line of the listing used to take. A person's word for the LISTING (Firebase Charm_Sku_Aliases/{listing id}.listingKind,
// written by the page's op listingKindPut, kind "custom", with the words the person left in `note`) makes every order of the listing a Custom Order that never takes a repo design, whatever
// SKU a variation carries. No code list: the next pre-made listing is one more word, not one more commit.
// Offline: no Etsy call, no paid AI reading, nothing live is written (the cloud is the local fake, bridge-server.cjs). The three lines are the Sep 17 snapshot's own lines (listing, SKU,
// options, quantity as Etsy sent them; no buyer names).
//   node tests/charm-nest/premade-listing.cjs [playwright-core dir]
// Part 1 (node): the line reads as a custom order with its reason in words, whatever the master holds (and any SKU the listing carries: variation SKUs, none); other listings of the same SKU
//   are untouched; the golden replay of the snapshot (only the two lines of the listing change); what the order waits for before Complete Order and nothing after it (evaluateOrder,
//   CharmNestReadiness.orderReports, OrderPieces); the server op keeps the word (and the person's note) on the listing's alias document and leaves the other listings' answers alone.
// Part 2 (Chromium, the real sorter on the fake site, the word read through aliasGet as the live page reads it): Review → Custom Orders card for the ring (reason line, buttons, Send to Sheet greyed,
//   no master drawing, the Etsy photo), the Orders table, the order window (SKU, no drawing, pieces), Complete Order (customPut only: no pool, no sheet, no cut), the order after it.
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const R = require(path.join(root, 'charm-nest-readiness.js'));
const OP = require(path.join(root, 'charm-nest-order-pieces.js'));
const { start } = require('./bridge-server.cjs');
const noNested = require('./_noNestedArrays.cjs');
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/addon-snapshot-lines.json'), 'utf8'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 30 * DAY, updateTs: SHIP - 30 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (f, extra) => Object.assign({ transactionId: f.tid, listingId: f.listing, sku: f.sku, title: f.title, quantity: f.q, expectedShipDate: SHIP, variations: f.vars.map(([name, value]) => ({ name, value })), metalKey: f.mk, metalLabel: f.mk, personalization: [], buyerMessage: f.msg ? 'x' : '' }, extra || {});
const KNOWN = FIX.known;
const entryOf = s => { const k = KNOWN[String(s).toUpperCase()]; return k ? Object.assign({ sku: s }, k.blocked ? { blocked: k.blocked } : {}, k.sizes ? { sizes: Object.fromEntries(k.sizes.map(z => [z, {}])) } : {}, k.pair ? { pair: k.pair } : {}, k.facing ? { facing: k.facing } : {}, k.sym ? { sym: k.sym } : {}) : null; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryOf, masterLoose: s => FIX.loose[String(s)] || '' }, extra || {});
const rowOf = (rid, tid) => FIX.lines.find(l => l.rid === rid && l.tid === tid);
const RING18 = rowOf('4174384672', '5216801873'), RING16 = rowOf('4173025188', '5215239593'), CHAIN = rowOf('4174384672', '5216801871');
const LISTING = '1096331450', NOTE = 'pre-made piece, not laser cut: Paul, 12 Oct 2026';
const WORD = { kind: 'custom', by: 'PREMADE', at: 1791830000000, title: 'Dainty Circle Choker Necklace', note: NOTE };
const WHY = `listing ${LISTING}: PREMADE said every order of it is a custom order (${NOTE})`;
const spec = (f, c, extra) => O.interpretLine(order(f.rid, []), line(f, extra), c || ctx());
const flagged = extra => ctx(Object.assign({ aliases: { [LISTING]: { listingKind: WORD } } }, extra || {}));
const KEY18 = '4174384672_5216801873', KEY_CHAIN = '4174384672_5216801871';

assert(RING18 && RING16 && CHAIN, 'the snapshot holds the three lines');
assert.deepEqual([RING18.listing, RING18.sku, RING18.q, JSON.stringify(RING18.vars)], [LISTING, 'Dainty_1450', 1, '[["Metal Choice / Necklace Length","Gold - 18\\""]]'], 'the ring of order 4174384672');
assert.deepEqual([RING16.listing, RING16.sku, JSON.stringify(RING16.vars)], [LISTING, 'Dainty_1450', '[["Metal Choice / Necklace Length","Gold - 16\\""]]'], 'the same listing, another order, another length');
assert.equal(CHAIN.listing, '1866902577'); assert.equal(CHAIN.sku, 'Cable Chain only', 'the order\'s second piece is another listing: a chain');
assert(KNOWN.DAINTY_1450, 'the master holds a DAINTY_1450 (the double ring): the exact SKU match that must lose');

/* ── 1 · node ── */
{
  // the control: with no word the ring is the master's DAINTY_1450, as it always was
  const was = spec(RING18);
  assert.equal(was.special, undefined); assert.equal(was.designSku, 'DAINTY_1450'); assert.equal(was.skuSource, 'transaction'); assert.deepEqual(was.problems, []); assert.equal(was.ownDesign, undefined);
  // the word: a custom order that takes no repo design, whatever the master holds
  for (const f of [RING18, RING16]) {
    const sp = spec(f, flagged(), { personalization: ['size 7 please'] });
    assert.equal(sp.special.kind, 'customNecklace'); assert.equal(sp.special.label, 'Custom necklace'); assert.deepEqual(sp.special.signals, ['listing']); assert.equal(sp.special.own, true);
    assert.equal(sp.special.why, WHY, 'one plain reason that names why, with the words the person left');
    assert.equal(sp.ownDesign, true, 'its design is the order\'s own, never the master\'s DAINTY_1450'); assert.equal(sp.noDesign, false, 'nothing closes the order until a person completes it or sends designs');
    assert.deepEqual(sp.problems, [], 'no metal, SKU, option or engraving question'); assert.equal(sp.readable, false, 'no paid reading'); assert.equal(sp.engraveCandidate, false);
    assert.equal(sp.formHold, undefined, 'the form guard never holds it either'); assert.equal(sp.pieceCount, 1); assert.equal(sp.quantity, 1);
  }
  // a person's saved answer for this SKU on the listing (the old way to "match" it) does not bring the design back
  const matched = spec(RING18, flagged({ aliases: { [LISTING]: { sku: 'DAINTY_1450', v: 2, bySku: { DAINTY_1450: 'DAINTY_1450' }, listingKind: WORD } } }));
  assert.deepEqual([matched.special.kind, matched.ownDesign, matched.problems.length], ['customNecklace', true, 0], 'a saved SKU answer on the listing does not stand over the word');
  // every variation of the listing: another SKU, none, a quantity of 2
  const vars = [['a SKU the master has not', { sku: 'DAINTY_1450-RG', vars: [['Metal Choice / Necklace Length', 'Rose Gold - 20"']], mk: 'rose' }], ['no SKU at all', { sku: '' }], ['quantity 2', { q: 2 }], ['a lower-case SKU', { sku: 'dainty_1450' }]];
  for (const [what, over] of vars) {
    const sp = spec(Object.assign({}, RING18, over), flagged());
    assert.deepEqual([sp.special && sp.special.kind, sp.ownDesign, sp.problems.length, sp.noDesign], ['customNecklace', true, 0, false], what + ': ' + JSON.stringify(sp.problems));
    assert.equal(sp.special.why, WHY, what);
  }
  // quantity 2: two pieces ordered, ONE line: Complete Order completes the line, so both pieces are resolved by hand and none is wanted on a sheet
  const two = spec(Object.assign({}, RING18, { q: 2 }), flagged()); assert.deepEqual([two.quantity, two.pieceCount, two.ownDesign, two.problems.length], [2, 2, true, 0]);
  // the word is for the LISTING: another listing that happens to carry the same SKU is still the master's design, and the master entry is untouched
  const other = spec(Object.assign({}, RING18, { listing: '555' }), flagged());
  assert.deepEqual([other.special, other.designSku, other.problems.length], [undefined, 'DAINTY_1450', 0], 'another listing of DAINTY_1450 is untouched');
  assert.equal(O.specialOf.addOn.listings[LISTING], undefined, 'no list in the code: the word lives in Firebase');
  // taking the word out ("regular", or removing it) gives the line back to the master
  for (const kd of [{ kind: 'regular', by: 'Paul' }, undefined]) { const sp = spec(RING18, ctx({ aliases: { [LISTING]: { listingKind: kd } } })); assert.deepEqual([sp.special, sp.designSku, sp.ownDesign], [undefined, 'DAINTY_1450', undefined]); }
  // the wording system: a note the person left stands in the reason (at most 90 characters, one line), none leaves the plain sentence
  assert.equal(spec(RING18, ctx({ aliases: { [LISTING]: { listingKind: { kind: 'custom', by: 'Paul' } } } })).special.why, `listing ${LISTING}: Paul said every order of it is a custom order`);
  assert.equal(spec(RING18, ctx({ aliases: { [LISTING]: { listingKind: { kind: 'custom', by: 'Paul', note: 'x'.repeat(200) + '\n y' } } } })).special.why, `listing ${LISTING}: Paul said every order of it is a custom order (${'x'.repeat(90)})`);
  assert.doesNotMatch(O.specialOf.addOn.wait, /add-on/, 'the wait says "custom listing", not "add-on": ' + O.specialOf.addOn.wait);

  // the golden replay: with no word every one of the 411 lines reads as before; with the word only the two lines of the listing change
  const sig = s => JSON.stringify({ special: s.special ? s.special.kind + '/' + s.special.signals.join(',') : null, noDesign: !!s.noDesign, readable: !!s.readable, problems: s.problems.map(p => p.kind + (p.optionName ? ':' + p.optionName : '')).join(), pieces: s.pieceCount, engrave: !!s.engraveCandidate });
  const diff = c => FIX.lines.filter(f => sig(spec(f, c)) !== JSON.stringify({ special: f.before.special, noDesign: f.before.noDesign, readable: f.before.readable, problems: f.before.problems, pieces: f.before.pieces, engrave: f.before.engrave })).map(f => f.rid + ' ' + f.sku);
  const base = diff(ctx());
  assert.deepEqual(diff(flagged()).filter(x => !base.includes(x)), ['4174384672 Dainty_1450', '4173025188 Dainty_1450'], 'only the two lines of the listing change');
  assert.equal(FIX.lines.filter(f => f.listing === LISTING).length, 2);

  // what the order waits for. The ring (Custom Orders, its own design) and the chain, which is its own question (here decided as chain only, as Claude's reading or a person does)
  const decided = { [KEY_CHAIN]: { kind: 'chainOnly', by: 'x' } };
  const rowFor = (f, c, st, extra) => { const o = order(f.rid, []), l = line(f), sp = O.interpretLine(o, l, c), key = O.lineKey(o, l); return Object.assign({ key, order: o, line: l, spec: sp, problems: sp.problems.slice(), state: st || (sp.noDesign ? 'noDesign' : sp.ownDesign ? 'held' : 'pulled'), reason: sp.ownDesign ? O.specialOf.addOn.wait : null, poolIds: [], hold: null }, extra || {}); };
  const waiting = [rowFor(RING18, flagged({ customDecided: decided })), rowFor(CHAIN, flagged({ customDecided: decided }))];
  assert.deepEqual(waiting.map(r => [r.state, !!r.spec.noDesign, !!r.spec.ownDesign]), [['held', false, true], ['noDesign', true, false]], 'the ring waits for a person; the chain is never cut');
  const ev0 = O.evaluateOrder(waiting); assert.equal(ev0.committable, false); assert.deepEqual(ev0.held, { line: KEY18, why: O.specialOf.addOn.wait }, 'the order waits for the ring, and says so in words');
  const rep0 = R.orderReports(waiting, [])['4174384672']; assert.deepEqual([rep0.ready, rep0.key, rep0.line], [false, 'held', KEY18], 'readiness names the ring: ' + JSON.stringify(rep0));
  // Complete Order: the ring is completed by hand (the record customPut keeps), the chain still the chain: nothing waits, nothing is cut, no sheet is wanted
  const DONE = { [KEY18]: { state: 'completed', how: 'button', completedBy: 'Test Operator', completedAt: 1791831000000 } };
  const done = [rowFor(RING18, flagged({ customDecided: decided, customDone: DONE })), rowFor(CHAIN, flagged({ customDecided: decided, customDone: DONE }))];
  assert.deepEqual(done.map(r => [r.state, r.spec.noDesign, !!r.spec.ownDesign, !!r.spec.customDone]), [['noDesign', true, true, true], ['noDesign', true, false, false]], 'completed by hand: no design to cut (it keeps no master design either)'); assert.match(done[0].spec.noDesignWhy, /completed by hand/);
  assert.deepEqual(O.evaluateOrder(done), { committable: true, held: null, lines: [{ key: KEY18, ok: true, why: 'completed by hand (Custom Orders)' }, { key: KEY_CHAIN, ok: true, why: 'no design (chain/packaging)' }] });
  assert.deepEqual(R.orderReports(done, [])['4174384672'], { ready: true }, 'readiness: nothing holds the order');
  const piecesOf = rows => OP.resolve({ orderId: '4174384672', lines: rows.map(r => ({ key: r.key, transactionId: r.line.transactionId, sku: r.line.sku || '', title: r.line.title, material: r.spec.material, quantity: r.spec.quantity, state: r.state, poolIds: [], hold: null, changePending: false, problems: r.problems.map(p => ({ kind: p.kind })), spec: { noDesign: r.spec.noDesign, customDone: r.spec.customDone || null }, reason: r.reason || '' })), pools: [], sheets: [], sheetsKnown: true });
  const p0 = piecesOf(waiting).find(p => p.lineKey === KEY18), p1 = piecesOf(done).find(p => p.lineKey === KEY18);
  assert.deepEqual([!!p0.hand, p0.nested, p0.loading], [false, false, false], 'before: a piece that waits for a person');
  assert.deepEqual([!!p1.hand, p1.nested, p1.loading, p1.problem], [true, false, false, null], 'after Complete Order: resolved by hand, it needs no sheet, never "not on a sheet yet": ' + JSON.stringify([p1.problem, p1.reason, p1.why]));
  { const R2 = Object.assign({}, RING18, { q: 2 }), mk = c => { const o = order(R2.rid, []), l = line(R2), sp = O.interpretLine(o, l, c), key = O.lineKey(o, l); return { key, order: o, line: l, spec: sp, problems: sp.problems.slice(), state: sp.noDesign ? 'noDesign' : 'held', reason: sp.ownDesign ? O.specialOf.addOn.wait : null, poolIds: [], hold: null }; };
    const w2 = mk(flagged()), d2 = mk(flagged({ customDone: DONE }));
    const resolve = r => OP.resolve({ orderId: '4174384672', lines: [{ key: r.key, transactionId: r.line.transactionId, sku: r.line.sku, title: r.line.title, material: r.spec.material, quantity: r.spec.quantity, state: r.state, poolIds: [], hold: null, changePending: false, problems: [], spec: { noDesign: r.spec.noDesign, customDone: r.spec.customDone || null }, reason: r.reason || '' }], pools: [], sheets: [], sheetsKnown: true }).filter(p => p.lineKey === r.key);
    assert.deepEqual(resolve(w2).map(p => !!p.hand), [false, false], 'two pieces wait for a person');
    assert.deepEqual(resolve(d2).map(p => [!!p.hand, p.problem, p.nested]), [[true, null, false], [true, null, false]], 'both resolved by one Complete Order');
    assert.deepEqual(O.evaluateOrder([d2]).committable, true); assert.deepEqual(R.orderReports([d2], [])['4174384672'], { ready: true }); }
  console.log('  ✓ the ring of the listing is a custom order that takes no repo design (every variation, quantity 2, 2 of 411 snapshot lines change); the order waits for Complete Order and nothing after it');
}

/* ── the server op ── */
async function server() {
  const srv = await start({ receipts: [] });
  try {
    srv.st.strictArrays = true;
    const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body);
    // the two aliases that stand live today (copied byte for byte from the read-only backup of 12 Oct): the word must leave them alone
    const live = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/premade-live-aliases.json'), 'utf8'));
    for (const [id, d] of Object.entries(live)) srv.st.put('Charm_Sku_Aliases', id, JSON.parse(JSON.stringify(d)));
    const before = JSON.stringify(Object.fromEntries(Object.keys(live).map(id => [id, srv.st.doc('Charm_Sku_Aliases', id)])));
    const put = await call({ op: 'listingKindPut', listingId: LISTING, kind: 'custom', by: 'PREMADE', title: 'Dainty Circle Choker Necklace', note: NOTE });
    assert.deepEqual(put, { ok: true, listingId: LISTING, kind: 'custom' });
    const all = (await call({ op: 'aliasGet' })).aliases;
    assert.deepEqual(Object.keys(all).sort(), [LISTING, '1648594139', '1744372161'], 'one new alias document, the others still there');
    assert.equal(JSON.stringify(Object.fromEntries(Object.keys(live).map(id => [id, srv.st.doc('Charm_Sku_Aliases', id)]))), before, 'the Tennis link and the Schrodinger Cat link are byte for byte as they were');
    const got = all[LISTING]; assert.equal(got.listingKind.kind, 'custom'); assert.equal(got.listingKind.by, 'PREMADE'); assert.equal(got.listingKind.note, NOTE); assert.equal(got.listingKind.title, 'Dainty Circle Choker Necklace'); assert(got.listingKind.at > 0); assert.equal(got.listingId, LISTING);
    assert.deepEqual(Object.keys(got).sort(), ['listingId', 'listingKind', 'updatedAt'], 'nothing else is on the document: no SKU answer, no pair');
    assert.equal(spec(RING18, ctx({ aliases: { [LISTING]: got } })).special.why.replace(/ \(.*$/, ''), `listing ${LISTING}: PREMADE said every order of it is a custom order`, 'what aliasGet gives is what the classifier reads');
    assert.equal(spec(RING18, ctx({ aliases: { [LISTING]: got } })).ownDesign, true);
    for (const [k, d] of srv.st.docs) if (/Charm_Sku_Aliases/.test(k)) noNested(d, k);
    console.log('  ✓ listingKindPut {kind custom, by, note}: one alias document, the other two untouched, read back by aliasGet');
  } finally { srv.close(); }
}

/* ── 2 · the sorter in Chromium ── */
async function browser() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return false; }
  const srv = await start({ receipts: [] });
  const b = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body);
    // the word is written the way the live write does it, and read by the page through aliasGet
    assert.equal((await call({ op: 'listingKindPut', listingId: LISTING, kind: 'custom', by: 'PREMADE', title: 'Dainty Circle Choker Necklace', note: NOTE })).ok, true);
    const context = await b.newContext({ viewport: { width: 1440, height: 950 } });
    // the Etsy listing photo: a warm photo of a chain with a ring (not the blue "custom" picture of an add-on listing), from the page's own image proxy route
    const PHOTO = '<svg xmlns="http://www.w3.org/2000/svg" width="240" height="240"><rect width="240" height="240" fill="#b9805a"/><circle cx="120" cy="120" r="30" fill="none" stroke="#e8c15a" stroke-width="6"/></svg>';
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => r.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: PHOTO }));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(photos => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(photos)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, [[LISTING, '/.netlify/functions/imageProxy?url=listing-' + LISTING]]);
    const page = await context.newPage(), errors = [], ops = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (/charmNestLibrary/.test(rq.url()) && rq.method() === 'POST') { try { const op = JSON.parse(rq.postData() || '{}').op; if (/Put|Decide|Delete|Reopen|Reset|Purge|Update|Apply|Save|Commit/.test(op || '')) ops.push(op); } catch (_) {} } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const o = order('4174384672', [line(RING18), line(CHAIN)]);
    // the master holds the DAINTY_1450 double ring (47.9 x 47.9 pt, two bodies); its drawing is counted, not drawn
    await page.evaluate(async o => {
      window.__drawn = 0;
      CustomRead.later = () => {};   // (Claude's reading of lines is never started by a test: nothing is paid for)
      B.master.entries.set('DAINTY_1450', { sku: 'DAINTY_1450', widthPt: 47.854, heightPt: 47.854, members: 2, thumbUrl: '', aiPath: 'charmnest/master/DAINTY_1450.ai', charmHash: '914f6070' });
      for (const k of ['masterPreview', 'masterFront']) { const keep = Pool[k]; Pool[k] = function () { window.__drawn++; return 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" fill="#c33"/></svg>'); }; Pool['_' + k] = keep; }
      await Orders.loadMaps(true);
      for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, o);
    const st = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { kind: r.spec.special && r.spec.special.kind, why: r.spec.special && r.spec.special.why, problems: r.problems.length, state: r.state, reason: r.reason, own: r.spec.ownDesign, aliasWord: B.maps.aliases['1096331450'] && B.maps.aliases['1096331450'].listingKind.kind }; }, KEY18);
    assert.deepEqual([st.kind, st.why, st.problems, st.own, st.aliasWord], ['customNecklace', WHY, 0, true, 'custom'], 'the page read the word through aliasGet: ' + JSON.stringify(st));
    const chips = () => page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(x => x.textContent.trim()));
    assert((await chips()).some(t => /^Custom Orders1/.test(t)), 'the ring is under Custom Orders: ' + (await chips()).join(' | '));
    assert(!(await chips()).some(t => /^Options/.test(t)), 'nothing asked about its options: ' + (await chips()).join(' | '));
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const cardSel = `#rvList .reviewListRow[data-row="${KEY18}"]`;
    await page.waitForFunction(s => document.querySelector(s), cardSel);
    const info = await page.evaluate(sel => { const n = document.querySelector(sel); return { cls: n.className, queue: n.querySelector('.queueLabel')?.textContent, label: n.querySelector('.engravingIdentity .purchaseLabel')?.textContent, sku: n.querySelector('.sku')?.textContent, reason: n.querySelector('.reviewReason')?.textContent, text: n.textContent,
      buttons: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()), send: (() => { const s = n.querySelector('[data-cu-send]'); return s ? { aria: s.getAttribute('aria-disabled'), cls: s.className } : null; })() }; }, cardSel);
    assert.match(info.cls, /cuRow/); assert.match(info.cls, /cuInfo/, 'a card that asks nothing'); assert.equal(info.queue, 'Custom order'); assert.equal(info.label, 'Custom necklace');
    assert.equal(info.sku, 'DAINTY_1450', 'the SKU Etsy sold, as written on the card (it is not a match: no drawing is attached to it)');
    assert.equal(info.reason, `Custom necklace · ${WHY} · Release Hold or print its QR label when it is ready, or drop its .ai / .dxf designs and Send to Sheet`, 'ONE plain reason line: ' + info.reason);
    assert(!/add-on listing/.test(info.text) && !/not mapped/.test(info.text), 'no add-on wording, no question: ' + info.text.slice(0, 240));
    assert.deepEqual(info.buttons, ['Print QR label', 'Complete Order', 'Send to Sheet', 'Drop .ai / .dxf designs here'], 'no Hold on a Review card (NOHOLDREV, Paul 17 Oct 2026)');
    assert.equal(info.send.aria, 'true', 'Send to Sheet is greyed until a design .ai is dropped'); assert.doesNotMatch(info.send.cls, /gold/);
    await page.waitForFunction(sel => { const n = document.querySelector(sel); return n.querySelector('[data-vector]').textContent.includes('No vector available') && n.querySelector('[data-listing] img'); }, cardSel, { timeout: 20000 });
    const pics = await page.evaluate(async sel => { const n = document.querySelector(sel), img = n.querySelector('[data-listing] img'); await img.decode().catch(() => {}); return { vector: n.querySelector('[data-vector]').textContent.trim(), vectorImg: !!n.querySelector('[data-vector] img'), drawn: window.__drawn, w: img.naturalWidth, cap: [...n.querySelectorAll('figcaption')].map(x => x.textContent.replace(/↺/g, '').trim()) }; }, cardSel);
    assert.deepEqual([pics.vector, pics.vectorImg, pics.drawn], ['No vector available', false, 0], 'the double ring is never drawn for it: ' + JSON.stringify(pics)); assert(pics.w > 0, 'the Etsy listing photo is beside it'); assert.deepEqual(pics.cap, ['Vector design', 'Etsy listing']);
    // the Orders table: its own pill, and nowhere a design column naming the repo charm
    const pill = await page.evaluate(k => Orders.statePill(B.orders.byKey.get(k)), KEY18); assert.deepEqual(pill, ['info', 'custom · own design']);

    // the order window (the screenshot's): SKU as Etsy sold it, no drawing, the pieces
    await page.evaluate(k => OrderWin.open(k), KEY18);
    await page.waitForFunction(() => OrderWin.isOpen() && document.getElementById('owSku').textContent.length > 5, null, { timeout: 15000 });
    await page.waitForTimeout(600);
    const win = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, sku: document.getElementById('owSku').textContent, vector: document.getElementById('owVector').textContent.replace(/↺/g, '').trim(), vectorImg: !!document.querySelector('#owVector img, #owVector canvas'),
      meta: [...document.querySelectorAll('#owMeta .m')].map(m => m.querySelector('i').textContent + ': ' + m.querySelector('span').textContent), pieces: OrderPieces.of('4174384672').map(p => ({ key: p.key, lineKey: p.lineKey, label: p.label, problem: p.problem, hand: !!p.hand, nested: p.nested, thumb: !!p.thumb })), drawn: window.__drawn }));
    assert.match(win.title, /4174384672/); assert.equal(win.sku, 'SKU: DAINTY_1450'); assert.equal(win.vector, 'No vector available', 'the Vector design box of the screenshot is empty now'); assert.equal(win.vectorImg, false); assert.equal(win.drawn, 0);
    assert(win.meta.includes(`Custom order: Custom necklace · SKU DAINTY_1450 · ${WHY}`) || win.meta.some(m => m.startsWith('Custom order: Custom necklace') && m.includes(WHY)), 'the order window says why: ' + JSON.stringify(win.meta));
    assert.equal(win.pieces.length, 2, 'two pieces: the ring and the chain'); assert.deepEqual(win.pieces.map(p => p.lineKey).sort(), [KEY18, KEY_CHAIN].sort());
    await page.click('#owClose');

    // Complete Order: customPut only: nothing pooled, nothing on a sheet, nothing cut
    await page.click(`${cardSel} [data-cu-release]`);
    await page.waitForFunction(k => B.maps.customDone[k], KEY18, { timeout: 30000 });
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.noDesign === true, KEY18, { timeout: 15000 });
    const rec = srv.st.doc('Charm_Custom_Orders', KEY18);
    assert(rec && rec.how === 'button' && rec.state === 'completed' && rec.completedBy === 'Test Operator' && !rec.prints, 'completed by hand, no label printed: ' + JSON.stringify(rec && [rec.how, rec.state, rec.completedBy, rec.prints]));
    const after = await page.evaluate(k => { const r = B.orders.byKey.get(k), rep = CharmNestReadiness.orderReports(B.orders.rows, [])['4174384672']; return { state: r.state, noDesign: r.spec.noDesign, why: r.spec.noDesignWhy, own: r.spec.ownDesign, pool: r.poolIds.length, poolRows: B.pool.rows.size, onSheets: allSheets().reduce((n, sh) => n + (sh.charms || []).length, 0), pill: Orders.statePill(r), rep: rep && { ready: rep.ready, key: rep.key, line: rep.line }, drawn: window.__drawn,
      piece: OrderPieces.of('4174384672').filter(p => p.lineKey === k).map(p => ({ hand: !!p.hand, problem: p.problem, nested: p.nested, loading: p.loading })) }; }, KEY18);
    assert.deepEqual([after.noDesign, after.pool, after.poolRows, after.onSheets, after.drawn], [true, 0, 0, 0, 0], 'nothing pooled, nothing on a sheet, nothing drawn or cut: ' + JSON.stringify(after));
    assert.deepEqual(after.pill, ['ok', 'custom · done']); assert.match(after.why, /completed by hand/);
    assert.deepEqual(after.piece, [{ hand: true, problem: null, nested: false, loading: false }], 'the ring piece is resolved by hand: never "not on a sheet yet"');
    assert.notEqual(after.rep && after.rep.line, KEY18, 'readiness no longer names the ring: ' + JSON.stringify(after.rep));
    // Reopen (the completion taken back): the ring waits for a person again, and the order with it
    await page.waitForFunction(() => /moved to Completed · completed by Test Operator with no label printed$/.test(document.querySelector('.mNote .mNoteT')?.textContent || ''), null, { timeout: 8000 });
    await page.evaluate(() => document.querySelectorAll('.mNote').forEach(n => n.close()));
    await page.click('#reviewView .rvSeg [data-cseg="done"]');
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"] [data-cu-reopen]`), KEY18);
    await page.click(`#rvList .reviewListRow[data-row="${KEY18}"] [data-cu-reopen]`);
    await page.waitForFunction(k => !B.maps.customDone[k] && !document.querySelector('.cuStat, .btn.working'), KEY18, { timeout: 30000 });
    const back = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { state: r.state, own: r.spec.ownDesign, noDesign: r.spec.noDesign, committable: CharmNestOrders.evaluateOrder(B.orders.rows.filter(x => x.order.receiptId === '4174384672')).committable, rep: CharmNestReadiness.orderReports(B.orders.rows, [])['4174384672'].line, poolRows: B.pool.rows.size }; }, KEY18);
    assert.deepEqual([back.state, back.own, back.noDesign, back.committable, back.rep, back.poolRows], ['pulled', true, false, false, KEY18, 0], 'reopened: a ring that waits again (the run holds it with its reason when it next looks), nothing pooled: ' + JSON.stringify(back));
    assert.deepEqual([...new Set(ops)].sort(), ['customPut', 'customReopen'], 'the only writes the page made were customPut and customReopen: ' + [...new Set(ops)].join());
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Chromium: the ring is one Custom Orders card (reason line, Print QR label, Complete Order, Send to Sheet greyed), no drawing, the Etsy photo; the order window and Orders table; Complete Order writes customPut only');
  } finally { await b.close(); srv.close(); }
  return true;
}

(async () => {
  { const h = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');   // the page asks for the new scripts (other workers add their own tokens after -pm1)
    for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) assert(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-pm1(?:-[a-z0-9]+)*"').test(h), f + ' carries the -pm1 cache token'); }
  await server();
  const ran = await browser();
  console.log((ran ? 'Pre-made listing OK:' : 'Pre-made listing (node part only, no browser):') + ' listing 1096331450 (DAINTY_1450) is a custom order that takes no repo design, Complete Order frees its order, nothing cut or sheeted, nothing live written');
})().catch(e => { console.error(e); process.exit(1); });
