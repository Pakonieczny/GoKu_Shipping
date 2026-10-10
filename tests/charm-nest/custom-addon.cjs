// Add-on listings are custom orders (ADDONCUSTOM; Paul, 10 Oct 2026, order 4176744752: "This should be classified as a custom order because it's in addition of a charm
// by itself, and it has the classical blue Custom thumbnail"). The line is listing 1777293722 "Huggie Charm + Shipping", SKU Huggie_3722 (the master file holds a HUGGIE_3722:
// a ladybug), quantity 2, option "Price: Gold Filled - Single". It used to be read as that ladybug and held with `option "Price: Gold Filled - Single" not mapped`.
// Offline: no Etsy call, no paid AI reading, nothing live is written (the cloud is the local fake, bridge-server.cjs).
//   node tests/charm-nest/custom-addon.cjs [playwright-core dir]
// Part 1 (node): what says a listing is an add-on listing (the listing id in the code's list and in a person's Firebase list, the title's first phrase "Add a … Charm" /
//   "… Charm + Shipping", never "add on" somewhere in a long title); the golden replay of every line of the Sep 17 snapshot (411 lines): only the add-on lines differ;
//   the server's listingKindPut (kept on the listing's alias document, read by aliasGet, the sandbox's own copy, no nested arrays).
// Part 2 (Chromium, the real sorter on the fake site): the cached line for 4176744752 is one Custom Orders card — no "not mapped" question, no master drawing (the ladybug), the
//   Etsy listing photo beside it, Print QR label / Complete Order / Send to Sheet greyed until a design is dropped / Hold — and it re-reads by itself when a person's word for
//   the listing changes (a line held with the old question waits for its designs; one with pieces already pooled keeps them).
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const { start } = require('./bridge-server.cjs');
const noNested = require('./_noNestedArrays.cjs');
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/addon-snapshot-lines.json'), 'utf8'));

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const line = (f, extra) => Object.assign({ transactionId: f.tid, listingId: f.listing, sku: f.sku, title: f.title, quantity: f.q, expectedShipDate: SHIP, variations: f.vars.map(([name, value]) => ({ name, value })), metalKey: f.mk, metalLabel: f.mk, personalization: [], buyerMessage: f.msg ? 'x' : '' }, extra || {});
const KNOWN = FIX.known, known = { has: s => !!KNOWN[String(s).toUpperCase()], add: s => { KNOWN[String(s).toUpperCase()] = {}; }, delete: s => { delete KNOWN[String(s).toUpperCase()]; } };   // (the master as the live library held it: SKU → what the reader looks at)
const entryOf = s => { const k = KNOWN[String(s).toUpperCase()]; return k ? Object.assign({ sku: s }, k.blocked ? { blocked: k.blocked } : {}, k.sizes ? { sizes: Object.fromEntries(k.sizes.map(z => [z, {}])) } : {}, k.pair ? { pair: k.pair } : {}, k.facing ? { facing: k.facing } : {}, k.sym ? { sym: k.sym } : {}) : null; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryOf, masterLoose: s => FIX.loose[String(s)] || '' }, extra || {});
const rowOf = (rid, tid) => FIX.lines.find(l => l.rid === rid && l.tid === tid);
const HUGGIE = rowOf('4176744752', '5219514968'), CHAIN = rowOf('4176744752', '5219514966');
assert(HUGGIE && HUGGIE.listing === '1777293722' && HUGGIE.sku === 'Huggie_3722' && HUGGIE.q === 2 && JSON.stringify(HUGGIE.vars) === '[["Price","Gold Filled - Single"]]', 'the cached line of order 4176744752');
assert(known.has('HUGGIE_3722'), 'the master holds a design named like the add-on listing\'s SKU (the ladybug)');
const spec = (f, c, extra) => O.interpretLine(order(f.rid, []), line(f, extra), c || ctx());
const read = (title, over) => spec(Object.assign({}, HUGGIE, { title, sku: 'NEWSKU_9', listing: '99', tid: '1', vars: [['Metal', 'Gold']] }, over || {}));

/* ── 1 · node ── */
{
  // the cached line of 4176744752
  const sp = spec(HUGGIE, ctx(), { personalization: ['one bear and one griffin please'] });
  assert.equal(sp.special.kind, 'customHuggies'); assert.equal(sp.special.label, 'Custom huggies'); assert.deepEqual(sp.special.signals, ['listing']); assert.equal(sp.special.own, true);
  assert.match(sp.special.why, /^listing 1777293722 “Huggie Charm \+ Shipping” is an add-on listing/, 'it says why, in words: ' + sp.special.why);
  assert.deepEqual(sp.problems, [], 'no "option Price not mapped", no metal, SKU or engraving question');
  assert.equal(sp.ownDesign, true); assert.equal(sp.noDesign, false, 'it is not "no design": nothing closes its order until its own designs are sent or a person completes it');
  assert.equal(sp.readable, false, 'a rule is not read by Claude: no paid reading'); assert.equal(sp.engraveCandidate, false, 'the buyer\'s words are the design request, not an engraving');
  assert.equal(O.evaluateOrder([{ key: 'k', state: 'held', reason: O.specialOf.addOn.wait, spec: sp, problems: [] }]).committable, false, 'its order waits for it');
  // the same line, the listing taken out by a person: read as before, the option asked about again
  const reg = spec(HUGGIE, ctx({ aliases: { 1777293722: { listingKind: { kind: 'regular', by: 'Paul' } } } }));
  assert.equal(reg.special, undefined); assert.equal(reg.ownDesign, undefined); assert.deepEqual(reg.problems.map(p => p.kind + ':' + p.optionName), ['needsMapping:Price'], 'what it was');
  // the golden replay of the Sep 17 snapshot: every one of its 411 lines as before, the add-on lines aside
  const changed = [];
  for (const f of FIX.lines) {
    const s = spec(f), now = { special: s.special ? s.special.kind + '/' + s.special.signals.join(',') : null, noDesign: !!s.noDesign, readable: !!s.readable, problems: s.problems.map(p => p.kind + (p.optionName ? ':' + p.optionName : '')).sort().join(','), pieces: s.pieceCount, engrave: !!s.engraveCandidate };
    if (JSON.stringify(now) !== JSON.stringify(f.before)) changed.push([f.rid + ' ' + f.sku, f.before, now]);
  }
  assert.equal(FIX.lines.length, 411);
  assert.deepEqual(changed.map(c => c[0]), ['4176744752 Huggie_3722', '4174476673 Custom_6673'], 'only the two add-on lines of the snapshot change');
  assert.deepEqual(changed[0][2], { special: 'customHuggies/listing', noDesign: false, readable: false, problems: '', pieces: 2, engrave: false });
  assert.deepEqual([changed[1][1].special, changed[1][2].special], ['customCharm/sku', 'customCharm/listing'], 'a line that was custom stays custom (its kind), now by its listing, and no longer cut as the master design its SKU matches (CUSTOM_6673 is a bunny)');
  // lines that were custom by their codes still read as before (golden above), and the five custom kinds
  for (const f of FIX.lines.filter(x => x.before.special && !/^(?:customHuggies|customCharm)\/listing/.test(x.before.special) && x.rid !== '4174476673')) assert.equal(spec(f).special ? spec(f).special.kind + '/' + spec(f).special.signals.join(',') : null, f.before.special, f.rid + ' unchanged');

  // the wording: the title's FIRST phrase and nothing else; "add on" anywhere in a long title is no rule (Paul, 25 Sep)
  const kind = t => { const s = read(t); return s.special ? s.special.kind + '/' + s.special.signals[0] + (s.special.own ? '/own' : '') : null; };
  for (const t of ['Huggie Charm + Shipping', 'Custom Charm + Shipping', 'Add a Huggie Charm', 'Add an Initial Charm', 'Add a Charm - Gold Filled, Sterling Silver', 'Add a Huggie Charm + Shipping', 'Mini Heart Charm+Shipping', 'ADD A HUGGIE CHARM']) assert.match(kind(t) || '', /^custom\w*\/listing\/own$/, t);
  for (const t of ['Add a Name Charm Necklace Personalized', 'Fairy Charm Gold Charm Add On Charm Gold Fairy Pendant', 'Charm Necklace + Shipping Upgrade', 'Bunny Charm', 'Add On Charm', 'Add-on: second initial', 'Add a Very Long Name Of Charm Here Charm', 'Gold Charm Necklace, Add a Charm Option', 'Charm + Shipping Included Necklace']) assert.equal(kind(t), null, t + ' is not an add-on listing');
  assert.equal(read('Add a Huggie Charm').special.kind, 'customHuggies'); assert.equal(read('Add an Initial Charm').special.kind, 'customCharm', 'its form from its words, as every custom piece');
  // it wins over a master design the SKU matches, over Claude's reading, and costs no reading
  assert.equal(read('Add a Huggie Charm', { sku: 'HUGGIE_3722' }).special.signals[0], 'listing');
  const aiSaysRegular = read('Add a Huggie Charm', { }); assert.equal(aiSaysRegular.readable, false);
  const withRead = O.interpretLine(order('1', []), line(Object.assign({}, HUGGIE, { sku: 'NEWSKU_9', title: 'Add a Huggie Charm', listing: '99', tid: '1' })), ctx({ customRead: { '1_1': { kind: 'regular', confidence: 0.95, summary: 'a catalogue charm' } } }));
  assert.equal(withRead.special.signals[0], 'listing', 'the listing\'s wording stands over Claude\'s reading');
  // a person's decision on the line itself still stands over everything
  const decided = O.interpretLine(order('1', []), line(Object.assign({}, HUGGIE, { sku: 'NEWSKU_9', title: 'Add a Huggie Charm', listing: '99', tid: '1' })), ctx({ customDecided: { '1_1': { kind: 'regular', by: 'Ann' } } }));
  assert.equal(decided.special, undefined, 'a person\'s "Not custom" on the line');
  // a person's list (Firebase, the listing's alias document): any listing, any kind, "regular" to take one out, and the title rule needs no code change for the next one
  const named = (kindDoc, over) => spec(Object.assign({}, HUGGIE, { listing: '555', sku: 'ADDTEST_55', title: 'Bunny Charm', tid: '7', vars: [['Metal', 'Gold']] }, over || {}), ctx({ aliases: { 555: { listingKind: kindDoc } } }));
  assert.equal(known.has('ADDTEST_55'), false); known.add('ADDTEST_55');
  assert.equal(named(undefined).special, undefined, 'nothing said: a catalogue charm');
  const c1 = named({ kind: 'custom', by: 'Paul' }); assert.equal(c1.special.kind, 'customCharm'); assert.equal(c1.special.own, true); assert.match(c1.special.why, /^listing 555: Paul said every order of it is a custom order/); assert.equal(c1.ownDesign, true);
  const w1 = named({ kind: 'rework', by: 'Paul' }); assert.equal(w1.special.kind, 'rework'); assert.equal(w1.ownDesign, true);
  const ch = named({ kind: 'chainOnly', by: 'Paul' }); assert.equal(ch.special.kind, 'chainOnly'); assert.equal(ch.noDesign, true, 'chain only is never cut'); assert.equal(ch.ownDesign, undefined);
  const a1 = named({ kind: 'addOnToOrder', by: 'Paul' }); assert.equal(a1.special.label, 'Add-on to an order');
  assert.equal(named({ kind: 'nonsense' }).special, undefined, 'an unknown kind says nothing');
  assert.equal(named({ kind: 'regular' }, { title: 'Add a Huggie Charm' }).special, undefined, '"regular" takes a listing out of the wording rule');
  known.delete('ADDTEST_55');
  // a chain-only choice by the buyer still reads first, a completed line is still completed by hand
  const done = spec(HUGGIE, ctx({ customDone: { '4176744752_5219514968': { state: 'completed', how: 'button' } } })); assert.equal(done.noDesign, true); assert.match(done.noDesignWhy, /completed by hand/);
  console.log('  ✓ the rule, the wording, the golden replay of the snapshot (2 of 411 lines change), a person\'s word for a listing');
}

async function server() {
  const srv = await start({ receipts: [] });
  try {
    srv.st.strictArrays = true;
    const call = async body => JSON.parse((await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} })).body);
    assert.equal((await call({ op: 'listingKindPut', listingId: '1777293722', kind: 'custom', by: 'Paul', title: 'Huggie Charm + Shipping', note: 'Add a huggie charm' })).ok, true);
    await call({ op: 'aliasPut', listingId: '1777293722', sku: 'HUGGIE_3722', by: 'Ann', title: 'x' });   // (a SKU answer on the same document: neither touches the other)
    const got = (await call({ op: 'aliasGet' })).aliases['1777293722'];
    assert.equal(got.listingKind.kind, 'custom'); assert.equal(got.listingKind.by, 'Paul'); assert(got.listingKind.at > 0); assert.equal(got.sku, 'HUGGIE_3722', 'the SKU answer is kept beside it');
    assert.equal(spec(HUGGIE, ctx({ aliases: { 1777293722: got } })).special.why.slice(0, 40), 'listing 1777293722: Paul said every orde', 'what aliasGet gives is what the classifier reads');
    assert.match((await call({ op: 'listingKindPut', listingId: '1', kind: 'banana' })).error, /kind must be one of/); assert.match((await call({ op: 'listingKindPut', kind: 'custom' })).error, /listingId/);
    assert.equal((await call({ op: 'listingKindPut', listingId: '555', kind: '' })).ok, true); assert.equal(srv.st.doc('Charm_Sku_Aliases', '555'), undefined, 'taking a word off a listing that has none writes nothing');
    assert.equal((await call({ op: 'listingKindPut', listingId: '1777293722', kind: 'regular', by: 'Paul' })).kind, 'regular');
    assert.equal((await call({ op: 'listingKindPut', listingId: '1777293722', kind: null })).ok, true);
    const gone = (await call({ op: 'aliasGet' })).aliases['1777293722']; assert.equal(gone.listingKind, undefined, 'removed'); assert.equal(gone.sku, 'HUGGIE_3722', 'the SKU answer stays');
    // the sandbox keeps its own copy and never writes the shared document
    await call({ op: 'listingKindPut', listingId: '777', kind: 'custom', by: 'x', sandbox: true });
    assert(srv.st.doc('Sandbox_Charm_Sku_Aliases', '777') && !srv.st.doc('Charm_Sku_Aliases', '777'), 'the sandbox\'s own copy');
    for (const [k, d] of srv.st.docs) if (/Charm_Sku_Aliases/.test(k)) noNested(d, k);
    console.log('  ✓ listingKindPut / aliasGet: kept on the listing\'s alias document, the SKU answer untouched, sandbox copy, no nested arrays');
  } finally { srv.close(); }
}

/* ── 2 · the sorter in Chromium ── */
async function browser() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const b = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await b.newContext({ viewport: { width: 1440, height: 950 } });
    // the Etsy listing photo: a generic blue picture saying ADD A HUGGIE CHARM (what the listing's first photo is), from the page's own image proxy route
    const BLUE = '<svg xmlns="http://www.w3.org/2000/svg" width="240" height="240"><rect width="240" height="240" fill="#a7d8e6"/><text x="120" y="124" font-size="20" text-anchor="middle" fill="#222">ADD A HUGGIE CHARM</text></svg>';
    await context.route(/\/\.netlify\/functions\/imageProxy/, r => r.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: BLUE }));
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(photos => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(photos)); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, [['1777293722', '/.netlify/functions/imageProxy?url=listing-1777293722']]);
    const page = await context.newPage(), errors = [], writes = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('request', rq => { if (/charmNestLibrary/.test(rq.url()) && rq.method() === 'POST') { try { const op = JSON.parse(rq.postData() || '{}').op; if (/Put|Decide|Delete|Reopen|Reset|Purge|poolPut|runPut/.test(op || '')) writes.push(op); } catch (_) {} } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomPrint && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    const o = order('4176744752', [line(HUGGIE), line(CHAIN)]);
    // the master holds a HUGGIE_3722 (the ladybug); its drawing is counted, not drawn
    await page.evaluate(async o => {
      window.__drawn = 0; window.__reads = 0;
      CustomRead.later = () => { window.__reads++; };   // (Claude's reading of lines is never started by a test: nothing is paid for)
      B.master.entries.set('HUGGIE_3722', { sku: 'HUGGIE_3722', widthPt: 18, heightPt: 30, thumbUrl: '', aiPath: 'charmnest/master/HUGGIE_HOOPS-_LADYBUG_1.ai' });
      for (const k of ['masterPreview', 'masterFront']) { const keep = Pool[k]; Pool[k] = function () { window.__drawn++; return 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" fill="#c33"/></svg>'); }; Pool['_' + k] = keep; }
      await Orders.loadMaps(true);
      for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, o);
    const KEY = '4176744752_5219514968';
    const chips = () => page.evaluate(() => [...document.querySelectorAll('#reviewView .ordBar .egTab')].map(x => x.textContent.trim()));
    assert.deepEqual((await chips()).filter(t => /^(Custom Orders|Options|Unknown)/.test(t)), ['Custom Orders2'], 'the huggie add-on and the chain: both under Custom Orders, nothing under Options: ' + (await chips()).join(' | '));
    const st = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { special: r.spec.special.label, why: r.spec.special.why, problems: r.problems.length, state: r.state, reason: r.reason, own: r.spec.ownDesign, items: Review.items().filter(i => i.rows && i.rows.some(x => x.key === k)).length }; }, KEY);
    assert.deepEqual([st.special, st.problems, st.own, st.items], ['Custom huggies', 0, true, 0], 'no question, no Review item of its own: ' + JSON.stringify(st));
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = `#rvList .reviewListRow[data-rid="4176744752"]`;
    await page.waitForFunction(k => document.querySelector(`#rvList .reviewListRow[data-row="${k}"]`), KEY);
    const cardSel = `#rvList .reviewListRow[data-row="${KEY}"]`;
    const info = await page.evaluate(sel => { const n = document.querySelector(sel); return { cls: n.className, queue: n.querySelector('.queueLabel')?.textContent, label: n.querySelector('.engravingIdentity .purchaseLabel')?.textContent, sku: n.querySelector('.sku')?.textContent, text: n.textContent,
      buttons: [...n.querySelectorAll('.rowActions button')].map(x => x.textContent.trim()), send: (() => { const s = n.querySelector('[data-cu-send]'); return s ? { aria: s.getAttribute('aria-disabled'), cls: s.className, title: s.title } : null; })(), drop: !!n.querySelector('[data-cu-designs].cuHint') }; }, cardSel);
    assert.match(info.cls, /cuRow/); assert.match(info.cls, /cuInfo/, 'a card that asks nothing'); assert.equal(info.queue, 'Custom order'); assert.equal(info.label, 'Custom huggies');
    assert(!/not mapped/.test(info.text), 'no "option … not mapped" on the card: ' + info.text.slice(0, 200)); assert.match(info.text, /add-on listing, no master design/);
    assert.deepEqual(info.buttons.filter(x => x !== 'Hold'), ['Print QR label', 'Complete Order', 'Send to Sheet', 'Drop .ai / .dxf designs here']); assert(info.buttons.includes('Hold'), 'the Hold button is still there: ' + info.buttons.join(', '));
    assert.equal(info.send.aria, 'true', 'Send to Sheet is greyed until a design is dropped'); assert.doesNotMatch(info.send.cls, /gold/); assert.equal(info.drop, true);
    // the pictures: no master drawing (the ladybug) in the vector box, the Etsy listing's blue photo beside it
    await page.waitForFunction(sel => { const n = document.querySelector(sel); return n.querySelector('[data-vector]').textContent.includes('No vector available') && n.querySelector('[data-listing] img'); }, cardSel, { timeout: 20000 });
    const pics = await page.evaluate(async sel => { const n = document.querySelector(sel), img = n.querySelector('[data-listing] img'); await img.decode().catch(() => {}); const cv = document.createElement('canvas'); cv.width = cv.height = 8; const c = cv.getContext('2d'); c.drawImage(img, 0, 0, 8, 8); const d = c.getImageData(1, 1, 1, 1).data;
      return { vector: n.querySelector('[data-vector]').textContent.trim(), vectorImg: !!n.querySelector('[data-vector] img'), drawn: window.__drawn, px: [d[0], d[1], d[2]], w: img.naturalWidth, cap: [...n.querySelectorAll('figcaption')].map(x => x.textContent.replace(/↺/g, '').trim()) }; }, cardSel);
    assert.deepEqual([pics.vector, pics.vectorImg, pics.drawn], ['No vector available', false, 0], 'the ladybug is never drawn for it: ' + JSON.stringify(pics)); assert(pics.w > 0 && pics.px[2] > pics.px[0] + 20, 'the Etsy listing photo, the blue one: ' + JSON.stringify(pics.px)); assert.deepEqual(pics.cap, ['Vector design', 'Etsy listing']);
    // its designs: dropped on the card, Send to Sheet is the next step; nothing is written until a person presses it
    const { DESIGN_AI } = await designFile();
    await page.evaluate(({ sel, b64 }) => { const dt = new DataTransfer(); dt.items.add(new File([Uint8Array.from(atob(b64), c => c.charCodeAt(0))], 'grizzly.ai')); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt })); }, { sel: cardSel, b64: DESIGN_AI });
    await page.waitForFunction(() => document.querySelector('#cuDlg[open] .cuFile .cuThumb img'), null, { timeout: 30000 });
    await page.click('#cuDlg [data-x]');
    const sendNow = await page.evaluate(sel => { const s = document.querySelector(sel + ' [data-cu-send]'); return { aria: s.getAttribute('aria-disabled'), cls: s.className, strip: document.querySelector(sel + ' .cuDesigns')?.textContent }; }, cardSel);
    assert.equal(sendNow.aria, null, 'ready'); assert.match(sendNow.cls, /gold/, 'Send to Sheet is the next step once a .ai is dropped'); assert.match(sendNow.strip, /1 design/);
    assert.deepEqual(writes.filter(w => !/^(aliasPut|optionMapPut)$/.test(w)), [], 'nothing was written by looking: ' + writes.join());

    // the line is held with the OLD question (what a stored line looks like) → it re-reads by itself when the word for its listing changes, and waits for its designs
    const flip = await page.evaluate(k => {
      const row = B.orders.byKey.get(k), out = [], was = B.maps.aliases;
      B.maps.aliases = Object.assign({}, was, { 1777293722: { listingKind: { kind: 'regular', by: 'Paul' } } }); Orders.interpretAll(); out.push([row.spec.special ? row.spec.special.kind : null, row.problems.map(p => p.kind + ':' + p.optionName).join()]);   // (as read before the rule)
      row.state = 'held'; row.reason = 'option "Price: Gold Filled - Single" not mapped';
      B.maps.aliases = was; Orders.interpretAll(); out.push([row.spec.special ? row.spec.special.kind : null, row.problems.length, row.state, row.reason]);
      // (a line whose pieces are already pooled keeps them: nothing is taken off a sheet)
      row.state = 'pooled'; row.poolIds = [k + '_1', k + '_2']; Orders.interpretAll(); out.push([row.state, row.poolIds.join()]);
      row.state = 'held'; row.poolIds = []; Orders.interpretAll(); Review.syncOrderItems(); Review.render();
      return out;
    }, KEY);
    assert.deepEqual(flip, [[null, 'needsMapping:Price'], ['customHuggies', 0, 'held', O.specialOf.addOn.wait], ['pooled', KEY + '_1,' + KEY + '_2']], 'before: asked; after: custom, no question, held for its designs; pooled pieces kept: ' + JSON.stringify(flip));
    // the ladybug does get drawn for the same line when it is a regular line (the stub counts, so the zero above was a proof); as a custom line the piece dots and the stations get no drawing either
    const ctl = await page.evaluate(async k => {
      B.master.entries.set('HUGGIE_3722', { sku: 'HUGGIE_3722', widthPt: 18, heightPt: 30, thumbUrl: '', aiPath: 'charmnest/master/HUGGIE_HOOPS-_LADYBUG_1.ai' });   // (the library may have been read again by now: its answer replaces the map)
      const row = B.orders.byKey.get(k), own = { key: PieceMedia.vectorKey(row), thumb: await PieceMedia.vectorThumb(row), drawn: window.__drawn };
      B.maps.aliases = Object.assign({}, B.maps.aliases, { 1777293722: { listingKind: { kind: 'regular' } } }); Orders.interpretAll(); Review.syncOrderItems(); Review.render();
      return { own, regular: { key: PieceMedia.vectorKey(row), thumb: !!(await PieceMedia.vectorThumb(row)), drawn: window.__drawn, special: !!row.spec.special } };
    }, KEY);
    assert.deepEqual([ctl.own.key, ctl.own.thumb, ctl.own.drawn], ['', null, 0], 'no drawing of it anywhere: ' + JSON.stringify(ctl.own));
    assert.match(ctl.regular.key, /^sku:HUGGIE_3722/); assert.equal(ctl.regular.thumb, true, JSON.stringify(ctl)); assert(ctl.regular.drawn > 0, 'the same line, as a regular line, is drawn as the ladybug: ' + JSON.stringify(ctl.regular)); assert.equal(ctl.regular.special, false);
    assert((await chips()).some(t => /^Options/.test(t)), 'as a regular line it is asked about its option again: ' + (await chips()).join(' | '));
    await page.evaluate(() => { B.maps.aliases = {}; Orders.interpretAll(); Review.syncOrderItems(); Review.render(); });
    assert.equal(await page.evaluate(k => B.orders.byKey.get(k).spec.special.label, KEY), 'Custom huggies', 'back as custom with the word taken off: the wording alone says it');
    assert.equal(await page.evaluate(() => window.__reads > 0), true, 'the page asked for Claude\'s reading of the chain line, which the test swallowed'); assert.equal(await page.evaluate(k => B.orders.byKey.get(k).spec.readable, KEY), false, 'the add-on line is never one of those');
    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ Chromium: one Custom Orders card for 4176744752, no question, no ladybug, the blue Etsy photo, Send to Sheet greyed until a .ai, Hold; re-read by itself, pooled pieces kept');
  } finally { await b.close(); srv.close(); }
}
async function designFile() {
  global.window = global; global.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
  const D = require(path.join(root, 'charm-nest-dxf.js'));
  const G = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
  const tag = G(0, 'SECTION', 2, 'ENTITIES', 0, 'CIRCLE', 8, '0', 10, 0, 20, 0, 40, 7, 0, 'CIRCLE', 8, '0', 10, 0, 20, 5, 40, 1, 0, 'ENDSEC', 0, 'EOF');
  return { DESIGN_AI: Buffer.from((await D.toPdf(tag, 'tag.dxf')).bytes).toString('base64') };   // an .ai is a PDF
}

(async () => {
  await server();
  await browser();
  console.log('Add-on listings OK: the Huggie Charm + Shipping line of 4176744752 is a custom order (no question, no master drawing, Send to Sheet after a design), 2 of the 411 snapshot lines change, nothing live written');
})().catch(e => { console.error(e); process.exit(1); });
