// LOOKALIKE (12 Oct 2026): a necklace line must never be silently pooled on a drawing that has no hanging hole when a differently drawn lookalike WITH a ring carries the same name.
//
// THE CASE (four times on 12 Oct the exact SKU put the wrong drawing on an order and Paul found it by eye): the master holds "CELESTIAL3-COMET" (a stud-size comet, 9.3 x 9.1 mm, NO hole) and
// "CELESTIAL 3 - COMET" (the necklace comet, 8.3 x 14.4 mm, 2 holes). Etsy's SKU "Celestial3-Comet" equals the first one exactly, so the exact name won, silently, on a comet NECKLACE.
// The Running Shoe, Mouse, Sheep, Crown and Jupiter necklaces are the same mistake (a stud-size master named like the necklace one).
//
// THE RULE (charm-nest-orders.js section 3b, lookalikeRead): a line whose FORM is "necklace" that matched a 0-hole drawing by its exact name (or the -CO base, or Etsy's inventory SKU) while another master entry
// of the same name once spacing and punctuation are set aside is a DIFFERENT drawing (another design file and charm), is not blocked, has a hole and a box at least 1.2 times as big, is NOT pooled. It waits with one
// plain line naming the drawings; the Review row offers each as a tap, and the tap is the per-listing, per-SKU saved link (aliasPut bySku) that already beats the exact match.
// It stays silent when a person has answered for the listing and SKU, when the two names are one drawing, when either is blocked, when the form agrees with the exact name, when the ringed one is not clearly
// the bigger (Playing Cards and Megaphone: the pictures contradict their hole counts), or when no lookalike lister is given (a module without ctx.masterLike reads exactly as before).
// Offline: no network, no Etsy, no AI, nothing written.   node tests/charm-nest/lookalike.cjs [path to a folder with playwright-core]
const assert = require('assert/strict'), path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

// the real entries (the master index of 10 Oct 2026: hole count, box in points, design file, charm hash)
const E = [
  ['CELESTIAL3-COMET', 0, 26.24, 25.677, 'charmnest/sandbox/master/CELESTIAL3-COMET.ai', '00375e40'], ['CELESTIAL 3 - COMET', 2, 23.408, 40.94, 'charmnest/sandbox/master/COMET_9427.ai', '69ad6256'],
  ['SPORTS9-RUNNING SHOE', 0, 28.57, 17.652, 'charmnest/master/SPORTS9-RUNNING_SHOE.ai', 'cb1b5d3e'], ['SPORTS 9 - RUNNING SHOE', 1, 29.896, 35.03, 'charmnest/master/SPORTS_9_-_RUNNING_SHOE.ai', '9db229cc'],
  ['MOUSE2', 0, 27.641, 20.154, 'charmnest/sandbox/master/MOUSE2.ai', '039bae02'], ['MOUSE 2', 3, 34.815, 29.579, 'charmnest/sandbox/master/MOUSE_2.ai', '48f17d65'],
  ['SHEEP3', 0, 20.997, 26.874, 'charmnest/sandbox/master/SHEEP3.ai', '25132944'], ['SHEEP 3', 1, 25.74, 39.283, 'charmnest/sandbox/master/SHEEP_3.ai', '6e41acf0'],
  ['CROWN2', 0, 27.084, 19.509, 'charmnest/sandbox/master/CROWN2.ai', '43284fa5'], ['CROWN 2', 1, 34.815, 30.686, 'charmnest/sandbox/master/CROWN_2.ai', '3e490c63'],
  ['SPACE 1 - JUPITER', 0, 27.945, 27.943, 'charmnest/sandbox/master/SPACE_1_-_JUPITER.ai', '4ca11afd'], ['SPACE1-JUPITER', 1, 31.346, 36.437, 'charmnest/sandbox/master/SPACE1-JUPITER.ai', '2a68b3e9'],
  // the two pairs the size condition keeps quiet: the "ringed" twin is SMALLER than the loop-less one (the master draws or counts them the other way round; their pictures do not bear the hole counts out)
  ['CARDS2- 4 CARDS', 0, 32.728, 35.413, 'charmnest/sandbox/master/DECK_0120.ai', '8b11a601'], ['CARDS 2 - (4 CARDS)', 1, 27.606, 27.405, 'charmnest/sandbox/master/CARDS_2_-_4_CARDS_.ai', '1fbfa8b7'],
  ['CHEER2- MEGAPHONE(CHEER)', 0, 49.41, 26.631, 'charmnest/master/CHEER2-_MEGAPHONE_CHEER_.ai', 'b76cd5d0'], ['CHEER 2 - MEGAPHONE(CHEER)', 2, 33.597, 26.527, 'charmnest/sandbox/master/CHEER_2_-_MEGAPHONE_CHEER_.ai', '539f567f'],
  // synthetic: one drawing under two names (one design file; and one charm hash in two files), a pair with the ring on both, a pair with no hole count, a blocked twin
  ['ONEFILE', 0, 20, 20, 'charmnest/master/ONEFILE.ai', 'aaaa0001'], ['ONE FILE', 1, 30, 30, 'charmnest/master/ONEFILE.ai', 'aaaa0002'],
  ['SAMEHASH', 0, 20, 20, 'charmnest/master/SAMEHASH_A.ai', 'bbbb0001'], ['SAME HASH', 1, 30, 30, 'charmnest/master/SAMEHASH_B.ai', 'bbbb0001'],
  ['BOTHRING', 1, 20, 20, 'charmnest/master/BOTHRING_A.ai', 'cccc0001'], ['BOTH RING', 2, 30, 30, 'charmnest/master/BOTHRING_B.ai', 'cccc0002'],
  ['NOHOLES', 0, 20, 20, 'charmnest/master/NOHOLES_A.ai', 'dddd0001'], ['NO HOLES', null, 30, 30, 'charmnest/master/NOHOLES_B.ai', 'dddd0002'],
  ['BLOCKTWIN', 0, 20, 20, 'charmnest/master/BLOCKTWIN_A.ai', 'eeee0001'], ['BLOCK TWIN', 1, 30, 30, 'charmnest/master/BLOCKTWIN_B.ai', 'eeee0002'],
  ['THIRD3', 0, 20, 20, 'charmnest/master/THIRD3_A.ai', 'ffff0001'], ['THIRD 3', 1, 30, 30, 'charmnest/master/THIRD3_B.ai', 'ffff0002'], ['THIRD-3', 2, 31, 31, 'charmnest/master/THIRD3_C.ai', 'ffff0003'],
  ['PLAIN_ONE', 1, 30, 30, 'charmnest/master/PLAIN_ONE.ai', '12340001']
];
const lib = new Map(E.map(([sku, holes, widthPt, heightPt, aiPath, charmHash]) => [sku, { sku, aiPath, widthPt, heightPt, holes, charmHash, members: 3, sizes: null, blocked: sku === 'BLOCK TWIN' ? 'SKU present in two master files' : null }]));
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const loose = {}, like = {}, byFile = new Map();
for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const sharedFor = s => { const e = entryFor(s); return e ? byFile.get(e.aiPath) || [] : []; };
const likeFor = s => (like[O.looseKey(s)] || []).slice();
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor, masterLike: likeFor, masterSkus: () => [...lib.keys()] }, extra || {});
let tx = 8100000000;
const NECK = [['Metal Choice', 'Gold'], ['Necklace Length in inches', '18"']], STUD = [['Metal Choice', 'Silver - Pair']];
const mk = (sku, listing, title, vars) => ({ transactionId: String(++tx), listingId: String(listing), sku, title, quantity: 1, metalKey: /silver/i.test(JSON.stringify(vars || NECK)) ? 'silver' : 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: (vars || NECK).map(([name, value]) => ({ name, value })) });
const read = (l, extra) => O.interpretLine({ receiptId: 'R' + l.transactionId, updateTs: 1 }, JSON.parse(JSON.stringify(l)), ctx(extra));
const alias = (listing, v) => ({ aliases: { [String(listing)]: v } });
const waits = sp => !!sp.formHold && !!sp.formHold.lookalike;
const unmatched = sp => sp.problems.filter(p => p.kind === 'unmatchedSku');

// [Etsy's SKU as the sandbox order wrote it, listing, title, the loop-less master entry it equals, the ringed lookalike]
const CASES = [
  ['sports9-running shoe', '1703006901', 'Running Shoe Necklace, Runner\'s Gift, Marathon Gift', 'SPORTS9-RUNNING SHOE', 'SPORTS 9 - RUNNING SHOE'],
  ['Mouse2', '1745415624', 'Mouse Necklace Gold Mouse Pendant Charm Gift for Her', 'MOUSE2', 'MOUSE 2'],
  ['Sheep3', '1697711981', 'Sheep Pendant, Lamb Necklace, Animal Jewelry', 'SHEEP3', 'SHEEP 3'],
  ['Crown2', '1680266261', 'Crown Necklace Gold Charm Queen Pendant', 'CROWN2', 'CROWN 2'],
  ['SPACE 1 - Jupiter', '1804990842', 'Jupiter Charm Planet Pendant Add On Charm', 'SPACE 1 - JUPITER', 'SPACE1-JUPITER'],
  ['Celestial3-Comet', '1687890868', 'Comet Pendant Charm Necklace Dainty Space Jewelry', 'CELESTIAL3-COMET', 'CELESTIAL 3 - COMET']
];

function theCase() {
  for (const [sku, listing, title, X, Y] of CASES) {
    const sp = read(mk(sku, listing, title));
    ok(sp.lineForm === 'necklace', sku + ' is a necklace line: ' + sp.lineForm);
    ok(waits(sp), sku + ' waits instead of taking the loop-less drawing');
    eq([sp.designSku, sp.skuSource, sp.sources.sku], [null, null, null], sku + ': no design is on the line, so nothing can be pooled');
    eq(sp.formHold.design, X, sku + ': the held design is the exact name');
    eq(sp.formHold.candidates.map(c => c.sku), [X, Y], sku + ': the two drawings, the exact name first');
    eq(sp.formHold.candidates.map(c => c.ring), [false, true], sku + ': one without a loop, one with a ring');
    const p = unmatched(sp); eq(p.length, 1, sku + ': ONE question'); ok(p[0].formHold === true && p[0].lookalike === listing, 'the question is the listing\'s own: ' + p[0].lookalike);
    eq(p[0].candidates, sp.formHold.candidates, 'the chips are the problem\'s candidates');
    ok(p[0].reason.includes(X) && p[0].reason.includes(Y) && /no loop/.test(p[0].reason) && /with ring/.test(p[0].reason) && /mm/.test(p[0].reason) && /Pick the right one\./.test(p[0].reason) && !/SKU \?/.test(p[0].reason), 'ONE plain line naming both drawings: ' + p[0].reason);
    eq(sp.boughtSku, String(sku).trim().toUpperCase(), 'the SKU as bought stays on the line (the saved link is keyed by it)');
  }
  const comet = read(mk('Celestial3-Comet', '1687890868', 'Comet Pendant Charm Necklace'));
  eq(unmatched(comet)[0].reason, 'This SKU matches two different drawings: CELESTIAL3-COMET (no loop, 9.3 × 9.1 mm) and CELESTIAL 3 - COMET (with ring, 8.3 × 14.4 mm), and this listing is a necklace, which a drawing with no loop cannot hang on. Pick the right one.', 'the comet line, word for word');
  step('six necklace lines (Running Shoe, Mouse, Sheep, Crown, Jupiter, Comet) wait with one plain line and two drawings; no design is on the line');
}

function whatStaysQuiet() {
  // the same SKUs on a pair of stud earrings: the loop-less drawing is right; said once in the reading, nothing waits
  for (const [sku, listing, title, X] of CASES) {
    const sp = read(mk(sku, listing, title.replace(/Necklace|Pendant|Charm/g, 'Earrings') + ' Stud Earrings', STUD));
    ok(!waits(sp) && sp.designSku === X && sp.skuSource === 'transaction' && !unmatched(sp).length, sku + ' on stud earrings: ' + X + ' as before (' + sp.lineForm + ')');
    ok(sp.lookalike && sp.lookalike.state === 'agrees' && sp.pair.notes.some(t => /look-alike/.test(t) && /no loop/.test(t)), 'and the reading says so: ' + JSON.stringify(sp.pair.notes));
  }
  // a necklace on the RINGED entry is right
  { const sp = read(mk('CELESTIAL 3 - COMET', '1687890868', 'Comet Pendant Charm Necklace')); ok(!waits(sp) && sp.designSku === 'CELESTIAL 3 - COMET' && sp.skuSource === 'transaction', 'a necklace on the ringed comet stays'); eq(sp.lookalike.state, 'agrees');
    ok(sp.pair.notes.some(t => /look-alike/.test(t) && /with ring/.test(t)), 'it says so in one line: ' + JSON.stringify(sp.pair.notes)); }
  // a line whose form the title and options do not say reads as before (nothing can judge it)
  { const sp = read(mk('Celestial3-Comet', '1687890868', 'Comet Charm', [['Metal Choice', 'Gold']])); eq([sp.lineForm, sp.designSku, !!sp.lookalike, unmatched(sp).length], ['unknown', 'CELESTIAL3-COMET', false, 0], 'a line of unknown form reads as before'); }
  // Playing Cards and Megaphone: the ringed twin is SMALLER than the loop-less drawing (0.65 and 0.68 of its box), the thumbnails show a ring on the "no hole" one: the exact name stays, nothing is stopped
  for (const [sku, listing, title, X] of [['Cards2- 4 cards', '1733710503', 'Playing Cards Charm Necklace', 'CARDS2- 4 CARDS'], ['Cheer2- Megaphone(Cheer)', '1719529614', 'Megaphone Cheer Necklace', 'CHEER2- MEGAPHONE(CHEER)']]) {
    const sp = read(mk(sku, listing, title)); eq([sp.lineForm, sp.designSku, !!sp.lookalike, unmatched(sp).length], ['necklace', X, false, 0], sku + ' is not stopped: the ringed twin is not the bigger drawing');
  }
  // one drawing under two names (one design file; or one charm in two files): nothing to choose
  for (const [sku, X] of [['OneFile', 'ONEFILE'], ['SameHash', 'SAMEHASH']]) { const sp = read(mk(sku, '555', 'Necklace Charm')); eq([sp.designSku, !!sp.lookalike, unmatched(sp).length], [X, false, 0], sku + ': two names, one drawing'); }
  // both on the same side (both ringed), no hole count, a blocked twin, no lookalike at all: the exact name stands
  for (const [sku, X] of [['BothRing', 'BOTHRING'], ['NoHoles', 'NOHOLES'], ['BlockTwin', 'BLOCKTWIN'], ['Plain_One', 'PLAIN_ONE']]) { const sp = read(mk(sku, '556', 'Necklace Charm')); eq([sp.designSku, !!sp.lookalike, unmatched(sp).length], [X, false, 0], sku + ': the exact name stands'); }
  // the exact entry itself blocked: the lookalike rule says nothing (the blocked-SKU path is its own)
  { const was = lib.get('SHEEP3').blocked; lib.get('SHEEP3').blocked = 'blocked for the test'; const sp = read(mk('Sheep3', '1697711981', 'Sheep Necklace')); ok(!sp.lookalike && !waits(sp), 'a blocked exact entry is not this rule\'s'); lib.get('SHEEP3').blocked = was; }
  // the module without a lookalike lister (a netlify function, an older page) reads exactly as before
  { const c = ctx(); delete c.masterLike; const sp = O.interpretLine({ receiptId: 'R1', updateTs: 1 }, mk('Sheep3', '1697711981', 'Sheep Necklace'), c); eq([sp.designSku, sp.skuSource, !!sp.lookalike], ['SHEEP3', 'transaction', false], 'without ctx.masterLike nothing changes'); }
  step('quiet: stud earrings on the loop-less drawing, a necklace on the ringed one, unknown form, Playing Cards and Megaphone, one drawing under two names, ring on both sides, no hole count, a blocked twin, no ctx.masterLike');
}

function savedWins() {
  const L = '1687890868', Y = 'CELESTIAL 3 - COMET', X = 'CELESTIAL3-COMET', t = 'Comet Pendant Charm Necklace';
  // the tap on the ringed chip: the per-listing, per-SKU link names it and beats the exact name (DACHMAP), no wait
  let sp = read(mk('Celestial3-Comet', L, t), alias(L, { v: 2, bySku: { 'CELESTIAL3-COMET': Y } }));
  eq([sp.designSku, sp.skuSource, !!sp.formHold, !!sp.lookalike, unmatched(sp).length], [Y, 'alias', false, false, 0], 'a saved link to the ringed comet wins');
  // the tap on the loop-less chip (a person decided it is right): the link names the SKU itself, so the exact name stands and the rule does not ask again
  sp = read(mk('Celestial3-Comet', L, t), alias(L, { v: 2, bySku: { 'CELESTIAL3-COMET': X } }));
  eq([sp.designSku, !!sp.formHold, !!sp.lookalike, unmatched(sp).length], [X, false, false, 0], 'a saved link to the loop-less comet is a person\'s word: no question again');
  // a saved pair, the older listing-wide answer, and the SKU written another way
  sp = read(mk('Celestial3-Comet', L, t), alias(L, { v: 2, pairBySku: { 'CELESTIAL3-COMET': { L: X, R: Y } } })); ok(!waits(sp) && !sp.lookalike, 'a saved pair is a person\'s word too');
  sp = read(mk('Celestial3-Comet', L, t), alias(L, { sku: Y })); eq([sp.designSku, !!sp.formHold, !!sp.lookalike], [X, false, false], 'an older listing-wide answer (no v) is a person\'s word for the listing: the rule stays quiet and the exact name stands as DACHMAP leaves it');
  sp = read(mk('CELESTIAL3-COMET ', L, t), alias(L, { v: 2, bySku: { 'CELESTIAL3-COMET': Y } })); eq(sp.designSku, Y, 'the same SKU written another way');
  // a link for ANOTHER listing or ANOTHER SKU is not an answer for this one
  ok(waits(read(mk('Celestial3-Comet', L, t), alias(L, { v: 2, bySku: { 'SOMETHING ELSE': Y } }))), 'a link for another SKU of the listing does not silence it');
  ok(waits(read(mk('Celestial3-Comet', L, t), alias('999', { v: 2, bySku: { 'CELESTIAL3-COMET': Y } }))), 'a link for another listing does not silence it');
  ok(waits(read(mk('Celestial3-Comet', L, t), alias(L, { v: 2, sku: Y }))), 'a listing-wide v2 answer never overrode an exact master SKU (DACHMAP), so it does not here');
  step('a saved link, either chip of it, a pair, the older listing-wide answer: all end the wait for good; a link for another listing or SKU does not');
}

function wordsAndSets() {
  // three lookalikes (one loop-less, two ringed): both ringed ones are offered after the exact one
  const sp = read(mk('Third3', '557', 'Third Necklace Charm')); ok(waits(sp), 'THIRD3 waits'); eq(sp.formHold.candidates.map(c => c.sku), ['THIRD3', 'THIRD 3', 'THIRD-3'], 'the exact name first, then each ringed lookalike');
  ok(/This SKU matches 3 different drawings/.test(unmatched(sp)[0].reason), unmatched(sp)[0].reason);
  // an add-on / huggie line is the form guard's: a huggie design is never a lookalike candidate here (it has its own hold)
  const hs = O.lookalikeRead('CELESTIAL3-COMET', { form: 'necklace' }, { masterEntry: entryFor, masterLike: likeFor }); eq(hs.state, 'held', 'lookalikeRead is the one place the rule lives');
  eq(O.lookalikeRead('CELESTIAL3-COMET', { form: 'earring' }, { masterEntry: entryFor, masterLike: likeFor }).state, 'agrees', 'an earring line agrees with the loop-less drawing');
  eq(O.lookalikeRead('CELESTIAL3-COMET', { form: 'unknown' }, { masterEntry: entryFor, masterLike: likeFor }), null, 'an unknown form says nothing');
  eq(O.lookalikeRead('CELESTIAL3-COMET', { form: 'necklace' }, { masterEntry: entryFor }), null, 'without a lister it says nothing');
  step('three lookalikes are offered in order; the reader is a pure function that says nothing without a lister or a form');
}

function pageIsWired() {
  const h = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), b = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-lk1(?:-[a-z0-9]+)*"').test(h), f + ' carries the -lk1 cache token');
  ok(/masterLike: sku => Master\.likeFor\(sku\)/.test(b) && /function likeFor\(sku\)/.test(b), 'the bridge gives the module its lookalike lister');
  ok(/p\.lookalike \? SEP \+ "K" \+ p\.lookalike/.test(b), 'one decision per listing and SKU');
  step('the page asks for the new scripts and the bridge lists lookalikes');
}

/* ── the browser part: the page shows the wait, the chips and the saved link; an order already pooled keeps its pieces ── */
async function browser() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return false; }
  const { start } = require('./bridge-server.cjs');
  const b = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const SHIP = Math.floor(Date.UTC(2026, 9, 19, 17) / 1000), DAY = 86400, LISTING = '1687890868';
  const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const line = (tid, sku, title, vars) => ({ transactionId: tid, listingId: LISTING, productId: '31551438733', sku, title, quantity: 1, expectedShipDate: SHIP, variations: vars.map(([name, value]) => ({ name, value })), metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', staffNote: '', messages: [] });
  const entries = ['CELESTIAL3-COMET', 'CELESTIAL 3 - COMET'].map(s => Object.assign({}, lib.get(s), { thumbUrl: '/.netlify/functions/imageProxy?url=thumb-' + encodeURIComponent(s) }));
  async function scenario(o, ents, run) {
    const srv = await start({ receipts: [] });
    try {
      const context = await b.newContext({ viewport: { width: 1440, height: 950 } });
      const PHOTO = '<svg xmlns="http://www.w3.org/2000/svg" width="240" height="240"><rect width="240" height="240" fill="#c9a25a"/><circle cx="120" cy="120" r="34" fill="#4a7a3a"/></svg>';
      await context.route(/\/\.netlify\/functions\/imageProxy/, r => r.fulfill({ status: 200, contentType: 'image/svg+xml', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: PHOTO }));
      await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
      await context.addInitScript(photos => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.listingPhotos.v1', JSON.stringify(photos)); } catch (_) {} window.prompt = () => 'Test Operator'; }, [[LISTING, '/.netlify/functions/imageProxy?url=listing-' + LISTING]]);
      const page = await context.newPage(), errors = [], writes = [];
      page.setDefaultTimeout(30000);
      page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
      page.on('request', rq => { if (/charmNestLibrary/.test(rq.url()) && rq.method() === 'POST') { try { const body = JSON.parse(rq.postData() || '{}'); if (/Put|Decide|Delete|Reopen|Reset|Purge|Update|Apply|Save|Commit/.test(body.op || '')) writes.push(body); } catch (_) {} } });
      await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
      await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
      await page.evaluate(async ({ entries, o }) => {
        CustomRead.later = () => {};   // (Claude's reading of lines is never started by a test: nothing is paid for)
        B.master.entries = new Map(entries.map(e => [e.sku, Object.assign({ members: 1 }, e)])); B.master.index = { seeded: true }; B.master.loadedAt = Date.now();
        for (const k of ['masterPreview', 'masterFront']) { const keep = Pool[k]; Pool[k] = function () { return 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" fill="#3a7"/></svg>'); }; Pool['_' + k] = keep; }
        await Orders.loadMaps(true);
        for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, { entries: ents, o });
      const keys = await page.evaluate(o => o.lines.map(l => CharmNestOrders.lineKey(o, l)), o);
      await run({ page, srv, keys, writes, errors });
      eq(errors, [], 'no page errors');
    } finally { srv.close(); }
  }
  const NECKV = [['Metal Choice', 'Gold'], ['Necklace Length in inches', '18&quot;']], STUDV = [['Metal Choice', 'Silver - Pair']];

  /* 1 · the comet necklace waits: the plain line, two one-tap chips with their drawings, no "use both"; a tap saves the listing's link once and the question goes */
  await scenario(order('4197740562', [line('9200000001', 'Celestial3-Comet', 'Comet Pendant Charm Necklace Dainty Space Jewelry', NECKV)]), entries, async ({ page, keys, writes }) => {
    const rowSel = `#rvList .reviewListRow[data-row="${keys[0]}"]`;
    await page.waitForSelector(rowSel + ' .sibOffer [data-dsku]');
    const rv = await page.evaluate(sel => { const nd = document.querySelector(sel); return { reason: nd.querySelector('.reviewReason').textContent, ask: nd.querySelector('.sibAsk').textContent, chips: [...nd.querySelectorAll('.sibOffer [data-dsku]')].map(x => x.dataset.dsku), both: nd.querySelectorAll('.sibOffer [data-both]').length, pics: nd.querySelectorAll('.sibOffer [data-dsku] img, .sibOffer [data-dsku] svg, .sibOffer [data-dsku] canvas').length }; }, rowSel);
    eq(rv.reason, 'This SKU matches two different drawings: CELESTIAL3-COMET (no loop, 9.3 × 9.1 mm) and CELESTIAL 3 - COMET (with ring, 8.3 × 14.4 mm), and this listing is a necklace, which a drawing with no loop cannot hang on. Pick the right one.', 'the Review row reads the one plain line');
    eq(rv.chips, ['CELESTIAL3-COMET', 'CELESTIAL 3 - COMET'], 'two chips, the exact name first'); eq(rv.both, 0, 'no "Use both"'); ok(rv.pics >= 2, 'every chip carries its master drawing: ' + rv.pics); ok(!/SKU \?/.test(rv.reason + rv.ask), 'no "SKU ?"');
    eq(await page.evaluate(() => B.review.items.filter(i => i.kind === 'unmatchedSku').length), 1, 'ONE card in Review');
    await page.evaluate(k => OrderWin.open(k), keys[0]);
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 20000 });
    await page.waitForFunction(() => /matches two different drawings/.test(document.getElementById('owNowCard').textContent), null, { timeout: 20000 });
    await page.click('#owClose');
    eq(writes.length, 0, 'reading the line and opening the window wrote nothing: ' + JSON.stringify(writes.map(w => w.op)));
    await page.click(`${rowSel} .sibOffer [data-dsku="CELESTIAL 3 - COMET"]`);
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.skuSource === 'alias', keys[0], { timeout: 30000 });
    const aw = writes.filter(w => w.op === 'aliasPut'); eq(aw.length, 1, 'one write, a person\'s');
    eq([aw[0].listingId, aw[0].sku, aw[0].fromSku], [LISTING, 'CELESTIAL 3 - COMET', 'CELESTIAL3-COMET'], 'the per-listing, per-SKU saved link');
    eq(await page.evaluate(k => { const r = B.orders.byKey.get(k); return [r.spec.designSku, !!r.spec.formHold, r.problems.length]; }, keys[0]), ['CELESTIAL 3 - COMET', false, 0], 'the line reads as the ringed comet from then on');
    eq(await page.evaluate(() => B.review.items.filter(i => i.kind === 'unmatchedSku').length), 0, 'the question is gone');
  });
  console.log('  ✓ Chromium: the comet necklace shows one plain line and two chips with drawings (no Use both); a tap saves the per-listing per-SKU link once and the question goes');

  /* 2 · the same SKU on stud earrings: nothing in Review, the loop-less comet */
  await scenario(order('4197740563', [line('9200000002', 'Celestial3-Comet', 'Comet Stud Earrings', STUDV)]), entries, async ({ page, keys, writes }) => {
    eq(await page.evaluate(k => { const r = B.orders.byKey.get(k); return [r.spec.designSku, r.problems.length, B.review.items.filter(i => i.kind === 'unmatchedSku').length]; }, keys[0]), ['CELESTIAL3-COMET', 0, 0], 'stud earrings keep the loop-less comet, no question');
    eq(writes.length, 0, 'nothing written');
  });
  console.log('  ✓ Chromium: stud earrings on the loop-less comet ask nothing');

  /* 3 · an order already pooled as the loop-less comet (the library had no ringed twin when it was read): the twin arrives, the line is read again, the question is raised, the row keeps its state and pieces */
  await scenario(order('4197740564', [line('9200000003', 'Celestial3-Comet', 'Comet Pendant Charm Necklace', NECKV)]), [entries[0]], async ({ page, keys, writes }) => {
    const st = await page.evaluate(k => { const r = B.orders.byKey.get(k); r.state = 'pooled'; r.poolIds = ['P-TEST-1']; return [r.spec.designSku, r.problems.length, B.review.items.filter(i => i.kind === 'unmatchedSku').length]; }, keys[0]);
    eq(st, ['CELESTIAL3-COMET', 0, 0], 'with only the loop-less comet in the library the line reads and pools as it did');
    await page.evaluate(ents => { B.master.entries = new Map(ents.map(e => [e.sku, Object.assign({ members: 1 }, e)])); Orders.interpretAll(); Review.syncOrderItems(); Review.render(); }, entries);
    const after = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { state: r.state, pools: r.poolIds.slice(), design: r.spec.designSku, hold: !!r.spec.formHold, look: r.spec.lookalike && r.spec.lookalike.state, items: B.review.items.filter(i => i.kind === 'unmatchedSku').length }; }, keys[0]);
    eq(after, { state: 'pooled', pools: ['P-TEST-1'], design: null, hold: true, look: 'held', items: 1 }, 'the row stays pooled with its pieces; one question is raised for a person');
    eq(writes.length, 0, 'nothing written');
  });
  console.log('  ✓ Chromium: an order already pooled keeps its state and pieces when the lookalike arrives; one question is raised and nothing is written');
  await b.close();
  return true;
}

(async () => {
  theCase(); whatStaysQuiet(); savedWins(); wordsAndSets(); pageIsWired();
  const ran = await browser();
  console.log((ran ? 'Lookalike OK:' : 'Lookalike (node part only, no browser):') + ` ${n} checks. A necklace on a loop-less drawing whose lookalike has a ring waits for one tap; every other line reads as before; a saved link ends the wait.`);
})().catch(e => { console.error(e); process.exit(1); });
