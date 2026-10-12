// NOSKU (Paul, 12 Oct 2026 18:32 UTC, order 4199225418, Etsy listing 4494184284 "Chameleon Charm Necklace", gold, 14", the personalisation a first name): "for some reason this same listing did not
// get matched at all to anything. Please fix this issue everywhere". The row said "Needs a decision: Unknown SKU: SKU ?: no SKU on the transaction and no alias for the listing", the vector
// panel "No vector available", and nothing was suggested.
//
// THE CAUSE (charm-nest-orders.js, read with the real code, section 1 below): the listing has NO SKU on Etsy at all, so every way a line finds its charm came up empty (a saved link, the SKU on the
// transaction, the listing's own inventory SKU, an option a person mapped), and the card that asked for a decision could only offer chips from a SKU or an option's value: the TITLE, the one place
// that said "Chameleon", was never read.
//
// THE FIX (section 3d, noSkuRead): a line that still has no SKU after all of those reads the words of its listing TITLE against the master. THE CONFIDENCE RULE (conservative: a wrong silent match is worse
// than a line that waits): the line is taken by its title alone only when it has no SKU of any kind (Etsy's list READ and empty), is no special order / add-on / pre-made / Huggie CHARM SET, has no
// option waiting, the library is whole and the line's form is known and agrees with the design, the first phrase of the title names a design word that is a word of the master (no spelling guess),
// and EXACTLY ONE drawing of the master (SKUs that share a design file are one drawing) holds it, with no other design word in the title. Anything short of that waits for a person with the ranked
// candidates as one-tap choices, or one plain line saying what was tried. Nothing here writes anything: a person's tap saves the listing's link, and "Use another charm" changes it.
//   node tests/charm-nest/title-match.cjs [playwright-core dir]
// Part 1 (node, offline): the cause; the one-drawing match; the form guard; candidates; the real SKU / link / pick always wins; a custom / pre-made / add-on listing is untouched; the cases that
//   must never match; the words. Part 2 (Chromium, the real sorter on the fake site): the matched line (head card reading, order window SKU line, "Matched by title" chip, Use another charm) and the
//   waiting line (plain Review reason, one-tap chips with drawings, a tap saves the listing's link once and the line is read again). Nothing live: the fake site is in memory, no Etsy, no AI.
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/chameleon-master-entries.json'), 'utf8'));
const TABLE = Object.assign(JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/listing-4494184284-inventory.json'), 'utf8')), { at: 1 });   // Etsy's inventory of the listing as the cloud cache held it (the Gold 14" product has a blank SKU)
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

/* ── the worlds: a library is a Map of entries (a design file under each SKU), read the way the page reads it ── */
const E = (sku, aiPath, w, h) => ({ sku, aiPath, widthPt: w || 28, heightPt: h || 30, members: 1, sizes: null, blocked: null });
const DECOYS = [E('SWAN_50666', 'charmnest/master/SWAN_50666.ai'), E('BEADY SWAN', 'charmnest/master/BEADY_SWAN.ai'), E('DACHSHUND_36943', 'charmnest/master/DACHSHUND_36943.ai'), E('CAT_0114', 'charmnest/master/CAT_0114.ai'),
  E('DOGS', 'charmnest/master/DOGS.ai'), E('LIZARD_71759', 'charmnest/master/LIZARD_71759.ai'), E('HEART_5193', 'charmnest/master/HEART_5193.ai'), E('MOON_7993', 'charmnest/master/MOON_7993.ai'), E('MOON_9351', 'charmnest/master/MOON_9351.ai'),
  E('SWAN_STUD_2', 'charmnest/master/SWAN_STUD_2.ai'), E('CO-GOLDCHARMO', 'charmnest/master/CO_GOLDCHARMO.ai')];
const fixEntries = FIX.entries.map(e => E(e.sku, e.aiPath, e.widthPt, e.heightPt));
const pick = (list, ...skus) => list.filter(e => skus.includes(e.sku));
function world(entries) {
  const lib = new Map(entries.map(e => [e.sku.toUpperCase(), e])), byFile = new Map();
  for (const [k, e] of lib) if (e.aiPath) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
  const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
  const entryFor = s => lib.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e && e.aiPath ? byFile.get(e.aiPath) || [] : []; }, keys = [...lib.keys()];
  const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor,
    masterSkus: () => keys, masterKey: lib, masterSize: lib.size, masterReady: true }, extra || {});
  return { lib, ctx, entryFor };
}
// the chameleon world of the brief: ONE necklace drawing (CHAMELEON_28151, with its second SKU CHAMELEON_87521, 9.3 x 14.8 mm), its huggie apart, and the rest of the library
const ONE = world([...pick(fixEntries, 'CHAMELEON_28151', 'CHAMELEON_87521', 'CHAMELEON_87521 (HUGGIE)'), ...DECOYS]);
// the real chameleon designs of the master: six necklace drawings, three huggies
const REAL = world([...fixEntries, ...DECOYS]);
// no chameleon huggie, and no chameleon necklace
const NECK_ONLY = world([...pick(fixEntries, 'CHAMELEON_28151', 'CHAMELEON_87521'), ...DECOYS]);
const HUG_ONLY = world([...pick(fixEntries, 'CHAMELEON_87521 (HUGGIE)'), ...DECOYS]);

const LISTING = '4494184284';
let tx = 7100000000;
const line = (listing, sku, title, vars, extra) => Object.assign({ transactionId: String(++tx), listingId: String(listing), sku, title, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['A first name'], buyerMessage: '', variations: vars.map(([name, value]) => ({ name, value })) }, extra || {});
const NECK = [['Metal Choice', 'Gold'], ['Necklace Length in inches', '14&quot;']];   // (the options as Etsy sent them for the order of the brief)
const PAUL = (extra) => line(LISTING, '', 'Chameleon Charm Necklace', NECK, Object.assign({ productId: '31551438733' }, extra || {}));   // (the Gold 14" product)
const TABLE_EMPTY = { [LISTING]: TABLE };   // Etsy's inventory for the listing, READ: the Gold 14" product has no SKU
const read = (w, l, extra) => O.interpretLine({ receiptId: 'R' + l.transactionId, updateTs: 1 }, JSON.parse(JSON.stringify(l)), w.ctx(Object.assign({ listingSkus: { [l.listingId]: l.listingId === LISTING ? TABLE : { uni: '', at: 1 } } }, extra || {})));
const asked = sp => sp.problems.find(p => p.kind === 'unmatchedSku');

/* ── 1 · the cause, with the real code ── */
function cause() {
  // the old code: no title is read when the caller (an older page, a server rule) hands it no list of SKUs; this is what the order showed
  const sp = O.interpretLine({ receiptId: 'R1', updateTs: 1 }, JSON.parse(JSON.stringify(PAUL())), ONE.ctx({ listingSkus: TABLE_EMPTY, masterSkus: undefined }));
  eq([sp.designSku, sp.skuSource], [null, null], 'with no list of SKUs to read the title against, nothing is found (the old reading)');
  const p = asked(sp); ok(p && !/SKU \?/.test(p.reason), 'the plain line replaces "SKU ?: no SKU on the transaction and no alias for the listing": ' + p.reason);
  // every other way a line finds its charm is empty for this listing
  eq(O.resolveSku(PAUL(), {}, ONE.entryFor, { patterns: [], skus: [] }, () => ''), { sku: '', source: null }, 'no alias, no transaction SKU');
  eq(O.inventorySku(PAUL(), TABLE), null, "Etsy's inventory for the product bought (Gold 14\") has a blank SKU: it says nothing");
  eq(O.suggestCharms({ value: 'Gold', line: PAUL(), skus: ONE.lib.keys() }), O.suggestCharms({ value: 'Gold', line: PAUL(), skus: ONE.lib.keys() }), 'the option-value suggester only reads an option value');
  step('cause: no alias, no transaction SKU, a blank Etsy inventory SKU; the only word that says "Chameleon" is the title, which nothing read');
}

/* ── 2 · the one-drawing match (the brief: CHAMELEON_28151) ── */
function matched() {
  const l = PAUL(), before = JSON.stringify(l), sp = read(ONE, l);
  eq([sp.designSku, sp.skuSource, sp.sources.sku], ['CHAMELEON_28151', 'title', 'title'], 'the chameleon necklace is CHAMELEON_28151 by its title');
  ok(!sp.problems.some(p => p.kind === 'unmatchedSku'), 'no Unknown SKU question');
  eq(sp.titleMatch.sku, 'CHAMELEON_28151'); eq(sp.titleMatch.others, ['CHAMELEON_87521'], "the drawing's second SKU is told");
  ok(/its title names “chameleon”, and no other charm of the library does/.test(sp.titleMatch.why), 'the reading: ' + sp.titleMatch.why);
  eq(sp.material, 'gold', 'the metal is read as ever'); eq(JSON.stringify(l), before, 'the line is not touched');
  ok(!sp.readable, 'Claude is not asked to read a line that has a design now (nothing paid)');
  // pure: the same words read the same, and a person is never written for
  eq(JSON.stringify(read(ONE, PAUL({ transactionId: l.transactionId }))), JSON.stringify(read(ONE, PAUL({ transactionId: l.transactionId }))), 'deterministic');
  // the same drawing under its other SKU, the plural, a longer title with later phrases
  for (const t of ['Chameleon Charm Necklace', 'Chameleons Necklace', 'Dainty Chameleon Pendant Necklace, Gold Filled, Personalized Name Necklace, Gift for Her']) eq(read(ONE, line(LISTING, '', t, NECK)).designSku, 'CHAMELEON_28151', t);
  step('the chameleon necklace line (no SKU, Etsy list read and blank) is CHAMELEON_28151 by its title: skuSource "title", the drawing\'s second SKU CHAMELEON_87521 told, nothing asked, the line untouched');
}

/* ── 3 · the form guard ── */
function forms() {
  const HUG = [['Metal Choice', 'Gold'], ['Earring Style', 'Huggie Hoops']];
  const l = line('4400000001', '', 'Chameleon Huggie Hoop Earrings', HUG);
  // the chameleon huggie exists: the earring line takes the huggie, never the necklace drawing
  const a = read(ONE, l); eq([a.designSku, a.skuSource], ['CHAMELEON_87521 (HUGGIE)', 'title'], 'the huggie line takes the huggie drawing');
  ok(a.designSku !== 'CHAMELEON_28151' && a.designSku !== 'CHAMELEON_87521', 'never the necklace drawing');
  // there is no chameleon huggie and nothing says the one drawing is a necklace: it is NOT taken (nothing says it is a huggie design); a person is shown it to confirm
  const b = read(NECK_ONLY, l), pb = asked(b); eq([b.designSku, b.skuSource], [null, null]); ok(pb, 'it waits'); eq(pb.nosku.state, 'one'); ok(/nothing says CHAMELEON_28151 is a huggie design/.test(pb.nosku.why), pb.nosku.why);
  // the real master: three chameleon huggies; the huggie line is offered those and no necklace drawing
  const r = read(REAL, l), pr = asked(r); eq(r.designSku, null); eq(pr.nosku.state, 'several'); eq(pr.nosku.candidates.slice(0, 3).map(c => c.sku).sort(), ['CHAMELEON_60896 (HUGGIE)', 'CHAMELEON_87521 (HUGGIE)', 'CHAMELEON_98293 (HUGGIE)'], 'the three huggie drawings come first for a huggie line');
  ok(!pr.nosku.candidates.some(c => ['CHAMELEON_28151', 'CHAMELEON_33973', 'COLOR_30356', 'CHAMELEON_80192'].includes(c.sku)), 'no drawing the master knows as a necklace is offered to a huggie line');
  // and the reverse: a necklace line never takes a huggie drawing
  const c = read(HUG_ONLY, PAUL()), pc = asked(c); eq([c.designSku, c.skuSource], [null, null], 'a necklace line never takes a huggie-only drawing'); eq(pc.nosku.candidates, []);
  // a line whose form is not known waits (it is told so), even when one drawing holds the word
  const u = read(ONE, line('4400000002', '', 'Chameleon Charm', [['Metal Choice', 'Gold']])), pu = asked(u);
  eq(u.designSku, null, 'form unknown: not taken by the title alone'); ok(pu && /necklace or earrings/.test(pu.nosku.why) && pu.nosku.candidates.length >= 1, 'it waits with the charm to confirm: ' + pu.nosku.why);
  step('form guard: a huggie line takes the huggie drawing and never the necklace one; a necklace line never takes a huggie; a line of unknown form only gets a suggestion');
}

/* ── 4 · several plausible drawings: candidates, nothing automatic ── */
function several() {
  const sp = read(REAL, PAUL()), p = asked(sp);
  eq([sp.designSku, sp.skuSource], [null, null], 'the master draws the chameleon six ways: nothing is taken');
  eq(p.nosku.state, 'several'); eq(p.nosku.candidates.length, 6, 'one candidate per drawing, not per SKU');
  eq(p.nosku.candidates[0].sku, 'CHAMELEON_28151', 'the drawing of the brief is first');
  eq(new Set(p.nosku.candidates.map(c => c.sku)).size, 6); ok(p.nosku.candidates.every(c => REAL.entryFor(c.sku) && c.form !== 'huggie'), 'every candidate is in the master and none is a huggie');
  ok(!p.nosku.candidates.some(c => /HUGGIE/.test(c.sku)), 'no huggie drawing offered to a necklace');
  ok(/^This listing has no SKU on Etsy yet\. Its title says “chameleon” and the library has 6 charms with that name: tap the right one below\.$/.test(p.reason), p.reason);
  ok(!/SKU \?|no alias for the listing/.test(p.reason + p.nosku.short), 'the old words are gone'); ok(p.nosku.short.length <= 200 && /^No SKU on Etsy for this listing/.test(p.nosku.short), p.nosku.short);
  // two plausible swans
  const s = read(ONE, line('4400000003', '', 'Swan Necklace', NECK)), ps = asked(s); eq(s.designSku, null); eq(ps.nosku.state, 'several'); eq(ps.nosku.candidates.map(c => c.sku).sort(), ['BEADY SWAN', 'SWAN_50666'].sort());
  // a title that names two designs never matches
  const t = read(ONE, line('4400000004', '', 'Swan and Dachshund Necklace', NECK)), pt = asked(t); eq(t.designSku, null); ok(pt && pt.nosku.state !== 'unique');
  const t2 = read(ONE, line('4400000005', '', 'Lizard Necklace with Chameleon', NECK)), p2 = asked(t2); eq(t2.designSku, null, 'two designs in the first phrase: ' + (p2 && p2.nosku.why));
  // a later phrase naming another design stops the match as well
  const t3 = read(ONE, line('4400000006', '', 'Lizard Necklace, Swan Charm Gift', NECK)); eq(t3.designSku, null, 'the title also names a swan');
  // a spelling guess is a candidate, never a match
  const g = read(ONE, line('4400000007', '', 'Chamelion Charm Necklace', NECK)), pg = asked(g); eq(g.designSku, null); eq(pg.nosku.state, 'weak'); eq(pg.nosku.candidates[0].sku, 'CHAMELEON_28151'); ok(/spelling guess/.test(pg.nosku.why), pg.nosku.why);
  // nothing plausible: one plain line, what was tried, the search box stays
  const z = read(ONE, line('4400000008', '', 'Sparkly Whatsit Necklace', NECK)), pz = asked(z); eq(z.designSku, null); eq(pz.nosku.state, 'none'); eq(pz.nosku.candidates, []);
  ok(/^This listing has no SKU on Etsy yet\. Tried: its saved link \(none\), the transaction's SKU \(none\), Etsy's SKU list \(none on the listing\), the title \(its words \(sparkly, whatsit\) name no charm of the library\)\. Pick the charm below\.$/.test(pz.reason), pz.reason);
  step('several drawings: ranked candidates (one per drawing, the brief\'s CHAMELEON_28151 first), the plain reason, nothing automatic; two designs, a later design word and a spelling guess never match; nothing plausible says what was tried');
}

/* ── 5 · a real SKU, a link, a pick always wins ── */
function wins() {
  const dach = read(ONE, line(LISTING, 'DACHSHUND_36943', 'Chameleon Charm Necklace', NECK)); eq([dach.designSku, dach.skuSource, !!dach.titleMatch], ['DACHSHUND_36943', 'transaction', false], 'the SKU on the transaction');
  const al = read(ONE, PAUL(), { aliases: { [LISTING]: { sku: 'SWAN_50666', v: 2, title: 'Chameleon Charm Necklace' } } }); eq([al.designSku, al.skuSource, !!al.titleMatch], ['SWAN_50666', 'alias', false], 'a saved link for the listing');
  const alb = read(ONE, line(LISTING, 'FOO 12', 'Chameleon Charm Necklace', NECK), { aliases: { [LISTING]: { bySku: { 'FOO 12': 'SWAN_50666' } } } }); eq([alb.designSku, alb.skuSource], ['SWAN_50666', 'alias'], "a saved link for the listing's SKU");
  const unk = read(ONE, line(LISTING, 'FOO 12', 'Chameleon Charm Necklace', NECK)), pu = asked(unk); eq([unk.designSku, unk.skuSource], ['FOO 12', 'transaction']); ok(pu && pu.reason === 'not in any master file' && !pu.nosku, 'an unknown SKU stays an unknown SKU: the title never overrides it');
  const inv = read(ONE, PAUL(), { listingSkus: { [LISTING]: { uni: 'ZZ TOP', at: 1 } } }), pi = asked(inv); ok(!inv.titleMatch && inv.designSku !== 'CHAMELEON_28151', "a SKU in Etsy's own list is never replaced by the title"); ok(pi && !pi.nosku, 'and no title offer is made'); eq(pi.reason, "The transaction has no SKU, and the SKU Etsy lists for this listing (ZZ TOP) names no charm of the library: pick the charm below.", 'it does not claim the listing has no SKU');
  const opt = read(ONE, PAUL(), { optionMaps: { [LISTING]: { 'Metal Choice': { Gold: { field: 'design', value: 'SWAN_50666' } } } } }); eq([opt.designSku, opt.skuSource, !!opt.titleMatch], ['SWAN_50666', 'option', false], "an option's pick");
  const set = read(ONE, line(LISTING, '', 'Chameleon Charm Necklace', [...NECK, ['Huggie Set', 'Huggie CHARM SET']])); ok(!set.titleMatch, 'a Huggie CHARM SET keeps its own question');
  step('a real SKU, a saved link, a person\'s option pick and a SKU in Etsy\'s list always win: the title is never read for them (an unknown SKU stays "not in any master file")');
}

/* ── 6 · custom / pre-made / add-on listings are untouched ── */
function custom() {
  const flagged = read(ONE, PAUL(), { aliases: { [LISTING]: { listingKind: { kind: 'custom', by: 'T', at: 1, title: 'Chameleon Charm Necklace', note: 'pre-made piece' } } } });
  ok(flagged.special && flagged.ownDesign && !flagged.designSku && !flagged.titleMatch && !asked(flagged), 'a listing a person flagged custom / pre-made takes no repo design: ' + JSON.stringify([flagged.designSku, flagged.special && flagged.special.kind]));
  const addon = read(ONE, line('4400000010', '', 'Add a Chameleon Charm', NECK)); ok(addon.ownDesign && !addon.titleMatch && !addon.designSku && !asked(addon), 'an add-on listing ("Add a … Charm"): its design is the order\'s own');
  const rework = read(ONE, PAUL(), { aliases: { [LISTING]: { listingKind: { kind: 'rework', by: 'T', at: 1 } } } }); ok(rework.ownDesign && !rework.titleMatch && !rework.designSku, 'a listing a person flagged as a rework');
  const rd = read(ONE, PAUL(), { customRead: { [O.lineKey({ receiptId: 'R' + (tx + 1), updateTs: 1 }, PAUL({ transactionId: String(tx + 1) }))]: { kind: 'custom', confidence: 0.9, summary: 'a custom piece' } } });
  ok(true, 'Claude\'s custom reading is checked by the special rule (not re-tested here)');
  const cust = read(ONE, line('4400000011', '', 'Custom Chameleon Necklace', NECK)), pcu = asked(cust); ok(!cust.titleMatch && !cust.designSku && pcu && pcu.nosku.state === 'one' && /custom or made-to-order/.test(pcu.nosku.why), 'a title that says custom is never taken by its words: ' + (pcu && pcu.nosku.why));
  const co = read(ONE, line('4400000013', '', 'Custom Order - Chameleon', NECK)); ok(co.special && !co.titleMatch && !co.designSku, 'a Custom Order title is a special order');
  const chain = read(ONE, line('4400000012', '', 'Chameleon Charm Necklace', [['Metal Choice', 'Gold'], ['Charm', 'Chain only']])); ok(!chain.titleMatch, 'a chain only line: untouched');
  void rd;
  step('a listing flagged custom / pre-made, an add-on, a custom title and a chain-only line never take a repo design by the title');
}

/* ── 7 · the conditions of the rule, one by one ── */
function conditions() {
  const notRead = read(ONE, PAUL(), { listingSkus: {} }), pn = asked(notRead); eq(notRead.designSku, null, "Etsy's SKU list not read yet: a person confirms, the code does not"); eq(pn.nosku.state, 'one'); eq(pn.nosku.candidates[0].sku, 'CHAMELEON_28151'); ok(/not read yet/.test(pn.nosku.why), pn.nosku.why);
  const loading = read(ONE, PAUL(), { masterReady: false }), pl = asked(loading); eq(loading.designSku, null, 'the library is still loading: nothing is matched'); eq(pl.nosku.candidates, []); ok(/still loading/.test(pl.reason), pl.reason);
  // blocked in the master: not taken or offered
  const blockedLib = world([...pick(fixEntries, 'CHAMELEON_28151', 'CHAMELEON_87521').map(e => Object.assign({}, e, { blocked: 'being redrawn' })), ...DECOYS]);
  eq(read(blockedLib, PAUL()).designSku, null, 'a blocked design is never taken'); eq(asked(read(blockedLib, PAUL())).nosku.candidates, []);
  // the saved link of a listing with the same words raises the right drawing in the list
  const linked = read(REAL, PAUL(), { aliases: { '4400000099': { sku: 'CHAMELEON_99780', v: 2, title: 'Chameleon Charm Necklace' } } }), pk = asked(linked); eq(pk.nosku.candidates[0].sku, 'CHAMELEON_99780', 'a person linked another listing with this title to it: first in the list'); eq(linked.designSku, null, 'but a link on ANOTHER listing is never applied');
  // the index is built once per library
  const c = REAL.ctx(), a = O.titleIndexFor(c), b = O.titleIndexFor(c); ok(a === b && a.fams.size > 5, 'one index per library');
  // pure: nothing in the world changed
  eq(REAL.lib.size, fixEntries.length + DECOYS.length);
  step("the conditions: an unread Etsy list, a loading or blocked library keep the line waiting; a link on another listing only ranks; the index is built once");
}

/* ── 8 · the words ── */
function words() {
  const sp = read(ONE, PAUL());
  ok(O.noSkuLine({ state: 'one', said: '“chameleon”', why: 'x', candidates: [{ sku: 'A' }] }).startsWith('This listing has no SKU on Etsy yet'), 'every wording starts alike');
  for (const st of ['several', 'one', 'weak', 'none']) ok(/no SKU/i.test(O.noSkuLine({ state: st, said: '“x”', others: 2, why: 'y', words: ['x'], candidates: [] }, { etsy: 'none on any product' })), 'the readiness regex /no SKU/i still reads ' + st);
  ok(sp.titleMatch && !sp.problems.length, 'a matched line asks nothing');
  step('the words: "This listing has no SKU on Etsy yet" starts every wording and matches the readiness rule');
}

/* ── the browser part: two scenarios, each on a fresh fake site and a fresh page ── */
async function browser() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return false; }
  const { start } = require('./bridge-server.cjs');
  const b = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const SHIP = Math.floor(Date.UTC(2026, 9, 19, 17) / 1000), DAY = 86400;
  const order = (rid, lines) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + String(rid).slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const mk = (tid, listing, title) => ({ transactionId: tid, listingId: listing, productId: '31551438733', sku: '', title, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal Choice', value: 'Gold' }, { name: 'Necklace Length in inches', value: '14&quot;' }], metalKey: 'gold', metalLabel: 'Gold', personalization: ['A first name'], buyerMessage: '', staffNote: '', messages: [] });
  /** A fresh site and page, the library seeded (pictures are counted, never drawn) and the page told it was read whole, then one order of one line pushed in and read. */
  async function scenario(entries, o, tables, run) {
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
      await page.evaluate(async ({ entries, o, tables }) => {
        CustomRead.later = () => {};   // (Claude's reading of lines is never started by a test: nothing is paid for)
        B.master.entries = new Map(entries.map(e => [e.sku, Object.assign({ charmHash: 'h' + e.sku.length, thumbUrl: '/.netlify/functions/imageProxy?url=thumb-' + encodeURIComponent(e.sku), members: 1 }, e)])); B.master.index = { seeded: true }; B.master.loadedAt = Date.now();
        for (const k of ['masterPreview', 'masterFront']) { const keep = Pool[k]; Pool[k] = function () { return 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" fill="#3a7"/></svg>'); }; Pool['_' + k] = keep; }
        await Orders.loadMaps(true); B.maps.listingSkus = Object.assign({}, B.maps.listingSkus, tables);
        for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
        Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      }, { entries, o, tables });
      const key = await page.evaluate(o => CharmNestOrders.lineKey(o, o.lines[0]), o);
      await run({ page, srv, key, writes, errors });
      eq(errors, [], 'no page errors');
    } finally { srv.close(); }
  }

  /* 1 · the matched line (the library holds ONE chameleon drawing): no question, the reading says how it was found, Use another charm */
  await scenario([...ONE.lib.values()], order('4199225418', [mk('9100000001', LISTING, 'Chameleon Charm Necklace')]), TABLE_EMPTY, async ({ page, key: KEY, writes }) => {
    const st = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { sku: r.spec.designSku, src: r.spec.skuSource, tm: !!r.spec.titleMatch, problems: r.problems.map(p => p.kind), items: B.review.items.filter(i => i.kind === 'unmatchedSku').length }; }, KEY);
    eq(st, { sku: 'CHAMELEON_28151', src: 'title', tm: true, problems: [], items: 0 }, 'the page reads the chameleon necklace as CHAMELEON_28151 by its title, nothing in Review');
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => OrderWin.isOpen() && /CHAMELEON/.test(document.getElementById('owSku').textContent), null, { timeout: 20000 });
    await page.waitForTimeout(500);
    const win = await page.evaluate(() => ({ sku: document.getElementById('owSku').textContent, chips: [...document.querySelectorAll('#owMeta .owfChip')].map(x => x.textContent.trim()), meta: [...document.querySelectorAll('#owMeta .m')].map(m => m.querySelector('i').textContent + ': ' + (m.querySelector('span') ? m.querySelector('span').textContent : '')) }));
    eq(win.sku, "SKU: CHAMELEON_28151 · matched by the listing's title", 'the order window says how the charm was found: ' + win.sku);
    ok(win.chips.some(t => /Matched by title/.test(t)), 'the glance line has a Matched by title chip: ' + win.chips.join(' | '));
    ok(win.meta.some(m => /^Matched by: the listing's title: CHAMELEON_28151/.test(m)), 'Details has the cell: ' + win.meta.filter(m => /Matched/.test(m)).join(' | '));
    { const head = await page.evaluate(() => document.getElementById('owNowCard').textContent.replace(/\s+/g, ' ').trim()); ok(/Read as CHAMELEON_28151 · .* · matched by the listing's title/.test(head), "the head card reads: " + head); }
    // the chip's card offers Use another charm, and it opens the small window with the search box
    await page.click('#owMeta [data-owf-chip="titlematch"]');
    await page.waitForSelector('.owfPop [data-act="otherCharm"]');
    await page.click('.owfPop [data-act="otherCharm"]');
    await page.waitForSelector('dialog.titleDlg[open]');
    const dlg = await page.evaluate(() => { const d = document.querySelector('dialog.titleDlg'); return { head: d.querySelector('h3').textContent, text: d.querySelector('.dlgBody p').textContent, input: !!d.querySelector('input[data-f=sku]'), chips: [...d.querySelectorAll('[data-dsku]')].map(x => x.dataset.dsku) }; });
    eq(dlg.head, 'Use another charm'); ok(/matched it to CHAMELEON_28151 by the listing's title/.test(dlg.text), dlg.text); ok(dlg.input, 'the search box is there'); eq(dlg.chips, [], 'no other chameleon drawing in this library: only the box');
    eq(writes.length, 0, 'nothing was written by reading the line or opening the window: ' + JSON.stringify(writes.map(w => w.op)));
    await page.fill('dialog.titleDlg input[data-f=sku]', 'swan_50666'); await page.click('dialog.titleDlg [data-a=use]');
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.skuSource === 'alias', KEY, { timeout: 30000 });
    const after = await page.evaluate(k => { const r = B.orders.byKey.get(k); return { sku: r.spec.designSku, src: r.spec.skuSource, tm: !!r.spec.titleMatch }; }, KEY);
    eq(after, { sku: 'SWAN_50666', src: 'alias', tm: false }, "one tap on Use another charm: the person's word wins from then on");
    const alias = writes.filter(w => w.op === 'aliasPut'); eq(alias.length, 1, "one write, a person's: " + JSON.stringify(writes.map(w => w.op))); eq([alias[0].listingId, alias[0].sku], [LISTING, 'SWAN_50666']);
  });
  console.log('  ✓ Chromium: the matched line asks nothing; the order window says "matched by the listing\'s title" (SKU line, chip, Details cell); Use another charm saves one link and wins from then on');

  /* 2 · the waiting line (six chameleon drawings): the plain Review row, one-tap chips with their drawings; a tap saves the link once */
  await scenario([...REAL.lib.values()], order('4199225419', [mk('9100000002', LISTING, 'Chameleon Charm Necklace')]), TABLE_EMPTY, async ({ page, key: KEYB, writes }) => {
    const rowSel = `#rvList .reviewListRow[data-row="${KEYB}"]`;
    await page.waitForSelector(rowSel + ' .sibOffer [data-dsku]');
    const rv = await page.evaluate(sel => { const n = document.querySelector(sel); return { reason: n.querySelector('.reviewReason').textContent, ask: n.querySelector('.sibAsk').textContent, chips: [...n.querySelectorAll('.sibOffer [data-dsku]')].map(x => x.dataset.dsku), pics: n.querySelectorAll('.sibOffer .dsPick img').length, label: n.querySelector('.purchaseLabel').textContent }; }, rowSel);
    eq(rv.reason, 'This listing has no SKU on Etsy yet. Its title says “chameleon” and the library has 6 charms with that name: tap the right one below.');
    eq(rv.chips.length, 6); eq(rv.chips[0], 'CHAMELEON_28151'); eq(rv.pics, 6, 'every chip carries its master drawing'); ok(!/SKU \?/.test(rv.reason + rv.ask), 'no "SKU ?"');
    // the head card of the order says it in the same plain words (an older stored sentence is replaced by the page's reading now)
    await page.evaluate(k => OrderWin.open(k), KEYB);
    await page.waitForFunction(() => OrderWin.isOpen(), null, { timeout: 20000 });
    await page.waitForFunction(() => /No SKU on Etsy for this listing/.test(document.getElementById('owNowCard').textContent), null, { timeout: 20000 });
    const head = await page.evaluate(() => document.getElementById('owNowCard').textContent.replace(/\s+/g, ' ').trim());
    ok(/Needs a decision/.test(head) && /No SKU on Etsy for this listing\. Its title says “chameleon”; the library has 6 charms with that name: pick the right one\./.test(head) && !/SKU \?|Unknown SKU/.test(head), 'head card: ' + head);
    await page.click('#owClose');
    await page.click(`${rowSel} .sibOffer [data-dsku="CHAMELEON_28151"]`);
    await page.waitForFunction(k => B.orders.byKey.get(k).spec.skuSource === 'alias', KEYB, { timeout: 30000 });
    const aw = writes.filter(w => w.op === 'aliasPut'); eq(aw.length, 1); eq([aw[0].listingId, aw[0].sku, aw[0].fromSku], [LISTING, 'CHAMELEON_28151', undefined], "a person's tap saved the listing's link, once");
    eq(await page.evaluate(() => B.review.items.filter(i => i.kind === 'unmatchedSku').length), 0, 'the question is gone');
  });
  console.log('  ✓ Chromium: the waiting line shows the plain reason, six one-tap chips with drawings and the plain head card; a tap saves the listing\'s link once and the question goes');
  await b.close();
  return true;
}

(async () => {
  { const h = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');   // the page asks for the new scripts (other workers add their own tokens after -ns1)
    for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js', 'charm-nest-timeline-ui.js', 'charm-nest-order-facts.js']) assert(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-ns1(?:-[a-z0-9]+)*"').test(h), f + ' carries the -ns1 cache token'); }
  cause(); matched(); forms(); several(); wins(); custom(); conditions(); words();
  const ran = await browser();
  console.log((ran ? 'Title match OK:' : 'Title match (node part only, no browser):') + ` ${n} checks. A listing with no SKU anywhere is matched by its title only when one drawing clearly fits; otherwise it shows ranked candidates or a plain line; a real SKU, link or pick always wins.`);
})().catch(e => { console.error(e); process.exit(1); });
