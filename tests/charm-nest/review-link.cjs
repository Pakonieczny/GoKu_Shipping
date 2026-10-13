// REVLINK (Paul, 12 Oct 2026): "once the user chooses a given vector file to match to a given listing then that connection has to be automatically made forever and that has to be recorded so next
// time a buyer purchases the same listing the system automatically knows which vector to pull ... even in sandbox mode ... if I make choices those choices should be saved moving forward whether
// I'm doing it in sandbox or in live mode."
// What this proves, offline, on the REAL Library function (an in-memory Firestore that refuses nested arrays), the REAL matcher (charm-nest-orders.js interpretLine) and the REAL planner
// (charm-nest-review-link.js), in BOTH modes (production, and the sandbox: every call tagged sandbox:true):
//   1 · a pick on a line matched BY ITSELF to another charm (the exact SKU, the "saved link beats exact match" case): one write, read back (verified), a NEW order of the listing (new numbers) reads as the pick, no question
//   2 · one link per VARIATION: a listing whose variations each have their own SKU keeps a link for each; picking one leaves the other alone
//   3 · a line whose charm comes from an OPTION (a charm an option picks, a letter, a zodiac sign): the pick is kept on the option value, because the matcher reads the option BEFORE the SKU; a link under the SKU
//       alone would never be read (shown)
//   4 · a line with no SKU: the listing's own link; a line whose pick cannot be kept (two designs; a sign read from the buyer's note) says so and writes nothing
//   5 · the variation's SKU Etsy keeps in its inventory table: the link is under THAT SKU, and the proof says what a page without that table would read (the one case a link depends on the table)
//   6 · the sandbox: the pick is in the SHARED store (never Sandbox_…), survives Reset and Purge, and production reads it; a charm the library does not hold is refused, nothing written
//   7 · nothing is saved that was not read back: a write the cloud does not confirm throws (nothing counted as saved)
//   node tests/charm-nest/review-link.cjs        (no network, no Etsy, no AI)
'use strict';
const assert = require('assert'), path = require('path');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const O = require(path.join(root, 'charm-nest-orders.js'));
const RL = require(path.join(root, 'charm-nest-review-link.js'));
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
const repoStamp = require('./_repoToken.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), JSON.parse(JSON.stringify(b)), m); n++; };
const pass = name => console.log('  ✓', name);

// the library: neutral designs, each its own drawing (a design file of its own, so no two are "one drawing")
const SKUS = ['DACHSHUND', 'DACHSHUND_36943', 'DACHSHUND_88528', 'ANIMAL_ONE', 'ANIMAL_TWO', 'PICK_ONE', 'PICK_TWO', 'UMBRELLA', 'STAR_PISCES', 'STAR_ARIES', 'STAR_PISCES_B', 'ZSIGN_PISCES', 'ZSIGN_PISCES_B', 'ZSIGN_ARIES', 'LETTER_A', 'LETTER_B', 'LETTER_A_B', 'SOLO_CHARM', 'SOLO_CHARM_B', 'PAIR_ONE', 'PAIR_TWO'];
const entry = (sku, i) => ({ sku, aiPath: `charmnest/master/${sku}.ai`, widthPt: 40 + i, heightPt: 50, members: 1, sizes: null, blocked: null });
const lib0 = new Map(SKUS.map((s, i) => [s, entry(s, i)]));
for (const [k, e] of lib0) Fk.store.set('Charm_Master_Index/' + k, JSON.parse(JSON.stringify(e)));
const loose = {}; for (const k of lib0.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const byFile = new Map(); for (const [k, e] of lib0) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const entryFor = s => lib0.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e ? byFile.get(e.aiPath) || [] : []; };

const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(await repoStamp('charmNestLibrary', body)) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const must = async body => { const r = await post(body); assert.strictEqual(r.status, 200, JSON.stringify(body).slice(0, 140) + ' → ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };

/** One page of the sorter in a mode: its maps are what the Library answers (aliasGet / optionMapGet, as loadMaps reads them); `tables` is the page's listing-SKU tables. */
function page(sb, tables) {
  const P = { maps: { optionMaps: {}, aliases: {} }, tables: tables || {}, calls: [] };
  const tag = b => Object.assign({}, b, sb ? { sandbox: true } : {});
  P.api = async (fn, body) => { P.calls.push(body.op); return must(tag(body)); };
  P.load = async () => { P.maps.aliases = (await must(tag({ op: 'aliasGet' }))).aliases; P.maps.optionMaps = (await must(tag({ op: 'optionMapGet' }))).maps; };
  P.ctx = () => ({ optionMaps: P.maps.optionMaps, aliases: P.maps.aliases, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: P.tables, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor });
  P.Orders = { loadMaps: P.load, ctx: P.ctx };
  P.read = line => { const order = { receiptId: 'R' + line.transactionId, updateTs: 1, createTs: 1 }; return { line, order, spec: O.interpretLine(order, JSON.parse(JSON.stringify(line)), P.ctx()) }; };
  P.save = (row, sku) => RL.save(row, sku, { by: 'Name One', api: P.api, orders: P.Orders });
  return P;
}
let tx = 8100000000, lidN = 1810000000;
const V = (name, value, pid, vid) => Object.assign({ name, value }, pid ? { propertyId: pid, valueId: vid } : {});
const mk = (listing, sku, variations, extra) => Object.assign({ transactionId: String(++tx), listingId: String(listing), sku, title: 'A neutral charm listing', quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '', variations: variations || [V('Metal Choice', 'Gold Filled')] }, extra || {});
const newListing = () => String(++lidN);
const designOf = sp => [sp.designSku, sp.skuSource];

async function mode(sb) {
  const W = sb ? 'the sandbox' : 'production';
  const L = lid => ({ Alias: () => Fk.store.get('Charm_Sku_Aliases/' + lid), Own: () => Fk.store.get('Sandbox_Charm_Sku_Aliases/' + lid), Opt: () => Fk.store.get('Charm_Option_Map/' + lid), OwnOpt: () => Fk.store.get('Sandbox_Charm_Option_Map/' + lid) });

  /* ── 1 · a pick on a line the matcher matched by itself ── */
  { const P = page(sb), listing = newListing(), l = L(listing);
    const first = P.read(mk(listing, 'DACHSHUND')); eq(designOf(first.spec), ['DACHSHUND', 'transaction'], W + ': before the pick the exact SKU is the charm');
    const plan = RL.planOf(first, 'DACHSHUND_36943'); eq([plan.ok, plan.via, plan.body.fromSku, plan.body.listingId], [true, 'alias', 'DACHSHUND', listing], W + ': the link is kept for the listing and the SKU the line came with');
    Fk.writes.length = 0;
    const r = await P.save(first, 'DACHSHUND_36943');
    ok(r.ok && r.verified === true && r.via === 'alias' && r.readBack.kind === 'bySku' && r.readBack.sku === 'DACHSHUND_36943', W + ': one write, read back by the cloud: ' + JSON.stringify(r));
    eq(Fk.writes.filter(w => /Aliases|Option_Map/.test(w.path)).map(w => w.kind + ' ' + w.path), ['set Charm_Sku_Aliases/' + listing], W + ': exactly ONE small write, in the shared store');
    ok(!l.Own() && !l.OwnOpt(), W + ': nothing in a sandbox copy');
    eq(l.Alias().bySku, { DACHSHUND: 'DACHSHUND_36943' }, W + ': stored under the SKU');
    ok(r.proof.ok && r.proof.designSku === 'DACHSHUND_36943' && r.proof.skuSource === 'alias', W + ': a new order of the listing reads as the pick: ' + JSON.stringify(r.proof));
    // THE NEXT ORDER: a different page (a reload, another computer), new numbers, nobody asked
    const other = page(sb); await other.load();
    const next = other.read(mk(listing, 'DACHSHUND'));
    eq(designOf(next.spec), ['DACHSHUND_36943', 'alias'], W + ': the next order of the listing is matched by the saved link on a fresh page');
    eq(next.spec.problems.map(p => p.kind), [], W + ': with no question, so nothing reaches Review');
    eq(designOf(other.read(mk(newListing(), 'DACHSHUND')).spec), ['DACHSHUND', 'transaction'], W + ': another listing with the same SKU is untouched');
    ok(RL.linkOf(next, other.maps).sku === 'DACHSHUND_36943', W + ': linkOf names the saved link');
    // picking again overwrites the same key (the latest pick stands), still one document
    const again = await P.save(first, 'DACHSHUND_88528'); ok(again.proof.ok, W + ': a later pick for the same SKU replaces the earlier');
    eq(l.Alias().bySku, { DACHSHUND: 'DACHSHUND_88528' }, W + ': one key, the latest pick');
    // the pick that IS the SKU (the person confirms the match): kept, and silent
    const same = await P.save(P.read(mk(listing, 'DACHSHUND_36943')), 'DACHSHUND_36943'); ok(same.ok && same.proof.ok, W + ': picking the charm already matched is kept too');
    pass(W + ' · 1 · a pick on a matched line: one write, read back, the next order matched by it, no Review'); }

  /* ── 2 · one link per variation ── */
  { const P = page(sb), listing = newListing(), l = L(listing);
    const v1 = P.read(mk(listing, 'ANIMAL_ONE', [V('Charm', 'One')])), v2 = P.read(mk(listing, 'ANIMAL_TWO', [V('Charm', 'Two')]));
    await P.save(v1, 'PICK_ONE'); eq(l.Alias().bySku, { ANIMAL_ONE: 'PICK_ONE' }, W + ': the first variation has its link');
    eq(designOf(P.read(mk(listing, 'ANIMAL_TWO', [V('Charm', 'Two')])).spec), ['ANIMAL_TWO', 'transaction'], W + ': the other variation is untouched by it');
    await P.save(v2, 'PICK_TWO'); eq(l.Alias().bySku, { ANIMAL_ONE: 'PICK_ONE', ANIMAL_TWO: 'PICK_TWO' }, W + ': each variation keeps its own link');
    eq([designOf(P.read(mk(listing, 'ANIMAL_ONE', [V('Charm', 'One')])).spec), designOf(P.read(mk(listing, 'ANIMAL_TWO', [V('Charm', 'Two')])).spec)], [['PICK_ONE', 'alias'], ['PICK_TWO', 'alias']], W + ': both read as their own pick');
    pass(W + ' · 2 · one link per variation'); }

  /* ── 3 · the charm comes from an option ── */
  { const P = page(sb), listing = newListing(), l = L(listing);
    // a person once said "Zodiac Sign: Pisces" is ZSIGN_PISCES on this listing; the line's SKU is the one the whole listing shares (UMBRELLA)
    await P.api('charmNestLibrary', { op: 'optionMapPut', listingId: listing, optionName: 'Zodiac Sign', optionValue: 'Pisces', map: { field: 'design', value: 'ZSIGN_PISCES' }, by: 'Name One' }); await P.load();
    const line = mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')]);
    const row = P.read(line); eq(designOf(row.spec), ['ZSIGN_PISCES', 'option'], W + ': the option decides the charm');
    const plan = RL.planOf(row, 'ZSIGN_PISCES_B');
    eq([plan.ok, plan.via, plan.body.op, plan.body.optionName, plan.body.optionValue], [true, 'option', 'optionMapPut', 'Zodiac Sign', 'Pisces'], W + ': the pick is kept on the option value, not under the SKU');
    // a link under the SKU alone would NEVER be read: the option is read before it
    await P.api('charmNestLibrary', { op: 'aliasPut', listingId: listing, sku: 'ZSIGN_PISCES_B', fromSku: 'UMBRELLA', by: 'Name One' }); await P.load();
    eq(designOf(P.read(mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')])).spec), ['ZSIGN_PISCES', 'option'], W + ': (shown) a link under the SKU is hidden by the option answer');
    Fk.store.set('Charm_Sku_Aliases/' + listing, undefined); Fk.store.delete('Charm_Sku_Aliases/' + listing); Fk.store.delete('Sandbox_Charm_Sku_Aliases/' + listing); await P.load();
    const r = await P.save(row, 'ZSIGN_PISCES_B');
    ok(r.ok && r.via === 'option' && r.verified && r.proof.ok && r.proof.skuSource === 'option', W + ': saved on the option and the new order reads it: ' + JSON.stringify(r));
    eq(l.Opt().map['zodiac sign'].pisces, Object.assign({ field: 'design', value: 'ZSIGN_PISCES_B', by: 'Name One' }, { at: l.Opt().map['zodiac sign'].pisces.at }), W + ': in the shared option map');
    ok(!l.OwnOpt() && !l.Own(), W + ': no sandbox copy');
    const fresh = page(sb); await fresh.load();
    eq(designOf(fresh.read(mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')])).spec), ['ZSIGN_PISCES_B', 'option'], W + ': the next order of the listing, on a fresh page');
    eq(designOf(fresh.read(mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Aries'), V('Metal Choice', 'Gold Filled')])).spec)[1] === 'option', false, W + ': another sign of the listing is not touched by it');
    // a zodiac sign read by the matcher's own rule from an option (the shared zodiac SKU) is kept on that option value too
    pass(W + ' · 3 · an option-driven line: the pick is kept on the option value'); }

  /* ── 3b · a letter option ── */
  { const P = page(sb), listing = newListing();
    await P.api('charmNestLibrary', { op: 'optionMapPut', listingId: listing, optionName: 'Letter', optionValue: 'A', map: { field: 'design', value: 'LETTER_A' }, by: 'Name One' }); await P.load();
    const row = P.read(mk(listing, 'LETTER_B', [V('Letter', 'A'), V('Metal Choice', 'Gold Filled')]));
    eq(designOf(row.spec), ['LETTER_A', 'option'], W + ': the letter option decides');
    const r = await P.save(row, 'LETTER_A_B'); ok(r.ok && r.via === 'option' && r.proof.ok, W + ': ' + JSON.stringify(r.proof));
    pass(W + ' · 3b · a letter option'); }

  /* ── 4 · no SKU on the line; a pick that cannot be kept ── */
  { const P = page(sb), listing = newListing(), l = L(listing);
    const row = P.read(mk(listing, ''));
    const plan = RL.planOf(row, 'SOLO_CHARM'); eq([plan.ok, plan.via, plan.body.fromSku, plan.body.huggie], [true, 'alias', undefined, undefined], W + ': a line with no SKU is answered for the listing');
    const r = await P.save(row, 'SOLO_CHARM'); ok(r.ok && r.proof.ok && l.Alias().sku === 'SOLO_CHARM' && l.Alias().v === 2, W + ': the listing\'s own link: ' + JSON.stringify(l.Alias()));
    { const fresh = page(sb); await fresh.load(); eq(designOf(fresh.read(mk(listing, '')).spec), ['SOLO_CHARM', 'alias'], W + ': the next no-SKU order of the listing, on a fresh page, reads it'); }
    // a pair of two designs and a line with no listing number cannot be kept as one charm
    const none = RL.planOf({ line: mk('', 'X'), spec: {} }, 'SOLO_CHARM'); ok(none.ok === false && /listing number/.test(none.why), W + ': no listing number: ' + none.why);
    const pr = RL.planOf({ line: mk(listing, 'X'), spec: { skuSource: 'pair' } }, 'SOLO_CHARM'); ok(pr.ok === false && /two designs/.test(pr.why), W + ': a pair: ' + pr.why);
    const zn = RL.planOf({ line: mk(listing, 'X'), spec: { skuSource: 'zodiac', designSku: 'STAR_PISCES', options: [] } }, 'STAR_ARIES'); ok(zn.ok === false && /buyer's own words/.test(zn.why), W + ': a sign from the buyer\'s note: ' + zn.why);
    Fk.writes.length = 0; const w = await RL.save({ line: mk(listing, 'X'), spec: { skuSource: 'pair' } }, 'SOLO_CHARM', { by: 'Name One', api: P.api, orders: P.Orders }); ok(w.ok === false && !Fk.writes.length, W + ': a pick that cannot be kept writes nothing');
    pass(W + ' · 4 · no SKU: the listing\'s own link; what cannot be kept says so and writes nothing'); }

  /* ── 5 · the variation's SKU from Etsy's inventory table ── */
  { const listing = newListing(), l = L(listing);
    // the transaction carries the listing's SKU (UMBRELLA, held by three products); Etsy's table gives the Pisces product its own SKU
    const table = { at: Date.now(), products: [{ id: '501', sku: 'STAR_PISCES', d: 0, pv: [['2001', '3001']] }, { id: '502', sku: 'STAR_ARIES', d: 0, pv: [['2001', '3002']] }, { id: '503', sku: 'UMBRELLA', d: 0, pv: [['2001', '3003']] }, { id: '504', sku: 'UMBRELLA', d: 0, pv: [['2001', '3004']] }] };
    const P = page(sb, { [listing]: table });
    const line = () => mk(listing, 'UMBRELLA', [V('Sign', 'Pisces', '2001', '3001'), V('Metal Choice', 'Gold Filled')], { productId: '501' });
    const row = P.read(line()); eq(designOf(row.spec), ['STAR_PISCES', 'inventory'], W + ': the table gives the product its own SKU');
    const plan = RL.planOf(row, 'STAR_PISCES_B'); eq([plan.via, plan.body.fromSku], ['alias', 'STAR_PISCES'], W + ': the link is under the SKU the line was matched with (the variation\'s)');
    const r = await P.save(row, 'STAR_PISCES_B'); ok(r.ok && r.proof.ok && r.proof.skuSource === 'alias', W + ': the next order reads it: ' + JSON.stringify(r.proof));
    eq(l.Alias().bySku, { STAR_PISCES: 'STAR_PISCES_B' }, W + ': one link for this product, none for the others');
    eq(designOf(P.read(mk(listing, 'UMBRELLA', [V('Sign', 'Aries', '2001', '3002'), V('Metal Choice', 'Gold Filled')], { productId: '502' })).spec), ['STAR_ARIES', 'inventory'], W + ': the other products of the listing are untouched');
    // 5b · the table is NOT needed to find it again: the pick also names WHICH PRODUCT it was for (byVia), so a page that never asked Etsy for the table (or holds an older copy) reads a new order of it by the link
    ok(r.proof.withoutTable && r.proof.withoutTable.ok === true && r.proof.withoutTable.skuSource === 'alias', W + ': the proof reads a new order WITHOUT Etsy\'s table too: ' + JSON.stringify(r.proof.withoutTable));
    eq(l.Alias().byVia, { P501: 'STAR_PISCES', 'O2001:3001': 'STAR_PISCES' }, W + ': the link names the product (product id, option ids), kept beside bySku, in the one write');
    ok(r.readBack && r.readBack.key === 'STAR_PISCES', W + ': read back by the cloud (verified)');
    { const none = page(sb); await none.load();   // a fresh computer: the alias documents only, NO table
      const next = none.read(mk(listing, 'UMBRELLA', [V('Sign', 'Pisces', '2001', '3001'), V('Metal Choice', 'Gold Filled')], { productId: '501' }));
      eq(designOf(next.spec), ['STAR_PISCES_B', 'alias'], W + ': a NEW order of that product, no table on the page: matched by the saved link');
      eq(next.spec.problems.map(p => p.kind), [], W + ': no question, so no Review and no Etsy table is asked for');
      eq([next.spec.viaInventory.by, next.spec.viaInventory.sku, next.spec.boughtSku], ['link', 'STAR_PISCES', 'STAR_PISCES'], W + ': read under the SKU the link is kept under');
      eq(designOf(none.read(mk(listing, 'UMBRELLA', [V('Sign', 'Pisces', '2001', '3001')], {})).spec), ['STAR_PISCES_B', 'alias'], W + ': a transaction with no product id is found by its option ids');
      // never another product's link: the Aries product, the same shared SKU, on the same page, is NOT read as Pisces' pick
      const other = none.read(mk(listing, 'UMBRELLA', [V('Sign', 'Aries', '2001', '3002'), V('Metal Choice', 'Gold Filled')], { productId: '502' }));
      ok(other.spec.designSku !== 'STAR_PISCES_B' && other.spec.viaInventory === undefined, W + ': another product of the listing is not read by this link: ' + JSON.stringify(designOf(other.spec)));
      const noIds = none.read(mk(listing, 'UMBRELLA', [V('Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')], {}));
      ok(noIds.spec.designSku !== 'STAR_PISCES_B', W + ': a line that names no product by id is not guessed from the options\' words');
      ok(!none.calls.some(c => /listingSkus/.test(c)), W + ': no table was asked for'); }
    pass(W + ' · 5b · the variation\'s link is found again with no Etsy table on the page (by its product), and never for another product'); }

  /* ── 5c · a listing of ONE product: when exactly one link exists and nothing can tell it from another, it is its link; never otherwise ── */
  { const listing = newListing(), l = L(listing), P = page(sb);
    const first = P.read(mk(listing, 'ODD_SKU_ONE', [V('Personalization', 'Name One')], {}));
    ok(first.spec.problems.some(p => p.kind === 'unmatchedSku'), W + ': the first order of the listing asks (its SKU names no design)');
    const r = await P.save(first, 'ANIMAL_ONE'); ok(r.ok && !l.Alias().byVia, W + ': a plain line records no product keys (it was read by its own SKU)');
    const fresh = page(sb); await fresh.load();
    eq(designOf(fresh.read(mk(listing, 'ODD_SKU_ONE', [V('Personalization', 'Name One')], {})).spec), ['ANIMAL_ONE', 'alias'], W + ': the same SKU again: the link');
    eq(designOf(fresh.read(mk(listing, '', [], {})).spec), ['ANIMAL_ONE', 'alias'], W + ': the shop took the SKU off the listing (no SKU, no option, no product to tell apart): one link, one product: its link');
    // a SKU of its own is a variation of its own ("Use this charm" is the variation's): never given the other's link
    ok(fresh.read(mk(listing, 'ODD_SKU_TWO', [V('Personalization', 'Name One')], {})).spec.designSku !== 'ANIMAL_ONE', W + ': another SKU of the listing is asked about, not given it');
    // a line that names an option the link cannot be told from, or a SKU that names a design, is NOT read by it
    ok(fresh.read(mk(listing, '', [V('Size', 'Large')], {})).spec.designSku !== 'ANIMAL_ONE', W + ': a line with an option of its own waits for a person');
    ok(fresh.read(mk(listing, 'PICK_ONE', [V('Personalization', 'Name One')], {})).spec.designSku === 'PICK_ONE', W + ': a SKU that names a design stands');
    // two links for the listing: the line's SKU cannot tell them apart, so a person decides
    await P.save(P.read(mk(listing, 'ODD_SKU_THREE', [V('Size', 'Large')], {})), 'ANIMAL_TWO');   // (a second answer for the listing, for a line with an option of its own)
    const two = page(sb); await two.load(); eq(Object.keys(two.maps.aliases[listing].bySku).sort(), ['ODD_SKU_ONE', 'ODD_SKU_THREE'], W + ': two links now');
    const unsure = two.read(mk(listing, '', [V('Personalization', 'Name One')], {}));
    ok(unsure.spec.designSku !== 'ANIMAL_ONE' && unsure.spec.designSku !== 'ANIMAL_TWO' && unsure.spec.problems.some(p => p.kind === 'unmatchedSku'), W + ': several links and no way to tell them apart: it waits for a person, never guesses: ' + JSON.stringify(designOf(unsure.spec)));
    pass(W + ' · 5c · exactly one link for a one-product listing is its link; several, or an option, wait for a person'); }

  /* ── 6 · the library is the only authority on a charm ── */
  { const P = page(sb), listing = newListing(), l = L(listing); Fk.writes.length = 0;
    let threw = null; try { await P.save(P.read(mk(listing, 'DACHSHUND')), 'NOT_IN_THE_LIBRARY'); } catch (e) { threw = e; }
    ok(threw && /master index/.test(threw.message), W + ': a charm the library does not hold is refused: ' + (threw && threw.message));
    ok(!l.Alias() && !l.Own() && !Fk.writes.some(w => /Aliases/.test(w.path)), W + ': and nothing is written');
    pass(W + ' · 6 · a charm the library does not hold is refused'); }

  /* ── 7 · nothing counts as saved that the cloud did not read back ── */
  { const listing = newListing(), row = page(sb).read(mk(listing, 'DACHSHUND'));
    for (const [what, answer] of [['no answer', null], ['ok without the read-back', { ok: true }], ['another charm read back', { ok: true, verified: true, sku: 'PICK_ONE' }], ['an error', { error: 'the link was written but could not be read back' }]]) {
      let threw = null; try { await RL.save(row, 'DACHSHUND_36943', { by: 'Name One', api: async () => answer, orders: { loadMaps: async () => { throw new Error('must not read the maps'); } } }); } catch (e) { threw = e; }
      ok(threw, W + ': ' + what + ' → not counted as saved (' + (threw && threw.message) + ')'); }
    let nameless = null; try { await RL.save(row, 'DACHSHUND_36943', { by: '', api: async () => ({}), orders: {} }); } catch (e) { nameless = e; } ok(nameless && /signed-in name/.test(nameless.message), W + ': the signed-in name is needed, never typed here');
    pass(W + ' · 7 · a write the cloud does not confirm is not counted as saved'); }
}

(async () => {
  await repoStamp.prime();
  for (const sb of [false, true]) await mode(sb);

  /* ── 8 · THE SANDBOX: the pick survives Reset and Purge and production reads it ── */
  const listing = newListing(), sbp = page(true), prod = page(false);
  const row = sbp.read(mk(listing, 'DACHSHUND'));
  const r = await sbp.save(row, 'DACHSHUND_36943'); ok(r.ok && r.proof.ok, 'the pick made in the sandbox: ' + JSON.stringify(r.proof));
  const optRow = sbp.read(mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')]));
  await sbp.api('charmNestLibrary', { op: 'optionMapPut', listingId: listing, optionName: 'Zodiac Sign', optionValue: 'Pisces', map: { field: 'design', value: 'ZSIGN_PISCES' }, by: 'Name One' });
  // a rehearsal answer that is NOT a charm stays the sandbox's own copy (and goes at the reset)
  await sbp.api('charmNestLibrary', { op: 'optionMapPut', listingId: listing, optionName: 'Size', optionValue: 'Large', map: { field: 'size', value: 'L' }, by: 'Name One' });
  const mine = () => ['Charm_Sku_Aliases/' + listing, 'Charm_Option_Map/' + listing].map(k => Fk.canon(Fk.store.get(k)));
  const before = mine();
  await prod.load(); eq(designOf(prod.read(mk(listing, 'DACHSHUND')).spec), ['DACHSHUND_36943', 'alias'], 'production reads the link the sandbox saved');
  for (let calls = 0, r2 = { more: true }; r2.more && calls < 50; calls++) r2 = (await post({ op: 'sandboxReset', sandbox: true })).body;
  ok(![...Fk.store.keys()].some(k => k.startsWith('Sandbox_Charm_Sku_Aliases/' + listing) || k.startsWith('Sandbox_Charm_Option_Map/' + listing)), 'the reset cleared the sandbox\'s own copies');
  eq(mine(), before, 'RESET: the pick is byte-identical in the shared store');
  let p2 = { body: { more: true } }; for (let calls = 0; p2.body.more && calls < 50; calls++) p2 = await post({ op: 'purgeHistory', code: '975311', force: true, sandbox: true });
  ok(p2.status === 200 && p2.body.ok, 'the purge finishes: ' + JSON.stringify(p2.body).slice(0, 120));
  eq(mine(), before, 'PURGE: the pick is byte-identical in the shared store');
  const after = page(true); await after.load();
  eq(designOf(after.read(mk(listing, 'DACHSHUND')).spec), ['DACHSHUND_36943', 'alias'], 'after Reset and Purge the sandbox matches the next order of the listing by the saved link');
  eq(designOf(after.read(mk(listing, 'UMBRELLA', [V('Zodiac Sign', 'Pisces'), V('Metal Choice', 'Gold Filled')])).spec), ['ZSIGN_PISCES', 'option'], 'and by the saved option charm');
  const prod2 = page(false); await prod2.load();
  eq(designOf(prod2.read(mk(listing, 'DACHSHUND')).spec), ['DACHSHUND_36943', 'alias'], 'production reads the same link');
  eq(after.maps.aliases[listing], prod2.maps.aliases[listing], 'the sandbox and production read the very same document');
  pass('8 · Reset and Purge never reach a pick; sandbox and production read the same links');

  console.log(`review-link: ${n} checks ok`);
})().catch(e => { console.error(e); process.exit(1); });
