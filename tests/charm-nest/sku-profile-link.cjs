// SKUPROFILE (Paul, 27 Oct 2026): "For some reason the system refuses to update some listings on etsy." The Change SKU window for Etsy listing 1801964100 (the playing cards charm, "Available as a
// necklace charm or huggie charms"), order 4202871419: ETSY SKU NOW "CARDS 2 - (4 CARDS)" → NEW SKU "CARDS2- 4 CARDS", refused under the field with "Not saved: a new order of this listing would still
// read as CARDS 1 - (2 CARDS) (profile)". The listing's charm-only row (charm-nest-orders.js CHARM_ONLY_LISTINGS "cards-charm-only", CHARMONLY 19 Oct 2026) replaced every reading of the listing with
// its fixed entry, a person's saved link for the listing's SKU included (its ownSku is never true), so the save's own check (charm-nest-review-link.js proof, run by fixCharm before anything is written)
// refused it, rightly for the rule as it stood. Now, on a row of fixed entries, a person's word wins for the line's form:
//   1 · before: the 4-card variation's necklace reads CARDS 1 - (2 CARDS) by the row (the reading it replaced: CARDS 2 - (4 CARDS), the transaction's)
//   2 · the Change SKU check, as fixCharm runs it (CharmNestCharmFix offerOf, planOf, proof with the planned link): passes; the link is ONE aliasPut on the REAL Library function, read back
//   3 · a reload: the next orders of that variation (every metal; the order itself read again) are CARDS2- 4 CARDS, one piece, no question
//   4 · Etsy carries CARDS2- 4 CARDS now (Change SKU's Etsy write, here on the transaction and in Etsy's table): the next orders read it, not the row's CARDS 1
//   5 · the other variation (2 cards) is still its own design, and every Huggie CHARM SET keeps the row's huggie entry (a necklace link never reaches the huggie form)
//   6 · the check still refuses a link that would not hold (a necklace SKU to a "(HUGGIE)" drawing); the 19 Oct guards stand (a link of a SKU to itself, a listing-wide answer)
//   7 · a person's pick for the Huggie CHARM SET, under its own "(HUGGIE)" SKU, holds for the huggie set only
//   8 · another listing with the same SKU is not touched
// Offline: the real intake (interpretLine), the real link planner and proof, the real charmNestLibrary aliasPut / aliasGet over an in-memory Firestore. No Etsy, no network, nothing real.
//   node tests/charm-nest/sku-profile-link.cjs
'use strict';
const assert = require('assert'), path = require('path');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const O = require(path.join(root, 'charm-nest-orders.js'));
const RL = require(path.join(root, 'charm-nest-review-link.js'));
const CF = require(path.join(root, 'charm-nest-charm-fix.js'));
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
const repoStamp = require('./_repoToken.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), JSON.parse(JSON.stringify(b)), m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the library: the cards under their real names (sizes, holes and files as the master of 10-12 Oct 2026 has them: lookalike.cjs, hug-twin-link.cjs) ── */
const CARDS1 = 'CARDS 1 - (2 CARDS)', CARDS1H = 'CARDS 1 - (2 CARDS) (HUGGIE)', CARDS2 = 'CARDS 2 - (4 CARDS)', CARDS2H = 'CARDS 2 - (4 CARDS) (HUGGIE)', DECK = 'CARDS2- 4 CARDS';
const HUGDECK = 'HUGGIE HOOPS- DECK4';   // (made up: a huggie drawing of the four cards, for part 7)
const MM = 1 / 0.352778, F = f => 'charmnest/sandbox/master/' + f + '.ai';
const E = [
  { sku: CARDS1, holes: 1, widthPt: 26, heightPt: 30, aiPath: F('CARDS_1_-_2_CARDS_') }, { sku: 'CARDS 1 - 2 CARDS', holes: 1, widthPt: 26, heightPt: 30, aiPath: F('CARDS_1_-_2_CARDS_') },
  { sku: 'PLAYING_0805', holes: 1, widthPt: 26, heightPt: 30, aiPath: F('CARDS_1_-_2_CARDS_') }, { sku: CARDS1H, holes: 7, widthPt: 20, heightPt: 26, aiPath: F('CARDS_1_-_2_CARDS__HUGGIE_') },
  { sku: CARDS2, holes: 1, widthPt: 9.7 * MM, heightPt: 9.7 * MM, aiPath: F('CARDS_2_-_4_CARDS_') }, { sku: CARDS2H, holes: 7, widthPt: 8.1 * MM, heightPt: 10.4 * MM, aiPath: F('CARDS_2_-_4_CARDS_HUGGIE_') },
  { sku: DECK, holes: 0, widthPt: 32.728, heightPt: 35.413, aiPath: F('DECK_0120') }, { sku: HUGDECK, holes: 7, widthPt: 23, heightPt: 29.5, aiPath: F('HUGGIE_HOOPS-_DECK4') }
].map(e => Object.assign({ members: 1, sizes: null, blocked: null }, e));
const lib0 = new Map(E.map(e => [e.sku, e]));
for (const [k, e] of lib0) Fk.store.set('Charm_Master_Index/' + k, JSON.parse(JSON.stringify(e)));
const loose = {}, like = {}, byFile = new Map();
for (const k of lib0.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; (like[l] = like[l] || []).push(k); }
for (const [k, e] of lib0) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const entryFor = s => lib0.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e ? byFile.get(e.aiPath) || [] : []; };

const post = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(await repoStamp('charmNestLibrary', body)) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const must = async body => { const r = await post(body); assert.strictEqual(r.status, 200, JSON.stringify(body).slice(0, 140) + ' → ' + JSON.stringify(r.body).slice(0, 300)); return r.body; };
/** One sorter page: the saved links are what the Library answers (aliasGet / optionMapGet, as loadMaps reads them); tables: the page's Etsy SKU tables. A new page() is a reload. */
function page(tables) {
  const P = { maps: { optionMaps: {}, aliases: {} }, tables: tables || {} };
  P.api = async (fn, body) => must(body);
  P.load = async () => { P.maps.aliases = (await must({ op: 'aliasGet' })).aliases; P.maps.optionMaps = (await must({ op: 'optionMapGet' })).maps; };
  P.ctx = () => ({ optionMaps: P.maps.optionMaps, aliases: P.maps.aliases, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: P.tables, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor, masterLike: s => (like[O.looseKey(s)] || []).slice(), masterSkus: () => [...lib0.keys()] });
  P.Orders = { loadMaps: P.load, ctx: P.ctx };
  P.read = (line, rid) => { const order = { receiptId: rid || 'R' + line.transactionId, updateTs: 1, createTs: 1, buyerMessage: '', staffNote: '' }; return { line, order, spec: O.interpretLine(order, JSON.parse(JSON.stringify(line)), P.ctx()), state: 'pooled' }; };
  return P;
}
const reload = async tables => { const p = page(tables); await p.load(); return p; };

/* ── the listing: Metal Choice, Charm Type (Necklace CHARM / Huggie CHARM SET) and the design, Cards: 2 Cards (CARDS 1 - (2 CARDS)) / 4 Cards (CARDS 2 - (4 CARDS)); the necklace and the huggie products of a
   design share its SKU (one SKU for both forms, as Etsy keeps it), so the form is the Charm Type's ── */
const LID = '1801964100', TITLE = 'Playing Cards Charm Add On Charm Poker Card Charm Ace of Hearts Jewelry Birthday Gift Card Player Gift';
const METALS = [{ key: 'gold', label: 'Gold Filled', vid: '1' }, { key: 'silver', label: 'Sterling Silver', vid: '2' }];
const TYPES = { neck: { value: 'Necklace CHARM', vid: '11' }, hug: { value: 'Huggie CHARM SET', vid: '13' } }, DESIGNS = { two: { value: '2 Cards', vid: '21' }, four: { value: '4 Cards', vid: '22' } };
const pidOf = (m, t, d) => '9' + m.vid + TYPES[t].vid + DESIGNS[d].vid;
/** Etsy's table for the listing: every product's SKU (skuOf(design) → its SKU now). */
const tableOf = skuOf => ({ at: Date.now(), products: METALS.flatMap(m => Object.keys(TYPES).flatMap(t => Object.keys(DESIGNS).map(d => ({ id: pidOf(m, t, d), sku: skuOf(d), d: 0, pv: [['100', m.vid], ['200', TYPES[t].vid], ['300', DESIGNS[d].vid]] })))) });
const SKU_BEFORE = d => d === 'two' ? CARDS1 : CARDS2, SKU_AFTER = d => d === 'two' ? CARDS1 : DECK;   // (after: the Change SKU's Etsy write put CARDS2- 4 CARDS where CARDS 2 - (4 CARDS) was)
let tx = 7700000000;
/** A line as Etsy gives it: o { design: two | four, type: neck | hug, metal, sku (the transaction's; default the design's SKU before the change), listingId, plain (Metal + Charm Type only, as order 4202871419's fixture) } */
const card = (o = {}) => { const m = o.metal || METALS[0], t = o.type || 'neck', d = o.design || 'four', plain = !!o.plain;
  const vars = [{ name: 'Metal Choice', value: m.label, propertyId: '100', valueId: m.vid }, { name: 'Charm Type', value: TYPES[t].value, propertyId: '200', valueId: TYPES[t].vid }].concat(plain ? [] : [{ name: 'Cards', value: DESIGNS[d].value, propertyId: '300', valueId: DESIGNS[d].vid }]);
  return Object.assign({ transactionId: String(++tx), listingId: o.listingId || LID, sku: o.sku != null ? o.sku : SKU_BEFORE(d), title: TITLE, quantity: 1, metalKey: m.key, metalLabel: m.label, personalization: [], buyerMessage: '', variations: vars }, plain ? {} : { productId: pidOf(m, t, d) }); };
const design = sp => [sp.designSku, sp.skuSource];
const asks = sp => (sp.problems || []).map(p => p.kind);
const H = () => ({ RL, O });

(async () => {
  /* ── 1 · before: the row reads every necklace of the listing as its fixed entry ── */
  const P0 = await reload({ [LID]: tableOf(SKU_BEFORE) });
  const A = P0.read(card({ plain: true }), '4202871419');   // (order 4202871419 as the 19 Oct fixture has it: Etsy SKU CARDS 2 - (4 CARDS), Charm Type: Necklace CHARM)
  eq(design(A.spec), [CARDS1, 'profile'], 'before: order 4202871419 reads the row\'s necklace entry');
  eq(A.spec.listingProfile.was, { sku: CARDS2, source: 'transaction' }, 'before: the reading the row replaced is the Etsy SKU of the variation bought');
  const V4 = P0.read(card({ design: 'four' })), V2 = P0.read(card({ design: 'two' }));
  eq([design(V4.spec), asks(V4.spec), design(V2.spec), asks(V2.spec)], [[CARDS1, 'profile'], [], [CARDS1, 'profile'], []], 'before: both variations read CARDS 1 - (2 CARDS), no question');
  pass('1 · before: the cards row reads the 4-card variation as CARDS 1 - (2 CARDS) (profile)');

  /* ── 2 · Change SKU: the check fixCharm runs before anything is written, then the ONE link ── */
  for (const row of [A, V4]) {
    const off = CF.offerOf(row, H());
    eq([off.ok, off.via, off.from, off.cur, off.key], [true, 'alias', CARDS2, CARDS1, `${LID}|${CARDS2}`], 'Change SKU is offered, kept under the listing and the SKU of the variation bought: ' + JSON.stringify(off));
    const plan = RL.planOf(row, DECK), pre = RL.proof(row, DECK, Object.assign({}, P0.ctx(), CF.mapsWith(P0.maps, plan, O)));
    eq([pre.designOk, pre.ok, pre.designSku, pre.skuSource, pre.why], [true, true, DECK, 'alias', ''], 'the check before the write: a new order would read the pick (it said "would still read as CARDS 1 - (2 CARDS) (profile)")');
  }
  const r = await RL.pick(V4, DECK, { by: 'Paul K', api: P0.api, orders: P0.Orders });
  ok(r.ok && r.verified === true && r.via === 'alias' && r.key === `${LID}|${CARDS2}` && r.readBack.kind === 'bySku' && r.readBack.sku === DECK, 'saved and read back by the cloud: ' + JSON.stringify(r.readBack));
  eq(Fk.store.get('Charm_Sku_Aliases/' + LID).bySku, { [CARDS2]: DECK }, 'stored under the variation\'s SKU, nothing listing-wide');
  ok(r.proof.ok && r.proof.designSku === DECK && r.proof.skuSource === 'alias', 'the proof after the save: a new order reads the pick: ' + JSON.stringify(r.proof));
  pass('2 · Change SKU CARDS 2 - (4 CARDS) → CARDS2- 4 CARDS passes its check and is saved (one link, read back)');

  /* ── 3 · a reload, then the next orders of that variation ── */
  for (const tables of [{ [LID]: tableOf(SKU_BEFORE) }, {}]) {
    const P = await reload(tables), w = Object.keys(tables).length ? '(Etsy\'s table on the page)' : '(no table)';
    // (with no table on the page the Cards option waits for Etsy's SKU list, exactly as it did before the change: the same question, no new one)
    const was = Object.keys(tables).length ? [] : asks(page({}).read(card({ design: 'four' })).spec);
    for (const m of METALS) {
      const sp = P.read(card({ design: 'four', metal: m })).spec;
      eq([design(sp), asks(sp), sp.pieceCount, sp.lineForm, !!sp.listingProfile], [[DECK, 'alias'], was, 1, 'necklace', false], `${w} the next 4-card necklace (${m.label}) is CARDS2- 4 CARDS, one piece, ${was.length ? 'the same question as before' : 'no question'}`);
    }
    const again = P.read(A.line, '4202871419').spec;
    eq([design(again), asks(again)], [[DECK, 'alias'], []], `${w} order 4202871419 read again is CARDS2- 4 CARDS`);
  }
  pass('3 · after a reload every next order of the 4-card variation reads CARDS2- 4 CARDS');

  /* ── 4 · Etsy carries CARDS2- 4 CARDS now: the next orders read it, never the row's CARDS 1 ── */
  const P1 = await reload({ [LID]: tableOf(SKU_AFTER) });
  for (const [what, ln] of [['with the Cards option', card({ design: 'four', sku: DECK })], ['Metal + Charm Type only', card({ plain: true, sku: DECK })], ['Sterling Silver', card({ design: 'four', sku: DECK, metal: METALS[1] })]]) {
    const sp = P1.read(ln).spec;
    eq([sp.designSku, asks(sp), sp.pieceCount, !!sp.listingProfile], [DECK, [], 1, false], `Etsy SKU CARDS2- 4 CARDS (${what}): the design a person moved the listing to, no question`);
  }
  { const P = await reload({}); eq(P.read(card({ plain: true, sku: DECK })).spec.designSku, DECK, 'Etsy SKU CARDS2- 4 CARDS on a page with no table: CARDS2- 4 CARDS'); }
  pass('4 · once Etsy carries CARDS2- 4 CARDS the next orders read it (the row no longer reads them as CARDS 1)');

  /* ── 5 · the other variation, and the huggie form ── */
  for (const P of [P0, await reload({ [LID]: tableOf(SKU_BEFORE) }), P1]) {
    const two = P.read(card({ design: 'two' })).spec;
    eq([design(two), asks(two)], [[CARDS1, 'profile'], []], 'the 2-card variation is still its own design, CARDS 1 - (2 CARDS)');
  }
  for (const [what, ln, P] of [['4 cards', card({ design: 'four', type: 'hug' }), await reload({ [LID]: tableOf(SKU_BEFORE) })], ['2 cards', card({ design: 'two', type: 'hug' }), P1],
    ['4 cards after Etsy changed', card({ design: 'four', type: 'hug', sku: DECK }), P1], ['Metal + Charm Type only', card({ plain: true, type: 'hug' }), await reload({})]]) {
    const sp = P.read(ln).spec;
    eq([design(sp), sp.pieceCount, sp.pair && sp.pair.sides], [[CARDS1H, 'profile'], 2, ['L', 'R']], `the Huggie CHARM SET (${what}) keeps the row's huggie entry on both ears: the necklace link never reaches it`);
  }
  { const P = await reload({}), ln = card({ plain: true });
    ln.variations[1].value = 'Huggie Charm';   // (a huggie line that is not a "Huggie CHARM SET": it looks the link up under the SKU both forms share)
    eq(design(P.read(ln).spec), [CARDS1H, 'profile'], 'a huggie line under the shared SKU is never given the necklace drawing the link names'); }
  pass('5 · the 2-card variation keeps CARDS 1 - (2 CARDS); every huggie keeps CARDS 1 - (2 CARDS) (HUGGIE)');

  /* ── 6 · the check still refuses what would not hold; the 19 Oct guards stand ── */
  { const P = await reload({}), row = P.read(card({ plain: true, design: 'two', sku: CARDS1 }));
    const plan = RL.planOf(row, CARDS2H), pre = RL.proof(row, CARDS2H, Object.assign({}, P.ctx(), CF.mapsWith(P.maps, plan, O)));
    eq([pre.designOk, pre.why], [false, 'a new order of this listing would still read as CARDS 1 - (2 CARDS) (profile)'], 'a necklace SKU linked to a "(HUGGIE)" drawing would not hold: still refused, before anything is written'); }
  { const c = extra => Object.assign(page({}).ctx(), { aliases: { [LID]: extra } }), rd = (ln, ctx) => O.interpretLine({ receiptId: 'X', updateTs: 1 }, ln, ctx);
    eq([rd(card({ plain: true }), c({ bySku: { [CARDS2]: CARDS2 } })).designSku, rd(card({ plain: true, type: 'hug' }), c({ bySku: { [CARDS2H]: CARDS2H } })).designSku], [CARDS1, CARDS1H], 'a link of a SKU to itself is the name, not a choice: the row reads as on 19 Oct');
    eq(rd(card({ plain: true, sku: '' }), c({ sku: CARDS2, v: 2 })).designSku, CARDS1, 'a listing-wide answer does not win (19 Oct)');
    eq(rd(card({ plain: true, sku: CARDS2 }), c({ bySku: { 'PLAYING CARDS': CARDS2H } })).designSku, CARDS1, 'a link to a huggie drawing never makes a necklace line read it'); }
  pass('6 · the check refuses a link of the other form; a link of a SKU to itself and a listing-wide answer still do not win');

  /* ── 7 · a pick for the Huggie CHARM SET under its own "(HUGGIE)" SKU holds for the huggie set only ── */
  { const P = await reload({}), row = P.read(card({ plain: true, type: 'hug' }));
    eq([design(row.spec), row.spec.boughtSku], [[CARDS1H, 'profile'], CARDS2H], 'the huggie set is read under its own SKU, CARDS 2 - (4 CARDS) (HUGGIE)');
    const no = RL.proof(row, 'PLAYING_0805', Object.assign({}, P.ctx(), CF.mapsWith(P.maps, RL.planOf(row, 'PLAYING_0805'), O)));
    eq([no.designOk, no.why], [false, 'a new order of this listing would still read as CARDS 1 - (2 CARDS) (HUGGIE) (profile)'], 'a huggie set linked to a necklace drawing (PLAYING_0805) would not hold: refused');
    const plan = RL.planOf(row, HUGDECK), pre = RL.proof(row, HUGDECK, Object.assign({}, P.ctx(), CF.mapsWith(P.maps, plan, O)));
    eq([plan.key, pre.designOk, pre.skuSource], [`${LID}|${CARDS2H}`, true, 'alias'], 'a huggie drawing for the huggie set, under its own SKU: the check passes');
    const r2 = await RL.pick(row, HUGDECK, { by: 'Paul K', api: P.api, orders: P.Orders });
    ok(r2.ok && r2.proof.ok && r2.proof.designSku === HUGDECK, 'saved, and a new huggie set order reads it: ' + JSON.stringify(r2.proof));
    const Q = await reload({});
    eq([design(Q.read(card({ plain: true, type: 'hug', metal: METALS[1] })).spec), design(Q.read(card({ plain: true })).spec)], [[HUGDECK, 'alias'], [DECK, 'alias']], 'after a reload: the huggie set reads its pick, the necklace keeps its own (CARDS2- 4 CARDS)'); }
  pass('7 · a pick for the Huggie CHARM SET under its own SKU holds for the huggie set only; one of the necklace form is refused');

  /* ── 8 · another listing with the same SKU is its own ── */
  { const P = await reload({});
    eq(design(P.read(card({ plain: true, listingId: '1749318013' })).spec), [CARDS2, 'transaction'], 'the card stud listing 1749318013 still reads CARDS 2 - (4 CARDS)');
    eq(design(P.read(card({ plain: true, listingId: '1700000999', sku: DECK })).spec), [DECK, 'transaction'], 'CARDS2- 4 CARDS on any other listing reads as before'); }
  pass('8 · the link is the listing\'s: other listings with the same SKU are not touched');

  console.log(`sku-profile-link: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
