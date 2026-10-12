// CHAMMAP (Paul, 12 Oct 2026, order 4199572384): a CHAMELEON necklace was read as an ASTRONAUT. Etsy listing 4494184284 "Chameleon Charm Necklace" has 60 products; 20 of them (the five charm-only metals,
// four lengths each) carry the shop's SHARED placeholder SKUs CO-GoldCharmO / CO-SilverChar / CO-RoseCharmO / CO-10kSolidCh / CO-14kSolidCh (the Wolf Face listing 4494182906 has the same five), and the master
// holds the label "CO-GOLDCHARMO" on the astronaut's drawing (ASTRONAUT_.ai). The Etsy SKU matched that master entry exactly, at once, for a listing of ANY design.
//   node tests/charm-nest/generic-sku.cjs
// Offline: the real charm-nest-orders.js, real master entries (read 9 Oct 2026, urls removed: fixtures/chammap-master-entries.json), the real order line (listing, SKU, options as Etsy sent them).
//  A · genericSku: the shop's shared charm-only placeholders, and ONLY those (a list of real design names that must never read as one)
//  B · the reading: no saved answer → no design on the row, one plain unmatched-SKU line, the decision is per listing; the listing's saved answer (bySku) → the chameleon, no question;
//      another listing's answer, an older listing-wide answer, the no-design list, the inventory path, a SKU the master does not hold, ordinary SKUs and "-CO" variations: as before
//  C · the Pool refuses the held line (nothing pooled) and pools the linked one as the chameleon
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/chammap-master-entries.json'), 'utf8')).entries;
const CHAM = 'CHAMELEON_28151', CHAM2 = 'CHAMELEON_87521', ASTRO = 'CO-GOLDCHARMO', LID = '4494184284', WOLF = '4494182906', RAW = 'CO-GoldCharmO';
const TITLE = 'Chameleon Charm Necklace';

(async () => {
  /* ── A · genericSku ── */
  for (const s of ['CO-GoldCharmO', 'CO-GOLDCHARMO', 'CO-SilverChar', 'CO-RoseCharmO', 'CO-10kSolidCh', 'CO-14kSolidCh', 'co-goldcharmo', 'CO GOLD CHARM', 'CO_GOLDCHARMO', 'CO-CHARMO', 'CO-14KGOLDCHARMO', 'CHARM', 'SILVER CHARM', 'GOLD CHARM', 'CHARM ONLY'])
    ok(O.genericSku(s) === true, s + ' is a generic charm-only placeholder');
  // real names of the master / of Etsy listings that must stay designs (the audit of the 6,961 master SKUs of 9 Oct found exactly three generic names: CO-GOLDCHARMO, CHARM, SILVER CHARM)
  for (const s of ['', 'CO', 'CO-', 'COBRA 1', 'COBRA', 'COW_123', 'COWGIRL_56191', 'CO-OWL', 'CO-ROSE', 'CO-GOLD', 'CO-CHAIN', 'CO-BEADY', 'ROSE', 'GOLD', 'SILVER', 'ROSE CHARM', 'BEADY CHARM', 'CHARM_1234', 'CHARMING', 'BIRTH_1605', 'GOLD_0187', 'SILVER_CHARM_1',
    CHAM, CHAM2, 'CHAMELEON_87521 (HUGGIE)', 'ASTRONAUT_', 'ASTRONAUT_9809', 'HUMMINGBIRD CUT OUT', 'CHAMELEON_28151-CO', 'Howling_60875-CO', 'MAPLE_8065-CO', 'Silhouette_61316-CO'])
    ok(O.genericSku(s) === false, JSON.stringify(s) + ' is not a placeholder');
  pass('A · genericSku: the shop\'s shared charm-only placeholders and only those');

  /* ── B · the reading ── */
  const mk = list => { const m = new Map(list.map(e => [e.sku.toUpperCase(), e])), loose = new Map(); for (const k of m.keys()) { const key = O.looseKey(k); loose.set(key, loose.has(key) ? '' : k); } return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' }; };
  const ctx = (extra, list) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } }, mk(list || masters), extra);
  const V = (name, value) => ({ name, value });
  const ORDER = { receiptId: '4199572384', updateTs: 1791839563 };
  const lineOf = over => Object.assign({ transactionId: '5247356127', listingId: LID, sku: RAW, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: ['/'], productId: '33658953874',
    variations: [V('Metal Choice', 'Gold-Charm Only'), V('Necklace Length in inches', 'Charm Only-No Chain'), V('Personalization', '/')] }, over || {});
  const read = (over, c) => { const line = lineOf(over); return { line, spec: O.interpretLine(ORDER, line, c || ctx()) }; };
  const link = { [LID]: { listingId: LID, bySku: { [ASTRO]: CHAM } } };
  const q = sp => (sp.problems || []).find(p => p.kind === 'unmatchedSku');

  // the order as it was: the exact master label won (what Paul saw: an astronaut for a chameleon necklace). Now: no design on the row, one plain line, nothing guessed
  { const { spec: sp } = read();
    ok(sp.designSku === null && sp.skuSource === null, 'no design on the row (' + sp.designSku + ')');
    ok(!JSON.stringify(sp).includes('ASTRONAUT'), 'the astronaut is nowhere in the reading');
    const p = q(sp);
    ok(p && p.sku === ASTRO && p.listingId === LID && p.formHold === true && p.generic === LID, 'one unmatched-SKU line for this SKU, asked per listing: ' + JSON.stringify(p));
    ok(/^SKU “CO-GOLDCHARMO” is a generic charm-only SKU the shop uses on many listings, so it does not say which charm this listing is/.test(p.reason) && !/\bnull\b|\bundefined\b/.test(p.reason), 'in plain words: ' + p.reason);
    eq(sp.problems.length, 1, 'that is the only question'); eq([sp.formHold.generic, sp.formHold.reason === p.reason, sp.formHold.siblings], [true, true, []], 'the form guard\'s own hold shape (makePool, the order window and the readiness read it)');
    ok(sp.noDesign === false && !sp.special, 'not a no-design or special line'); }
  // the decision is one per LISTING: the Wolf Face listing carries the same placeholder, its answer is its own
  { const a = q(read().spec), b = q(read({ listingId: WOLF, title: 'Wolf Face Charm Necklace' }).spec); ok(a.generic === LID && b.generic === WOLF && a.generic !== b.generic, 'a different listing is a different decision'); }
  // the listing's own saved answer for this SKU (what "Use this charm" saves; the one live link): the chameleon, read as a person's word, no question
  { const sp = read({}, ctx({ aliases: link })).spec;
    eq([sp.designSku, sp.skuSource, sp.problems, sp.formHold || null], [CHAM, 'alias', [], null], 'saved link → CHAMELEON_28151, source alias, nothing asked');
    ok(sp.noDesign === false && !JSON.stringify(sp).includes('ASTRONAUT'), 'cut as the chameleon, never the astronaut');
    const low = read({ sku: 'co-goldcharmo' }, ctx({ aliases: link })).spec; eq(low.designSku, CHAM, 'the SKU is matched as upper case, as every SKU is'); }
  // the other spelling of the same drawing is as good (one card, two SKUs)
  { const sp = read({}, ctx({ aliases: { [LID]: { bySku: { [ASTRO]: CHAM2 } } } })).spec; eq([sp.designSku, sp.problems.length], [CHAM2, 0], 'CHAMELEON_87521 is the same drawing and reads as well'); }
  // another listing's answer is not this listing's; another SKU of this listing is not covered by it
  { const sp = read({ listingId: WOLF }, ctx({ aliases: link })).spec; ok(sp.designSku === null && q(sp), 'the saved link of one listing does not answer another listing with the same SKU');
    const other = read({ sku: 'CO-SilverChar' }, ctx({ aliases: link })).spec; ok(other.designSku === 'CO-SILVERCHAR' || other.designSku === null, 'another SKU of the listing is not answered by it'); }
  // a saved link naming a design the master does not hold: the line waits on THAT design (never the astronaut)
  { const sp = read({}, ctx({ aliases: { [LID]: { bySku: { [ASTRO]: 'CHAMELEON_00000' } } } })).spec; ok(sp.designSku !== ASTRO && !JSON.stringify(sp).includes('ASTRONAUT'), 'a missing design is never replaced by the astronaut (' + sp.designSku + ')');
    ok((sp.problems || []).some(p => p.kind === 'unmatchedSku' && /not in any master file/.test(p.reason) && p.sku === 'CHAMELEON_00000'), 'it waits with the master question for the saved design'); }
  // an older answer for the whole listing (alias.sku, no v) still answers it
  { const sp = read({}, ctx({ aliases: { [LID]: { sku: CHAM } } })).spec; eq([sp.designSku, sp.skuSource, sp.problems.length], [CHAM, 'alias', 0], 'an old listing-wide answer stands as before'); }
  // a SKU on the no-design list is still that
  { const sp = read({}, ctx({ noDesign: { patterns: [], skus: [ASTRO] } })).spec; ok(sp.noDesign === true && !q(sp), 'on the no-design list: not cut, no question (as before)'); }
  // ordinary SKUs and the "-CO" mark are untouched
  { const a = read({ sku: CHAM, listingId: '999' }).spec; eq([a.designSku, a.skuSource, a.problems.length], [CHAM, 'transaction', 0], 'an ordinary master SKU is its design');
    const b = read({ sku: CHAM + '-CO', listingId: '999' }).spec; eq([b.designSku, b.skuSource, b.problems.length], [CHAM, 'variation', 0], '"-CO" is still the charm-only mark of its design');
    const c = read({ sku: 'CHAMELEON_49645', listingId: '999' }).spec; eq([c.designSku, c.problems.length], ['CHAMELEON_49645', 0], 'the chameleon stud keeps its own design'); }
  // a placeholder the master does not hold is the unknown SKU it always was (no new wording, no per-listing key)
  { const sp = read({ sku: 'CO-SilverChar' }).spec, p = q(sp); ok(p && /not in any master file/.test(p.reason) && !p.generic && !sp.formHold, 'CO-SilverChar: "not in any master file", as before'); }
  // the master's other generic labels hold too (SILVER CHARM on a skull, CHARM on a birth flower), with a saved answer they resolve
  for (const raw of ['SILVER CHARM', 'CHARM']) { const sp = read({ sku: raw }).spec; ok(sp.designSku === null && q(sp) && q(sp).generic === LID, raw + ' is held, not matched to the drawing it is a label on');
    const ok2 = read({ sku: raw }, ctx({ aliases: { [LID]: { bySku: { [raw]: CHAM } } } })).spec; eq(ok2.designSku, CHAM, raw + ' resolves with the listing\'s saved answer'); }
  // the Etsy table's SKU for the product bought stands in for a missing transaction SKU: a placeholder there is as generic
  { const table = { at: 1, n: 1, products: [{ id: '33658953874', sku: RAW, d: 0, pv: [['514', '1501824822441'], ['513', '166994992627']] }], names: {}, props: {} };
    const sp = read({ sku: '' }, ctx({ listingSkus: { [LID]: table } })).spec; ok(sp.designSku === null && q(sp) && q(sp).generic === LID && !JSON.stringify(sp).includes('ASTRONAUT'), 'a product SKU from the listing table that is a placeholder is held too (' + sp.skuSource + ')'); }
  // no SKU at all and no table: the no-SKU question, untouched by this guard
  { const sp = read({ sku: '' }).spec, p = q(sp); ok(p && /no SKU on the transaction/.test(p.reason) && !p.generic, 'no SKU: the old question'); }
  // the order's other piece reads exactly as before
  { const hb = read({ transactionId: '5249047218', listingId: '1710777569', sku: 'Hummingbird Cut out', title: 'Hummingbird Earrings Gold Bird Jewelry Gift for Her Stud Earrings Bird Lover Hummingbird Charm Jewelry Bird Earrings', productId: '24791732737', personalization: [],
      variations: [V('Metal', 'Gold Filled')] }).spec;
    eq([hb.designSku, hb.skuSource, hb.pieceCount, hb.problems.length], ['HUMMINGBIRD CUT OUT', 'transaction', 2, 0], 'the hummingbird studs: a pair, L and R, no question'); }
  pass('B · held with one plain line per listing; the listing\'s saved answer reads the chameleon; every other reading as before');

  /* ── C · the Pool ── */
  const F = require('./pairs-fixtures.cjs');
  const keyWorld = require('./_keyWorld.cjs')(root);
  const { DESIGNS, world, lineRow } = keyWorld;
  DESIGNS[CHAM] = () => F.charmOf('ONE-PENDANT'); DESIGNS[ASTRO] = () => F.charmOf('PAIR-STUD');
  const worldOf = () => { const w = world([CHAM, ASTRO]); w.c.P.parseSource = async (_b, name) => ({ name }); w.c.P.groupCharmsAsync = async parsed => ({ charms: [structuredClone(DESIGNS[String(parsed.name).replace(/\.ai$/, '')]())], orphans: [] }); return w; };
  const rowOf = c => { const { line, spec } = read({}, c); spec.material = 'gold'; return Object.assign(lineRow('4199572384', '5247356127', spec.designSku, 1, {}), { spec, problems: spec.problems.slice(), line: { transactionId: line.transactionId, listingId: line.listingId, sku: line.sku, title: line.title } }); };
  { const w = worldOf(), row = rowOf(); const r = await w.Pool.poolAdd(row, null);
    eq([row.state, w.calls.filter(c => c.body && c.body.op === 'poolPut').length], ['unmatched', 0], 'the held line is not pooled: nothing is written');
    ok(/generic charm-only SKU/.test(row.reason) && row.problems.some(p => p.generic === LID), 'it waits with the plain reason and the per-listing question (' + row.reason + ')'); void r; }
  { const w = worldOf(), row = rowOf(ctx({ aliases: link })); await w.Pool.poolAdd(row, null);
    const pools = (w.calls.find(c => c.body && c.body.op === 'poolPut') || { body: { pools: [] } }).body.pools; eq([row.state, pools.map(p => p.sku)], ['pooled', [CHAM]], 'the linked line is pooled as the chameleon, one piece'); }
  pass('C · the Pool: the held line is never pooled; the linked one is made as the chameleon');
  console.log(`generic-sku: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
