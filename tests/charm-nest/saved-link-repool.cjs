// COMETMAP (Paul, 12 Oct 2026 18:41 UTC, order 4197740562, Etsy listing 1687890868 "Comet Pendant Charm Necklace", Etsy SKU "Celestial3-Comet"): "Update the Mapping of this etsy listing to be
// matched with the Repo charm SKU". The library holds TWO comets: "CELESTIAL3-COMET" (the old drawing, 9.3 x 9.1 mm, NO hanging loop: the very entry the Etsy SKU names) and Paul's repo charm
// "CELESTIAL 3 - COMET" (= COMET_9427, 8.3 x 14.4 mm, 2 holes, with the loop). A saved link for the listing and SKU now beats the exact master match (DACHMAP, saved-link-wins.cjs); THIS test is
// the other half: an order that was ALREADY pooled as the old drawing is told about the link.
//   · one design a person saved for this listing and SKU (spec.skuSource "alias"), on a line whose pieces were pooled as ANOTHER drawing, is offered as the Review "Order changed" card, Accept the
//     new order, exactly as a saved pair of charms already is (SCHROLINK); the offer writes nothing, says what it was and what it is now, and is made once
//   · never on a sheet that is released (full), recalled or cut: left as it was, said once in the log AND once on the order's timeline, in words
//   · never when the pieces already are that drawing (under any of its names), when a piece's record names no design, when nothing was made yet, when the line is held, or when the SKU was not
//     read from a saved link
//   node tests/charm-nest/saved-link-repool.cjs         Offline: no network, no Etsy, no AI, nothing live.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const keyWorld = require('./_keyWorld.cjs')(root);
const { DESIGNS, world, lineRow } = keyWorld;
const F = require('./pairs-fixtures.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

const OLD = 'CELESTIAL3-COMET', NEW = 'CELESTIAL 3 - COMET', TWIN = 'COMET_9427', LID = '1687890868', RAW = 'Celestial3-Comet';
const TITLE = 'Comet Pendant Charm Necklace Dainty Space Jewelry Tiny Celestial Charm Space Necklace Cosmic Pendant Space Jewelry Gift for Her';
// the real index entries (read-only masterGet, 12 Oct 2026): three entries, two drawings
const real = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/comet-master-entries.json'), 'utf8')).entries;
const lib = new Map(real.map(e => [e.sku, e])), loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const ctx = aliases => ({ optionMaps: {}, aliases, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '' });
const V = (name, value) => ({ name, value });
const lineOf = over => Object.assign({ transactionId: '5246703786', listingId: LID, sku: RAW, title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '',
  variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '18"')] }, over || {});
const SAVED = { [LID]: { v: 2, bySku: { [OLD]: NEW }, by: 'COMETMAP' } };
const read = (over, aliases) => { const line = lineOf(over); return { line, spec: O.interpretLine({ receiptId: '4197740562', updateTs: 1 }, line, ctx(aliases === undefined ? SAVED : aliases)) }; };

(async () => {
  /* ── A · the reading, on the real entries ── */
  ok(lib.get(OLD).charmHash !== lib.get(NEW).charmHash && lib.get(OLD).holes === 0 && lib.get(NEW).holes === 2, 'the two comets are two drawings, the old one with no loop (0 holes), the repo charm with 2');
  eq([lib.get(NEW).charmHash, lib.get(NEW).aiPath], [lib.get(TWIN).charmHash, lib.get(TWIN).aiPath], 'the repo card\'s two SKUs (CELESTIAL 3 - COMET, COMET_9427) are one drawing in one file; CELESTIAL3-COMET is not on that card');
  { const sp = read({}, {}).spec; eq([sp.designSku, sp.skuSource], [OLD, 'transaction'], 'without a link the Etsy SKU is the old loop-less entry (what Paul saw)'); }
  { const sp = read().spec; eq([sp.designSku, sp.skuSource, sp.problems.length], [NEW, 'alias', 0], 'with the saved link the line reads as Paul\'s repo charm, no question'); }
  eq([read({ listingId: '999' }).spec.designSku, read({ sku: 'CELESTIAL 3 - COMET' }).spec.designSku], [OLD, NEW], 'the link is this listing\'s and this SKU\'s only (another listing: the old entry; the card\'s own SKU: itself)');
  pass('A · the link makes the line read as CELESTIAL 3 - COMET for this listing and SKU only');

  /* ── B · the Pool: a line pooled BEFORE the link ── */
  DESIGNS[OLD] = () => F.charmOf('ONE-PENDANT'); DESIGNS[NEW] = () => F.charmOf('PAIR-HOOP'); DESIGNS[TWIN] = () => F.charmOf('PAIR-HOOP');
  const worldOf = () => {
    const w = world([OLD, NEW, TWIN]), reviews = [], timeline = [];
    w.entries[TWIN].aiPath = w.entries[NEW].aiPath;   // (one drawing, two names, as the library keeps it)
    w.c.Review = { add: it => reviews.push(it) }; w.c.B.review = { items: reviews }; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false };
    // (INPLACE, Paul 20 Oct 2026: the Pool asks the bridge's replaceCutWhy which sheets may change: the real one, from the same source)
    { const src = keyWorld.src, i = src.indexOf('function replaceCutWhy(sh, ids) {'), j = src.indexOf('window.replaceCutWhy = replaceCutWhy;', i); assert(i > 0 && j > i, 'replaceCutWhy is where the test expects it'); require('vm').runInContext(src.slice(i, j), w.c); }
    w.c.TL = { line: (row, type, o) => { timeline.push({ type, o }); return true; }, hash: s => String(s).length.toString(36) };
    return Object.assign(w, { reviews, timeline });
  };
  const rowOf = (over, aliases, qty) => { const { line, spec } = read(over, aliases); spec.material = 'gold'; if (qty) spec.quantity = qty; return Object.assign(lineRow('4197740562', '5246703786', spec.designSku, qty || 1, {}), { spec, line: { transactionId: line.transactionId, listingId: line.listingId, sku: line.sku, title: line.title } }); };
  const pooledBefore = (w, row, sku, extra) => { const id = '4197740562_5246703786_1'; row.poolIds = [id]; w.c.B.pool.rows.set(id, Object.assign({ poolId: id, state: 'ready', sku: sku === undefined ? OLD : sku, copy: 1, quantity: 1, charmHash: 'x' }, extra || {})); return id; };
  const writes = w => w.calls.filter(c => c.body && /pool|sheet|alias|master|engrave|back/i.test(c.body.op || '')).length;

  { const w = worldOf(), row = rowOf(), id = pooledBefore(w, row);
    w.Pool.pinPooled(row);
    eq([row.changePending, row.repoolChanged, row.poolIds], [true, true, [id]], 'the line is flagged to be made up again');
    eq(w.reviews.length, 1, 'one card'); const it = w.reviews[0];
    eq([it.kind, it.key, it.row === row, it.old.spec.designSku], ['orderChanged', 'chg:' + row.key, true, OLD], 'the Order changed card, for this line, "Was" the old comet');
    ok(/^Now CELESTIAL 3 - COMET \(a saved link\)\. Accept the new order to make it up again as 1 piece: the old CELESTIAL3-COMET comes off the open sheet it is on\. Nothing cut or released is touched\.$/.test(it.why), 'in plain words: ' + it.why);
    w.Pool.pinPooled(row); eq(w.reviews.length, 1, 'asked again it adds nothing');
    eq(writes(w), 0, 'nothing is written by the offer (no pool, sheet, alias, master or engraving call)');
    eq(w.timeline.length, 0, 'an open sheet needs no timeline note: the card is the answer'); }
  // quantity 2: the card says 2 pieces
  { const w = worldOf(), row = rowOf({ quantity: 2 }, undefined, 2); pooledBefore(w, row); w.c.B.pool.rows.set('4197740562_5246703786_2', { poolId: '4197740562_5246703786_2', state: 'ready', sku: OLD, copy: 2, quantity: 2, charmHash: 'x' }); row.poolIds.push('4197740562_5246703786_2');
    w.Pool.pinPooled(row); eq(w.reviews.length, 1, 'quantity 2: one card'); ok(/as 2 pieces: the old CELESTIAL3-COMET comes off/.test(w.reviews[0].why), 'it says 2 pieces and names the old design once: ' + w.reviews[0].why); }
  // never on a sheet the laser has cut, or one recalled from the library: left as it was, said once in the log and once on the timeline
  // (INPLACE, Paul 20 Oct 2026: "Unless a sheet has been marked as fully completed and laser cut then that charm needs to come off that particular sheet": a full sheet changes, below)
  for (const [what, set] of [['laser cut', sh => { sh.laserDoneAt = 1; }], ['recalled', sh => { sh.recalled = true; }], ['cut', sh => { sh.roseCutAt = 1; }]]) {
    const w = worldOf(), row = rowOf(), id = pooledBefore(w, row); w.pages.gold.charms.push({ poolId: id }); w.pages.gold.fileBase = 'SS Draft 1'; set(w.pages.gold);
    w.Pool.pinPooled(row); w.Pool.pinPooled(row);
    eq([row.changePending || false, row.repoolChanged || false, w.reviews.length], [false, false, 0], `a piece on a sheet ${what} is left as it was: no flag, no card`);
    eq(w.logs.filter(t => /a saved link makes this line CELESTIAL 3 - COMET, but its piece is on SS Draft 1, released or cut: left as it was/.test(t)).length, 1, 'the log says so once');
    eq(w.timeline.length, 1, 'and the order\'s timeline says so once'); ok(w.timeline[0].type === 'interpreted' && /^Saved link: now CELESTIAL 3 - COMET, but CELESTIAL3-COMET is on SS Draft 1, released or cut, so the sheet was left as it was$/.test(w.timeline[0].o.text) && w.timeline[0].o.once === true, 'in words: ' + w.timeline[0].o.text);
    eq(writes(w), 0, 'nothing is written to a sheet or a record'); }
  // a full sheet not cut yet (INPLACE, Paul 20 Oct 2026): offered, like a draft
  { const w = worldOf(), row = rowOf(), id = pooledBefore(w, row); w.pages.gold.charms.push({ poolId: id }); w.pages.gold.releaseFull = true; w.pages.gold.fileBase = 'SS Full 1';
    w.Pool.pinPooled(row); eq([row.changePending, w.reviews.length], [true, 1], 'a piece on a full sheet not laser cut is offered the new reading'); }
  // a draft sheet that is open (not full, not in a set): offered
  { const w = worldOf(), row = rowOf(), id = pooledBefore(w, row); w.pages.gold.charms.push({ poolId: id }); w.pages.gold.draft = true; w.pages.gold.fileBase = 'SS Draft 2';
    w.Pool.pinPooled(row); eq([row.changePending, w.reviews.length], [true, 1], 'a piece on an open draft sheet is offered the new reading'); }
  // nothing to offer
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row, NEW); w.Pool.pinPooled(row); eq([row.changePending || false, w.reviews.length], [false, 0], 'a line already made as the link says is left alone'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row, TWIN); w.Pool.pinPooled(row); eq([row.changePending || false, w.reviews.length], [false, 0], 'the same drawing under its other name (COMET_9427) is the same design: left alone'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row, ''); w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a piece whose record names no design is not guessed at'); }
  { const w = worldOf(), row = rowOf(); w.Pool.pinPooled(row); eq([row.changePending || false, w.reviews.length], [false, 0], 'nothing pooled yet: the line is simply made from the link'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row); row.hold = 'held by a person'; w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a held line is not offered anything'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row); row.state = 'committed'; w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a committed (cut) line is never offered anything'); }
  { const w = worldOf(), row = rowOf({ listingId: '999' }, {}); pooledBefore(w, row, TWIN); row.spec = Object.assign({}, row.spec, { designSku: NEW }); w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a line whose SKU was NOT read from a saved link (an exact master match, an option, an Etsy table) is never offered a change here'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row); row.spec.noDesign = true; w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a line that is not cut is not offered a change'); }
  { const w = worldOf(), row = rowOf(); pooledBefore(w, row); row.spec.problems = [{ kind: 'unmatchedSku' }]; w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a line with an open question is not offered a change'); }
  pass('B · an order pooled as the old comet is offered the Order changed card on an open or full sheet (once, no writes); a laser cut, recalled or cut sheet is left as it was and said so; every other case is quiet');

  console.log(`saved-link-repool: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
