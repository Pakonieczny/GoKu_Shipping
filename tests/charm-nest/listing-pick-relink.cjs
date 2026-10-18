// LISTPICK (Paul, 18 Oct 2026; order 4201900041 "Charm picked: FEATHER_4102" while 4204208130 of the same listing kept its old feather): which open orders a person's pick for a listing changes, and how.
// charm-nest-listing-relink.js over the REAL reader (charm-nest-orders.js interpretLine) and the REAL link lookup (charm-nest-review-link.js linkOf): no page, no network.
//   · the same listing is matched at the granularity the matcher reads a link at: listing + SKU (alias), or listing + the option value that gives the charm (option); another option value,
//     another SKU or another listing is never touched
//   · not made yet → "design"; pooled or on an open sheet → "swap"; cut / committed / released / protected contour → "flag" (held for a person only when this page holds the sheet and the line is
//     not committed); in Review → "kept"; a person's own pick for the order wins; cancelled, gone, skipped → nothing
//   · a second pass over the state after the swaps answers "none" for every line (idempotent)
//   node tests/charm-nest/listing-pick-relink.cjs
'use strict';
const assert = require('assert'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const RL = require(path.join(root, 'charm-nest-review-link.js'));
const L = require(path.join(root, 'charm-nest-listing-relink.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const pass = name => console.log('  ✓', name);

const LID = '1700000001', OPT_LID = '1700000002', OTHER_LID = '1700000099';
const E = s => ({ sku: s, widthPt: 20, heightPt: 30, holes: 1, charmHash: 'h' + s, aiPath: 'x/' + s + '.ai' });
const lib = new Map(['FEATHER', 'FEATHER_4102', 'FEATHER_B', 'FEATHER_4102_TWIN'].map(s => [s, E(s)]));
lib.get('FEATHER_4102_TWIN').charmHash = 'hFEATHER_4102'; lib.get('FEATHER_4102_TWIN').aiPath = 'x/FEATHER_4102.ai';   // (one drawing, two names)
const ctxOf = maps => ({ optionMaps: maps.optionMaps || {}, aliases: maps.aliases || {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: () => '' });
const V = (name, value) => ({ name, value });
const lineOf = (tid, over) => Object.assign({ transactionId: tid, listingId: LID, sku: 'FEATHER', title: 'Feather Necklace Dainty Gold Feather Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [V('Metal', 'Gold'), V('Length', '18"')] }, over || {});
const OM0 = { [OPT_LID]: { charm: { 'feather a': { field: 'design', value: 'FEATHER', by: 'Ann', at: 1 }, 'feather b': { field: 'design', value: 'FEATHER_B', by: 'Ann', at: 1 } } } };
const BEFORE = { aliases: {}, optionMaps: OM0 };
const AFTER = { aliases: { [LID]: { bySku: { FEATHER: 'FEATHER_4102' }, by: 'Paul', listingId: LID } }, optionMaps: OM0 };
const AFTER_OPT = { aliases: {}, optionMaps: { [OPT_LID]: { charm: { 'feather a': { field: 'design', value: 'FEATHER_4102', by: 'Paul', at: 2 }, 'feather b': OM0[OPT_LID].charm['feather b'] } } } };
const read = (row, maps) => { row.spec = O.interpretLine(row.order, row.line, ctxOf(maps)); return row; };
const rowOf = (rid, tid, over, state) => ({ key: `${rid}_${tid}`, order: { receiptId: rid, updateTs: 1 }, line: lineOf(tid, over), state: state || 'pulled', poolIds: [], hold: null, spec: null });

/* the world: pool rows (id → { sku, state }), sheets ({ name, flags, ids }), the Review's marks, cancelled orders */
function world() {
  const pool = new Map(), sheets = [], marks = {}, cancelled = new Set(), inReview = new Set();
  const put = (row, sku, sheet, state) => { const id = `${row.key}_1`; row.poolIds = [id]; pool.set(id, { poolId: id, sku, state: state || 'ready', copy: 1 }); if (sheet) sheet.ids.add(id); row.state = 'pooled'; return id; };
  const sheet = (name, flags) => { const s = Object.assign({ name, ids: new Set() }, flags || {}); sheets.push(s); return s; };
  const h = maps => ({
    maps, linkOf: (r, m) => RL.linkOf(r, m),
    made: r => { const ids = r.poolIds || []; if (!ids.length) return []; const got = ids.map(id => pool.get(id)); if (got.some(x => !x)) return null; return got.filter(x => !['abandoned', 'superseded'].includes(x.state)).map(x => String(x.sku || '').toUpperCase()); },
    fixedOf: r => sheets.filter(s => (r.poolIds || []).some(id => s.ids.has(id))).map(s => ({ name: s.name, why: s.cut ? 'cut' : s.released ? 'released to the laser' : s.contour ? 'inside the protected Rose Gold contour' : '', cut: !!s.cut, local: true })).filter(f => f.why),
    keepsPlace: r => inReview.has(String(r.order.receiptId)) && sheets.some(s => (r.poolIds || []).some(id => s.ids.has(id))),
    ownPick: r => { const m = marks[r.order.receiptId]; return m && m.how === 'pick' && (!m.rowKey || m.rowKey === r.key) ? m.sku : ''; },
    sameDesign: (a, b) => O.sameDesign(a, b, k => lib.get(String(k || '').toUpperCase()) || null),
    cancelled: r => cancelled.has(String(r.order.receiptId)),
    readNow: r => O.interpretLine(r.order, r.line, ctxOf(maps))
  });
  return { pool, sheets, marks, cancelled, inReview, put, sheet, h };
}
const decide = (rows, prev, next, w) => {
  const snap = L.snapshot(rows, prev, next, (r, m) => RL.linkOf(r, m));
  for (const r of rows) if (r.state !== 'committed') read(r, next);   // (the page reads every line again with the new maps; a committed line is never read again, Orders.interpretAll)
  const out = {}; for (const e of snap) out[e.key] = L.classify(e, w.h(next));
  return { snap, out };
};

/* ── A · the listing's three orders and their neighbours ── */
{
  ok(L.changedListings(BEFORE, AFTER).size === 1 && L.changedListings(BEFORE, AFTER).has(LID), 'the alias record of ONE listing changed');
  eq([...L.changedListings(BEFORE, AFTER_OPT)], [OPT_LID], 'an option value\'s answer changed on ONE listing');
  eq(L.changedListings(AFTER, JSON.parse(JSON.stringify(AFTER))).size, 0, 'a copy that reads the same changed nothing');

  const w = world();
  const open1 = w.sheet('GF Sheet 1'), cut2 = w.sheet('GF Sheet 2', { cut: true }), rel3 = w.sheet('GF Sheet 3', { released: true }), rose = w.sheet('RG Sheet 1', { contour: true });
  // the three orders of the listing (Paul's case): the one he corrected (on GF Sheet 1), one not pooled yet, one nested on the open GF Sheet 1
  const picked = rowOf('4201900041', '11'), fresh = rowOf('4201900042', '12'), nested = rowOf('4204208130', '13');
  // and the rest: cut, released, contour, committed, in Review, a person's own pick, cancelled, another listing with the same SKU, a quantity-2 line pooled but not on a sheet, the twin drawing already
  const cut = rowOf('4204208131', '14'), released = rowOf('4204208132', '15'), contour = rowOf('4204208133', '16'), done = rowOf('4204208134', '17', null, 'committed');
  const review = rowOf('4204208135', '18'), own = rowOf('4204208136', '19'), gone = rowOf('4204208137', '20'), other = rowOf('4204208138', '21', { listingId: OTHER_LID });
  const twin = rowOf('4204208139', '22'), pooledOnly = rowOf('4204208140', '23');
  const rows = [picked, fresh, nested, cut, released, contour, done, review, own, gone, other, twin, pooledOnly];
  for (const r of rows) read(r, BEFORE);
  ok(rows.every(r => r.spec.designSku === 'FEATHER' && r.spec.skuSource === 'transaction'), 'before the pick every line reads as the old FEATHER (an exact master match)');
  w.put(picked, 'FEATHER', open1); w.put(nested, 'FEATHER', open1); w.put(cut, 'FEATHER', cut2, 'committed'); w.put(released, 'FEATHER', rel3); w.put(contour, 'FEATHER', rose);
  w.put(done, 'FEATHER', cut2, 'committed'); w.put(review, 'FEATHER', open1); w.put(own, 'FEATHER', open1); w.put(gone, 'FEATHER', open1); w.put(twin, 'FEATHER_4102_TWIN', open1); w.put(pooledOnly, 'FEATHER', null);
  gone.state = 'gone'; done.state = 'committed'; w.cancelled.add('4204208137'); w.inReview.add('4204208135'); w.marks['4204208136'] = { how: 'pick', sku: 'FEATHER_B' }; w.marks['4201900041'] = { how: 'pick', sku: 'FEATHER_4102', rowKey: picked.key };

  const { snap, out } = decide(rows, BEFORE, AFTER, w);
  ok(!snap.some(e => e.key === other.key), 'another listing with the same SKU is not even looked at');
  ok(!snap.some(e => e.key === gone.key), 'a line gone from Etsy is not looked at');
  ok(snap.every(e => e.from === 'FEATHER' && e.was === null), 'each line is noted as it read BEFORE: the old drawing, read by no saved link');
  const act = k => out[k] && out[k].action;
  eq([act(picked.key), out[picked.key].to, out[picked.key].from], ['swap', 'FEATHER_4102', 'FEATHER'], 'the corrected order itself: its piece on the open GF Sheet 1 is made up again as FEATHER_4102');
  eq([act(fresh.key), out[fresh.key].from, out[fresh.key].to], ['design', 'FEATHER', 'FEATHER_4102'], 'the order not pooled yet simply reads the new charm (no write)');
  eq(act(nested.key), 'swap', 'the order nested on the open sheet: taken off and made up again');
  eq(act(pooledOnly.key), 'swap', 'a line pooled but on no sheet yet: its pool rows are made up again');
  eq([act(cut.key), out[cut.key].hold, out[cut.key].fixed[0].cut], ['flag', true, true], 'a piece on a CUT sheet: flagged, held for a person, nothing changed');
  ok(/cut on GF Sheet 2/.test(out[cut.key].why), 'says where: ' + out[cut.key].why);
  eq([act(released.key), out[released.key].hold], ['flag', true], 'a piece on a sheet released to the laser: flagged and held (its set waits)');
  eq([act(contour.key), /protected Rose Gold contour/.test(out[contour.key].why)], ['flag', true], 'a piece inside a protected Rose Gold contour: flagged');
  eq([act(done.key), out[done.key].hold, out[done.key].to], ['flag', false, 'FEATHER_4102'], 'a committed line (cut, its set committed) is read on the side and TOLD, never held or changed');
  eq(act(review.key), 'kept', 'an order in Review keeps its piece on its sheet (REVHOLD) until the Review is resolved');
  eq([act(own.key), /a person picked FEATHER_B/.test(out[own.key].why)], ['skip', true], 'a person\'s own pick for that order (another charm) wins over the listing\'s');
  eq(act(twin.key), 'none', 'a piece already the picked drawing under another name: nothing to do');
  pass('A · the corrected order, the unpooled one and the nested one are updated; cut, released, contour and committed lines are flagged; Review, own pick, gone and another listing are left');

  /* ── B · the second pass: after the swaps nothing is left to do ── */
  for (const r of [picked, nested, pooledOnly]) w.pool.get(r.poolIds[0]).sku = 'FEATHER_4102';
  const again = {}; for (const e of snap) again[e.key] = L.classify(e, w.h(AFTER));
  for (const r of [picked, fresh, nested, pooledOnly, twin]) ok(['none', 'design'].includes(again[r.key].action) && (r !== fresh || again[r.key].action === 'design'), `${r.key}: ${again[r.key].action}`);
  // a second sweep from the same maps (snapshot taken AFTER: the lines already read the pick) finds nothing at all
  const snap2 = L.snapshot(rows, AFTER, AFTER, (r, m) => RL.linkOf(r, m));
  eq(snap2.length, 0, 'the same maps again: no listing changed, no line looked at');
  const later = L.snapshot(rows, BEFORE, AFTER, (r, m) => RL.linkOf(r, m));   // (lines read with the new maps already: their link did not move)
  ok(later.filter(e => e.key !== done.key).every(e => L.classify(e, w.h(AFTER)).action !== 'swap'), 'noted again after the change, no line is made up a second time');
  eq(later.filter(e => [picked.key, nested.key, fresh.key, pooledOnly.key].includes(e.key)).map(e => L.classify(e, w.h(AFTER)).action), ['none', 'none', 'none', 'none'], 'a line already read by the pick, its pieces already the pick: "none" (no event, no write)');
  { const e = L.snapshot([fresh], AFTER, AFTER, (r, m) => RL.linkOf(r, m))[0]; ok(!e, 'and maps that did not move note nothing'); }
  { const e = { key: nested.key, row: nested, from: 'FEATHER', was: { sku: 'FEATHER_4102', key: `${LID}|FEATHER` } }; eq([L.classify(e, w.h(AFTER)).action, /did not change/.test(L.classify(e, w.h(AFTER)).why)], ['skip', true], 'a line read by the same link value before and now: "the link did not change"'); }
  // the pick changed AGAIN (a person picked another charm for the listing): the corrected order's own pick (FEATHER_4102) now wins for it, the others follow the new pick
  const AGAIN = { aliases: { [LID]: { bySku: { FEATHER: 'FEATHER_B' }, by: 'Ann', listingId: LID } }, optionMaps: OM0 };
  const { out: out3 } = decide(rows, AFTER, AGAIN, w);
  eq([out3[picked.key].action, out3[nested.key].action, out3[nested.key].to], ['skip', 'swap', 'FEATHER_B'], 'a later pick: the order whose own Review picked FEATHER_4102 keeps it; the others follow FEATHER_B');
  pass('B · a second pass changes nothing; a later pick is followed, and an order\'s own pick still wins');
}

/* ── C · option value granularity ── */
{
  const w = world(), open = w.sheet('GF Sheet 1');
  const opt = (rid, tid, val) => rowOf(rid, tid, { listingId: OPT_LID, sku: '', variations: [V('Metal', 'Gold'), V('Charm', val)] });
  const a1 = opt('4300000001', '31', 'Feather A'), a2 = opt('4300000002', '32', 'Feather A'), b1 = opt('4300000003', '33', 'Feather B');
  const rows = [a1, a2, b1]; for (const r of rows) read(r, BEFORE);
  eq(rows.map(r => [r.spec.designSku, r.spec.skuSource]), [['FEATHER', 'option'], ['FEATHER', 'option'], ['FEATHER_B', 'option']], 'the charm comes from the option value');
  w.put(a1, 'FEATHER', open); w.put(b1, 'FEATHER_B', open);
  const { snap, out } = decide(rows, BEFORE, AFTER_OPT, w);
  eq(snap.length, 3, 'every live line of the listing is looked at');
  eq([out[a1.key].action, out[a2.key].action, out[b1.key].action], ['swap', 'design', 'skip'], 'Feather A (on a sheet) is made up again, Feather A (not pooled) reads the pick, Feather B is untouched');
  ok(/did not change/.test(out[b1.key].why) && b1.spec.designSku === 'FEATHER_B', 'another option value of the same listing keeps its own charm: ' + out[b1.key].why);
  eq(out[a1.key].link.key, `${OPT_LID}|Charm=Feather A`, 'the pick is matched under the option value it was saved for');
  pass('C · a pick saved for one option value changes only the lines with that value');
}

/* ── D · what is never touched ── */
{
  const w = world(), open = w.sheet('GF Sheet 1');
  const mk = (rid, tid, state) => { const r = rowOf(rid, tid, null, state); read(r, BEFORE); w.put(r, 'FEATHER', open); if (state) r.state = state; return r; };
  const skipped = mk('4400000001', '41', 'skipped'), noDesign = mk('4400000002', '42', 'noDesign'), held = mk('4400000003', '43'), asks = mk('4400000004', '44'), unknown = mk('4400000005', '45');
  held.hold = 'held by Ann'; w.pool.get(held.poolIds[0]).state = 'ready';
  w.pool.delete(unknown.poolIds[0]);
  const rows = [skipped, noDesign, held, asks, unknown];
  const snap = L.snapshot(rows, BEFORE, AFTER, (r, m) => RL.linkOf(r, m)); for (const r of rows) read(r, AFTER);
  asks.spec.problems = [{ kind: 'needsMaterial' }];
  const out = {}; for (const e of snap) out[e.key] = L.classify(e, w.h(AFTER));
  eq([out[skipped.key].action, out[noDesign.key].action], ['skip', 'skip'], 'a skipped line and a no-design line are left as they are');
  eq([out[held.key].action, /held by a person/.test(out[held.key].why)], ['skip', true], 'a line a person holds with its piece still live is left to that person');
  eq([out[asks.key].action, /question/.test(out[asks.key].why)], ['skip', true], 'a line that still asks a question is left to its question');
  eq([out[unknown.key].action, out[unknown.key].retry], ['skip', true], 'pieces the page has not read yet: asked again later, never guessed');
  // a held line whose pieces were taken off (abandoned): nothing to take off, it reads the pick and is made with it when released
  const heldOff = mk('4400000006', '46'); heldOff.hold = 'held'; w.pool.get(heldOff.poolIds[0]).state = 'abandoned';
  const s2 = L.snapshot([heldOff], BEFORE, AFTER, (r, m) => RL.linkOf(r, m)); read(heldOff, AFTER);
  eq(L.classify(s2[0], w.h(AFTER)).action, 'design', 'a held line whose pieces were taken off reads the new charm (it is made with it when released)');
  pass('D · skipped, no design, held, asking and not-yet-read lines are never changed');
}

console.log(`listing-pick-relink: ${n} checks passed`);
