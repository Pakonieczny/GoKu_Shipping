// SCHROLINK (Paul, 12 Oct 2026, order 4198409468): a NECKLACE of two cat charms, Etsy listing 1648594139, SKU "SCHRODINGER CAT". The sorter matched the SKU to the one master design of that name (the HUGGIE skull cat)
// and made ONE piece. Paul: "This order is a dual piece/charm necklace ... Since this is a necklace piece how did this get past your safeguards? Associate this etsy listing with both of the charms ...
// SCHRODINGER CAT 1 and SCHRODINGER CAT 2 ... the most effective and long lasting approach."
// What this checks: the saved link (aliasPut with pair {L, R} → Charm_Sku_Aliases/{listing}.pairBySku, the way TENNISLINK saved the tennis ball / racket) on a line that is NOT sold as earrings is a necklace of
// TWO DIFFERENT charms: two pieces per unit, one of each design, one group, no Left or Right, nothing turned over. An earring line with the same link is still the mismatched pair it always was.
//   node tests/charm-nest/schro-link.cjs
// Offline: an in-memory Firestore that refuses nested arrays (_sandboxFakes.cjs), the real Library function, the real code of the page, the real master entries (read live 12 Oct 2026), the real order line.
//  A · the store: one record, a map, only this listing and SKU, nothing else written
//  B · the reading: SCHRODINGER CAT 1 + SCHRODINGER CAT 2, 2 pieces per unit, no question, not the huggie; another listing with the same SKU unaffected; earrings unchanged; a missing design is never replaced
//  C · the pieces: one of each design, one unit, one group, no side
//  E · the page (real Chromium, skipped without playwright-core): both cats side by side with no chips, a picture per piece, no Review row, the Order changed row's Accept button
//  D · the Pool (the bridge's real source in a vm): two pool rows, one per design, one group, no side or mirror; the unlinked line is still one piece; a line pooled before the link is OFFERED the new
//      reading as the Review "Order changed" card (never on a released or cut sheet) and Accept makes it up again
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const O = require(path.join(root, 'charm-nest-orders.js'));
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

const CAT = 'SCHRODINGER CAT', CAT1 = 'SCHRODINGER CAT 1', CAT2 = 'SCHRODINGER CAT 2', HUG1 = 'SCHRODINGER CAT 1 (HUGGIE)', HUG2 = 'SCHRODINGER CAT 2 (HUGGIE)';
const LID = '1648594139', RAW = 'SCHRODINGER CAT';
const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/schro-master-entries.json'), 'utf8')).entries;
for (const e of masters) Fk.store.set('Charm_Master_Index/' + e.sku, JSON.parse(JSON.stringify(e)));
const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const fromQuery = async q => { const r = await lib.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: q }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const TITLE = "Schrödinger's Cat Necklace, Whimsical Cat Gift, Fun Jewelry Gift, Birthday Gift, Cat Necklace Cute Jewelry, Dead and Alive Cat Necklace";

(async () => {
  /* ── A · the store ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  eq((await fromQuery({ op: 'aliasGet' })).body.aliases, {}, 'production starts with no saved alias for this listing');
  Fk.writes.length = 0;
  const put = await call({ op: 'aliasPut', listingId: LID, fromSku: RAW, pair: { L: CAT1, R: CAT2 }, by: 'SCHROLINK', title: TITLE });
  eq([put.status, put.body], [200, { ok: true, verified: true, kind: 'pair', listingId: LID, key: RAW, sku: CAT1 + ' + ' + CAT2 }], 'the pair is saved and read back (REVLINK)');
  eq(Fk.writes.map(w => w.kind + ' ' + w.path), ['set Charm_Sku_Aliases/' + LID], 'exactly ONE record is written, the listing\'s own');
  const doc = Fk.store.get('Charm_Sku_Aliases/' + LID);
  eq(Object.keys(doc).sort(), ['by', 'listingId', 'pairBySku', 'title', 'updatedAt'], 'the record holds the answer and who gave it, nothing else');
  eq([doc.listingId, Object.keys(doc.pairBySku)], [LID, [RAW]], 'for this listing, under the SKU its line carries');
  eq([doc.pairBySku[RAW].L, doc.pairBySku[RAW].R], [CAT1, CAT2], 'the two master designs');
  refuseNestedArrays(doc, 'record'); ok(!JSON.stringify(doc).includes('['), 'no array anywhere in the record');
  Fk.writes.length = 0;
  for (const [pair, re, why] of [[{ L: CAT1, R: CAT1 }, /two different/, 'the same design twice'], [{ L: CAT1, R: 'SCHRODINGER CAT 3' }, /not in the master index/, 'a design that is not in the master']]) {
    const r = await call({ op: 'aliasPut', listingId: LID, fromSku: RAW, by: 'x', pair }); ok(r.status === 400 && re.test(r.body.error), why + ': ' + JSON.stringify(r.body));
  }
  eq(Fk.writes, [], 'a refused pair writes nothing');
  pass('A · the store: one record, a map, only for this listing and SKU, two different master designs');

  /* ── B · the reading ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const aliases = (await call({ op: 'aliasGet' })).body.aliases;   // exactly what the page reads
  const mk = list => { const m = new Map(list.map(e => [e.sku.toUpperCase(), e])), loose = new Map(); for (const k of m.keys()) { const key = O.looseKey(k); loose.set(key, loose.has(key) ? '' : k); } return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' }; };
  const ctx = (extra, list) => Object.assign({ optionMaps: {}, aliases, noDesign: { patterns: [], skus: [] } }, mk(list || masters), extra);
  const V = (name, value) => ({ name, value });
  const ORDER = { receiptId: '4198409468', updateTs: 1791826702 };
  const lineOf = over => Object.assign({ transactionId: '5247566510', listingId: LID, sku: 'SCHRODINGER CAT', title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: ['I did not select + engraving'], variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '18"'), V('Personalization', 'I did not select + engraving')] }, over || {});
  const read = (over, c, ord) => { const line = lineOf(over); return { line, spec: O.interpretLine(Object.assign({}, ORDER, ord || {}), line, c || ctx()) }; };

  // the order as it is today, with no link: the exact-SKU master, the huggie-sized skull cat, one piece (the defect)
  { const sp = read({}, ctx({ aliases: {} })).spec; eq([sp.designSku, sp.pieceCount, !!sp.pair.set], [CAT, 1, false], 'without the link: one piece of the SKU\'s own master design (what Paul saw)'); }
  // the order with the link
  { const { spec: sp } = read();
    eq([sp.designSku, sp.pieceCount, sp.pair.perUnit], [CAT1, 2, 2], 'with the link: the first design leads, 2 pieces per unit');
    eq([!!sp.pair.set, sp.pair.set && sp.pair.set.designs, sp.pair.set && sp.pair.set.source], [true, [CAT1, CAT2], 'alias'], 'a set of TWO saved charms, CAT 1 and CAT 2, from the saved link');
    eq([sp.pair.earring, sp.pair.mismatched, sp.pair.single, sp.pair.glued], [false, false, false, false], 'not an earring, not a mismatched pair, not glued');
    eq(sp.pair.members, null, 'no Left and no Right design anywhere in it');
    eq(sp.pair.sides, [null, null], 'no side on either piece'); eq(sp.problems, [], 'no question, no unmatched SKU, nothing for Review');
    ok(sp.pair.notes.some(t => /two designs a person saved for this listing and SKU, SCHRODINGER CAT 1 and SCHRODINGER CAT 2/.test(t) && /no Left or Right, nothing mirrored/.test(t)), 'the order window says where it comes from: ' + sp.pair.notes.join(' | '));
    eq(O.setDesignsOf(sp), [CAT1, CAT2], 'the module hands the two designs to the pool and the pictures');
    ok(!JSON.stringify([sp.designSku, sp.pair.set]).includes('HUGGIE'), 'never a huggie design'); }
  // the link is this listing's and this SKU's only
  { const other = read({ listingId: '999000111' }).spec; eq([other.designSku, other.pieceCount, !!other.pair.set], [CAT, 1, false], 'another listing with the same SKU: not read from this link');
    const sku = read({ sku: 'SCHRODINGER CAT BIG' }).spec; ok(!sku.pair.set, 'another SKU of this listing: not read from the link');
    const low = read({ sku: 'schrodinger cat' }).spec; eq(low.pair.set && low.pair.set.designs, [CAT1, CAT2], 'the SKU is matched as upper case, as every SKU is'); }
  // quantity 2: two of each
  { const sp = read({ quantity: 2 }).spec; eq([sp.pieceCount, sp.pair.perUnit], [4, 2], 'quantity 2: 4 pieces'); }
  // an earring line with the same link is the mismatched earring pair it always was (the TENNISLINK path)
  { const ears = read({ title: 'Schrodinger Cat Stud Earrings', variations: [V('Metal Choice', 'Silver')] }).spec;
    ok(ears.pair.earring && ears.pair.mismatched && !ears.pair.set, 'earrings: still a Left and a Right (' + JSON.stringify([ears.pair.earring, ears.pair.mismatched, ears.pair.set]) + ')');
    eq((ears.pair.members || []).map(m => m.side + ':' + m.sku), ['L:' + CAT1, 'R:' + CAT2], 'CAT 1 Left, CAT 2 Right'); eq(ears.pieceCount, 2, 'two pieces'); }
  // a design that left the master is never replaced by a look-alike, and the line never falls back in silence to one piece of the SKU's own design: it waits for a person, with the reason
  for (const gone of [CAT2, CAT1]) {
    const sp = read({}, ctx({}, masters.filter(e => e.sku !== gone))).spec, q = sp.problems.find(p => p.kind === 'unmatchedSku' && p.savedPair);
    ok(!sp.pair.set, gone + ' is not in the master: no set is made');
    ok(q && q.listingId === LID && new RegExp(gone + ' is not in any master file').test(q.reason) && /never made as one piece of SCHRODINGER CAT/.test(q.reason), 'the line waits and says why: ' + (q && q.reason));
    ok(!JSON.stringify(sp.pair).includes('HUGGIE') && !sp.designSku.includes('HUGGIE'), 'never a huggie look-alike'); }
  { const noCat2 = ctx({}, masters.filter(e => e.sku !== CAT2)), ears = read({ title: 'Mismatched Schrodinger Cat Stud Earrings', variations: [V('Metal Choice', 'Silver')] }, noCat2).spec;   // an earring line that says two designs keeps its own question (TENNISLINK)
    ok(ears.problems.some(p => p.pairSecond) && !ears.problems.some(p => p.savedPair), 'an earring line that says two designs asks its own question, once');
    const plain = read({ title: 'Schrodinger Cat Stud Earrings', variations: [V('Metal Choice', 'Silver')] }, noCat2).spec;
    ok(plain.problems.some(p => p.savedPair), 'an earring line that does not say it waits too: never two of the SKU\'s own design in silence'); }
  // a person's "the same design on both" for ONE order is that order's own last word (it is not lost)
  { const same = read({}, ctx({ optionMaps: { [LID]: { [O.norm('Two designs on this line')]: { [O.norm('4198409468/5247566510')]: { field: 'ignore', value: null } } } } })).spec;
    eq([!!same.pair.set, same.designSku, same.pieceCount, same.problems], [false, CAT, 1, []], 'a person said "the same on both" for that one order: it is read as that, with no question'); }
  // the old one-design alias path is unchanged
  { const one = read({ sku: 'WHATEVER-1' }, ctx({ aliases: { [LID]: { bySku: { 'WHATEVER-1': CAT1 } } } })).spec; eq([one.designSku, !!one.pair.set, one.pieceCount], [CAT1, false, 1], 'a one-design alias still resolves one design'); }
  pass('B · two pieces CAT 1 + CAT 2, no question, never the huggie; another listing / SKU unaffected; earrings unchanged; a missing design never replaced');

  /* ── C · the pieces ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  { const sp = read().spec, pcs = O.piecesOf(sp);
    eq(pcs.map(p => p.sku), [CAT1, CAT2], 'piece 1 is the first design, piece 2 the second'); eq(pcs.map(p => p.side), [null, null], 'no side');
    eq(pcs.map(p => p.bodyIndex), [0, 0], 'each design\'s own body, as drawn'); eq(new Set(pcs.map(p => p.unit)).size, 1, 'one unit: the two pieces belong together'); }
  { const sp = read({ quantity: 2 }).spec, pcs = O.piecesOf(sp);
    eq(pcs.map(p => p.sku), [CAT1, CAT2, CAT1, CAT2], 'quantity 2: CAT 1, CAT 2, CAT 1, CAT 2'); eq(pcs.map(p => p.unit), [1, 1, 2, 2], 'two units of two'); }
  // a plain necklace piece list is unchanged (no sku on a piece)
  { const sp = read({ listingId: '999000111' }).spec, pcs = O.piecesOf(sp); eq(pcs.map(p => 'sku' in p), [false], 'an ordinary line has no per-piece design'); }
  pass('C · the pieces: one of each design, one unit per necklace, no side');

  /* ── D · the Pool ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const F = require('./pairs-fixtures.cjs');
  const keyWorld = require('./_keyWorld.cjs')(root);
  const { DESIGNS, world, lineRow, Pair } = keyWorld;
  DESIGNS[CAT] = () => F.charmOf('PAIR-STUD'); DESIGNS[CAT1] = () => F.charmOf('ONE-PENDANT'); DESIGNS[CAT2] = () => F.charmOf('PAIR-HOOP');   // (three different one-body designs: by size they have three different hashes)
  const worldOf = () => {
    const w = world([CAT1, CAT2, CAT]), reviews = [];
    w.c.P.parseSource = async (_b, name) => ({ name });
    w.c.P.groupCharmsAsync = async parsed => ({ charms: [structuredClone(DESIGNS[String(parsed.name).replace(/\.ai$/, '')]())], orphans: [] });
    w.c.Review = { add: it => reviews.push(it) }; w.c.B.review = { items: reviews }; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false };
    return Object.assign(w, { reviews });
  };
  const rowOf = (over, qty) => { const { line, spec } = read(Object.assign({ quantity: qty || 1 }, over || {})); spec.material = 'gold'; return Object.assign(lineRow('4198409468', '5247566510', spec.designSku, qty || 1, {}), { spec, line: { transactionId: line.transactionId, listingId: line.listingId, sku: line.sku, title: line.title } }); };
  for (const qty of [1, 2]) {
    const w = worldOf(), row = rowOf({}, qty);
    await w.Pool.poolAdd(row, null);
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms;
    eq(pools.map(p => p.sku), qty === 1 ? [CAT1, CAT2] : [CAT1, CAT2, CAT1, CAT2], `×${qty}: one piece of each design per unit, in order`);
    eq([row.state, charms.length, pools.length, w.Pool.pieceCountOf(row)], ['pooled', 2 * qty, 2 * qty, 2 * qty], 'pooled whole, counted as ' + 2 * qty);
    ok(!pools.some(p => 'side' in p || 'mirror' in p || 'bodyIndex' in p), 'no side, no mirror, no body number: nothing is turned over');
    ok(!pools.some(p => p.sku === CAT || /HUGGIE/.test(p.sku)), 'neither the SKU\'s own master design nor a huggie');
    eq(new Set(pools.map(p => p.groupKey)).size, 1, 'ONE group'); ok(pools[0].groupKey, 'with a group key');
    ok(pools.every(p => p.groupSize === 2 * qty), 'the group is every piece of the line (' + 2 * qty + ')');
    ok(Pair.mustShareSheet(pools[0], pools[1]), 'the pieces must share a sheet');
    const h = [...new Set(pools.map(p => p.charmHash))]; eq(h.length, 2, 'two different charms'); ok(pools[0].charmHash === charms[0].hash && pools[1].charmHash === charms[1].hash, 'each piece\'s hash is its own design\'s');
    ok(charms[0].name.includes(CAT1) && charms[1].name.includes(CAT2), 'named by the design it is cut from');
    ok(charms.every(c => !('side' in c) && !c.mirror), 'no sheet charm is turned over');
    ok(charms[0].bbox.join() !== charms[1].bbox.join(), 'cut from two different drawings');
    ok(!!row.spec.pair.set, 'the line keeps its set reading');
  }
  { const w = worldOf(), row = rowOf({ listingId: '999000111' }); await w.Pool.poolAdd(row, null);   // the same SKU on a listing nobody linked: what it always made
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools; eq(pools.map(p => p.sku), [CAT], 'an unlinked listing with the same SKU is still ONE piece'); }
  pass('D1 · the Pool: one piece of each design per unit, one group on one sheet, no side, nothing turned over; an unlinked listing is untouched');

  // a line pooled BEFORE the link (one piece of the SKU's own design): the Review "Order changed" card offers the new reading; nothing is written by the offer
  const pooledBefore = (w, row, extra) => { const id = '4198409468_5247566510_1'; row.poolIds = [id]; w.c.B.pool.rows.set(id, Object.assign({ poolId: id, state: 'ready', sku: CAT, copy: 1, quantity: 1, charmHash: 'x' }, extra || {})); return id; };
  { const w = worldOf(), row = rowOf({}, 1), id = pooledBefore(w, row);
    w.Pool.pinPooled(row);
    eq([row.changePending, row.repoolChanged, row.spec.pieceCount, row.poolIds], [true, true, 2, [id]], 'the line is flagged to be made up again; its count is NOT pinned down to the one piece it had');
    eq(w.reviews.length, 1, 'one card'); const it = w.reviews[0];
    eq([it.kind, it.key, it.row === row, it.old.spec.designSku], ['orderChanged', 'chg:' + row.key, true, CAT], 'the Order changed card, for this line, "Was" the one piece of the SKU\'s own design');
    ok(/SCHRODINGER CAT 1 \+ SCHRODINGER CAT 2/.test(it.why) && /Accept the new order/.test(it.why) && /open sheet/.test(it.why) && /Nothing cut or released is touched/.test(it.why), 'in plain words: ' + it.why);
    w.Pool.pinPooled(row); eq(w.reviews.length, 1, 'asked again it adds nothing');
    eq(w.calls.filter(c => c.body && /pool|sheet|alias/i.test(c.body.op || '')).length, 0, 'nothing is written by the offer (no pool, sheet or alias call)'); }
  // never on a sheet that was released or cut; left as it was, said once
  // (INPLACE, Paul 20 Oct 2026, as charm-nest-bridge.js says it: only a sheet the laser has CUT keeps its old piece; a full or released sheet not yet cut gives it up: so the laser-cut sheet, not the released one, is left as it was)
  for (const [what, set] of [['cut by the laser', sh => { sh.laserDoneAt = 1; }], ['recalled', sh => { sh.recalled = true; }], ['cut', sh => { sh.roseCutAt = 1; }]]) {
    const w = worldOf(), row = rowOf({}, 1), id = pooledBefore(w, row); w.pages.gold.charms.push({ poolId: id }); w.pages.gold.fileBase = 'SS Draft 9'; set(w.pages.gold);
    w.Pool.pinPooled(row); w.Pool.pinPooled(row);
    eq([row.changePending || false, row.repoolChanged || false, w.reviews.length], [false, false, 0], `a piece on a sheet ${what} is left as it was: no flag, no card`);
    eq(w.logs.filter(t => /released or cut: left as it was/.test(t)).length, 1, 'the log says so once'); }
  // a draft sheet, a sheet with the piece on it that is open: offered
  { const w = worldOf(), row = rowOf({}, 1), id = pooledBefore(w, row); w.pages.gold.charms.push({ poolId: id }); w.pages.gold.draft = true; w.pages.gold.setId = 's1'; w.c.Gate.modern = () => true;
    w.Pool.pinPooled(row); eq([row.changePending, w.reviews.length], [true, 1], 'a piece on a draft sheet of a modern set is offered the new reading'); }
  // already made as the link says, held, taken off, or not linked: nothing
  { const w = worldOf(), row = rowOf({}, 1); await w.Pool.poolAdd(row, null);
    const rows = w.calls.find(x => x.body.op === 'poolPut').body.pools; for (const p of rows) w.c.B.pool.rows.set(p.poolId, Object.assign({ state: 'ready' }, p));
    w.Pool.pinPooled(row); eq([row.changePending || false, w.reviews.length], [false, 0], 'a line already made as the link says is left alone'); }
  { const w = worldOf(), row = rowOf({}, 1); pooledBefore(w, row); row.hold = 'held by a person'; w.Pool.pinPooled(row); eq(w.reviews.length, 0, 'a held line is not offered anything'); }
  { const w = worldOf(), row = rowOf({ listingId: '999000111' }, 1); pooledBefore(w, row); w.Pool.pinPooled(row); eq([row.changePending || false, w.reviews.length], [false, 0], 'an unlinked listing is never offered a change'); }
  pass('D2 · a line pooled before the link is offered the Order changed card (draft sheets only, once, no writes); released or cut sheets are left as they were');

  /* ── E · the page (its own process: the page's fake site runs the real functions over its own in-memory Firestore, not the one above) ── */
  const r = require('child_process').spawnSync(process.execPath, [path.join(__dirname, 'schro-link-browser.cjs')].concat(process.argv.slice(2)), { stdio: 'inherit', env: process.env });
  if (r.status !== 0) { console.error('the browser part failed'); process.exit(r.status || 1); }
  console.log(`schro-link: ${n} checks passed (A to D), part E above`);
})().catch(e => { console.error(e); process.exit(1); });
