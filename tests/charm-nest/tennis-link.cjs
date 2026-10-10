// TENNISLINK (Paul, 10 Oct 2026, 16:01 UTC): orders 4171010675 (Silver, 8.5mm) and 4173373368 (14k Solid Gold, 11mm), SKU "Huggie Hoops-Tennis Ball/Racket3", Etsy listing 1744372161,
// title "Mismatched Tennis Ball and Raquet Huggie Hoops Charm…": "These should not be failing to pass through since these are mismatched hoop earrings. We have both charms in our repository
// so I need you to link those charms to this listing specifically, one is for the left and one is for the right, so there's no duplicating."
// What this checks: the app's own saved answer for a listing and SKU can now name TWO master designs, the Left and the Right (aliasPut with pair {L, R} → pairBySku on the listing's
// Charm_Sku_Aliases record; aliasGet hands it to the sorter, the sandbox included; charm-nest-orders.js pairMembers reads it before any reading of words).
//   node tests/charm-nest/tennis-link.cjs        (the browser part needs playwright-core and chrome, as pair-rows-lists.cjs; skipped without)
// Offline: an in-memory Firestore that refuses nested arrays (_sandboxFakes.cjs), the real Library function, the real code of the page, the two real master designs (their index entries as
// read live on 10 Oct 2026 and their real .ai files), the two real order lines. No network, no Etsy, nothing live.
//  A · the store: one record, a map (never an array), the sandbox's own copy apart from production's, both designs must be in the master, nothing else is written
//  B · the reading: Ball Left + Racket Right, 2 pieces per unit, no question; this listing and this SKU only; a saved link beats the words; a missing design never gets a look-alike
//  C · the page (real Chromium): Orders row, Review card and the order window draw BOTH designs side by side, "Vector design" never reads "Unavailable · Retry"
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const Fk = require('./_sandboxFakes.cjs'); Fk.install();
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const O = require(path.join(root, 'charm-nest-orders.js'));
const lib = require(path.join(root, 'netlify/functions/charmNestLibrary.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

const BALL = 'TENNIS BALL (HUGGIE)', RACKET = 'TENNIS RACKET (HUGGIE)', NECK_RACKET = 'TENNIS RACKET';
const LID = '1744372161', RAW = 'HUGGIE HOOPS-TENNIS BALL/RACKET3';
const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tennis-master-entries.json'), 'utf8')).entries;
for (const e of masters) Fk.store.set('Charm_Master_Index/' + e.sku, JSON.parse(JSON.stringify(e)));
const call = async body => { const r = await lib.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return { status: r.statusCode, body: JSON.parse(r.body) }; };
const fromQuery = async q => { const r = await lib.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: q }); return { status: r.statusCode, body: JSON.parse(r.body) }; };   // (how a read-only check reaches the live function)

(async () => {
  /* ── A · the store ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  eq((await fromQuery({ op: 'aliasGet' })).body.aliases, {}, 'production starts with no saved alias (as read live at 16:07 UTC: 0 documents)');
  Fk.writes.length = 0;
  const put = await call({ op: 'aliasPut', listingId: LID, fromSku: RAW, pair: { L: BALL, R: RACKET }, by: 'TENNISLINK', title: 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm' });
  eq([put.status, put.body], [200, { ok: true }], 'the pair is saved');
  eq(Fk.writes.map(w => w.kind + ' ' + w.path), ['set Charm_Sku_Aliases/' + LID], 'exactly ONE record is written, the listing\'s own (no sandbox copy, no option map, no master change, no other document)');
  const doc = Fk.store.get('Charm_Sku_Aliases/' + LID);
  eq(Object.keys(doc).sort(), ['by', 'listingId', 'pairBySku', 'title', 'updatedAt'], 'the record holds the answer and who gave it, nothing else');
  eq([doc.listingId, doc.by, Object.keys(doc.pairBySku)], [LID, 'TENNISLINK', [RAW]], 'for this listing, under the SKU its lines carry');
  const link = doc.pairBySku[RAW]; eq([link.L, link.R, typeof link.at, Object.keys(link).sort()], [BALL, RACKET, 'number', ['L', 'R', 'at', 'by']], 'the Left design and the Right design, each once, a map and no array');
  refuseNestedArrays(doc, 'record');   // (Firestore refuses an array inside an array: there is no array at all)
  ok(!JSON.stringify(doc).includes('['), 'no array anywhere in the record');
  ok(!('sku' in doc) && !('bySku' in doc) && !('v' in doc), 'it is not a one-design alias: no sku, no bySku (an older page reads nothing from it and so changes nothing)');
  const got = (await fromQuery({ op: 'aliasGet' })).body;
  eq(got.aliases[LID].pairBySku[RAW].L, BALL, 'aliasGet hands it to the sorter'); eq(Object.keys(got.aliases), [LID], 'and nothing else');
  // a second SKU of the same listing, and an ordinary one-design alias, join the record (they never replace it)
  await call({ op: 'aliasPut', listingId: LID, fromSku: 'HUGGIE HOOPS-OTHER', pair: { L: RACKET, R: BALL }, by: 'TENNISLINK' });
  await call({ op: 'aliasPut', listingId: LID, fromSku: 'SOME OTHER SKU', sku: BALL, by: 'TENNISLINK' });
  const joined = Fk.store.get('Charm_Sku_Aliases/' + LID);
  eq(Object.keys(joined.pairBySku).sort(), ['HUGGIE HOOPS-OTHER', RAW].sort(), 'a second pair for another SKU joins the first'); eq(joined.pairBySku[RAW].L, BALL, 'the first is untouched'); eq(joined.bySku, { 'SOME OTHER SKU': BALL }, 'a one-design alias joins too');
  Fk.store.delete('Charm_Sku_Aliases/' + LID);   // (back to the single answer, for the rest)
  await call({ op: 'aliasPut', listingId: LID, fromSku: RAW, pair: { L: BALL, R: RACKET }, by: 'TENNISLINK' });
  // the refusals write nothing
  Fk.writes.length = 0;
  const refuse = async (body, re, why) => { const r = await call(Object.assign({ op: 'aliasPut', listingId: LID, fromSku: RAW, by: 'x' }, body)); ok(r.status === 400 && re.test(r.body.error), why + ': ' + JSON.stringify(r.body)); };
  await refuse({ pair: { L: BALL, R: BALL } }, /two different/, 'the same design twice (no duplicating)');
  await refuse({ pair: { L: BALL, R: 'TENNIS RACKET (HUGGIES)' } }, /not in the master index/, 'a design that is not in the master (a look-alike name)');
  await refuse({ pair: { L: BALL } }, /two different/, 'one design only');
  await refuse({ pair: { L: BALL, R: RACKET }, fromSku: '' }, /listingId and fromSku/, 'no SKU to key it by');
  await refuse({ pair: { L: BALL, R: RACKET }, listingId: '' }, /listingId and fromSku/, 'no listing');
  await refuse({ pair: { L: BALL, R: ['TENNIS', 'RACKET'] } }, /two different|not in the master/, 'an array for a design');
  eq(Fk.writes, [], 'a refused pair writes nothing');
  // the sandbox: its answers are its own copy; it READS production's (so a sandbox wipe never removes the production link and real orders use it)
  Fk.writes.length = 0;
  await call({ op: 'aliasPut', sandbox: true, listingId: LID, fromSku: 'SANDBOX ONLY', sku: BALL, by: 's' });
  eq(Fk.writes.map(w => w.path), ['Sandbox_Charm_Sku_Aliases/' + LID], 'a sandbox answer is written to the sandbox copy only');
  const sbx = (await call({ op: 'aliasGet', sandbox: true })).body.aliases[LID];
  eq([sbx.pairBySku[RAW].R, sbx.bySku], [RACKET, { 'SANDBOX ONLY': BALL }], 'the sandbox reads production\'s pair together with its own one-design answer');
  await call({ op: 'aliasPut', sandbox: true, listingId: LID, fromSku: 'SANDBOX PAIR', pair: { L: RACKET, R: BALL }, by: 's' });
  const sbx2 = (await call({ op: 'aliasGet', sandbox: true })).body.aliases[LID];
  eq(Object.keys(sbx2.pairBySku).sort(), [RAW, 'SANDBOX PAIR'].sort(), 'and a sandbox pair joins production\'s, it does not replace it');
  eq(Object.keys((await call({ op: 'aliasGet' })).body.aliases[LID].pairBySku), [RAW], 'production never sees the sandbox pair');
  pass('A · the store: one record, a map, only for this listing and SKU, two different master designs, the sandbox reads production\'s');

  /* ── B · the reading ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const names = masters.map(e => e.sku).concat(['STAR', 'MOON', 'SPORTS 15 - PIN PONG RACKET']);
  const mk = list => { const m = new Map(list.map(k => [k, {}])), loose = new Map(); for (const k of m.keys()) { const key = O.looseKey(k); loose.set(key, loose.has(key) ? '' : k); } return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' }; };
  const aliases = (await call({ op: 'aliasGet' })).body.aliases;   // exactly what the page reads
  const ctx = (extra, list) => Object.assign({ optionMaps: {}, aliases, noDesign: { patterns: [], skus: [] } }, mk(list || names), extra);
  const V = (name, value, ids) => Object.assign({ name, value }, ids ? { propertyId: ids[0], valueId: ids[1] } : {});
  const TITLE = 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Sports Jewelry';
  const REAL = {
    4171010675: { order: { receiptId: '4171010675', updateTs: 1 }, line: { transactionId: '5213678588', listingId: LID, productId: '27738389707', sku: 'Huggie Hoops-Tennis Ball/Racket3', title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '', variations: [V('METAL CHOICE', 'Silver', ['514', '55196991045']), V('HOOP SIZE', '8.5mm', ['513', '110638330075'])] } },
    4173373368: { order: { receiptId: '4173373368', updateTs: 1 }, line: { transactionId: '5215153110', listingId: LID, productId: '28104491116', sku: 'Huggie Hoops-Tennis Ball/Racket3', title: TITLE, quantity: 1, metalKey: '14k', metalLabel: '14K Gold', personalization: [], buyerMessage: '', variations: [V('METAL CHOICE', '14k Solid Gold', ['514', '148615583071']), V('HOOP SIZE', '11mm', ['513', '65252824563'])] } }
  };
  const read = (rid, over, c, ord) => { const r = REAL[rid], line = Object.assign({}, r.line, over || {}); return { line, spec: O.interpretLine(Object.assign({}, r.order, ord || {}), line, c || ctx()) }; };
  const members = sp => (sp.pair.members || []).map(m => m.side + ':' + m.sku);
  const kinds = sp => sp.problems.map(p => p.kind);
  for (const rid of ['4171010675', '4173373368']) {
    const { spec: sp } = read(rid);
    eq(members(sp), ['L:' + BALL, 'R:' + RACKET], rid + ': the Left is the Tennis Ball, the Right the Tennis Racket');
    eq([sp.pair.source, sp.pair.mismatched, sp.pair.earring, sp.designSku], ['alias', true, true, BALL], rid + ': read from the saved link, a mismatched earring pair, pooled from its Left design first');
    eq(sp.problems, [], rid + ': no question, no unmatched SKU, no "name the second"'); eq([sp.pieceCount, sp.pair.sides.join(''), sp.pair.kind], [2, 'LR', 'mismatched'], rid + ': 2 pieces, a Left and a Right');
    ok(!sp.pair.second, rid + ': nothing waits for a second design'); ok(sp.pair.notes.some(t => /saved for this listing and SKU/.test(t)), rid + ': the order window says where it comes from: ' + sp.pair.notes.join(' | '));
    ok(sp.size === '8.5MM' || sp.size === '11MM', rid + ': the hoop size is still read (' + sp.size + ')');
    const pcs = O.piecesOf(sp); eq(pcs.map(p => p.side), ['L', 'R'], rid + ': two pieces, Left then Right'); eq(pcs.map(p => p.bodyIndex), [0, 1], rid + ': each its own design (bodies 0 and 1)');
    eq(new Set(pcs.map(p => p.unit)).size, 1, rid + ': one unit, one order line: its two pieces belong together');
  }
  { const two = read('4173373368', { quantity: 2 }).spec; eq([two.pieceCount, two.pair.sides.join('')], [4, 'LRLR'], 'quantity 2: two of each side'); eq(O.piecesOf(two).map(p => p.bodyIndex), [0, 1, 0, 1], 'Ball Left, Racket Right, twice: neither design duplicated onto the other ear'); }
  pass('B1 · both real orders read as 2 pieces, Tennis Ball Left + Tennis Racket Right, with no question');

  // the link decides, not the words: a line whose SKU and title say nothing of tennis (the same listing, the same SKU) still reads as the pair
  { const s = read('4171010675', { title: 'Sports Huggie Hoops Charm', sku: RAW.toLowerCase() }, ctx({ aliases: { [LID]: { pairBySku: { [RAW]: { L: BALL, R: RACKET } } } } }), null).spec;
    eq([members(s), s.pair.source, kinds(s)], [['L:' + BALL, 'R:' + RACKET], 'alias', []], 'the saved pair is read whatever the words say (the SKU is matched as upper case, as every SKU is)');
    // swapped words: the saved Left/Right win over the order the title names them in
    const sw = read('4171010675', { title: 'Mismatched Tennis Racket and Tennis Ball Huggie Hoops', sku: 'Huggie Hoops-Racket/Tennis Ball' }, ctx({ aliases: { [LID]: { pairBySku: { 'HUGGIE HOOPS-RACKET/TENNIS BALL': { L: BALL, R: RACKET } } } } })).spec;
    eq(members(sw), ['L:' + BALL, 'R:' + RACKET], 'a person\'s saved Left and Right beat the order the SKU names them in'); }
  // only this listing and only this SKU
  { const other = read('4171010675', { listingId: '999000111' }).spec;   // the same SKU and title on ANOTHER listing: no saved link there
    ok(other.pair.source !== 'alias', 'another listing with the same SKU is not read from this listing\'s link (source ' + other.pair.source + ')');
    const sku = read('4171010675', { sku: 'Huggie Hoops-Tennis Ball/Racket4' }).spec; ok(sku.pair.source !== 'alias', 'another SKU of this listing is not read from the link (source ' + sku.pair.source + ')');
    // a similar title on a listing that is not linked still asks, for designs the master does not hold: no look-alike, no general "Mismatched A and B" rule
    const similar = read('4171010675', { listingId: '999000222', sku: 'Huggie Hoops-Tennis Ball/Zebra', title: 'Mismatched Tennis Ball and Zebra Huggie Hoops Charm' }).spec, q = similar.problems.find(p => p.pairSecond);
    ok(!similar.pair.mismatched && q, 'a similar title on another listing, with a design the master does not hold, still asks'); ok(/no single master design reads as “Zebra”/.test(q.pairSecond.why), 'in one plain line: ' + q.pairSecond.why);
    const stars = read('4171010675', { listingId: '999000333', sku: 'Huggie Hoops-Star/Moon', title: 'Mismatched Star and Moon Huggie Hoops Charm' }).spec;
    ok(!stars.pair.mismatched && stars.problems.some(p => p.pairSecond), 'two designs the master holds only as necklace-size charms (STAR, MOON): never a huggie, so still asks'); }
  // a design that left the master is never replaced by a look-alike: the line says so
  { const noRacket = ctx({}, names.filter(k => k !== RACKET)), sp = read('4171010675', null, noRacket).spec, q = sp.problems.find(p => p.pairSecond);
    ok(!sp.pair.mismatched && !sp.pair.members, 'the saved Right design is not in the master: no pair is made'); ok(q && /pair saved for this listing and SKU names TENNIS BALL \(HUGGIE\) \(Left\) and TENNIS RACKET \(HUGGIE\) \(Right\), but TENNIS RACKET \(HUGGIE\) is not in any master file/.test(q.pairSecond.why), 'the line says which: ' + (q && q.pairSecond.why));
    ok(!JSON.stringify(sp.pair.members || []).includes(NECK_RACKET + '"'), 'the necklace-size TENNIS RACKET never stands in'); }
  // a person's "the same on both ears" for ONE order is the last word (it is that order's own answer)
  { const same = read('4171010675', null, ctx({ optionMaps: { [LID]: { [O.norm('Two designs on this line')]: { [O.norm('4171010675/5213678588')]: { field: 'ignore', value: null } } } } })).spec;
    ok(!same.pair.mismatched && same.pair.second && same.pair.second.answered === 'same', 'a person said "the same on both ears" for that one order: it is read as that');
    ok(read('4173373368', null, ctx({ optionMaps: { [LID]: { [O.norm('Two designs on this line')]: { [O.norm('4171010675/5213678588')]: { field: 'ignore', value: null } } } } })).spec.pair.source === 'alias', 'and the other order keeps the link'); }
  // a line that is not sold as earrings is never split into ears by a link (nothing changes for a necklace)
  { const neck = read('4171010675', { title: 'Tennis Ball Racket Charm Necklace', variations: [V('Metal Choice', 'Silver')] }, ctx({ aliases: { [LID]: { pairBySku: { 'HUGGIE HOOPS-TENNIS BALL/RACKET3': { L: BALL, R: RACKET } } } } }), null).spec;
    ok(!neck.pair.earring && neck.pieceCount === 1, 'a necklace line is one piece, no Left and Right (' + neck.pieceCount + ')'); }
  pass('B2 · the link is this listing\'s and this SKU\'s only, beats the words, never takes a look-alike, a similar title elsewhere still asks, a person\'s one-order answer stands');

  // the old one-design alias path is unchanged
  { const one = read('4171010675', { sku: 'WHATEVER-1', title: 'Huggie Hoops Charm' }, ctx({ aliases: { [LID]: { bySku: { 'WHATEVER-1': BALL } } } })).spec;
    eq([one.designSku, one.pair.source, one.pair.mismatched], [BALL, null, false], 'a one-design alias still resolves one design (a matching pair)'); }
  pass('B3 · the one-design alias reads exactly as before');

  /* ── C · the page ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  // (its own process: the page's fake site runs the real functions over its own in-memory Firestore, not the one above)
  const r = require('child_process').spawnSync(process.execPath, [path.join(__dirname, 'tennis-link-browser.cjs')].concat(process.argv.slice(2)), { stdio: 'inherit', env: process.env });
  if (r.status !== 0) { console.error('the browser part failed'); process.exit(r.status || 1); }
  console.log(`tennis-link: ${n} checks passed (A and B), part C above`);
})().catch(e => { console.error(e); process.exit(1); });
