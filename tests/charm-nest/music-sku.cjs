// MUSICSKU (Paul, 11 Oct 2026): "Update the SKU in our Master charm repo to match the Etsy listing. In this case that would mean update "Music 4" to "Music4"."
// Order 4198591262 (a Silver 14" necklace) carries the Etsy SKU "MUSIC4 - SINGLE W/ DOUBLE LINE": the Review row said "SKU MUSIC4 - SINGLE W/ DOUBLE LINE: not in any master file".
// Offline: no Etsy, no live read, no write, no paid call.   node tests/charm-nest/music-sku.cjs
// What it proves (charm-nest-orders.js KNOWN_SKU_NAMES / knownName / namedSku / masterSku / resolveSku / sameDesign; the bridge, the station and the re-read signature read the same table):
//   · ONE definition: a table of one exact SKU, compared as every SKU is (case, spacing and punctuation set aside, nothing fuzzier). NOT a rule "<master SKU> - <words>": the words after
//     " - " name another design more often than not (the master's own pairs: MUSIC3 / MUSIC3 - DOUBLE MUSIC NOTE, APPLE2 / APPLE2 - DISC), so every other SKU of the family still waits
//   · the order reads as the music note, with no question, EXACTLY as the same line carrying the plain SKU does (necklace, Silver, 14", one piece), before the master write ("MUSIC 4")
//     and after it ("MUSIC4"); without the design in the master it still waits "not in any master file"; a person's saved answer still wins
//   · the rename itself: "MUSIC 4" (kept in the master: stored sheets and orders name it) keeps reading as "MUSIC 4"; "MUSIC4" reads as MUSIC4; the huggie set reads as before;
//     a line moving from one name to the other is no change of design (sameDesign: revalidate never raises "SKU changed" for it)
//   · the sweep over every master SKU (when the saved list is on this machine) changes nothing: each reads as itself, no SKU of the master is "named"
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '..', '..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

// ── the master: the designs the cases need, read the way the page reads it (Master.entryFor, Master.looseFor: a spelling is matched only when exactly one master SKU reads that way) ──
const FILE = { 'MUSIC 4': 'charmnest/master/MUSIC_4.ai', 'MUSIC4': 'charmnest/master/MUSIC_4.ai', 'MUSIC 4 (HUGGIE)': 'charmnest/master/MUSIC_4_HUGGIE.ai' };
const master = skus => { const M = new Map(skus.map(s => [s, { sku: s, aiPath: FILE[s] || 'charmnest/master/' + s + '.ai' }])), L = new Map();
  for (const k of M.keys()) { const key = O.looseKey(k); L.set(key, L.has(key) ? '' : k); }
  return { entry: s => M.get(String(s || '').toUpperCase()) || null, loose: s => L.get(O.looseKey(s)) || '' }; };
const TODAY = master(['MUSIC 4', 'MUSIC 4 (HUGGIE)', 'MUSIC 3', 'MUSIC3', 'MUSIC3 - DOUBLE MUSIC NOTE', 'MUSIC 1', 'APPLE2', 'APPLE2 - DISC', 'OWL', 'OWL_40570']);
const AFTER = master(['MUSIC 4', 'MUSIC4', 'MUSIC 4 (HUGGIE)', 'MUSIC 3', 'MUSIC3', 'MUSIC3 - DOUBLE MUSIC NOTE', 'MUSIC 1', 'APPLE2', 'APPLE2 - DISC', 'OWL', 'OWL_40570']);
const WITHOUT = master(['MUSIC 3', 'MUSIC 1']);
const ctx = (m, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: m.entry, masterLoose: m.loose }, extra || {});
const NAMED = 'MUSIC4 - SINGLE W/ DOUBLE LINE', LISTING = '1111111111';
const line = (sku, over) => Object.assign({ transactionId: '5000000001', listingId: LISTING, productId: '', sku, title: 'Music Note Necklace, Eighth Note Pendant, Musician Gift', quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [],
  variations: [{ name: 'Metal Choice', value: 'Silver' }, { name: 'Necklace Length in inches', value: '14"' }] }, over || {});
const read = (sku, m, c, over) => O.interpretLine({ receiptId: '4198591262', updateTs: 0 }, JSON.parse(JSON.stringify(line(sku, over))), c || ctx(m));
const rs = (sku, m, aliases) => O.resolveSku({ sku, listingId: LISTING }, aliases || {}, m.entry, {}, m.loose);
const problems = sp => sp.problems.map(p => p.kind + (p.reason ? ':' + p.reason : ''));
const strip = sp => { const c = JSON.parse(JSON.stringify(sp)); for (const k of ['designSku', 'skuSource', 'boughtSku', 'sources']) delete c[k]; return c; };

// ── 1 · one definition, one exact entry, compared as every SKU is ──
eq(Object.keys(O.KNOWN_SKU_NAMES), [NAMED], 'the table has ONE entry');
eq(O.KNOWN_SKU_NAMES[NAMED], 'MUSIC4', 'it names the design');
ok(Object.isFrozen(O.KNOWN_SKU_NAMES), 'the table cannot be changed at run time');
for (const s of [NAMED, 'music4 - single w/ double line', '  MUSIC4 - SINGLE W/ DOUBLE LINE ', 'Music4-Single W/ Double Line', 'MUSIC4 SINGLE W DOUBLE LINE', 'MUSIC 4 - SINGLE W/ DOUBLE LINE']) eq(O.knownName(s), 'MUSIC4', `${JSON.stringify(s)}: the named SKU, compared as every SKU is`);
for (const s of ['MUSIC4', 'MUSIC 4', 'MUSIC4 - DOUBLE NOTE', 'MUSIC4 - SINGLE', 'MUSIC4 - SINGLE W/ DOUBLE LINES', 'MUSIC4 - SINGLE W/ DOUBLE LINE 2', 'MUSIC3 - SINGLE W/ DOUBLE LINE', 'MUSIC4 - SINGLE W/ DOUBLE LINE-CO', NAMED + ' (HUGGIE)', 'MUSIC', '', null, undefined])
  eq(O.knownName(s), '', `${JSON.stringify(s)}: not the named SKU, nothing fuzzier`);
// the typo table is untouched
eq(Object.keys(O.KNOWN_SKU_TYPOS), ['NITIAL_DISC_4571'], 'the typo table still has its ONE entry');
// only charm-nest-orders.js spells the named SKU (the sorter, the bridge, the station and the search read this one table)
{ const skip = new Set(['node_modules', '.git', 'tests', 'plans', 'public-site', 'cherry-viewer-dist', 'netlify/production-functions']), found = [];
  const walk = d => { for (const f of fs.readdirSync(path.join(root, d), { withFileTypes: true })) { const rel = d ? d + '/' + f.name : f.name;
    if (f.isDirectory()) { if (!skip.has(f.name) && !skip.has(rel) && !['vendor', 'seeds', 'assets', 'docs', 'investor', 'cherry-viewer', 'shopify'].includes(f.name)) walk(rel); }
    else if (/\.(js|cjs|mjs|html)$/.test(f.name) && /MUSIC4[ _.\-]*-[ _.\-]*SINGLE[ _.\-]*W/i.test(fs.readFileSync(path.join(root, rel), 'utf8'))) found.push(rel); } };
  walk(''); eq(found, ['charm-nest-orders.js'], 'only charm-nest-orders.js spells the named SKU'); }

// ── 2 · order 4198591262, the line (a stand-in title; the SKU, the metal and the length are the Review row's) ──
{ const before = JSON.stringify(line(NAMED));
  // the Review row as it was: unknown SKU, no design (the master code of main before this change read the SKU as written: nothing named it)
  const none = read(NAMED, WITHOUT);
  eq(problems(none), ['unmatchedSku:not in any master file'], 'without the design in the master the line waits "not in any master file" (the name stands only for a design the master holds)');
  eq([none.designSku, none.skuSource], [NAMED, 'transaction'], 'and keeps the SKU as Etsy wrote it');
  // today's master ("MUSIC 4"): read as that design, with no question
  const a = read(NAMED, TODAY);
  eq([a.designSku, a.skuSource], ['MUSIC 4', 'named'], 'before the master write: the line reads as the master design MUSIC 4 (source: named)');
  eq(problems(a), [], 'no question: not "unmatchedSku"');
  // after the write ("MUSIC4" beside "MUSIC 4")
  const b = read(NAMED, AFTER);
  eq([b.designSku, b.skuSource], ['MUSIC4', 'named'], 'after the master write: the line reads as MUSIC4');
  eq(problems(b), [], 'no question');
  // the whole reading is the one the plain SKU gives (necklace, Silver, 14", one piece)
  const plain = read('MUSIC4', AFTER);
  eq(strip(b), strip(plain), 'the whole reading is the one the plain SKU MUSIC4 gives (form, size, chain, material, options, questions, engraving flags)');
  eq([b.material, b.form, b.size, b.chain, b.quantity, b.pieceCount, b.noDesign, b.special || null], ['silver', null, null, '14"', 1, 1, false, null], 'a Silver necklace charm with a 14" chain, one piece (no form option: the necklace charm, never the huggie)');
  eq([O.lineFamily(line(NAMED)), O.purchaseDetails(line(NAMED), b).type, O.METAL_TO_CARD.silver], ['necklace', 'Necklace', O.METAL_TO_CARD.silver], 'a necklace; Silver goes to the silver sheets');
  ok(!/huggie/i.test(b.designSku), 'never the huggie design');
  eq(JSON.stringify(line(NAMED)), before, 'the line it was given is not changed'); }
// the spelling of the named SKU: any case or spacing reads the same
for (const s of ['music4 - single w/ double line', 'Music4-Single W/ Double Line', 'MUSIC 4 - SINGLE W/ DOUBLE LINE']) eq(rs(s, AFTER), { sku: 'MUSIC4', source: 'named' }, `${s}: read as MUSIC4`);
// a person's saved answer still wins (an alias for this SKU on this listing; the listing's answer)
eq(rs(NAMED, AFTER, { [LISTING]: { v: 2, bySku: { [NAMED]: 'MUSIC 1' } } }), { sku: 'MUSIC 1', source: 'alias' }, 'a saved answer for this SKU on this listing stands over the named design');
eq(rs(NAMED, AFTER, { [LISTING]: { sku: 'MUSIC 1' } }), { sku: 'MUSIC 1', source: 'alias' }, 'so does the listing\'s answer');
// the option map and a SKU held by the master itself: the named SKU is never read over a SKU the master holds
{ const withIt = master(['MUSIC 4', 'MUSIC4', NAMED]); eq(rs(NAMED, withIt), { sku: NAMED, source: 'transaction' }, 'if the master ever holds the Etsy SKU itself, that is the design (as every master SKU)'); }

// ── 3 · every other SKU of the family still waits (a descriptor usually names another design) ──
for (const s of ['MUSIC4 - DOUBLE NOTE', 'MUSIC4 - SINGLE', 'MUSIC4 - SINGLE W/ DOUBLE LINES', 'MUSIC4 - BEADY', 'MUSIC4 - DISC', 'MUSIC 4 - DOUBLE', 'MUSIC4 - SINGLE W/ DOUBLE LINE-CO'.replace('LINE-CO', 'LINE 2'), 'MUSIC3 - SINGLE NOTE', 'APPLE2 - BEADY', 'OWL - SNOWY', 'MUSIC1 - SINGLE W/ DOUBLE LINE']) {
  const r = rs(s, AFTER); eq([r.sku, r.source], [s, 'transaction'], `${s}: still waits as written (not in any master file)`);
  eq(problems(read(s, AFTER)), ['unmatchedSku:not in any master file'], `${s}: the Review question is the one it always was`); }
// the master's own counter-examples read as themselves, never as the SKU before the dash
for (const s of ['MUSIC3', 'MUSIC3 - DOUBLE MUSIC NOTE', 'APPLE2', 'APPLE2 - DISC', 'OWL', 'OWL_40570']) eq(rs(s, AFTER), { sku: s, source: 'transaction' }, `${s}: the master's own SKU`);
// MUSIC3 - DOUBLE MUSIC NOTE is NOT MUSIC3 (two designs); the unseen "MUSIC3 - TRIPLE NOTE" is neither
eq(rs('MUSIC3 - TRIPLE NOTE', AFTER).source, 'transaction', 'an unseen descriptor of an existing family waits');

// ── 4 · the rename: what each spelling reads as before and after the master write ──
eq(rs('MUSIC 4', TODAY), { sku: 'MUSIC 4', source: 'transaction' }, 'MUSIC 4 reads as MUSIC 4 today');
eq(rs('MUSIC 4', AFTER), { sku: 'MUSIC 4', source: 'transaction' }, 'MUSIC 4 still reads as MUSIC 4 after the write: the old entry stays, stored sheets and orders that name it read as they did');
eq(rs('MUSIC4', TODAY), { sku: 'MUSIC 4', source: 'spelling' }, 'MUSIC4 read as MUSIC 4 (the spacing rule) before');
eq(rs('MUSIC4', AFTER), { sku: 'MUSIC4', source: 'transaction' }, 'MUSIC4 reads as MUSIC4 after');
eq(rs('music4', AFTER), { sku: 'MUSIC4', source: 'transaction' }, 'in any case');
eq(rs('MUSIC4-CO', AFTER), { sku: 'MUSIC4', source: 'variation' }, 'the charm-only mark still reads');
eq(rs('MUSIC4 (HUGGIE)', AFTER), { sku: 'MUSIC 4 (HUGGIE)', source: 'spelling' }, 'the huggie design is found by its spelling, as before (it is a different design: never MUSIC4)');
eq(rs('MUSIC 4 (HUGGIE)', AFTER), { sku: 'MUSIC 4 (HUGGIE)', source: 'transaction' }, 'and by its own name');
// a design the library keeps under two names reads the same piece
{ const x = read('MUSIC 4', AFTER), y = read('MUSIC4', AFTER); eq(strip(x), strip(y), 'a line named MUSIC 4 and one named MUSIC4 read the same in everything but the SKU'); }
// sameDesign: an Etsy update that moves a line between the two names is no change of design (bridge revalidate: skuChanged)
ok(O.sameDesign('MUSIC 4', 'MUSIC4', AFTER.entry), 'MUSIC 4 and MUSIC4 share one design file');
ok(O.sameDesign('music 4', 'MUSIC 4', AFTER.entry), 'the same SKU in another case');
ok(O.sameDesign(null, undefined, AFTER.entry) && O.sameDesign('', null, AFTER.entry), 'no SKU on either side is no change');
ok(!O.sameDesign('MUSIC 4', 'MUSIC 3', AFTER.entry), 'two designs are two designs');
ok(!O.sameDesign('MUSIC 4', 'MUSIC3 - DOUBLE MUSIC NOTE', AFTER.entry), 'MUSIC3 - DOUBLE MUSIC NOTE is not MUSIC 4');
ok(!O.sameDesign('MUSIC 4', 'GONE', AFTER.entry) && !O.sameDesign('GONE', 'MUSIC 4', AFTER.entry) && !O.sameDesign('MUSIC 4', 'MUSIC4', null), 'a SKU the master does not hold (or no master) is a change');
ok(O.sameDesign('A', 'B', s => ({ sizes: { S: { aiPath: 'f.ai' } } })), 'a design drawn in sizes: its first file counts');
{ const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), live = fs.readFileSync(path.join(root, 'netlify/functions/_stationLive.js'), 'utf8');
  ok(/skuChanged = oldSpec && !O\.sameDesign\(oldSpec\.designSku, r\.spec\.designSku, sku => Master\.entryFor\(sku\)\)/.test(bridge), 'revalidate: the same drawing under another of its names is not "SKU changed"');
  ok(/O\.knownTypo \? O\.knownTypo\(sku\) \|\| \(O\.knownName \? O\.knownName\(sku\) : ""\)/.test(bridge), 'the line is read again when the library gets or loses the named design');
  ok(/ordersLib\.knownName/.test(live), 'the Design Station looks the named SKU up under its design');
  const L2 = require('../../netlify/functions/_stationLive.js');
  eq([L2._t.designKey(NAMED), L2._t.designKey('MUSIC4'), L2._t.designKey('nitial_Disc_4571'), L2._t.designKey('MUSIC4 - DOUBLE NOTE')], ['MUSIC4', 'MUSIC4', 'INITIAL_DISC_4571', 'MUSIC4 - DOUBLE NOTE'], 'the station reads the named SKU as its design, the typo as before, any other SKU as it is'); }

// ── 5 · the sweep: every master SKU reads as itself; none is "named" (needs the saved list of the master, /mnt/project-files/plans/flag-fix-1011/master-skus-1011.json) ──
const SAVED = '/mnt/project-files/plans/flag-fix-1011/master-skus-1011.json';
if (fs.existsSync(SAVED)) {
  const list = JSON.parse(fs.readFileSync(SAVED, 'utf8')), skus = list.map(e => e.sku), all = master(skus), twice = master(skus.concat(['MUSIC4']));
  eq(skus.length, new Set(skus).size, 'the saved master list has no SKU twice');
  let named = 0, odd = [];
  for (const m of [all, twice]) for (const s of skus.concat(['MUSIC4'])) { const r = O.resolveSku({ sku: s, listingId: '1' }, {}, m.entry, {}, m.loose); if (m.entry(s)) { if (r.sku !== s || r.source !== 'transaction') odd.push(s); } if (r.source === 'named') named++; }
  eq([named, odd], [0, []], `${skus.length} master SKUs (before and after the write): each reads as itself, none through the named table`);
  // the probes: "<master SKU> - SINGLE W/ DOUBLE LINE" and a few other descriptors, for every master SKU: only the named SKU's own spellings change
  const changed = [];
  for (const s of skus) for (const d of [' - SINGLE W/ DOUBLE LINE', ' - DOUBLE NOTE', ' - BEADY', ' - DISC']) { const r = O.resolveSku({ sku: s + d, listingId: '1' }, {}, all.entry, {}, all.loose); if (r.source === 'named') changed.push(s + d + ' -> ' + r.sku + ' (' + r.source + ')'); }
  eq(changed, ['MUSIC 4 - SINGLE W/ DOUBLE LINE -> MUSIC 4 (named)'], 'of ' + (skus.length * 4) + ' probes (descriptors on every master SKU) only the named SKU, spelled with the space, is read through the table (the spacing rule reads what it always read)');
} else console.log('(the saved master list is not on this machine: the sweep is skipped)');

console.log(`music-sku: ${n} checks passed`);
