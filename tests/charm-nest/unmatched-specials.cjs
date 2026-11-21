// CUSTOMWAIT (Paul, 20 Nov 2026: "Some orders are still not updating their vector files"): the 10 open lines the cloud world showed as
// "unmatched · SKU not in any master file" on 20 Nov 2026, read again OFFLINE with the real shapes (tests/charm-nest/fixtures/unmatched-lines.json:
// the world records read with worldGet, buyer names replaced). The cloud's own reading is used (netlify/functions/_charmNestWorld.js readLine +
// afterRead, which run charm-nest-orders.js interpretLine exactly as the sorter's Orders.readLine / interpretAll do), starting from the run record's
// state the world line carries, so the printed state is what every page now shows.
//   · a custom piece (a SKU the custom-listing tool made, CUSTOM-<family>-<price>-<thread>, or a line Claude / a person reads as custom or rework) whose
//     design is not in the charm repository yet is custom work waiting for its design: Review → Custom Orders, "waits on a person", never "SKU not in any
//     master file". It is the buyer's own (spec.ownDesign, as an add-on listing's line is): held with the add-on wait until its designs are sent to a sheet,
//     saved to the repository (customSaveCommit: the listing's alias names the saved SKU, and the line reads that design) or it is completed by hand.
//   · Add_4822 reads as an add-on by its SKU code, unless Claude's reading of the line says "regular" (Paul, 25 Sep: the reading stands over the SKU codes; a
//     person's decision over both): live it is "regular", so it is a regular line with an unknown SKU, as before. A person's word for the listing (add-on to an
//     order) makes every order of it an add-on that waits for a person.
//   · Gold_4613 and ST054 are regular lines whose SKU no master holds and no alias or option map answers: they wait, as before (a person adds or maps them once).
//   · CHAPPARAL LOWERCASE is the letter listing: "I'd like the letter “t” please." is no single letter by its rule (LETTERCHOICE), so it asks which letter, as before.
//   node tests/charm-nest/unmatched-specials.cjs
'use strict';
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const World = require(path.join(root, 'netlify/functions/_charmNestWorld.js'));
const FIX = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/unmatched-lines.json'), 'utf8'));

// the master as far as these lines look at it: none of the ten SKUs (masterGetMany found none of them, 20 Nov 2026), the letter family the letter listing reads
const MASTER = new Map();
for (let i = 0; i < 26; i++) { MASTER.set('LETTER_NECKLACE-' + i, { sku: 'LETTER_NECKLACE-' + i }); MASTER.set('LETTER_NECKLACE-' + i + ' (HUGGIE)', { sku: 'LETTER_NECKLACE-' + i + ' (HUGGIE)' }); }
const ctxOf = (extra, more) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [], rows: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, seen: {},
  masterEntry: s => MASTER.get(String(s || '').toUpperCase()) || (more && more.get(String(s || '').toUpperCase())) || null, masterLoose: () => '', masterShared: () => [], masterLike: () => [], masterSkus: () => [...MASTER.keys()] }, extra || {});
// Claude's readings as the world records kept them; Add_4822's world record has no special, so its stored reading said "regular" (the only reading that leaves an ADD_ SKU with no listing word and no design unspecial)
const READS = {}; for (const f of FIX.lines) if (f.read) READS[f.key] = f.read;
const ADD = FIX.lines.find(f => f.line.sku === 'Add_4822');
const LIVE = ctxOf({ customRead: Object.assign({}, READS, { [ADD.key]: { kind: 'regular', confidence: 0.8, summary: 'reads as a regular listing', hash: ADD.run.crh } }) });

/** One line as the cloud reads it: the run record's state, readLine, afterRead (what _charmNestWorld.js build does for each line). */
function cloudRead(f, ctx, sent) {
  const row = { key: f.key, order: f.order, line: f.line, state: f.run.state || 'pulled', reason: f.run.reason || null, poolIds: (f.run.poolIds || []).slice(), hold: f.run.hold || null };
  row.spec = World.readLine(row, ctx); World.afterRead(row, null, sent || null);
  return row;
}
const said = r => ({ state: r.state, reason: r.reason || '', special: r.spec.special ? r.spec.special.kind : null, own: !!r.spec.ownDesign, readable: !!r.spec.readable, problems: (r.problems || []).map(p => p.kind + (p.reason ? ': ' + p.reason : '')) });
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

/* ── 1 · the ten lines, as every page shows them now ── */
console.log('  line                    SKU                    world record (live)                          ->  read now');
const now = {};
for (const f of FIX.lines) {
  const r = cloudRead(f, LIVE), s = said(r); now[f.key] = { r, s };
  const live = `${f.live.state}${f.live.special ? ' [' + f.live.special + ']' : ''} · ${(f.live.problems[0] && (f.live.problems[0].kind + ': ' + f.live.problems[0].reason)) || f.live.reason || ''}`;
  const read = `${s.state}${s.special ? ' [' + s.special + (s.own ? ', own design' : '') + ']' : ''} · ${s.problems[0] || s.reason}`;
  console.log(`  ${f.key}  ${String(f.line.sku).padEnd(21)}  ${live.slice(0, 92).padEnd(92)}  ->  ${read.slice(0, 140)}`);
}

const CUSTOM = { 'CUSTOM-N-095-149299': 'customNecklace', 'CUSTOM-N-144-876573': 'customNecklace', 'CUSTOM-N-080-512879': 'customNecklace', 'CUSTOM-H-020-993702': 'customHuggies', 'CUSTOM-N-144-077292': 'customNecklace', 'CUSTOM-H-014-433731': 'rework' };
for (const f of FIX.lines) {
  const { r, s } = now[f.key], sku = String(f.line.sku);
  if (CUSTOM[sku]) {
    eq(f.live.problems.map(p => p.kind), ['unmatchedSku'], `${sku}: the world record said "not in any master file" (the fixture is the live record)`);
    eq(s.special, CUSTOM[sku], `${sku}: still read as ${CUSTOM[sku]} (from Claude's stored reading)`);
    eq(s.problems, [], `${sku}: a custom piece with no design yet asks nothing: no "SKU not in any master file" — ${JSON.stringify(s.problems)}`);
    ok(s.own, `${sku}: it is the buyer's own work (spec.ownDesign), as an add-on listing's line is`);
    eq([s.state, s.reason], ['held', O.specialOf.addOn.wait], `${sku}: the run's "unmatched" becomes the custom wait (waits for its own designs, Send to Sheet, or Complete Order)`);
    ok(s.readable, `${sku}: Claude still reads it (its summary and the earlier order it names stay on the card)`);
    ok(!r.spec.noDesign && !r.spec.engraveCandidate, `${sku}: not "no design" (its order waits for it) and no engraving job of its own`);
    ok(O.evaluateOrder([{ key: f.key, state: r.state, reason: r.reason, spec: r.spec, problems: r.problems }]).committable === false, `${sku}: its order waits for it`);
  }
}
{ const f = ADD, s = now[f.key].s;
  eq([s.special, s.state, s.problems.map(p => p.split(':')[0])], [null, 'held', ['needsMaterial', 'unmatchedSku']], 'Add_4822 with Claude\'s "regular" reading: a regular line with an unknown SKU, exactly as the world record (the reading stands over the SKU code)');
  eq(f.live.problems.map(p => p.kind), ['needsMaterial', 'unmatchedSku'], 'Add_4822: the world record');
  // the same line before any reading: the shop's own code ADD_ makes it an add-on, which waits for a person (it is never "not in any master file")
  const bare = said(cloudRead(f, ctxOf({ customRead: READS })));
  eq([bare.special, bare.own, bare.problems, bare.state], ['addOn', true, [], 'held'], 'Add_4822 by its SKU code: an add-on waiting for a person, no design asked for: ' + JSON.stringify(bare));
  // a person's word for the listing (charmNestLibrary listingKindPut: "every order of it is an add-on to an order"): every order of listing 198504822, whatever Claude read
  const word = cloudRead(f, ctxOf({ customRead: LIVE.customRead, aliases: { 198504822: { listingKind: { kind: 'addOnToOrder', by: 'Paul' } } } })), ws = said(word);
  eq([ws.special, word.spec.special.label, ws.own, ws.problems, ws.state], ['addOn', 'Add-on to an order', true, [], 'held'], 'Add_4822 with a person\'s word for its listing: ' + JSON.stringify(ws));
}
for (const sku of ['Gold_4613', 'ST054']) {
  const f = FIX.lines.find(x => x.line.sku === sku), s = now[f.key].s;
  eq([s.special, s.problems], [null, ['unmatchedSku: not in any master file']], `${sku}: a regular line whose SKU is in no master file and no alias or option map: it waits, as before`);
  eq(s.state, f.live.state, `${sku}: state as the world record`);
}
{ const f = FIX.lines.find(x => x.line.sku === 'CHAPPARAL LOWERCASE'), s = now[f.key].s;
  eq([s.special, s.problems], [null, [`unmatchedSku: ${f.live.problems[0].reason}`]], 'CHAPPARAL LOWERCASE: the letter listing asks which letter ("I’d like the letter “t” please." is not one letter by the LETTERCHOICE rule), as before');
}

/* ── 2 · the custom piece once a person has made its design ── */
{
  const f = FIX.lines.find(x => x.line.sku === 'CUSTOM-N-144-077292');
  // saved to the charm repository (customSaveCommit): the index entry of the saved SKU and the listing's alias bySku[the line's SKU] = that SKU (a CUSTOM SKU itself is never the saved one: charm-nest-sku.js ownWhy)
  const saved = new Map([['PINE_FOX_0722', { sku: 'PINE_FOX_0722' }]]);
  const r = cloudRead(f, ctxOf({ customRead: READS, aliases: { [f.line.listingId]: { bySku: { 'CUSTOM-N-144-077292': 'PINE_FOX_0722' } } } }, saved)), s = said(r);
  eq([r.spec.designSku, r.spec.skuSource, s.special, s.own, s.problems], ['PINE_FOX_0722', 'alias', 'customNecklace', false, []], 'saved to the repository: the line reads the saved design (its vector), still a custom order, nothing asked: ' + JSON.stringify(s));
  // its designs sent to a sheet from its card (CustomSheet): the line goes on to be cut from them
  const sent = cloudRead(f, ctxOf({ customRead: READS }), { metal: 'gold', pieces: 1, sent: true });
  eq([sent.state, sent.problems], ['pulled', []], 'sent to a sheet from its card: it goes on to be cut');
  // completed by hand (Review → Release Hold / its QR label printed): not cut
  const done = cloudRead(f, ctxOf({ customRead: READS, customDone: { [f.key]: { state: 'completed', how: 'button', completedBy: 'Paul' } } }));
  eq([done.state, done.spec.noDesign, done.problems], ['noDesign', true, []], 'completed by hand: nothing is cut for it');
  // a person says it is not a custom order: back to its own questions (the SKU no master holds)
  const reg = said(cloudRead(f, ctxOf({ customRead: READS, customDecided: { [f.key]: { kind: 'regular', by: 'Paul' } } })));
  eq([reg.special, reg.own, reg.problems], [null, false, ['unmatchedSku: not in any master file']], 'a person\'s "Not a custom order": a regular line with an unknown SKU again');
}

/* ── 3 · what does not change ── */
{
  // a custom SKU whose design IS in the master by a person's link stays read as that design; chain only stays not cut; a regular unknown SKU still asks
  const base = FIX.lines.find(x => x.line.sku === 'CUSTOM-N-080-512879');
  const chain = Object.assign({}, base, { key: base.key.replace(/_\d+$/, '_1'), line: Object.assign({}, base.line, { transactionId: '1', sku: 'CHAIN_8941', title: 'CHAIN REPLACEMENT' }), read: null });
  const c = said(cloudRead(chain, ctxOf()));
  eq([c.special, c.state, c.problems], ['chainOnly', 'noDesign', []], 'chain only: not cut, as before');
  const unk = Object.assign({}, base, { key: base.key.replace(/_\d+$/, '_2'), line: Object.assign({}, base.line, { transactionId: '2', sku: 'NEWSKU_77', title: 'Sea Turtle Charm Necklace' }) });
  const u = said(cloudRead(unk, ctxOf()));
  eq([u.special, u.own, u.problems], [null, false, ['unmatchedSku: not in any master file']], 'a regular line with an unknown SKU still asks for its design');
  // the custom-listing tool's SKU with no reading at all: custom by its SKU code, waiting for its design
  const fresh = said(cloudRead(base, ctxOf()));
  eq([fresh.special, fresh.own, fresh.problems, fresh.readable], ['customNecklace', true, [], true], 'CUSTOM-N-… before Claude has read it: custom work waiting for its design, still sent to Claude to read');
}

console.log(`unmatched-specials: ${n} checks passed`);
