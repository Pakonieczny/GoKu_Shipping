// ADV-SPECIALS (Paul, 15 Oct 2026: "resilient against lost internet connections, interruptions and unexpected user behaviour"): the special products, from intake to the pool, with hostile input.
// OFFLINE, node only: the REAL intake (charm-nest-orders.js), the REAL pool maker (the bridge's Pool in a vm, tests/charm-nest/_keyWorld.cjs), the REAL words reader (charm-nest-engrave-read.js), the master slice of the
// real library read 11 Oct 2026 (fixtures/huggie-guard-world.json). Nothing live, nothing written. What is lost, duplicated, stuck or wrong when ...
//   1 · a hostile line (junk in every field of a set / pair / zodiac / Huggie twin / custom / add-on line) reaches the intake: the line may be held, but it never throws (one throw stops every line after it: the
//       intake reads the whole list in one loop), its piece count is a whole number and its list of pieces is that long
//   2 · the second bar of a SET OF 2 BARS cannot be had (its file corrupt, its SKU not in the master, blocked): nothing of the set is pooled, nothing is on a sheet, no half set; when the file is good again the next try
//       makes BOTH bars, once
//   3 · the buyer's words for the two bars are odd (one bar's words only, a blank second, the lengths in the other order, nothing at all): a bar is never given a guess or the other bar's words
//   4 · a MIXED order (a custom line waiting for its designs, a standard line, a set line): the order is not committable until the custom line is decided; the standard and set lines never wait on a
//       decision they do not depend on to be READ, only to be committed
//   node tests/charm-nest/adv-specials.cjs
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), R = require(path.join(root, 'charm-nest-engrave-read.js'));
const F = require('./pairs-fixtures.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a === undefined ? null : a)), b, m); n++; };
const failures = [];
const section = async (name, fn) => { try { await fn(); console.log('  ok  ', name); } catch (e) { failures.push(name); console.log('  FAIL', name, '\n     ', String(e && e.stack || e).split('\n').slice(0, 6).join('\n      ')); } };

const guard = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8'));
const real = sku => guard.master.find(e => e.sku === sku);
const BAR = 'BAR_1548', SKINNY = 'SKINNY_0191', LID = '532401548';
const setMasters = [BAR, SKINNY].map(s => Object.assign({ holes: 2 }, real(s)));
const mk = list => { const m = new Map(list.map(e => [e.sku.toUpperCase(), e])), loose = new Map(); for (const k of m.keys()) loose.set(O.looseKey(k), k); return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '', masterSkus: () => [...m.keys()], masterShared: () => [] }; };
const ctxOf = (list, extra) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } }, mk(list), extra);
const V = (name, value) => ({ name, value });
const ORDER = { receiptId: '4999999999', updateTs: 1791826702 };
const TITLE = 'Bar Necklace SET of 2, Gold, Rose Gold, Silver, Custom Gold Bar Set, Engraved Necklace, Customized Name Bar Set, Personalized Bar Necklace';
const setLine = (words, over) => Object.assign({ transactionId: '5300000001', listingId: LID, sku: BAR, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: words,
  variations: [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA')].concat(words.length ? [V('Personalization', words.join('\n'))] : []) }, over || {});

(async () => {
  await section('1 · a hostile line never throws and always has a whole number of pieces, in every special family', async () => {
    const all = guard.master, c = ctxOf(all);
    // (the shapes of the special lines of the shop: a set of bars, a zodiac huggie, the Huggie twins, a custom order, an add-on, a mismatched pair, a "2 symbols" stud)
    const FAMILIES = [
      { listingId: LID, sku: BAR, title: 'Bar Necklace SET of 2', variations: [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA')] },
      { listingId: '1749318013', sku: 'CARDS 2 - (4 CARDS)', title: 'Playing Card Earring Studs', variations: [V('Metal', 'Gold')] },
      { listingId: '1749318013', sku: 'CARDS 2 - (4 CARDS) (HUGGIE)', title: 'Playing Card Earring Huggies', variations: [V('Charm Type', 'Huggie CHARM SET')] },
      { listingId: '1816178147', sku: 'HUGGIE HOOPS-ZODIAC', title: 'Zodiac Huggie Hoops', variations: [V('METAL CHOICE', 'Gold'), V('HOOP SIZE', '8.5mm'), V('ZODIAC SIGN', 'Libra')] },
      { listingId: '1816178147', sku: 'HUGGIE HOOPS-ZODIAC', title: 'Zodiac Huggie Hoops', variations: [V('ZODIAC SIGN', 'Ophiuchus')] },
      { listingId: '1818', sku: 'A', title: 'Zodiac REVAMP Stud Earrings', variations: [V('Options', 'Silver • 2 symbols')] },
      { listingId: '1777293722', sku: 'Huggie_3722', title: 'Huggie Charm + Shipping', variations: [V('Price', 'Gold Filled - Single')] },
      { listingId: '1800006673', sku: 'CUSTOM_6673', title: 'CUSTOM CHARM', variations: [V('Price', '28')] },
      { listingId: '1819', sku: 'A', title: 'Mismatched Tennis Ball and Racket Huggie Hoops', variations: [V('HOOP SIZE', '8.5mm')] }
    ];
    const JUNK = [undefined, null, '', ' ', '\u0000', '\u{1F319}'.repeat(300), '<b>x</b>', 'a\nb', 'x'.repeat(20000), 0, -1, 1e9, NaN, {}, [], true];
    const QTY = [1, 2, 3, 0, -1, '2', 1.5, NaN, null, undefined, 999, 'many'];
    const WORDS = [[], [null], [undefined], ['16"= a 18"= b'], [{}], ['\u{1F319}'], ['x'.repeat(5000)], ['Libra'], ['Libra and Leo'], ['Ophiuchus'], ['same on both ears']];
    const VARS = [[], [{}], [V(null, null)], [V(undefined, 'Necklace')], [V('ZODIAC SIGN', null)], [V('ZODIAC SIGN', 'Ophiuchus')], [V('HOOP SIZE', undefined)], [V('Options', '2 symbols')]];
    let runs = 0; const bad = [];
    for (let i = 0; i < 3000; i++) {
      const fam = FAMILIES[i % FAMILIES.length], pick = (arr, k) => arr[(i * 7919 + k * 104729 + (i >> 3)) % arr.length];
      const line = Object.assign({ transactionId: pick(JUNK, 1), metalKey: pick(['gold', 'silver', 'rose', undefined, null, 'plutonium'], 2), metalLabel: pick(['Gold', undefined, null], 3), quantity: pick(QTY, 4), personalization: pick(WORDS, 5), buyerMessage: pick(JUNK, 6), staffNote: pick(JUNK, 7) }, fam,
        i % 3 ? {} : { variations: fam.variations.concat(pick(VARS, 8)) });
      const order = Object.assign({}, ORDER, i % 5 ? {} : { buyerMessage: pick(JUNK, 9) });
      runs++;
      try {
        const sp = O.interpretLine(order, line, c), count = O.pieceCountOf(sp), pcs = O.piecesOf({ order, line, spec: sp, poolIds: [] });
        if (!Number.isInteger(count) || count < 0) bad.push(['count ' + count, fam.sku]);
        else if (Array.isArray(pcs) && pcs.length !== count) bad.push(['pieces ' + pcs.length + ' != count ' + count, fam.sku]);
        O.setPiecesOf({ order, line, spec: sp }); O.specialOf(line);
      } catch (e) { bad.push(['THROW ' + String(e.message).slice(0, 80) + ' @ ' + String(e.stack).split('\n')[1].trim().slice(0, 60), fam.sku, JSON.stringify(line).slice(0, 120)]); }
    }
    eq(bad.slice(0, 5), [], runs + ' hostile lines read: ' + bad.length + ' bad');
  });

  await section('2 · the second bar of a set cannot be had: no half set is pooled, and the next try makes both bars', async () => {
    const keyWorld = require('./_keyWorld.cjs')(root), { DESIGNS, world, lineRow } = keyWorld;
    DESIGNS[BAR] = () => F.charmOf('ONE-PENDANT'); DESIGNS[SKINNY] = () => F.charmOf('PAIR-HOOP');
    const bad = { sku: null };
    const worldOf = () => {
      const w = world([BAR, SKINNY]); w.reviews = [];
      w.c.P.parseSource = async (_b, name) => ({ name });
      w.c.P.groupCharmsAsync = async parsed => { const s = String(parsed.name).replace(/\.ai$/, ''); if (s === bad.sku) throw new Error('the design file of ' + s + ' is corrupt'); return { charms: [structuredClone(DESIGNS[s]())], orphans: [] }; };
      w.c.Review = { add: it => w.reviews.push(it) }; w.c.B.review = { items: w.reviews }; w.c.Sets = { ofRun: () => [] }; w.c.Gate = { modern: () => false };
      return w;
    };
    const rowOf = (words, qty) => { const line = setLine(words, { quantity: qty || 1 }), spec = O.interpretLine(ORDER, line, ctxOf(setMasters)); spec.material = 'gold';
      return Object.assign(lineRow('4999999999', '5300000001', spec.designSku, qty || 1, {}), { spec, line: { transactionId: line.transactionId, listingId: line.listingId, sku: line.sku, title: line.title } }); };
    const pools = w => w.calls.filter(c => c.body.op === 'poolPut');
    // (a) a corrupt design file, on either bar: the throw reaches the intake's per-line hold; nothing is pooled
    for (const which of [SKINNY, BAR]) {
      bad.sku = which; const w = worldOf(), row = rowOf(['16"= Katie 18"= Ginger']);
      let err = ''; try { await w.Pool.poolAdd(row, null); } catch (e) { err = e.message; }
      ok(/corrupt/.test(err), which + ' corrupt: the line fails whole: ' + err);
      eq([pools(w).length, w.pages.gold.charms.length, row.poolIds, row.state === 'pooled'], [0, 0, [], false], which + ' corrupt: no pool row written, nothing on a sheet, the line is not pooled');
      // the file is good again: the same line, tried again in the same page, is both bars, once
      bad.sku = null; await w.Pool.poolAdd(row, null);
      eq([row.state, w.pages.gold.charms.length, pools(w).length, pools(w)[0].body.pools.map(p => p.sku)], ['pooled', 2, 1, [BAR, SKINNY]], which + ' good again: both bars, one write');
      await w.Pool.poolAdd(row, null);
      eq([w.pages.gold.charms.length, [...new Set(pools(w).flatMap(c => c.body.pools.map(p => p.poolId)))].length], [2, 2], 'and trying a third time puts nothing more on the sheet and writes the same two pool ids (a second press, a second bench)');
    }
    // (b) the second bar is not in the master / is blocked: the line is held whole, with its reason; no half set
    for (const [what, tweak] of [['not in the master', w => { const mine = w.c.Master.entryFor; w.c.Master.entryFor = s => (s === SKINNY ? null : mine(s)); }], ['blocked', w => { w.entries[SKINNY].blocked = 'bad file'; }]]) {
      bad.sku = null; const w = worldOf(); tweak(w); const row = rowOf(['16"= Katie 18"= Ginger']);
      await w.Pool.poolAdd(row, null);
      eq([row.state === 'pooled', pools(w).length, w.pages.gold.charms.length], [false, 0, 0], 'the second bar ' + what + ': held whole, nothing pooled (' + row.state + ': ' + row.reason + ')');
      ok(row.reason && /master|blocked/i.test(row.reason), 'and it says why: ' + row.reason);
    }
  });

  await section('3 · odd words for the two bars: a bar is never given a guess or the other bar\'s words', async () => {
    const wire = new Function('O', 'lineRowOf', 'R', /R\.slotInfo = \(row, slot\) => \{[\s\S]*?\n    \};/.exec(fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'))[0] + '\nreturn R.slotInfo;');
    R.slotInfo = wire(O, r => (r && r.parentRow) || r, R);
    const jobs = [{ slot: 'D1' }, { slot: 'D2' }], c = ctxOf(setMasters);
    const run = words => {
      const line = setLine(words), spec = O.interpretLine(ORDER, line, c), slots = R.slotsOf({ spec, line }, jobs), reading = R.readPersonalisation(words.join('\n'), { lengths: R.lengthsOfLine(line) });
      return { spec, res: R.assign(reading, slots), slots };
    };
    // both lengths, in the other order: the words follow the LENGTH, not the order they were typed in (the 16" chain is the thinner bar D2)
    { const r = run(['18"= Ginger 16"= Katie']); ok(r.res.ok, 'reversed: ' + r.res.why); eq([r.res.bySlot.D1.words, r.res.bySlot.D2.words], ['Ginger', 'Katie'], 'the 18" words go to the large bar D1, the 16" words to the small bar D2'); }
    // one bar's words only, a blank second, no words at all, a bare word with no length: never both bars given the same words, never a bar given what was not said for it
    for (const [what, words] of [['one length only', ['16"= Katie']], ['a blank second bar', ['16"= Katie 18"= ']], ['no words at all', []], ['a bare word with no length', ['Katie']]]) {
      const r = run(words), by = (r.res && r.res.bySlot) || {}, w1 = by.D1 && by.D1.words, w2 = by.D2 && by.D2.words;
      ok(!(w1 && w2 && w1 === w2), what + ': the two bars never get the same words (' + JSON.stringify([w1, w2]) + ')');
      ok(!(w1 && /katie/i.test(w1)), what + ': the large bar (18") never gets the 16" words (' + JSON.stringify(w1) + ')');
      eq(r.spec.pieceCount, 2, what + ': still two pieces: the set is both bars whatever was written');
    }
    // the words of ONE bar left to a person are not a throw and the set keeps both pieces
    { const r = run(['\u{1F319}'.repeat(300)]); eq(r.spec.pieceCount, 2, 'a huge emoji box: two pieces, no throw'); }
  });

  await section('4 · a mixed order (custom waiting + standard + set): not committable until the custom line is decided', async () => {
    const lineOf = (key, state, extra) => Object.assign({ key, state, reason: null, problems: [], spec: { engraveCandidate: false }, engrave: { needed: false, state: 'none', approved: true } }, extra || {});
    const standard = lineOf('4999999999_1', 'written'), set = lineOf('4999999999_2', 'written');
    // the custom line waits for its designs: no design of its own, not completed, held by the add-on / custom wait
    const waiting = lineOf('4999999999_3', 'held', { reason: O.specialOf.addOn ? O.specialOf.addOn.wait : 'waiting for its designs', spec: { ownDesign: true, engraveCandidate: false } });
    let ev = O.evaluateOrder([standard, set, waiting]);
    eq([ev.committable, ev.held && ev.held.line], [false, '4999999999_3'], 'the custom line that waits for its designs holds the whole order: ' + JSON.stringify(ev.held));
    // a person completes it by hand (Review → Complete Order): the order goes on
    ev = O.evaluateOrder([standard, set, Object.assign({}, waiting, { spec: Object.assign({}, waiting.spec, { customDone: { state: 'completed', how: 'button', completedAt: 1, completedBy: 'Paul' } }) })]);
    eq(ev.committable, true, 'completed by hand: the order is committable');
    // ... or its designs are sent to the sheets (how 'sheet') and its piece is written like any other
    ev = O.evaluateOrder([standard, set, Object.assign({}, waiting, { state: 'written', reason: null, spec: Object.assign({}, waiting.spec, { customDone: { state: 'completed', how: 'sheet' } }) })]);
    eq(ev.committable, true, 'sent to the sheets and written: committable');
    // a Reopen (state open) takes a hand completion back: the line blocks again
    ev = O.evaluateOrder([standard, set, Object.assign({}, waiting, { spec: Object.assign({}, waiting.spec, { customDone: { state: 'open', how: 'button' } }) })]);
    eq(ev.committable, false, 'reopened: the custom line holds the order again');
    // a change from Etsy on the set line (the buyer changed the order after it was placed) holds the order until a person accepts it
    ev = O.evaluateOrder([standard, Object.assign({}, set, { changePending: true, reason: 'Etsy updated order 4999999999 after the pull' }), Object.assign({}, waiting, { state: 'written' })]);
    eq([ev.committable, ev.held && ev.held.line], [false, '4999999999_2'], 'a changed set line holds the order: ' + JSON.stringify(ev.held));
  });

  if (failures.length) { console.log(`adv-specials: ${failures.length} section(s) FAILED: ${failures.join(' | ')}`); process.exit(1); }
  console.log(`adv-specials: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
