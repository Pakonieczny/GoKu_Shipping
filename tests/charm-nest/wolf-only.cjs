// WOLFONLY (13 Oct 2026). Paul, looking at the Library card of the WOLF master (a howling wolf marked "Left" and a crescent moon marked "Right", "Left + Right pair", 18.7 x 13.5 mm
// both together): "Remove the "Right" moon charm from this repo charm. Only the wolf should remain since this is a necklace charm."
// The master WOLF.ai draws a wolf and a moon, and the index holds its four SKUs (WOLF, WOLF BEADY, WOLF CHARM, WOLF_6691) as a mismatched Left + Right pair. The change is a RULE IN CODE
// (CharmNestPair.onlyBodyOf, section 4d): nothing stored is touched (the .ai, the index records, every order, sheet and piece stay as they are), and it is undone by deleting one table row.
// Offline: no Etsy, no network, no paid call. The browser part runs the real sorter page over the in-memory fake site.
//   node tests/charm-nest/wolf-only.cjs        (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome> for the browser part; skipped without)
//  1 · charm-nest-pair.js: the four SKUs name the wolf; nothing else does; soloOf cuts the wolf alone on a line that is not an earring pair; an earring line keeps both bodies
//  2 · the intake (charm-nest-orders.js): a necklace of any of the four SKUs reads ONE wolf piece, no Left / Right, no mirror; the same SKUs as earrings and a real mismatched pair are as before
//  3 · the bridge's Pool (the real source in a vm): the necklace makes ONE piece cut from the wolf body alone; earrings keep both; a line pooled before stays whole
//  4 · the real WOLF.ai and the real sorter page (when the master backup is there): the card shows the wolf alone (no pill, no drop-downs, its own size), the picture is the wolf, the
//      real file cuts apart, and a real mismatched pair (MISMATCHED_7134) is still drawn and read as two bodies
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), os = require('os');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const { DESIGNS, hashOf, entryOf, world, lineRow, specOf } = require('./_keyWorld.cjs')(root);
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const pass = name => console.log('  ✓', name);
const J = x => JSON.parse(JSON.stringify(x));

const FOUR = ['WOLF', 'WOLF BEADY', 'WOLF CHARM', 'WOLF_6691'];
const TWO = { v: 1, bodies: 2, mismatched: true };
// a stand-in for the WOLF file: two DIFFERENT bodies (a ball and a racket, so they are two charms by shape), each in its own drawing group of the master, named WOLF
const wolfDesign = () => { const c = F.charmOf('TENNIS-MIS'); Pair.bodiesOf(c).forEach((b, i) => b.members.forEach(m => { m.parent = i; })); c.sku = 'WOLF'; return c; };
DESIGNS.WOLF = wolfDesign;
const wolf = wolfDesign();
ok(Pair.isMismatched(wolf) && Pair.bodiesOf(wolf).length === 2, 'the stand-in draws two different bodies');
const necklace = (sku = 'WOLF CHARM', extra) => Object.assign({ receiptId: '4199000001', transactionId: '5300000001', quantity: 1, form: 'necklace', spec: { designSku: sku, quantity: 1, pair: { earring: false, single: false, mismatched: false, twoBodies: true } } }, extra || {});
const ears = (sku = 'WOLF CHARM') => ({ receiptId: '4199000002', transactionId: '5300000002', quantity: 1, form: 'earrings', spec: { designSku: sku, quantity: 1, pieceCount: 2, pair: { earring: true, single: false, mismatched: true, kind: 'mismatched', sides: ['L', 'R'] } } });

const failed = [];
const section = async (name, fn) => { try { await fn(); pass(name); } catch (e) { failed.push(name); console.log('  ✗', name, '\n      ', String(e && e.message || e).split('\n')[0]); } };   // (every part runs, so a red run says which behaviours are missing)

(async () => {
  /* ── 1 · the module ── */
  await section('the four WOLF SKUs name the wolf; a necklace is cut from it alone; earrings, real pairs and every other design are as before', async () => {
    for (const s of FOUR.concat([' wolf  charm ', 'Wolf_6691'])) { const d = Pair.onlyBodyOf(s); ok(d && d.index === 0 && d.what === 'the wolf', `"${s}" names the wolf`); }
    for (const s of ['WOLF_67948', 'WOLF_35258', 'WOLF (HUGGIE)', 'MISMATCHED_7134', 'KEY_5598', 'TENNIS-MIS', 'MITTENS-MIS', '', null, undefined]) ok(Pair.onlyBodyOf(s) === null, `"${s}" names nothing`);
    ok(Pair.onlyBodyOf({ sku: 'WOLF CHARM' }) && Pair.onlyBodyOf({ entry: { sku: 'WOLF' } }) && Pair.onlyBodyOf(necklace('WOLF BEADY')) && Pair.onlyBodyOf(wolf), 'a record, an entry, a line and the design charm are read the same way');
    ok(Pair.onlyBodyOf({ spec: { designSku: 'KEY_5598' }, sku: 'WOLF' }) === null, 'a line whose design is another one is not named by its Etsy SKU');
    const d = Pair.onlyBodyOf('WOLF');
    eq([d.count, d.holes, d.upAngle, d.upSource, d.widthPt > 20 && d.widthPt < 40, d.heightPt > 20 && d.heightPt < 40], [2, 0, 90, 'drawn', true, true], 'the wolf\'s own size, holes and up angle (not the pair\'s 52.9 x 38.2 pt, one hole, 97 degrees)');
    ok(Array.isArray(Pair.ONE_BODY) && Pair.ONE_BODY.length === 1 && Pair.ONE_BODY[0].skus.length === 4, 'one table row, the four SKUs: deleting it undoes the change');
    // the master record, before any file is read
    for (const s of FOUR) ok(Pair.alikeDesign({ sku: s, pair: TWO }) === true && Pair.alikeDesign({ sku: s, pair: Object.assign({ alike: false }, TWO) }) === true, s + ': a person\'s word stands over what an indexer wrote');
    ok(Pair.alikeDesign({ sku: 'CAT_1111', pair: TWO }) === null && Pair.alikeDesign({ sku: 'CAT_1111', pair: Object.assign({ alike: false }, TWO) }) === false && Pair.alikeDesign({ sku: 'KEY_5598', pair: TWO }) === true, 'every other design is read as before');
    ok(Pair.alikeDesign(wolf) === true, 'the design charm named WOLF counts although its two bodies are different charms');
    ok(Pair.alikeDesign(DESIGNS.TENNIS_MIS()) === false, 'a ball and a racket not named by anyone are still two different charms');
    // which lines cut the wolf alone
    const s1 = Pair.soloOf(necklace(), wolf); eq([s1.index, s1.count, s1.rule, s1.how], [0, 2, "a person's word", 'named'], 'a necklace: the wolf, body 0, by a person\'s word'); ok(/wolf/.test(s1.why) && /moon/.test(s1.why), 'and says why');
    for (const s of FOUR) eq(Pair.soloOf(necklace(s), wolf).index, 0, s + ': a necklace is the wolf');
    ok(Pair.soloOf(ears(), wolf) === null, 'an earring pair of the same SKUs keeps its Left (the wolf) and its Right (the moon)');
    ok(Pair.soloOf({ receiptId: '1', transactionId: '2', quantity: 1, form: 'single', spec: { quantity: 1, designSku: 'WOLF', pair: { earring: false, single: true, mismatched: true, sideSaid: 'R', sides: ['R'] } } }, wolf) === null, 'a single earring that names its ear keeps that ear\'s own body');
    ok(Pair.soloOf(necklace('WOLF', { spec: { designSku: 'WOLF', quantity: 1, pair: { earring: false, glued: true } } }), wolf) === null, 'an old glued line stays as it was');
    ok(Pair.soloOf(necklace('WOLF', { spec: { designSku: 'WOLF', quantity: 1, pair: { earring: false, legacy: true } } }), wolf) === null, 'an old legacy line stays as it was');
    const ask = Pair.soloOf(necklace('WOLF', { spec: { designSku: 'WOLF', quantity: 1, pair: { earring: false, useBody: 1 } } }), wolf); eq([ask.index, ask.rule], [1, "a person's choice"], 'a person\'s choice on a line still picks the body');
    ok(Pair.soloOf(necklace('TENNIS-MIS'), DESIGNS.TENNIS_MIS()) === null && Pair.soloOf(necklace('MITTENS-MIS'), wolf) === null, 'a design nobody named keeps both bodies (the line\'s own design says which design it is)');
    eq([Pair.pieceCountOf(necklace(), wolf), Pair.pieceCountOf(necklace('WOLF', { quantity: 3, spec: { designSku: 'WOLF', quantity: 3, pair: necklace().spec.pair } }), wolf), Pair.pieceCountOf(ears(), wolf)], [1, 3, 2], 'one piece per necklace, two per earring pair');
    eq(Pair.piecesFor(necklace(), wolf).map(p => [p.side, p.bodyIndex, p.mirror]), [[null, 0, false]], 'a necklace piece: no Left or Right, body 0, never mirrored');
    eq(Pair.piecesFor(ears(), wolf).map(p => p.side + p.bodyIndex), ['L0', 'R1'], 'earrings: the wolf is the Left, the moon the Right');
    // the card's drop-downs
    const wc = Pair.facingControl({ sku: 'WOLF', pair: TWO, sym: 'directional' }, ''), mc = Pair.facingControl({ sku: 'MISMATCHED_7134', pair: TWO, sym: 'directional' }, '');
    ok(wc.show === false && wc.options.length === 0, 'WOLF: no "Left: not set" / "Right: not set" (nothing to turn on a necklace)');
    ok(mc.show === true && mc.bodies && mc.bodies.length === 2, 'a real mismatched pair still has its two drop-downs');

  });

  /* ── 2 · the intake ── */
  await section('the intake: a WOLF necklace reads ONE wolf piece (no side, no mirror); its earrings and real mismatched pairs read as before', async () => {
    const order = { receiptId: '4199000010', updateTs: 1789100000 }, V = (name, value) => ({ name, value });
    const MASTER = { 'MISMATCHED_7134': Object.assign(F.entryOf('MITTENS-MIS'), { sku: 'MISMATCHED_7134' }), 'TENNIS-MIS': F.entryOf('TENNIS-MIS') };
    for (const s of FOUR) MASTER[s] = { sku: s, pair: TWO, members: 3, widthPt: 52.9, heightPt: 38.2, charmHash: '953c22e8' };
    const ctx = () => ({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => MASTER[s] || null });
    const mk = over => Object.assign({ transactionId: '5300000010', listingId: '1718', sku: 'WOLF CHARM', title: 'Wolf Necklace Gold Wolf Pendant Charm Jewelry', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: [V('Metal Choice', 'Gold'), V('Necklace Length in inches', '16"')] }, over);
    const read = line => { const spec = O.interpretLine(order, line, ctx()); return { order, line, spec, poolIds: [] }; };
    const sides = r => O.piecesOf(r).map(p => (p.side || '-') + p.bodyIndex).join(' ');
    const asks = r => r.spec.problems.filter(p => p.kind !== 'needsMaterial').map(p => p.kind);
    for (const sku of FOUR) {
      const r = read(mk({ sku })), p = r.spec.pair;
      eq([r.spec.pieceCount, p.earring, p.mismatched, p.perUnit, p.kind, sides(r)], [1, false, false, 1, 'single', '-0'], sku + ': a necklace is ONE piece, no side');
      eq([p.oneBody.index, p.oneBody.count, p.oneBody.rule], [0, 2, "a person's word"], sku + ': the wolf body, by a person\'s word');
      ok(p.notes.some(x => /made as ONE charm, the left body only, no Left or Right/.test(x) && /wolf and a moon/.test(x) && /nothing duplicated or mirrored/.test(x)), sku + ': the order window says ONE charm and why');
      ok(!p.notes.some(x => /both bodies|glued/.test(x)), sku + ': and not "both bodies"');
      eq(asks(r), [], sku + ': nothing to ask a person');
      ok(!JSON.stringify(r.spec).includes('"mirror":true'), sku + ': nothing mirrored');
      const q2 = read(mk({ sku, quantity: 2 })); eq([q2.spec.pieceCount, sides(q2)], [2, '-0 -0'], sku + ': quantity 2 is two wolves');
      const e = read(mk({ sku, title: 'Wolf Stud Earrings', variations: [V('Metal Choice', 'Gold')] })); eq([e.spec.pieceCount, sides(e), e.spec.pair.oneBody, e.spec.pair.kind], [2, 'L0 R1', undefined, 'mismatched'], sku + ': earrings keep the wolf as the Left and the moon as the Right');
    }
    // the 5 real Sep 17 style lines: SKU written in lower case words
    eq(read(mk({ sku: 'Wolf Charm', title: 'Wolf Necklace Charm Necklace Gold Wolf Pendant' })).spec.pair.oneBody.rule, "a person's word", 'the SKU as an Etsy line writes it (Wolf Charm) is the same design');
    // the notes can be taken back (a line pooled before this was made whole) and put again, in the same words
    const base = read(mk({ sku: 'WOLF' })), sp = J(base.spec); O.solo(sp, null);
    eq(sp.pair.oneBody, undefined, 'taken back: no one-body words'); ok(sp.pair.notes.some(x => /made as one piece with both bodies/.test(x)), 'taken back: the old words');
    O.solo(sp, Pair.soloOf(necklace('WOLF'), wolf)); eq(sp.pair.oneBody.rule, "a person's word", 'put again'); ok(sp.pair.notes.some(x => /ONE charm/.test(x) && /wolf and a moon/.test(x)) && !sp.pair.notes.some(x => /both bodies/.test(x)), 'put again: ONE charm, the wolf\'s own reason, not "the same outline twice"');
    // a real mismatched pair, the MISMATCHED_7134 shape: two bodies as before (earrings), and the necklace words it had before (the same outline twice, the left body)
    const mis = read(mk({ sku: 'MISMATCHED_7134', title: 'Mittens Mismatched Stud Earrings', variations: [V('Metal Choice', 'Gold')] })); eq([mis.spec.pair.earring, mis.spec.pair.mismatched, mis.spec.pieceCount, sides(mis)], [true, true, 2, 'L0 R1'], 'MISMATCHED_7134 sold as a pair: two bodies, a Left and a Right');
    const misN = read(mk({ sku: 'MISMATCHED_7134', title: 'Mittens Necklace Pendant' })); eq(misN.spec.pair.oneBody.rule, 'the left body', 'MISMATCHED_7134 as a necklace: unchanged (the left body, its own rule)');
    ok(misN.spec.pair.notes.some(x => /the same outline twice/.test(x)), 'with its own words');
    const ten = read(mk({ sku: 'TENNIS-MIS', title: 'Tennis Necklace Pendant' })); eq([ten.spec.pair.oneBody, ten.spec.pieceCount, sides(ten)], [undefined, 1, '-0'], 'a ball and a racket as a necklace: one piece holding both, as before'); ok(ten.spec.pair.notes.some(x => /both bodies/.test(x)), 'with the old words');
    const tenE = read(mk({ sku: 'TENNIS-MIS', title: 'Tennis Earrings', variations: [V('Metal Choice', 'Gold')] })); eq([tenE.spec.pieceCount, sides(tenE)], [2, 'L0 R1'], 'a ball and a racket as earrings: a Left and a Right');

  });

  /* ── 3 · the bridge's Pool ── */
  await section('the Pool: a WOLF necklace is ONE piece cut from the wolf alone (its own up angle); earrings keep both; lines pooled before stay whole', async () => {
    const W = (extraSeed) => { const w = world(['WOLF'], extraSeed); w.use('WOLF'); w.entries.WOLF.charmHash = 'idx953c22e8'; w.entries.WOLF.pair = Object.assign({}, TWO); return w; };   // (the indexer's hash is not the page's hash of the folded charm: as live)
    const whole = DESIGNS.WOLF(), bodies = Pair.bodiesOf(whole), wholeHash = hashOf(whole);
    const lineOf = (w, rid, tid, title, qty, extra, vars) => { const entry = w.entries.WOLF, sp0 = specOf(rid, tid, 'WOLF', title, vars || [], entry); return lineRow(rid, tid, 'WOLF', qty, Object.assign({ pair: sp0.pair, pieceCount: sp0.pieceCount * qty, form: sp0.form }, extra || {})); };
    // a new WOLF necklace: ONE piece, the wolf alone
    for (const qty of [1, 2]) {
      const w = W(), row = lineOf(w, '4199000020', '5300000020', 'Wolf Necklace Pendant', qty);
      eq(J(row.spec.pair.oneBody), { index: 0, count: 2, rule: "a person's word" }, 'the intake says the wolf');
      await w.Pool.poolAdd(row, null);
      const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms;
      eq([pools.length, charms.length], [qty, qty], `x${qty}: ONE piece per unit, never the moon`);
      ok(!pools.some(p => 'side' in p || 'mirror' in p || 'bodyIndex' in p || 'groupKey' in p || 'groupSize' in p), 'no ear, no body number, no group: the record a plain charm has');
      for (const ch of charms) {
        eq(ch.members.length, bodies[0].members.length, 'cut from the wolf body only: its members, none of the moon\'s'); eq(J(ch.bbox), J(bodies[0].bbox), 'the sheet gets the wolf\'s box');
        ok(ch.bbox[2] - ch.bbox[0] < whole.bbox[2] - whole.bbox[0] - 10, 'narrower than both bodies together'); ok(!('side' in ch) && !('groupKey' in ch) && !('mirror' in ch) && ch.oneBody === 0, 'no Left / Right on the sheet charm');
        eq(ch.upAngle, 90, 'the wolf keeps its own up angle (the record holds 97 degrees, the moon\'s hole)'); ok(ch.hash !== wholeHash, 'its hash is the body\'s, the mark that tells it from the whole design');
      }
      eq(pools.map(p => p.charmHash), charms.map(c => c.hash), 'the pool rows hold the body\'s hash');
      ok(row.spec.pair.notes.some(x => /ONE charm/.test(x)) && !row.spec.pair.notes.some(x => /both bodies|glued/.test(x)) && row.spec.pair.glued === false, 'noted as ONE charm, not glued');
      eq(w.Pool.pieceCountOf(row), qty, 'counted as ' + qty);
      const src = w.c.B.pool.sources.get('charmnest/master/sku/WOLF.ai').charms[0]; eq(src.members.length, whole.members.length, 'the design\'s own charm is untouched and still holds both bodies');
      eq(w.oneRow(row), 0, 'the Review / Orders picture of the line is the wolf'); eq(w.pairRow(row), false, 'and says no "Left + Right"');
    }
    // earrings of the same SKU: a Left and a Right, each its own body (the moon is the Right)
    {
      const w = W(), row = lineOf(w, '4199000021', '5300000021', 'Wolf Stud Earrings', 1, { form: 'earrings' });
      await w.Pool.poolAdd(row, null);
      const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms;
      eq(J(pools.map(p => [p.side, p.bodyIndex])), [['L', 0], ['R', 1]], 'earrings: Left = the wolf, Right = the moon'); eq([charms[0].members.length, charms[1].members.length], bodies.map(b => b.members.length), 'each cut from its own body');
      eq([w.oneRow(row), w.pairRow(row)], [-1, true], 'the earring line draws Left + Right');
    }
    // a line pooled BEFORE this change holds the folded charm (the moon glued on): it stays whole, whatever hash form it stored
    for (const [name, h] of [['the page\'s hash of the folded charm', wholeHash], ['the indexer\'s hash of the record', 'idx953c22e8']]) {
      const w = W(), row = lineOf(w, '4199000022', '5300000022', 'Wolf Necklace Pendant', 1); row.poolIds = ['4199000022_5300000022_1'];
      w.c.B.pool.rows.set(row.poolIds[0], { poolId: row.poolIds[0], state: 'ready', charmHash: h, quantity: 1, sku: 'WOLF' });
      w.Pool.pinPooled(row); eq(row.spec.pair.oneBody, undefined, name + ': the words about one body are taken back'); ok(row.spec.pair.notes.some(x => /both bodies/.test(x)), name + ': the old words');
      eq(w.oneRow(row), -1, name + ': and its picture is the whole it was cut as');
      if (h === wholeHash) { await w.Pool.poolAdd(row, null); const ch = w.pages.gold.charms[0]; eq([ch.members.length, ch.hash], [whole.members.length, wholeHash], name + ': cut whole again, exactly as it was'); }   // (a pool row stores the page's own hash of the piece it cut: the indexer's is only read defensively, for the words and the picture)
    }
    // the same line read before this page has read the design file: a pooled piece cannot be told from the folded charm there, so it is taken as made whole
    {
      const w = W(), row = lineOf(w, '4199000023', '5300000023', 'Wolf Necklace Pendant', 1); row.poolIds = ['4199000023_5300000023_1'];
      w.c.B.pool.rows.set(row.poolIds[0], { poolId: row.poolIds[0], state: 'ready', charmHash: 'cb1cb29a-unknown', quantity: 1, sku: 'WOLF' });
      w.c.B.pool.sources.clear(); eq(w.oneRow(row), -1, 'a pooled line of a design this page has not read is whole');
      w.Pool.pinPooled(row); eq(row.spec.pair.oneBody, undefined, 'and its words are the old ones');
    }
    // a piece pooled AFTER this change holds the wolf body's hash: it reads back as the wolf
    {
      const w = W(), entry = w.entries.WOLF, s = await w.Pool.masterCharm(entry, null), solo = await w.Pool.soloBodies(s);
      eq(solo.length, 2, 'both bodies can be cut apart'); eq(solo[0].upAngle, 90, 'the wolf body: up 90'); ok(solo[1].upAngle !== 90 || w.c.B.pool.sources.get('charmnest/master/sku/WOLF.ai').charms[0].upAngle === 90, 'the other body is not given the wolf\'s');
      eq(await w.Pool.ensureBase(s, null, { charmHash: solo[0].hash }), solo[0], 'a pool row\'s hash of the wolf: the wolf'); eq(await w.Pool.ensureBase(s, null, { charmHash: wholeHash }), s.charms[0], 'an old record: the whole design');
      const drawn = []; w.c.P.thumbnail = (charm, px) => { drawn.push({ charm, px }); return 'thumb'; };   // (the stub's picture maker: what the card's drawing is handed is the real oneBody's)
      eq(await w.Pool.masterPreview(entry, null, false, { one: 0 }), 'thumb', 'the card picture of the wolf is drawn'); eq([Pair.bodiesOf(drawn[0].charm).length, Pair.isMismatched(drawn[0].charm), J(drawn[0].charm.bbox)], [1, false, J(bodies[0].bbox)], 'handed the wolf body alone: no moon, no Left / Right chips');
      eq(await w.Pool.masterPreview(entry, null, false), s.charms[0].thumb, 'the picture of the design as a whole (an earring line, a card of another design) is as it was'); eq(drawn.length, 1, 'and draws nothing more');
      eq(await w.Pool.masterPreview(entry, null, false, { one: 0 }), 'thumb', 'asked again: the same picture'); eq(drawn.length, 1, 'drawn once');
    }

  });

  /* ── 4 · the real file and the real page ── */
  const WOLF_AI = process.env.WOLF_AI || '/mnt/project-files/backups/catalog-1009/pass5-LIVING/files/charmnest/master/WOLF.ai';
  const MIS_AI = '/mnt/project-files/plans/pairs-1009/PAIRMASTER-samples/MISMATCHED_7134.ai';
  const RECS = '/mnt/project-files/backups/wolfonly-1013';
  const pwDir = process.env.PW_DIR || '/opt/node22/lib/node_modules/playwright/node_modules';
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { /* no browser */ }
  const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!chromium || !fs.existsSync(CHROME) || !fs.existsSync(WOLF_AI) || !fs.existsSync(MIS_AI)) console.log('  - the real WOLF.ai, the samples or the browser are not here: part 4 skipped');
  else await section('the real WOLF.ai on the real page: the card is the wolf alone (no pill, no drop-downs, its own size and up angle), the file cuts apart, a real mismatched pair is as before', async () => {
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  try {
    const url = p => 'https://firebasestorage.googleapis.com/v0/b/test-bucket/o/' + encodeURIComponent(p) + '?alt=media&token=t';
    const rec = sku => { const f = path.join(RECS, `live-masterGet-${sku.replace(' ', '_')}-real.json`); if (fs.existsSync(f)) return JSON.parse(fs.readFileSync(f, 'utf8')).entry; return { pair: TWO, sym: 'directional', sku, masterHash: 'e6e4ec00', masterName: 'MASTER', charmHash: '953c22e8', widthPt: 52.90980999999988, heightPt: 38.189799999999195, areaPt2: 718.75, members: 3, holes: 1, engravable: false, upAngle: 96.67900142191831, upSource: 'hole', aiPath: 'charmnest/master/WOLF.ai', indexedAt: 1791587214067 }; };
    st.blobs.set('charmnest/master/WOLF.ai', { buf: fs.readFileSync(WOLF_AI), generation: 7001, meta: { contentType: 'application/octet-stream' } });
    st.blobs.set('charmnest/master/MISMATCHED_7134.ai', { buf: fs.readFileSync(MIS_AI), generation: 7002, meta: { contentType: 'application/octet-stream' } });
    for (const sku of FOUR) st.docs.set('Charm_Master_Index/' + sku, Object.assign({}, rec(sku), { sku, aiPath: 'charmnest/master/WOLF.ai', aiUrl: url('charmnest/master/WOLF.ai'), thumbPath: null, thumbUrl: null }));
    st.docs.set('Charm_Master_Index/MISMATCHED_7134', { sku: 'MISMATCHED_7134', pair: TWO, sym: 'directional', masterHash: 'aa11', masterName: 'MASTER', charmHash: 'bb22', widthPt: 60, heightPt: 40, areaPt2: 800, members: 4, holes: 0, engravable: false, upAngle: 90, upSource: 'drawn', aiPath: 'charmnest/master/MISMATCHED_7134.ai', aiUrl: url('charmnest/master/MISMATCHED_7134.ai'), thumbPath: null, thumbUrl: null, labelSource: 'text', indexedAt: 1791587214067 });
    const before = JSON.stringify([...st.docs.entries()].filter(([k]) => k.startsWith('Charm_Master_Index/')));
    const ctx = await browser.newContext({ viewport: { width: 1500, height: 1000 } });
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: '' }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(/^https:\/\/firebasestorage\.googleapis\.com\//, r => { const u = new URL(r.request().url()); const m = /\/o\/(.+)$/.exec(u.pathname); const b = st.blobs.get(m ? decodeURIComponent(m[1]) : ''); if (!b) return r.fulfill({ status: 404, body: 'no blob' }); r.fulfill({ status: 200, contentType: b.meta.contentType || 'application/octet-stream', headers: { 'Access-Control-Allow-Origin': '*', 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: b.buf }); });
    await ctx.addInitScript(() => { window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) { /* none */ } });
    const page = await ctx.newPage(), errs = [];
    page.on('pageerror', e => errs.push(e.message));
    await page.goto(sorterOrigin + '/charm-nest-1.html');
    await page.waitForFunction(() => window.CN && window.CN.S && CN.S.cloud.ok !== null);
    await page.evaluate(() => { try { window.CharmNestThumbs && CharmNestThumbs.clear(); } catch (_) { /* none */ } CN.setMode('master'); });
    await page.waitForSelector('#mGrid .skuTile[data-sku="WOLF"]', { timeout: 60000 });
    await page.waitForFunction(() => [...document.querySelectorAll('#mGrid .skuTile')].every(t => t.querySelector('[data-preview-sku] img') || /unavailable/.test(t.textContent)), null, { timeout: 90000 });
    // the card, as a person reads it
    const tile = sku => page.evaluate(s => {
      const t = document.querySelector(`#mGrid .skuTile[data-sku="${s}"]`); if (!t) return null;
      const img = t.querySelector('[data-preview-sku] img'), host = t.querySelector('[data-preview-sku]');
      return { text: t.innerText.replace(/\s+/g, ' ').trim(), pill: !!t.querySelector('.pill'), selects: [...t.querySelectorAll('select')].map(x => x.options[x.selectedIndex].text), up: t.querySelector('input[data-up]').value, one: host.dataset.one == null ? null : host.dataset.one, src: img ? img.src : null, hasImg: !!img, dl: !!t.querySelector('[data-dl]') };
    }, sku);
    const ink = src => page.evaluate(async s => {   // the picture's ink: its box, and how much near-black (a Left / Right chip is a black pill)
      const im = new Image(); im.src = s; await im.decode(); const c = document.createElement('canvas'); c.width = im.naturalWidth; c.height = im.naturalHeight; const x = c.getContext('2d'); x.drawImage(im, 0, 0);
      const d = x.getImageData(0, 0, c.width, c.height).data, bg = [d[0], d[1], d[2]]; let x0 = 1e9, y0 = 1e9, x1 = -1, y1 = -1, dark = 0, inkPx = 0;
      for (let y = 0; y < c.height; y++) for (let xx = 0; xx < c.width; xx++) { const i = (y * c.width + xx) * 4, df = Math.abs(d[i] - bg[0]) + Math.abs(d[i + 1] - bg[1]) + Math.abs(d[i + 2] - bg[2]); if (df > 60) { inkPx++; if (xx < x0) x0 = xx; if (xx > x1) x1 = xx; if (y < y0) y0 = y; if (y > y1) y1 = y; } if (d[i] + d[i + 1] + d[i + 2] < 120) dark++; }
      let solid = 0;   // (a Left / Right chip is a solid black pill: a pixel whose whole 3 x 3 neighbourhood is near-black; a drawn line is one or two pixels wide)
      const isDark = (xx, y) => { const i = (y * c.width + xx) * 4; return d[i] + d[i + 1] + d[i + 2] < 150; };
      for (let y = 2; y < c.height - 2; y++) for (let xx = 2; xx < c.width - 2; xx++) { let all = true; for (let dy = -1; dy <= 1 && all; dy++) for (let dx = -1; dx <= 1; dx++) if (!isDark(xx + dx, y + dy)) { all = false; break; } if (all) solid++; }
      return { w: x1 - x0 + 1, h: y1 - y0 + 1, W: c.width, H: c.height, dark, inkPx, solid };
    }, src);
    const w = await tile('WOLF'), m = await tile('MISMATCHED_7134');
    ok(w && m, 'the WOLF card and the MISMATCHED_7134 card are in the grid'); ok(w.hasImg && m.hasImg, 'both pictures are drawn');
    ok(!/Left \+ Right pair/.test(w.text) && !w.pill, 'WOLF: no "Left + Right pair"'); eq(w.selects, [], 'WOLF: no "Left: not set" / "Right: not set" drop-downs');
    ok(!/both together/.test(w.text), 'WOLF: not "both together"'); ok(/\b4 SKUs\b/.test(w.text) && /WOLF BEADY/.test(w.text) && /WOLF CHARM/.test(w.text) && /WOLF_6691/.test(w.text), 'WOLF: still one card with its four SKUs');
    const mm = /([\d.]+) × ([\d.]+) mm · (\d+) holes?/.exec(w.text); ok(mm, 'WOLF: a size line'); const dd = Pair.onlyBodyOf('WOLF');
    ok(Math.abs(+mm[1] - dd.widthPt * 25.4 / 72) < .06 && Math.abs(+mm[2] - dd.heightPt * 25.4 / 72) < .06, `WOLF: the wolf's own size (${mm[1]} x ${mm[2]} mm), not 18.7 x 13.5 both together`); eq(+mm[3], 0, 'WOLF: the wolf\'s holes');
    eq(w.up, '90', 'WOLF: up 90 (the wolf as drawn), not the moon\'s 97'); eq(w.one, '0', 'WOLF: the tile asks for the picture of body 0'); ok(w.dl, 'the .ai download is still there');
    ok(/Left \+ Right pair/.test(m.text) && m.pill, 'MISMATCHED_7134: a real mismatched pair still says "Left + Right pair"'); eq(m.selects.length, 2, 'MISMATCHED_7134: still its two drop-downs'); ok(/both together/.test(m.text) && m.one === null, 'MISMATCHED_7134: both together, no one-body picture');
    if (process.env.WO_SAVE) for (const [k, t] of [['wolf', w], ['mis', m]]) fs.writeFileSync(path.join(process.env.WO_SAVE, `wolf-only-${k}.png`), Buffer.from(String(t.src).replace(/^data:image\/png;base64,/, ''), 'base64'));
    const wi = await ink(w.src), mi = await ink(m.src);
    if (process.env.WO_DEBUG) console.log('ink wolf', JSON.stringify(wi), 'mis', JSON.stringify(mi));
    ok(wi.w / wi.h > .85 && wi.w / wi.h < 1.2, `the WOLF picture is the wolf (about square, ${(wi.w / wi.h).toFixed(2)}; the moon is 0.76, both together are wider)`);
    ok(mi.w / mi.h > 1.2, `a real mismatched pair is still drawn as both bodies side by side (${(mi.w / mi.h).toFixed(2)})`); ok(wi.solid < 40 && mi.solid > 100, `no Left / Right chips on the WOLF picture (solid black ${wi.solid} px against ${mi.solid} px of the two chips of the pair)`);
    ok(wi.inkPx > 200, 'and it is not empty');
    // the real file, cut apart by the real code
    const real = await page.evaluate(async () => {
      const e = Master.entryFor('WOLF'), s = await Pool.masterCharm(e, null), c = s.charms[0], bodies = CharmNestPair.bodiesOf(c), solo = await Pool.soloBodies(s);
      const named = CharmNestPair.onlyBodyOf(e), line = { receiptId: '4199000030', transactionId: '1', quantity: 1, form: 'necklace', spec: { designSku: 'WOLF CHARM', quantity: 1, pair: { earring: false, single: false, mismatched: false, twoBodies: true } } };
      const ear = { receiptId: '4199000031', transactionId: '1', quantity: 1, form: 'earrings', spec: { designSku: 'WOLF CHARM', quantity: 1, pieceCount: 2, pair: { earring: true, kind: 'mismatched', mismatched: true, sides: ['L', 'R'] } } };
      const sol = CharmNestPair.soloOf(line, c), pieces = CharmNestPair.piecesFor(line, c), earPieces = CharmNestPair.piecesFor(ear, c);
      const g = x => x && { hash: x.hash, w: x.widthPt, h: x.heightPt, members: x.members.length, holes: CharmNestPDF.cutLinesOf(x).length, up: x.upAngle };
      const tops = bodies.map(b => [...new Set(b.members.map(m => m.parent != null ? m.parent : m.index).filter(t => t != null))]);
      return { n: bodies.length, mismatched: CharmNestPair.isMismatched(c), fold: g(c), solo: solo && solo.map(g), named: { w: named.widthPt, h: named.heightPt, holes: named.holes, up: named.upAngle }, tops, sol: sol && { index: sol.index, rule: sol.rule }, pieces: pieces.map(p => [p.side, p.bodyIndex, p.mirror]), earPieces: earPieces.map(p => [p.side, p.bodyIndex]), alike: CharmNestPair.alikeDesign(c) };
    });
    if (process.env.WO_DEBUG) console.log('real', JSON.stringify(real));
    eq([real.n, real.mismatched], [2, true], 'the real WOLF.ai draws two bodies (the wolf and the moon)'); ok(real.solo && real.solo.length === 2, 'the two bodies sit in drawing groups of their own, so the wolf can be cut alone');
    ok(real.tops[0].every(t => !real.tops[1].includes(t)), 'the wolf\'s groups are not the moon\'s');
    eq(real.solo[0].members, 1, 'the wolf body is its own outline'); ok(real.solo[0].members < real.fold.members, 'less than the folded charm');
    ok(Math.abs(real.solo[0].w - real.named.w) < .05 && Math.abs(real.solo[0].h - real.named.h) < .05, `the table's size is the real wolf's (${real.solo[0].w.toFixed(2)} x ${real.solo[0].h.toFixed(2)} pt)`);
    eq([real.solo[0].holes, real.solo[0].up], [real.named.holes, real.named.up], 'the table\'s holes and up angle are the real wolf\'s'); ok(real.solo[1].w < real.fold.w && real.solo[0].hash !== real.solo[1].hash && real.solo[0].hash !== real.fold.hash, 'three different hashes: the wolf, the moon, both together');
    ok(real.solo[1].holes === 1, 'the moon carries the record\'s one hole and its 97 degrees (that is why the card needed its own numbers)');
    eq(real.sol, { index: 0, rule: "a person's word" }, 'the real design on a necklace line: the wolf'); eq(real.pieces, [[null, 0, false]], 'one piece, no side, never mirrored'); eq(real.earPieces, [['L', 0], ['R', 1]], 'the real design on an earring line: the wolf and the moon');
    // the records the page read are untouched by all of it
    ok(JSON.stringify([...st.docs.entries()].filter(([k]) => k.startsWith('Charm_Master_Index/'))) === before, 'no master record was written');
    ok(!st.calls.some(c => c.op && /masterPatch|masterPutIndex|masterRemove|masterPutFile|startMaster/.test(c.op)), 'and the page asked for no master write');
    ok(errs.length === 0, 'no page error: ' + errs.join(' | '));
    await ctx.close();
  } finally { await browser.close(); srv.close(); }
  });
  if (failed.length) { console.log(`wolf-only: ${failed.length} part(s) FAILED, ${n} checks passed`); process.exit(1); }
  console.log(`wolf-only: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
