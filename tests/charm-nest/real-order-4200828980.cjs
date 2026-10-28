// BFREAL (Paul, 12 Oct 2026 17:50 UTC): order 4200828980, listing 870782383, SKU BIRTHFLOWER_NECKLACE_DISC-9, run end to end with the REAL names of its screenshot, not guessed ones:
//   options  "Birth Flower" = "Marigold"   and   "Length/Engrave:1 Side/2 Sides?" = "Gold - 18\" - 2 sides"   (Etsy ids on both), personalisation "Typewriter font" / blank / "Georgia Ben Cora Smith" / "October 26 2020",
//   station metal GF 14/20, the listing's SKU table not loaded, the real master entries of the 30 BIRTHFLOWER_NECKLACE_DISC designs (the library index of 9 Oct 2026).
// His screenshot still said: head card "NEEDS A DECISION  Options: option "Birth Flower: Marigold" not mapped - Etsy's SKU list for listing 870782383 is not loaded yet", no engraving anywhere.
// What this proves, with the page's own code (charm-nest-orders.js, engrave-faces.js, timeline-ui.js; the bridge's own functions, sliced, over stand-ins):
//   1  the intake: no question of any kind, the design is DISC-9, the engraving is BACK with the buyer's name and date (mode back, source option, sides 2, no person asked), the font Typewriter -> Crimson Text, "Typewriter font"
//      is not engraved; the same for all 12 flowers x all 18 values of the second option x flower first or last x three spellings of its name
//   2  the head card sentence is a RECORD: the question was written to the order's timeline when the line was first read, and only a person's answer closed it, so a question the intake no longer asks stayed on the card for
//      good (a hard reload too). blockerOf(events, live) lets go of a question of a kind the intake derives when the page's own reading of THAT line no longer raises it, and of nothing else
//   3  liveAsks (the bridge) answers true / false / not known per line, and says "not known" whenever it cannot tell (library not loaded, line not held here, no reading)
//   4  the Engrave tab: ONE back job from the reading, state ready, nothing paid for, nothing approved, no front job; "1 side" for the same line is cut plain
//   5  the order window says the engraving at once (chip "Back engraving", the words, the font, the sides, "Waiting for the Engrave tab") while Engrave holds nothing; the plain default back stays quiet
//   node tests/charm-nest/real-order-4200828980.cjs
'use strict';
const assert = require('assert/strict'), path = require('path'), fs = require('fs'), vm = require('vm');
const root = path.join(__dirname, '../..');
const transport = require.resolve(path.join(root, 'netlify/functions/_etsyMailAnthropic.js'));
require.cache[transport] = { id: transport, filename: transport, loaded: true, exports: {} };
const O = require(path.join(root, 'charm-nest-orders.js'));
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const Seals = require(path.join(root, 'charm-nest-engraving-seals.js'));
const F = require('./pairs-fixtures.cjs');

let passed = 0, failed = 0; const t = async (name, fn) => { try { await fn(); passed++; console.log('  ok  ' + name); } catch (e) { failed++; console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 8).join('\n      ')); } };
const J = x => (x === undefined ? undefined : JSON.parse(JSON.stringify(x)));

/* ── the real master: BIRTHFLOWER_NECKLACE_DISC-0..29 as the library index of 9 Oct 2026 holds them (members, holes, size; no keep-out layers) ── */
const DISC = [[4, 48.354, 56.288], ...Array.from({ length: 29 }, () => [2, 48.354, 56.288])];   // (-0 is the carnation: three engraved flowers; every other month one)
DISC[9] = [2, 48.354, 56.287];
const lib = new Map(DISC.map(([members, w, h], i) => ['BIRTHFLOWER_NECKLACE_DISC-' + i, { sku: 'BIRTHFLOWER_NECKLACE_DISC-' + i, members, holes: 0, backKeepOut: [], widthPt: w, heightPt: h, engravable: true, engravableBy: 'index' }]));
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, listingSkus: {}, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});

/* ── the real order ── */
const TITLE = 'Birth Flower Necklace, Gold Birth Flower Necklace, Silver Birth Flower Necklace, Gold Flower Necklace, Birth Month Necklace, Bridesmaid Gift';
const SIDES = 'Length/Engrave:1 Side/2 Sides?', VALUE = 'Gold - 18" - 2 sides', WORDS = 'Typewriter font\n\nGeorgia Ben Cora Smith\nOctober 26 2020';
const V = (name, value, p, v) => ({ name, value, propertyId: p || '', valueId: v || '' });
const ORDER = { receiptId: '4200828980', updateTs: 1, staffNote: '', buyerMessage: '' };
const line = (extra) => Object.assign({ transactionId: '5200000001', listingId: '870782383', productId: '8800000001', sku: 'BIRTHFLOWER_NECKLACE_DISC-9', title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [WORDS],
  variations: [V('Birth Flower', 'Marigold', '514', '209'), V(SIDES, VALUE, '513', '101')] }, extra || {});
const read = (extra, c) => O.interpretLine(ORDER, line(extra), c || ctx());

/* the head card's sentence, exactly as the screenshot has it (what Review writes to the timeline when the OLD reading asked: bridge KIND_WORDS + problemText) */
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const srcLine = start => { const l = bridgeSrc.split('\n').find(x => x.startsWith(start)); assert.ok(l, 'bridge line ' + start); return l; };
const cut = (from, to) => { const a = bridgeSrc.indexOf(from), b = bridgeSrc.indexOf(to, a + 1); assert.ok(a > 0 && b > a, 'slice ' + from.trim().slice(0, 40)); return bridgeSrc.slice(a, b); };
const textCtx = vm.createContext({ labelOf: x => x }); vm.runInContext(srcLine('  const KIND_WORDS = ') + '\n' + srcLine('  function problemText(p) {') + '\nthis.sentence = p => `${KIND_WORDS[p.kind]}: ${problemText(p)}`;', textCtx);
const OLD_PROBLEM = { kind: 'needsMapping', listingId: '870782383', optionName: 'Birth Flower', optionValue: 'Marigold', title: 'Birth Flower Necklace', why: "Etsy's SKU list for listing 870782383 is not loaded yet" };   // (what the module before BFWHY, e87289a, read for this very line: two of them, this the first)
const SCREENSHOT = 'Options: option "Birth Flower: Marigold" not mapped — Etsy\'s SKU list for listing 870782383 is not loaded yet';

const UI = (() => { const c = vm.createContext({ console, document: { getElementById: () => ({}) } }); vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'), c); return c.OrderTimelineUI; })();

(async () => {
console.log('BFREAL · order 4200828980 with its real names');

await t('1 the intake with the real names: no question, DISC-9, BACK engraving with the name and date, the font Typewriter -> Crimson Text, no person asked', () => {
  const sp = read(); assert.deepEqual(J(sp.problems), [], JSON.stringify(sp.problems));
  assert.equal(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-9'); assert.equal(sp.skuSource, 'transaction'); assert.equal(sp.material, 'gold'); assert.equal(sp.materialLabel, 'GF 14/20');
  assert.deepEqual(J(sp.options), [{ name: 'Birth Flower', value: 'Marigold', mapped: { field: 'design', value: 'BIRTHFLOWER_NECKLACE_DISC-9', source: 'sku:index' } }], 'the metal / length / sides drop-down is the metal option, the flower is settled by its own numbered SKU');
  const e = sp.engraving; assert.ok(e, 'a reading'); assert.equal(e.mode, 'back'); assert.equal(e.source, 'option'); assert.equal(e.needsPerson, false); assert.equal(e.front, null); assert.deepEqual(J(e.sides), { n: 2, art: 'index' });
  assert.deepEqual(e.back.lines, ['Georgia Ben Cora Smith', 'October 26 2020'], 'the font line and the blank line after it are not words'); assert.equal(e.back.fontWord, 'Typewriter'); assert.equal(sp.engraveCandidate, true);
  assert.equal(sp.fontText.id, 'crimson-text'); assert.equal(O.fontLine(sp), 'Font: Typewriter → Crimson Text'); assert.equal(O.pieceFont(sp, 0, null).id, 'crimson-text');
  const st = Faces.setupOf({ spec: sp }); assert.deepEqual([st.mode, st.faces, st.needsPerson, st.source], ['back', ['back'], false, 'option']); assert.equal(Faces.describe(st), 'Back: Georgia Ben Cora Smith / October 26 2020');
  // an older page without a table store at all, and a table that is loaded and empty, read the same
  for (const c of [ctx({ listingSkus: undefined }), ctx({ listingSkus: { '870782383': { products: [], at: Date.now() } } })]) assert.deepEqual(J(read({}, c).problems), []);
});

await t('2 all 12 flowers x all 18 values x flower first or last x three spellings of the drop-down\'s name x the shapes of the personalisation: the same reading every time (back for 2 sides, none for 1 side)', () => {
  const months = ['Carnation', 'Violet', 'Daffodil', 'Daisy', 'Lily of the Valley', 'Rose', 'Water Lily', 'Poppy', 'Morning Glory', 'Marigold', 'Chrysanthemum', 'Pointsetta'], values = [];
  for (const m of ['Gold', 'Silver', 'Rose']) for (const len of [16, 17, 18]) for (const s of [1, 2]) values.push(`${m}${m === 'Gold' ? ' - ' : '- '}${len}"${m === 'Gold' ? ' - ' : '- '}${s} side${s > 1 ? 's' : ''}`);
  assert.equal(values.length, 18); let n = 0;
  for (const nm of [SIDES, 'Length/Engrave:1 side/2 sides?', 'LENGTH/ENGRAVE:1 SIDE/2 SIDES?']) for (const [i, fl] of months.entries()) for (const val of values) for (const first of [true, false]) for (const pers of [[WORDS], ['Typewriter font', '', 'Georgia Ben Cora Smith', 'October 26 2020'], [WORDS.replace(/\n/g, '\r\n')]]) {
    const vars = [V('Birth Flower', fl, '514', String(200 + i)), V(nm, val, '513', '101')]; if (!first) vars.reverse(); const two = /2 side/.test(val);
    const sp = read({ sku: 'BIRTHFLOWER_NECKLACE_DISC-' + i, personalization: pers, variations: vars, metalKey: /silver/i.test(val) ? 'silver' : /rose/i.test(val) ? 'rose' : 'gold' }); n++;
    const w = nm + ' | ' + fl + ' | ' + val + ' | ' + first;
    assert.deepEqual(J(sp.problems), [], w); assert.equal(sp.designSku, 'BIRTHFLOWER_NECKLACE_DISC-' + i, w); assert.equal(sp.engraving.mode, two ? 'back' : 'none', w); assert.equal(sp.engraving.needsPerson, false, w); assert.equal(sp.fontText.id, 'crimson-text', w);
    if (two) assert.deepEqual(sp.engraving.back.lines, ['Georgia Ben Cora Smith', 'October 26 2020'], w);
  }
  assert.equal(n, 3 * 12 * 18 * 2 * 3);
  // what Etsy and the shop may write in the value: the dash, the quote and the space in any form
  for (const v of ['Gold – 18" – 2 sides', 'Gold - 18" - 2 sides', 'Gold - 18” - 2 sides', 'Gold - 18&quot; - 2 sides', 'Gold - 18&#34; - 2 sides', 'Gold - 18" - 2 Sides', 'GOLD - 18" - 2 SIDES']) {
    const sp = read({ variations: [V('Birth Flower', 'Marigold', '514', '209'), V(SIDES, v, '513', '101')] }); assert.deepEqual(J(sp.problems), [], v); assert.equal(sp.engraving.mode, 'back', v);
  }
});

await t('3 the design-front test passes for the REAL entry of the marigold disc and for all 30 of the family (art = members - outline - holes, not long): no miss to flag', () => {
  for (const [sku, e] of lib) { const f = O.frontArtOf(e); assert.equal(f.front, true, sku); assert.ok(f.art >= 1, sku); assert.equal(f.long, false, sku); }
  assert.equal(O.frontArtOf(lib.get('BIRTHFLOWER_NECKLACE_DISC-9')).art, 1); assert.ok(56.287 / 48.354 < 1.4);
});

await t('4 the head card\'s sentence is a RECORD: the old reading wrote it once, nothing but a person\'s answer closed it, so it stayed; blockerOf(events, live) lets go of it when the line no longer asks, and of nothing else', () => {
  assert.equal(textCtx.sentence(OLD_PROBLEM), SCREENSHOT, 'the words of the screenshot are exactly what Review wrote for the old reading');
  const ev = (type, at, extra) => Object.assign({ id: type + at, type, at, by: 'System', orderId: '4200828980', lineKey: '4200828980_5200000001' }, extra || {});
  const asked = ev('needsDecision', 5, { text: SCREENSHOT, data: { kind: 'needsMapping', custom: false, why: SCREENSHOT } });
  const events = [ev('arrived', 1, { lineKey: '' }), ev('pulled', 2), ev('interpreted', 3, { text: 'Read as BIRTHFLOWER_NECKLACE_DISC-9 · gold · 2 questions', data: { questions: ['needsMapping', 'needsMapping'] } }), asked, ev('placed', 9, { lineKey: '', text: 'On GF Sheet 1' })];
  const live = v => ({ asks: (lk, kind) => (lk === '4200828980_5200000001' ? (typeof v === 'function' ? v(kind) : v) : null) });
  // as it was and as it still is without the page's reading: the card says it
  let b = UI.blockerOf(events); assert.equal(b.label, 'Needs a decision'); assert.equal(b.text, SCREENSHOT);
  assert.equal(UI.blockerOf(events, null).text, SCREENSHOT); assert.equal(UI.blockerOf(events, {}).text, SCREENSHOT, 'a live with no asks says nothing');
  // the page reads the line now: it no longer raises the question, so it is not what holds the order
  assert.equal(UI.blockerOf(events, live(false)), null, 'a question its line no longer raises is no blocker');
  assert.equal(UI.blockerOf(events, live(true)).text, SCREENSHOT, 'a question its line still raises stays');
  assert.equal(UI.blockerOf(events, live(null)).text, SCREENSHOT, 'not known: the record stands');
  assert.equal(UI.blockerOf(events, { asks: () => false }), null, 'false for every line: nothing');
  // only the kinds the intake derives, only a line the page knows, never a hold, never a custom order's or the engraving's question
  for (const kind of ['fontMissing', 'notRepresentable', 'orderChanged', 'flipFailed', 'customOrder']) assert.equal(UI.blockerOf(events.map(e => e === asked ? Object.assign({}, e, { data: { kind } }) : e), live(false)).text, SCREENSHOT, kind + ' is not the intake\'s to withdraw');
  // (KEYNOENG, 17 Oct 2026: the engraving words' question IS withdrawn, but only when the page says the line is never engraved (a key: liveAsks answers false for that and null for everything else), and the record stands when it cannot tell)
  const words = e => e === asked ? Object.assign({}, e, { data: { kind: 'engraveWords' } }) : e;
  assert.equal(UI.blockerOf(events.map(words), live(false)), null, 'a design that is never engraved no longer asks for engraving words');
  assert.equal(UI.blockerOf(events.map(words), live(null)).text, SCREENSHOT, 'engraving words: not known, the record stands');
  assert.equal(UI.blockerOf(events.map(words)).text, SCREENSHOT, 'engraving words: without the page\'s reading, the record stands');
  assert.equal(UI.blockerOf(events.map(e => e === asked ? Object.assign({}, e, { data: { kind: 'needsMapping', custom: true } }) : e), live(false)).text, SCREENSHOT, 'a custom order\'s card keeps its record');
  assert.equal(UI.blockerOf(events.map(e => e === asked ? Object.assign({}, e, { lineKey: '' }) : e), live(false)).text, SCREENSHOT, 'a question with no line is not withdrawn');
  for (const kind of ['needsMaterial', 'needsMapping', 'unmatchedSku', 'blockedSku', 'missingSize']) assert.equal(UI.blockerOf(events.map(e => e === asked ? Object.assign({}, e, { data: { kind } }) : e), live(false)), null, kind);
  const held = [...events, ev('held', 10, { text: 'Held by Rita', data: { reason: 'waiting for the buyer' } })]; assert.equal(UI.blockerOf(held, live(false)).label, 'On hold', 'a hold is nobody\'s reading to withdraw');
  // two lines: one question gone, one still asked: the card names the one that is still asked
  const other = ev('needsDecision', 6, { lineKey: '4200828980_5200000002', text: 'Options: option "Size: L" not mapped', data: { kind: 'needsMapping' } });
  const two = [...events, other], mixed = { asks: (lk) => (lk === '4200828980_5200000002' ? true : false) };
  assert.equal(UI.blockerOf(two, mixed).text, 'Options: option "Size: L" not mapped'); assert.equal(UI.blockerOf(two, { asks: () => false }), null);
  // a line with two kinds of question, the later one gone and the earlier one still asked
  const mat = ev('needsDecision', 4, { text: 'Options · metal: needs material (none)', data: { kind: 'needsMaterial' } });
  assert.equal(UI.blockerOf([mat, asked], live(k => k === 'needsMaterial')).text, 'Options · metal: needs material (none)');
  // a person's answer still closes it, with or without the page's reading, and an answer to a question the page no longer asks clears no other line's
  const decided = [...events, ev('decided', 11, { by: 'Rita', text: 'Decided by Rita · Options', data: { kind: 'needsMapping' } })];
  assert.equal(UI.blockerOf(decided), null); assert.equal(UI.blockerOf(decided, live(true)), null);
  assert.equal(UI.blockerOf([...two, ev('decided', 12, { by: 'Rita', data: { kind: 'needsMapping' } })], { asks: lk => lk === '4200828980_5200000002' ? true : false }).text, 'Options: option "Size: L" not mapped', 'the answer was to the line the page had let go of');
  // the card itself (nowStamps): the sentence is the blocker with no live reading, and gone with it
  const withOut = UI.nowStamps(events, {}), withLive = UI.nowStamps(events, { live: live(false) });
  assert.ok(withOut.blocker && /Needs a decision/.test(withOut.recent) && withOut.recent.includes('Marigold'), withOut.recent);
  assert.equal(withLive.blocker, null); assert.ok(!/Needs a decision|not mapped/.test(withLive.recent), withLive.recent);
});

await t('5 liveAsks (the bridge\'s own function): true / false per line from the page\'s reading, null whenever it cannot tell', () => {
  const body = cut('  function liveAsks(r) {', '  function nowOf(r) {');
  const make = (entries, rows) => { const c = vm.createContext({ B: { master: { entries: new Map(entries) } }, Orders: { rows: () => rows }, tryDo: (f, d) => { try { return f(); } catch (_) { return d; } }, Array, Map, Set, String, Object }); vm.runInContext(body + '\nthis.liveAsks = liveAsks;', c); return c.liveAsks; };
  const sp0 = read(), key = '4200828980_5200000001', row = (extra) => Object.assign({ key, order: { receiptId: '4200828980' }, line: line(), spec: sp0, problems: [], state: 'pulled' }, extra || {});
  const lib1 = [['X', {}]];
  const r = row(); const live = (rr, rows, entries) => make(entries || lib1, rows || [])(rr);
  assert.equal(live(r, [r]).asks(key, 'needsMapping'), false, 'read clean: it does not ask'); assert.equal(live(r, [r]).asks(key, 'needsMaterial'), false);
  assert.equal(live(r, []).asks(key, 'needsMapping'), false, 'a line the page holds only from the records (the order window\'s row) counts too');
  assert.equal(live(r, [r]).asks('4200828980_5200000009', 'needsMapping'), null, 'another line of the order is not known');
  const ask = row({ spec: Object.assign({}, sp0, { problems: [{ kind: 'needsMapping', optionName: 'A', optionValue: 'b' }] }) }); assert.equal(live(ask, [ask]).asks(key, 'needsMapping'), true); assert.equal(live(ask, [ask]).asks(key, 'missingSize'), false);
  const pooling = row({ problems: [{ kind: 'unmatchedSku' }] }); assert.equal(live(pooling, [pooling]).asks(key, 'unmatchedSku'), true, 'a question the pooling step raised on the row');
  assert.equal(live(row({ spec: Object.assign({}, sp0, { problems: [{ kind: 'needsMaterial' }] }), materialOverride: 'gold' }), []).asks(key, 'needsMaterial'), false, 'a person\'s metal answers it');
  assert.equal(live(r, [r], []), null, 'the library is not loaded: not known'); assert.equal(live(row({ loading: true }), []), null); assert.equal(live(null, []), null);
  assert.equal(live(row({ spec: { quantity: 1 } }), []).asks(key, 'needsMapping'), null, 'a reading that failed (no problems list) says nothing'); assert.equal(live(row({ spec: null }), []).asks(key, 'needsMapping'), null);
  assert.equal(live(row({ state: 'gone' }), []).asks(key, 'needsMapping'), null);
  assert.equal(live(r, [r, row({ order: { receiptId: '1' }, key: 'x' })]).asks('x', 'needsMapping'), null, 'a line of another order is not this order\'s');
});

/* ── the page's own Engrave pipeline over stand-ins: no browser, no network, nothing paid for ── */
const W = F.world({ orders: [{ rid: F.rid(4), lines: [{ n: 40, kind: 'single', on: 'sh-gf1' }] }], sheets: [{ id: 'sh-gf1', metal: 'gold', n: 1 }] });
const SLICE1 = cut('  const SIDES = ', '  function setNone(job'), SLICE2 = cut("  /** The line's effective engraving setup", '  let previewRecovery'), SLICE3 = cut("  /** A line whose setup asks for a front that has no front job yet", '  let classifyPass'), SLICE4 = cut('  function revokeBacks(job)', '  /* ── 7.5 · the review controls');
function harness(rows, o = {}) {
  const jobs = new Map(), asked = [], events = [], apiCalls = [], pool = new Map(W.pool.map(p => [p.poolId, p])), byKey = new Map(rows.map(r => [r.key, r]));
  const win = { CharmNestEngraveSides: Sides, CharmNestPair: Pair, CharmNestEngraveFaces: Faces };
  const c = {
    window: win, items: () => jobs, B: { pool: { rows: pool }, orders: { byKey }, run: null }, Pool: { charmOf: id => { const r = pool.get(id); return r ? F.charmOf(r.sku) : null; }, update: async () => { apiCalls.push('poolUpdate'); } }, Master: { entryFor: sku => F.entryOf(sku) }, CNEngravingSeals: Seals, DECIDED: ['approved', 'written', 'skipped'],
    O: { FINISHED_LINE: new Set(['committed', 'gone', 'cancelled']), engravingNote: null }, S: { settings: { engraveConfidence: .65 } }, render() {}, agent() {}, Date, Promise, Map, Set, WeakMap, Array, Object, String, Number, JSON, Math, RegExp, Error,
    Review: { add() {}, remove() {}, render() {} }, RunCtl: { poke() {}, save: async () => {} }, Session: { schedule() {} }, Orders: { render() {}, rows: () => rows, lineRecord: r => [r.key, { key: r.key }] },
    TL: { line: (row, type, d) => { events.push({ row, type, id: d && d.id, text: d && d.text, data: d && d.data }); }, hash: s => String(s).length }, sheets: [], allSheets: () => [], refreshBacks() {}, backQueue: Promise.resolve(), toast() {}, api: async (fn, body) => { apiCalls.push(body); return {}; }, goes() {}, EG_TAB: x => x, showDecided() {}, CNListActivity: { touch() {} }, humanAct() {}, syncEditedBack: async () => {},
    faceWord: j => (j && j.face === 'front' ? 'front' : 'back'), employeeName: () => 'Paul', needEmployee: async () => 'Paul', charmFor: () => null,
    setReady(job) { job.state = 'ready'; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'ready', text: job.text, approved: false }); return job; },
    toWords(job, why) { job.state = 'words'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: true, state: 'words', text: job.text, approved: false, reason: why }); return job; },
    setNone(job, why) { job.state = 'none'; job.reason = why; job.row.engrave = Object.assign({}, job.row.engrave, { needed: false, state: 'none', reason: why, approved: true }); return job; },
    skip: async () => { throw new Error('nothing is skipped'); }, agentCall: async (mode, ask) => { asked.push(ask); throw new Error('a paid reading was asked for'); },
  };
  vm.createContext(c); for (const s of [SLICE1, SLICE2, SLICE3, SLICE4]) vm.runInContext(s, c);
  return { ctx: c, jobs, asked, events, apiCalls, byKey };
}
const rowFor = sp => { const o = W.orders[0], l = o.lines[0]; return { key: F.lineKey(o.rid, F.tx(40)), poolIds: l.pieces.map(p => p.poolId), order: { receiptId: o.rid }, line: { transactionId: F.tx(40), sku: l.sku, title: 'Birth Flower Necklace' }, spec: sp, state: 'pooled' }; };

await t('6 the Engrave tab takes the line up: ONE back job from the reading, ready, with the name and date and the font word, nothing paid for, nothing approved, no front job; "1 side" for the same line is cut plain', async () => {
  const sp = read(), row = rowFor(sp), h = harness([row]); await h.ctx.classify(row);
  assert.equal(h.asked.length, 0, 'no paid reading'); assert.deepEqual([...h.jobs.keys()], [row.key], 'one job, the back, under the line\'s own key'); const j = h.jobs.get(row.key);
  assert.equal('face' in j, false); assert.equal(j.state, 'ready'); assert.equal(j.text, 'Georgia Ben Cora Smith\nOctober 26 2020'); assert.equal(j.source, 'option'); assert.equal(j.requests.side, 'back'); assert.equal(j.requests.font, 'Typewriter'); assert.equal(j.faceFontWord, 'Typewriter');
  assert.equal(j.approvedAt == null, true); assert.equal(row.engrave.approved, false); assert.equal(row.engrave.state, 'ready'); assert.equal(h.apiCalls.length, 0, 'nothing written anywhere by the reading');
  assert.match(h.events.find(e => e.type === 'engraveNeeded').text, /^Back engraving: “Georgia Ben Cora Smith \/ October 26 2020”/);
  const one = read({ variations: [V('Birth Flower', 'Marigold', '514', '209'), V(SIDES, 'Gold - 18" - 1 side', '513', '101')] }), r1 = rowFor(one), h1 = harness([r1]); await h1.ctx.classify(r1);
  assert.equal(h1.asked.length, 0); const j1 = h1.jobs.get(r1.key); assert.equal(j1.state, 'none'); assert.match(j1.reason, /^Buyer chose 1 side; words not engraved: “Georgia Ben Cora Smith \/ October 26 2020”$/); assert.equal(r1.engrave.approved, true);
  // the piece itself: its pool ids and its sheet are exactly what they were (the pipeline never reads or writes a placement)
  assert.deepEqual(J(row.poolIds), J(rowFor(sp).poolIds));
});

/* ── the order window's facts: the reading, said at once ── */
const body = cut('  function engReadingOf(r) {', "  /** The order's facts, gathered for OrderFacts");
const say = (r, extra) => { const c = vm.createContext(Object.assign({ window: { CharmNestEngraveFaces: Faces }, O, tryDo: (f, d) => { try { return f(); } catch (_) { return d; } }, esc: s => String(s == null ? '' : s).replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch])), String, Number }, extra || {})); vm.runInContext(body + '\nthis.engReadingOf = engReadingOf;', c); return c.engReadingOf(r); };
const rowOf = (sp, extra) => Object.assign({ key: 'k', spec: sp, line: line(), order: ORDER, engrave: null }, extra || {});
await t('7 the order window says the engraving at once: "Back engraving", the words, the font, the sides chosen and "Waiting for the Engrave tab"; "1 side" says no engraving; a plain default back, a person\'s "no engraving" and an Engrave record keep what they had', () => {
  const sp = read(), s = say(rowOf(sp)); assert.equal(s.chip, 'Back engraving');
  assert.equal(s.html, '<span>Back: Georgia Ben Cora Smith / October 26 2020<br>Font: Typewriter → Crimson Text<br>2 sides chosen: the design&#39;s own art is the front<br>Waiting for the Design &amp; Engrave tab</span>');
  const j = say(rowOf(sp, { engrave: { needed: false, state: 'none' } })); assert.ok(!/Waiting for the Design &amp; Engrave tab/.test(j.html), 'a record exists: it is not waiting');
  const one = read({ variations: [V('Birth Flower', 'Marigold', '514', '209'), V(SIDES, 'Gold - 18" - 1 side', '513', '101')] }), o = say(rowOf(one, { engrave: { needed: false, state: 'none', approved: true } }));
  assert.equal(o.chip, 'No engraving'); assert.match(o.html, /No engraving<br>1 side chosen: only the design&#39;s own art is engraved<br>Buyer chose 1 side; words not engraved: /);
  const none = say(rowOf(read({ variations: [V('Birth Flower', 'Marigold', '514', '209')], personalization: [] }))); assert.equal(none, null, 'nothing read, nothing said');
  const plain = say(rowOf(O.interpretLine(ORDER, line({ sku: 'X_PLAIN', title: 'Name Disc', variations: [V('Metal', 'Gold')], personalization: ['ANNA'] }), ctx()))); assert.equal(plain, null, 'a plain default back with no word of its own stays quiet, as it always did');
  const mine = say(rowOf(sp, { engravingOverride: { mode: 'none', by: 'Rita', at: 5 } })); assert.equal(mine, null, 'a person said no engraving: the facts say nothing the person did not');
  const edited = say(rowOf(sp, { engravingOverride: { mode: 'back', back: { text: 'ANNA' }, by: 'Rita', at: 5 } })); assert.match(edited.html, /^<span>Back: ANNA/);
  const both = say(rowOf(read({ sku: 'NOT_IN_MASTER' }))); assert.equal(both.chip, 'Front + back'); assert.match(both.html, /Front: words to confirm · Back: words to confirm/); assert.match(both.html, /do not say which go on the front/);
  assert.equal(say(rowOf(sp, { spec: null })), null); assert.equal(say(null), null);
});

await t('8 the page loads the new code: cache tokens keep every earlier suffix and hold this one', () => {
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  for (const [file, was] of [['charm-nest-timeline-ui.js', /-tn1-tb1(?:-[a-z0-9]+)*-br1(?:-[a-z0-9]+)*"/], ['charm-nest-bridge.js', /-sd1-sl1-or1-fa1(?:-[a-z0-9]+)*-br1(?:-[a-z0-9]+)*"/]]) { const m = new RegExp(file.replace('.', '\\.') + '\\?v=[^"]*"').exec(html); assert.ok(m, file); assert.ok(was.test(m[0]), file + ': ' + m[0].slice(-60)); }
  const orders = /charm-nest-orders\.js\?v=[^"]*"/.exec(html)[0]; assert.ok(/-bw1-sg1-hg1-bf1-sd1-sl1/.test(orders), 'the orders token of the earlier fixes is kept');
});

console.log(`\n${passed} passed, ${failed} failed`); if (failed) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
