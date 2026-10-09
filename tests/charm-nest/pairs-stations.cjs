// PAIRSTATIONS (pairs-1009, area 12): the stations and the people, offline. Paul, 9 Oct 2026: every earring pair (matching or mismatched) is one LEFT and
// one RIGHT piece per unit of quantity; a necklace of discs is n pieces of one group with no sides; a single earring or a pendant is one piece. Every
// station that shows or hands off an order or a piece must know that, and the efficiency counts per piece must not double or lose a pair.
//   A · station-live-order.js (the page half every station shares): counts, sides, group, the limit of 24, byte-identical for a line that is no pair,
//       with the intake's rules off (today), on (Paul's decision), absent (charm-nest-orders.js not loaded) and a mismatched line the pool still makes whole
//   B · the live layer on the server (_stationLive.js): cleanPiece / pairFields, expandPairs from the master's `pair`, and the door and the reader end to end
//       over the shared fake Firestore that REFUSES NESTED ARRAYS (tests/charm-nest/pairs-fixtures.cjs)
//   C · the page client (station-activity.js, jsdom): side / group / both kept, a pair never cut in half by the size limit or the 24 limit
//   D · the Laser summary (_laserSheetTime.js): an order whose pieces are on two sheets is ONE order; older records still sum
//   E · the pages: every station page that counts loads the intake's file and the shared helper, with one token each
//   JSDOM_DIR=<dir with jsdom> node tests/charm-nest/pairs-stations.cjs      (C is skipped, and says so, when jsdom is not found)
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict'), Module = require('module');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
let checks = 0; const ok = m => { checks++; console.log('  ok ' + m); };
const J = o => JSON.parse(JSON.stringify(o));
const read = f => fs.readFileSync(path.join(root, f), 'utf8');

/* ═══ the Etsy lines (raw transactions, as the station pages hold them) ═══ */
const RID = '3521000777';
const line = (o) => Object.assign({ receipt_id: RID, transaction_id: 5001, listing_id: 111222333, sku: 'CRAB_53423', title: 'Stud earrings', quantity: 1, variations: [] }, o);
const STUD = line({}), STUD2 = line({ transaction_id: 5002, quantity: 2, sku: 'SURFER_4264' });
const HOOP = line({ transaction_id: 5003, title: 'Huggie hoop earrings', sku: 'HUGGIE_3722', variations: [{ formatted_name: 'Size', formatted_value: 'Medium' }] });
const SINGLE = line({ transaction_id: 5004, title: 'Single stud earring', sku: 'FAIRY3' });
const NECK = line({ transaction_id: 5005, title: 'Dainty charm necklace', sku: 'LOVE_1', quantity: 2 });
const DISC3 = line({ transaction_id: 5006, title: 'Initial disc necklace', sku: 'INITIAL_8391', variations: [{ formatted_name: 'Number of Discs / Metal', formatted_value: '3 discs • gold' }] });
const MIS = line({ transaction_id: 5007, title: 'Mismatched stud earrings', sku: 'MISMATCHED_7134' });
const MIS_OPT = line({ transaction_id: 5008, title: 'Stud earrings', sku: 'MITTENS_1', variations: [{ formatted_name: 'Pair', formatted_value: 'Mismatched pair' }] });
const TITLE_ONLY = line({ transaction_id: 5009, title: 'Mismatched or matching studs', sku: 'CRAB_53423', variations: [{ formatted_name: 'Pair', formatted_value: 'Matching pair' }] });
const NOT_PAIRS = [SINGLE, NECK, DISC3];

/* ═══ the intake's contract (CharmNestOrders.pieceCountOf, lineSignals, lineMismatched, PIECE_RULES), as a stub; the real module is checked too when it has them ═══ */
function stubOrders(rules) {
  const R = Object.assign({ pairFormsMakeTwo: false, discsMakeN: false, mismatchedMakesTwo: false }, rules);
  const text = l => [l.title].concat((l.variations || []).map(v => v.formatted_value || v.value)).join(' ');
  const lineSignals = l => { const t = text(l || {}); return { says: /mis-?match/i.test(t), soldAs: /\bsingle\b/i.test(t) ? 'single' : /\b(earrings?|studs?|huggies|hoops?)\b/i.test(t) ? 'pair' : null, discs: (m => (m ? +m[1] : 0))(/(\d)\s*discs?/i.exec(t)) }; };
  const pieceCountOf = x => {
    const own = Math.floor(+x.pieceCount || +(x.spec && x.spec.pieceCount) || 0); if (own > 0) return own;
    const p = R.pairFormsMakeTwo || R.discsMakeN ? lineSignals(x) : null, q = Math.max(1, Math.round(+x.quantity || 1)); let per = 1;
    if (p && p.discs > 1 && R.discsMakeN) per = p.discs; else if (p && p.soldAs === 'pair' && R.pairFormsMakeTwo) per = 2;
    return q * per;
  };
  // piecesOf: [{ n, of, unit, side, bodyIndex }] as the intake lists them (an earring pair L R L R, a single earring the side its line names)
  const piecesOf = x => { const n = pieceCountOf(x), s = lineSignals(x), q = Math.max(1, Math.round(+x.quantity || 1)), t = text(x), named = /\bleft\b/i.test(t) ? 'L' : /\bright\b/i.test(t) ? 'R' : null;
    return Array.from({ length: n }, (_, i) => ({ n: i + 1, of: n, unit: Math.floor(i / (n / q)) + 1, side: s.soldAs === 'pair' && R.pairFormsMakeTwo ? (i % 2 ? 'R' : 'L') : s.soldAs === 'single' && q === 1 ? named : null, bodyIndex: 0 })); };
  return { PIECE_RULES: R, lineSignals, lineMismatched: l => lineSignals(l).says, pieceCountOf, piecesOf };
}

/* the HEAD helper's own pieces(), the oracle for a line that is no pair */
function oldPieces(list) {
  const out = []; let total = 0, i = 0;
  const clean = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, n), digits = (v, n) => String(v == null ? '' : v).replace(/\D/g, '').slice(0, n);
  const sizeOf = t => { for (const v of (t.variations || [])) { if (!/size/i.test(String(v.formatted_name || ''))) continue; const x = clean(v.formatted_value, 30).toLowerCase(); if (/^(medium|m)\b/.test(x)) return 'M'; return ''; } return ''; };
  for (const t of list) {
    i++; const q = Math.max(1, Math.floor(Number(t.quantity != null ? t.quantity : t.__qty)) || 1); total += q;
    const base = String(t.transaction_id || t.listing_id || i).replace(/[^\w.:-]/g, '_').slice(0, 30), label = clean(t.title, 60), sku = clean(t.sku || t.__sku, 60), listingId = digits(t.listing_id, 20), size = sizeOf(t);
    for (let k = 1; k <= q && out.length < 24; k++) { const p = { id: base + '-' + k, label }; if (sku) p.sku = sku; if (listingId) p.listingId = listingId; if (size) p.size = size; out.push(p); }
  }
  return { pieces: out, pieceCount: total };
}

/* ═══════════════════════ A · station-live-order.js ═══════════════════════ */
function helper(O) {
  const win = {}; if (O) win.CharmNestOrders = O;
  const told = []; win.StationActivity = { working: o => { told.push(J(o)); return true; }, idle: () => true, who: () => ({ person: 'Tess Welder' }), current: () => [], touch() {} };
  const ctx = vm.createContext({ window: win, document: { addEventListener() {} }, Date, JSON, Math, String, Number, Array, Object, Set, RegExp });
  vm.runInContext(read('station-live-order.js'), ctx);
  const S = win.StationLiveOrder; S.told = told; return S;
}
const sidesOf = ps => Array.prototype.map.call(ps, p => p.side || '-').join('');

function partA() {
  const ON = { pairFormsMakeTwo: true, discsMakeN: true, mismatchedMakesTwo: true };
  /* A1 · the rules off (the app today) and no intake file at all: every line is counted and told exactly as before */
  for (const [name, O] of [['rules off', stubOrders()], ['no charm-nest-orders.js', null], ['an older charm-nest-orders.js (no pieceCountOf)', {}]]) {
    const S = helper(O), lines = [STUD, STUD2, HOOP, SINGLE, NECK, DISC3, TITLE_ONLY];
    assert.deepEqual(J(S.pieces(lines, RID)), oldPieces(lines), name + ': pieces are the old pieces');
    assert.equal(S.units(lines), 1 + 2 + 1 + 1 + 2 + 1 + 1, name + ': parts are the Etsy quantity'); assert.equal(S.units(lines, t => t === STUD2), 2);
    for (const t of lines) { assert.equal(S.tag(t), '', name + ': no words added to ' + t.title); assert.deepEqual(J(S.sides(t, RID)), []); }
    // a mismatched line is told as the ONE glued piece the pool makes (its left and right drawn together), with its group
    const m = S.pieces([MIS, MIS_OPT], RID);
    assert.equal(m.pieceCount, 2); assert.deepEqual(J(m.pieces.map(p => [p.id, p.both, p.side, p.grp, p.of])), [['5007-1', true, null, RID + ':5007', null], ['5008-1', true, null, RID + ':5008', null]]);
    assert.equal(S.tag(MIS), 'Pair: Left + Right'); assert.equal(S.tag(MIS_OPT), 'Pair: Left + Right'); assert.equal(S.kind(MIS).mismatched, true); assert.equal(S.kind(MIS).kind, 'mismatched'); assert.equal(S.units([MIS]), 1);
    assert.equal(S.kind(TITLE_ONLY).mismatched, false, name + ': the listing title alone does not make a line mismatched');
  }
  ok('A1 rules off / no intake file: counts, pieces and words exactly as before; a mismatched line is one glued piece (both), group kept');

  /* A2 · Paul's rules on: an earring pair is a Left and a Right per unit; discs are n of one group; singles and necklaces as before */
  for (const [name, O] of [['stub', stubOrders(ON)]].concat((() => { try { const real = require(path.join(root, 'charm-nest-orders.js')); return typeof real.pieceCountOf === 'function' && real.PIECE_RULES ? [['the real charm-nest-orders.js', real]] : []; } catch (_) { return []; } })())) {
    if (O.PIECE_RULES) Object.assign(O.PIECE_RULES, ON);
    const S = helper(O);
    const one = S.pieces([STUD], RID);
    assert.equal(one.pieceCount, 2, name); assert.deepEqual(J(one.pieces.map(p => [p.id, p.side, p.grp, p.of, p.n])), [['5001-1', 'L', RID + ':5001', 2, 1], ['5001-2', 'R', RID + ':5001', 2, 2]]);
    assert.equal(S.units([STUD]), 2); assert.equal(S.tag(STUD), 'Pair: Left + Right'); assert.deepEqual(J(S.kind(STUD)), { kind: 'pair', mismatched: false, sided: true, pieces: 2 });
    const two = S.pieces([STUD2], RID);
    assert.equal(two.pieceCount, 4); assert.equal(sidesOf(two.pieces), 'LRLR'); assert.deepEqual(J(two.pieces.map(p => p.n)), [1, 2, 3, 4]); assert.ok(two.pieces.every(p => p.of === 4 && p.grp === RID + ':5002'));
    assert.equal(S.tag(STUD2), '2 pairs: Left + Right each'); assert.equal(S.units([STUD2]), 4);
    assert.equal(S.units([HOOP]), 2, name + ': a huggie hoop pair is two');
    // a line that is no pair is told exactly as before (byte for byte), single earrings, necklaces and all
    assert.deepEqual(J(S.pieces([SINGLE, NECK], RID)), oldPieces([SINGLE, NECK]), name + ': single earring and necklace are the old pieces');
    assert.equal(S.tag(SINGLE), ''); assert.equal(S.tag(NECK), ''); assert.equal(S.kind(SINGLE).kind, 'single'); assert.equal(S.kind(NECK).kind, 'multi');
    // a necklace of 3 discs: three pieces of ONE group, never sided
    const d = S.pieces([DISC3], RID);
    assert.equal(d.pieceCount, 3); assert.equal(sidesOf(d.pieces), '---'); assert.deepEqual(J(d.pieces.map(p => [p.grp, p.of, p.n])), [[RID + ':5006', 3, 1], [RID + ':5006', 3, 2], [RID + ':5006', 3, 3]]); assert.equal(S.tag(DISC3), '');
    // a mismatched pair is the same Left and Right, and kind says mismatched
    const m = S.pieces([MIS], RID); assert.equal(sidesOf(m.pieces), 'LR'); assert.ok(!m.pieces[0].both); assert.equal(S.kind(MIS).kind, 'mismatched');
    // a Left is always followed by its own Right, whatever the limit of 24 cuts (13 pairs = 26 pieces, 24 told, the count is true)
    const many = Array.from({ length: 13 }, (_, i) => line({ transaction_id: 6000 + i }));
    const big = S.pieces(many, RID);
    assert.equal(big.pieceCount, 26); assert.equal(big.pieces.length, 24); assert.equal(sidesOf(big.pieces), 'LR'.repeat(12));
    const odd = S.pieces([NECK, NECK, NECK, NECK, NECK, NECK, NECK, NECK, NECK, NECK, NECK, line({ transaction_id: 7001 }), line({ transaction_id: 7002 })].map((t, i) => Object.assign({}, t, { transaction_id: 7100 + i })), RID);   // 22 necklace pieces, then two pairs: the first pair fits (24), the second is not told, never half of it
    assert.equal(odd.pieces.length, 24); assert.ok(odd.pieces.slice(22).map(p => p.side).join('') === 'LR' || odd.pieces.length === 22, 'a pair goes in whole or not at all');
    // a pair line mixed with others: the others unchanged, the pair's parts counted once each
    assert.equal(S.units([STUD, SINGLE, NECK]), 2 + 1 + 2);
    // start() tells the live layer the pieces and the true count
    S.start(RID, [STUD, SINGLE]); const told = S.told[0]; assert.equal(told.pieceCount, 3); assert.equal(told.pieces.length, 3); assert.equal(sidesOf(told.pieces), 'LR-');
    ok('A2 ' + name + ': pair = Left + Right per unit (q units = q Left + q Right), discs = n of one group, single / necklace byte-identical, 24 limit never cuts a pair, start() tells it');
  }
  /* A2b · the stickers an order prints (QR Printer.html dataObj.pieces): a Left then a Right per pair, whatever the count rules say */
  for (const [name, O] of [['rules off', stubOrders()], ['rules on', stubOrders(ON)]]) {
    const S = helper(O), e = J(S.ears([STUD2, SINGLE, NECK, STUD]));
    assert.deepEqual(e.map(x => x.side + x.n + '/' + x.of), ['L1/3', 'R1/3', 'L2/3', 'R2/3', 'L3/3', 'R3/3'], name + ': three pairs, numbered across the order');
    assert.deepEqual(J(S.ears([SINGLE, NECK, DISC3])), [], name + ': an order with no earring pair prints the one sticker it always did');
    assert.deepEqual(J(S.ears([MIS])).map(x => x.side), ['L', 'R']); assert.deepEqual(J(S.ears(null)), []);
    assert.equal(S.ears(Array.from({ length: 30 }, (_, i) => line({ transaction_id: 9000 + i }))).length, 40, 'at most 40 pages, whole pairs');
  }
  { const S = helper(null); assert.deepEqual(J(S.ears([MIS])).map(x => x.side), ['L', 'R'], 'no intake file: a line that says mismatched still prints both ears'); assert.deepEqual(J(S.ears([STUD])), [], 'no intake file: a plain stud line prints the one sticker it always did'); }
  { // the QR Printer reads exactly this shape
    const src = read('QR Printer.html'), m = /function piecesOf\(dataObj\) \{[\s\S]*?\n\}/.exec(src); assert.ok(m, 'QR Printer.html has piecesOf');
    const piecesOf = new Function(m[0] + '; return piecesOf;')(), S = helper(stubOrders(ON)), ears = S.ears([STUD, STUD2]);
    assert.equal(piecesOf({ pieces: ears }).length, 6, 'QR Printer prints one page for each ear'); assert.equal(piecesOf({}).length, 0);
    for (const f of ['sorting.html', 'sorting-2.html']) assert.ok(/StationLiveOrder\.ears\(itemsForOrder\)/.test(read(f)), f + ' hands the ears to the QR Printer');
  }
  ok('A2b ears(): a Left and a Right sticker per pair, numbered across the order, none for an order with no pair; QR Printer.html reads them; both sorting pages hand them over');

  /* A2c · when the intake lists the pieces (piecesOf), its sides are the word: a single earring that names its side is one Left piece, and says no "pair" */
  { const S = helper(stubOrders(ON)), LEFT = line({ transaction_id: 5010, title: 'Single stud earring', variations: [{ formatted_name: 'Side', formatted_value: 'Left ear' }] });
    const one = S.pieces([LEFT], RID); assert.equal(one.pieceCount, 1); assert.equal(sidesOf(one.pieces), 'L'); assert.equal(S.tag(LEFT), '', 'one ear is not a pair'); assert.equal(S.kind(LEFT).kind, 'single'); assert.equal(S.units([LEFT]), 1);
    assert.deepEqual(J(S.pieces([STUD], RID).pieces).map(p => p.side), ['L', 'R'], 'a pair still lists its two ears'); }
  ok('A2c a single earring that names its side is one Left or Right piece, never a pair');

  /* A3 · the counts follow the intake's switch: turn the pair rule off again and the parts go back to the quantity */
  { const O = stubOrders(ON), S = helper(O); assert.equal(S.units([STUD, STUD2]), 6); O.PIECE_RULES.pairFormsMakeTwo = false; assert.equal(S.units([STUD, STUD2]), 3); assert.equal(sidesOf(S.pieces([STUD], RID).pieces), '-'); O.PIECE_RULES.pairFormsMakeTwo = true; assert.equal(S.units([STUD, STUD2]), 6); ok('A3 the parts follow PIECE_RULES at once (one switch, every station)'); }
  /* A4 · never throws on junk */
  { const S = helper(stubOrders(ON)); for (const bad of [null, undefined, 5, 'x', [null, 5, {}], { quantity: 'a' }]) { S.pieces(bad, RID); S.units(bad); S.tag(bad); S.kind(bad); S.sides(bad); } assert.equal(S.units([null, { quantity: 0 }, {}]), 1 + 1); ok('A4 junk in, no throw'); }
}

/* ═══════════════════════ B · the server's live layer ═══════════════════════ */
let door, eff, L, fsx;
function loadServer() {
  fsx = F.fakeFirestore();
  const real = Module._load;
  Module._load = function (req, ...rest) { if (/[\/]firebaseAdmin(\.js)?$/.test(req) || req === 'firebase-admin') return fsx.admin; return real.call(this, req, ...rest); };
  for (const k of Object.keys(require.cache)) if (k.startsWith(path.join(root, 'netlify/functions'))) delete require.cache[k];
  try {
    door = require(path.join(root, 'netlify/functions/firebaseOrders.js')); eff = require(path.join(root, 'netlify/functions/employeeEfficiency.js')); L = require(path.join(root, 'netlify/functions/_stationLive.js'));
  } finally { Module._load = real; }
  process.env.EDIT_PASSCODE = 'synthetic-pass-9f3k';
}
const URL_PAIR = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fmis-1.png?alt=media&token=t1', URL_ONE = 'https://firebasestorage.googleapis.com/v0/b/shop/o/thumbs%2Fcrab.png?alt=media&token=t2';
let ipN = 0, NOW = Date.now(); const realNow = Date.now; Date.now = () => NOW; const tick = ms => { NOW += ms; };
const work = (pieces, o = {}) => ({ v: 1, event: 'work', station: o.station || 'welding', device: o.device || 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', person: o.person || 'Tess Welder', startAt: Date.now() - 3600e3,
  order: { kind: 'order', rid: RID, orderNumber: RID, scannedAt: Date.now(), pieces, pieceCount: o.pieceCount != null ? o.pieceCount : pieces.length } });
const post = async (live) => { const r = await door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '198.51.100.' + (++ipN % 250) }, queryStringParameters: {}, body: JSON.stringify({ live }) }); return { status: r.statusCode, body: JSON.parse(r.body || '{}') }; };
const board = async (station) => { tick(20000); const r = await eff._t.handle({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (++ipN % 250) }, body: JSON.stringify({ op: 'live', key: 'synthetic-pass-9f3k' }) }, fsx.db); assert.equal(r.statusCode, 200); return JSON.parse(r.body).stations.find(s => s.key === (station || 'welding')); };

async function partB() {
  loadServer();
  /* B1 · a piece keeps its side, group, place and `both`; nothing else; junk is dropped */
  assert.deepEqual(L.pairFields({ side: 'L', grp: RID + ':5001', of: 2, n: 1, both: true, extra: 'x' }), { side: 'L', grp: RID + ':5001', of: 2, n: 1, both: true });
  assert.deepEqual(L.pairFields({ side: 'X', grp: 'abc', of: 2, n: 1, both: 'yes' }), {}); assert.deepEqual(L.pairFields(null), {}); assert.deepEqual(L.pairFields({ side: 'R' }), { side: 'R' });
  assert.deepEqual(L.cleanPiece({ id: 'a-1', label: 'Stud earrings', sku: 'CRAB_53423', side: 'R', grp: RID + ':5001', of: 2, n: 2 }), { id: 'a-1', label: 'Stud earrings', sku: 'CRAB_53423', side: 'R', grp: RID + ':5001', of: 2, n: 2 });
  assert.deepEqual(L.cleanPiece({ id: 'a-1', label: 'Necklace' }), { id: 'a-1', label: 'Necklace' }, 'a piece with no pair fields is as before');
  ok('B1 cleanPiece / pairFields keep side, group, size, number and both; a piece with none is as before');

  /* B2 · expandPairs: a piece whose design the master says is a mismatched pair becomes a Left and a Right; nothing else changes */
  const M = new Map([['MIS-1', { pair: { bodies: 2, mismatched: true } }], ['CRAB', { pair: null }], ['TRIO', { pair: { bodies: 3, mismatched: true } }], ['TWIN', { pair: { bodies: 2, mismatched: false } }]]);
  const c = { rid: RID, pieces: [{ id: '5001-1', sku: 'MIS-1', both: true }, { id: '5002-1', sku: 'CRAB' }, { id: '5003-1', sku: 'MIS-1', side: 'L', grp: RID + ':5003' }, { id: '5004-1', sku: 'TRIO' }, { id: '5005-1', sku: 'TWIN' }], pieceCount: 5 };
  L.expandPairs(c, M);
  assert.deepEqual(c.pieces.map(p => [p.id, p.side || '', p.both ? 'both' : '', p.grp || '']), [['5001-1-L', 'L', 'both', RID + ':5001'], ['5001-1-R', 'R', 'both', RID + ':5001'], ['5002-1', '', '', ''], ['5003-1', 'L', '', RID + ':5003'], ['5004-1', '', '', ''], ['5005-1', '', '', '']]);
  assert.equal(c.pieceCount, 6, 'one piece split into two: the count grows by one'); assert.deepEqual([c.pieces[0].of, c.pieces[1].n], [2, 2]);
  const rec = { rid: RID, pieces: [{ id: RID + '_5001_1', sku: 'MIS-1' }], pieceCount: 1 }; L.expandPairs(rec, M); assert.equal(rec.pieces[0].grp, RID + ':5001', 'the group is read from a saved order\'s piece id');
  const full = { rid: RID, pieces: Array.from({ length: 24 }, (_, i) => ({ id: 'p' + i, sku: i === 23 ? 'MIS-1' : 'CRAB' })), pieceCount: 24 }; L.expandPairs(full, M); assert.equal(full.pieces.length, 24, 'no room for both: it stays one piece, never half a pair'); assert.equal(full.pieceCount, 24);
  const none = { rid: RID, pieces: [{ id: 'a', sku: 'MIS-1' }] }; L.expandPairs(none, null); assert.equal(none.pieces.length, 1); L.expandPairs({}, M); L.expandPairs(null, M);
  ok('B2 expandPairs: master pair mismatched with 2 bodies -> Left + Right (both), others untouched, 24 never exceeded, no master / junk safe');

  /* B3 · the door and the reader end to end over the shared fake (nested arrays refused): sided pieces, a glued piece the master splits */
  fsx.put('Charm_Master_Index', 'MIS-1', { sku: 'MIS-1', thumbUrl: URL_PAIR, pair: { v: 1, bodies: 2, mismatched: true } });
  fsx.put('Charm_Master_Index', 'CRAB_53423', { sku: 'CRAB_53423', thumbUrl: URL_ONE });
  let r = await post(work([{ id: '5001-1', label: 'Stud earrings', sku: 'CRAB_53423', side: 'L', grp: RID + ':5001', of: 2, n: 1 }, { id: '5001-2', label: 'Stud earrings', sku: 'CRAB_53423', side: 'R', grp: RID + ':5001', of: 2, n: 2 }, { id: '5004-1', label: 'Single', sku: 'CRAB_53423' }]));
  assert.equal(r.status, 200); assert.equal(r.body.written, 1);
  let w = await board('welding'), cur = w.current[0];
  assert.deepEqual(cur.pieces.map(p => [p.id, p.side || '', p.grp || '']), [['5001-1', 'L', RID + ':5001'], ['5001-2', 'R', RID + ':5001'], ['5004-1', '', '']]); assert.equal(cur.pieceCount, 3);
  assert.equal(cur.pieces[0].thumbUrl, URL_ONE, 'the design\'s own picture on each ear'); assert.equal(cur.pieces[2].side, undefined, 'a single piece carries no side');
  r = await post(work([{ id: '5007-1', label: 'Mismatched studs', sku: 'MIS-1', both: true, grp: RID + ':5007' }], { person: 'Ray Welder', device: 'assembly-2', station: 'assembly' }));
  assert.equal(r.status, 200);
  w = await board('assembly'); cur = w.current.find(x => x.person === 'Ray Welder');
  assert.deepEqual(cur.pieces.map(p => [p.id, p.side, !!p.both, p.of, p.n]), [['5007-1-L', 'L', true, 2, 1], ['5007-1-R', 'R', true, 2, 2]]); assert.equal(cur.pieceCount, 2);
  assert.equal(cur.pieces[0].thumbUrl, URL_PAIR, 'a mismatched pair shows the design\'s picture of both bodies');
  assert.ok(fsx.st && fsx.strict(), 'the fake refuses nested arrays'); ok('B3 door + reader over the strict fake: sided pieces round-trip, a glued mismatched piece is split Left + Right from the master, singles unchanged');

  /* B4 · the reader's whole answer carries no PIN-like or stray field from a pair */
  const text = JSON.stringify(cur); assert.ok(!/listingId|"size"/.test(text), 'the lookup hints do not leave the server'); ok('B4 lookup hints stay on the server');
}

/* ═══════════════════════ C · the page client (jsdom) ═══════════════════════ */
async function partC() {
  let JSDOM;
  for (const d of [process.env.JSDOM_DIR, process.argv[2], path.join(root, 'node_modules'), '/tmp/ps1-jsdom/node_modules'].filter(Boolean)) { try { ({ JSDOM } = require(path.join(d, 'jsdom'))); break; } catch (_) {} }
  if (!JSDOM) { console.log('  SKIP C (jsdom not found; set JSDOM_DIR): the page client\'s side / group / both and the pair-aware cut are not checked here'); return; }
  const code = read('station-activity.js'), orders = stubOrders({ pairFormsMakeTwo: true, discsMakeN: true, mismatchedMakesTwo: true });
  const page = () => {
    const dom = new JSDOM('<!doctype html><html><body></body></html>', { url: 'http://station.test/weld-1.html', pretendToBeVisual: true, runScripts: 'outside-only' }), w = dom.window;
    const h = { w, calls: [], timeouts: [], intervals: [] };
    w.setInterval = (fn, ms) => { h.intervals.push({ fn, ms }); return h.intervals.length; }; w.clearInterval = () => {}; w.setTimeout = (fn, ms) => { h.timeouts.push({ fn, ms }); return h.timeouts.length; }; w.clearTimeout = () => {};
    w.TextEncoder = TextEncoder; w.Blob = function (parts) { this.parts = parts; };
    Object.defineProperty(w.navigator, 'sendBeacon', { value: () => true, configurable: true });
    w.fetch = (u, opt) => { const body = JSON.parse(opt.body); h.calls.push(body); return door.handler({ httpMethod: 'POST', headers: { 'x-nf-client-connection-ip': '203.0.113.' + (100 + (++ipN % 100)) }, queryStringParameters: {}, body: opt.body }).then(r => ({ status: r.statusCode, json: () => Promise.resolve(JSON.parse(r.body || '{}')) })); };
    w.StationSession = { who: () => ({ person: 'Tess Welder', station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN', session: 'weld-1-ABCD-k1-XYZW', startAt: Date.now() - 30000 }), page: () => ({ station: 'welding', device: 'weld-1', computer: 'pc-ABCDEFGHJKMN' }) };
    w.CharmNestOrders = orders; w.eval(code); w.eval(read('station-live-order.js'));
    h.settle = async () => { for (let i = 0; i < 40; i++) await new Promise(r => setImmediate(r)); };
    h.zero = async () => { for (let n = 0; n < 10; n++) { const z = h.timeouts.filter(t => t.ms === 0 || t.ms === undefined); h.timeouts = h.timeouts.filter(t => !z.includes(t)); if (!z.length) break; z.forEach(t => t.fn()); await h.settle(); } await h.settle(); };
    return h;
  };
  /* C1 · a pair goes from the page to the board with its sides, group and numbers */
  let h = page();
  assert.equal(h.w.StationLiveOrder.start(RID, [STUD, SINGLE]), true); await h.zero();
  const sent = h.calls.find(c => c.live && c.live.event === 'work').live.order;
  assert.deepEqual(sent.pieces.map(p => [p.id, p.side || '', p.grp || '', p.of || 0, p.n || 0]), [['5001-1', 'L', RID + ':5001', 2, 1], ['5001-2', 'R', RID + ':5001', 2, 2], ['5004-1', '', '', 0, 0]]); assert.equal(sent.pieceCount, 3);
  const bd = await board('welding'); assert.equal(sidesOf(bd.current.find(x => x.rid === RID).pieces), 'LR-');
  ok('C1 page -> door -> reader: Left, Right, single with group and numbers');
  /* C2 · a mismatched glued piece keeps `both` on the way */
  h = page(); h.w.CharmNestOrders = stubOrders(); h.w.StationLiveOrder.start('3521000888', [Object.assign({}, MIS, { receipt_id: '3521000888' })]); await h.zero();
  const glued = h.calls.find(c => c.live && c.live.event === 'work').live.order.pieces[0]; assert.equal(glued.both, true); assert.equal(glued.grp, '3521000888:5007'); assert.equal(glued.side, undefined);
  ok('C2 a mismatched piece the pool still makes whole is told `both`, with its group');
  /* C3 · 12 pairs plus a big request: the size limit trims 4 at a time and still never leaves a Left without its Right; 13 pairs are cut to 12 whole */
  h = page(); const wide = Array.from({ length: 13 }, (_, i) => line({ transaction_id: 8000 + i, title: 'Stud earrings with a long long listing title '.repeat(2) + i, sku: 'SKU_' + 'X'.repeat(40) + i }));
  h.w.StationLiveOrder.start('3521000999', wide.map(t => Object.assign({}, t, { receipt_id: '3521000999' }))); await h.zero();
  const big = h.calls.find(c => c.live && c.live.event === 'work').live.order;
  assert.ok(big.pieces.length <= 24 && big.pieces.length % 2 === 0, 'whole pairs only'); big.pieces.forEach((p, i) => { assert.equal(p.side, i % 2 ? 'R' : 'L'); }); assert.equal(big.pieceCount, 26, 'the true count is told');
  ok('C3 13 pairs: 24 told as 12 whole pairs, true count 26; never a Left without its Right');
  /* C4 · a piece with a bad side or group is not sent */
  h = page(); h.w.StationActivity.working({ rid: '3521000555', pieces: [{ id: 'a', label: 'x', side: 'Q', grp: 'zz', of: 2, n: 1 }, { id: 'b', label: 'y', side: 'R', grp: '3521000555:1', of: 2, n: 2 }] }); await h.zero();
  const bad = h.calls.find(c => c.live && c.live.event === 'work').live.order.pieces; assert.deepEqual(J(bad[0]), { id: 'a', label: 'x' }); assert.equal(bad[1].side, 'R'); ok('C4 a bad side / group is dropped on the page');
}

/* ═══════════════════════ D · the Laser summary ═══════════════════════ */
function partD() {
  const LT = require(path.join(root, 'netlify/functions/_laserSheetTime.js'));
  const row = (o) => Object.assign({ sheetId: 's', person: 'Lee', pieces: 0, orders: 0, ms: 600000, _oids: [] }, o);
  // an order whose pair is on two sheets is ONE order
  const a = row({ sheetId: 'a', pieces: 5, orders: 3, _oids: ['3521000001', '3521000002', '3521000003'] }), b = row({ sheetId: 'b', pieces: 4, orders: 3, _oids: ['3521000003', '3521000004', '3521000005'] });
  const s = LT.summarize([a, b]); assert.equal(s.pieces, 9); assert.equal(s.orders, 5, 'five distinct orders, not six'); assert.equal(s.sheets, 2);
  // older rows (no order ids) still sum their own count, and mix with new ones
  const old1 = row({ sheetId: 'o1', pieces: 2, orders: 2 }), old2 = row({ sheetId: 'o2', pieces: 3, orders: 1 });
  assert.equal(LT.summarize([old1, old2]).orders, 3, 'older records: the sum as before'); assert.equal(LT.summarize([old1, a]).orders, 2 + 3);
  assert.equal(LT.summarize([a, a]).orders, 3, 'the same sheet twice is the same orders');
  assert.deepEqual(LT.oidsOf(['3521000001', 3521000001, 'x', '12', null, '3521000002']), ['3521000001', '3521000002'], 'digits only, at least 4, no duplicates');
  assert.equal('_oids' in LT.plain(a), false, 'a page never gets the order ids'); assert.equal('sheetId' in LT.plain(a), true);
  ok('D laser: an order whose pieces are on two sheets is one order; older records sum as before; order ids stay on the server');
}

/* ═══════════════════════ E · the pages ═══════════════════════ */
function partE() {
  const pages = ['weld-1', 'assembly-1', 'assembly-2', 'assembly-3', 'assembly-4', 'shipping-1', 'shipping-2', 'shipping-3', 'design', 'design-1', 'design-message', 'design-message-1', 'sorting', 'sorting-2'];
  const tok = (src, file) => { const m = new RegExp('<script src="' + file.replace('.', '\\.') + '\\?v=([^"]+)"').exec(src); return m && m[1]; };
  for (const p of pages) {
    const src = read(p + '.html'), o = tok(src, 'charm-nest-orders.js'), l = tok(src, 'station-live-order.js'), a = tok(src, 'station-activity.js');
    assert.ok(o && l && a, p + ': loads charm-nest-orders.js, station-live-order.js and station-activity.js with tokens');
    assert.ok(src.indexOf('<script src="charm-nest-orders.js') < src.indexOf('<script src="station-live-order.js'), p + ': the intake file is loaded before the helper');
    assert.equal(l, a, p + ': helper and activity share one token (' + l + ')'); assert.ok(!/charm-nest-pair\.js/.test(src), p + ': station pages do not load the pair module (the count is the intake\'s)');
    assert.ok(/StationLiveOrder\.(tag|units|sides|pieces)/.test(src), p + ': uses the helper');
  }
  const tokens = new Set(pages.map(p => tok(read(p + '.html'), 'station-live-order.js'))); assert.equal(tokens.size, 1, 'one token for the helper on every page');
  const tags = pages.filter(p => /StationLiveOrder\.tag\(/.test(read(p + '.html'))); assert.ok(tags.length >= 10, 'the cells that show a quantity say Left + Right (' + tags.length + ' pages)');
  // the pages that did not change keep their tokens (no piece logic in them)
  for (const p of ['weld-scan-1', 'sort-scan', 'design-print-1', 'design-scan-1', 'scanner']) { try { assert.ok(!/charm-nest-orders\.js/.test(read(p + '.html')), p + ': untouched'); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
  ok('E ' + pages.length + ' station pages load charm-nest-orders.js then the helper, one token; ' + tags.length + ' say Left + Right in a quantity cell');
}

(async () => {
  console.log('PAIRSTATIONS');
  process.on('exit', () => { Date.now = realNow; });
  partA(); await partB(); await partC(); partD(); partE();
  console.log('\nPAIRSTATIONS: ' + checks + ' groups of checks passed');
  process.exit(0);
})().catch(e => { console.error('FAIL', e && e.stack || e); process.exit(1); });
