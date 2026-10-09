// PAIRS FIXTURES: one small shared helper that builds OFFLINE orders, pool rows, sheet records, set records and Etsy receipts for earring pairs,
// mismatched pairs and multi-piece (disc, letters) orders, on the repo's fake Firestore with the no-nested-arrays check, and a checker that
// fails when pieces of one group disagree about their sheet, side, group or MIRROR. Written by PAIRTESTS (pairs-1009, area 15); the other
// workers reuse it. No browser, no network, no paid call, nothing outside memory.
//
//   const F = require('./pairs-fixtures.cjs');
//
// WORDS (plan.md, contract.md AMENDMENT 2): PIECE = one physical cut charm of an order line. GROUP = every piece of one order line (receipt id +
// transaction id; key "rid:tx", CharmNestPair.groupKey). EARRING PAIR = a stud, hoop or huggie pair, matching or mismatched: ALWAYS one LEFT and
// one RIGHT piece per unit of quantity (quantity 2 = 2 L + 2 R), copies alternate L, R. The RIGHT is the LEFT MIRRORED (x -> -x about the body's own
// bounding-box centre); `mirror` is true on the piece that is the mirror of the as-drawn master design (which one that is depends on the way the
// drawing faces: F.DESIGN_FACING, CharmNestPair.facingOf). MISMATCHED PAIR = the design draws TWO different bodies: the Left earring is the left
// body, the Right earring the right body, each oriented to face its own side. Discs, letters, necklace charms and "Single" earring lines: side null,
// mirror false, never mirrored. The nester may TURN a piece, never reflect it. Pool id = rid_tx_copy; an order line's key = rid_tx.
//
// ── WHAT CHANGED WITH AMENDMENT 2 (read this if you used the helper before) ──────────────────────────────────────────────────────────────────
//   * a matching pair's pieces now carry side "L" then "R" (they used to carry null), and `mirror` (true on the piece that is the mirror of the drawing)
//   * kinds: 'pair' | 'hoop' | 'mismatched' (earring pairs: 2 pieces per unit) | 'earring-single' (the "Single" line: 1 piece per unit, no side) |
//            'single' (a pendant or charm) | 'discs' (n discs) | 'letters' (n letters): the last four never have a side
//   * the default pair design is PAIR-FACE-L (drawn facing left, asymmetric, so mirroring is visible); PAIR-FACE-R faces right (a person set `facing: "R"` on it: a shape cannot say which way it faces); PAIR-SET-R carries a person's `facing: "R"`
//   * every sheet charm of a world carries `shapeJson`: its laid outline as a JSON STRING (a list of polygons is an array in an array: Firestore refuses it)
//   * F.problems has new codes: side-missing, side-unexpected, mirror-missing, mirror-mismatch, mirror-pairing, mirror-unexpected, shape-mismatch, reflected, not-mirror
//   * the set records of a world list each copy with its sheet and side, as a committed set does (orders[rid].lines[{ transactionId, sku, copies:[{ copy, sheetId, sheet, poolId, side }] }]); a split inside one set stores no tracking field of its own, so `tracked` stays an input of the checker
//   * a world is `{ ..., kinds }` (groupKey -> kind) and the run record's lines carry `kind` and `pieceCount`, as the intake will set spec.pair.kind / spec.pieceCount
//
// ── THE NAMED CASES (F.cases) — each is a ready world, ids fixed, `expect` says what the checker must find ───────────────────────────────────
//   pairOneSheet            a pair drawn facing left on one sheet: Left as drawn, Right mirrored, with filler orders
//   pairFacesRight          a pair drawn facing RIGHT: the Right is as drawn, the Left is the mirror
//   pairSymmetric           a symmetric stud pair: still a Left and a Right, the Right flagged mirrored (the outlines look alike)
//   pairQty2                quantity 2: 2 Left + 2 Right (copies L, R, L, R)
//   pairRotated             pairs turned by 0, 90, 180, 270 degrees: a turn is allowed (nothing reported)
//   pairSplitOneSet         a pair split over two sheets of ONE set (allowed only as a tracked split)
//   pairSplitTwoSets        a pair split over two sheets of two SETS (never allowed: the cardinal rule)
//   discs3Sheets            a 3-disc necklace, one disc on each of three sheets of one set (tracked split)
//   lettersNecklace         a 4-letter necklace on one sheet
//   singleLine              a "Single" earring line, quantity 2 (two pieces, no side), next to a pair
//   mixedSheet              one sheet holding studs + hoops + discs + letters + a Single line + a pendant + a mismatched pair
//   mismatchedTwoOutlines   a mismatched pair with two different outlines (a ball and a racket) side by side on one sheet, L then R
//   mismatchedMittens       the picture (MISMATCHED_7134): two mitten bodies drawn facing left, the Right earring is body 2 mirrored
//   mismatchedSplit         the same mismatched pair, its left piece on one sheet and its right piece on another of the same set (tracked)
//   pairWaiting             a pair of which one piece is on a sheet and one still waits (a half-placed group)
//   Each: F.cases.<id>() -> world (fresh object every call).   F.caseList() -> [{ id, about, world, expect }].
//
// ── BUILDING A WORLD ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
//   F.world({ orders:[{ rid, lines:[{ n, kind, sku?, qty?, metal?, discs?, letters?, form?, on? }] }], sheets:[{ id, metal, n, set }], tracked?, rotate? })
//       kind: 'pair' | 'hoop' | 'mismatched' | 'earring-single' | 'single' | 'discs' | 'letters'   (default sku, form and piece count follow the kind)
//       on:   a sheet id (every piece on it) | an array with one entry per piece (a sheet id, or null = that piece waits) | omitted = all wait
//       tracked: groupKeys whose split over sheets is tracked on purpose (R3), e.g. ['4190000001:5000000010']
//       rotate: true turns the pieces by 0, 90, 180, 270 in turn (the shapes then lie turned; the checker must still pass)
//   -> { orders, pieces, pool, sheets, sets, run, designs, kinds, tracked }   (all plain data; docs have NO array inside an array)
//   Every pool row and sheet charm carries side, bodyIndex, groupKey, groupSize, mirror (F.PAIR_FIELDS); every sheet charm also `shapeJson`.
//   F.pieceCount(kind, qty, count)   the number of pieces a line makes in the PLAN's words (earring pair 2 per unit, n discs n, a single 1; times quantity), worked out here
//                                    independently of CharmNestPair; every fixture line carries it as `pieceCount` (as the intake's spec.pieceCount will).
//   F.legacy(world)                  the same world as the app stored it before pairs: no side, bodyIndex, groupKey, groupSize, mirror, shapeJson anywhere
//   F.clone(world)                   a deep copy (mutate a copy to make a broken world)
//   F.designs() / F.charmOf(sku) / F.entryOf(sku)   the designs: PAIR-FACE-L (default pair), PAIR-FACE-R, PAIR-SET-R (symmetric with a person's facing R), PAIR-STUD
//       (symmetric), PAIR-HOOP (a hoop welded to its body: ONE body), PAIR-TWIN (two identical bodies drawn: NOT mismatched), ONE-PENDANT, DISC-14, LETTER-DISC,
//       MITTENS-MIS (the picture in plans/pairs-1009), TENNIS-MIS (a ball and a racket: two different outlines). charmOf = a charm CharmNestPair.bodiesOf reads;
//       entryOf = the master index entry (with `pair` when the design has more than one body, `facing` when a person set one).
//   F.DESIGN_FACING   which way each design's body faces in the drawing ("L" | "R" | null), written by hand: pairs-tests.cjs checks CharmNestPair.facingOf against it
//   F.expectedMirror(sku, side, bodyIndex)  what the mirror flag of that piece must be (side !== facing, facing null = "L")
//   F.ids: rid(n) tx(n) poolId(rid, tx, copy) lineKey(rid, tx) groupKey(rid, tx)
//
// ── DIRECTION ON THE SHEET (shapeJson) ───────────────────────────────────────────────────────────────────────────────────────────────────────
//   F.laidShape(sku, piece, { angle, cx, cy })  the polygons of the piece's outline as it lies on a sheet: its own body, mirrored when piece.mirror, turned, placed
//   F.shapeJsonOf(...same)                       the same as the JSON string a sheet charm stores (`charm.shapeJson`; a list of polygons must be a string in Firestore)
//   F.shapeState(laid, drawn)                    "asDrawn" | "mirrored" | "either" (a symmetric shape fits both) | "none" (fits neither), judged under the BEST turn: a turn
//                                                can never turn a mirror image into the original, a reflection can, which is how the checker finds a reflected piece
//   F.mirrorPolys(polys) / F.layPolys(polys, deg, cx, cy)   x -> -x about the box centre / turn about the box centre (never a reflection)
//   F.reflectedPlacement(placement)             true when a placement says it was reflected (flipX, reflect, mirrored, flip, negative scale); a turn never sets one
//   A nester that wants to be checked writes each charm's laid outline into `shapeJson` and never sets a reflect flag on a placement.
//
// ── THE FAKE FIRESTORE ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
//   const fs = F.fakeFirestore();            // { st, admin, db, put(coll,id,doc), get(coll,id), list(coll), del(coll,id), seed(world), docsOf(), writes }
//   fs.seed(world)                           writes pool rows, sheets, sets, the run, through the fake: ANY array inside an array throws "3 INVALID_ARGUMENT:
//                                            Nested arrays are not allowed (path)", as live Firestore does (tests/charm-nest/_noNestedArrays.cjs)
//   fs.db                                    a firebase-admin firestore() fake (collection/doc/where/orderBy/batch/runTransaction/getAll), the one bridge-server uses
//   F.functions(fs, ['charmNestLibrary'])    -> { handlers, call(name, body), lib(op, args), restore() }: the REAL netlify functions loaded over that fake
//                                            (firebase-admin and ./firebaseAdmin are answered by it): fs.seed(world); await fns.lib('getOrderPieces', {...})
//   F.COLL                                   the collection names: sheets pool sets runs timeline cancelled custom poolBack master
//
// ── THE PAIR CHECKER (the placement oracle's pair rule, usable anywhere) ─────────────────────────────────────────────────────────────────────
//   F.problems(docs, opts)  docs = { pool:[rows], sheets:[sheet docs], sets:[set docs], designs?, kinds?, runs? }  (F.docsOf(fs) or a world)
//                           opts = { tracked:[groupKey], mirror:false (skip every mirror and shape check: records made before Amendment 2), allowWaiting, allowSplit }
//   -> [{ code, groupKey, poolId?, text }]  codes:
//        key-mismatch      a piece's stored groupKey is not its pool id's (pool row or sheet charm)
//        size-mismatch     pieces of one group disagree about groupSize, or the group's rows are more than that many
//        missing-piece     a group has fewer pool rows than its groupSize (or a hole in the copy numbers)
//        side-missing      a piece of an earring pair (pool row or sheet charm) has no side
//        side-unexpected   a disc, letter, single or pendant piece says Left or Right
//        side-mismatch     a piece's side differs between its pool row and its sheet charm, or from its copy (odd copy Left, even copy Right)
//        side-pairing      an earring group has not as many L as R, or a body does not fit its side (a mismatched Left is body 0, Right body 1; a matching pair's are body 0)
//        mirror-missing    a piece of an earring pair has no `mirror` flag (pool row or sheet charm)
//        mirror-mismatch   a piece's mirror flag differs between pool row and sheet, or from what its side and the design's facing give (the Right of a left-facing design is mirrored)
//        mirror-pairing    a matching pair's Left and Right are both mirrored or both as drawn (one of the two must be the mirror of the other)
//        mirror-unexpected a disc, letter, single or pendant piece is marked mirrored
//        shape-mismatch    a sheet charm's laid outline (shapeJson) is not its design body, as drawn or mirrored, at any turn
//        reflected         a laid outline is mirrored when the piece is not, or as drawn when it should be mirrored (a piece was reflected), or a placement carries a reflect flag
//        not-mirror        a matching pair's Right does not lie as the mirror of its Left (both lie as drawn, or both mirrored)
//        sheet-disagree    the pool row's sheetId, the sheet's poolIds, its charms and its placements do not say the same sheet
//        duplicate-piece   one piece is listed by two sheets
//        untracked-split   the pieces of one group are on more than one sheet (or on one and waiting), and nothing says it is tracked
//        split-across-sets the sheets that hold one group are in different sets (never allowed)
//        metal-mismatch    the pieces of one group (or a sheet and its pieces) differ in metal
//        set-disagree      a sheet's setId, the set's sheetIds and the pieces' setId do not agree
//        half-held         some pieces of a group are on hold or cancelled and some are not
//        set-copy-disagree a committed set record (orders[rid].lines[].copies[]: copy, sheetId, poolId, side) lists a piece on another sheet than the sheet records have it, or with another side than its pool row
//   The kind of a group comes from docs.kinds, else the run records' lines (`kind`), else the rows' sides and the design and form.
//   F.sheetsOf(group, docs) / F.groupsOf(docs)   the groups (key -> pieces with their sheet) as the checker reads them
//   Legacy records (no pair fields) are checked only for the things that need none: sheet-disagree, duplicate-piece, untracked-split, split-across-sets,
//   metal-mismatch, set-disagree, half-held (a group is derived from its pool ids, a side from nothing).
//
// ── ETSY RECEIPTS (the sandbox stream, intake) ───────────────────────────────────────────────────────────────────────────────────────────────
//   F.receipts(world)   -> Etsy-shaped receipts { receipt_id, transactions:[{ transaction_id, listing_id, sku, title, quantity, variations:[{formatted_name,
//                          formatted_value}] }] }, one per order of the world: what the emulated Etsy lists. The sandbox stream carries them unchanged
//                          (netlify/functions/etsySandbox.js arrival() only moves times): tests/charm-nest/pairs-tests.cjs proves it.
//   F.EXAMPLES          the real listings of the Sep 17 snapshot each case stands for (listing ids, SKUs and order numbers: no buyer data), see PAIRTESTS-points.md
'use strict';
const path = require('path');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const Pair = require('../../charm-nest-pair.js');
const { makeState, fakeAdmin } = require('./bridge-server.cjs');

const COLL = { sheets: 'Charm_Nest_Sheets', pool: 'Charm_Pool', sets: 'Charm_Nest_Sets', runs: 'Charm_Nest_Runs', timeline: 'Order_Timeline', cancelled: 'Charm_Nest_Cancelled', custom: 'Charm_Custom_Orders', poolBack: 'Charm_Pool_Back', master: 'Charm_Master_Index' };
const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
const DAY = '2026-10-09', RUN = 'run-pairs', T0 = 1791500000000;

/* ═══════════════════════════ ids ═══════════════════════════ */
const rid = n => String(4190000000 + n), tx = n => String(5000000000 + n);
const poolId = (r, t, copy) => `${r}_${t}_${copy}`, lineKey = (r, t) => `${r}_${t}`, groupKey = (r, t) => `${r}:${t}`;
const ids = { rid, tx, poolId, lineKey, groupKey };
const str = v => String(v == null ? '' : v);
const poolParts = id => { const m = /^(\d{4,20})_([^_]*)_(\d{1,3})$/.exec(str(id)); return m ? { rid: m[1], tx: m[2], copy: +m[3] } : null; };
const gkOfPool = id => { const p = poolParts(id); return p ? groupKey(p.rid, p.tx) : ''; };

/* ═══════════════════════════ designs (synthetic charms CharmNestPair reads) ═══════════════════════════ */
const seg = pts => [pts.map((p, i) => [i ? 'l' : 'm', p])];
const bboxOf = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
const path_ = (pts, extra) => Object.assign({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: bboxOf(pts), subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['l', pts[0]]])] }, extra || {});
const rect = (x0, y0, x1, y1, extra) => path_([[x0, y0], [x1, y0], [x1, y1], [x0, y1]], extra);
const ngon = (cx, cy, rx, ry, n) => path_(Array.from({ length: n || 24 }, (_, i) => [cx + rx * Math.cos(2 * Math.PI * i / (n || 24)), cy + ry * Math.sin(2 * Math.PI * i / (n || 24))]));
const ink = (x0, y0, x1, y1, rgb) => rect(x0, y0, x1, y1, { layer: 'ENGRAVE', strokeRGB: rgb || [1, 0, 0] });
const mittenR = x => [[x + 14, 0], [x, 0], [x, 30], [x + 14, 30], [x + 14, 20], [x + 20, 16], [x + 14, 12]];   // the mirror image of mitten(x): the thumb sticks out to the right
const mitten = x => [[x + 6, 0], [x + 20, 0], [x + 20, 30], [x + 6, 30], [x + 6, 20], [x, 16], [x + 6, 12]];   // a mitten-like cut line: left of the body a thumb
// each design: bodies = [{ outline, inks[] }] (left to right in the drawing), plus a hoop ring where the design has one
const DESIGN_SRC = {
  'PAIR-STUD':   () => [{ outline: ngon(10, 12, 9, 11), inks: [ink(5, 8, 15, 12)] }],
  'PAIR-HOOP':   () => [{ outline: rect(0, 0, 20, 26), inks: [ink(4, 8, 16, 12)], ring: rect(8, 26, 12, 30) }],
  'PAIR-TWIN':   () => [{ outline: rect(0, 0, 20, 26), inks: [ink(4, 8, 16, 12)] }, { outline: rect(26, 0, 46, 26), inks: [ink(30, 8, 42, 12)] }],   // the same charm drawn twice
  'ONE-PENDANT': () => [{ outline: ngon(12, 14, 11, 13), inks: [ink(6, 10, 18, 16, [0, 0, 1])] }],
  'DISC-14':     () => [{ outline: ngon(7, 7, 7, 7), inks: [] }],
  'MITTENS-MIS': () => [{ outline: path_(mitten(0)), inks: [ink(8, 2, 20, 6)] }, { outline: path_(mitten(26)), inks: [ink(34, 2, 46, 6), ink(30, 10, 34, 14, [0, 0, 1]), ink(38, 12, 42, 16, [0, 0, 1])] }],   // MISMATCHED_7134: MITTENS 1 + MITTENS 2
  'PAIR-FACE-L': () => [{ outline: path_(mitten(0)), inks: [ink(8, 2, 20, 6)] }],                                            // an earring drawn FACING LEFT (the thumb sticks out to the left): as drawn it is the Left
  'PAIR-FACE-R': () => [{ outline: path_(mittenR(0)), inks: [ink(0, 2, 12, 6)] }],                                           // the same, drawn FACING RIGHT: as drawn it is the Right, the Left is its mirror
  'PAIR-SET-R':  () => [{ outline: ngon(10, 12, 9, 11), inks: [ink(5, 8, 15, 12)] }],                                       // symmetric, but a person set `facing: "R"` on its record: as drawn it is the Right
  'LETTER-DISC': () => [{ outline: ngon(7, 7, 7, 7), inks: [] }],                                                           // a letter disc of a letters necklace
  'TENNIS-MIS':  () => [{ outline: ngon(10, 10, 10, 10), inks: [ink(4, 9, 16, 11)] }, { outline: path_([[28, 0], [34, 0], [34, 30], [31, 36], [28, 30]]), inks: [ink(29, 20, 33, 28)] }],   // a ball and a racket: two different outlines
};
// which way each design's body faces in the drawing (declared here by hand; pairs-tests.cjs checks CharmNestPair.facingOf against it): "L", "R", or null (symmetric: as drawn is the Left)
const DESIGN_FACING = { 'PAIR-STUD': [null], 'PAIR-HOOP': [null], 'PAIR-TWIN': [null, null], 'ONE-PENDANT': [null], 'DISC-14': [null], 'LETTER-DISC': [null], 'PAIR-FACE-L': ['L'], 'PAIR-FACE-R': ['R'], 'PAIR-SET-R': ['R'], 'MITTENS-MIS': ['L', 'L'], 'TENNIS-MIS': [null, null] };
const KINDS = {   // what a kind of line means: default design, the Etsy form, the pieces per unit, and what the intake calls it (line.pair.kind)
  pair: { sku: 'PAIR-FACE-L', form: 'earrings', per: 2, pair: 'pair' }, hoop: { sku: 'PAIR-HOOP', form: 'hoop', per: 2, pair: 'pair' }, mismatched: { sku: 'MITTENS-MIS', form: 'earrings', per: 2, pair: 'mismatched' },
  single: { sku: 'ONE-PENDANT', form: 'necklace', per: 1, pair: 'single' }, 'earring-single': { sku: 'PAIR-STUD', form: 'earring-single', per: 1, pair: 'single' },
  discs: { sku: 'DISC-14', form: 'necklace', per: 1, pair: 'discs' }, letters: { sku: 'LETTER-DISC', form: 'necklace', per: 1, pair: 'letters' },
};
const EARRING_PAIR = new Set(['pair', 'hoop', 'mismatched']);
/** How many pieces a line makes (independent of CharmNestPair: the tests compare the two): an earring pair is a Left and a Right per unit, n discs or n letters are n, a single is 1; times quantity. */
const pieceCount = (kind, qty, count) => ((kind === 'discs' || kind === 'letters') ? Math.max(1, count | 0) : KINDS[kind].per) * Math.max(1, qty | 0);
function charmOf(sku) {
  const src = DESIGN_SRC[sku]; if (!src) throw new Error('no such fixture design: ' + sku);
  const bodies = src(), members = [];
  for (const b of bodies) { members.push(b.outline); for (const i of b.inks) members.push(i); if (b.ring) members.push(b.ring); }
  const bbox = members.reduce((u, m) => [Math.min(u[0], m.bbox[0]), Math.min(u[1], m.bbox[1]), Math.max(u[2], m.bbox[2]), Math.max(u[3], m.bbox[3])], [1e9, 1e9, -1e9, -1e9]);
  return { sku, ...(sku === 'PAIR-SET-R' || sku === 'PAIR-FACE-R' ? { facing: 'R' } : {}), outline: bodies[0].outline, members, bbox, widthPt: bbox[2] - bbox[0], heightPt: bbox[3] - bbox[1], areaPt2: (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) };
}
/** The master index entry of a design: `pair` only when it draws more than one body (the contract's Charm_Master_Index field). */
function entryOf(sku) {
  const c = charmOf(sku), d = Pair.describe(c), e = { sku, members: c.members.length, widthPt: c.widthPt, heightPt: c.heightPt, areaPt2: c.areaPt2, holes: 0, engravable: false };
  if (d.count > 1) e.pair = { v: 1, bodies: d.count, mismatched: !!d.mismatched };
  if (c.facing) e.facing = c.facing;   // (a person's setting on the record)
  return e;
}
const designs = () => Object.fromEntries(Object.keys(DESIGN_SRC).map(sku => [sku, { sku, charm: charmOf(sku), entry: entryOf(sku), facings: DESIGN_FACING[sku].slice(), bodies: Pair.bodiesOf(charmOf(sku)).map(b => ({ index: b.index, side: b.side, w: b.outlineBbox[2] - b.outlineBbox[0], h: b.outlineBbox[3] - b.outlineBbox[1] })) }]));

/* ═══════════════════════════ the world ═══════════════════════════ */
const SHEETS = [{ id: 'sh-gf1', metal: 'gold', n: 1, set: 'set-1' }, { id: 'sh-gf2', metal: 'gold', n: 2, set: 'set-1' }, { id: 'sh-gf3', metal: 'gold', n: 3, set: 'set-1' }, { id: 'sh-gf4', metal: 'gold', n: 4, set: 'set-2' }, { id: 'sh-ss1', metal: 'silver', n: 1, set: 'set-1' }];
const SET_SEQ = { 'set-1': 1, 'set-2': 2, 'set-3': 3 };
const fileBase = s => `${CODE[s.metal] || 'GF'}_${DAY}_Set-${SET_SEQ[s.set] || 1}_Sheet-${s.n}`;
const sheetLabel = s => `${CODE[s.metal] || 'GF'} Sheet ${s.n}`;

/** An order line as the page and the cloud hold it, and the pieces it makes (side and mirror from the shared module, checked here against what the plan says). */
function lineOf(rid_, l) {
  const kind = l.kind || 'single', K = KINDS[kind]; if (!K) throw new Error('no such kind: ' + kind);
  const t = tx(l.n), sku = l.sku || K.sku, qty = Math.max(1, l.qty | 0 || 1), count = kind === 'discs' ? (l.discs || 3) : kind === 'letters' ? (l.letters || 3) : 0, metal = l.metal || 'gold', form = l.form || K.form;
  // (the line carries its own piece count and its kind, as the intake sets spec.pieceCount and spec.pair.kind)
  const want = pieceCount(kind, qty, count), charm = charmOf(sku);
  const line = { receiptId: rid_, transactionId: t, quantity: qty, form, sku, pieceCount: want, pair: { kind: K.pair }, ...(kind === 'discs' ? { discs: count } : kind === 'letters' ? { letters: count } : {}),
    title: l.title || (kind === 'earring-single' ? `${sku} Charm + Shipping` : kind === 'discs' ? `Disc necklace, ${count} discs` : kind === 'letters' ? `Letters necklace, ${count} letters` : kind === 'mismatched' ? `Mismatched ${sku} earrings` : `${sku} ${form}`) };
  const per = Pair.piecesFor(line, charm, l.facing ? { facing: l.facing } : undefined);
  if (per.length !== want) throw new Error(`CharmNestPair.piecesFor makes ${per.length} piece(s) for ${kind} x${qty}${count ? ' (' + count + ')' : ''}, the fixture expects ${want}`);
  const on = l.on == null ? Array(want).fill(null) : Array.isArray(l.on) ? l.on.slice() : Array(want).fill(l.on);
  if (on.length !== want) throw new Error(`line ${l.n}: ${want} pieces but ${on.length} placements`);
  const pieces = per.map((p, i) => ({ poolId: poolId(rid_, t, i + 1), orderId: rid_, transactionId: t, lineKey: lineKey(rid_, t), sku, material: metal, copy: i + 1, quantity: qty, form, kind: Pair.kindOf(line, charm), lineKind: kind,
    side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.of, mirror: !!p.mirror, on: on[i] || null }));
  return { n: l.n, kind, sku, qty, metal, form, count, discs: kind === 'discs' ? count : 0, line, charm, pieces };
}
const PAIR_FIELDS = ['side', 'bodyIndex', 'groupKey', 'groupSize', 'mirror'];

/* ═══════════════════════════ shapes: direction is never lost ═══════════════════════════
   Each piece's cut outline as it lies on its sheet is a polygon list (laid): the design body, mirrored when the piece is a mirror, turned by the placement's angle, put at its place.
   The sheet charm carries it as `shapeJson` (a JSON STRING: a list of point lists is an array in an array, which Firestore refuses). */
const polysOfSeg = seg => Pair._flatten(seg, 8);
const bboxOfPolys = polys => { let x0 = 1e9, y0 = 1e9, x1 = -1e9, y1 = -1e9; for (const p of polys) for (const q of p) { if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; } return [x0, y0, x1, y1]; };
const round2 = v => Math.round(v * 100) / 100;
/** The polygons of the piece's own outline, as drawn in the master (the as-drawn body of the design, before any mirror). */
function bodyPolys(sku, bodyIndex) { const c = charmOf(sku), bodies = Pair.bodiesOf(c), b = bodies[Math.min(bodyIndex || 0, bodies.length - 1)]; return polysOfSeg(b.outline); }
/** Mirror about the vertical axis through the polygons' own bounding-box centre (x -> -x), as CharmNestPair.mirrorOf does. */
function mirrorPolys(polys) { const b = bboxOfPolys(polys), cx = (b[0] + b[2]) / 2; return polys.map(p => p.map(q => [2 * cx - q[0], q[1]])); }
/** Turn polygons by `deg` degrees about their own bounding-box centre, then put that centre at (cx, cy). A rotation: never a reflection. */
function layPolys(polys, deg, cx, cy) {
  const b = bboxOfPolys(polys), mx = (b[0] + b[2]) / 2, my = (b[1] + b[3]) / 2, a = (deg || 0) * Math.PI / 180, c = Math.cos(a), sn = Math.sin(a);
  return polys.map(p => p.map(q => [round2((q[0] - mx) * c - (q[1] - my) * sn + (cx || 0)), round2((q[0] - mx) * sn + (q[1] - my) * c + (cy || 0))]));
}
/** The outline of THIS piece laid on a sheet: the piece's own body (a mismatched pair's left or right), mirrored when piece.mirror, turned, placed. */
function laidShape(sku, piece, place) {
  let polys = bodyPolys(sku, piece.bodyIndex); if (piece.mirror) polys = mirrorPolys(polys);
  return layPolys(polys, (place && place.angle) || 0, (place && place.cx) || 0, (place && place.cy) || 0);
}
// the shape of a piece against its design: how far is it from the design as drawn, and from the design mirrored, under the BEST turn? (a Hausdorff distance over sampled boundary points, in
// units of the design's size: the placement's angle and sign convention are not needed, and a reflection can never be turned into the original, which is the point)
const outerOf = polys => polys.map(p => { let a = 0, cx = 0, cy = 0; for (let i = 0, j = p.length - 1; i < p.length; j = i++) { const f = p[j][0] * p[i][1] - p[i][0] * p[j][1]; a += f; cx += (p[j][0] + p[i][0]) * f; cy += (p[j][1] + p[i][1]) * f; } a /= 2; return { p, a: Math.abs(a), cx: a ? cx / (6 * a) : p[0][0], cy: a ? cy / (6 * a) : p[0][1] }; }).sort((x, y) => y.a - x.a)[0];
function boundary(poly, n) {
  const segs = []; let total = 0;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const L = Math.hypot(poly[i][0] - poly[j][0], poly[i][1] - poly[j][1]); segs.push({ a: poly[j], b: poly[i], L }); total += L; }
  const out = []; let k = 0, acc = 0;
  for (let i = 0; i < n; i++) { const d = (i + .5) * total / n; while (k < segs.length - 1 && acc + segs[k].L < d) { acc += segs[k].L; k++; } const sg = segs[k], t = sg.L ? (d - acc) / sg.L : 0; out.push([sg.a[0] + (sg.b[0] - sg.a[0]) * t, sg.a[1] + (sg.b[1] - sg.a[1]) * t]); }
  return out;
}
const distToPoly = (pt, poly) => { let best = Infinity; for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const ax = poly[j][0], ay = poly[j][1], dx = poly[i][0] - ax, dy = poly[i][1] - ay, L = dx * dx + dy * dy, t = L ? Math.max(0, Math.min(1, ((pt[0] - ax) * dx + (pt[1] - ay) * dy) / L)) : 0, ex = ax + t * dx - pt[0], ey = ay + t * dy - pt[1], d = ex * ex + ey * ey; if (d < best) best = d; } return Math.sqrt(best); };
function shapeFit(laidPolys, drawnPolys) {
  const L = outerOf(laidPolys), D = outerOf(drawnPolys); if (!L || !D) return { asDrawn: Infinity, mirrored: Infinity };
  const lp = L.p.map(q => [q[0] - L.cx, q[1] - L.cy]), size = Math.max(...bboxOfPolys([lp]).slice(2).map(Math.abs), 1e-6) * 2, la = boundary(lp, 48);
  const err = (poly, deg) => { const a = deg * Math.PI / 180, c = Math.cos(a), sn = Math.sin(a), r = poly.map(q => [q[0] * c - q[1] * sn, q[0] * sn + q[1] * c]); let worst = 0; for (const q of boundary(r, 48)) worst = Math.max(worst, distToPoly(q, lp)); for (const q of la) worst = Math.max(worst, distToPoly(q, r)); return worst / size; };
  const best = poly => { let b = Infinity, bd = 0; for (let d = 0; d < 360; d += 3) { const e = err(poly, d); if (e < b) { b = e; bd = d; } } for (let d = bd - 3; d <= bd + 3; d += .25) b = Math.min(b, err(poly, d)); return b; };
  const dp = D.p.map(q => [q[0] - D.cx, q[1] - D.cy]), dm = dp.map(q => [-q[0], q[1]]);
  return { asDrawn: best(dp), mirrored: best(dm) };
}
const SHAPE_TOL = 0.045, SHAPE_LOOSE = 0.08, SHAPE_RATIO = 1.5, SHAPE_GAP = 0.02;
/** What a laid outline is: "asDrawn" | "mirrored" | "either" (the two fit alike: a symmetric shape) | "none" (fits neither: the wrong body, or a distorted one).
 *  Judged by the two distances (shapeFit, in units of the shape's size) and their ratio, so a raster outline from the nester (thin features, a closed silhouette) still reads:
 *  a state wins when its distance is under SHAPE_LOOSE (.08) and the other is at least SHAPE_RATIO (1.5) times worse AND at least SHAPE_GAP (.02) worse (so noise in two tiny numbers decides nothing); both under SHAPE_LOOSE and closer than that = "either";
 *  both over SHAPE_LOOSE = "none". (A vector outline from the fixtures fits its own drawing at about .01, the wrong orientation at .1 and more.) */
function shapeState(laidPolys, drawnPolys) {
  const f = shapeFit(laidPolys, drawnPolys), a = f.asDrawn, m = f.mirrored, lo = Math.min(a, m), hi = Math.max(a, m);
  if (!(lo < SHAPE_LOOSE)) return 'none';
  if (hi >= lo * SHAPE_RATIO && hi - lo >= SHAPE_GAP) return a < m ? 'asDrawn' : 'mirrored';
  return 'either';
}
const REFLECT_FLAGS = ['flipX', 'reflect', 'reflected', 'mirrored', 'flipped', 'flip'];   // a placement that says it was reflected (a rotation never sets one)
const reflectedPlacement = pl => !!pl && (REFLECT_FLAGS.some(k => pl[k] === true) || +pl.scaleX < 0 || +pl.sx < 0 || +pl.scale < 0);

/** Build every document a spec needs. See the header. spec.rotate: true turns pieces by 0, 90, 180, 270 in turn (a rotation is always allowed). */
function world(spec) {
  const orders = (spec.orders || []).map(o => ({ rid: o.rid, lines: o.lines.map(l => lineOf(o.rid, l)) }));
  const sheetDefs = (spec.sheets || SHEETS).map(s => Object.assign({ id: s.id, metal: s.metal, n: s.n, set: s.set }, {})), byId = Object.fromEntries(sheetDefs.map(s => [s.id, s]));
  const pieces = orders.flatMap(o => o.lines.flatMap(l => l.pieces));
  for (const p of pieces) if (p.on && !byId[p.on]) throw new Error(`piece ${p.poolId} is on ${p.on}, which the world does not have`);
  const designMap = designs();
  const pool = pieces.map((p, i) => Object.assign({ poolId: p.poolId, orderId: p.orderId, transactionId: p.transactionId, lineKey: p.lineKey, sku: p.sku, material: p.material, form: p.form, copy: p.copy, quantity: p.quantity, runId: RUN,
    state: p.on ? 'written' : 'ready', sheetId: p.on || null, setId: p.on ? byId[p.on].set : null, sheetName: p.on ? fileBase(byId[p.on]) : null, createdAt: T0 + i, updatedAt: T0 + 1000 + i },
    Object.fromEntries(PAIR_FIELDS.map(k => [k, p[k]]))));
  const sheets = sheetDefs.map(s => {
    const mine = pieces.filter(p => p.on === s.id); if (!mine.length) return null;
    const placements = [];
    const charms = mine.map((p, i) => {
      const body = designMap[p.sku].bodies[Math.min(p.bodyIndex, designMap[p.sku].bodies.length - 1)], id = `${s.id}-c${i}`, angle = spec.rotate ? [0, 90, 180, 270][i % 4] : 0, cx = 30 + (i % 6) * 40, cy = 30 + Math.floor(i / 6) * 40;
      placements.push({ id, cxPt: cx, cyPt: cy, angle, wPt: +body.w.toFixed(2), hPt: +body.h.toFixed(2) });
      return { id, poolId: p.poolId, order: p.orderId, name: `${p.orderId} · ${p.sku}${p.groupSize > 1 ? ` · ${p.copy}/${p.groupSize}` : ''}`, sku: p.sku, lineKey: p.lineKey, side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.groupSize, mirror: p.mirror,
        widthPt: +body.w.toFixed(2), heightPt: +body.h.toFixed(2), shapeJson: JSON.stringify(laidShape(p.sku, p, { angle, cx, cy })) };
    });
    return { id: s.id, setId: s.set, setSeq: SET_SEQ[s.set] || 1, sheetIndex: s.n, runId: RUN, metal: s.metal, day: DAY, fileBase: fileBase(s), folder: fileBase(s), status: 'complete', placedCount: placements.length, charmCount: charms.length, density: .5,
      stock: { wPt: 300, hPt: 150 }, placements, charms, poolIds: charms.map(c => c.poolId), orders: [...new Set(charms.map(c => c.order))], verification: { ok: true }, outputs: {}, label: { files: [], orders: [...new Set(charms.map(c => c.order))] }, archived: false, createdAt: T0, updatedAt: T0 + 5000 };
  }).filter(Boolean);
  const sets = [...new Set(sheets.map(s => s.setId))].map(id => {
    const mine = sheets.filter(s => s.setId === id), ord = {};   // (a set record lists each copy with its sheet and side: orders[rid].lines[{ transactionId, sku, copies:[{ copy, sheetId, sheet, poolId, side }] }])
    for (const sh of mine) for (const c of sh.charms) { const pc = pieces.find(p => p.poolId === c.poolId), o = ord[pc.orderId] = ord[pc.orderId] || { held: null, lines: [] }; let line = o.lines.find(l => l.transactionId === pc.transactionId); if (!line) { line = { transactionId: pc.transactionId, sku: pc.sku, copies: [] }; o.lines.push(line); } line.copies.push({ copy: pc.copy, sheetId: sh.id, sheet: sh.fileBase, poolId: pc.poolId, backPoolId: null, ...(pc.side ? { side: pc.side } : {}) }); }
    return { setId: id, seq: SET_SEQ[id] || 1, day: DAY, runId: RUN, sheetIds: mine.map(s => s.id), materials: [...new Set(mine.map(s => s.metal))], orders: ord, labelFiles: [], status: 'labelled' }; });
  const lines = {}, kinds = {}; for (const o of orders) for (const l of o.lines) { lines[lineKey(o.rid, tx(l.n))] = { orderId: o.rid, transactionId: tx(l.n), sku: l.sku, state: l.pieces.every(p => p.on) ? 'written' : 'pooled', quantity: l.qty, material: l.metal, poolIds: l.pieces.map(p => p.poolId), kind: KINDS[l.kind].pair, pieceCount: l.pieces.length }; kinds[groupKey(o.rid, tx(l.n))] = KINDS[l.kind].pair; }
  const run = { runId: RUN, status: 'running', step: 'nest', day: DAY, lines, orders: orders.map(o => o.rid), sheets: {}, holds: {}, errors: [], resumable: true };
  return { orders, pieces, pool, sheets, sets, run, designs: designMap, kinds, tracked: (spec.tracked || []).slice(), sheetDefs };
}
const clone = w => (typeof structuredClone === 'function' ? structuredClone(w) : JSON.parse(JSON.stringify(w)));
/** The same world as the app stored it before pairs existed: no side, bodyIndex, groupKey, groupSize or mirror on any piece, no shapes, no kinds. */
function legacy(w) {
  const c = clone(w); for (const p of c.pool) for (const k of PAIR_FIELDS) delete p[k];
  for (const s of c.sheets) for (const ch of s.charms) { for (const k of PAIR_FIELDS) delete ch[k]; delete ch.shapeJson; }
  for (const p of c.pieces) for (const k of PAIR_FIELDS) delete p[k];
  for (const set of c.sets) for (const o of Object.values(set.orders || {})) for (const l of o.lines) for (const cp of l.copies) delete cp.side;
  for (const l of Object.values(c.run.lines)) { delete l.kind; delete l.pieceCount; }
  c.kinds = {}; c.legacy = true; return c;
}

/* ═══════════════════════════ the named cases ═══════════════════════════ */
const FILLER = [{ rid: rid(90), lines: [{ n: 90, kind: 'single', on: 'sh-gf1' }] }, { rid: rid(91), lines: [{ n: 91, kind: 'single', on: 'sh-gf2' }] }, { rid: rid(92), lines: [{ n: 92, kind: 'single', on: 'sh-gf4', metal: 'gold' }] }, { rid: rid(93), lines: [{ n: 93, kind: 'single', metal: 'silver', on: 'sh-ss1' }] }];
const gk = (o, n) => groupKey(rid(o), tx(n));
const CASES = {
  pairOneSheet: { about: 'a pair of earrings drawn facing left (a Left as drawn, a Right mirrored) on one sheet', build: () => world({ orders: [{ rid: rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairFacesRight: { about: 'a pair drawn facing RIGHT: the Right is as drawn, the Left is the mirror', build: () => world({ orders: [{ rid: rid(11), lines: [{ n: 10, kind: 'pair', sku: 'PAIR-FACE-R', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairSymmetric: { about: 'a symmetric stud pair: still a Left and a Right, the Right flagged mirrored (the shapes look alike)', build: () => world({ orders: [{ rid: rid(12), lines: [{ n: 10, kind: 'pair', sku: 'PAIR-STUD', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairQty2: { about: 'a pair with quantity 2: two Left and two Right, copies alternate L, R, L, R', build: () => world({ orders: [{ rid: rid(13), lines: [{ n: 10, kind: 'pair', qty: 2, on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairRotated: { about: 'a pair and a hoop pair turned by 0, 90, 180, 270 degrees: a turn is always allowed (nothing to report)', build: () => world({ rotate: true, orders: [{ rid: rid(14), lines: [{ n: 10, kind: 'pair', qty: 2, on: 'sh-gf1' }, { n: 11, kind: 'mismatched', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairSplitOneSet: { about: 'a pair split over two sheets of one set (tracked)', build: () => world({ orders: [{ rid: rid(2), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }, ...FILLER], tracked: [gk(2, 10)] }), expect: [] },
  pairSplitTwoSets: { about: 'a pair split over two sheets of two sets (never allowed)', build: () => world({ orders: [{ rid: rid(3), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf4'] }] }, ...FILLER], tracked: [gk(3, 10)] }), expect: ['split-across-sets'] },
  discs3Sheets: { about: 'a 3-disc necklace, a disc on each of three sheets of one set (tracked): no side, never mirrored', build: () => world({ orders: [{ rid: rid(4), lines: [{ n: 10, kind: 'discs', discs: 3, on: ['sh-gf1', 'sh-gf2', 'sh-gf3'] }] }, ...FILLER], tracked: [gk(4, 10)] }), expect: [] },
  lettersNecklace: { about: 'a letters necklace of 4 letters on one sheet: 4 pieces, no side, never mirrored', build: () => world({ orders: [{ rid: rid(15), lines: [{ n: 10, kind: 'letters', letters: 4, on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  singleLine: { about: 'a "Single" earring line, quantity 2: two pieces with no side (a single is not a pair), next to a pair', build: () => world({ orders: [{ rid: rid(16), lines: [{ n: 10, kind: 'earring-single', qty: 2, on: 'sh-gf1' }, { n: 11, kind: 'pair', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  mixedSheet: { about: 'one sheet with studs, hoops, discs, letters, a Single line, a pendant and a mismatched pair', build: () => world({ orders: [
      { rid: rid(5), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }, { n: 11, kind: 'hoop', on: 'sh-gf1' }] },
      { rid: rid(6), lines: [{ n: 10, kind: 'discs', discs: 4, on: 'sh-gf1' }, { n: 11, kind: 'single', on: 'sh-gf1' }] },
      { rid: rid(7), lines: [{ n: 10, kind: 'mismatched', on: 'sh-gf1' }] },
      { rid: rid(17), lines: [{ n: 10, kind: 'letters', letters: 3, on: 'sh-gf1' }, { n: 11, kind: 'earring-single', qty: 2, on: 'sh-gf1' }] }, ...FILLER.slice(1)] }), expect: [] },
  mismatchedTwoOutlines: { about: 'a mismatched pair (a ball and a racket) on one sheet, left then right', build: () => world({ orders: [{ rid: rid(8), lines: [{ n: 10, kind: 'mismatched', sku: 'TENNIS-MIS', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  mismatchedMittens: { about: 'the picture: MITTENS 1 and MITTENS 2 under one label, both drawn facing left, so the Right earring is the second body mirrored', build: () => world({ orders: [{ rid: rid(18), lines: [{ n: 10, kind: 'mismatched', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  mismatchedSplit: { about: 'a mismatched pair, the left on one sheet and the right on another of one set (tracked)', build: () => world({ orders: [{ rid: rid(9), lines: [{ n: 10, kind: 'mismatched', on: ['sh-gf1', 'sh-gf2'] }] }, ...FILLER], tracked: [gk(9, 10)] }), expect: [] },
  pairWaiting: { about: 'a pair of which one piece is on a sheet and one waits (a half-placed group)', build: () => world({ orders: [{ rid: rid(10), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', null] }] }, ...FILLER] }), expect: ['untracked-split'] },
};
const cases = Object.fromEntries(Object.entries(CASES).map(([k, v]) => [k, v.build]));
const caseList = () => Object.entries(CASES).map(([id, c]) => ({ id, about: c.about, world: c.build(), expect: c.expect.slice() }));

/* ═══════════════════════════ the fake Firestore ═══════════════════════════ */
function fakeFirestore(opts) {
  const st = makeState(Object.assign({ strictArrays: true }, opts || {})), admin = fakeAdmin(st, 'http://127.0.0.1:0'), db = admin.firestore(), writes = [];
  const fs = { st, admin, db, writes, strict: () => st.strictArrays,
    put(coll, id, doc) { if (st.strictArrays) refuseNestedArrays(doc, coll + '/' + id); writes.push(coll + '/' + id); st.docs.set(coll + '/' + id, clone(doc)); return doc; },
    get: (coll, id) => st.doc(coll, id), list: coll => st.list(coll), del(coll, id) { writes.push('-' + coll + '/' + id); st.docs.delete(coll + '/' + id); },
    seed(w) {
      for (const p of w.pool) fs.put(COLL.pool, p.poolId, p);
      for (const s of w.sheets) fs.put(COLL.sheets, s.id, s);
      for (const s of w.sets) fs.put(COLL.sets, s.setId, s);
      fs.put(COLL.runs, w.run.runId, w.run); return fs;
    },
    docsOf() { return { pool: st.list(COLL.pool), sheets: st.list(COLL.sheets).filter(s => !s.archived).map(s => Object.assign({ id: s._id }, s)), sets: st.list(COLL.sets), runs: st.list(COLL.runs), designs: designs() }; } };
  return fs;
}
/** The REAL netlify functions over that fake: { handlers, call(name, body), lib(op, args), restore() }. */
function functions(fs, names) {
  const Module = require('module'), realLoad = Module._load, fnDir = path.join(__dirname, '../../netlify/functions'), handlers = {};
  Module._load = function (req, ...rest) { if (req === 'firebase-admin' || /[\/]firebaseAdmin(\.js)?$/.test(req) || req === './firebaseAdmin') return fs.admin; return realLoad.call(this, req, ...rest); };
  for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k];   // (modules bind firebase-admin when first loaded: load them again over this fake)
  process.env.CHARM_NEST_DELETE_CODE = process.env.CHARM_NEST_DELETE_CODE || 'pairs-' + Math.random().toString(36).slice(2, 10);
  for (const n of names || ['charmNestLibrary']) handlers[n] = require(path.join(fnDir, n + '.js'));
  const call = async (name, body) => { const out = await handlers[name].handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body || {}), queryStringParameters: {} }); return JSON.parse(out.body || '{}'); };
  return { handlers, call, lib: (op, args) => call('charmNestLibrary', Object.assign({ op }, args || {})), restore() { Module._load = realLoad; for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k]; } };
}

/* ═══════════════════════════ the pair checker ═══════════════════════════ */
const held = r => !!r && r.state === 'abandoned' && (+r.heldAt > 0 || r.heldBy || +r.removedAt > 0 || r.removedBy);
const HEALTHY = r => r && r.state !== 'abandoned' && r.state !== 'superseded';
/** Groups from the cloud documents: Map(groupKey -> { key, pieces:[{ poolId, copy, row, sheets:[sheet doc], charm, charms:[{ sheet, charm, placement }] }] }). A group is derived from the pool id, never from the stored field. */
function groupsOf(docs) {
  const sheets = (docs.sheets || []).filter(s => !s.archived), out = new Map();
  const at = (k, poolId) => { if (!out.has(k)) out.set(k, { key: k, pieces: new Map() }); const g = out.get(k); if (!g.pieces.has(poolId)) g.pieces.set(poolId, { poolId, copy: (poolParts(poolId) || {}).copy || 0, row: null, sheets: [], charm: null, charms: [], placement: null }); return g.pieces.get(poolId); };
  for (const r of docs.pool || []) { const k = gkOfPool(r.poolId); if (k) at(k, r.poolId).row = r; }
  for (const s of sheets) for (const id of s.poolIds || []) { const k = gkOfPool(id); if (!k) continue; const p = at(k, id); p.sheets.push(s); const c = (s.charms || []).find(c => c && c.poolId === id); p.charms.push({ sheet: s, charm: c || null, placement: c ? (s.placements || []).find(q => q && q.id === c.id) || null : null }); p.charm = p.charm || c || null; }
  for (const g of out.values()) g.pieces = [...g.pieces.values()].sort((a, b) => a.copy - b.copy);
  return out;
}
const sheetsOf = g => [...new Set(g.pieces.flatMap(p => p.sheets.map(s => s.id || s._id)))].sort();
/** What kind of line each group is (key "rid:tx" -> pair | mismatched | single | discs | letters ...), from the run documents (their lines carry `kind`, as the intake sets spec.pair.kind). */
function kindsOfRuns(runs) { const out = {}; for (const r of runs || []) for (const [k, l] of Object.entries((r && r.lines) || {})) { const m = /^(\d{4,20})_(.+)$/.exec(k); if (m && l && l.kind) out[groupKey(m[1], m[2])] = l.kind; } return out; }
// the laid outline of a sheet charm against its design: memoised (the search over turns is the slow part)
const SHAPE_MEMO = new Map();
function shapeOfCharm(design, bodyIndex, shapeJson) {
  if (!design || !shapeJson) return null;
  const key = design.sku + '|' + bodyIndex + '|' + shapeJson, hit = SHAPE_MEMO.get(key); if (hit) return hit;
  let laid; try { laid = JSON.parse(shapeJson); } catch (e) { return { state: 'unreadable' }; }
  const bodies = Pair.bodiesOf(design.charm), b = bodies[Math.min(bodyIndex || 0, bodies.length - 1)];
  const out = { state: shapeState(laid, polysOfSeg(b.outline)) }; SHAPE_MEMO.set(key, out); return out;
}
/** Everything that makes one group's pieces disagree about their sheet, side, mirror or group. See the header for the codes. */
function problems(docs, opts) {
  opts = opts || {}; const out = [], tracked = new Set(opts.tracked || docs.tracked || []), designs_ = docs.designs || {}, sheetById = new Map((docs.sheets || []).filter(s => !s.archived).map(s => [s.id || s._id, s]));
  const kinds = Object.assign({}, kindsOfRuns(docs.runs || (docs.run ? [docs.run] : [])), docs.kinds || {}), mirrorOn = opts.mirror !== false;
  const add = (code, g, text, poolId) => out.push({ code, groupKey: g ? g.key : '', poolId: poolId || null, text });
  const sid = s => s.id || s._id, word = s => (s === 'L' ? 'Left' : s === 'R' ? 'Right' : 'no side'), yes = b => (b ? 'mirrored' : 'as drawn');
  for (const g of groupsOf(docs).values()) {
    const ps = g.pieces, rows = ps.map(p => p.row).filter(Boolean), hasFields = rows.some(r => r.groupKey != null || r.side !== undefined) || ps.some(p => p.charm && p.charm.groupKey != null);
    const sku = (rows[0] && rows[0].sku) || (ps.find(p => p.charm && p.charm.sku) || { charm: {} }).charm.sku, d = sku && designs_[sku] ? designs_[sku] : null, misDesign = d ? Pair.isMismatched(d.charm) : false;
    // what the line is: the run says; else the rows' sides say; else the design and the form
    let kind = kinds[g.key] || null;
    if (!kind) kind = rows.some(r => r.side === 'L' || r.side === 'R') ? (misDesign ? 'mismatched' : 'pair') : misDesign ? 'mismatched' : (rows[0] && rows[0].form && Pair.isEarringPair({ form: rows[0].form }, d && d.charm)) ? 'pair' : (rows.length > 1 ? 'multi' : 'single');
    const earring = kind === 'pair' || kind === 'mismatched';
    // key and size
    for (const p of ps) {
      if (p.row && p.row.groupKey != null && p.row.groupKey !== g.key) add('key-mismatch', g, `${p.poolId}: its pool row says group ${p.row.groupKey}, its pool id says ${g.key}`, p.poolId);
      for (const c of p.charms) if (c.charm && c.charm.groupKey != null && c.charm.groupKey !== g.key) add('key-mismatch', g, `${p.poolId}: ${sheetLabel2(c.sheet)} says group ${c.charm.groupKey}, its pool id says ${g.key}`, p.poolId);
    }
    const sizes = new Set([...rows.map(r => r.groupSize), ...ps.flatMap(p => p.charms.map(c => c.charm && c.charm.groupSize))].filter(v => v != null));
    if (sizes.size > 1) add('size-mismatch', g, `the pieces of ${g.key} disagree about how many there are (${[...sizes].join(' and ')})`);
    const size = sizes.size === 1 ? [...sizes][0] : null;
    if (size != null && rows.length) {
      const copies = rows.map(r => r.copy).sort((a, b) => a - b), absent = Array.from({ length: size }, (_, i) => i + 1).filter(c => !copies.includes(c));
      if (absent.length) add('missing-piece', g, `${g.key} is ${size} piece(s) but the pool has no copy ${absent.join(', ')} (it holds ${rows.length})`);
      if (rows.length > size) add('size-mismatch', g, `${g.key} says ${size} piece(s) but the pool holds ${rows.length}`);
    }
    // sides: an earring pair is a Left and a Right per unit (copies alternate L, R), matching or mismatched; discs, letters, singles have none
    const sideOfCopy = copy => Pair.sideOf((copy - 1) % 2, 2);
    for (const p of ps) {
      const rs = p.row && p.row.side, cs = p.charms.filter(c => c.charm).map(c => c.charm.side);
      for (const v of cs) if (p.row && p.row.side !== undefined && v !== undefined && (v || null) !== (rs || null)) add('side-mismatch', g, `${p.poolId}: the pool row says ${word(rs)}, the sheet says ${word(v)}`, p.poolId);
      if (new Set(p.charms.map(c => c.charm && (c.charm.side || null))).size > 1) add('side-mismatch', g, `${p.poolId}: its sheets disagree about its side`, p.poolId);
    }
    if (hasFields) {
      const L = rows.filter(r => r.side === 'L').length, R = rows.filter(r => r.side === 'R').length;
      for (const p of ps) {
        const sides = [p.row && p.row.side, ...p.charms.map(c => c.charm && c.charm.side)];
        if (earring) {
          const want = sideOfCopy(p.copy);
          if (p.row && p.row.side !== 'L' && p.row.side !== 'R') add('side-missing', g, `${p.poolId}: a piece of an earring pair has no side (copy ${p.copy} is the ${word(want)})`, p.poolId);
          else if (p.row && p.row.side !== want) add('side-mismatch', g, `${p.poolId}: copy ${p.copy} of an earring pair is the ${word(want)}, the pool row says ${word(p.row.side)}`, p.poolId);
          for (const c of p.charms) if (c.charm && c.charm.side !== 'L' && c.charm.side !== 'R') add('side-missing', g, `${p.poolId}: ${sheetLabel2(c.sheet)} holds a piece of an earring pair with no side`, p.poolId);
        } else {
          for (const v of sides) if (v === 'L' || v === 'R') { add('side-unexpected', g, `${p.poolId}: a ${kind} piece (not an earring pair) says it is the ${word(v)}: only earring pairs have a side`, p.poolId); break; }
        }
      }
      if (earring) {
        if (L !== R) add('side-pairing', g, `${g.key} has ${L} left and ${R} right piece(s): an earring pair makes one of each per unit`);
        for (const r of rows) { const wantBody = kind === 'mismatched' ? (r.side === 'L' ? 0 : r.side === 'R' ? 1 : null) : 0; if (r.bodyIndex != null && wantBody != null && r.bodyIndex !== wantBody) add('side-pairing', g, `${r.poolId}: body ${r.bodyIndex} cannot be the ${word(r.side)} of a ${kind} pair`, r.poolId); }
      }
    }
    // mirror: the Right is the Left mirrored; a piece's flag follows its side and the way its body faces; the sheet agrees with the pool
    if (mirrorOn && hasFields) {
      const facingOfPiece = r => { const f = d && d.facings ? d.facings[Math.min(r.bodyIndex || 0, d.facings.length - 1)] : null; return f || 'L'; };
      for (const p of ps) {
        const rm = p.row ? p.row.mirror : undefined, cm = p.charms.filter(c => c.charm).map(c => c.charm.mirror);
        for (const v of cm) if (rm !== undefined && v !== undefined && !!v !== !!rm) add('mirror-mismatch', g, `${p.poolId}: the pool row says ${yes(rm)}, ${sheetLabel2(p.sheets[0])} says ${yes(v)}`, p.poolId);
        if (earring) {
          if (p.row && rm === undefined) add('mirror-missing', g, `${p.poolId}: a piece of an earring pair has no mirror flag`, p.poolId);
          for (const c of p.charms) if (c.charm && c.charm.mirror === undefined) add('mirror-missing', g, `${p.poolId}: ${sheetLabel2(c.sheet)} holds a piece of an earring pair with no mirror flag`, p.poolId);
          if (p.row && rm !== undefined && d && (p.row.side === 'L' || p.row.side === 'R')) { const want = p.row.side !== facingOfPiece(p.row); if (!!rm !== want) add('mirror-mismatch', g, `${p.poolId}: the ${word(p.row.side)} of ${sku} (drawn facing ${facingOfPiece(p.row) === 'L' ? 'left' : 'right'}) must be ${yes(want)}, the pool row says ${yes(rm)}`, p.poolId); }
        } else if (rm === true || cm.some(v => v === true)) add('mirror-unexpected', g, `${p.poolId}: a ${kind} piece (not an earring pair) is marked mirrored: only the Right of an earring pair is`, p.poolId);
      }
      if (earring && kind === 'pair') {   // one body: of a Left and a Right, exactly one is mirrored (the other is the drawing as it is)
        const byUnit = new Map(); for (const r of rows) { if (r.mirror === undefined || (r.side !== 'L' && r.side !== 'R')) continue; const u = Math.ceil(r.copy / 2); if (!byUnit.has(u)) byUnit.set(u, {}); byUnit.get(u)[r.side] = !!r.mirror; }
        for (const [u, m] of byUnit) if (m.L !== undefined && m.R !== undefined && m.L === m.R) add('mirror-pairing', g, `${g.key} unit ${u}: the Left and the Right are both ${yes(m.L)}: one of the two must be the mirror of the other`);
      }
    }
    // direction on the sheet: each laid outline (shapeJson) is the piece's own body, mirrored exactly when the piece is a mirror; a Right is the Left mirrored; the nester only turned it
    if (mirrorOn && d) {
      const shapes = [];   // [{ p, state, flag }]
      for (const p of ps) for (const c of p.charms) {
        if (!c.charm) continue;
        const bodyIndex = (p.row && p.row.bodyIndex != null) ? p.row.bodyIndex : (c.charm.bodyIndex || 0), flag = p.row && p.row.mirror !== undefined ? !!p.row.mirror : !!c.charm.mirror;
        if (reflectedPlacement(c.placement)) add('reflected', g, `${p.poolId}: ${sheetLabel2(c.sheet)} places it with a reflection (${REFLECT_FLAGS.filter(k => c.placement[k] === true).join(', ') || 'negative scale'}): the nester may turn a piece, never reflect it`, p.poolId);
        const sh = shapeOfCharm(d, bodyIndex, c.charm.shapeJson); if (!sh) continue;
        if (sh.state === 'none' || sh.state === 'unreadable') { add('shape-mismatch', g, `${p.poolId}: the outline on ${sheetLabel2(c.sheet)} is not the outline of ${sku}${d.bodies && d.bodies.length > 1 ? ' body ' + bodyIndex : ''}, as drawn or mirrored, at any turn`, p.poolId); continue; }
        shapes.push({ p, state: sh.state, flag, sheet: c.sheet });
        if (sh.state !== 'either' && (sh.state === 'mirrored') !== flag) add('reflected', g, `${p.poolId}: it should lie ${yes(flag)} but its outline on ${sheetLabel2(c.sheet)} is ${yes(sh.state === 'mirrored')}: it was reflected (a piece may be turned, never reflected)`, p.poolId);
      }
      if (kind === 'pair') {
        const byUnit = new Map(); for (const s of shapes) { const sd = s.p.row && s.p.row.side; if ((sd !== 'L' && sd !== 'R') || s.state === 'either') continue; const u = Math.ceil(s.p.copy / 2); if (!byUnit.has(u)) byUnit.set(u, {}); byUnit.get(u)[sd] = s; }
        for (const [u, m] of byUnit) if (m.L && m.R && m.L.state === m.R.state) add('not-mirror', g, `${g.key} unit ${u}: the Right (${m.R.p.poolId}) and the Left (${m.L.p.poolId}) both lie ${yes(m.L.state === 'mirrored')}: the Right must be the Left mirrored`, m.R.p.poolId);
      }
    }
    // sheet agreement, duplicates
    for (const p of ps) {
      if (p.sheets.length > 1) add('duplicate-piece', g, `${p.poolId} is listed by ${p.sheets.map(sid).join(' and ')}`, p.poolId);
      const on = p.sheets[0];
      if (p.row && HEALTHY(p.row)) {
        if (on && p.row.sheetId && p.row.sheetId !== sid(on)) add('sheet-disagree', g, `${p.poolId}: the pool row says ${p.row.sheetId}, ${sid(on)} lists it`, p.poolId);
        if (on && !p.row.sheetId) add('sheet-disagree', g, `${p.poolId}: ${sid(on)} lists it, the pool row says no sheet`, p.poolId);
        if (!on && p.row.sheetId && sheetById.has(p.row.sheetId)) add('sheet-disagree', g, `${p.poolId}: the pool row says ${p.row.sheetId}, which does not list it`, p.poolId);
      }
      for (const c of p.charms) {
        if (!c.charm) add('sheet-disagree', g, `${p.poolId}: ${sheetLabel2(c.sheet)} lists it in poolIds but holds no charm for it`, p.poolId);
        else if (!c.placement) add('sheet-disagree', g, `${p.poolId}: ${sheetLabel2(c.sheet)} holds its charm but no placement`, p.poolId);
      }
    }
    for (const s of sheetById.values()) for (const c of s.charms || []) { const k = gkOfPool(c.poolId); if (k === g.key && !(s.poolIds || []).includes(c.poolId)) add('sheet-disagree', g, `${c.poolId}: ${sheetLabel2(s)} holds its charm but poolIds does not list it`, c.poolId); }
    // together
    const onSheets = sheetsOf(g), live = ps.filter(p => !p.row || HEALTHY(p.row)), placed = live.filter(p => p.sheets.length), waiting = live.filter(p => !p.sheets.length);
    if (live.length > 1 && (onSheets.length > 1 || (onSheets.length === 1 && waiting.length && !opts.allowWaiting)) && !tracked.has(g.key) && !opts.allowSplit) add('untracked-split', g, `${g.key}: ${placed.length} piece(s) on ${onSheets.join(' + ') || 'no sheet'}, ${waiting.length} waiting, and nothing says the split is tracked`);
    const sets = [...new Set(onSheets.map(id => sheetById.get(id)).filter(Boolean).map(s => s.setId || null))];
    if (onSheets.length > 1 && sets.length > 1) add('split-across-sets', g, `${g.key} is on ${onSheets.join(' and ')}, in sets ${sets.map(x => x || 'none').join(' and ')}: the sheets of one group stay in ONE set`);
    // metal
    const metals = new Set(rows.map(r => r.material).filter(Boolean)); if (metals.size > 1) add('metal-mismatch', g, `${g.key} is made in ${[...metals].join(' and ')}: one metal for every piece`);
    for (const p of ps) for (const s of p.sheets) if (p.row && p.row.material && s.metal && p.row.material !== s.metal) add('metal-mismatch', g, `${p.poolId} is ${p.row.material} but ${sheetLabel2(s)} is ${s.metal}`, p.poolId);
    // sets
    for (const p of ps) { const s = p.sheets[0]; if (s && p.row && HEALTHY(p.row) && p.row.setId != null && (s.setId || null) !== (p.row.setId || null)) add('set-disagree', g, `${p.poolId}: the pool row says set ${p.row.setId}, its sheet ${sheetLabel2(s)} is in ${s.setId || 'none'}`, p.poolId); }
    // held
    const heldN = rows.filter(r => held(r)).length; if (heldN && heldN < rows.length) add('half-held', g, `${g.key}: ${heldN} of ${rows.length} piece(s) are on hold or cancelled, the rest are not`);
  }
  // the set record lists each copy with its sheet and side (orders[rid].lines[].copies[]): it must say where the piece is and which ear it is
  const rowById = new Map((docs.pool || []).map(r => [r.poolId, r])), sheetOfPiece = new Map(); for (const s of sheetById.values()) for (const id of s.poolIds || []) sheetOfPiece.set(id, sid(s));
  for (const set of docs.sets || []) for (const [rid_, od] of Object.entries(set.orders || {})) for (const ln of (Array.isArray(od && od.lines) ? od.lines : Object.values((od && od.lines) || {}))) for (const cp of (ln && ln.copies) || []) {
    const where = sheetOfPiece.get(cp.poolId), row = rowById.get(cp.poolId), k = gkOfPool(cp.poolId), bad = t => out.push({ code: 'set-copy-disagree', groupKey: k, poolId: cp.poolId, text: `set ${set.setId || set._id}, order ${rid_}: ${cp.poolId} ${t}` });
    if (where && cp.sheetId && where !== cp.sheetId) bad(`is listed on ${cp.sheetId}, the sheet records have it on ${where}`);
    else if (!where && cp.sheetId && sheetById.has(cp.sheetId) && row && HEALTHY(row)) bad(`is listed on ${cp.sheetId}, which does not hold it`);
    if (cp.side && row && row.side !== undefined && (row.side || null) !== cp.side) bad(`is listed as the ${word(cp.side)}, the pool row says ${word(row.side)}`);
  }
  for (const set of docs.sets || []) for (const id of set.sheetIds || []) { const s = sheetById.get(id); if (s && (s.setId || null) !== (set.setId || set._id)) out.push({ code: 'set-disagree', groupKey: '', poolId: null, text: `set ${set.setId || set._id} lists ${id}, whose own record says set ${s.setId || 'none'}` }); }
  for (const s of sheetById.values()) if (s.setId && (docs.sets || []).length) { const set = (docs.sets || []).find(x => (x.setId || x._id) === s.setId); if (set && !(set.sheetIds || []).includes(sid(s))) out.push({ code: 'set-disagree', groupKey: '', poolId: null, text: `${sid(s)} says set ${s.setId}, which does not list it` }); }
  return out;
}
const sheetLabel2 = s => s ? `${CODE[s.metal] || 'GF'} Sheet ${s.sheetIndex || (/_Sheet-(\d+)/.exec(s.fileBase || '') || [])[1] || '?'}` : 'a sheet';

/* ═══════════════════════════ Etsy receipts (the emulated Etsy / the sandbox stream) ═══════════════════════════ */
const METAL_WORD = { gold: '14k Gold Filled', silver: 'Sterling Silver', rose: '14k Rose Gold Filled' };
function receipts(w, at) {
  const t = Math.floor((at || T0) / 1000);
  return w.orders.map((o, i) => ({ receipt_id: Number(o.rid), order_number: o.rid, name: 'Buyer ' + o.rid.slice(-3), country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: t - 3600 * (w.orders.length - i), created_timestamp: t - 3600 * (w.orders.length - i), update_timestamp: t, updated_timestamp: t, status: 'Paid', is_paid: true, is_shipped: false,
    transactions: o.lines.map(l => ({ transaction_id: Number(tx(l.n)), receipt_id: Number(o.rid), listing_id: 1900000000 + l.n, sku: l.sku, title: l.line.title, quantity: l.qty, expected_ship_date: t + 5 * 86400, shipped_timestamp: null,
      variations: [l.kind === 'discs' ? { formatted_name: 'Number of Discs / Metal', formatted_value: `${l.discs} discs • ${l.metal === 'gold' ? 'gold' : l.metal}` } : l.kind === 'earring-single' ? { formatted_name: 'Price', formatted_value: `${METAL_WORD[l.metal] || l.metal} - Single` } : { formatted_name: 'Metal Choice', formatted_value: METAL_WORD[l.metal] || l.metal }], is_personalized: false })) }));
}
// the real listings of the Sep 17 sandbox snapshot each case stands for (listing ids and SKUs only; see PAIRTESTS-points.md for counts and order numbers)
const EXAMPLES = {
  mismatchedTwoOutlines: { listing: 1744372161, sku: 'Huggie Hoops-Tennis Ball/Racket3', orders: ['4173373368', '4171010675'], note: 'Mismatched Tennis Ball and Raquet Huggie Hoops: the SKU is in no master; the catalogue holds TENNIS BALL (HUGGIE) and TENNIS RACKET (HUGGIE)' },
  mismatchedTwoOutlinesMaster: { skus: ['MISMATCHED', 'MISMATCHED_6849', 'MISMATCHED_7134'], note: 'catalogue designs named for the case (members 3, 3, 2)' },
  discs3Sheets: { listing: 234758391, sku: 'Initial_8391', orders: ['4172791262'], note: '3 discs, variation "3 discs • gold"' },
  discs2: { listing: 1008014571, sku: 'nitial_Disc_4571', orders: ['4175370240'], note: '2 discs, variation "ROSEGOLD - 2 Disc"' },
  pairOneSheet: { note: 'every earring line with quantity 1: 102 of the 105 earring lines of the snapshot' },
};

const expectedMirror = (sku, side, bodyIndex) => { const f = (DESIGN_FACING[sku] || [null])[Math.min(bodyIndex || 0, (DESIGN_FACING[sku] || [null]).length - 1)]; return side === 'L' || side === 'R' ? side !== (f || 'L') : false; };
const shapeJsonOf = (sku, piece, place) => JSON.stringify(laidShape(sku, piece, place));
module.exports = { COLL, CODE, DAY, RUN, ids, rid, tx, poolId, lineKey, groupKey, poolParts, gkOfPool, KINDS, pieceCount, designs, charmOf, entryOf, world, lineOf, clone, legacy, cases, caseList, CASES, SHEETS, FILLER,
  fakeFirestore, functions, problems, groupsOf, sheetsOf, kindsOfRuns, receipts, EXAMPLES, fileBase, sheetLabel: s => sheetLabel(s), PAIR_FIELDS, refuseNestedArrays,
  DESIGN_FACING, expectedMirror, laidShape, shapeJsonOf, shapeState, shapeFit, SHAPE_TOL, SHAPE_LOOSE, SHAPE_RATIO, SHAPE_GAP, mirrorPolys, layPolys, bodyPolys, reflectedPlacement, REFLECT_FLAGS };
