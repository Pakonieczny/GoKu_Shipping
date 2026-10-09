// PAIRS FIXTURES: one small shared helper that builds OFFLINE orders, pool rows, sheet records, set records and Etsy receipts for pairs,
// mismatched pairs and multi-piece (disc) orders, on the repo's fake Firestore with the no-nested-arrays check. Written by PAIRTESTS
// (pairs-1009, area 15); the other workers reuse it. No browser, no network, no paid call, nothing outside memory.
//
//   const F = require('./pairs-fixtures.cjs');
//
// WORDS (plan.md): PIECE = one physical cut charm of an order line. GROUP = every piece of one order line (receipt id + transaction id; key
// "rid:tx", CharmNestPair.groupKey). MATCHING PAIR = two pieces of one design. MISMATCHED PAIR = the design draws TWO different bodies, so the
// two pieces are different charms, side "L" then "R". Pool id = rid_tx_copy; an order line's key = rid_tx.
//
// ── THE NAMED CASES (F.cases) — each is a ready world, ids fixed, `expect` says what the checker must find ───────────────────────────────────
//   pairOneSheet            a matching pair (2 studs) on one sheet, with filler orders
//   pairSplitOneSet         a matching pair split over two sheets of ONE set (allowed only as a tracked split)
//   pairSplitTwoSets        a matching pair split over two sheets of two SETS (never allowed: the cardinal rule)
//   discs3Sheets            a 3-disc necklace, one disc on each of three sheets of one set (tracked split)
//   mixedSheet              one sheet holding studs + hoops + discs + a single pendant + a mismatched pair
//   mismatchedTwoOutlines   a mismatched pair (a ball and a racket: two different outlines) side by side on one sheet, L then R
//   mismatchedSplit         the same mismatched pair, its left piece on one sheet and its right piece on another of the same set (tracked)
//   pairWaiting             a pair of which one piece is on a sheet and one still waits (a half-placed group)
//   Each: F.cases.<id>() -> world (fresh object every call).   F.caseList() -> [{ id, about, world, expect }].
//
// ── BUILDING A WORLD ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
//   F.world({ orders:[{ rid, lines:[{ n, kind, sku?, qty?, metal?, discs?, form?, on? }] }], sheets:[{ id, metal, n, set }], tracked? })
//       kind: 'pair' | 'hoop' | 'mismatched' | 'single' | 'earring-single' | 'discs'     (default sku, form and piece count follow the kind)
//       on:   a sheet id (every piece on it) | an array with one entry per piece (a sheet id, or null = that piece waits) | omitted = all wait
//       tracked: groupKeys whose split over sheets is tracked on purpose (R3), e.g. ['4190000001:5000000010']
//   -> { orders, pieces, pool, sheets, sets, run, designs, tracked }   (all plain data; docs have NO array inside an array)
//   F.pieceCount(kind, qty, discs)        the number of pieces a line makes in the PLAN's words (pair 2, n discs n, mismatched a left and a right per unit, single 1; times quantity),
//                                         worked out here independently of CharmNestPair; every fixture line carries it as `pieceCount` (as the intake's spec.pieceCount will).
//                                         NOTE: with no explicit count CharmNestPair.pieceCountOf follows today's app (quantity pieces; only a mismatched design doubles): pairs-tests.cjs prints where the two differ
//   F.legacy(world)                       the same world as the app stored it before pairs: no side, bodyIndex, groupKey, groupSize anywhere
//   F.clone(world)                        a deep copy (mutate a copy to make a broken world)
//   F.designs() / F.charmOf(sku) / F.entryOf(sku)   the designs: PAIR-STUD, PAIR-HOOP (a hoop welded to its body: ONE body), PAIR-TWIN (two identical
//       bodies drawn: NOT mismatched), ONE-PENDANT, DISC-14, MITTENS-MIS (two cut shapes alike, inks differ: the picture in plans/pairs-1009),
//       TENNIS-MIS (a ball and a racket: two different outlines). charmOf = a charm CharmNestPair.bodiesOf reads; entryOf = the master index entry
//       (with `pair` when the design has more than one body).
//   F.ids: rid(n) tx(n) poolId(rid, tx, copy) lineKey(rid, tx) groupKey(rid, tx)
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
//   F.problems(docs, opts)  docs = { pool:[rows], sheets:[sheet docs], sets:[set docs], designs? }  (F.docsOf(fs) or a world)  opts = { tracked:[groupKey], legacy }
//   -> [{ code, groupKey, poolId?, text }]  codes:
//        key-mismatch      a piece's stored groupKey is not its pool id's (pool row or sheet charm)
//        size-mismatch     pieces of one group disagree about groupSize, or the group's rows are not that many
//        side-mismatch     a piece's side differs between its pool row and its sheet charm, or from the design (L then R; matching: none)
//        side-pairing      a mismatched group has not as many L as R, or two pieces share a body
//        sheet-disagree    the pool row's sheetId, the sheet's poolIds, its charms and its placements do not say the same sheet
//        duplicate-piece   one piece is listed by two sheets
//        untracked-split   the pieces of one group are on more than one sheet (or on one and waiting), and nothing says it is tracked
//        split-across-sets the sheets that hold one group are in different sets (never allowed)
//        metal-mismatch    the pieces of one group (or a sheet and its pieces) differ in metal
//        set-disagree      a sheet's setId, the set's sheetIds and the pieces' setId do not agree
//        half-held         some pieces of a group are on hold or cancelled and some are not
//        missing-piece     a group has fewer pool rows than its groupSize (or a hole in the copy numbers)
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
const mitten = x => [[x + 6, 0], [x + 20, 0], [x + 20, 30], [x + 6, 30], [x + 6, 20], [x, 16], [x + 6, 12]];   // a mitten-like cut line: left of the body a thumb
// each design: bodies = [{ outline, inks[] }] (left to right in the drawing), plus a hoop ring where the design has one
const DESIGN_SRC = {
  'PAIR-STUD':   () => [{ outline: ngon(10, 12, 9, 11), inks: [ink(5, 8, 15, 12)] }],
  'PAIR-HOOP':   () => [{ outline: rect(0, 0, 20, 26), inks: [ink(4, 8, 16, 12)], ring: rect(8, 26, 12, 30) }],
  'PAIR-TWIN':   () => [{ outline: rect(0, 0, 20, 26), inks: [ink(4, 8, 16, 12)] }, { outline: rect(26, 0, 46, 26), inks: [ink(30, 8, 42, 12)] }],   // the same charm drawn twice
  'ONE-PENDANT': () => [{ outline: ngon(12, 14, 11, 13), inks: [ink(6, 10, 18, 16, [0, 0, 1])] }],
  'DISC-14':     () => [{ outline: ngon(7, 7, 7, 7), inks: [] }],
  'MITTENS-MIS': () => [{ outline: path_(mitten(0)), inks: [ink(8, 2, 20, 6)] }, { outline: path_(mitten(26)), inks: [ink(34, 2, 46, 6), ink(30, 10, 34, 14, [0, 0, 1]), ink(38, 12, 42, 16, [0, 0, 1])] }],   // MISMATCHED_7134: MITTENS 1 + MITTENS 2
  'TENNIS-MIS':  () => [{ outline: ngon(10, 10, 10, 10), inks: [ink(4, 9, 16, 11)] }, { outline: path_([[28, 0], [34, 0], [34, 30], [31, 36], [28, 30]]), inks: [ink(29, 20, 33, 28)] }],   // a ball and a racket: two different outlines
};
const KINDS = {   // what a kind of line means: default design, the Etsy form, the pieces per unit
  pair: { sku: 'PAIR-STUD', form: 'earrings', per: 2 }, hoop: { sku: 'PAIR-HOOP', form: 'hoop', per: 2 }, mismatched: { sku: 'MITTENS-MIS', form: 'earrings', per: 2 },
  single: { sku: 'ONE-PENDANT', form: 'necklace', per: 1 }, 'earring-single': { sku: 'PAIR-STUD', form: 'earring-single', per: 1 }, discs: { sku: 'DISC-14', form: 'necklace', per: 1 },
};
/** How many pieces a line makes (independent of CharmNestPair: the tests compare the two). */
const pieceCount = (kind, qty, discs) => (kind === 'discs' ? Math.max(1, discs | 0) : KINDS[kind].per) * Math.max(1, qty | 0);
function charmOf(sku) {
  const src = DESIGN_SRC[sku]; if (!src) throw new Error('no such fixture design: ' + sku);
  const bodies = src(), members = [];
  for (const b of bodies) { members.push(b.outline); for (const i of b.inks) members.push(i); if (b.ring) members.push(b.ring); }
  const bbox = members.reduce((u, m) => [Math.min(u[0], m.bbox[0]), Math.min(u[1], m.bbox[1]), Math.max(u[2], m.bbox[2]), Math.max(u[3], m.bbox[3])], [1e9, 1e9, -1e9, -1e9]);
  return { sku, outline: bodies[0].outline, members, bbox, widthPt: bbox[2] - bbox[0], heightPt: bbox[3] - bbox[1], areaPt2: (bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) };
}
/** The master index entry of a design: `pair` only when it draws more than one body (the contract's Charm_Master_Index field). */
function entryOf(sku) {
  const c = charmOf(sku), d = Pair.describe(c), e = { sku, members: c.members.length, widthPt: c.widthPt, heightPt: c.heightPt, areaPt2: c.areaPt2, holes: 0, engravable: false };
  if (d.count > 1) e.pair = { v: 1, bodies: d.count, mismatched: !!d.mismatched };
  return e;
}
const designs = () => Object.fromEntries(Object.keys(DESIGN_SRC).map(sku => [sku, { sku, charm: charmOf(sku), entry: entryOf(sku), bodies: Pair.bodiesOf(charmOf(sku)).map(b => ({ index: b.index, side: b.side, w: b.outlineBbox[2] - b.outlineBbox[0], h: b.outlineBbox[3] - b.outlineBbox[1] })) }]));

/* ═══════════════════════════ the world ═══════════════════════════ */
const SHEETS = [{ id: 'sh-gf1', metal: 'gold', n: 1, set: 'set-1' }, { id: 'sh-gf2', metal: 'gold', n: 2, set: 'set-1' }, { id: 'sh-gf3', metal: 'gold', n: 3, set: 'set-1' }, { id: 'sh-gf4', metal: 'gold', n: 4, set: 'set-2' }, { id: 'sh-ss1', metal: 'silver', n: 1, set: 'set-1' }];
const SET_SEQ = { 'set-1': 1, 'set-2': 2, 'set-3': 3 };
const fileBase = s => `${CODE[s.metal] || 'GF'}_${DAY}_Set-${SET_SEQ[s.set] || 1}_Sheet-${s.n}`;
const sheetLabel = s => `${CODE[s.metal] || 'GF'} Sheet ${s.n}`;

/** An order line as the page and the cloud hold it, and the pieces it makes. */
function lineOf(rid_, l) {
  const kind = l.kind || 'single', K = KINDS[kind]; if (!K) throw new Error('no such kind: ' + kind);
  const t = tx(l.n), sku = l.sku || K.sku, qty = Math.max(1, l.qty | 0 || 1), discs = kind === 'discs' ? (l.discs || 3) : 0, metal = l.metal || 'gold', form = l.form || K.form;
  // (the line carries its own piece count, as the intake sets spec.pieceCount: the plan's words: a pair of earrings is 2 pieces, n discs are n, a mismatched pair is a left and a right per unit)
  const want = pieceCount(kind, qty, discs), charm = charmOf(sku), line = { receiptId: rid_, transactionId: t, quantity: qty, form, sku, pieceCount: want, ...(discs ? { discs } : {}), title: l.title || (kind === 'discs' ? `Disc necklace, ${discs} discs` : kind === 'mismatched' ? `Mismatched ${sku} earrings` : `${sku} ${form}`) };
  const per = Pair.piecesFor(line, charm);
  if (per.length !== want) throw new Error(`CharmNestPair.piecesFor makes ${per.length} piece(s) for ${kind} x${qty}${discs ? ' (' + discs + ' discs)' : ''}, the fixture expects ${want}`);
  const on = l.on == null ? Array(want).fill(null) : Array.isArray(l.on) ? l.on.slice() : Array(want).fill(l.on);
  if (on.length !== want) throw new Error(`line ${l.n}: ${want} pieces but ${on.length} placements`);
  const pieces = per.map((p, i) => ({ poolId: poolId(rid_, t, i + 1), orderId: rid_, transactionId: t, lineKey: lineKey(rid_, t), sku, material: metal, copy: i + 1, quantity: qty, form, kind: Pair.kindOf(line, charm), side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.of, on: on[i] || null }));
  return { n: l.n, kind, sku, qty, metal, form, discs, line, charm, pieces };
}
const PAIR_FIELDS = ['side', 'bodyIndex', 'groupKey', 'groupSize'];
/** Build every document a spec needs. See the header. */
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
    const charms = mine.map((p, i) => { const body = designMap[p.sku].bodies[Math.min(p.bodyIndex, designMap[p.sku].bodies.length - 1)]; return { id: `${s.id}-c${i}`, poolId: p.poolId, order: p.orderId, name: `${p.orderId} · ${p.sku}${p.groupSize > 1 ? ` · ${p.copy}/${p.groupSize}` : ''}`, sku: p.sku, lineKey: p.lineKey, side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.groupSize, widthPt: +body.w.toFixed(2), heightPt: +body.h.toFixed(2) }; });
    const placements = charms.map((c, i) => ({ id: c.id, cxPt: 30 + (i % 6) * 40, cyPt: 30 + Math.floor(i / 6) * 40, angle: 0, wPt: c.widthPt, hPt: c.heightPt }));
    return { id: s.id, setId: s.set, setSeq: SET_SEQ[s.set] || 1, sheetIndex: s.n, runId: RUN, metal: s.metal, day: DAY, fileBase: fileBase(s), folder: fileBase(s), status: 'complete', placedCount: placements.length, charmCount: charms.length, density: .5,
      stock: { wPt: 300, hPt: 150 }, placements, charms, poolIds: charms.map(c => c.poolId), orders: [...new Set(charms.map(c => c.order))], verification: { ok: true }, outputs: {}, label: { files: [], orders: [...new Set(charms.map(c => c.order))] }, archived: false, createdAt: T0, updatedAt: T0 + 5000 };
  }).filter(Boolean);
  const sets = [...new Set(sheets.map(s => s.setId))].map(id => { const mine = sheets.filter(s => s.setId === id); return { setId: id, seq: SET_SEQ[id] || 1, day: DAY, runId: RUN, sheetIds: mine.map(s => s.id), materials: [...new Set(mine.map(s => s.metal))], orders: {}, labelFiles: [], status: 'labelled' }; });
  const lines = {}; for (const o of orders) for (const l of o.lines) lines[lineKey(o.rid, tx(l.n))] = { orderId: o.rid, transactionId: tx(l.n), sku: l.sku, state: l.pieces.every(p => p.on) ? 'written' : 'pooled', quantity: l.qty, material: l.metal, poolIds: l.pieces.map(p => p.poolId), ...(l.kind !== 'single' ? { kind: l.kind, pieceCount: l.pieces.length } : {}) };
  const run = { runId: RUN, status: 'running', step: 'nest', day: DAY, lines, orders: orders.map(o => o.rid), sheets: {}, holds: {}, errors: [], resumable: true };
  return { orders, pieces, pool, sheets, sets, run, designs: designMap, tracked: (spec.tracked || []).slice(), sheetDefs };
}
const clone = w => (typeof structuredClone === 'function' ? structuredClone(w) : JSON.parse(JSON.stringify(w)));
/** The same world as the app stored it before pairs existed: no side, bodyIndex, groupKey or groupSize on any piece. */
function legacy(w) {
  const c = clone(w); for (const p of c.pool) for (const k of PAIR_FIELDS) delete p[k];
  for (const s of c.sheets) for (const ch of s.charms) for (const k of PAIR_FIELDS) delete ch[k];
  for (const p of c.pieces) for (const k of PAIR_FIELDS) delete p[k];
  for (const l of Object.values(c.run.lines)) { delete l.kind; delete l.pieceCount; }
  c.legacy = true; return c;
}

/* ═══════════════════════════ the named cases ═══════════════════════════ */
const FILLER = [{ rid: rid(90), lines: [{ n: 90, kind: 'single', on: 'sh-gf1' }] }, { rid: rid(91), lines: [{ n: 91, kind: 'single', on: 'sh-gf2' }] }, { rid: rid(92), lines: [{ n: 92, kind: 'single', on: 'sh-gf4', metal: 'gold' }] }, { rid: rid(93), lines: [{ n: 93, kind: 'single', metal: 'silver', on: 'sh-ss1' }] }];
const gk = (o, n) => groupKey(rid(o), tx(n));
const CASES = {
  pairOneSheet: { about: 'a matching pair on one sheet', build: () => world({ orders: [{ rid: rid(1), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
  pairSplitOneSet: { about: 'a matching pair split over two sheets of one set (tracked)', build: () => world({ orders: [{ rid: rid(2), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf2'] }] }, ...FILLER], tracked: [gk(2, 10)] }), expect: [] },
  pairSplitTwoSets: { about: 'a matching pair split over two sheets of two sets (never allowed)', build: () => world({ orders: [{ rid: rid(3), lines: [{ n: 10, kind: 'pair', on: ['sh-gf1', 'sh-gf4'] }] }, ...FILLER], tracked: [gk(3, 10)] }), expect: ['split-across-sets'] },
  discs3Sheets: { about: 'a 3-disc necklace, a disc on each of three sheets of one set (tracked)', build: () => world({ orders: [{ rid: rid(4), lines: [{ n: 10, kind: 'discs', discs: 3, on: ['sh-gf1', 'sh-gf2', 'sh-gf3'] }] }, ...FILLER], tracked: [gk(4, 10)] }), expect: [] },
  mixedSheet: { about: 'one sheet with studs, hoops, discs, a pendant and a mismatched pair', build: () => world({ orders: [
      { rid: rid(5), lines: [{ n: 10, kind: 'pair', on: 'sh-gf1' }, { n: 11, kind: 'hoop', on: 'sh-gf1' }] },
      { rid: rid(6), lines: [{ n: 10, kind: 'discs', discs: 4, on: 'sh-gf1' }, { n: 11, kind: 'single', on: 'sh-gf1' }] },
      { rid: rid(7), lines: [{ n: 10, kind: 'mismatched', on: 'sh-gf1' }] }, ...FILLER.slice(1)] }), expect: [] },
  mismatchedTwoOutlines: { about: 'a mismatched pair (a ball and a racket) on one sheet, left then right', build: () => world({ orders: [{ rid: rid(8), lines: [{ n: 10, kind: 'mismatched', sku: 'TENNIS-MIS', on: 'sh-gf1' }] }, ...FILLER] }), expect: [] },
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
    docsOf() { return { pool: st.list(COLL.pool), sheets: st.list(COLL.sheets).filter(s => !s.archived).map(s => Object.assign({ id: s._id }, s)), sets: st.list(COLL.sets), designs: designs() }; } };
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
/** Groups from the cloud documents: Map(groupKey -> { key, pieces:[{ poolId, copy, row, sheets:[sheet doc], charm, placement }] }). A group is derived from the pool id, never from the stored field. */
function groupsOf(docs) {
  const sheets = (docs.sheets || []).filter(s => !s.archived), out = new Map();
  const at = (k, poolId) => { if (!out.has(k)) out.set(k, { key: k, pieces: new Map() }); const g = out.get(k); if (!g.pieces.has(poolId)) g.pieces.set(poolId, { poolId, copy: (poolParts(poolId) || {}).copy || 0, row: null, sheets: [], charm: null, charms: [], placement: null }); return g.pieces.get(poolId); };
  for (const r of docs.pool || []) { const k = gkOfPool(r.poolId); if (k) at(k, r.poolId).row = r; }
  for (const s of sheets) for (const id of s.poolIds || []) { const k = gkOfPool(id); if (!k) continue; const p = at(k, id); p.sheets.push(s); const c = (s.charms || []).find(c => c && c.poolId === id); p.charms.push({ sheet: s, charm: c || null, placement: c ? (s.placements || []).find(q => q && q.id === c.id) || null : null }); p.charm = p.charm || c || null; }
  for (const g of out.values()) g.pieces = [...g.pieces.values()].sort((a, b) => a.copy - b.copy);
  return out;
}
const sheetsOf = g => [...new Set(g.pieces.flatMap(p => p.sheets.map(s => s.id || s._id)))].sort();
/** Everything that makes one group's pieces disagree about their sheet, side or group. See the header for the codes. */
function problems(docs, opts) {
  opts = opts || {}; const out = [], tracked = new Set(opts.tracked || docs.tracked || []), designs_ = docs.designs || {}, sheetById = new Map((docs.sheets || []).filter(s => !s.archived).map(s => [s.id || s._id, s]));
  const add = (code, g, text, poolId) => out.push({ code, groupKey: g ? g.key : '', poolId: poolId || null, text });
  const sid = s => s.id || s._id;
  for (const g of groupsOf(docs).values()) {
    const ps = g.pieces, rows = ps.map(p => p.row).filter(Boolean), pair = rows.some(r => r.groupKey != null || r.side !== undefined) || ps.some(p => p.charm && p.charm.groupKey != null);
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
    // sides
    for (const p of ps) {
      const rs = p.row && p.row.side, cs = p.charms.map(c => c.charm && c.charm.side).filter(v => v !== undefined);
      for (const v of cs) if (p.row && p.row.side !== undefined && (v || null) !== (rs || null)) add('side-mismatch', g, `${p.poolId}: the pool row says ${rs || 'no side'}, the sheet says ${v || 'no side'}`, p.poolId);
      if (new Set(p.charms.map(c => c.charm && (c.charm.side || null))).size > 1) add('side-mismatch', g, `${p.poolId}: its sheets disagree about its side`, p.poolId);
    }
    if (pair) {
      const sk = rows[0] && rows[0].sku, d = sk && designs_[sk], mis = d ? Pair.isMismatched(d.charm) : rows.some(r => r.side === 'L' || r.side === 'R');
      const sides = rows.map(r => r.side || null), L = sides.filter(s => s === 'L').length, R = sides.filter(s => s === 'R').length;
      if (d) for (const r of rows) { const want = mis ? Pair.sideOf((r.copy - 1) % 2, 2) : null; if ((r.side || null) !== want) add('side-mismatch', g, `${r.poolId}: the design ${sk} makes ${want ? want === 'L' ? 'Left' : 'Right' : 'no side'} for copy ${r.copy}, the pool row says ${r.side ? r.side === 'L' ? 'Left' : 'Right' : 'no side'}`, r.poolId); }
      if (mis || L || R) {
        if (L !== R) add('side-pairing', g, `${g.key} has ${L} left and ${R} right piece(s): a mismatched pair makes one of each`);
        for (const r of rows) if (r.bodyIndex != null && r.side != null && r.side !== (r.bodyIndex === 0 ? 'L' : r.bodyIndex === 1 ? 'R' : null)) add('side-pairing', g, `${r.poolId}: body ${r.bodyIndex} cannot be side ${r.side}`, r.poolId);
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
  for (const set of docs.sets || []) for (const id of set.sheetIds || []) { const s = sheetById.get(id); if (s && (s.setId || null) !== (set.setId || set._id)) out.push({ code: 'set-disagree', groupKey: '', poolId: null, text: `set ${set.setId || set._id} lists ${id}, whose own record says set ${s.setId || 'none'}` }); }
  for (const s of sheetById.values()) if (s.setId && (docs.sets || []).length) { const set = (docs.sets || []).find(x => (x.setId || x._id) === s.setId); if (set && !(set.sheetIds || []).includes(sid(s))) out.push({ code: 'set-disagree', groupKey: '', poolId: null, text: `${sid(s)} says set ${s.setId}, which does not list it` }); }
  return out;
}
const sheetLabel2 = s => `${CODE[s.metal] || 'GF'} Sheet ${s.sheetIndex || (/_Sheet-(\d+)/.exec(s.fileBase || '') || [])[1] || '?'}`;

/* ═══════════════════════════ Etsy receipts (the emulated Etsy / the sandbox stream) ═══════════════════════════ */
const METAL_WORD = { gold: '14k Gold Filled', silver: 'Sterling Silver', rose: '14k Rose Gold Filled' };
function receipts(w, at) {
  const t = Math.floor((at || T0) / 1000);
  return w.orders.map((o, i) => ({ receipt_id: Number(o.rid), order_number: o.rid, name: 'Buyer ' + o.rid.slice(-3), country_iso: 'US', city: 'Austin', message_from_buyer: '', create_timestamp: t - 3600 * (w.orders.length - i), created_timestamp: t - 3600 * (w.orders.length - i), update_timestamp: t, updated_timestamp: t, status: 'Paid', is_paid: true, is_shipped: false,
    transactions: o.lines.map(l => ({ transaction_id: Number(tx(l.n)), receipt_id: Number(o.rid), listing_id: 1900000000 + l.n, sku: l.sku, title: l.line.title, quantity: l.qty, expected_ship_date: t + 5 * 86400, shipped_timestamp: null,
      variations: [{ formatted_name: l.kind === 'discs' ? 'Number of Discs / Metal' : 'Metal Choice', formatted_value: l.kind === 'discs' ? `${l.discs} discs • ${l.metal === 'gold' ? 'gold' : l.metal}` : METAL_WORD[l.metal] || l.metal }], is_personalized: false })) }));
}
// the real listings of the Sep 17 sandbox snapshot each case stands for (listing ids and SKUs only; see PAIRTESTS-points.md for counts and order numbers)
const EXAMPLES = {
  mismatchedTwoOutlines: { listing: 1744372161, sku: 'Huggie Hoops-Tennis Ball/Racket3', orders: ['4173373368', '4171010675'], note: 'Mismatched Tennis Ball and Raquet Huggie Hoops: the SKU is in no master; the catalogue holds TENNIS BALL (HUGGIE) and TENNIS RACKET (HUGGIE)' },
  mismatchedTwoOutlinesMaster: { skus: ['MISMATCHED', 'MISMATCHED_6849', 'MISMATCHED_7134'], note: 'catalogue designs named for the case (members 3, 3, 2)' },
  discs3Sheets: { listing: 234758391, sku: 'Initial_8391', orders: ['4172791262'], note: '3 discs, variation "3 discs • gold"' },
  discs2: { listing: 1008014571, sku: 'nitial_Disc_4571', orders: ['4175370240'], note: '2 discs, variation "ROSEGOLD - 2 Disc"' },
  pairOneSheet: { note: 'every earring line with quantity 1: 102 of the 105 earring lines of the snapshot' },
};

module.exports = { COLL, CODE, DAY, RUN, ids, rid, tx, poolId, lineKey, groupKey, poolParts, gkOfPool, KINDS, pieceCount, designs, charmOf, entryOf, world, lineOf, clone, legacy, cases, caseList, CASES, SHEETS, FILLER,
  fakeFirestore, functions, problems, groupsOf, sheetsOf, receipts, EXAMPLES, fileBase, sheetLabel: s => sheetLabel(s), PAIR_FIELDS, refuseNestedArrays };
