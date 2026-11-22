// NQ (Paul, 21 Nov 2026): Gold Filled / Sterling Silver sheets nested by the CLOUD leave big empty spaces; the 10K / 14K nest packs much better ("take its cues from it").
// Cause: the cloud nested a GF / SS sheet three orders at a time (NestPlan.feedTurn), and every nest after the first ("Nest the rest", the cloud's own auto nest) pinned each piece
// already on the sheet where it lay (arrivalPin -> lockedPlacements: walls) and placed the next few around them, in ONE search judged without its dead space. 10K / 14K take their whole
// batch in one search, six careful searches side by side (multiStart, KEEPSEARCH) and keep the least wasteful layout (tight).
// Fix (_charmNestNestSlot.js wholeOf / wholePick): a GF / SS sheet still being formed (a draft, or in no set; never cut, laser cut, released, committed, in a set, or promised to stay
// where it is) is laid out whole by every cloud nest with the 10K / 14K search; only a person's pin is held; one slot still appends to the saved layout and the pick prefers the layout
// that keeps every piece already on the sheet.
//   A  the job each nest prepares (the REAL nestStart and slot prepare, offline fakes): whole for GF / SS drafts, unchanged for 10K / 14K and for sheets of a set.
//   B  the REAL cloud nest (nestStart, every slot's search, the pick) on a realistic 44-piece GF sheet of the shop's own charm silhouettes: the old flow lays the first half, then
//      "Nest the rest" the old way and the new way; packed area, free share behind the front and holes are measured.
//   NODE_PATH=<deps> node tests/charm-nest/cloud-nest-quality.cjs        (NQ_QUICK=1: part A only)
'use strict';
const path = require('path'), Module = require('module');
process.env.EDIT_PASSCODE = 'test-pass';
const F = require('./_cloudFakes.cjs')({ now: Date.now(), clock: false });
const L = require('./pocket-fill-lib.cjs'), FL = require('./nest-front-lib.cjs'), S = L.solverOf(L.REPO);
const log0 = console.log.bind(console); console.log = () => {}; console.warn = () => {};
let failed = 0; const ok = (n, c, d) => { log0((c ? 'OK   ' : 'FAIL ') + n + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 900))); if (!c) failed++; };

// the cloud makes each piece again from its master file: here from the shop's own library silhouettes (the solver's PROBES), the record's manual mark read by the REAL manualOf
const realPieces = require(path.join(F.fnDir, '_charmNestNestPieces.js')), SHAPES = new Map(), prevLoad = Module._load;
Module._load = function (req, parent, ...rest) {
  if (req === './_charmNestNestPieces' && parent && /_charmNestNestSlot\.js$/.test(parent.filename || '')) return Object.assign({}, realPieces, { create: () => ({
    rebuild: async (rec, rows) => ({ missing: [], charms: (rec.charms || []).filter(rc => rc && !rc.excluded).map(rc => { const pool = rows.get(String(rc.poolId)) || {}, man = realPieces.manualOf(rc), sh = SHAPES.get(rc.id);
      return Object.assign({ id: String(rc.id), name: rc.name, sku: pool.sku || null, order: String(pool.orderId || rc.id), orderDate: +pool.orderDate || 0, arrivedAt: 0, poolId: String(rc.poolId), metal: rec.metal, excluded: false, pinned: null,
        w: sh.w, h: sh.h, scale: sh.scale, bits: sh.bits, areaPt2: sh.areaPt2, widthPt: sh.widthPt, heightPt: sh.heightPt, hash: rc.hash, orderInfo: { receiptId: pool.orderId } }, man ? { manual: man } : {}); }) }) }) });
  return prevLoad.call(this, req, parent, ...rest);
};
const lib = F.load('charmNestLibrary.js'), Nest = F.load('_charmNestNest.js'), Slot = F.load('_charmNestNestSlot.js'), Lease = F.load('_charmNestLease.js');
const DEFAULT_WHOLE = Slot.WHOLE_METALS.slice(), WHOLE = ['gold', 'silver'], whole = on => { Slot.WHOLE_METALS.length = 0; if (on) Slot.WHOLE_METALS.push(...WHOLE); };

/* ── the world ── */
const R = 'run-2026-11-21-nq';
/** `n` charms of the shop's library, enlarged by `f`, in orders of 1 to 4 pieces, oldest first (seeded). */
function makePieces(n, f, seed) {
  const r = L.rng(seed || 7), sizes = [1, 2, 1, 3, 1, 2, 4, 1, 2, 1, 3, 2, 1, 1, 2, 3, 1, 2, 1, 4], out = []; let o = 0;
  while (out.length < n) { const k = Math.min(sizes[o % sizes.length], n - out.length); for (let j = 0; j < k; j++) { const idx = Math.floor(r() * S.PROBES.length), id = 'c' + out.length; out.push({ id, order: String(4200000000 + o), date: Date.UTC(2026, 10, 1) + o * 3600e3, shape: L.charm(S, id, idx, f, null, 0) }); } o++; }
  return out;
}
function seedWorld(metal, pieces, o = {}) {
  F.reset();
  F.seed('Charm_Nest_Open/drive', { mode: 'on', runMode: 'auto', cloudSteps: ['nest', 'files'], flippedAt: Date.now() - 3600e3 }); if (lib._driveForget) lib._driveForget();
  F.seed('Charm_Nest_Runs/' + R, { runId: R, status: 'running', step: 'nest', mode: 'auto', day: '2026-11-21', sheets: {}, holds: {}, errors: [], orders: [] });
  F.seed('Charm_Nest_Run_Carrier/' + R, { runId: R, carrier: { dev: 'cloud', by: 'Cloud', at: Date.now(), since: Date.now() - 3600e3 } });
  if (o.budgetS) F.seed('Charm_Nest_Shared/sorter-settings', { f: { budgetS: { j: JSON.stringify(o.budgetS) } } });
  SHAPES.clear(); const pools = [];
  for (const p of pieces) { SHAPES.set(p.id, p.shape); p.poolId = `${p.order}_${p.order}1_${p.id}`; pools.push(p.poolId); F.seed('Charm_Pool/' + p.poolId, { poolId: p.poolId, runId: R, orderId: p.order, orderDate: p.date, state: 'placed', sku: 'T_' + p.id }); }
  const SH = metal + '-s19-nq';
  F.seed('Charm_Nest_Sheets/' + SH, Object.assign({ id: SH, runId: R, metal, draft: true, fileBase: metal.toUpperCase() + '_NQ_Sheet-19', status: 'ready', layoutRev: 1, contentRev: 1, poolIds: pools, placements: [],
    charms: pieces.map(p => ({ id: p.id, poolId: p.poolId, hash: 'h' + p.id, name: p.id, areaPt2: p.shape.areaPt2, widthPt: p.shape.widthPt, heightPt: p.shape.heightPt, excluded: false })) }, o.rec || {}));
  return SH;
}
const rec = SH => F.doc('Charm_Nest_Sheets/' + SH), put = (SH, patch) => F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, rec(SH), patch));
const owed = SH => Nest.owedNest(rec(SH), F.doc('Charm_Nest_Runs/' + R), Date.now()).owed;
/** The REAL prepare (slot 0) of the nest nestStart would ask now. */
async function prepared(SH, mode) {
  const r = Object.assign({ id: SH }, rec(SH)), S0 = await Nest.readSettings(F.db), plate = Nest.plateOf(r, S0, null, await Nest.readPolicy(F.db));
  const job = { id: 'nest-nq-' + mode + '-' + r.layoutRev, sheetId: SH, layoutBase: +r.layoutRev || 0, contentBase: +r.contentRev || 0, inputKey: Nest.inputKeyOf(r, await Nest.poolRowsOf(F.db, r), S0, plate, mode), mode, seed: 7 };
  return Slot.prepare({ db: F.db, bucket: F.bucket, job, log: () => {}, tick: async () => {}, stop: false });
}
/** One REAL cloud nest of the sheet, nestStart to the pick, the slots run one after another in this process through their real leases. → the job */
async function cloudNest(SH, fresh) {
  const st = await Nest.start({ db: F.db, FV: F.FieldValue, PREFIX: '', MARKED: true, driveMode: async () => 'on' }, { sheetId: SH, fresh: !!fresh, auto: !fresh, why: fresh ? 'Nest' : 'Nest the rest' });
  if (!st.ok || !st.created) throw new Error('nestStart: ' + JSON.stringify(st));
  const jobId = st.jobId, queue = [], dispatch = async i => ({ nonce: (await Lease.dispatch(F.db, Nest.slotRef(F.db, jobId, i), 'test', { heartbeat: true })).nonce, jobId, slot: i });
  queue.push(await dispatch(0));
  for (let g = 0; queue.length && g < 40; g++) {
    const k = queue.shift(), out = await Slot.run({ db: F.db, FV: F.FieldValue, bucket: F.bucket, nonce: k.nonce, jobId, slot: k.slot, log: () => {}, kickSlot: x => queue.push(x), kickDriver: () => {} });
    if (out && out.prepared && out.searchLater) queue.push(await dispatch(0));
  }
  const j = F.doc('Charm_Nest_Jobs/' + jobId); if (j.status !== 'done') throw new Error(`job ${j.status}: ${j.error || ''}`);
  return j;
}
/** The sheet writer, as far as the layout goes: the job's layout onto the record (a piece turned away moves to another sheet: it leaves this one). */
function apply(SH, j) {
  const r = rec(SH), gone = new Set((j.result.rejects || []).map(String));
  put(SH, { charms: r.charms.map(c => (gone.has(c.id) ? Object.assign({}, c, { excluded: true }) : c)), placements: j.result.placements.map(p => Object.assign({}, p)), rejects: [], layoutRev: (+r.layoutRev || 0) + 1, nestJob: Object.assign({}, r.nestJob, { status: 'applied' }) });
}
/** How tightly a 100 x 50 mm sheet is packed: placed share of the usable area, the front (nest-front-lib: p90 reach, cavern), the free share behind the front, and its holes
    (free regions behind the front, inside the 1.5 mm band, each big enough for a 5 x 5 mm square). */
function measure(SH) {
  const r = rec(SH), charms = r.charms.filter(c => !c.excluded).map(c => SHAPES.get(c.id)), byId = new Map(charms.map(c => [c.id, c])), fr = FL.front({ charms, placements: r.placements });
  const FW = Math.round(L.PT(100) * 2), FH = Math.round(L.PT(50) * 2), occ = new Uint8Array(FW * FH);
  for (const p of r.placements) { const v = S.prepareVariant(byId.get(p.id), p.angle, -.5, 2), x0 = Math.round(p.cxPt * 2 - v.solid.cx), y0 = Math.round(p.cyPt * 2 - v.solid.cy);
    for (let y = 0; y < v.fine.h; y++) for (let x = 0; x < v.fine.w; x++) if (v.fine.bits[y * v.fine.w + x]) { const X = x0 + x, Y = y0 + y; if (X >= 0 && Y >= 0 && X < FW && Y < FH) occ[Y * FW + X] = 1; } }
  const band = Math.round(L.PT(1.5) * 2), xf = Math.round(L.PT(fr.frontP90) * 2), seen = new Uint8Array(FW * FH), minCells = (L.PT(5) * 2) ** 2; let holes = 0, holeCells = 0;
  for (let y = band; y < FH - band; y++) for (let x = band; x < xf; x++) {
    const i = y * FW + x; if (occ[i] || seen[i]) continue; let n = 0; const st = [i]; seen[i] = 1;
    while (st.length) { const k = st.pop(), kx = k % FW, ky = (k / FW) | 0; n++; for (const [dx, dy] of [[1, 0], [-1, 0], [0, 1], [0, -1]]) { const X = kx + dx, Y = ky + dy; if (X < band || Y < band || X >= xf || Y >= FH - band) continue; const q = Y * FW + X; if (!occ[q] && !seen[q]) { seen[q] = 1; st.push(q); } } }
    if (n >= minCells) { holes++; holeCells += n; }
  }
  const placedPt2 = r.placements.reduce((n, p) => n + SHAPES.get(p.id).areaPt2, 0), usable = (L.PT(100) - 3) * (L.PT(50) - 3);
  return { placed: r.placements.length, of: charms.length, packed: +(placedPt2 / usable).toFixed(3), frontMm: fr.frontP90, cavernMm: fr.cavernMm, freeBehind: +(1 - fr.blockFill).toFixed(3), holes, holeMm2: Math.round(holeCells / 4 * L.MM * L.MM) };
}
const snapOf = SH => JSON.parse(JSON.stringify(rec(SH))), restore = (SH, r, bump) => F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, r, { layoutRev: (+r.layoutRev || 0) + bump }));
/** A saved layout of the first `k` pieces, made by the solver (each piece at a spot of its own): the state a sheet is in after the cloud's earlier nests. */
async function savedLayout(pieces, k) {
  const job = { sheet: { wPt: L.PT(100), hPt: L.PT(50), insetPt: 1.5, cornerPt: 0 }, clearancePt: -.5, angles: [0, 90], fineRes: 2, coarseRes: .5, timeBudgetMs: 20000, maxTrials: 1, seed: 3,
    pieces: pieces.slice(0, k).map(p => Object.assign({}, p.shape, { order: p.order, orderDate: p.date })) };
  const res = await S.solve(job, {}); return res.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle, scale: 0.971, wPt: p.wPt, hPt: p.hPt }));
}

(async () => {
  /* ── A: the job each cloud nest prepares ── */
  {
    const pieces = makePieces(44, 1.05, 7), SH = seedWorld('gold', pieces), saved = await savedLayout(pieces, 22), ids = saved.map(p => p.id);
    put(SH, { placements: saved, layoutRev: 2 });
    // a person's pin on one of the saved pieces (Manual Control: the record's manual mark, pinned at the record's pose)
    const pinId = ids[3], pinAt = saved[3]; put(SH, { charms: rec(SH).charms.map(c => (c.id === pinId ? Object.assign({}, c, { manual: { by: 'Ann', at: Date.now() - 6e4 }, pinned: { cxPt: pinAt.cxPt, cyPt: pinAt.cyPt, angle: pinAt.angle } }) : c)) });
    whole(false); const old = await prepared(SH, 'auto');
    const oj = old.solverJob, oPinned = oj.pieces.filter(p => p.pinned && !p.hold).length;
    ok('A1 before (the cause): "Nest the rest" of a GF draft with 22 of 44 pieces placed fed only the next three orders (the rest waited their turn), held the 22 saved pieces as walls, ran one search',
      oj.pieces.length < 30 && old.feedWait.length > 10 && (oj.lockedPlacements || []).length === 22 && oPinned === 21 && old.n === 1 && !oj.multiStart && !oj.tight, { pieces: oj.pieces.length, feedWait: old.feedWait.length, locked: (oj.lockedPlacements || []).length, oPinned, n: old.n });
    whole(true); const now = await prepared(SH, 'auto'), nj = now.solverJob, alt = now.altJob, byId = id => nj.pieces.find(p => p.id === id);
    ok('A2 now: the whole batch in one search (no turn of three orders), nothing held but the person\'s pin, searched as 10K / 14K are (six careful searches side by side, KEEPSEARCH, tight)',
      nj.pieces.length === 44 && !now.feedWait.length && nj.pieces.filter(p => p.pinned).length === 1 && byId(pinId).hold === true && byId(pinId).pinned.cxPt === pinAt.cxPt && (nj.lockedPlacements || []).map(p => p.id).join() === pinId
        && nj.multiStart === undefined && nj.tight === true && now.n === 2,
      { pieces: nj.pieces.length, feedWait: now.feedWait.length, pinned: nj.pieces.filter(p => p.pinned).map(p => p.id), locked: (nj.lockedPlacements || []).map(p => p.id), ms: nj.multiStart, n: now.n });
    ok('A3 ... and one slot appends to the saved layout as before (every saved piece held, the whole batch), so the pick can keep every piece already on the sheet',
      !!alt && now.input.altSlot === now.n - 1 && alt.altAppend === true && alt.pieces.length === 44 && (alt.lockedPlacements || []).length === 22 && now.input.meta.whole === true && now.input.meta.seated.length === 22, { alt: alt && (alt.lockedPlacements || []).length, slot: now.input.altSlot, n: now.n });
    // (GOLDLOCK: a sheet with a person's pin is searched the single way; with no pin the side-by-side searches run)
    put(SH, { charms: rec(SH).charms.map(c => { const o = Object.assign({}, c); delete o.manual; delete o.pinned; return o; }), layoutRev: 3 });
    const free = await prepared(SH, 'auto');
    ok('A4 with no person\'s pin: six searches side by side (five of the whole layout, KEEPSEARCH on, and the slot that appends)', free.n === 6 && free.solverJob.multiStart === 6 && free.input.altSlot === 5 && free.keep && free.keep.max >= 6 && !(free.solverJob.lockedPlacements || []).length, { n: free.n, ms: free.solverJob.multiStart, keep: free.keep && free.keep.max });
    const fr = await prepared(SH, 'fresh');
    ok('A5 a person\'s Nest (fresh): the whole batch, six searches, no slot appends (nothing is kept)', fr.n === 6 && fr.solverJob.pieces.length === 44 && !fr.altJob && !fr.input.alt && !fr.feedWait.length, { n: fr.n, alt: !!fr.altJob });
    // never laid out again: a sheet of a set, one released full, committed, cut by the laser, or promised to stay where it is (Merge by hand / made again in place)
    for (const [why, patch] of [['in a set', { setId: 'set-1', draft: false }], ['committed', { committedAt: Date.now() }], ['merged by hand (appendOnly)', { appendOnly: true }]]) {
      const before = rec(SH); put(SH, Object.assign({}, patch, { layoutRev: +before.layoutRev + 1 }));
      const p = await prepared(SH, 'auto');
      ok(`A6 ${why}: nested as before (its saved pieces held, the next turn only, no whole layout)`, p.solverJob.pieces.length < 44 && (p.solverJob.lockedPlacements || []).length === 22 && !p.altJob && !p.input.meta.whole, { pieces: p.solverJob.pieces.length, locked: (p.solverJob.lockedPlacements || []).length });
      F.seed('Charm_Nest_Sheets/' + SH, Object.assign({}, before, { layoutRev: +before.layoutRev + 2 }));
    }
    whole(true);
    ok('A7 wholeOf: GF and SS drafts and sheets in no set; never 10K, 14K, Rose Gold, a set\'s sheet, a released, committed, approved, cut or laser cut one',
      Slot.wholeOf({ metal: 'gold', draft: true }) && Slot.wholeOf({ metal: 'silver' }) && Slot.wholeOf({ metal: 'gold', setId: 's', draft: true }) && !Slot.wholeOf({ metal: 'gold10k', draft: true }) && !Slot.wholeOf({ metal: 'gold14k' }) && !Slot.wholeOf({ metal: 'rose' })
        && !Slot.wholeOf({ metal: 'gold', setId: 's' }) && !Slot.wholeOf({ metal: 'silver', releaseFull: true }) && !Slot.wholeOf({ metal: 'gold', approvedAt: 1 }) && !Slot.wholeOf({ metal: 'gold', roseCutAt: 1 }) && !Slot.wholeOf({ metal: 'gold', laserDoneAt: 5 }));
    const box = (id, x) => ({ id, cxPt: x, cyPt: 20, angle: 0, xPt: x - 5, yPt: 15, wPt: 10, hPt: 10 }), sheet = { wPt: 283, hPt: 142 };
    const a = { placements: [box('n1', 10), box('n2', 22), box('n3', 34)], deadPt2: 10 }, b = { placements: [box('s1', 10), box('n1', 22), box('n2', 34)], deadPt2: 900 };
    ok('A8 wholePick: the layout that keeps every piece already on the sheet wins over a tighter one that drops one; with all kept, the least wasteful wins',
      Slot.wholePick([a, b], ['s1'], sheet).k === 1 && Slot.wholePick([b, Object.assign({}, b, { deadPt2: 5 })], ['s1'], sheet).k === 1 && Slot.wholePick([null, a], [], sheet).k === 1);
    // 10K / 14K: exactly the job they had (WHOLE on or off makes no difference)
    for (const metal of ['gold10k', 'gold14k']) {
      const SK = seedWorld(metal, makePieces(30, 1, 11)); put(SK, { placements: (await savedLayout(makePieces(30, 1, 11), 12)), layoutRev: 2 });
      const strip = p => JSON.stringify(Object.assign({}, p.input, { jobId: 0 }));
      whole(false); const off = [await prepared(SK, 'auto'), await prepared(SK, 'fresh')]; whole(true); const on = [await prepared(SK, 'auto'), await prepared(SK, 'fresh')];
      ok(`A9 ${metal}: the cloud prepares the very same nest as before, "Nest the rest" and "Nest" (input byte for byte; no whole layout, no appending slot)`, strip(off[0]) === strip(on[0]) && strip(off[1]) === strip(on[1]) && !on[0].input.meta.whole && !on[1].altJob && on[1].n === off[1].n);
    }
  }
  if (process.env.NQ_QUICK) { log0(failed ? `FAIL cloud-nest-quality (${failed})` : 'PASS cloud-nest-quality (quick)'); process.exit(failed ? 1 : 0); }

  /* ── B: the real cloud nest on a realistic GF draft (44 pieces of the shop's silhouettes, 100 x 50 mm), first half by the old flow, then "Nest the rest" ── */
  {
    // (the cloud runs its slots side by side, one invocation each; here they run one after another in one process, so the whole layout is searched by NQ_SEARCHES searches
    //  (default 1, the cloud runs five) beside the slot that appends: the least the cloud does)
    const NP = require(path.join(L.REPO, 'charm-nest-nest-plan.js')), realMS = NP.multiStartFor; NP.multiStartFor = () => Math.max(1, +process.env.NQ_SEARCHES || 1);
    const t0 = Date.now(), BUD = +process.env.NQ_BUDGET_S || 40, pieces = makePieces(44, 1.05, 7), SH = seedWorld('gold', pieces, { budgetS: BUD });
    whole(false); let turns = 0;
    while (owed(SH) && turns < 30 && rec(SH).placements.length < 22) { apply(SH, await cloudNest(SH, false)); turns++; }
    const half = snapOf(SH), keptIds = half.placements.map(p => p.id), m0 = measure(SH);
    let tb = 0; while (owed(SH) && tb < 30) { apply(SH, await cloudNest(SH, false)); tb++; }
    const before = measure(SH);
    restore(SH, half, 100); whole(true);
    const j = await cloudNest(SH, false); apply(SH, j); let ta = 1; while (owed(SH) && ta < 5) { apply(SH, await cloudNest(SH, false)); ta++; }
    const after = measure(SH), keptNow = keptIds.filter(id => rec(SH).placements.some(p => p.id === id)).length;
    log0(`     half by the old flow (${turns} nests): ${JSON.stringify(m0)}`);
    log0(`     before ("Nest the rest" the old way, ${tb} more nests): ${JSON.stringify(before)}`);
    log0(`     after  ("Nest the rest" now, ${ta} nest(s), ${j.result.slots} slots, ${j.result.appended ? 'the appending slot won' : 'laid out whole'}): ${JSON.stringify(after)}  (${Math.round((Date.now() - t0) / 1000)} s in all)`);
    ok('B1 "Nest the rest" now places the whole rest in one nest (the old way took one nest per three orders) and keeps every piece of the sheet on it', ta === 1 && tb > 2 && after.placed === after.of && after.placed >= before.placed && keptNow === keptIds.length, { ta, tb, after, before, keptNow });
    ok('B2 the same pieces sit in a tighter block: the front reaches less far, less free space behind it, no more holes than before', after.frontMm < before.frontMm && after.freeBehind < before.freeBehind && after.holes <= before.holes && after.holeMm2 <= before.holeMm2, { before, after });
    ok('B3 the layout is checked as every cloud nest is (the solver\'s verifier)', j.result.verification && j.result.verification.ok === true, j.result.verification);
    NP.multiStartFor = realMS;
  }
  ok('NOWHOLE · by default no Gold Filled or Silver sheet is laid out whole again: the pieces on it stay where they lie', DEFAULT_WHOLE.length === 0);
  log0(failed ? `FAIL cloud-nest-quality (${failed})` : 'PASS cloud-nest-quality'); process.exit(failed ? 1 : 0);
})().catch(e => { log0('FAIL cloud-nest-quality: ' + (e && e.stack || e)); process.exit(1); });
