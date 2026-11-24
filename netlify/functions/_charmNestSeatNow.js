/*  netlify/functions/_charmNestSeatNow.js — where a line's new pieces lie on the sheet that holds its old ones (Paul, 23 Nov 2026: a person's change is seated at once, never nested): where the old piece lay, else the
 *  nearest free room. The nest's own pieces, plate, grid, angles and check (charm-nest-solver.js makeSheetGrid, prepareVariant, verify; _charmNestNestPieces.js); nothing on the sheet is moved. The pieces near the old spot are
 *  drawn, the farther ones stand as the circle they turn in. { poses } for every new piece (a pair together or not at all), or { why }: the sheet then nests them as before.                                             */
"use strict";
const NestPlan = require("../../charm-nest-nest-plan.js");
const num = v => (Number.isFinite(+v) ? +v : 0), RES = 2, DRAW_MS = 3500;

/** o: { db, bucket?, rec (the sheet, its old pieces on it), lineKey, charms (the new entries), rows (their records), say } */
async function seat(o) {
  const { db, rec, charms } = o, t0 = Date.now(), Nest = require("./_charmNestNest"), Sv = require("./_charmNestSolver"), mine = c => String(c.poolId || "").startsWith(o.lineKey + "_");
  const old = (rec.charms || []).filter(c => c && mine(c)), poseOf = new Map((rec.placements || []).map(p => [String(p.id), p])), spots = old.map(c => ({ id: String(c.poolId), p: poseOf.get(String(c.id)) })).filter(x => x.p);
  if (!spots.length) return { why: "the old piece was not laid out" };
  if (charms.some(c => c.custom)) return { why: "a custom design keeps its own size" };
  const [S, policy, stock, rowsOf] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db), Nest.roseStockOf(db, rec), Nest.poolRowsOf(db, rec)]), plate = Nest.plateOf(rec, S, stock, policy);
  for (const r of o.rows) rowsOf.set(String(r.poolId), Object.assign({}, rowsOf.get(String(r.poolId)) || {}, r));
  const Pieces = require("./_charmNestNestPieces").create({ db, bucket: o.bucket || require("./firebaseAdmin").storage().bucket(), S, log: t => o.say && o.say(t), budgetMs: 30000 }), home = { x: spots.reduce((a, s) => a + num(s.p.cxPt), 0) / spots.length, y: spots.reduce((a, s) => a + num(s.p.cyPt), 0) / spots.length };
  for (const c of charms) await Pieces.fetchEntry(String(c.sku || "").toUpperCase());
  const fresh = []; for (const c of charms) fresh.push(await Pieces.charmOf(c, rowsOf.get(String(c.poolId)), rec));
  // what stays: nearest first, drawn while there is time
  const reach = Math.max(...fresh.map(c => Math.hypot(num(c.widthPt), num(c.heightPt)))), stay = (rec.charms || []).filter(c => c && !c.excluded && !mine(c) && poseOf.has(String(c.id))).map(c => { const p = poseOf.get(String(c.id)); return { c, p, r: Math.hypot(num(c.widthPt) || num(p.wPt), num(c.heightPt) || num(p.hPt)) / 2, d: Math.hypot(num(p.cxPt) - home.x, num(p.cyPt) - home.y) }; }).sort((a, b) => a.d - b.d), drawn = [], round = [];
  for (const s of stay) { if (s.d < s.r + 1.5 * reach && Date.now() - t0 < DRAW_MS) { try { drawn.push(Object.assign(s, { q: await Pieces.charmOf(s.c, rowsOf.get(String(s.c.poolId)), rec) })); await new Promise(r => setImmediate(r)); continue; } catch (_) { /* it stands as its circle */ } } round.push(s); }
  for (const s of drawn) s.q.pinned = { cxPt: s.p.cxPt, cyPt: s.p.cyPt, angle: s.p.angle };
  const saved = drawn.map(s => ({ id: String(s.c.id), angle: num(s.p.angle), cxPt: +s.p.cxPt, cyPt: +s.p.cyPt, wPt: num(s.p.wPt), hPt: num(s.p.hPt), scale: num(s.p.scale) || NestPlan.CHARM_SCALE }));
  const view = Object.assign({ metal: rec.metal, runId: rec.runId, sheetId: rec.id, charms: drawn.map(s => s.q).concat(fresh), placements: saved, rejects: [], topup: null, roseCutAt: null, roseProtected: null, nestFlow: "standard", nestInitial: saved, appendOnly: true, intakeAppend: true, runSeed: 1 }, plate);
  const job = NestPlan.buildJob(view, Object.assign(Nest.planEnv(S), { nestItems: sh => NestPlan.activeCharms(sh) })), pieceOf = new Map(job.pieces.map(p => [String(p.id), p])), clear = job.clearancePt;
  const fixed = drawn.map(s => { const q = pieceOf.get(String(s.c.id)); return { piece: { bits: q.bits, w: q.w, h: q.h, scale: q.scale }, placement: s.p }; }), grid = Sv.makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: fixed }), clear, RES);
  for (const s of round) { const R = s.r * RES, X = s.p.cxPt * RES, Y = s.p.cyPt * RES; for (let y = Math.max(0, Math.floor(Y - R)); y < Math.min(grid.H, Y + R); y++) for (let x = Math.max(0, Math.floor(X - R)); x < Math.min(grid.W, X + R); x++) if (Math.hypot(x + .5 - X, y + .5 - Y) <= R) grid.set(x, y); }
  const poses = [], ring = new Map(), turns = a => ((a % 360) + 360) % 360, angles = [...new Set((job.angles || []).map(turns))];
  for (const c of fresh.slice().sort((a, b) => num(b.areaPt2) - num(a.areaPt2))) {
    const q = pieceOf.get(String(c.id)), was = (spots.find(s => s.id === String(c.poolId)) || spots[0]).p, variants = [...new Set([turns(num(was.angle)), ...angles])].map(a => Sv.prepareVariant({ bits: q.bits, w: q.w, h: q.h, scale: q.scale }, a, clear, RES)).filter(Boolean);
    grid.buildSAT(); let best = null;
    // the nearest spot wins; of near ones the one that touches its neighbours most. First round the old spot, then the whole sheet
    for (const far of [1.2 * reach * RES, Infinity]) {
      for (const v of variants) {
        const w = v.fine.w, h = v.fine.h, full = w * h - v.fine.pm.cells, cx = num(was.cxPt) * RES - v.solid.cx, cy = num(was.cyPt) * RES - v.solid.cy, y0 = Math.max(0, Math.ceil(cy - far)), y1 = Math.min(grid.H - h, Math.floor(cy + far)), x0 = Math.max(0, Math.ceil(cx - far)), x1 = Math.min(grid.W - w, Math.floor(cx + far));
        if (!ring.has(v)) { const g = Sv.ring(v.fine.bits, w, h, 4); ring.set(v, Sv.packShifted(g.bits, g.w, g.h)); }
        for (let y = y0; y <= y1; y += 2) for (let x = x0; x <= x1; x += 2) { const d = Math.hypot(x - cx, y - cy) / RES + (v === variants[0] ? 0 : 1); if (d > far / RES || (best && d - 6 >= best.s) || grid.boxSum(x, y, x + w, y + h) > full || !grid.fits(v.fine.pm, x, y)) continue; const s = d - 6 * grid.overlap(ring.get(v), x - 4, y - 4, 1e9) / Math.max(1, ring.get(v).cells); if (!best || s < best.s) best = { s, v, x, y }; }
        await new Promise(r => setImmediate(r));
      }
      if (best) break;
    }
    if (!best) return { why: "no free room on the sheet" };
    grid.stamp(best.v.fine.bits, best.v.fine.w, best.v.fine.h, best.x, best.y);
    poses.push({ poolId: String(c.poolId), id: String(c.id), cxPt: (best.x + best.v.solid.cx) / RES, cyPt: (best.y + best.v.solid.cy) / RES, angle: best.v.angle, wPt: best.v.solid.w / RES, hPt: best.v.solid.h / RES, scale: NestPlan.CHARM_SCALE });
  }
  // the check every nest has, on what was drawn; the circles are never entered
  const v = Sv.verify(Object.assign({}, job, { lockedPlacements: saved }), saved.concat(poses), [4, 6, 8].includes(+S.settings.verifyRes) ? +S.settings.verifyRes : 6), ids = new Set(poses.map(p => p.id));
  if ((v.overlappingPairs || []).flat().some(id => ids.has(String(id))) || (v.cornerIds || []).some(id => ids.has(String(id))) || v.outsidePx || v.removedPx) return { why: "the free room does not pass the sheet's check" };
  if (o.say) o.say(`[seat] ${rec.fileBase || rec.id}: ${poses.length} piece(s) seated beside ${drawn.length} drawn and ${round.length} kept clear of, in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  return { poses };
}
module.exports = { seat };
