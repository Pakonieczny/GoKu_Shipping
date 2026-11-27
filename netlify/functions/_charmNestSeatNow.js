/*  netlify/functions/_charmNestSeatNow.js — where a line's new pieces lie on the sheet that holds its old ones (Paul, 23 Nov 2026: a person's change is seated at once, never nested). The nest's own pieces, plate, grid, angles
 *  and check (charm-nest-solver.js makeSheetGrid, prepareVariant, verify; _charmNestNestPieces.js); nothing on the sheet is moved. In this order: the old piece's own place (exactly, else any turn, reaching into the free room
 *  beside it); the same place with the pieces a percent smaller at a time, 10 % at most, both pieces of a pair by the same (Paul: "Make sure the 10% rule is also added for any other updates to a charm on a given sheet. If it doesn't fit after
 *  the update allow up to a 10% shrinkage and only if it doesn't fit by that point move it onto a draft sheet"; a design whose exact size is frozen keeps it); the other free room of the sheet at full size, then smaller.
 *  seat({ db, bucket?, rec (the sheet, its old pieces on it), lineKey, charms (the new entries), rows (their records; none: the pieces named are the sheet's own), changed? (then: the ones whose outline changed), say? }) →
 *  A piece whose outline is the old one's stays exactly where it lies (smaller only with its mate).
 *  { poses: [{ poolId, id, cxPt, cyPt, angle, wPt, hPt, scale, size? (0.90 to 0.99), here? (the old piece's place), shape? (inside a green line) }] }, every new piece or none; fitted(charm, pose, at) gives the entry its size
 *  (manual.size, fit "inplace", pinned there); { why }: no room on this sheet (the line goes on the next Draft of its colour at full size); { why, open }: the old piece is not laid out yet (the new one waits for that nest
 *  with the rest). A green line stays as drawn or cut: a piece that stood inside it stays inside it.                                                                                                                    */
"use strict";
const NestPlan = require("../../charm-nest-nest-plan.js");
const num = v => (Number.isFinite(+v) ? +v : 0), RES = 2, DRAW_MS = 3500, parse = s => { try { return s ? JSON.parse(s) : null; } catch (_) { return null; } };
/** The sheet's green line as it stands (drawn, kept or recorded): the metal used before it, the line's own outline, and whether the old pieces stand inside it. A recorded cut is in its stock's outline
    already (and later cuts too): the metal before it is the newest standing cut of another sheet before its own. */
async function lineOf(db, rec, stock, spots) {
  const plan = parse(rec.rosePlanJson), guard = plan ? null : parse(rec.roseProtectedJson); let line = (plan || guard || {}).profile || null, before = parse(stock && stock.profileJson);
  if (rec.roseCutAt && stock) {
    const cuts = (await db.collection("Charm_Nest_Rose_Stock").doc(stock.id).collection("cuts").get()).docs.map(d => d.data()).filter(c => !c.setAsideAt).sort((a, b) => num(b.revision) - num(a.revision)), own = cuts.find(c => c.sheetId === rec.id), of = c => (parse(c && c.planJson) || {}).profile || null;
    line = line || of(own); before = of(cuts.find(c => c.sheetId !== rec.id && num(c.revision) < (num(rec.roseCutRevision) || num(own && own.revision))));
  }
  return { line, before, inside: !!line && (!guard || (guard.placements || []).some(p => spots.some(s => String(s.p.id) === String(p.id)))) };
}
/** A square of s at x, y stands inside a profile (the metal its line takes), tol past it at most. */
const within = (p, x, y, s, tol) => { const [d, t] = p.axis === "x" ? [x, y] : [y, x], a = Math.max(0, Math.floor(t / p.step)), b = Math.max(a, Math.min(p.values.length - 1, Math.floor((t + s - 1e-9) / p.step))); for (let i = a; i <= b; i++) if (!(p.values[i] >= d + s - tol)) return false; return true; };
const pathWithin = (p, path, tol) => path.every((a, i) => { const b = path[(i + 1) % path.length], n = Math.max(1, Math.ceil(Math.hypot(b[0] - a[0], b[1] - a[1]) * 4)); for (let k = 0; k < n; k++) if (!within(p, a[0] + (b[0] - a[0]) * k / n, a[1] + (b[1] - a[1]) * k / n, 0, tol)) return false; return true; });

async function seat(o) {
  const { db, rec, charms } = o, t0 = Date.now(), Nest = require("./_charmNestNest"), Sv = require("./_charmNestSolver"), own = new Set(charms.map(c => String(c.poolId))), mine = c => (o.rows.length ? String(c.poolId || "").startsWith(o.lineKey + "_") : own.has(String(c.poolId)));
  const old = (rec.charms || []).filter(c => c && mine(c)), poseOf = new Map((rec.placements || []).map(p => [String(p.id), p])), spots = old.map(c => ({ id: String(c.poolId), p: poseOf.get(String(c.id)) })).filter(x => x.p);
  if (!spots.length) return { why: "the old piece was not laid out", open: true };
  const [S, policy, stock, rowsOf] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db), Nest.roseStockOf(db, rec), Nest.poolRowsOf(db, rec)]), L = await lineOf(db, rec, stock, spots);
  if (rec.roseCutAt && !L.line) return { why: "its green line could not be read" };
  const plate = Nest.plateOf(rec, S, stock && Object.assign({}, stock, { profileJson: L.before ? JSON.stringify(L.before) : null }), policy);
  const before = new Map(rowsOf); for (const r of o.rows) rowsOf.set(String(r.poolId), Object.assign({}, rowsOf.get(String(r.poolId)) || {}, r));
  const Pieces = o.pieces || require("./_charmNestNestPieces").create({ db, bucket: o.bucket || require("./firebaseAdmin").storage().bucket(), S, log: t => o.say && o.say(t), budgetMs: 30000 }), home = { x: spots.reduce((a, s) => a + num(s.p.cxPt), 0) / spots.length, y: spots.reduce((a, s) => a + num(s.p.cyPt), 0) / spots.length };
  for (const c of charms) await Pieces.fetchEntry(String(c.sku || "").toUpperCase());
  const fresh = []; for (const c of charms) fresh.push(await Pieces.charmOf(c, rowsOf.get(String(c.poolId)), rec));
  // (the old piece as its own file draws it; one that names no file is looked at)
  const outline = q => `${q.w}x${q.h}|${Buffer.from(q.bits).toString("base64")}`, still = new Map();
  for (const c of fresh) { const z = old.find(q => String(q.poolId) === String(c.poolId)), p = z && poseOf.get(String(z.id)); if (!p) continue; const q = o.changed || !(z.aiPath || z.custom) ? null : await Pieces.charmOf(z, before.get(String(c.poolId)), rec).catch(() => null); if (o.changed ? !o.changed.has(String(c.poolId)) : q && Math.abs(q.widthPt - num(z.widthPt)) < .3 && Math.abs(q.heightPt - num(z.heightPt)) < .3 && outline(q) === outline(c)) still.set(String(c.id), Object.assign({}, p, { poolId: String(c.poolId), id: String(c.id), here: true })); }
  if (still.size === fresh.length) return { poses: [...still.values()] };
  // what stays: nearest first, drawn while there is time; no room beside the circles of the farther ones: those are drawn too and it is looked at once more
  const reach = Math.max(...fresh.map(c => Math.hypot(num(c.widthPt), num(c.heightPt)))), stay = (rec.charms || []).filter(c => c && !c.excluded && !mine(c) && poseOf.has(String(c.id))).map(c => { const p = poseOf.get(String(c.id)); return { c, p, r: Math.hypot(num(c.widthPt) || num(p.wPt), num(c.heightPt) || num(p.hPt)) / 2, d: Math.hypot(num(p.cxPt) - home.x, num(p.cyPt) - home.y) }; }).sort((a, b) => a.d - b.d), drawn = [], round = [];
  // (OWNROW: a neighbour is trusted as drawn only when the drawing is the piece its sheet keeps, its size within 3 % of the record's; any other drawing (a wrong body, a file folded whole) stands as its circle)
  const sizeOk = (q, c) => { const w = num(c.widthPt), h = num(c.heightPt); return !(w > 0 && h > 0) || (Math.abs(num(q.widthPt) - w) <= .03 * w + .3 && Math.abs(num(q.heightPt) - h) <= .03 * h + .3); };
  for (const s of stay) { if ((o.all || s.d < s.r + 1.5 * reach) && Date.now() - t0 < (o.all ? 6 : 1) * DRAW_MS) { try { const q = await Pieces.charmOf(s.c, rowsOf.get(String(s.c.poolId)), rec); if (!sizeOk(q, s.c)) throw new Error(`drawn ${num(q.widthPt).toFixed(1)}x${num(q.heightPt).toFixed(1)}, kept ${num(s.c.widthPt)}x${num(s.c.heightPt)}`); drawn.push(Object.assign(s, { q })); await new Promise(r => setImmediate(r)); continue; } catch (e) { if (o.say && e && /^drawn /.test(e.message || "")) o.say(`[seat] ${rec.fileBase || rec.id}: ${s.c.poolId} ${e.message}: kept clear of as its circle`); /* it stands as its circle */ } } round.push(s); }
  for (const s of drawn) s.q.pinned = { cxPt: s.p.cxPt, cyPt: s.p.cyPt, angle: s.p.angle };
  const saved = drawn.map(s => ({ id: String(s.c.id), angle: num(s.p.angle), cxPt: +s.p.cxPt, cyPt: +s.p.cyPt, wPt: num(s.p.wPt), hPt: num(s.p.hPt), scale: num(s.p.scale) || NestPlan.CHARM_SCALE }));
  const view = Object.assign({ metal: rec.metal, runId: rec.runId, sheetId: rec.id, charms: drawn.map(s => s.q).concat(fresh), placements: saved, rejects: [], topup: null, roseCutAt: null, roseProtected: null, nestFlow: "standard", nestInitial: saved, appendOnly: true, intakeAppend: true, runSeed: 1 }, plate);
  const job = NestPlan.buildJob(view, Object.assign(Nest.planEnv(S), { nestItems: sh => NestPlan.activeCharms(sh) })), pieceOf = new Map(job.pieces.map(p => [String(p.id), p])), clear = job.clearancePt;
  const fixed = drawn.map(s => { const q = pieceOf.get(String(s.c.id)); return { piece: { bits: q.bits, w: q.w, h: q.h, scale: q.scale }, placement: s.p }; }), base = Sv.makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: fixed }, L.line && !L.inside ? { remnant: L.line } : {}), clear, RES);
  for (const s of round) { const R = s.r * RES, X = s.p.cxPt * RES, Y = s.p.cyPt * RES; for (let y = Math.max(0, Math.floor(Y - R)); y < Math.min(base.H, Y + R); y++) for (let x = Math.max(0, Math.floor(X - R)); x < Math.min(base.W, X + R); x++) if (Math.hypot(x + .5 - X, y + .5 - Y) <= R) base.set(x, y); }
  const turns = a => ((a % 360) + 360) % 360, angles = [...new Set((job.angles || []).map(turns))];
  const vres = [4, 6, 8].includes(+S.settings.verifyRes) ? +S.settings.verifyRes : 6, oldOf = new Map(old.map(c => [String(c.poolId), c])), spotOf = c => (spots.find(s => s.id === String(c.poolId)) || spots[0]), half = s => { const z = oldOf.get(s.id) || {}; return Math.hypot(num(z.widthPt) || num(s.p.wPt), num(z.heightPt) || num(s.p.hPt)) / 2; };
  // a piece's old place, to the hair: for one no bigger than the old one, or one that grew clear of the sheet's edge (as a nest keeps it) and of the circles
  const ownSpot = (c, size, vOf) => { const p = (spots.find(s => s.id === String(c.poolId)) || {}).p, z = oldOf.get(String(c.poolId)); if (!p || still.has(String(c.id)) || (z && num(z.widthPt) > 0 && num(c.widthPt) * size <= num(z.widthPt) + .05 && num(c.heightPt) * size <= num(z.heightPt) + .05)) return p || null;
    const v = vOf(c, turns(num(p.angle))), w = v ? v.solid.w / RES / 2 : 0, h = v ? v.solid.h / RES / 2 : 0, rim = num(job.sheet.insetPt) - .5, r = Math.hypot(num(c.widthPt), num(c.heightPt)) * size / 2; return v && p.cxPt - w >= rim && p.cyPt - h >= rim && p.cxPt + w <= job.sheet.wPt - rim && p.cyPt + h <= job.sheet.hPt - rim && !round.some(q => Math.hypot(num(q.p.cxPt) - p.cxPt, num(q.p.cyPt) - p.cyPt) < q.r + r) ? p : null; };
  /** The pieces at one size, each in its old one's place (here) or anywhere on the sheet: their poses, or why not. A spot the sheet's own check refuses is left out and the next one tried. */
  async function look(here, size) {
    const bad = [], at1 = p => (size < 1 ? Object.assign({}, p, { scale: p.scale / size, areaPt2: num(p.areaPt2) * size * size }) : p), J1 = size < 1 ? Object.assign({}, job, { pieces: job.pieces.map(p => (own.has(String((fresh.find(c => String(c.id) === String(p.id)) || {}).poolId)) ? at1(p) : p)) }) : job; let why = "";
    const vOf = (c, angle) => { const q = at1(pieceOf.get(String(c.id))); return Sv.prepareVariant({ bits: q.bits, w: q.w, h: q.h, scale: q.scale }, angle, clear, RES); };
    for (let t = here && fresh.every(c => ownSpot(c, size, vOf)) ? -1 : 0; t < 8; t++) {
      const poses = [], at = new Map(), grid = base.clone();
      const put = (c, v, cxPt, cyPt, angle) => poses.push(size === 1 && still.has(String(c.id)) ? still.get(String(c.id)) : Object.assign({ poolId: String(c.poolId), id: String(c.id), cxPt, cyPt, angle, wPt: v.solid.w / RES, hPt: v.solid.h / RES, scale: c.frozen ? 1 : +(NestPlan.CHARM_SCALE * NestPlan.manualSize(c) * size).toFixed(5) }, size < 1 ? { size } : {}, here ? { here: true } : {}));
      if (t < 0) for (const c of fresh) { const p = ownSpot(c, size, vOf); put(c, vOf(c, turns(num(p.angle))), +p.cxPt, +p.cyPt, num(p.angle)); }
      // (inside a line the cells past it are taken: a quarter of a point over it at first, as a piece's coarse cells reach past its outline, and the outline decides)
      else if (L.inside) for (let y = 0; y < grid.H; y++) for (let x = 0; x < grid.W; x++) if (!within(L.line, x / RES, y / RES, 1 / RES, t ? -.35 : .25)) grid.set(x, y);
      if (t >= 0) for (const c of fresh.slice().sort((a, b) => still.has(String(b.id)) - still.has(String(a.id)) || num(b.areaPt2) - num(a.areaPt2))) {
        if (still.has(String(c.id))) { const p = still.get(String(c.id)), v = vOf(c, turns(num(p.angle))); put(c, v, +p.cxPt, +p.cyPt, num(p.angle)); grid.stamp(v.fine.bits, v.fine.w, v.fine.h, Math.round(p.cxPt * RES - v.solid.cx), Math.round(p.cyPt * RES - v.solid.cy)); continue; }
        const spot = spotOf(c), was = spot.p, out = bad.filter(b => b.id === String(c.id)), far = here ? half(spot) * RES : Infinity, step = here ? 1 : 2, variants = [...new Set([turns(num(was.angle)), ...angles])].map(a => vOf(c, a)).filter(Boolean);
        grid.buildSAT(); let best = null;
        // the nearest spot wins; of near ones the one that touches its neighbours most
        for (const v of variants) {
          const w = v.fine.w, h = v.fine.h, full = w * h - v.fine.pm.cells, cx = num(was.cxPt) * RES - v.solid.cx, cy = num(was.cyPt) * RES - v.solid.cy, y0 = Math.max(0, Math.ceil(cy - far)), y1 = Math.min(grid.H - h, Math.floor(cy + far)), x0 = Math.max(0, Math.ceil(cx - far)), x1 = Math.min(grid.W - w, Math.floor(cx + far)), g = Sv.ring(v.fine.bits, w, h, 4), rim = Sv.packShifted(g.bits, g.w, g.h);
          for (let y = y0; y <= y1; y += step) for (let x = x0; x <= x1; x += step) { const d = Math.hypot(x - cx, y - cy) / RES + (v === variants[0] ? 0 : 1); if (d - 1 > far / RES || (best && d - 6 >= best.s) || grid.boxSum(x, y, x + w, y + h) > full || !grid.fits(v.fine.pm, x, y) || out.some(b => b.angle === v.angle && Math.abs(b.x - x) < 3 && Math.abs(b.y - y) < 3)) continue; const s = d - 6 * grid.overlap(rim, x - 4, y - 4, 1e9) / Math.max(1, rim.cells); if (!best || s < best.s) best = { s, v, x, y }; }
          await new Promise(r => setImmediate(r));
        }
        if (!best) return { why: why || "no free room on the sheet" };
        grid.stamp(best.v.fine.bits, best.v.fine.w, best.v.fine.h, best.x, best.y); at.set(String(c.id), { id: String(c.id), angle: best.v.angle, x: best.x, y: best.y });
        put(c, best.v, (best.x + best.v.solid.cx) / RES, (best.y + best.v.solid.cy) / RES, best.v.angle);
      }
      // the check every nest has, on what was drawn (the circles are never entered), and the new pieces alone against the sheet's edge and the metal already used
      const v = Sv.verify(Object.assign({}, J1, { lockedPlacements: saved }), saved.concat(poses), vres), edge = id => { const e = Sv.verify(Object.assign({}, J1, { lockedPlacements: null }), poses.filter(p => p.id === id), vres, null, { noGap: true }); return e.outsidePx || e.removedPx; };
      if (L.inside) for (const s of require("../../charm-nest-rose.js").shapes(fresh, poses)) poses.find(p => p.id === s.id).shape = s;
      const off = poses.filter(p => !still.has(p.id) && ((v.overlappingPairs || []).flat().map(String).includes(p.id) || (v.cornerIds || []).map(String).includes(p.id) || edge(p.id) || (p.shape && !p.shape.paths.every(x => pathWithin(L.line, x, -.1)))));
      if (!off.length) return { poses };
      why = "the free room does not pass the sheet's check"; bad.push(...off.map(p => at.get(p.id)).filter(Boolean));
    }
    return { why };
  }
  const sizes = fresh.some(c => c.frozen) ? [1] : Array.from({ length: 11 }, (_, i) => (100 - i) / 100); let got = null;
  const tries = o.all ? sizes.map(z => [false, z]) : sizes.map(z => [true, z]).concat(round.length ? [[false, 1]] : sizes.map(z => [false, z]));
  for (const [here, size] of tries) { got = await look(here, size); if (got.poses) break; }
  if (!got.poses) return round.length && !o.all ? seat(Object.assign({}, o, { all: true, pieces: Pieces })) : got;
  if (o.say) o.say(`[seat] ${rec.fileBase || rec.id}: ${got.poses.length} piece(s) seated${got.poses[0].size ? ` at ${Math.round(got.poses[0].size * 100)}%` : ""} beside ${drawn.length} drawn and ${round.length} kept clear of, in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  return got;
}
/** The sheet's entry of a piece seated smaller to fit its place: its size, as the in-place fit keeps it (every nest, check, picture and file reads it), and held there. Full size: the entry as it is. */
const fitted = (c, pose, at) => (pose && pose.size ? Object.assign({}, c, { manual: { by: "in place", at, size: pose.size, fit: "inplace" }, pinned: { cxPt: pose.cxPt, cyPt: pose.cyPt, angle: pose.angle, size: pose.size } }) : c);
module.exports = { seat, fitted };
