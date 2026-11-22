/*  netlify/functions/_charmNestFreeMap.js — FITFIRST (Paul, 21 Nov 2026: "You just try to place it in an empty spot"). A sheet's map of filled and empty space, saved by the sheet
 *  writer each time it writes a sheet (it has every piece drawn then), and read back by the placement's fit check: a new order's pieces are tried in the empty spots of that map with
 *  the nester's own tools (charm-nest-solver.js makeSheetGrid, prepareVariant, bestSpots, stampVariant: the same grid, gap, inset and corners a nest uses). Nothing is redrawn.
 *
 *  sig(placements)               the layout a map belongs to (every placed piece's id and pose); a map whose sig is not the sheet's current layout is not used
 *  build(job, placements)        → the map of a sheet (job: NestPlan.buildJob of the sheet, with its pieces' bits)
 *  save(bucket, sheetId, map), load(bucket, sheetId)
 *  fits(job, map)                → { fits, placed: [{ id, angle, cxPt, cyPt }], why }: job's pieces (the new order's only) tried one after another in the map's empty space          */
"use strict";
const zlib = require("zlib");
const crypto = require("crypto");
const Solver = require("./_charmNestSolver");

const PATH = id => `charmnest/maps/${String(id).replace(/[^\w\-]/g, "_")}.json.gz`;
const r2 = v => Math.round(+v * 100) / 100;
function sig(placements) {
  const rows = (placements || []).filter(p => p && p.id != null).map(p => `${p.id}@${r2(p.cxPt)},${r2(p.cyPt)},${r2(p.angle || 0)}`).sort();
  return crypto.createHash("sha256").update(rows.join("|")).digest("hex").slice(0, 24);
}
function build(job, placements) {
  const res = job.fineRes || 2, byId = new Map((job.pieces || []).map(p => [String(p.id), p]));
  const fixed = (placements || []).map(p => ({ piece: byId.get(String(p.id)), placement: p })).filter(x => x.piece && x.piece.bits);
  const g = Solver.makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: fixed }), job.clearancePt, res);
  return { v: 1, res, W: g.W, H: g.H, clearancePt: job.clearancePt, sig: sig(placements), n: fixed.length, at: Date.now(), occ: Buffer.from(g.occ.buffer, g.occ.byteOffset, g.occ.byteLength).toString("base64") };
}
async function save(bucket, sheetId, map) {
  await bucket.file(PATH(sheetId)).save(zlib.gzipSync(Buffer.from(JSON.stringify(map))), { contentType: "application/gzip", resumable: false });
}
async function load(bucket, sheetId) {
  try { const [b] = await bucket.file(PATH(sheetId)).download(); return JSON.parse(zlib.gunzipSync(b).toString("utf8")); } catch (_) { return null; }
}
function gridOf(job, map) {
  const g = Solver.makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: [] }), job.clearancePt, map.res);
  const buf = Buffer.from(map.occ, "base64"), occ = new Uint32Array(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
  if (g.W !== map.W || g.H !== map.H || occ.length !== g.occ.length) return null;   // (the plate or the settings changed since the map was made)
  for (let i = 0; i < occ.length; i++) g.occ[i] |= occ[i];
  for (let y = 0; y < g.H; y++) for (let x = 0; x < g.W; x++) if (g.get(x, y)) g.free[y * g.W + x] = 0;
  g.buildSAT();
  return g;
}
function fits(job, map) {
  if (!map || +map.clearancePt !== +job.clearancePt) return { fits: null, why: "the sheet has no map at these settings" };
  const g = gridOf(job, map); if (!g) return { fits: null, why: "the sheet's map was made for another plate" };
  const angles = Array.isArray(job.angles) && job.angles.length ? job.angles : [0, 90, 180, 270], placed = [];
  for (const p of job.pieces || []) {
    const variants = angles.map(a => Solver.prepareVariant(p, a, job.clearancePt, map.res)).filter(Boolean);
    const spot = variants.length ? Solver.bestSpots(g, variants, 1)[0] : null, v = spot && variants.find(x => x.angle === spot.angle);
    if (!spot || !v) return { fits: false, placed, why: `${p.name || p.id} has no empty spot big enough` };
    Solver.stampVariant(g, v, spot.x, spot.y);
    placed.push({ id: p.id, angle: v.angle, cxPt: (spot.x + v.solid.cx) / g.fineRes, cyPt: (spot.y + v.solid.cy) / g.fineRes });
  }
  return { fits: true, placed, why: "" };
}

module.exports = { sig, build, save, load, fits, PATH };
