/*  Hoops: every jump ring is one piece with its charm, and its hole is the only circle left.
 *
 *  The cases are real charms of the three master files (fixtures/hoops-masters.json). The masters draw a hoop in two
 *  sizes (6.5 pt outside / 3.7 pt hole, and the HUGGIE ring 9.35 / 5.85) and four ways (one path holding both circles,
 *  or two separate circles; cyan or black). The old rule knew only circles of 3.5 to 8 pt:
 *    - a HUGGIE ring drawn as one path was never welded (a loose washer beside the body: "hoop not attached");
 *    - a HUGGIE ring drawn as two circles had its 5.85 pt HOLE taken for the ring: welded as a solid disc with a tiny
 *      hole, and the real 9.35 pt outside circle left behind as an extra large circle around it, counted as a hole;
 *    - a standard ring drawn as two circles, the inner one first in the file, got a spare hole and sliver holes.
 *  Run: node tests/charm-nest/hoops.cjs                                                                              */
"use strict";
const assert = require("node:assert/strict"), fs = require("fs"), path = require("path");
global.self = global; global.PDFLib = require("../../vendor/pdf-lib-1.17.1.min.js"); require("../../charm-nest-pdf.js");
const P = global.CharmNestPDF, V = require("../../charm-nest-vector.js"), G = require("../../charm-nest-geom.js");
const MM = 25.4 / 72;
const fx = JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures/hoops-masters.json"), "utf8"));
const area = ps => Math.abs(ps.reduce((v, p, i) => v + p[0] * ps[(i + 1) % ps.length][1] - p[1] * ps[(i + 1) % ps.length][0], 0)) / 2;
const bboxOf = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
const clone = x => JSON.parse(JSON.stringify(x));
function charmOf(c, reorder) {
  const members = clone(c.members); const outline = members[c.outlineIdx];
  const list = reorder ? members.slice().reverse() : members;
  return { sku: c.sku, outline, members: list, bbox: members.reduce((a, m) => a ? [Math.min(a[0], m.bbox[0]), Math.min(a[1], m.bbox[1]), Math.max(a[2], m.bbox[2]), Math.max(a[3], m.bbox[3])] : m.bbox.slice(), null), topIndices: [] };
}
const circles = (m) => (m.subpaths || []).map(sp => P.circleOf(sp, V)).filter(Boolean);
/** the material the laser leaves: outline exteriors (one piece each) minus nothing; the holes are the cut-line members */
const exteriors = c => V.boolean(c.outline.subpaths.map(sp => V.flatten(sp).points), [], "union", "evenodd").filter(p => !p.hole);
const inMaterial = (c, x, y) => { const polys = c.outline.subpaths.map(sp => V.flatten(sp).points); let ins = false; for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const a = poly[i], b = poly[j]; if ((a[1] > y) !== (b[1] > y) && x < (b[0] - a[0]) * (y - a[1]) / (b[1] - a[1]) + a[0]) ins = !ins; } return ins && !P.cutLinesOf(c).some(h => { const hp = h.subpaths.map(sp => V.flatten(sp).points); let hin = false; for (const poly of hp) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) { const a = poly[i], b = poly[j]; if ((a[1] > y) !== (b[1] > y) && x < (b[0] - a[0]) * (y - a[1]) / (b[1] - a[1]) + a[0]) hin = !hin; } return hin; }); };
const byId = id => fx.cases.find(c => c.id === id);
const hoopOf = c => P.findHoops(c, V)[0];
let checks = 0; const ok = (m) => { checks++; console.log("  ✓ " + m); };

// ── the real HUGGIE charms of Paul's screenshot ────────────────────────────────────────────────────────────────
// [id, holes the finished charm must have: the hoop's hole + the body's own cut-outs]
for (const [id, holes] of [["moon-huggie", 1], ["cheer-58940", 2], ["charging-16491", 1], ["cheer-64472", 1], ["cheerleader-60369", 1]]) {
  const raw = byId(id), c = charmOf(raw);
  const hoop = hoopOf(c); assert(hoop && hoop.aperture, `${id}: the hoop is found by its shape`);
  assert(Math.abs(hoop.outer.r * 2 - 9.35) < 0.05 && Math.abs(hoop.aperture.r * 2 - 5.85) < 0.05, `${id}: the HUGGIE ring is 9.35 outside / 5.85 hole: ${hoop.outer.r * 2} / ${hoop.aperture.r * 2}`);
  const before = { cx: hoop.cx, cy: hoop.cy, ro: hoop.outer.r, ri: hoop.aperture.r };
  const res = P.integrateRings(c);
  assert.equal(res.welded, 1, `${id}: the hoop is welded: ${JSON.stringify(res)}`); assert.deepEqual(res.left, [], `${id}: nothing left over`);
  // one piece: a single exterior, and the hoop's wall is part of it
  assert.equal(exteriors(c).length, 1, `${id}: the cut outline is one piece with the hoop`);
  const u = [(before.ro + before.ri) / 2];
  for (const a of [0, 90, 180, 270]) { const x = before.cx + Math.cos(a * Math.PI / 180) * u[0], y = before.cy + Math.sin(a * Math.PI / 180) * u[0]; const near = inMaterial(c, x, y); assert(near, `${id}: the hoop wall at ${a} degrees is cut material (the hoop is attached all the way round)`); }
  assert(c.outline.bbox[3] >= before.cy + before.ro - 0.05 || c.outline.bbox[1] <= before.cy - before.ro + 0.05 || c.outline.bbox[0] <= before.cx - before.ro + 0.05 || c.outline.bbox[2] >= before.cx + before.ro - 0.05, `${id}: the outline runs round the hoop's OUTSIDE circle`);
  // the hole is the only circle: no loose circle of hoop size or larger is left, and exactly one hole sits at the hoop
  for (const m of c.members) { if (m === c.outline) continue; for (const ci of circles(m)) assert(ci.r * 2 < 9 || m.layer === "HATCH" || m.layer === "ENGRAVE", `${id}: no extra large circle is left around the hoop (found d=${(ci.r * 2).toFixed(2)})`); }
  const atHoop = P.cutLinesOf(c).filter(m => circles(m).some(ci => Math.hypot(ci.cx - before.cx, ci.cy - before.cy) < 0.1));
  assert.equal(atHoop.length, 1, `${id}: exactly one hole at the hoop`);
  const hole = circles(atHoop[0])[0]; assert(Math.abs(hole.r - before.ri) < 0.05, `${id}: the hole keeps the drawn size ${hole.r * 2}`);
  assert.equal(P.cutLinesOf(c).length, holes, `${id}: ${holes} hole(s) (the hoop's hole and the body's own cut-outs), not extra circles or slivers: ${P.cutLinesOf(c).length}`);
  assert.equal(P.findHoops(c, V).filter(h => !h.aperture).length, 0, `${id}: no loose circle is taken for a hoop afterwards`);
  // welding twice changes nothing
  const a0 = exteriors(c).reduce((s, p) => s + area(p.points), 0), n0 = P.cutLinesOf(c).length;
  assert.equal(P.integrateRings(c).welded, 0, `${id}: welding is idempotent`); assert.equal(P.cutLinesOf(c).length, n0);
  assert(Math.abs(exteriors(c).reduce((s, p) => s + area(p.points), 0) - a0) < 1e-6);
  // the file order of the two circles never matters
  const r2 = charmOf(raw, true); assert.equal(P.integrateRings(r2).welded, 1, `${id}: member order reversed`);
  assert(Math.abs(exteriors(r2).reduce((s, p) => s + area(p.points), 0) - a0) < 1e-6 && P.cutLinesOf(r2).length === n0, `${id}: the same charm whichever circle comes first in the file`);
  ok(`${raw.sku}: hoop welded, one piece, one hole, no extra circle`);
}

// ── the standard ring: no spare holes, no slivers, order of the circles never matters ───────────────────────────
{
  const flower = charmOf(byId("flower-4")); assert.equal(P.integrateRings(flower).welded, 1);
  assert.equal(P.cutLinesOf(flower).length, 1, "a standard ring leaves one hole, not the old four (three sliver holes along the outline)");
  assert.equal(exteriors(flower).length, 1);
  const apple = charmOf(byId("apple-2")), appleR = charmOf(byId("apple-2"), true);
  assert.equal(P.integrateRings(apple).welded, 1); assert.equal(P.integrateRings(appleR).welded, 1);
  assert.equal(P.cutLinesOf(apple).length, 1, "two circles, any order: one hole"); assert.equal(P.cutLinesOf(appleR).length, 1);
  assert(Math.abs(exteriors(apple).reduce((s, p) => s + area(p.points), 0) - exteriors(appleR).reduce((s, p) => s + area(p.points), 0)) < 1e-6, "the same outline whichever circle comes first");
  ok("standard 6.5/3.7 ring: one hole, same result in either order");
}

// ── circles that are not hoops are left alone ────────────────────────────────────────────────────────────────
{
  const compass = charmOf(byId("compass-7-huggie")), before = JSON.stringify(compass.outline.subpaths), n = P.cutLinesOf(compass).length;
  const r = P.integrateRings(compass);
  assert.equal(r.welded, 0, "a hole circle inside the outline's own hoop is not welded again: " + JSON.stringify(r));
  assert.equal(JSON.stringify(compass.outline.subpaths), before); assert.equal(P.cutLinesOf(compass).length, n);
  const plate = charmOf(byId("skinny-mini-plate")), nPlate = P.cutLinesOf(plate).length;
  assert.equal(P.integrateRings(plate).welded, 0, "a hole of a second plate is not a hoop of the first");
  assert.equal(P.cutLinesOf(plate).length, nPlate);
  ok("a hole circle of the outline or of a neighbouring plate is not moved or welded");
}

// ── a hoop the master draws clear of the body is bridged where it stands ────────────────────────────────────────
{
  const raw = byId("huggie-gap"), c = charmOf(raw); const hoop = hoopOf(c); const centre = [hoop.cx, hoop.cy];
  const r = P.integrateRings(c);
  assert.equal(r.welded, 1, JSON.stringify(r)); assert.equal(r.bridged.length, 1, "a neck was needed"); assert(r.bridged[0].gapPt > 4, "the master leaves a gap of " + r.bridged[0].gapPt + " pt");
  assert.equal(exteriors(c).length, 1, "body, neck and hoop are one piece");
  const hole = circles(P.cutLinesOf(c).find(m => circles(m).length && Math.hypot(circles(m)[0].cx - centre[0], circles(m)[0].cy - centre[1]) < 0.1))[0];
  assert(hole, "the hoop stayed where the artist drew it (its hole is still at the drawn centre)");
  ok("a gap is bridged by a neck; the hoop is not moved");
}

// ── attached from every side: a gap, a graze, a pointed tip, a concave corner ───────────────────────────────────
{
  const circle = (cx, cy, r) => { const k = 0.5522847498 * r; return [["m", [cx + r, cy]], ["c", [cx + r, cy + k], [cx + k, cy + r], [cx, cy + r]], ["c", [cx - k, cy + r], [cx - r, cy + k], [cx - r, cy]], ["c", [cx - r, cy - k], [cx - k, cy - r], [cx, cy - r]], ["c", [cx + k, cy - r], [cx + r, cy - k], [cx + r, cy]], ["h"]]; };
  const mem = (subpaths, color) => ({ kind: "path", closed: true, stroke: true, fill: false, strokeRGB: color, lwPt: 0.25, layer: "CUT", paintOp: "S", subpaths, bbox: bboxOf(subpaths.flatMap(s => V.flatten(s).points)) });
  const shapes = {
    box: [["m", [-12, -30]], ["l", [12, -30]], ["l", [12, 0]], ["l", [-12, 0]], ["h"]],
    tip: [["m", [-14, -30]], ["l", [14, -30]], ["l", [0, 0]], ["h"]],                                  // a sharp point toward the hoop
    notch: [["m", [-14, -30]], ["l", [14, -30]], ["l", [14, 0]], ["l", [3, 0]], ["l", [0, -6]], ["l", [-3, 0]], ["l", [-14, 0]], ["h"]],
  };
  let n = 0;
  for (const [name, sh] of Object.entries(shapes)) for (const gap of [-1.2, -0.2, 0, 0.4, 3, 9]) for (const [ro, ri] of [[4.675, 2.925], [3.25, 1.84]]) for (const ang of [90, 60, 120, 75]) {
    const d = ro + gap, cx = Math.cos(ang * Math.PI / 180) * d, cy = Math.sin(ang * Math.PI / 180) * d;
    const body = mem([sh], [0, 0, 0]), ring = mem([circle(cx, cy, ro), circle(cx, cy, ri)], [0, 1, 1]);
    const c = { sku: name, outline: body, members: [body, ring], bbox: [Math.min(body.bbox[0], ring.bbox[0]), Math.min(body.bbox[1], ring.bbox[1]), Math.max(body.bbox[2], ring.bbox[2]), Math.max(body.bbox[3], ring.bbox[3])], topIndices: [] };
    const r = P.integrateRings(c); n++;
    assert.equal(r.welded, 1, `${name} gap ${gap} ro ${ro} at ${ang}: ${JSON.stringify(r)}`);
    assert.equal(exteriors(c).length, 1, `${name} gap ${gap} ro ${ro} at ${ang}: one piece`);
    const wall = (ro + ri) / 2;
    for (const a of [0, 90, 180, 270]) assert(inMaterial(c, cx + Math.cos(a * Math.PI / 180) * wall, cy + Math.sin(a * Math.PI / 180) * wall), `${name} gap ${gap} ro ${ro} at ${ang}: wall at ${a} is material`);
    if (name === "notch") assert(P.cutLinesOf(c).length >= 1, `${name} gap ${gap}: the hoop's hole`);   // the notch may close into a pocket of its own
    else assert.equal(P.cutLinesOf(c).length, 1, `${name} gap ${gap}: one hole`);
  }
  ok(`${n} hoops (gap, graze, overlap; flat edge, point, notch; both ring sizes; four directions) always end attached as one piece`);
}

// ── the parser and the writer: a hoop welded at index time survives the per-SKU file and is not welded twice ─────
(async () => {
  for (const id of ["moon-huggie", "charging-16491", "cheerleader-60369"]) {
    const raw = byId(id), members = clone(raw.members);
    const b = members.reduce((a, m) => [Math.min(a[0], m.bbox[0]), Math.min(a[1], m.bbox[1])], [Infinity, Infinity]);
    const shift = sp => sp.map(op => [op[0], ...op.slice(1).map(p => [p[0] - b[0] + 20, p[1] - b[1] + 20])]);
    const placed = members.map(m => Object.assign({}, m, { subpaths: m.subpaths.map(shift), synthetic: true }));
    const doc = await PDFLib.PDFDocument.create(), page = doc.addPage([80, 80]); page.node.normalize();
    page.node.addContentStream(doc.context.register(doc.context.flateStream(P.syntheticOps(placed))));
    const parsed = await P.parseSource(await doc.save(), id);
    const g = P.groupCharms(parsed, { minPt: 6 }); assert.equal(g.charms.length, 1, `${id}: one charm in the page (hoop grouped with its body)`);
    const c = g.charms[0], r = P.integrateRings(c);
    assert.equal(r.welded, 1, `${id}: parsed from a file, the hoop is welded: ${JSON.stringify(r)}`);
    const holes = P.cutLinesOf(c).length;
    const copy = await P.parseSource(await P.buildSingleCharm(Object.assign({}, c, { name: id }), parsed), id + " copy");
    const g2 = P.groupCharms(copy, { minPt: 6 }); assert.equal(g2.charms.length, 1, `${id}: the per-SKU file is one charm`);
    const c2 = g2.charms[0];
    assert.equal(P.integrateRings(c2).welded, 0, `${id}: the per-SKU file is not welded a second time`);
    assert.equal(P.cutLinesOf(c2).length, holes, `${id}: the same holes after the file is read back`);
    assert.equal(exteriors(c2).length, 1, `${id}: one piece after the file is read back`);
    assert(Math.abs(exteriors(c2).reduce((s, p) => s + area(p.points), 0) - exteriors(c).reduce((s, p) => s + area(p.points), 0)) < 0.05, `${id}: the same outline after the file is read back`);
  }
  ok("parse -> weld -> write the per-SKU file -> read it back: same outline, same holes, nothing welded twice");
  console.log(`Hoops OK: ${checks} groups of checks (HUGGIE and standard rings, any file order, gaps, grazes, tips, non-hoop circles, per-SKU file)`);
})().catch(e => { console.error(e); process.exitCode = 1; });
