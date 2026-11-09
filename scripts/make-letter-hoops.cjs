#!/usr/bin/env node
/*  scripts/make-letter-hoops.cjs — the two hooped alphabets (LETTERS-HOOPS, Paul 9 Nov 2026).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul: "Extract all of these letters from my charm repo (a-z) and make 2 additional complete new sets of alphabet letters.
 *  Set A) Add a Necklace charm style hoop to the top of each letter (a-z). Set B) Add a Huggie Charm style hoop to each letter (a-z)."
 *
 *  Source: the 26 master charms LETTER_EARRING-0 … LETTER_EARRING-25 (a = 0 … z = 25), their per-SKU .ai files as the library
 *  stores them (read with the free GET ops masterList / masterGet; the files are the public aiUrl links). Each letter's outline is
 *  copied as it is (same points, same size, same counters); only a hoop is added.
 *
 *  The hoops are the repo's own, never invented (measured from the library's per-SKU files with the app's parser):
 *    necklace  outside Ø 6.50 pt, hole Ø 3.684 pt   NECKLACE_CHARM-0…9 (listing 1871838409), AFRICA 1- ZEBRA (NECKLACE), BEADY DOVE 4
 *    huggie    outside Ø 9.354 pt, hole Ø 5.844 pt  NECKLACE_CHARM-0…9 (HUGGIE), DOVE 4 (HUGGIE), HUGGIE HOOPS- LOWERCASE INITIALS
 *  (the two sizes charm-nest-pdf.js names in "6c · hoops welded into the cut line": 6.5 / 3.7 and 9.35 / 5.85). How the hoop sits on
 *  the body is the number set's: on NECKLACE_CHARM-7 the hole's lowest point is 0.19 pt above the flat top of the 7, on
 *  NECKLACE_CHARM-7 (HUGGIE) it touches it (0.01 pt); the ring's lower part overlaps the body and is welded into one cut outline.
 *
 *  Each letter is scaled whole to Paul's sizes, without its hoop (SIZES below): necklace 11–12 mm, huggie 6–7 mm by shape; the hoops keep their fixed sizes.
 *  Where on the letter: centred on the letter's visual top. A letter with an ascender (b d f h k l t) or a dot (i j) carries it on the
 *  top of that stem or dot (its own top); every other letter at the centre of its box, at the height of the letter's own top there
 *  (the two arms of u v w x y are bridged by the ring). A ring that would come nearer than 0.5 pt to a counter (the hole of an a, e,
 *  o …) is lifted just enough to keep 0.5 pt of metal (never moved sideways); one that still cannot is refused.
 *
 *  Output (folder given with --out): a master sheet (the 52 charms, each with its SKU written under it, the way every master is
 *  drawn), then the app's own local indexer (scripts/index-master.cjs --out-dir: parse, read the SKUs, weld each hoop into the cut
 *  line with integrateRings, write one .ai and one picture per SKU, and records.json, exactly what masterPutIndex would get), and
 *  placement.json (where each hoop went and the checks). Nothing is sent anywhere: the upload is Paul's press in the Library
 *  (charm-nest-repo-adds.js).
 *
 *    node scripts/make-letter-hoops.cjs --src <folder with 0.ai … 25.ai> --out charm-repo-adds/letters-hoops-2026-11-09
 *  SKUs: LETTER_NECKLACE-n and LETTER_NECKLACE-n (HUGGIE), the repo's twin pattern (NECKLACE_CHARM-n / NECKLACE_CHARM-n (HUGGIE)).
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const fs = require("fs"), path = require("path");
const root = path.join(__dirname, "..");
const { PDFDocument, PDFName, rgb, StandardFonts } = require(path.join(root, "vendor/pdf-lib-1.17.1.min.js"));
const { CharmNestPDF: P } = require(path.join(root, "netlify/functions/_charmNestPdf.js"));
const V = require(path.join(root, "charm-nest-vector.js"));
const MM = 72 / 25.4;

const LETTERS = "abcdefghijklmnopqrstuvwxyz";
const HOOPS = Object.freeze({
  necklace: Object.freeze({ outer: 3.249, hole: 1.842, gap: 0.19, sku: n => `LETTER_NECKLACE-${n}` }),
  huggie: Object.freeze({ outer: 4.677, hole: 2.922, gap: 0.01, sku: n => `LETTER_NECKLACE-${n} (HUGGIE)` })
});
const STEM = new Set("bdfhklt"), DOT = new Set("ij");
const COUNTER_CLEAR_PT = 0.5, MIN_JOINT_PT = 2, SPECK_PT2 = 5;

/** The gaps the ring closes off between itself and the letter (it bridges a notch: the top of n, p, r; the arms of u v w x y).
 *  A gap under SPECK_PT2 (about 0.6 mm2) is a speck the laser cannot cut cleanly: it is filled (metal under the ring). A larger one is the letter's
 *  own open space with the ring across its top (the inside of a u): it stays a window. Returns { fill: [polygon], windows: [{ areaPt2, wPt, hPt }], specks: [...] }. */
function pockets(polys, rule, cx, cy, H) {
  const counters = V.boolean(polys, [], "union", rule).filter(p => p.hole).map(p => p.points);
  const holes = V.boolean(polys, [circlePts(cx, cy, H.outer)], "union", rule, "nonzero").filter(p => p.hole).map(p => p.points);
  const isCounter = h => counters.some(c => Math.abs(Math.abs(area(c)) - Math.abs(area(h))) < 0.01 && V.boolean([c], [h], "xor").reduce((v, p) => v + Math.abs(area(p.points)), 0) < 0.01);
  const out = { fill: [], windows: [], specks: [] };
  for (const h of holes) {
    if (isCounter(h)) continue;
    const b = bboxOf([h]), f = { areaPt2: +Math.abs(area(h)).toFixed(2), wPt: +(b[2] - b[0]).toFixed(2), hPt: +(b[3] - b[1]).toFixed(2) };
    if (Math.abs(area(h)) < SPECK_PT2) { out.fill.push(h); out.specks.push(f); } else out.windows.push(f);
  }
  return out;
}

/** The x-intervals of a set of closed polygons cut by the horizontal line y (even-odd). */
function intervalsAt(polys, y) {
  const xs = [];
  for (const p of polys) for (let i = 0; i < p.length; i++) { const a = p[i], b = p[(i + 1) % p.length]; if ((a[1] - y) * (b[1] - y) < 0) xs.push(a[0] + (y - a[1]) / (b[1] - a[1]) * (b[0] - a[0])); }
  xs.sort((a, b) => a - b); const out = []; for (let i = 0; i + 1 < xs.length; i += 2) out.push([xs[i], xs[i + 1]]); return out;
}
const bboxOf = polys => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of polys) for (const q of p) { if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; } return [x0, y0, x1, y1]; };
const area = p => p.reduce((v, q, i) => v + q[0] * p[(i + 1) % p.length][1] - q[1] * p[(i + 1) % p.length][0], 0) / 2;
const circlePts = (cx, cy, r, n = 96) => Array.from({ length: n }, (_, i) => [cx + r * Math.cos(2 * Math.PI * i / n), cy + r * Math.sin(2 * Math.PI * i / n)]);
function distToPolys(x, y, polys) { let d = Infinity; for (const p of polys) for (let i = 0; i < p.length; i++) { const a = p[i], b = p[(i + 1) % p.length], dx = b[0] - a[0], dy = b[1] - a[1], L = dx * dx + dy * dy, t = L ? Math.max(0, Math.min(1, ((x - a[0]) * dx + (y - a[1]) * dy) / L)) : 0; d = Math.min(d, Math.hypot(x - a[0] - t * dx, y - a[1] - t * dy)); } return d; }

/** The highest point of the letter's material over x ∈ [x0, x1] (its own top there). */
function topOver(material, x0, x1) {
  const strip = V.boolean(material, [[[x0, -1e4], [x1, -1e4], [x1, 1e4], [x0, 1e4]]], "intersection");
  let top = -Infinity; for (const p of strip) if (!p.hole) for (const q of p.points) top = Math.max(top, q[1]); return top;
}

/** Where the hoop of kind `kind` goes on one letter: { cx, cy, rule, checks } in the letter's own coordinates. Throws when the letter cannot carry it cleanly. */
function placeHoop(ch, outlineSubpaths, fillRule, kind) {
  const H = HOOPS[kind];
  const polys = outlineSubpaths.map(sp => V.flatten(sp).points);
  const material = V.boolean(polys, [], "union", fillRule).filter(p => !p.hole).map(p => p.points);
  const counters = V.boolean(polys, [], "union", fillRule).filter(p => p.hole).map(p => p.points);
  const [x0, , x1, y1] = bboxOf(polys);
  let cx, rule;
  if (STEM.has(ch) || DOT.has(ch)) {
    // the stem (or dot) that reaches the top: below the serif, 4 pt down, the interval that sits under the top's own material
    const top = intervalsAt(polys, y1 - 0.3);
    const tops = top.length ? [Math.min(...top.map(t => t[0])), Math.max(...top.map(t => t[1]))] : [x0, x1];
    const depth = DOT.has(ch) ? 1 : 4;
    const under = intervalsAt(polys, y1 - depth).filter(t => t[1] > tops[0] && t[0] < tops[1]);
    const s = under.length ? under.reduce((a, b) => (b[1] - b[0] < a[1] - a[0] ? b : a)) : tops;
    cx = (s[0] + s[1]) / 2; rule = DOT.has(ch) ? "on the dot" : "on the top of the stem";
  } else { cx = (x0 + x1) / 2; rule = "centre of the letter"; }
  // the ring at cx: { cx, cy, rule, top, checks } or { fail } (a counter it cannot clear, or too small a joint)
  const at = (cx, rule) => {
    const top = topOver(material, cx - H.hole, cx + H.hole);
    if (!Number.isFinite(top)) return { fail: `${ch} (${kind}): no material under the hoop` };
    // checks: the ring never comes near a counter (where it would, it is lifted just enough); the ring and the letter share a real joint
    const counterGapAt = y => { const ring = circlePts(cx, y, H.outer); return counters.length ? Math.min(...counters.map(c => V.boolean([c], [ring], "intersection").length ? 0 : Math.max(0, distToPolys(cx, y, [c]) - H.outer))) : Infinity; };
    let cy = top + H.hole + H.gap, lifted = 0;
    while (counterGapAt(cy) < COUNTER_CLEAR_PT && lifted < 1.5) { cy += 0.01; lifted += 0.01; }
    const counterGap = counterGapAt(cy);
    if (counterGap < COUNTER_CLEAR_PT) return { fail: `${ch} (${kind}): the hoop would come ${counterGap.toFixed(2)} pt from a counter` };
    if (lifted) rule += `, lifted ${lifted.toFixed(2)} pt to keep ${COUNTER_CLEAR_PT} pt of metal over the counter`;
    const ring = circlePts(cx, cy, H.outer);
    const joint = V.boolean(material, [ring], "intersection").filter(p => !p.hole);
    const jb = joint.length ? bboxOf(joint.map(p => p.points)) : null, jointW = jb ? jb[2] - jb[0] : 0;
    if (jointW < MIN_JOINT_PT) return { fail: `${ch} (${kind}): the hoop touches the letter over only ${jointW.toFixed(2)} pt` };
    const holeHitsLetter = V.boolean(material, [circlePts(cx, cy, H.hole)], "intersection").reduce((v, p) => v + Math.abs(area(p.points)), 0);
    return { cx, cy, rule, top, checks: { counterGapPt: Number.isFinite(counterGap) ? +counterGap.toFixed(3) : null, liftedPt: +lifted.toFixed(2), jointPt: +jointW.toFixed(3), holeOverLetterPt2: +holeHitsLetter.toFixed(4), ringAboveTopPt: +(cy + H.outer - y1).toFixed(3) } };
  };
  let r = at(cx, rule);
  // the centre has too little metal under the ring (the r, once large: its stem and its arm with a gap between): the nearest top of the letter's own metal that holds it
  if (r.fail) {
    const tops = intervalsAt(polys, y1 - 1).map(t => (t[0] + t[1]) / 2).sort((p, q) => Math.abs(p - cx) - Math.abs(q - cx));
    for (const c of tops) { const t = at(c, `on the nearest top of the letter (the centre has too little metal under the hoop), ${(c - cx).toFixed(1)} pt from the centre`); if (!t.fail) { r = t; break; } }
  }
  if (r.fail) throw new Error(r.fail);
  return r;
}

const num = n => (+n).toFixed(4);
const circleOps = (cx, cy, r) => { const k = 0.5522847498 * r; return `${num(cx + r)} ${num(cy)} m ${num(cx + r)} ${num(cy + k)} ${num(cx + k)} ${num(cy + r)} ${num(cx)} ${num(cy + r)} c ${num(cx - k)} ${num(cy + r)} ${num(cx - r)} ${num(cy + k)} ${num(cx - r)} ${num(cy)} c ${num(cx - r)} ${num(cy - k)} ${num(cx - k)} ${num(cy - r)} ${num(cx)} ${num(cy - r)} c ${num(cx + k)} ${num(cy - r)} ${num(cx + r)} ${num(cy - k)} ${num(cx + r)} ${num(cy)} c h`; };
const pathOps = (subs, dx, dy) => subs.map(sp => sp.map(o => o[0] === "m" ? `${num(o[1][0] + dx)} ${num(o[1][1] + dy)} m` : o[0] === "l" ? `${num(o[1][0] + dx)} ${num(o[1][1] + dy)} l` : o[0] === "c" ? `${o.slice(1).map(p => `${num(p[0] + dx)} ${num(p[1] + dy)}`).join(" ")} c` : o[0] === "h" ? "h" : "").join(" ")).join(" ");

async function readLetter(file) {
  const buf = fs.readFileSync(file);
  const parsed = await P.parseSource(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength), path.basename(file));
  const g = P.groupCharms(parsed, { minPt: 6 }), live = g.charms.filter(c => c.mergedInto == null);
  if (live.length !== 1) throw new Error(`${file}: ${live.length} charms, expected 1`);
  const c = live[0];
  if (c.members.length !== 1) throw new Error(`${file}: ${c.members.length} members, expected the outline only`);
  const o = c.outline, even = String(o.paintOp || "").endsWith("*");
  return { subpaths: o.subpaths, fill: !!o.fill, stroke: !!o.stroke, paintOp: o.paintOp, rule: o.fill && !even ? "nonzero" : "evenodd", bbox: bboxOf(o.subpaths.map(sp => V.flatten(sp).points)) };
}

/* THE SIZES (Paul, 9 Nov 2026: "the necklace charms need to be closer to 11-12 mm in size excluding the hoop and the huggy charms need to be 6-7mm in size excluding the hoop";
   "make sure the hoop of the huggie charm hoops size stays constant while the charm shrinks").
   Each LETTER (never its hoop: the hoops keep the shop's sizes, as NECKLACE_CHARM-n and NECKLACE_CHARM-n (HUGGIE) do) is scaled whole, the same way across and up (its shape and
   its counters stay as they are), so its longest side is
     necklace: 11 mm for a compact letter (a c e n o r s u v x z), 12 mm for one with an ascender, a descender or a dot (b d f g h i j k l p q t y) and the wide m w;
     huggie:    6 mm compact, 7 mm tall or wide;
   a letter already within 1 % of its size is left as it is. */
const SIZES = Object.freeze({ necklace: Object.freeze({ compactMm: 11, longMm: 12 }), huggie: Object.freeze({ compactMm: 6, longMm: 7 }), long: "bdfghijklpqtymw" });
function sizeScale(L, ch, kind) {
  const w = L.bbox[2] - L.bbox[0], h = L.bbox[3] - L.bbox[1], long = Math.max(w, h), S = SIZES[kind];
  const tall = SIZES.long.includes(ch), want = (tall ? S.longMm : S.compactMm) * MM;
  const scale = Math.abs(long - want) <= want * 0.01 ? 1 : want / long;
  return { scale, fromMm: +(long / MM).toFixed(2), toMm: +(long * scale / MM).toFixed(2), rule: `${tall ? "tall or wide" : "compact"} letter: longest side ${(long / MM).toFixed(1)} mm${scale !== 1 ? ` → ${(long * scale / MM).toFixed(1)} mm` : " (kept)"}` };
}
/** The letter scaled about its own bottom-left corner (every point, curves included: a uniform scale keeps them exact). */
function scaleLetter(L, k) {
  const ox = L.bbox[0], oy = L.bbox[1], f = q => [ox + (q[0] - ox) * k, oy + (q[1] - oy) * k];
  const subpaths = L.subpaths.map(sp => sp.map(o => o[0] === "h" ? o : [o[0]].concat(o.slice(1).map(f))));
  return Object.assign({}, L, { subpaths, bbox: [ox, oy, ox + (L.bbox[2] - ox) * k, oy + (L.bbox[3] - oy) * k] });
}

/** The master sheet: row 1–2 the necklace set, row 3–4 the huggie set, a to z; each SKU 2 mm under its charm, centred (how the masters are written). */
async function buildSheet(letters, outFile) {
  const doc = await PDFDocument.create(); const font = await doc.embedFont(StandardFonts.Helvetica);
  const COLS = 13, PITCH_X = 96, PITCH_Y = 96, MARGIN = 30;
  const W = MARGIN * 2 + COLS * PITCH_X, H = MARGIN * 2 + 4 * PITCH_Y;
  const page = doc.addPage([W, H]); const ops = [], labels = [], placed = [];
  for (const kind of ["necklace", "huggie"]) {
    letters.forEach((L0, n) => {
      const ch = LETTERS[n], sz = sizeScale(L0, ch, kind), L = sz.scale === 1 ? L0 : scaleLetter(L0, sz.scale);
      const hp = placeHoop(ch, L.subpaths, L.rule, kind), Hh = HOOPS[kind];
      const row = (kind === "necklace" ? 0 : 2) + Math.floor(n / COLS), col = n % COLS;
      const slotX = MARGIN + col * PITCH_X + PITCH_X / 2, baseY = H - MARGIN - row * PITCH_Y - PITCH_Y + 16;   // the letter's bottom sits 16 pt above the slot's foot
      const dx = slotX - (L.bbox[0] + L.bbox[2]) / 2, dy = baseY - L.bbox[1];
      // the letter as a black hairline cut outline (its counters are its own subpaths), then the hoop as a washer of two black circles: integrateRings welds it.
      // (A black FILL would do for the letter, but on a narrow one, the l, the grouping takes the stroked ring for the outline.) A speck the ring would close off is metal (pockets).
      const pk = pockets(L.subpaths.map(sp => V.flatten(sp).points), L.rule, hp.cx, hp.cy, Hh);
      const subs = pk.fill.length ? V.boolean(L.subpaths.map(sp => V.flatten(sp).points), pk.fill, "union", L.rule, "nonzero").map(p => V.subpath(p.points)) : L.subpaths;
      ops.push(`q 0 0 0 RG 0.25 w ${pathOps(subs, dx, dy)} S Q`);
      ops.push(`q 0 0 0 RG 0.25 w ${circleOps(hp.cx + dx, hp.cy + dy, Hh.outer)} S Q`);
      ops.push(`q 0 0 0 RG 0.25 w ${circleOps(hp.cx + dx, hp.cy + dy, Hh.hole)} S Q`);
      labels.push({ text: Hh.sku(n), x: slotX, y: baseY - 2 * MM });
      placed.push({ sku: Hh.sku(n), source: `LETTER_EARRING-${n}`, letter: ch, kind, rule: hp.rule, scale: +sz.scale.toFixed(6), size: sz.rule ? { rule: sz.rule, fromMm: sz.fromMm, toMm: sz.toMm } : null, hoop: { cx: +(hp.cx - L.bbox[0]).toFixed(3), cy: +(hp.cy - L.bbox[1]).toFixed(3), outerPt: Hh.outer * 2, holePt: Hh.hole * 2 }, letterPt: [+(L.bbox[2] - L.bbox[0]).toFixed(3), +(L.bbox[3] - L.bbox[1]).toFixed(3)], checks: Object.assign({}, hp.checks, { specksFilled: pk.specks, windows: pk.windows }) });
    });
  }
  const ref = doc.context.register(doc.context.flateStream(ops.join("\n") + "\n"));
  page.node.set(PDFName.of("Contents"), doc.context.obj([ref]));
  const size = 5;
  for (const l of labels) { const w = font.widthOfTextAtSize(l.text, size); page.drawText(l.text, { x: l.x - w / 2, y: l.y - size * 0.75, size, font, color: rgb(0.1, 0.1, 0.1) }); }
  doc.setTitle("LETTER HOOPS 2026-11-09"); doc.setProducer("Brites Charm Nesting Station"); doc.setCreator("scripts/make-letter-hoops.cjs");
  doc.setCreationDate(new Date("2026-11-09T00:00:00Z")); doc.setModificationDate(new Date("2026-11-09T00:00:00Z"));   // (fixed: the same letters make the same file, the same hash)
  const bytes = await doc.save({ useObjectStreams: false });
  fs.writeFileSync(outFile, bytes);
  return placed;
}

/** The contact sheet Paul looks at before he presses: each source letter beside its two new charms, with the SKU, the size and what the hoop did. Static, same-origin pictures only. */
function contactSheet(rec, placed, srcMeta, cut) {
  const esc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  const url = p => p.split("/").map(encodeURIComponent).join("/");
  const mm = pt => (pt * 25.4 / 72).toFixed(1);
  const byS = new Map(rec.entries.map(e => [e.sku, e])), byP = new Map(placed.map(x => [x.sku, x]));
  const say = x => { const c = x.checks, w = []; if (x.size && x.scale !== 1) w.push(`letter ${x.scale > 1 ? "made larger" : "made smaller"}: ${x.size.fromMm.toFixed(1)} → ${x.size.toMm.toFixed(1)} mm`); if (c.liftedPt) w.push(`ring lifted ${c.liftedPt} pt off the counter`); if (c.specksFilled.length) w.push(`${c.specksFilled.length === 1 ? "a speck" : c.specksFilled.length + " specks"} under the ring filled (${c.specksFilled.map(s => s.areaPt2 + " pt²").join(", ")})`); if (c.windows.length) w.push("the ring closes the top of the letter's own gap (a window)"); return w.join("; "); };
  const card = (sku, kind) => { const e = byS.get(sku), x = byP.get(sku); const k = cut.get(sku); return `<figure class="c"><img src="files/${url(e.thumbPath)}" alt="${esc(sku)}" loading="lazy"><figcaption><b>${esc(sku)}</b><span><b>${mm(e.bodyPt[0])} × ${mm(e.bodyPt[1])} mm</b> without the hoop (${mm(k[0])} × ${mm(k[1])} mm with it) · ${kind} · ${e.holes} hole${e.holes === 1 ? "" : "s"} cut</span>${say(x) ? `<i>${esc(say(x))}</i>` : ""}</figcaption></figure>`; };
  const rows = [];
  for (let n = 0; n < 26; n++) {
    const s = srcMeta[n];
    rows.push(`<section class="r"><h2>${LETTERS[n]}</h2><figure class="c src"><img src="source/LETTER_EARRING-${n}.png" alt="LETTER_EARRING-${n}" loading="lazy"><figcaption><b>LETTER_EARRING-${n}</b><span>source · ${mm(cut.get("LETTER_EARRING-" + n)[0])} × ${mm(cut.get("LETTER_EARRING-" + n)[1])} mm cut</span></figcaption></figure>${card(HOOPS.necklace.sku(n), "necklace ring")}${card(HOOPS.huggie.sku(n), "huggie hoop")}</section>`);
  }
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Letter charms with hoops</title>
<style>
:root{--ink:#2a2724;--ink70:#5d5852;--line:#ddd6ca;--card:#fbf9f5;--bg:#f3efe7;--gold:#b98a2e}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1100px;margin:0 auto;padding:20px 16px 60px}h1{font-size:22px;margin:0 0 4px}p{margin:6px 0;color:var(--ink70)}
ol{margin:8px 0 14px;padding-left:22px}li{margin:3px 0}code{font:12px ui-monospace,Menlo,monospace;background:#fff;border:1px solid var(--line);border-radius:5px;padding:0 4px}
.keys{display:flex;flex-wrap:wrap;gap:8px 18px;margin:10px 0 18px;font-size:13px;color:var(--ink70)}
.r{display:grid;grid-template-columns:32px repeat(3,minmax(0,1fr));gap:10px;align-items:start;padding:12px 0;border-top:1px solid var(--line)}
.r h2{margin:0;font:600 22px/1 Georgia,serif;color:var(--gold);padding-top:6px}
.c{margin:0;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px;min-width:0}
.c img{display:block;width:100%;max-width:168px;height:auto;margin:0 auto 6px;border-radius:6px}
.c figcaption{display:grid;gap:1px;font-size:12px;overflow-wrap:anywhere}.c figcaption span{color:var(--ink70)}.c figcaption i{color:#8a5a14;font-style:normal}
.src{opacity:.9}
@media (max-width:640px){.r{grid-template-columns:repeat(3,minmax(0,1fr))}.r h2{grid-column:1/-1}}
</style></head><body><main>
<h1>Letter charms with hoops: 26 necklace + 26 huggie</h1>
<p>Every size is the letter alone, <b>without the hoop</b> (the size with the hoop is in brackets). The Library card shows the same.</p>
<p>Each source letter from the charm repo (<code>LETTER_EARRING-0 … 25</code>, a … z) with its two new charms. Each letter keeps its own outline and is scaled whole, without its hoop, to your sizes: <b>necklace 11 mm</b> (a c e n o r s u v x z) or <b>12 mm</b> (b d f g h i j k l p q t y, m w) on its longest side; <b>huggie 6 mm</b> or <b>7 mm</b> by the same rule. The hoops keep their fixed sizes. Only the hoop is added, at the top centre (on the stem of b d f h k l t, on the dot of i j), welded into one cut outline, its hole cut.</p>
<div class="keys"><span><b>Necklace ring</b>: Ø ${mm(HOOPS.necklace.outer * 2)} mm, hole ${mm(HOOPS.necklace.hole * 2)} mm (as <code>NECKLACE_CHARM-7</code>)</span><span><b>Huggie hoop</b>: Ø ${mm(HOOPS.huggie.outer * 2)} mm, hole ${mm(HOOPS.huggie.hole * 2)} mm (as <code>NECKLACE_CHARM-7 (HUGGIE)</code>)</span><span><b>SKUs</b>: <code>LETTER_NECKLACE-n</code> and <code>LETTER_NECKLACE-n (HUGGIE)</code>, a = 0 … z = 25 (the shop's twin naming)</span></div>
<p><b>To put them in the charm repo</b> (they replace the first, wrongly sized drawings of the same SKUs):</p>
<ol><li>Open the Brites Charm Sorter, open the <b>Workspace</b> menu (top right) and choose <b>Master files</b>.</li><li>Under <b>＋ Add a master file</b>, on the line “New charms ready: Letter charms with hoops”, press <b>Add these charms to the repo</b>.</li><li>Read the line and press <b>Yes, add them</b>.</li><li>Type the repo password in the lock window.</li><li>Wait for “Done”. Pressing again says “Already there”.</li></ol>
${rows.join("\n")}
</main></body></html>
`;
}

async function main(argv, log = console.log) {
  const a = { src: "", out: "", replaces: [] };
  for (let i = 2; i < argv.length; i++) { if (argv[i] === "--src") a.src = argv[++i]; else if (argv[i] === "--out") a.out = argv[++i]; else if (argv[i] === "--replaces") a.replaces.push(argv[++i]); }   // (--replaces: the master hash of an earlier version of this sheet whose entries these replace)
  if (!a.src || !a.out) throw new Error("usage: node scripts/make-letter-hoops.cjs --src <folder with 0.ai … 25.ai> --out <folder>");
  fs.mkdirSync(a.out, { recursive: true });
  const letters = []; for (let n = 0; n < 26; n++) { const f = [path.join(a.src, `${n}.ai`), path.join(a.src, `LETTER_EARRING-${n}.ai`)].find(x => fs.existsSync(x)); if (!f) throw new Error(`no source file for LETTER_EARRING-${n}`); letters.push(await readLetter(f)); }
  const sheet = path.join(a.out, "LETTER-HOOPS_2026-1109.ai");
  const placed = await buildSheet(letters, sheet);
  log(`master sheet: ${sheet} (52 charms)`);
  const IM = require(path.join(root, "scripts/index-master.cjs"));
  // the app's own indexer stages straight into the folder: files/charmnest/master/<SKU>.ai|.png and records.json (what was there before is replaced)
  fs.rmSync(path.join(a.out, "files"), { recursive: true, force: true }); fs.rmSync(path.join(a.out, "records.json"), { force: true });
  const work = path.join(a.out, ".work");
  const rep = await IM.main(["node", "index-master", sheet, "--out-dir", a.out, "--max-mm", "0", "--no-pairs", "--work-dir", work].concat(a.replaces.flatMap(h => ["--replaces", h])), l => log("  " + l));
  fs.rmSync(work, { recursive: true, force: true });
  if (rep.unlabelled && rep.unlabelled.length) throw new Error(`${rep.unlabelled.length} charm(s) of the sheet were not read under their SKU`);
  const recFile = path.join(a.out, "records.json"), rec = JSON.parse(fs.readFileSync(recFile, "utf8"));
  if (rec.entries.length !== 52 || (rec.blocked || []).length) throw new Error(`the indexer staged ${rec.entries.length} entries, ${(rec.blocked || []).length} blocked`);
  // the order Paul reads them in (a…z necklace, then a…z huggie), and the sheet's page for the upload history
  // each entry's size WITHOUT its hoop (bodyPt [w, h], the letter's cut box): the Library card shows it (Paul: "only include the actual charm and not the hoop")
  for (const e of rec.entries) { const x = placed.find(y => y.sku.toUpperCase() === e.sku.toUpperCase()); e.bodyPt = x.letterPt.slice(); }
  const order = placed.map(x => x.sku.toUpperCase()); rec.entries.sort((x, y) => order.indexOf(x.sku.toUpperCase()) - order.indexOf(y.sku.toUpperCase()));
  const parsed = await P.parseSource(new Uint8Array(fs.readFileSync(sheet)), path.basename(sheet));
  rec.sheet = { file: path.basename(sheet), pageW: +parsed.pageW.toFixed(3), pageH: +parsed.pageH.toFixed(3) };
  rec.what = "The 52 hooped letter charms, staged by the app's own indexer (scripts/index-master.cjs --out-dir) from the master sheet beside this file; the one press in Workspace > Master files (charm-nest-repo-adds.js) stores files/ and writes these entries. No buyer data.";
  fs.writeFileSync(recFile, JSON.stringify(rec, null, 1) + "\n");
  fs.writeFileSync(path.join(a.out, "placement.json"), JSON.stringify({ what: "Where each hoop went on its letter (pt, from the letter's own bottom-left), and the checks. scripts/make-letter-hoops.cjs", hoops: HOOPS, charms: placed }, null, 1) + "\n");
  const srcMeta = (() => { try { return JSON.parse(fs.readFileSync(path.join(a.src, "letters.json"), "utf8")).letters; } catch (_) { return null; } })();
  // the cut size of every charm, from its own file (the outline's box): the sources and the 52 new ones
  const cut = new Map();
  const boxOf = async f => { const b = fs.readFileSync(f), pr = await P.parseSource(new Uint8Array(b.buffer, b.byteOffset, b.byteLength), path.basename(f)), c = P.groupCharms(pr, { minPt: 6 }).charms.filter(x => x.mergedInto == null)[0], bb = bboxOf(c.outline.subpaths.map(sp => V.flatten(sp).points)); return [bb[2] - bb[0], bb[3] - bb[1]]; };
  for (let n = 0; n < 26; n++) cut.set(`LETTER_EARRING-${n}`, [letters[n].bbox[2] - letters[n].bbox[0], letters[n].bbox[3] - letters[n].bbox[1]]);
  for (const e of rec.entries) cut.set(e.sku, await boxOf(path.join(a.out, "files", e.aiPath)));
  if (srcMeta && srcMeta.length === 26) fs.writeFileSync(path.join(a.out, "contact-sheet.html"), contactSheet(rec, placed, srcMeta, cut));
  return { rep, placed, sheet, rec };
}
module.exports = { main, placeHoop, HOOPS, LETTERS, intervalsAt, SIZES, sizeScale, scaleLetter };
if (require.main === module) main(process.argv).catch(e => { console.error("make-letter-hoops:", e.message); process.exit(1); });
