/*  charm-nest-pair.js — pairs, mismatched pairs and multi-piece orders: ONE shared definition.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul (9 Oct 2026): some earrings are sold mismatched (a left charm and a different right charm under one listing:
 *  MISMATCHED_7134 = MITTENS 1 + MITTENS 2), some orders are matching pairs, some are n discs. Every step of the app must
 *  know a line makes several PIECES, which of them are the left and the right ear, and that they travel together.
 *  This file is the single place that says so; pages, workers, the solver and the server all read it.
 *
 *  Loads three ways, one source:  window.CharmNestPair (page, or a worker after importScripts("charm-nest-pair.js"))
 *                                 module.exports       (node tests and Netlify functions: require("../../charm-nest-pair.js"))
 *  Pure data in, plain data out. The geometry it reads (outline, members) is the charm the page already holds; it needs
 *  charm-nest-geom.js only to tell a cut line from engraving (looked up lazily, with a built-in fallback), nothing else.
 *
 *  Words (plan.md): PIECE = one physical cut charm of an order line · GROUP = every piece of one order line (receipt id +
 *  transaction id) · MATCHING PAIR = two pieces of one design · MISMATCHED PAIR = the line's design draws TWO different
 *  bodies, so its two pieces are different charms, side "L" (left ear) and "R" (right ear), left to right in the drawing.
 *
 *    bodiesOf(charm)            → [{ index, outline, bbox, area, members, outlineBbox, side }]  separate cut bodies, left to right
 *    isMismatched(entryOrCharm) → two bodies that are not the same charm
 *    sideOf(index, count)       → "L" | "R" | null          sideLabel(side) → "Left" | "Right" | ""
 *    groupKey(rowOrPiece)       → "receiptId:transactionId" (also read from a pool id "receipt_transaction_copy")
 *    piecesFor(line, charm)     → [{ side, bodyIndex, groupKey, n, of }]
 *    kindOf(line, charm)        → "single" | "pair" | "mismatched" | "multi"
 *    mustShareSheet(a, b)       → true when two different pieces are of one group
 *  Added to the contract (never renamed): describe(charm), sameBody(a, b), sideForPiece(piece, bodies), pieceFields(line, charm),
 *  groupOf(pieces), siblingsOf(piece, pieces), splitAcross(pieces, sheetOf), designPair(entry), pieceCountOf(line, charm), discsOf(line).
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.CharmNestPair = factory();
})(typeof self !== "undefined" ? self : this, function () {
  "use strict";

  /* ═══ 1 · small geometry (self-contained: flatten, containment, distance) ═══ */
  const BODY_MIN_PT = 6;          // a body is at least this wide and tall (groupCharms' minPt)
  const RING_MAX_PT = 13;         // a closed cut path this small beside a body is its hoop or jump ring (groupCharms' ringMaxPt)
  const RING_NEAR_PT = 6;         // ...when it touches or sits this close to the body (groupCharms' nearPt)
  const SECOND_BODY_MIN_RATIO = 0.25;   // a second body is at least this share of the biggest body's area (smaller is a sample or a detail)

  const geom = () => {
    if (typeof self !== "undefined" && self.CharmNestGeom) return self.CharmNestGeom;
    if (typeof window !== "undefined" && window.CharmNestGeom) return window.CharmNestGeom;
    try { if (typeof require === "function") return require("./charm-nest-geom.js"); } catch (_) { /* fallback below */ }
    return null;
  };

  function flatten(seg, steps) {
    steps = steps || 8; const polys = [];
    for (const sub of (seg && seg.subpaths) || []) {
      let poly = [], cur = null;
      for (const sg of sub) {
        if (sg[0] === "m") { if (poly.length > 1) polys.push(poly); poly = [sg[1]]; cur = sg[1]; }
        else if (sg[0] === "l") { poly.push(sg[1]); cur = sg[1]; }
        else if (sg[0] === "c" && cur) { const a = sg[1], b = sg[2], c = sg[3]; for (let i = 1; i <= steps; i++) { const t = i / steps, u = 1 - t; poly.push([u * u * u * cur[0] + 3 * u * u * t * a[0] + 3 * u * t * t * b[0] + t * t * t * c[0], u * u * u * cur[1] + 3 * u * u * t * a[1] + 3 * u * t * t * b[1] + t * t * t * c[1]]); } cur = c; }
      }
      if (poly.length > 1) polys.push(poly);
    }
    return polys;
  }
  function inPolys(x, y, polys) {   // even-odd across all closed subpaths
    let inside = false;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const xi = poly[i][0], yi = poly[i][1], xj = poly[j][0], yj = poly[j][1];
      if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) inside = !inside;
    }
    return inside;
  }
  function distPolys(x, y, polys) {
    let best = Infinity;
    for (const poly of polys) for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
      const ax = poly[j][0], ay = poly[j][1], bx = poly[i][0], by = poly[i][1], dx = bx - ax, dy = by - ay, L = dx * dx + dy * dy;
      const t = L ? Math.max(0, Math.min(1, ((x - ax) * dx + (y - ay) * dy) / L)) : 0, px = ax + t * dx - x, py = ay + t * dy - y, d = px * px + py * py;
      if (d < best) best = d;
    }
    return Math.sqrt(best);
  }
  function areaOfPolys(polys) {   // sum of the subpaths' absolute areas (a cut outline is one subpath; a hole subpath is subtracted by even-odd below)
    let outer = 0, holes = 0;
    const list = polys.map(p => { let a = 0; for (let i = 0, j = p.length - 1; i < p.length; j = i++) a += p[j][0] * p[i][1] - p[i][0] * p[j][1]; return { p, a: Math.abs(a) / 2 }; }).sort((x, y) => y.a - x.a);
    for (const e of list) {   // nesting depth by one interior point: even depth adds, odd depth subtracts
      let depth = 0; const pt = e.p[0];
      for (const o of list) if (o !== e && o.a > e.a && inPolys(pt[0], pt[1], [o.p])) depth++;
      if (depth % 2) holes += e.a; else outer += e.a;
    }
    return Math.max(0, outer - holes);
  }
  const bbArea = b => Math.max(0, b[2] - b[0]) * Math.max(0, b[3] - b[1]);
  const bbUnion = (a, b) => !a ? (b ? b.slice() : null) : !b ? a.slice() : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])];
  const bbNear = (a, b, g) => !(a[2] + g < b[0] || b[2] + g < a[0] || a[3] + g < b[1] || b[3] + g < a[1]);
  const sampleOf = (seg, polys) => {   // up to ~40 points that stand for a segment
    if (seg.kind === "path" && polys.length) { const pts = polys.flat(); if (pts.length > 40) { const step = Math.ceil(pts.length / 40); return pts.filter((_, i) => i % step === 0); } return pts; }
    const b = seg.bbox; return b ? [[(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], [b[0], b[1]], [b[2], b[1]], [b[2], b[3]], [b[0], b[3]]] : [];
  };

  /* A closed cut path, as the grouping reads one. The geometry module owns the rule (layer, role, colour); the fallback is its rule. */
  const hatchBlue = c => !!c && c.length >= 3 && c[2] >= 0.5 && c[2] - Math.max(c[0], c[1]) >= 0.4;
  const achromatic = c => !!c && (Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2])) <= 0.15;
  function roleOf(m) {
    const role = String((m && m.manufacturingRole) || "").toLowerCase();
    if (["cut", "cutout", "outline"].includes(role)) return "cut";
    if (["engrave", "hatch", "artwork"].includes(role)) return "artwork";
    if (m && m.kind === "path" && m.fill && !m.stroke && hatchBlue(m.fillRGB)) return "artwork";
    const layer = String((m && m.layer) || "").trim();
    if (/^(?:engrave|engraving|hatch|front detail)(?:$|[\s:_()\/-])/i.test(layer)) return "artwork";
    if (/^(?:cut|cutout|cut-out|cutline|cut line)(?:$|[\s:_()\/-])/i.test(layer)) return "cut";
    return null;
  }
  function isCut(m) {
    const G = geom();
    if (G && G.isCutLine) return !!G.isCutLine(m);
    return !!m && m.kind === "path" && !!m.closed && roleOf(m) !== "artwork" && (roleOf(m) === "cut" ? !!(m.stroke || m.fill) : !!m.stroke && achromatic(m.strokeRGB));
  }

  /* ═══ 2 · bodies of a charm ════════════════════════════════════════════
     A charm is the outline groupCharms chose plus every segment assigned to it. A design that draws two charm bodies under one
     label reaches the app as one glued charm (the second body's outline and its ink are members of the first: charm-nest-bridge
     readMasterCharm folds a per-SKU file that splits into several charms back into one). So the bodies are read back from the
     members: the outline is body 0's seed; a closed cut path that no body holds and that is no hoop beside one is another body. */
  const cache = typeof WeakMap === "function" ? new WeakMap() : null;

  function bodyRecord(outline, members, polys) {
    const ob = (outline.bbox || bbOfPolys(polys)).slice();
    return { outline, polys, outlineBbox: ob, bbox: ob.slice(), area: areaOfPolys(polys), members: [outline] };
  }
  function bbOfPolys(polys) { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const p of polys) for (const q of p) { if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; } return x0 === Infinity ? [0, 0, 0, 0] : [x0, y0, x1, y1]; }

  function computeBodies(charm, opts) {
    opts = opts || {};
    const outline = charm && charm.outline;
    if (!outline) return [];
    const members = Array.isArray(charm.members) && charm.members.length ? charm.members : [outline];
    const first = bodyRecord(outline, members, flatten(outline, 8));
    // candidate second bodies: closed cut paths of body size that are not the outline itself
    const seeds = [];
    for (const m of members) {
      if (m === outline || !m.bbox || m.kind !== "path" || !m.closed || !isCut(m)) continue;
      if ((m.bbox[2] - m.bbox[0]) < BODY_MIN_PT || (m.bbox[3] - m.bbox[1]) < BODY_MIN_PT) continue;
      seeds.push(m);
    }
    const bodies = [first];
    if (seeds.length) {
      seeds.sort((a, b) => bbArea(b.bbox) - bbArea(a.bbox));   // biggest first, so a hole inside a body is met after the body that holds it
      const polysOf = new Map();
      const pl = s => { let p = polysOf.get(s); if (!p) { p = flatten(s, 8); polysOf.set(s, p); } return p; };
      const all = [first];   // every accepted body, plus the biggest seeds are tried against them
      // the biggest closed cut path may be bigger than the chosen outline (a custom outline): it competes on area, not on being first
      for (const s of seeds) {
        const polys = pl(s), pts = sampleOf(s, polys);
        const sameBox = b => { const iw = Math.min(s.bbox[2], b.outlineBbox[2]) - Math.max(s.bbox[0], b.outlineBbox[0]), ih = Math.min(s.bbox[3], b.outlineBbox[3]) - Math.max(s.bbox[1], b.outlineBbox[1]); return iw > 0 && ih > 0 && iw * ih >= 0.85 * (bbArea(s.bbox) + bbArea(b.outlineBbox) - iw * ih); };   // the same path drawn twice (a stroked outline and its twin)
        const holder = all.find(b => sameBox(b) || (bbNear(s.bbox, b.bbox, 0) && pts.length && pts.filter(p => inPolys(p[0], p[1], b.polys)).length / pts.length >= 0.6));
        if (holder) continue;                                  // a cut-out or inner ring of a body
        const maxDim = Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]);
        const ringLike = maxDim <= RING_MAX_PT && (s.subpaths || []).length <= 2 && (s.subpaths || []).every(sp => sp.length <= 20);
        if (ringLike && all.some(b => bbNear(s.bbox, b.bbox, RING_NEAR_PT + 1) && Math.min(...pts.map(p => distPolys(p[0], p[1], b.polys))) <= RING_NEAR_PT + (s.lwPt || 0) / 2 + (b.outline.lwPt || 0) / 2)) continue;   // a hoop beside a body is part of it
        // The bodies of one design stand SIDE BY SIDE, as the master draws a pair (the same row rule masterPairs uses: they overlap in height). A closed cut path drawn above or below a body
        // (a second bar of a bracelet, a plate under a charm) is part of that one body, not a second charm: without this the reader's nearly-closed-loop repair turned four plain bar designs into "pairs".
        const vOv = b => Math.min(s.bbox[3], b.outlineBbox[3]) - Math.max(s.bbox[1], b.outlineBbox[1]);
        if (!all.some(b => vOv(b) >= PAIR_DEFAULTS.vo * Math.min(s.bbox[3] - s.bbox[1], b.outlineBbox[3] - b.outlineBbox[1]))) continue;
        const rec = bodyRecord(s, members, polys);
        all.push(rec);
      }
      for (let i = 1; i < all.length; i++) bodies.push(all[i]);
    }
    // a second body must be a body, not a sample or a detail: at least a quarter of the biggest one
    let keep = bodies;
    if (bodies.length > 1) { const big = Math.max(...bodies.map(b => b.area || bbArea(b.outlineBbox))); keep = bodies.filter(b => (b.area || bbArea(b.outlineBbox)) >= SECOND_BODY_MIN_RATIO * big); if (!keep.length) keep = [bodies[0]]; }
    // every other member belongs to the body that holds most of its points, else the nearest (a one-body charm keeps everything)
    if (keep.length === 1) { keep[0].members = members.slice(); for (const m of members) if (m.bbox) keep[0].bbox = bbUnion(keep[0].bbox, m.bbox); }
    else {
      const owner = new Map(keep.map(b => [b.outline, b]));
      for (const m of members) {
        if (owner.has(m)) continue;
        const polys = m.kind === "path" ? flatten(m, 4) : [], pts = sampleOf(m, polys);
        let best = null, bestF = -1, bestD = Infinity;
        for (const b of keep) {
          const f = pts.length ? pts.filter(p => inPolys(p[0], p[1], b.polys)).length / pts.length : 0;
          const d = f >= 0.5 ? 0 : Math.min(...pts.map(p => distPolys(p[0], p[1], b.polys)));
          if (f > bestF + 1e-9 || (Math.abs(f - bestF) <= 1e-9 && d < bestD)) { best = b; bestF = f; bestD = d; }
        }
        if (best) { best.members.push(m); if (m.bbox) best.bbox = bbUnion(best.bbox, m.bbox); }
      }
    }
    keep.sort((a, b) => ((a.outlineBbox[0] + a.outlineBbox[2]) - (b.outlineBbox[0] + b.outlineBbox[2])) || ((b.outlineBbox[1] + b.outlineBbox[3]) - (a.outlineBbox[1] + a.outlineBbox[3])));
    const n = keep.length;
    return keep.map((b, i) => ({ index: i, outline: b.outline, bbox: b.bbox, area: b.area, members: b.members, outlineBbox: b.outlineBbox, side: sideOf(i, n) }));
  }

  /** The charm's separate cut bodies, left to right by their x position in the drawing. A normal charm (a hoop or jump ring
   *  welded to its body is ONE body) gives length 1; a design that draws two charm bodies under one label gives 2. */
  function bodiesOf(charm) {
    if (!charm || typeof charm !== "object") return [];
    if (!charm.outline) {   // a record with no geometry: one body from its box, if it has one
      const b = charm.bbox || (charm.widthPt != null ? [0, 0, +charm.widthPt || 0, +charm.heightPt || 0] : null);
      return b ? [{ index: 0, outline: null, bbox: b.slice(), area: +charm.areaPt2 || bbArea(b), members: [], outlineBbox: b.slice(), side: null }] : [];
    }
    const sig = (charm.members ? charm.members.length : 0) + ":" + (charm.bbox ? charm.bbox.join(",") : "");
    if (cache) { const hit = cache.get(charm); if (hit && hit.sig === sig) return hit.bodies; }
    const bodies = computeBodies(charm);
    if (cache) cache.set(charm, { sig, bodies });
    return bodies;
  }

  /* ═══ 3 · the same charm twice, or two different charms ═════════════════
     MITTENS 1 and MITTENS 2 have the same cut body (same area to the hundredth) and different engraving: they ARE two charms.
     Two bodies are "the same" only when the cut line AND the ink on it agree. */
  function inkSignature(body) {
    const sig = { n: 0, area: 0, layers: {} };
    for (const m of body.members || []) {
      if (m === body.outline || !m.bbox) continue;
      sig.n++; sig.area += bbArea(m.bbox);
      const key = (String(m.layer || "") + "|" + (m.fill && m.fillRGB ? m.fillRGB.map(v => Math.round(v * 4)).join("") : "-") + "|" + (m.stroke && m.strokeRGB ? m.strokeRGB.map(v => Math.round(v * 4)).join("") : "-")).toLowerCase();
      sig.layers[key] = (sig.layers[key] || 0) + 1;
    }
    return sig;
  }
  /** Do two bodies draw the same charm? (same cut shape within 2 percent AND the same ink.) */
  function sameBody(a, b) {
    if (!a || !b) return false;
    const ar = a.area || bbArea(a.outlineBbox), br = b.area || bbArea(b.outlineBbox);
    if (Math.abs(ar - br) > 0.02 * Math.max(ar, br, 1e-6)) return false;
    const aw = a.outlineBbox[2] - a.outlineBbox[0], ah = a.outlineBbox[3] - a.outlineBbox[1], bw = b.outlineBbox[2] - b.outlineBbox[0], bh = b.outlineBbox[3] - b.outlineBbox[1];
    if (Math.abs(aw - bw) > 0.02 * Math.max(aw, bw) + 0.1 || Math.abs(ah - bh) > 0.02 * Math.max(ah, bh) + 0.1) return false;
    // the cut line: every sampled point of one lies on the other once their boxes are laid over each other
    if (a.outline && b.outline) {
      const pa = flatten(a.outline, 6), pb = flatten(b.outline, 6), dx = a.outlineBbox[0] - b.outlineBbox[0], dy = a.outlineBbox[1] - b.outlineBbox[1];
      const shifted = pb.map(p => p.map(q => [q[0] + dx, q[1] + dy])), tol = 0.03 * Math.max(aw, ah) + 0.15;
      const off = (from, onto) => { const pts = sampleOf({ kind: "path" }, from); return pts.some(p => distPolys(p[0], p[1], onto) > tol); };
      if (off(pa, shifted) || off(shifted, pa)) return false;
    }
    const sa = inkSignature(a), sb = inkSignature(b);
    if (sa.n !== sb.n) return false;
    if (Math.abs(sa.area - sb.area) > 0.05 * Math.max(sa.area, sb.area, 1e-6)) return false;
    const ka = Object.keys(sa.layers), kb = Object.keys(sb.layers);
    if (ka.length !== kb.length || ka.some(k => sa.layers[k] !== sb.layers[k])) return false;
    return true;
  }
  /** What the charm is, with the reason: { count, bodies, mismatched, same, why }. */
  function describe(charm) {
    const bodies = bodiesOf(charm), n = bodies.length;
    if (n < 2) return { count: n, bodies, mismatched: false, same: false, why: n ? "one body" : "no body" };
    if (n === 2) { const same = sameBody(bodies[0], bodies[1]); return { count: 2, bodies, mismatched: !same, same, why: same ? "two identical bodies (a matching pair drawn twice)" : "two different bodies" }; }
    return { count: n, bodies, mismatched: false, same: false, many: true, why: n + " bodies" };
  }
  /** True when the design draws two bodies that are not the same charm. Takes a charm, or a master index entry carrying `pair`. */
  function isMismatched(x) {
    if (!x || typeof x !== "object") return false;
    if (x.pair && typeof x.pair === "object" && !x.outline) return !!x.pair.mismatched && (+x.pair.bodies || 2) === 2;
    return describe(x).mismatched;
  }

  /* ═══ 4 · sides, groups, pieces ═══════════════════════════════════════ */
  const sideOf = (index, count) => (count === 2 ? (index === 0 ? "L" : index === 1 ? "R" : null) : null);
  const sideLabel = side => (side === "L" ? "Left" : side === "R" ? "Right" : "");

  const POOL_ID = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;   // receiptId_transactionId_copy (Orders.poolId)
  const val = v => (v == null ? "" : String(v));
  /** receiptId:transactionId, shared by every piece of one order line. Reads a pool row, a placement, a piece, an order line, or a pool id string. */
  function groupKey(x) {
    if (x == null) return ":";
    if (typeof x === "string" || typeof x === "number") { const m = POOL_ID.exec(String(x)); return m ? m[1] + ":" + m[2] : String(x).indexOf(":") > 0 ? String(x) : ":"; }
    if (x.groupKey && String(x.groupKey) !== ":") return String(x.groupKey);
    const o = x.order || {}, l = x.line || {}, oi = x.orderInfo || {};
    const rid = x.receiptId ?? x.receipt_id ?? x.orderId ?? x.rid ?? oi.receiptId ?? o.receiptId ?? o.receipt_id ?? l.receiptId ?? l.receipt_id;
    const tid = x.transactionId ?? x.transaction_id ?? x.txId ?? oi.transactionId ?? l.transactionId ?? l.transaction_id ?? (x.row && x.row.line && x.row.line.transactionId);
    if (rid != null && rid !== "" && rid !== "—") return val(rid) + ":" + val(tid);
    const id = x.poolId || x.id; const m = id && POOL_ID.exec(String(id));
    if (m) return m[1] + ":" + m[2];
    return ":";
  }
  const keyOk = k => typeof k === "string" && k.length > 1 && !/^:/.test(k);

  const num = v => (Number.isFinite(+v) && +v > 0 ? Math.floor(+v) : 0);
  const quantityOf = line => Math.max(1, Math.round(+(line && ((line.spec && line.spec.quantity) || line.quantity || line.qty || (line.row && line.row.spec && line.row.spec.quantity))) || 1));
  /** n for a disc necklace ("3 discs"), or 0: INFORMATION ONLY (the app counts no more pieces for it today; see pieceCountOf). Read from an explicit count on the line, else from the listing text and variations. */
  function discsOf(line) {
    if (!line) return 0;
    const e = num(line.discs || line.discCount || (line.spec && (line.spec.discs || line.spec.discCount)));
    if (e) return e;
    const texts = [line.title, line.sku, line.variation, line.variations && JSON.stringify(line.variations), line.spec && line.spec.variation].filter(Boolean).join(" ");
    const m = /(\d{1,2})\s*(?:x\s*)?(?:discs?|disks?|circles?)\b/i.exec(texts) || /\b(?:discs?|disks?)\s*[:\-x]\s*(\d{1,2})\b/i.exec(texts);
    return m ? +m[1] : 0;
  }
  /* Earring pairs (Paul, 9 Oct 18:46-18:47): every earring pair (stud, hoop, huggie, matching or mismatched) is one LEFT and one RIGHT piece per
     unit of quantity, side "L"/"R" on every piece; necklace discs, letters, charms and a single earring make pieces with side null and are never
     mirrored. The intake says what a line is (spec.pair.kind, spec.pieceCount); a line without them is read by its form. */
  const PAIR_FORMS = new Set(["earrings", "earring", "pair", "pair of earrings", "stud", "studs", "stud earrings", "hoop", "hoops", "hoop earrings", "huggie", "huggies", "huggie earrings", "huggie hoops", "huggie charm set"]);
  const formOf = line => String((line && ((line.spec && line.spec.form) || line.form || (line.row && line.row.spec && line.row.spec.form))) || "").toLowerCase().trim();
  const pairSpecOf = line => line && ((line.spec && line.spec.pair) || line.pair || (line.row && line.row.spec && line.row.spec.pair)) || null;
  /** Is this line an earring PAIR (its pieces are a Left and a Right)? The intake's own answer wins (spec.pair.earring, which is true only for an earring pair line
   *  that is not a mismatched design counted as one glued copy and not an old line pinned to the pieces it already has); else spec.pair.kind; else a mismatched
   *  design is; else the form decides. */
  function isEarringPair(line, charm) {
    const pr = pairSpecOf(line);
    if (pr && typeof pr.earring === "boolean") return pr.earring === true && !pr.glued && !pr.legacy;
    if (pr && pr.kind) return pr.kind === "pair" || pr.kind === "mismatched";
    if (charm && isMismatched(charm)) return true;
    return PAIR_FORMS.has(formOf(line));
  }
  /** The side the intake gave each piece, in order (spec.pair.sides: "L" | "R" | null each, a flat array), or null when the line carries none. */
  function sidesSaid(line) {
    const pr = pairSpecOf(line);
    return pr && Array.isArray(pr.sides) && pr.sides.length ? pr.sides.map(x => x === "L" || x === "R" ? x : null) : null;
  }
  /** How many pieces one order line makes. An explicit count wins (the intake sets spec.pieceCount: one source of truth in charm-nest-orders.js; a
   *  row's pool ids are the fact). Without one: an earring pair (or a mismatched design) makes two per unit, anything else one per unit. */
  function pieceCountOf(line, charm) {
    const explicit = num(line && (line.pieceCount || (line.spec && line.spec.pieceCount) || (line.row && line.row.spec && line.row.spec.pieceCount))) ||
      (line && Array.isArray(line.poolIds) && line.poolIds.length) || (line && line.row && Array.isArray(line.row.poolIds) && line.row.poolIds.length) || 0;
    if (explicit) return explicit;
    const q = quantityOf(line), form = formOf(line);
    if (form === "earring-single" || form === "single earring" || form === "single") return q;   // one ear alone: one piece per unit
    return isEarringPair(line, charm) ? 2 * q : q;
  }
  /** The pieces one order line makes, in order: [{ side, bodyIndex, groupKey, n, of, mirror }]. An earring pair alternates Left, Right (L first) and every
   *  piece has its side; a mismatched design's Left is its left body (bodyIndex 0) and its Right its right body (bodyIndex 1), a matching pair's pieces
   *  are both bodyIndex 0. `mirror` is true for a piece that is the mirror image of the as-drawn design (facingOf decides which of the two that is).
   *  Anything that is not an earring pair (a necklace of discs, a single earring): side null, mirror false. */
  function piecesFor(line, charm, opts) {
    const key = groupKey(line), total = pieceCountOf(line, charm), pr = pairSpecOf(line), out = [];
    // a mismatched design (by its geometry, or because the intake says so) is two bodies, unless the line is one glued copy per unit or an old line pinned to the pieces it had
    const mis = !(pr && (pr.glued || pr.legacy)) && ((!!charm && isMismatched(charm)) || !!(pr && pr.mismatched === true));
    const pair = total >= 2 && isEarringPair(line, charm);
    let said = sidesSaid(line);   // (the intake's own sides win; without them the pieces alternate L, R)
    if (!said && total === 1 && pr && pr.single && (pr.sideSaid === "L" || pr.sideSaid === "R")) said = [pr.sideSaid];   // a single earring that names its ear
    const bodies = mis && charm ? bodiesOf(charm) : null, byBody = !!bodies && bodies.length >= 2;
    for (let i = 0; i < total; i++) {
      const side = said && i < said.length ? said[i] : pair ? (i % 2 === 0 ? "L" : "R") : null;
      const bodyIndex = mis ? (side === "R" ? 1 : side === "L" ? 0 : i % 2) : 0;
      let mirror = false;
      if (side && (opts && opts.facing || !readsOneWay(charm))) { const f = (opts && opts.facing) || (byBody ? facingOfBody(bodies[Math.min(bodyIndex, bodies.length - 1)], charm) : mis ? facingSetFor({ index: bodyIndex }, charm) : facingOf(charm)); mirror = side !== (f || "L"); }   // (an index entry with no geometry: the words it holds for each body)   // (a design that reads one way, letters and numbers, is cut as drawn on both sides)
      out.push({ side, bodyIndex, groupKey: key, n: i + 1, of: total, mirror });
    }
    return out;
  }
  /** single (one piece) | pair (two pieces of one design) | mismatched (a left and a right body) | multi (three or more pieces, or several pairs). */
  function kindOf(line, charm) {
    const pr = pairSpecOf(line);
    const total = pieceCountOf(line, charm);
    if (pr && (typeof pr.mismatched === "boolean" || typeof pr.earring === "boolean")) {   // the intake's facts: the same rule as charm-nest-orders.js kindFor
      if (total < 2) return pr.mismatched ? "mismatched" : "single";
      if (pr.earring && !pr.glued && !pr.legacy && total === 2) return pr.mismatched ? "mismatched" : "pair";
      return "multi";
    }
    if (pr && /^(single|pair|mismatched|multi)$/.test(pr.kind || "")) return pr.kind;   // the intake's own answer
    const mis = !!charm && isMismatched(charm);
    if (mis) return total === 2 ? "mismatched" : total <= 1 ? "mismatched" : "multi";   // (one glued copy of a mismatched design is still the mismatched kind)
    if (total <= 1) return "single";
    return total === 2 ? "pair" : "multi";
  }
  /** True when two different pieces are in one group (one order line): they must be on the same sheet and the same metal, and
   *  anything that moves, removes or completes one must know about the other. */
  function mustShareSheet(a, b) {
    if (!a || !b || a === b) return false;
    const ka = groupKey(a), kb = groupKey(b);
    if (!keyOk(ka) || ka !== kb) return false;
    const ida = a.poolId || a.id || (typeof a === "string" ? a : null), idb = b.poolId || b.id || (typeof b === "string" ? b : null);
    return !(ida != null && ida === idb);
  }

  /* ═══ 5 · helpers for the callers (added; the names above are the contract) ═══ */
  /** The side a stored piece has: its own `side`, else derived from its body index and the design's bodies, else null. */
  function sideForPiece(piece, bodiesOrCount) {
    if (!piece) return null;
    if (piece.side === "L" || piece.side === "R") return piece.side;
    const count = Array.isArray(bodiesOrCount) ? bodiesOrCount.length : +bodiesOrCount || 0;
    if (piece.bodyIndex == null || count !== 2) return null;
    return sideOf(+piece.bodyIndex, count);
  }
  /** The piece fields (side, bodyIndex, groupKey, groupSize) for copy `copy` (1-based) of a line, or for a stored piece that has none: derived, never stored here. */
  function pieceFields(line, charm, copy) {
    const list = piecesFor(line, charm), p = list[Math.max(0, (+copy || 1) - 1)] || list[0] || { side: null, bodyIndex: 0, groupKey: groupKey(line), of: 1 };
    return { side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: p.of };
  }
  /** Group a flat list of pieces (anything groupKey reads) by their line: Map(groupKey → [piece]). Pieces with no usable key stay alone under their own id. */
  function groupOf(pieces) {
    const m = new Map();
    for (const p of pieces || []) { let k = groupKey(p); if (!keyOk(k)) k = "alone:" + (p && (p.poolId || p.id) || m.size); if (!m.has(k)) m.set(k, []); m.get(k).push(p); }
    return m;
  }
  /** The other pieces of this piece's group, from a list. */
  function siblingsOf(piece, pieces) { const k = groupKey(piece); return keyOk(k) ? (pieces || []).filter(p => p !== piece && groupKey(p) === k) : []; }
  /** Given pieces and a function giving each one's sheet id (or null), the groups whose pieces are on more than one sheet (or on a sheet and off every sheet):
   *  [{ groupKey, pieces, sheets:[sheetId], unplaced:[piece] }]. The R3 question every set builder and remover asks. */
  function splitAcross(pieces, sheetOf) {
    const out = [];
    for (const [k, list] of groupOf(pieces)) {
      if (!keyOk(k) || list.length < 2) continue;
      const sheets = [...new Set(list.map(p => sheetOf(p)).filter(Boolean))], unplaced = list.filter(p => !sheetOf(p));
      if (sheets.length > 1 || (sheets.length === 1 && unplaced.length)) out.push({ groupKey: k, pieces: list, sheets, unplaced });
    }
    return out;
  }
  /** The `pair` field of a master index entry, or null (a normal design). */
  const designPair = entry => (entry && entry.pair && typeof entry.pair === "object" && +entry.pair.bodies > 1 ? { v: 1, bodies: +entry.pair.bodies, mismatched: !!entry.pair.mismatched } : null);

  /* ═══ 5b · direction: Left and Right earrings are MIRROR images (Paul, 9 Oct 18:47) ═══════════════════════════════
     "each pair contains a left and a right earring so they're not two identical charms: one is flipped on a vertical Y axis 180 degrees so that the
     left one face is left and the right one face is right." The Right piece is the Left piece with x -> -x about its own box centre (cut outline,
     holes, hoop, engraving art and hatching; engraved TEXT stays readable and is placed, not reversed, by the engraving code). The nester may
     rotate a piece, never reflect it: a Right piece reaches the solver already mirrored. This is NOT the sheet "flip" used to engrave the back. */

  /* WHICH WAY DOES A DESIGN FACE? (PAIRMIRROR, 9 Oct; evidence in plans/pairs-1009/PAIRMIRROR-facing.json and PAIRMIRROR-points.md)
     Measured on the live library (3,843 design files, 6,827 SKUs), by eye on about 100 designs: the masters follow NO drawing convention. Profile animals
     face left about as often as right (panda, wolf, duck, fox, iguana face left; horse, seal, beaver, penguin, boot, dragon face right), mittens are drawn
     thumb-left, and no shape measure (centre of mass, where the ink sits, which end is heavier) predicts the side better than a coin flip (the old centre-of-mass
     rule was right for 12 of 24 labelled designs). A shape cannot say which end is the head. So:
       1. whether a design NEEDS a facing is measured (it is, when its mirror image differs: symmetryOf), and that is sound;
       2. which way it faces is a person's word (`facing` on the master record, set in the Master tab; a re-index never overwrites it);
       3. a second body of a mismatched pair is read from the first (drawn as the mirror image of it: it faces the other way; drawn the same: the same way);
       4. everything else is UNKNOWN (null), and unknown means as drawn is the Left and the Right is the mirror (the way Paul's picture draws both mittens).
     Nothing here guesses a side from a shape. */
  const SYM_N = 64, SYM_CUT_OK = 0.04, SYM_CUT_DIR = 0.065, SYM_ART_OK = 0.10, SYM_ART_DIR = 0.14;   // (calibrated on 3,833 live designs: of 50 labelled by eye, 14 of 14 left-facing and 10 of 12 right-facing read directional, 38 of 50 symmetric ones read symmetric)
  const symCache = typeof WeakMap === "function" ? new WeakMap() : null;

  function rasterPolys(polys, k, cx, cy, N) {   // even-odd fill of closed polygons into an N x N mask, y up, the square k centred on (cx, cy)
    const m = new Uint8Array(N * N);
    for (let j = 0; j < N; j++) for (let i = 0; i < N; i++) { const x = cx + ((i + 0.5) / N - 0.5) * k, y = cy + (0.5 - (j + 0.5) / N) * k; if (inPolys(x, y, polys)) m[j * N + i] = 1; }
    return m;
  }
  function rasterStroke(polylines, lw, k, cx, cy, N, m) {   // a line of width lw into the mask
    const cell = k / N, r = Math.max(0.5, lw / 2 / cell);
    for (const pl of polylines) for (let i = 1; i < pl.length; i++) {
      const a = pl[i - 1], b = pl[i], L = Math.hypot(b[0] - a[0], b[1] - a[1]), st = Math.max(1, Math.ceil(L / (cell * 0.5)));
      for (let s = 0; s <= st; s++) {
        const t = s / st, gi = (a[0] + (b[0] - a[0]) * t - cx) / k * N + N / 2, gj = N / 2 - (a[1] + (b[1] - a[1]) * t - cy) / k * N;
        for (let jj = Math.max(0, Math.floor(gj - r)); jj <= Math.min(N - 1, Math.ceil(gj + r)); jj++) for (let ii = Math.max(0, Math.floor(gi - r)); ii <= Math.min(N - 1, Math.ceil(gi + r)); ii++) if ((ii + 0.5 - gi) ** 2 + (jj + 0.5 - gj) ** 2 <= r * r + 0.25) m[jj * N + ii] = 1;
      }
    }
  }
  const flipRows = (m, N) => { const o = new Uint8Array(N * N); for (let j = 0; j < N; j++) for (let i = 0; i < N; i++) o[j * N + i] = m[j * N + N - 1 - i]; return o; };
  function fillHolesMask(m, N) {   // everything the outside cannot reach is part of the shape (a hoop's hole, a cut-out)
    const out = new Uint8Array(N * N), st = [], push = (i, j) => { if (i < 0 || j < 0 || i >= N || j >= N) return; const q = j * N + i; if (out[q] || m[q]) return; out[q] = 1; st.push(q); };
    for (let i = 0; i < N; i++) { push(i, 0); push(i, N - 1); push(0, i); push(N - 1, i); }
    while (st.length) { const q = st.pop(), i = q % N, j = (q - i) / N; push(i + 1, j); push(i - 1, j); push(i, j + 1); push(i, j - 1); }
    const r = new Uint8Array(N * N); for (let q = 0; q < N * N; q++) r[q] = out[q] ? 0 : 1; return r;
  }
  function edgeCells(m, N) { const e = new Uint8Array(N * N); for (let j = 0; j < N; j++) for (let i = 0; i < N; i++) { const q = j * N + i; if (m[q] && (i === 0 || j === 0 || i === N - 1 || j === N - 1 || !m[q - 1] || !m[q + 1] || !m[q - N] || !m[q + N])) e[q] = 1; } return e; }
  function distanceTo(m, N) {   // chamfer distance (in cells) from every cell to the nearest set cell
    const D = new Float32Array(N * N).fill(1e9); for (let q = 0; q < N * N; q++) if (m[q]) D[q] = 0;
    for (let j = 0; j < N; j++) for (let i = 0; i < N; i++) { const q = j * N + i; let v = D[q]; if (i > 0) v = Math.min(v, D[q - 1] + 1); if (j > 0) { v = Math.min(v, D[q - N] + 1); if (i > 0) v = Math.min(v, D[q - N - 1] + 1.414); if (i < N - 1) v = Math.min(v, D[q - N + 1] + 1.414); } D[q] = v; }
    for (let j = N - 1; j >= 0; j--) for (let i = N - 1; i >= 0; i--) { const q = j * N + i; let v = D[q]; if (i < N - 1) v = Math.min(v, D[q + 1] + 1); if (j < N - 1) { v = Math.min(v, D[q + N] + 1); if (i < N - 1) v = Math.min(v, D[q + N + 1] + 1.414); if (i > 0) v = Math.min(v, D[q + N - 1] + 1.414); } D[q] = v; }
    return D;
  }
  function chamferP90(A, B, N) {   // the distance within which 90 percent of A's cells find a cell of B (and the other way round, the larger of the two), as a share of the drawing's size
    const dB = distanceTo(B, N), dA = distanceTo(A, N), a = [], b = [];
    for (let q = 0; q < N * N; q++) { if (A[q]) a.push(dB[q]); if (B[q]) b.push(dA[q]); }
    const p90 = v => { if (!v.length) return 0; v.sort((x, y) => x - y); return v[Math.min(v.length - 1, Math.floor(0.9 * v.length))]; };
    return Math.max(p90(a), p90(b)) / N;
  }
  /** How far a body is from its own mirror image: { cut, art, level, N }. cut = the cut line's distance from its mirror image (shares of the drawing's size, holes filled),
   *  art = the engraving's (null when the body has none). level "symmetric" (the mirror image looks the same: nothing to decide), "directional" (it does not), "slight" (in between:
   *  a hoop off to one side, a hand-drawn wobble). The mirror axis is the one mirrorOf uses: the vertical through the middle of the body's box. */
  function symmetryOf(body) {
    if (!body || !body.outline) return { cut: 0, art: null, level: "symmetric", N: SYM_N, none: true };
    if (symCache) { const hit = symCache.get(body); if (hit) return hit; }
    const N = SYM_N, bb = body.bbox || body.outlineBbox || body.outline.bbox, cx = (bb[0] + bb[2]) / 2, cy = (bb[1] + bb[3]) / 2, k = Math.max(bb[2] - bb[0], bb[3] - bb[1], 1e-6) * 1.04;
    const S = fillHolesMask(rasterPolys(flatten(body.outline, 8), k, cx, cy, N), N), I = new Uint8Array(N * N);
    for (const m of body.members || []) {
      if (m === body.outline || m.kind !== "path" || !m.bbox || (isCut(m) && m.closed)) continue;   // a hole or a hoop belongs to the cut line, which S already holds
      const polys = flatten(m, 6);
      if (m.fill) { const f = rasterPolys(polys, k, cx, cy, N); for (let q = 0; q < I.length; q++) if (f[q]) I[q] = 1; }
      if (m.stroke || !m.fill) rasterStroke(polys, m.lwPt || 0.5, k, cx, cy, N, I);
    }
    let sN = 0, iN = 0; for (let q = 0; q < N * N; q++) { sN += S[q]; iN += I[q]; }
    const cut = chamferP90(edgeCells(S, N), edgeCells(flipRows(S, N), N), N);
    const art = iN > 0.01 * sN ? chamferP90(edgeCells(I, N), edgeCells(flipRows(I, N), N), N) : null;
    const level = cut >= SYM_CUT_DIR || (art != null && art >= SYM_ART_DIR) ? "directional" : cut < SYM_CUT_OK && (art == null || art < SYM_ART_OK) ? "symmetric" : "slight";
    const out = { cut: +cut.toFixed(3), art: art == null ? null : +art.toFixed(3), level, N };
    if (symCache) symCache.set(body, out);
    return out;
  }
  /** The side a stored value says ("L" | "R") or null. A person's "X" (words, letters, numbers: it reads one way) is not a side: see readsOneWay. */
  const facingValue = v => (v === "L" || v === "R" ? v : null);
  /* DESIGNS THAT READ ONE WAY (letters, numbers, scripture, words): a Right piece turned over would read backwards ("Engraved TEXT stays readable", Paul 18:47),
     so both pieces are cut as drawn (side still Left and Right, mirror false). A person says so with facing "X" on the record; by default a design whose SKU names it
     a letter, an initial, a number, an alphabet or scripture does (a narrow rule: "HEART LOVE LETTER" and "SWORD" do not). Lettering drawn INTO a picture (a SHERIFF
     badge, the N E S W of a compass) cannot be told from the drawing: a person sets "X" on those designs too. */
  const LETTERING = /(?:^|[\s_-])(?:LETTERS?|INITIALS?|ALPHABET|NUMBERS?|SCRIPTURE|SCRIPT)(?:$|[\s_\d(-])/i, NOT_LETTERING = /LOVE\s+LETTER|SWORD/i;
  const nameOf = x => String((x && (x.sku || (x.entry && x.entry.sku) || x.name)) || "");
  function readsOneWay(charmOrEntry) {
    if (!charmOrEntry || typeof charmOrEntry !== "object") return false;
    const rec = charmOrEntry.entry || charmOrEntry;
    if (rec.facing === "X" || charmOrEntry.facing === "X") return true;
    if (facingValue(rec.facing) || facingValue(charmOrEntry.facing)) return false;   // a person's side stands over the name
    const n = nameOf(charmOrEntry); return LETTERING.test(n) && !NOT_LETTERING.test(n);
  }
  const opposite = f => (f === "L" ? "R" : f === "R" ? "L" : null);
  /** The side a PERSON said this body faces, from the record (`facings[index]` of a mismatched record, or `facing`, which is body 0's), else null. */
  function facingSetFor(body, charmOrEntry) {
    const c = charmOrEntry || {}, rec = c.entry || c, idx = body && body.index != null ? body.index : 0;
    const per = Array.isArray(rec.facings) && rec.facings[idx] != null ? facingValue(rec.facings[idx]) : null;
    if (per) return per;
    return idx === 0 ? facingValue(rec.facing) || facingValue(c.facing) : null;
  }
  /** Which way ONE body of the design faces, read from what is known: "L" | "R" | null (unknown, or symmetric: as drawn is the Left, the Right is the mirror).
   *  A person's word (record `facing` / `facings[i]`) wins; else the second body of a mismatched pair from the first (drawn as its mirror image: it faces the
   *  other way, drawn the same way: the same way); else null. No shape measure is used to guess a side (see the note above). */
  function facingOfBody(body, charmOrEntry) {
    const set = facingSetFor(body, charmOrEntry); if (set) return set;
    return relationFacing(body, charmOrEntry);
  }
  const REL_IOU = 0.85, REL_MARGIN = 0.15;   // two bodies are "mirror images" (or "the same way") when that overlap is this high and clearly higher than the other (measured on the live pairs: 0.88 against 0.65 is a mirror pair)
  function relationFacing(body, charmOrEntry) {
    if (!body || !body.index || !charmOrEntry || !charmOrEntry.outline) return null;
    const bodies = bodiesOf(charmOrEntry); if (bodies.length !== 2 || !bodies[0] || bodies[0] === body) return null;
    const sim = shapeSimilarity(bodies[0], body), f0 = facingSetFor(bodies[0], charmOrEntry);
    if (sim.mirrored >= REL_IOU && sim.mirrored - sim.same >= REL_MARGIN) return opposite(f0 || "L");   // drawn as a pair of ears already: the second faces away from the first
    if (sim.same >= REL_IOU && sim.same - sim.mirrored >= REL_MARGIN && f0) return f0;   // drawn the same way as the first, and a person said which way that is
    return null;   // drawn the same way (or not alike) and nobody said: unknown, so the Left as drawn and the Right turned
  }
  /** What is known about the way a body faces: { facing, confidence 0..1, source, directional, level, symmetry:{cut,art}, why }. source: "person" | "mirror-of-first" | "unknown" | "symmetric".
   *  directional says the mirror image differs from the drawing (the design needs a facing); facing null with directional true is a design a person should set. */
  function facingInfo(body, charmOrEntry) {
    const sym = symmetryOf(body), set = facingSetFor(body, charmOrEntry);
    const base = { directional: sym.level === "directional", level: sym.level, symmetry: { cut: sym.cut, art: sym.art } };
    if (set) return Object.assign({ facing: set, confidence: 1, source: "person", why: "a person set it" }, base);
    const rel = relationFacing(body, charmOrEntry);
    if (rel) return Object.assign({ facing: rel, confidence: 0.9, source: "mirror-of-first", why: "drawn as the mirror image of the first body" }, base);
    if (sym.level === "symmetric") return Object.assign({ facing: null, confidence: 1, source: "symmetric", why: "its mirror image looks the same" }, base);
    return Object.assign({ facing: null, confidence: 0, source: "unknown", why: sym.level === "directional" ? "its mirror image differs and the drawing does not say which way it faces" : "nearly symmetric (a hoop off to one side or a wobble)" }, base);
  }
  /** Does this design need a person to say which way it faces? (directional, and nobody has said.) Takes a charm (geometry read) or a body. */
  function needsFacing(charmOrBody, charm) {
    const body = charmOrBody && charmOrBody.outline && charmOrBody.index == null ? bodiesOf(charmOrBody)[0] : charmOrBody, c = charm || charmOrBody;
    if (!body) return false;
    const info = facingInfo(body, c); return info.facing == null && info.directional;
  }
  /** Which way the design (its first body) faces in the master drawing: "L" | "R" | null (symmetric or unknown: then as drawn is the Left and the Right is the mirror).
   *  A `facing` ("L" | "R") a person set on the record or charm wins and a re-index never overwrites it. */
  function facingOf(charm) {
    if (!charm || typeof charm !== "object") return null;
    const rec = charm.entry || charm, set = facingValue(rec.facing) || facingValue(charm.facing);
    if (set) return set;
    if (!charm.outline) return null;
    return facingOfBody(bodiesOf(charm)[0], charm);
  }

  /** What the Master tab card shows for "which way this design faces" (Paul, 9 Oct: left and right earrings are mirror images, so the app must know which way the drawing faces):
   *  { show, mismatched, value, options, bodies, hint }. Shown only for a design that is not the same in a mirror (the index field `sym` written at indexing, or `level` when the page
   *  measured it) or one a person already set; a symmetric design has nothing to decide, and a design whose geometry is not known shows nothing. value "" = not set (as drawn is the
   *  Left), "L" | "R" = the way the master draws it, "X" = it reads one way (letters, numbers: never turned over). A MISMATCHED pair (a left body and a right body under one SKU) is
   *  always shown, with one box per body (`bodies`: [{ index, label, value, options }]): each body is judged on its own, because the two are not alike. */
  function facingControl(entry, level) {
    const e = entry || {}, word = v => (v === "L" || v === "R" ? v : ""), set = e.facing === "L" || e.facing === "R" || e.facing === "X" ? e.facing : "", lv = e.sym || level || "";
    const hint = "Which way the master file draws this design. A pair is a Left and a Right earring that are mirror images, so the Right is cut turned over; say which way the drawing faces so the Left earring faces left. \"Reads one way\" is for letters, numbers and words: both earrings are cut as drawn. Not set: the drawing is taken as the Left. Applies to orders made up from now on.";
    if (e.pair && e.pair.mismatched === true) {
      const per = Array.isArray(e.facings) ? e.facings : [], one = i => word(per[i]) || (i === 0 ? word(e.facing) : "");
      const opts = who => [["", who + ": not set"], ["L", who + " faces left"], ["R", who + " faces right"]];
      return { show: true, mismatched: true, value: one(0), options: opts("Left"), bodies: [0, 1].map(i => ({ index: i, label: i ? "Right" : "Left", value: one(i), options: opts(i ? "Right" : "Left") })),
        hint: "A left body and a right body under one SKU. Say which way each body is drawn (facing left or right) so the Left earring faces left and the Right earring faces right; a body drawn facing the wrong way is cut turned over. Not set: the left body is taken as the Left and the right body is turned over. Applies to orders made up from now on." };
    }
    return {
      show: !!set || lv === "directional", mismatched: false, value: set, bodies: null,
      options: [["", !set && readsOneWay(e) ? "reads one way (name)" : "faces: not set"], ["L", "faces left"], ["R", "faces right"], ["X", "reads one way"]], hint   // (a letter, number or script by its name is cut as drawn without a word: the box says so)
    };
  }

  const mx = (p, cx) => [2 * cx - p[0], p[1]];
  const boxOfPts = pts => { let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity; for (const q of pts) { if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; } return pts.length ? [x0, y0, x1, y1] : null; };
  function mirrorSeg(seg, cx, writable) {
    const subpaths = (seg.subpaths || []).map(sub => sub.map(o => o[0] === "m" || o[0] === "l" ? [o[0], mx(o[1], cx)] : o[0] === "c" ? ["c", mx(o[1], cx), mx(o[2], cx), mx(o[3], cx)] : o.slice()));
    const b = seg.bbox ? [2 * cx - seg.bbox[2], seg.bbox[1], 2 * cx - seg.bbox[0], seg.bbox[3]] : null;
    const out = Object.assign({}, seg, { subpaths, bbox: b, mirrored: !seg.mirrored, original: seg.original || seg });
    if (writable && seg.kind === "path") { out.synthetic = true; out.transformed = true; }   // no bytes in the source: the writer draws it from its geometry (syntheticOps)
    return out;
  }
  const tops = members => [...new Set(members.map(m => (m.parent != null ? m.parent : m.index)).filter(i => i != null))];
  function mirrorCharm(c, cx) {
    if (cx == null) cx = (c.bbox[0] + c.bbox[2]) / 2;
    const map = new Map(), members = [], dropped = new Set(c.dropIndices || []), unmirrored = [];
    for (const m of c.members || [c.outline]) {
      if (m.kind === "path") { const k = mirrorSeg(m, cx, true); map.set(m, k); members.push(k); const t = m.parent != null ? m.parent : m.index; if (t != null) dropped.add(t); }
      else { members.push(m); unmirrored.push(m); }   // text, image and shading objects have no geometry to reflect: they stay as drawn (sample text is never in a charm; engraving text is placed by the engraving code)
    }
    const out = Object.assign({}, c, { outline: map.get(c.outline) || mirrorSeg(c.outline, cx, true), members, bbox: [2 * cx - c.bbox[2], c.bbox[1], 2 * cx - c.bbox[0], c.bbox[3]], mirrored: !c.mirrored, mirror: !c.mirror, dropIndices: dropped, unmirrored });
    // a cut line chained from several open strokes is a synthetic outline that is only geometry (its real parts are members and are written themselves): it is never added as a member, or the laser would cut it twice
    if (c.outline && Array.isArray(c.outline.parts)) out.outline.parts = c.outline.parts.map(q => map.get(q) || q);
    else if (!map.has(c.outline)) out.members = [out.outline].concat(members);
    if (Array.isArray(c.extras)) out.extras = c.extras.map(e => map.get(e) || e);
    if (Array.isArray(c.centerPt)) out.centerPt = mx(c.centerPt, cx);
    if (Array.isArray(c.bboxOuter)) out.bboxOuter = [2 * cx - c.bboxOuter[2], c.bboxOuter[1], 2 * cx - c.bboxOuter[0], c.bboxOuter[3]];
    if (typeof c.upAngle === "number") out.upAngle = ((180 - c.upAngle) % 360 + 360) % 360;   // 90 (up) stays up; the direction turns about the vertical axis
    if (c.bits && c.w) out.bits = flipMask(c.bits, c.w, c.h);
    if (c.facing === "L" || c.facing === "R") out.facing = c.facing === "L" ? "R" : "L";   // the mirror image of a design that faces left faces right
    delete out.thumb; delete out.hash2;
    return out;
  }
  function flipMask(bits, w, h) { const o = new bits.constructor(bits.length); for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) o[y * w + x] = bits[y * w + (w - 1 - x)]; return o; }
  /** The mirror image (x -> -x about the body's own bounding-box centre, or about `cx` when given) of any geometry the app holds: a point list or a list of
   *  them (polygons), a path segment, a list of segments, a charm ({ outline, members, bbox }) or a silhouette mask ({ bits, w, h }). Returns a new
   *  value; the original is never touched. A mirrored path is marked synthetic (the writers draw it from its geometry) and its source Do is dropped. */
  function mirrorOf(g, cx) {
    if (g == null) return g;
    if (Array.isArray(g)) {
      if (!g.length) return [];
      if (typeof g[0] === "number") return mx(g, cx == null ? g[0] : cx);   // one point about cx (itself when no axis is given)
      if (Array.isArray(g[0]) && typeof g[0][0] === "number") { const c = cx == null ? (boxOfPts(g)[0] + boxOfPts(g)[2]) / 2 : cx; return g.map(p => mx(p, c)); }
      if (Array.isArray(g[0]) && Array.isArray(g[0][0])) { const all = g.flat(), bx = boxOfPts(all), c = cx == null ? (bx[0] + bx[2]) / 2 : cx; return g.map(poly => poly.map(p => mx(p, c))); }
      const bs = g.filter(m => m && m.bbox).reduce((a, m) => bbUnion(a, m.bbox), null), c = cx == null ? (bs ? (bs[0] + bs[2]) / 2 : 0) : cx;   // a list of segments
      return g.map(m => (m && m.kind === "path" ? mirrorSeg(m, c, true) : m));
    }
    if (g.bits && g.w && g.h && !g.outline) return Object.assign({}, g, { bits: flipMask(g.bits, g.w, g.h) });
    if (g.outline) return mirrorCharm(g, cx);
    if (g.subpaths) return mirrorSeg(g, cx == null ? (g.bbox[0] + g.bbox[2]) / 2 : cx, true);
    return g;
  }
  const geomCache = typeof WeakMap === "function" ? new WeakMap() : null;
  /** One body of a charm as a charm of its own (the other body's members left out). */
  function charmOfBody(charm, body) {
    const c = Object.assign({}, charm, { outline: body.outline, members: body.members.slice(), bbox: body.bbox.slice(), extras: [], bodyIndex: body.index });
    c.topIndices = tops(c.members);
    for (const k of ["bits", "w", "h", "scale", "bboxOuter", "thumb", "areaPt2", "centerPt", "hash", "widthPt", "heightPt", "open"]) delete c[k];   // a single body has its own silhouette: the caller traces it
    c.needsSilhouette = true;
    return c;
  }
  /** The geometry of THIS piece: for a mismatched design the body the piece is (piece.bodyIndex), alone; mirrored (mirrorOf) when piece.mirror. A piece with
   *  neither is the charm itself. Cached by charm, body and mirror (a mirrored variant is never taken for the as-drawn one). */
  function pieceGeometry(charm, piece) {
    if (!charm || typeof charm !== "object" || !charm.outline) return charm;
    const bodyIndex = piece && piece.bodyIndex != null ? +piece.bodyIndex : 0, mirror = !!(piece && piece.mirror);
    const bodies = bodiesOf(charm), whole = bodies.length === 2 && isMismatched(charm) ? false : true;
    const key = (whole ? "w" : "b" + bodyIndex) + (mirror ? "m" : "");
    let per = geomCache && geomCache.get(charm); if (per && per.sig === (charm.members || []).length && per.map.has(key)) return per.map.get(key);
    let g = charm;
    if (!whole) { const body = bodies[bodyIndex] || bodies[0]; g = charmOfBody(charm, body); }
    if (mirror) g = mirrorOf(g);
    if (geomCache) { if (!per || per.sig !== (charm.members || []).length) { per = { sig: (charm.members || []).length, map: new Map() }; geomCache.set(charm, per); } per.map.set(key, g); }
    return g;
  }

  /* ═══ 6 · the master side: which designs draw two (or more) bodies under one label ═══════════════════════════════
     The masters draw some designs as a ROW of bodies with ONE label centred under the whole row (MITTENS 1 beside MITTENS 2 under
     "Mismatched_7134", a star beside a moon, a key beside a lock, bacon beside an egg). groupCharms reads each body as a charm of its
     own; only the one the label sits under reaches the library, so the other body was never in the catalogue. This reads the row back
     from the grouping (it changes nothing in it): connected components of touching charms of one size, exactly one of them labelled,
     the label centred under the row. A row of two is a pair; of three or more is a set; size ladders (the same shape at two sizes)
     and charm-plus-sample rows are told apart and are not pairs. */
  const PAIR_DEFAULTS = { gx: 6, vo: 0.6, areaRatio: [0.4, 2.5], off: 0.18, maxBodies: 6, sameShapeIoU: 0.9, sameSizeTol: 0.08, maxOverlap: 0.3, stackIoU: 0.7, minLinearRatio: 0.55 };

  /** Shape of an outline normalised to its own box (uniform scale, centred) as an N x N mask; `mirror` flips it left to right. */
  function shapeMask(body, N, mirror) {
    const polys = flatten(body.outline, 8), b = body.outlineBbox, w = b[2] - b[0], h = b[3] - b[1], k = Math.max(w, h) || 1;
    const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2, bits = new Uint8Array(N * N);
    for (let j = 0; j < N; j++) for (let i = 0; i < N; i++) {
      const u = (mirror ? N - 1 - i : i), x = cx + ((u + 0.5) / N - 0.5) * k, y = cy + (0.5 - (j + 0.5) / N) * k;
      if (inPolys(x, y, polys)) bits[j * N + i] = 1;
    }
    return bits;
  }
  function iou(a, b) { let i = 0, u = 0; for (let k = 0; k < a.length; k++) { if (a[k] & b[k]) i++; if (a[k] | b[k]) u++; } return u ? i / u : 0; }
  /** Do two bodies have one shape, whatever their scale? (silhouette overlap of the boxes laid over each other, 1 = identical). With mirror: the best of as drawn and flipped. */
  function shapeSimilarity(a, b, N) { N = N || 40; const A = shapeMask(a, N, false); return { same: iou(A, shapeMask(b, N, false)), mirrored: iou(A, shapeMask(b, N, true)) }; }

  /** Components of touching, similar charms. charms: groupCharms().charms. Returns arrays of charms, left to right. Pure geometry on outline boxes. */
  function rowsOf(charms, opts) {
    opts = Object.assign({}, PAIR_DEFAULTS, opts || {});
    const live = (charms || []).filter(c => c && c.outline && c.outline.bbox && c.mergedInto == null);
    const I = live.map(c => { const b = c.outline.bbox; return { c, b, w: b[2] - b[0], h: b[3] - b[1], a: (b[2] - b[0]) * (b[3] - b[1]) }; }).sort((x, y) => x.b[0] - y.b[0]);
    const parent = I.map((_, i) => i), find = i => { while (parent[i] !== i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; };
    for (let i = 0; i < I.length; i++) {
      const a = I[i];
      for (let j = i + 1; j < I.length; j++) {
        const b = I[j]; if (b.b[0] > a.b[2] + opts.gx) break;   // sorted by left edge: nothing later can touch
        const gx = Math.max(b.b[0] - a.b[2], a.b[0] - b.b[2]); if (gx > opts.gx) continue;
        if (gx < -opts.maxOverlap * Math.min(a.w, b.w)) continue;   // one drawn over the other (a stacked twin, a layer): not a row
        { const iw = Math.min(a.b[2], b.b[2]) - Math.max(a.b[0], b.b[0]), ih = Math.min(a.b[3], b.b[3]) - Math.max(a.b[1], b.b[1]); if (iw > 0 && ih > 0 && iw * ih >= opts.stackIoU * (a.a + b.a - iw * iw * 0 - iw * ih)) continue; }
        const ov = Math.min(a.b[3], b.b[3]) - Math.max(a.b[1], b.b[1]); if (ov / Math.min(a.h, b.h) < opts.vo) continue;
        const ar = a.a / b.a; if (ar < opts.areaRatio[0] || ar > opts.areaRatio[1]) continue;
        parent[find(i)] = find(j);
      }
    }
    const groups = new Map(); I.forEach((x, i) => { const r = find(i); if (!groups.has(r)) groups.set(r, []); groups.get(r).push(x.c); });
    return [...groups.values()].filter(g => g.length >= 2 && g.length <= opts.maxBodies).map(g => g.sort((x, y) => x.outline.bbox[0] - y.outline.bbox[0] || y.outline.bbox[3] - x.outline.bbox[3]));
  }

  /** The rows a label is centred under, and what each is.
   *    labelsOf(charm) -> [{ sku, bbox }] the label lines the sheet puts under that charm (first line first), or []
   *  Returns [{ charms:[index left to right], labelled:[index], owner:index, skus:[sku], offset, bodies:n, kind, why, sim:[{same,mirrored}] }], where kind is
   *    "mismatched"  two bodies that are not the same charm (a different shape, or one shape with different engraving)
   *    "set"         three or more bodies under one label
   *    "sizes"       one shape at two sizes (a size ladder: normal)
   *    "twins"       two identical bodies drawn side by side (a matching pair drawn twice: doubtful)
   *    "neighbours"  every body of the row has a label of its own (separate designs that touch: normal)
   *    "tag"         a second labelled body whose label is not under the row (doubtful)
   *    "tagged"      the same, where the other body's SKU says MISMATCHED (AVOCADO_1948 with MISMATCHED): which SKU is the pair? (doubtful)
   *    "mirror"      the second body is the first flipped: a front and back view of one charm (custom samples), not a pair of designs (doubtful)
   *    "sample"      a body far smaller than the other (charm plus sample: normal)
   *  and `sure` says whether the layer may rewrite the design without a person looking (kind mismatched, one owner, a well centred label). */
  function masterPairs(charms, labelsOf, opts) {
    opts = Object.assign({}, PAIR_DEFAULTS, opts || {});
    const out = [];
    for (const row of rowsOf(charms, opts)) {
      const labelled = row.filter(c => (labelsOf(c) || []).length);
      if (!labelled.length) continue;
      const ub = row.reduce((a, c) => bbUnion(a, c.outline.bbox), null), ucx = (ub[0] + ub[2]) / 2, uw = Math.max(1e-6, ub[2] - ub[0]);
      const offOf = c => { let best = null; for (const L of labelsOf(c) || []) { if (!L.bbox) continue; const o = ((L.bbox[0] + L.bbox[2]) / 2 - ucx) / uw; if (best == null || Math.abs(o) < Math.abs(best)) best = o; } return best; };
      const centred = labelled.filter(c => { const o = offOf(c); return o != null && Math.abs(o) <= opts.off; });
      if (!centred.length) continue;
      const owner = centred.reduce((a, c) => Math.abs(offOf(c)) < Math.abs(offOf(a)) ? c : a);
      const rec = { charms: row.map(c => c.index), labelled: labelled.map(c => c.index), owner: owner.index, skus: (labelsOf(owner) || []).map(l => l.sku), offset: +offOf(owner).toFixed(3), bodies: row.length, kind: null, why: "", sim: [], sure: false, union: ub };
      const others = labelled.filter(c => c !== owner);
      if (others.length) {
        const separate = others.every(c => { const o = offOf(c); return o == null || Math.abs(o) > opts.off; });   // each has its own label under itself
        const bodies = row.map(c => bodiesOf(c)[0]);
        // a neighbour whose label is its own and the row's label is not centred under all of it: separate designs that touch
        const tagged = others.length === 1 && row.length === 2 && /MISMATCH/i.test((labelsOf(others[0])[0] || {}).sku || "");
        rec.kind = tagged ? "tagged" : separate && others.length >= row.length - 1 ? "neighbours" : "tag";
        if (tagged) { const A = bodies[0], B = bodies[1], sm = shapeSimilarity(A, B); rec.sim = [sm]; rec.partnerSku = (labelsOf(others[0])[0] || {}).sku; rec.why = "the other body carries the SKU " + rec.partnerSku + " (a tag beside the label, not under the row): is the pair " + (rec.skus[0] || "") + "?"; out.push(rec); continue; }
        rec.why = rec.kind === "neighbours" ? "every body has a label of its own" : "a second body carries its own label (" + others.map(c => (labelsOf(c)[0] || {}).sku).join(", ") + ") beside the one centred under the row";
        if (rec.kind === "tag") rec.sim = row.slice(1).map((c, i) => shapeSimilarity(bodies[i], bodies[i + 1]));
        out.push(rec); continue;
      }
      const bodies = row.map(c => bodiesOf(c)[0]);
      if (row.length > 2) { rec.kind = "set"; rec.why = row.length + " bodies under one label"; out.push(rec); continue; }
      const A = bodies[0], B = bodies[1], sim = shapeSimilarity(A, B); rec.sim = [sim];
      const la = Math.sqrt(A.area || bbArea(A.outlineBbox)), lb = Math.sqrt(B.area || bbArea(B.outlineBbox)), lin = Math.min(la, lb) / Math.max(la, lb);
      rec.linearRatio = +lin.toFixed(3);
      const sameShape = sim.same >= opts.sameShapeIoU;
      if (lin < opts.minLinearRatio) { rec.kind = "sample"; rec.why = "one body is " + Math.round(lin * 100) + "% of the other's size (a charm and a sample)"; }
      else if (sameShape && Math.abs(1 - lin) > opts.sameSizeTol) { rec.kind = "sizes"; rec.why = "one shape at two sizes (" + Math.round(lin * 100) + "%)"; }
      else if (sameShape && sameBody(A, B)) { rec.kind = "twins"; rec.why = "two identical bodies, cut line and engraving"; }
      else if (!sameShape && sim.mirrored >= opts.sameShapeIoU) { rec.kind = "mirror"; rec.why = "the second body is the first one flipped (a front and a back view, or a left and a right of one design)"; rec.mirrored = true; }
      else { rec.kind = "mismatched"; rec.why = sameShape ? "one cut shape, different engraving" : "two different shapes"; rec.mirrored = false; }
      const lab0 = rec.skus[0] || "", words = lab0.split(/[^A-Za-z0-9]+/).filter(Boolean);
      const weak = lab0.replace(/[^A-Z0-9]/gi, "").length < 4 || (words.length > 0 && words.every(w => w.length <= 2));   // a label of a few characters, or of short words only ("V1 V2", "L R"), is a note, not a SKU
      if (weak) rec.weak = "the label \"" + (rec.skus[0] || "") + "\" is too short to be a SKU";
      rec.sure = rec.kind === "mismatched" && Math.abs(rec.offset) <= opts.off && !weak;
      out.push(rec);
    }
    return out;
  }

  /** The design's charm with its row's other bodies folded in (a new object: the grouping's charms are left as they are), the same fold readMasterCharm does
   *  for a per-SKU file that splits in two. Every body is welded with its own hoop first (integrateRings on each charm, passed in). */
  function foldRow(owner, partners) {
    const members = owner.members.slice(), seen = new Set(members);
    let bbox = owner.bbox.slice(), top = new Set(owner.topIndices || []), extras = (owner.extras || []).slice();
    const drop = new Set(owner.dropIndices || []);
    for (const p of partners) {
      for (const m of p.members) if (!seen.has(m)) { seen.add(m); members.push(m); }
      bbox = bbUnion(bbox, p.bbox); for (const t of p.topIndices || []) top.add(t);
      for (const e of p.extras || []) if (!extras.includes(e)) extras.push(e);
      for (const d of p.dropIndices || []) drop.add(d);
    }
    const folded = Object.assign({}, owner, { members, bbox, topIndices: [...top], extras, strokePt: Math.max(owner.strokePt || 0.5, ...partners.map(p => p.strokePt || 0.5)) });
    if (drop.size) folded.dropIndices = drop;
    return folded;
  }
  /** The master index entry's `pair` field for a row: { v: 1, bodies, mismatched }. */
  const pairField = rec => ({ v: 1, bodies: rec.bodies, mismatched: rec.kind === "mismatched" });

  /** The pair layer of the INDEXERS (scripts/index-master.cjs and netlify/functions/charmMaster-background.js read it from here, so the two cannot disagree).
   *  A master draws some designs as a ROW of bodies with ONE label centred under the whole row ("Mismatched_7134": MITTENS 1 beside MITTENS 2). groupCharms reads
   *  each body as a charm of its own and only the labelled one was ever indexed, so the library held half of the design. This reads the rows back from the grouping
   *  (masterPairs; it changes nothing in the grouping) and, for a row it is sure is a mismatched pair, builds ONE design from it: each body is welded with its own
   *  hoop, then the bodies' members are folded into the labelled charm (foldRow, the same fold the app does when it reads a per-SKU file that splits in two). Every
   *  other design is not touched: it reaches the indexer's loop as the same charm object it always did.
   *    P  the PDF module (integrateRings, cutLinesOf)      G  charm-nest-geom (flatten, upAngleOf, backView, engraveMask, largestRectangles)
   *    g  groupCharms()'s answer      lab  labelCharms()'s answer      items  [{ index, l, c }] the labelled charms this run builds (a row is folded only when its owner is among them)
   *    o  { engraveMarginMm, pairAlso: Set | array | "SKU,SKU" (doubtful one-label rows of these SKUs are folded too) }      log  a function that takes a line
   *  Returns { rows (for the report), fold: Map(owner's charm index → { rec, charm, bodies, view, open, holes, engrave, field }), refused: Map(owner's charm index → why) }. */
  function pairLayer(P, G, g, lab, items, o, log) {
    o = o || {}; log = log || (() => {});
    const out = { rows: [], fold: new Map(), refused: new Map() }, MM = 25.4 / 72;   // refused: owner index → why, for a row that is a pair but could not be folded (its lone labelled body must not be written in its place)
    const byIndex = new Map(g.charms.map(c => [c.index, c])), building = new Set(items.map(it => it.index));
    const linesOf = c => { const l = lab.labels.get(c.index); return l ? [l].concat(l.extra || []).map(x => ({ sku: x.sku, bbox: x.bbox })) : []; };
    const also = o.pairAlso ? new Set((typeof o.pairAlso === "string" ? o.pairAlso.split(",") : [...o.pairAlso]).map(x => String(x || "").trim().toUpperCase()).filter(Boolean)) : null;
    const union = (a, b) => [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])];
    const openOf = c => { const polys = G.flatten(c.outline, 12); return !polys.length || polys.some(p => Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) > 1.5 && !c.outline.closed); };
    // the same readings the indexer takes of a lone charm (up direction, back view, room for an engraving), taken of one body
    const engraveOf = c => {
      let engravable = true, upAngle = null, upSource = "drawn", flipOk = true, flipWhy = null;
      try { const up = G.upAngleOf(c); upAngle = up.angle; upSource = up.source; const view = G.backView(c, { res: 6, upAngle }); const mask = G.engraveMask(view, { marginMm: o.engraveMarginMm }); const r = G.largestRectangles(mask, 1)[0]; engravable = !!r && ((r.wPt * MM >= 6 && r.hPt * MM >= 3) || (r.wPt * MM >= 3 && r.hPt * MM >= 6)); }
      catch (e) { flipOk = false; flipWhy = e.message; engravable = false; }
      return { engravable, upAngle, upSource, flipOk, flipWhy };
    };
    for (const r of masterPairs(g.charms, linesOf)) {
      const forced = !!also && r.skus.some(s => also.has(String(s).toUpperCase())) && r.labelled.length === 1 && r.kind !== "neighbours";
      const row = { kind: r.kind, sure: !!r.sure, folded: false, bodies: r.bodies, owner: r.owner, skus: r.skus, charms: r.charms, labelled: r.labelled, offset: r.offset, why: r.why };
      if (r.linearRatio != null) row.linearRatio = r.linearRatio; if (r.weak) row.weak = r.weak; if (r.partnerSku) row.partnerSku = r.partnerSku;
      out.rows.push(row);
      if (!(r.sure || forced)) continue;
      if (!building.has(r.owner)) { row.note = "not built in this run"; continue; }
      const bodyCharms = r.charms.map(i => byIndex.get(i)), owner = byIndex.get(r.owner);
      if (bodyCharms.some(c => !c) || !owner) { row.note = "a body of the row is missing from the grouping"; out.refused.set(r.owner, row.note); continue; }
      try {
        for (const c of bodyCharms) { const x = P.integrateRings(c); if (x.left.length) log(`  ! ${r.skus[0]}: a hoop could not join its charm: ${x.left[0]}`); }   // each body with its own hoop, before the bodies are folded
        const charm = foldRow(owner, bodyCharms.filter(c => c !== owner));
        const outlines = bodyCharms.map(c => c.outline);
        const merged = Object.assign({}, owner.outline, { subpaths: [].concat(...outlines.map(x => x.subpaths || [])), bbox: outlines.map(x => x.bbox).reduce(union), closed: outlines.every(x => x.closed) });
        const eng = bodyCharms.map(engraveOf), first = eng[bodyCharms.indexOf(owner)];   // (the labelled body's up direction is the one the library always held for this SKU)
        // each body can be written as a form of its own only when no two bodies draw from one top-level group of the master (then the per-SKU file keeps them apart)
        const parentsOf = c => new Set(c.members.map(m => (m.parent != null ? m.parent : m.index)).filter(t => t != null));
        const par = bodyCharms.map(parentsOf), apart = par.every((a, i) => par.every((b, j) => i === j || ![...a].some(t => b.has(t))));
        row.folded = true; row.forced = forced && !r.sure; row.partners = bodyCharms.filter(c => c !== owner).map(c => c.index);
        if (!apart) row.note = "the bodies share a top-level group of the master: written as one group";
        out.fold.set(r.owner, {
          rec: r, charm, bodies: apart ? bodyCharms : null, view: Object.assign({}, charm, { outline: merged }),                      // (the merged outline is only what the area and the hash are read from; the charm keeps a real body's outline, so its bodies can be told apart again)
          open: bodyCharms.some(openOf), holes: bodyCharms.reduce((n, c) => n + P.cutLinesOf(c).length, 0),
          engrave: { engravable: eng.every(e => e.engravable), upAngle: first.upAngle, upSource: first.upSource, flipOk: eng.every(e => e.flipOk), flipWhy: (eng.find(e => !e.flipOk) || {}).flipWhy || null },
          field: { v: 1, bodies: r.bodies, mismatched: r.kind === "mismatched" || (forced && r.bodies === 2 && !["twins", "sizes", "sample"].includes(r.kind)) }
        });
      } catch (e) { row.folded = false; row.note = "not folded: " + e.message; out.refused.set(r.owner, row.note); log(`  ! ${r.skus[0]}: the pair could not be folded: ${e.message}`); }
    }
    const folded = out.rows.filter(x => x.folded), unbuilt = out.rows.filter(x => x.sure && x.note === "not built in this run"), doubtful = out.rows.filter(x => !x.folded && !x.sure && !/^(neighbours|sizes|sample)$/.test(x.kind));
    if (out.rows.length) log(`  pairs: ${out.rows.length} row(s) of touching bodies under one label · ${folded.length} folded into one design (${folded.map(x => x.skus[0]).slice(0, 40).join(", ")}${folded.length > 40 ? " …" : ""})${unbuilt.length ? ` · ${unbuilt.length} sure pair(s) not built in this run` : ""}${doubtful.length ? ` · ${doubtful.length} left as they were because a person has to look (${doubtful.map(x => `${x.skus[0] || "#" + x.owner}: ${x.kind}`).slice(0, 30).join(", ")})` : ""}`);
    return out;
  }

  /** Does the library's entry for a SKU (and size) hold the design as a PAIR (a `pair` field of two bodies or more)? entry: a master index entry, as the library returns it. */
  function heldPair(entry, size) {
    if (!entry) return false;
    const t = size && entry.sizes && entry.sizes[String(size).toUpperCase()] ? entry.sizes[String(size).toUpperCase()] : entry;
    return !!(t && t.pair && +t.pair.bodies >= 2);
  }
  /** For an indexer that draws ONE body per label (the Master tab's own indexer: its silhouettes come from a canvas) and so cannot build a pair design: which of the charms it is about
   *  to write it must leave alone, because writing the labelled body alone would turn a pair design back into one body (a file with half the design, a record that still says two).
   *    charms  the grouping's charms     labelsOf(charm) → [{ sku, bbox, size? }]     known  Map(SKU → the library's entry) or null
   *  Returns Map(charm index → { why, kind: "row" | "held", skus }): "row" = this charm is the labelled one of a row of two bodies that is surely a mismatched pair;
   *  "held" = the library already holds one of its SKUs as a pair and this charm shows fewer bodies than that. A charm that already carries every body (a review merged them) is not held back. */
  function keepPairs(charms, labelsOf, known) {
    const out = new Map(), byIndex = new Map((charms || []).map(c => [c.index, c]));
    for (const r of masterPairs(charms, labelsOf)) if (r.sure) out.set(r.owner, { kind: "row", skus: r.skus.slice(), why: "its drawing is a row of " + r.bodies + " bodies under one label (a mismatched pair)" });
    if (known) for (const c of charms || []) {
      if (c.mergedInto != null || out.has(c.index)) continue;
      const lines = labelsOf(c) || []; if (!lines.length) continue;
      const held = lines.map(l => ({ l, e: known.get(String(l.sku || "").toUpperCase()) })).find(x => heldPair(x.e, x.l.size));
      if (!held) continue;
      const t = held.l.size && held.e.sizes && held.e.sizes[String(held.l.size).toUpperCase()] || held.e;
      let n = 1; try { n = bodiesOf(c).length; } catch (_) {}
      if (n < +t.pair.bodies) out.set(c.index, { kind: "held", skus: lines.map(l => l.sku), why: "the library holds " + held.l.sku + " as a pair of " + t.pair.bodies + " bodies and this drawing shows " + n });
    }
    return out;
  }

  /** The extra fields an indexer writes on a design's entry besides `pair` (PAIRMIRROR's, shared so the browser, the server and the script write the same words):
   *  sym (does the design look the same in a mirror: "symmetric" | "slight" | "directional") for every design, facings ([null, "mirror"…]) for a folded pair drawn as mirror images.
   *  Anything the module cannot say is left out; never throws. */
  function entryFields(c, folded) {
    const out = {};
    try { const bs = bodiesOf(c); if (folded && bs.length === 2) { const rel = facingOfBody(bs[1], c); if (rel) out.facings = [null, rel]; } const sym = bs[0] ? symmetryOf(bs[0]).level : undefined; if (sym) out.sym = sym; } catch (_) {}
    return out;
  }

  return {
    BODY_MIN_PT, RING_MAX_PT, SECOND_BODY_MIN_RATIO,
    bodiesOf, isMismatched, sideOf, sideLabel, groupKey, piecesFor, kindOf, mustShareSheet,
    describe, sameBody, sidesSaid, sideForPiece, pieceFields, groupOf, siblingsOf, splitAcross, designPair, pieceCountOf, discsOf,
    facingOf, facingOfBody, facingInfo, symmetryOf, needsFacing, readsOneWay, facingControl, mirrorOf, pieceGeometry, isEarringPair, charmOfBody,
    PAIR_DEFAULTS, shapeSimilarity, rowsOf, masterPairs, foldRow, pairField, pairLayer, entryFields, heldPair, keepPairs,
    _flatten: flatten, _inPolys: inPolys, _distPolys: distPolys, _isCut: isCut
  };
});
