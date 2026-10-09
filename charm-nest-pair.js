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
        const holder = all.find(b => bbNear(s.bbox, b.bbox, 0) && pts.length && pts.filter(p => inPolys(p[0], p[1], b.polys)).length / pts.length >= 0.6);
        if (holder) continue;                                  // a cut-out or inner ring of a body
        const maxDim = Math.max(s.bbox[2] - s.bbox[0], s.bbox[3] - s.bbox[1]);
        const ringLike = maxDim <= RING_MAX_PT && (s.subpaths || []).length <= 2 && (s.subpaths || []).every(sp => sp.length <= 20);
        if (ringLike && all.some(b => bbNear(s.bbox, b.bbox, RING_NEAR_PT + 1) && Math.min(...pts.map(p => distPolys(p[0], p[1], b.polys))) <= RING_NEAR_PT + (s.lwPt || 0) / 2 + (b.outline.lwPt || 0) / 2)) continue;   // a hoop beside a body is part of it
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

  const FORMS_PAIR = new Set(["earrings", "earring", "stud", "studs", "hoop", "hoops", "huggie", "huggies", "stud earrings", "hoop earrings", "huggie earrings", "pair"]);
  const FORMS_SINGLE = new Set(["earring-single", "single earring", "single", "charm", "pendant", "keychain", "bracelet", "anklet"]);
  const formOf = line => String((line && ((line.spec && line.spec.form) || line.form || (line.row && line.row.spec && line.row.spec.form))) || "").toLowerCase().trim();
  const num = v => (Number.isFinite(+v) && +v > 0 ? Math.floor(+v) : 0);
  const quantityOf = line => num(line && ((line.spec && line.spec.quantity) || line.quantity || line.qty || (line.row && line.row.spec && line.row.spec.quantity))) || 1;
  /** n for a disc necklace ("3 discs"), or 0. Read from an explicit count on the line, else from the listing text and variations. */
  function discsOf(line) {
    if (!line) return 0;
    const e = num(line.discs || line.discCount || (line.spec && (line.spec.discs || line.spec.discCount)));
    if (e) return e;
    const texts = [line.title, line.sku, line.variation, line.variations && JSON.stringify(line.variations), line.spec && line.spec.variation].filter(Boolean).join(" ");
    const m = /(\d{1,2})\s*(?:x\s*)?(?:discs?|disks?|circles?)\b/i.exec(texts) || /\b(?:discs?|disks?)\s*[:\-x]\s*(\d{1,2})\b/i.exec(texts);
    return m ? +m[1] : 0;
  }
  /** How many pieces one order line makes. An explicit count on the line wins (the intake sets it: pieceCount / poolIds); else
   *  a mismatched design makes two per unit, earrings two per unit, a disc necklace its discs, anything else one per unit. */
  function pieceCountOf(line, charm) {
    const explicit = num(line && (line.pieceCount || line.pieces_n)) || (line && Array.isArray(line.poolIds) && line.poolIds.length) || 0;
    if (explicit) return explicit;
    const q = quantityOf(line), d = discsOf(line);
    if (d > 1) return d * q;
    if (charm && isMismatched(charm)) return 2 * q;
    const f = formOf(line);
    if (FORMS_PAIR.has(f) && !FORMS_SINGLE.has(f)) return 2 * q;
    return q;
  }
  /** The pieces one order line makes, in order: [{ side, bodyIndex, groupKey, n, of }]. A mismatched design: every unit makes its left piece
   *  (bodyIndex 0, "L") then its right piece (bodyIndex 1, "R"); anything else: bodyIndex 0, side null. */
  function piecesFor(line, charm) {
    const key = groupKey(line), mis = !!charm && isMismatched(charm), total = pieceCountOf(line, charm), out = [];
    for (let i = 0; i < total; i++) {
      const bodyIndex = mis ? i % 2 : 0;
      out.push({ side: mis ? sideOf(bodyIndex, 2) : null, bodyIndex, groupKey: key, n: i + 1, of: total });
    }
    return out;
  }
  /** single | pair | mismatched | multi (n discs, or any count above two). */
  function kindOf(line, charm) {
    if (charm && isMismatched(charm) && pieceCountOf(line, charm) === 2) return "mismatched";
    const total = pieceCountOf(line, charm);
    if (discsOf(line) > 1) return "multi";
    if (charm && isMismatched(charm)) return "multi";   // several mismatched pairs on one line: two or more of each side
    if (total <= 1) return "single";
    if (total === 2) return "pair";
    return "multi";
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

  return {
    BODY_MIN_PT, RING_MAX_PT, SECOND_BODY_MIN_RATIO,
    bodiesOf, isMismatched, sideOf, sideLabel, groupKey, piecesFor, kindOf, mustShareSheet,
    describe, sameBody, sideForPiece, pieceFields, groupOf, siblingsOf, splitAcross, designPair, pieceCountOf, discsOf,
    _flatten: flatten, _inPolys: inPolys, _distPolys: distPolys, _isCut: isCut
  };
});
