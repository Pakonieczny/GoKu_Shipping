/*  charm-nest-dxf.js — a .dxf drawing, read into the same PDF the rest of the sorter reads.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul, 27 Sep: a custom order's own designs are dropped on its Custom Orders card as .ai or .dxf files. An .ai is
 *  already a PDF (CharmNestPDF.parseSource); a .dxf is turned into one here, so the grouping, the silhouettes, the
 *  sheets and the laser files take it exactly as they take an .ai: cut lines drawn black, engraving in its colour.
 *
 *  Read: LINE, LWPOLYLINE (with bulges), POLYLINE/VERTEX, CIRCLE, ARC, ELLIPSE, SPLINE, HATCH (its boundaries, filled)
 *  and INSERT (blocks, nested, arrays), in the drawing's units ($INSUNITS; a unitless drawing reads as millimetres, or
 *  inches when it is too small to be a charm in millimetres). Colours follow DXF: the entity's own, its block's, or its
 *  layer's. Grey and black read as cut lines, any colour as engraving, as in the .ai masters; a layer named for
 *  engraving (engrave, etch, hatch, mark, score) is engraving and one named for cutting (cut, outline) is cut, whatever
 *  its colour. Lines and arcs drawn one by one are joined where their ends meet, so an outline is one closed path.
 *  Layers turned off or frozen are left out; text and dimensions are left out and counted in `skipped`.
 *
 *  toPdf(bytes | text, name) → Promise<{ bytes: Uint8Array, meta: { units, widthMm, heightMm, paths, skipped } }>
 *  Needs pdf-lib (window.PDFLib in the page, require()'d in node). Pure otherwise.
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory(root);
  else root.CharmNestDXF = factory(root);
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  const MM = 72 / 25.4;
  // $INSUNITS → [name, points per unit]
  const UNITS = { 1: ["in", 72], 2: ["ft", 864], 4: ["mm", MM], 5: ["cm", MM * 10], 6: ["m", MM * 1000], 8: ["µin", 72e-6], 9: ["mil", 0.072], 10: ["yd", 2592], 13: ["µm", MM / 1000], 14: ["dm", MM * 100] };
  const ENGRAVE = /^(?:.*[\s_-])?(?:engrav\w*|etch\w*|hatch\w*|mark\w*|score\w*|front detail|artwork)(?:$|[\s:_()\/-])/i;
  const CUT = /^(?:.*[\s_-])?(?:cut|cutout|cut-out|cutline|cut line|outline|thru|through)(?:$|[\s:_()\/-])/i;
  const PDF_LIB = () => root.PDFLib || (typeof require === "function" ? require("./vendor/pdf-lib-1.17.1.min.js") : null);

  /* ── colours: the AutoCAD Colour Index, near enough to tell grey from colour and to draw the thumbnail ── */
  function aci(n) {
    n = Math.abs(+n) | 0;
    const base = { 1: [1, 0, 0], 2: [1, 1, 0], 3: [0, 1, 0], 4: [0, 1, 1], 5: [0, 0, 1], 6: [1, 0, 1], 7: [0, 0, 0], 8: [.5, .5, .5], 9: [.75, .75, .75] };
    if (base[n]) return base[n];
    if (n >= 250 && n <= 255) { const g = [.2, .31, .41, .51, .68, 1][n - 250]; return [g, g, g]; }
    if (n < 10 || n > 249) return [0, 0, 0];
    const hue = Math.floor((n - 10) / 10) * 15, k = n % 10, v = [1, 1, .65, .65, .5, .5, .3, .3, .15, .15][k], s = k % 2 ? .5 : 1;
    const c = v * s, x = c * (1 - Math.abs((hue / 60) % 2 - 1)), m = v - c;
    const [r, g, b] = hue < 60 ? [c, x, 0] : hue < 120 ? [x, c, 0] : hue < 180 ? [0, c, x] : hue < 240 ? [0, x, c] : hue < 300 ? [x, 0, c] : [c, 0, x];
    return [r + m, g + m, b + m];
  }
  const trueColour = v => { v = +v >>> 0; return [((v >> 16) & 255) / 255, ((v >> 8) & 255) / 255, (v & 255) / 255]; };
  const achromatic = c => Math.max(c[0], c[1], c[2]) - Math.min(c[0], c[1], c[2]) <= 0.15;

  /* ── the file: group-code pairs, sections, tables, blocks and entities ── */
  function textOf(input) {
    if (typeof input === "string") return input;
    const b = input instanceof Uint8Array ? input : new Uint8Array(input);
    const head = String.fromCharCode.apply(null, b.subarray(0, 22));
    if (head.startsWith("AutoCAD Binary DXF")) throw new Error("this is a binary DXF — save it again as an ASCII DXF");
    try { return new TextDecoder("utf-8", { fatal: true }).decode(b); } catch (_) { return new TextDecoder("latin1").decode(b); }
  }
  function pairsOf(text) {
    const t = text.split(/\r\n|\r|\n/), out = [];
    let i = 0; while (i < t.length && t[i].trim() === "") i++;
    for (; i + 1 < t.length; i += 2) {
      const code = parseInt(t[i], 10);
      if (!Number.isFinite(code)) { if (t[i].trim() === "" && i + 2 >= t.length) break; throw new Error(`not a DXF drawing (line ${i + 1} is not a group code)`); }
      out.push([code, t[i + 1].trim()]);
    }
    if (!out.some(p => p[0] === 0 && p[1] === "SECTION")) throw new Error("not a DXF drawing (no SECTION)");
    return out;
  }
  /** An entity: its type and its group codes, up to the next code 0. */
  function entity(pairs, i) {
    const e = { type: pairs[i][1], codes: [] };
    for (i++; i < pairs.length && pairs[i][0] !== 0; i++) e.codes.push(pairs[i]);
    return [e, i];
  }
  const get = (e, code, dflt) => { for (const p of e.codes) if (p[0] === code) return p[1]; return dflt; };
  const num = (e, code, dflt) => { const v = get(e, code); const n = v == null ? NaN : parseFloat(v); return Number.isFinite(n) ? n : dflt; };
  /** Entities from a run of pairs (ENTITIES or a BLOCK), with a POLYLINE's VERTEX entities folded into it. */
  function entitiesOf(pairs, i, stop) {
    const list = [];
    while (i < pairs.length && !(pairs[i][0] === 0 && stop.includes(pairs[i][1]))) {
      if (pairs[i][0] !== 0) { i++; continue; }
      let e; [e, i] = entity(pairs, i);
      if (e.type === "POLYLINE") {
        e.vertices = [];
        while (i < pairs.length && pairs[i][0] === 0 && pairs[i][1] === "VERTEX") { let v; [v, i] = entity(pairs, i); e.vertices.push(v); }
        if (i < pairs.length && pairs[i][0] === 0 && pairs[i][1] === "SEQEND") { let s; [s, i] = entity(pairs, i); }
      }
      list.push(e);
    }
    return [list, i];
  }
  function parse(text) {
    const pairs = pairsOf(text), doc = { header: {}, layers: new Map(), blocks: new Map(), entities: [] };
    for (let i = 0; i < pairs.length; i++) {
      if (!(pairs[i][0] === 0 && pairs[i][1] === "SECTION") || !pairs[i + 1] || pairs[i + 1][0] !== 2) continue;
      const name = pairs[i + 1][1]; i += 2;
      if (name === "HEADER") {
        for (; i < pairs.length && !(pairs[i][0] === 0 && pairs[i][1] === "ENDSEC"); i++) if (pairs[i][0] === 9 && pairs[i + 1]) doc.header[pairs[i][1]] = pairs[i + 1][1];
      } else if (name === "TABLES") {
        for (; i < pairs.length && !(pairs[i][0] === 0 && pairs[i][1] === "ENDSEC"); ) {
          if (pairs[i][0] === 0 && pairs[i][1] === "LAYER") {
            let e; [e, i] = entity(pairs, i);
            const colour = num(e, 62, 7), tc = get(e, 420);
            doc.layers.set(get(e, 2, "0"), { rgb: tc != null ? trueColour(tc) : aci(colour), off: colour < 0 || (num(e, 70, 0) & 1) === 1 });
          } else i++;
        }
      } else if (name === "BLOCKS") {
        for (; i < pairs.length && !(pairs[i][0] === 0 && pairs[i][1] === "ENDSEC"); ) {
          if (pairs[i][0] === 0 && pairs[i][1] === "BLOCK") {
            let head; [head, i] = entity(pairs, i);
            let list; [list, i] = entitiesOf(pairs, i, ["ENDBLK", "ENDSEC"]);
            if (i < pairs.length && pairs[i][1] === "ENDBLK") { let x; [x, i] = entity(pairs, i); }
            doc.blocks.set(get(head, 2, ""), { base: [num(head, 10, 0), num(head, 20, 0)], entities: list });
          } else i++;
        }
      } else if (name === "ENTITIES") {
        let list; [list, i] = entitiesOf(pairs, i, ["ENDSEC"]); doc.entities = list;
      }
    }
    return doc;
  }

  /* ── geometry: every entity becomes subpaths of lines and cubic Béziers, in drawing units ── */
  // a subpath: { start: [x, y], cmds: [["L", p] | ["C", c1, c2, p]], closed }
  function arcCmds(cx, cy, rx, ry, rot, t0, sweep) {
    // an elliptical arc (rx along the direction rot, ry across it) from parameter t0 through sweep, as Béziers of ≤ 90°
    const n = Math.max(1, Math.ceil(Math.abs(sweep) / (Math.PI / 2) - 1e-9)), d = sweep / n, k = 4 / 3 * Math.tan(d / 4);
    const cr = Math.cos(rot), sr = Math.sin(rot);
    const at = t => { const x = rx * Math.cos(t), y = ry * Math.sin(t); return [cx + x * cr - y * sr, cy + x * sr + y * cr]; };
    const tan = t => { const x = -rx * Math.sin(t), y = ry * Math.cos(t); return [x * cr - y * sr, x * sr + y * cr]; };
    const cmds = [];
    for (let i = 0; i < n; i++) {
      const a = t0 + i * d, b = a + d, p0 = at(a), p3 = at(b), ta = tan(a), tb = tan(b);
      cmds.push(["C", [p0[0] + k * ta[0], p0[1] + k * ta[1]], [p3[0] - k * tb[0], p3[1] - k * tb[1]], p3]);
    }
    return { start: at(t0), cmds };
  }
  /** The arc a bulge draws from a to b (DXF: bulge = tan(sweep / 4), counter-clockwise when positive). */
  function bulgeCmds(a, b, bulge) {
    const dx = b[0] - a[0], dy = b[1] - a[1], d = Math.hypot(dx, dy);
    if (!d || Math.abs(bulge) < 1e-9) return [["L", b]];
    const ux = dx / d, uy = dy / d, h = (1 - bulge * bulge) / (4 * bulge) * d;
    const cx = (a[0] + b[0]) / 2 - uy * h, cy = (a[1] + b[1]) / 2 + ux * h, r = Math.hypot(a[0] - cx, a[1] - cy);
    const arc = arcCmds(cx, cy, r, r, 0, Math.atan2(a[1] - cy, a[0] - cx), 4 * Math.atan(bulge));
    const last = arc.cmds[arc.cmds.length - 1]; last[3] = b;                  // end exactly where the next vertex starts
    return arc.cmds;
  }
  function polyCmds(verts, closed) {
    // verts: [{ p: [x, y], bulge }]
    if (verts.length < 2) return null;
    const cmds = [];
    for (let i = 0; i < verts.length - 1; i++) cmds.push(...bulgeCmds(verts[i].p, verts[i + 1].p, verts[i].bulge || 0));
    if (closed) { const a = verts[verts.length - 1], b = verts[0]; if (a.bulge || Math.hypot(a.p[0] - b.p[0], a.p[1] - b.p[1]) > 1e-12) cmds.push(...bulgeCmds(a.p, b.p, a.bulge || 0)); }
    return { start: verts[0].p, cmds, closed: !!closed };
  }
  /** A B-spline, rational or not, sampled finely (a spline has no exact Bézier form in general); fit points alone are joined. */
  function splineSub(e) {
    const deg = num(e, 71, 3), flags = num(e, 70, 0), knots = [], ctrl = [], weights = [], fit = [];
    let cx = null, fx = null;
    for (const [c, v] of e.codes) {
      const f = parseFloat(v);
      if (c === 40) knots.push(f); else if (c === 41) weights.push(f);
      else if (c === 10) cx = f; else if (c === 20 && cx != null) { ctrl.push([cx, f]); cx = null; }
      else if (c === 11) fx = f; else if (c === 21 && fx != null) { fit.push([fx, f]); fx = null; }
    }
    const closed = (flags & 1) === 1;
    if (ctrl.length < deg + 1 || knots.length !== ctrl.length + deg + 1) {
      if (fit.length < 2) return null;
      return { start: fit[0], cmds: fit.slice(1).map(p => ["L", p]), closed };
    }
    const w = weights.length === ctrl.length ? weights : ctrl.map(() => 1);
    const lo = knots[deg], hi = knots[ctrl.length];
    const at = t => {
      let k = deg; while (k < ctrl.length - 1 && t >= knots[k + 1]) k++;
      const d = []; for (let j = 0; j <= deg; j++) { const i = k - deg + j, q = ctrl[i], ww = w[i]; d.push([q[0] * ww, q[1] * ww, ww]); }
      for (let r = 1; r <= deg; r++) for (let j = deg; j >= r; j--) {
        const i = k - deg + j, den = knots[i + deg - r + 1] - knots[i], a = den ? (t - knots[i]) / den : 0;
        d[j] = [(1 - a) * d[j - 1][0] + a * d[j][0], (1 - a) * d[j - 1][1] + a * d[j][1], (1 - a) * d[j - 1][2] + a * d[j][2]];
      }
      const z = d[deg][2] || 1; return [d[deg][0] / z, d[deg][1] / z];
    };
    const n = Math.min(4000, Math.max(24, (ctrl.length - deg) * 24)), pts = [];
    for (let i = 0; i <= n; i++) pts.push(at(lo + (hi - lo) * i / n));
    return { start: pts[0], cmds: pts.slice(1).map(p => ["L", p]), closed };
  }
  /** A HATCH's boundary loops (polyline loops, and edge loops of lines, arcs and ellipses), one closed subpath each. */
  function hatchSubs(e) {
    const c = e.codes, subs = [];
    let i = c.findIndex(p => p[0] === 91); if (i < 0) return subs;
    const loops = parseInt(c[i][1], 10); i++;
    const next = code => { while (i < c.length && c[i][0] !== code) i++; return i < c.length ? parseFloat(c[i++][1]) : NaN; };
    for (let L = 0; L < loops && i < c.length; L++) {
      const type = next(92);
      if (type & 2) {                                                       // a polyline loop
        const hasBulge = next(72), closed = next(73), n = next(93), verts = [];
        for (let v = 0; v < n; v++) { const x = next(10), y = next(20); let b = 0; if (hasBulge) { if (c[i] && c[i][0] === 42) b = parseFloat(c[i++][1]); } verts.push({ p: [x, y], bulge: b }); }
        const s = polyCmds(verts, true); if (s) subs.push(s);
      } else {                                                              // edges: 1 line, 2 arc, 3 ellipse, 4 spline
        const n = next(93); let sub = null;
        const join = part => { if (!sub) sub = { start: part.start, cmds: [], closed: true }; else if (Math.hypot(part.start[0] - endOf(sub)[0], part.start[1] - endOf(sub)[1]) > 1e-6) sub.cmds.push(["L", part.start]); sub.cmds.push(...part.cmds); };
        for (let k = 0; k < n; k++) {
          const et = next(72);
          if (et === 1) { const a = [next(10), next(20)], b = [next(11), next(21)]; join({ start: a, cmds: [["L", b]] }); }
          else if (et === 2) { const cx = next(10), cy = next(20), r = next(40), s = next(50), en = next(51), ccw = next(73); let sw = ((en - s) % 360 + 360) % 360 || 360; let a0 = s; if (!ccw) { a0 = -s; sw = -sw; } join(arcCmds(cx, cy, r, r, 0, a0 * Math.PI / 180, (ccw ? 1 : 1) * sw * Math.PI / 180)); }
          else if (et === 3) { const cx = next(10), cy = next(20), mx = next(11), my = next(21), q = next(40), s = next(50), en = next(51), ccw = next(73); const rx = Math.hypot(mx, my); let sw = ((en - s) % 360 + 360) % 360 || 360; join(arcCmds(cx, cy, rx, rx * q, Math.atan2(my, mx), (ccw ? s : -s) * Math.PI / 180, (ccw ? sw : -sw) * Math.PI / 180)); }
          else { return subs; }                                             // a spline edge: the loop is left out (counted by the caller)
        }
        if (sub) subs.push(sub);
      }
      // the loop's source objects (97, then 330 each) are skipped by next()
    }
    return subs;
  }
  const endOf = s => { const c = s.cmds[s.cmds.length - 1]; return c ? c[c.length - 1] : s.start; };

  /** Every drawable entity, flattened through its blocks, with its transform and colour. */
  const WCS = new Set(["LINE", "SPLINE", "ELLIPSE"]);
  function collect(doc) {
    const out = [], skipped = {};
    const skip = t => { skipped[t] = (skipped[t] || 0) + 1; };
    const layerOf = name => doc.layers.get(name) || { rgb: [0, 0, 0], off: false };
    const mul = (A, B) => [A[0] * B[0] + A[2] * B[1], A[1] * B[0] + A[3] * B[1], A[0] * B[2] + A[2] * B[3], A[1] * B[2] + A[3] * B[3], A[0] * B[4] + A[2] * B[5] + A[4], A[1] * B[4] + A[3] * B[5] + A[5]];
    function walk(list, M, inLayer, inRgb, depth) {
      for (const e of list) {
        let layer = get(e, 8, "0"); if (layer === "0" && inLayer != null) layer = inLayer;   // DXF: a block's layer-0 entities take the insert's layer
        const L = layerOf(layer); if (L.off) { skip("hidden layer"); continue; }
        const tc = get(e, 420), ci = num(e, 62, 256);
        let rgb = tc != null ? trueColour(tc) : ci === 0 ? (inRgb || [0, 0, 0]) : ci === 256 ? L.rgb : aci(ci);
        if (ENGRAVE.test(layer)) rgb = achromatic(rgb) ? [0.85, 0.1, 0.1] : rgb; else if (CUT.test(layer)) rgb = [0, 0, 0];
        // an entity drawn mirrored (extrusion 0,0,-1): its object coordinates are mirrored in x
        const flip = num(e, 230, 1) < 0, OCS = flip ? mul(M, [-1, 0, 0, 1, 0, 0]) : M;
        if (e.type === "INSERT") {
          const b = doc.blocks.get(get(e, 2, "")); if (!b) { skip("missing block"); continue; }
          if (depth > 8) { skip("nested too deep"); continue; }
          const sx = num(e, 41, 1), sy = num(e, 42, 1), rot = num(e, 50, 0) * Math.PI / 180, ix = num(e, 10, 0), iy = num(e, 20, 0);
          const cols = Math.max(1, num(e, 70, 1)), rows = Math.max(1, num(e, 71, 1)), cs = num(e, 44, 0), rs = num(e, 45, 0);
          const cr = Math.cos(rot), sr = Math.sin(rot);
          for (let r = 0; r < rows; r++) for (let k = 0; k < cols; k++) {
            const ox = k * cs, oy = r * rs;   // array spacing is along the insert's own rotated axes
            const T = [cr * sx, sr * sx, -sr * sy, cr * sy, ix + ox * cr - oy * sr, iy + ox * sr + oy * cr];
            walk(b.entities, mul(OCS, mul(T, [1, 0, 0, 1, -b.base[0], -b.base[1]])), layer, rgb, depth + 1);
          }
          continue;
        }
        const subs = [], fill = e.type === "HATCH";
        switch (e.type) {
          case "LINE": subs.push({ start: [num(e, 10, 0), num(e, 20, 0)], cmds: [["L", [num(e, 11, 0), num(e, 21, 0)]]], closed: false }); break;
          case "LWPOLYLINE": {
            const verts = []; let x = null;
            for (const [c, v] of e.codes) { if (c === 10) x = parseFloat(v); else if (c === 20 && x != null) { verts.push({ p: [x, parseFloat(v)], bulge: 0 }); x = null; } else if (c === 42 && verts.length) verts[verts.length - 1].bulge = parseFloat(v); }
            const s = polyCmds(verts, (num(e, 70, 0) & 1) === 1); if (s) subs.push(s); break;
          }
          case "POLYLINE": {
            const f = num(e, 70, 0); if (f & (16 | 64)) { skip("3D mesh"); break; }
            const verts = (e.vertices || []).filter(v => !(num(v, 70, 0) & 16)).map(v => ({ p: [num(v, 10, 0), num(v, 20, 0)], bulge: num(v, 42, 0) }));
            const s = polyCmds(verts, (f & 1) === 1); if (s) subs.push(s); break;
          }
          case "CIRCLE": { const r = num(e, 40, 0); if (r > 0) { const a = arcCmds(num(e, 10, 0), num(e, 20, 0), r, r, 0, 0, 2 * Math.PI); a.closed = true; subs.push(a); } break; }
          case "ARC": {
            const r = num(e, 40, 0), s = num(e, 50, 0), en = num(e, 51, 360);
            const sw = ((en - s) % 360 + 360) % 360 || 360;
            if (r > 0) { const a = arcCmds(num(e, 10, 0), num(e, 20, 0), r, r, 0, s * Math.PI / 180, sw * Math.PI / 180); a.closed = sw >= 360 - 1e-9; subs.push(a); }
            break;
          }
          case "ELLIPSE": {
            const mx = num(e, 11, 0), my = num(e, 21, 0), q = num(e, 40, 1), t0 = num(e, 41, 0), t1 = num(e, 42, 2 * Math.PI), rx = Math.hypot(mx, my);
            let sw = t1 - t0; while (sw <= 0) sw += 2 * Math.PI;
            if (rx > 0) { const a = arcCmds(num(e, 10, 0), num(e, 20, 0), rx, rx * q, Math.atan2(my, mx), t0, sw); a.closed = sw >= 2 * Math.PI - 1e-9; subs.push(a); }
            break;
          }
          case "SPLINE": { const s = splineSub(e); if (s) subs.push(s); break; }
          case "HATCH": { const s = hatchSubs(e); if (!s.length) skip("hatch"); subs.push(...s); break; }
          case "POINT": case "VIEWPORT": case "ATTDEF": case "ATTRIB": case "SEQEND": case "VERTEX": break;
          default: skip(e.type.toLowerCase());
        }
        // LINE, SPLINE and ELLIPSE are drawn in world coordinates; the rest in their object's (mirrored when flipped)
        if (subs.length) out.push({ subs, rgb, layer, M: WCS.has(e.type) ? M : OCS, fill });
      }
    }
    walk(doc.entities, [1, 0, 0, 1, 0, 0], null, null, 0);
    return { items: out, skipped };
  }

  /* ── joining: lines and arcs drawn one by one become one path where their ends meet ── */
  function reverse(s) {
    const pts = [s.start].concat(s.cmds.map(c => c[c.length - 1])), cmds = [];
    for (let i = s.cmds.length - 1; i >= 0; i--) { const c = s.cmds[i], to = pts[i]; cmds.push(c[0] === "L" ? ["L", to] : ["C", c[2], c[1], to]); }
    return { start: pts[pts.length - 1], cmds, closed: s.closed };
  }
  function chain(subs, tol) {
    const open = subs.filter(s => !s.closed), done = subs.filter(s => s.closed);
    const near = (a, b) => Math.abs(a[0] - b[0]) <= tol && Math.abs(a[1] - b[1]) <= tol;
    const cell = p => Math.round(p[0] / (tol * 4)) + "," + Math.round(p[1] / (tol * 4));
    const grid = new Map(), add = (p, s) => { const k = cell(p); if (!grid.has(k)) grid.set(k, new Set()); grid.get(k).add(s); };
    const around = p => { const [cx, cy] = cell(p).split(",").map(Number), out = []; for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) { const g = grid.get((cx + dx) + "," + (cy + dy)); if (g) out.push(...g); } return out; };
    const unlink = s => { for (const p of [s.start, endOf(s)]) { const g = grid.get(cell(p)); if (g) g.delete(s); } };
    for (const s of open) { add(s.start, s); add(endOf(s), s); }
    const used = new Set();
    for (const s0 of open) {
      if (used.has(s0)) continue; used.add(s0); unlink(s0);
      let s = { start: s0.start, cmds: s0.cmds.slice(), closed: false };
      for (let grew = true; grew && !near(s.start, endOf(s)); ) {
        grew = false;
        for (const t of around(endOf(s))) {
          if (used.has(t)) continue;
          const u = near(t.start, endOf(s)) ? t : near(endOf(t), endOf(s)) ? reverse(t) : null; if (!u) continue;
          used.add(t); unlink(t); s.cmds.push(...u.cmds); grew = true; break;
        }
        if (grew) continue;
        for (const t of around(s.start)) {
          if (used.has(t)) continue;
          const u = near(endOf(t), s.start) ? t : near(t.start, s.start) ? reverse(t) : null; if (!u) continue;
          used.add(t); unlink(t); s = { start: u.start, cmds: u.cmds.concat(s.cmds), closed: false }; grew = true; break;
        }
      }
      if (s.cmds.length && near(s.start, endOf(s))) { const last = s.cmds[s.cmds.length - 1]; last[last.length - 1] = s.start; s.closed = true; }
      done.push(s);
    }
    return done;
  }

  /* ── the PDF: one page just larger than the drawing, every path in points ── */
  async function toPdf(input, name) {
    const L = PDF_LIB(); if (!L) throw new Error("the PDF library is not loaded");
    const label = name || "drawing.dxf";
    let doc; try { doc = parse(textOf(input)); } catch (e) { throw new Error(`${label}: ${e.message}`); }
    const { items, skipped } = collect(doc);
    if (!items.length) throw new Error(`${label}: nothing drawn in it that can be cut${Object.keys(skipped).length ? ` (only ${Object.keys(skipped).join(", ")})` : ""}`);
    // units: the header's, else millimetres, else inches for a drawing too small to be a charm in millimetres
    const apply = (M, p) => [M[0] * p[0] + M[2] * p[1] + M[4], M[1] * p[0] + M[3] * p[1] + M[5]];
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const it of items) for (const s of it.subs) for (const p of [s.start].concat(s.cmds.flatMap(c => c.slice(1)))) { const q = apply(it.M, p); if (q[0] < x0) x0 = q[0]; if (q[0] > x1) x1 = q[0]; if (q[1] < y0) y0 = q[1]; if (q[1] > y1) y1 = q[1]; }
    const ins = parseInt(doc.header.$INSUNITS, 10), span = Math.max(x1 - x0, y1 - y0);
    let [units, k] = UNITS[ins] || (span > 0 && span < 3 ? ["in", 72] : ["mm", MM]);
    if (!UNITS[ins]) units += " (assumed)";
    const pad = 12, W = (x1 - x0) * k + 2 * pad, H = (y1 - y0) * k + 2 * pad;
    if (!(W > 2 * pad + 0.5 && H > 2 * pad + 0.5)) throw new Error(`${label}: the drawing has no size`);
    if (W > 14400 || H > 14400) throw new Error(`${label}: the drawing is ${((x1 - x0) * k / MM).toFixed(0)} × ${((y1 - y0) * k / MM).toFixed(0)} mm — too large to be a charm (check its units)`);
    const at = (M, p) => { const q = apply(M, p); return [(q[0] - x0) * k + pad, (q[1] - y0) * k + pad]; };
    // joined per colour and layer, in page points (a hundredth of a millimetre apart counts as touching)
    const groups = new Map();
    for (const it of items) {
      const key = (it.fill ? "F" : "S") + it.rgb.map(v => v.toFixed(3)).join(",") + "|" + it.layer;
      if (!groups.has(key)) groups.set(key, { rgb: it.rgb, fill: it.fill, subs: [] });
      const g = groups.get(key);
      for (const s of it.subs) g.subs.push({ start: at(it.M, s.start), cmds: s.cmds.map(c => c[0] === "L" ? ["L", at(it.M, c[1])] : ["C", at(it.M, c[1]), at(it.M, c[2]), at(it.M, c[3])]), closed: !!s.closed });
    }
    const pdf = await L.PDFDocument.create(), page = pdf.addPage([W, H]);
    const ops = [];
    let paths = 0;
    // cut lines last, so they draw over the engraving (as in the masters)
    const order = [...groups.values()].sort((a, b) => (achromatic(a.rgb) && !a.fill) - (achromatic(b.rgb) && !b.fill));
    for (const g of order) {
      const subs = g.fill ? g.subs : chain(g.subs, 0.01 * MM);
      // each closed outline its own path (the grouping reads one outline per path); open strokes likewise
      const f = n => +n.toFixed(4);
      if (g.fill) {
        ops.push(L.pushGraphicsState(), L.setFillingRgbColor(...g.rgb));
        for (const s of subs) { ops.push(L.moveTo(f(s.start[0]), f(s.start[1]))); for (const c of s.cmds) ops.push(c[0] === "L" ? L.lineTo(f(c[1][0]), f(c[1][1])) : L.appendBezierCurve(f(c[1][0]), f(c[1][1]), f(c[2][0]), f(c[2][1]), f(c[3][0]), f(c[3][1]))); ops.push(L.closePath()); }
        ops.push(L.PDFOperator.of("f*"), L.popGraphicsState()); paths++;
        continue;
      }
      ops.push(L.pushGraphicsState(), L.setStrokingRgbColor(...g.rgb), L.setLineWidth(0.25));
      for (const s of subs) {
        ops.push(L.moveTo(f(s.start[0]), f(s.start[1])));
        for (const c of s.cmds) ops.push(c[0] === "L" ? L.lineTo(f(c[1][0]), f(c[1][1])) : L.appendBezierCurve(f(c[1][0]), f(c[1][1]), f(c[2][0]), f(c[2][1]), f(c[3][0]), f(c[3][1])));
        if (s.closed) ops.push(L.closePath());
        ops.push(L.stroke()); paths++;
      }
      ops.push(L.popGraphicsState());
    }
    page.pushOperators(...ops);
    const bytes = await pdf.save({ useObjectStreams: false });
    return { bytes, meta: { units, widthMm: (x1 - x0) * k / MM, heightMm: (y1 - y0) * k / MM, paths, skipped } };
  }
  const isDxf = (name, bytes) => /\.dxf$/i.test(name || "") || (!!bytes && /^\s*0\s*[\r\n]+\s*SECTION/.test(new TextDecoder("latin1").decode(bytes.subarray(0, 64))));
  return { toPdf, parse, isDxf, aci };
});
