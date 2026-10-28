/* charm-nest-ce-ops.js — THE CHARM EDITOR'S OBJECT COMMANDS: the Illustrator commands a charm designer reaches for most (Paul, 28 Oct 2026: "A lot of the high-end features are
 * missing and I would like you to put all the useful ones in ... I just feel like I need more control and abilities that Photoshop and illustrator both have in this application ...
 * You need to follow the 70/30 Ruel where we're looking only for the most commonly used 70% of the features without overburdening the need and requirements").
 *
 * What is here, and what is NOT built again: the editor (charm-nest-charm-editor.js) already has the studio's align engine (smart guides while dragging), group / ungroup, the
 * clipboard, Duplicate, the four arrange moves (⌘] ⌘[ and the right-click menu), Lock / Hide of the selection and Unlock / Show everything (its More menu); those stay
 * the editor's, and the tree work here is CharmEditor.model's (normaliseAncestors, pushDown, reid, retext, outlinesOf ...). New here:
 *   Pathfinder      Unite · Minus Front · Intersect · Exclude · Divide · Trim, on the repo's own booleans (CharmNestVector.boolean, Clipper at 0.002 mm)
 *   Path            Offset Path (mm; round, mitre or bevel corners) · Cut line round the art (one cut line round the selection at a gap, the lead's "contour cut") ·
 *                   Outline Stroke · Make / Release Compound Path (⌘8 / ⌥⇧⌘8) · Round Corners (a radius, mm)
 *   Align           left / centre / right / top / middle / bottom, to the selection, to a key object, to the charm's outline or to its centre of area (where a heart
 *                   balances, not its box's middle) · Distribute the lefts / centres / rights / tops / middles / bottoms, or the gaps (even, or a value in mm) ·
 *                   Centre on the charm (one click)
 *   Transform       Move (⇧⌘M) / Rotate / Scale / Reflect by values, each with Copy, about the selection, the charm or each thing's own middle · Flip, Rotate 90° ·
 *                   Transform Again (⌘D after a transform with a copy, the lead's keymap; otherwise ⌘D duplicates as it always has; ⇧⌘D repeats any) · Step and Repeat
 *   Select          Same role · Same fill · Same line (colour and width) · Inverse · Lock / Hide everything else · Unlock all · Show all
 * Where they are: the head's Object menu (by group), the right-click menu (the common ones), two flyouts on the property bar (Align and distribute; Pathfinder) that
 * stay open while you click through them, and small value dialogs at the top right of the work area that draw their answer over the charm while Preview is ticked.
 * Every command is ONE undo step. A boolean on big paths (over 3000 chord corners) runs in a Worker (a Blob URL loading Clipper, CharmNestVector and this file) with a
 * "working" line over the work area, so the page never waits; a charm-sized one takes a few ms on the page.
 *
 * CURVES STAY CURVES. Clipper works on polygons, so every operand is cut into chords that stray at most 0.002 mm from its curves, and every chord's corner is
 * remembered by WHERE IT IS (Clipper never moves an input vertex): the polygon that comes back is read edge by edge, every run of edges along one curve of the input is written
 * back as THAT curve (the very same control points when the whole curve survives, the exact piece of it, de Casteljau, when the cut ends on it), and only what has no curve
 * under it (a cut across a chord no input drew) is a straight line. New geometry (an offset, an outlined stroke) has no curve under it: its polygon is fitted with Béziers
 * (Schneider's fit, corners kept, straight runs written as lines).
 *
 * Loads in the page (window.CharmEditorOps; it registers its commands on window.CharmEditor), in a Worker (the kernel only) and in node (module.exports: the kernel and the
 * operations on items, for tests/charm-nest/ce-ops.cjs). The commands use the editor's extension points (CharmEditor.commands / onOpen / onClose / beforeDuplicate and
 * the ctx the editor hands them) and touch nothing else of the editor. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  else { root.CharmEditorOps = api; if (api.install) api.install(); }
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, PT = 72 / 25.4;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r9 = v => { const r = Math.round(v * 1e9) / 1e9; return Object.is(r, -0) ? 0 : r; };
  const clipperLib = () => (root && root.ClipperLib) || (typeof require === 'function' ? (() => { try { return require('./vendor/clipper-6.4.2.js'); } catch (e) { return null; } })() : null);

  /* ═══ 1 · THE KERNEL: plain subpaths in, plain subpaths out (points, y down; ops ["m",[x,y]] ["l",[x,y]] ["c",p1,p2,p3] ["h"]). No editor, no page: it runs in a Worker. ═══ */
  const FLAT = 0.002 / MM;        // a chord strays at most this far from its curve, pt: 0.002 mm, CharmNestVector's own promise
  const FIT = 0.01;               // a fitted curve strays at most this far from the polygon it replaces, pt (3.5 µm: over twice FLAT, so the chords' own wobble is not chased)
  const ARC = 0.002;              // a round corner of an offset is drawn within this, pt
  const ON = 2.5;                 // a point lies on a chord when it is within this many grid units of it (Clipper rounds a crossing to the nearest unit)
  const bez = (a0, a1, a2, a3, t) => { const u = 1 - t; return u * u * u * a0 + 3 * u * u * t * a1 + 3 * u * t * t * a2 + t * t * t * a3; };
  const bezD = (a0, a1, a2, a3, t) => { const u = 1 - t; return 3 * u * u * (a1 - a0) + 6 * u * t * (a2 - a1) + 3 * t * t * (a3 - a2); };
  const lerp = (a, b, t) => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
  const ptAt = (s, t) => (s.c ? [bez(s.p0[0], s.p1[0], s.p2[0], s.p3[0], t), bez(s.p0[1], s.p1[1], s.p2[1], s.p3[1], t)] : t <= 0 ? s.p0 : t >= 1 ? s.p3 : lerp(s.p0, s.p3, t));
  const sub2 = (a, b) => [a[0] - b[0], a[1] - b[1]];
  const len2 = v => Math.hypot(v[0], v[1]);
  const unit = v => { const l = len2(v); return l > 1e-12 ? [v[0] / l, v[1] / l] : [0, 0]; };
  /** The piece of a cubic between t0 and t1 (t1 < t0: the piece walked backwards), exact (de Casteljau). */
  function cubicPiece(s, t0, t1) {
    if (t0 === 0 && t1 === 1) return [s.p0, s.p1, s.p2, s.p3];
    if (t0 === 1 && t1 === 0) return [s.p3, s.p2, s.p1, s.p0];
    const lo = Math.min(t0, t1), hi = Math.max(t0, t1);
    const cut = (P, t) => {   // the left piece [0,t] and the right piece [t,1]
      const a = lerp(P[0], P[1], t), b = lerp(P[1], P[2], t), c = lerp(P[2], P[3], t), d = lerp(a, b, t), e = lerp(b, c, t), f = lerp(d, e, t);
      return [[P[0], a, d, f], [f, e, c, P[3]]];
    };
    let P = [s.p0, s.p1, s.p2, s.p3];
    if (hi < 1) P = cut(P, hi)[0];
    if (lo > 0) P = cut(P, lo / hi)[1];
    return t1 < t0 ? [P[3], P[2], P[1], P[0]] : P;
  }

  /** The segments of subpaths: [{ p0, p1, p2, p3, c (a cubic), sub (which subpath), open }], each subpath closed by a line back to its start (a filled area closes itself). */
  function segmentsOf(sps) {
    const out = [];
    (sps || []).forEach((sub, si) => {
      let cur = null, start = null; const mine = [];
      for (const op of sub || []) {
        const k = op && op[0];
        if (k === 'm') { cur = op[1]; start = op[1]; }
        else if (k === 'l' && cur) { if (cur[0] !== op[1][0] || cur[1] !== op[1][1]) mine.push({ p0: cur, p1: cur, p2: op[1], p3: op[1], c: false, sub: si }); cur = op[1]; }
        else if (k === 'c' && cur) { mine.push({ p0: cur, p1: op[1], p2: op[2], p3: op[3], c: true, sub: si }); cur = op[3]; }
      }
      const closed = (sub || []).some(op => op && op[0] === 'h');
      if (cur && start && (cur[0] !== start[0] || cur[1] !== start[1])) mine.push({ p0: cur, p1: cur, p2: start, p3: start, c: false, sub: si, closing: true });
      mine.forEach(s => { s.open = !closed; out.push(s); });
    });
    return out;
  }
  /** How many chords a cubic needs to stay within tol of itself (n chords keep a cubic within 3/4 · max|second difference| / n² of itself). */
  function chordsFor(s, tol) {
    if (!s.c) return 1;
    const dd = Math.max(Math.hypot(s.p0[0] - 2 * s.p1[0] + s.p2[0], s.p0[1] - 2 * s.p1[1] + s.p2[1]), Math.hypot(s.p1[0] - 2 * s.p2[0] + s.p3[0], s.p1[1] - 2 * s.p2[1] + s.p3[1]));
    return clamp(Math.ceil(Math.sqrt(0.75 * dd / tol)), 1, 1024);
  }

  /* ── THE TAGGED POLYGONS: every operand cut into chords, every chord corner remembered by where it is on the integer grid the booleans work on ── */
  // (the booleans are the repo's own, CharmNestVector.boolean: Clipper at SCALE integers a point; a corner keeps its integers through it, so it is found again by them)
  const vec = () => (root && root.CharmNestVector) || (typeof require === 'function' ? (() => { try { return require('./charm-nest-vector.js'); } catch (e) { return null; } })() : null);
  let SC = 1e5;
  const grid = () => { const V = vec(); if (!V) throw new Error('CharmNestVector is not on the page'); SC = V.SCALE || 1e5; return V; };
  const keyOf = (x, y) => Math.round(x * SC) + ':' + Math.round(y * SC);
  class Tags {
    constructor() { this.segs = []; this.at = new Map(); }
    add(key, s, k) { let l = this.at.get(key); if (!l) this.at.set(key, l = []); l.push(s, k); }
    /** An operand's subpaths as rings of points on the grid, each corner tagged with (segment, chord index). */
    rings(sps, tol) {
      const out = [];
      const bySub = new Map();
      for (const s of segmentsOf(sps)) { if (!bySub.has(s.sub)) bySub.set(s.sub, []); bySub.get(s.sub).push(s); }
      for (const list of bySub.values()) {
        const ring = [];
        let lx = NaN, ly = NaN;
        for (const s of list) {
          s.id = this.segs.length; s.n = chordsFor(s, tol); this.segs.push(s);
          for (let k = 0; k <= s.n; k++) {
            const p = k === 0 ? s.p0 : k === s.n ? s.p3 : ptAt(s, k / s.n);
            const X = Math.round(p[0] * SC), Y = Math.round(p[1] * SC);
            this.add(X + ':' + Y, s.id, k);
            if (X !== lx || Y !== ly) { ring.push([X / SC, Y / SC]); lx = X; ly = Y; }
          }
        }
        while (ring.length > 1 && keyOf(...ring[0]) === keyOf(...ring[ring.length - 1])) ring.pop();
        if (ring.length >= 3) out.push(ring);
      }
      return out;
    }
  }
  /** The parameter on segment s of a point that lies on its chord k..k+1 (within ON grid units), or null. */
  function onChord(s, k, x, y) {
    if (k < 0 || k >= s.n) return null;
    const a = k === 0 ? s.p0 : ptAt(s, k / s.n), b = k + 1 === s.n ? s.p3 : ptAt(s, (k + 1) / s.n);
    const X = x * SC, Y = y * SC, ax = a[0] * SC, ay = a[1] * SC, dx = b[0] * SC - ax, dy = b[1] * SC - ay, L = dx * dx + dy * dy;
    if (L < 1e-9) return null;
    const rl = Math.sqrt(L), u = ((X - ax) * dx + (Y - ay) * dy) / L;
    if (u < -ON / rl || u > 1 + ON / rl) return null;
    const d = Math.abs((X - ax) * dy - (Y - ay) * dx) / rl;
    if (d > ON) return null;
    let t = (k + clamp(u, 0, 1)) / s.n;
    if (s.c) {   // (onto the curve itself: Newton steps from the chord's guess)
      for (let i = 0; i < 3; i++) {
        const px = bez(s.p0[0], s.p1[0], s.p2[0], s.p3[0], t) - x, py = bez(s.p0[1], s.p1[1], s.p2[1], s.p3[1], t) - y;
        const dxt = bezD(s.p0[0], s.p1[0], s.p2[0], s.p3[0], t), dyt = bezD(s.p0[1], s.p1[1], s.p2[1], s.p3[1], t);
        const den = dxt * dxt + dyt * dyt; if (den < 1e-18) break;
        t = clamp(t - (px * dxt + py * dyt) / den, (k - 0.5) / s.n, (k + 1.5) / s.n);
      }
      t = clamp(t, 0, 1);
    }
    return { t, d };
  }
  /** One ring the booleans gave back, as a subpath with the input's own curves wherever its edges run along them. */
  function rebuildRing(ring, tags) {
    const n = ring.length; if (n < 3) return null;
    const T = ring.map(p => tags.at.get(keyOf(p[0], p[1])) || null);
    const att = new Array(n);
    // pass 1: an edge between two tagged corners of one segment's chords; or an edge from a new corner (a crossing) along a chord that ends at a tagged one
    for (let i = 0; i < n; i++) {
      const j = (i + 1) % n, A = T[i], B = T[j];
      let best = null;
      if (A && B) {
        for (let a = 0; a < A.length && !(best && best.dk === 1); a += 2) for (let b = 0; b < B.length; b += 2) {
          if (A[a] !== B[b]) continue;
          const s = tags.segs[A[a]], dk = Math.abs(A[a + 1] - B[b + 1]);
          if (dk < 1) continue;
          if (!best || dk < best.dk) best = { s: A[a], ta: A[a + 1] / s.n, tb: B[b + 1] / s.n, dk };
          if (dk === 1) break;
        }
      }
      if (!best) {
        // one end (or both) is not an input corner of this edge's segment: the edge runs along a chord that has the other end as a corner
        const tryFrom = (Tk, other, otherIsB) => {
          if (!Tk) return null;
          let r = null;
          for (let q = 0; q < Tk.length; q += 2) {
            const s = tags.segs[Tk[q]], k = Tk[q + 1];
            for (const kk of [k - 1, k]) {
              const hit = onChord(s, kk, other[0], other[1]); if (!hit) continue;
              if (!r || hit.d < r.d) r = otherIsB ? { s: s.id, ta: k / s.n, tb: hit.t, d: hit.d } : { s: s.id, ta: hit.t, tb: k / s.n, d: hit.d };
            }
          }
          return r;
        };
        best = tryFrom(A, ring[j], true) || tryFrom(B, ring[i], false);
      }
      att[i] = best;
    }
    // pass 2: an edge between two crossings (a sliver along one chord): the chord a neighbouring edge found for either end
    for (let i = 0; i < n; i++) {
      if (att[i]) continue;
      const j = (i + 1) % n;
      for (const nb of [att[(i + n - 1) % n], att[j]]) {
        if (!nb) continue;
        const s = tags.segs[nb.s], tRef = nb === att[j] ? nb.ta : nb.tb, k = Math.min(s.n - 1, Math.floor(tRef * s.n));
        let h1 = null, h2 = null;
        for (const kk of [k - 1, k, k + 1]) { h1 = h1 || onChord(s, kk, ring[i][0], ring[i][1]); h2 = h2 || onChord(s, kk, ring[j][0], ring[j][1]); }
        if (h1 && h2) { att[i] = { s: s.id, ta: h1.t, tb: h2.t }; break; }
      }
    }
    // runs: edges along one segment walking on in one direction become one piece of it
    const same = (a, b) => a && b && a.s === b.s && Math.abs(a.tb - b.ta) < 1e-9 && Math.sign(a.tb - a.ta) === Math.sign(b.tb - b.ta) && a.tb !== a.ta;
    let i0 = 0;
    for (let i = 0; i < n; i++) if (!same(att[(i + n - 1) % n], att[i])) { i0 = i; break; }
    const runs = [];
    for (let q = 0; q < n; q++) {
      const i = (i0 + q) % n, a = att[i];
      const last = runs[runs.length - 1];
      if (q > 0 && last && last.s != null && same(last.lastAtt, a)) { last.tb = a.tb; last.lastAtt = a; last.to = (i + 1) % n; continue; }
      runs.push(a ? { s: a.s, ta: a.ta, tb: a.tb, lastAtt: a, from: i, to: (i + 1) % n } : { s: null, from: i, to: (i + 1) % n });
    }
    // the corners between runs: an input corner where it is one (exact), else where the run's own curve puts it
    const runEnd = r => (r.s == null ? ring[r.to] : ptAt(tags.segs[r.s], r.tb));
    const runStart = r => (r.s == null ? ring[r.from] : ptAt(tags.segs[r.s], r.ta));
    const sp = [];
    let cur = runStart(runs[0]);
    sp.push(['m', cur]);
    runs.forEach((r, ri) => {
      const isLast = ri === runs.length - 1;
      const end = isLast ? sp[0][1] : runEnd(r);
      if (r.s == null || !tags.segs[r.s].c) {
        if (!isLast) sp.push(['l', end]);
      } else {
        const Cb = cubicPiece(tags.segs[r.s], r.ta, r.tb);
        // (the piece begins where the last one ended and ends where the next begins; the gap is under a micron, its handles keep their direction)
        const d0 = sub2(cur, Cb[0]), d1 = sub2(end, Cb[3]);
        sp.push(['c', [Cb[1][0] + d0[0], Cb[1][1] + d0[1]], [Cb[2][0] + d1[0], Cb[2][1] + d1[1]], end]);
      }
      cur = end;
    });
    sp.push(['h']);
    return sp;
  }

  /* ── the booleans (CharmNestVector.boolean), once per operand and then between them ── */
  const ringArea = r => { let a = 0; for (let i = 0, j = r.length - 1; i < r.length; j = i++) a += (r[j][0] + r[i][0]) * (r[j][1] - r[i][1]); return a / 2; };
  const areaOfRings = rings => rings.reduce((a, r) => a + ringArea(r), 0);
  /** rings op rings -> the result's nodes [{ points, hole, depth }] (depth-first: an outline, then what is inside it). */
  function boolNodes(op, subj, clip, ruleS, ruleC) {
    const V = grid();
    if ((V.ops || []).includes(op) || op === 'union' || op === 'difference') return V.boolean(subj || [], clip || [], op, ruleS || 'nonzero', ruleC || 'nonzero');
    // (a CharmNestVector without intersection / xor: A ∩ B = A − (A − B), A ⊕ B = (A − B) ∪ (B − A))
    const P = nodes => nodes.map(o => o.points);
    if (op === 'intersection') return V.boolean(subj, P(V.boolean(subj, clip, 'difference', ruleS, ruleC)), 'difference', ruleS, 'nonzero');
    if (op === 'xor') return V.boolean(P(V.boolean(subj, clip, 'difference', ruleS, ruleC)), P(V.boolean(clip, subj, 'difference', ruleC, ruleS)), 'union');
    throw new Error('boolean: ' + op);
  }
  const bool = (op, subj, clip, ruleS, ruleC) => boolNodes(op, subj, clip, ruleS, ruleC).map(o => o.points);
  /** An operand as ONE clean shape (its own rule: non-zero, or even-odd when it says so), every ring wound so non-zero fills it. */
  function shapeOf(tags, operand) {
    // (a group is one operand, each of its parts with its own rule)
    if (Array.isArray(operand.parts)) { const all = [].concat(...operand.parts.map(p => shapeOf(tags, p))); return operand.parts.length > 1 ? bool('union', all, []) : all; }
    return bool('union', tags.rings(operand.subpaths, FLAT), [], operand.evenOdd ? 'evenodd' : 'nonzero', 'nonzero');
  }
  const subpathsOfOperand = o => (Array.isArray(o) ? o : Array.isArray(o.parts) ? [].concat(...o.parts.map(subpathsOfOperand)) : o.subpaths || []);
  /** Islands of a result: each outline with its holes. */
  function islands(nodes) {
    const out = [], at = [];
    for (const o of nodes) { if (!o.hole) { at[o.depth] = out.length; out.push([o.points]); } else if (at[o.depth - 1] != null) out[at[o.depth - 1]].push(o.points); }
    return out;
  }
  const MIN_AREA = 1e-4;   // pt²: a piece smaller than a hundredth of a point square (1.3 µm²) is a sliver of rounding, not a shape
  const rebuild = (rings, tags) => rings.filter(r => r.length >= 3 && Math.abs(ringArea(r)) > MIN_AREA).map(r => rebuildRing(r, tags)).filter(Boolean);

  /** PATHFINDER. operands: [{ subpaths, evenOdd }] from the BACK of the stack to the front (points). op: 'unite' | 'minus' | 'intersect' | 'exclude' | 'divide' | 'trim'.
   *  -> { subpaths } (unite, minus, intersect, exclude: one shape; empty subpaths when nothing is left)
   *     { pieces: [{ subpaths, of }] }  (divide: every face of the overlap, `of` the frontmost operand over it; trim: each operand less what is in front of it, `of` itself) */
  function pathfinder(operands, op) {
    grid();
    const tags = new Tags();
    const shapes = operands.map(o => shapeOf(tags, o));
    if (op === 'unite') return { subpaths: rebuild(bool('union', [].concat(...shapes), []), tags) };
    if (op === 'minus') return { subpaths: rebuild(bool('difference', shapes[0], [].concat(...shapes.slice(1))), tags) };
    if (op === 'intersect' || op === 'exclude') {
      let acc = shapes[0];
      for (let i = 1; i < shapes.length; i++) acc = bool(op === 'intersect' ? 'intersection' : 'xor', acc, shapes[i]);
      return { subpaths: rebuild(acc, tags) };
    }
    if (op === 'trim') {
      const pieces = [];
      for (let i = 0; i < shapes.length; i++) {
        const above = [].concat(...shapes.slice(i + 1));
        const left = above.length ? bool('difference', shapes[i], above) : shapes[i];
        const sps = rebuild(left, tags); if (sps.length) pieces.push({ subpaths: sps, of: i });
      }
      return { pieces };
    }
    if (op === 'divide') {
      // the faces of the overlap: each region knows the frontmost operand over it; a new operand splits every region into what it covers and what it does not
      let regions = [];
      shapes.forEach((sh, i) => {
        const next = [];
        for (const r of regions) {
          const inn = bool('intersection', r.rings, sh), out = bool('difference', r.rings, sh);
          if (Math.abs(areaOfRings(inn)) > MIN_AREA) next.push({ rings: inn, top: i });
          if (Math.abs(areaOfRings(out)) > MIN_AREA) next.push({ rings: out, top: r.top });
        }
        const prior = [].concat(...shapes.slice(0, i));
        const fresh = prior.length ? bool('difference', sh, prior) : sh;
        if (Math.abs(areaOfRings(fresh)) > MIN_AREA) next.push({ rings: fresh, top: i });
        regions = next;
      });
      const pieces = [];
      for (const r of regions) for (const isl of islands(boolNodes('union', r.rings, []))) {
        const sps = rebuild(isl, tags); if (sps.length) pieces.push({ subpaths: sps, of: r.top });
      }
      return { pieces };
    }
    throw new Error('pathfinder: unknown op ' + op);
  }

  /* ── fitting Béziers to a polygon (Schneider, Graphics Gems I: chord-length parameters, least squares, Newton reparameterisation, split at the worst point) ── */
  function fitPolyline(pts, closed, tol) {
    // corners: where the polygon turns more than 26° at one vertex (an offset's round corner turns a few degrees a step, a mitre a right angle)
    const n = pts.length;
    if (n < 2) return null;
    const isCorner = new Array(n).fill(false);
    const COS = Math.cos(26 * Math.PI / 180);
    if (closed) for (let i = 0; i < n; i++) {
      const a = pts[(i + n - 1) % n], b = pts[i], c = pts[(i + 1) % n];
      const u = unit(sub2(b, a)), v = unit(sub2(c, b));
      if (u[0] * v[0] + u[1] * v[1] < COS) isCorner[i] = true;
    } else { isCorner[0] = true; isCorner[n - 1] = true; for (let i = 1; i < n - 1; i++) { const u = unit(sub2(pts[i], pts[i - 1])), v = unit(sub2(pts[i + 1], pts[i])); if (u[0] * v[0] + u[1] * v[1] < COS) isCorner[i] = true; } }
    const L = pts.map((p, i) => (i < n - 1 || closed ? len2(sub2(pts[(i + 1) % n], p)) : 0));
    const out = [];
    const tol2 = tol * tol;
    const emit = (seg) => {
      // a cubic whose handles lie on its chord is a line
      const [p0, p1, p2, p3] = seg, ch = sub2(p3, p0), cl = len2(ch);
      if (cl > 1e-9) {
        const dist = p => Math.abs((p[0] - p0[0]) * ch[1] - (p[1] - p0[1]) * ch[0]) / cl;
        const along = p => ((p[0] - p0[0]) * ch[0] + (p[1] - p0[1]) * ch[1]) / (cl * cl);
        if (dist(p1) < tol * 0.25 && dist(p2) < tol * 0.25 && along(p1) > -1e-6 && along(p1) < 1 + 1e-6 && along(p2) > -1e-6 && along(p2) < 1 + 1e-6) { out.push(['l', p3]); return; }
      }
      out.push(['c', p1, p2, p3]);
    };
    // the tangent at a run's end from its first three points (a parabola through them: half a step's angle better than the first chord, which is what lets a quarter
    // circle of an offset's corner be ONE curve)
    const endTan = (a, b, c) => {
      const ch = unit(sub2(b, a)); if (!c) return ch;
      const t = unit([4 * b[0] - 3 * a[0] - c[0], 4 * b[1] - 3 * a[1] - c[1]]);
      return t[0] * ch[0] + t[1] * ch[1] > 0.9 ? t : ch;
    };
    const fitRun = P => {
      if (P.length === 2) { out.push(['l', P[1]]); return; }
      const m = P.length;
      fitCubic(P, 0, m - 1, endTan(P[0], P[1], P[2]), endTan(P[m - 1], P[m - 2], P[m - 3]), tol2, emit, 0);
    };
    // a long straight edge is a line whatever the angle at its ends (a round-cornered offset of a square is four lines and four arcs, not eleven curves): an edge over 4 pt
    // (1.4 mm) and four times its neighbours. (A chord of a flattened curve is that long only on a curve of 500 pt radius and more, which a line draws within the tolerance.)
    for (let i = 0; i < n; i++) {
      if (!closed && i === n - 1) break;
      const prev = closed ? L[(i + n - 1) % n] : (i > 0 ? L[i - 1] : 0), next = closed ? L[(i + 1) % n] : (i < n - 2 ? L[i + 1] : 0);
      if (L[i] > 4 && L[i] > 4 * Math.max(prev, next)) { isCorner[i] = true; isCorner[(i + 1) % n] = true; }
    }
    let start = closed ? isCorner.indexOf(true) : 0;
    if (closed && start < 0) {
      // a smooth loop: begin anywhere, the two ends sharing one tangent
      const t = unit(sub2(pts[1], pts[n - 1]));
      out.push(['m', pts[0]]);
      fitCubic(pts.concat([pts[0]]), 0, n, t, [-t[0], -t[1]], tol2, emit, 0);
      out.push(['h']);
      return out;
    }
    out.push(['m', pts[start]]);
    let run = [pts[start]];
    const total = closed ? n : n - 1;
    for (let q = 1; q <= total; q++) {
      const i = (start + q) % n;
      run.push(pts[i]);
      if (isCorner[i] || q === total) { fitRun(run); run = [pts[i]]; }
    }
    if (closed) { if (out.length > 1 && out[out.length - 1][0] === 'l') out.pop(); out.push(['h']); }
    return out;
  }
  function fitCubic(P, first, last, tHat1, tHat2, err2, emit, depth) {
    const nPts = last - first + 1;
    if (nPts === 2) {
      const dist = len2(sub2(P[last], P[first])) / 3;
      emit([P[first], [P[first][0] + tHat1[0] * dist, P[first][1] + tHat1[1] * dist], [P[last][0] + tHat2[0] * dist, P[last][1] + tHat2[1] * dist], P[last]]);
      return;
    }
    let u = chordParams(P, first, last);
    let bz = generateBezier(P, first, last, u, tHat1, tHat2);
    let me = maxError(P, first, last, bz, u);
    if (me.err < err2) { emit(bz); return; }
    if (me.err < err2 * 16) {
      for (let i = 0; i < 12; i++) {
        const u2 = reparam(P, first, last, u, bz);
        bz = generateBezier(P, first, last, u2, tHat1, tHat2);
        me = maxError(P, first, last, bz, u2);
        if (me.err < err2) { emit(bz); return; }
        u = u2;
      }
    }
    if (depth > 40) { for (let i = first + 1; i <= last; i++) emit([P[i - 1], P[i - 1], P[i], P[i]]); return; }
    const sp = clamp(me.at, first + 1, last - 1);
    const tc = unit(sub2(P[sp - 1], P[sp + 1]));
    fitCubic(P, first, sp, tHat1, tc, err2, emit, depth + 1);
    fitCubic(P, sp, last, [-tc[0], -tc[1]], tHat2, err2, emit, depth + 1);
  }
  function chordParams(P, first, last) {
    const u = [0];
    for (let i = first + 1; i <= last; i++) u.push(u[u.length - 1] + len2(sub2(P[i], P[i - 1])));
    const L = u[u.length - 1] || 1; return u.map(v => v / L);
  }
  function generateBezier(P, first, last, u, t1, t2) {
    const p0 = P[first], p3 = P[last];
    let c00 = 0, c01 = 0, c11 = 0, x0 = 0, x1 = 0;
    for (let i = 0; i < u.length; i++) {
      const t = u[i], s = 1 - t, b0 = s * s * s, b1 = 3 * t * s * s, b2 = 3 * t * t * s, b3 = t * t * t;
      const a1 = [t1[0] * b1, t1[1] * b1], a2 = [t2[0] * b2, t2[1] * b2];
      c00 += a1[0] * a1[0] + a1[1] * a1[1]; c01 += a1[0] * a2[0] + a1[1] * a2[1]; c11 += a2[0] * a2[0] + a2[1] * a2[1];
      const q = P[first + i], tmp = [q[0] - (p0[0] * (b0 + b1) + p3[0] * (b2 + b3)), q[1] - (p0[1] * (b0 + b1) + p3[1] * (b2 + b3))];
      x0 += a1[0] * tmp[0] + a1[1] * tmp[1]; x1 += a2[0] * tmp[0] + a2[1] * tmp[1];
    }
    const det = c00 * c11 - c01 * c01;
    let al = det ? (x0 * c11 - x1 * c01) / det : 0, ar = det ? (c00 * x1 - c01 * x0) / det : 0;
    const segLen = len2(sub2(p3, p0)), eps = 1e-6 * segLen;
    if (!(al >= eps) || !(ar >= eps) || al > segLen * 4 || ar > segLen * 4) { al = ar = segLen / 3; }
    return [p0, [p0[0] + t1[0] * al, p0[1] + t1[1] * al], [p3[0] + t2[0] * ar, p3[1] + t2[1] * ar], p3];
  }
  const bzAt = (b, t) => [bez(b[0][0], b[1][0], b[2][0], b[3][0], t), bez(b[0][1], b[1][1], b[2][1], b[3][1], t)];
  function maxError(P, first, last, bz, u) {
    let err = 0, at = Math.floor((first + last) / 2);
    for (let i = first + 1; i < last; i++) { const p = bzAt(bz, u[i - first]), d = (p[0] - P[i][0]) ** 2 + (p[1] - P[i][1]) ** 2; if (d >= err) { err = d; at = i; } }
    return { err, at };
  }
  function reparam(P, first, last, u, bz) {
    const d1 = [0, 1, 2].map(i => [3 * (bz[i + 1][0] - bz[i][0]), 3 * (bz[i + 1][1] - bz[i][1])]);
    const d2 = [0, 1].map(i => [2 * (d1[i + 1][0] - d1[i][0]), 2 * (d1[i + 1][1] - d1[i][1])]);
    return u.map((t, i) => {
      const p = bzAt(bz, t), q = P[first + i], s = 1 - t;
      const D1 = [d1[0][0] * s * s + 2 * d1[1][0] * s * t + d1[2][0] * t * t, d1[0][1] * s * s + 2 * d1[1][1] * s * t + d1[2][1] * t * t];
      const D2 = [d2[0][0] * s + d2[1][0] * t, d2[0][1] * s + d2[1][1] * t];
      const num2 = (p[0] - q[0]) * D1[0] + (p[1] - q[1]) * D1[1], den = D1[0] * D1[0] + D1[1] * D1[1] + (p[0] - q[0]) * D2[0] + (p[1] - q[1]) * D2[1];
      return den ? clamp(t - num2 / den, 0, 1) : t;
    });
  }
  /** Rings of points as smooth subpaths: fitted, corners and straight runs kept. */
  const fitRings = (rings, tol) => rings.filter(r => r.length >= 3 && Math.abs(ringArea(r)) > MIN_AREA).map(r => fitPolyline(r, true, tol || FIT)).filter(Boolean);
  // (the offsets are Clipper's own ClipperOffset, as the editor's Weight is: CharmNestVector has none)
  const CL = () => { const L = clipperLib(); if (!L) throw new Error('ClipperLib is not on the page'); return L; };
  const toInt = r => r.map(p => ({ X: Math.round(p[0] * SC), Y: Math.round(p[1] * SC) }));
  const fromInt = r => r.map(p => [p.X / SC, p.Y / SC]);

  /** OFFSET PATH: the shape grown (shrunk, for a negative d) by d pt on every side, its corners round, mitred (up to miterLimit × d, then bevelled, as Illustrator does) or
   *  bevelled. An open subpath is grown round its line (a closed outline round it). -> subpaths, fitted with curves. */
  function offsetPath(operand, d, join, miterLimit, evenOdd, fillHoles) {
    grid();
    const L = CL(), tags = new Tags();
    // (operand: subpaths, or { subpaths, evenOdd }, or a group's { parts }; its closed subpaths are grown as a shape, its open ones round their lines)
    const isClosed = sub => (sub || []).some(op => op && op[0] === 'h');
    const closedOnly = o => (Array.isArray(o) ? { subpaths: o.filter(isClosed), evenOdd } : Array.isArray(o.parts) ? { parts: o.parts.map(closedOnly) } : { subpaths: (o.subpaths || []).filter(isClosed), evenOdd: !!o.evenOdd });
    const openSps = subpathsOfOperand(operand).filter(sub => !isClosed(sub) && (sub || []).length);
    const shape = subpathsOfOperand(operand).some(isClosed) ? shapeOf(tags, closedOnly(operand)) : [];
    const JT = join === 'round' ? L.JoinType.jtRound : join === 'bevel' ? L.JoinType.jtSquare : L.JoinType.jtMiter;
    const co = new L.ClipperOffset(Math.max(1, num(miterLimit, 4)), ARC * SC);
    bevelOn(co, L);
    if (shape.length) co.AddPaths(shape.map(toInt), JT, L.EndType.etClosedPolygon);
    for (const sub of openSps) { const line = polylineOf(sub); if (line.length >= 2) co.AddPath(toInt(line), JT, join === 'round' ? L.EndType.etOpenRound : L.EndType.etOpenButt); }
    const out = new L.Paths();
    co.Execute(out, (openSps.length ? Math.abs(d) : d) * SC);
    let rings = out.map(fromInt);
    // (a cut line round the art: its outlines only, every hole and every island in a hole filled, the parts made one)
    if (fillHoles) rings = boolNodes('union', rings, []).filter(o => !o.hole && o.depth === 0).map(o => o.points);
    return fitRings(rings);
  }
  /** Clipper squares a corner off at the offset's distance; a BEVEL joins the two offset edges straight across (its DoSquare with no slant). Used for 'bevel', and for a mitre past
   *  its limit, as Illustrator bevels it. Only this one ClipperOffset is changed. */
  function bevelOn(co, L) {
    co.DoSquare = function (j, k) {
      const R = L.ClipperOffset.Round, s = this.m_srcPoly[j], d = this.m_delta, nk = this.m_normals[k], nj = this.m_normals[j];
      this.m_destPoly.push(new L.IntPoint2(R(s.X + d * nk.X), R(s.Y + d * nk.Y)));
      this.m_destPoly.push(new L.IntPoint2(R(s.X + d * nj.X), R(s.Y + d * nj.Y)));
    };
  }
  /** A subpath as one polyline of points, its curves cut into chords. */
  function polylineOf(sub) {
    const out = [];
    for (const s of segmentsOf([sub])) {
      if (s.closing && s.open) continue;
      const n = chordsFor(s, FLAT);
      for (let k = out.length ? 1 : 0; k <= n; k++) out.push(ptAt(s, k / n));
    }
    return out;
  }
  /** OUTLINE STROKE: the area a line of width w pt covers (round joins and round ends, as the editor draws a line) as a filled shape. */
  function outlineStroke(sps, w) {
    grid();
    const L = CL(), co = new L.ClipperOffset(4, ARC * SC);
    for (const sub of sps || []) {
      const closed = (sub || []).some(op => op && op[0] === 'h'), line = toInt(polylineOf(sub));
      if (line.length < 2) continue;
      if (closed) while (line.length > 1 && line[0].X === line[line.length - 1].X && line[0].Y === line[line.length - 1].Y) line.pop();
      co.AddPath(line, L.JoinType.jtRound, closed ? L.EndType.etClosedLine : L.EndType.etOpenRound);
    }
    const out = new L.Paths();
    co.Execute(out, Math.max(0.001, w / 2) * SC);
    return fitRings(out.map(fromInt));
  }

  /* ── round corners ── */
  /** Arc length of a segment from t0 to t1 (chords: plenty for a fillet's reach). */
  function arcLen(s, t0, t1) {
    if (!s.c) return len2(sub2(s.p3, s.p0)) * Math.abs(t1 - t0);
    let L = 0, prev = ptAt(s, t0); const n = 32;
    for (let i = 1; i <= n; i++) { const p = ptAt(s, t0 + (t1 - t0) * i / n); L += len2(sub2(p, prev)); prev = p; }
    return L;
  }
  /** The parameter that is `d` along the segment from its start (fromEnd: back from its end). */
  function tAtLen(s, d, fromEnd) {
    const total = arcLen(s, 0, 1); if (!(total > 0)) return fromEnd ? 1 : 0;
    if (!s.c) return fromEnd ? 1 - d / total : d / total;
    let lo = 0, hi = 1;
    for (let i = 0; i < 40; i++) { const m = (lo + hi) / 2, l = fromEnd ? arcLen(s, m, 1) : arcLen(s, 0, m); if ((l < d) !== fromEnd) lo = m; else hi = m; }
    return (lo + hi) / 2;
  }
  const tanEnd = s => { const v = s.c ? (len2(sub2(s.p3, s.p2)) > 1e-9 ? sub2(s.p3, s.p2) : len2(sub2(s.p3, s.p1)) > 1e-9 ? sub2(s.p3, s.p1) : sub2(s.p3, s.p0)) : sub2(s.p3, s.p0); return unit(v); };
  const tanStart = s => { const v = s.c ? (len2(sub2(s.p1, s.p0)) > 1e-9 ? sub2(s.p1, s.p0) : len2(sub2(s.p2, s.p0)) > 1e-9 ? sub2(s.p2, s.p0) : sub2(s.p3, s.p0)) : sub2(s.p3, s.p0); return unit(v); };
  /** ROUND CORNERS: every corner of the subpaths (where the path turns at a point, more than 3°) rounded with an arc of radius r pt that meets both sides tangentially; a radius
   *  too big for a side is cut down so two arcs on one side never cross (each takes at most half of it). Smooth points and the ends of an open path are left alone. */
  function roundCorners(sps, r) {
    if (!(r > 0)) return sps;
    const out = [];
    for (const sub of sps || []) {
      const closed = (sub || []).some(op => op && op[0] === 'h');
      const segs = segmentsOf([sub]).filter(s => !(s.closing && !closed));
      if (!segs.length) { out.push(sub); continue; }
      const nS = segs.length, lens = segs.map(s => arcLen(s, 0, 1));
      // the corner at the end of segment i (between i and i+1)
      const corners = segs.map((s, i) => {
        if (!closed && i === nS - 1) return null;
        const nx = segs[(i + 1) % nS], a = tanEnd(s), b = tanStart(nx);
        const cos = clamp(a[0] * b[0] + a[1] * b[1], -1, 1), turn = Math.acos(cos);
        if (turn < 3 * Math.PI / 180 || turn > Math.PI - 1e-3) return null;
        return { turn, d: r * Math.tan(turn / 2) };
      });
      for (let i = 0; i < nS; i++) {
        const c = corners[i]; if (!c) continue;
        const j = (i + 1) % nS;
        c.d = Math.min(c.d, lens[i] * (corners[(i + nS - 1) % nS] && (closed || i > 0) ? 0.5 : 1), lens[j] * (corners[j] && (closed || j < nS - 1) ? 0.5 : 1));
        c.r = c.d / Math.tan(c.turn / 2);
      }
      const sp = [];
      const trimmed = segs.map((s, i) => {
        const cin = corners[(i + nS - 1) % nS], cout = corners[i];
        const dIn = cin && (closed || i > 0) ? cin.d : 0, dOut = cout ? cout.d : 0;
        if (!dIn && !dOut) return s;
        const t0 = dIn ? tAtLen(s, dIn, false) : 0, t1 = dOut ? tAtLen(s, dOut, true) : 1;
        if (!s.c) return { p0: lerp(s.p0, s.p3, t0), p1: null, p2: null, p3: lerp(s.p0, s.p3, t1), c: false };
        const P = cubicPiece(s, t0, t1); return { p0: P[0], p1: P[1], p2: P[2], p3: P[3], c: true };
      });
      sp.push(['m', trimmed[0].p0]);
      for (let i = 0; i < nS; i++) {
        const s = trimmed[i];
        sp.push(s.c ? ['c', s.p1, s.p2, s.p3] : ['l', s.p3]);
        const c = corners[i];
        if (c && (closed || i < nS - 1)) {
          const nx = trimmed[(i + 1) % nS], a = tanEnd(segs[i]), b = tanStart(segs[(i + 1) % nS]);
          const ta = s.c ? tanEnd(s) : a, tb = nx.c ? tanStart(nx) : b;
          const h = 4 / 3 * Math.tan(c.turn / 4) * c.r;
          sp.push(['c', [s.p3[0] + ta[0] * h, s.p3[1] + ta[1] * h], [nx.p0[0] - tb[0] * h, nx.p0[1] - tb[1] * h], nx.p0]);
        }
      }
      if (closed) { if (sp[sp.length - 1][0] === 'l') sp.pop(); sp.push(['h']); }
      out.push(sp.map(op => op.map(v => (Array.isArray(v) ? [r9(v[0]), r9(v[1])] : v))));
    }
    return out;
  }

  /* ── measures: the exact area of curves (Green's theorem, three-point Gauss: exact for a cubic), the centre of area ── */
  const GL = [[-0.7745966692414834, 0.5555555555555556], [0, 0.8888888888888888], [0.7745966692414834, 0.5555555555555556]];
  /** Signed area of subpaths (each closed), pt²: positive one way round, negative the other. Holes wound against their outline subtract. */
  function signedArea(sps) {
    let A = 0;
    for (const s of segmentsOf(sps)) {
      if (!s.c) { A += (s.p0[0] * s.p3[1] - s.p3[0] * s.p0[1]) / 2; continue; }
      for (const [x, w] of GL) {
        const t = (x + 1) / 2, px = bez(s.p0[0], s.p1[0], s.p2[0], s.p3[0], t), py = bez(s.p0[1], s.p1[1], s.p2[1], s.p3[1], t);
        const dx = bezD(s.p0[0], s.p1[0], s.p2[0], s.p3[0], t), dy = bezD(s.p0[1], s.p1[1], s.p2[1], s.p3[1], t);
        A += w / 2 * (px * dy - py * dx) / 2;
      }
    }
    return A;
  }
  /** The filled area (non-zero) of subpaths, pt², through Clipper at the fine tolerance (for a shape whose rings may overlap). */
  function filledArea(sps, evenOdd) { grid(); return Math.abs(areaOfRings(shapeOf(new Tags(), { subpaths: sps, evenOdd }))); }
  /** The centre of area of a filled shape (its rings made one shape first): where a heart balances, not the middle of its box. */
  function centroidOf(sps, evenOdd) {
    grid();
    const shape = shapeOf(new Tags(), { subpaths: sps, evenOdd });
    let A = 0, cx = 0, cy = 0;
    for (const r of shape) for (let i = 0; i < r.length; i++) {
      const a = r[i], b = r[(i + 1) % r.length], x0 = a[0], y0 = a[1], x1 = b[0], y1 = b[1], cr = x0 * y1 - x1 * y0;
      A += cr; cx += (x0 + x1) * cr; cy += (y0 + y1) * cr;
    }
    return Math.abs(A) > 1e-12 ? [cx / (3 * A), cy / (3 * A)] : null;
  }
  /** Rings oriented by how deep they sit (outlines one way, holes the other, islands in holes the first way again), so non-zero and even-odd draw the same: a compound path. */
  function orientNested(sps) {
    const rings = sps.map(sub => { const pl = polylineOf(sub); return { sub, pl, a: signedArea([sub]) }; });
    const inside = (p, pl) => { let w = false; for (let i = 0, j = pl.length - 1; i < pl.length; j = i++) { const a = pl[i], b = pl[j]; if ((a[1] > p[1]) !== (b[1] > p[1]) && p[0] < (b[0] - a[0]) * (p[1] - a[1]) / (b[1] - a[1]) + a[0]) w = !w; } return w; };
    return rings.map((r, i) => {
      if (!r.pl.length) return r.sub;
      const probe = r.pl[0];
      let depth = 0; rings.forEach((o, j) => { if (j !== i && o.pl.length > 2 && Math.abs(o.a) > Math.abs(r.a) && inside(probe, o.pl)) depth++; });
      const want = depth % 2 === 0 ? 1 : -1;
      return Math.sign(r.a) === want || !r.a ? r.sub : reverseSub(r.sub);
    });
  }
  /** A subpath walked the other way round. */
  function reverseSub(sub) {
    const segs = segmentsOf([sub]), closed = sub.some(op => op && op[0] === 'h');
    const list = segs.filter(s => !(s.closing && !closed));
    if (!list.length) return sub;
    const out = [['m', list[list.length - 1].p3]];
    for (let i = list.length - 1; i >= 0; i--) { const s = list[i]; out.push(s.c ? ['c', s.p2, s.p1, s.p0] : ['l', s.p0]); }
    if (closed) { if (out[out.length - 1][0] === 'l') out.pop(); out.push(['h']); }
    return out;
  }

  const kernel = { pathfinder, offsetPath, outlineStroke, roundCorners, signedArea, filledArea, centroidOf, orientNested, reverseSub, fitPolyline, cubicPiece, segmentsOf, FLAT, FIT };

  /* ═══ 2 · ON THE EDITOR'S ITEMS: (items, selection, ...) -> { items, sel } or { error }; pure, the editor's tree untouched (CharmEditor.model does the tree work) ═══ */
  const editorApi = () => (root && root.CharmEditor && root.CharmEditor.model ? root.CharmEditor : typeof require === 'function' ? (() => { try { return require('./charm-nest-charm-editor.js'); } catch (e) { return null; } })() : null);
  const MD = () => { const E = editorApi(); if (!E || !E.model) throw new Error('CharmEditor is not on the page'); return E.model; };
  const evenOf = it => !!(it && (it.evenOdd || it.even === true || it.fillRule === 'evenodd'));
  const ROLE_NAME = { cut: 'Cut line', line: 'Line engrave', hatch: 'Hatch engrave', art: 'Artwork' };
  const nameOf = it => (it.name != null && String(it.name).trim() ? String(it.name) : it.kind === 'text' ? (String(it.text || '').trim() || 'Text') : it.kind === 'group' ? 'Group' : ROLE_NAME[it.role] || 'Path');
  const leavesOf = (it, out) => { out = out || []; if (it.kind === 'group') (it.children || []).forEach(c => { if (!c.hidden) leavesOf(c, out); }); else out.push(it); return out; };
  // (what a new path made from an item keeps of it: its role, colours, line width and anything else it carries (layer ...); not its geometry, words or transform)
  const NOT_PAINT = new Set(['id', 'kind', 'subpaths', 'children', 'text', 'fontKey', 'sizePt', 'align', 'lineGap', 'weightMm', 'matrix', 'pivot', 'bbox', 'tx', 'ty', 'sx', 'sy', 'rot',
    'evenOdd', 'even', 'fillRule', 'name', 'hidden', 'locked', 'source', 'group', 'changed', 'origin', 'editable']);
  function paintOf(it, M) {
    const leaf = it.kind === 'group' ? leavesOf(it)[0] || it : it, o = {};
    for (const k of Object.keys(leaf)) if (!NOT_PAINT.has(k) && k[0] !== '_') o[k] = leaf[k];
    o.role = leaf.role || 'art';
    if (leaf.kind === 'text') { o.fill = M.paintFill(leaf); o.stroke = M.paintStroke(leaf); }   // (words are painted by their role: filled words fill, line words stroke)
    return o;
  }
  const newPath = (paint, subpaths, name, M) => Object.assign({}, paint, { id: M.newId(), kind: 'path', source: 'added', name, hidden: false, locked: false, subpaths, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 });
  const newGroup = (children, name, M) => {
    const roles = {}; children.forEach(k => { roles[k.role] = (roles[k.role] || 0) + 1; });
    return { id: M.newId(), kind: 'group', role: Object.keys(roles).sort((a, b) => roles[b] - roles[a])[0] || 'art', source: 'added', name, hidden: false, locked: false, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, children };
  };
  /** An item as a boolean operand in face coordinates: its outlines (a text's glyphs, a group's visible parts each with its own rule). */
  function operandOf(e, geo, M) {
    const it = e.it;
    if (it.kind !== 'group') return { subpaths: M.outlinesOf(it, geo, e.P), evenOdd: evenOf(it) };
    const parts = [];
    const rec = (g, P) => { const W = M.mul(P, geo.matrix(g)); for (const ch of g.children || []) { if (ch.hidden) continue; if (ch.kind === 'group') rec(ch, W); else parts.push({ subpaths: M.outlinesOf(ch, geo, W), evenOdd: evenOf(ch) }); } };
    rec(it, e.P);
    return { parts };
  }
  /** The selection's own items, unlocked, back of the stack first, with the transforms of the groups above them pushed into them (so face coordinates are their parent's). */
  function picked(items, sel, geo, M) {
    const next = M.normaliseAncestors(items, sel || [], geo), ix = M.indexTree(next, geo);
    const top = new Set(M.topmost((sel || []).filter(id => ix.has(id)), ix).filter(id => !ix.get(id).it.locked));
    const ids = []; M.walk(next, it => { if (top.has(it.id)) ids.push(it.id); });
    return { items: next, ix, ids, entries: ids.map(id => ix.get(id)) };
  }
  /** Items taking the places of others: map id -> [items] (an empty list removes it), at any depth. */
  function spliceIn(items, map) {
    let changed = false; const out = [];
    for (const it of items) {
      if (map.has(it.id)) { changed = true; out.push(...map.get(it.id)); continue; }
      if (it.kind === 'group' && it.children) { const ch = spliceIn(it.children, map); if (ch !== it.children) { changed = true; if (ch.length) out.push(Object.assign({}, it, { children: ch })); continue; } }
      out.push(it);
    }
    return changed ? out : items;
  }
  const foldAll = (it, geo, M) => (it.kind === 'group' ? (() => { const ch = it.children.map(c => foldAll(c, geo, M)); return ch.every((c, i) => c === it.children[i]) ? it : Object.assign({}, it, { children: ch }); })() : M.foldTextScale(it, geo));

  /* ── Pathfinder ── */
  const PF = { unite: 'Unite', minus: 'Minus Front', intersect: 'Intersect', exclude: 'Exclude', divide: 'Divide', trim: 'Trim' };
  /** The operands of a Pathfinder op on the selection (and what is needed to put the answer back), or { error }. */
  function pathfinderJob(items, sel, op, geo) {
    const M = MD(), pk = picked(items, sel, geo, M);
    const one = pk.entries.length === 1 && op === 'unite' && pk.entries[0].it.kind !== 'text';
    if (pk.entries.length < 2 && !one) return { error: `${PF[op] || op} needs two things selected or more` };
    return { pk, op, operands: pk.entries.map(e => operandOf(e, geo, M)) };
  }
  /** A Pathfinder answer (kernel.pathfinder's) put in place of its operands: one path (Unite, Intersect, Exclude: the paint of the frontmost, where it was; Minus Front: the
   *  backmost's paint, in its place, as Illustrator does), or a group of pieces (Divide, Trim: each piece painted as the operand it came from), where the frontmost was. */
  function placePathfinder(job, r) {
    const M = MD(), es = job.pk.entries, front = es[es.length - 1];
    let made;
    if (r.subpaths) {
      if (!r.subpaths.length) return { error: 'Nothing is left of the shapes' };
      const src = job.op === 'minus' ? es[0].it : front.it;
      made = newPath(paintOf(src, M), r.subpaths, nameOf(src), M);
    } else {
      if (!r.pieces.length) return { error: 'Nothing is left of the shapes' };
      const kids = r.pieces.map((p, i) => newPath(paintOf(es[p.of].it, M), p.subpaths, nameOf(es[p.of].it) + ' ' + (i + 1), M));
      made = kids.length === 1 ? kids[0] : newGroup(kids, job.op === 'divide' ? 'Divided' : 'Trimmed', M);
    }
    const map = new Map(job.pk.ids.map(id => [id, []])); map.set((job.op === 'minus' ? es[0] : front).it.id, [made]);
    return { items: spliceIn(job.pk.items, map), sel: [made.id] };
  }
  function pathfinderItems(items, sel, op, geo) {
    const job = pathfinderJob(items, sel, op, geo); if (job.error) return job;
    return placePathfinder(job, kernel.pathfinder(job.operands, op));
  }

  /* ── the charm: its outline (the biggest cut line) and its centre of area ── */
  function charmOutline(items, geo, frame) {
    const M = MD(); let best = null;
    const rec = (list, P) => { for (const it of list || []) { if (it.hidden) continue; const W = M.mul(P, geo.matrix(it)); if (it.kind === 'group') { rec(it.children, W); continue; } if (it.role !== 'cut') continue; const b = geo.drawn(it, P); if (!b) continue; const a = (b[2] - b[0]) * (b[3] - b[1]); if (!best || a > best.a) best = { it, P, b, a }; } };
    rec(items, M.I);
    if (best) return best;
    const f = frame || { x0: 0, y0: 0, wPt: 72, hPt: 72 };
    return { it: null, P: M.I, b: [f.x0, f.y0, f.x0 + f.wPt, f.y0 + f.hPt] };
  }
  /** The charm's centre of area: where it balances (a heart's is below its box's middle). The middle of the frame when it has no cut line. */
  function charmCentre(items, geo, frame) {
    const o = charmOutline(items, geo, frame);
    if (o.it) { try { const c = centroidOf(MD().outlinesOf(o.it, geo, o.P), evenOf(o.it)); if (c) return c; } catch (e) { /* no Clipper: the box's middle */ } }
    return [(o.b[0] + o.b[2]) / 2, (o.b[1] + o.b[3]) / 2];
  }

  /* ── Offset Path, the cut line round the art, Outline Stroke, compound paths, round corners ── */
  /** OFFSET PATH (o.mm, o.join 'round' | 'miter' | 'bevel', o.miter): a NEW path beside each selected thing, grown (or shrunk) by o.mm, just behind it (Illustrator's). With
   *  o.contour, ONE cut line round all of it at o.mm (round corners, every hole filled, the parts made one), at the back: the laser-cut charm's outline. */
  function offsetItems(items, sel, geo, o) {
    const M = MD(), pk = picked(items, sel, geo, M);
    if (!pk.entries.length) return { error: 'Select what to offset' };
    const d = num(+o.mm, 0) * PT;
    if (o.contour) {
      const operand = { parts: pk.entries.map(e => operandOf(e, geo, M)) };
      const sps = offsetPath(operand, Math.abs(d), 'round', 4, false, true);
      if (!sps.length) return { error: 'Nothing to put a cut line round' };
      const cut = charmOutline(pk.items, geo, null).it;
      const paint = { role: 'cut', stroke: true, fill: false }; if (cut && cut.lwPt != null) paint.lwPt = cut.lwPt;
      const made = newPath(paint, sps, 'Cut line', M);
      return { items: [made].concat(pk.items), sel: [made.id] };
    }
    if (Math.abs(d) < 1e-6) return { error: 'An offset of 0 mm is the path itself' };
    const map = new Map(), made = [];
    for (const e of pk.entries) {
      const sps = offsetPath(operandOf(e, geo, M), d, o.join || 'miter', num(+o.miter, 4), false, false);
      if (!sps.length) continue;
      const n = newPath(paintOf(e.it, M), sps, `${nameOf(e.it)} · offset ${d > 0 ? '+' : '−'}${trimNum(Math.abs(+o.mm), 3)} mm`, M);
      map.set(e.it.id, [n, e.it]); made.push(n.id);
    }
    if (!made.length) return { error: 'Shrunk to nothing: try a smaller inset' };
    return { items: spliceIn(pk.items, map), sel: made };
  }
  const trimNum = (v, dp) => String(+(+v).toFixed(dp));
  const strokeRole = { line: 'hatch', art: 'art', hatch: 'hatch' };
  /** OUTLINE STROKE: every line of the selection (a Line engrave, an outlined Artwork, words engraved as a line) becomes the filled shape its width covers, engraved as a fill (a
   *  Line engrave becomes a Hatch engrave). A shape both filled and lined becomes a group of the two. Cut lines are left alone: a cut is a line by nature. */
  function outlineStrokeItems(items, sel, geo) {
    const M = MD(), pk = picked(items, sel, geo, M), map = new Map(), made = [];
    for (const e of pk.entries) for (const leaf of leavesOf(e.it)) {
      if (leaf.locked || leaf.role === 'cut' || !M.paintStroke(leaf)) continue;
      const le = pk.ix.get(leaf.id);
      const sps = outlineStroke(M.outlinesOf(leaf, geo, le.P), M.lwOf(leaf));
      if (!sps.length) continue;
      const paint = Object.assign(paintOf(leaf, M), { role: strokeRole[leaf.role] || 'hatch', fill: true, stroke: false }); delete paint.lwPt;
      const n = newPath(paint, sps, nameOf(leaf) + ' · outlined', M);
      let put = [n];
      if (M.paintFill(leaf) && leaf.kind !== 'text') { const fillOnly = newPath(Object.assign(paintOf(leaf, M), { fill: true, stroke: false }), M.outlinesOf(leaf, geo, le.P), nameOf(leaf), M); put = [newGroup([fillOnly, n], nameOf(leaf), M)]; }
      map.set(leaf.id, put); made.push(put[0].id);
    }
    if (!made.length) return { error: 'Nothing selected has a line to outline (a cut line stays a line)' };
    return { items: spliceIn(pk.items, map), sel: made };
  }
  /** MAKE COMPOUND PATH (⌘8): the selection's shapes as ONE path; where they overlap, a hole (even-odd, and every ring wound by how deep it sits so the laser and the screen
   *  agree whichever rule they read). It takes the paint of the backmost, where the frontmost was. */
  function makeCompound(items, sel, geo) {
    const M = MD(), pk = picked(items, sel, geo, M);
    const leaves = [].concat(...pk.entries.map(e => leavesOf(e.it).map(l => ({ l, P: pk.ix.get(l.id).P }))));
    if (leaves.length < 2) return { error: 'A compound path needs two shapes or more' };
    const sps = orientNested([].concat(...leaves.map(x => M.outlinesOf(x.l, geo, x.P))));
    const made = Object.assign(newPath(paintOf(leaves[0].l, M), sps, 'Compound path', M), { even: true });
    const map = new Map(pk.ids.map(id => [id, []])); map.set(pk.ids[pk.ids.length - 1], [made]);
    return { items: spliceIn(pk.items, map), sel: [made.id] };
  }
  /** RELEASE COMPOUND PATH (⌥⇧⌘8): a path of several rings becomes one path per ring, each where it was, its holes filled shapes of their own (as Illustrator's). */
  function releaseCompound(items, sel, geo) {
    const M = MD(), pk = picked(items, sel, geo, M), map = new Map(), made = [];
    for (const e of pk.entries) for (const leaf of leavesOf(e.it)) {
      if (leaf.kind !== 'path' || leaf.locked || (leaf.subpaths || []).length < 2) continue;
      const sps = M.outlinesOf(leaf, geo, pk.ix.get(leaf.id).P), paint = paintOf(leaf, M);
      const parts = sps.map((sp, i) => newPath(paint, [sp], `${nameOf(leaf)} ${i + 1}`, M));
      map.set(leaf.id, parts); made.push(...parts.map(p => p.id));
    }
    if (!made.length) return { error: 'Nothing selected is a compound path' };
    return { items: spliceIn(pk.items, map), sel: made };
  }
  /** ROUND CORNERS (mm): every corner of the selected paths rounded. A path turned and scaled evenly keeps its transform (the radius in its own units); one stretched unevenly
   *  is redrawn in place first, so the arcs are round on the charm. */
  function roundCornerItems(items, sel, geo, mm) {
    const M = MD(), pk = picked(items, sel, geo, M), map = new Map(), made = [];
    const r = num(+mm, 0) * PT; if (!(r > 0)) return { error: 'A radius above 0 mm' };
    for (const e of pk.entries) for (const leaf of leavesOf(e.it)) {
      if (leaf.kind !== 'path' || leaf.locked) continue;
      const sx = Math.abs(num(leaf.sx, 1)), sy = Math.abs(num(leaf.sy, 1));
      let n;
      if (Math.abs(sx - sy) < 1e-9 * Math.max(1, sx) && sx > 0) {
        const sp = roundCorners(leaf.subpaths || [], r / sx); if (sp === leaf.subpaths) continue;
        n = M.retext(leaf, { subpaths: sp }, geo);
      } else n = Object.assign({}, leaf, { subpaths: roundCorners(M.outlinesOf(leaf, geo, pk.ix.get(leaf.id).P), r), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 });
      map.set(leaf.id, [n]); made.push(leaf.id);
    }
    if (!made.length) return { error: 'Select a path to round (words are rounded by their font)' };
    return { items: spliceIn(pk.items, map), sel: sel.slice() };
  }

  /* ── Align and Distribute ── */
  /** ALIGN (how: 'left' | 'hcenter' | 'right' | 'top' | 'vcenter' | 'bottom') to o.to: 'selection' (its box), 'key' (o.key stays where it is, the rest come to it), 'charm' (the
   *  charm's outline: its edges and its box's middle) or 'centre' (the charm's centre of area: every edge or middle asked for goes to that point). */
  function alignItems(items, sel, geo, how, o) {
    o = o || {}; const M = MD(), pk = picked(items, sel, geo, M), to = o.to || 'selection';
    if (!pk.entries.length) return { error: 'Select what to align' };
    const boxes = new Map(pk.entries.map(e => [e.it.id, geo.drawn(e.it, e.P)]));
    let ref;
    if (to === 'key') { if (!boxes.has(o.key)) return { error: 'Pick the key object first' }; ref = boxes.get(o.key); }
    else if (to === 'charm') ref = charmOutline(pk.items, geo, o.frame).b;
    else if (to === 'centre') { const c = charmCentre(pk.items, geo, o.frame); ref = [c[0], c[1], c[0], c[1]]; }
    else { if (pk.entries.length < 2) return { error: 'Select two things or more to align them to each other' }; for (const b of boxes.values()) ref = M.union(ref, b); }
    if (!ref) return { error: 'Nothing to align to' };
    const map = new Map();
    for (const e of pk.entries) {
      if (to === 'key' && e.it.id === o.key) continue;
      const b = boxes.get(e.it.id); if (!b) continue;
      let dx = 0, dy = 0;
      if (how === 'left') dx = ref[0] - b[0]; else if (how === 'right') dx = ref[2] - b[2]; else if (how === 'hcenter') dx = (ref[0] + ref[2]) / 2 - (b[0] + b[2]) / 2;
      else if (how === 'top') dy = ref[1] - b[1]; else if (how === 'bottom') dy = ref[3] - b[3]; else if (how === 'vcenter') dy = (ref[1] + ref[3]) / 2 - (b[1] + b[3]) / 2;
      else if (how === 'centre') { dx = (ref[0] + ref[2]) / 2 - (b[0] + b[2]) / 2; dy = (ref[1] + ref[3]) / 2 - (b[1] + b[3]) / 2; }
      if (Math.abs(dx) < 1e-9 && Math.abs(dy) < 1e-9) continue;
      map.set(e.it.id, [M.pushDown(e.it, M.tr(dx, dy), geo)]);
    }
    return { items: map.size ? spliceIn(pk.items, map) : items, sel: sel.slice() };
  }
  /** DISTRIBUTE (how: 'left' | 'hcenter' | 'right' | 'top' | 'vcenter' | 'bottom': those edges or middles evenly between the outermost two; 'hspace' | 'vspace': the gaps
   *  between them equal, or o.gapMm each, counted from the key object (or the first) when a gap is given). */
  function distributeItems(items, sel, geo, how, o) {
    o = o || {}; const M = MD(), pk = picked(items, sel, geo, M);
    const H = /^(left|hcenter|right|hspace)$/.test(how), lo = H ? 0 : 1, hi = H ? 2 : 3;
    const list = pk.entries.map(e => ({ e, b: geo.drawn(e.it, e.P) })).filter(x => x.b);
    const gap = o.gapMm != null && o.gapMm !== '' && fin(+o.gapMm) ? +o.gapMm * PT : null;
    if (list.length < (/space$/.test(how) && gap != null ? 2 : 3)) return { error: gap != null ? 'Select two things or more' : 'Select three things or more to distribute them' };
    const shift = new Map();
    if (/space$/.test(how)) {
      list.sort((a, b) => a.b[lo] - b.b[lo]);
      if (gap == null) {
        const span = Math.max(...list.map(x => x.b[hi])) - list[0].b[lo], sizes = list.reduce((a, x) => a + x.b[hi] - x.b[lo], 0), g = (span - sizes) / (list.length - 1);
        let at = list[0].b[hi];
        for (let i = 1; i < list.length; i++) { const x = list[i]; shift.set(x.e.it.id, at + g - x.b[lo]); at += g + x.b[hi] - x.b[lo]; }
      } else {
        let k = list.findIndex(x => x.e.it.id === o.key); if (k < 0) k = 0;
        let at = list[k].b[hi];
        for (let i = k + 1; i < list.length; i++) { const x = list[i]; shift.set(x.e.it.id, at + gap - x.b[lo]); at += gap + x.b[hi] - x.b[lo]; }
        at = list[k].b[lo];
        for (let i = k - 1; i >= 0; i--) { const x = list[i]; shift.set(x.e.it.id, at - gap - x.b[hi]); at -= gap + x.b[hi] - x.b[lo]; }
      }
    } else {
      const c = b => (/left|top/.test(how) ? b[lo] : /right|bottom/.test(how) ? b[hi] : (b[lo] + b[hi]) / 2);
      list.sort((a, b) => c(a.b) - c(b.b));
      const c0 = c(list[0].b), step = (c(list[list.length - 1].b) - c0) / (list.length - 1);
      for (let i = 1; i < list.length - 1; i++) shift.set(list[i].e.it.id, c0 + i * step - c(list[i].b));
    }
    const map = new Map();
    for (const x of list) { const d = shift.get(x.e.it.id); if (!d || Math.abs(d) < 1e-9) continue; map.set(x.e.it.id, [M.pushDown(x.e.it, H ? M.tr(d, 0) : M.tr(0, d), geo)]); }
    return { items: map.size ? spliceIn(pk.items, map) : items, sel: sel.slice() };
  }

  /* ── Transform by values, Transform Again, Step and Repeat ── */
  /** A transform: t = { dx, dy (pt), deg (clockwise, as the editor's angle), sx, sy (factors), reflect: null | 'v' (across a vertical line: left and right swap) | 'h' }, about
   *  the point c: reflect, scale and turn about c, then move. */
  function matrixOf(t, c) {
    const M = MD();
    let L = M.I;
    if (t.reflect === 'v') L = M.scM(-1, 1); else if (t.reflect === 'h') L = M.scM(1, -1);
    L = M.mul(M.scM(num(t.sx, 1), num(t.sy, 1)), L);
    L = M.mul(M.rotM(num(t.deg, 0)), L);
    return M.mul(M.tr(num(t.dx, 0), num(t.dy, 0)), M.about(L, c[0], c[1]));
  }
  const isNone = t => !num(t.dx, 0) && !num(t.dy, 0) && !num(t.deg, 0) && num(t.sx, 1) === 1 && num(t.sy, 1) === 1 && !t.reflect;
  /** The point a transform turns about: 'selection' (the middle of its box), 'charm' (the middle of the charm's outline), 'each' (every thing its own middle). */
  function pivotOf(pk, geo, pivot, frame) {
    const M = MD();
    if (pivot === 'charm') { const b = charmOutline(pk.items, geo, frame).b; return () => [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2]; }
    if (pivot === 'each') return e => { const b = geo.drawn(e.it, e.P); return b ? [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2] : [0, 0]; };
    let u = null; for (const e of pk.entries) u = M.union(u, geo.drawn(e.it, e.P));
    const c = u ? [(u[0] + u[2]) / 2, (u[1] + u[3]) / 2] : [0, 0];
    return () => c;
  }
  /** MOVE / ROTATE / SCALE / REFLECT by values (o.copy: a copy is transformed, just in front of its original, and selected; the original stays). */
  function transformItems(items, sel, geo, t, o) {
    o = o || {}; const M = MD(), pk = picked(items, sel, geo, M);
    if (!pk.entries.length) return { error: 'Select what to transform' };
    if (isNone(t) && !o.copy) return { items, sel: sel.slice() };
    const at = pivotOf(pk, geo, o.pivot, o.frame), map = new Map(), out = [];
    for (const e of pk.entries) {
      const X = matrixOf(t, at(e));
      if (o.copy) { const c = foldAll(M.pushDown(M.reid(e.it), X, geo, M.I, true), geo, M); map.set(e.it.id, [e.it, c]); out.push(c.id); }
      else { map.set(e.it.id, [foldAll(M.pushDown(e.it, X, geo), geo, M)]); out.push(e.it.id); }
    }
    return { items: spliceIn(pk.items, map), sel: out };
  }
  /** STEP AND REPEAT: o.count copies of the selection, the k-th moved k × (dx, dy), turned k × deg and scaled scale^k about the pivot (the selection's middle, or the charm's:
   *  a ring of hearts round a round charm). The copies are selected with the originals. */
  function stepRepeatItems(items, sel, geo, t, o) {
    o = o || {}; const M = MD(), pk = picked(items, sel, geo, M);
    const n = clamp(Math.round(num(+o.count, 0)), 0, 500);
    if (!pk.entries.length) return { error: 'Select what to repeat' };
    if (!n) return { error: 'How many copies?' };
    const at = pivotOf(pk, geo, o.pivot === 'each' ? 'selection' : o.pivot, o.frame), map = new Map(), out = [];
    for (const e of pk.entries) {
      const list = [e.it]; out.push(e.it.id);
      for (let k = 1; k <= n; k++) {
        const tk = { dx: k * num(t.dx, 0), dy: k * num(t.dy, 0), deg: k * num(t.deg, 0), sx: Math.pow(num(t.sx, 1), k), sy: Math.pow(num(t.sy, 1), k), reflect: k % 2 ? t.reflect : null };
        const c = foldAll(M.pushDown(M.reid(e.it), matrixOf(tk, at(e)), geo, M.I, true), geo, M);
        list.push(c); out.push(c.id);
      }
      map.set(e.it.id, list);
    }
    return { items: spliceIn(pk.items, map), sel: out };
  }

  /* ── Select Same, Select Inverse, lock and hide ── */
  const colourKey = v => (Array.isArray(v) ? v.map(x => Math.round(+x * 1000)).join(',') : v ? 'on' : 'off');
  const SAME = {
    role: it => it.role,
    fill: (it, M) => (M.paintFill(it) ? it.role + '|' + colourKey(it.fill) : '-'),
    stroke: (it, M) => (M.paintStroke(it) ? it.role + '|' + colourKey(it.stroke) + '|' + Math.round(M.lwOf(it) * 1e4) : '-')
  };
  /** SELECT SAME (how: 'role' | 'fill' | 'stroke'): every visible, unlocked part of the face whose role / fill / line (colour and width) is the first selected part's. */
  function selectSame(items, sel, geo, how) {
    const M = MD(), ix = M.indexTree(items, geo), first = (sel || []).map(id => ix.get(id)).find(Boolean);
    if (!first || !SAME[how]) return { error: 'Select one thing to match' };
    const ref = leavesOf(first.it)[0]; if (!ref) return { error: 'Select one thing to match' };
    const want = SAME[how](ref, M), out = [];
    const rec = list => { for (const it of list || []) { if (it.hidden || it.locked) continue; if (it.kind === 'group') rec(it.children); else if (SAME[how](it, M) === want) out.push(it.id); } };
    rec(items);
    return { items, sel: out };
  }
  /** Everything at the top of the face that is not selected (and has nothing selected inside it), visible and unlocked. */
  function othersOf(items, sel, geo) {
    const M = MD(), ix = M.indexTree(items, geo), on = new Set();
    for (const id of sel || []) { const e = ix.get(id); if (!e) continue; on.add(id); e.anc.forEach(a => on.add(a)); }
    return items.filter(it => !on.has(it.id));
  }
  const selectInverse = (items, sel, geo) => ({ items, sel: othersOf(items, sel, geo).filter(it => !it.hidden && !it.locked).map(it => it.id) });
  const flagged = (it, flag, v) => { const n = Object.assign({}, it, { [flag]: v }); if (flag === 'locked') { delete n._autoLock; delete n._lockedIn; } return n; };
  /** LOCK (HIDE) EVERYTHING ELSE: all that is not selected, so the selection can be worked on alone. */
  function flagOthers(items, sel, geo, flag) {
    const others = othersOf(items, sel, geo).filter(it => !it[flag]);
    if (!others.length) return { error: 'Nothing else to ' + (flag === 'locked' ? 'lock' : 'hide') };
    const map = new Map(others.map(it => [it.id, [flagged(it, flag, true)]]));
    return { items: spliceIn(items, map), sel: (sel || []).slice() };
  }
  /** UNLOCK ALL / SHOW ALL, at every depth. */
  function clearFlagAll(items, flag) {
    let any = false;
    const rec = list => list.map(it => {
      let n = it;
      if (it[flag]) { n = flagged(it, flag, false); any = true; }
      if (it.kind === 'group' && it.children) { const ch = rec(it.children); if (ch.some((c, i) => c !== it.children[i])) n = Object.assign({}, n, { children: ch }); }
      return n;
    });
    const out = rec(items);
    return any ? { items: out, sel: null } : { error: flag === 'locked' ? 'Nothing is locked' : 'Nothing is hidden' };
  }

  /** CENTRE ON THE CHARM: the selection, kept together, moved so the middle of its box is on the charm's centre of area (one click: words centred on a heart). */
  function centreOnCharm(items, sel, geo, frame) {
    const M = MD(), pk = picked(items, sel, geo, M);
    let u = null; for (const e of pk.entries) u = M.union(u, geo.drawn(e.it, e.P));
    if (!u) return { error: 'Select what to centre' };
    const c = charmCentre(pk.items, geo, frame);
    return transformItems(items, sel, geo, { dx: c[0] - (u[0] + u[2]) / 2, dy: c[1] - (u[1] + u[3]) / 2 }, {});
  }
  const ops = { pathfinderItems, pathfinderJob, placePathfinder, offsetItems, outlineStrokeItems, makeCompound, releaseCompound, roundCornerItems, alignItems, distributeItems,
    transformItems, stepRepeatItems, matrixOf, centreOnCharm, selectSame, selectInverse, flagOthers, clearFlagAll, charmOutline, charmCentre, spliceIn };

  /* ═══ 3 · THE WORKSPACE: the commands (the head's Object menu, the right-click menu, keys), the Align and Pathfinder flyouts on the property bar, the value dialogs with a
     live preview drawn over the charm, the Worker for big booleans and ⌘D. Everything a command changes goes through ctx and is ONE undo step (the editor closes it when the
     command's run ends; a dialog's run ends when it closes). ═══ */
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { /* the log is a courtesy */ } };
  const myUrl = doc && doc.currentScript && doc.currentScript.src ? doc.currentScript.src : null;

  /** The work as `next` (and the selection `sel`) through the ctx: the top level's changed places replaced, the rest added or taken away; all of it one undo step. */
  function applyTree(ctx, next, sel) {
    const cur = ctx.items();
    if (next && next !== cur) {
      const map = new Map(), n = Math.min(cur.length, next.length);
      for (let i = 0; i < n; i++) if (cur[i] !== next[i]) map.set(cur[i].id, next[i]);
      if (map.size) ctx.replaceItems(map);
      if (next.length > cur.length) ctx.addItems(next.slice(cur.length), { select: false });
      else if (next.length < cur.length) ctx.removeItems(cur.slice(next.length).map(it => it.id));
    }
    if (sel) ctx.select(sel);
  }
  /** An operation's answer put into the work (or its reason said over the work area). */
  function land(ctx, r, what) {
    if (!r || r.error) { ctx.say(r ? r.error : 'Nothing happened', 3200); return false; }
    applyTree(ctx, r.items, r.sel);
    log('editorOp', { op: what, face: ctx.face(), n: (r.sel || []).length });
    return true;
  }
  const nSel = ctx => ctx.selection().length;
  const selEntries = ctx => ctx.selection().map(id => ctx.entry(id)).filter(Boolean);
  const leavesSel = ctx => [].concat(...selEntries(ctx).map(e => leavesOf(e.it))).filter(l => !l.locked);
  const hasPath = ctx => leavesSel(ctx).some(l => l.kind === 'path');
  // (one thing alone is worth uniting only when it is several shapes: a group, or a path of several rings)
  const canUniteOne = ctx => { const es = selEntries(ctx); return es.length === 1 && (es[0].it.kind === 'group' ? leavesOf(es[0].it).length > 1 : es[0].it.kind === 'path' && (es[0].it.subpaths || []).length > 1); };

  /* ── icons: the editor's hand (24-unit box, 1.6 stroke, round ends); the Pathfinder's are Illustrator's two squares, the part that is kept filled ── */
  const SV = (d, extra) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${extra || ''}>${d}</svg>`;
  const SQ_A = 'M4 4h10v10H4z', SQ_B = 'M10 10h10v10H10z';
  const IC = {
    unite: SV(`<path d="M4 4h10v6h6v10H10v-6H4z" fill="currentColor" fill-opacity=".85"/>`),
    minus: SV(`<path d="M4 4h10v6h-4v4H4z" fill="currentColor" fill-opacity=".85"/><path d="${SQ_B}" stroke-dasharray="2 2"/>`),
    intersect: SV(`<path d="${SQ_A}"/><path d="${SQ_B}"/><path d="M10 10h4v4h-4z" fill="currentColor"/>`),
    exclude: SV(`<path d="M4 4h10v6h-4v4H4z M14 10h6v10H10v-6h4z" fill="currentColor" fill-opacity=".85"/><path d="M10 10h4v4h-4z"/>`),
    divide: SV(`<path d="M4 4h10v6h-4v4H4z" fill="currentColor" fill-opacity=".45"/><path d="M10 10h4v4h-4z" fill="currentColor"/><path d="M14 10h6v10H10v-6h4z" fill="currentColor" fill-opacity=".7"/><path d="${SQ_A}"/><path d="${SQ_B}"/>`),
    trim: SV(`<path d="M4 4h10v6h-4v4H4z" fill="currentColor" fill-opacity=".45"/><path d="${SQ_B}" fill="currentColor" fill-opacity=".85"/>`),
    alignL: SV('<path d="M4 3v18"/><rect x="7" y="6" width="11" height="4"/><rect x="7" y="14" width="7" height="4"/>'),
    alignC: SV('<path d="M12 3v18"/><rect x="5.5" y="6" width="13" height="4"/><rect x="8" y="14" width="8" height="4"/>'),
    alignR: SV('<path d="M20 3v18"/><rect x="6" y="6" width="11" height="4"/><rect x="10" y="14" width="7" height="4"/>'),
    alignT: SV('<path d="M3 4h18"/><rect x="6" y="7" width="4" height="11"/><rect x="14" y="7" width="4" height="7"/>'),
    alignM: SV('<path d="M3 12h18"/><rect x="6" y="5.5" width="4" height="13"/><rect x="14" y="8" width="4" height="8"/>'),
    alignB: SV('<path d="M3 20h18"/><rect x="6" y="6" width="4" height="11"/><rect x="14" y="10" width="4" height="7"/>'),
    distL: SV('<path d="M4 3v18M12 3v18M20 3v18"/><rect x="4" y="8" width="5" height="8"/><rect x="12" y="6" width="3" height="12"/><rect x="20" y="9" width="1" height="6"/>'),
    distC: SV('<path d="M5 3v18M12 3v18M19 3v18"/><rect x="3" y="8" width="4" height="8"/><rect x="10.5" y="6" width="3" height="12"/><rect x="17" y="9" width="4" height="6"/>'),
    distR: SV('<path d="M4 3v18M12 3v18M20 3v18"/><rect x="0" y="8" width="4" height="8" transform="translate(0)"/><rect x="9" y="6" width="3" height="12"/><rect x="16" y="9" width="4" height="6"/>'),
    distT: SV('<path d="M3 4h18M3 12h18M3 20h18"/><rect x="8" y="4" width="8" height="5"/><rect x="6" y="12" width="12" height="3"/><rect x="9" y="20" width="6" height="1"/>'),
    distM: SV('<path d="M3 5h18M3 12h18M3 19h18"/><rect x="8" y="3" width="8" height="4"/><rect x="6" y="10.5" width="12" height="3"/><rect x="9" y="17" width="6" height="4"/>'),
    distB: SV('<path d="M3 4h18M3 12h18M3 20h18"/><rect x="8" y="0" width="8" height="4"/><rect x="6" y="9" width="12" height="3"/><rect x="9" y="16" width="6" height="4"/>'),
    spaceH: SV('<rect x="3" y="7" width="5" height="10"/><rect x="16" y="7" width="5" height="10"/><path d="M10 12h4M10 10.5v3M14 10.5v3"/>'),
    spaceV: SV('<rect x="7" y="3" width="10" height="5"/><rect x="7" y="16" width="10" height="5"/><path d="M12 10v4M10.5 10h3M10.5 14h3"/>'),
    align: SV('<path d="M12 3v18"/><rect x="5.5" y="6" width="13" height="4"/><rect x="8" y="14" width="8" height="4"/>'),
    flipH: SV('<path d="M12 3v18" stroke-dasharray="2 2"/><path d="M9 7 4 17h5Z"/><path d="M15 7l5 10h-5Z" fill="currentColor" fill-opacity=".3"/>'),
    flipV: SV('<path d="M3 12h18" stroke-dasharray="2 2"/><path d="M7 9 17 4v5Z"/><path d="M7 15l10 5v-5Z" fill="currentColor" fill-opacity=".3"/>'),
    rot90: SV('<path d="M6 8a8 8 0 0 1 13 4"/><path d="m19 7 .2 5-4.6-1.4"/><rect x="5" y="13" width="7" height="7"/>'),
    rot90l: SV('<path d="M18 8a8 8 0 0 0-13 4"/><path d="m5 7-.2 5 4.6-1.4"/><rect x="12" y="13" width="7" height="7"/>'),
    offset: SV('<rect x="8" y="8" width="8" height="8" rx=".6"/><rect x="4" y="4" width="16" height="16" rx="3.2" stroke-dasharray="2.2 2.2"/>'),
    cutline: SV('<path d="M12 7.5c1.5-2.5 5.5-2 5.5 1.3 0 2.6-3.2 4.6-5.5 6.7-2.3-2.1-5.5-4.1-5.5-6.7 0-3.3 4-3.8 5.5-1.3Z" fill="currentColor" fill-opacity=".35"/><path d="M12 4.2c2.8-3 9-2 9 3.8 0 4.6-5 7.6-9 11.2-4-3.6-9-6.6-9-11.2 0-5.8 6.2-6.8 9-3.8Z"/>'),
    outline: SV('<path d="M4 17 17 4"/><path d="M5 20 20 5" stroke-width="1"/><path d="M3 15 15 3" stroke-width="1"/>'),
    compound: SV('<rect x="3.5" y="3.5" width="17" height="17" rx="1" fill="currentColor" fill-opacity=".3"/><circle cx="12" cy="12" r="4.5" fill="#fff"/>'),
    release: SV('<rect x="3.5" y="3.5" width="12" height="12" rx="1"/><circle cx="15" cy="15" r="4.5" fill="currentColor" fill-opacity=".3"/>'),
    corners: SV('<path d="M4 20V10a6 6 0 0 1 6-6h10"/><path d="M4 4h3M4 4v3" stroke-dasharray="1.5 1.5"/>'),
    move: SV('<path d="M12 3v18M3 12h18"/><path d="m9 6 3-3 3 3M9 18l3 3 3-3M6 9l-3 3 3 3M18 9l3 3-3 3"/>'),
    rotate: SV('<path d="M5 12a7 7 0 1 0 2.1-5"/><path d="M5 4v4h4"/>'),
    scale: SV('<rect x="3.5" y="10.5" width="10" height="10"/><path d="M13 11 20 4M15 4h5v5"/>'),
    reflect: SV('<path d="M12 3v18" stroke-dasharray="2 2"/><path d="M9 7 4 17h5Z"/><path d="M15 7l5 10h-5Z"/>'),
    again: SV('<path d="M4 12a8 8 0 0 1 14-5.3"/><path d="M18 3v4h-4"/><path d="M20 12a8 8 0 0 1-14 5.3"/><path d="M6 21v-4h4"/>'),
    repeat: SV('<rect x="3" y="3" width="7" height="7"/><rect x="8.5" y="8.5" width="7" height="7" stroke-dasharray="2 1.5"/><rect x="14" y="14" width="7" height="7" stroke-dasharray="2 1.5"/>'),
    same: SV('<circle cx="7" cy="12" r="3.5" fill="currentColor"/><circle cx="17" cy="12" r="3.5" fill="currentColor"/>'),
    inverse: SV('<rect x="3.5" y="3.5" width="17" height="17" stroke-dasharray="3 2"/><circle cx="12" cy="12" r="4" fill="currentColor"/>'),
    centre: SV('<rect x="3.5" y="3.5" width="17" height="17" rx="5"/><path d="M12 8v8M8 12h8"/>'),
    lock: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><rect x="4.5" y="10.5" width="15" height="10"/><path d="M8 10.5V7a4 4 0 0 1 8 0v3.5"/></svg>',
    eye: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M3 12s3.5-6 9-6 9 6 9 6-3.5 6-9 6-9-6-9-6Z"/><circle cx="12" cy="12" r="2.4"/></svg>',
    caret: '<i class="ce-caret"></i>'
  };

  const CSS = `
.cops-fly{ min-width:0; width:292px; padding:8px 8px 6px; }
.cops-fly .ce-menu__head{ padding:6px 2px 5px; }
.cops-grid{ display:grid; grid-template-columns:repeat(6, 1fr); gap:3px; }
.cops-grid--4{ grid-template-columns:repeat(4, 1fr); }
.cops-b{ height:34px; border:1px solid var(--hair); background:#fff; cursor:pointer; color:var(--ink); display:inline-flex; align-items:center; justify-content:center; padding:0; position:relative; }
.cops-b svg{ width:18px; height:18px; }
.cops-b:not(:disabled):hover{ background:var(--hov); border-color:var(--hov-line); }
.cops-b:not(:disabled):active{ background:var(--press); transform:translateY(1px); }
.cops-b:disabled{ opacity:.3; cursor:default; }
.cops-b--lab{ height:50px; flex-direction:column; gap:4px; padding:4px 2px 3px; }
.cops-b--lab span{ font-size:8.5px; font-weight:700; letter-spacing:.06em; text-transform:uppercase; color:var(--muted); white-space:nowrap; }
.cops-b--lab:not(:disabled):hover span{ color:var(--ink); }
.cops-row{ display:flex; align-items:center; gap:6px; margin-top:6px; min-width:0; }
.cops-row .ce-seg{ flex:1 1 auto; } .cops-row .ce-seg button{ flex:1 1 0; padding:0 6px; font-size:9px; letter-spacing:.1em; }
.cops-row .ce-num{ flex:0 0 auto; }
.cops-row .ce-num input{ width:44px; }
.cops-key{ font-size:11px; color:var(--muted); padding:5px 2px 2px; line-height:1.45; }
.cops-key select{ font:inherit; font-size:11.5px; color:var(--ink); border:1px solid var(--hair); background:#fff; padding:3px 4px; max-width:100%; }
.cops-note{ font-size:10.5px; line-height:1.5; color:var(--muted); padding:7px 2px 2px; }
.cops-modal.ce-ask{ background:transparent; align-items:flex-start; justify-content:flex-end; padding:58px 18px 16px; pointer-events:auto; }
.cops-card{ width:min(330px,100%); background:#fff; border:1px solid var(--ink); box-shadow:0 18px 44px rgba(28,29,29,.22); padding:16px 16px 14px; outline:none; }
.cops-card h3{ margin:0 0 4px; font-family:var(--serif); font-weight:500; font-size:21px; line-height:1.15; color:var(--ink); }
.cops-card p{ margin:0 0 10px; font-size:11.5px; line-height:1.55; color:var(--muted); }
.cops-card .cops-f{ display:flex; align-items:center; justify-content:space-between; gap:10px; margin-top:8px; }
.cops-card .cops-f > span{ font-size:9.5px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); white-space:nowrap; }
.cops-card .ce-num{ width:112px; justify-content:flex-end; }
.cops-card .ce-num input{ flex:1 1 auto; width:0; min-width:0; }
.cops-card .ce-seg button{ padding:0 9px; font-size:9px; }
.cops-tabs{ margin:6px 0 2px; display:flex; } .cops-tabs .ce-seg{ flex:1 1 auto; } .cops-tabs .ce-seg button{ flex:1 1 0; }
.cops-check{ display:flex; align-items:center; gap:7px; margin:12px 0 0; font-size:11.5px; color:var(--ink); cursor:pointer; user-select:none; }
.cops-card .ce-ask__b{ margin-top:14px; }
.cops-card .ce-btn{ padding:10px 16px; letter-spacing:.2em !important; }
.cops-preview{ position:absolute; left:0; top:0; width:100%; height:100%; pointer-events:none; overflow:visible; }
.cops-preview path{ fill:rgba(165,138,82,.16); stroke:#a58a52; stroke-width:1.25; stroke-dasharray:5 3; vector-effect:non-scaling-stroke; fill-rule:nonzero; }
@media (max-width:640px){ .cops-modal.ce-ask{ padding:48px 8px 8px; justify-content:center; } .cops-fly{ width:min(292px,calc(100vw - 16px)); } }
`;
  function injectCss() { if (!doc || doc.getElementById('ceOpsCss')) return; const s = doc.createElement('style'); s.id = 'ceOpsCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); }

  /* ── what each workspace keeps: the Align flyout's choices, the dialogs' last values, the last transform (for Transform Again) ── */
  const memo = { alignTo: null, key: null, gap: '', dlg: {} };
  let last = null;   // { t, o, items, face }: the last transform made here and the work it left
  const remember = (ctx, t, o) => { last = { t, o, items: ctx.items(), face: ctx.face() }; };
  /** TRANSFORM AGAIN: the last transform once more, on what is selected now (with a copy if it made one): ⌘D after a Move / Rotate / ... with Copy steps on, as Illustrator's. */
  function again(ctx) {
    if (!last || !nSel(ctx)) { ctx.say('Nothing to repeat yet: transform something by values first', 2600); return false; }
    const r = ops.transformItems(ctx.items(), ctx.selection(), ctx.geo, last.t, Object.assign({}, last.o, { frame: ctx.frame() }));
    if (!land(ctx, r, 'transformAgain')) return false;
    remember(ctx, last.t, last.o);
    return true;
  }

  /* ── the flyouts on the property bar: Align (and Distribute) and Pathfinder; they stay open while you click through them, as Illustrator's panels do ── */
  let fly = null, flySkip = false;
  function closeFly() { if (fly && fly.el.parentNode) fly.el.parentNode.removeChild(fly.el); fly = null; }
  function openFly(ctx, kind, cmdId) {
    if (flySkip) { flySkip = false; return; }   // (the press on its own button closed it: a second click folds it away)
    closeFly();
    const btn = ctx.root.querySelector(`.ce-xbar [data-xcmd="${cmdId}"]`) || ctx.root.querySelector('[data-drop="xcmd"] .ce-drop__b');
    const el = doc.createElement('div'); el.className = 'ce-menu cops-fly'; el.setAttribute('role', 'menu'); el.dataset.opsFly = kind;
    ctx.root.appendChild(el);
    fly = { el, kind, ctx, btn, cmdId };
    paintFly();
    const r = btn ? btn.getBoundingClientRect() : { left: 80, right: 80, top: 80, bottom: 80 };
    const vw = doc.documentElement.clientWidth || root.innerWidth, vh = doc.documentElement.clientHeight || root.innerHeight, mr = el.getBoundingClientRect();
    let left = r.left; if (left + mr.width > vw - 8) left = Math.max(8, vw - 8 - mr.width);
    let top = r.bottom + 5; if (top + mr.height > vh - 8) top = Math.max(8, vh - 8 - mr.height);
    el.style.left = left + 'px'; el.style.top = top + 'px'; el.classList.add('is-in');
  }
  function paintFly() {
    if (!fly) return;
    const { ctx, el, kind } = fly, n = nSel(ctx);
    const b = (id, icon, title, on) => `<button class="cops-b" type="button" data-ops="${id}" title="${esc(title)}" aria-label="${esc(title)}"${on ? '' : ' disabled'}>${icon}</button>`;
    if (kind === 'pf') {
      const two = n >= 2;
      const lb = (id, icon, label, title, on) => `<button class="cops-b cops-b--lab" type="button" data-ops="${id}" title="${esc(title)}"${on ? '' : ' disabled'}>${icon}<span>${esc(label)}</span></button>`;
      el.innerHTML = `<div class="ce-menu__head">Shape modes</div><div class="cops-grid cops-grid--4">${lb('pf:unite', IC.unite, 'Unite', 'Unite: one shape of them all', two || canUniteOne(ctx))}${lb('pf:minus', IC.minus, 'Minus front', 'Minus Front: the backmost less everything in front of it', two)}${lb('pf:intersect', IC.intersect, 'Intersect', 'Intersect: only where they all overlap', two)}${lb('pf:exclude', IC.exclude, 'Exclude', 'Exclude: where they overlap becomes a hole', two)}</div>`
        + `<div class="ce-menu__head">Pathfinders</div><div class="cops-grid cops-grid--4">${lb('pf:divide', IC.divide, 'Divide', 'Divide: every overlap its own piece', two)}${lb('pf:trim', IC.trim, 'Trim', 'Trim: each shape less what is in front of it', two)}</div>`
        + `<div class="cops-note">Curves stay curves. The answer is one step: ⌘Z takes it back.</div>`;
      return;
    }
    const to = memo.alignTo || (n >= 2 ? 'selection' : 'charm');
    const sel = selEntries(ctx);
    if (to === 'key' && !sel.some(e => e.it.id === memo.key)) memo.key = sel.length ? sel[sel.length - 1].it.id : null;
    const seg = (v, label) => `<button type="button" data-ops="to:${v}" class="${to === v ? 'is-on' : ''}">${label}</button>`;
    const canA = to === 'selection' ? n >= 2 : to === 'key' ? n >= 2 && !!memo.key : n >= 1;
    el.innerHTML = `<div class="ce-menu__head">Align to</div><div class="cops-row"><div class="ce-seg" role="radiogroup">${seg('selection', 'Selection')}${seg('key', 'Key')}${seg('charm', 'Charm')}${seg('centre', 'Centre')}</div></div>`
      + (to === 'key' ? `<div class="cops-key">Key object (stays put): <select data-ops-key>${sel.map(e => `<option value="${esc(e.it.id)}"${e.it.id === memo.key ? ' selected' : ''}>${esc(nameOf(e.it))}</option>`).join('')}</select></div>` : '')
      + (to === 'charm' ? `<div class="cops-key">To the charm's outline: its edges and the middle of its box.</div>` : to === 'centre' ? `<div class="cops-key">To the charm's centre of area: where it balances (a heart's sits below its box's middle).</div>` : '')
      + `<div class="ce-menu__head">Align</div><div class="cops-grid">${b('al:left', IC.alignL, 'Align left edges', canA)}${b('al:hcenter', IC.alignC, 'Align centres across', canA)}${b('al:right', IC.alignR, 'Align right edges', canA)}${b('al:top', IC.alignT, 'Align top edges', canA)}${b('al:vcenter', IC.alignM, 'Align middles down', canA)}${b('al:bottom', IC.alignB, 'Align bottom edges', canA)}</div>`
      + `<div class="ce-menu__head">Distribute</div><div class="cops-grid">${b('di:left', IC.distL, 'Left edges evenly', n >= 3)}${b('di:hcenter', IC.distC, 'Centres evenly across', n >= 3)}${b('di:right', IC.distR, 'Right edges evenly', n >= 3)}${b('di:top', IC.distT, 'Top edges evenly', n >= 3)}${b('di:vcenter', IC.distM, 'Middles evenly down', n >= 3)}${b('di:bottom', IC.distB, 'Bottom edges evenly', n >= 3)}</div>`
      + `<div class="ce-menu__head">Spacing</div><div class="cops-row">${b('di:hspace', IC.spaceH, 'Even gaps across (or the gap below)', n >= (memo.gap !== '' ? 2 : 3))}${b('di:vspace', IC.spaceV, 'Even gaps down (or the gap below)', n >= (memo.gap !== '' ? 2 : 3))}`
      + `<label class="ce-num" title="The gap between them, mm: empty spreads them evenly between the outermost two; a value is counted from the key object (or the first)"><input data-ops-gap inputmode="decimal" autocomplete="off" spellcheck="false" placeholder="auto" value="${esc(memo.gap)}" aria-label="Gap, mm"><i>mm</i></label></div>`;
    const g = el.querySelector('[data-ops-gap]');
    if (g) { g.__ceKey = e => { if (e.key === 'Escape') { e.preventDefault(); closeFly(); } else if (e.key === 'Enter') { e.preventDefault(); memo.gap = g.value.trim(); paintFly(); } }; g.addEventListener('input', () => { memo.gap = g.value.trim(); }); g.addEventListener('blur', () => { if (fly) paintFly(); }); }
    const k = el.querySelector('[data-ops-key]'); if (k) { k.__ceKey = e => { if (e.key === 'Escape') { e.preventDefault(); closeFly(); } }; k.addEventListener('change', () => { memo.key = k.value; }); }
  }
  function flyClick(e) {
    const t = e.target.closest && e.target.closest('[data-ops]'); if (!t || !fly || t.disabled) return;
    e.preventDefault(); e.stopPropagation();
    const { ctx } = fly, [kind, v] = t.dataset.ops.split(':');
    if (kind === 'to') { memo.alignTo = v; paintFly(); return; }
    const gapNum = memo.gap === '' ? null : parseFloat(String(memo.gap).replace(',', '.').replace(/−/g, '-'));
    if (kind === 'pf') { closeFly(); runPathfinder(ctx, v); return; }
    let r;
    if (kind === 'al') r = ops.alignItems(ctx.items(), ctx.selection(), ctx.geo, v, { to: memo.alignTo || (nSel(ctx) >= 2 ? 'selection' : 'charm'), key: memo.key, frame: ctx.frame() });
    else if (kind === 'di') r = ops.distributeItems(ctx.items(), ctx.selection(), ctx.geo, v, { gapMm: fin(gapNum) ? gapNum : null, key: (memo.alignTo === 'key' && memo.key) || null });
    if (land(ctx, r, kind === 'al' ? 'align:' + v : 'distribute:' + v)) ctx.commit(kind === 'al' ? 'Align' : 'Distribute');
    paintFly();
  }

  /* ── Pathfinder: at once for what a frame can take, in a Worker for big paths (a traced image, a long text): the page never waits ── */
  const BIG = 3000;   // chord corners: under this a Pathfinder op takes well under 50 ms of the page's time (tests/charm-nest/ce-ops.cjs measures it); over it, the Worker
  const sizeOf = operands => { let n = 0; const add = o => { if (o.parts) o.parts.forEach(add); else for (const s of segmentsOf(o.subpaths)) n += chordsFor(s, FLAT); }; operands.forEach(add); return n; };
  let worker = null, workerBad = false, wSeq = 0;
  const wWaiting = new Map();
  function getWorker() {
    if (worker || workerBad) return worker;
    try {
      const abs = re => { const s = doc && Array.from(doc.scripts || []).find(x => re.test(x.src || '')); return s ? new URL(s.src, doc.baseURI).href : null; };
      const urls = [abs(/vendor\/clipper-6\.4\.2\.js/), abs(/charm-nest-vector\.js/), myUrl || abs(/charm-nest-ce-ops\.js/)];
      if (urls.some(u => !u) || !root.Worker || !root.Blob || !root.URL) { workerBad = true; return null; }
      const src = `importScripts(${urls.map(u => JSON.stringify(u)).join(',')});\nself.onmessage = function (e) { var d = e.data; try { self.postMessage({ id: d.id, r: self.CharmEditorOps.kernel.pathfinder(d.operands, d.op) }); } catch (er) { self.postMessage({ id: d.id, error: String(er && er.message || er) }); } };`;
      worker = new root.Worker(root.URL.createObjectURL(new root.Blob([src], { type: 'text/javascript' })));
      worker.onmessage = e => { const w = wWaiting.get(e.data.id); if (!w) return; wWaiting.delete(e.data.id); if (e.data.error) w.no(new Error(e.data.error)); else w.ok(e.data.r); };
      worker.onerror = e => { workerBad = true; worker = null; for (const w of wWaiting.values()) w.no(new Error((e && e.message) || 'worker failed')); wWaiting.clear(); };
    } catch (e) { workerBad = true; worker = null; }
    return worker;
  }
  const inWorker = (operands, op) => new Promise((ok, no) => { const w = getWorker(); if (!w) { no(new Error('no worker')); return; } const id = ++wSeq; wWaiting.set(id, { ok, no }); w.postMessage({ id, operands, op }); });
  async function runPathfinder(ctx, op) {
    const items0 = ctx.items(), job = ops.pathfinderJob(items0, ctx.selection(), op, ctx.geo);
    if (job.error) { ctx.say(job.error, 3200); return false; }
    let r;
    if (sizeOf(job.operands) > BIG && getWorker()) {
      ctx.say(`${PF[op]}… working`, 0);
      try { r = await inWorker(job.operands, op); } catch (e) { r = null; }
      if (ctx.items() !== items0) { ctx.say(`The shapes changed while ${PF[op]} worked: nothing was done, try again`, 3600); return false; }
      ctx.say('');
    }
    if (!r) { try { r = kernel.pathfinder(job.operands, op); } catch (e) { ctx.say(`${PF[op]} could not be done: ${String(e && e.message || e).slice(0, 100)}`, 4000); return false; } }
    const ok = land(ctx, ops.placePathfinder(job, r), 'pathfinder:' + op);
    if (ok) ctx.commit(PF[op]);
    return ok;
  }

  /* ── the value dialogs (Offset Path, the cut line round the art, Round Corners, Move / Rotate / Scale / Reflect, Step and Repeat): a card at the top right of the work area, the
     charm left in sight, the answer drawn over it while Preview is ticked; OK (or Enter) applies it as one step, Copy applies it to a copy, Esc or Cancel leaves the work as it was ── */
  function numIn(v) { const x = parseFloat(String(v == null ? '' : v).replace(',', '.').replace(/−/g, '-')); return fin(x) ? x : NaN; }
  function dialog(ctx, spec) {
    return new Promise(done => {
      const win = ctx.root.querySelector('.ce-win') || ctx.root;
      const vals = Object.assign({}, spec.values, memo.dlg[spec.id] || {});
      let tab = spec.tabs ? (spec.tab || vals.__tab || spec.tabs[0][0]) : null;
      const box = doc.createElement('div'); box.className = 'ce-ask cops-modal'; box.tabIndex = -1;
      const card = doc.createElement('div'); card.className = 'cops-card'; card.tabIndex = -1; card.setAttribute('role', 'dialog'); card.setAttribute('aria-modal', 'true');
      box.appendChild(card); win.appendChild(box);
      const svg = doc.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.setAttribute('class', 'cops-preview'); svg.setAttribute('aria-hidden', 'true'); ctx.stage.appendChild(svg);
      let previewOn = !!memo.dlg.__preview, raf = 0;
      const fieldsOf = () => (typeof spec.fields === 'function' ? spec.fields(tab, vals) : spec.fields);
      const paint = () => {
        const fs = fieldsOf();
        card.setAttribute('aria-label', spec.title);
        card.innerHTML = `<h3>${esc(typeof spec.title === 'function' ? spec.title(tab) : spec.title)}</h3>${spec.note ? `<p>${esc(typeof spec.note === 'function' ? spec.note(tab, vals) : spec.note)}</p>` : ''}`
          + (spec.tabs ? `<div class="cops-tabs"><div class="ce-seg">${spec.tabs.map(([v, l]) => `<button type="button" data-dtab="${v}" class="${v === tab ? 'is-on' : ''}">${esc(l)}</button>`).join('')}</div></div>` : '')
          + fs.map(f => {
            if (f.type === 'seg') return `<div class="cops-f"><span>${esc(f.label)}</span><div class="ce-seg">${f.options.map(([v, l]) => `<button type="button" data-dseg="${f.id}" data-v="${esc(v)}" class="${String(vals[f.id]) === String(v) ? 'is-on' : ''}">${esc(l)}</button>`).join('')}</div></div>`;
            return `<label class="cops-f"><span>${esc(f.label)}</span><span class="ce-num"><input data-dnum="${f.id}" inputmode="decimal" autocomplete="off" spellcheck="false" value="${esc(vals[f.id] == null ? '' : vals[f.id])}" aria-label="${esc(f.label)}"><i>${esc(f.unit || '')}</i></span></label>`;
          }).join('')
          + (spec.preview ? `<label class="cops-check"><input type="checkbox" data-dprev${previewOn ? ' checked' : ''}> Preview on the charm</label>` : '')
          + `<div class="ce-ask__b"><button class="ce-btn ce-btn--ghost" type="button" data-db="cancel">Cancel</button>${spec.copy ? '<button class="ce-btn ce-btn--ghost" type="button" data-db="copy">Copy</button>' : ''}<button class="ce-btn ce-btn--ink" type="button" data-db="ok">${esc(spec.ok || 'OK')}</button></div>`;
        for (const el of card.querySelectorAll('input,button')) el.__ceKey = onKey;
        card.__ceKey = onKey; box.__ceKey = onKey;
      };
      const read = () => { for (const inp of card.querySelectorAll('[data-dnum]')) vals[inp.dataset.dnum] = inp.value; return vals; };
      const drawPreview = () => {
        raf = 0; svg.innerHTML = '';
        if (!previewOn || !spec.preview) return;
        const t0 = Date.now();
        let r = null; try { r = spec.result(read(), tab, false); } catch (e) { r = null; }
        // (a preview that costs the page more than a blink is turned off for this time: the answer is still made by OK, in one go)
        if (Date.now() - t0 > 150) { previewOn = false; const cb = card.querySelector('[data-dprev]'); if (cb) cb.checked = false; ctx.say('Preview is off for shapes this big: OK makes it', 3000); return; }
        if (!r || r.error || !r.items) return;
        const was = new Set(); MD().walk(ctx.items(), it => was.add(it));
        const M = MD(), ix = M.indexTree(r.items, ctx.geo), V = ctx.view();
        let d = '', n = 0;
        M.walk(r.items, it => { if (n > 400 || it.kind === 'group' || was.has(it) || it.hidden) return; const e = ix.get(it.id); for (const sp of M.outlinesOf(it, ctx.geo, e.P)) d += M.dOf([sp]); n++; });
        if (d) svg.innerHTML = `<path d="${d}" transform="matrix(${V.map(v => +(+v).toFixed(6)).join(' ')})"/>`;
      };
      const queue = () => { if (!raf) raf = requestAnimationFrame(drawPreview); };
      const close = v => {
        if (raf) cancelAnimationFrame(raf);
        if (svg.parentNode) svg.parentNode.removeChild(svg);
        if (box.parentNode) box.parentNode.removeChild(box);
        memo.dlg[spec.id] = Object.assign({}, read(), { __tab: tab });
        try { (ctx.root.querySelector('.ce-win') || ctx.root).focus({ preventScroll: true }); } catch (e) { /* fine */ }
        done(v);
      };
      const commitWith = copy => {
        read();
        const r = spec.result(vals, tab, copy);
        if (r && r.error) { ctx.say(r.error, 3200); return; }
        close({ vals, tab, copy, r });
      };
      function onKey(e) {
        if (e.key === 'Escape') { e.preventDefault(); close(null); return; }
        if (e.key === 'Enter' && !(e.target && e.target.tagName === 'BUTTON')) { e.preventDefault(); commitWith(false); return; }
        if ((e.key === 'ArrowUp' || e.key === 'ArrowDown') && e.target && e.target.dataset && e.target.dataset.dnum) {
          e.preventDefault();
          const f = fieldsOf().find(x => x.id === e.target.dataset.dnum), st = (f && f.step) || 0.1, v = numIn(e.target.value);
          e.target.value = String(+((fin(v) ? v : 0) + (e.key === 'ArrowUp' ? 1 : -1) * st * (e.shiftKey ? 10 : 1)).toFixed(4));
          queue();
        }
      }
      card.addEventListener('input', () => { read(); queue(); });
      card.addEventListener('click', e => {
        const t = e.target;
        const tb = t.closest('[data-dtab]'); if (tb) { read(); tab = tb.dataset.dtab; paint(); queue(); const i = card.querySelector('[data-dnum]'); if (i) { i.focus(); i.select(); } return; }
        const sg = t.closest('[data-dseg]'); if (sg) { read(); vals[sg.dataset.dseg] = sg.dataset.v; paint(); queue(); return; }
        if (t.closest('[data-dprev]')) { previewOn = !!t.closest('[data-dprev]').checked; memo.dlg.__preview = previewOn; queue(); return; }
        const b = t.closest('[data-db]'); if (!b) return;
        if (b.dataset.db === 'cancel') close(null); else commitWith(b.dataset.db === 'copy');
      });
      box.addEventListener('pointerdown', e => { if (e.target === box) { e.preventDefault(); close(null); } });
      paint(); queue();
      requestAnimationFrame(() => { const i = card.querySelector('[data-dnum]'); try { if (i) { i.focus(); i.select(); } else card.focus(); } catch (e) { /* fine */ } });
    });
  }
  const PIVOTS = [['selection', 'Selection'], ['charm', 'Charm'], ['each', 'Each']];
  /** One dialog for Move / Rotate / Scale / Reflect (its tabs), as Illustrator's four: values in mm, degrees (clockwise, as the editor's angle field) and percent. */
  function transformDialog(ctx, tab) {
    const tOf = (v, tb) => {
      if (tb === 'move') return { dx: numIn(v.dx) * PT, dy: numIn(v.dy) * PT };
      if (tb === 'rotate') return { deg: numIn(v.deg) };
      if (tb === 'scale') return v.uniform === 'yes' ? { sx: numIn(v.pct) / 100, sy: numIn(v.pct) / 100 } : { sx: numIn(v.pctX) / 100, sy: numIn(v.pctY) / 100 };
      return v.axis === 'angle' ? { deg: 2 * numIn(v.axisDeg), reflect: 'h' } : { reflect: v.axis === 'horizontal' ? 'h' : 'v' };
    };
    const bad = t => Object.keys(t).some(k => k !== 'reflect' && !fin(t[k])) || (fin(t.sx) && !(Math.abs(t.sx) > 1e-6)) || (fin(t.sy) && !(Math.abs(t.sy) > 1e-6));
    return dialog(ctx, {
      id: 'transform', tab, tabs: [['move', 'Move'], ['rotate', 'Rotate'], ['scale', 'Scale'], ['reflect', 'Reflect']], copy: true, preview: true,
      title: tb => ({ move: 'Move', rotate: 'Rotate', scale: 'Scale', reflect: 'Reflect' }[tb]),
      note: tb => ({ move: 'By how far, in mm: right and down are positive.', rotate: 'Degrees clockwise, about the point below.', scale: 'Percent of the size now.', reflect: 'Mirror across a line through the point below.' }[tb]),
      values: { dx: '0', dy: '0', deg: '90', pct: '100', pctX: '100', pctY: '100', uniform: 'yes', axis: 'vertical', axisDeg: '45', pivot: 'selection' },
      fields: (tb, v) => tb === 'move' ? [{ id: 'dx', label: 'Across', unit: 'mm', step: 0.1 }, { id: 'dy', label: 'Down', unit: 'mm', step: 0.1 }]
        : tb === 'rotate' ? [{ id: 'deg', label: 'Angle', unit: '°', step: 1 }, { id: 'pivot', type: 'seg', label: 'About', options: PIVOTS }]
          : tb === 'scale' ? [{ id: 'uniform', type: 'seg', label: 'Proportions', options: [['yes', 'Kept'], ['no', 'Free']] }].concat(v.uniform === 'yes' ? [{ id: 'pct', label: 'Scale', unit: '%', step: 1 }] : [{ id: 'pctX', label: 'Across', unit: '%', step: 1 }, { id: 'pctY', label: 'Down', unit: '%', step: 1 }], [{ id: 'pivot', type: 'seg', label: 'About', options: PIVOTS }])
            : [{ id: 'axis', type: 'seg', label: 'Across a', options: [['vertical', 'Upright'], ['horizontal', 'Level'], ['angle', 'Angle']] }].concat(v.axis === 'angle' ? [{ id: 'axisDeg', label: 'Line at', unit: '°', step: 1 }] : [], [{ id: 'pivot', type: 'seg', label: 'About', options: PIVOTS }]),
      result: (v, tb, copy) => { const t = tOf(v, tb); if (bad(t)) return { error: 'A number in every field' }; return ops.transformItems(ctx.items(), ctx.selection(), ctx.geo, t, { copy, pivot: tb === 'move' ? 'selection' : v.pivot, frame: ctx.frame() }); }
    }).then(a => {
      if (!a) return false;
      const t = tOf(a.vals, a.tab), o = { copy: a.copy, pivot: a.tab === 'move' ? 'selection' : a.vals.pivot };
      if (!land(ctx, a.r, a.tab + (a.copy ? '+copy' : ''))) return false;
      remember(ctx, t, o); return true;
    });
  }
  const quick = (ctx, t, what) => { const o = { copy: false, pivot: 'selection' }; const r = ops.transformItems(ctx.items(), ctx.selection(), ctx.geo, t, Object.assign({ frame: ctx.frame() }, o)); if (land(ctx, r, what)) remember(ctx, t, o); };

  /* ── the commands (the editor lists them by group in the head's Object menu; `context` ones also in the right-click menu; `bar` ones are buttons on the property bar) ── */
  const any = ctx => nSel(ctx) > 0, two = ctx => nSel(ctx) >= 2;
  const commands = {
    'ops.alignBar': { title: 'Align and distribute', group: 'Align', icon: IC.align + IC.caret, bar: true, menu: false, enabled: any, run: ctx => openFly(ctx, 'align', 'ops.alignBar') },
    'ops.pfBar': { title: 'Pathfinder', group: 'Pathfinder', icon: IC.unite + IC.caret, bar: true, menu: false, enabled: ctx => two(ctx) || canUniteOne(ctx), run: ctx => openFly(ctx, 'pf', 'ops.pfBar') },
    'ops.unite': { title: 'Unite', group: 'Pathfinder', icon: IC.unite, enabled: ctx => two(ctx) || canUniteOne(ctx), run: ctx => runPathfinder(ctx, 'unite') },
    'ops.minus': { title: 'Minus Front', group: 'Pathfinder', icon: IC.minus, enabled: two, run: ctx => runPathfinder(ctx, 'minus') },
    'ops.intersect': { title: 'Intersect', group: 'Pathfinder', icon: IC.intersect, enabled: two, context: false, run: ctx => runPathfinder(ctx, 'intersect') },
    'ops.exclude': { title: 'Exclude', group: 'Pathfinder', icon: IC.exclude, enabled: two, context: false, run: ctx => runPathfinder(ctx, 'exclude') },
    'ops.divide': { title: 'Divide', group: 'Pathfinder', icon: IC.divide, enabled: two, context: false, run: ctx => runPathfinder(ctx, 'divide') },
    'ops.trim': { title: 'Trim', group: 'Pathfinder', icon: IC.trim, enabled: two, context: false, run: ctx => runPathfinder(ctx, 'trim') },
    'ops.offset': { title: 'Offset Path…', group: 'Path', icon: IC.offset, enabled: any, run: ctx => dialog(ctx, {
      id: 'offset', title: 'Offset Path', preview: true, note: 'A new path round each thing selected, this far out (a minus number: in). The original stays.',
      values: { mm: '0.5', join: 'round', miter: '4' },
      fields: (tb, v) => [{ id: 'mm', label: 'Offset', unit: 'mm', step: 0.05 }, { id: 'join', type: 'seg', label: 'Corners', options: [['round', 'Round'], ['miter', 'Miter'], ['bevel', 'Bevel']] }].concat(v.join === 'miter' ? [{ id: 'miter', label: 'Miter limit', unit: '×', step: 1 }] : []),
      result: v => (fin(numIn(v.mm)) ? ops.offsetItems(ctx.items(), ctx.selection(), ctx.geo, { mm: numIn(v.mm), join: v.join, miter: numIn(v.miter) }) : { error: 'How far, in mm?' })
    }).then(a => !!a && land(ctx, a.r, 'offset')) },
    'ops.contour': { title: 'Cut line round the art…', group: 'Path', icon: IC.cutline, enabled: any, run: ctx => dialog(ctx, {
      id: 'contour', title: 'Cut line round the art', ok: 'Make the cut line', preview: true, note: 'One cut line round everything selected, this far from it: round corners, every hole filled, the parts joined. The art stays.',
      values: { mm: '0.8' }, fields: [{ id: 'mm', label: 'Gap', unit: 'mm', step: 0.05 }],
      result: v => (numIn(v.mm) >= 0 ? ops.offsetItems(ctx.items(), ctx.selection(), ctx.geo, { mm: numIn(v.mm), contour: true }) : { error: 'A gap of 0 mm or more' })
    }).then(a => !!a && land(ctx, a.r, 'contourCut')) },
    'ops.outline': { title: 'Outline Stroke', group: 'Path', icon: IC.outline, enabled: ctx => leavesSel(ctx).some(l => l.role !== 'cut' && MD().paintStroke(l)), run: ctx => land(ctx, ops.outlineStrokeItems(ctx.items(), ctx.selection(), ctx.geo), 'outlineStroke') },
    'ops.compound': { title: 'Make Compound Path', group: 'Path', icon: IC.compound, key: 'Mod+8', enabled: ctx => leavesSel(ctx).length >= 2, run: ctx => land(ctx, ops.makeCompound(ctx.items(), ctx.selection(), ctx.geo), 'compound') },
    'ops.release': { title: 'Release Compound Path', group: 'Path', icon: IC.release, key: 'Mod+Alt+Shift+8', enabled: ctx => leavesSel(ctx).some(l => l.kind === 'path' && (l.subpaths || []).length > 1), run: ctx => land(ctx, ops.releaseCompound(ctx.items(), ctx.selection(), ctx.geo), 'release') },
    'ops.round': { title: 'Round Corners…', group: 'Path', icon: IC.corners, context: false, enabled: hasPath, run: ctx => dialog(ctx, {
      id: 'round', title: 'Round Corners', preview: true, note: 'Every corner of the selected paths rounded to this radius (a side too short for it takes what fits).',
      values: { mm: '1' }, fields: [{ id: 'mm', label: 'Radius', unit: 'mm', step: 0.1 }],
      result: v => ops.roundCornerItems(ctx.items(), ctx.selection(), ctx.geo, numIn(v.mm))
    }).then(a => !!a && land(ctx, a.r, 'roundCorners')) },
    'ops.move': { title: 'Move…', group: 'Transform', icon: IC.move, key: 'Mod+Shift+M', context: false, enabled: any, run: ctx => transformDialog(ctx, 'move') },
    'ops.rotate': { title: 'Rotate…', group: 'Transform', icon: IC.rotate, context: false, enabled: any, run: ctx => transformDialog(ctx, 'rotate') },
    'ops.scale': { title: 'Scale…', group: 'Transform', icon: IC.scale, context: false, enabled: any, run: ctx => transformDialog(ctx, 'scale') },
    'ops.reflect': { title: 'Reflect…', group: 'Transform', icon: IC.reflect, context: false, enabled: any, run: ctx => transformDialog(ctx, 'reflect') },
    'ops.flipH': { title: 'Flip left to right', group: 'Transform', icon: IC.flipH, enabled: any, run: ctx => quick(ctx, { reflect: 'v' }, 'flipH') },
    'ops.flipV': { title: 'Flip top to bottom', group: 'Transform', icon: IC.flipV, enabled: any, run: ctx => quick(ctx, { reflect: 'h' }, 'flipV') },
    'ops.rot90': { title: 'Rotate 90° clockwise', group: 'Transform', icon: IC.rot90, enabled: any, run: ctx => quick(ctx, { deg: 90 }, 'rotate90') },
    'ops.rot90l': { title: 'Rotate 90° anticlockwise', group: 'Transform', icon: IC.rot90l, context: false, enabled: any, run: ctx => quick(ctx, { deg: -90 }, 'rotate-90') },
    'ops.again': { title: 'Transform Again', group: 'Transform', icon: IC.again, key: 'Mod+Shift+D', context: false, enabled: ctx => !!last && any(ctx), run: ctx => again(ctx) },
    'ops.repeat': { title: 'Step and Repeat…', group: 'Transform', icon: IC.repeat, context: false, enabled: any, run: ctx => dialog(ctx, {
      id: 'repeat', title: 'Step and Repeat', preview: true, note: 'Copies, each one step further than the last: moved, turned and scaled by the step. Turn about the charm for a ring round it.',
      values: { count: '3', dx: '2', dy: '0', deg: '0', pct: '100', pivot: 'selection' },
      fields: [{ id: 'count', label: 'Copies', unit: '', step: 1 }, { id: 'dx', label: 'Across', unit: 'mm', step: 0.1 }, { id: 'dy', label: 'Down', unit: 'mm', step: 0.1 }, { id: 'deg', label: 'Turn', unit: '°', step: 1 }, { id: 'pct', label: 'Scale', unit: '%', step: 1 }, { id: 'pivot', type: 'seg', label: 'Turn about', options: PIVOTS.slice(0, 2) }],
      result: v => { const t = { dx: numIn(v.dx) * PT, dy: numIn(v.dy) * PT, deg: numIn(v.deg), sx: numIn(v.pct) / 100, sy: numIn(v.pct) / 100 }; if (Object.values(t).some(x => !fin(x)) || !(t.sx > 0)) return { error: 'A number in every field' }; return ops.stepRepeatItems(ctx.items(), ctx.selection(), ctx.geo, t, { count: numIn(v.count), pivot: v.pivot, frame: ctx.frame() }); }
    // (it is its own repeat: ⌘D after it duplicates what it left selected, the originals and every copy, rather than turning the whole ring once more)
    }).then(a => { if (!a || !land(ctx, a.r, 'stepRepeat')) return false; last = null; return true; }) },
    'ops.centre': { title: 'Centre on the charm', group: 'Align', icon: IC.centre, enabled: any, run: ctx => land(ctx, ops.centreOnCharm(ctx.items(), ctx.selection(), ctx.geo, ctx.frame()), 'centreOnCharm') },
    'ops.sameRole': { title: 'Select the same role', group: 'Select', icon: IC.same, enabled: any, run: ctx => land(ctx, ops.selectSame(ctx.items(), ctx.selection(), ctx.geo, 'role'), 'selectSame:role') },
    'ops.sameFill': { title: 'Select the same fill', group: 'Select', icon: IC.same, context: false, enabled: any, run: ctx => land(ctx, ops.selectSame(ctx.items(), ctx.selection(), ctx.geo, 'fill'), 'selectSame:fill') },
    'ops.sameStroke': { title: 'Select the same line', group: 'Select', icon: IC.same, context: false, enabled: any, run: ctx => land(ctx, ops.selectSame(ctx.items(), ctx.selection(), ctx.geo, 'stroke'), 'selectSame:stroke') },
    'ops.inverse': { title: 'Select Inverse', group: 'Select', icon: IC.inverse, context: false, run: ctx => land(ctx, ops.selectInverse(ctx.items(), ctx.selection(), ctx.geo), 'selectInverse') },
    'ops.lockOthers': { title: 'Lock everything else', group: 'Lock and hide', icon: IC.lock, context: false, enabled: any, run: ctx => land(ctx, ops.flagOthers(ctx.items(), ctx.selection(), ctx.geo, 'locked'), 'lockOthers') },
    'ops.unlockAll': { title: 'Unlock all', group: 'Lock and hide', icon: IC.lock, context: false, run: ctx => land(ctx, ops.clearFlagAll(ctx.items(), 'locked'), 'unlockAll') },
    'ops.hideOthers': { title: 'Hide everything else', group: 'Lock and hide', icon: IC.eye, context: false, enabled: any, run: ctx => land(ctx, ops.flagOthers(ctx.items(), ctx.selection(), ctx.geo, 'hidden'), 'hideOthers') },
    'ops.showAll': { title: 'Show all', group: 'Lock and hide', icon: IC.eye, context: false, run: ctx => land(ctx, ops.clearFlagAll(ctx.items(), 'hidden'), 'showAll') }
  };

  /** ⌘D (Lead's keymap, 28 Oct 2026): Transform Again when the last thing done was a transform with a copy (Move / Rotate / ... with Copy, Step and Repeat) and nothing has
   *  changed since; otherwise the editor duplicates as it always has. ⇧⌘D is Transform Again whatever the last transform was. */
  function beforeDuplicate(ctx, e) {
    if (e && e.shiftKey) { if (again(ctx)) ctx.commit('Transform Again'); return true; }
    if (!last || !last.o.copy || ctx.items() !== last.items || ctx.face() !== last.face || !nSel(ctx)) return false;
    if (again(ctx)) ctx.commit('Transform Again');
    return true;
  }

  function install() {
    if (!doc) return;
    const CE = root.CharmEditor || (root.CharmEditor = {});
    CE.commands = CE.commands && typeof CE.commands === 'object' ? CE.commands : {};
    for (const id of Object.keys(commands)) if (!CE.commands[id]) CE.commands[id] = commands[id];
    CE.beforeDuplicate = beforeDuplicate;
    (CE.onOpen = Array.isArray(CE.onOpen) ? CE.onOpen : []).push(ctx => {
      injectCss(); last = null; memo.key = null; closeFly();
      // (a press on a flyout's own button while it is open folds it: the editor closes menus on the press, the click must not open it again)
      const down = e => {
        if (fly && !fly.el.isConnected) fly = null;
        const b = fly && ctx.root.querySelector(`.ce-xbar [data-xcmd="${fly.cmdId}"]`);   // (the bar is drawn again as the selection changes: its button is found afresh)
        flySkip = !!(b && e.target.closest && b.contains(e.target));
      };
      doc.addEventListener('pointerdown', down, true);
      ctx.root.addEventListener('click', flyClick, true);
      ctx.__opsOff = () => { doc.removeEventListener('pointerdown', down, true); };
    });
    (CE.onClose = Array.isArray(CE.onClose) ? CE.onClose : []).push(ctx => { closeFly(); last = null; if (ctx && ctx.__opsOff) ctx.__opsOff(); });
  }

  return { kernel, ops, commands, install, again: beforeDuplicate, MM, PT };
});
