/* charm-nest-ce-paths.js — DEFORMING LINES in the charm editor: the Pen, Direct Selection, the anchor tools, Scissors, Join, Simplify, Smooth and the Pencil (Design & Engrave,
 * item 9a; Paul, 28 Oct 2026: "designing custom charms, which requires blinds and deforming lines basically a (Designer) of a charm would use to manipulate a vector illustrator
 * file and design a charm design. You need to follow the 70/30 Ruel where we're looking only for the most commonly used 70% of the features"; the lead's D3: "deforming lines" =
 * direct editing of anchors and curve handles, the pen ...).
 *
 * Illustrator's own behaviour, the 70% a charm designer reaches for (Adobe's help pages on the Pen, the Anchor Point tools, Join, Scissors, Simplify and the Pencil's options):
 *   Pen (P)              click: a corner anchor · drag: a smooth anchor, its handles pulled out (Alt while dragging breaks them: the outgoing handle moves alone · Shift keeps the
 *                        handle, or a clicked anchor, to 45°) · click the first anchor (the ring) to close · click the last anchor to take its outgoing handle in (drag: pull a new
 *                        one alone) · an end of an open path: click it to go on drawing from there; while drawing, click another path's end to connect them · over a selected
 *                        path: a segment adds an anchor, an anchor deletes it (Illustrator's Auto Add/Delete) · Esc or Enter ends the path · ⌘/Ctrl-click ends it and drops it
 *   Direct Selection (A) anchors and handles: click an anchor (Shift adds or takes away), drag on empty space to sweep a box round anchors, drag them; a handle of a smooth anchor
 *                        turns its partner with it (Alt breaks it, Shift: 45°); a segment dragged: a straight one moves its two anchors, a curve bends through the cursor;
 *                        arrows nudge the anchors (0.05 mm, Shift 1 mm; a run of taps is one undo step); Delete takes the anchors out with their segments, as Illustrator does
 *   Add anchor (+)       a click on a segment: an anchor there, the shape unchanged (the curve split where it was clicked)
 *   Delete anchor (−)    a click on an anchor: gone, the path kept joined and as near its old shape as one curve can be (the two curves fitted as one)
 *   Anchor Point (⇧C)    click an anchor: smooth ↔ corner · drag from an anchor: pull out smooth handles · drag a handle: it moves alone (the anchor becomes a corner)
 *   Scissors (C)         a click on a segment or an anchor cuts the path there: a closed path opens; an open path becomes two (the second its own part)
 *   Join (⌘J)            two ends picked with Direct Selection: joined (with a straight segment, or into one anchor when they touch). Whole open paths selected (an imported DXF's
 *                        line soup: hundreds of short lines whose ends only nearly meet): every pair of ends within 0.05 mm made one anchor and every path that nearly closes
 *                        closed, each laser role on its own, a gap wider than that never bridged; when nothing is that near, one open path is closed and two are joined at
 *                        their nearest ends, as Illustrator does
 *   Close path           every open path selected closed (ends within 0.05 mm joined into one anchor, the rest with a straight segment)
 *   Simplify             fewer anchors within a tolerance in mm (a slider, the anchors counted before and after, the result shown as it moves); corners stay corners
 *   Smooth               each press smooths the selected paths a step (bumps and kinks of a hand drawing ironed out)
 *   Pencil (N)           freehand: the stroke is smoothed and fitted with Béziers (corners kept where the hand turned sharply); ending near the start closes it; starting at the
 *                        end of a selected open path carries that path on
 *
 * It works on any path part (a master's part too) where it is drawn: in face coordinates through each part's own transform and its groups' (a group with a transform of its own
 * is pushed into its parts first, as the editor does before any edit inside a group). The part keeps its transform: a turned part's anchors move on screen exactly where they are
 * dragged, and its turn and scale stay on the bar. A locked part (the cut line opens locked) is never touched; a hidden one is not reached. A part keeps its laser role: a red line
 * stays a line, a hatch stays a hatch; a new path is a line engraving (the role menu on the bar changes it, as for a drawn shape). Text is edited as words: these tools leave it be.
 * Every press, drag and key paints in the same frame (only the parts that change are drawn again; the anchors are a handful of SVG paths over the work area); each action is ONE
 * undo step (a pen anchor with its handles, a drag, a run of arrow taps, a command).
 *
 * Loads in the page (window.CharmEditorPaths, and registers with window.CharmEditor's extension points: tools, commands) and in node (module.exports: the pure model, for tests).
 * For an importer (charm-nest-ce-import.js): CharmEditorPaths.model.chainSubpaths(subpaths, tolPt) -> { subpaths, joins, closes } and simplifySubpaths(subpaths, tolPt) work on
 * the editor's ops directly, so a drawing read in as line soup can arrive as whole paths.
 */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmEditorPaths = api;
  if (root && root.document && typeof api.register === 'function') api.register(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const MM = 25.4 / 72, PT = 72 / 25.4;
  const NUDGE_MM = 0.05, NUDGE_BIG_MM = 1;            // (the editor's own: Paul, 27 Oct 2026, arrows 0.05 mm, Shift + arrow 1 mm)
  const LW_LASER_PT = 0.25;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r9 = v => { const r = Math.round(v * 1e9) / 1e9; return Object.is(r, -0) ? 0 : r; };
  const R9 = p => [r9(p[0]), r9(p[1])];

  /* ═══ 1 · small vector and matrix maths (points [x, y]; matrices in SVG order [a,b,c,d,e,f]) ═══ */
  const add = (a, b) => [a[0] + b[0], a[1] + b[1]];
  const sub = (a, b) => [a[0] - b[0], a[1] - b[1]];
  const mulS = (a, s) => [a[0] * s, a[1] * s];
  const dot = (a, b) => a[0] * b[0] + a[1] * b[1];
  const len = a => Math.hypot(a[0], a[1]);
  const dist = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1]);
  const lerp = (a, b, t) => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
  const unit = a => { const l = len(a); return l > 1e-12 ? [a[0] / l, a[1] / l] : [0, 0]; };
  const same = (a, b, eps) => !!a && !!b && Math.abs(a[0] - b[0]) <= (eps || 1e-9) && Math.abs(a[1] - b[1]) <= (eps || 1e-9);
  const mul = (m, n) => [m[0] * n[0] + m[2] * n[1], m[1] * n[0] + m[3] * n[1], m[0] * n[2] + m[2] * n[3], m[1] * n[2] + m[3] * n[3], m[0] * n[4] + m[2] * n[5] + m[4], m[1] * n[4] + m[3] * n[5] + m[5]];
  const ap = (m, p) => [m[0] * p[0] + m[2] * p[1] + m[4], m[1] * p[0] + m[3] * p[1] + m[5]];
  const apL = (m, p) => [m[0] * p[0] + m[2] * p[1], m[1] * p[0] + m[3] * p[1]];
  function inv(m) {
    const det = m[0] * m[3] - m[1] * m[2]; if (!det || !Number.isFinite(det)) return null;
    const a = m[3] / det, b = -m[1] / det, c = -m[2] / det, d = m[0] / det;
    return [a, b, c, d, -(a * m[4] + c * m[5]), -(b * m[4] + d * m[5])];
  }
  const I = Object.freeze([1, 0, 0, 1, 0, 0]);
  const isI = m => Math.abs(m[0] - 1) < 1e-12 && Math.abs(m[1]) < 1e-12 && Math.abs(m[2]) < 1e-12 && Math.abs(m[3] - 1) < 1e-12 && Math.abs(m[4]) < 1e-9 && Math.abs(m[5]) < 1e-9;
  /** A point turned about `o` to the nearest 45° (Shift). */
  function snap45(o, p) {
    const d = sub(p, o), L = len(d); if (L < 1e-12) return p.slice();
    const a = Math.round(Math.atan2(d[1], d[0]) / (Math.PI / 4)) * (Math.PI / 4);
    return [o[0] + Math.cos(a) * L, o[1] + Math.sin(a) * L];
  }
  const bez = (p0, p1, p2, p3, t) => { const u = 1 - t, a = u * u * u, b = 3 * u * u * t, c = 3 * u * t * t, d = t * t * t; return [a * p0[0] + b * p1[0] + c * p2[0] + d * p3[0], a * p0[1] + b * p1[1] + c * p2[1] + d * p3[1]]; };
  const bezD = (p0, p1, p2, p3, t) => { const u = 1 - t; return [3 * (u * u * (p1[0] - p0[0]) + 2 * u * t * (p2[0] - p1[0]) + t * t * (p3[0] - p2[0])), 3 * (u * u * (p1[1] - p0[1]) + 2 * u * t * (p2[1] - p1[1]) + t * t * (p3[1] - p2[1]))]; };
  const bezDD = (p0, p1, p2, p3, t) => { const u = 1 - t; return [6 * (u * (p2[0] - 2 * p1[0] + p0[0]) + t * (p3[0] - 2 * p2[0] + p1[0])), 6 * (u * (p2[1] - 2 * p1[1] + p0[1]) + t * (p3[1] - 2 * p2[1] + p1[1]))]; };
  function bboxPts(pts) {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const p of pts) { if (p[0] < x0) x0 = p[0]; if (p[0] > x1) x1 = p[0]; if (p[1] < y0) y0 = p[1]; if (p[1] > y1) y1 = p[1]; }
    return x0 <= x1 ? [x0, y0, x1, y1] : null;
  }

  /* ═══ 2 · CONTOURS: a path's subpaths as anchors with handles, the way a person sees them ═══
     contour { closed, pts: [{ p, i, o }] }   p the anchor, i its handle INTO it (from the segment before), o its handle OUT of it (null: none, the segment is straight there).
     Read from the editor's ops (["m",p] ["l",p] ["c",c1,c2,p] ["h"]); a closed subpath that returns to its start by an explicit last point (as most drawings do) is read as one
     anchor there, the closing curve's handle its `i`. A contour nobody edits is written back as the very ops it came from (`src`), so an edit to one anchor of a letter changes
     only the subpath it is in. */
  function toContours(sps) {
    const out = [];
    for (const sp of sps || []) {
      const mine = [];
      let c = null;
      const flush = () => { if (c && c.pts.length) mine.push(c); c = null; };
      const start = p => { flush(); c = { closed: false, pts: [{ p: [p[0], p[1]], i: null, o: null }] }; };
      for (const op of sp || []) {
        const k = op && op[0];
        if (k === 'm') start(op[1]);
        else if (k === 'l' || k === 'c') {
          if (!c) { const last = mine.length ? mine[mine.length - 1] : null; start(last ? last.pts[0].p : (k === 'l' ? op[1] : op[3])); if (!last) continue; }
          else if (c.closed) start(c.pts[0].p);
          const prev = c.pts[c.pts.length - 1];
          if (k === 'l') c.pts.push({ p: [op[1][0], op[1][1]], i: null, o: null });
          else {
            const p = [op[3][0], op[3][1]];
            prev.o = same(op[1], prev.p) ? null : [op[1][0], op[1][1]];
            c.pts.push({ p, i: same(op[2], p) ? null : [op[2][0], op[2][1]], o: null });
          }
        } else if (k === 'h') {
          if (!c) continue;
          c.closed = true;
          const n = c.pts.length;
          if (n >= 2 && same(c.pts[n - 1].p, c.pts[0].p, 1e-6)) { c.pts[0].i = c.pts[n - 1].i; c.pts.pop(); }
          flush();
        }
      }
      flush();
      if (mine.length === 1) mine[0].src = sp;   // (one contour in that subpath: it can be written back as it came)
      out.push(...mine);
    }
    return out;
  }
  /** The editor's ops of contours (an unedited one as it came; points to a billionth of a point). */
  function toSubpaths(cs) {
    const out = [];
    for (const c of cs || []) {
      if (!c || !c.pts || !c.pts.length) continue;
      if (c.src) { out.push(c.src); continue; }
      const P = c.pts, n = P.length, sp = [['m', R9(P[0].p)]];
      const seg = (a, b) => { if (!a.o && !b.i) sp.push(['l', R9(b.p)]); else sp.push(['c', R9(a.o || a.p), R9(b.i || b.p), R9(b.p)]); };
      for (let k = 1; k < n; k++) seg(P[k - 1], P[k]);
      if (c.closed) { if (n > 1 && (P[n - 1].o || P[0].i)) seg(P[n - 1], P[0]); else if (n === 1 && (P[0].o || P[0].i)) seg(P[0], P[0]); sp.push(['h']); }
      out.push(sp);
    }
    return out;
  }
  const mkC = (pts, closed) => ({ closed: !!closed, pts });
  const cloneA = a => ({ p: a.p.slice(), i: a.i ? a.i.slice() : null, o: a.o ? a.o.slice() : null });
  const nSegs = c => (c.closed ? (c.pts.length > 1 ? c.pts.length : 0) : Math.max(0, c.pts.length - 1));
  /** Segment k of a contour as its four control points (a straight one has its handles on its ends). */
  function segOf(c, k) {
    const n = c.pts.length, a = c.pts[k], b = c.pts[(k + 1) % n];
    return { a, b, ka: k, kb: (k + 1) % n, curve: !!(a.o || b.i), q: [a.p, a.o || a.p, b.i || b.p, b.p] };
  }
  /** Is the anchor smooth: two handles in one line through it (2°). */
  function isSmooth(a) {
    if (!a.i || !a.o) return false;
    const u = sub(a.o, a.p), v = sub(a.p, a.i), lu = len(u), lv = len(v); if (lu < 1e-9 || lv < 1e-9) return false;
    return dot(u, v) > 0 && Math.abs(u[0] * v[1] - u[1] * v[0]) / (lu * lv) < 0.035;
  }
  const isEnd = (c, k) => !c.closed && (k === 0 || k === c.pts.length - 1);
  function reverseC(c) {
    const pts = c.pts.slice().reverse().map(a => ({ p: a.p.slice(), i: a.o ? a.o.slice() : null, o: a.i ? a.i.slice() : null }));
    return mkC(pts, c.closed);
  }
  /** The direction a segment leaves its start and arrives at its end (its first and last handle that is not on its anchor). */
  function tanOut(q) { for (const p of [q[1], q[2], q[3]]) { const d = sub(p, q[0]); if (len(d) > 1e-9) return unit(d); } return [0, 0]; }
  function tanIn(q) { for (const p of [q[2], q[1], q[0]]) { const d = sub(q[3], p); if (len(d) > 1e-9) return unit(d); } return [0, 0]; }

  /* ═══ 3 · anchor edits (each makes a NEW contour; the one given is never changed) ═══ */
  /** The segment k split at t (de Casteljau): an anchor there, the shape exactly as it was. → { c, k } (k: the new anchor's index) */
  function splitSeg(c, k, t) {
    const n = c.pts.length, s = segOf(c, k), pts = c.pts.map(cloneA);
    let na;
    if (!s.curve) na = { p: lerp(s.a.p, s.b.p, t), i: null, o: null };
    else {
      const [p0, p1, p2, p3] = s.q;
      const p01 = lerp(p0, p1, t), p12 = lerp(p1, p2, t), p23 = lerp(p2, p3, t), p012 = lerp(p01, p12, t), p123 = lerp(p12, p23, t), p0123 = lerp(p012, p123, t);
      pts[k].o = p01; pts[(k + 1) % n].i = p23;
      na = { p: p0123, i: p012, o: p123 };
    }
    pts.splice(k + 1, 0, na);
    return { c: mkC(pts, c.closed), k: k + 1 };
  }
  /** Generate one cubic from a to b through the points `d` (Schneider's least squares for the two handle lengths, the directions given). */
  function generateBezier(d, u, a, b, t1, t2) {
    let c00 = 0, c01 = 0, c11 = 0, x0 = 0, x1 = 0;
    for (let i = 0; i < d.length; i++) {
      const s = u[i], m = 1 - s, b0 = m * m * m, b1 = 3 * m * m * s, b2 = 3 * m * s * s, b3 = s * s * s;
      const A0 = mulS(t1, b1), A1 = mulS(t2, b2);
      c00 += dot(A0, A0); c01 += dot(A0, A1); c11 += dot(A1, A1);
      const tmp = sub(d[i], add(mulS(a, b0 + b1), mulS(b, b2 + b3)));
      x0 += dot(A0, tmp); x1 += dot(A1, tmp);
    }
    const det = c00 * c11 - c01 * c01, L = dist(a, b);
    let al = det ? (x0 * c11 - x1 * c01) / det : 0, ar = det ? (c00 * x1 - c01 * x0) / det : 0;
    if (!(al > 1e-6 * L) || !(ar > 1e-6 * L) || al > 4 * L || ar > 4 * L) { al = L / 3; ar = L / 3; }
    return [a, add(a, mulS(t1, al)), add(b, mulS(t2, ar)), b];
  }
  function chordParams(d) {
    const u = [0]; for (let i = 1; i < d.length; i++) u.push(u[i - 1] + dist(d[i], d[i - 1]));
    const L = u[u.length - 1] || 1; return u.map(v => v / L);
  }
  function newtonParams(q, d, u) {
    return u.map((s, i) => {
      const Q = bez(q[0], q[1], q[2], q[3], s), Q1 = bezD(q[0], q[1], q[2], q[3], s), Q2 = bezDD(q[0], q[1], q[2], q[3], s), e = sub(Q, d[i]);
      const den = dot(Q1, Q1) + dot(e, Q2); if (!den) return s;
      return clamp(s - dot(e, Q1) / den, 0, 1);
    });
  }
  function maxErr(q, d, u) {
    let worst = 0, at = (d.length / 2) | 0;
    for (let i = 1; i < d.length - 1; i++) { const e = dist(bez(q[0], q[1], q[2], q[3], u[i]), d[i]); if (e > worst) { worst = e; at = i; } }
    return { e: worst, at };
  }
  /** Fit cubics through points d within tol (Schneider, Graphics Gems I, "An algorithm for automatically fitting digitized curves"): the end directions t1 (leaving d[0]) and
   *  t2 (pointing back into the curve from its last point) may be given. → [[p0,p1,p2,p3], ...] */
  function fitCubics(d, tol, t1, t2) {
    const out = [];
    if (!d || d.length < 2) return out;
    const n = d.length;
    // (an end's direction, when none is given, from the points within about two tolerances of it: a hand's last jitter must not hook the line)
    const reach = (i0, di) => { let i = i0, L = 0; while (i + di >= 0 && i + di < n && L < 2 * tol) { L += dist(d[i], d[i + di]); i += di; } return i === i0 ? i0 + di : i; };
    t1 = t1 && len(t1) > 1e-12 ? unit(t1) : unit(sub(d[reach(0, 1)], d[0]));
    t2 = t2 && len(t2) > 1e-12 ? unit(t2) : unit(sub(d[reach(n - 1, -1)], d[n - 1]));
    const rec = (lo, hi, ta, tb, depth) => {
      const pts = d.slice(lo, hi + 1);
      if (pts.length === 2 || depth > 40) { const L = dist(pts[0], pts[pts.length - 1]) / 3; out.push([pts[0], add(pts[0], mulS(ta, L)), add(pts[pts.length - 1], mulS(tb, L)), pts[pts.length - 1]]); return; }
      let u = chordParams(pts), q = generateBezier(pts, u, pts[0], pts[pts.length - 1], ta, tb), m = maxErr(q, pts, u);
      if (m.e <= tol) { out.push(q); return; }
      if (m.e <= tol * 4) {
        for (let it = 0; it < 4; it++) { u = newtonParams(q, pts, u); q = generateBezier(pts, u, pts[0], pts[pts.length - 1], ta, tb); m = maxErr(q, pts, u); if (m.e <= tol) { out.push(q); return; } }
      }
      const s = clamp(lo + m.at, lo + 1, hi - 1);
      let tc = unit(sub(d[Math.max(lo, s - 2)], d[Math.min(hi, s + 2)])); if (len(tc) < 1e-12) tc = unit(sub(d[s - 1], d[s]));
      rec(lo, s, ta, tc, depth + 1); rec(s, hi, mulS(tc, -1), tb, depth + 1);
    };
    rec(0, n - 1, t1, t2, 0);
    return out;
  }
  /** Points along segments (the anchors among them), about `step` apart and never fewer than `minPer` on a curve. */
  function sampleSegs(segs, step, minPer) {
    const d = [];
    segs.forEach((s, j) => {
      if (!j) d.push(s.q[0]);
      if (!s.curve) { const L = dist(s.q[0], s.q[3]), n = clamp(Math.ceil(L / step), 1, 400); for (let i = 1; i <= n; i++) d.push(lerp(s.q[0], s.q[3], i / n)); return; }
      const L = dist(s.q[0], s.q[1]) + dist(s.q[1], s.q[2]) + dist(s.q[2], s.q[3]), n = clamp(Math.ceil(L / step), minPer || 6, 400);
      for (let i = 1; i <= n; i++) d.push(bez(s.q[0], s.q[1], s.q[2], s.q[3], i / n));
    });
    return d;
  }
  /** The anchor k taken out, the path kept joined: the two segments either side fitted as one curve with the directions they had at their far ends (both straight: one straight
   *  segment). An end of an open path simply goes with its segment. → the contour, or null when too little is left. */
  function removeAnchor(c, k) {
    const n = c.pts.length;
    if (!c.closed && (k === 0 || k === n - 1)) { if (n <= 2) return null; const pts = c.pts.map(cloneA); pts.splice(k, 1); if (k === 0) pts[0].i = null; else pts[pts.length - 1].o = null; return mkC(pts, false); }
    if (n <= 2) return null;
    const ka = (k - 1 + n) % n, kb = (k + 1) % n, s1 = segOf(c, ka), s2 = segOf(c, k);
    const pts = c.pts.map(cloneA), A = pts[ka], B = pts[kb];
    if (!s1.curve && !s2.curve) { A.o = null; B.i = null; }
    else {
      const d = sampleSegs([s1, s2], Math.max(dist(A.p, B.p) / 40, 1e-6), 16);
      const t1 = tanOut(s1.q), t2 = mulS(tanIn(s2.q), -1);
      let u = chordParams(d), q = generateBezier(d, u, A.p, B.p, t1, t2);
      for (let it = 0; it < 6; it++) { u = newtonParams(q, d, u); q = generateBezier(d, u, A.p, B.p, t1, t2); }
      A.o = q[1]; B.i = q[2];
    }
    pts.splice(k, 1);
    if (c.closed && pts.length < 2) return null;
    return mkC(pts, c.closed);
  }
  /** Illustrator's Delete key on anchors: the anchors go WITH the segments that touch them; what is left is open paths (a run of one anchor is dropped). → [contours] */
  function deleteAnchors(c, set) {
    const n = c.pts.length, gone = k => set.has(k);
    if (![...Array(n).keys()].some(gone)) return [c];
    const runs = [];
    if (c.closed) {
      const s0 = [...Array(n).keys()].find(gone);
      let run = [];
      for (let j = 1; j <= n; j++) { const k = (s0 + j) % n; if (gone(k)) { if (run.length) runs.push(run); run = []; } else run.push(k); }
      if (run.length) runs.push(run);
    } else {
      let run = [];
      for (let k = 0; k < n; k++) { if (gone(k)) { if (run.length) runs.push(run); run = []; } else run.push(k); }
      if (run.length) runs.push(run);
    }
    return runs.filter(r => r.length >= 2).map(r => { const pts = r.map(k => cloneA(c.pts[k])); pts[0].i = null; pts[pts.length - 1].o = null; return mkC(pts, false); });
  }
  /** Scissors at anchor k: a closed path opens there (its ends on that very point); an open one becomes two. → [contours] (the original when there is nothing to cut) */
  function cutAt(c, k) {
    const n = c.pts.length;
    if (c.closed) {
      const pts = []; for (let j = 0; j <= n; j++) pts.push(cloneA(c.pts[(k + j) % n]));
      pts[0].i = null; pts[n].o = null;
      return [mkC(pts, false)];
    }
    if (k <= 0 || k >= n - 1) return [c];
    const a = c.pts.slice(0, k + 1).map(cloneA), b = c.pts.slice(k).map(cloneA);
    a[a.length - 1].o = null; b[0].i = null;
    return [mkC(a, false), mkC(b, false)];
  }
  /** Auto handles for anchor k (a third of the way to each neighbour, along the line from the one before to the one after); an anchor that has two handles keeps their
   *  lengths and only straightens them into one line. */
  function smoothAnchor(c, k) {
    const n = c.pts.length, pts = c.pts.map(cloneA), a = pts[k];
    const prev = c.closed || k > 0 ? c.pts[(k - 1 + n) % n] : null, next = c.closed || k < n - 1 ? c.pts[(k + 1) % n] : null;
    if (a.i && a.o) {
      const u = unit(add(unit(sub(a.o, a.p)), unit(sub(a.p, a.i)))); if (len(u) < 1e-9) return c;
      a.o = add(a.p, mulS(u, dist(a.o, a.p))); a.i = sub(a.p, mulS(u, dist(a.i, a.p)));
      return mkC(pts, c.closed);
    }
    if (n < 2 || (!prev && !next)) return c;
    let dir = prev && next ? unit(sub(next.p, prev.p)) : next ? unit(sub(next.p, a.p)) : unit(sub(a.p, prev.p));
    if (len(dir) < 1e-9) dir = next ? unit(sub(next.p, a.p)) : [1, 0];
    if (prev && n > 1) a.i = sub(a.p, mulS(dir, dist(prev.p, a.p) / 3));
    if (next && n > 1) a.o = add(a.p, mulS(dir, dist(next.p, a.p) / 3));
    return mkC(pts, c.closed);
  }
  function cornerAnchor(c, k) { const pts = c.pts.map(cloneA); pts[k].i = null; pts[k].o = null; return mkC(pts, c.closed); }
  /** The anchors in `set` (and their handles) moved by d. */
  function moveAnchors(c, set, d) {
    return mkC(c.pts.map((a, k) => (set.has(k) ? { p: add(a.p, d), i: a.i ? add(a.i, d) : null, o: a.o ? add(a.o, d) : null } : cloneA(a))), c.closed);
  }
  /** Handle `which` ('i' | 'o') of anchor k put at q. mode 'smooth': the other handle turns with it (keeping its own length) when the anchor was smooth; 'free': alone;
   *  'mirror': the other is its mirror image (a new smooth anchor pulled out). */
  function setHandle(c, k, which, q, mode, wasSmooth) {
    const pts = c.pts.map(cloneA), a = pts[k], other = which === 'o' ? 'i' : 'o';
    a[which] = same(q, a.p, 1e-9) ? null : q.slice();
    if (mode === 'mirror') a[other] = a[which] ? sub(mulS(a.p, 2), q) : null;
    else if (mode === 'smooth' && wasSmooth && a[other] && a[which]) { const L = dist(a[other], a.p); a[other] = add(a.p, mulS(unit(sub(a.p, q)), L)); }
    return mkC(pts, c.closed);
  }
  /** Two contours joined: the end `e1` of c1 ('first' | 'last') to the end `e2` of c2; ends that touch (within eps) become one anchor. Same contour: closed. */
  function joinContours(c1, e1, c2, e2, eps) {
    if (c2 === c1 || c2 == null) {
      const pts = c1.pts.map(cloneA), n = pts.length;
      if (n >= 2 && dist(pts[0].p, pts[n - 1].p) <= eps) { pts[0].i = pts[n - 1].i; pts.pop(); }
      return mkC(pts, true);
    }
    const a = e1 === 'first' ? reverseC(c1) : mkC(c1.pts.map(cloneA), false), b = e2 === 'last' ? reverseC(c2) : mkC(c2.pts.map(cloneA), false);
    const A = a.pts, B = b.pts;
    if (dist(A[A.length - 1].p, B[0].p) <= eps) { A[A.length - 1].o = B[0].o; B.shift(); }
    return mkC(A.concat(B), false);
  }
  /** A contour mapped by matrix m (anchors and handles). */
  const mapC = (c, m) => mkC(c.pts.map(a => ({ p: ap(m, a.p), i: a.i ? ap(m, a.i) : null, o: a.o ? ap(m, a.o) : null })), c.closed);
  /** LINE SOUP (28 Oct 2026, the lead: an imported DXF is often hundreds of short open lines and arcs whose ends meet only nearly): open contours chained wherever two ends lie
   *  within tol of each other (the nearest pairs first, each end used once; the two ends become one anchor), and a chain whose own two ends meet within tol closed. Ends farther
   *  apart are never bridged here: that is a person's choice (two ends picked, Join). A grid of cells tol wide finds the pairs, so thousands of pieces join at once.
   *  → { chains: [{ members: [indexes into cs, in drawing order], c: the contour, changed }], joins, closes } (closed contours and lone pieces come back unchanged). */
  function chainContours(cs, tol) {
    const n = cs.length, ends = [], grid = new Map(), cell = Math.max(tol, 1e-9);
    const gk = (x, y) => Math.floor(x / cell) + ',' + Math.floor(y / cell);
    cs.forEach((c, j) => {
      if (c.closed || c.pts.length < 2) return;
      for (const s of [0, 1]) { const p = s ? c.pts[c.pts.length - 1].p : c.pts[0].p, e = { j, s, p }; ends.push(e); const k = gk(p[0], p[1]); if (!grid.has(k)) grid.set(k, []); grid.get(k).push(e); }
    });
    const pairs = [];
    for (const e of ends) {
      const cx = Math.floor(e.p[0] / cell), cy = Math.floor(e.p[1] / cell);
      for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) for (const f of grid.get((cx + dx) + ',' + (cy + dy)) || []) {
        if (f === e || (f.j < e.j) || (f.j === e.j && f.s <= e.s)) continue;
        if (f.j === e.j && cs[e.j].pts.length < 3 && !cs[e.j].pts.some(a => a.i || a.o)) continue;   // (a lone straight piece does not close on itself)
        const d = dist(e.p, f.p); if (d <= tol) pairs.push({ e, f, d });
      }
    }
    pairs.sort((a, b) => a.d - b.d);
    const link = new Map(), parent = cs.map((c, j) => j), loop = new Set();
    const find = j => { while (parent[j] !== j) { parent[j] = parent[parent[j]]; j = parent[j]; } return j; };
    const ek = e => e.j * 2 + e.s;
    let joins = 0, closes = 0;
    for (const { e, f } of pairs) {
      if (link.has(ek(e)) || link.has(ek(f))) continue;
      const a = find(e.j), b = find(f.j);
      if (a === b) { if (loop.has(a)) continue; loop.add(a); closes++; } else { parent[a] = b; if (loop.has(a)) loop.add(b); joins++; }
      link.set(ek(e), f); link.set(ek(f), e);
    }
    const done = new Set(), chains = [];
    const walk = (j0, s0) => {   // from contour j0 entered at its end s0 (that end first), following the links
      const members = [], parts = [];
      let j = j0, s = s0;
      for (;;) {
        done.add(j); members.push(j);
        parts.push(s === 0 ? cs[j] : reverseC(cs[j]));
        const out = link.get(j * 2 + (1 - s));
        if (!out || done.has(out.j)) return { members, parts, closedBy: out && out.j === j0 && out.s === s0 };
        j = out.j; s = out.s;
      }
    };
    const emit = r => {
      let c = r.parts[0];
      for (let i = 1; i < r.parts.length; i++) c = joinContours(c, 'last', r.parts[i], 'first', Infinity);
      if (r.closedBy) c = joinContours(c, 'last', null, null, Infinity);
      chains.push({ members: r.members, c, changed: r.members.length > 1 || !!r.closedBy });
    };
    for (let j = 0; j < n; j++) {   // chains with a free end start from it; what is left are loops
      if (done.has(j) || cs[j].closed || cs[j].pts.length < 2) continue;
      const free0 = !link.has(j * 2), free1 = !link.has(j * 2 + 1);
      if (free0 || free1) emit(walk(j, free0 ? 0 : 1));
    }
    for (let j = 0; j < n; j++) if (!done.has(j) && !cs[j].closed && cs[j].pts.length >= 2) emit(walk(j, 0));
    for (let j = 0; j < n; j++) if (!done.has(j)) chains.push({ members: [j], c: cs[j], changed: false });
    return { chains, joins, closes };
  }
  /** chainContours on the editor's ops (for an importer: a DXF read in as line soup made into whole paths before it reaches the editor). → { subpaths, joins, closes } */
  function chainSubpaths(sps, tol) {
    const cs = toContours(sps), r = chainContours(cs, tol);
    return { subpaths: toSubpaths(r.chains.map(ch => ch.c)), joins: r.joins, closes: r.closes };
  }

  /* ═══ 4 · Simplify, Smooth and the Pencil's fit ═══ */
  /** The anchors where a contour turns sharply (more than `deg` between the way in and the way out), and every end of an open one. */
  function cornersOf(c, deg) {
    const n = c.pts.length, cos = Math.cos(deg * Math.PI / 180), out = [];
    for (let k = 0; k < n; k++) {
      if (isEnd(c, k)) { out.push(k); continue; }
      const sIn = segOf(c, (k - 1 + n) % n), sOut = segOf(c, k), a = tanIn(sIn.q), b = tanOut(sOut.q);
      if ((len(a) < 1e-9 || len(b) < 1e-9) || dot(a, b) < cos) out.push(k);
    }
    return out;
  }
  /** Cubics as anchors: [a0, a1, ...] with handles (a near-straight cubic made straight). */
  function anchorsOfFit(cubics, tol) {
    const pts = [{ p: cubics[0][0].slice(), i: null, o: null }];
    for (const q of cubics) {
      const a = pts[pts.length - 1], ch = sub(q[3], q[0]), L = len(ch), nrm = L > 1e-12 ? [-ch[1] / L, ch[0] / L] : [0, 0];
      const off = p => Math.abs(dot(sub(p, q[0]), nrm)), along = p => (L > 1e-12 ? dot(sub(p, q[0]), ch) / (L * L) : 0);
      const straight = L > 1e-12 && off(q[1]) <= tol * 0.02 && off(q[2]) <= tol * 0.02 && along(q[1]) >= -1e-6 && along(q[2]) <= 1 + 1e-6;
      a.o = straight ? null : q[1].slice();
      pts.push({ p: q[3].slice(), i: straight ? null : q[2].slice(), o: null });
    }
    return pts;
  }
  /** Fewer anchors, the drawing kept within tol (the contour's own units): the corners stay, each run between them is fitted again (a run that would come back with more
   *  anchors keeps its own). */
  function simplifyContour(c, tol) {
    const n = c.pts.length; if (n < 3 && !(c.closed && n >= 2)) return c;
    const segsN = nSegs(c); if (!segsN) return c;
    let cr = cornersOf(c, 30);
    const smoothLoop = c.closed && !cr.length;
    if (smoothLoop) cr = [0];
    const runs = [];
    for (let j = 0; j < cr.length; j++) {
      const k0 = cr[j], k1 = j + 1 < cr.length ? cr[j + 1] : (c.closed ? cr[0] + n : n - 1);
      if (k1 <= k0 && !(c.closed && cr.length === 1)) continue;
      const segs = []; for (let k = k0; k < (k1 > k0 ? k1 : k0 + n); k++) segs.push(segOf(c, k % n));
      if (segs.length) runs.push({ k0, segs });
    }
    const outPts = [];
    for (const r of runs) {
      const d = sampleSegs(r.segs, Math.max(tol, 1e-6), 6);
      let t1 = tanOut(r.segs[0].q), t2 = mulS(tanIn(r.segs[r.segs.length - 1].q), -1);
      if (smoothLoop) { const sIn = segOf(c, n - 1), sOut = segOf(c, 0), m = unit(add(tanIn(sIn.q), tanOut(sOut.q))); if (len(m) > 1e-9) { t1 = m; t2 = mulS(m, -1); } }
      const allStraight = r.segs.every(s => !s.curve);
      let fit = allStraight && r.segs.length === 1 ? null : fitCubics(d, tol, t1, t2);
      let run;
      if (!fit || fit.length >= r.segs.length) run = r.segs.map((s, j) => (j ? null : cloneA(s.a))).filter(Boolean).concat(r.segs.map(s => cloneA(s.b)));
      else run = anchorsOfFit(fit, tol);
      // (the run's first anchor keeps the handle it came in with, its last the one it goes out with: those belong to the runs either side)
      if (outPts.length) { const last = outPts[outPts.length - 1]; last.o = run[0].o; run.shift(); }
      else run[0].i = c.closed ? c.pts[r.k0].i : null;
      outPts.push(...run);
    }
    if (c.closed && outPts.length > 1 && same(outPts[outPts.length - 1].p, outPts[0].p, 1e-9)) { outPts[0].i = outPts[outPts.length - 1].i; outPts.pop(); }
    if (!c.closed) outPts[outPts.length - 1].o = null;
    if (outPts.length >= n) return c;
    return mkC(outPts, c.closed);
  }
  /** Points along a whole contour, about `step` apart (closed: the start not repeated at the end). */
  function sampleContour(c, step) {
    const segs = []; for (let k = 0; k < nSegs(c); k++) segs.push(segOf(c, k));
    if (!segs.length) return c.pts.map(a => a.p.slice());
    const d = sampleSegs(segs, step, 4);
    if (c.closed && d.length > 1 && same(d[0], d[d.length - 1], 1e-9)) d.pop();
    return d;
  }
  /** Taubin's smoothing (a pull toward the neighbours, then a push back, so the shape does not shrink): ends of an open line stay. */
  function relax(d, closed, rounds) {
    let p = d.map(q => q.slice());
    const n = p.length; if (n < 3) return p;
    const pass = f => {
      const q = p.map(v => v.slice());
      for (let i = 0; i < n; i++) {
        if (!closed && (i === 0 || i === n - 1)) continue;
        const a = p[(i - 1 + n) % n], b = p[(i + 1) % n];
        q[i] = [p[i][0] + f * ((a[0] + b[0]) / 2 - p[i][0]), p[i][1] + f * ((a[1] + b[1]) / 2 - p[i][1])];
      }
      p = q;
    };
    for (let r = 0; r < rounds; r++) { pass(0.63); pass(-0.67); }
    return p;
  }
  /** Points (a hand's stroke, or a contour sampled) fitted as a contour: sharp turns (more than `cornerDeg` over a few points) kept as corners, the rest smooth cubics within
   *  tol; a closed one is fitted round its loop. */
  function fitPoints(d0, tol, closed, cornerDeg) {
    let d = [];
    for (const p of d0) if (!d.length || dist(p, d[d.length - 1]) > tol * 0.05) d.push(p.slice());
    if (closed && d.length > 2 && dist(d[0], d[d.length - 1]) <= tol * 0.05) d.pop();
    if (d.length < 2) return null;
    if (d.length === 2) return mkC([{ p: d[0], i: null, o: null }, { p: d[1], i: null, o: null }], false);
    const n = d.length, w = 3, cos = Math.cos((cornerDeg || 60) * Math.PI / 180), sharp = [];
    for (let i = 0; i < n; i++) {
      if (!closed && (i < w || i > n - 1 - w)) continue;
      const a = d[(i - w + n) % n], b = d[(i + w) % n], u = unit(sub(d[i], a)), v = unit(sub(b, d[i]));
      if (len(u) > 0 && len(v) > 0 && dot(u, v) < cos) sharp.push({ i, c: dot(u, v) });
    }
    // one corner per sharp turn: the sharpest point of each cluster
    const corners = [];
    for (const s of sharp) { const last = corners[corners.length - 1]; if (last && s.i - last.i <= w) { if (s.c < last.c) corners[corners.length - 1] = s; } else corners.push(s); }
    if (closed && corners.length > 1 && corners[0].i + n - corners[corners.length - 1].i <= w) { if (corners[0].c > corners[corners.length - 1].c) corners.shift(); else corners.pop(); }
    let breaks = corners.map(s => s.i);
    const pts = [];
    if (!closed) {
      breaks = [0].concat(breaks, [n - 1]);
      for (let j = 0; j + 1 < breaks.length; j++) {
        const run = d.slice(breaks[j], breaks[j + 1] + 1); if (run.length < 2) continue;
        const a = anchorsOfFit(fitCubics(run, tol), tol);
        if (pts.length) { pts[pts.length - 1].o = a[0].o; a.shift(); }
        pts.push(...a);
      }
      return pts.length >= 2 ? mkC(pts, false) : null;
    }
    const smoothLoop = !breaks.length;
    if (smoothLoop) breaks = [0];
    for (let j = 0; j < breaks.length; j++) {
      const s = breaks[j], e = j + 1 < breaks.length ? breaks[j + 1] : breaks[0] + n;
      const run = []; for (let i = s; i <= e; i++) run.push(d[i % n]);
      let t1 = null, t2 = null;
      if (smoothLoop) { const m = unit(sub(d[1], d[n - 1])); t1 = m; t2 = mulS(m, -1); }
      const a = anchorsOfFit(fitCubics(run, tol, t1, t2), tol);
      if (pts.length) { pts[pts.length - 1].o = a[0].o; a.shift(); }
      pts.push(...a);
    }
    if (pts.length > 1 && same(pts[pts.length - 1].p, pts[0].p, 1e-9)) { pts[0].i = pts[pts.length - 1].i; pts.pop(); }
    return pts.length >= 2 ? mkC(pts, true) : null;
  }
  /** One step of Smooth: the contour sampled, relaxed and fitted again (an open line keeps its ends). */
  function smoothContour(c, step) {
    if (c.pts.length < 2 || !nSegs(c)) return c;
    const d = sampleContour(c, step); if (d.length < 4) return c;
    const out = fitPoints(relax(d, c.closed, 8), step * 0.5, c.closed, 179);
    if (!out) return c;
    if (!c.closed) { out.pts[0].p = c.pts[0].p.slice(); out.pts[out.pts.length - 1].p = c.pts[c.pts.length - 1].p.slice(); }
    return out;
  }
  const anchorCount = cs => cs.reduce((a, c) => a + c.pts.length, 0);
  /** Simplify on the editor's ops (for an importer: a drawing read in as thousands of short lines, made into a few curves before it reaches the editor). tol in the ops' units. */
  const simplifySubpaths = (sps, tol) => toSubpaths(toContours(sps).map(c => simplifyContour(c, tol)));

  /* ═══ 5 · on an item: its contours in its own geometry; written back with its transform kept ═══ */
  const ccache = new WeakMap();
  function contoursOf(it) {
    const sp = Array.isArray(it && it.subpaths) ? it.subpaths : [];
    let c = ccache.get(sp); if (!c) { c = toContours(sp); ccache.set(sp, c); }
    return c;
  }
  /** Contours written as ops, and remembered as the contours of those very ops (as reading them back would give them: a handle on its own anchor is none, an open path's outer
   *  handles are none, each contour its written subpath's own), so the next frame of a drag does not read thousands of ops again. */
  function writeContours(cs) {
    const list = cs.filter(c => c && c.pts && c.pts.length), sps = toSubpaths(list);
    if (list.length !== sps.length) return sps;
    const norm = [];
    for (let k = 0; k < list.length; k++) {
      const c = list[k], n = c.pts.length;
      if (c.src) { norm.push(c); continue; }
      if (c.closed && n >= 2 && same(c.pts[n - 1].p, c.pts[0].p, 1e-6)) return sps;   // (read back, those two would be one anchor: let it be read)
      const pts = c.pts.map((a, i) => ({ p: a.p, i: a.i && !same(a.i, a.p) && (c.closed || i > 0) ? a.i : null, o: a.o && !same(a.o, a.p) && (c.closed || i < n - 1) ? a.o : null }));
      norm.push({ closed: c.closed, pts, src: sps[k] });
    }
    ccache.set(sps, norm);
    return sps;
  }
  /** The item with new contours (null: nothing left of it). Its transform is kept exactly: the editor's transform is about the centre of the geometry's own box, so with new
   *  geometry the move (tx, ty) is set so the matrix is the one it had and every anchor nobody touched stays where it is on the charm. */
  function rebuild(it, cs, geo) {
    const sps = writeContours(cs);
    if (!sps.length) return null;
    const n = Object.assign({}, it, { subpaths: sps });
    const W = geo.matrix(it);
    if (isI(W)) return n;
    const b = geo.box(n); if (!b) return n;
    const c = [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], L = apL(W, c);
    n.tx = r9(W[4] - c[0] + L[0]); n.ty = r9(W[5] - c[1] + L[1]);
    return n;
  }

  const model = {
    toContours, toSubpaths, segOf, nSegs, isSmooth, isEnd, reverseC, splitSeg, removeAnchor, deleteAnchors, cutAt, smoothAnchor, cornerAnchor, moveAnchors, setHandle,
    joinContours, chainContours, chainSubpaths, simplifySubpaths, mapC, fitCubics, simplifyContour, smoothContour, fitPoints, sampleContour, anchorCount, contoursOf, rebuild, bez, snap45
  };

  /* ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
     6 · THE TOOLS IN THE EDITOR (CharmEditor's extension points: tools and commands; ctx is the open workspace's, see charm-nest-charm-editor.js "EXTENSION POINTS")
     ════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
  const doc = root && root.document ? root.document : null;
  const SV = (d, extra) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${extra || ''}>${d}</svg>`;
  /* the icons, in the editor's hand (24-unit box, 1.6 stroke, round ends): Illustrator's own pictures, the nib, the hollow arrow, the pencil, the blades */
  const NIB = '<path d="M12.5 3.5 18 13l-3 7.5H9L6 13Z"/><path d="M12.5 3.5v7"/><circle cx="12.5" cy="12.2" r="1.5"/><path d="M9 20.5h6"/>';
  const IC = {
    pen: SV(NIB),
    direct: SV('<path d="m5 3 6.5 17 2.1-6.9L20.5 11Z" fill="#fff"/>'),
    add: SV('<path d="M10 3.5 15 12l-2.6 6.5H7.6L5 12Z"/><path d="M10 3.5v6"/><path d="M15.5 17.5h6"/><path d="M18.5 14.5v6"/>'),
    del: SV('<path d="M10 3.5 15 12l-2.6 6.5H7.6L5 12Z"/><path d="M10 3.5v6"/><path d="M15.5 17.5h6"/>'),
    convert: SV('<path d="M4 19 12 5l8 14"/><path d="M8.6 11h6.8" stroke-dasharray="2 2"/><circle cx="12" cy="5" r="1.6" fill="currentColor"/>'),
    scissors: SV('<circle cx="6" cy="7" r="2.6"/><circle cx="6" cy="17" r="2.6"/><path d="M8.2 8.4 20 17"/><path d="M8.2 15.6 20 7"/>'),
    pencil: SV('<path d="M4 20l1.2-4.6L16.4 4.2a1.9 1.9 0 0 1 2.7 0l.7.7a1.9 1.9 0 0 1 0 2.7L8.6 18.8Z"/><path d="m14.6 6 3.4 3.4"/><path d="M5.2 15.4l3.4 3.4"/>'),
    join: SV('<path d="M3.5 16c3-8 6-8 8.5-4"/><path d="M20.5 16c-3-8-6-8-8.5-4"/><circle cx="12" cy="12" r="1.7" fill="currentColor"/>'),
    close: SV('<path d="M13.6 4.2A8 8 0 1 0 19.8 10.4"/><path d="M15.6 4.4h4v4" stroke-dasharray="1.6 1.6"/><circle cx="19.8" cy="10.4" r="1.3" fill="currentColor"/><circle cx="13.6" cy="4.2" r="1.3" fill="currentColor"/>'),
    simplify: SV('<path d="M3.5 15c2-1 2.6-6 4.4-6s1.8 4 3.4 4 2-5 3.8-5 2 4 3.4 4 1.4-1 2-1.6" stroke-dasharray="1.6 1.8"/><path d="M3.5 18c5-6 12-6 17 0"/>'),
    smooth: SV('<path d="M3.5 15c3-6 6-6 8.5-1s5.5 5 8.5-1"/><path d="M4 9.5l2.5-2 2.5 2 2.5-2 2.5 2 2.5-2 2.5 2"/>'),
    corner: SV('<path d="M4 19 12 6l8 13"/><rect x="10.3" y="4.3" width="3.4" height="3.4" fill="currentColor"/>'),
    smoothPt: SV('<path d="M4 18c2-8 14-8 16 0"/><path d="M5 12h14"/><rect x="10.3" y="10.3" width="3.4" height="3.4" fill="currentColor"/><circle cx="5" cy="12" r="1.3" fill="currentColor"/><circle cx="19" cy="12" r="1.3" fill="currentColor"/>'),
    removePt: SV('<path d="M3.5 17c3-8 14-8 17 0"/><rect x="10.3" y="7.3" width="3.4" height="3.4"/><path d="M8 4l8 8"/>'),
    cutPt: SV('<path d="M3.5 16c3-7 5-8 7.4-8.6"/><path d="M13.1 7.4c2.4.6 4.4 1.6 7.4 8.6"/><rect x="10.3" y="5.6" width="3.4" height="3.4"/><path d="M12 11.5v8" stroke-dasharray="1.6 1.6"/>')
  };
  /* the cursors (as Illustrator's: the nib and what a click will do beside it; the hollow arrow; the pencil), each with its hot spot on its point */
  const cur = (svg, x, y, fb) => `url("data:image/svg+xml,${encodeURIComponent(`<svg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24'>${svg}</svg>`)}") ${x} ${y}, ${fb || 'crosshair'}`;
  const NIBC = "<g transform='rotate(-38 2 2)'><path d='M2 2 L7 11 L5.6 16 L-1.6 16 L-3 11 Z' fill='#fff' stroke='#000' stroke-width='1.1' stroke-linejoin='round'/><path d='M2 2 L2 8.6' stroke='#000'/><circle cx='2' cy='9.6' r='1' fill='#000'/><rect x='-1.6' y='16' width='7.2' height='2.6' fill='#000'/></g>";
  const MARK = {
    '': '', new: "<path d='M16 18h6M19 15v6' stroke='#fff' stroke-width='3'/><path d='M16.5 18.5h5M19 16v5' stroke='#000' stroke-width='1.2' transform='translate(0 -.5)'/>",
    add: "<path d='M15 19h7M18.5 15.5v7' stroke='#fff' stroke-width='3.2'/><path d='M15.5 19h6M18.5 16v6' stroke='#000' stroke-width='1.3'/>",
    del: "<path d='M15 19h7' stroke='#fff' stroke-width='3.2'/><path d='M15.5 19h6' stroke='#000' stroke-width='1.3'/>",
    close: "<circle cx='19' cy='19' r='2.6' fill='none' stroke='#fff' stroke-width='3'/><circle cx='19' cy='19' r='2.6' fill='none' stroke='#000' stroke-width='1.2'/>",
    cont: "<path d='M16 22l6-6' stroke='#fff' stroke-width='3.2'/><path d='M16.4 21.6l5.2-5.2' stroke='#000' stroke-width='1.3'/>",
    conv: "<path d='M15.5 21l3-5 3 5' fill='none' stroke='#fff' stroke-width='3'/><path d='M15.8 20.8l2.7-4.6 2.7 4.6' fill='none' stroke='#000' stroke-width='1.2'/>",
    join: "<path d='M15 17.5h3.4v3.4h-3.4z' fill='#000' stroke='#fff' stroke-width='.8'/><path d='M19.5 19h3' stroke='#000' stroke-width='1.3'/>"
  };
  const penCursor = m => cur(NIBC + (MARK[m] || ''), 2, 2);
  const CURSOR = {
    pen: penCursor('new'), direct: cur("<path d='M3 2 L3 18 L7.4 14 L10.4 20.6 L12.9 19.6 L10 13 L15.8 13 Z' fill='#fff' stroke='#000' stroke-width='1.1' stroke-linejoin='round'/>", 3, 2, 'default'),
    add: penCursor('add'), del: penCursor('del'), convert: cur("<path d='M2 2 L10 18 M2 2 L18 10' stroke='#fff' stroke-width='3'/><path d='M2 2 L10 18 M2 2 L18 10' stroke='#000' stroke-width='1.2'/>", 2, 2),
    pencil: cur("<path d='M2 22 L3.2 17.4 L16 4.6 L19.4 8 L6.6 20.8 Z' fill='#fff' stroke='#000' stroke-width='1.1' stroke-linejoin='round'/><path d='M2 22 L3.2 17.4 L6.6 20.8 Z' fill='#000'/>", 2, 22),
    scissors: 'crosshair'
  };
  const CSS = `
.ce-ui .cep-line{ fill:none; stroke:var(--gold,#a58a52); stroke-width:1.2; }
.ce-ui .cep-hov{ fill:none; stroke:var(--gold,#a58a52); stroke-width:1.5; opacity:.9; }
.ce-ui .cep-hl{ fill:none; stroke:var(--gold,#a58a52); stroke-width:1; }
.ce-ui .cep-hk{ fill:#fff; stroke:var(--gold,#a58a52); stroke-width:1.1; }
.ce-ui .cep-a{ fill:#fff; stroke:var(--ink,#1c1d1d); stroke-width:1; }
.ce-ui .cep-a--on{ fill:var(--ink,#1c1d1d); stroke:var(--ink,#1c1d1d); stroke-width:1; }
.ce-ui .cep-ring{ fill:none; stroke:var(--ink,#1c1d1d); stroke-width:1.2; }
.ce-ui .cep-band{ fill:none; stroke:var(--c-line,#d4291f); stroke-width:1; stroke-dasharray:4 3; }
.ce-ui .cep-ink{ fill:none; stroke:var(--c-line,#d4291f); stroke-width:1.5; stroke-linecap:round; stroke-linejoin:round; }
.ce-ui .cep-marq{ fill:rgba(28,29,29,.05); stroke:var(--ink,#1c1d1d); stroke-width:1; stroke-dasharray:4 3; }
.ce-stage[data-tool="direct"] .ce-ui .u-box,.ce-stage[data-tool="direct"] .ce-ui .u-box--under,.ce-stage[data-tool="pen"] .ce-ui .u-box,.ce-stage[data-tool="pen"] .ce-ui .u-box--under,
.ce-stage[data-tool="anchorAdd"] .ce-ui .u-box,.ce-stage[data-tool="anchorAdd"] .ce-ui .u-box--under,.ce-stage[data-tool="anchorDel"] .ce-ui .u-box,.ce-stage[data-tool="anchorDel"] .ce-ui .u-box--under,
.ce-stage[data-tool="anchorConvert"] .ce-ui .u-box,.ce-stage[data-tool="anchorConvert"] .ce-ui .u-box--under,.ce-stage[data-tool="scissors"] .ce-ui .u-box,.ce-stage[data-tool="scissors"] .ce-ui .u-box--under{ display:none; }
.ce-stage .cep-prev{ position:absolute; left:0; top:0; width:100%; height:100%; overflow:hidden; pointer-events:none; z-index:3; }
.cep-prev .cep-prev__p{ fill:none; stroke:var(--ink,#1c1d1d); stroke-width:1.2; }
.cep-prev .cep-prev__a{ fill:#fff; stroke:var(--ink,#1c1d1d); stroke-width:1; }
.cep-pop{ position:absolute; z-index:12; width:268px; padding:12px 14px 12px; background:#fff; border:1px solid var(--hair,#e6e4e0); box-shadow:0 14px 38px rgba(28,29,29,.18); font-size:12px; color:var(--ink,#1c1d1d); }
.cep-pop h4{ margin:0 0 9px; font:700 9.5px/1.2 var(--sans,sans-serif); letter-spacing:.2em; text-transform:uppercase; color:var(--muted,#6d6a66); }
.cep-pop__row{ display:flex; align-items:center; gap:10px; }
.cep-pop input[type=range]{ flex:1 1 auto; min-width:0; accent-color:var(--ink,#1c1d1d); margin:0; }
.cep-pop__v{ flex:none; width:64px; text-align:right; font-variant-numeric:tabular-nums; font-weight:600; }
.cep-pop__n{ margin:8px 0 12px; color:var(--muted,#6d6a66); font-variant-numeric:tabular-nums; }
.cep-pop__b{ display:flex; justify-content:flex-end; gap:8px; }
.cep-pop__b button{ font:700 10px/1 var(--sans,sans-serif); letter-spacing:.18em; text-transform:uppercase; padding:9px 14px; border:1px solid var(--ink,#1c1d1d); background:#fff; color:var(--ink,#1c1d1d); cursor:pointer; }
.cep-pop__b button.is-go{ background:var(--ink,#1c1d1d); color:#fff; }
`;
  function injectCss() {
    if (!doc || doc.getElementById('cepCss')) return;
    const s = doc.createElement('style'); s.id = 'cepCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  const say = (ctx, t) => { if (ctx && typeof ctx.say === 'function') ctx.say(t); };
  const logEv = (op, data) => { try { if (root && root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event('pathEdit', Object.assign({ op }, data || {})); } catch (e) { /* the log never stops an edit */ } };

  /* ── the workspace's state (one editor open at a time: reset when a new ctx turns up) ── */
  let St = null;
  function S(ctx) {
    if (!St || St.ctx !== ctx) St = { ctx, anc: new Map(), g: null, pen: null, hov: null, hovId: null, run: null, pop: null, ink: null, ixFor: null, ix: null };
    return St;
  }
  const CEM = () => (root && root.CharmEditor && root.CharmEditor.model) || null;
  const viewOf = ctx => { const v = typeof ctx.view === 'function' ? ctx.view() : null; return Array.isArray(v) && v.length === 6 && v.every(fin) ? v : [1, 0, 0, 1, 0, 0]; };
  const pxPerPt = ctx => { const v = viewOf(ctx); return Math.hypot(v[0], v[1]) || 1; };
  /** id -> { it, P, anc } of the face's tree (the editor's indexTree), cached by the items. */
  function index(ctx) {
    const st = S(ctx), items = ctx.items();
    if (st.ixFor !== items) { st.ix = CEM().indexTree(items, ctx.geo); st.ixFor = items; }
    return st.ix;
  }
  /** A path part's drawing: its contours (own geometry) and W, own geometry -> face. Null for a part these tools do not touch (text, a group, locked, hidden, or in a locked or
   *  hidden group). */
  function infoOf(ctx, id) {
    const ix = index(ctx), e = ix.get(id); if (!e) return null;
    const it = e.it; if (!it || it.kind !== 'path' || it.locked || it.hidden) return null;
    if (e.anc.some(a => { const g = ix.get(a); return g && (g.it.locked || g.it.hidden); })) return null;
    const W = mul(e.P, ctx.geo.matrix(it)), Wi = inv(W); if (!Wi) return null;
    return { id, it, W, Wi, cs: contoursOf(it), P: e.P };
  }
  /** The path parts under the selection (a selected group's parts too) that can be edited. */
  function targets(ctx) {
    const ix = index(ctx), out = [], seen = new Set();
    const rec = it => { if (!it || seen.has(it.id)) return; seen.add(it.id); if (it.kind === 'group') (it.children || []).forEach(rec); else { const inf = infoOf(ctx, it.id); if (inf) out.push(inf); } };
    for (const id of ctx.selection()) { const e = ix.get(id); if (e) rec(e.it); }
    return out;
  }
  /** Every path part on the face that can be edited (for the sweep of anchors). */
  function everyPath(ctx) {
    const out = [];
    const rec = (list, off) => { for (const it of list || []) { const o = off || it.locked || it.hidden; if (it.kind === 'group') rec(it.children, o); else if (!o && it.kind === 'path') { const inf = infoOf(ctx, it.id); if (inf) out.push(inf); } } };
    rec(ctx.items(), false);
    return out;
  }
  /** The path part under a face point (Alt-click's reach: the part itself, inside its groups). */
  function pathAt(ctx, p, tolPt) {
    const hit = CEM().hitTest(ctx.items(), p.x, p.y, tolPt, ctx.geo, { deep: true, filter: it => it.kind === 'path' });
    return hit ? infoOf(ctx, hit.id) : null;
  }
  /** The anchors picked on a part, as a set of "contour:anchor" keys still valid on its drawing now (an undo may have taken some away). */
  function picked(st, inf) {
    const s = st.anc.get(inf.id); if (!s || !s.size) return new Set();
    const ok = new Set(); for (const k of s) { const [ci, ai] = k.split(':').map(Number); if (inf.cs[ci] && ai < inf.cs[ci].pts.length) ok.add(k); }
    return ok;
  }
  const key = (ci, ai) => ci + ':' + ai;

  /* ── finding things under the pointer (screen tolerances, as Illustrator's: the anchors 5 px, the segments 4 px) ── */
  function anchorHit(inf, p, tol, filter) {
    let best = null;
    inf.cs.forEach((c, ci) => c.pts.forEach((a, ai) => {
      if (filter && !filter(c, ai)) return;
      const q = ap(inf.W, a.p), d = Math.hypot(q[0] - p.x, q[1] - p.y);
      if (d <= tol && (!best || d < best.d)) best = { inf, ci, ai, d };
    }));
    return best;
  }
  /** The handles shown on a part (those of the picked anchors and, facing them, of their neighbours; every handle when `all`). */
  function shownHandles(st, inf, all) {
    const out = [], s = picked(st, inf);
    inf.cs.forEach((c, ci) => {
      const n = c.pts.length, on = new Set();
      c.pts.forEach((a, ai) => { if (all || s.has(key(ci, ai))) { on.add(ai + ':i'); on.add(ai + ':o'); if (c.closed || ai > 0) on.add(((ai - 1 + n) % n) + ':o'); if (c.closed || ai < n - 1) on.add(((ai + 1) % n) + ':i'); } });
      for (const k of on) { const [ai, w] = k.split(':'), a = c.pts[+ai]; if (a && a[w]) out.push({ ci, ai: +ai, which: w, q: a[w], p: a.p }); }
    });
    return out;
  }
  function handleHit(st, infs, p, tol, all) {
    let best = null;
    for (const inf of infs) for (const h of shownHandles(st, inf, all)) {
      const q = ap(inf.W, h.q), d = Math.hypot(q[0] - p.x, q[1] - p.y);
      if (d <= tol && (!best || d < best.d)) best = Object.assign({ inf, d }, h);
    }
    return best;
  }
  /** The nearest point on a part's segments (face space) within tol: { inf, ci, k (segment), t, d }. */
  function segHit(inf, p, tol) {
    let best = null;
    inf.cs.forEach((c, ci) => {
      for (let k = 0; k < nSegs(c); k++) {
        const q = segOf(c, k).q.map(v => ap(inf.W, v)), bb = bboxPts(q);
        if (p.x < bb[0] - tol || p.x > bb[2] + tol || p.y < bb[1] - tol || p.y > bb[3] + tol) continue;
        const N = 24; let bt = 0, bd = Infinity;
        for (let i = 0; i <= N; i++) { const t = i / N, b = bez(q[0], q[1], q[2], q[3], t), d = Math.hypot(b[0] - p.x, b[1] - p.y); if (d < bd) { bd = d; bt = t; } }
        let lo = Math.max(0, bt - 1 / N), hi = Math.min(1, bt + 1 / N);
        for (let i = 0; i < 24; i++) {
          const m1 = lo + (hi - lo) / 3, m2 = hi - (hi - lo) / 3, b1 = bez(q[0], q[1], q[2], q[3], m1), b2 = bez(q[0], q[1], q[2], q[3], m2);
          if (Math.hypot(b1[0] - p.x, b1[1] - p.y) < Math.hypot(b2[0] - p.x, b2[1] - p.y)) hi = m2; else lo = m1;
        }
        const t = (lo + hi) / 2, b = bez(q[0], q[1], q[2], q[3], t), d = Math.hypot(b[0] - p.x, b[1] - p.y);
        if (d <= tol && (!best || d < best.d)) best = { inf, ci, k, t, d };
      }
    });
    return best;
  }
  /** Something of the targets, or of the part under the pointer, at p: an anchor, else a segment. */
  function pathThingAt(ctx, p, opts) {
    opts = opts || {};
    const k = pxPerPt(ctx), tA = 5 / k, tS = 4 / k, ts = targets(ctx), near = (a, b) => (!a || (b && b.d < a.d) ? b : a);
    let a0 = null, s0 = null;
    for (const inf of ts) a0 = near(a0, anchorHit(inf, p, tA, opts.anchorFilter));
    if (a0) return Object.assign({ kind: 'anchor' }, a0);
    for (const inf of ts) s0 = near(s0, segHit(inf, p, tS));
    if (s0) return Object.assign({ kind: 'seg' }, s0);
    if (opts.targetsOnly) return null;
    const inf = pathAt(ctx, p, tS); if (!inf) return null;
    const a = anchorHit(inf, p, tA, opts.anchorFilter); if (a) return Object.assign({ kind: 'anchor' }, a);
    const s = segHit(inf, p, tS); if (s) return Object.assign({ kind: 'seg' }, s);
    return { kind: 'body', inf };
  }

  /* ── writing: the groups above a part with a transform of their own pushed into it first (as the editor does before an edit inside a group), then the part's new
     drawing; everything through ctx.replaceItems, so it is drawn at once and gathered into one undo step ── */
  function flatten(ctx, ids) {
    const M = CEM(), ix = index(ctx), map = new Map();
    for (const id of ids) {
      const e = ix.get(id); if (!e) continue;
      const top = e.anc.find(a => { const g = ix.get(a); return g && !isI(ctx.geo.matrix(g.it)); });
      if (top && !map.has(top)) { const t = ix.get(top); map.set(top, M.pushDown(t.it, null, ctx.geo, t.P, true)); }
    }
    if (map.size) ctx.replaceItems(map);
  }
  /** Contours for parts written: map id -> contours (null or [] removes the part). */
  function write(ctx, byId) {
    const ix = index(ctx), map = new Map();
    for (const [id, cs] of byId) { const e = ix.get(id); if (!e) continue; map.set(id, cs && cs.length ? rebuild(e.it, cs, ctx.geo) : null); }
    if (map.size) ctx.replaceItems(map);
  }
  /** A new path part in face coordinates, as a line engraving (a drawn shape's default: red, the laser's own width). */
  function newPathItem(cs, name) {
    const M = CEM();
    return { id: M.newId(), kind: 'path', role: 'line', source: 'added', name: name || 'Path', hidden: false, locked: false, subpaths: toSubpaths(cs), stroke: true, fill: false, lwPt: LW_LASER_PT, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
  }
  function selectAnchors(ctx, list, additive) {
    const st = S(ctx);
    if (!additive) st.anc = new Map();
    for (const { id, ci, ai } of list) { if (!st.anc.has(id)) st.anc.set(id, new Set()); st.anc.get(id).add(key(ci, ai)); }
    const ids = [...new Set((additive ? targets(ctx).map(t => t.id) : []).concat(list.map(a => a.id)))];
    const cur = ctx.selection();
    if (ids.length !== cur.length || ids.some(id => !cur.includes(id))) ctx.select(ids);
    if (typeof ctx.redraw === 'function') ctx.redraw();
  }

  /* ── the overlay: the parts being edited drawn over the charm with their anchors and handles, in work-area px (a handful of paths however many anchors) ── */
  const f1 = v => Math.round(v * 10) / 10;
  /* (the editor draws its overlays again on every change, and a drag changes the work and the read-out one after the other: what is drawn is kept with what it was drawn from,
     so the second pass of a frame costs nothing) */
  const treeNo = new WeakMap(); let treeSeq = 0;
  function overlay(ctx, tool) {
    const st = S(ctx), V = viewOf(ctx), items = ctx.items();
    if (!treeNo.has(items)) treeNo.set(items, ++treeSeq);
    const g = st.g, memo = [tool, treeNo.get(items), V.join(','), ctx.selection().join(','), [...st.anc].map(([id, k]) => id + ':' + [...k].join(',')).join(';'), st.hovId,
      st.pen ? [st.pen.id, st.pen.n, st.pen.out, st.pen.in0, st.pen.end].join(',') : '', st.hov && st.pen ? st.hov.x + ',' + st.hov.y : '', st.ring || '', g ? g.kind + (g.live ? 1 : 0) : '',
      g && g.kind === 'marq' ? [g.x0, g.y0, g.x, g.y].join(',') : '', st.ink ? st.ink.length : 0].join('|');
    if (st.ovMemo === memo) return st.ovOut;
    st.ovMemo = memo; st.ovOut = overlayNow(ctx, tool, st, V);
    return st.ovOut;
  }
  function overlayNow(ctx, tool, st, V) {
    const stg = stageSize(ctx), vw = stg.w, vh = stg.h;
    const inView = q => q[0] > -20 && q[1] > -20 && q[0] < vw + 20 && q[1] < vh + 20;
    let h = '', aOff = '', aOn = '', hl = '', hk = '';
    const allHandles = tool === 'anchorConvert';
    const draw = (inf, cls) => {
      const VW = mul(V, inf.W);
      h += `<path class="${cls}" d="${CEM().dOf(inf.it.subpaths)}" transform="matrix(${VW.map(v => +v.toFixed(6)).join(' ')})" vector-effect="non-scaling-stroke"/>`;
      if (tool === 'pencil') return;
      const s = picked(st, inf);
      inf.cs.forEach((c, ci) => c.pts.forEach((a, ai) => {
        const q = ap(VW, a.p); if (!inView(q)) return;
        const r = 3.5, sq = `M${f1(q[0] - r)} ${f1(q[1] - r)}h${2 * r}v${2 * r}h${-2 * r}Z`;
        if (s.has(key(ci, ai))) aOn += sq; else aOff += sq;
      }));
      for (const hd of shownHandles(st, inf, allHandles)) {
        const a = ap(VW, hd.p), q = ap(VW, hd.q); if (!inView(a) && !inView(q)) continue;
        hl += `M${f1(a[0])} ${f1(a[1])}L${f1(q[0])} ${f1(q[1])}`;
        hk += `M${f1(q[0] - 3)} ${f1(q[1])}a3 3 0 1 0 6 0a3 3 0 1 0 -6 0`;
      }
    };
    const ts = targets(ctx), ids = new Set(ts.map(t => t.id));
    for (const inf of ts) draw(inf, 'cep-line');
    if (st.hovId && !ids.has(st.hovId) && !(st.g && st.g.live)) { const inf = infoOf(ctx, st.hovId); if (inf) draw(inf, 'cep-hov'); }
    if (hl) h += `<path class="cep-hl" d="${hl}"/>`;
    if (hk) h += `<path class="cep-hk" d="${hk}"/>`;
    if (aOff) h += `<path class="cep-a" d="${aOff}"/>`;
    if (aOn) h += `<path class="cep-a--on" d="${aOn}"/>`;
    // the pen: the handle out of the last anchor (which no segment holds yet), the rubber band to the pointer (the next segment as it will be), the ring on the start
    // when a click would close
    if (tool === 'pen' && st.pen) {
      const a = penActive(ctx), last = a && a.c.pts[a.c.pts.length - 1];
      if (last && last.o) { const VW = mul(V, a.inf.W), P0 = ap(VW, last.p), O = ap(VW, last.o); h += `<path class="cep-hl" d="M${f1(P0[0])} ${f1(P0[1])}L${f1(O[0])} ${f1(O[1])}"/><circle class="cep-hk" cx="${f1(O[0])}" cy="${f1(O[1])}" r="3"/>`; }
    }
    if (tool === 'pen' && st.pen && st.hov && !(st.g && st.g.live)) {
      const a = penActive(ctx);
      if (a) {
        const last = a.c.pts[a.c.pts.length - 1], P0 = ap(V, ap(a.inf.W, last.p)), O = last.o ? ap(V, ap(a.inf.W, last.o)) : null, Q = ap(V, [st.hov.x, st.hov.y]);
        h += `<path class="cep-band" d="M${f1(P0[0])} ${f1(P0[1])}${O ? `C${f1(O[0])} ${f1(O[1])} ${f1(Q[0])} ${f1(Q[1])} ${f1(Q[0])} ${f1(Q[1])}` : `L${f1(Q[0])} ${f1(Q[1])}`}"/>`;
      }
    }
    if (st.ring) { const q = ap(V, st.ring); h += `<circle class="cep-ring" cx="${f1(q[0])}" cy="${f1(q[1])}" r="7"/>`; }
    if (st.g && st.g.kind === 'marq' && st.g.live) {
      const a = ap(V, [st.g.x0, st.g.y0]), b = ap(V, [st.g.x, st.g.y]);
      h += `<rect class="cep-marq" x="${f1(Math.min(a[0], b[0]))}" y="${f1(Math.min(a[1], b[1]))}" width="${f1(Math.abs(b[0] - a[0]))}" height="${f1(Math.abs(b[1] - a[1]))}"/>`;
    }
    if (st.ink && st.ink.length > 1) { let d = ''; st.ink.forEach((p, i) => { const q = ap(V, p); d += (i ? 'L' : 'M') + f1(q[0]) + ' ' + f1(q[1]); }); h += `<path class="cep-ink" d="${d}"/>`; }
    return h;
  }
  /** The work area's size in px, read again at most every 0.7 s (a read of the layout on every frame would make the browser lay the page out again after each change). */
  function stageSize(ctx, fresh) {
    const st = S(ctx);
    const now = Date.now();
    if (fresh || !st.vs || now - st.vs.at > 700) { const r = ctx.stage && ctx.stage.getBoundingClientRect ? ctx.stage.getBoundingClientRect() : null; st.vs = r && r.width ? { w: r.width, h: r.height, at: now } : { w: 1e6, h: 1e6, at: now }; }
    return st.vs;
  }
  /** The cursor for what a click will do here (the pen's marks beside its nib), set on the work area while the pointer is over it. */
  function setCursor(ctx, c) { const s = ctx.stage; if (!s || !s.style || !c || s.__cepCur === c) return; s.__cepCur = c; s.style.setProperty('--xcur', c); }
  /* LIVE FEEDBACK, as the editor's own tools give it (Paul, 28 Oct 2026: "I'm not seeing a real time read out ... in the work area ... and in the X and Y in the menu"): a point
     dragged or placed snaps to the smart guides (ctx.snap: Alt pauses them; Shift's 45° wins over them), and the gold chip says where the anchor is, or how long a handle is
     and at what angle, while the bar's X / Y / W / H follow the part as it changes */
  const fmtMm = v => (Math.abs(v) < 0.0005 ? '0.00' : v.toFixed(2));
  const snapTo = (ctx, q, ev) => (typeof ctx.snap === 'function' && !(ev && ev.shiftKey) ? ctx.snap(q[0], q[1], ev) : q);
  const posText = (ctx, q) => { const f = ctx.frame(); return `X ${fmtMm((q[0] - f.x0) * MM)}  Y ${fmtMm((q[1] - f.y0) * MM)} mm`; };
  const handleText = (a, q) => { let deg = -Math.atan2(q[1] - a[1], q[0] - a[0]) * 180 / Math.PI; if (deg <= -180) deg += 360; return `Handle ${fmtMm(dist(a, q) * MM)} mm · ${(Math.round(deg * 10) / 10) || 0}°`; };
  function liveOf(ctx, id, text) {
    if (typeof ctx.live !== 'function') return;
    const e = ctx.entry(id), b = e && ctx.geo.drawn(e.it, e.P);
    if (b) ctx.live({ box: b, text, angle: num(e.it.rot, 0) });
  }

  /* ═══ 7 · THE PEN ═══ */
  /** The open contour the pen is drawing: { inf, ci, c } with its active end LAST (reversed when the pen took it up at its first anchor), or null. */
  /* (the handle OUT of the last anchor of an open path, and the one INTO its first, belong to no segment yet, so the editor's ops cannot hold them: the pen keeps them, raw,
     with the count of anchors they were made at, until the next anchor or the closing click gives them a segment; an undo that takes an anchor away leaves them behind) */
  function penActive(ctx) {
    const st = S(ctx), pen = st.pen; if (!pen) return null;
    const inf = infoOf(ctx, pen.id), c0 = inf && inf.cs[pen.ci];
    if (!c0 || c0.closed) { st.pen = null; return null; }
    let c = pen.end === 'first' ? reverseC(c0) : c0;
    if (pen.n === c.pts.length && (pen.out || pen.in0)) {
      c = mkC(c.pts.map(cloneA), false);
      if (pen.out) c.pts[c.pts.length - 1].o = pen.out.slice();
      if (pen.in0 && !c.pts[0].i) c.pts[0].i = pen.in0.slice();
    }
    return { inf, ci: pen.ci, c };
  }
  /** The pen's contour written (its active end last; it stays reversed from here on), the two handles no segment holds kept. */
  function penWrite(ctx, a, c) {
    const st = S(ctx), cs = a.inf.cs.slice(); cs[a.ci] = c;
    write(ctx, new Map([[a.inf.id, cs]]));
    if (st.pen) { const n = c.pts.length; Object.assign(st.pen, { end: 'last', n, out: !c.closed && c.pts[n - 1].o ? c.pts[n - 1].o.slice() : null, in0: !c.closed && c.pts[0].i ? c.pts[0].i.slice() : null }); }
    st.anc = new Map([[a.inf.id, new Set([key(a.ci, c.closed ? 0 : c.pts.length - 1)])]]);   // (the anchor just placed is the one picked, as Illustrator shows it filled)
  }
  function penEnd(ctx, opts) {
    const st = S(ctx), a = penActive(ctx);
    st.pen = null; st.ring = null;
    if (a && a.c.pts.length < 2) {   // (a single anchor is no line: it goes, as the stray point Illustrator leaves would be no use to the laser)
      const cs = a.inf.cs.filter((c, i) => i !== a.ci);
      write(ctx, new Map([[a.inf.id, cs]]));
      if (typeof ctx.commit === 'function') ctx.commit('Pen');
    }
    if (opts && opts.drop) ctx.select([]);
    if (typeof ctx.redraw === 'function') ctx.redraw();
  }
  /** The end of an open contour of a target, or of the part under the pointer, at p: { inf, ci, end } */
  function endAt(ctx, p, skip) {
    const k = pxPerPt(ctx), tol = 5 / k;
    const look = inf => { const h = anchorHit(inf, p, tol, (c, ai) => isEnd(c, ai) && !(skip && skip.id === inf.id && skip.ci === inf.cs.indexOf(c))); return h ? { inf, ci: h.ci, end: h.ai === 0 ? 'first' : 'last' } : null; };
    for (const inf of targets(ctx)) { const r = look(inf); if (r) return r; }
    const inf = pathAt(ctx, p, tol); return inf ? look(inf) : null;
  }
  const pen = {
    id: 'pen', icon: IC.pen, title: 'Pen', key: 'P', cursor: CURSOR.pen, flyout: 'Pen',
    tip: 'Anchors and curves',
    hint: 'Click for a corner · drag for a curve (Alt breaks the handles, Shift 45°) · click the first anchor to close · Esc or Enter ends the line',
    overlay: ctx => overlay(ctx, 'pen'),
    onArm(ctx) { injectCss(); stageSize(ctx, true); },
    onDisarm(ctx) { penEnd(ctx); if (ctx.stage) ctx.stage.__cepCur = null; },
    onDown(ctx, ev, p) {
      if (ev.button) return;
      const st = S(ctx), k = pxPerPt(ctx), tol = 5 / k;
      if (ev.metaKey || ev.ctrlKey) { penEnd(ctx, { drop: true }); return; }
      const a = penActive(ctx);
      if (a) {
        const n = a.c.pts.length, last = a.c.pts[n - 1], first = a.c.pts[0];
        const lastQ = ap(a.inf.W, last.p), firstQ = ap(a.inf.W, first.p);
        if (n >= 2 && Math.hypot(firstQ[0] - p.x, firstQ[1] - p.y) <= tol) {   // the ring: close
          const cs = a.c.pts.map(cloneA), c = mkC(cs, true);
          penWrite(ctx, a, c); st.g = { kind: 'penClose', id: a.inf.id, ci: a.ci, at: firstQ, live: false };
          st.pen = null; st.ring = null; return;
        }
        if (Math.hypot(lastQ[0] - p.x, lastQ[1] - p.y) <= tol) {             // the last anchor: its outgoing handle in (a drag pulls a new one, alone)
          const c = mkC(a.c.pts.map(cloneA), false); c.pts[n - 1].o = null;
          penWrite(ctx, a, c); st.g = { kind: 'penCusp', live: false }; return;
        }
        const other = endAt(ctx, p, { id: a.inf.id, ci: a.ci });
        if (other) { penConnect(ctx, a, other); return; }
        const q = ev.shiftKey ? snap45(lastQ, [p.x, p.y]) : snapTo(ctx, [p.x, p.y], ev);
        const c = mkC(a.c.pts.map(cloneA).concat([{ p: ap(a.inf.Wi, q), i: null, o: null }]), false);
        penWrite(ctx, a, c); st.g = { kind: 'penPt', at: q, live: false }; return;
      }
      // not drawing: an end of an open path takes the pen up again; over a selected path a segment adds an anchor and an anchor goes (Illustrator's Auto Add/Delete)
      const end = endAt(ctx, p);
      if (end) { flatten(ctx, [end.inf.id]); st.pen = { id: end.inf.id, ci: end.ci, end: end.end }; selectAnchors(ctx, [{ id: end.inf.id, ci: end.ci, ai: end.end === 'first' ? 0 : end.inf.cs[end.ci].pts.length - 1 }]); st.g = { kind: 'penCusp', live: false, resumed: true }; return; }
      const t = pathThingAt(ctx, p, { targetsOnly: true });
      if (t && t.kind === 'seg') { addAnchorAt(ctx, t); st.g = { kind: 'none' }; return; }
      if (t && t.kind === 'anchor') { delAnchorAt(ctx, t); st.g = { kind: 'none' }; return; }
      // a new path, its first anchor here
      const q0 = snapTo(ctx, [p.x, p.y], ev), c0 = mkC([{ p: q0, i: null, o: null }], false), it = newPathItem([c0], 'Pen path');
      ctx.addItems([it], { select: true });
      st.pen = { id: it.id, ci: 0, end: 'last' }; st.anc = new Map([[it.id, new Set(['0:0'])]]);
      st.g = { kind: 'penPt', at: q0, live: false };
      if (typeof ctx.redraw === 'function') ctx.redraw();
    },
    onMove(ctx, ev, p) {
      const st = S(ctx), g = st.g;
      if (!g || !(ev.buttons & 1)) { penHover(ctx, p); return; }
      const k = pxPerPt(ctx);
      if (!g.live) { if (!g.at && g.kind !== 'penCusp') return; g.live = true; }
      if (g.kind === 'penPt') {   // the anchor just placed: its handles pulled out (Alt: the outgoing one alone; Shift: 45°)
        const a = penActive(ctx); if (!a) return;
        let q = [p.x, p.y]; if (ev.shiftKey) q = snap45(g.at, q);
        if (Math.hypot(q[0] - g.at[0], q[1] - g.at[1]) * k < 2 && !g.pulled) return;
        g.pulled = true;
        const n = a.c.pts.length, c = setHandle(a.c, n - 1, 'o', ap(a.inf.Wi, q), ev.altKey || g.alt ? 'free' : 'mirror');
        if (ev.altKey) g.alt = true;
        penWrite(ctx, a, c); liveOf(ctx, a.inf.id, handleText(g.at, q));
      } else if (g.kind === 'penCusp') {
        const a = penActive(ctx); if (!a) return;
        const n = a.c.pts.length, last = a.c.pts[n - 1], at = ap(a.inf.W, last.p);
        let q = [p.x, p.y]; if (ev.shiftKey) q = snap45(at, q);
        penWrite(ctx, a, setHandle(a.c, n - 1, 'o', ap(a.inf.Wi, q), 'free')); liveOf(ctx, a.inf.id, handleText(at, q));
      } else if (g.kind === 'penClose') {   // closing with a drag: the start becomes smooth through the pointer (Alt: only the handle into it)
        const inf = infoOf(ctx, g.id), c = inf && inf.cs[g.ci]; if (!c) return;
        let q = [p.x, p.y]; if (ev.shiftKey) q = snap45(g.at, q);
        const mirror = sub(mulS(g.at, 2), q);
        let c2 = setHandle(c, 0, 'i', ap(inf.Wi, mirror), 'free');
        if (!ev.altKey) c2 = setHandle(c2, 0, 'o', ap(inf.Wi, q), 'free');
        const cs = inf.cs.slice(); cs[g.ci] = c2; write(ctx, new Map([[inf.id, cs]])); liveOf(ctx, inf.id, handleText(g.at, q));
      }
    },
    onUp(ctx) { const st = S(ctx); st.g = null; if (typeof ctx.redraw === 'function') ctx.redraw(); },
    onKey(ctx, ev) {
      const st = S(ctx);
      if ((ev.key === 'Escape' || ev.key === 'Enter') && st.pen) { ev.preventDefault(); penEnd(ctx); return true; }
      return directKeys(ctx, ev);
    }
  };
  function penHover(ctx, p) {
    const st = S(ctx), k = pxPerPt(ctx), tol = 5 / k;
    st.hov = p; st.ring = null;
    let mark = 'new';
    const a = penActive(ctx);
    if (a) {
      const n = a.c.pts.length, firstQ = ap(a.inf.W, a.c.pts[0].p), lastQ = ap(a.inf.W, a.c.pts[n - 1].p);
      if (n >= 2 && Math.hypot(firstQ[0] - p.x, firstQ[1] - p.y) <= tol) { mark = 'close'; st.ring = firstQ; }
      else if (Math.hypot(lastQ[0] - p.x, lastQ[1] - p.y) <= tol) mark = 'conv';
      else if (endAt(ctx, p, { id: a.inf.id, ci: a.ci })) mark = 'join';
      else mark = '';
    } else if (endAt(ctx, p)) mark = 'cont';
    else { const t = pathThingAt(ctx, p, { targetsOnly: true }); if (t && t.kind === 'seg') mark = 'add'; else if (t && t.kind === 'anchor') mark = 'del'; }
    setCursor(ctx, penCursor(mark));
    if (typeof ctx.redraw === 'function') ctx.redraw();
  }
  /** While drawing, a click on another open path's end: the two made one (into the part being drawn, the other's drawing brought into its frame), and the pen is done. */
  function penConnect(ctx, a, other) {
    const st = S(ctx);
    flatten(ctx, [other.inf.id]); other = Object.assign({}, other, { inf: infoOf(ctx, other.inf.id) }); a = penActive(ctx);
    if (!a || !other.inf) return;
    if (other.inf.id === a.inf.id) {
      const c2 = other.end === 'last' ? reverseC(a.inf.cs[other.ci]) : a.inf.cs[other.ci];
      const j = joinContours(a.c, 'last', c2, 'first', 1e-6), cs = a.inf.cs.slice(); cs[a.ci] = j; cs.splice(other.ci, 1);
      write(ctx, new Map([[a.inf.id, cs]]));
    } else {
      const m = mul(a.inf.Wi, other.inf.W), oc = mapC(other.inf.cs[other.ci], m);
      const j = joinContours(a.c, 'last', oc, other.end, 1e-6), cs = a.inf.cs.slice(); cs[a.ci] = j;
      write(ctx, new Map([[a.inf.id, cs], [other.inf.id, other.inf.cs.filter((c, i) => i !== other.ci)]]));
    }
    st.pen = null; st.g = { kind: 'none' };
    ctx.select([a.inf.id]);
  }

  /* ═══ 8 · DIRECT SELECTION and the anchor tools ═══ */
  function addAnchorAt(ctx, t) {
    flatten(ctx, [t.inf.id]); const inf = infoOf(ctx, t.inf.id); if (!inf) return;
    const r = splitSeg(inf.cs[t.ci], t.k, t.t), cs = inf.cs.slice(); cs[t.ci] = r.c;
    write(ctx, new Map([[inf.id, cs]]));
    selectAnchors(ctx, [{ id: inf.id, ci: t.ci, ai: r.k }]);
  }
  function delAnchorAt(ctx, t) {
    flatten(ctx, [t.inf.id]); const inf = infoOf(ctx, t.inf.id); if (!inf) return;
    const c = removeAnchor(inf.cs[t.ci], t.ai), cs = inf.cs.slice();
    if (c) cs[t.ci] = c; else cs.splice(t.ci, 1);
    write(ctx, new Map([[inf.id, cs]]));
    S(ctx).anc.delete(inf.id);
    if (cs.length) ctx.select([inf.id]);
  }
  /** The drag of anchors: every picked anchor of every target moved by the pointer's travel (in each part's own frame). */
  function beginAnchorDrag(ctx, p, extra) {
    const st = S(ctx), ts = targets(ctx), parts = [];
    flatten(ctx, ts.filter(t => picked(st, t).size).map(t => t.id));
    for (const t of targets(ctx)) { const s = picked(st, t); if (s.size) parts.push({ id: t.id, Wi: t.Wi, cs: t.cs, s }); }
    return Object.assign({ kind: 'anchors', x0: p.x, y0: p.y, parts, live: false }, extra || {});
  }
  /** The picked anchors moved with the pointer; the anchor that was pressed (g.lead, face pt) snaps to the smart guides and its place is read out. */
  function dragAnchors(ctx, g, ev, p) {
    let d = [p.x - g.x0, p.y - g.y0];
    if (ev.shiftKey) { const q = snap45([g.x0, g.y0], [p.x, p.y]); d = [q[0] - g.x0, q[1] - g.y0]; }
    else if (g.lead) { const q = snapTo(ctx, [g.lead[0] + d[0], g.lead[1] + d[1]], ev); d = [q[0] - g.lead[0], q[1] - g.lead[1]]; }
    const map = new Map();
    for (const part of g.parts) {
      const dr = apL(part.Wi, d), bySub = new Map();
      for (const kk of part.s) { const [ci, ai] = kk.split(':').map(Number); if (!bySub.has(ci)) bySub.set(ci, new Set()); bySub.get(ci).add(ai); }
      map.set(part.id, part.cs.map((c, ci) => (bySub.has(ci) ? moveAnchors(c, bySub.get(ci), dr) : c)));
    }
    write(ctx, map);
    if (g.leadId) liveOf(ctx, g.leadId, g.lead ? posText(ctx, [g.lead[0] + d[0], g.lead[1] + d[1]]) : undefined);
  }
  /** A handle dragged: the anchor's partner turns with it when the anchor was smooth (Alt: alone; Shift: 45° about its anchor). */
  function dragHandle(ctx, g, ev, p) {
    const inf = infoOf(ctx, g.id); if (!inf) return;
    let q = [p.x, p.y]; if (ev.shiftKey) q = snap45(g.at, q);
    const c = setHandle(g.c0, g.ai, g.which, ap(inf.Wi, q), g.free || ev.altKey ? 'free' : 'smooth', g.smooth);
    const cs = g.cs0.slice(); cs[g.ci] = c; write(ctx, new Map([[g.id, cs]]));
    liveOf(ctx, g.id, handleText(g.at, q));
  }
  /** A segment dragged: a straight one moves its two anchors; a curve bends so the point that was pressed follows the pointer (its handles moved, more the nearer one). */
  function dragSeg(ctx, g, ev, p) {
    const d = [p.x - g.x0, p.y - g.y0], dr = apL(g.Wi, d), c = g.c0, s = segOf(c, g.k);
    let c2;
    if (!s.curve) c2 = moveAnchors(c, new Set([s.ka, s.kb]), dr);
    else {
      const t = clamp(g.t, 0.08, 0.92), u = 1 - t, sc = 1 / (3 * u * t * (u * u + t * t));
      const pts = c.pts.map(cloneA), A = pts[s.ka], B = pts[s.kb], wasA = isSmooth(c.pts[s.ka]), wasB = isSmooth(c.pts[s.kb]);
      A.o = add(A.o || A.p, mulS(dr, u * sc)); B.i = add(B.i || B.p, mulS(dr, t * sc));
      c2 = mkC(pts, c.closed);
      if (wasA && A.i) c2 = setHandle(c2, s.ka, 'o', A.o, 'smooth', true);
      if (wasB && B.o) c2 = setHandle(c2, s.kb, 'i', c2.pts[s.kb].i, 'smooth', true);
    }
    const cs = g.cs0.slice(); cs[g.ci] = c2; write(ctx, new Map([[g.id, cs]]));
    liveOf(ctx, g.id);
  }
  /** The anchors inside a box (face space) on every part that can be edited. */
  function anchorsIn(ctx, r) {
    const x0 = Math.min(r[0], r[2]), x1 = Math.max(r[0], r[2]), y0 = Math.min(r[1], r[3]), y1 = Math.max(r[1], r[3]), out = [];
    for (const inf of everyPath(ctx)) inf.cs.forEach((c, ci) => c.pts.forEach((a, ai) => { const q = ap(inf.W, a.p); if (q[0] >= x0 && q[0] <= x1 && q[1] >= y0 && q[1] <= y1) out.push({ id: inf.id, ci, ai }); }));
    return out;
  }
  function directDown(ctx, ev, p, tool) {
    const st = S(ctx), k = pxPerPt(ctx), tA = 5 / k;
    if (st.run) endRun(ctx);
    const ts = targets(ctx);
    // a handle or an anchor, whichever is nearer (a short handle sits within a few px of its own anchor: a press right on the anchor takes the anchor), then a segment,
    // the body of a part, or nothing: the sweep
    let h = handleHit(st, ts, p, tA, tool === 'anchorConvert');
    if (h) { for (const inf of ts) { const a = anchorHit(inf, p, tA); if (a && a.d <= h.d) { h = null; break; } } }
    if (h) {
      flatten(ctx, [h.inf.id]); const inf = infoOf(ctx, h.inf.id); if (!inf) return;
      st.g = { kind: 'handle', id: inf.id, ci: h.ci, ai: h.ai, which: h.which, at: ap(inf.W, inf.cs[h.ci].pts[h.ai].p), c0: inf.cs[h.ci], cs0: inf.cs, smooth: isSmooth(inf.cs[h.ci].pts[h.ai]), free: tool === 'anchorConvert', live: false };
      return;
    }
    const t = pathThingAt(ctx, p);
    if (tool === 'anchorConvert') {
      if (t && t.kind === 'anchor') { selectAnchors(ctx, [{ id: t.inf.id, ci: t.ci, ai: t.ai }]); flatten(ctx, [t.inf.id]); const inf = infoOf(ctx, t.inf.id); st.g = { kind: 'pull', id: inf.id, ci: t.ci, ai: t.ai, at: ap(inf.W, inf.cs[t.ci].pts[t.ai].p), cs0: inf.cs, live: false }; }
      else if (t && t.inf) { ctx.select([t.inf.id]); st.anc.delete(t.inf.id); }
      else { st.anc = new Map(); ctx.select([]); }
      return;
    }
    if (t && t.kind === 'anchor') {
      const kk = key(t.ci, t.ai), on = picked(st, t.inf).has(kk) && ctx.selection().includes(t.inf.id);
      if (ev.shiftKey) {
        if (on) { st.anc.get(t.inf.id).delete(kk); if (typeof ctx.redraw === 'function') ctx.redraw(); st.g = { kind: 'none' }; return; }
        selectAnchors(ctx, [{ id: t.inf.id, ci: t.ci, ai: t.ai }], true);
      } else if (!on) selectAnchors(ctx, [{ id: t.inf.id, ci: t.ci, ai: t.ai }]);
      st.g = beginAnchorDrag(ctx, p, { lead: ap(t.inf.W, t.inf.cs[t.ci].pts[t.ai].p), leadId: t.inf.id });
      return;
    }
    if (t && t.kind === 'seg') {
      const s = segOf(t.inf.cs[t.ci], t.k);
      selectAnchors(ctx, ev.shiftKey ? [{ id: t.inf.id, ci: t.ci, ai: s.ka }, { id: t.inf.id, ci: t.ci, ai: s.kb }] : [], ev.shiftKey);
      if (!ev.shiftKey) { st.anc.delete(t.inf.id); if (!ctx.selection().includes(t.inf.id) || ctx.selection().length !== 1) ctx.select([t.inf.id]); }
      flatten(ctx, [t.inf.id]); const inf = infoOf(ctx, t.inf.id);
      st.g = { kind: 'seg', id: inf.id, ci: t.ci, k: t.k, t: t.t, x0: p.x, y0: p.y, Wi: inf.Wi, c0: inf.cs[t.ci], cs0: inf.cs, live: false };
      return;
    }
    if (t && t.kind === 'body') {   // inside a filled part: the whole part (all its anchors), dragged as one
      const all = []; t.inf.cs.forEach((c, ci) => c.pts.forEach((a, ai) => all.push({ id: t.inf.id, ci, ai })));
      selectAnchors(ctx, all, ev.shiftKey);
      st.g = beginAnchorDrag(ctx, p, { leadId: t.inf.id });
      return;
    }
    if (!ev.shiftKey) { st.anc = new Map(); if (ctx.selection().length) ctx.select([]); }
    st.g = { kind: 'marq', x0: p.x, y0: p.y, x: p.x, y: p.y, add: !!ev.shiftKey, base: new Map([...st.anc].map(([id, s]) => [id, new Set(s)])), baseSel: ev.shiftKey ? ctx.selection() : [], live: false };
    if (typeof ctx.redraw === 'function') ctx.redraw();
  }
  function directMove(ctx, ev, p, tool) {
    const st = S(ctx), g = st.g, k = pxPerPt(ctx);
    if (!g || !(ev.buttons & 1)) { directHover(ctx, p, tool); return; }
    if (!g.live) { const x0 = g.x0 != null ? g.x0 : g.at ? g.at[0] : p.x, y0 = g.y0 != null ? g.y0 : g.at ? g.at[1] : p.y; if (g.kind !== 'handle' && Math.hypot(p.x - x0, p.y - y0) * k < 3) return; g.live = true; }
    if (g.kind === 'anchors') dragAnchors(ctx, g, ev, p);
    else if (g.kind === 'handle') dragHandle(ctx, g, ev, p);
    else if (g.kind === 'seg') dragSeg(ctx, g, ev, p);
    else if (g.kind === 'pull') {   // Anchor Point tool: smooth handles pulled out of the anchor
      const inf = infoOf(ctx, g.id); if (!inf) return;
      let q = [p.x, p.y]; if (ev.shiftKey) q = snap45(g.at, q);
      const c = setHandle(g.cs0[g.ci], g.ai, 'o', ap(inf.Wi, q), 'mirror'), cs = g.cs0.slice(); cs[g.ci] = c; write(ctx, new Map([[g.id, cs]]));
      liveOf(ctx, g.id, handleText(g.at, q));
    } else if (g.kind === 'marq') {
      g.x = p.x; g.y = p.y;
      st.anc = new Map([...g.base].map(([id, s]) => [id, new Set(s)]));
      for (const { id, ci, ai } of anchorsIn(ctx, [g.x0, g.y0, g.x, g.y])) { if (!st.anc.has(id)) st.anc.set(id, new Set()); st.anc.get(id).add(key(ci, ai)); }
      const ids = [...new Set(g.baseSel.concat([...st.anc.keys()].filter(id => st.anc.get(id).size)))];
      const cur = ctx.selection(); if (ids.length !== cur.length || ids.some(id => !cur.includes(id))) ctx.select(ids);
      if (typeof ctx.redraw === 'function') ctx.redraw();
    }
  }
  function directUp(ctx, ev, p, tool) {
    const st = S(ctx), g = st.g; st.g = null;
    if (g && g.kind === 'pull' && !g.live) {   // a click with the Anchor Point tool: smooth <-> corner
      const inf = infoOf(ctx, g.id); if (inf) { const c = g.cs0[g.ci], a = c.pts[g.ai], cs = g.cs0.slice(); cs[g.ci] = a.i || a.o ? cornerAnchor(c, g.ai) : smoothAnchor(c, g.ai); write(ctx, new Map([[g.id, cs]])); }
    }
    if (typeof ctx.redraw === 'function') ctx.redraw();
  }
  function directHover(ctx, p, tool) {
    const st = S(ctx), k = pxPerPt(ctx);
    let id = null;
    if (!targets(ctx).some(t => anchorHit(t, p, 5 / k) || segHit(t, p, 4 / k))) { const inf = pathAt(ctx, p, 4 / k); id = inf ? inf.id : null; }
    if (id !== st.hovId) { st.hovId = id; if (typeof ctx.redraw === 'function') ctx.redraw(); }
  }
  /** Keys with anchors picked: the arrows move them (a run of taps is one undo step, as the editor's own nudge), Delete takes them out with their segments. False: not mine. */
  function directKeys(ctx, ev) {
    const st = S(ctx), ts = targets(ctx), any = ts.some(t => picked(st, t).size);
    if (!any) return false;
    const k = ev.key;
    if (/^Arrow(Left|Right|Up|Down)$/.test(k) && !(ev.metaKey || ev.ctrlKey || ev.altKey)) {
      ev.preventDefault();
      const s = (ev.shiftKey ? NUDGE_BIG_MM : NUDGE_MM) * PT, d = [k === 'ArrowLeft' ? -s : k === 'ArrowRight' ? s : 0, k === 'ArrowUp' ? -s : k === 'ArrowDown' ? s : 0];
      const sig = ts.map(t => t.id + '=' + [...picked(st, t)].join(',')).join('|');
      if (!st.run || st.run.sig !== sig) { endRun(ctx); st.run = { sig }; }
      const g = beginAnchorDrag(ctx, { x: 0, y: 0 });
      dragAnchors(ctx, g, {}, { x: d[0], y: d[1] });
      clearTimeout(st.run.timer); st.run.timer = setTimeout(() => endRun(ctx), 1200);
      return 'open';
    }
    if (k === 'Delete' || k === 'Backspace') {
      ev.preventDefault();
      flatten(ctx, ts.map(t => t.id));
      const map = new Map();
      for (const t0 of ts) {
        const t = infoOf(ctx, t0.id), s = picked(st, t); if (!s.size) continue;
        const by = new Map(); for (const kk of s) { const [ci, ai] = kk.split(':').map(Number); if (!by.has(ci)) by.set(ci, new Set()); by.get(ci).add(ai); }
        const cs = []; t.cs.forEach((c, ci) => { if (by.has(ci)) cs.push(...deleteAnchors(c, by.get(ci))); else cs.push(c); });
        map.set(t.id, cs);
      }
      st.anc = new Map(); write(ctx, map);
      if (typeof ctx.commit === 'function') ctx.commit('Delete anchor points');
      return true;
    }
    if (k === 'Escape') { st.anc = new Map(); if (typeof ctx.redraw === 'function') ctx.redraw(); return false; }
    return false;
  }
  function endRun(ctx) { const st = S(ctx); if (!st.run) return; clearTimeout(st.run.timer); st.run = null; if (typeof ctx.commit === 'function') ctx.commit('Nudge anchor points'); }

  const tool = (id, o) => Object.assign({ id, overlay: ctx => overlay(ctx, id), onArm(ctx) { injectCss(); stageSize(ctx, true); }, onDisarm(ctx) { const st = S(ctx); if (st.run) endRun(ctx); st.hovId = null; if (ctx.stage) ctx.stage.__cepCur = null; } }, o);
  const direct = tool('direct', {
    icon: IC.direct, title: 'Direct Selection', key: 'A', cursor: CURSOR.direct, tip: 'Anchors and handles',
    hint: 'Click an anchor (Shift adds) or sweep a box round anchors · drag anchors, handles or a segment · arrows nudge · Alt breaks a handle · Delete removes anchors',
    onDown: (ctx, ev, p) => { if (!ev.button) directDown(ctx, ev, p, 'direct'); },
    onMove: (ctx, ev, p) => directMove(ctx, ev, p, 'direct'),
    onUp: (ctx, ev, p) => directUp(ctx, ev, p, 'direct'),
    onKey: (ctx, ev) => directKeys(ctx, ev)
  });
  /** A tool that acts on one click: its own find (an anchor or a segment, of a target or of the part under the pointer) and act. */
  const clickTool = (id, o, find, act, mark) => tool(id, Object.assign(o, {
    onDown(ctx, ev, p) { if (ev.button) return; const st = S(ctx); if (st.run) endRun(ctx); const t = find(ctx, p); if (t) act(ctx, t); else { st.anc = new Map(); if (ctx.selection().length) ctx.select([]); } },
    onMove(ctx, ev, p) {
      const st = S(ctx), t = find(ctx, p), id2 = t ? t.inf.id : null;
      if (mark) setCursor(ctx, t ? mark : o.cursor);
      if (id2 !== st.hovId) { st.hovId = id2; if (typeof ctx.redraw === 'function') ctx.redraw(); }
    },
    onUp(ctx) { if (typeof ctx.redraw === 'function') ctx.redraw(); },
    onKey: (ctx, ev) => directKeys(ctx, ev)
  }));
  const anchorAdd = clickTool('anchorAdd', { icon: IC.add, title: 'Add Anchor Point', key: '+', flyout: 'Pen', cursor: CURSOR.add, tip: 'Click a segment', hint: 'Click a segment: an anchor there, the shape unchanged' },
    (ctx, p) => { const t = pathThingAt(ctx, p, { anchorFilter: () => false }); return t && t.kind === 'seg' ? t : null; }, addAnchorAt);
  const anchorDel = clickTool('anchorDel', { icon: IC.del, title: 'Delete Anchor Point', key: '-', flyout: 'Pen', cursor: CURSOR.del, tip: 'Click an anchor', hint: 'Click an anchor: it goes, the path stays joined' },
    (ctx, p) => { const t = pathThingAt(ctx, p); return t && t.kind === 'anchor' ? t : null; }, delAnchorAt);
  const anchorConvert = tool('anchorConvert', {
    icon: IC.convert, title: 'Anchor Point', key: 'Shift+C', flyout: 'Pen', cursor: CURSOR.convert, tip: 'Smooth or corner',
    hint: 'Click an anchor: smooth ↔ corner · drag from an anchor: pull out curve handles · drag a handle: it moves alone',
    onDown: (ctx, ev, p) => { if (!ev.button) directDown(ctx, ev, p, 'anchorConvert'); },
    onMove: (ctx, ev, p) => directMove(ctx, ev, p, 'anchorConvert'),
    onUp: (ctx, ev, p) => directUp(ctx, ev, p, 'anchorConvert'),
    onKey: (ctx, ev) => directKeys(ctx, ev)
  });
  /** Scissors: the path cut at an anchor, or at the point of a segment that was clicked; an open path's second piece becomes a part of its own (as Illustrator's). */
  function scissorsAt(ctx, t) {
    flatten(ctx, [t.inf.id]); let inf = infoOf(ctx, t.inf.id); if (!inf) return;
    let ci = t.ci, ai = t.ai, c = inf.cs[ci];
    if (t.kind === 'seg') { const r = splitSeg(c, t.k, t.t); c = r.c; ai = r.k; }
    if (isEnd(c, ai)) { say(ctx, 'An end of a path has nothing to cut'); return; }
    const parts = cutAt(c, ai), cs = inf.cs.slice();
    cs[ci] = parts[0];
    const map = new Map([[inf.id, cs]]);
    write(ctx, map);
    const picks = [{ id: inf.id, ci, ai: c.closed ? 0 : ai }];
    if (c.closed) picks.push({ id: inf.id, ci, ai: parts[0].pts.length - 1 });
    if (parts[1]) {
      // the second piece: a part of its own with the first's role, paint and transform, placed exactly where it was drawn
      inf = infoOf(ctx, inf.id);
      const tmp = Object.assign({}, inf.it, { id: CEM().newId(), source: 'added', name: (inf.it.name ? String(inf.it.name) : 'Path') + ' (cut)' });
      delete tmp._autoLock; delete tmp._lockedIn;
      const piece = rebuild(Object.assign({}, inf.it, { subpaths: toSubpaths([parts[1]]) }), [parts[1]], ctx.geo);
      const it2 = Object.assign(tmp, { subpaths: piece.subpaths, tx: num(piece.tx, 0), ty: num(piece.ty, 0) });
      // (rebuild kept the matrix of the part it was made from; the piece is placed by the same matrix, its own centre)
      const W = ctx.geo.matrix(inf.it), b = ctx.geo.box(it2);
      if (b && !isI(W)) { const cc = [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2], L = apL(W, cc); it2.tx = r9(W[4] - cc[0] + L[0]); it2.ty = r9(W[5] - cc[1] + L[1]); }
      const e = index(ctx).get(inf.id), parent = e && e.parent ? e.parent.id : null;
      ctx.addItems([it2], parent ? { parent, select: false } : { select: false });
      picks.push({ id: it2.id, ci: 0, ai: 0 });
    }
    selectAnchors(ctx, picks);
  }
  const scissors = clickTool('scissors', { icon: IC.scissors, title: 'Scissors', key: 'C', flyout: 'Pencil', cursor: CURSOR.scissors, tip: 'Cut a path', hint: 'Click a segment or an anchor: the path is cut there' },
    (ctx, p) => { const t = pathThingAt(ctx, p); return t && (t.kind === 'anchor' || t.kind === 'seg') ? t : null; }, scissorsAt);

  /* ═══ 9 · THE PENCIL ═══ */
  const PENCIL = { fidelityPx: 2.5, closePx: 15, editPx: 12 };
  const pencil = tool('pencil', {
    icon: IC.pencil, title: 'Pencil', key: 'N', cursor: CURSOR.pencil, flyout: 'Pencil', tip: 'Draw freehand',
    hint: 'Draw freehand: the line is smoothed into curves · end near the start to close · start at the end of a selected line to carry it on',
    onDown(ctx, ev, p) { if (ev.button) return; const st = S(ctx); st.ink = [[p.x, p.y]]; st.g = { kind: 'ink', live: true }; },
    onMove(ctx, ev, p) {
      const st = S(ctx); if (!st.ink || !(ev.buttons & 1)) return;
      const k = pxPerPt(ctx), last = st.ink[st.ink.length - 1];
      if (Math.hypot(p.x - last[0], p.y - last[1]) * k < 1) return;
      st.ink.push([p.x, p.y]);
      const b = bboxPts(st.ink); if (b && typeof ctx.live === 'function') ctx.live({ box: b }); else if (typeof ctx.redraw === 'function') ctx.redraw();
    },
    onUp(ctx) {
      const st = S(ctx), ink = st.ink; st.ink = null; st.g = null;
      if (!ink || ink.length < 2) { if (typeof ctx.redraw === 'function') ctx.redraw(); return; }
      const k = pxPerPt(ctx), tol = PENCIL.fidelityPx / k;
      let L = 0; for (let i = 1; i < ink.length; i++) L += dist(ink[i], ink[i - 1]);
      if (L * k < 4) { if (typeof ctx.redraw === 'function') ctx.redraw(); return; }
      const closed = dist(ink[0], ink[ink.length - 1]) * k <= PENCIL.closePx && L * k > 3 * PENCIL.closePx;
      // carry on a selected open line when the stroke starts at one of its ends
      let carry = null;
      if (!closed) for (const inf of targets(ctx)) { inf.cs.forEach((c, ci) => { if (carry || c.closed) return; for (const ai of [0, c.pts.length - 1]) { const q = ap(inf.W, c.pts[ai].p); if (dist(q, ink[0]) * k <= PENCIL.editPx) { carry = { inf, ci, end: ai ? 'last' : 'first' }; return; } } }); if (carry) break; }
      const pts = relax(ink, closed, 2);
      if (carry) pts[0] = ap(carry.inf.W, carry.inf.cs[carry.ci].pts[carry.end === 'first' ? 0 : carry.inf.cs[carry.ci].pts.length - 1].p);
      const c = fitPoints(pts, tol, closed, 70); if (!c) return;
      if (carry) {
        flatten(ctx, [carry.inf.id]); const inf = infoOf(ctx, carry.inf.id);
        const j = joinContours(inf.cs[carry.ci], carry.end, mapC(c, inf.Wi), 'first', 1e-6), cs = inf.cs.slice(); cs[carry.ci] = j;
        write(ctx, new Map([[inf.id, cs]]));
        ctx.select([inf.id]);
      } else {
        const it = newPathItem([c], 'Pencil line');
        ctx.addItems([it], { select: true });
      }
      if (typeof ctx.redraw === 'function') ctx.redraw();
    },
    onKey: () => false
  });

  /* ═══ 10 · COMMANDS (the Object menu, the right-click menu, the bar while anchors are picked) ═══ */
  /** The picked anchors grouped by part and contour: [{ inf, by: Map(ci -> Set(ai)) }] */
  function pickedBy(ctx) {
    const st = S(ctx), out = [];
    for (const t of targets(ctx)) {
      const s = picked(st, t); if (!s.size) continue;
      const by = new Map(); for (const kk of s) { const [ci, ai] = kk.split(':').map(Number); if (!by.has(ci)) by.set(ci, new Set()); by.get(ci).add(ai); }
      out.push({ inf: t, by });
    }
    return out;
  }
  const anchorsPicked = ctx => pickedBy(ctx).length > 0;
  const pathTool = ctx => ['direct', 'pen', 'anchorAdd', 'anchorDel', 'anchorConvert', 'scissors'].includes(typeof ctx.tool === 'function' ? ctx.tool() : '');
  /** Each picked anchor's contour changed by fn(c, ai) (one anchor at a time, last first, so the indexes still hold). */
  function eachPicked(ctx, fn, label) {
    const pb = pickedBy(ctx); if (!pb.length) return;
    flatten(ctx, pb.map(x => x.inf.id));
    const map = new Map();
    for (const { inf: i0, by } of pb) {
      const inf = infoOf(ctx, i0.id), cs = inf.cs.slice();
      for (const [ci, set] of [...by].sort((a, b) => b[0] - a[0])) {
        let parts = [cs[ci]];
        for (const ai of [...set].sort((a, b) => b - a)) { const r = fn(parts[0], ai); parts = (Array.isArray(r) ? r : [r]).filter(Boolean).concat(parts.slice(1)); if (!parts.length) break; }
        cs.splice(ci, 1, ...parts);
      }
      map.set(inf.id, cs);
    }
    write(ctx, map);
    if (label && typeof ctx.commit === 'function') ctx.commit(label);
  }
  /** The open ends to join: two picked anchors that are ends, else the ends of the whole open paths selected. */
  function joinSel(ctx) {
    const st = S(ctx), eps = 0.01;
    const ends = [];
    for (const { inf, by } of pickedBy(ctx)) for (const [ci, set] of by) for (const ai of set) if (isEnd(inf.cs[ci], ai)) ends.push({ id: inf.id, ci, end: ai === 0 ? 'first' : 'last' });
    if (ends.length === 2 || ends.length === 1) {
      if (ends.length === 1) { const e = ends[0], inf = infoOf(ctx, e.id); if (inf.cs[e.ci].pts.length < 2) return say(ctx, 'To join, pick two open ends'); ends.push({ id: e.id, ci: e.ci, end: e.end === 'first' ? 'last' : 'first' }); }
      return joinPair(ctx, ends[0], ends[1], eps);
    }
    if (ends.length > 2) return say(ctx, 'To join, pick two open ends (or select whole open paths)');
    // whole paths (an imported drawing's line soup among them): every pair of ends within JOIN.mm made one anchor and every nearly closed path closed, each laser role on its
    // own (a red line never joins the cut line); when that finds nothing, Illustrator's own: one open path closed, two joined at their nearest ends
    const ts = targets(ctx);
    if (!ts.some(t => t.cs.some(c => !c.closed && c.pts.length >= 2))) return say(ctx, 'Nothing open to join: select open paths, or pick two ends with Direct Selection');
    const r = chainParts(ctx, ts, JOIN.mm * PT, false);
    if (r.joins + r.closes) { say(ctx, `Joined ${r.joins} and closed ${r.closes} within ${JOIN.mm} mm`); return finishJoin(ctx, r.ids, 'join', r); }
    const opens = []; for (const t of ts) t.cs.forEach((c, ci) => { if (!c.closed && c.pts.length >= 2) opens.push({ id: t.id, ci, inf: t }); });
    if (opens.length === 1) return joinPair(ctx, { id: opens[0].id, ci: opens[0].ci, end: 'last' }, { id: opens[0].id, ci: opens[0].ci, end: 'first' }, eps);
    if (opens.length === 2) {
      const E = o => [ap(o.inf.W, o.inf.cs[o.ci].pts[0].p), ap(o.inf.W, o.inf.cs[o.ci].pts[o.inf.cs[o.ci].pts.length - 1].p)];
      const A = E(opens[0]), B = E(opens[1]); let best = null;
      for (let a = 0; a < 2; a++) for (let b = 0; b < 2; b++) { const d = dist(A[a], B[b]); if (!best || d < best.d) best = { d, a, b }; }
      return joinPair(ctx, { id: opens[0].id, ci: opens[0].ci, end: best.a ? 'last' : 'first' }, { id: opens[1].id, ci: opens[1].ci, end: best.b ? 'last' : 'first' }, eps);
    }
    say(ctx, `No ends within ${JOIN.mm} mm of each other · pick two ends with Direct Selection to join across a gap`);
  }
  /** Tolerance joining over parts: their open contours chained in face space, each role on its own; a chain lands in the part its first piece came from (in that part's own
   *  frame), and a part left with nothing goes. closeAll: every open chain left is then closed too (Close). → { joins, closes, ids } */
  const JOIN = { mm: 0.05 };
  function chainParts(ctx, ts, tolPt, closeAll) {
    flatten(ctx, ts.map(t => t.id));
    const infs = ts.map(t => infoOf(ctx, t.id)).filter(Boolean), byRole = new Map();
    for (const inf of infs) { const r = inf.it.role || 'art'; if (!byRole.has(r)) byRole.set(r, []); byRole.get(r).push(inf); }
    const out = new Map(infs.map(inf => [inf.id, inf.cs.slice()])), touched = new Set();
    let joins = 0, closes = 0;
    for (const group of byRole.values()) {
      const list = [];
      for (const inf of group) inf.cs.forEach((c, ci) => { if (!c.closed && c.pts.length >= 2) list.push({ inf, ci, c: mapC(c, inf.W) }); });
      if (!list.length) continue;
      const r = chainContours(list.map(x => x.c), tolPt);
      joins += r.joins; closes += r.closes;
      for (const ch of r.chains) {
        let c = ch.c, changed = ch.changed;
        if (closeAll && !c.closed && c.pts.length >= 2) { c = joinContours(c, 'last', null, null, tolPt); changed = true; closes++; }
        if (!changed) continue;
        const home = list[ch.members[0]];
        for (const m of ch.members) { const x = list[m]; out.get(x.inf.id)[x.ci] = null; touched.add(x.inf.id); }
        out.get(home.inf.id)[home.ci] = mapC(c, home.inf.Wi);
      }
    }
    const map = new Map(); for (const id of touched) map.set(id, out.get(id).filter(Boolean));
    if (map.size) write(ctx, map);
    return { joins, closes, ids: [...touched].filter(id => map.get(id).length) };
  }
  function finishJoin(ctx, ids, op, r) {
    S(ctx).anc = new Map(); if (ids.length) ctx.select(ids);
    if (typeof ctx.commit === 'function') ctx.commit(op === 'close' ? 'Close' : 'Join');
    logEv(op, r ? { joins: r.joins, closes: r.closes } : null);
  }
  /** Close: every open path selected closed: ends that nearly meet (JOIN.mm) are joined into one anchor first, the rest closed with a straight segment. */
  function closeSel(ctx) {
    const ts = targets(ctx); if (!ts.some(t => t.cs.some(c => !c.closed && c.pts.length >= 2))) return say(ctx, 'Nothing open to close');
    const r = chainParts(ctx, ts, JOIN.mm * PT, true);
    say(ctx, `Closed ${r.closes} path${r.closes === 1 ? '' : 's'}${r.joins ? ` · joined ${r.joins} ends within ${JOIN.mm} mm` : ''}`);
    finishJoin(ctx, r.ids, 'close', r);
  }
  function joinPair(ctx, e1, e2, eps) {
    const st = S(ctx);
    flatten(ctx, [e1.id, e2.id]);
    const i1 = infoOf(ctx, e1.id), i2 = infoOf(ctx, e2.id);
    const scale = Math.sqrt(Math.abs(i1.W[0] * i1.W[3] - i1.W[1] * i1.W[2])) || 1;
    if (e1.id === e2.id && e1.ci === e2.ci) {
      const cs = i1.cs.slice(); cs[e1.ci] = joinContours(cs[e1.ci], 'last', null, null, eps / scale); write(ctx, new Map([[i1.id, cs]]));
    } else if (e1.id === e2.id) {
      const cs = i1.cs.slice(), j = joinContours(cs[e1.ci], e1.end, cs[e2.ci], e2.end, eps / scale);
      cs[e1.ci] = j; cs.splice(e2.ci, 1); write(ctx, new Map([[i1.id, cs]]));
    } else {
      const oc = mapC(i2.cs[e2.ci], mul(i1.Wi, i2.W)), cs = i1.cs.slice(); cs[e1.ci] = joinContours(cs[e1.ci], e1.end, oc, e2.end, eps / scale);
      write(ctx, new Map([[i1.id, cs], [i2.id, i2.cs.filter((c, ci) => ci !== e2.ci)]]));
    }
    st.anc = new Map(); ctx.select([i1.id]);
    if (typeof ctx.commit === 'function') ctx.commit('Join');
    logEv('join');
  }
  /** The parts Simplify and Smooth work on: the picked anchors' parts, else the selected path parts. */
  const workParts = ctx => targets(ctx);
  function smoothSel(ctx) {
    const ts = workParts(ctx); if (!ts.length) return say(ctx, 'Select the paths to smooth');
    flatten(ctx, ts.map(t => t.id));
    const map = new Map(), k = pxPerPt(ctx);
    for (const t0 of ts) {
      const t = infoOf(ctx, t0.id), sc = Math.sqrt(Math.abs(t.W[0] * t.W[3] - t.W[1] * t.W[2])) || 1;
      const step = Math.max(2 / k, 0.02 * PT) / sc;   // (about 2 screen px, never finer than 0.02 mm on the charm)
      map.set(t.id, t.cs.map(c => smoothContour(c, step)));
    }
    S(ctx).anc = new Map(); write(ctx, map);
    if (typeof ctx.commit === 'function') ctx.commit('Smooth');
    logEv('smooth', { parts: ts.length });
  }
  /** Simplify: a small panel over the work area (the tolerance in mm on a slider, the anchors before and after) and the result drawn over the charm as the slider moves, the
   *  parts themselves dimmed under it; nothing is changed until Apply, which is ONE undo step; Cancel (or Esc, or a press on the work area) leaves everything as it was. */
  const SIMP = { mm: 0.02 };
  const tolOf = v => Math.round(Math.pow(10, -3 + 3 * v) * 1000) / 1000;           // slider 0..1 -> 0.001 .. 1 mm
  const sliderOf = mm => clamp((Math.log10(mm) + 3) / 3, 0, 1);
  const scaleOf = W => Math.sqrt(Math.abs(W[0] * W[3] - W[1] * W[2])) || 1;
  function simplifyParts(base, mm) { const out = new Map(); for (const t of base) out.set(t.id, t.cs.map(c => simplifyContour(c, mm * PT / scaleOf(t.W)))); return out; }
  function simplifySel(ctx) {
    const st = S(ctx);
    if (st.pop) return;
    const ts = workParts(ctx); if (!ts.length) return say(ctx, 'Select the paths to simplify');
    const ids = ts.map(t => t.id), before = ts.reduce((a, t) => a + anchorCount(t.cs), 0);
    const host = (ctx.stage && ctx.stage.parentNode) || doc.body, SVGNS = 'http://www.w3.org/2000/svg';
    injectCss();
    const pop = doc.createElement('div'); pop.className = 'cep-pop'; pop.setAttribute('role', 'dialog'); pop.setAttribute('aria-label', 'Simplify');
    pop.innerHTML = '<h4>Simplify</h4><div class="cep-pop__row"><input type="range" min="0" max="1" step="0.001" aria-label="Tolerance, mm"><span class="cep-pop__v"></span></div>'
      + '<div class="cep-pop__n" role="status"></div><div class="cep-pop__b"><button type="button" data-x="no">Cancel</button><button type="button" class="is-go" data-x="yes">Apply</button></div>';
    host.appendChild(pop);
    const sr = ctx.stage.getBoundingClientRect(), hr = host.getBoundingClientRect();
    Object.assign(pop.style, { left: Math.round(sr.left - hr.left + 12) + 'px', top: Math.round(sr.top - hr.top + 12) + 'px' });
    // the preview: its own layer over the charm (whatever tool is in hand), drawn again when the view moves
    const prev = doc.createElementNS(SVGNS, 'svg'); prev.setAttribute('class', 'cep-prev'); prev.setAttribute('aria-hidden', 'true');
    ctx.stage.appendChild(prev);
    const dim = doc.createElement('style'); dim.textContent = ids.map(id => `.ce-items [data-id="${String(id).replace(/["\\]/g, '\\$&')}"]`).join(',') + '{ opacity:.16; }';
    doc.head.appendChild(dim);
    const range = pop.querySelector('input'), val = pop.querySelector('.cep-pop__v'), cnt = pop.querySelector('.cep-pop__n');
    range.value = String(sliderOf(SIMP.mm));
    let raf = 0, cur = null;
    const paint = () => {
      if (!cur) return;
      const V = viewOf(ctx); let h = '', sq = '';
      for (const t of ts) {
        const cs = cur.get(t.id), VW = mul(V, t.W);
        h += `<path class="cep-prev__p" d="${CEM().dOf(toSubpaths(cs))}" transform="matrix(${VW.map(v => +v.toFixed(6)).join(' ')})" vector-effect="non-scaling-stroke"/>`;
        for (const c of cs) for (const a of c.pts) { const q = ap(VW, a.p); sq += `M${f1(q[0] - 2.5)} ${f1(q[1] - 2.5)}h5v5h-5Z`; }
      }
      prev.innerHTML = h + `<path class="cep-prev__a" d="${sq}"/>`;
    };
    const run = () => {
      raf = 0;
      const mm = tolOf(+range.value); SIMP.mm = mm; val.textContent = mm + ' mm';
      cur = simplifyParts(ts, mm);
      cnt.textContent = `Anchors: ${before} → ${[...cur.values()].reduce((a, cs) => a + anchorCount(cs), 0)}`;
      paint();
    };
    const art = ctx.stage.querySelector('.ce-art'), mo = art && root.MutationObserver ? new root.MutationObserver(paint) : null;
    if (mo) mo.observe(art, { attributes: true, attributeFilter: ['viewBox'] });
    const onStage = () => done(false);
    const done = keep => {
      if (!st.pop) return;
      if (raf) { cancelAnimationFrame(raf); raf = 0; }
      st.pop = null; pop.remove(); prev.remove(); dim.remove(); if (mo) mo.disconnect();
      ctx.stage.removeEventListener('pointerdown', onStage, true);
      if (keep) {
        flatten(ctx, ids);
        const now = ids.map(id => infoOf(ctx, id)).filter(Boolean), out = simplifyParts(now, SIMP.mm);
        write(ctx, out); st.anc = new Map();
        if (typeof ctx.commit === 'function') ctx.commit('Simplify');
        logEv('simplify', { mm: SIMP.mm, parts: now.length, before, after: [...out.values()].reduce((a, cs) => a + anchorCount(cs), 0) });
      }
      try { const w = ctx.root && ctx.root.querySelector && ctx.root.querySelector('.ce-win'); if (w) w.focus({ preventScroll: true }); } catch (e) { /* fine */ }
    };
    range.addEventListener('input', () => { if (!raf) raf = requestAnimationFrame(run); });
    pop.addEventListener('click', e => { const b = e.target.closest('[data-x]'); if (b) done(b.dataset.x === 'yes'); });
    ctx.stage.addEventListener('pointerdown', onStage, true);
    // (the editor takes every key at the window; an element that carries __ceKey is handed its own)
    const keys = e => { if (e.key === 'Enter') { e.preventDefault(); done(true); } else if (e.key === 'Escape') { e.preventDefault(); done(false); } };
    for (const el of pop.querySelectorAll('input,button')) el.__ceKey = keys;
    st.pop = { el: pop, done };
    run();
    try { range.focus({ preventScroll: true }); } catch (e) { /* fine */ }
  }
  const commands = {
    pathJoin: { title: 'Join', key: 'Mod+J', group: 'Path', icon: IC.join, run: joinSel, enabled: ctx => targets(ctx).some(t => t.cs.some(c => !c.closed && c.pts.length >= 2)) },
    pathClose: { title: 'Close path', group: 'Path', icon: IC.close, run: closeSel, enabled: ctx => targets(ctx).some(t => t.cs.some(c => !c.closed && c.pts.length >= 2)) },
    pathSimplify: { title: 'Simplify…', group: 'Path', icon: IC.simplify, run: simplifySel, enabled: ctx => targets(ctx).some(t => t.cs.some(c => c.pts.length > 2)) },
    pathSmooth: { title: 'Smooth', group: 'Path', icon: IC.smooth, run: smoothSel, enabled: ctx => targets(ctx).length > 0 },
    anchorsCorner: { title: 'Convert anchors to corner', group: 'Anchor points', icon: IC.corner, bar: true, run: ctx => eachPicked(ctx, cornerAnchor, 'Convert to corner'), enabled: ctx => pathTool(ctx) && anchorsPicked(ctx) },
    anchorsSmooth: { title: 'Convert anchors to smooth', group: 'Anchor points', icon: IC.smoothPt, bar: true, run: ctx => eachPicked(ctx, smoothAnchor, 'Convert to smooth'), enabled: ctx => pathTool(ctx) && anchorsPicked(ctx) },
    anchorsRemove: { title: 'Remove anchor points', group: 'Anchor points', icon: IC.removePt, bar: true, run: ctx => { eachPicked(ctx, (c, ai) => removeAnchor(c, ai), 'Remove anchor points'); S(ctx).anc = new Map(); }, enabled: ctx => pathTool(ctx) && anchorsPicked(ctx) },
    anchorsCut: { title: 'Cut path at anchors', group: 'Anchor points', icon: IC.cutPt, bar: true, run: ctx => { eachPicked(ctx, (c, ai) => cutAt(c, ai), 'Cut path'); logEv('cut'); }, enabled: ctx => pathTool(ctx) && anchorsPicked(ctx) },
    // (the keys Illustrator gives the anchor tools: + and − are the tools' own, Shift+C needs the command map to tell it from C, and = is + without its Shift)
    toolAnchorConvert: { title: 'Anchor Point tool', key: 'Shift+C', menu: false, run: ctx => ctx.setTool('anchorConvert') },
    toolAnchorAdd: { title: 'Add Anchor Point tool', key: '=', menu: false, run: ctx => ctx.setTool('anchorAdd') }
  };
  const tools = [pen, anchorAdd, anchorDel, anchorConvert, direct, pencil, scissors];

  /** Registered with the editor's extension points (CharmEditor.tools, CharmEditor.commands), whether the editor loaded before this file or after it. */
  function register(r) {
    const CE = r.CharmEditor || (r.CharmEditor = {});
    const list = CE.tools || (CE.tools = []);
    for (const t of tools) if (!list.some(x => x && x.id === t.id)) list.push(t);
    const cmds = CE.commands || (CE.commands = {});
    for (const k of Object.keys(commands)) if (!cmds[k]) cmds[k] = commands[k];
    (CE.onClose || (CE.onClose = [])).push(ctx => { if (St && St.ctx === ctx) { if (St.pop) { St.pop.el.remove(); } if (St.run) clearTimeout(St.run.timer); St = null; } });
  }

  return { model, tools, commands, register, PENCIL, SIMP, JOIN, state: () => St, version: 1 };
});
