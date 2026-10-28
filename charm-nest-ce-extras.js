/* charm-nest-ce-extras.js — THE CHARM EDITOR'S EXTRAS: the high-end features a charm designer reaches for that no other file of the Design & Engrave update builds (Paul,
 * 28 Oct 2026: "A lot of the high-end features are missing and I would like you to put all the useful ones in ... I'm trying to eliminate the use of the Adobe platform since
 * it's costing me $500 a month ... designing custom charms ... You need to follow the 70/30 Ruel"; items 8 and 10, the research's MISSING list, briefs/research-70-30.md §3).
 * It plugs into charm-nest-charm-editor.js through its EXTENSION POINTS (CharmEditor.tools / commands / onOpen / onChange / overlays / textLayout) and changes nothing else.
 *
 *   1 SHAPES   the Shapes flyout gains the charm blanks, as the website studio has them (J 9042, 10650-10680): a rounded rectangle (its corner on the bar), a star (points,
 *              inner %), a heart, a ring (two circles, one cut: a washer or a donut charm) and the JUMP RING (the shop's standard ring, 6.5 pt outside / 3.7 pt hole, or the
 *              Huggie's 9.35 / 5.85: charm-nest-pdf.js 3026), set on the top edge of what is selected, as a cut, and welded into it when that is the charm's cut line (the
 *              polygon is the editor's own). Shift keeps the shape's own proportions, Alt or Ctrl draws from the centre (D1), the shape snaps and its size is read out as it
 *              is drawn (ctx.snap, ctx.live); a click without a drag makes one of the size last drawn. A blank keeps its numbers: its corner, points or wall change after.
 *   2 TEXT     LETTER SPACING (Illustrator's tracking, thousandths of an em) for any words, on the bar; WORDS ALONG A PATH (Type on a Path) or ROUND THE CHARM: the words stay
 *              words (double-click to edit, the font, size and weight as any text), with Along (where they sit on the path), Rise (off the path) and Flip on the bar; the
 *              editor hands them back as outlines, as all words. A text along a path carries its path in itself (pathD, in its own coordinates) and plain numbers, so it
 *              travels anywhere a text does.
 *   3 CLIP     ⌘7 CLIP TO THE CHARM (Illustrator's clipping mask): the engraving that runs past the cut line is cut back to it, as real geometry (CharmNestVector.boolean;
 *              lines by Clipper's open paths), so nothing is engraved off the metal; with the topmost of several selected things a closed path, the others are clipped to
 *              it instead. ⌥⌘7 gives every clipped part back as it was (moved as it has been moved since).
 *   4 CHECK    THE LASER CHECK runs by itself a moment after every change, in slices of a few ms: bridges and walls thinner than 0.3 mm, cut lines that are not closed,
 *              engraving past the cut line or into the jump-ring hole, letters smaller than the engraver makes (the Settings' engraveMinCapMm, 1.6 mm). A short list with
 *              Show (it selects the part and zooms to the spot); Save says "N to check" and never waits for it.
 *   5 VIEW     ⌘Y OUTLINE VIEW: every part a hairline in its role's colour, no fills (doubled cut lines and stray points show).
 *   6 KEYS     L the ellipse, \ the line (in the editor's own key map), ⌘7 / ⌥⌘7, ⌘Y. Alt-drag copies (the editor's select tool).
 *
 * Nothing here saves or writes: what it changes goes through the ctx (one undo step each) and leaves the editor in its result. Every action is logged for the employee
 * (CNDesignLog: shapePreset, textOnPath, clipToCharm, designCheck ...). Loads in the page (window.CharmNestCeExtras) and in node (module.exports: the geometry, for tests). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root && typeof root === 'object' && root.document) root.CharmNestCeExtras = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const req = p => { try { return typeof require === 'function' ? require(p) : null; } catch (e) { return null; } };
  const CE = (root && root.CharmEditor && root.CharmEditor.model ? root.CharmEditor : null) || req('./charm-nest-charm-editor.js');
  if (!CE || !CE.model) return { ready: false };
  const M = CE.model;
  const Vec = () => (root && root.CharmNestVector) || req('./charm-nest-vector.js');
  const Clip = () => (root && root.ClipperLib) || req('./vendor/clipper-6.4.2.js');
  const doc = root && root.document ? root.document : null;

  const MM = 25.4 / 72, PT = 72 / 25.4;
  const fin = v => typeof v === 'number' && Number.isFinite(v);
  const num = (v, d) => (fin(v) ? v : d);
  const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
  const r4 = v => { const r = Math.round(v * 1e4) / 1e4; return Object.is(r, -0) ? 0 : r; };
  const r9 = v => { const r = Math.round(v * 1e9) / 1e9; return Object.is(r, -0) ? 0 : r; };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const trim = (v, dp) => String(+(+v).toFixed(dp));
  const numIn = v => parseFloat(String(v == null ? '' : v).replace(',', '.').replace(/−/g, '-'));
  const log = (kind, data) => { try { if (root && root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (e) { /* the log is a courtesy */ } };
  const now = () => (root && root.performance && root.performance.now ? root.performance.now() : Date.now());

  /* ═══ 1 · SHAPES: the charm blanks, each made to fill a box exactly (its tight box, curves included, is the box) ═══ */
  const KAPPA = 0.5522847498;
  /** A rectangle with round corners of radius r (pt) filling b = [x0, y0, x1, y1]; r is held to half the shorter side. */
  function rrectSP(b, r) {
    const x0 = b[0], y0 = b[1], x1 = b[2], y1 = b[3];
    r = clamp(num(r, 0), 0, Math.min(x1 - x0, y1 - y0) / 2);
    if (r < 1e-6) return M.rectSP(x0, y0, x1, y1);
    const k = r * KAPPA;
    return [[['m', [x0 + r, y0]], ['l', [x1 - r, y0]], ['c', [x1 - r + k, y0], [x1, y0 + r - k], [x1, y0 + r]], ['l', [x1, y1 - r]],
      ['c', [x1, y1 - r + k], [x1 - r + k, y1], [x1 - r, y1]], ['l', [x0 + r, y1]], ['c', [x0 + r - k, y1], [x0, y1 - r + k], [x0, y1 - r]],
      ['l', [x0, y0 + r]], ['c', [x0, y0 + r - k], [x0 + r - k, y0], [x0 + r, y0]], ['h']]];
  }
  const polySP = pts => [[['m', pts[0]]].concat(pts.slice(1).map(p => ['l', p]), [['h']])];
  /** A star of n points about 0,0, the points 1 out and the notches `inner` (0..1) of that, a point straight up (the studio's, J 10650, with its points and depth free). */
  function starUnit(n, inner) {
    n = clamp(Math.round(num(n, 5)), 3, 40); inner = clamp(num(inner, 0.45), 0.05, 0.98);
    const pts = []; for (let i = 0; i < 2 * n; i++) { const a = -Math.PI / 2 + i * Math.PI / n, r = i % 2 ? inner : 1; pts.push([Math.cos(a) * r, Math.sin(a) * r]); }
    return polySP(pts);
  }
  /** The studio's heart (J 10668-10673): two cubics from the point to the dip and back, in a unit box. */
  const HEART = [[['m', [0.5, 1]], ['c', [-0.12, 0.55], [0.08, -0.10], [0.5, 0.28]], ['c', [0.92, -0.10], [1.12, 0.55], [0.5, 1]], ['h']]];
  /** Subpaths moved and stretched so their TIGHT box is `box`. */
  function fitTo(sps, box) {
    const b = M.bboxOf(sps); if (!b) return sps;
    const sx = (box[2] - box[0]) / ((b[2] - b[0]) || 1), sy = (box[3] - box[1]) / ((b[3] - b[1]) || 1);
    return M.transformSubpaths(sps, [sx, 0, 0, sy, box[0] - b[0] * sx, box[1] - b[1] * sy]);
  }
  const aspectOf = sps => { const b = M.bboxOf(sps); return b ? (b[2] - b[0]) / ((b[3] - b[1]) || 1) : 1; };
  /** Two ellipses, the inner one `wall` (pt) inside the outer (at most 90 % of it gone): one compound path, its middle open (even-odd). */
  function ringSP(b, wall) {
    const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2, rx = (b[2] - b[0]) / 2, ry = (b[3] - b[1]) / 2;
    const w = clamp(num(wall, PT), 0, 0.9 * Math.min(rx, ry));
    return M.ellipseSP(cx, cy, rx, ry).concat(M.ellipseSP(cx, cy, Math.max(rx - w, 0.1 * rx), Math.max(ry - w, 0.1 * ry)));
  }
  /* the shop's two rings (charm-nest-pdf.js 3026: "6.5 pt outside / 3.7 pt hole (the standard ring) · 9.35 pt outside / 5.85 pt hole (the HUGGIE ring)") */
  const RING = { std: { outPt: 6.5, holePt: 3.7, name: 'Standard ring' }, huggie: { outPt: 9.35, holePt: 5.85, name: 'Huggie ring' } };
  const ringKey = k => (RING[k] ? k : 'std');
  const bailSP = (cx, cy, k) => { const R = RING[ringKey(k)]; return M.ellipseSP(cx, cy, R.outPt / 2, R.outPt / 2).concat(M.ellipseSP(cx, cy, R.holePt / 2, R.holePt / 2)); };

  /* each blank: what it is called, the role it is drawn in (a ring is cut, as the studio's; the others engraved along the line, as the editor's rectangle), the numbers it
     keeps, its own proportions (Shift) and how it is made in a box; s = the part's own scale, so a number in mm stays mm on the charm */
  const SHAPES = {
    rrect: { title: 'Rounded rectangle', params: ['corner'], aspect: () => 1, make: (box, o, s) => rrectSP(box, num(o.corner, 1) * PT / (s || 1)),
      hint: 'Drag a rounded rectangle · Shift for a square · Alt or Ctrl from the centre · the corner on the bar' },
    star: { title: 'Star', params: ['points', 'inner'], aspect: o => aspectOf(starUnit(o.points, o.inner / 100)), make: (box, o) => fitTo(starUnit(o.points, num(o.inner, 45) / 100), box),
      hint: 'Drag a star · Shift keeps it regular · Alt or Ctrl from the centre · points and depth on the bar' },
    heart: { title: 'Heart', params: [], aspect: () => aspectOf(HEART), make: box => fitTo(HEART, box), hint: 'Drag a heart · Shift keeps its shape · Alt or Ctrl from the centre' },
    ring: { title: 'Ring', role: 'cut', evenOdd: true, params: ['wall'], aspect: () => 1, make: (box, o, s) => ringSP(box, num(o.wall, 1) * PT / (s || 1)),
      hint: 'Drag a ring (cut through, its middle open) · Shift for a circle · Alt or Ctrl from the centre · the wall on the bar' }
  };
  const DEF = { corner: 1, points: 5, inner: 45, wall: 1, ring: 'std', w: 6 };
  const opt = Object.assign({}, DEF);
  try { const v = root && root.localStorage && JSON.parse(root.localStorage.getItem('ce.extras') || 'null'); if (v && typeof v === 'object') for (const k of Object.keys(DEF)) if (typeof v[k] === typeof DEF[k]) opt[k] = v[k]; } catch (e) { /* storage blocked: the shop's numbers */ }
  const keepOpt = () => { try { if (root && root.localStorage) root.localStorage.setItem('ce.extras', JSON.stringify(opt)); } catch (e) { /* fine */ } };
  /** A fingerprint of a drawing: a blank's numbers are offered only while its drawing is still the one they made (the pen may have moved its points since). */
  function sigOf(sps) {
    let n = 0, s = 0;
    for (const sub of sps || []) for (const op of sub || []) { n++; for (const p of op.slice(1)) if (Array.isArray(p)) s += p[0] * 0.618 + p[1] * 1.414; }
    return n + ':' + s.toFixed(3);
  }
  const shapeRec = (kind, o, sps) => { const r = { kind, sig: sigOf(sps) }; for (const p of (SHAPES[kind] || { params: [] }).params) r[p] = o[p]; if (kind === 'bail') r.ring = ringKey(o.ring); return r; };
  /** A new blank of `kind` filling `box` (pt), as the editor draws a new shape (lines at the laser's width). */
  function shapeItem(kind, box, o) {
    const d = SHAPES[kind], sps = d.make(box, o, 1), role = d.role || 'line';
    const it = { kind: 'path', role, source: 'added', name: d.title, hidden: false, locked: false, stroke: role !== 'hatch', fill: role === 'hatch', lwPt: M.LW_LASER_PT,
      subpaths: sps, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, shape: shapeRec(kind, o, sps) };
    if (d.evenOdd) it.evenOdd = true;
    return it;
  }
  /** The blank's numbers, while its drawing is still the one they made; else null. */
  const shapeOf = it => (it && it.kind === 'path' && it.shape && typeof it.shape === 'object' && (SHAPES[it.shape.kind] || it.shape.kind === 'bail') && it.shape.sig === sigOf(it.subpaths) ? it.shape : null);
  /** The blank drawn again with new numbers, in its own box (its move, turn and scale kept: the box is the same, so nothing else of it moves). */
  function reshape(it, patch, geo) {
    const sh = shapeOf(it); if (!sh) return null;
    const o = Object.assign({}, sh, patch), rb = geo.box(it); if (!rb) return null;
    const s = Math.sqrt(Math.abs(num(it.sx, 1) * num(it.sy, 1))) || 1;
    let sps;
    if (sh.kind === 'bail') sps = bailSP((rb[0] + rb[2]) / 2, (rb[1] + rb[3]) / 2, o.ring);
    else sps = SHAPES[sh.kind].make(rb, o, s);
    return Object.assign({}, it, { subpaths: sps, shape: shapeRec(sh.kind, o, sps) });
  }
  /** The box a drag from a to q makes: aspect > 0 keeps those proportions (Shift), centred draws it out from a (Alt or Ctrl). */
  function dragBox(a, q, aspect, centred) {
    let w = q[0] - a[0], h = q[1] - a[1];
    if (aspect > 0) { const k = Math.max(Math.abs(w) / aspect, Math.abs(h)); w = Math.sign(w || 1) * k * aspect; h = Math.sign(h || 1) * k; }
    const x0 = centred ? a[0] - w : a[0], y0 = centred ? a[1] - h : a[1], x1 = a[0] + w, y1 = a[1] + h;
    const b = [Math.min(x0, x1), Math.min(y0, y1), Math.max(x0, x1), Math.max(y0, y1)];
    if (b[2] - b[0] < 0.05) b[2] = b[0] + 0.05; if (b[3] - b[1] < 0.05) b[3] = b[1] + 0.05;
    return b;
  }
  /** Where the vertical through x first meets the outlines from above (pt), or null. */
  function topAt(sps, x) {
    let best = null;
    for (const pl of M.flatten(sps)) {
      const p = pl.pts, n = p.length / 2;
      for (let i = 0; i < (pl.closed ? n : n - 1); i++) {
        const j = (i + 1) % n, ax = p[2 * i], ay = p[2 * i + 1], bx = p[2 * j], by = p[2 * j + 1];
        if ((ax - x) * (bx - x) > 0 || ax === bx) continue;
        const y = ay + (by - ay) * (x - ax) / (bx - ax); if (best == null || y < best) best = y;
      }
    }
    return best;
  }
  const allClosed = sps => (sps || []).length > 0 && sps.every(sub => (sub || []).some(op => op && op[0] === 'h'));

  /* ═══ 2 · TEXT: letter spacing and words along a path, laid out for the editor (CharmEditor.textLayout) ═══ */
  const graphemes = s => {
    try { if (typeof Intl === 'object' && Intl.Segmenter) return Array.from(new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(s), x => x.segment); } catch (e) { /* old engines */ }
    return Array.from(s);
  };
  const countCache = new Map();
  /** How many subpaths the engine draws for one letter (a letter is drawn the same alone as among others: its outlines, in order). */
  function countOf(engine, g, fontKey, size) {
    const k = engine === CE.builtinTextToPaths ? '' : null;
    const key = (k != null ? 'b' : 'e') + '\u0001' + fontKey + '\u0001' + size + '\u0001' + g;
    let n = countCache.get(key);
    if (n == null) { let r = null; try { r = engine(g, fontKey, size, { align: 'left' }); } catch (e) { r = null; } n = r && Array.isArray(r.subpaths) ? r.subpaths.length : 0; countCache.set(key, n); if (countCache.size > 4000) countCache.delete(countCache.keys().next().value); }
    return n;
  }
  /** The words as the engine draws them, cut into letters: [{ line, at (its place in the line), of (letters in the line), subs }], or null when the letters do not add up
   *  to the words (a ligature, a shaped script: the engine's own drawing is then the words, unspaced). */
  function letters(engine, words, it, size, again) {
    const full = engine(words, it.fontKey, size, { align: it.align || 'left', lineGap: it.lineGap });
    const subs = full && Array.isArray(full.subpaths) ? full.subpaths : [];
    const out = []; let k = 0;
    String(words).split(/\r?\n/).forEach((line, li) => {
      const gs = graphemes(line);
      gs.forEach((g, gi) => { const n = countOf(engine, g, it.fontKey, size); out.push({ line: li, at: gi, of: gs.length, subs: subs.slice(k, k + n) }); k += n; });
    });
    if (k === subs.length) return { out, full };
    // (a font the words need may have arrived since the letters were counted, charm-nest-ce-text.js loads them on demand: counted once more before giving up)
    if (!again) { countCache.clear(); capCache.clear(); return letters(engine, words, it, size, true); }
    return null;
  }
  const move = (sps, dx, dy) => (dx || dy ? M.transformSubpaths(sps, [1, 0, 0, 1, dx, dy]) : sps);

  /* the path a text runs along: SVG path data (M L C Z, absolute), kept on the text as `pathD` in the text's own coordinates */
  function dStr(sps) {
    const f = v => String(Math.round(v * 100) / 100);
    let d = '';
    for (const sub of sps || []) for (const op of sub || []) {
      if (op[0] === 'm') d += 'M' + f(op[1][0]) + ' ' + f(op[1][1]);
      else if (op[0] === 'l') d += 'L' + f(op[1][0]) + ' ' + f(op[1][1]);
      else if (op[0] === 'c') d += 'C' + [op[1], op[2], op[3]].map(p => f(p[0]) + ' ' + f(p[1])).join(' ');
      else if (op[0] === 'h') d += 'Z';
    }
    return d;
  }
  function parseD(d) {
    const t = String(d || '').match(/[MLCZmlcz]|[-+]?(?:\d+\.?\d*|\.\d+)(?:e[-+]?\d+)?/g) || [];
    const out = []; let sub = null, i = 0, cmd = '';
    const n = () => +t[i++], p = () => [n(), n()];
    while (i < t.length) {
      if (/[A-Za-z]/.test(t[i])) cmd = t[i++].toUpperCase();
      if (cmd === 'M') { sub = [['m', p()]]; out.push(sub); cmd = 'L'; }
      else if (cmd === 'L' && sub) sub.push(['l', p()]);
      else if (cmd === 'C' && sub) sub.push(['c', p(), p(), p()]);
      else if (cmd === 'Z') { if (sub) sub.push(['h']); sub = null; }
      else i++;
    }
    return out.filter(s => s.length > 1 && s.every(op => op.slice(1).every(q => fin(q[0]) && fin(q[1]))));
  }
  /** A path as a line to walk along: its points, the distance walked to each, where on it a distance lands (and which way it runs there). */
  function walker(pts, closed) {
    if (closed && (pts[0][0] !== pts[pts.length - 1][0] || pts[0][1] !== pts[pts.length - 1][1])) pts = pts.concat([pts[0]]);
    const cum = [0]; for (let i = 1; i < pts.length; i++) cum.push(cum[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]));
    const L = cum[cum.length - 1];
    const dirAt = i => { const a = pts[i], b = pts[i + 1], l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1; return [(b[0] - a[0]) / l, (b[1] - a[1]) / l]; };
    function point(s) {
      if (!(L > 0)) return { x: pts[0][0], y: pts[0][1], tx: 1, ty: 0 };
      if (closed) s = ((s % L) + L) % L;
      else if (s < 0) { const d = dirAt(0); return { x: pts[0][0] + d[0] * s, y: pts[0][1] + d[1] * s, tx: d[0], ty: d[1] }; }
      else if (s > L) { const d = dirAt(pts.length - 2), e = pts[pts.length - 1]; return { x: e[0] + d[0] * (s - L), y: e[1] + d[1] * (s - L), tx: d[0], ty: d[1] }; }
      let lo = 0, hi = cum.length - 1; while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (cum[mid] <= s) lo = mid; else hi = mid; }
      const seg = cum[lo + 1] - cum[lo] || 1, t = (s - cum[lo]) / seg, a = pts[lo], b = pts[lo + 1], d = dirAt(lo);
      return { x: a[0] + (b[0] - a[0]) * t, y: a[1] + (b[1] - a[1]) * t, tx: d[0], ty: d[1] };
    }
    const extreme = sign => { let bi = 0; for (let i = 1; i < pts.length; i++) if (sign * pts[i][1] < sign * pts[bi][1]) bi = i; return L > 0 ? cum[bi] / L : 0; };
    return { pts, cum, L, closed, point, top: () => extreme(1), bottom: () => extreme(-1) };
  }
  /** A subpath as a polyline (cubics in chords of a quarter point at most). */
  function polyOf(sub) {
    const pts = []; let cur = null, closed = false;
    for (const op of sub || []) {
      if (op[0] === 'm' || op[0] === 'l') { cur = op[1]; pts.push([cur[0], cur[1]]); }
      else if (op[0] === 'c' && cur) {
        const a = cur, p1 = op[1], p2 = op[2], p3 = op[3];
        const len = Math.hypot(p1[0] - a[0], p1[1] - a[1]) + Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) + Math.hypot(p3[0] - p2[0], p3[1] - p2[1]);
        const n = clamp(Math.ceil(len / 0.25), 4, 256);
        for (let i = 1; i <= n; i++) { const t = i / n, u = 1 - t; pts.push([u * u * u * a[0] + 3 * u * u * t * p1[0] + 3 * u * t * t * p2[0] + t * t * t * p3[0], u * u * u * a[1] + 3 * u * u * t * p1[1] + 3 * u * t * t * p2[1] + t * t * t * p3[1]]); }
        cur = p3;
      } else if (op[0] === 'h') closed = true;
    }
    return { pts, closed };
  }
  const longest = sps => { let best = null, bl = -1; for (const sub of sps || []) { const p = polyOf(sub); if (p.pts.length < 2) continue; const w = walker(p.pts, p.closed); if (w.L > bl) { bl = w.L; best = sub; } } return best; };
  const guideCache = new Map();
  /** The path a text runs along, from its pathD: { sps, fwd, rev } (rev: the same path walked the other way, for Flip); cached by the string. */
  function guideOf(d) {
    let g = guideCache.get(d); if (g) return g;
    const sps = parseD(d), sub = longest(sps); if (!sub) return null;
    const p = polyOf(sub); if (p.pts.length < 2) return null;
    const fwd = walker(p.pts, p.closed), rev = walker(p.pts.slice().reverse(), p.closed);
    g = { sps: [sub], fwd, rev, closed: p.closed, L: fwd.L };
    guideCache.set(d, g); if (guideCache.size > 200) guideCache.delete(guideCache.keys().next().value);
    return g;
  }
  /** A closed path turned to run clockwise on the charm (so words stand outward on its top), an open one to run left to right (so they read). */
  function orient(sub) {
    const p = polyOf(sub); if (p.pts.length < 2) return sub;
    let rev;
    if (p.closed) { let a = 0; for (let i = 0; i < p.pts.length; i++) { const q = p.pts[i], r = p.pts[(i + 1) % p.pts.length]; a += q[0] * r[1] - r[0] * q[1]; } rev = a < 0; }
    else rev = p.pts[p.pts.length - 1][0] < p.pts[0][0];
    return rev ? reverseSub(sub) : sub;
  }
  function reverseSub(sub) {
    const segs = []; let cur = null, start = null, closed = false;
    for (const op of sub) { if (op[0] === 'm') { cur = op[1]; start = cur; } else if (op[0] === 'l') { segs.push(['l', cur, op[1]]); cur = op[1]; } else if (op[0] === 'c') { segs.push(['c', cur, op[1], op[2], op[3]]); cur = op[3]; } else if (op[0] === 'h') closed = true; }
    if (closed && cur && start && (cur[0] !== start[0] || cur[1] !== start[1])) segs.push(['l', cur, start]);
    const out = [['m', (segs.length ? (segs[segs.length - 1][0] === 'c' ? segs[segs.length - 1][4] : segs[segs.length - 1][2]) : start)]];
    for (let i = segs.length - 1; i >= 0; i--) { const s = segs[i]; out.push(s[0] === 'c' ? ['c', s[3], s[2], s[1]] : ['l', s[1]]); }
    if (closed) out.push(['h']);
    return out;
  }
  /** A capital's ink from its baseline, pt, y down: [top, bottom] of an H's outlines ([-0.7 of the size, 0] when the engine draws none); capOf: its height. */
  const capCache = new Map();
  function capBand(engine, fontKey, size) {
    const key = fontKey + '\u0001' + size; let v = capCache.get(key);
    if (v == null) { let b = null; try { const r = engine('H', fontKey, size, { align: 'left' }); b = r && (M.bboxOf(r.subpaths) || r.bbox); } catch (e) { b = null; } v = b && b[3] > b[1] ? [b[1], b[3]] : [-0.7 * size, 0]; capCache.set(key, v); if (capCache.size > 500) capCache.delete(capCache.keys().next().value); }
    return v;
  }
  const capOf = (engine, fontKey, size) => { const b = capBand(engine, fontKey, size); return b[1] - b[0]; };
  const isBent = it => !!it && it.kind === 'text' && typeof it.pathD === 'string' && it.pathD.length > 0;
  /** CharmEditor.textLayout: words spaced (tracking) and words along their path. */
  const textLayout = {
    key(it) {
      const tr = num(it.tracking, 0), bent = isBent(it);
      if (!tr && !bent) return '';
      return 't' + tr + (bent ? '|' + it.pathD + '|' + num(it.pathAt, 0.5) + '|' + (it.pathFlip ? 1 : 0) + '|' + num(it.pathRisePt, 0) : '');
    },
    bent: isBent,
    layout(it, engine) {
      const size = num(it.sizePt, 10), track = num(it.tracking, 0) / 1000 * size, bent = isBent(it);
      let words = String(it.text == null ? '' : it.text);
      if (bent) words = words.replace(/\s*\r?\n\s*/g, ' ');
      const L = letters(engine, words, Object.assign({}, it, bent ? { align: it.align || 'center' } : {}), size);
      if (!L) return null;
      const align = it.align || 'left', dxOf = g => g.at * track - (align === 'center' ? (g.of - 1) * track / 2 : align === 'right' ? (g.of - 1) * track : 0);
      const out = [];
      if (!bent) { for (const g of L.out) for (const sp of move(g.subs, dxOf(g), 0)) out.push(sp); return { subpaths: out, bbox: M.bboxOf(out) }; }
      const G = guideOf(it.pathD); if (!G) return null;
      const flip = !!it.pathFlip, W = flip ? G.rev : G.fwd, at = num(it.pathAt, 0.5), rise = num(it.pathRisePt, 0);
      const s0 = flip ? G.L * (1 - at) : G.L * at;
      for (const g of L.out) {
        if (!g.subs.length) continue;
        const b = M.bboxOf(g.subs); if (!b) continue;
        const xc = (b[0] + b[2]) / 2, p = W.point(s0 + xc + dxOf(g)), c = p.tx, s = p.ty;
        // the letter turned to the path where its middle falls, its baseline on the path (raised by rise, along the letter's own up: (sin, −cos))
        const m = [c, s, -s, c, p.x + s * rise - c * xc, p.y - c * rise - s * xc];
        for (const sp of M.transformSubpaths(g.subs, m)) out.push(sp);
      }
      return { subpaths: out, bbox: M.bboxOf(out) };
    }
  };
  /** A text put on a path: guideFace (face coordinates) the path, P the text's parent's matrix; rise and the consumed path's role as the caller decided. */
  function onPathItem(t, guideFace, P, o) {
    o = o || {};
    const Pi = M.inv(P || M.I) || M.I, sub0 = longest(M.transformSubpaths(guideFace, Pi)); if (!sub0) return null;
    const sub = orient(sub0), b = M.bboxOf([sub]), R0 = [(b[0] + b[2]) / 2, (b[1] + b[3]) / 2];
    const pathD = dStr(M.transformSubpaths([sub], [1, 0, 0, 1, -R0[0], -R0[1]]));
    const G = guideOf(pathD); if (!G || !(G.L > 0)) return null;
    const scale = Math.sqrt(Math.abs(num(t.sx, 1) * num(t.sy, 1))) || 1;
    const n = Object.assign({}, t, { align: 'center', sizePt: r4(num(t.sizePt, 10) * scale), tx: r9(R0[0]), ty: r9(R0[1]), sx: 1, sy: 1, rot: 0, pathD, pathAt: r4(G.closed ? G.fwd.top() : 0.5), pathFlip: false, pathRisePt: r4(num(o.risePt, 0)) });
    if (o.role) n.pathRole = String(o.role); else delete n.pathRole;
    return n;
  }
  /** The words off their path, straight, their anchor where it was on the path; and the path back as a part (when it was taken in): { text, path | null }. */
  function offPathItems(t, geo) {
    const G = guideOf(t.pathD), W = geo.matrix(t);
    const a = G ? G.fwd.point(G.L * num(t.pathAt, 0.5)) : { x: 0, y: 0 }, target = M.ap(W, a.x, a.y);
    const n = Object.assign({}, t); for (const k of ['pathD', 'pathAt', 'pathFlip', 'pathRisePt', 'pathRole']) delete n[k];
    n.tx = 0; n.ty = 0; const p1 = M.ap(geo.matrix(n), 0, 0); n.tx = r9(target[0] - p1[0]); n.ty = r9(target[1] - p1[1]);
    let path = null;
    if (t.pathRole && G) {
      const role = ['cut', 'line', 'hatch', 'art'].includes(t.pathRole) ? t.pathRole : 'line';
      path = { kind: 'path', role, source: 'added', name: 'Path', stroke: role !== 'hatch', fill: role === 'hatch', lwPt: M.LW_LASER_PT, subpaths: M.transformSubpaths(G.sps, W), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 };
    }
    return { text: n, path };
  }

  /* ═══ 3 · GEOMETRY FOR THE LASER: rings, the metal, Clipper (vendor/clipper-6.4.2.js, as the editor's Weight) ═══ */
  const SC = 1e4;   // (Clipper works in integers: a ten-thousandth of a point)
  const toC = ring => ring.map(p => ({ X: Math.round(p[0] * SC), Y: Math.round(p[1] * SC) }));
  const fromC = path => path.map(q => [q.X / SC, q.Y / SC]);
  const areaMm2 = paths => { const C = Clip(); let a = 0; for (const p of paths) a += C.Clipper.Area(p); return a / (SC * SC) * MM * MM; };
  const boxOfC = paths => { let b = null; for (const p of paths) for (const q of p) { const x = q.X / SC, y = q.Y / SC; b = b ? [Math.min(b[0], x), Math.min(b[1], y), Math.max(b[2], x), Math.max(b[3], y)] : [x, y, x, y]; } return b; };
  function cRun(type, subj, clip, sRule, cRule) {
    const C = Clip(), cl = new C.Clipper(), out = new C.Paths(), R = r => (r === 'evenodd' ? C.PolyFillType.pftEvenOdd : C.PolyFillType.pftNonZero);
    cl.AddPaths(subj, C.PolyType.ptSubject, true); if (clip && clip.length) cl.AddPaths(clip, C.PolyType.ptClip, true);
    cl.Execute(type, out, R(sRule), R(cRule)); return out;
  }
  const ARC = 0.004 * PT * SC;   // (round corners within 4 µm)
  function cOffset(paths, d, round) {
    const C = Clip(), co = new C.ClipperOffset(2, ARC), out = new C.Paths();
    co.AddPaths(paths, round ? C.JoinType.jtRound : C.JoinType.jtMiter, C.EndType.etClosedPolygon); co.Execute(out, d); return out;
  }
  /** Open lines (closed rings walked once round) clipped by filled rings: the pieces inside (op 'in') or outside ('out'). */
  function clipLines(lines, rings, rule, op) {
    const C = Clip(), cl = new C.Clipper(), tree = new C.PolyTree();
    for (const l of lines) { const path = toC(l.closed ? l.pts.concat([l.pts[0]]) : l.pts); if (path.length >= 2) cl.AddPath(path, C.PolyType.ptSubject, false); }
    cl.AddPaths(rings, C.PolyType.ptClip, true);
    const R = rule === 'evenodd' ? C.PolyFillType.pftEvenOdd : C.PolyFillType.pftNonZero;
    cl.Execute(op === 'out' ? C.ClipType.ctDifference : C.ClipType.ctIntersection, tree, C.PolyFillType.pftNonZero, R);
    return C.Clipper.OpenPathsFromPolyTree(tree).map(fromC);
  }
  const lenOf = pls => { let s = 0; for (const p of pls) for (let i = 1; i < p.length; i++) s += Math.hypot(p[i][0] - p[i - 1][0], p[i][1] - p[i - 1][1]); return s; };
  /** Subpaths (already placed) as closed rings and open lines, flattened within tolMm (default 0.002 mm, CharmNestVector.flatten's own); a ring whose ends are 0.01 mm
   *  apart or less is closed (the laser closes it). */
  function ringsOf(sps, tolMm) {
    const V = Vec(), rings = [], lines = [], tol = (tolMm || 0.002) * PT;
    for (const sub of sps || []) {
      let f; try { f = V.flatten(sub, tol); } catch (e) { continue; }
      const p = f.points; if (p.length < 2) continue;
      const gap = Math.hypot(p[0][0] - p[p.length - 1][0], p[0][1] - p[p.length - 1][1]) * MM;
      const closed = f.closed || (p.length >= 3 && gap <= 0.01);
      if (closed && p.length >= 3) { rings.push(p); lines.push({ pts: p, closed: true }); } else lines.push({ pts: p, closed: false, gapMm: gap });
    }
    return { rings, lines };
  }
  const CHECK_TOL_MM = 0.005;   // (the check reads curves within 5 µm: a sixtieth of the thinnest bridge it looks for)
  const evenOddOf = it => !!(it.evenOdd || it.even || it.fillRule === 'evenodd');
  /** Every leaf of a face, placed: [{ it, W (leaf -> face), P, hidden (it or a group above it) }], bottom first. */
  function leavesOf(items, geo) {
    const out = [];
    const rec = (list, P, hid) => { for (const it of list || []) { const W = M.mul(P, geo.matrix(it)), h = hid || !!it.hidden; if (it.kind === 'group') rec(it.children, W, h); else out.push({ it, W, P, hidden: h }); } };
    rec(items, M.I, false);
    return out;
  }
  const placed = (l, geo) => M.transformSubpaths(geo.raw(l.it).subpaths, l.W);
  /** A generator run to its end at once (node, tests, and a command that cannot wait): its return value. */
  const drain = g => { let r = g.next(); while (!r.done) r = g.next(r.value); return r.value; };
  /** THE METAL of a face, step by step: every closed cut line, even-odd (the laser cuts them all; what lies inside an odd number of them is the piece), as Clipper paths
   *  cleaned within 3 µm: { metal, outer (the outlines, their holes filled), holes ([{ path, box, ring }]: ring, a hoop-sized round hole), open (cut lines not closed) };
   *  metal null when the face has no closed cut line. */
  function* metalSteps(leaves, geo) {
    const rings = [], open = [];
    for (const l of leaves) {
      if (l.it.role !== 'cut') continue;
      const r = ringsOf(placed(l, geo), CHECK_TOL_MM);
      rings.push(...r.rings);
      const gaps = r.lines.filter(x => !x.closed);
      if (gaps.length) open.push({ id: l.it.id, n: gaps.length, box: M.bboxOf(gaps.map(g => [['m', g.pts[0]], ['l', g.pts[g.pts.length - 1]]])) });
    }
    if (!rings.length) return { metal: null, open };
    yield;
    const C = Clip(), cl = new C.Clipper(), tree = new C.PolyTree();
    cl.AddPaths(rings.map(toC), C.PolyType.ptSubject, true); cl.Execute(C.ClipType.ctUnion, tree, C.PolyFillType.pftEvenOdd, C.PolyFillType.pftEvenOdd);
    const metal = C.Clipper.CleanPolygons(C.Clipper.PolyTreeToPaths(tree), 0.003 * PT * SC).filter(p => p.length >= 3), outs = tree.Childs().map(n => n.Contour());
    yield;
    const outer = cRun(C.ClipType.ctUnion, outs, null, 'nonzero', 'nonzero'), holePaths = cRun(C.ClipType.ctDifference, outer, metal, 'nonzero', 'nonzero');
    const holes = holePaths.filter(h => C.Clipper.Area(h) > 0).map(h => { const b = boxOfC([h]), w = b[2] - b[0], hh = b[3] - b[1]; return { path: h, pts: fromC(h), box: b, ring: Math.max(w, hh) <= 13.6 && Math.abs(w - hh) < 1 }; });
    return { metal, outer, outerPts: outer.map(fromC), holes, open };
  }
  /** Does any edge of these rings reach into the box (cross it or lie in it)? */
  function edgesIn(rings, b) {
    for (const r of rings) for (let i = 0, n = r.length; i < n; i++) {
      const a = r[i], c = r[(i + 1) % n];
      if (Math.max(a[0], c[0]) < b[0] || Math.min(a[0], c[0]) > b[2] || Math.max(a[1], c[1]) < b[1] || Math.min(a[1], c[1]) > b[3]) continue;
      if ((a[0] >= b[0] && a[0] <= b[2] && a[1] >= b[1] && a[1] <= b[3]) || (c[0] >= b[0] && c[0] <= b[2] && c[1] >= b[1] && c[1] <= b[3])) return true;
      // the segment against the box's four sides
      const side = (x1, y1, x2, y2) => { const d = (c[0] - a[0]) * (y2 - y1) - (c[1] - a[1]) * (x2 - x1); if (!d) return false; const t = ((x1 - a[0]) * (y2 - y1) - (y1 - a[1]) * (x2 - x1)) / d, u = ((x1 - a[0]) * (c[1] - a[1]) - (y1 - a[1]) * (c[0] - a[0])) / d; return t >= 0 && t <= 1 && u >= 0 && u <= 1; };
      if (side(b[0], b[1], b[2], b[1]) || side(b[2], b[1], b[2], b[3]) || side(b[0], b[3], b[2], b[3]) || side(b[0], b[1], b[0], b[3])) return true;
    }
    return false;
  }
  /** Is the point inside these rings (non-zero)? */
  function inside(rings, x, y) {
    let w = 0;
    for (const r of rings) for (let i = 0, n = r.length; i < n; i++) { const a = r[i], c = r[(i + 1) % n]; if (a[1] <= y) { if (c[1] > y && (c[0] - a[0]) * (y - a[1]) - (x - a[0]) * (c[1] - a[1]) > 0) w++; } else if (c[1] <= y && (c[0] - a[0]) * (y - a[1]) - (x - a[0]) * (c[1] - a[1]) < 0) w--; }
    return w !== 0;
  }
  const metalOf = (leaves, geo) => drain(metalSteps(leaves, geo));
  /** THIN PARTS of the metal, step by step (a morphological opening by a disc of minMm: what it leaves out is thinner than minMm): bridges and walls (the part touches the
   *  rest in two places or more: it holds something), and long thin points; the tip of a sharp corner is neither. [{ kind: 'bridge' | 'thin', widthMm, box, areaMm2 }] */
  function* thinSteps(metal, minMm) {
    if (!metal || !metal.length) return [];
    const C = Clip(), r = minMm / 2 * PT * SC, eps = 0.02 * PT * SC;
    const eroded = cOffset(metal, -r, true); yield;
    const opened = cOffset(eroded, r, true); yield;
    const cl = new C.Clipper(), tree = new C.PolyTree();
    cl.AddPaths(metal, C.PolyType.ptSubject, true); cl.AddPaths(opened, C.PolyType.ptClip, true);
    cl.Execute(C.ClipType.ctDifference, tree, C.PolyFillType.pftNonZero, C.PolyFillType.pftNonZero);
    const pieces = [];
    const visit = node => { for (const ch of node.Childs()) { if (!ch.IsHole()) pieces.push([ch.Contour()].concat(ch.Childs().filter(h => h.IsHole()).map(h => h.Contour()))); visit(ch); } };
    visit(tree);
    const out = [];
    for (const pc of pieces) {
      const a = areaMm2(pc); if (a < 0.002) continue;
      yield;
      const touch = cRun(C.ClipType.ctIntersection, cOffset(pc, eps, false), opened, 'nonzero', 'nonzero').filter(p => C.Clipper.Area(p) > 0 && Math.abs(C.Clipper.Area(p)) / (SC * SC) * MM * MM > 1e-5);
      const kind = touch.length >= 2 ? 'bridge' : a > minMm * 0.6 ? 'thin' : null;
      if (!kind) continue;
      // its width: twice the largest circle inside it (halving search, to a hundredth of the limit)
      let lo = 0, hi = r;
      for (let i = 0; i < 7; i++) { const mid = (lo + hi) / 2; if (cOffset(pc, -mid, true).some(p => C.Clipper.Area(p) > 0)) lo = mid; else hi = mid; }
      out.push({ kind, widthMm: 2 * lo / SC * MM, box: boxOfC([pc[0]]), areaMm2: a });
    }
    return out;
  }
  const thinParts = (metal, minMm) => drain(thinSteps(metal, minMm));

  /* ═══ 4 · CLIP TO THE CHARM (⌘7) and give it back (⌥⌘7) ═══ */
  /** What ⌘7 changes, step by step: { map: Map(id -> item), n, to: 'charm' | 'path', none?: why } (pure: items, the geometry, the selection's ids; o.metal: the face's metal
   *  when the check has it already). */
  function* clipSteps(items, geo, sel, o) {
    sel = (sel || []).map(String);
    const ix = M.indexTree(items, geo), all = leavesOf(items, geo);
    let mask = null, maskId = null, rule = 'evenodd', to = 'charm';
    if (sel.length >= 2) {
      // (Illustrator: the topmost of what is selected is the mask, when it is a closed path)
      const order = []; M.walk(items, it => { if (sel.includes(it.id)) order.push(it.id); });
      const te = ix.get(order[order.length - 1]);
      if (te && te.it.kind === 'path' && allClosed(te.it.subpaths)) {
        const W = M.mul(te.P, geo.matrix(te.it)), r = ringsOf(M.transformSubpaths(te.it.subpaths, W));
        if (r.rings.length) { mask = r.rings.map(toC); maskId = te.it.id; rule = evenOddOf(te.it) ? 'evenodd' : 'nonzero'; to = 'path'; }
      }
    }
    if (!mask) { const mt = o && o.metal ? o.metal : yield* metalSteps(all, geo); if (!mt.metal) return { map: new Map(), n: 0, none: 'This side has no closed cut line to clip to' }; mask = mt.metal; rule = 'nonzero'; }
    const under = new Set(); if (sel.length) for (const id of sel) { const e = ix.get(id); if (e) M.walk([e.it], it => under.add(it.id)); }
    const targets = all.filter(l => !l.hidden && l.it.role !== 'cut' && l.it.id !== maskId && (!sel.length || under.has(l.it.id)));
    const map = new Map(); let n = 0;
    for (const l of targets) { yield; const c = clipLeaf(l, geo, mask, rule); if (c) { map.set(l.it.id, c); n++; } }
    return { map, n, to };
  }
  const clipPlan = (items, geo, sel, o) => drain(clipSteps(items, geo, sel, o));
  /** One leaf cut back to the mask (Clipper paths, rule): the new leaf (a path in its parent, its original kept in clipOf), or null when nothing of it is outside. */
  function clipLeaf(l, geo, mask, rule) {
    const it = l.it, sps = placed(l, geo), Pi = M.inv(l.P) || M.I;
    const fill = M.paintFill(it), was = it.clipOf || it;
    const base = { id: it.id, kind: 'path', role: it.role, source: it.source, name: it.name || (it.kind === 'text' ? String(it.text || 'Text').split('\n')[0].slice(0, 40) : undefined),
      hidden: false, locked: !!it.locked, tx: 0, ty: 0, sx: 1, sy: 1, rot: 0, clipOf: was };
    for (const k of ['stroke', 'fill', 'lwPt', 'weightMm', 'layer']) if (it[k] !== undefined) base[k] = it[k];
    if (it.kind === 'text') { base.text = it.text; if (fill) { base.fill = true; base.stroke = false; } else { base.stroke = true; base.fill = false; } }
    if (fill) {
      const r = ringsOf(sps); if (!r.rings.length) return null;
      const A = r.rings.map(toC), ruleA = it.kind !== 'text' && evenOddOf(it) ? 'evenodd' : 'nonzero', C = Clip();
      const outside = cRun(C.ClipType.ctDifference, A, mask, ruleA, rule);
      if (areaMm2(outside) < 1e-4) return null;
      // A minus (A minus the mask), as CharmNestVector.boolean does it (a union and a difference: its two operations)
      const V = Vec(), D = V.boolean(r.rings, mask.map(fromC), 'difference', ruleA, rule);
      const kept = D.length ? V.boolean(r.rings, D.map(x => x.points), 'difference', ruleA, 'evenodd') : V.boolean(r.rings, [], 'union', ruleA, 'nonzero');
      if (!kept.length) return Object.assign({}, it, { hidden: true, clipOf: was });
      return Object.assign(base, { subpaths: M.transformSubpaths(kept.map(x => V.subpath(x.points)), Pi), evenOdd: true });
    }
    const r = ringsOf(sps); if (!r.lines.length) return null;
    const total = lenOf(r.lines.map(x => (x.closed ? x.pts.concat([x.pts[0]]) : x.pts))), inside = clipLines(r.lines, mask, rule, 'in');
    if (total - lenOf(inside) < 0.02 * PT) return null;
    if (!inside.length) return Object.assign({}, it, { hidden: true, clipOf: was });
    const subs = inside.filter(p => p.length >= 2).map(p => { const s = [['m', p[0]]].concat(p.slice(1).map(q => ['l', q])); const a = p[0], b = p[p.length - 1]; if (p.length > 2 && Math.hypot(a[0] - b[0], a[1] - b[1]) < 1e-4) { s.pop(); s.push(['h']); } return s; });
    return Object.assign(base, { subpaths: M.transformSubpaths(subs, Pi) });
  }
  /** What ⌥⌘7 changes: every clipped part (of the selection, or of the face) as it was before, moved, turned and scaled as the clipped part has been since. */
  function releasePlan(items, geo, sel) {
    sel = (sel || []).map(String);
    const ix = M.indexTree(items, geo), under = new Set();
    if (sel.length) for (const id of sel) { const e = ix.get(id); if (e) M.walk([e.it], it => under.add(it.id)); }
    const map = new Map();
    M.walk(items, it => {
      if (!it.clipOf || typeof it.clipOf !== 'object' || (sel.length && !under.has(it.id))) return;
      const own = geo.matrix(it);
      let back = Object.assign({}, it.clipOf, { id: it.id });
      if (!M.isI(own) && !it.hidden) back = M.pushDown(back, own, geo, M.I, true);
      map.set(it.id, back);
    });
    return map;
  }

  /* ═══ 5 · THE LASER CHECK ═══ */
  const LIMIT = { bridgeMm: 0.3, outsideMm2: 0.01, outsideMm: 0.05 };
  const minCapMm = () => { try { const s = typeof S !== 'undefined' && S && S.settings; const v = s && +s.engraveMinCapMm; return v > 0 ? v : 1.6; } catch (e) { return 1.6; } };   // (the engraver's smallest, Settings: charm-nest-engrave-preview.js reads it the same way)
  const KIND_ORDER = { open: 0, bridge: 1, thin: 2, outside: 3, hole: 4, cutout: 5, small: 6 };
  const nameOf = it => String((it.name != null && String(it.name).trim()) || (it.kind === 'text' ? String(it.text || '').split('\n')[0].trim() || 'Words' : { cut: 'Cut line', line: 'Line engraving', hatch: 'Hatched part', art: 'Artwork' }[it.role] || 'A part')).slice(0, 40);
  const mm2 = v => trim(v, v < 1 ? 2 : 1);
  /** Which cut lines, placed where: the metal is made again only when this changes (a face's cache holds the ids it gave its items). */
  function metalKeyOf(leaves, cache) {
    const ids = cache.ids || (cache.ids = new WeakMap());
    const idOf = it => { let v = ids.get(it); if (!v) { v = cache.seq = (cache.seq || 0) + 1; ids.set(it, v); } return v; };
    return leaves.filter(l => l.it.role === 'cut').map(l => idOf(l.it) + '@' + l.W.map(v => v.toFixed(6)).join(',')).join('|');
  }
  /** The check of one face, step by step (a generator: the page runs it in slices; checkFace runs it through). cache: { metalKey, metal, thin, leaf: WeakMap } kept between runs. */
  function* checkSteps(face, items, geo, cache, o) {
    o = o || {};
    const out = [], leaves = leavesOf(items, geo), C = Clip(), cuts = leaves.filter(l => l.it.role === 'cut'), key = metalKeyOf(leaves, cache);
    if (cache.metalKey !== key) { cache.metalKey = null; cache.metal = yield* metalSteps(leaves, geo); cache.metalKey = key; cache.thin = null; cache.leaf = new WeakMap(); yield; }
    const mt = cache.metal;
    for (const g of mt.open) out.push({ kind: 'open', ids: [g.id], box: g.box, text: `Cut line not closed: ${g.n === 1 ? 'one end is' : g.n + ' ends are'} open` });
    if (mt.metal) {
      if (!cache.thin) { cache.thin = yield* thinSteps(mt.metal, LIMIT.bridgeMm); yield; }
      const cutIds = cuts.map(l => l.it.id);
      for (const t of cache.thin) out.push({ kind: t.kind, ids: cutIds, box: t.box, text: t.kind === 'bridge' ? `Bridge ${trim(t.widthMm, 2)} mm wide: under ${LIMIT.bridgeMm} mm, it may break` : `A part ${trim(t.widthMm, 2)} mm thin (under ${LIMIT.bridgeMm} mm)` });
    }
    const cap = num(o.minCapMm, minCapMm());
    for (const l of leaves) {
      if (l.hidden || l.it.role === 'cut') continue;
      const k = key + '#' + l.W.map(v => v.toFixed(6)).join(',');
      let hit = cache.leaf.get(l.it);
      if (!hit || hit.k !== k) {
        const ps = [];
        if (l.it.kind === 'text' && String(l.it.text || '').trim()) {
          const capMm = capOf(geo.engine, l.it.fontKey, num(l.it.sizePt, 10)) * Math.sqrt(Math.abs(l.W[0] * l.W[3] - l.W[1] * l.W[2])) * MM;
          if (capMm < cap - 1e-6) ps.push({ kind: 'small', box: geo.drawn(l.it, l.P), text: `${nameOf(l.it)}: letters ${trim(capMm, 2)} mm tall, under the engraver's ${cap} mm` });
        }
        if (mt.metal) {
          const sps = placed(l, geo), r = ringsOf(sps, CHECK_TOL_MM), fill = M.paintFill(l.it), all = r.lines.map(x => x.pts), lb = M.bboxOf(all.map(p => [['m', p[0]]].concat(p.slice(1).map(q => ['l', q])))), p0 = all.length && all[0][0];
          // (the quick answer, exact: no edge of the outline reaches into the part's box, so one of its points says on which side all of it is; the same for each hole)
          const clear = lb && p0 && !edgesIn(mt.outerPts, lb) && inside(mt.outerPts, p0[0], p0[1]) && mt.holes.every(h => !edgesIn([h.pts], lb) && !inside([h.pts], p0[0], p0[1]));
          if (clear) { /* wholly on the metal */ } else if (fill && r.rings.length) {
            const A = r.rings.map(toC), ruleA = l.it.kind !== 'text' && evenOddOf(l.it) ? 'evenodd' : 'nonzero';
            const off = cRun(C.ClipType.ctDifference, A, mt.outer, ruleA, 'nonzero'), a = areaMm2(off);
            if (a > LIMIT.outsideMm2) ps.push({ kind: 'outside', box: boxOfC(off), text: `${nameOf(l.it)} runs past the cut line (${mm2(a)} mm²)` });
            for (const h of mt.holes) { const x = cRun(C.ClipType.ctIntersection, A, [h.path], ruleA, 'nonzero'), ax = areaMm2(x); if (ax > LIMIT.outsideMm2) ps.push({ kind: h.ring ? 'hole' : 'cutout', box: boxOfC(x), text: `${nameOf(l.it)} runs into ${h.ring ? 'the jump-ring hole' : 'a cut-out'}` }); }
          } else if (!fill && r.lines.length) {
            const off = clipLines(r.lines, mt.outer, 'nonzero', 'out'), len = lenOf(off) * MM;
            if (len > LIMIT.outsideMm) ps.push({ kind: 'outside', box: M.bboxOf(off.map(p => [['m', p[0]]].concat(p.slice(1).map(q => ['l', q])))), text: `${nameOf(l.it)} runs past the cut line (${trim(len, 2)} mm of line)` });
            for (const h of mt.holes) { const x = clipLines(r.lines, [h.path], 'nonzero', 'in'), lx = lenOf(x) * MM; if (lx > LIMIT.outsideMm) ps.push({ kind: h.ring ? 'hole' : 'cutout', box: h.box, text: `${nameOf(l.it)} runs into ${h.ring ? 'the jump-ring hole' : 'a cut-out'}` }); }
          }
        }
        hit = { k, ps }; cache.leaf.set(l.it, hit);
        yield;
      }
      for (const p of hit.ps) out.push(Object.assign({ ids: [l.it.id] }, p));
    }
    out.forEach(p => { p.face = face; });
    out.sort((a, b) => KIND_ORDER[a.kind] - KIND_ORDER[b.kind]);
    return out;
  }
  /** The whole check of one face at once (node, tests): the problems found. */
  const checkFace = (items, geo, o) => drain(checkSteps((o && o.face) || 'front', items, geo, {}, o));

  /* ═══ 6 · THE WORKSPACE: tools, commands, the bar, the check's list, Outline view (the page only) ═══ */
  const SV = (d, extra) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${extra || ''}>${d}</svg>`;
  const IC = {
    rrect: SV('<rect x="3.5" y="5.5" width="17" height="13" rx="4"/>'),
    star: SV('<path d="M12 3.2 14.6 8.9 20.6 9.5 16 13.6 17.3 19.6 12 16.5 6.7 19.6 8 13.6 3.4 9.5 9.4 8.9Z"/>'),
    heart: SV('<path d="M12 19.5S4.5 15 4.5 9.6A4.1 4.1 0 0 1 12 7.3a4.1 4.1 0 0 1 7.5 2.3c0 5.4-7.5 9.9-7.5 9.9Z"/>'),
    ring: SV('<circle cx="12" cy="12" r="8.5"/><circle cx="12" cy="12" r="4"/>'),
    bail: SV('<path d="M5.5 11.5h13v8.5h-13Z"/><circle cx="12" cy="7.3" r="3.6"/><circle cx="12" cy="7.3" r="1.5"/>'),
    clip: SV('<circle cx="12" cy="12" r="7.5"/><path d="M2.5 9h4M17.5 9h4M2.5 15h4M17.5 15h4" stroke-dasharray="1.6 1.6"/><path d="M6 9h12M6 15h12"/>'),
    unclip: SV('<circle cx="12" cy="12" r="7.5" stroke-dasharray="2.4 2"/><path d="M2.5 9h19M2.5 15h19"/>'),
    onPath: SV('<path d="M4 17.5C7 9 17 9 20 17.5" stroke-dasharray="2 2"/><path d="M8.2 12.6 9.7 8.6l1.6 3.8M8.6 11.4h2.3M13.3 8.3v4.3h1.7a1.1 1.1 0 0 0 0-2.2h-1.7"/>'),
    offPath: SV('<path d="M4 18.5h16" stroke-dasharray="2 2"/><path d="M7 15 9 9l2 6M7.7 13h2.6M13.5 9v6h2.1a1.6 1.6 0 0 0 0-3.2h-2.1"/>'),
    outline: SV('<rect x="4" y="5" width="9" height="9" rx=".6"/><circle cx="14.5" cy="14.5" r="5"/>'),
    check: SV('<path d="M12 3.5 21 19.5H3Z"/><path d="M12 9.5v4.5"/><path d="M12 16.6v.2"/>'),
    flip: SV('<path d="M4 9a8 8 0 0 1 16 0"/><path d="M20 15a8 8 0 0 1-16 0"/><path d="m17.5 6.5 2.5 2.5 2.5-2.5"/><path d="m6.5 17.5-2.5-2.5-2.5 2.5"/>')
  };
  const CSS = `
.cx-bar{ display:inline-flex; align-items:center; gap:6px; flex:0 0 auto; flex-wrap:wrap; row-gap:6px; }
.cx-bar[hidden]{ display:none !important; }
.cx-bar .ce-num > b{ font-size:9px; font-weight:800; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); white-space:nowrap; }
.cx-bar .ce-num .cx-va{ font-size:11px; letter-spacing:-.02em; text-transform:none; text-decoration:underline; text-underline-offset:2px; color:var(--ink); opacity:.7; }
.cx-bar .ce-num input{ width:38px; }
.cx-bar .ce-prop.is-on{ border-color:var(--ink); background:var(--band); }
.cx-bar .ce-prop svg{ width:16px; height:16px; }
.cx-chk{ gap:7px; letter-spacing:.1em; color:#9b2c22; border-color:#e7c3bd; background:#fdf6f4; }
.cx-chk i{ width:7px; height:7px; border-radius:50%; background:#d4291f; flex:none; }
.cx-chk.is-clear{ color:var(--muted); border-color:var(--hair); background:#fff; } .cx-chk.is-clear i{ background:#3d8b5a; }
.cx-badge{ font-style:normal; margin-left:2px; padding:2px 6px; background:rgba(255,255,255,.22); font-size:9px; letter-spacing:.12em; font-weight:700; white-space:nowrap; }
.ce-menu.cx-list{ min-width:300px; max-width:min(420px,calc(100vw - 32px)); }
.cx-row{ display:flex; align-items:center; gap:9px; padding:6px 6px 6px 9px; font-size:12px; line-height:1.4; color:var(--ink); }
.cx-row + .cx-row{ border-top:1px solid var(--hair); }
.cx-row > i{ width:7px; height:7px; border-radius:50%; flex:none; background:#d4291f; }
.cx-row > i.cx-k--small,.cx-row > i.cx-k--thin{ background:#c9871f; }
.cx-row > span{ flex:1 1 auto; min-width:0; }
.cx-row > span em{ font-style:normal; color:var(--muted); font-size:10px; letter-spacing:.08em; text-transform:uppercase; margin-right:5px; }
.cx-show{ flex:none; border:1px solid var(--hair); background:#fff; cursor:pointer; padding:4px 9px; font:inherit; font-size:9.5px; font-weight:700; letter-spacing:.14em; text-transform:uppercase; color:var(--ink); }
.cx-show:hover{ background:var(--hov); border-color:var(--hov-line); }
.ce-ui .cx-mark{ fill:rgba(212,41,31,.06); stroke:#d4291f; stroke-width:1.5; stroke-dasharray:5 3; }
.ce-ui .cx-guide{ fill:none; stroke:var(--gold); stroke-width:1; stroke-dasharray:3 3; }
.ce-ui .cx-anchor{ fill:#fff; stroke:var(--gold); stroke-width:1.5; }
.ce-root.cx-outline .ce-art .ce-p{ fill:none !important; stroke-width:1px !important; }
.ce-root.cx-outline .ce-art .ce-p--cut{ stroke:var(--c-cut) !important; } .ce-root.cx-outline .ce-art .ce-p--line{ stroke:var(--c-line) !important; }
.ce-root.cx-outline .ce-art .ce-p--hatch{ stroke:var(--c-hatch) !important; } .ce-root.cx-outline .ce-art .ce-p--art{ stroke:var(--c-art) !important; }
`;
  function injectCss() {
    if (!doc || doc.getElementById('cxCss')) return;
    const s = doc.createElement('style'); s.id = 'cxCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }

  /* one open workspace's own state (the editor opens one at a time) */
  let W = null;
  const sel = ctx => ctx.selection().map(id => ctx.entry(id)).filter(Boolean);
  const leavesUnder = es => { const out = []; for (const e of es) M.walk([e.it], it => { if (it.kind !== 'group') out.push(it); }); return out; };
  const sizeText = b => `${((b[2] - b[0]) * MM).toFixed(2)} × ${((b[3] - b[1]) * MM).toFixed(2)} mm`;
  /** The editor's size chip (u-dim), for an editor without ctx.live. */
  function chipSvg(ctx, b, text) {
    const a = ctx.stageOf(b[0], b[1]), c = ctx.stageOf(b[2], b[3]), sw = ctx.stage.clientWidth, sh = ctx.stage.clientHeight;
    const w = text.length * 5.9 + 12, x = clamp((a[0] + c[0]) / 2 - w / 2, 4, Math.max(4, sw - w - 4)); let y = c[1] + 14; if (y + 18 > sh) y = a[1] - 30;
    return `<g class="u-dim"><rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${w.toFixed(1)}" height="16"/><text x="${(x + 6).toFixed(1)}" y="${(y + 11.5).toFixed(1)}">${esc(text)}</text></g>`;
  }

  /* ── 6a · the blanks on the Shapes flyout ── */
  function presetTool(kind) {
    const d = SHAPES[kind];
    return {
      id: 'shape-' + kind,
      title: d.title, flyout: 'shapes', icon: IC[kind], cursor: 'crosshair', tip: d.hint.split(' · ')[0], hint: d.hint,
      onArm(ctx) { syncBar(ctx); }, onDisarm(ctx) { this.g = null; syncBar(ctx); },
      onDown(ctx, ev, p) { if (!p || ev.button) return; this.g = { a: [p.x, p.y], cx: ev.clientX, cy: ev.clientY, id: null, live: false, box: null }; },
      onMove(ctx, ev, p) {
        const g = this.g; if (!g || !p || !(ev.buttons & 1)) return;
        if (!g.live && Math.hypot(ev.clientX - g.cx, ev.clientY - g.cy) < 3) return;
        g.live = true;
        let q = [p.x, p.y];
        if (typeof ctx.snap === 'function') { try { const s = ctx.snap(p.x, p.y, ev); if (Array.isArray(s) && fin(s[0]) && fin(s[1])) q = s; } catch (e) { /* drawn where the hand is */ } }
        drawBlank(ctx, g, kind, dragBox(g.a, q, ev.shiftKey ? d.aspect(opt) : 0, ev.altKey || ev.ctrlKey || ev.metaKey));
      },
      onUp(ctx, ev, p) {
        const g = this.g; this.g = null; if (!g) return;
        if (!g.live && p) { const w = clamp(num(opt.w, 6), 0.5, 200) * PT, h = w / (d.aspect(opt) || 1); drawBlank(ctx, g, kind, [p.x - w / 2, p.y - h / 2, p.x + w / 2, p.y + h / 2]); }
        if (typeof ctx.live === 'function') { try { ctx.live(null); } catch (e) { /* fine */ } }
        if (!g.id) return;
        if (g.box) { opt.w = r4((g.box[2] - g.box[0]) * MM); keepOpt(); }
        ctx.commit(d.title); log('shapePreset', { kind, face: ctx.face(), wMm: r4((g.box[2] - g.box[0]) * MM), hMm: r4((g.box[3] - g.box[1]) * MM) });
        ctx.setTool('select');
      },
      overlay(ctx) { const g = this.g; return g && g.box && typeof ctx.live !== 'function' ? chipSvg(ctx, g.box, sizeText(g.box)) : ''; }
    };
  }
  function drawBlank(ctx, g, kind, box) {
    const it = shapeItem(kind, box, opt);
    g.box = box;
    if (!g.id) g.id = ctx.addItems([it])[0];
    else { const e = ctx.entry(g.id); if (e) ctx.replaceItems({ [g.id]: Object.assign({}, e.it, { subpaths: it.subpaths, shape: it.shape }) }); }
    if (typeof ctx.live === 'function') { try { ctx.live({ box, text: sizeText(box) }); } catch (e) { /* the chip is a courtesy */ } }
  }
  /** THE JUMP RING on the top edge of what is selected (the shop's ring, as a cut): the vertical through the selection's middle meets its top at y, and the ring's centre
   *  sits so that edge runs through the middle of the ring's wall. A selection that is one closed cut line takes the ring INTO it (its outside united with the outline,
   *  CharmNestVector.boolean; its hole a cut of its own), so the laser cuts one piece. */
  function bailOnTop(ctx, k) {
    const es = sel(ctx); if (!es.length) return false;
    k = ringKey(k || opt.ring);
    const geo = ctx.geo, R = RING[k];
    let bb = null; const outs = [];
    for (const e of es) { bb = M.union(bb, geo.drawn(e.it, e.P)); outs.push(...M.outlinesOf(e.it, geo, e.P)); }
    if (!bb) return false;
    const cx = (bb[0] + bb[2]) / 2, top = topAt(outs, cx), y = top == null ? bb[1] : top, cy = y - (R.outPt + R.holePt) / 4;
    const one = es.length === 1 && es[0].it.kind === 'path' && es[0].it.role === 'cut' && allClosed(es[0].it.subpaths) ? es[0] : null;
    if (one) {
      const V = Vec(), r = ringsOf(M.outlinesOf(one.it, geo, one.P)), disc = ringsOf(M.ellipseSP(cx, cy, R.outPt / 2, R.outPt / 2)).rings;
      const u = V.boolean(r.rings, disc, 'union', evenOddOf(one.it) ? 'evenodd' : 'nonzero', 'nonzero');
      if (u.length) {
        const Pi = M.inv(one.P) || M.I, welded = Object.assign({}, one.it, { subpaths: M.transformSubpaths(u.map(x => V.subpath(x.points)), Pi), tx: 0, ty: 0, sx: 1, sy: 1, rot: 0 });
        delete welded.shape;
        ctx.replaceItems({ [one.it.id]: welded });
        const hole = ctx.addItems([{ kind: 'path', role: 'cut', name: 'Jump-ring hole', stroke: true, fill: false, lwPt: num(one.it.lwPt, M.LW_LASER_PT), subpaths: M.ellipseSP(cx, cy, R.holePt / 2, R.holePt / 2) }], { select: false })[0];
        ctx.select([one.it.id, hole]);
        ctx.say(`${R.name} welded into the cut line on top: ${trim(R.outPt * MM, 2)} mm outside, ${trim(R.holePt * MM, 2)} mm hole`, 3200);
        log('jumpRing', { face: ctx.face(), ring: k, welded: true });
        return true;
      }
    }
    const sps = bailSP(cx, cy, k);
    ctx.addItems([{ kind: 'path', role: 'cut', name: 'Jump ring', stroke: true, fill: false, evenOdd: true, lwPt: M.LW_LASER_PT, subpaths: sps, shape: shapeRec('bail', { ring: k }, sps) }]);
    ctx.say(`${R.name} on top, as a cut: unite it with the outline to cut one piece`, 3200);
    log('jumpRing', { face: ctx.face(), ring: k, welded: false });
    return true;
  }
  const bailTool = {
    id: 'shape-bail', title: 'Jump ring', flyout: 'shapes', icon: IC.bail, cursor: 'crosshair', tip: 'The shop\'s ring, on top of the selection',
    hint: 'Click to place the jump ring (the shop\'s standard ring, cut) · with something selected it goes on its top edge',
    onArm(ctx) {
      syncBar(ctx);
      if (ctx.selection().length) Promise.resolve().then(() => { if (ctx.tool() !== 'shape-bail') return; if (bailOnTop(ctx)) ctx.commit('Jump ring'); ctx.setTool('select'); });
    },
    onDisarm(ctx) { syncBar(ctx); },
    onDown() {}, onMove() {},
    onUp(ctx, ev, p) {
      if (!p) return;
      const k = ringKey(opt.ring), sps = bailSP(p.x, p.y, k);
      ctx.addItems([{ kind: 'path', role: 'cut', name: 'Jump ring', stroke: true, fill: false, evenOdd: true, lwPt: M.LW_LASER_PT, subpaths: sps, shape: shapeRec('bail', { ring: k }, sps) }]);
      ctx.commit('Jump ring'); log('jumpRing', { face: ctx.face(), ring: k, welded: false }); ctx.setTool('select');
    }
  };

  /* ── 6b · the bar: the blank's numbers, letter spacing, a text's place on its path ── */
  function buildBar(ctx) {
    const props = ctx.root.querySelector('.ce-props'); if (!props) return null;
    const el = doc.createElement('span'); el.className = 'cx-bar'; el.hidden = true;
    const f = (id, label, unit, title, cls) => `<label class="ce-num${cls ? ' ' + cls : ''}" data-cx-f="${id}" title="${esc(title)}" hidden>${label}<input data-cx="${id}" inputmode="decimal" autocomplete="off" spellcheck="false" aria-label="${esc(title)}">${unit ? `<i>${unit}</i>` : ''}</label>`;
    el.innerHTML = f('corner', '<b>Corner</b>', 'mm', 'Corner radius, mm · ↑ ↓ 0.1 mm, Shift 1 mm')
      + f('points', '<b>Points</b>', '', 'How many points the star has · ↑ ↓')
      + f('inner', '<b>Depth</b>', '%', 'The star\'s notches, % of its points (small: deep) · ↑ ↓ 1 %, Shift 5 %')
      + f('wall', '<b>Wall</b>', 'mm', 'The ring\'s wall, mm · ↑ ↓ 0.1 mm, Shift 1 mm')
      + `<div class="ce-seg" data-cx-f="ring" hidden title="The jump ring: the shop's standard ring or the Huggie's"><button type="button" data-cx-ring="std">Standard</button><button type="button" data-cx-ring="huggie">Huggie</button></div>`
      + f('track', '<b class="cx-va">VA</b>', '', 'Letter spacing (tracking), thousandths of an em as in Illustrator: 0 the font\'s own, −50 tighter, 100 looser · ↑ ↓ 10, Shift 50')
      + f('along', '<b>Along</b>', 'mm', 'Where the words sit on their path: mm from its start · ↑ ↓ 0.1 mm, Shift 1 mm')
      + f('rise', '<b>Rise</b>', 'mm', 'The words lifted off their path (minus: below it), mm · ↑ ↓ 0.05 mm, Shift 0.5 mm')
      + `<button class="ce-prop" type="button" data-cx-act="flip" hidden title="Flip: the words to the other side of their path (round a charm: from the top to the bottom, reading the right way)" aria-label="Flip the words">${IC.flip}</button>`;
    props.insertBefore(el, props.querySelector('.ce-xbar'));
    const STEP = { corner: [0.1, 1], points: [1, 1], inner: [1, 5], wall: [0.1, 1], track: [10, 50], along: [0.1, 1], rise: [0.05, 0.5] };
    for (const inp of el.querySelectorAll('input[data-cx]')) {
      const id = inp.dataset.cx;
      inp.addEventListener('focus', () => { inp.__v0 = inp.value; try { inp.select(); } catch (e) { /* fine */ } });
      inp.addEventListener('blur', () => { if (inp.__revert) { inp.__revert = false; syncBar(ctx, true); return; } if (inp.value !== inp.__v0) setField(ctx, id, numIn(inp.value), false); ctx.commit(); });
      // (the editor hears every key first; a field of an extension gets its keys through __ceKey: Enter, Esc and the arrows)
      inp.__ceKey = e => {
        if (e.key === 'Enter') { e.preventDefault(); inp.blur(); return; }
        if (e.key === 'Escape') { e.preventDefault(); inp.__revert = true; inp.blur(); return; }
        if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
          e.preventDefault();
          const st = STEP[id][e.shiftKey ? 1 : 0], v0 = numIn(inp.value), v = Math.round(((fin(v0) ? v0 : 0) + (e.key === 'ArrowUp' ? st : -st)) * 1000) / 1000;
          inp.value = trim(v, 3); inp.__v0 = inp.value; setField(ctx, id, v, true);
        }
      };
    }
    el.addEventListener('click', e => {
      const rb = e.target.closest('[data-cx-ring]'); if (rb) { setField(ctx, 'ring', rb.dataset.cxRing, false); ctx.commit('Jump ring'); return; }
      if (e.target.closest('[data-cx-act="flip"]')) { flipSel(ctx); ctx.commit('Flip the words'); }
    });
    return el;
  }
  /** What the bar speaks to now: { shape: { kind, item? }, texts: [items], path: item | null }. */
  function barTarget(ctx) {
    const es = sel(ctx), leaves = leavesUnder(es).filter(l => !l.locked);
    const one = es.length === 1 ? es[0].it : null, sh = one && shapeOf(one);
    // (a tool of this file in hand, with nothing selected: its numbers are the next blank's)
    const tool = String(ctx.tool() || ''), armed = !es.length && /^shape-/.test(tool) ? tool.slice(6) : null;
    return { shape: sh ? { kind: sh.kind, item: one, rec: sh } : armed ? { kind: armed } : null, texts: leaves.filter(l => l.kind === 'text'), path: one && isBent(one) && !one.locked ? one : null };
  }
  const PARAMS = { rrect: ['corner'], star: ['points', 'inner'], ring: ['wall'], bail: ['ring'], heart: [] };
  function syncBar(ctx, force) {
    const el = W && W.bar; if (!el || !el.isConnected) return;
    const t = barTarget(ctx), show = new Set();
    if (t.shape) for (const p of PARAMS[t.shape.kind] || []) show.add(p);
    if (t.texts.length) show.add('track');
    if (t.path) { show.add('along'); show.add('rise'); show.add('flip'); }
    const scaleOf = it => Math.sqrt(Math.abs(num(it.sx, 1) * num(it.sy, 1))) || 1;
    const val = id => {
      if (['corner', 'points', 'inner', 'wall', 'ring'].includes(id)) { const src = t.shape.rec || opt; return src[id] != null ? src[id] : opt[id]; }
      if (id === 'track') { const v = num(t.texts[0].tracking, 0); return t.texts.every(x => num(x.tracking, 0) === v) ? v : ''; }
      const G = guideOf(t.path.pathD), s = scaleOf(t.path);
      if (id === 'along') return G ? r4(num(t.path.pathAt, 0.5) * G.L * s * MM) : '';
      if (id === 'rise') return r4(num(t.path.pathRisePt, 0) * s * MM);
      return '';
    };
    for (const f of el.querySelectorAll('[data-cx-f]')) {
      const id = f.dataset.cxF, on = show.has(id); f.hidden = !on; if (!on) continue;
      if (id === 'ring') { const v = ringKey(val('ring')); f.querySelectorAll('[data-cx-ring]').forEach(b => b.classList.toggle('is-on', b.dataset.cxRing === v)); continue; }
      const inp = f.querySelector('input'); if (inp && (force || doc.activeElement !== inp)) { const v = val(id); inp.value = v === '' ? '' : trim(v, id === 'track' || id === 'points' || id === 'inner' ? 0 : 2); }
    }
    const fb = el.querySelector('[data-cx-act="flip"]'); fb.hidden = !show.has('flip'); if (t.path) fb.classList.toggle('is-on', !!t.path.pathFlip);
    el.hidden = !show.size;
  }
  /** A number typed or stepped on the bar, to what it speaks to (the next blank, the selected blank, the words, their place on the path). */
  function setField(ctx, id, v, live) {
    const t = barTarget(ctx), geo = ctx.geo;
    if (id === 'ring') v = ringKey(v); else if (!fin(v)) { syncBar(ctx, true); return; }
    if (id === 'corner' || id === 'wall') v = clamp(v, 0, 100); if (id === 'points') v = clamp(Math.round(v), 3, 40); if (id === 'inner') v = clamp(Math.round(v), 5, 98);
    if (['corner', 'points', 'inner', 'wall', 'ring'].includes(id)) {
      if (!t.shape) return;
      if (!t.shape.item) { opt[id] = v; keepOpt(); syncBar(ctx, true); return; }
      const n = reshape(t.shape.item, { [id]: v }, geo); if (n) ctx.replaceItems({ [n.id]: n });
      opt[id] = v; keepOpt();
    } else if (id === 'track') {
      v = clamp(Math.round(v), -500, 2000);
      const map = {}; for (const it of t.texts) if (num(it.tracking, 0) !== v) { const n = M.retext(it, { tracking: v }, geo); if (!v) delete n.tracking; map[it.id] = n; }
      if (Object.keys(map).length) ctx.replaceItems(map);
    } else if (t.path && (id === 'along' || id === 'rise')) {
      const it = t.path, s = Math.sqrt(Math.abs(num(it.sx, 1) * num(it.sy, 1))) || 1, G = guideOf(it.pathD); if (!G) return;
      const patch = id === 'along' ? { pathAt: r4(G.closed ? (((v * PT / s / G.L) % 1) + 1) % 1 : clamp(v * PT / s / G.L, 0, 1)) } : { pathRisePt: r4(v * PT / s) };
      ctx.replaceItems({ [it.id]: M.retext(it, patch, geo) });
    }
    if (!live) syncBar(ctx, true);
  }
  /** FLIP: the words to the other side of their path. Round a charm, words at its top go to its bottom and keep their band (the same distance in from the edge), reading
   *  the right way, as a coin's words do; elsewhere they cross the path where they are. */
  function flipPatch(it, geo) {
    const G = guideOf(it.pathD); if (!G) return null;
    const at = num(it.pathAt, 0.5), top = G.fwd.top(), bottom = G.fwd.bottom(), near = (a, b) => Math.min(Math.abs(a - b), 1 - Math.abs(a - b)) < 0.03;
    const patch = { pathFlip: !it.pathFlip };
    if (G.closed && (near(at, top) || near(at, bottom))) {
      // (the letters' ink from y0 to y1 off the baseline lies between rise − y1 and rise − y0 off the path; on the other side, turned, the same band needs rise' = y0 + y1 − rise)
      const b = capBand(geo.engine, it.fontKey, num(it.sizePt, 10));
      patch.pathAt = r4(near(at, top) ? bottom : top);
      patch.pathRisePt = r4(b[0] + b[1] - num(it.pathRisePt, 0));
    }
    return patch;
  }
  function flipSel(ctx) {
    const t = barTarget(ctx), it = t.path; if (!it) return;
    const patch = flipPatch(it, ctx.geo); if (!patch) return;
    ctx.replaceItems({ [it.id]: M.retext(it, patch, ctx.geo) });
    log('textOnPath', { face: ctx.face(), flip: patch.pathFlip });
  }

  /* ── 6c · commands ── */
  const textAndPath = ctx => {
    const es = sel(ctx), texts = es.filter(e => e.it.kind === 'text' && !e.it.locked), paths = es.filter(e => e.it.kind === 'path');
    return texts.length === 1 && es.length === texts.length + paths.length && paths.length <= 1 ? { t: texts[0], g: paths[0] || null } : null;
  };
  function onPathRun(ctx) {
    const tp = textAndPath(ctx); if (!tp) return;
    const t = tp.t.it, geo = ctx.geo, size = num(t.sizePt, 10) * (Math.sqrt(Math.abs(num(t.sx, 1) * num(t.sy, 1))) || 1), cap = capOf(geo.engine, t.fontKey, size);
    let guide, rise = 0, role = null, said;
    if (tp.g) {
      guide = M.outlinesOf(tp.g.it, geo, tp.g.P);
      // the charm's own cut line stays (the words go inside it, a capital and 0.6 mm in); any other path becomes the words' path, as Illustrator's does
      if (tp.g.it.role === 'cut') { rise = -(cap + 0.6 * PT); said = 'Words along the cut line, inside it'; } else { role = tp.g.it.role; said = 'Words along the path (the path is theirs now: Words off the path gives it back)'; }
    } else {
      const f = ctx.frame(), R = Math.min(f.wPt, f.hPt) / 2 - 0.8 * PT - cap;
      if (!(R > cap)) { ctx.say('This charm is too small for words round it'); return; }
      guide = M.ellipseSP(f.x0 + f.wPt / 2, f.y0 + f.hPt / 2, R, R); said = 'Words round the charm, 0.8 mm in from its edge';
    }
    const n = onPathItem(t, guide, tp.t.P, { risePt: rise, role }); if (!n) { ctx.say('That path is too short for words'); return; }
    ctx.replaceItems({ [t.id]: n }); if (tp.g && role) ctx.removeItems([tp.g.it.id]); ctx.select([t.id]);
    ctx.say(said + ' · Along, Rise and Flip are on the bar', 3600);
    log('textOnPath', { face: ctx.face(), text: String(t.text || '').slice(0, 60), along: tp.g ? (tp.g.it.role === 'cut' ? 'cut' : 'path') : 'charm' });
  }
  function offPathRun(ctx) {
    const es = sel(ctx).filter(e => isBent(e.it)); if (!es.length) return;
    const ids = [];
    for (const e of es) {
      const r = offPathItems(e.it, ctx.geo);
      ctx.replaceItems({ [e.it.id]: r.text }); ids.push(e.it.id);
      if (r.path) { const sps = M.transformSubpaths(r.path.subpaths, e.P); ids.push(...ctx.addItems([Object.assign(r.path, { subpaths: sps })], { select: false, parent: e.parent ? e.parent.id : undefined })); }
    }
    ctx.select(ids); log('textOffPath', { face: ctx.face(), n: es.length });
  }
  function hasCut(ctx) { let any = false; M.walk(ctx.items(), it => { if (!any && it.kind !== 'group' && it.role === 'cut') any = true; }); return any; }
  /** ⌘7: the plan made in slices of 10 ms at most (the face's metal from the check when it has it), then put in at once as one step. */
  function clipRun(ctx) {
    const items0 = ctx.items(), c = W && W.cache[ctx.face()], leaves = c && c.metal ? leavesOf(items0, ctx.geo) : null;
    const metal = c && c.metal && c.metalKey === metalKeyOf(leaves, c) ? c.metal : null;
    const g = clipSteps(items0, ctx.geo, ctx.selection(), { metal });
    return new Promise(done => {
      let slices = 0;
      const step = () => {
        if (ctx.items() !== items0) { ctx.say('The charm changed while it was being clipped: ⌘7 again'); done(); return; }
        const t0 = now(); let r;
        do r = g.next(); while (!r.done && now() - t0 < 10);
        if (!r.done) { if (++slices === 2) ctx.say('Clipping…', 0); setTimeout(step, 0); return; }
        clipPut(ctx, r.value); done();
      };
      step();
    });
  }
  function clipPut(ctx, plan) {
    if (plan.none) { ctx.say(plan.none); return; }
    if (!plan.n) { ctx.say(plan.to === 'path' ? 'Nothing selected runs past that path' : 'Nothing runs past the cut line: nothing to clip'); return; }
    ctx.replaceItems(plan.map);
    ctx.say(`${plan.n === 1 ? 'One part' : plan.n + ' parts'} clipped to the ${plan.to === 'path' ? 'path' : 'cut line'} · ⌥⌘7 gives ${plan.n === 1 ? 'it' : 'them'} back`, 3200);
    log('clipToCharm', { face: ctx.face(), n: plan.n, to: plan.to });
  }
  function releaseRun(ctx) {
    const map = releasePlan(ctx.items(), ctx.geo, ctx.selection()); if (!map.size) { ctx.say('Nothing here is clipped'); return; }
    ctx.replaceItems(map); ctx.say(`${map.size === 1 ? 'One part' : map.size + ' parts'} as before the clip`, 2400);
    log('clipRelease', { face: ctx.face(), n: map.size });
  }
  const anyClipped = ctx => { const es = sel(ctx); let any = false; M.walk(es.length ? es.map(e => e.it) : ctx.items(), it => { if (it.clipOf && typeof it.clipOf === 'object') any = true; }); return any; };
  function outlineToggle(ctx) {
    const on = !ctx.root.classList.contains('cx-outline');
    ctx.root.classList.toggle('cx-outline', on); if (W) W.outline = on;
    ctx.say(on ? 'Outline view: every part as a thin line, no fill (⌘Y again: back)' : 'Preview: as it will be engraved', 2000);
  }

  /* ── 6d · the laser check: run in slices after every change, Save's "N to check", the list ── */
  function scheduleCheck(ctx, ms) {
    if (!W) return;
    clearTimeout(W.checkT); W.gen++;
    const my = W.gen;
    W.checkT = setTimeout(() => runCheck(ctx, my), ms == null ? 260 : ms);
  }
  function runCheck(ctx, my) {
    if (!W || my !== W.gen || !CE.current) return;
    const faces = ['front', 'back'].filter(f => f === ctx.face() || (CE.current.items(f) || []).length);
    const jobs = faces.map(f => { const c = W.cache[f] || (W.cache[f] = {}); return { f, gen: checkSteps(f, CE.current.items(f), ctx.geo, c, {}) }; });
    const found = [];
    const slice = () => {
      if (!W || my !== W.gen) return;
      const t0 = now();
      while (jobs.length && now() - t0 < 8) {
        const r = jobs[0].gen.next();
        if (r.done) { found.push(...r.value); jobs.shift(); }
      }
      if (jobs.length) { W.checkT = setTimeout(slice, 0); return; }
      W.problems = found; W.checked = true; showCheck(ctx);
    };
    slice();
  }
  function showCheck(ctx) {
    if (!W) return;
    const n = W.problems.length, chip = W.chip, badge = W.badge;
    if (chip) { chip.hidden = false; chip.classList.toggle('is-clear', !n); chip.querySelector('span').textContent = n ? `${n} to check` : 'Laser check: clear'; chip.title = n ? 'What to check before the laser: click for the list (it never stops a save)' : 'Nothing to check for the laser'; }
    if (badge) { badge.hidden = !n; badge.textContent = `${n} to check`; const sb = badge.parentNode; if (sb) sb.title = n ? `Saves now; ${n} thing${n === 1 ? '' : 's'} to check for the laser` : ''; }
    if (W.list && !W.list.hidden) fillList(ctx);
  }
  const KIND_WORD = { open: 'Cut', bridge: 'Bridge', thin: 'Thin', outside: 'Off metal', hole: 'Ring hole', cutout: 'Cut-out', small: 'Small' };
  function fillList(ctx) {
    const P = W.problems;
    W.list.innerHTML = `<div class="ce-menu__head">For the laser · ${P.length ? P.length + ' to check' : 'nothing to check'}</div>`
      + P.map((p, i) => `<div class="cx-row" data-cx-row="${i}"><i class="cx-k--${p.kind}"></i><span><em>${esc(KIND_WORD[p.kind] || '')}${p.face !== ctx.face() ? ' · ' + esc(p.face) : ''}</em>${esc(p.text)}</span>${p.box ? `<button class="cx-show" type="button" data-cx-show="${i}">Show</button>` : ''}</div>`).join('')
      + `<div class="ce-menu__note">Bridges under <b>${LIMIT.bridgeMm} mm</b>, open cut lines, engraving off the metal or in the ring hole, letters under <b>${minCapMm()} mm</b>. Nothing here stops a save.</div>`;
  }
  function placeList(ctx) {
    const l = W.list, r = W.chip.getBoundingClientRect(), vw = doc.documentElement.clientWidth || root.innerWidth;
    l.hidden = false; l.style.visibility = 'hidden'; l.style.left = '0px'; l.style.top = '0px';
    const mr = l.getBoundingClientRect();
    l.style.left = clamp(r.left, 8, Math.max(8, vw - 8 - mr.width)) + 'px'; l.style.top = Math.max(8, r.top - 6 - mr.height) + 'px'; l.style.visibility = ''; l.classList.add('is-in');
  }
  function openList(ctx) {
    if (!W || !W.list) return;
    if (!W.list.hidden) { W.list.hidden = true; return; }
    if (!W.checked) runCheckNow(ctx);
    fillList(ctx); placeList(ctx);
  }
  function runCheckNow(ctx) {
    const faces = ['front', 'back'].filter(f => f === ctx.face() || (CE.current.items(f) || []).length), found = [];
    for (const f of faces) { const g = checkSteps(f, CE.current.items(f), ctx.geo, W.cache[f] || (W.cache[f] = {}), {}); let r = g.next(); while (!r.done) r = g.next(); found.push(...r.value); }
    W.problems = found; W.checked = true; showCheck(ctx);
  }
  function showProblem(ctx, p) {
    W.list.hidden = true;
    const go = () => {
      const ids = (p.ids || []).filter(id => ctx.entry(id));
      if (ids.length) ctx.select(ids);
      if (p.box) {
        const pad = 1.2 * PT, b = [p.box[0] - pad, p.box[1] - pad, p.box[2] + pad, p.box[3] + pad];
        if (typeof ctx.zoomToBox === 'function') ctx.zoomToBox(b);
        W.mark = { face: p.face, box: p.box, items: ctx.items() }; ctx.redraw();
      }
      ctx.say(p.text, 4200);
    };
    if (p.face !== ctx.face()) { const b = ctx.root.querySelector(`.ce-head [data-face="${p.face}"]`); if (b) b.click(); requestAnimationFrame(go); } else go();
  }

  /* ── 6e · the workspace's own wiring: the bar, the chip and Save's badge, the overlays ── */
  function onOpen(ctx) {
    if (!doc) return;
    injectCss();
    W = { ctx, gen: 0, checkT: 0, cache: {}, problems: [], checked: false, mark: null, outline: false, items: null, face: ctx.face() };
    W.bar = buildBar(ctx);
    const foot = ctx.root.querySelector('.ce-foot'), actions = foot && foot.querySelector('.ce-actions');
    if (foot && actions) {
      const chip = doc.createElement('button'); chip.type = 'button'; chip.className = 'ce-prop ce-drop__b cx-chk'; chip.hidden = true; chip.setAttribute('data-cx-check', '');
      chip.innerHTML = '<i aria-hidden="true"></i><span></span>';
      foot.insertBefore(chip, actions); W.chip = chip;
      const list = doc.createElement('div'); list.className = 'ce-menu cx-list'; list.setAttribute('role', 'menu'); list.hidden = true;
      ctx.root.querySelector('.ce-win').appendChild(list); W.list = list;
      chip.addEventListener('click', e => { e.stopPropagation(); openList(ctx); });
      list.addEventListener('click', e => { const b = e.target.closest('[data-cx-show]'); if (b) { const p = W.problems[+b.dataset.cxShow]; if (p) showProblem(ctx, p); } });
      const save = actions.querySelector('[data-act="save"]');
      if (save) { const b = doc.createElement('i'); b.className = 'cx-badge'; b.hidden = true; save.appendChild(b); W.badge = b; }
    }
    W.items = ctx.items();
    syncBar(ctx);
    scheduleCheck(ctx, 450);
  }
  function onChange(ctx) {
    if (!W || W.ctx !== ctx) return;
    syncBar(ctx);
    if (W.items !== ctx.items() || W.face !== ctx.face()) {
      const faceChanged = W.face !== ctx.face();
      W.items = ctx.items(); W.face = ctx.face();
      if (W.mark && (W.mark.items !== ctx.items() || W.mark.face !== ctx.face())) W.mark = null;
      if (!faceChanged || !W.checked) scheduleCheck(ctx);
    }
  }
  function onClose(ctx, result) {
    if (!W || W.ctx !== ctx) return;
    clearTimeout(W.checkT); W.gen++;
    if (result) log('designCheck', { problems: W.problems.length, kinds: W.problems.reduce((o, p) => { o[p.kind] = (o[p.kind] || 0) + 1; return o; }, {}), checked: W.checked });
    W = null;
  }
  /** Over the charm: the path of the words selected (a dashed line, their anchor a dot) and the spot a Show points at. */
  function overlay(ctx) {
    if (!W || W.ctx !== ctx) return '';
    let h = '';
    const V = ctx.view();
    for (const id of ctx.selection()) {
      const e = ctx.entry(id); if (!e || !isBent(e.it)) continue;
      const G = guideOf(e.it.pathD); if (!G) continue;
      const m = M.mul(V, M.mul(e.P, ctx.geo.matrix(e.it))), a = G.fwd.point(G.L * num(e.it.pathAt, 0.5)), q = M.ap(m, a.x, a.y);
      h += `<path class="cx-guide" d="${M.dOf(G.sps)}" transform="matrix(${m.map(v => +v.toFixed(6)).join(' ')})" vector-effect="non-scaling-stroke"/><circle class="cx-anchor" cx="${q[0].toFixed(1)}" cy="${q[1].toFixed(1)}" r="3.5"/>`;
    }
    if (W.mark && W.mark.face === ctx.face()) {
      const b = W.mark.box, a = ctx.stageOf(b[0], b[1]), c = ctx.stageOf(b[2], b[3]), pad = 7;
      h += `<rect class="cx-mark" x="${(a[0] - pad).toFixed(1)}" y="${(a[1] - pad).toFixed(1)}" width="${(c[0] - a[0] + 2 * pad).toFixed(1)}" height="${(c[1] - a[1] + 2 * pad).toFixed(1)}" rx="4"/>`;
    }
    return h;
  }

  /* ── registering (the header's EXTENSION POINTS; the lists are read each time a workspace opens) ── */
  const presets = ['rrect', 'star', 'heart', 'ring'].map(presetTool);
  CE.tools = Array.isArray(CE.tools) ? CE.tools : [];
  CE.tools.push(...presets, bailTool);
  CE.commands = CE.commands && typeof CE.commands === 'object' ? CE.commands : {};
  Object.assign(CE.commands, {
    'cx.clip': { title: 'Clip to the charm', group: 'Clipping mask', key: 'Mod+7', icon: IC.clip, enabled: ctx => hasCut(ctx) || ctx.selection().length >= 2, run: clipRun },
    'cx.unclip': { title: 'Release the clip', group: 'Clipping mask', key: 'Mod+Alt+7', icon: IC.unclip, enabled: anyClipped, run: releaseRun },
    'cx.onPath': { title: 'Words along the path', group: 'Type', icon: IC.onPath, enabled: ctx => !!textAndPath(ctx), run: onPathRun },
    'cx.offPath': { title: 'Words off the path', group: 'Type', icon: IC.offPath, enabled: ctx => sel(ctx).some(e => isBent(e.it)), run: offPathRun },
    'cx.jumpRing': { title: 'Jump ring on top', group: 'Shapes', icon: IC.bail, enabled: ctx => ctx.selection().length > 0, run: ctx => { bailOnTop(ctx); } },
    'cx.outline': { title: 'Outline view', group: 'View', key: 'Mod+Y', icon: IC.outline, context: false, run: outlineToggle },
    'cx.check': { title: 'Laser check', group: 'View', icon: IC.check, context: false, run: ctx => { if (W) { if (W.list && !W.list.hidden) W.list.hidden = true; Promise.resolve().then(() => openList(ctx)); } } }
  });
  for (const k of ['onOpen', 'onChange', 'onClose', 'overlays']) if (!Array.isArray(CE[k])) CE[k] = [];
  CE.onOpen.push(onOpen); CE.onChange.push(onChange); CE.onClose.push(onClose); CE.overlays.push(overlay);
  CE.textLayout = textLayout;

  return {
    ready: true, version: 1, RING, LIMIT, SHAPES, rrectSP, starUnit, HEART, fitTo, ringSP, bailSP, shapeItem, shapeOf, reshape, dragBox, topAt, sigOf,
    textLayout, letters, dStr, parseD, guideOf, orient, onPathItem, offPathItems, flipPatch, capOf, capBand,
    ringsOf, metalOf, thinParts, clipPlan, clipLeaf, releasePlan, checkSteps, checkFace, leavesOf, minCapMm,
    current: () => W
  };
});
